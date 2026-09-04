"""
Glue Streaming Job - Multi-Topic CDC with DQ, Transforms, and SCD Type 2.

Reads from multiple Kafka topics (one per table), applies:
1. Schema from S3 config
2. DQ rules from S3 config (quarantine failures)
3. Transforms from S3 config
4. SCD Type 2 MERGE to Delta Lake

Architecture:
- One streaming query per table (parallel execution)
- Config-driven: schema, DQ rules, transforms all in tables.yaml
- MERGE with SCD Type 2 (effective dates, is_current flag)
"""

import json
import logging
import os
import re
import sys
from typing import Any, Dict, List

import boto3
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, from_json, when
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, DecimalType, DateType, BooleanType
)

# Native AWS Glue Data Quality analyzer (DQDL + EvaluateDataQuality).
# Ships with the Glue 6.0 runtime - no external JARs or wheels required.
from glue_dq_analyzer import GlueDQAnalyzer, create_dq_analyzer

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GlueStreamingJob")

logger.info("Native Glue Data Quality analyzer loaded - DQDL metrics enabled")


def log_dataframe_sample(df: DataFrame, label: str, table_name: str,
                         num_rows: int = 3) -> None:
    """Log a sample of DataFrame rows for debugging."""
    try:
        count = df.count()
        if count == 0:
            logger.info("[%s] %s: 0 rows", table_name, label)
            return
        logger.info("[%s] %s: %d rows, schema: %s",
                    table_name, label, count, df.columns)
        sample_rows = df.limit(num_rows).collect()
        for i, row in enumerate(sample_rows):
            logger.info("[%s] %s row[%d]: %s", table_name, label, i, row.asDict())
    except Exception as exc:
        logger.warning("[%s] Could not log %s: %s", table_name, label, exc)


# ============================================================================
# DQ Registry
# ============================================================================
class DQRegistry:
    """Registry of DQ rule implementations."""

    def __init__(self):
        self._rules = {
            "range": self._range_check,
            "allowed_values": self._allowed_values_check,
            "regex": self._regex_check,
            "not_null": self._not_null_check,
            "unique": self._unique_check,
            "length": self._length_check,
        }
        self._builtin_rules = set(self._rules.keys())

    def register(self, rule_type: str, handler) -> None:
        """Register a custom DQ rule handler. Built-in rules cannot be overwritten."""
        if rule_type in self._builtin_rules:
            logger.warning("Cannot overwrite built-in DQ rule: %s", rule_type)
            return
        if rule_type in self._rules:
            logger.warning("Overwriting existing registered DQ rule: %s", rule_type)
        self._rules[rule_type] = handler

    def list_rules(self) -> List[str]:
        """Return a list of all built-in and registered rule type names."""
        return list(self._rules.keys())

    def apply(self, dataframe: DataFrame, rule_type: str, column: str,
              params: Dict[str, Any], table_name: str, rule_id: str) -> tuple:
        """Apply a DQ rule. Returns (passed_df, failed_df)."""
        if rule_type not in self._rules:
            logger.warning("[%s] Unknown DQ rule type: %s, skipping",
                           table_name, rule_type)
            return dataframe, dataframe.limit(0)
        try:
            logger.info("[%s] Applying DQ rule '%s': type=%s, column=%s, params=%s",
                        table_name, rule_id, rule_type, column, params)
            return self._rules[rule_type](dataframe, column, params)
        except Exception as exc:
            logger.error("[%s] DQ rule %s failed: %s", table_name, rule_type, exc)
            return dataframe, dataframe.limit(0)

    def _range_check(self, dataframe: DataFrame, column: str,
                     params: Dict[str, Any]) -> tuple:
        """Check column value is within min/max range."""
        cond = col(column).isNotNull()
        if "min" in params:
            cond = cond & (col(column) >= params["min"])
        if "max" in params:
            cond = cond & (col(column) <= params["max"])
        return dataframe.filter(cond), dataframe.filter(~cond)

    def _allowed_values_check(self, dataframe: DataFrame, column: str,
                              params: Dict[str, Any]) -> tuple:
        """Check column value is in allowed list."""
        allowed = params.get("values", [])
        if not allowed:
            return dataframe, dataframe.limit(0)
        cond = col(column).isin(allowed)
        return dataframe.filter(cond), dataframe.filter(~cond)

    def _regex_check(self, dataframe: DataFrame, column: str,
                     params: Dict[str, Any]) -> tuple:
        """Check column value matches regex pattern."""
        pattern = params.get("pattern", ".*")
        cond = col(column).rlike(pattern)
        return dataframe.filter(cond), dataframe.filter(~cond)

    def _not_null_check(self, dataframe: DataFrame, column: str,
                        params: Dict[str, Any]) -> tuple:
        """Partition into records where column is not null (passed) vs null (failed)."""
        cond = col(column).isNotNull()
        return dataframe.filter(cond), dataframe.filter(~cond)

    def _unique_check(self, dataframe: DataFrame, column: str,
                      params: Dict[str, Any]) -> tuple:
        """Partition into records with unique column values (passed) vs duplicates (failed)."""
        from pyspark.sql.functions import count as spark_count

        window = Window.partitionBy(column)
        df_with_count = dataframe.withColumn("_dup_count", spark_count("*").over(window))
        passed_df = df_with_count.filter(col("_dup_count") == 1).drop("_dup_count")
        failed_df = df_with_count.filter(col("_dup_count") > 1).drop("_dup_count")
        return passed_df, failed_df

    def _length_check(self, dataframe: DataFrame, column: str,
                      params: Dict[str, Any]) -> tuple:
        """Partition into records where string length is within min/max (passed) vs outside (failed)."""
        from pyspark.sql.functions import length as str_length

        col_len = str_length(col(column))
        cond = col(column).isNotNull()
        if "min" in params:
            cond = cond & (col_len >= params["min"])
        if "max" in params:
            cond = cond & (col_len <= params["max"])
        return dataframe.filter(cond), dataframe.filter(~cond)


# ============================================================================
# Transform Registry
# ============================================================================
class TransformRegistry:
    """Registry of transform implementations."""

    def __init__(self):
        from pyspark.sql.functions import trim, lower, upper, round as spark_round
        from pyspark.sql.functions import concat, substring

        self._trim = trim
        self._lower = lower
        self._upper = upper
        self._round = spark_round
        self._concat = concat
        self._substring = substring

        self._transforms = {
            "trim": self._trim_transform,
            "lower": self._lower_transform,
            "upper": self._upper_transform,
            "round": self._round_transform,
            "mask_pii": self._mask_pii_transform,
            "cast": self._cast_transform,
            "default_value": self._default_value_transform,
            "rename": self._rename_transform,
        }
        self._builtin_transforms = set(self._transforms.keys())

    def register(self, transform_type: str, handler) -> None:
        """Register a custom transform handler. Built-in transforms cannot be overwritten."""
        if transform_type in self._builtin_transforms:
            logger.warning("Cannot overwrite built-in transform: %s", transform_type)
            return
        if transform_type in self._transforms:
            logger.warning("Overwriting existing registered transform: %s", transform_type)
        self._transforms[transform_type] = handler

    def list_transforms(self) -> List[str]:
        """Return a list of all built-in and registered transform type names."""
        return list(self._transforms.keys())

    def apply(self, dataframe: DataFrame, transform_type: str, column: str,
              params: Dict[str, Any], table_name: str, transform_id: str) -> DataFrame:
        """Apply a transform. Returns modified DataFrame."""
        if transform_type not in self._transforms:
            logger.warning("[%s] Unknown transform type: %s, skipping",
                           table_name, transform_type)
            return dataframe
        if column not in dataframe.columns:
            logger.warning("[%s] Column %s not found, skipping transform",
                           table_name, column)
            return dataframe
        try:
            logger.info("[%s] Applying transform '%s': type=%s, column=%s",
                        table_name, transform_id, transform_type, column)
            return self._transforms[transform_type](dataframe, column, params)
        except Exception as exc:
            logger.error("[%s] Transform %s failed: %s", table_name, transform_type, exc)
            return dataframe

    def _trim_transform(self, dataframe: DataFrame, column: str,
                        params: Dict[str, Any]) -> DataFrame:
        return dataframe.withColumn(column, self._trim(col(column)))

    def _lower_transform(self, dataframe: DataFrame, column: str,
                         params: Dict[str, Any]) -> DataFrame:
        return dataframe.withColumn(column, self._lower(col(column)))

    def _upper_transform(self, dataframe: DataFrame, column: str,
                         params: Dict[str, Any]) -> DataFrame:
        return dataframe.withColumn(column, self._upper(col(column)))

    def _round_transform(self, dataframe: DataFrame, column: str,
                         params: Dict[str, Any]) -> DataFrame:
        decimals = params.get("decimals", 2)
        return dataframe.withColumn(column, self._round(col(column), decimals))

    def _mask_pii_transform(self, dataframe: DataFrame, column: str,
                            params: Dict[str, Any]) -> DataFrame:
        visible = params.get("visible_chars", 4)
        return dataframe.withColumn(
            column,
            self._concat(self._substring(col(column), 1, visible), lit("****"))
        )

    def _cast_transform(self, dataframe: DataFrame, column: str,
                        params: Dict[str, Any]) -> DataFrame:
        """Cast column to specified Spark type via target_type param."""
        spark_type = build_spark_type(params["target_type"])
        return dataframe.withColumn(column, col(column).cast(spark_type))

    def _default_value_transform(self, dataframe: DataFrame, column: str,
                                 params: Dict[str, Any]) -> DataFrame:
        """Replace null values in column with specified default value."""
        return dataframe.withColumn(
            column,
            when(col(column).isNull(), lit(params["value"])).otherwise(col(column))
        )

    def _rename_transform(self, dataframe: DataFrame, column: str,
                          params: Dict[str, Any]) -> DataFrame:
        """Rename column to new_name param."""
        return dataframe.withColumnRenamed(column, params["new_name"])


# Global registries
DQ_REGISTRY = None
TRANSFORM_REGISTRY = None


def get_registries():
    """Get or create global registries."""
    global DQ_REGISTRY, TRANSFORM_REGISTRY
    if DQ_REGISTRY is None:
        DQ_REGISTRY = DQRegistry()
    if TRANSFORM_REGISTRY is None:
        TRANSFORM_REGISTRY = TransformRegistry()
    return DQ_REGISTRY, TRANSFORM_REGISTRY



# ============================================================================
# Spark and Config
# ============================================================================
def get_spark() -> SparkSession:
    """Get or create Spark session with Delta Lake."""
    return SparkSession.builder \
        .appName("StreamingETL-DQ-Transform-SCD2") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def load_config_from_s3(s3_path: str) -> Dict[str, Any]:
    """Load YAML config from S3 and resolve placeholders."""
    logger.info("Loading config from: %s", s3_path)
    s3 = boto3.client("s3")
    sts = boto3.client("sts")
    
    path = s3_path.replace("s3://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])
    
    # Get AWS account ID
    account_id = sts.get_caller_identity()["Account"]
    
    # Derive stack name from bucket name (format: <stack>-assets-<account>)
    stack_name = bucket.replace(f"-assets-{account_id}", "")
    
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    
    # Replace placeholders
    content = content.replace("${STACK_NAME}", stack_name)
    content = content.replace("${AWS_ACCOUNT_ID}", account_id)
    
    logger.info("Resolved config placeholders: STACK_NAME=%s, AWS_ACCOUNT_ID=%s", 
                stack_name, account_id)
    
    config = yaml.safe_load(content)
    
    # Store stack name in config for downstream use
    config["_stack_name"] = stack_name
    
    # Resolve Kafka bootstrap servers from MSK
    if config.get("kafka", {}).get("bootstrap_servers") == "PLACEHOLDER_BOOTSTRAP_SERVERS":
        bootstrap = get_msk_bootstrap_servers(stack_name)
        config["kafka"]["bootstrap_servers"] = bootstrap
        logger.info("Resolved MSK bootstrap servers: %s", bootstrap)
    
    return config


def get_msk_bootstrap_servers(stack_name: str) -> str:
    """Get MSK bootstrap servers from the cluster."""
    kafka_client = boto3.client("kafka")
    
    # List clusters and find the one matching our stack
    clusters = kafka_client.list_clusters()
    for cluster in clusters.get("ClusterInfoList", []):
        if stack_name in cluster.get("ClusterName", ""):
            cluster_arn = cluster["ClusterArn"]
            response = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
            bootstrap = response.get("BootstrapBrokerStringSaslScram", "")
            if bootstrap:
                return bootstrap
    
    raise ValueError(f"Could not find MSK cluster for stack: {stack_name}")


def get_kafka_password(config: Dict) -> str:
    """Get Kafka password from Secrets Manager."""
    client = boto3.client("secretsmanager")
    stack_name = config.get("_stack_name", "")
    
    try:
        # Try the expected secret name first (CloudFormation creates this)
        if stack_name:
            secret_name = f"AmazonMSK_{stack_name}_scram"
            try:
                response = client.get_secret_value(SecretId=secret_name)
                secret = json.loads(response["SecretString"])
                logger.info("Found Kafka secret: %s", secret_name)
                return secret.get("password", "")
            except client.exceptions.ResourceNotFoundException:
                logger.info("Secret %s not found, searching...", secret_name)
        
        # The secret name is deterministic (created by CloudFormation as
        # AmazonMSK_{stack}_scram). No list_secrets fallback: it would require
        # a broad secretsmanager:ListSecrets grant, and a missing secret is a
        # deployment error that should fail loudly, not be papered over.
        raise ValueError(
            f"MSK SCRAM secret 'AmazonMSK_{stack_name}_scram' not found - "
            "verify the CloudFormation stack created it"
        )
    except Exception as e:
        logger.error("Failed to get Kafka password: %s", e)
        raise


# ============================================================================
# Schema Building
# ============================================================================
def build_spark_type(type_str: str):
    """Convert config type string to Spark type."""
    from pyspark.sql.types import DoubleType
    type_lower = type_str.lower()
    if type_lower in ("string", "varchar", "text"):
        return StringType()
    if type_lower in ("integer", "int"):
        return IntegerType()
    if type_lower in ("long", "bigint"):
        return LongType()
    if type_lower in ("double", "float", "real"):
        return DoubleType()
    if type_lower == "timestamp":
        return TimestampType()
    if type_lower == "date":
        return DateType()
    if type_lower == "boolean":
        return BooleanType()
    if type_lower.startswith("decimal"):
        match = re.match(r"decimal\((\d+),(\d+)\)", type_lower)
        if match:
            return DecimalType(int(match.group(1)), int(match.group(2)))
        return DecimalType(10, 2)
    return StringType()


def build_schema_from_config(schema_config: List[Dict]) -> StructType:
    """Build Spark StructType from config schema definition."""
    fields = []
    for field_def in schema_config:
        name = field_def["name"]
        spark_type = build_spark_type(field_def["type"])
        nullable = field_def.get("nullable", True)
        fields.append(StructField(name, spark_type, nullable))
    return StructType(fields)


def build_dms_envelope_schema(data_schema: StructType) -> StructType:
    """Build schema for DMS CDC envelope with nested data."""
    metadata_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("record-type", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("schema-name", StringType(), True),
        StructField("table-name", StringType(), True),
    ])
    return StructType([
        StructField("data", data_schema, True),
        StructField("metadata", metadata_schema, True),
    ])



# ============================================================================
# Kafka Options
# ============================================================================
def get_kafka_options(config: Dict, topic: str, password: str) -> Dict:
    """Build Kafka connection options."""
    kafka_config = config["kafka"]
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{kafka_config["sasl_username"]}" '
        f'password="{password}";'
    )
    return {
        "kafka.bootstrap.servers": kafka_config["bootstrap_servers"],
        "subscribe": topic,
        "kafka.security.protocol": kafka_config["security_protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
        "kafka.sasl.jaas.config": jaas,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


# ============================================================================
# Data Processing Functions
# ============================================================================
def parse_dms_messages(batch_df: DataFrame, table_config: Dict,
                       table_name: str) -> DataFrame:
    """Parse DMS CDC messages and add operation column."""
    data_schema = build_schema_from_config(table_config["schema"])
    envelope_schema = build_dms_envelope_schema(data_schema)

    parsed_df = batch_df.select(
        from_json(col("value").cast("string"), envelope_schema).alias("parsed")
    ).select(
        col("parsed.data.*"),
        col("parsed.metadata.operation").alias("_dms_op"),
    )

    result_df = parsed_df.withColumn(
        "_operation",
        when(col("_dms_op") == "insert", "INSERT")
        .when(col("_dms_op") == "update", "UPDATE")
        .when(col("_dms_op") == "delete", "DELETE")
        .when(col("_dms_op") == "load", "INSERT")
        .otherwise("UNKNOWN")
    ).drop("_dms_op")

    # Filter out rows where all data columns are null (control messages)
    data_cols = [f["name"] for f in table_config["schema"]]
    not_null_cond = None
    for c in data_cols:
        if not_null_cond is None:
            not_null_cond = col(c).isNotNull()
        else:
            not_null_cond = not_null_cond | col(c).isNotNull()

    if not_null_cond is not None:
        result_df = result_df.filter(not_null_cond)

    return result_df


def apply_dq_rules(dataframe: DataFrame, table_config: Dict,
                   dq_registry: DQRegistry, table_name: str) -> tuple:
    """Apply DQ rules from config. Returns (valid_df, failed_df)."""
    dq_rules = table_config.get("dq_rules", [])
    if not dq_rules:
        logger.info("[%s] No DQ rules configured", table_name)
        return dataframe, dataframe.limit(0)

    logger.info("[%s] Applying %d DQ rules", table_name, len(dq_rules))

    valid_df = dataframe
    all_failed = None

    for rule in dq_rules:
        rule_id = rule.get("id", "unknown")
        rule_type = rule.get("type")
        column = rule.get("column")
        params = rule.get("params", {})
        severity = rule.get("severity", "error")

        if not rule_type or not column:
            continue

        passed, failed = dq_registry.apply(valid_df, rule_type, column, params,
                                           table_name, rule_id)

        if failed.count() > 0 and severity == "error":
            failed_with_meta = failed.withColumn("_failed_rule", lit(rule_id))
            if all_failed is None:
                all_failed = failed_with_meta
            else:
                all_failed = all_failed.unionByName(failed_with_meta, allowMissingColumns=True)
            valid_df = passed

    return valid_df, all_failed if all_failed else dataframe.limit(0)



def apply_transforms(dataframe: DataFrame, table_config: Dict,
                     transform_registry: TransformRegistry,
                     table_name: str) -> DataFrame:
    """Apply transforms from config in order."""
    transforms = table_config.get("transforms", [])
    if not transforms:
        logger.info("[%s] No transforms configured", table_name)
        return dataframe

    logger.info("[%s] Applying %d transforms", table_name, len(transforms))
    sorted_transforms = sorted(transforms, key=lambda x: x.get("order", 0))

    result = dataframe
    for tx in sorted_transforms:
        tx_id = tx.get("id", "unknown")
        tx_type = tx.get("type")
        column = tx.get("column")
        params = tx.get("params", {})

        if not tx_type or not column:
            continue

        result = transform_registry.apply(result, tx_type, column, params,
                                          table_name, tx_id)

    return result


def write_to_quarantine(failed_df: DataFrame, table_name: str,
                        quarantine_path: str) -> Dict[str, Any]:
    """Write failed records to quarantine bucket with hourly run_date partition.
    
    Returns quarantine metadata dict for SNS notification, or empty dict if nothing quarantined.
    """
    if failed_df.isEmpty():
        return {}

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    run_date = now.strftime("%Y-%m-%dT%H:00:00")
    path = "{}run_date={}/{}/".format(quarantine_path, run_date, table_name)
    count = failed_df.count()

    # Collect which rules failed (distinct _failed_rule values)
    failed_rules = []
    if "_failed_rule" in failed_df.columns:
        rule_rows = failed_df.select("_failed_rule").distinct().collect()
        failed_rules = [row["_failed_rule"] for row in rule_rows if row["_failed_rule"]]

    failed_df.withColumn("_quarantined_at", current_timestamp()) \
        .write.mode("append").parquet(path)
    logger.info("[%s] QUARANTINED %d records to %s", table_name, count, path)

    return {
        "table": table_name,
        "quarantined_count": count,
        "failed_rules": failed_rules,
        "s3_path": path,
        "timestamp": now.isoformat(),
    }


def add_scd2_columns(dataframe: DataFrame) -> DataFrame:
    """Add SCD Type 2 metadata columns."""
    now = current_timestamp()
    return dataframe \
        .withColumn("_effective_from", now) \
        .withColumn("_effective_to", lit(None).cast("timestamp")) \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_ingested_at", now)


def send_quarantine_notification(
    quarantine_info: Dict[str, Any],
    batch_id: int,
    total_records: int,
    config: Dict[str, Any],
) -> None:
    """Send SNS notification for quarantined records."""
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    if not sns_topic_arn or not quarantine_info:
        return

    try:
        stack_name = config.get("_stack_name", "unknown")
        table = quarantine_info["table"]
        count = quarantine_info["quarantined_count"]
        rules = quarantine_info.get("failed_rules", [])
        s3_path = quarantine_info["s3_path"]
        ts = quarantine_info["timestamp"]

        rules_str = ", ".join(rules) if rules else "unknown"

        body = (
            "Stack: {stack}\n"
            "Batch ID: {batch}\n"
            "Timestamp: {ts}\n"
            "\n"
            "QUARANTINE SUMMARY\n"
            "{sep}\n"
            "Table: {table}\n"
            "  Total Records in Batch: {total}\n"
            "  Quarantined: {count} records\n"
            "  Failed Rules: {rules}\n"
            "  S3 Path: {path}\n"
            "{sep}\n"
        ).format(
            stack=stack_name,
            batch=batch_id,
            ts=ts,
            sep="─" * 50,
            table=table,
            total=total_records,
            count=count,
            rules=rules_str,
            path=s3_path,
        )

        subject = "DQ Failure Notification — {}".format(stack_name)
        # SNS subject max 100 chars
        if len(subject) > 100:
            subject = subject[:97] + "..."

        sns = boto3.client("sns")
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=body,
        )
        logger.info("[%s] SNS notification sent: %d quarantined records", table, count)
    except Exception as exc:
        logger.warning("Failed to send SNS notification: %s", exc)


def deduplicate_by_pk(dataframe: DataFrame, primary_key: str) -> DataFrame:
    """Deduplicate DataFrame by primary key, keeping latest record."""
    from pyspark.sql.functions import row_number
    window = Window.partitionBy(primary_key).orderBy(col("_ingested_at").desc())
    return dataframe.withColumn("_rn", row_number().over(window)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")



# ============================================================================
# Main Batch Processing
# ============================================================================
def process_batch(batch_df: DataFrame, batch_id: int, table_config: Dict,
                  config: Dict, spark: SparkSession, dq_analyzer=None) -> None:
    """Process a micro-batch with DQ, transforms, and SCD Type 2."""
    from delta.tables import DeltaTable

    table_name = table_config["name"]
    delta_path = table_config["delta_path"]
    pk = table_config["primary_key"]
    quarantine_path = config["settings"].get("quarantine_path", "")

    logger.info("[%s] BATCH %d START", table_name, batch_id)

    if batch_df.isEmpty():
        logger.info("[%s] Empty batch, skipping", table_name)
        return

    dq_registry, transform_registry = get_registries()

    # Step 1: Parse DMS envelope
    parsed_df = parse_dms_messages(batch_df, table_config, table_name)
    if parsed_df.count() == 0:
        logger.info("[%s] No data records after parsing", table_name)
        return

    # Step 2: Native Glue Data Quality evaluation (DQDL ruleset per table).
    # Results publish to CloudWatch + Glue Studio natively, and are appended
    # to the Delta metrics table for Athena trend queries.
    quality_checks = table_config.get("quality_checks", [])
    if dq_analyzer and quality_checks:
        metrics_df = dq_analyzer.analyze_batch(parsed_df, table_name, batch_id, quality_checks)
        if not metrics_df.isEmpty():
            dq_analyzer.write_metrics(metrics_df)

    # Step 3: Apply DQ rules
    valid_df, failed_df = apply_dq_rules(parsed_df, table_config, dq_registry, table_name)
    
    total_records = parsed_df.count()
    if quarantine_path and not failed_df.isEmpty():
        quarantine_info = write_to_quarantine(failed_df, table_name, quarantine_path)
        if quarantine_info:
            send_quarantine_notification(quarantine_info, batch_id, total_records, config)

    if valid_df.count() == 0:
        logger.info("[%s] No valid records after DQ", table_name)
        return

    # Step 4: Apply transforms
    transformed_df = apply_transforms(valid_df, table_config, transform_registry, table_name)

    # Step 5: Add SCD2 columns
    staged_df = add_scd2_columns(transformed_df)

    # Step 6: Deduplicate
    staged_df = deduplicate_by_pk(staged_df, pk)
    final_count = staged_df.count()

    if final_count == 0:
        return

    # Step 7: Write to Delta
    if not DeltaTable.isDeltaTable(spark, delta_path):
        inserts = staged_df.filter(col("_operation") == "INSERT")
        if inserts.count() > 0:
            logger.info("[%s] Creating Delta table at %s", table_name, delta_path)
            inserts.write.format("delta").mode("overwrite").save(delta_path)
            # Generate symlink manifest for Athena compatibility
            try:
                dt = DeltaTable.forPath(spark, delta_path)
                dt.generate("symlink_format_manifest")
                logger.info("[%s] Generated symlink manifest for Athena", table_name)
            except Exception as exc:
                logger.warning("[%s] Failed to generate manifest: %s", table_name, exc)
        return

    # SCD2 MERGE
    delta_table = DeltaTable.forPath(spark, delta_path)
    merge_cond = f"target.{pk} = source.{pk} AND target._is_current = true"

    # Close old records for updates/deletes
    updates_deletes = staged_df.filter(col("_operation").isin("UPDATE", "DELETE"))
    if updates_deletes.count() > 0:
        delta_table.alias("target").merge(
            updates_deletes.alias("source"), merge_cond
        ).whenMatchedUpdate(set={
            "_is_current": lit(False),
            "_effective_to": col("source._effective_from"),
        }).execute()

    # Insert new records
    new_records = staged_df.filter(col("_operation").isin("INSERT", "UPDATE"))
    if new_records.count() > 0:
        new_records.write.format("delta").mode("append").save(delta_path)

    # Regenerate symlink manifest for Athena after every write
    try:
        delta_table.generate("symlink_format_manifest")
    except Exception as exc:
        logger.warning("[%s] Failed to generate manifest: %s", table_name, exc)

    logger.info("[%s] BATCH %d COMPLETE - %d records", table_name, batch_id, final_count)



# ============================================================================
# Stream Management
# ============================================================================
def start_stream_for_table(spark: SparkSession, config: Dict,
                           table_config: Dict, kafka_password: str,
                           dq_analyzer=None) -> Any:
    """Start a streaming query for a single table."""
    table_name = table_config["name"]
    topic = table_config["topic"]
    checkpoint = config["settings"]["checkpoint_location"] + table_name + "/"

    logger.info("[%s] Starting stream from topic: %s", table_name, topic)

    kafka_options = get_kafka_options(config, topic, kafka_password)

    stream_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    def batch_processor(df, bid):
        process_batch(df, bid, table_config, config, spark, dq_analyzer)

    query = stream_df.writeStream \
        .foreachBatch(batch_processor) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime=config["settings"]["trigger_interval"]) \
        .queryName("stream_" + table_name) \
        .start()

    logger.info("[%s] Stream started, query ID: %s", table_name, query.id)
    return query


def main():
    """Main entry point."""
    logger.info("GLUE STREAMING ETL - DQ + TRANSFORMS + SCD2")

    # CONFIG_PATH is required — fail loudly if missing. A silent fallback to a
    # fixed, globally-predictable bucket name would be vulnerable to S3 bucket
    # squatting (an attacker pre-registering the name could serve malicious
    # config). CloudFormation always passes --CONFIG_PATH to this job.
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_PATH"])
    config_path = args["CONFIG_PATH"]

    # Read optional SNS_TOPIC_ARN (separate try-except since it's optional)
    try:
        from awsglue.utils import getResolvedOptions
        sns_args = getResolvedOptions(sys.argv, ["SNS_TOPIC_ARN"])
        os.environ["SNS_TOPIC_ARN"] = sns_args["SNS_TOPIC_ARN"]
        logger.info("SNS topic ARN configured: %s", sns_args["SNS_TOPIC_ARN"])
    except Exception:
        logger.info("SNS_TOPIC_ARN not configured, DQ notifications disabled")

    logger.info("Config path: %s", config_path)

    spark = get_spark()
    config = load_config_from_s3(config_path)
    kafka_password = get_kafka_password(config)

    get_registries()

    # Initialize the native Glue Data Quality analyzer. Stateless per batch —
    # no S3 state directory needed (trends come from CloudWatch + the Delta
    # metrics table).
    try:
        delta_bucket = config["settings"]["delta_bucket"]
    except KeyError:
        raise ValueError(
            "settings.delta_bucket is required in the config — no default is "
            "provided, to avoid S3 bucket squatting on a predictable name"
        )
    dq_metrics_path = f"s3://{delta_bucket}/dq_metrics/"
    dq_results_prefix = f"s3://{delta_bucket}/dq_native_results/"
    dq_analyzer = create_dq_analyzer(spark, dq_metrics_path, dq_results_prefix)
    logger.info("Glue DQ analyzer initialized - metrics: %s", dq_metrics_path)

    tables = config.get("tables", [])
    streaming_tables = [t for t in tables if not t.get("internal", False)]
    logger.info("Found %d tables to process", len(streaming_tables))

    queries = []
    for table_config in streaming_tables:
        try:
            query = start_stream_for_table(spark, config, table_config, kafka_password,
                                           dq_analyzer)
            queries.append(query)
        except Exception as exc:
            logger.error("Failed to start stream for %s: %s", table_config["name"], exc)

    logger.info("Started %d streaming queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
