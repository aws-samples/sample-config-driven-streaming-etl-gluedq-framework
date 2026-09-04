"""
Native AWS Glue Data Quality analyzer for the Streaming ETL Framework.

Compiles YAML quality checks into DQDL (Data Quality Definition Language)
rulesets and evaluates them per micro-batch with the built-in
EvaluateDataQuality transform (awsgluedq) — no external JARs or wheels.

Results are:
- published to CloudWatch metrics + the Glue Studio Data Quality tab
  (native publishingOptions), and
- appended to a Delta Lake metrics table so quality trends stay queryable
  in Athena (so batch-level quality trends stay SQL-queryable).

Glue 6.0 runtime: Spark 4.1 / Python 3.13.
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dqdl_compiler import SUPPORTED_METRICS, build_dqdl_rules, build_dqdl_ruleset

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GlueDQAnalyzer")


# Schema for the DQ results DataFrame written to Delta (Athena-queryable)
METRICS_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("batch_id", LongType(), False),
    StructField("rule", StringType(), False),
    StructField("outcome", StringType(), False),
    StructField("failure_reason", StringType(), True),
    StructField("evaluated_metrics", StringType(), True),
    StructField("passed", BooleanType(), False),
    StructField("timestamp", TimestampType(), False),
])

class GlueDQAnalyzer:
    """
    Per-batch data quality evaluation using native AWS Glue Data Quality.

    Stateless by design: each micro-batch is evaluated independently, and
    trend analysis happens downstream (CloudWatch metrics + the Delta/Athena
    metrics table).
    """

    def __init__(
        self,
        spark: SparkSession,
        metrics_path: str,
        results_s3_prefix: str = ""
    ):
        """
        Args:
            spark: SparkSession instance
            metrics_path: S3 path for the Delta metrics table
            results_s3_prefix: optional S3 prefix where Glue DQ writes its
                native evaluation results (publishingOptions.resultsS3Prefix)
        """
        self.spark = spark
        self.metrics_path = metrics_path.rstrip("/") + "/"
        self.results_s3_prefix = results_s3_prefix
        self._glue_context = None
        self._warned_fallback = False

    def _get_glue_context(self):
        """Lazily create a GlueContext (import kept local so the DQDL compiler
        stays importable off-Glue, e.g. in local unit tests)."""
        if self._glue_context is None:
            from awsglue.context import GlueContext
            self._glue_context = GlueContext(self.spark.sparkContext)
        return self._glue_context

    def analyze_batch(
        self,
        df: DataFrame,
        table_name: str,
        batch_id: int,
        checks: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Evaluate the batch against the DQDL ruleset compiled from `checks`.

        Prefers the native EvaluateDataQuality transform. If the awsgluedq
        package is unavailable in the running interpreter — VERIFIED LIVE
        (2026-09-02): the Glue 6.0 gluestreaming runner ships without
        awsgluedq (present in the glueetl/batch environment) — the same DQDL
        rules are evaluated with equivalent Spark aggregations and published
        to CloudWatch directly, so the metrics layer works in both modes.

        Returns a DataFrame with METRICS_SCHEMA (empty on no rules/failure).
        """
        if df.isEmpty():
            logger.info("[%s] Empty DataFrame, skipping DQ evaluation", table_name)
            return self._create_empty_metrics_df()

        rule_pairs = build_dqdl_rules(checks)
        if not rule_pairs:
            logger.info("[%s] No quality checks configured", table_name)
            return self._create_empty_metrics_df()

        ruleset = "Rules = [" + ", ".join(rule for rule, _ in rule_pairs) + "]"
        logger.info("[%s] ===== GLUE DATA QUALITY START (batch %d) =====",
                    table_name, batch_id)
        logger.info("[%s] DQDL: %s", table_name, ruleset)

        try:
            metrics_df = self._evaluate_native(
                df, ruleset, table_name, batch_id
            )
        except (ImportError, ModuleNotFoundError):
            if not self._warned_fallback:
                logger.warning(
                    "awsgluedq is not available in this runtime (known gap in "
                    "the Glue 6.0 streaming runner) — evaluating DQDL rules "
                    "with the built-in Spark fallback and publishing metrics "
                    "to CloudWatch directly."
                )
                self._warned_fallback = True
            try:
                metrics_df = self._evaluate_fallback(
                    df, rule_pairs, table_name, batch_id
                )
            except Exception as exc:
                logger.error("[%s] Fallback DQ evaluation failed: %s",
                             table_name, exc)
                return self._create_empty_metrics_df()
        except Exception as exc:
            logger.error("[%s] Glue DQ evaluation failed: %s", table_name, exc)
            return self._create_empty_metrics_df()

        self._log_outcomes(metrics_df, rule_pairs, table_name)
        logger.info("[%s] ===== GLUE DATA QUALITY END =====", table_name)
        return metrics_df

    def _evaluate_native(
        self,
        df: DataFrame,
        ruleset: str,
        table_name: str,
        batch_id: int
    ) -> DataFrame:
        """Native path: the EvaluateDataQuality transform shipped with Glue."""
        from awsglue.dynamicframe import DynamicFrame
        from awsgluedq.transforms import EvaluateDataQuality

        glue_ctx = self._get_glue_context()
        dynf = DynamicFrame.fromDF(df, glue_ctx, f"dq_{table_name}_{batch_id}")

        publishing_options = {
            # Namespace metrics per table so CloudWatch dashboards can
            # track quality trends per stream.
            "dataQualityEvaluationContext": f"{table_name}",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        }
        if self.results_s3_prefix:
            publishing_options["resultsS3Prefix"] = self.results_s3_prefix

        dq_results = EvaluateDataQuality.apply(
            frame=dynf,
            ruleset=ruleset,
            publishing_options=publishing_options,
        )
        return self._results_to_dataframe(dq_results.toDF(), table_name, batch_id)

    def _evaluate_fallback(
        self,
        df: DataFrame,
        rule_pairs: List[Tuple[str, Dict[str, Any]]],
        table_name: str,
        batch_id: int
    ) -> DataFrame:
        """
        Evaluate the compiled DQDL rules with equivalent Spark aggregations:
          Completeness  -> non-null / total
          Uniqueness    -> values occurring exactly once / total
          ColumnValues matches -> regex matches / total
          RowCount      -> total row count
        Metrics are also published to CloudWatch (namespace
        StreamingETL/DataQuality) to mirror the native publishing behavior.
        """
        from pyspark.sql.functions import col as f_col

        total = df.count()
        rows = []
        now = datetime.now()
        cw_metrics = []

        for rule, check in rule_pairs:
            metric = str(check.get("metric", "")).lower()
            column = check.get("column")
            threshold = check.get("threshold")
            value = None

            if metric == "size":
                value = float(total)
                passed = total >= int(threshold)
            elif metric == "completeness":
                non_null = df.filter(f_col(column).isNotNull()).count()
                value = (non_null / total) if total else 0.0
                passed = value >= float(threshold)
            elif metric == "uniqueness":
                singletons = (df.groupBy(column).count()
                              .filter(f_col("count") == 1).count())
                value = (singletons / total) if total else 0.0
                passed = value >= float(threshold)
            elif metric == "compliance":
                pattern = check.get("pattern", ".*")
                matches = df.filter(f_col(column).rlike(pattern)).count()
                value = (matches / total) if total else 0.0
                passed = value >= float(threshold) if threshold is not None else matches == total
            else:  # pragma: no cover - compiler only emits the four above
                continue

            rows.append({
                "table_name": table_name,
                "batch_id": int(batch_id),
                "rule": rule,
                "outcome": "Passed" if passed else "Failed",
                "failure_reason": (
                    None if passed else
                    f"Value {value:.4f} below threshold {threshold} (Spark fallback)"
                ),
                "evaluated_metrics": str({f"{metric}.{column or '*'}": value}),
                "passed": passed,
                "timestamp": now,
            })
            cw_metrics.append({
                "MetricName": f"dq.{metric}",
                "Dimensions": [
                    {"Name": "table", "Value": table_name},
                    {"Name": "column", "Value": column or "*"},
                ],
                "Value": value,
            })

        self._publish_cloudwatch(cw_metrics, table_name)

        if not rows:
            return self._create_empty_metrics_df()
        return self.spark.createDataFrame(rows, METRICS_SCHEMA)

    def _publish_cloudwatch(self, metric_data: List[Dict], table_name: str) -> None:
        """Publish fallback metrics to CloudWatch (best effort)."""
        if not metric_data:
            return
        try:
            import boto3
            cw = boto3.client("cloudwatch")
            cw.put_metric_data(
                Namespace="StreamingETL/DataQuality",
                MetricData=metric_data,
            )
            logger.info("[%s] Published %d DQ metrics to CloudWatch",
                        table_name, len(metric_data))
        except Exception as exc:
            logger.warning("[%s] CloudWatch metric publish failed: %s",
                           table_name, exc)

    def _results_to_dataframe(
        self,
        results_df: DataFrame,
        table_name: str,
        batch_id: int
    ) -> DataFrame:
        """Convert EvaluateDataQuality output (Rule/Outcome/FailureReason/
        EvaluatedMetrics) into the framework metrics schema."""
        rows = []
        now = datetime.now()

        for row in results_df.collect():
            outcome = str(row["Outcome"]) if row["Outcome"] is not None else "Unknown"
            evaluated = row["EvaluatedMetrics"]
            rows.append({
                "table_name": table_name,
                "batch_id": int(batch_id),
                "rule": str(row["Rule"]),
                "outcome": outcome,
                "failure_reason": (
                    str(row["FailureReason"])
                    if row["FailureReason"] is not None else None
                ),
                "evaluated_metrics": str(dict(evaluated)) if evaluated else None,
                "passed": outcome == "Passed",
                "timestamp": now,
            })

        if not rows:
            return self._create_empty_metrics_df()
        return self.spark.createDataFrame(rows, METRICS_SCHEMA)

    def _log_outcomes(
        self,
        metrics_df: DataFrame,
        rule_pairs: List[Tuple[str, Dict[str, Any]]],
        table_name: str
    ) -> None:
        """Log failed rules at the severity configured in YAML."""
        if metrics_df.isEmpty():
            return

        severity_by_rule = {rule: check.get("severity", "warning")
                            for rule, check in rule_pairs}
        try:
            for row in metrics_df.collect():
                if row["passed"]:
                    logger.info("[%s] DQ PASS: %s", table_name, row["rule"])
                    continue
                message = (
                    f"[{table_name}] DQ RULE FAILED: {row['rule']} "
                    f"(reason: {row['failure_reason'] or 'n/a'})"
                )
                if severity_by_rule.get(row["rule"], "warning") == "error":
                    logger.error(message)
                else:
                    logger.warning(message)
        except Exception as exc:
            logger.error("[%s] Failed to log DQ outcomes: %s", table_name, exc)

    def write_metrics(self, metrics_df: DataFrame) -> None:
        """Append DQ results to the Delta metrics table (Athena-queryable)."""
        if metrics_df.isEmpty():
            logger.info("No DQ results to write")
            return

        try:
            metrics_with_ts = metrics_df.withColumn("_ingested_at", current_timestamp())
            # Unpartitioned by design: Delta stores partition values in paths
            # (not in the parquet files), so a partitioned table would surface
            # NULL table_name through Athena's symlink-manifest reader. The
            # table is small; partitioning buys nothing here.
            metrics_with_ts.write \
                .format("delta") \
                .mode("append") \
                .save(self.metrics_path)
            logger.info("Wrote %d DQ results to Delta table: %s",
                        metrics_df.count(), self.metrics_path)

            # Regenerate the symlink manifest so Athena sees new results
            # immediately (same pattern as the data tables in process_batch).
            try:
                from delta.tables import DeltaTable
                DeltaTable.forPath(self.spark, self.metrics_path) \
                    .generate("symlink_format_manifest")
            except Exception as exc:
                logger.warning("Failed to generate dq_metrics manifest: %s", exc)

        except Exception as exc:
            logger.error("Failed to write DQ results to Delta: %s", exc)
            try:
                # Distinct sibling location so the fallback never points at the
                # path the Delta write just failed on (review round-1 fix #6).
                fallback_path = self.metrics_path.rstrip("/") + "_parquet_fallback/"
                metrics_df.withColumn("_ingested_at", current_timestamp()) \
                    .write.mode("append").parquet(fallback_path)
                logger.warning("Wrote DQ results to fallback Parquet: %s", fallback_path)
            except Exception as fallback_exc:
                logger.error("Fallback write also failed: %s", fallback_exc)

    def _create_empty_metrics_df(self) -> DataFrame:
        return self.spark.createDataFrame([], METRICS_SCHEMA)


def create_dq_analyzer(
    spark: SparkSession,
    metrics_path: str,
    results_s3_prefix: str = ""
) -> GlueDQAnalyzer:
    """Factory function to create a GlueDQAnalyzer instance."""
    return GlueDQAnalyzer(spark, metrics_path, results_s3_prefix)
