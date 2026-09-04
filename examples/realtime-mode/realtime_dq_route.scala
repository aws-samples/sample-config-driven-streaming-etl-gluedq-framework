/*
 * Real-Time Mode DQ Router — AWS Glue 6.0 companion example.
 *
 * Demonstrates Glue 6.0 Spark Real-Time Mode (sub-second latency) for the
 * STATELESS slice of the streaming DQ framework: validate each record as it
 * arrives and route it to a "clean" or "quarantine" Kafka topic, tagged with
 * the failed rule id — the same quarantine-with-lineage concept as the main
 * pipeline, at millisecond latency.
 *
 * Real-Time Mode constraints (why this is a separate Scala job):
 *   - Scala only, Kafka source/sink only, STATELESS operations only
 *   - outputMode("update") required; Trigger.RealTime(checkpointIntervalMs)
 *   - No forEachBatch, no auto-scaling (fixed worker count)
 * The main pipeline (SCD Type 2 Delta MERGE) is stateful and stays in
 * micro-batch mode — see src/glue_streaming_job.py.
 *
 * Job requirements:
 *   --enable-real-time-mode true      (job argument, opt-in)
 *   --job-language scala
 *   Glue version 6.0, fixed NumberOfWorkers (do NOT enable auto-scaling)
 *
 * Job arguments:
 *   --BOOTSTRAP_SERVERS  MSK SASL/SCRAM bootstrap string (port 9096)
 *   --MSK_SECRET_ARN     Secrets Manager ARN of the AmazonMSK_*_scram secret
 *   --SOURCE_TOPIC       e.g. cdc-vehicle_telemetry
 *   --CLEAN_TOPIC        e.g. rt-vehicle_telemetry-clean
 *   --QUARANTINE_TOPIC   e.g. rt-vehicle_telemetry-quarantine
 *   --CHECKPOINT_BUCKET  S3 bucket for the stream checkpoint (e.g. the assets bucket)
 */

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

// AWS SDK v2 (the only SDK on Glue 6.0 — SDK v1 was removed)
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

import scala.collection.JavaConverters._

object RealtimeDQRoute {

  /** Fetch the SASL/SCRAM password from Secrets Manager (never in job args). */
  def kafkaPassword(secretArn: String): (String, String) = {
    val client = SecretsManagerClient.create()
    try {
      val secretString = client.getSecretValue(
        GetSecretValueRequest.builder().secretId(secretArn).build()
      ).secretString()
      // Secret shape: {"username":"kafkaadmin","password":"..."}
      val pattern = """"username"\s*:\s*"([^"]+)".*"password"\s*:\s*"([^"]+)"""".r.unanchored
      secretString match {
        case pattern(user, pass) => (user, pass)
        case _ => throw new IllegalStateException("MSK secret is not in the expected JSON shape")
      }
    } finally client.close()
  }

  def main(sysArgs: Array[String]): Unit = {
    val args = GlueArgParser.getResolvedOptions(
      sysArgs,
      Seq("JOB_NAME", "BOOTSTRAP_SERVERS", "MSK_SECRET_ARN",
          "SOURCE_TOPIC", "CLEAN_TOPIC", "QUARANTINE_TOPIC",
          "CHECKPOINT_BUCKET").toArray
    )

    val sc = new SparkContext()
    val glueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val (saslUser, saslPass) = kafkaPassword(args("MSK_SECRET_ARN"))
    val jaas =
      s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$saslUser" password="$saslPass";"""

    // ---- Source: DMS CDC envelope on the source topic -----------------------
    // data{} carries the row; metadata{} carries operation/table info.
    val telemetrySchema = new StructType()
      .add("id", IntegerType).add("vehicle_id", IntegerType)
      .add("speed_kmh", DoubleType)
      .add("latitude", DoubleType).add("longitude", DoubleType)
      .add("fuel_level_pct", DoubleType)
    val envelopeSchema = new StructType()
      .add("data", telemetrySchema)
      .add("metadata", new StructType()
        .add("operation", StringType)
        .add("table-name", StringType))

    val source = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args("BOOTSTRAP_SERVERS"))
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
      .option("kafka.sasl.jaas.config", jaas)
      .option("subscribe", args("SOURCE_TOPIC"))
      .option("startingOffsets", "latest")
      .load()

    val parsed = source
      .select(from_json(col("value").cast("string"), envelopeSchema).as("j"))
      .select(col("j.data.*"), col("j.metadata.operation").as("_operation"))
      .filter(col("id").isNotNull) // drop DMS control messages

    // ---- Stateless per-record DQ (mirror of tel_001..tel_004 in tables.yaml)
    // Every predicate is row-local: no aggregations, joins, or windows —
    // the operations Real-Time Mode supports.
    val failedRule = when(col("latitude") < -90 || col("latitude") > 90, lit("tel_001"))
      .when(col("longitude") < -180 || col("longitude") > 180, lit("tel_002"))
      .when(col("speed_kmh") < 0 || col("speed_kmh") > 350, lit("tel_003"))
      .when(col("vehicle_id").isNull, lit("tel_004"))
      .otherwise(lit(null))

    val routed = parsed
      .withColumn("_failed_rule", failedRule)
      .withColumn("_validated_at", current_timestamp())
      // Per-record topic routing: the Kafka sink honors the "topic" column
      // when no fixed topic option is set on the writer.
      .withColumn("topic",
        when(col("_failed_rule").isNotNull, lit(args("QUARANTINE_TOPIC")))
          .otherwise(lit(args("CLEAN_TOPIC"))))
      .select(
        col("topic"),
        col("id").cast("string").as("key"),
        to_json(struct(
          col("id"), col("vehicle_id"), col("speed_kmh"),
          col("latitude"), col("longitude"), col("fuel_level_pct"),
          col("_operation"), col("_failed_rule"), col("_validated_at")
        )).as("value")
      )

    // ---- Sink: Real-Time Mode ----------------------------------------------
    // Trigger.RealTime processes records as they arrive (sub-second latency).
    // The argument is the CHECKPOINT interval in ms, not a batch interval.
    // outputMode("update") is required (append throws OUTPUT_MODE_NOT_SUPPORTED).
    val query = routed.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args("BOOTSTRAP_SERVERS"))
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
      .option("kafka.sasl.jaas.config", jaas)
      .option("checkpointLocation", s"s3://${args("CHECKPOINT_BUCKET")}/checkpoints/realtime-dq-route/")
      .outputMode("update")
      .trigger(Trigger.RealTime(60000L))
      .start()

    query.awaitTermination()
    Job.commit()
  }
}
