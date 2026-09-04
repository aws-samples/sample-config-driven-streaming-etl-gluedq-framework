# Real-Time Mode Companion Example (AWS Glue 6.0)

AWS Glue 6.0 introduces [Spark Real-Time Mode](https://docs.aws.amazon.com/glue/latest/dg/glue-streaming-execution-models.html)
— a streaming execution model with sub-second (single-digit-millisecond for
eligible workloads) end-to-end latency. This example shows the **stateless
slice** of the framework's data quality story running in Real-Time Mode:
records are validated one at a time as they arrive and routed to a `clean` or
`quarantine` Kafka topic, tagged with the failed rule id (`_failed_rule`) —
quarantine-with-lineage at millisecond latency.

## Why this is a separate job (read this first)

Real-Time Mode has hard constraints. It is the right tool for exactly this
kind of workload, and the wrong tool for the main pipeline:

| | Micro-batch (main pipeline) | Real-Time Mode (this example) |
|---|---|---|
| Latency | seconds (trigger interval) | sub-second |
| Language | PySpark | **Scala only** |
| Operations | stateful (SCD2 MERGE, dedup) | **stateless only** |
| Source/Sink | Kafka -> Delta Lake on S3 | **Kafka -> Kafka only** |
| API | `forEachBatch` | `writeStream` + `Trigger.RealTime` |
| Output mode | any | **update only** |
| Auto-scaling | supported | **not supported** (fixed workers) |

The framework's SCD Type 2 Delta Lake MERGE is stateful, so it stays in
micro-batch mode (`src/glue_streaming_job.py`). Use this example when a
consumer needs validated events faster than the micro-batch interval — for
example feeding a real-time alerting or routing system — while the lakehouse
path continues in parallel.

## Deploy

Real-Time Mode requires Glue 6.0, the `--enable-real-time-mode` job argument,
and a fixed worker count (do not enable auto-scaling).

```bash
STACK="<your-stack-name>"
REGION="<region>"
ASSETS_BUCKET="${STACK}-assets-<account-id>"
BOOTSTRAP="<msk-sasl-bootstrap:9096>"
SECRET_ARN="<arn of AmazonMSK_${STACK}_scram>"

# Upload the script
aws s3 cp realtime_dq_route.scala "s3://${ASSETS_BUCKET}/scripts/" --region "$REGION"

# Create the source/target topics (reuse the framework's kafka-admin Lambda),
# then create the job:
aws glue create-job \
  --name "${STACK}-realtime-dq-route" \
  --role "<glue-role-arn-from-stack>" \
  --glue-version "6.0" \
  --number-of-workers 2 --worker-type G.1X \
  --command "Name=gluestreaming,ScriptLocation=s3://${ASSETS_BUCKET}/scripts/realtime_dq_route.scala" \
  --connections "Connections=${STACK}-msk-connection" \
  --default-arguments '{
    "--job-language": "scala",
    "--class": "RealtimeDQRoute",
    "--enable-real-time-mode": "true",
    "--BOOTSTRAP_SERVERS": "'"$BOOTSTRAP"'",
    "--MSK_SECRET_ARN": "'"$SECRET_ARN"'",
    "--SOURCE_TOPIC": "cdc-vehicle_telemetry",
    "--CLEAN_TOPIC": "rt-vehicle_telemetry-clean",
    "--QUARANTINE_TOPIC": "rt-vehicle_telemetry-quarantine",
    "--CHECKPOINT_BUCKET": "'"$ASSETS_BUCKET"'"
  }' \
  --region "$REGION"

aws glue start-job-run --job-name "${STACK}-realtime-dq-route" --region "$REGION"
```

## Verify

Consume the two output topics (e.g. with the kafka-admin Lambda or a console
consumer). Clean records land on `rt-vehicle_telemetry-clean`; records failing
any rule land on `rt-vehicle_telemetry-quarantine` with `_failed_rule` set
(`tel_001` = latitude range, `tel_002` = longitude range, `tel_003` = speed
range, `tel_004` = null vehicle_id) and `_validated_at` timestamps.

## Cost note

This is an additional continuously-running Glue streaming job (2 × G.1X
minimum) on top of the main pipeline. Stop it when not testing:

```bash
aws glue batch-stop-job-run --job-name "${STACK}-realtime-dq-route" \
  --job-run-ids "$(aws glue get-job-runs --job-name "${STACK}-realtime-dq-route" \
    --query 'JobRuns[?JobRunState==`RUNNING`].Id' --output text)" --region "$REGION"
```
