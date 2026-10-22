#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
: "${REGION:=asia-east1}"
: "${CLUSTER_NAME:=ecommerce-spark-demo}"
: "${KAFKA_BOOTSTRAP_SERVERS:?Use the kafka_internal_bootstrap_server Terraform output}"
: "${DATA_LAKE_BUCKET:=${PROJECT_ID}-ecommerce-lake}"
: "${ASYNC:=true}"

JOB_URI="gs://${DATA_LAKE_BUCKET}/jobs/spark_bigquery_consumer.py"

gcloud storage cp scripts/spark_bigquery_consumer.py "${JOB_URI}"

# This is a long-running, restartable Structured Streaming job on a persistent
# single-node demonstration cluster. The cluster and job run until stopped.
ASYNC_ARGS=()
if [[ "${ASYNC}" == "true" ]]; then
  ASYNC_ARGS+=(--async "--format=value(reference.jobId)")
fi

gcloud dataproc jobs submit pyspark "${JOB_URI}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --cluster="${CLUSTER_NAME}" \
  --properties='spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  --max-failures-per-hour=5 \
  --max-failures-total=240 \
  "${ASYNC_ARGS[@]}" \
  -- \
  --bootstrap-servers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --data-lake-uri="gs://${DATA_LAKE_BUCKET}"
