#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
: "${REGION:=asia-east1}"
: "${SUBNET:=ecommerce-streaming-${REGION}}"
: "${KAFKA_BOOTSTRAP_SERVERS:?Use the kafka_internal_bootstrap_server Terraform output}"
: "${PRODUCER_TARGET_EPS:=}"
: "${PRODUCER_RUN_ID:=}"
: "${PRODUCER_SEED:=42}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/ecommerce-streaming/producer:latest"

PRODUCER_ARGS=()
if [[ -n "${PRODUCER_TARGET_EPS}" ]]; then
  PRODUCER_ARGS+=(
    "--args=--target-eps,${PRODUCER_TARGET_EPS},--run-id,${PRODUCER_RUN_ID:-cloud-e2e},--seed,${PRODUCER_SEED}"
  )
fi

gcloud builds submit \
  --project="${PROJECT_ID}" \
  --config=deploy/cloud-run/cloudbuild.producer.yaml \
  --substitutions="_IMAGE=${IMAGE}" \
  .

gcloud alpha run worker-pools deploy ecommerce-producer \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --image="${IMAGE}" \
  --service-account="producer-worker@${PROJECT_ID}.iam.gserviceaccount.com" \
  --network=ecommerce-streaming \
  --subnet="${SUBNET}" \
  --set-env-vars="KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}" \
  "${PRODUCER_ARGS[@]}" \
  --scaling=1
