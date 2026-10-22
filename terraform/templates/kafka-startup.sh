#!/bin/bash
set -euo pipefail

apt-get update
apt-get install -y ca-certificates curl docker.io
systemctl enable --now docker

DATA_DEVICE=/dev/disk/by-id/google-kafka-data
DATA_MOUNT=/var/lib/kafka-data
KAFKA_DATA_DIR="${DATA_MOUNT}/data"
mkdir -p "${DATA_MOUNT}"
if ! blkid "${DATA_DEVICE}"; then
  mkfs.ext4 -F "${DATA_DEVICE}"
fi
if ! grep -q "${DATA_DEVICE}" /etc/fstab; then
  echo "${DATA_DEVICE} ${DATA_MOUNT} ext4 defaults,nofail 0 2" >> /etc/fstab
fi
mount -a
# ext4 creates lost+found at the filesystem root. Kafka treats every direct
# child of log.dirs as a topic-partition directory, so use a dedicated child
# instead of mounting the filesystem root as /var/lib/kafka/data.
mkdir -p "${KAFKA_DATA_DIR}"
chown -R 1000:1000 "${KAFKA_DATA_DIR}"

INTERNAL_IP=$(hostname -I | awk '{print $1}')
docker run -d \
  --name ecommerce-kafka \
  --restart unless-stopped \
  -p 9092:9092 \
  -p 127.0.0.1:9999:9999 \
  -v "${KAFKA_DATA_DIR}:/var/lib/kafka/data" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT \
  -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://${INTERNAL_IP}:9092 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_JMX_PORT=9999 \
  -e KAFKA_JMX_HOSTNAME=127.0.0.1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  confluentinc/cp-kafka:7.6.0

# A RUNNING VM does not mean its startup script or Kafka is ready. Publish a
# serial-console marker only after the broker accepts a real metadata request;
# cloud E2E polls this marker before submitting Spark or starting Producer.
KAFKA_READY=false
for attempt in $(seq 1 60); do
  if ! docker inspect --format='{{.State.Running}}' ecommerce-kafka 2>/dev/null | grep -q true; then
    docker logs ecommerce-kafka || true
    echo "KAFKA_STARTUP_FAILED container exited" >&2
    exit 1
  fi
  if docker exec ecommerce-kafka kafka-topics \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    KAFKA_READY=true
    break
  fi
  sleep 5
done

if [[ "${KAFKA_READY}" != "true" ]]; then
  docker logs ecommerce-kafka || true
  echo "KAFKA_STARTUP_FAILED broker did not become ready" >&2
  exit 1
fi
echo "KAFKA_READY bootstrap=${INTERNAL_IP}:9092"

# Install the Google Cloud Ops Agent after Kafka is serving traffic, then add
# Kafka JMX metrics. This keeps observability setup from delaying data-plane
# readiness while retaining the same least-privilege VM identity.
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash add-google-cloud-ops-agent-repo.sh --also-install
cat >/etc/google-cloud-ops-agent/config.yaml <<'EOF'
metrics:
  receivers:
    kafka:
      type: kafka
      endpoint: localhost:9999
  service:
    pipelines:
      kafka:
        receivers: [kafka]
EOF
systemctl restart google-cloud-ops-agent
