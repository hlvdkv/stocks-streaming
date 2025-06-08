#!/usr/bin/env bash

set -euo pipefail

TOPIC=${KAFKA_TOPIC:-stocks-topic}
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

echo "[reset] Kafka topic"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" --partitions 3 --replication-factor 1

if [[ -n "${BUCKET:-}" ]]; then
  echo "[reset] GCS gs://$BUCKET/stocks/{output,checkpoints}"
  gsutil -m rm -r "gs://$BUCKET/stocks/output" \
                  "gs://$BUCKET/stocks/checkpoints" 2>/dev/null || true
else
  echo "[reset] HDFS /stocks/{output,checkpoints}"
  hdfs dfs -rm -r -f /stocks/output /stocks/checkpoints 2>/dev/null || true
fi
echo "[reset] Done."

