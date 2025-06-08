#!/usr/bin/env bash

set -euo pipefail

TOPIC=${KAFKA_TOPIC:-stocks-topic}
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

echo "[reset] Kafka topic"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" --partitions 3 --replication-factor 1

echo "[reset] Checkpoints & output"
hdfs dfs -rm -r -f /stocks/output /stocks/checkpoints 2>/dev/null || true

