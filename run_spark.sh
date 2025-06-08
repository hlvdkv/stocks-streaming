#!/usr/bin/env bash
# Użycie:  ./run_spark.sh A        # tryb A
#          ./run_spark.sh C        # tryb C


set -euo pipefail
DELAY=${1:-A} 
PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-core_2.12:2.3.0
spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages "$PACKAGES" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --files ~/stocks-streaming/symbols_valid_meta.csv \
  ~/stocks-streaming/spark_job.py \
  --symbols-meta gs://$BUCKET/symbols_valid_meta.csv \
  --output      gs://$BUCKET/stocks/output \
  --checkpoint  gs://$BUCKET/stocks/checkpoints \
  --delay "$DELAY"