#!/usr/bin/env bash
# Użycie:  ./run_spark.sh A        # tryb A
#          ./run_spark.sh C        # tryb C

set -euo pipefail
DELAY=${1:-A}
PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 

spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages "$PACKAGES" \
  /home/$USER/stocks-streaming/spark_job_parquet.py \
  --symbols-meta file:///home/$USER/stocks-streaming/symbols_valid_meta.csv \
  --delay "$DELAY"
