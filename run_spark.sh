#!/usr/bin/env bash
# Użycie:  ./run_spark.sh A        # tryb A
#          ./run_spark.sh C        # tryb C

set -euo pipefail
DELAY=${1:-A}

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name stocks-streaming \
  --files data/symbols_meta.csv \
  spark_job_parquet.py \
    --symbols-meta symbols_meta.csv \
    --delay "$DELAY"
