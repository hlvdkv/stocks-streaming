#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, split, to_timestamp, window, avg,
                                   max as spark_max, min as spark_min, lit)
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, LongType)

META_SCHEMA = StructType([
    StructField("NasdaqTraded", StringType()),  # 12 kolumn – interesują nas tylko 3
    StructField("Symbol", StringType()),
    StructField("SecurityName", StringType()),
    StructField("ListingExchange", StringType()),
    StructField("MarketCategory", StringType()),
    StructField("ETF", StringType()),
    StructField("RoundLotSize", StringType()),
    StructField("TestIssue", StringType()),
    StructField("FinancialStatus", StringType()),
    StructField("CQSSymbol", StringType()),
    StructField("NASDAQSymbol", StringType()),
    StructField("NextShares", StringType())
])

def args():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols-meta", required=True,
                   help="CSV z metadanymi (lokalnie, HDFS lub gs://)")
    p.add_argument("--delay", choices=("A", "C"), default="A",
                   help="A = minimalne opóźnienie (update), C = tylko final (append)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "stocks-topic"))
    bucket = os.getenv("BUCKET")
    default_out = f"gs://{bucket}/stocks/output"      if bucket else "hdfs:///stocks/output"
    default_chk = f"gs://{bucket}/stocks/checkpoints" if bucket else "hdfs:///stocks/checkpoints"
    p.add_argument("--output",    default=os.getenv("OUTPUT_URI", default_out))
    p.add_argument("--checkpoint", default=os.getenv("CHECKPOINT_URI", default_chk))
    return p.parse_args()

def main() -> None:
    a = args()
    spark = SparkSession.builder.appName("StocksStreamingParquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    meta = (spark.read.option("header", "true")
            .schema(META_SCHEMA)
            .csv(a.symbols_meta)
            .filter(col("NasdaqTraded") == "Y")
            .select("Symbol", "SecurityName", "MarketCategory")
            .cache())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", a.topic)
           .option("startingOffsets", "earliest")
           .load())

    trades = (raw.selectExpr("CAST(value AS STRING) AS csv")
              .withColumn("p", split(col("csv"), ","))
              .select(
                  to_timestamp(col("p")[0], "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("event_ts"),
                  col("p")[1].cast(DoubleType()).alias("Open"),
                  col("p")[2].cast(DoubleType()).alias("High"),
                  col("p")[3].cast(DoubleType()).alias("Low"),
                  col("p")[4].cast(DoubleType()).alias("Close"),
                  col("p")[5].cast(DoubleType()).alias("AdjClose"),
                  col("p")[6].cast(LongType()).alias("Volume"),
                  col("p")[7].alias("Stock")))

    enriched = trades.join(meta, "Stock", "left")

    watermark = "1 second" if a.delay == "A" else "7 days"
    mode      = "append"
    trig_sec  = 5 if a.delay == "A" else 60

    bars = (enriched
            .withWatermark("event_ts", watermark)
            .groupBy(
                window(col("event_ts"), "5 days", "1 day"),
                col("Stock"), col("SecurityName"))
            .agg(
                avg("Close").alias("avg_close"),
                spark_max("High").alias("max_high"),
                spark_min("Low").alias("min_low"),
                avg("Volume").alias("avg_volume")))

    anomalies = (bars
                 .withColumn("ratio", (col("max_high") - col("min_low")) / col("max_high"))
                 .filter(col("ratio") >= lit(0.20)))

    def sink(df, name, m=mode):
        return (df.writeStream
                .format("parquet")
                .option("path", f"{a.output}/{name}")
                .option("checkpointLocation", f"{a.checkpoint}/{name}")
                .outputMode(m)
                .trigger(processingTime=f"{trig_sec} seconds"))

    sink(bars, "realtime_bars").start()
    sink(anomalies, "anomalies", "append").start()
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
