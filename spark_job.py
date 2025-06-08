#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, to_timestamp, window, avg,
    max as spark_max, min as spark_min, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from delta.tables import DeltaTable

# ─── Schemat metadanych ─────────────────────────
META_SCHEMA = StructType([
    StructField("NasdaqTraded",   StringType()),
    StructField("Symbol",         StringType()),
    StructField("SecurityName",   StringType()),
    StructField("ListingExchange",StringType()),
    StructField("MarketCategory", StringType()),
    StructField("ETF",            StringType()),
    StructField("RoundLotSize",   StringType()),
    StructField("TestIssue",      StringType()),
    StructField("FinancialStatus",StringType()),
    StructField("CQSSymbol",      StringType()),
    StructField("NASDAQSymbol",   StringType()),
    StructField("NextShares",     StringType()),
])

# ─── Argumenty ──────────────────────────────────
def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols-meta", required=True)
    p.add_argument("--delay", choices=("A","C"), default="A")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"))
    p.add_argument("--topic",     default=os.getenv("KAFKA_TOPIC","stocks-topic"))
    p.add_argument("--output",    required=True)
    p.add_argument("--checkpoint",required=True)
    return p.parse_args()

# ─── Upsert funkcja ─────────────────────────────
def upsert_to_delta(microBatchDF, batchId, spark, path):
    deltaExists = DeltaTable.isDeltaTable(spark, path)
    if not deltaExists:
        # pierwszy batch: po prostu zapisz cały DF
        microBatchDF.write.format("delta")           \
            .mode("overwrite")                       \
            .option("overwriteSchema", "true")       \
            .save(path)
    else:
        deltaTbl = DeltaTable.forPath(spark, path)
        # klucz to window.start+Stock
        deltaTbl.alias("tgt").merge(
            microBatchDF.alias("src"),
            "tgt.Stock = src.Stock AND tgt.window.start = src.window.start"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

# ─── Główna funkcja ─────────────────────────────
def main():
    a = cli()
    spark = SparkSession.builder \
        .appName("StocksStreamingDeltaUpsert") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Metadane
    meta = (spark.read.option("header","true")
            .schema(META_SCHEMA)
            .csv(a.symbols_meta)
            .filter(col("NasdaqTraded")=="Y")
            .select("Symbol","SecurityName","MarketCategory")
            .cache())

    # 2) Kafka
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers",a.bootstrap)
           .option("subscribe",a.topic)
           .option("startingOffsets","earliest")
           .load())

    trades = (raw.selectExpr("CAST(value AS STRING) AS csv")
              .withColumn("p", split(col("csv"),","))
              .select(
                  to_timestamp(col("p")[0],"yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("event_ts"),
                  col("p")[1].cast(DoubleType()).alias("Open"),
                  col("p")[2].cast(DoubleType()).alias("High"),
                  col("p")[3].cast(DoubleType()).alias("Low"),
                  col("p")[4].cast(DoubleType()).alias("Close"),
                  col("p")[5].cast(DoubleType()).alias("AdjClose"),
                  col("p")[6].cast(LongType()).alias("Volume"),
                  col("p")[7].alias("Stock")
              ))

    # 3) Join
    enriched = (trades.join(meta, trades.Stock==meta.Symbol, "left")
                      .select(trades["*"],meta.SecurityName,meta.MarketCategory))

    # 4) Tryby
    watermark = "1 second" if a.delay=="A" else "7 days"
    trigger   = 5          if a.delay=="A" else 60
    mode      = "append"   # i tak użyjemy foreachBatch w A

    # 5) Agregacja
    bars = (enriched
            .withWatermark("event_ts", watermark)
            .groupBy(
                window(col("event_ts"),"5 days","1 day"),
                col("Stock"), col("SecurityName")
            )
            .agg(
                avg("Close").alias("avg_close"),
                spark_max("High").alias("max_high"),
                spark_min("Low").alias("min_low"),
                avg("Volume").alias("avg_volume")
            ))

    anomalies = (bars
                 .withColumn("ratio",(col("max_high")-col("min_low"))/col("max_high"))
                 .filter(col("ratio")>=lit(0.20)))

    # 6) Sink dla realtime_bars
    path_rt = f"{a.output}/realtime_bars"
    if a.delay=="A":
        bars.writeStream \
            .foreachBatch(lambda df, bid: upsert_to_delta(df, bid, spark, path_rt)) \
            .option("checkpointLocation", f"{a.checkpoint}/realtime_bars") \
            .trigger(processingTime=f"{trigger} seconds") \
            .start()
    else:
        bars.writeStream \
            .format("delta") \
            .option("path", path_rt) \
            .option("checkpointLocation", f"{a.checkpoint}/realtime_bars") \
            .outputMode("append") \
            .trigger(processingTime=f"{trigger} seconds") \
            .start()

    # 7) Sink dla anomalies (append)
    anomalies.writeStream \
        .format("delta") \
        .option("path", f"{a.output}/anomalies") \
        .option("checkpointLocation", f"{a.checkpoint}/anomalies") \
        .outputMode("append") \
        .trigger(processingTime=f"{trigger} seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__=="__main__":
    main()
