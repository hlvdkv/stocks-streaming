import os
from pyspark.sql import SparkSession

def show_delta_tail(spark, path, name, n=10):
    print(f"\n=== Ostatnie {n} wierszy z `{name}` (Delta) ===")
    try:
        df = spark.read.format("delta").load(path)
        df.orderBy("window.start", ascending=False).show(n, truncate=False)
    except Exception as e:
        print(f"Błąd odczytu {path}: {e}")

def main():
    output = os.getenv("OUTPUT_URI")  # Ustaw w środowisku
    if not output:
        print("Brak OUTPUT_URI (ustaw zmienną środowiskową)")
        return

    spark = SparkSession.builder \
        .appName("ReadDeltaResults") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    show_delta_tail(spark, f"{output}/realtime_bars", "realtime_bars")
    show_delta_tail(spark, f"{output}/anomalies", "anomalies")

    spark.stop()

if __name__ == "__main__":
    main()