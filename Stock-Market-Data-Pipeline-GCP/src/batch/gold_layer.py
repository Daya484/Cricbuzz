"""
Gold Layer — Business Aggregations & Analytics (Dataproc PySpark).

Reads from Silver BigQuery table, computes:
  - Technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands)
  - Daily returns
  - Summary statistics per symbol

Input:  BigQuery → stock_market_data.silver_stock_data
Output: BigQuery → stock_market_data.gold_technical_indicators
        BigQuery → stock_market_data.gold_daily_summary

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark \\
        src/batch/gold_layer.py \\
        --cluster=stock-market-batch-cluster \\
        --region=us-central1 \\
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \\
        -- \\
        --input_table your-project.stock_market_data.silver_stock_data \\
        --indicators_table your-project.stock_market_data.gold_technical_indicators \\
        --summary_table your-project.stock_market_data.gold_daily_summary \\
        --temp_gcs_bucket dv-stock-market-pipeline-bucket
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# Technical Indicator Functions
# ============================================================

def add_sma(df, column, window_size, output_col):
    """Simple Moving Average."""
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(
        -(window_size - 1), 0
    )
    return df.withColumn(output_col, F.avg(column).over(w))


def add_rsi(df, column="close", period=14):
    """Relative Strength Index."""
    w = Window.partitionBy("symbol").orderBy("date")
    w_period = Window.partitionBy("symbol").orderBy("date").rowsBetween(
        -(period - 1), 0
    )

    df = df.withColumn("_prev_close", F.lag(column, 1).over(w))
    df = df.withColumn("_change", F.col(column) - F.col("_prev_close"))
    df = df.withColumn("_gain", F.when(F.col("_change") > 0, F.col("_change")).otherwise(0))
    df = df.withColumn("_loss", F.when(F.col("_change") < 0, -F.col("_change")).otherwise(0))
    df = df.withColumn("_avg_gain", F.avg("_gain").over(w_period))
    df = df.withColumn("_avg_loss", F.avg("_loss").over(w_period))

    df = df.withColumn(
        "rsi_14",
        F.when(F.col("_avg_loss") == 0, 100.0)
         .otherwise(100.0 - (100.0 / (1.0 + F.col("_avg_gain") / F.col("_avg_loss"))))
    )
    return df.drop("_prev_close", "_change", "_gain", "_loss", "_avg_gain", "_avg_loss")


def add_bollinger_bands(df, column="close", window_size=20, num_std=2):
    """Bollinger Bands: SMA ± num_std × stddev."""
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(
        -(window_size - 1), 0
    )
    df = df.withColumn("_bb_sma", F.avg(column).over(w))
    df = df.withColumn("_bb_std", F.stddev(column).over(w))
    df = df.withColumn("bollinger_upper", F.col("_bb_sma") + num_std * F.col("_bb_std"))
    df = df.withColumn("bollinger_lower", F.col("_bb_sma") - num_std * F.col("_bb_std"))
    return df.drop("_bb_sma", "_bb_std")


def add_daily_return(df, column="close"):
    """Daily return percentage."""
    w = Window.partitionBy("symbol").orderBy("date")
    df = df.withColumn("_prev", F.lag(column, 1).over(w))
    df = df.withColumn(
        "daily_return_pct",
        F.when(F.col("_prev").isNotNull() & (F.col("_prev") != 0),
               ((F.col(column) - F.col("_prev")) / F.col("_prev")) * 100)
         .otherwise(None)
    )
    return df.drop("_prev")


# ============================================================
# Gold Layer Pipeline
# ============================================================

def run_gold(args):
    spark = (
        SparkSession.builder
        .appName("StockPipeline_GoldLayer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"═══════════════════════════════════════════════")
    print(f"  GOLD LAYER — Business Aggregations")
    print(f"  Input:      {args.input_table}")
    print(f"  Indicators: {args.indicators_table}")
    print(f"  Summary:    {args.summary_table}")
    print(f"═══════════════════════════════════════════════")

    # ── Read from Silver table ──────────────────────────────
    silver_df = (
        spark.read
        .format("bigquery")
        .option("table", args.input_table)
        .load()
    )

    silver_count = silver_df.count()
    symbols = [row.symbol for row in silver_df.select("symbol").distinct().collect()]
    print(f"  Silver rows read: {silver_count}")
    print(f"  Symbols: {symbols}")

    # ══════════════════════════════════════════════════════════
    # Gold Table 1: Technical Indicators
    # ══════════════════════════════════════════════════════════

    print("  Computing SMA (20, 50) …")
    df = add_sma(silver_df, "close", 20, "sma_20")
    df = add_sma(df, "close", 50, "sma_50")

    print("  Computing EMA approximations (12, 26) …")
    df = add_sma(df, "close", 12, "ema_12")
    df = add_sma(df, "close", 26, "ema_26")

    print("  Computing MACD …")
    df = df.withColumn("macd", F.col("ema_12") - F.col("ema_26"))
    df = add_sma(df, "macd", 9, "macd_signal")

    print("  Computing RSI (14) …")
    df = add_rsi(df, column="close", period=14)

    print("  Computing Bollinger Bands …")
    df = add_bollinger_bands(df, column="close", window_size=20, num_std=2)

    print("  Computing daily returns …")
    df = add_daily_return(df, column="close")

    # Add gold metadata
    df = df.withColumn("gold_load_timestamp", F.current_timestamp())

    # Select final indicator columns
    indicators_df = df.select(
        "symbol", "date", "close",
        "sma_20", "sma_50",
        "ema_12", "ema_26",
        "rsi_14",
        "macd", "macd_signal",
        "bollinger_upper", "bollinger_lower",
        "daily_return_pct",
        "gold_load_timestamp",
    )

    # Write Gold Table 1
    (
        indicators_df.write
        .format("bigquery")
        .option("table", args.indicators_table)
        .option("temporaryGcsBucket", args.temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .mode("overwrite")
        .save()
    )

    ind_count = indicators_df.count()
    print(f"  ✅ Gold indicators: {ind_count} rows → {args.indicators_table}")

    # ══════════════════════════════════════════════════════════
    # Gold Table 2: Daily Summary per Symbol
    # ══════════════════════════════════════════════════════════

    print("  Computing daily summary aggregations …")

    summary_df = (
        silver_df
        .groupBy("symbol")
        .agg(
            F.count("date").alias("total_trading_days"),
            F.min("date").alias("first_date"),
            F.max("date").alias("last_date"),
            F.avg("close").alias("avg_close"),
            F.min("low").alias("all_time_low"),
            F.max("high").alias("all_time_high"),
            F.avg("volume").cast("long").alias("avg_daily_volume"),
            F.sum("volume").alias("total_volume"),
            F.stddev("close").alias("close_stddev"),
            F.avg("adjusted_close").alias("avg_adjusted_close"),
        )
        .withColumn("gold_load_timestamp", F.current_timestamp())
    )

    # Write Gold Table 2
    (
        summary_df.write
        .format("bigquery")
        .option("table", args.summary_table)
        .option("temporaryGcsBucket", args.temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .mode("overwrite")
        .save()
    )

    summ_count = summary_df.count()
    print(f"  ✅ Gold summary: {summ_count} rows → {args.summary_table}")
    print(f"")
    print(f"  ═══ GOLD LAYER COMPLETE ═══")
    print(f"  Technical Indicators: {ind_count} rows")
    print(f"  Daily Summary:       {summ_count} rows")

    spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Layer — Business Aggregations")
    parser.add_argument("--input_table", required=True,
                        help="Silver BigQuery table (project.dataset.table)")
    parser.add_argument("--indicators_table", required=True,
                        help="Gold indicators BigQuery table")
    parser.add_argument("--summary_table", required=True,
                        help="Gold summary BigQuery table")
    parser.add_argument("--temp_gcs_bucket", required=True,
                        help="GCS bucket for Spark temp files")
    args = parser.parse_args()

    run_gold(args)
