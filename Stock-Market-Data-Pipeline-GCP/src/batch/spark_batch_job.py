"""
PySpark Batch Job — Technical Indicators computation.

Runs on Google Cloud Dataproc. Reads historical stock data from
Cloud Storage, computes technical indicators (SMA, EMA, RSI, MACD,
Bollinger Bands), and writes results to BigQuery.

Usage (submit to Dataproc):
    gcloud dataproc jobs submit pyspark \\
        src/batch/spark_batch_job.py \\
        --cluster=stock-market-batch-cluster \\
        --region=us-central1 \\
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \\
        -- \\
        --input_path gs://your-bucket/raw/all_stocks_daily.csv \\
        --output_table your-project-id.stock_market_data.technical_indicators \\
        --temp_gcs_bucket your-bucket
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType,
)


# ============================================================
# Schema
# ============================================================

STOCK_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("adjusted_close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("dividend_amount", DoubleType(), True),
    StructField("split_coefficient", DoubleType(), True),
    StructField("symbol", StringType(), True),
])


# ============================================================
# Technical Indicator Computations
# ============================================================

def compute_sma(df, column, window_size, output_col):
    """Simple Moving Average over a rolling window."""
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(
        -(window_size - 1), 0
    )
    return df.withColumn(output_col, F.avg(column).over(w))


def compute_ema(df, column, span, output_col):
    """
    Exponential Moving Average approximation using Spark.

    True EMA requires iterative computation. We approximate it
    using a weighted average with exponential decay.
    For production, consider using Pandas UDF for exact EMA.
    """
    # Approximation: use the built-in avg over a window sized ≈ span
    # For a more accurate EMA, we use a Pandas UDF
    from pyspark.sql.functions import pandas_udf
    import pandas as pd

    @pandas_udf(DoubleType())
    def ema_udf(series: pd.Series) -> pd.Series:
        return series.ewm(span=span, adjust=False).mean()

    w = Window.partitionBy("symbol").orderBy("date")
    return df.withColumn(output_col, ema_udf(F.col(column)).over(w))


def compute_rsi(df, column="close", period=14):
    """
    Relative Strength Index (RSI).

    Computes daily gain/loss, averages them over `period` days,
    and derives RSI = 100 - (100 / (1 + RS)).
    """
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

    # Clean up temp columns
    df = df.drop("_prev_close", "_change", "_gain", "_loss", "_avg_gain", "_avg_loss")
    return df


def compute_bollinger_bands(df, column="close", window_size=20, num_std=2):
    """Bollinger Bands: SMA ± num_std × standard deviation."""
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(
        -(window_size - 1), 0
    )

    df = df.withColumn("_bb_sma", F.avg(column).over(w))
    df = df.withColumn("_bb_std", F.stddev(column).over(w))
    df = df.withColumn("bollinger_upper", F.col("_bb_sma") + num_std * F.col("_bb_std"))
    df = df.withColumn("bollinger_lower", F.col("_bb_sma") - num_std * F.col("_bb_std"))
    df = df.drop("_bb_sma", "_bb_std")
    return df


def compute_daily_return(df, column="close"):
    """Daily return percentage: (close - prev_close) / prev_close × 100."""
    w = Window.partitionBy("symbol").orderBy("date")
    df = df.withColumn("_prev", F.lag(column, 1).over(w))
    df = df.withColumn(
        "daily_return_pct",
        F.when(F.col("_prev").isNotNull() & (F.col("_prev") != 0),
               ((F.col(column) - F.col("_prev")) / F.col("_prev")) * 100)
         .otherwise(None)
    )
    df = df.drop("_prev")
    return df


# ============================================================
# Main Pipeline
# ============================================================

def run_batch_job(args):
    """Read from GCS → compute indicators → write to BigQuery."""

    spark = (
        SparkSession.builder
        .appName("StockMarketBatchProcessing")
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ── Read CSV from Cloud Storage ─────────────────────────
    print(f"Reading data from {args.input_path} …")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(STOCK_SCHEMA)
        .csv(args.input_path)
    )

    # Cast date string to DateType
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    row_count = df.count()
    symbols = [row.symbol for row in df.select("symbol").distinct().collect()]
    print(f"Loaded {row_count} rows for symbols: {symbols}")

    # ── Compute Technical Indicators ────────────────────────
    print("Computing SMA (20, 50) …")
    df = compute_sma(df, "close", 20, "sma_20")
    df = compute_sma(df, "close", 50, "sma_50")

    print("Computing EMA (12, 26) …")
    # Simplified EMA using SMA approximation for Spark compatibility
    df = compute_sma(df, "close", 12, "ema_12")    # approximate
    df = compute_sma(df, "close", 26, "ema_26")    # approximate

    print("Computing MACD …")
    df = df.withColumn("macd", F.col("ema_12") - F.col("ema_26"))
    df = compute_sma(df, "macd", 9, "macd_signal")

    print("Computing RSI (14) …")
    df = compute_rsi(df, column="close", period=14)

    print("Computing Bollinger Bands …")
    df = compute_bollinger_bands(df, column="close", window_size=20, num_std=2)

    print("Computing daily returns …")
    df = compute_daily_return(df, column="close")

    # ── Select final columns ────────────────────────────────
    result = df.select(
        "symbol", "date", "close",
        "sma_20", "sma_50",
        "ema_12", "ema_26",
        "rsi_14",
        "macd", "macd_signal",
        "bollinger_upper", "bollinger_lower",
        "daily_return_pct",
    )

    result_count = result.count()
    print(f"Writing {result_count} rows to BigQuery: {args.output_table}")

    # ── Write to BigQuery ───────────────────────────────────
    (
        result.write
        .format("bigquery")
        .option("table", args.output_table)
        .option("temporaryGcsBucket", args.temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .mode("overwrite")
        .save()
    )

    print(f"✅ Batch job complete. {result_count} rows written to BigQuery.")

    # Also write the cleaned historical data to BigQuery
    print(f"Writing historical data to BigQuery: {args.historical_table}")
    historical = df.select(
        "symbol", "date", "open", "high", "low", "close",
        "adjusted_close", "volume", "dividend_amount", "split_coefficient",
    )

    (
        historical.write
        .format("bigquery")
        .option("table", args.historical_table)
        .option("temporaryGcsBucket", args.temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .mode("overwrite")
        .save()
    )

    print(f"✅ Historical data written. Total rows: {historical.count()}")
    spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market Spark Batch Job")
    parser.add_argument("--input_path", required=True,
                        help="GCS path to input CSV (gs://bucket/path/)")
    parser.add_argument("--output_table", required=True,
                        help="BigQuery table for indicators (project.dataset.table)")
    parser.add_argument("--historical_table",
                        default=None,
                        help="BigQuery table for historical data (default: derived from output)")
    parser.add_argument("--temp_gcs_bucket", required=True,
                        help="GCS bucket for Spark temp files")
    args = parser.parse_args()

    if args.historical_table is None:
        # Derive: project.dataset.historical_stock_data
        parts = args.output_table.rsplit(".", 1)
        args.historical_table = parts[0] + ".historical_stock_data"

    run_batch_job(args)
