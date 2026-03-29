"""
Silver Layer — Data Cleansing & Standardization (Dataproc PySpark).

Reads from Bronze BigQuery table, applies:
  - Data type casting (string date → DATE)
  - Null/missing value handling
  - Deduplication
  - Data quality filters (remove invalid prices, negative volumes)
  - Standardized column naming

Input:  BigQuery → stock_market_data.bronze_stock_data
Output: BigQuery → stock_market_data.silver_stock_data

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark \\
        src/batch/silver_layer.py \\
        --cluster=stock-market-batch-cluster \\
        --region=us-central1 \\
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \\
        -- \\
        --input_table your-project.stock_market_data.bronze_stock_data \\
        --output_table your-project.stock_market_data.silver_stock_data \\
        --temp_gcs_bucket dv-stock-market-pipeline-bucket
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# Silver Layer Pipeline
# ============================================================

def run_silver(args):
    spark = (
        SparkSession.builder
        .appName("StockPipeline_SilverLayer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"═══════════════════════════════════════════════")
    print(f"  SILVER LAYER — Data Cleansing")
    print(f"  Input:  {args.input_table}")
    print(f"  Output: {args.output_table}")
    print(f"═══════════════════════════════════════════════")

    # ── Read from Bronze table ──────────────────────────────
    bronze_df = (
        spark.read
        .format("bigquery")
        .option("table", args.input_table)
        .load()
    )

    bronze_count = bronze_df.count()
    print(f"  Bronze rows read: {bronze_count}")

    # ── Step 1: Cast data types ─────────────────────────────
    df = bronze_df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    # ── Step 2: Remove nulls in critical columns ────────────
    df = df.filter(
        F.col("symbol").isNotNull() &
        F.col("date").isNotNull() &
        F.col("close").isNotNull()
    )

    null_dropped = bronze_count - df.count()
    print(f"  Rows dropped (null critical fields): {null_dropped}")

    # ── Step 3: Data quality filters ────────────────────────
    # Remove rows with invalid/negative prices or volumes
    df = df.filter(
        (F.col("open") > 0) &
        (F.col("high") > 0) &
        (F.col("low") > 0) &
        (F.col("close") > 0) &
        (F.col("volume") >= 0)
    )

    # Validate: high >= low, high >= open, high >= close
    df = df.filter(
        (F.col("high") >= F.col("low")) &
        (F.col("high") >= F.col("open")) &
        (F.col("high") >= F.col("close")) &
        (F.col("low") <= F.col("open")) &
        (F.col("low") <= F.col("close"))
    )

    quality_dropped = bronze_count - null_dropped - df.count()
    print(f"  Rows dropped (quality filters): {quality_dropped}")

    # ── Step 4: Deduplication ───────────────────────────────
    # Keep the latest record per (symbol, date) based on load_timestamp
    w = Window.partitionBy("symbol", "date").orderBy(F.col("load_timestamp").desc())
    df = (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    dedup_count = df.count()
    print(f"  Rows after deduplication: {dedup_count}")

    # ── Step 5: Fill remaining nulls with defaults ──────────
    df = df.fillna({
        "adjusted_close": 0.0,
        "dividend_amount": 0.0,
        "split_coefficient": 1.0,
    })

    # ── Step 6: Add silver layer metadata ───────────────────
    silver_df = (
        df
        .withColumn("silver_load_timestamp", F.current_timestamp())
        .select(
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
            "dividend_amount",
            "split_coefficient",
            "source_file",
            "load_timestamp",
            "silver_load_timestamp",
        )
    )

    # ── Write to BigQuery (Silver table) ────────────────────
    (
        silver_df.write
        .format("bigquery")
        .option("table", args.output_table)
        .option("temporaryGcsBucket", args.temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .mode("overwrite")
        .save()
    )

    final_count = silver_df.count()
    print(f"  ✅ Silver layer complete: {final_count} rows → {args.output_table}")
    print(f"  📊 Data quality summary:")
    print(f"       Bronze input:    {bronze_count}")
    print(f"       Nulls removed:   {null_dropped}")
    print(f"       Quality filtered:{quality_dropped}")
    print(f"       After dedup:     {dedup_count}")
    print(f"       Silver output:   {final_count}")

    spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer — Data Cleansing")
    parser.add_argument("--input_table", required=True,
                        help="Bronze BigQuery table (project.dataset.table)")
    parser.add_argument("--output_table", required=True,
                        help="Silver BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_gcs_bucket", required=True,
                        help="GCS bucket for Spark temp files")
    args = parser.parse_args()

    run_silver(args)
