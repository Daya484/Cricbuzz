"""
Bronze Layer — Raw Data Ingestion (Dataproc PySpark).

Reads raw stock CSV data from Cloud Storage and writes it
to BigQuery with minimal transformation:
  - Schema enforcement
  - Add ingestion metadata (load_timestamp, source_file)
  - No data cleaning or business logic

Input:  gs://<bucket>/raw/*.csv
Output: BigQuery → stock_market_data.bronze_stock_data

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark \\
        src/batch/bronze_layer.py \\
        --cluster=stock-market-batch-cluster \\
        --region=us-central1 \\
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \\
        -- \\
        --input_path gs://dv-stock-market-pipeline-bucket/raw/all_stocks_daily.csv \\
        --output_table your-project.stock_market_data.bronze_stock_data \\
        --temp_gcs_bucket dv-stock-market-pipeline-bucket
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)


# ============================================================
# Schema — enforced on raw CSV input
# ============================================================

RAW_SCHEMA = StructType([
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
# Bronze Layer Pipeline
# ============================================================

def run_bronze(args):
    spark = (
        SparkSession.builder
        .appName("StockPipeline_BronzeLayer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"═══════════════════════════════════════════════")
    print(f"  BRONZE LAYER — Raw Data Ingestion")
    print(f"  Input:  {args.input_path}")
    print(f"  Output: {args.output_table}")
    print(f"═══════════════════════════════════════════════")

    # ── Read raw CSV with schema enforcement ────────────────
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")          # keep malformed rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_SCHEMA)
        .csv(args.input_path)
    )

    raw_count = df.count()
    print(f"  Raw rows read: {raw_count}")

    # ── Add ingestion metadata ──────────────────────────────
    bronze_df = (
        df
        .withColumn("load_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("load_date", F.current_date())
    )

    # ── Write to BigQuery (Bronze table) ────────────────────
    (
        bronze_df.write
        .format("bigquery")
        .option("table", args.output_table)
        .option("temporaryGcsBucket", args.temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .mode("overwrite")
        .save()
    )

    final_count = bronze_df.count()
    print(f"  ✅ Bronze layer complete: {final_count} rows → {args.output_table}")

    spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer — Raw Ingestion")
    parser.add_argument("--input_path", required=True,
                        help="GCS path to raw CSV files")
    parser.add_argument("--output_table", required=True,
                        help="BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_gcs_bucket", required=True,
                        help="GCS bucket for Spark temp files")
    args = parser.parse_args()

    run_bronze(args)
