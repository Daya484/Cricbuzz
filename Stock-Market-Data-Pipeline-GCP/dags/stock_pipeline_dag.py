"""
Stock Market Data Pipeline — Cloud Composer (Airflow) DAG.

Environment-aware DAG that reads configuration from the ENV
Airflow variable (dv / qa / pd).

Orchestrates the full pipeline:
  1. Fetch historical stock data from Alpha Vantage → local CSV
  2. Upload CSV to Cloud Storage
  3. Submit Dataproc (PySpark) batch job → BigQuery
  4. Run data quality checks on BigQuery tables

Schedule: Weekdays at 9:00 AM UTC (US market hours)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.trigger_rule import TriggerRule


# ============================================================
# Environment-aware Configuration
# ============================================================
# Set the "ENV" Airflow variable to: dv, qa, or pd
# Only project_id differs between environments.
# All resource names are identical — isolation is via separate GCP projects.
# ============================================================

PIPELINE_ENV = Variable.get("ENV", default_var=os.environ.get("ENV", "dv"))

# Only project_id, bucket, and topic change per environment
ENV_CONFIG = {
    "dv": {
        "project_id": "your-gcp-project-id-dev",
        "gcs_bucket": "dv-stock-market-pipeline-bucket",
        "pubsub_topic": "dv-stock-market-ticks",
    },
    "qa": {
        "project_id": "your-gcp-project-id-qa",
        "gcs_bucket": "qa-stock-market-pipeline-bucket",
        "pubsub_topic": "qa-stock-market-ticks",
    },
    "pd": {
        "project_id": "your-gcp-project-id-prod",
        "gcs_bucket": "pd-stock-market-pipeline-bucket",
        "pubsub_topic": "pd-stock-market-ticks",
    },
}

CFG = ENV_CONFIG[PIPELINE_ENV]

GCP_PROJECT_ID = CFG["project_id"]
GCP_REGION = "us-central1"
GCS_BUCKET = CFG["gcs_bucket"]
DATAPROC_CLUSTER = "stock-market-batch-cluster"
BQ_DATASET = "stock_market_data"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

PYSPARK_JOB_URI = f"gs://{GCS_BUCKET}/scripts/spark_batch_job.py"
SPARK_BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar"


# ============================================================
# DAG Default Arguments
# ============================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ============================================================
# Dataproc Cluster Config
# ============================================================

DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 100,
        },
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 100,
        },
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.jars": SPARK_BQ_JAR,
        },
    },
}


# ============================================================
# Python Callables
# ============================================================

def fetch_historical_data(**context):
    """Fetch stock data from Alpha Vantage and save as CSV."""
    import os
    sys.path.insert(0, "/home/airflow/gcs/dags/")

    from src.batch.fetch_historical_data import fetch_daily_adjusted
    import pandas as pd

    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY not set in Airflow variables/env")

    all_dfs = []
    for symbol in SYMBOLS:
        df = fetch_daily_adjusted(api_key, symbol)
        if df is not None:
            all_dfs.append(df)

    if not all_dfs:
        raise ValueError("No data fetched for any symbol.")

    combined = pd.concat(all_dfs, ignore_index=True)
    output_path = "/tmp/all_stocks_daily.csv"
    combined.to_csv(output_path, index=False)

    context["ti"].xcom_push(key="local_csv_path", value=output_path)
    context["ti"].xcom_push(key="row_count", value=len(combined))
    print(f"[{PIPELINE_ENV}] Fetched {len(combined)} rows for {len(all_dfs)} symbols → {output_path}")


def check_bq_data_quality(**context):
    """Run data quality checks on all medallion layers."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)

    checks = {
        "bronze_stock_data": "Bronze",
        "silver_stock_data": "Silver",
        "gold_technical_indicators": "Gold (Indicators)",
        "gold_daily_summary": "Gold (Summary)",
    }

    for table_name, layer in checks.items():
        query = f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`"
        try:
            result = list(client.query(query).result())
            count = result[0][0] if result else 0
            print(f"  ✅ [{PIPELINE_ENV}] {layer} → {table_name}: {count} rows")

            if count == 0:
                raise ValueError(f"Data quality failed: {table_name} has 0 rows!")
        except Exception as e:
            print(f"  ❌ [{PIPELINE_ENV}] {layer} → {table_name}: {e}")
            raise


# ============================================================
# DAG Definition — Medallion Architecture
# ============================================================
# fetch_data → upload_to_gcs → upload_scripts → create_cluster
#   → bronze_job → silver_job → gold_job
#   → delete_cluster → data_quality_check
# ============================================================

BRONZE_SCRIPT = f"gs://{GCS_BUCKET}/scripts/bronze_layer.py"
SILVER_SCRIPT = f"gs://{GCS_BUCKET}/scripts/silver_layer.py"
GOLD_SCRIPT = f"gs://{GCS_BUCKET}/scripts/gold_layer.py"

with DAG(
    dag_id=f"{PIPELINE_ENV}_stock_market_data_pipeline",
    default_args=default_args,
    description=f"[{PIPELINE_ENV.upper()}] Medallion pipeline: Bronze → Silver → Gold → BigQuery",
    schedule_interval="0 9 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["stock-market", "data-pipeline", "gcp", PIPELINE_ENV, "medallion"],
) as dag:

    # ── Task 1: Fetch historical data ──────────────────────
    fetch_data = PythonOperator(
        task_id="fetch_historical_data",
        python_callable=fetch_historical_data,
        provide_context=True,
    )

    # ── Task 2: Upload CSV to GCS ──────────────────────────
    upload_to_gcs = BashOperator(
        task_id="upload_csv_to_gcs",
        bash_command=(
            f"gsutil cp /tmp/all_stocks_daily.csv "
            f"gs://{GCS_BUCKET}/raw/all_stocks_daily.csv"
        ),
    )

    # ── Task 3: Upload all layer scripts to GCS ────────────
    upload_scripts = BashOperator(
        task_id="upload_layer_scripts",
        bash_command=(
            f"gsutil cp /home/airflow/gcs/dags/src/batch/bronze_layer.py gs://{GCS_BUCKET}/scripts/bronze_layer.py && "
            f"gsutil cp /home/airflow/gcs/dags/src/batch/silver_layer.py gs://{GCS_BUCKET}/scripts/silver_layer.py && "
            f"gsutil cp /home/airflow/gcs/dags/src/batch/gold_layer.py gs://{GCS_BUCKET}/scripts/gold_layer.py"
        ),
    )

    # ── Task 4: Create Dataproc cluster ────────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER,
        region=GCP_REGION,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
    )

    # ── Task 5: BRONZE — Raw data ingestion ────────────────
    bronze_job = DataprocSubmitPySparkJobOperator(
        task_id="bronze_layer",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
        main=BRONZE_SCRIPT,
        arguments=[
            "--input_path", f"gs://{GCS_BUCKET}/raw/all_stocks_daily.csv",
            "--output_table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.bronze_stock_data",
            "--temp_gcs_bucket", GCS_BUCKET,
        ],
        dataproc_jars=[SPARK_BQ_JAR],
    )

    # ── Task 6: SILVER — Data cleansing ────────────────────
    silver_job = DataprocSubmitPySparkJobOperator(
        task_id="silver_layer",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
        main=SILVER_SCRIPT,
        arguments=[
            "--input_table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.bronze_stock_data",
            "--output_table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.silver_stock_data",
            "--temp_gcs_bucket", GCS_BUCKET,
        ],
        dataproc_jars=[SPARK_BQ_JAR],
    )

    # ── Task 7: GOLD — Business aggregations ───────────────
    gold_job = DataprocSubmitPySparkJobOperator(
        task_id="gold_layer",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
        main=GOLD_SCRIPT,
        arguments=[
            "--input_table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.silver_stock_data",
            "--indicators_table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.gold_technical_indicators",
            "--summary_table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.gold_daily_summary",
            "--temp_gcs_bucket", GCS_BUCKET,
        ],
        dataproc_jars=[SPARK_BQ_JAR],
    )

    # ── Task 8: Delete Dataproc cluster (always) ───────────
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task 9: Data quality checks ────────────────────────
    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_bq_data_quality,
        provide_context=True,
    )

    # ── Task Dependencies (Medallion Chain) ────────────────
    # fetch → upload → create cluster → bronze → silver → gold → cleanup
    fetch_data >> [upload_to_gcs, upload_scripts]
    [upload_to_gcs, upload_scripts] >> create_cluster
    create_cluster >> bronze_job >> silver_job >> gold_job
    gold_job >> [delete_cluster, data_quality_check]

