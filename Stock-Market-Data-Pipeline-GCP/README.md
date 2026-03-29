# 📈 Stock Market Data Pipeline — GCP

An end-to-end data pipeline built on **Google Cloud Platform** that ingests stock market data in both **real-time** and **batch** modes, processes it, and stores everything in **BigQuery** for analytics.

---

## 🏗️ Architecture

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                    Google Cloud Platform                │
                        │                                                         │
  ┌──────────────┐      │   ┌──────────┐     ┌──────────────┐     ┌───────────┐  │
  │  Stock API   │──────┼──▶│  Pub/Sub  │────▶│   Dataflow   │────▶│           │  │
  │ (Alpha       │      │   │  Topic    │     │ (Apache Beam)│     │           │  │
  │  Vantage)    │      │   └──────────┘     └──────────────┘     │           │  │
  └──────────────┘      │                                          │  BigQuery │  │
                        │   ┌──────────┐     ┌──────────────┐     │           │  │
  ┌──────────────┐      │   │  Cloud   │     │   Dataproc   │     │  (Data    │  │
  │ Historical   │──────┼──▶│  Storage │────▶│  (PySpark)   │────▶│Warehouse) │  │
  │ Data (CSV)   │      │   │  (GCS)   │     │              │     │           │  │
  └──────────────┘      │   └──────────┘     └──────────────┘     └───────────┘  │
                        │                                                         │
                        │   ┌──────────────────────────────────────────────────┐  │
                        │   │         Cloud Composer (Apache Airflow)           │  │
                        │   │              Orchestration Layer                  │  │
                        │   └──────────────────────────────────────────────────┘  │
                        └─────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
Stock-Market-Data-Pipeline-GCP/
│
├── config/
│   └── config.yaml                  # Central configuration
│
├── src/
│   ├── realtime/
│   │   ├── publisher.py             # Pub/Sub publisher (API → Pub/Sub)
│   │   ├── cloud_run_publisher.py   # Cloud Run entry point (health check + publisher)
│   │   ├── dataflow_pipeline.py     # Dataflow pipeline (Pub/Sub → BigQuery)
│   │   └── schema.py               # BigQuery schema definitions
│   │
│   ├── batch/
│   │   ├── fetch_historical_data.py # Download historical stock CSVs
│   │   ├── upload_to_gcs.py         # Upload files to Cloud Storage
│   │   └── spark_batch_job.py       # PySpark job (GCS → BigQuery)
│   │
│   └── utils/
│       ├── bq_helpers.py            # BigQuery helper functions
│       ├── gcs_helpers.py           # Cloud Storage helper functions
│       └── logging_config.py        # Centralized logging
│
├── dags/
│   └── stock_pipeline_dag.py        # Airflow DAG for orchestration
│
├── terraform/
│   ├── main.tf                      # Infrastructure as Code
│   ├── variables.tf                 # Terraform variables
│   ├── outputs.tf                   # Terraform outputs
│   └── terraform.tfvars.example     # Example variable values
│
├── Dockerfile                       # Container image for Cloud Run
├── .dockerignore                    # Docker build exclusions
├── .env.example                     # Environment variable template
├── .gitignore
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## 🔧 GCP Services Used

| Service | Purpose |
|---------|---------|
| **Pub/Sub** | Real-time message ingestion from stock APIs |
| **Cloud Run** | Always-on containerized publisher service |
| **Dataflow** | Stream processing with Apache Beam (Pub/Sub → BigQuery) |
| **Cloud Storage** | Data lake for batch CSV files |
| **Dataproc** | Batch processing with Apache Spark (GCS → BigQuery) |
| **BigQuery** | Serverless data warehouse for analytics |
| **Cloud Composer** | Workflow orchestration with Apache Airflow |
| **Artifact Registry** | Docker image repository for Cloud Run |
| **Secret Manager** | Secure API key storage |
| **Vertex AI Workbench** | Jupyter notebooks for development & testing |
| **IAM** | Service account & permissions management |

---

## 🚀 GCP Deployment Guide

### Prerequisites

- **GCP Account** with billing enabled
- **3 GCP Projects** (one per environment: dev, qa, prod)
- Access to **Cloud Shell** (no local installs needed)

### Step 1: Setup GCP Project (run in Cloud Shell)

```bash
# Clone the repo in Cloud Shell
git clone <repo-url>
cd Stock-Market-Data-Pipeline-GCP

# Setup DEV environment (creates all services, IAM, buckets, topics, etc.)
chmod +x scripts/setup_gcp.sh
./scripts/setup_gcp.sh your-gcp-project-id-dev dv

# Setup QA
./scripts/setup_gcp.sh your-gcp-project-id-qa qa

# Setup PROD
./scripts/setup_gcp.sh your-gcp-project-id-prod pd
```

This single command enables all APIs, creates the service account with IAM roles, GCS bucket, Pub/Sub topics, BigQuery dataset, Artifact Registry, and Secret Manager.

### Step 2: Store Your API Key

```bash
# Replace with your actual Alpha Vantage API key
echo 'YOUR_API_KEY' | gcloud secrets versions add alpha-vantage-api-key --data-file=-
```

### Step 3: Deploy Pipeline

```bash
# Deploy to DEV
chmod +x scripts/deploy.sh
./scripts/deploy.sh your-gcp-project-id-dev dv
```

This uploads PySpark scripts to GCS, builds the Docker image, deploys Cloud Run, and starts Dataflow.

### Step 4: Upload DAG to Cloud Composer

```bash
chmod +x scripts/upload_dag.sh
./scripts/upload_dag.sh your-gcp-project-id-dev dv stock-pipeline-composer
```

### Step 5 (Optional): Terraform

If you prefer Infrastructure as Code:

```bash
cd terraform
terraform init
terraform apply -var-file=envs/dv.tfvars    # Dev
terraform apply -var-file=envs/qa.tfvars    # QA
terraform apply -var-file=envs/pd.tfvars    # Prod
```

### Enable Required GCP APIs (done automatically by setup script)

```bash
gcloud services enable \
    pubsub.googleapis.com \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    dataproc.googleapis.com \
    composer.googleapis.com \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    notebooks.googleapis.com \
    aiplatform.googleapis.com \
    secretmanager.googleapis.com
```

---

## 📡 Real-Time Pipeline

### How It Works

1. **Publisher** fetches intraday stock data from Alpha Vantage
2. Publishes each tick as JSON to a **Pub/Sub topic**
3. **Dataflow** (Apache Beam) reads from the subscription
4. Parses, validates, and windows the data (1-min OHLCV aggregation)
5. Writes raw ticks + aggregated data to **BigQuery**

### Run the Publisher (Locally)

```bash
# Start publishing stock data to Pub/Sub
python -m src.realtime.publisher --config config/config.yaml
```

### Deploy Publisher to Cloud Run

The publisher can be containerized and deployed as an **always-on Cloud Run service**:

```bash
# Step 1: Build & push Docker image to Artifact Registry
gcloud builds submit \
    --tag us-central1-docker.pkg.dev/YOUR_PROJECT/stock-pipeline-repo/stock-publisher:latest

# Step 2: Deploy to Cloud Run
gcloud run deploy stock-publisher \
    --image us-central1-docker.pkg.dev/YOUR_PROJECT/stock-pipeline-repo/stock-publisher:latest \
    --region us-central1 \
    --set-env-vars ALPHA_VANTAGE_API_KEY=your-key,GCP_PROJECT_ID=your-project-id \
    --min-instances 1 \
    --max-instances 1 \
    --memory 512Mi \
    --cpu 1 \
    --no-allow-unauthenticated
```

The Cloud Run service includes:
- **Health check endpoint** at `/health` for Cloud Run probes
- **Background publisher thread** that continuously fetches & publishes data
- **Auto-restart** on crashes via Cloud Run's built-in resilience

### Deploy Dataflow Pipeline

```bash
# Local testing
python -m src.realtime.dataflow_pipeline \
    --runner DirectRunner \
    --project your-project-id \
    --input_subscription projects/your-project-id/subscriptions/stock-market-ticks-sub

# Deploy to GCP
python -m src.realtime.dataflow_pipeline \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --input_subscription projects/your-project-id/subscriptions/stock-market-ticks-sub \
    --temp_location gs://your-bucket/temp/ \
    --staging_location gs://your-bucket/staging/ \
    --max_num_workers 3
```

---

## 📦 Batch Pipeline

### How It Works

1. **Fetch** historical daily stock data from Alpha Vantage → CSV
2. **Upload** CSVs to **Cloud Storage** (data lake)
3. **Dataproc** runs a PySpark job that:
   - Reads data from GCS
   - Computes technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands)
   - Writes results to **BigQuery**

### Run Manually

```bash
# Step 1: Fetch historical data
python -m src.batch.fetch_historical_data --symbols AAPL GOOGL MSFT

# Step 2: Upload to GCS
python -m src.batch.upload_to_gcs --source data/historical

# Step 3: Submit Spark job to Dataproc
gcloud dataproc jobs submit pyspark \
    src/batch/spark_batch_job.py \
    --cluster=stock-market-batch-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \
    -- \
    --input_path gs://your-bucket/raw/all_stocks_daily.csv \
    --output_table your-project.stock_market_data.technical_indicators \
    --temp_gcs_bucket your-bucket
```

---

## 🎼 Orchestration (Cloud Composer)

The Airflow DAG in `dags/stock_pipeline_dag.py` automates the **entire batch pipeline**:

```
fetch_data → upload_to_gcs → create_cluster → spark_job → delete_cluster → quality_check
```

- **Schedule**: Weekdays at 9:00 AM UTC
- **Auto-scales**: Creates Dataproc cluster on demand, deletes after job
- **Data quality**: Validates BigQuery row counts after each run

Upload the DAG to your Cloud Composer environment:
```bash
gsutil cp dags/stock_pipeline_dag.py gs://your-composer-bucket/dags/
```

---

## 📊 BigQuery Tables

| Table | Source | Description |
|-------|--------|-------------|
| `raw_stock_ticks` | Real-time (Dataflow) | Raw intraday tick data with OHLCV |
| `stock_aggregated_1min` | Real-time (Dataflow) | 1-minute windowed aggregations with VWAP |
| `historical_stock_data` | Batch (Dataproc) | Daily adjusted OHLCV with dividends & splits |
| `technical_indicators` | Batch (Dataproc) | SMA, EMA, RSI, MACD, Bollinger Bands |

### Sample Queries

```sql
-- Latest prices for each stock
SELECT symbol, price, timestamp
FROM `stock_market_data.raw_stock_ticks`
WHERE DATE(timestamp) = CURRENT_DATE()
ORDER BY timestamp DESC
LIMIT 10;

-- Daily returns ranked by performance
SELECT symbol, date, daily_return_pct
FROM `stock_market_data.technical_indicators`
WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY daily_return_pct DESC;

-- Stocks where RSI indicates oversold (< 30)
SELECT symbol, date, close, rsi_14
FROM `stock_market_data.technical_indicators`
WHERE rsi_14 < 30
ORDER BY date DESC;
```

---

## 🔐 Security

- All GCP resources use a **dedicated service account** with least-privilege IAM roles
- API keys are stored in **environment variables** (never hardcoded)
- Pub/Sub messages are retained for 7 days with dead-letter queue support
- BigQuery tables use **column-level** partitioning and clustering for cost optimization

---

## 💰 Cost Optimization

- **Dataproc**: Ephemeral clusters — created before jobs, deleted after completion
- **BigQuery**: Partitioned by date & clustered by symbol to reduce scan costs
- **GCS**: Lifecycle policies auto-move data to Nearline after 90 days
- **Free tier**: Alpha Vantage free API (5 calls/min) is sufficient for this demo

---

## 📚 Learning Resources

- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Dataflow Documentation](https://cloud.google.com/dataflow/docs)
- [Dataproc + PySpark](https://cloud.google.com/dataproc/docs/tutorials/spark-quick-start)
- [BigQuery SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql)
- [Cloud Composer / Airflow](https://cloud.google.com/composer/docs)
