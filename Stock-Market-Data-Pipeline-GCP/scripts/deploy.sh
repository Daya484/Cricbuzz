#!/bin/bash
# ============================================================
# Deployment Script — Deploy Pipeline to GCP
# ============================================================
# Deploys all components to a specific environment.
#
# Usage:
#   chmod +x scripts/deploy.sh
#   ./scripts/deploy.sh <PROJECT_ID> <ENVIRONMENT>
#
# Example:
#   ./scripts/deploy.sh my-stock-dev dv
# ============================================================

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <PROJECT_ID> <ENVIRONMENT>"
    exit 1
fi

PROJECT_ID=$1
ENVIRONMENT=$2
REGION="us-central1"
BUCKET="${ENVIRONMENT}-stock-market-pipeline-bucket"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/stock-pipeline-repo"

echo "═══════════════════════════════════════════════════"
echo "  Deploy — Project: $PROJECT_ID | Env: $ENVIRONMENT"
echo "═══════════════════════════════════════════════════"

gcloud config set project "$PROJECT_ID"

# ── Step 1: Upload PySpark scripts to GCS ──────────────────
echo ""
echo "▶ [1/4] Uploading PySpark layer scripts to GCS..."
gsutil cp src/batch/bronze_layer.py "gs://${BUCKET}/scripts/bronze_layer.py"
gsutil cp src/batch/silver_layer.py "gs://${BUCKET}/scripts/silver_layer.py"
gsutil cp src/batch/gold_layer.py   "gs://${BUCKET}/scripts/gold_layer.py"
echo "  ✅ Scripts uploaded to gs://${BUCKET}/scripts/"

# ── Step 2: Build & Push Docker image ──────────────────────
echo ""
echo "▶ [2/4] Building and pushing Docker image..."
gcloud builds submit \
    --tag "${REGISTRY}/stock-publisher:latest" \
    --timeout=600
echo "  ✅ Docker image pushed to ${REGISTRY}/stock-publisher:latest"

# ── Step 3: Deploy Cloud Run ───────────────────────────────
echo ""
echo "▶ [3/4] Deploying Cloud Run publisher..."

# Get API key from Secret Manager
API_KEY=$(gcloud secrets versions access latest --secret="alpha-vantage-api-key" 2>/dev/null || echo "")

if [ -z "$API_KEY" ] || [ "$API_KEY" = "REPLACE_WITH_YOUR_API_KEY" ]; then
    echo "  ⚠️  API key not set in Secret Manager. Skipping Cloud Run deploy."
    echo "  Set it with: echo 'YOUR_KEY' | gcloud secrets versions add alpha-vantage-api-key --data-file=-"
else
    gcloud run deploy stock-publisher \
        --image "${REGISTRY}/stock-publisher:latest" \
        --region "$REGION" \
        --service-account "stock-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
        --set-env-vars "ALPHA_VANTAGE_API_KEY=${API_KEY},GCP_PROJECT_ID=${PROJECT_ID},ENV=${ENVIRONMENT},PUBSUB_TOPIC=${ENVIRONMENT}-stock-market-ticks" \
        --min-instances 1 \
        --max-instances 1 \
        --memory 512Mi \
        --cpu 1 \
        --no-allow-unauthenticated \
        --quiet
    echo "  ✅ Cloud Run publisher deployed"
fi

# ── Step 4: Deploy Dataflow Pipeline ───────────────────────
echo ""
echo "▶ [4/4] Deploying Dataflow streaming pipeline..."
python -m src.realtime.dataflow_pipeline \
    --runner DataflowRunner \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --input_subscription "projects/${PROJECT_ID}/subscriptions/${ENVIRONMENT}-stock-market-ticks-sub" \
    --temp_location "gs://${BUCKET}/temp/" \
    --staging_location "gs://${BUCKET}/staging/" \
    --max_num_workers 3 \
    --job_name "stock-market-stream-processor" \
    2>/dev/null &

echo "  ✅ Dataflow pipeline submitted (running in background)"

# ── Summary ────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✅ Deployment Complete!"
echo "═══════════════════════════════════════════════════"
echo ""
echo "  Cloud Run:  https://console.cloud.google.com/run?project=$PROJECT_ID"
echo "  Dataflow:   https://console.cloud.google.com/dataflow?project=$PROJECT_ID"
echo "  BigQuery:   https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo "  GCS:        https://console.cloud.google.com/storage?project=$PROJECT_ID"
echo ""
echo "  Remaining manual steps:"
echo "  1. Upload DAG to Cloud Composer:"
echo "     gsutil cp dags/stock_pipeline_dag.py gs://YOUR_COMPOSER_BUCKET/dags/"
echo "  2. Set ENV variable in Airflow UI → Admin → Variables → ENV=$ENVIRONMENT"
echo ""
