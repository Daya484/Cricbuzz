#!/bin/bash
# ============================================================
# GCP Project Setup Script
# ============================================================
# Run this in Cloud Shell to set up your GCP project from scratch.
# Usage:
#   chmod +x scripts/setup_gcp.sh
#   ./scripts/setup_gcp.sh <PROJECT_ID> <ENVIRONMENT>
#
# Example:
#   ./scripts/setup_gcp.sh my-stock-pipeline-dev dv
#   ./scripts/setup_gcp.sh my-stock-pipeline-qa qa
#   ./scripts/setup_gcp.sh my-stock-pipeline-prod pd
# ============================================================

set -euo pipefail

# ── Validate arguments ─────────────────────────────────────
if [ $# -lt 2 ]; then
    echo "Usage: $0 <PROJECT_ID> <ENVIRONMENT>"
    echo "  ENVIRONMENT: dv | qa | pd"
    exit 1
fi

PROJECT_ID=$1
ENVIRONMENT=$2
REGION="us-central1"
ZONE="us-central1-a"

echo "═══════════════════════════════════════════════════"
echo "  GCP Setup — Project: $PROJECT_ID | Env: $ENVIRONMENT"
echo "═══════════════════════════════════════════════════"

# ── Set project ────────────────────────────────────────────
echo ""
echo "▶ Setting active project..."
gcloud config set project "$PROJECT_ID"

# ── Enable APIs ────────────────────────────────────────────
echo ""
echo "▶ Enabling required GCP APIs..."
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
    iam.googleapis.com \
    cloudresourcemanager.googleapis.com \
    secretmanager.googleapis.com

echo "  ✅ All APIs enabled"

# ── Create Service Account ─────────────────────────────────
SA_NAME="stock-pipeline-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo ""
echo "▶ Creating service account: $SA_NAME..."
gcloud iam service-accounts create "$SA_NAME" \
    --display-name="Stock Market Pipeline Service Account" \
    --description="Service account for the stock market data pipeline" \
    2>/dev/null || echo "  (Service account already exists)"

# ── Assign IAM Roles ───────────────────────────────────────
echo ""
echo "▶ Assigning IAM roles..."
ROLES=(
    "roles/bigquery.admin"
    "roles/storage.admin"
    "roles/pubsub.admin"
    "roles/dataflow.admin"
    "roles/dataflow.worker"
    "roles/dataproc.admin"
    "roles/run.invoker"
    "roles/iam.serviceAccountUser"
    "roles/artifactregistry.writer"
    "roles/secretmanager.secretAccessor"
)

for ROLE in "${ROLES[@]}"; do
    echo "  Granting $ROLE..."
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="$ROLE" \
        --quiet
done
echo "  ✅ All roles assigned"

# ── Create GCS Bucket ──────────────────────────────────────
BUCKET_NAME="${ENVIRONMENT}-stock-market-pipeline-bucket"

echo ""
echo "▶ Creating GCS bucket: gs://$BUCKET_NAME..."
gsutil mb -p "$PROJECT_ID" -l "$REGION" -b on \
    "gs://$BUCKET_NAME" 2>/dev/null || echo "  (Bucket already exists)"

# Create directory structure
echo "  Creating folder structure..."
for PREFIX in raw/ processed/ staging/ temp/ scripts/; do
    echo "" | gsutil cp - "gs://$BUCKET_NAME/$PREFIX"
done
echo "  ✅ Bucket ready"

# ── Create Pub/Sub Topic & Subscription ────────────────────
TOPIC="${ENVIRONMENT}-stock-market-ticks"
SUBSCRIPTION="${TOPIC}-sub"
DLQ_TOPIC="${TOPIC}-dlq"

echo ""
echo "▶ Creating Pub/Sub resources..."
gcloud pubsub topics create "$TOPIC" 2>/dev/null || echo "  (Topic already exists)"
gcloud pubsub topics create "$DLQ_TOPIC" 2>/dev/null || echo "  (DLQ topic already exists)"
gcloud pubsub subscriptions create "$SUBSCRIPTION" \
    --topic="$TOPIC" \
    --ack-deadline=60 \
    --message-retention-duration=7d \
    2>/dev/null || echo "  (Subscription already exists)"
echo "  ✅ Pub/Sub ready"

# ── Create BigQuery Dataset ────────────────────────────────
BQ_DATASET="stock_market_data"

echo ""
echo "▶ Creating BigQuery dataset: $BQ_DATASET..."
bq --location=US mk -d \
    --description "Stock market data pipeline — $ENVIRONMENT" \
    "${PROJECT_ID}:${BQ_DATASET}" 2>/dev/null || echo "  (Dataset already exists)"
echo "  ✅ BigQuery dataset ready"

# ── Create Artifact Registry Repository ────────────────────
echo ""
echo "▶ Creating Artifact Registry repository..."
gcloud artifacts repositories create stock-pipeline-repo \
    --repository-format=docker \
    --location="$REGION" \
    --description="Docker images for stock pipeline" \
    2>/dev/null || echo "  (Repository already exists)"
echo "  ✅ Artifact Registry ready"

# ── Store API Key in Secret Manager ────────────────────────
echo ""
echo "▶ Creating Secret Manager placeholder for API key..."
echo "REPLACE_WITH_YOUR_API_KEY" | gcloud secrets create alpha-vantage-api-key \
    --data-file=- \
    --replication-policy="automatic" \
    2>/dev/null || echo "  (Secret already exists — update with: gcloud secrets versions add alpha-vantage-api-key --data-file=-)"
echo "  ✅ Secret Manager ready"
echo "  ⚠️  Remember to update the secret with your actual API key:"
echo "     echo 'YOUR_KEY' | gcloud secrets versions add alpha-vantage-api-key --data-file=-"

# ── Summary ────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✅ GCP Setup Complete!"
echo "═══════════════════════════════════════════════════"
echo ""
echo "  Project:          $PROJECT_ID"
echo "  Environment:      $ENVIRONMENT"
echo "  Service Account:  $SA_EMAIL"
echo "  GCS Bucket:       gs://$BUCKET_NAME"
echo "  Pub/Sub Topic:    $TOPIC"
echo "  BigQuery Dataset: $BQ_DATASET"
echo "  Registry:         $REGION-docker.pkg.dev/$PROJECT_ID/stock-pipeline-repo"
echo ""
echo "  Next steps:"
echo "  1. Update API key: echo 'YOUR_KEY' | gcloud secrets versions add alpha-vantage-api-key --data-file=-"
echo "  2. Deploy Cloud Run: ./scripts/deploy.sh $PROJECT_ID $ENVIRONMENT"
echo "  3. Upload DAG to Cloud Composer"
echo ""
