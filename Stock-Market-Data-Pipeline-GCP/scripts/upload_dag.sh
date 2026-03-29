#!/bin/bash
# ============================================================
# Upload DAG to Cloud Composer
# ============================================================
# Usage:
#   ./scripts/upload_dag.sh <PROJECT_ID> <ENVIRONMENT> <COMPOSER_ENV_NAME>
#
# Example:
#   ./scripts/upload_dag.sh my-stock-dev dv stock-pipeline-composer
# ============================================================

set -euo pipefail

if [ $# -lt 3 ]; then
    echo "Usage: $0 <PROJECT_ID> <ENVIRONMENT> <COMPOSER_ENVIRONMENT_NAME>"
    exit 1
fi

PROJECT_ID=$1
ENVIRONMENT=$2
COMPOSER_ENV=$3
REGION="us-central1"

gcloud config set project "$PROJECT_ID"

echo "▶ Finding Cloud Composer DAGs bucket..."
DAGS_BUCKET=$(gcloud composer environments describe "$COMPOSER_ENV" \
    --location="$REGION" \
    --format="value(config.dagGcsPrefix)")

if [ -z "$DAGS_BUCKET" ]; then
    echo "❌ Could not find Composer environment: $COMPOSER_ENV"
    exit 1
fi

echo "  DAGs bucket: $DAGS_BUCKET"

echo ""
echo "▶ Uploading DAG and source code..."
gsutil cp dags/stock_pipeline_dag.py "${DAGS_BUCKET}/stock_pipeline_dag.py"

# Upload src package so the DAG can import it
gsutil -m cp -r src/ "${DAGS_BUCKET}/src/"

echo ""
echo "▶ Setting Airflow variable: ENV=$ENVIRONMENT"
gcloud composer environments run "$COMPOSER_ENV" \
    --location="$REGION" \
    variables set -- ENV "$ENVIRONMENT"

echo ""
echo "▶ Setting Airflow variable: ALPHA_VANTAGE_API_KEY"
API_KEY=$(gcloud secrets versions access latest --secret="alpha-vantage-api-key" 2>/dev/null || echo "")
if [ -n "$API_KEY" ] && [ "$API_KEY" != "REPLACE_WITH_YOUR_API_KEY" ]; then
    gcloud composer environments run "$COMPOSER_ENV" \
        --location="$REGION" \
        variables set -- ALPHA_VANTAGE_API_KEY "$API_KEY"
    echo "  ✅ API key set in Airflow"
else
    echo "  ⚠️  API key not set. Set it manually in Airflow UI → Admin → Variables"
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✅ DAG uploaded to Cloud Composer!"
echo "═══════════════════════════════════════════════════"
echo "  Airflow UI: https://console.cloud.google.com/composer/environments?project=$PROJECT_ID"
echo "  DAG ID: ${ENVIRONMENT}_stock_market_data_pipeline"
echo ""
