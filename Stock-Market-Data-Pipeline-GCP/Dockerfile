# ============================================================
# Dockerfile — Stock Market Publisher (Cloud Run)
# ============================================================
# Containerizes the publisher.py script for deployment on
# Google Cloud Run as an always-on service.
#
# Build & Deploy:
#   gcloud builds submit --tag gcr.io/YOUR_PROJECT/stock-publisher
#   gcloud run deploy stock-publisher \
#       --image gcr.io/YOUR_PROJECT/stock-publisher \
#       --region us-central1 \
#       --set-env-vars ALPHA_VANTAGE_API_KEY=xxx,GCP_PROJECT_ID=xxx
# ============================================================

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (for Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config/ config/
COPY src/ src/

# Expose port for Cloud Run health checks
EXPOSE 8080

# Run the Cloud Run entry point (includes health check server + publisher)
CMD ["python", "-m", "src.realtime.cloud_run_publisher"]
