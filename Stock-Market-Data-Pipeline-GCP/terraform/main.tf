# ============================================================
# Stock Market Data Pipeline — Terraform Configuration
# ============================================================
# Provisions: Pub/Sub, BigQuery, Cloud Storage, Cloud Run, IAM
# ============================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}


# ────────────────────────────────────────────────────────────
# Cloud Storage — Data Lake
# ────────────────────────────────────────────────────────────

resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90    # Move to Nearline after 90 days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365   # Delete after 1 year
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project     = "stock-market-pipeline"
    environment = var.environment
  }
}

# Create directory structure in the bucket
resource "google_storage_bucket_object" "raw_prefix" {
  name    = "raw/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "processed_prefix" {
  name    = "processed/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "staging_prefix" {
  name    = "staging/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "temp_prefix" {
  name    = "temp/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "scripts_prefix" {
  name    = "scripts/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}


# ────────────────────────────────────────────────────────────
# Pub/Sub — Real-time Data Ingestion
# ────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "stock_ticks" {
  name = var.pubsub_topic_name

  labels = {
    project = "stock-market-pipeline"
  }

  message_retention_duration = "86400s"    # 24 hours
}

resource "google_pubsub_subscription" "stock_ticks_sub" {
  name  = var.pubsub_subscription_name
  topic = google_pubsub_topic.stock_ticks.name

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"   # 7 days

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    project = "stock-market-pipeline"
  }
}

# Dead Letter Topic
resource "google_pubsub_topic" "stock_ticks_dlq" {
  name = "${var.pubsub_topic_name}-dlq"
}


# ────────────────────────────────────────────────────────────
# BigQuery — Data Warehouse
# ────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "stock_data" {
  dataset_id                 = var.bq_dataset_id
  friendly_name              = "Stock Market Data"
  description                = "Stock market data from real-time and batch pipelines"
  location                   = var.bq_location
  delete_contents_on_destroy = true

  labels = {
    project     = "stock-market-pipeline"
    environment = var.environment
  }
}

resource "google_bigquery_table" "raw_stock_ticks" {
  dataset_id          = google_bigquery_dataset.stock_data.dataset_id
  table_id            = "raw_stock_ticks"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",         type = "STRING",    mode = "REQUIRED" },
    { name = "price",          type = "FLOAT64",   mode = "REQUIRED" },
    { name = "volume",         type = "INTEGER",   mode = "NULLABLE" },
    { name = "timestamp",      type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "open",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "high",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "low",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "close",          type = "FLOAT64",   mode = "NULLABLE" },
    { name = "ingestion_time", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "stock_aggregated_1min" {
  dataset_id          = google_bigquery_dataset.stock_data.dataset_id
  table_id            = "stock_aggregated_1min"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",       type = "STRING",    mode = "REQUIRED" },
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "open",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "high",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "low",          type = "FLOAT64",   mode = "NULLABLE" },
    { name = "close",        type = "FLOAT64",   mode = "NULLABLE" },
    { name = "vwap",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "total_volume", type = "INTEGER",   mode = "NULLABLE" },
    { name = "tick_count",   type = "INTEGER",   mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "bronze_stock_data" {
  dataset_id          = google_bigquery_dataset.stock_data.dataset_id
  table_id            = "bronze_stock_data"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "load_date"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",            type = "STRING",    mode = "NULLABLE" },
    { name = "date",              type = "STRING",    mode = "NULLABLE" },
    { name = "open",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "high",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "low",               type = "FLOAT64",   mode = "NULLABLE" },
    { name = "close",             type = "FLOAT64",   mode = "NULLABLE" },
    { name = "adjusted_close",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "volume",            type = "INTEGER",   mode = "NULLABLE" },
    { name = "dividend_amount",   type = "FLOAT64",   mode = "NULLABLE" },
    { name = "split_coefficient", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "load_timestamp",    type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "source_file",       type = "STRING",    mode = "NULLABLE" },
    { name = "load_date",         type = "DATE",      mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "silver_stock_data" {
  dataset_id          = google_bigquery_dataset.stock_data.dataset_id
  table_id            = "silver_stock_data"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",                type = "STRING",    mode = "NULLABLE" },
    { name = "date",                  type = "DATE",      mode = "NULLABLE" },
    { name = "open",                  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "high",                  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "low",                   type = "FLOAT64",   mode = "NULLABLE" },
    { name = "close",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "adjusted_close",        type = "FLOAT64",   mode = "NULLABLE" },
    { name = "volume",                type = "INTEGER",   mode = "NULLABLE" },
    { name = "dividend_amount",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "split_coefficient",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "source_file",           type = "STRING",    mode = "NULLABLE" },
    { name = "load_timestamp",        type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "silver_load_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "gold_technical_indicators" {
  dataset_id          = google_bigquery_dataset.stock_data.dataset_id
  table_id            = "gold_technical_indicators"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",              type = "STRING",    mode = "NULLABLE" },
    { name = "date",                type = "DATE",      mode = "NULLABLE" },
    { name = "close",               type = "FLOAT64",   mode = "NULLABLE" },
    { name = "sma_20",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "sma_50",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "ema_12",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "ema_26",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "rsi_14",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "macd",                type = "FLOAT64",   mode = "NULLABLE" },
    { name = "macd_signal",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "bollinger_upper",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "bollinger_lower",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "daily_return_pct",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "gold_load_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "gold_daily_summary" {
  dataset_id          = google_bigquery_dataset.stock_data.dataset_id
  table_id            = "gold_daily_summary"
  deletion_protection = false

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",              type = "STRING",    mode = "NULLABLE" },
    { name = "total_trading_days",  type = "INTEGER",   mode = "NULLABLE" },
    { name = "first_date",          type = "DATE",      mode = "NULLABLE" },
    { name = "last_date",           type = "DATE",      mode = "NULLABLE" },
    { name = "avg_close",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "all_time_low",        type = "FLOAT64",   mode = "NULLABLE" },
    { name = "all_time_high",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "avg_daily_volume",    type = "INTEGER",   mode = "NULLABLE" },
    { name = "total_volume",        type = "INTEGER",   mode = "NULLABLE" },
    { name = "close_stddev",        type = "FLOAT64",   mode = "NULLABLE" },
    { name = "avg_adjusted_close",  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "gold_load_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}


# ────────────────────────────────────────────────────────────
# IAM — Service Account for Pipeline
# ────────────────────────────────────────────────────────────

resource "google_service_account" "pipeline_sa" {
  account_id   = "stock-pipeline-sa"
  display_name = "Stock Market Pipeline Service Account"
}

resource "google_project_iam_member" "pipeline_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_pubsub_admin" {
  project = var.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_dataproc_admin" {
  project = var.project_id
  role    = "roles/dataproc.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}


# ────────────────────────────────────────────────────────────
# Artifact Registry — Docker Image Repository
# ────────────────────────────────────────────────────────────

resource "google_artifact_registry_repository" "docker_repo" {
  location      = var.region
  repository_id = "stock-pipeline-repo"
  description   = "Docker images for the stock market pipeline"
  format        = "DOCKER"

  labels = {
    project     = "stock-market-pipeline"
    environment = var.environment
  }
}


# ────────────────────────────────────────────────────────────
# Cloud Run — Stock Publisher Service (Always-on)
# ────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_service" "stock_publisher" {
  name     = "stock-publisher"
  location = var.region

  template {
    service_account = google_service_account.pipeline_sa.email

    scaling {
      min_instance_count = 1     # Always-on (at least 1 instance)
      max_instance_count = 1     # Only need 1 publisher
    }

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/stock-publisher:latest"

      ports {
        container_port = 8080
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "PUBSUB_TOPIC"
        value = var.pubsub_topic_name
      }

      env {
        name  = "STOCK_SYMBOLS"
        value = "AAPL,GOOGL,MSFT,AMZN,TSLA"
      }

      env {
        name  = "POLL_INTERVAL"
        value = "60"
      }

      # API key should be set via Secret Manager or manual env var
      # env {
      #   name = "ALPHA_VANTAGE_API_KEY"
      #   value_source {
      #     secret_key_ref {
      #       secret  = "alpha-vantage-api-key"
      #       version = "latest"
      #     }
      #   }
      # }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 5
        period_seconds        = 10
        failure_threshold     = 3
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        period_seconds = 30
      }
    }
  }

  labels = {
    project     = "stock-market-pipeline"
    environment = var.environment
  }

  depends_on = [
    google_artifact_registry_repository.docker_repo,
  ]
}

# IAM: Allow Cloud Run to act as the pipeline service account
resource "google_project_iam_member" "pipeline_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
