# ============================================================
# Terraform Variables
# ============================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "gcs_bucket_name" {
  description = "Name of the Cloud Storage bucket (must be globally unique)"
  type        = string
  default     = "stock-market-data-pipeline-bucket"
}

variable "pubsub_topic_name" {
  description = "Pub/Sub topic name for real-time stock ticks"
  type        = string
  default     = "stock-market-ticks"
}

variable "pubsub_subscription_name" {
  description = "Pub/Sub subscription name"
  type        = string
  default     = "stock-market-ticks-sub"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "stock_market_data"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}
