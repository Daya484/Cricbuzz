# ============================================================
# Terraform Outputs
# ============================================================

output "gcs_bucket_uri" {
  description = "Cloud Storage bucket URI"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "pubsub_topic_path" {
  description = "Pub/Sub topic full path"
  value       = google_pubsub_topic.stock_ticks.id
}

output "pubsub_subscription_path" {
  description = "Pub/Sub subscription full path"
  value       = google_pubsub_subscription.stock_ticks_sub.id
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.stock_data.dataset_id
}

output "service_account_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}

output "cloud_run_url" {
  description = "Cloud Run publisher service URL"
  value       = google_cloud_run_v2_service.stock_publisher.uri
}

output "artifact_registry_repo" {
  description = "Artifact Registry Docker repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}"
}
