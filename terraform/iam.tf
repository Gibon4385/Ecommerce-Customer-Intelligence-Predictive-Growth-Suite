# Purpose: Centralizes workload service accounts, least-privilege access, and human Google Group IAM.
# Producer, Kafka, Spark, and dbt use separate identities to prevent cross-workload privilege expansion.

locals {
  developer_roles = toset([
    "roles/compute.viewer",
    "roles/dataproc.viewer",
    "roles/logging.viewer",
    "roles/monitoring.viewer",
  ])

  # Portfolio-sized operator model. compute.instanceAdmin.v1 is intentionally
  # needed to start/stop/reset the Kafka VM; it does not grant service-account
  # impersonation or project IAM administration.
  operator_roles = toset([
    "roles/compute.instanceAdmin.v1",
    "roles/dataproc.editor",
    "roles/logging.viewer",
    "roles/monitoring.editor",
  ])
}

# Workload identities are separated so a compromise in one pipeline component
# does not grant access to another component's data or control plane.
resource "google_service_account" "producer_worker" {
  account_id   = "producer-worker"
  display_name = "Cloud Run continuous Kafka producer"
}

resource "google_service_account" "kafka_broker" {
  account_id   = "kafka-broker"
  display_name = "Kafka broker VM runtime"
}

resource "google_service_account" "spark_streaming" {
  account_id   = "spark-streaming"
  display_name = "Managed Spark streaming runtime"
}

resource "google_service_account" "dbt" {
  account_id   = "ecommerce-dataset"
  display_name = "dbt BigQuery transformation runtime"
}

resource "google_project_iam_member" "producer_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.producer_worker.email}"
}

resource "google_project_iam_member" "producer_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.producer_worker.email}"
}

resource "google_project_iam_member" "kafka_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.kafka_broker.email}"
}

resource "google_project_iam_member" "kafka_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.kafka_broker.email}"
}

resource "google_project_iam_member" "spark_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.spark_streaming.email}"
}

resource "google_project_iam_member" "spark_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.spark_streaming.email}"
}

resource "google_project_iam_member" "spark_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark_streaming.email}"
}

resource "google_project_iam_member" "dbt_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_storage_bucket_iam_member" "spark_lake_writer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark_streaming.email}"
}

resource "google_storage_bucket_iam_member" "dbt_lake_reader" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_member" "raw_viewer" {
  dataset_id = google_bigquery_dataset.raw_ecommerce.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_binding" "dbt_datasets_editor" {
  for_each   = local.dbt_datasets
  dataset_id = google_bigquery_dataset.dbt_datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"

  members = [
    "serviceAccount:${google_service_account.dbt.email}"
  ]
}

resource "google_project_iam_member" "developer_group" {
  for_each = var.enable_human_iam ? local.developer_roles : toset([])
  project  = var.project_id
  role     = each.value
  member   = "group:${var.developer_group_email}"
}

resource "google_project_iam_member" "operator_group" {
  for_each = var.enable_human_iam ? local.operator_roles : toset([])
  project  = var.project_id
  role     = each.value
  member   = "group:${var.operator_group_email}"
}

resource "google_bigquery_dataset_iam_member" "developer_raw_viewer" {
  count      = var.enable_human_iam ? 1 : 0
  dataset_id = google_bigquery_dataset.raw_ecommerce.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:${var.developer_group_email}"
}

resource "google_bigquery_dataset_iam_member" "developer_marts_viewer" {
  count      = var.enable_human_iam ? 1 : 0
  dataset_id = google_bigquery_dataset.dbt_datasets["dbt_ecommerce_marts"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:${var.developer_group_email}"
}
