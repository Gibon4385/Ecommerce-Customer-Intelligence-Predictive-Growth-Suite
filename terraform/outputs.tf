# Purpose: Centralizes Terraform outputs needed when deploying or submitting workloads.
# It currently exposes the private Kafka endpoint, data lake bucket, and Dataproc cluster name.

output "kafka_internal_bootstrap_server" {
  description = "Private Kafka endpoint for Cloud Run Worker Pool and Managed Spark"
  value       = "${google_compute_instance.kafka.network_interface[0].network_ip}:9092"
}

output "data_lake_bucket" {
  description = "GCS bucket used by Spark, BigQuery external tables, and dbt"
  value       = google_storage_bucket.data_lake.name
}

output "spark_cluster_name" {
  description = "Single-node non-HA Managed Spark demonstration cluster"
  value       = google_dataproc_cluster.spark_demo.name
}
