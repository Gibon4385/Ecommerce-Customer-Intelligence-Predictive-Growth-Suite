# Purpose: Creates the single-node Dataproc cluster that runs Spark Structured Streaming.
# This cluster is for portfolio demonstration only and does not provide production high availability.

# This single-node Managed Spark cluster exists to demonstrate Spark Structured
# Streaming. It is intentionally small, has no worker redundancy, and MUST NOT
# be described as a production high-availability deployment.
resource "google_dataproc_cluster" "spark_demo" {
  name   = "ecommerce-spark-demo"
  region = var.region

  # Dataproc expands the cloud-platform scope into equivalent default scopes
  # when it returns the cluster. Ignore that API normalization so a later
  # monitoring-only apply does not unnecessarily replace the running cluster.
  lifecycle {
    ignore_changes = [
      cluster_config[0].gce_cluster_config[0].service_account_scopes,
    ]
  }

  labels = {
    environment = "demo"
    workload    = "structured-streaming"
    ha          = "false"
  }

  cluster_config {
    staging_bucket = google_storage_bucket.data_lake.name
    temp_bucket    = google_storage_bucket.data_lake.name

    gce_cluster_config {
      zone                   = var.zone
      subnetwork             = google_compute_subnetwork.streaming.id
      internal_ip_only       = true
      service_account        = google_service_account.spark_streaming.email
      service_account_scopes = ["cloud-platform"]
      tags                   = ["spark-demo"]
    }

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-4"

      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 50
      }
    }

    software_config {
      image_version = "2.2-debian12"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers"                    = "true"
        "dataproc:dataproc.logging.stackdriver.job.driver.enable" = "true"
        "spark:spark.dynamicAllocation.enabled"                   = "false"
      }
    }

    endpoint_config {
      enable_http_port_access = false
    }
  }

  depends_on = [
    google_project_iam_member.spark_worker,
    google_storage_bucket_iam_member.spark_lake_writer,
    google_compute_router_nat.streaming
  ]
}
