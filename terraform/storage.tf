# Purpose: Creates the GCS data lake bucket for raw Parquet, Spark checkpoints, and Dataproc staging files.
# The bucket uses uniform access and object versioning, and Terraform cannot empty it automatically on deletion.

resource "google_storage_bucket" "data_lake" {
  name                        = "${var.project_id}-ecommerce-lake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = true
  }

  # This is a disposable portfolio/test lake. Retaining deleted objects would
  # keep generating storage charges after the E2E cleanup has completed.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}
