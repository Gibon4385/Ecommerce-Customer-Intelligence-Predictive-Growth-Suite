# Purpose: Creates the artifact bucket and GitHub Actions Workload Identity Federation for dbt CI/CD.
# GitHub Actions uses short-lived OIDC credentials to impersonate the dbt service account without JSON keys.

# Create GCS Bucket to store dbt artifacts (manifest.json, docs)
resource "google_storage_bucket" "dbt_artifacts" {
  name          = "${var.project_id}-dbt-artifacts"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

# Workload Identity Pool for GitHub Actions
resource "google_iam_workload_identity_pool" "github_pool" {
  count                     = var.enable_cicd_resources ? 1 : 0
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions Pool"
  description               = "Identity pool for GitHub Actions CI/CD"
}

# Workload Identity Provider for GitHub Actions
resource "google_iam_workload_identity_pool_provider" "github_provider" {
  count                              = var.enable_cicd_resources ? 1 : 0
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool[0].workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions-provider"
  display_name                       = "GitHub Actions Provider"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "attribute.repository == 'PoChaoWang/Ecommerce-Customer-Intelligence-Predictive-Growth-Suite'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# Bind the existing dbt Service Account to Workload Identity User
# This allows GitHub Actions repository to impersonate the Service Account
resource "google_service_account_iam_member" "wif_binding" {
  count              = var.enable_cicd_resources ? 1 : 0
  service_account_id = google_service_account.dbt.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool[0].name}/attribute.repository/PoChaoWang/Ecommerce-Customer-Intelligence-Predictive-Growth-Suite"
}

# Grant the Service Account permissions to manage the GCS Bucket
resource "google_storage_bucket_iam_binding" "bucket_admin" {
  bucket = google_storage_bucket.dbt_artifacts.name
  role   = "roles/storage.objectAdmin"

  members = [
    "serviceAccount:${google_service_account.dbt.email}"
  ]
}

# Grant BigQuery Admin/User permission to the Service Account at project level
# (Required for running dbt models and creating dynamic CI datasets)
resource "google_project_iam_binding" "bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"

  members = [
    "serviceAccount:${google_service_account.dbt.email}"
  ]
}
