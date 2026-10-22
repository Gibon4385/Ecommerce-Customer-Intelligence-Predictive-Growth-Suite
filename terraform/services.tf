# Purpose: Enables the GCP APIs managed by Terraform and required for observability.
# disable_on_destroy=false prevents shared APIs from being disabled when these resources are removed.

locals {
  observability_services = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "sts.googleapis.com",
  ])
}

resource "google_project_service" "observability" {
  for_each           = local.observability_services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
