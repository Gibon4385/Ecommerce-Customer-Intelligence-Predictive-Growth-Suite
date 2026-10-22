# Purpose: Configures the Google Cloud provider, project, and default region for this root module.
# The provider source and allowed version range are defined in versions.tf.

provider "google" {
  project = var.project_id
  region  = var.region
}
