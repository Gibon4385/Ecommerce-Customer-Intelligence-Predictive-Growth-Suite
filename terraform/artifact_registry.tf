# Purpose: Creates the Artifact Registry Docker repository used by the streaming pipeline.
# Cloud Build pushes the producer image here before it is run by a Cloud Run Worker Pool.

resource "google_artifact_registry_repository" "streaming" {
  location      = var.region
  repository_id = "ecommerce-streaming"
  format        = "DOCKER"
  description   = "Container images for the ecommerce streaming pipeline"
}
