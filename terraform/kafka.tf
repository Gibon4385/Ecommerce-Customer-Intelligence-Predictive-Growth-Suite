# Purpose: Creates the single-node Kafka Compute Engine VM, boot disk, and persistent data disk.
# The VM uses the private subnet from networking.tf; its startup script installs Docker, Kafka, and the Ops Agent.

resource "google_compute_disk" "kafka_data" {
  name = "ecommerce-kafka-data"
  type = "pd-balanced"
  zone = var.zone
  size = 30
}

resource "google_compute_instance" "kafka" {
  name         = "ecommerce-kafka"
  zone         = var.zone
  machine_type = "e2-standard-2"
  tags         = ["kafka-broker"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
    }
  }

  attached_disk {
    source      = google_compute_disk.kafka_data.id
    device_name = "kafka-data"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.streaming.id
  }

  metadata_startup_script = file("${path.module}/templates/kafka-startup.sh")

  service_account {
    email  = google_service_account.kafka_broker.email
    scopes = ["cloud-platform"]
  }
}
