# Purpose: Creates the private network path shared by the producer, Kafka, and Spark.
# It includes a custom VPC, subnet, Private Google Access, Cloud NAT, and a subnet-only Kafka firewall rule.

resource "google_compute_network" "streaming" {
  name                    = "ecommerce-streaming"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "streaming" {
  name                     = "ecommerce-streaming-${var.region}"
  region                   = var.region
  network                  = google_compute_network.streaming.id
  ip_cidr_range            = "10.20.0.0/24"
  private_ip_google_access = true
}

# The Kafka VM has no external IP. Cloud NAT provides outbound-only internet
# access for OS updates and container image downloads without allowing inbound
# connections from the public internet.
resource "google_compute_router" "streaming" {
  name    = "ecommerce-streaming-router"
  region  = var.region
  network = google_compute_network.streaming.id
}

resource "google_compute_router_nat" "streaming" {
  name                               = "ecommerce-streaming-nat"
  router                             = google_compute_router.streaming.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.streaming.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

resource "google_compute_firewall" "kafka_internal" {
  name      = "ecommerce-kafka-internal"
  network   = google_compute_network.streaming.name
  direction = "INGRESS"

  source_ranges = [google_compute_subnetwork.streaming.ip_cidr_range]
  target_tags   = ["kafka-broker"]

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }
}
