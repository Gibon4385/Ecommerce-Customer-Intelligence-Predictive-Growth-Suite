# Purpose: Constrains the supported Terraform CLI and Google Provider version ranges.
# Together with .terraform.lock.hcl, this keeps provider behavior consistent across environments.

terraform {
  required_version = ">= 1.5, < 2.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.39"
    }
  }
}
