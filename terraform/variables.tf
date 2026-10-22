# Purpose: Declares root-module inputs, including GCP locations and human Google Group addresses.
# Environment-specific values should come from tfvars or the CLI instead of edits to resource files.

variable "project_id" {
  type        = string
  description = "The GCP Project ID"
}

variable "region" {
  type        = string
  default     = "asia-east1"
  description = "The region to deploy resources"
}

variable "zone" {
  type        = string
  default     = "asia-east1-b"
  description = "Zone for the single-node Kafka VM"
}

variable "developer_group_email" {
  type        = string
  default     = "data-platform-developers@example.com"
  description = "Google Group for read-only platform access and developer alerts. Create it in Workspace/Cloud Identity first."
}

variable "operator_group_email" {
  type        = string
  default     = "data-platform-operators@example.com"
  description = "Google Group for operational access and urgent alerts. Create it in Workspace/Cloud Identity first."
}

variable "enable_human_iam" {
  type        = bool
  default     = true
  description = "Whether to bind developer and operator Google Groups. Disable only for isolated E2E deployments without real groups."
}

variable "enable_email_notifications" {
  type        = bool
  default     = true
  description = "Whether to create Google Group email notification channels. Disable only for isolated E2E deployments."
}

variable "enable_cicd_resources" {
  type        = bool
  default     = true
  description = "Whether to create GitHub Actions Workload Identity Federation. Disable for disposable E2E deployments because deleted pool IDs cannot be immediately reused."
}
