# Purpose: Creates email notification channels, log-based heartbeat metrics, dashboards, and pipeline alerts.
# Alerts cover the producer, Kafka VM and disk, Spark job errors, and Spark-to-GCS freshness.

resource "google_monitoring_notification_channel" "developers_email" {
  count        = var.enable_email_notifications ? 1 : 0
  display_name = "Data platform developers email"
  type         = "email"
  labels = {
    email_address = var.developer_group_email
  }
  force_delete = false
}

resource "google_monitoring_notification_channel" "operators_email" {
  count        = var.enable_email_notifications ? 1 : 0
  display_name = "Data platform operators email"
  type         = "email"
  labels = {
    email_address = var.operator_group_email
  }
  force_delete = false
}

locals {
  pipeline_notification_channels = [
    for channel in concat(
      google_monitoring_notification_channel.developers_email,
      google_monitoring_notification_channel.operators_email
    ) : channel.name
  ]
}

resource "google_logging_metric" "producer_heartbeat" {
  name   = "pipeline/producer_heartbeat"
  filter = <<-EOT
    resource.type="cloud_run_worker_pool"
    textPayload:"PIPELINE_HEARTBEAT component=producer"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_logging_metric" "spark_gcs_heartbeat" {
  name   = "pipeline/spark_gcs_heartbeat"
  filter = <<-EOT
    resource.type="cloud_dataproc_job"
    jsonPayload.message:"PIPELINE_HEARTBEAT component=spark_gcs"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_logging_metric" "spark_input_records" {
  name            = "pipeline/spark_input_records"
  filter          = google_logging_metric.spark_gcs_heartbeat.filter
  value_extractor = "REGEXP_EXTRACT(jsonPayload.message, \"input_records=([0-9]+)\")"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

resource "google_logging_metric" "spark_processed_records" {
  name            = "pipeline/spark_processed_records"
  filter          = google_logging_metric.spark_gcs_heartbeat.filter
  value_extractor = "REGEXP_EXTRACT(jsonPayload.message, \" records=([0-9]+)\")"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

resource "google_logging_metric" "spark_rejected_records" {
  name            = "pipeline/spark_rejected_records"
  filter          = google_logging_metric.spark_gcs_heartbeat.filter
  value_extractor = "REGEXP_EXTRACT(jsonPayload.message, \"rejected=([0-9]+)\")"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

resource "google_logging_metric" "spark_batch_duration" {
  name            = "pipeline/spark_batch_duration_seconds"
  filter          = google_logging_metric.spark_gcs_heartbeat.filter
  value_extractor = "REGEXP_EXTRACT(jsonPayload.message, \"duration_seconds=([0-9.]+)\")"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "s"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 0.01
    }
  }
}

locals {
  dbt_metric_descriptors = {
    run_success = {
      value_type  = "INT64"
      unit        = "1"
      description = "Whether the latest dbt build completed without failed models or tests"
    }
    models_failed = {
      value_type  = "INT64"
      unit        = "1"
      description = "Number of failed models in a dbt build"
    }
    tests_failed = {
      value_type  = "INT64"
      unit        = "1"
      description = "Number of failed tests in a dbt build"
    }
    run_duration_seconds = {
      value_type  = "DOUBLE"
      unit        = "s"
      description = "Total execution time reported by dbt run_results.json"
    }
    last_success_unixtime = {
      value_type  = "DOUBLE"
      unit        = "s"
      description = "Unix timestamp of the latest successful dbt build"
    }
    source_freshness_success = {
      value_type  = "INT64"
      unit        = "1"
      description = "Whether all evaluated streaming dbt sources passed freshness evaluation"
    }
    stale_sources = {
      value_type  = "INT64"
      unit        = "1"
      description = "Number of streaming dbt sources in an error freshness state"
    }
  }
}

resource "google_monitoring_metric_descriptor" "dbt" {
  for_each     = local.dbt_metric_descriptors
  type         = "custom.googleapis.com/dbt/${each.key}"
  metric_kind  = "GAUGE"
  value_type   = each.value.value_type
  unit         = each.value.unit
  display_name = "dbt ${replace(each.key, "_", " ")}"
  description  = each.value.description
}

resource "google_monitoring_alert_policy" "producer_heartbeat_missing" {
  display_name          = "Pipeline - producer heartbeat missing"
  combiner              = "OR"
  notification_channels = local.pipeline_notification_channels

  conditions {
    display_name = "No producer heartbeat for 10 minutes"
    condition_absent {
      filter   = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.producer_heartbeat.name}\" AND resource.type=\"cloud_run_worker_pool\""
      duration = "600s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  documentation {
    content   = "The continuous Python producer stopped reporting. Check the Cloud Run Worker Pool instance and its logs."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_alert_policy" "kafka_vm_down" {
  display_name          = "Pipeline - Kafka VM unavailable"
  combiner              = "OR"
  notification_channels = local.pipeline_notification_channels

  conditions {
    display_name = "Kafka VM uptime metric absent for 5 minutes"
    condition_absent {
      filter   = "metric.type=\"compute.googleapis.com/instance/uptime\" AND resource.type=\"gce_instance\" AND resource.label.instance_id=\"${google_compute_instance.kafka.instance_id}\""
      duration = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  documentation {
    content   = "The single-node Kafka VM is stopped or no longer reporting. This portfolio deployment is not HA."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_alert_policy" "kafka_disk_high" {
  display_name          = "Pipeline - Kafka disk usage high"
  combiner              = "OR"
  notification_channels = local.pipeline_notification_channels

  conditions {
    display_name = "Kafka VM filesystem over 85% for 5 minutes"
    condition_threshold {
      filter          = "metric.type=\"agent.googleapis.com/disk/percent_used\" AND resource.type=\"gce_instance\" AND resource.label.instance_id=\"${google_compute_instance.kafka.instance_id}\" AND metric.label.state=\"used\""
      comparison      = "COMPARISON_GT"
      threshold_value = 85
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  documentation {
    content   = "Kafka VM disk usage exceeded 85%. Check the persistent disk, Kafka retention, and old log segments."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_alert_policy" "spark_job_error" {
  display_name          = "Pipeline - Spark streaming job error"
  combiner              = "OR"
  notification_channels = local.pipeline_notification_channels

  conditions {
    display_name = "Dataproc job entered ERROR state"
    condition_threshold {
      filter          = "metric.type=\"dataproc.googleapis.com/job/state\" AND resource.type=\"cloud_dataproc_job\" AND metric.label.state=\"ERROR\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT_TRUE"
      }
    }
  }

  documentation {
    content   = "The managed-cluster Structured Streaming job entered ERROR. Inspect the Dataproc driver logs and restart/checkpoint recovery."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_alert_policy" "gcs_freshness_missing" {
  display_name          = "Pipeline - GCS data freshness breach"
  combiner              = "OR"
  notification_channels = local.pipeline_notification_channels

  conditions {
    display_name = "No successful Spark-to-GCS micro-batch for 15 minutes"
    condition_absent {
      filter   = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_gcs_heartbeat.name}\" AND resource.type=\"cloud_dataproc_job\""
      duration = "900s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  documentation {
    content   = "No successful Spark micro-batch wrote Parquet to GCS for 15 minutes. Check producer, Kafka lag, Spark logs, and GCS permissions."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_dashboard" "pipeline" {
  dashboard_json = jsonencode({
    displayName = "Ecommerce streaming pipeline health"
    gridLayout = {
      columns = 2
      widgets = [
        {
          title = "Producer heartbeat rate"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.producer_heartbeat.name}\" AND resource.type=\"cloud_run_worker_pool\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE" }
              } }
              plotType = "LINE"
            }]
            yAxis = { label = "heartbeats/s", scale = "LINEAR" }
          }
        },
        {
          title = "Spark to GCS heartbeat rate"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_gcs_heartbeat.name}\" AND resource.type=\"cloud_dataproc_job\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE" }
              } }
              plotType = "LINE"
            }]
            yAxis = { label = "successful batches/s", scale = "LINEAR" }
          }
        },
        {
          title = "Kafka VM CPU utilization"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"compute.googleapis.com/instance/cpu/utilization\" AND resource.type=\"gce_instance\" AND resource.label.instance_id=\"${google_compute_instance.kafka.instance_id}\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MEAN" }
              } }
              plotType = "LINE"
            }]
            yAxis = { label = "utilization", scale = "LINEAR" }
          }
        },
        {
          title = "Kafka VM disk percent used"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"agent.googleapis.com/disk/percent_used\" AND resource.type=\"gce_instance\" AND resource.label.instance_id=\"${google_compute_instance.kafka.instance_id}\" AND metric.label.state=\"used\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MEAN" }
              } }
              plotType = "LINE"
            }]
            thresholds = [{ value = 85 }]
            yAxis      = { label = "percent", scale = "LINEAR" }
          }
        },
        {
          title = "Cloud Run producer instances"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"run.googleapis.com/container/instance_count\" AND resource.type=\"cloud_run_worker_pool\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MEAN" }
              } }
              plotType = "LINE"
            }]
            yAxis = { label = "instances", scale = "LINEAR" }
          }
        },
        {
          title = "Dataproc job ERROR state occurrences"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"dataproc.googleapis.com/job/state\" AND resource.type=\"cloud_dataproc_job\" AND metric.label.state=\"ERROR\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_COUNT_TRUE" }
              } }
              plotType = "STACKED_BAR"
            }]
            yAxis = { label = "true samples/min", scale = "LINEAR" }
          }
        },
        {
          title = "Spark input vs processed records per batch (p50)"
          xyChart = {
            dataSets = [
              {
                legendTemplate = "input"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_input_records.name}\" AND resource.type=\"cloud_dataproc_job\""
                  aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_PERCENTILE_50" }
                } }
                plotType = "LINE"
              },
              {
                legendTemplate = "processed"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_processed_records.name}\" AND resource.type=\"cloud_dataproc_job\""
                  aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_PERCENTILE_50" }
                } }
                plotType = "LINE"
              }
            ]
            yAxis = { label = "records/batch", scale = "LINEAR" }
          }
        },
        {
          title = "Spark quarantine rejected records per batch (p99)"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_rejected_records.name}\" AND resource.type=\"cloud_dataproc_job\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_PERCENTILE_99" }
              } }
              plotType = "STACKED_BAR"
            }]
            yAxis = { label = "records/batch", scale = "LINEAR" }
          }
        },
        {
          title = "Spark micro-batch duration (p99)"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_batch_duration.name}\" AND resource.type=\"cloud_dataproc_job\""
                aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_PERCENTILE_99" }
              } }
              plotType = "LINE"
            }]
            thresholds = [{ value = 5 }]
            yAxis      = { label = "seconds", scale = "LINEAR" }
          }
        },
        {
          title = "dbt run success"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"custom.googleapis.com/dbt/run_success\" AND resource.type=\"global\""
                aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
              } }
              plotType = "STACKED_BAR"
            }]
            yAxis = { label = "1=success", scale = "LINEAR" }
          }
        },
        {
          title = "dbt failed models and tests"
          xyChart = {
            dataSets = [
              {
                legendTemplate = "models failed"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"custom.googleapis.com/dbt/models_failed\" AND resource.type=\"global\""
                  aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
                } }
                plotType = "STACKED_BAR"
              },
              {
                legendTemplate = "tests failed"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"custom.googleapis.com/dbt/tests_failed\" AND resource.type=\"global\""
                  aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
                } }
                plotType = "STACKED_BAR"
              }
            ]
            yAxis = { label = "failures", scale = "LINEAR" }
          }
        },
        {
          title = "dbt run duration"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"custom.googleapis.com/dbt/run_duration_seconds\" AND resource.type=\"global\""
                aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
              } }
              plotType = "LINE"
            }]
            yAxis = { label = "seconds", scale = "LINEAR" }
          }
        },
        {
          title = "dbt last successful run (Unix time)"
          xyChart = {
            dataSets = [{
              timeSeriesQuery = { timeSeriesFilter = {
                filter      = "metric.type=\"custom.googleapis.com/dbt/last_success_unixtime\" AND resource.type=\"global\""
                aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
              } }
              plotType = "LINE"
            }]
            yAxis = { label = "Unix seconds", scale = "LINEAR" }
          }
        },
        {
          title = "dbt source freshness and stale sources"
          xyChart = {
            dataSets = [
              {
                legendTemplate = "freshness success"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"custom.googleapis.com/dbt/source_freshness_success\" AND resource.type=\"global\""
                  aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
                } }
                plotType = "LINE"
              },
              {
                legendTemplate = "stale sources"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"custom.googleapis.com/dbt/stale_sources\" AND resource.type=\"global\""
                  aggregation = { alignmentPeriod = "86400s", perSeriesAligner = "ALIGN_MAX" }
                } }
                plotType = "STACKED_BAR"
              }
            ]
            yAxis = { label = "status/count", scale = "LINEAR" }
          }
        }
      ]
    }
  })

  depends_on = [google_monitoring_metric_descriptor.dbt]
}
