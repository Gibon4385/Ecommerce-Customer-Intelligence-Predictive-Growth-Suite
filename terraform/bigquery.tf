# Purpose: Creates the BigQuery raw/dbt datasets and raw external tables, and centralizes raw schemas.
# External tables query Parquet in the GCS data lake directly; data access is defined in iam.tf.

locals {
  dbt_datasets = toset([
    "dbt_ecommerce_staging",
    "dbt_ecommerce_intermediate",
    "dbt_ecommerce_marts"
  ])

  ingestion_metadata_schema = [
    { name = "_source_topic", type = "STRING", mode = "NULLABLE" },
    { name = "_kafka_partition", type = "INTEGER", mode = "NULLABLE" },
    { name = "_kafka_offset", type = "INTEGER", mode = "NULLABLE" },
    { name = "_kafka_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "_ingested_at", type = "TIMESTAMP", mode = "NULLABLE" }
  ]

  raw_table_schemas = {
    raw_users = concat([
      { name = "user_id", type = "STRING", mode = "NULLABLE" },
      { name = "name", type = "STRING", mode = "NULLABLE" },
      { name = "email", type = "STRING", mode = "NULLABLE" },
      { name = "gender", type = "STRING", mode = "NULLABLE" },
      { name = "city", type = "STRING", mode = "NULLABLE" },
      { name = "signup_date", type = "STRING", mode = "NULLABLE" }
    ], local.ingestion_metadata_schema)
    raw_events = concat([
      { name = "event_id", type = "STRING", mode = "NULLABLE" },
      { name = "user_id", type = "STRING", mode = "NULLABLE" },
      { name = "product_id", type = "STRING", mode = "NULLABLE" },
      { name = "event_type", type = "STRING", mode = "NULLABLE" },
      { name = "event_timestamp", type = "STRING", mode = "NULLABLE" }
    ], local.ingestion_metadata_schema)
    raw_orders = concat([
      { name = "order_id", type = "STRING", mode = "NULLABLE" },
      { name = "user_id", type = "STRING", mode = "NULLABLE" },
      { name = "order_date", type = "STRING", mode = "NULLABLE" },
      { name = "order_status", type = "STRING", mode = "NULLABLE" },
      { name = "total_amount", type = "FLOAT64", mode = "NULLABLE" }
    ], local.ingestion_metadata_schema)
    raw_order_items = concat([
      { name = "order_item_id", type = "STRING", mode = "NULLABLE" },
      { name = "order_id", type = "STRING", mode = "NULLABLE" },
      { name = "product_id", type = "STRING", mode = "NULLABLE" },
      { name = "user_id", type = "STRING", mode = "NULLABLE" },
      { name = "quantity", type = "INT64", mode = "NULLABLE" },
      { name = "item_price", type = "FLOAT64", mode = "NULLABLE" },
      { name = "item_total", type = "FLOAT64", mode = "NULLABLE" }
    ], local.ingestion_metadata_schema)
    raw_reviews = concat([
      { name = "review_id", type = "STRING", mode = "NULLABLE" },
      { name = "order_id", type = "STRING", mode = "NULLABLE" },
      { name = "product_id", type = "STRING", mode = "NULLABLE" },
      { name = "user_id", type = "STRING", mode = "NULLABLE" },
      { name = "rating", type = "INT64", mode = "NULLABLE" },
      { name = "review_text", type = "STRING", mode = "NULLABLE" },
      { name = "review_date", type = "STRING", mode = "NULLABLE" }
    ], local.ingestion_metadata_schema)
    raw_products = concat([
      { name = "product_id", type = "STRING", mode = "NULLABLE" },
      { name = "product_name", type = "STRING", mode = "NULLABLE" },
      { name = "category", type = "STRING", mode = "NULLABLE" },
      { name = "brand", type = "STRING", mode = "NULLABLE" },
      { name = "price", type = "FLOAT64", mode = "NULLABLE" },
      { name = "rating", type = "FLOAT64", mode = "NULLABLE" }
    ], local.ingestion_metadata_schema)
  }

  raw_table_paths = {
    raw_users       = "users"
    raw_events      = "events"
    raw_orders      = "orders"
    raw_order_items = "order_items"
    raw_reviews     = "reviews"
    raw_products    = "products"
  }
}

resource "google_bigquery_dataset" "raw_ecommerce" {
  dataset_id = "raw_ecommerce"
  location   = var.region
}

resource "google_bigquery_dataset" "dbt_datasets" {
  for_each   = local.dbt_datasets
  dataset_id = each.key
  location   = var.region
}

resource "google_bigquery_table" "raw_external" {
  for_each   = local.raw_table_schemas
  dataset_id = google_bigquery_dataset.raw_ecommerce.dataset_id
  table_id   = each.key
  schema     = jsonencode(each.value)

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris = [
      "gs://${google_storage_bucket.data_lake.name}/raw/${local.raw_table_paths[each.key]}/*"
    ]
  }

  deletion_protection = false
}
