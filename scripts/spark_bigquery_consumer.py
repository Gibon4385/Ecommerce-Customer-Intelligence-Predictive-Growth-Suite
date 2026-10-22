import os
import logging
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

logger = logging.getLogger(__name__)

# Define schemas for each topic
users_schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("city", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]
)

events_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
    ]
)

orders_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
    ]
)

order_items_schema = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("item_price", DoubleType(), True),
        StructField("item_total", DoubleType(), True),
    ]
)

reviews_schema = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_date", StringType(), True),
    ]
)


def main():
    parser = argparse.ArgumentParser(
        description="Stream Kafka ecommerce events into the GCS data lake"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--data-lake-uri",
        default=os.getenv("DATA_LAKE_URI"),
        help="GCS URI such as gs://PROJECT-ecommerce-lake",
    )
    args = parser.parse_args()
    if not args.data_lake_uri or not args.data_lake_uri.startswith("gs://"):
        parser.error("--data-lake-uri (or DATA_LAKE_URI) must be a gs:// URI")

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("🚀 Initializing Spark Session...")

    # Connector packages are supplied by spark-submit so the correct Scala
    # build is selected for the Managed Spark cluster runtime.
    spark = SparkSession.builder.appName("EcommerceLakeStreaming").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark Session initialized successfully.")

    # Read from Kafka multi-topic stream
    kafka_bootstrap_servers = args.bootstrap_servers
    logger.info(f"🔌 Connecting to Kafka broker at {kafka_bootstrap_servers}...")
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "db_users,db_events,db_orders,db_order_items,db_reviews")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 150000)
        .option("failOnDataLoss", "false")  # Avoid crash on offset loss
        .load()
    )

    # Preserve Kafka metadata while it is still available. These fields provide
    # ingestion lineage and deterministic evidence for troubleshooting in dbt.
    records_str = kafka_stream.selectExpr(
        "CAST(value AS STRING) AS json_str",
        "topic AS _source_topic",
        "partition AS _kafka_partition",
        "offset AS _kafka_offset",
        "timestamp AS _kafka_timestamp",
    )

    # Topic-to-lake mapping. BigQuery external tables expose these Parquet files
    # under the existing raw_ecommerce.raw_* names consumed by dbt.
    topics_config = {
        "db_users": {"schema": users_schema, "path": "users", "id": "user_id"},
        "db_events": {"schema": events_schema, "path": "events", "id": "event_id"},
        "db_orders": {"schema": orders_schema, "path": "orders", "id": "order_id"},
        "db_order_items": {
            "schema": order_items_schema,
            "path": "order_items",
            "id": "order_item_id",
        },
        "db_reviews": {"schema": reviews_schema, "path": "reviews", "id": "review_id"},
    }

    # Combined writer that writes all topics within a single micro-batch to GCS.
    def write_all_to_lake(batch_df, batch_id):
        batch_started = time.monotonic()
        # Cache the batch DataFrame in memory to avoid reading from Kafka multiple times
        batch_df.persist()

        write_details = []
        total_records = 0
        total_rejected = 0

        for topic_name, cfg in topics_config.items():
            parsed_df = (
                batch_df.filter(col("_source_topic") == topic_name)
                .withColumn("data", from_json(col("json_str"), cfg["schema"]))
                .persist()
            )

            invalid_condition = col("data").isNull() | col("data")[cfg["id"]].isNull()
            invalid_df = parsed_df.filter(invalid_condition).select(
                "json_str",
                "_source_topic",
                "_kafka_partition",
                "_kafka_offset",
                "_kafka_timestamp",
                lit("invalid_json_or_missing_primary_id").alias("_rejection_reason"),
                current_timestamp().alias("_rejected_at"),
            )
            rejected_count = invalid_df.count()
            if rejected_count > 0:
                quarantine = (
                    f"{args.data_lake_uri.rstrip('/')}/quarantine/{cfg['path']}"
                )
                invalid_df.write.mode("append").parquet(quarantine)
                total_rejected += rejected_count

            topic_df = parsed_df.filter(~invalid_condition).select(
                "data.*",
                "_source_topic",
                "_kafka_partition",
                "_kafka_offset",
                "_kafka_timestamp",
                current_timestamp().alias("_ingested_at"),
            )

            # Only write if there is data for this topic in the current batch
            count = topic_df.count()
            if count > 0:
                destination = f"{args.data_lake_uri.rstrip('/')}/raw/{cfg['path']}"
                topic_df.write.mode("append").parquet(destination)
                write_details.append(f"{topic_name}: {count:,} rows")
                total_records += count

            parsed_df.unpersist()

        if write_details:
            batch_duration_seconds = time.monotonic() - batch_started
            logger.info(
                f"⚡ Batch {batch_id} written to GCS | {', '.join(write_details)}"
            )
            logger.info(
                "PIPELINE_HEARTBEAT component=spark_gcs "
                f"batch_id={batch_id} input_records={total_records + total_rejected} "
                f"records={total_records} "
                f"rejected={total_rejected} duration_seconds={batch_duration_seconds:.3f}"
            )

        if total_rejected:
            logger.warning(
                "DATA_QUALITY component=spark_quarantine batch_id=%s rejected=%s",
                batch_id,
                total_rejected,
            )

        # Free memory cache
        batch_df.unpersist()

    checkpoint_path = f"{args.data_lake_uri.rstrip('/')}/checkpoints/kafka-to-gcs"

    logger.info("📡 Setting up combined stream: Kafka ➔ GCS data lake")

    # Start one checkpointed query for the combined Kafka stream.
    query = (
        records_str.writeStream.foreachBatch(write_all_to_lake)
        .option("checkpointLocation", checkpoint_path)
        .trigger(
            processingTime="5 seconds"
        )  # Pack micro-batches to reduce BQ API calls
        .start()
    )

    logger.info(
        "🎉 Started streaming query. Writing Parquet files to the GCS raw zone..."
    )
    logger.info("Press Ctrl+C in your terminal to stop.")

    # Wait for termination of the query
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("👋 Spark Streaming job stopped by user.")
        query.stop()


if __name__ == "__main__":
    main()
