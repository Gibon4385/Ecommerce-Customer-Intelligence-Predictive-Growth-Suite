# Kafka-to-GCS Load Tests

This directory contains repeatable capacity scenarios for the production producer code path. The runner invokes `scripts/run_kafka_producer.py`; it does not maintain a second synthetic-data implementation.

## What this proves

The producer summary proves the requested Kafka message rate and delivery-error count. End-to-end capacity also requires the Cloud Monitoring dashboard during the same run window:

- Spark input and processed rows remain balanced.
- Spark micro-batch duration remains below the trigger interval or returns to normal after a peak.
- No Spark batch fails.
- Spark-to-GCS heartbeat remains fresh.
- Kafka VM CPU and disk remain within operating limits.

Spark Structured Streaming stores source offsets in its checkpoint and does not provide a stable, conventional Kafka consumer-group lag for this job. Use Spark progress metrics and GCS freshness as the primary backlog signals rather than claiming that a Kafka group-lag metric alone proves capacity.

## Run locally

Start local Kafka and the Spark consumer first, then install the project dependencies and run:

```bash
python tests/load/run_load_test.py \
  --scenario smoke \
  --bootstrap-servers localhost:9092
```

Use `baseline`, `expected`, `peak`, `stress`, and `recovery` for the longer scenarios in `scenarios.yaml`. JSON summaries are written to the ignored `tests/load/results/` directory.

## Run against GCP

Run the tool from a temporary test VM attached to the `ecommerce-streaming-REGION` subnet because Kafka has no public endpoint:

```bash
python tests/load/run_load_test.py \
  --scenario expected \
  --bootstrap-servers 10.20.0.X:9092
```

Do not run stress tests automatically from pull-request CI. They create cloud cost and must use a controlled maintenance window. Keep the continuous Cloud Run producer stopped or use a distinct test window so its traffic does not contaminate the result.

## Validate end to end

After the run, record the dashboard values for the exact run window using `observations.example.json` as the schema, then execute:

```bash
python tests/load/validate_results.py \
  --scenario expected \
  --summary tests/load/results/RUN_ID.json \
  --observations tests/load/results/RUN_ID-observations.json
```

The first portfolio acceptance target is 50 EPS for 30 minutes, zero producer delivery errors, zero failed Spark batches, and GCS freshness below two minutes. The peak target is 100 EPS for 15 minutes. After stress traffic stops, the recovery scenario must return the pipeline to normal within ten minutes.
