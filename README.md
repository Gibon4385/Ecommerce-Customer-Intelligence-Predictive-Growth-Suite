# E-Commerce Customer Intelligence & Predictive Growth Platform

**English** | [繁體中文](README.zh-TW.md)

An end-to-end data platform on Google Cloud that turns e-commerce transactions, behavioral events, and customer reviews into actionable intelligence for CRM, marketing, and product teams.

It goes beyond reporting what happened to answer:

- Which customers should the team contact today?
- Which high-value customers are at risk of churn?
- Which abandoned carts are worth recovering, and which products need attention?
- Where should the next marketing dollar be invested?

## What This Project Delivers

- An end-to-end path from **real-time events and a data lake to analytics, machine learning, and BI**.
- **Customer 360, RFM segmentation, LTV forecasts, churn risk, cohort analysis, product health, and daily CRM action lists**.
- Kafka and Spark Structured Streaming ingestion with replayable Parquet raw data in GCS.
- Tested, contract-driven dbt models organized into staging, intermediate, and business-facing marts.
- Automated orchestration with Airflow and deployable, testable, observable infrastructure through GitHub Actions, Terraform, and Cloud Monitoring.
- Real GCP end-to-end acceptance tests and repeatable Kafka-to-GCS load testing.

## How It Works

```text
Historical CSVs              Simulated Real-Time Events
      │                                  │
      │                           Python Producer
      │                                  ▼
      │                    Kafka on a Private GCE VM
      │                                  ▼
      │                     Spark Structured Streaming
      │                                  │
      └──────────────────────────────────▼
                         GCS Data Lake (Parquet)
                                  ▼
                       BigQuery External Tables
                                  ▼
                  dbt Staging → Intermediate → Marts
                                  │
                   ┌──────────────┼──────────────┐
                   ▼              ▼              ▼
             Customer 360    LTV / Churn Risk  Product Insights
                   └──────────────┼──────────────┘
                                  ▼
                       CRM Actions and BI Dashboards
```

Spark owns streaming ingestion, schema validation, lineage, quarantine handling, and GCS delivery. dbt owns repeatable, testable business logic. This boundary prevents business KPIs from being duplicated across the streaming and warehouse layers.

## Business Outputs

| Output | Business Question Answered |
|---|---|
| **Customer 360** | What is each customer's value, behavior, sentiment, segment, and predicted outlook? |
| **LTV and churn risk** | Which customers justify higher retention investment? |
| **Daily CRM action list** | Who needs retention, customer-care, retargeting, or lookalike action? |
| **Cohort analysis** | Which acquisition cohorts deliver stronger retention and revenue? |
| **Product and brand health** | Where are ratings, negative reviews, or customer pain points deteriorating? |
| **Product affinity** | Which product combinations create cross-sell and recommendation opportunities? |

## Dashboard Preview

![Executive Overview Dashboard](<Dashboard Preview/slack-image_executive_overview_SeOi3potUINQYtgz_bM.png>)

The dashboard brings segment size, revenue contribution, average customer value, and sentiment risk into one decision view so leaders can quickly identify the customer groups that need attention.

## Technology Stack

| Area | Technologies |
|---|---|
| Streaming and processing | Apache Kafka, Spark Structured Streaming, Python |
| Data lake and warehouse | Google Cloud Storage, Parquet, BigQuery |
| Data modeling | dbt, SQL |
| ML and NLP | BigQuery ML, XGBoost, Scikit-learn, SHAP, VADER |
| Orchestration and BI | Apache Airflow, Lightdash |
| Cloud and DevOps | GCP, Terraform, Docker, GitHub Actions, Cloud Monitoring |

## Engineering Practices

- **Data quality:** dbt contracts plus uniqueness, non-null, relationship, accepted-value, and source-freshness tests.
- **Privacy and access control:** raw email removal, SHA-256 audience-matching data, and dataset-level IAM separation across raw, intermediate, and marts.
- **Keyless CI/CD:** GitHub Actions authenticates through Workload Identity Federation; Slim CI builds only changed dbt models and their downstream dependencies.
- **Observability:** monitoring spans the producer, Kafka, Spark, GCS freshness, dbt execution results, and source freshness.
- **End-to-end acceptance:** Cloud E2E deploys and validates Producer → Kafka → Spark → GCS → BigQuery → dbt → Monitoring, followed by guarded resource cleanup.

## Quick Local Demo

Start Kafka and Kafka UI locally to observe simulated e-commerce events:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d kafka kafka-ui
python scripts/run_gap_filler.py --action gap-fill
python scripts/run_kafka_producer.py --delay 1.0
```

Kafka UI: <http://localhost:8085>

The complete pipeline requires GCP, Terraform, Spark, BigQuery, and dbt. See:

- [GCP cloud end-to-end acceptance guide](tests/cloud_e2e/README.md)
- [Kafka-to-GCS load tests](tests/load/README.md)
- [dbt models](ecommerce_dbt/models)
- [Terraform infrastructure](terraform)

## Scope and Trade-offs

This is a business-focused portfolio project spanning Data Engineering, Analytics Engineering, ML, and DataOps. To keep demonstration costs practical, Kafka and Dataproc use single-node deployments. They demonstrate the complete data flow and operational model, but are intentionally not presented as a production high-availability architecture.
