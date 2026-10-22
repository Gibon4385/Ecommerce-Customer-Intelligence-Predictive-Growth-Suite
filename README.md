# E-Commerce Business Decision System

[English](README.md) | [繁體中文](README.zh-TW.md)

A full-stack data project integrating **Data Engineering**, **Data Analytics**, and **AI/ML** to transform raw e-commerce data into actionable business decisions.

> 💡 **Core Value**: The system does not just output static reports; it actively provides a "Daily Action Plan" for CRM, marketing, and product teams — directly answering: **"Who should we contact today? What offers should we give them? Who is at risk of churn?"**

---

## 🚀 Key Highlights & Engineering Metrics

* **Business-Value Driven**: Integrates RFM modeling with BigQuery ML predicted Customer Lifetime Value (LTV) to automatically generate Customer 360 profiles (C360) and high-priority CRM action recommendations.
* **High-Throughput Real-time Ingestion**: Supports real-time data streaming at **500 ~ 2,000+ EPS** (Events Per Second), processing approximately **43M ~ 170M+ records daily**, demonstrating the pipeline's stability and horizontal scalability under high-concurrency scenarios.
* **Enterprise CI/CD & Slim CI**: Employs GitHub Actions and GCP Workload Identity Federation (keyless authentication) combined with Slim CI to run tests only on modified dbt models, **reducing BigQuery node compute costs by over 90%**.
* **Zero-Query-Cost Monitoring**: Apache Airflow schedules daily runs and parses local `run_results.json` to generate execution reports and instant Slack/Email alerts, **without executing any extra queries on BigQuery**.
* **Security & Privacy Compliance (PII Protection)**: Sensitive email addresses are completely stripped in the Staging layer and standard SHA-256 hashed for irreversible masking, with fine-grained IAM dataset-level (Raw/Staging/Marts) access control managed via Terraform.
* **Pragmatic Cloud Streaming Architecture**: Runs the continuous Python producer as a Cloud Run Worker Pool, Kafka on a private single-node Compute Engine VM, and Structured Streaming on a small single-node Managed Spark cluster—without Kubernetes.
* **GCS Data Lake + BigQuery Lakehouse Access**: Spark persists raw events as Parquet in GCS. BigQuery external tables expose the lake through the existing `raw_ecommerce` dataset, allowing dbt to transform it without duplicating the raw layer into warehouse storage.

---

## 🛠️ Tech Stack

| Layer | Technologies & Tools |
|---|---|
| **Data Lake / Warehouse** | Google Cloud Storage (Parquet), Google BigQuery external tables |
| **Data Transformation** | dbt (SQL), Python |
| **Streaming Pipeline** | Apache Kafka, Apache Spark (Structured Streaming) |
| **Machine Learning** | BigQuery ML (XGBoost/Regression), Scikit-learn, Pandas |
| **Natural Language Processing (NLP)** | VADER (Sentiment Analysis) |
| **Orchestration** | Apache Airflow |
| **Infrastructure as Code (IaC)** | Terraform |
| **Cloud Runtime** | Cloud Run Worker Pools, Compute Engine, Docker |
| **BI & Visualization** | LightDash (Looker-compatible) |

---

## 📐 System Architecture

```
[Raw CSV History]       [Cloud Run Worker Pool: Python Producer]
       │                                │ continuous events
       │                                ▼
       │                 Kafka on a private Compute Engine VM
       │                                │
       │                                ▼
       │              Managed Spark Cluster (Structured Streaming)
       │                                │ Parquet + GCS checkpoints
       └────────────────┬───────────────┘
                        ▼
              GCS Data Lake (raw zone)
                        │
                        ▼
       BigQuery external tables (raw_ecommerce)
                           │
                           ▼
                       dbt Staging
                           │
                           ▼
                     dbt Intermediate (Feature Layer)
                           │
             ┌─────────────┴─────────────┐
             ▼                           │
    BigQuery ML / Python (ML Prediction) │
             │                           │
             ▼                           ▼
     prediction tables ──────────►   dbt Marts (Business Layer)
                                         │ (Integrate LTV for Priority Score)
                                         ▼
                             C360 / CRM Recommendation List
                                         │
                                         ▼
                             LightDash Dashboard + CRM Action
```

---

## 💾 Data & Modeling Design

### 1. Spark and dbt responsibility boundary

Spark handles processing that is specific to the live Kafka ingestion boundary; dbt owns repeatable warehouse transformations and business meaning.

| Spark Structured Streaming | dbt on BigQuery |
|---|---|
| Read Kafka topics and manage offsets through a GCS checkpoint | Deduplicate each entity by its business primary key |
| Parse JSON with an explicit per-topic schema | Cast and standardize dates, numbers, names, and null handling |
| Preserve Kafka topic, partition, offset, and timestamp lineage | Enforce tests such as uniqueness, non-null, relationships, and accepted values |
| Reject malformed records or missing primary IDs into `gs://.../quarantine/<entity>/` | Remove or hash PII and apply reusable data-quality rules |
| Write valid append-only Parquet records to the GCS raw zone | Build staging, intermediate features, and business-facing marts |

Business rules such as RFM, churn, cohort, LTV, product health, and marketing recommendations intentionally remain in dbt. They are easier to review, test, backfill, and rerun there. Spark does not join dimensions or calculate business KPIs; this prevents two competing transformation layers.

The raw external tables expose ingestion metadata (`_source_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingested_at`) for traceability. Historical files can legitimately contain null metadata because they did not originate from Kafka.

### 2. dbt Layer Design
The project follows a strict three-layer dbt architecture, each with defined responsibilities:
* **Staging (`stg_`)** — Lightweight cleaning and standardization of raw tables (e.g., [stg_users.sql](ecommerce_dbt/models/staging/stg_users.sql)). No business logic here; just consistent naming and types, along with PII masking.
* **Intermediate (`int_`)** — Where the core business logic resides. This layer computes RFM scores (`int_rfm_scores`), cohort retention features (`int_cohort_base`), and LTV training features (`int_ltv_training_features`), separating logic from presentation for maximum reusability.
* **Marts (`mart_`)** — Business-facing tables built for direct business use: `mart_c360_table` (customer 360 profile) and `mart_business_recommendations`. These are what the CRM and marketing teams actually query.

### 3. Machine Learning & AI Applications
* **LTV Forecast Model (BigQuery ML)**: Using customer RFM features and historical purchase behavior, a regression model predicts each customer's expected revenue over the next 90 days. This feeds directly into budget allocation logic — high-LTV customers receive high-touch retention treatment; low-LTV customers are routed to low-cost automated communication flows.
* **NLP Sentiment Analysis (Python / VADER)**: Processes customer reviews to extract sentiment labels and product pain points. The system flags high-value customers with negative sentiment as priority cases for customer service, catching churn signals before they materialize.

---

## 🔒 Enterprise Engineering Practices

### 1. PII Security & Dataset Access Control (Data Privacy & IAM)
* **PII De-identification**: [stg_users.sql](ecommerce_dbt/models/staging/stg_users.sql) completely strips raw email fields. The dedicated [stg_users_hashed.sql](ecommerce_dbt/models/staging/stg_users_hashed.sql) standardizes emails and stores them as **SHA-256** hashes (`to_hex(sha256(...))`) for secure third-party ad network (Lookalike) matching.
* **Fine-Grained Dataset IAM (GCP BigQuery)**:
  - **`raw_ecommerce` (Raw Layer)**: Restricted to data engineers and dbt as read-only (`dataViewer`) to prevent accidental raw data modifications.
  - **`dbt_ecommerce_staging` / `intermediate` (Semi-processed Layer)**: Strictly limited to the dbt execution service account (`dataEditor`) and **completely locked** for general analysts and marketing, preventing query errors and securing PII fields from bypass reading.
  - **`dbt_ecommerce_marts` (Marts Layer)**: Granted read-only (`dataViewer`) to data analysts and marketing teams, serving as the Single Source of Truth.

### 2. Modern CI/CD & Slim CI
* **Workload Identity Federation (WIF)**: Connects GitHub Actions with GCP using dynamic, short-lived OIDC tokens for **keyless authentication**, eliminating the risk of long-lived Service Account JSON key leaks. Infrastructure is defined in [cicd.tf](terraform/cicd.tf).
* **dbt Slim CI (State-Aware Testing)**: Upon PR submission, the CI pipeline ([dbt_ci.yml](.github/workflows/dbt_ci.yml)) downloads the production `manifest.json` and runs `dbt build --select state:modified+` **only for modified models and their downstream dependencies** inside a temporary dataset (`ci_pr_<pr_num>`), minimizing BigQuery compute costs.
* **Static Code Analysis (Linting)**: Integrates `ruff` (Python) and `sqlfluff` (SQL) for static style and syntax checking.

### 3. Production-Grade Airflow Scheduling & Zero-Cost Monitoring (Airflow Orchestration)
* **Scheduling Strategy**: Runs daily at 01:00 AM (Taipei Time) as defined in [ecommerce_dbt_dag.py](airflow/dags/ecommerce_dbt_dag.py). Configured with 3 automatic retries and custom task failure alerts (`on_failure_callback`) that email formatted HTML diagnostics to the operations team.
* **Zero-Query-Cost Monitoring**: After the dbt build completes, Airflow parses the local `run_results.json` metadata artifact directly to compile execution metrics and send Email/Slack reports, **incurring zero BigQuery scan fees for daily monitoring**.

### 4. Cloud Streaming Deployment (No Kubernetes)
* **Continuous producer**: The producer is packaged by [producer.Dockerfile](producer.Dockerfile) and runs as a Cloud Run Worker Pool. A Worker Pool is used instead of Cloud Scheduler because this producer is an always-on process rather than a finite scheduled task.
  The image can bootstrap from a built-in demo product catalog; ignored local CSV history is not baked into the image.
* **Kafka on Compute Engine**: Terraform provisions one private Kafka VM and a separate persistent disk. Port 9092 is allowed only from the streaming subnet. This intentionally favors portfolio-scale cost and simplicity over broker high availability.
* **Why not Google Managed Service for Apache Kafka?** The managed service is the better production choice for HA and reduced operations, but it provisions regional, multi-zone capacity and is disproportionately expensive for this low-volume synthetic workload. A single VM preserves Kafka protocol and operational learning while keeping the trade-off explicit: it is a single point of failure and not a production HA design.
* **Managed Spark Structured Streaming**: Terraform provisions a persistent single-node Managed Spark cluster and [submit.sh](deploy/managed-spark/submit.sh) submits a restartable, long-running consumer. GCS checkpoints allow recovery from Kafka offsets after failure.
* **Demonstration, not production HA**: The one-node cluster is intentionally small and exists to demonstrate Spark Structured Streaming. It has no master or worker redundancy and must not be represented as a production high-availability design.
* **Production alternative — Dataflow**: If demonstrating Spark is not a requirement, a continuously managed Dataflow streaming pipeline can replace the Spark cluster for Kafka-to-GCS/BigQuery processing and reduce cluster lifecycle operations.
* **Data lake access**: Spark writes Parquet to `gs://<project>-ecommerce-lake/raw/<entity>/`; Terraform creates BigQuery external tables with the existing `raw_ecommerce.raw_*` names used by dbt.

---

## 📊 Key Deliverables & Business Applications

| Deliverable | Business Problem Solved | Core Use Case |
|---|---|---|
| **C360 Table** | Who are my VVIPs vs. dormant customers? | Provides a complete customer profile and RFM segments for targeted campaigns. |
| **Cohort Analysis** | Are last quarter's new customers actually high-quality? | Evaluates retention curves over time to optimize user acquisition spend. |
| **LTV Forecast** | Which customers are worth investing in over the next 90 days? | Allocates marketing budget to high-LTV customers and sets up automation for low-LTV ones. |
| **Business Recommendation List** | Who should CRM contact *today*, and with what offer? | Filters the daily Top 100 attention-needed accounts based on priority score (`LTV × Churn Risk`). |
| **CRM Agent Report** | How to quickly summarize daily operations for executives? | Auto-generated daily Traditional Chinese summary report for business leaders. |

---

## 📂 Project Structure

```
/data               # Raw CSV datasets
/ecommerce_dbt      # All dbt models (staging / intermediate / marts)
/scripts            # Python ingestion and preprocessing scripts
/ipynb              # LTV modeling and sentiment analysis Jupyter Notebooks
/agents             # CRM AI agent logic
/deploy             # Kafka VM compose, Cloud Run, and Managed Spark deployment tools
/memo               # Architecture design docs, planning notes, and setup guides
/dashboard_preview  # Dashboard layout drafts (Executive Overview, Segmentation)
```

---

## ⚡ Local Quick Start

Use Docker Compose for the local Kafka development environment.

1. **Start the Infrastructure** (Zookeeper, Kafka, Kafka UI):
   ```bash
   # Start container services
   docker compose up -d
   ```
   *Visit [http://localhost:8085](http://localhost:8085) to monitor Kafka topics and messages in real-time.*
2. **Data Initialization & Backfill** (Automatically backfills history gaps up to yesterday):
   ```bash
   python scripts/run_gap_filler.py --action gap-fill
   ```
3. **Run the Streaming Pipeline** (Open two terminal windows/tabs):
   * **Terminal 1 (Mock Data Producer)**:
     By default, it runs at the `INFO` log level, which prints status summaries every 5 seconds (optimized for high speed):
     ```bash
     python scripts/run_kafka_producer.py --delay 1.0
     ```
     If you want to view every single generated event (user signup, event, order, review) in real-time, run in `DEBUG` mode:
     ```bash
     python scripts/run_kafka_producer.py --log-level DEBUG --delay 1.0
     ```
   * **Terminal 2 (PySpark Streaming Consumer, writes Parquet to GCS)**:
     ```bash
     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
       scripts/spark_bigquery_consumer.py \
       --data-lake-uri="gs://YOUR_PROJECT-ecommerce-lake"
     ```

## ☁️ Cloud Deployment Outline

The `terraform/` directory is one root module. Terraform loads every `.tf` file together; filenames only organize the configuration by domain. `providers.tf` and `versions.tf` define tooling, while `networking.tf`, `kafka.tf`, `dataproc.tf`, `storage.tf`, `bigquery.tf`, `iam.tf`, `monitoring.tf`, and `cicd.tf` group the resources. Outputs are centralized in `outputs.tf`; there is intentionally no special `main.tf` entrypoint.

1. Run `terraform apply` in `terraform/` to create the private network, Kafka VM and disk, GCS lake, service account, BigQuery external tables, and existing dbt infrastructure.
2. Set `PROJECT_ID` and `KAFKA_BOOTSTRAP_SERVERS`, then run `deploy/cloud-run/deploy-producer.sh`. It builds `producer.Dockerfile` and deploys a one-instance Worker Pool with Direct VPC egress.
3. Export `PROJECT_ID`, `KAFKA_BOOTSTRAP_SERVERS`, and optionally `REGION`/`CLUSTER_NAME`, then run `deploy/managed-spark/submit.sh`. The restartable Structured Streaming job remains active on the persistent cluster and reuses its GCS checkpoint after failure.
4. Upload historical CSV data to the same lake layout with `scripts/ingest_raw_data.py`; dbt continues reading `raw_ecommerce.raw_*` through BigQuery external tables.

The Kafka endpoint is private. Do not add a public 9092 firewall rule; Cloud Run, Kafka, and the Managed Spark cluster use the same VPC subnet.

### Security, IAM, and monitoring

Terraform provisions four workload identities with no shared service-account key files:

| Identity | Purpose | Main access |
|---|---|---|
| `producer-worker` | Continuous Cloud Run producer | Write logs and metrics |
| `kafka-broker` | Kafka VM and Ops Agent | Write logs and metrics |
| `spark-streaming` | Dataproc Structured Streaming | Dataproc worker plus object access to the lake |
| `ecommerce-dataset` | dbt through Workload Identity Federation | BigQuery jobs/data, dbt artifacts, and Monitoring metric writes |

Human access is assigned to `data-platform-developers@example.com` and `data-platform-operators@example.com` through additive IAM members. These are configuration examples: a Google Workspace or Cloud Identity administrator must first create both Groups, replace the Terraform variable values with the real domain, and allow mail from `alerting-noreply@google.com`. Terraform creates the email notification channels, but Google may require channel verification.

The Kafka startup script installs the Google Cloud Ops Agent and enables Kafka JMX plus host metrics. The custom **Ecommerce streaming pipeline health** dashboard covers the full path: producer and Spark-to-GCS heartbeats, Kafka CPU/disk, Cloud Run instances, Dataproc job state, Spark input/processed/rejected records and batch duration, plus dbt run success, failed models/tests, duration, last success, and source freshness. Five alert policies notify both Groups:

1. Producer heartbeat missing for 10 minutes.
2. Kafka VM uptime missing for 5 minutes.
3. Kafka filesystem usage above 85% for 5 minutes.
4. Dataproc streaming job entering `ERROR`.
5. No successful Spark-to-GCS micro-batch for 15 minutes (the GCS freshness signal).

The heartbeat alerts become meaningful only after the producer and Spark job have emitted their first heartbeat. This deployment intentionally uses one Kafka VM and a single-node Dataproc cluster for portfolio demonstration; neither is production HA. A production design should use a managed/replicated Kafka service and can replace Spark with the Google-provided Dataflow Kafka-to-BigQuery/GCS pattern where appropriate.

Spark dashboard metrics are extracted from structured heartbeat logs. After a production dbt build, [publish_dbt_metrics.py](scripts/publish_dbt_metrics.py) reads `run_results.json` and the source-freshness `sources.json`, then writes `custom.googleapis.com/dbt/*` metrics through Application Default Credentials. Both GitHub CD and the Airflow DAG invoke the same publisher; the dbt service account has only `monitoring.metricWriter` for this purpose. Dashboard tiles remain empty until each workload has emitted its first sample.

### Capacity testing

Repeatable Kafka-to-GCS load scenarios live in [tests/load](tests/load/README.md). The runner calls the production producer with `--target-eps`, `--duration-seconds`, `--run-id`, and `--metrics-output`, so test traffic uses the same generation and delivery path as the deployed workload. The first acceptance target is 50 EPS for 30 minutes; peak validation is 100 EPS for 15 minutes. Run cloud tests from a temporary VM on the private streaming subnet, then validate the producer summary together with Spark batch and GCS freshness observations from the monitoring dashboard. Stress tests are manual and must not run in pull-request CI.

### Cloud end-to-end acceptance

[tests/cloud_e2e/README.md](tests/cloud_e2e/README.md) documents the automated GCP deployment, private-network/IAM checks, short Producer→Kafka→Spark→GCS smoke run, BigQuery/dbt three-layer materialization, Monitoring time-series verification, reports, and guarded cleanup. Its operating guide is intentionally written in Traditional Chinese. A full run requires the exact project ID as destructive confirmation and cleans up by default so test infrastructure does not continue generating cost.

---

*Built as a portfolio project demonstrating end-to-end data engineering, Platform/DataOps, and applied machine learning in a commercial context.*
