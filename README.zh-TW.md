# 電商商業決策系統 (E-Commerce Business Decision System)

[English](README.md) | [繁體中文](README.zh-TW.md)

這是一個整合了**數據工程 (Data Engineering)**、**數據分析 (Data Analytics)** 與 **AI/ML** 的全棧數據專案，旨在將原始電商數據轉化為可執行的商業決策建議。

> 💡 **核心價值**：本系統不只產出靜態報表，而是主動為 CRM、行銷與產品團隊提供「每日行動方案」——直接回答：**「今天該聯繫哪些客戶？該提供什麼優惠？誰有流失風險？」**

---

## 🚀 專案亮點與工程指標 (Key Highlights)

* **商業價值驅動**：整合 RFM 模型與 BigQuery ML 預測的生命週期價值 (LTV)，自動產出客戶全視角畫像 (C360) 與高優先級 CRM 行動建議。
* **高吞吐量即時數據處理 (High-Throughput Real-time Ingestion)**：支援 **500 ~ 2,000+ EPS** (每秒事件數) 的即時數據吞吐量，每日處理資料量達 **4,300 萬 ~ 1.7 億筆**，驗證系統於高併發大規模數據流下的穩定性與水平擴充能力。
* **企業級 CI/CD 與 Slim CI**：利用 GitHub Actions 與 GCP Workload Identity Federation (無金鑰驗證)，搭配 Slim CI 僅測試修改的 dbt 模型，**BigQuery 節點運算成本降低 90% 以上**。
* **零查詢成本監控 (Zero-Query-Cost)**：Airflow 每日自動調度，並透過解析本地 `run_results.json` 產出執行日報與即時 Slack/Email 警報，**不對 BigQuery 發起任何額外查詢**。
* **隱私安全合規 (PII Protection)**：Staging 層移除敏感 Email，採 SHA-256 進行不可逆雜湊，並透過 Terraform 實施 BigQuery 資料集層級 (Raw/Staging/Marts) 的精細化 IAM 權限控管。
* **務實的雲端串流架構**：持續運行的 Python producer 使用 Cloud Run Worker Pool；Kafka 部署於私有 Compute Engine 單節點 VM；Structured Streaming 使用小型單節點 Managed Spark Cluster，不使用 Kubernetes。
* **GCS Data Lake + BigQuery Lakehouse 查詢**：Spark 將原始事件以 Parquet 寫入 GCS，BigQuery external tables 以既有的 `raw_ecommerce` dataset 對外提供查詢，dbt 可直接轉換 lake 中的資料。

---

## 🛠️ 技術棧 (Tech Stack)

| 層級 | 技術與工具 |
|---|---|
| **Data Lake / 數據倉庫** | Google Cloud Storage (Parquet)、Google BigQuery external tables |
| **數據轉換 (Data Transformation)** | dbt (SQL), Python |
| **數據管道 / 串流 (Streaming Pipeline)** | Apache Kafka, Apache Spark (Structured Streaming) |
| **機器學習 (Machine Learning)** | BigQuery ML (XGBoost/迴歸模型), Scikit-learn, Pandas |
| **自然語言處理 (NLP)** | VADER (情緒分析) |
| **自動化調度 (Orchestration)** | Apache Airflow |
| **基礎設施即程式碼 (IaC)** | Terraform |
| **雲端執行環境** | Cloud Run Worker Pools、Compute Engine、Docker |
| **BI 與視覺化 (BI & Visualization)** | LightDash (相容 Looker) |

---

## 📐 系統架構圖 (System Architecture)

```
[歷史原始 CSV]       [Cloud Run Worker Pool：Python Producer]
       │                                │ 持續產生事件
       │                                ▼
       │                 私有 Compute Engine VM 上的 Kafka
       │                                │
       │                                ▼
       │              Managed Spark Cluster Structured Streaming
       │                                │ Parquet + GCS checkpoints
       └────────────────┬───────────────┘
                        ▼
                 GCS Data Lake（raw zone）
                        │
                        ▼
            BigQuery external tables（raw_ecommerce）
                           │
                           ▼
                       dbt Staging
                           │
                           ▼
                     dbt Intermediate (特徵層)
                           │
             ┌─────────────┴─────────────┐
             ▼                           │
    BigQuery ML / Python (機器學習預測)    │
             │                           │
             ▼                           ▼
     prediction tables ──────────►   dbt Marts (業務層)
                                         │ (整合 LTV 計算優先級)
                                         ▼
                             C360 / 商業建議名單 (CRM 行動方案)
                                         │
                                         ▼
                             LightDash 儀表板 + CRM Action
```

---

## 💾 數據流與建模設計 (Data & Modeling Design)

### 1. Spark 與 dbt 的責任邊界

Spark 負責只有在 Kafka 即時攝取入口才能取得、且必須立刻處理的技術工作；dbt 負責可重跑的 warehouse transformation 與商業語意。

| Spark Structured Streaming | BigQuery 上的 dbt |
|---|---|
| 讀取 Kafka topics，透過 GCS checkpoint 管理 offsets | 依每個 entity 的 business primary key 去重 |
| 使用各 topic 的明確 schema 解析 JSON | 統一日期、數字、欄位與 null handling |
| 保留 Kafka topic、partition、offset、timestamp lineage | 執行 unique、not-null、relationships、accepted-values 等測試 |
| 將 malformed record 或缺少 primary ID 的資料寫入 `gs://.../quarantine/<entity>/` | 移除／雜湊 PII，套用可重複使用的資料品質規則 |
| 將有效資料以 append-only Parquet 寫入 GCS raw zone | 建立 staging、intermediate features 與 business marts |

RFM、churn、cohort、LTV、product health 與 marketing recommendation 等商業規則刻意保留在 dbt，因為 SQL models 較容易 review、test、backfill 與 rerun。Spark 不做 dimension joins 或 business KPI，避免產生兩套互相競爭的 transformation logic。

Raw external tables 會提供 `_source_topic`、`_kafka_partition`、`_kafka_offset`、`_kafka_timestamp`、`_ingested_at` 等 ingestion metadata。歷史檔案並非來自 Kafka，因此這些欄位可以是 null。

### 2. dbt 分層架構 (dbt Layer Design)
本專案遵循嚴格的 dbt 三層架構，每一層都有明確的職責：
* **Staging (`stg_`)** — 對原始資料進行輕量清洗與標準化（例如 [stg_users.sql](ecommerce_dbt/models/staging/stg_users.sql)）。此層不包含業務邏輯，僅負責統一命名規範與資料型別，並實施敏感資訊遮蔽。
* **Intermediate (`int_`)** — 業務邏輯的核心所在。此層負責計算 RFM 分數 (`int_rfm_scores`)、留存同期群特徵 (`int_cohort_base`) 以及 LTV 訓練特徵 (`int_ltv_training_features`)，將邏輯與最終展現分離以確保模型複用性。
* **Marts (`mart_`)** — 直接面向商業應用的資料表：例如 `mart_c360_table` (客戶全視角畫像) 與 `mart_business_recommendations` (商業建議)。這是 CRM 與行銷團隊真正查詢的對象。

### 3. 機器學習與 AI 應用
* **LTV 價值預測 (BigQuery ML)**：利用客戶的 RFM 特徵與歷史購買行為，透過回歸模型預測每位客戶未來 90 天的預期營收。預測結果直接驅動預算分配邏輯：高 LTV 客戶將獲得高成本的留存服務，低 LTV 客戶則導入低成本的自動化溝通流程。
* **NLP 情緒分析 (Python / VADER)**：處理客戶評論，萃取情緒標籤與產品痛點。系統會將帶有負面情緒的高價值客戶標記為客服優先處理個案，在客戶真正流失前捕捉危險訊號。

---

## 🔒 企業級工程實踐 (Enterprise Engineering Practices)

### 1. PII 安全防護與資料集權限控制 (Data Privacy & IAM)
* **PII 去識別化**：[stg_users.sql](ecommerce_dbt/models/staging/stg_users.sql) 直接剔除明文 Email，僅在獨立的 [stg_users_hashed.sql](ecommerce_dbt/models/staging/stg_users_hashed.sql) 中，將 Email 經由標準化後進行 **SHA-256** 雜湊（`to_hex(sha256(...))`）保存，供第三方廣告投放（Lookalike）安全匹配。
* **精細化 IAM 權限控管 (GCP BigQuery)**：
  - **`raw_ecommerce` (Raw 層)**：僅限數據工程師與 dbt 唯讀 (`dataViewer`)，防止原始數據被意外修改。
  - **`dbt_ecommerce_staging` / `intermediate` (半成品層)**：僅限 dbt 執行服務帳戶 (`dataEditor`) 讀寫，對一般分析師與行銷團隊**完全鎖定**，避免串接錯誤數據或旁路讀取敏感欄位。
  - **`dbt_ecommerce_marts` (業務產出層)**：分析師與行銷團隊僅授予 `dataViewer` (唯讀)，確保決策報表的高效與一致性（Single Source of Truth）。

### 2. 現代化 CI/CD 與 Slim CI
* **Workload Identity Federation (WIF)**：整合 GitHub Actions 與 GCP，採用動態 OIDC 短期 Token 進行**無金鑰驗證 (Keyless Authentication)**，杜絕長期 Service Account JSON 金鑰外洩風險。相關基礎設施定義於 [cicd.tf](terraform/cicd.tf)。
* **dbt Slim CI (狀態比對測試)**：當 PR 提交時，CI 管道 ([dbt_ci.yml](.github/workflows/dbt_ci.yml)) 自動下載 Production 的 `manifest.json`，**僅針對有變動的 Model 及其下游**在臨時資料集（`ci_pr_<pr_num>`）中執行 `dbt build --select state:modified+`，大幅降低 BigQuery 運算成本並縮短 CI 流程。
* **靜態代碼檢查 (Linting)**：整合 `ruff` (Python) 與 `sqlfluff` (SQL) 進行代碼語法與風格排版靜態檢查。

### 3. 生產級 Airflow 自動化調度與無成本監控 (Airflow Orchestration)
* **調度策略**：每日凌晨 01:00 (台北時間) 自動執行，DAG 設計見 [ecommerce_dbt_dag.py](airflow/dags/ecommerce_dbt_dag.py)。設有 3 次自動重試與任務失敗即時警報機制（`on_failure_callback`），出錯時自動向運維團隊寄送精美 HTML 報錯信件。
* **零查詢成本監控日報 (Zero-Query-Cost)**：在 dbt 流程執行完畢後，Airflow 會直接解析本地生成的 `run_results.json` 狀態檔案進行報表編譯並發送 Email/Slack，**完全無需對 BigQuery 進行二次查詢，從而達到零成本的日常監控**。

### 4. 雲端串流部署（不使用 Kubernetes）
* **持續運行的 Producer**：[producer.Dockerfile](producer.Dockerfile) 將 producer 容器化並部署為 Cloud Run Worker Pool。它是常駐程序而不是一次性排程任務，因此不使用 Cloud Scheduler 反覆啟動。
  Image 可由內建的 demo 商品目錄自行啟動，不會將被忽略的本機歷史 CSV 包進 image。
* **Compute Engine Kafka**：Terraform 建立一台私有 Kafka VM 與獨立 persistent disk，9092 僅允許 streaming subnet 存取。此設計刻意以作品集規模的成本與簡潔性優先，不提供 broker HA。
* **為何不使用 Google Managed Service for Apache Kafka？** Managed Kafka 適合需要高可用與降低維運負擔的正式環境，但其區域、多 zone 容量對本專案的小量虛擬資料成本過高。單 VM 仍可完整展示 Kafka protocol 與維運概念，但明確接受 single point of failure，因此不宣稱為 production HA 架構。
* **Managed Spark Structured Streaming**：Terraform 建立常駐單節點 Managed Spark Cluster，[submit.sh](deploy/managed-spark/submit.sh) 提交可重新啟動的長時間 consumer。GCS checkpoint 可在失敗後接續 Kafka offset。
* **僅供技術展示，並非 production HA**：單節點 cluster 刻意採小型配置，用於展示 Spark Structured Streaming；它沒有 master 或 worker redundancy，不得描述為 production high-availability 架構。
* **正式環境替代方案 — Dataflow**：如果沒有展示 Spark 的要求，可改用持續代管的 Dataflow streaming pipeline 處理 Kafka 到 GCS／BigQuery，降低 cluster lifecycle 維運工作。
* **Data Lake 查詢**：Spark 寫入 `gs://<project>-ecommerce-lake/raw/<entity>/`，Terraform 建立與 dbt 現有 source 相同名稱的 `raw_ecommerce.raw_*` BigQuery external tables。

---

## 📊 主要產出與商業應用 (Key Deliverables)

| 產出項目 | 解決的商業問題 | 核心應用場景 |
|---|---|---|
| **C360 Table** | 誰是我們的 VVIP 客戶？誰又是正在流失的沉睡客？ | 提供 CRM 團隊全視角客戶畫像與 RFM 分群，進行分眾行銷。 |
| **同期群分析 (Cohort Analysis)** | 上一季獲取的新客，長期來看真的是高品質客戶嗎？ | 評估不同時間段獲客的留存曲線，優化獲客預算。 |
| **LTV 價值預測** | 哪些客戶值得在未來 90 天內重點投資？ | 將行銷預算優先分配給高 LTV 客戶；對低 LTV 客戶實施自動化留存。 |
| **商業建議清單** | CRM 團隊「今天」該聯繫誰？該給他們什麼優惠？ | 依優先級分數 (`LTV × Churn Risk`) 篩選出每日最需關注的前 100 名帳戶。 |
| **CRM Agent 報告** | 如何快速向團隊主管彙整每日業務狀況？ | 為團隊主管自動產出的每日繁體中文數據摘要報告。 |

---

## 📂 專案目錄結構 (Project Structure)

```
/data               # 原始 CSV 數據集
/ecommerce_dbt      # 所有的 dbt 模型 (staging / intermediate / marts)
/scripts            # Python 數據導入與預處理腳本
/ipynb              # LTV 建模與情緒分析的 Jupyter Notebooks
/agents             # CRM AI 代理人邏輯
/deploy             # Kafka VM Compose、Cloud Run 與 Managed Spark 部署工具
/memo               # 架構設計文件、規劃筆記與詳細部署指引
/dashboard_preview  # 儀表板版面草案 (高層概覽、分群分析)
```

---

## ⚡ 本地快速啟動指引 (Local Quick Start)

本機開發環境使用 Docker Compose 啟動 Kafka。

1. **啟動基礎設施** (Zookeeper, Kafka, Kafka UI):
   ```bash
   # 啟動容器服務
   docker compose up -d
   ```
   *可開啟瀏覽器訪問 [http://localhost:8085](http://localhost:8085) 監控 Kafka 即時訊息與 Topic。*
2. **數據初始化與補齊** (自動補足歷史斷層至昨天):
   ```bash
   python scripts/run_gap_filler.py --action gap-fill
   ```
3. **執行即時串流管道** (請開啟兩個終端機視窗執行):
    * **終端機 1 (模擬數據生產者)**:
      預設會以 `INFO` 日誌級別啟動，每 5 秒輸出一次發送統計數據（優化發送效能）：
      ```bash
      python scripts/run_kafka_producer.py --delay 1.0
      ```
      如果您希望即時查看每一筆產生的資料細節（如使用者註冊、瀏覽、下單、評論），可以使用 `DEBUG` 模式啟動：
      ```bash
      python scripts/run_kafka_producer.py --log-level DEBUG --delay 1.0
      ```
    * **終端機 2（PySpark 串流消費者，將 Parquet 寫入 GCS）**:
      ```bash
      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        scripts/spark_bigquery_consumer.py \
        --data-lake-uri="gs://YOUR_PROJECT-ecommerce-lake"
      ```

## ☁️ 雲端部署概要

`terraform/` 是一個 root module。Terraform 會一起載入目錄內所有 `.tf`，檔名只負責依領域整理設定：`providers.tf`、`versions.tf` 定義工具；`networking.tf`、`kafka.tf`、`dataproc.tf`、`storage.tf`、`bigquery.tf`、`iam.tf`、`monitoring.tf`、`cicd.tf` 分類資源；所有輸出集中於 `outputs.tf`。本專案刻意不保留具有「入口」假象的 `main.tf`。

1. 在 `terraform/` 執行 `terraform apply`，建立私有網路、Kafka VM 與磁碟、GCS lake、service account、BigQuery external tables，以及原有 dbt 基礎設施。
2. 設定 `PROJECT_ID` 與 Terraform output 提供的 `KAFKA_BOOTSTRAP_SERVERS`，再執行 `deploy/cloud-run/deploy-producer.sh`；它會建置 image 並部署一個使用 Direct VPC egress 的 Worker Pool instance。
3. 設定 `PROJECT_ID`、`KAFKA_BOOTSTRAP_SERVERS`，以及選用的 `REGION`／`CLUSTER_NAME` 後，執行 `deploy/managed-spark/submit.sh`。Structured Streaming job 會持續運行於常駐 cluster，失敗重啟後沿用 GCS checkpoint。
4. 透過 `scripts/ingest_raw_data.py` 將歷史 CSV 上傳至相同的 lake 目錄；dbt 繼續從 BigQuery `raw_ecommerce.raw_*` external tables 讀取。

Kafka endpoint 僅供私有網路使用。請勿建立公開的 9092 firewall rule；Cloud Run、Kafka 與 Managed Spark Cluster 使用相同 VPC subnet。

### 安全、IAM 與監控

Terraform 建立四個彼此獨立的 workload identity，全程不使用共用 service-account key 檔案：

| 身份 | 用途 | 主要權限 |
|---|---|---|
| `producer-worker` | 持續運行的 Cloud Run producer | 寫入 logs 與 metrics |
| `kafka-broker` | Kafka VM 與 Ops Agent | 寫入 logs 與 metrics |
| `spark-streaming` | Dataproc Structured Streaming | Dataproc worker 與 Data Lake object 存取 |
| `ecommerce-dataset` | dbt（透過 Workload Identity Federation） | BigQuery jobs/data、dbt artifacts 與 Monitoring metric writes |

人員權限透過 additive IAM members 指派給 `data-platform-developers@example.com` 與 `data-platform-operators@example.com`。這兩個地址是設定範例：Google Workspace／Cloud Identity 管理員必須先建立 Groups、將 Terraform variables 換成實際網域，並允許接收 `alerting-noreply@google.com` 的郵件。Terraform 會建立 Email notification channels，但 Google 仍可能要求驗證通知信箱。

Kafka startup script 會安裝 Google Cloud Ops Agent，收集 Kafka JMX 與主機 metrics。自訂的 **Ecommerce streaming pipeline health** dashboard 涵蓋完整路徑：Producer 與 Spark-to-GCS heartbeat、Kafka CPU／磁碟、Cloud Run instances、Dataproc job state、Spark input／processed／rejected records、batch duration，以及 dbt run success、failed models/tests、duration、last success 和 source freshness。以下五項告警會同時寄給兩個 Groups：

1. Producer heartbeat 中斷 10 分鐘。
2. Kafka VM uptime metric 中斷 5 分鐘。
3. Kafka 檔案系統使用率超過 85% 且持續 5 分鐘。
4. Dataproc streaming job 進入 `ERROR`。
5. Spark-to-GCS 成功 micro-batch 中斷 15 分鐘（作為 GCS freshness 訊號）。

Heartbeat 告警必須等 Producer 與 Spark job 至少送出第一筆 heartbeat 後才具有判斷意義。本部署刻意採用單台 Kafka VM 與單節點 Dataproc Cluster 作為作品集展示，兩者都不是 production HA。正式環境應採 managed／replicated Kafka；若不需要特別展示 Spark，也可依需求改用 Google 提供的 Dataflow Kafka template／pipeline。

Spark dashboard metrics 從固定格式 heartbeat logs 擷取。Production dbt build 完成後，[publish_dbt_metrics.py](scripts/publish_dbt_metrics.py) 解析 `run_results.json` 與 source-freshness `sources.json`，再透過 Application Default Credentials 寫入 `custom.googleapis.com/dbt/*`。GitHub CD 與 Airflow DAG 都使用同一支 publisher；dbt service account 只額外取得此用途所需的 `monitoring.metricWriter`。每個 workload 送出第一筆 sample 前，對應 dashboard tile 會保持空白。

### 容量測試

可重複執行的 Kafka-to-GCS 負載情境位於 [tests/load](tests/load/README.md)。Runner 以 `--target-eps`、`--duration-seconds`、`--run-id`、`--metrics-output` 呼叫正式 Producer，因此測試與部署使用相同的資料產生及傳送路徑。第一階段驗收目標為 50 EPS 持續 30 分鐘，尖峰驗證為 100 EPS 持續 15 分鐘。雲端測試應由 streaming private subnet 內的臨時 VM 執行，再把 Producer summary 與 dashboard 上的 Spark batch、GCS freshness 觀測值一起驗收。Stress test 必須人工啟動，不放入 PR CI。

### 雲端端到端驗收

[tests/cloud_e2e/README.md](tests/cloud_e2e/README.md) 是完整的繁體中文操作手冊，涵蓋 GCP 自動部署、私有網路／IAM、短時間 Producer→Kafka→Spark→GCS smoke test、BigQuery/dbt 三層 materialization、Monitoring time-series、測試報告與 guarded cleanup。完整測試要求輸入完全一致的 project ID 作為破壞性操作確認，並預設在結束或失敗時清理，避免測試資源持續計費。

---

*本專案作為個人作品集，展示了在商業環境中端到端數據工程、運維工程 (Platform/DataOps) 與機器學習的整合實踐能力。*
