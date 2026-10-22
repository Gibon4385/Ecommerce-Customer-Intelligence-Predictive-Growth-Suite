# GCP 雲端端到端驗收測試

這套測試用來回答四個問題：

1. GCP 基礎設施是否能成功部署且沒有錯誤？
2. Producer、Kafka、Spark、GCS 是否能完成短時間的真實資料流？
3. BigQuery external tables 與 dbt staging／intermediate／marts 是否能在 GCP 建立？
4. Cloud Monitoring dashboard 是否存在，而且真的收到各元件的 metrics？

完整測試結束後預設執行 cleanup。這與 `tests/load/` 不同：`cloud_e2e` 驗證功能、部署與清理；`load` 才負責容量和壓力。

## 重要警告

這些腳本會建立及刪除真實 GCP 資源，可能產生費用。Cleanup 會刪除以下 datasets 與本專案資源：

- `raw_ecommerce`
- `dbt_ecommerce_staging`
- `dbt_ecommerce_intermediate`
- `dbt_ecommerce_marts`
- `ecommerce-kafka` VM 與 disk
- `ecommerce-spark-demo` Dataproc cluster
- `ecommerce-producer` Worker Pool
- `${PROJECT_ID}-ecommerce-lake`
- `${PROJECT_ID}-dbt-artifacts`
- `ecommerce-streaming` Artifact Registry、VPC、NAT、IAM、dashboard 與 alerts

腳本不會只因為看到 `--yes` 就刪除。任何 cleanup 都要求 `--confirm-project-id` 與設定中的完整 project ID 完全相同。刪除整個 GCP project 還需要第二個 `--confirm-delete-project`。

## 前置需求

本機需要：

```bash
gcloud
bq
terraform
python3
dbt
```

目前 Cloud Run Worker Pool 在部分較舊的 gcloud 版本仍只提供 alpha command group；測試腳本因此統一使用 `gcloud alpha run worker-pools`。Preflight 會先驗證這個 command group。若不存在，請先更新 Google Cloud CLI，或安裝 alpha component：

```bash
gcloud components install alpha
```

安裝後先執行 `gcloud alpha run worker-pools --help`，確認成功再開始付費部署。

安裝 Python dependencies。後續必須使用同一個 virtual environment 的
Python 啟動 E2E；preflight 會在建立任何付費資源前檢查 pandas、PyArrow
與 Google Cloud client libraries：

```bash
venv/bin/python -m pip install -r requirements.txt
```

登入並設定 project：

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

複製設定：

```bash
cp tests/cloud_e2e/config.example.yaml tests/cloud_e2e/config.yaml
```

`config.yaml` 被建議視為本機設定，不要放入真實秘密。E2E 預設關閉 Google Group IAM、Email channels 與 GitHub Workload Identity Federation，以免 placeholder group 或刪除後暫時保留的 WIF Pool ID 造成重複測試失敗；正式部署仍維持啟用。

建議使用獨立的測試 project，避免既有 BigQuery tables、service accounts 或 Artifact Registry repositories 與 Terraform 衝突。切換 project 前，必須先完成舊 project cleanup，並確認 `terraform state list` 為空；不可讓同一份 local state 同時管理兩個 project。接著同步修改 `config.yaml`、gcloud active project 與命令列確認值：

```bash
gcloud config set project NEW_PROJECT_ID
terraform -chdir=terraform state list
```

## 建議執行順序：deploy 一次、validate 多次、cleanup 一次

除錯期間不要在每次修正後重新執行 `full`。`full` 預設包含 cleanup，下一次
執行時會重新建立 Kafka VM 與 Dataproc cluster，也可能再次透過 Cloud NAT
下載 Kafka image、系統套件及 Spark dependencies，造成不必要的
`Cloud Nat Data Processing` 費用。

建議流程：

```text
deploy 一次 → validate 多次 → cleanup 一次
```

### 1. 第一次只建立環境

```bash
venv/bin/python tests/cloud_e2e/run.py deploy \
  --config tests/cloud_e2e/config.yaml \
  --enable-apis \
  --acknowledge-cost
```

只有 Terraform infrastructure 有變更、前一次 deploy 未完成，或資源已被刪除
時才重新執行 deploy。

### 2. 保留環境並重複驗證

```bash
venv/bin/python tests/cloud_e2e/run.py validate \
  --config tests/cloud_e2e/config.yaml
```

若 validate 失敗，先查看最新的 `tests/cloud_e2e/reports/<RUN_ID>/report.md`
與相關 log，修正後再執行 validate。只修改 Producer、Spark、dbt 或驗證程式
時，不要先 cleanup 或重新 deploy。

### 3. 全部驗證完成後只清理一次

```bash
venv/bin/python tests/cloud_e2e/run.py cleanup \
  --config tests/cloud_e2e/config.yaml \
  --confirm-project-id YOUR_PROJECT_ID \
  --execute
```

清理後確認 Terraform state：

```bash
terraform -chdir=terraform state list
```

理想結果應該沒有輸出。接著執行殘留資源檢查：

```bash
venv/bin/python tests/cloud_e2e/run.py audit \
  --config tests/cloud_e2e/config.yaml
```

## 完整測試：僅用於最終驗收

當 deploy、validate 與 cleanup 已分別確認正常後，才執行一次完整生命週期
測試：

```bash
venv/bin/python tests/cloud_e2e/run.py full \
  --config tests/cloud_e2e/config.yaml \
  --enable-apis \
  --confirm-project-id YOUR_PROJECT_ID
```

完整流程：

```text
Preflight
→ 啟用／驗證 APIs
→ Terraform init / validate / plan / apply
→ Kafka VM 與 Dataproc readiness
→ 私有網路與 IAM 檢查
→ 非同步提交 Spark job
→ 部署 Producer Worker Pool
→ 執行 60 秒 smoke window
→ 停止 Producer
→ 驗證 GCS raw Parquet 與 checkpoint
→ 取消 Spark job
→ 上傳可用的歷史 CSV Parquet
→ 建立空的 NLP/BQML interface fixtures
→ dbt source freshness / build
→ 驗證三層 BigQuery objects
→ 發送並驗證 Monitoring metrics
→ 清除非 Terraform 資源與資料
→ terraform destroy
→ 殘留計費資源 audit
```

即使測試失敗、Python 發生 exception 或收到 `Ctrl+C`，`full` 仍會進入
cleanup。若確實要保留資源除錯，必須明確指定：

```bash
venv/bin/python tests/cloud_e2e/run.py full \
  --config tests/cloud_e2e/config.yaml \
  --confirm-project-id YOUR_PROJECT_ID \
  --keep-resources \
  --acknowledge-cost
```

`--keep-resources` 代表 VM、Dataproc、NAT 與其他資源會繼續計費。完成除錯後
仍必須手動執行 cleanup。

## 測試階段

### Preflight

自動檢查 CLI、登入帳號、project 完整比對、billing、必要 APIs，並把執行前資源清單寫入 `inventory-before.json`。任何 project mismatch 都應停止後續流程。

### Infrastructure

Terraform 每一步都保存 log。部署後除了確認 Kafka VM 為 RUNNING，還會從
serial console 等待 startup script 的 `KAFKA_READY` marker；該 marker 只有在
broker 能完成真實 metadata request 後才會出現。之後才會提交 Spark job 與
部署 Producer。Dataproc cluster 與 GCS bucket 也分別輪詢；單純
`terraform apply` exit code 為零並不足以通過 readiness。

Kafka persistent disk 掛載於 `/var/lib/kafka-data`，broker 的 `log.dirs` 使用
其下的 `data/` 子目錄，避免 ext4 根目錄的 `lost+found` 被 Kafka 誤判為
topic-partition directory。


### Security

自動確認 Kafka VM 沒有 external IP、9092 firewall 只接受 `10.20.0.0/24`、Dataproc 為 internal-only、四個 workload identities 分離，且沒有 user-managed service-account keys。

### Pipeline smoke test

預設資料產生時間為 60 秒。30～60 秒指 Producer smoke window，不包含 Dataproc 建立時間。Spark job 進入 `RUNNING` 後還必須連續穩定 30 秒，才會部署 Producer；Producer 部署後還必須在 Cloud Logging 出現包含本次 run ID 的 heartbeat，才能開始 smoke window。任一啟動步驟失敗時 pipeline 會立即停止並交由 finally cleanup；第一個必要 GCS path timeout 後也會立即停止，不再等待其他不可能出現的 paths。測試會等待每個 streaming entity 的 raw Parquet 與 Spark checkpoint 出現，再停止 Producer 與 Spark job。

Cloud Build 使用根目錄 `.gcloudignore`，只讓 Producer image 所需檔案進入 build context，排除 venv、Terraform provider/state、dbt artifacts、測試報告與本機資料。

Spark 是否成功寫入 GCS，也同時證明 Producer → Kafka → Spark 的連線路徑可用。目前 runner 從一般本機無法連入 Kafka 私有 IP，所以主動 malformed-message injection 會標成 `SKIP`；不能以 SKIP 宣稱 quarantine 已完整驗證。若將 runner 放入相同 VPC，後續可補私有 injector。

Dataproc cluster 明確啟用
`dataproc.logging.stackdriver.job.driver.enable=true`。此設定必須在 cluster
建立時指定，讓 Spark driver heartbeat 以 `cloud_dataproc_job` resource 進入
Cloud Logging，才能產生 Spark-to-GCS 與 batch metrics。Dataproc driver log
的訊息位於 `jsonPayload.message`，Spark log-based metrics 必須從該欄位篩選與
擷取數值；Cloud Run Producer heartbeat 則位於 `textPayload`。

### BigQuery 與 dbt

測試會先以目前登入的 `gcloud` identity 將本機 CSV fixtures 轉成 Parquet 並
上傳到 raw zone；此測試 seed 不使用 `.env` 的 `SERVICE_ACCOUNT_JSON`，避免
切換 project 後誤用舊 service account。接著查詢六個 raw external tables，再執行：

```bash
dbt source freshness --target prod
dbt build --target prod
```

接著由 repo 內 `.sql` 自動推導預期 models，使用 BigQuery metadata 比對：

- staging 全部為 views
- intermediate 全部為 views
- marts 全部為 tables

Sentiment、LTV prediction、pain-points 原本由外部 Python／BQML jobs 產生。E2E 會建立明確的空 schema fixtures，僅驗證 dbt interface 與三層 materialization，不會把空 fixture 宣稱為 ML/NLP 結果正確。

### Monitoring

先從 Monitoring API 讀取 dashboard JSON，確認必要 metric filters 全部存在；再用共同的五分鐘 timeout 輪詢所有 Producer、Spark 與 dbt time series。Dashboard resource 存在但沒有 sample 時仍判定 `FAIL`。

Dashboard 使用 Cloud Monitoring `v1` API，time-series 則使用 `v3` API；任何
API 錯誤會保留在報告中，不會再被模糊地顯示成單純沒有 sample。

Cloud Monitoring 有寫入延遲，因此不要把 timeout 設得太短。

## Cleanup 預覽

只顯示操作，不刪除：

```bash
venv/bin/python tests/cloud_e2e/run.py cleanup \
  --config tests/cloud_e2e/config.yaml \
  --confirm-project-id YOUR_PROJECT_ID
```

真正執行：

```bash
venv/bin/python tests/cloud_e2e/run.py cleanup \
  --config tests/cloud_e2e/config.yaml \
  --confirm-project-id YOUR_PROJECT_ID \
  --execute
```

Cleanup 順序為：停止 workloads、等待 Cloud Run Direct VPC egress 的 serverless address 從 subnet 釋放、刪除 dbt datasets、以 recursive bucket deletion 清除所有 object versions 與測試 buckets、刪除 Artifact Registry packages、執行 Terraform destroy、最後 audit VM、disk、Dataproc、Worker Pool、VPC/subnet/serverless address、Artifact Registry、GCS 與 BigQuery。若 GCP 暫時回報 subnet 仍被使用，Terraform destroy 最多重試三次；完整 output 會保存為 `terraform-destroy.log`。

Terraform 已把 Dataproc staging 與 temp bucket 都指定為 Data Lake bucket，因此新測試不會再建立額外的 `dataproc-temp-*` bucket。兩個測試 bucket也停用 soft-delete retention，避免 cleanup 後繼續保存可計費的 soft-deleted objects；這項設定只適用於可拋棄的作品集測試資料，不應直接套用到需要誤刪復原能力的正式資料湖。

Cloud Build 建置 Producer image 時可能建立 `${PROJECT_ID}_cloudbuild` bucket；cleanup 也會清空並刪除這個非 Terraform bucket。Logging／Monitoring 的既有歷史資料會依 GCP retention policy 到期，停止 workloads 並刪除 metric descriptors 後不再產生新 samples。

## 刪除整個 GCP project

只有確定 project 內所有資源都可刪除時才使用：

```bash
venv/bin/python tests/cloud_e2e/run.py cleanup \
  --config tests/cloud_e2e/config.yaml \
  --confirm-project-id YOUR_PROJECT_ID \
  --delete-project \
  --confirm-delete-project YOUR_PROJECT_ID \
  --execute
```

Project deletion 是最強的成本終止方式，但 Billing 報表可能延遲，無法在同一秒證明帳單數字歸零。未刪除 project 時，測試以「沒有 VM、disk、Dataproc、Worker Pool、NAT、images、buckets、datasets 且 Terraform state 為空」作為不再持續計費的工程證據。

## 報告

每次執行會建立：

```text
tests/cloud_e2e/reports/<UTC timestamp>/
├── report.md
├── report.json
├── inventory-before.json
├── inventory-after.json
└── 各階段 logs
```

所有檢查只能是 `PASS`、`FAIL` 或 `SKIP`。`SKIP` 不視為成功，也不應在作品集說明中被描述為已驗證。
