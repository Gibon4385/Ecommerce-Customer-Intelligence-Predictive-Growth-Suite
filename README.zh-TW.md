# 電商客戶情報與預測成長平台

[English](README.md) | **繁體中文**

一套部署於 Google Cloud 的端到端資料平台，將電商交易、瀏覽行為與評論資料，轉換成可直接供 CRM、行銷與產品團隊使用的決策資訊。

它不只回答「發生了什麼」，更進一步回答：

- 今天應優先聯絡哪些客戶？
- 哪些高價值客戶有流失風險？
- 哪些購物車值得挽回、哪些商品需要改善？
- 行銷預算應優先投入在哪些客群？

## 專案成果

- 建立從 **即時事件、Data Lake、資料倉儲、機器學習到 BI** 的完整資料鏈路。
- 產出 **Customer 360、RFM 客群、LTV 預測、流失風險、同期群分析、商品健康度與每日 CRM 行動名單**。
- 以 Kafka 與 Spark Structured Streaming 處理事件，將可重放的 Parquet raw data 保存於 GCS。
- 以 dbt 建立具測試與資料契約的 staging、intermediate、marts 三層模型。
- 以 Airflow 自動排程，並以 GitHub Actions、Terraform、Cloud Monitoring 建立可部署、可測試、可觀測的工程流程。
- 提供真實 GCP 端到端驗收與可重複的 Kafka-to-GCS 負載測試。

## 系統如何運作

```text
歷史 CSV                  即時模擬事件
    │                          │
    │                    Python Producer
    │                          ▼
    │                 Kafka（私有 GCE VM）
    │                          ▼
    │              Spark Structured Streaming
    │                          │
    └──────────────────────────▼
                     GCS Data Lake（Parquet）
                               ▼
                    BigQuery External Tables
                               ▼
               dbt Staging → Intermediate → Marts
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
          Customer 360     LTV／流失風險    商品／品牌洞察
                └──────────────┼──────────────┘
                               ▼
                   CRM 行動名單與 BI 儀表板
```

Spark 專注於串流攝取、schema 驗證、lineage、異常資料隔離與 GCS 落地；dbt 負責可重跑、可測試的商業邏輯。這個責任邊界避免同一套 KPI 同時散落在串流與倉儲程式中。

## 主要商業輸出

| 輸出 | 解決的問題 |
|---|---|
| **Customer 360** | 整合客戶價值、行為、情緒、客群與預測結果 |
| **LTV 與流失風險** | 找出值得優先投入留存預算的客戶 |
| **每日 CRM 行動名單** | 建議 Retention、客服關懷、再行銷或相似受眾擴展 |
| **Cohort 分析** | 比較不同獲客批次的留存與營收品質 |
| **商品與品牌健康度** | 從評分、負面評論與痛點辨識產品風險 |
| **商品關聯分析** | 找出交叉銷售與推薦機會 |

## Dashboard 預覽

![Executive Overview Dashboard](<Dashboard Preview/slack-image_executive_overview_SeOi3potUINQYtgz_bM.png>)

Dashboard 將客群規模、營收貢獻、平均客戶價值與情緒風險放在同一個決策畫面，讓管理者能快速定位最值得關注的客群。

## 技術棧

| 領域 | 技術 |
|---|---|
| 串流與處理 | Apache Kafka、Spark Structured Streaming、Python |
| Data Lake / Warehouse | Google Cloud Storage、Parquet、BigQuery |
| 資料建模 | dbt、SQL |
| ML / NLP | BigQuery ML、XGBoost、Scikit-learn、SHAP、VADER |
| 調度與 BI | Apache Airflow、Lightdash |
| Cloud / DevOps | GCP、Terraform、Docker、GitHub Actions、Cloud Monitoring |

## 工程設計

- **資料品質**：dbt contracts、unique、not-null、relationships、accepted-values 與 source freshness 測試。
- **隱私與權限**：移除明文 Email、提供 SHA-256 投放匹配資料，並以 dataset-level IAM 隔離 raw、intermediate 與 marts。
- **無金鑰 CI/CD**：GitHub Actions 透過 Workload Identity Federation 使用短期憑證；Slim CI 只建置有變更的 dbt models 及其下游。
- **可觀測性**：監控 Producer、Kafka、Spark、GCS freshness、dbt 執行結果與資料新鮮度。
- **完整驗收**：Cloud E2E 實際部署並驗證 Producer → Kafka → Spark → GCS → BigQuery → dbt → Monitoring，再以 guarded cleanup 清除資源。

## 快速體驗

本機可先啟動 Kafka 與 Kafka UI，觀察模擬電商事件：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d kafka kafka-ui
python scripts/run_gap_filler.py --action gap-fill
python scripts/run_kafka_producer.py --delay 1.0
```

Kafka UI：<http://localhost:8085>

完整流程需要 GCP、Terraform、Spark、BigQuery 與 dbt。部署與驗收方式請參考：

- [GCP 雲端端到端驗收](tests/cloud_e2e/README.md)
- [Kafka-to-GCS 負載測試](tests/load/README.md)
- [dbt 模型](ecommerce_dbt/models)
- [Terraform 基礎設施](terraform)

## 專案範圍與取捨

這是一個以商業問題為核心的個人作品集，涵蓋 Data Engineering、Analytics Engineering、ML 與 DataOps。為控制展示成本，Kafka 與 Dataproc 採單節點設計，可完整展示資料流與維運能力，但不宣稱為正式環境的高可用架構。
