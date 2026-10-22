"""BigQuery raw access and dbt three-layer materialization checks."""

from __future__ import annotations

import os
from pathlib import Path
import sys

from .common import Context


RAW_TABLES = ("raw_users", "raw_events", "raw_orders", "raw_order_items", "raw_reviews", "raw_products")


def model_names(root: Path, layer: str) -> set[str]:
    return {path.stem for path in (root / "ecommerce_dbt" / "models" / layer).rglob("*.sql")}


def bq_tables(context: Context, dataset: str) -> dict[str, str]:
    rows = context.json_command(["bq", "ls", "--format=prettyjson", f"{context.project_id}:{dataset}"])
    return {
        row["tableReference"]["tableId"]: row.get("type", "UNKNOWN")
        for row in (rows or [])
    }


def run(context: Context):
    phase = "bigquery-dbt"
    ingest_env = os.environ.copy()
    ingest_env.update(
        {
            "PROJECT_ID": context.project_id,
            "DATA_LAKE_BUCKET": context.runtime["data_lake_bucket"],
        }
    )
    ingest = context.command(
        [sys.executable, "scripts/ingest_raw_data.py", "--upload-with-gcloud"],
        env=ingest_env,
        check=False,
        timeout=900,
    )
    ingest_failed = ingest.returncode != 0 or "❌" in ingest.stdout
    context.add(
        phase,
        "historical Parquet ingestion",
        "FAIL" if ingest_failed else "PASS",
        "uploaded available CSV fixtures" if not ingest_failed else (ingest.stdout + ingest.stderr).strip(),
    )

    for table in RAW_TABLES:
        query = f"SELECT COUNT(*) AS row_count FROM `{context.project_id}.raw_ecommerce.{table}`"
        result = context.command(
            ["bq", "query", "--use_legacy_sql=false", "--format=csv", query],
            check=False,
        )
        context.add(
            phase,
            f"raw table query:{table}",
            "PASS" if result.returncode == 0 else "FAIL",
            result.stdout.strip().splitlines()[-1]
            if result.returncode == 0 and result.stdout.strip()
            else (result.stderr or result.stdout).strip(),
        )

    # These empty tables represent outputs owned by the separate NLP/BQML jobs.
    # They let this E2E test prove dbt materialization without inventing results.
    fixture_sql = f"""
    CREATE TABLE IF NOT EXISTS `{context.project_id}.dbt_ecommerce_intermediate.int_review_sentiment_detail`
      (review_id STRING, compound_score FLOAT64, risk_flag INT64);
    CREATE TABLE IF NOT EXISTS `{context.project_id}.dbt_ecommerce_intermediate.int_review_sentiment_user`
      (user_id STRING, compound_score FLOAT64);
    CREATE TABLE IF NOT EXISTS `{context.project_id}.dbt_ecommerce_intermediate.int_ltv_predictions`
      (user_id STRING, predicted_profit_90_days FLOAT64, primary_driver STRING, primary_barrier STRING);
    CREATE TABLE IF NOT EXISTS `{context.project_id}.dbt_ecommerce_intermediate.int_product_pain_points`
      (product_id STRING, top_pain_points STRING);
    """
    fixtures = context.command(
        ["bq", "query", "--use_legacy_sql=false", fixture_sql], check=False
    )
    context.add(
        phase,
        "external-model schema fixtures",
        "PASS" if fixtures.returncode == 0 else "FAIL",
        "created empty NLP/BQML interface tables" if fixtures.returncode == 0 else fixtures.stderr.strip(),
    )

    env = os.environ.copy()
    env.update({"DBT_PROJECT": context.project_id, "DBT_PROFILES_DIR": str(context.repo_root / "ecommerce_dbt")})
    dbt_dir = context.repo_root / "ecommerce_dbt"
    for name, command in (
        ("dbt deps", ["dbt", "deps"]),
        ("dbt source freshness", ["dbt", "source", "freshness", "--target", "prod", "--target-path", "target/freshness"]),
        ("dbt build", ["dbt", "build", "--target", "prod"]),
    ):
        result = context.command(command, cwd=dbt_dir, env=env, check=False, timeout=1800)
        log = context.report_dir / f"{name.replace(' ', '-')}.log"
        log.write_text(result.stdout + result.stderr, encoding="utf-8")
        context.add(phase, name, "PASS" if result.returncode == 0 else "FAIL", f"exit={result.returncode}; log={log.name}")

    publisher = context.command(
        [
            sys.executable, "scripts/publish_dbt_metrics.py",
            "--project-id", context.project_id,
            "--run-results", "ecommerce_dbt/target/run_results.json",
            "--sources", "ecommerce_dbt/target/freshness/sources.json",
        ],
        check=False,
    )
    context.add(phase, "publish dbt metrics", "PASS" if publisher.returncode == 0 else "FAIL", publisher.stdout.strip() or publisher.stderr.strip())

    dataset_config = context.config["dbt_datasets"]
    expected_types = {"staging": "VIEW", "intermediate": "VIEW", "marts": "TABLE"}
    for layer, dataset in dataset_config.items():
        try:
            actual = bq_tables(context, dataset)
            expected = model_names(context.repo_root, layer)
            missing = sorted(expected - set(actual))
            wrong_type = sorted(name for name in expected & set(actual) if actual[name] != expected_types[layer])
            if missing or wrong_type:
                raise RuntimeError(f"missing={missing}; wrong_type={wrong_type}")
            context.add(phase, f"dbt {layer} objects", "PASS", f"{len(expected)} {expected_types[layer].lower()} objects")
        except Exception as error:  # noqa: BLE001
            context.add(phase, f"dbt {layer} objects", "FAIL", str(error))
