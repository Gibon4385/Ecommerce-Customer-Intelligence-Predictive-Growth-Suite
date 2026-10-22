#!/usr/bin/env python3
"""Publish dbt artifacts as Cloud Monitoring custom metrics without querying BigQuery."""

import argparse
import json
import time
from pathlib import Path

from google.cloud import monitoring_v3


SUCCESS_STATUSES = {"success", "pass", "skipped", "warn"}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_run(run_results: dict) -> dict[str, int | float]:
    results = run_results.get("results", [])
    failed_models = 0
    failed_tests = 0
    duration = 0.0

    for result in results:
        status = str(result.get("status", "unknown")).lower()
        node_type = str(result.get("unique_id", "unknown")).split(".", 1)[0]
        duration += float(result.get("execution_time") or 0.0)
        if status not in SUCCESS_STATUSES:
            if node_type == "test":
                failed_tests += 1
            elif node_type == "model":
                failed_models += 1

    return {
        "run_success": int(failed_models == 0 and failed_tests == 0),
        "models_failed": failed_models,
        "tests_failed": failed_tests,
        "run_duration_seconds": duration,
    }


def summarize_freshness(sources: dict) -> dict[str, int]:
    results = sources.get("results", [])
    stale = sum(
        1
        for result in results
        if str(result.get("status", "error")).lower() not in {"pass", "warn"}
    )
    return {
        "source_freshness_success": int(bool(results) and stale == 0),
        "stale_sources": stale,
    }


def write_metric(client, project_name: str, project_id: str, name: str, value):
    series = monitoring_v3.TimeSeries()
    series.metric.type = f"custom.googleapis.com/dbt/{name}"
    series.resource.type = "global"
    series.resource.labels["project_id"] = project_id

    now = time.time()
    point_value = {}
    if isinstance(value, int):
        point_value["int64_value"] = value
    else:
        point_value["double_value"] = float(value)
    series.points = [
        monitoring_v3.Point(
            {
                "interval": {
                    "end_time": {
                        "seconds": int(now),
                        "nanos": int((now - int(now)) * 1_000_000_000),
                    }
                },
                "value": point_value,
            }
        )
    ]
    client.create_time_series(name=project_name, time_series=[series])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--run-results", required=True, type=Path)
    parser.add_argument("--sources", type=Path)
    args = parser.parse_args()

    if args.run_results.exists():
        metrics = summarize_run(read_json(args.run_results))
    else:
        metrics = {
            "run_success": 0,
            "models_failed": 0,
            "tests_failed": 0,
            "run_duration_seconds": 0.0,
        }
    if args.sources and args.sources.exists():
        metrics.update(summarize_freshness(read_json(args.sources)))

    if metrics["run_success"]:
        metrics["last_success_unixtime"] = time.time()

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{args.project_id}"
    for name, value in metrics.items():
        write_metric(client, project_name, args.project_id, name, value)
        print(f"Published custom.googleapis.com/dbt/{name}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
