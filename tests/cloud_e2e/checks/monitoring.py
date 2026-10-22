"""Dashboard structure and emitted time-series checks."""

from __future__ import annotations

import datetime as dt
import json
import time
import urllib.parse

from .common import Context


EXPECTED_METRICS = (
    "logging.googleapis.com/user/pipeline/producer_heartbeat",
    "logging.googleapis.com/user/pipeline/spark_gcs_heartbeat",
    "logging.googleapis.com/user/pipeline/spark_input_records",
    "logging.googleapis.com/user/pipeline/spark_processed_records",
    "logging.googleapis.com/user/pipeline/spark_rejected_records",
    "logging.googleapis.com/user/pipeline/spark_batch_duration_seconds",
    "custom.googleapis.com/dbt/run_success",
    "custom.googleapis.com/dbt/models_failed",
    "custom.googleapis.com/dbt/tests_failed",
    "custom.googleapis.com/dbt/run_duration_seconds",
    "custom.googleapis.com/dbt/last_success_unixtime",
    "custom.googleapis.com/dbt/source_freshness_success",
    "custom.googleapis.com/dbt/stale_sources",
)


def run(context: Context):
    phase = "monitoring"
    dashboard_base = f"https://monitoring.googleapis.com/v1/projects/{context.project_id}"
    time_series_base = f"https://monitoring.googleapis.com/v3/projects/{context.project_id}"
    dashboards = context.google_api_get(f"{dashboard_base}/dashboards").get("dashboards", [])
    dashboard = next((item for item in dashboards if item.get("displayName") == "Ecommerce streaming pipeline health"), None)
    if dashboard:
        serialized = json.dumps(dashboard)
        missing = [metric for metric in EXPECTED_METRICS if metric not in serialized]
        context.add(phase, "dashboard structure", "FAIL" if missing else "PASS", f"missing={missing}" if missing else f"contains {len(EXPECTED_METRICS)} required metrics")
    else:
        context.add(phase, "dashboard structure", "FAIL", "dashboard not found")

    timeout = int(context.config["monitoring"].get("sample_timeout_seconds", 300))
    interval = int(context.config["monitoring"].get("poll_interval_seconds", 15))
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=2)

    def has_sample(metric):
        params = urllib.parse.urlencode(
            {
                "filter": f'metric.type="{metric}"',
                "interval.startTime": start.isoformat().replace("+00:00", "Z"),
                "interval.endTime": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "view": "HEADERS",
            }
        )
        return bool(
            context.google_api_get(f"{time_series_base}/timeSeries?{params}").get("timeSeries")
        )

    pending = set(EXPECTED_METRICS)
    last_errors = {}
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for metric in list(pending):
            try:
                if has_sample(metric):
                    pending.remove(metric)
            except Exception as error:  # retain API diagnostics in the report
                last_errors[metric] = str(error)
        if pending:
            time.sleep(interval)

    for metric in EXPECTED_METRICS:
        context.add(
            phase,
            f"metric sample:{metric}",
            "FAIL" if metric in pending else "PASS",
            (
                f"no sample before shared timeout; last API error: {last_errors[metric]}"
                if metric in pending and metric in last_errors
                else "no sample before shared timeout"
                if metric in pending
                else "time series found"
            ),
        )
