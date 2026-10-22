"""Short-lived Producer → Kafka → Spark → GCS smoke validation."""

from __future__ import annotations

import time

from .common import Context, wait_until
from .infrastructure import deploy_producer, deploy_spark_job


STREAM_PATHS = ("users", "events", "orders", "order_items", "reviews")


def _stop_worker_pool(context: Context):
    deletion = context.command(
        ["gcloud", "alpha", "run", "worker-pools", "delete", "ecommerce-producer", f"--region={context.region}", f"--project={context.project_id}", "--quiet"],
        check=False,
    )
    context.add(
        "pipeline",
        "stop Producer Worker Pool",
        "PASS" if deletion.returncode == 0 else "FAIL",
        "deleted" if deletion.returncode == 0 else (deletion.stderr or deletion.stdout).strip(),
    )


def _stop_spark_job(context: Context, job_id: str):
    cancellation = context.command(
        ["gcloud", "dataproc", "jobs", "kill", job_id, f"--region={context.region}", f"--project={context.project_id}", "--quiet"],
        check=False,
    )
    context.add(
        "pipeline",
        "stop Spark streaming job",
        "PASS" if cancellation.returncode == 0 else "FAIL",
        "cancelled after smoke window" if cancellation.returncode == 0 else (cancellation.stderr or cancellation.stdout).strip(),
    )


def run(context: Context):
    phase = "pipeline"
    submission = context.check(phase, "submit Spark job asynchronously", lambda: deploy_spark_job(context))
    if submission.status != "PASS":
        return
    job_id = context.runtime.get("spark_job_id")
    if not job_id:
        context.add(phase, "Spark job running", "FAIL", "job ID unavailable")
        return

    timeout = int(context.config["pipeline"].get("startup_timeout_seconds", 900))

    def spark_active():
        state = context.command(
            ["gcloud", "dataproc", "jobs", "describe", job_id, f"--region={context.region}", f"--project={context.project_id}", "--format=value(status.state)"],
            check=False,
        ).stdout.strip()
        if state in {"ERROR", "CANCELLED", "ATTEMPT_FAILURE"}:
            raise RuntimeError(f"Spark job entered {state}")
        return state == "RUNNING"

    active = context.check(
        phase,
        "Spark job active",
        lambda: wait_until("Spark streaming job active", timeout, 15, spark_active),
    )
    if active.status != "PASS":
        return

    stability_seconds = int(context.config["pipeline"].get("spark_stability_seconds", 30))

    def spark_stable():
        deadline = time.monotonic() + stability_seconds
        while time.monotonic() < deadline:
            state = context.command(
                ["gcloud", "dataproc", "jobs", "describe", job_id, f"--region={context.region}", f"--project={context.project_id}", "--format=value(status.state)"],
                check=False,
            ).stdout.strip()
            if state != "RUNNING":
                raise RuntimeError(f"Spark job left RUNNING state during stability window: {state or 'unknown'}")
            time.sleep(min(5, max(0, deadline - time.monotonic())))
        return f"remained RUNNING for {stability_seconds} seconds"

    stable = context.check(phase, "Spark job stable", spark_stable)
    if stable.status != "PASS":
        return

    producer = context.check(phase, "deploy Producer Worker Pool", lambda: deploy_producer(context))
    if producer.status != "PASS":
        _stop_spark_job(context, job_id)
        return

    heartbeat_timeout = int(
        context.config["pipeline"].get("producer_heartbeat_timeout_seconds", 180)
    )

    def producer_heartbeat():
        run_id = context.runtime["run_id"]
        log_filter = (
            'resource.type="cloud_run_worker_pool" '
            'AND textPayload:"PIPELINE_HEARTBEAT component=producer" '
            f'AND textPayload:"run_id={run_id}"'
        )
        result = context.command(
            [
                "gcloud", "logging", "read", log_filter,
                f"--project={context.project_id}", "--freshness=15m", "--limit=1",
                "--format=value(timestamp)",
            ],
            check=False,
        )
        return result.returncode == 0 and bool(result.stdout.strip())

    heartbeat = context.check(
        phase,
        "Producer heartbeat observed",
        lambda: wait_until(
            "Producer heartbeat", heartbeat_timeout, 10, producer_heartbeat
        ),
    )
    if heartbeat.status != "PASS":
        _stop_worker_pool(context)
        _stop_spark_job(context, job_id)
        return

    duration = int(context.config["pipeline"].get("duration_seconds", 60))
    started = time.monotonic()
    while time.monotonic() - started < duration:
        time.sleep(min(5, duration - (time.monotonic() - started)))
    context.add(phase, "producer smoke window", "PASS", f"ran for {duration} seconds")

    # Stop new test events before validating downstream persistence.
    _stop_worker_pool(context)

    bucket = context.runtime["data_lake_bucket"]
    data_timeout = int(context.config["pipeline"].get("data_timeout_seconds", 300))

    for path in STREAM_PATHS:
        def has_parquet(path=path):
            result = context.command(
                ["gcloud", "storage", "ls", "--recursive", f"gs://{bucket}/raw/{path}/"],
                check=False,
            )
            return result.returncode == 0 and ".parquet" in result.stdout

        parquet = context.check(
            phase,
            f"GCS raw/{path} has Parquet",
            lambda path=path: wait_until(f"raw/{path} Parquet", data_timeout, 10, lambda: has_parquet(path)),
        )
        if parquet.status != "PASS":
            _stop_spark_job(context, job_id)
            return

    def checkpoint_exists():
        result = context.command(
            ["gcloud", "storage", "ls", "--recursive", f"gs://{bucket}/checkpoints/kafka-to-gcs/"],
            check=False,
        )
        return result.returncode == 0 and bool(result.stdout.strip())

    context.check(phase, "Spark checkpoint exists", lambda: wait_until("GCS checkpoint", data_timeout, 10, checkpoint_exists))

    if context.config.get("private_kafka_reachable"):
        context.add(
            phase,
            "malformed-message quarantine",
            "SKIP",
            "private injection is enabled but an explicit safe injector has not been configured",
        )
    else:
        context.add(
            phase,
            "malformed-message quarantine",
            "SKIP",
            "runner cannot reach the private Kafka endpoint; run this optional check from the VPC",
        )

    state = context.command(
        ["gcloud", "dataproc", "jobs", "describe", job_id, f"--region={context.region}", f"--project={context.project_id}", "--format=value(status.state)"],
        check=False,
    ).stdout.strip()
    context.add(phase, "Spark final state", "FAIL" if state in {"ERROR", "CANCELLED"} else "PASS", state or "unknown")

    _stop_spark_job(context, job_id)
