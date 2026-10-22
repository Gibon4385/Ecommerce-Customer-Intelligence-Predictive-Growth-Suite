"""Terraform deployment and GCP control-plane health checks."""

from __future__ import annotations

import os
from pathlib import Path

from .common import Context, wait_until


def terraform_args(context: Context) -> list[str]:
    testing = context.config.get("testing", {})
    return [
        f"-var=project_id={context.project_id}",
        f"-var=region={context.region}",
        f"-var=zone={context.zone}",
        f"-var=enable_human_iam={str(testing.get('enable_human_iam', False)).lower()}",
        f"-var=enable_email_notifications={str(testing.get('enable_email_notifications', False)).lower()}",
        "-var=enable_cicd_resources=false",
    ]


def deploy(context: Context) -> bool:
    phase = "deploy"
    terraform_dir = context.repo_root / "terraform"
    plan_path = context.report_dir / "e2e.tfplan"
    commands = [
        ("terraform init", ["terraform", "init", "-input=false"]),
        ("terraform validate", ["terraform", "validate"]),
        ("terraform plan", ["terraform", "plan", "-input=false", f"-out={plan_path}", *terraform_args(context)]),
        ("terraform apply", ["terraform", "apply", "-input=false", "-auto-approve", str(plan_path)]),
    ]
    for name, command in commands:
        result = context.command(command, cwd=terraform_dir, check=False)
        log = context.report_dir / f"{name.replace(' ', '-')}.log"
        log.write_text(result.stdout + result.stderr, encoding="utf-8")
        context.add(phase, name, "PASS" if result.returncode == 0 else "FAIL", f"exit={result.returncode}; log={log.name}")
        if result.returncode != 0:
            return False

    outputs = context.json_command(["terraform", "output", "-json"], cwd=terraform_dir)
    for key, item in outputs.items():
        context.runtime[key] = str(item.get("value", ""))

    timeout = int(context.config["pipeline"].get("startup_timeout_seconds", 900))

    def vm_running():
        status = context.command(
            ["gcloud", "compute", "instances", "describe", "ecommerce-kafka", f"--zone={context.zone}", f"--project={context.project_id}", "--format=value(status)"],
            check=False,
        ).stdout.strip()
        return status == "RUNNING"

    context.check(phase, "Kafka VM running", lambda: wait_until("Kafka VM RUNNING", timeout, 15, vm_running))

    def kafka_ready():
        serial = context.command(
            [
                "gcloud", "compute", "instances", "get-serial-port-output",
                "ecommerce-kafka", f"--zone={context.zone}",
                f"--project={context.project_id}", "--port=1",
            ],
            check=False,
        )
        output = serial.stdout + serial.stderr
        if serial.returncode == 0 and "KAFKA_READY bootstrap=" in output:
            return True
        if "KAFKA_STARTUP_FAILED" in output:
            raise RuntimeError("Kafka startup script reported failure; inspect the VM serial console")
        return False

    kafka = context.check(
        phase,
        "Kafka broker ready",
        lambda: wait_until("Kafka broker readiness marker", timeout, 10, kafka_ready),
    )
    if kafka.status != "PASS":
        return False

    def cluster_running():
        status = context.command(
            ["gcloud", "dataproc", "clusters", "describe", "ecommerce-spark-demo", f"--region={context.region}", f"--project={context.project_id}", "--format=value(status.state)"],
            check=False,
        ).stdout.strip()
        return status == "RUNNING"

    context.check(phase, "Dataproc cluster running", lambda: wait_until("Dataproc RUNNING", timeout, 20, cluster_running))
    context.check(
        phase,
        "GCS data lake exists",
        lambda: context.command(["gcloud", "storage", "buckets", "describe", f"gs://{context.runtime['data_lake_bucket']}"]).stdout.strip() or context.runtime["data_lake_bucket"],
    )
    return not any(result.phase == phase and result.status == "FAIL" for result in context.results)


def deploy_spark_job(context: Context):
    env = os.environ.copy()
    env.update(
        {
            "PROJECT_ID": context.project_id,
            "REGION": context.region,
            "CLUSTER_NAME": context.runtime.get("spark_cluster_name", "ecommerce-spark-demo"),
            "KAFKA_BOOTSTRAP_SERVERS": context.runtime["kafka_internal_bootstrap_server"],
            "DATA_LAKE_BUCKET": context.runtime["data_lake_bucket"],
            "ASYNC": "true",
        }
    )
    completed = context.command(["bash", "deploy/managed-spark/submit.sh"], env=env)
    job_id = completed.stdout.strip().splitlines()[-1]
    context.runtime["spark_job_id"] = job_id
    return job_id


def deploy_producer(context: Context):
    env = os.environ.copy()
    env.update(
        {
            "PROJECT_ID": context.project_id,
            "REGION": context.region,
            "KAFKA_BOOTSTRAP_SERVERS": context.runtime["kafka_internal_bootstrap_server"],
            "PRODUCER_TARGET_EPS": str(context.config["pipeline"].get("target_eps", 10)),
            "PRODUCER_RUN_ID": context.runtime["run_id"],
            "PRODUCER_SEED": "42",
        }
    )
    context.command(["bash", "deploy/cloud-run/deploy-producer.sh"], env=env, timeout=1800)
    return "ecommerce-producer deployed"
