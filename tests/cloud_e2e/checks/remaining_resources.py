"""Audit resource types that can continue generating material GCP cost."""

from __future__ import annotations

import json

from .common import Context


AUDIT_COMMANDS = {
    "compute_instances": ["gcloud", "compute", "instances", "list", "--format=json"],
    "compute_disks": ["gcloud", "compute", "disks", "list", "--format=json"],
    "dataproc_clusters": ["gcloud", "dataproc", "clusters", "list", "--format=json"],
    "dataproc_jobs": ["gcloud", "dataproc", "jobs", "list", "--format=json"],
    "cloud_run_worker_pools": ["gcloud", "alpha", "run", "worker-pools", "list", "--format=json"],
    "artifact_repositories": ["gcloud", "artifacts", "repositories", "list", "--format=json"],
    "storage_buckets": ["gcloud", "storage", "buckets", "list", "--format=json"],
    "streaming_networks": ["gcloud", "compute", "networks", "list", "--filter=name=ecommerce-streaming", "--format=json"],
    "streaming_subnets": ["gcloud", "compute", "networks", "subnets", "list", "--filter=name:ecommerce-streaming", "--format=json"],
    "serverless_addresses": ["gcloud", "compute", "addresses", "list", "--filter=purpose=SERVERLESS", "--format=json"],
}


def run(context: Context, *, expect_empty: bool):
    phase = "resource-audit"
    inventory = {}
    for name, base_command in AUDIT_COMMANDS.items():
        command = [*base_command, f"--project={context.project_id}"]
        if name.startswith("dataproc_"):
            command.append(f"--region={context.region}")
        if name == "cloud_run_worker_pools":
            command.append(f"--region={context.region}")
        result = context.command(command, check=False)
        try:
            resources = json.loads(result.stdout or "[]") if result.returncode == 0 else []
        except json.JSONDecodeError:
            resources = []
        inventory[name] = resources
        if expect_empty:
            active = resources
            if name == "dataproc_jobs":
                active = [item for item in resources if item.get("status", {}).get("state") in {"PENDING", "RUNNING"}]
            context.add(phase, name, "FAIL" if active else "PASS", f"remaining={len(active)}")
        else:
            context.add(phase, name, "PASS", f"found={len(resources)}")

    datasets = context.command(
        ["bq", "ls", "--format=prettyjson", f"--project_id={context.project_id}"], check=False
    )
    try:
        dataset_rows = json.loads(datasets.stdout or "[]") if datasets.returncode == 0 else []
    except json.JSONDecodeError:
        dataset_rows = []
    inventory["bigquery_datasets"] = dataset_rows
    context.add(
        phase,
        "bigquery_datasets",
        "FAIL" if expect_empty and dataset_rows else "PASS",
        f"remaining={len(dataset_rows)}" if expect_empty else f"found={len(dataset_rows)}",
    )
    context.report_dir.mkdir(parents=True, exist_ok=True)
    filename = "inventory-after.json" if expect_empty else "inventory-current.json"
    (context.report_dir / filename).write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    return inventory
