"""Local tools, identity, project, billing, API, and initial inventory checks."""

from __future__ import annotations

import json
import re
import shutil
import sys

from .common import Context


REQUIRED_TOOLS = ("gcloud", "terraform", "bq", "dbt", "python3")
REQUIRED_APIS = (
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "sts.googleapis.com",
)

REQUIRED_PYTHON_MODULES = (
    "google.cloud.monitoring_v3",
    "google.cloud.storage",
    "pandas",
    "pyarrow",
    "yaml",
)


def run(context: Context, *, enable_apis: bool = False):
    phase = "preflight"
    for tool in REQUIRED_TOOLS:
        context.add(
            phase,
            f"tool:{tool}",
            "PASS" if shutil.which(tool) else "FAIL",
            shutil.which(tool) or "not found in PATH",
        )

    python_imports = context.command(
        [
            sys.executable,
            "-c",
            "import " + ", ".join(REQUIRED_PYTHON_MODULES),
        ],
        check=False,
    )
    context.add(
        phase,
        "Python environment dependencies",
        "PASS" if python_imports.returncode == 0 else "FAIL",
        f"{sys.executable}: required imports available"
        if python_imports.returncode == 0
        else (
            f"{sys.executable}: missing project dependencies; run "
            f"'{sys.executable} -m pip install -r requirements.txt'\n"
            f"{python_imports.stderr.strip()}"
        ),
    )

    worker_pool_cli = context.command(
        ["gcloud", "alpha", "run", "worker-pools", "--help"], check=False
    )
    context.add(
        phase,
        "Cloud Run Worker Pool CLI",
        "PASS" if worker_pool_cli.returncode == 0 else "FAIL",
        "gcloud alpha run worker-pools is available"
        if worker_pool_cli.returncode == 0
        else "install the gcloud alpha component or update gcloud before deployment",
    )

    def configured_project():
        actual = context.command(["gcloud", "config", "get-value", "project"]).stdout.strip()
        if actual != context.project_id:
            raise RuntimeError(f"gcloud project is '{actual}', expected '{context.project_id}'")
        return actual

    context.check(phase, "gcloud project matches", configured_project)

    def active_account():
        account = context.command(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"]
        ).stdout.strip()
        if not account:
            raise RuntimeError("no active gcloud account")
        context.runtime["gcloud_account"] = account
        return account

    context.check(phase, "active account", active_account)

    def billing():
        data = context.json_command(
            ["gcloud", "billing", "projects", "describe", context.project_id, "--format=json"]
        )
        if not data.get("billingEnabled"):
            raise RuntimeError("billing is not enabled")
        return data.get("billingAccountName", "billing enabled")

    context.check(phase, "billing enabled", billing)

    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", context.project_id):
        context.add(phase, "project ID syntax", "FAIL", "project ID is not a valid GCP project ID")
    else:
        context.add(phase, "project ID syntax", "PASS", context.project_id)

    enabled = set(
        context.command(
            [
                "gcloud", "services", "list", "--enabled",
                f"--project={context.project_id}", "--format=value(config.name)",
            ]
        ).stdout.splitlines()
    )
    missing = sorted(set(REQUIRED_APIS) - enabled)
    if missing and enable_apis:
        result = context.command(
            ["gcloud", "services", "enable", *missing, f"--project={context.project_id}"],
            check=False,
        )
        if result.returncode == 0:
            context.add(phase, "required APIs", "PASS", f"enabled: {', '.join(missing)}")
        else:
            context.add(phase, "required APIs", "FAIL", result.stderr.strip())
    elif missing:
        context.add(phase, "required APIs", "FAIL", f"missing: {', '.join(missing)}")
    else:
        context.add(phase, "required APIs", "PASS", "all required APIs enabled")

    inventory_commands = {
        "instances_before": ["gcloud", "compute", "instances", "list", f"--project={context.project_id}", "--format=json"],
        "clusters_before": ["gcloud", "dataproc", "clusters", "list", f"--project={context.project_id}", f"--region={context.region}", "--format=json"],
        "disks_before": ["gcloud", "compute", "disks", "list", f"--project={context.project_id}", "--format=json"],
    }
    inventory = {}
    for key, command in inventory_commands.items():
        result = context.command(command, check=False)
        inventory[key] = json.loads(result.stdout or "[]") if result.returncode == 0 else {"error": result.stderr}
    context.report_dir.mkdir(parents=True, exist_ok=True)
    (context.report_dir / "inventory-before.json").write_text(
        json.dumps(inventory, indent=2) + "\n", encoding="utf-8"
    )
    context.add(phase, "initial resource inventory", "PASS", "saved inventory-before.json")

    return not any(result.phase == phase and result.status == "FAIL" for result in context.results)
