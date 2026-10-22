"""Explicit, project-scoped cleanup with dry-run and double-confirmation guards."""

from __future__ import annotations

from pathlib import Path
import time

from checks.common import Context, wait_until
from checks.infrastructure import terraform_args
from checks.remaining_resources import run as audit_resources


DBT_DATASETS = (
    "raw_ecommerce",
    "dbt_ecommerce_staging",
    "dbt_ecommerce_intermediate",
    "dbt_ecommerce_marts",
    # Remove datasets created by older dbt/Fusion schema routing during E2E.
    "staging",
    "intermediate",
    "marts",
)


def _execute(context: Context, name: str, command: list[str], *, execute: bool, cwd: Path | None = None):
    phase = "cleanup"
    if not execute:
        context.add(phase, name, "SKIP", "DRY RUN: " + " ".join(command))
        return
    result = context.command(command, cwd=cwd, check=False, timeout=1800)
    context.add(
        phase,
        name,
        "PASS" if result.returncode == 0 else "FAIL",
        (result.stdout or result.stderr).strip()[-1000:] or f"exit={result.returncode}",
    )
    return result


def run(
    context: Context,
    *,
    execute: bool,
    confirmed_project_id: str,
    delete_project: bool = False,
):
    if confirmed_project_id != context.project_id:
        raise RuntimeError("--confirm-project-id must exactly match project_id")

    project = context.project_id
    region = context.region
    job_id = context.runtime.get("spark_job_id")
    if job_id:
        job_state = context.command(
            ["gcloud", "dataproc", "jobs", "describe", job_id, f"--region={region}", f"--project={project}", "--format=value(status.state)"],
            check=False,
        ).stdout.strip()
        if job_state in {"PENDING", "SETUP_DONE", "RUNNING"}:
            _execute(
                context,
                "kill Spark job",
                ["gcloud", "dataproc", "jobs", "kill", job_id, f"--region={region}", f"--project={project}", "--quiet"],
                execute=execute,
            )
        else:
            context.add(
                "cleanup",
                "kill Spark job",
                "PASS",
                f"job is not active ({job_state or 'absent'})",
            )
    worker_pool = context.command(
        [
            "gcloud", "alpha", "run", "worker-pools", "describe", "ecommerce-producer",
            f"--region={region}", f"--project={project}",
        ],
        check=False,
    )
    if worker_pool.returncode == 0:
        _execute(
            context,
            "delete Producer Worker Pool",
            ["gcloud", "alpha", "run", "worker-pools", "delete", "ecommerce-producer", f"--region={region}", f"--project={project}", "--quiet"],
            execute=execute,
        )
    else:
        context.add(
            "cleanup",
            "delete Producer Worker Pool",
            "PASS",
            "worker pool is absent",
        )

    if execute:
        subnet_name = f"ecommerce-streaming-{region}"
        allocation_poll_count = 0

        def serverless_address_released():
            nonlocal allocation_poll_count
            allocation_poll_count += 1
            addresses = context.command(
                [
                    "gcloud", "compute", "addresses", "list",
                    f"--project={project}", f"--regions={region}",
                    "--filter=purpose=SERVERLESS",
                    "--format=value(name,subnetwork)",
                ],
                check=False,
            )
            if addresses.returncode != 0:
                return False
            allocation_present = any(
                subnet_name in line for line in addresses.stdout.splitlines()
            )
            if allocation_present and allocation_poll_count % 4 == 0:
                print(
                    "WAIT [cleanup] Cloud Run serverless network allocation "
                    f"still exists for {subnet_name}",
                    flush=True,
                )
            return not allocation_present

        context.check(
            "cleanup",
            "Cloud Run serverless network allocation released",
            lambda: wait_until(
                "Cloud Run serverless network allocation release",
                600,
                15,
                serverless_address_released,
            ),
        )

    for dataset in DBT_DATASETS:
        _execute(
            context,
            f"delete BigQuery dataset:{dataset}",
            ["bq", "rm", "-r", "-f", f"{project}:{dataset}"],
            execute=execute,
        )

    managed_buckets = (f"{project}-ecommerce-lake", f"{project}-dbt-artifacts")
    cleanup_buckets = (*managed_buckets, f"{project}_cloudbuild")
    for bucket in cleanup_buckets:
        bucket_exists = context.command(
            ["gcloud", "storage", "buckets", "describe", f"gs://{bucket}"], check=False
        )
        if bucket_exists.returncode == 0:
            _execute(
                context,
                f"delete bucket and all object versions:{bucket}",
                ["gcloud", "storage", "rm", "--recursive", f"gs://{bucket}/"],
                execute=execute,
            )
        else:
            context.add("cleanup", f"delete bucket and all object versions:{bucket}", "PASS", "bucket is absent")

    # Container packages make Artifact Registry repositories non-empty and can
    # block Terraform deletion, so remove packages before destroy.
    packages = context.command(
        ["gcloud", "artifacts", "packages", "list", "--repository=ecommerce-streaming", f"--location={region}", f"--project={project}", "--format=value(name)"],
        check=False,
    ).stdout.splitlines()
    for package in packages:
        _execute(
            context,
            f"delete Artifact package:{package}",
            ["gcloud", "artifacts", "packages", "delete", package, "--repository=ecommerce-streaming", f"--location={region}", f"--project={project}", "--quiet"],
            execute=execute,
        )

    if execute:
        destroy_outputs = []
        destroy = None
        for attempt in range(1, 4):
            destroy = context.command(
                ["terraform", "destroy", "-input=false", "-auto-approve", *terraform_args(context)],
                cwd=context.repo_root / "terraform",
                check=False,
                timeout=1800,
            )
            destroy_outputs.append(
                f"===== ATTEMPT {attempt} =====\n{destroy.stdout}{destroy.stderr}"
            )
            if destroy.returncode == 0:
                break
            if "resourceInUseByAnotherResource" not in (destroy.stdout + destroy.stderr):
                break
            time.sleep(30)
        (context.report_dir / "terraform-destroy.log").write_text(
            "\n".join(destroy_outputs), encoding="utf-8"
        )
        context.add(
            "cleanup",
            "terraform destroy",
            "PASS" if destroy and destroy.returncode == 0 else "FAIL",
            (destroy.stdout or destroy.stderr).strip()[-1000:]
            if destroy
            else "terraform destroy was not executed",
        )
    else:
        _execute(
            context,
            "terraform destroy",
            ["terraform", "destroy", "-input=false", "-auto-approve", *terraform_args(context)],
            execute=False,
            cwd=context.repo_root / "terraform",
        )

    if delete_project:
        _execute(
            context,
            "delete entire GCP project",
            ["gcloud", "projects", "delete", project, "--quiet"],
            execute=execute,
        )
        return

    if execute:
        audit_resources(context, expect_empty=True)
