#!/usr/bin/env python3
"""Orchestrate deployment, cloud E2E validation, reporting, and cleanup."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import yaml


E2E_DIR = Path(__file__).resolve().parent
REPO_ROOT = E2E_DIR.parents[1]
sys.path.insert(0, str(E2E_DIR))

from checks import bigquery_dbt, infrastructure, monitoring, pipeline, preflight, security  # noqa: E402
from checks.common import Context  # noqa: E402
from checks.remaining_resources import run as audit_resources  # noqa: E402
from cleanup import run as cleanup_resources  # noqa: E402


def load_config(path: Path, project_override: str | None) -> dict:
    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if project_override:
        config["project_id"] = project_override
    if not config.get("project_id"):
        raise ValueError("config must define project_id or use --project-id")
    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("full", "deploy", "validate", "audit", "cleanup"))
    parser.add_argument("--config", type=Path, default=E2E_DIR / "config.example.yaml")
    parser.add_argument("--project-id")
    parser.add_argument("--confirm-project-id", default="")
    parser.add_argument("--enable-apis", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Execute cleanup; cleanup is dry-run without this flag")
    parser.add_argument("--keep-resources", action="store_true", help="Do not clean up after full; continues to incur cost")
    parser.add_argument("--acknowledge-cost", action="store_true")
    parser.add_argument("--delete-project", action="store_true")
    parser.add_argument("--confirm-delete-project", default="")
    args = parser.parse_args()

    try:
        config = load_config(args.config, args.project_id)
    except (OSError, ValueError, yaml.YAMLError) as error:
        parser.error(str(error))

    project_id = config["project_id"]
    destructive_command = args.command in {"full", "cleanup"} and not args.keep_resources
    if destructive_command and args.confirm_project_id != project_id:
        parser.error(f"--confirm-project-id must exactly equal {project_id}")
    if args.keep_resources and not args.acknowledge_cost:
        parser.error("--keep-resources requires --acknowledge-cost")
    if args.delete_project and args.confirm_delete_project != project_id:
        parser.error("--delete-project requires --confirm-delete-project with the full project ID")

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_dir = E2E_DIR / "reports" / run_id
    context = Context(REPO_ROOT, config, report_dir)
    context.runtime["run_id"] = run_id
    cleanup_needed = False

    try:
        if args.command == "cleanup":
            cleanup_resources(
                context,
                execute=args.execute,
                confirmed_project_id=args.confirm_project_id,
                delete_project=args.delete_project,
            )
        elif args.command == "audit":
            preflight.run(context, enable_apis=False)
            audit_resources(context, expect_empty=False)
        elif args.command == "deploy":
            if not args.acknowledge_cost:
                parser.error("deploy leaves resources running and requires --acknowledge-cost")
            if preflight.run(context, enable_apis=args.enable_apis):
                infrastructure.deploy(context)
        elif args.command == "validate":
            if preflight.run(context, enable_apis=False):
                terraform_outputs = context.json_command(
                    ["terraform", "output", "-json"], cwd=REPO_ROOT / "terraform"
                )
                context.runtime.update(
                    {key: str(value.get("value", "")) for key, value in terraform_outputs.items()}
                )
                security.run(context)
                pipeline.run(context)
                bigquery_dbt.run(context)
                monitoring.run(context)
        else:
            if preflight.run(context, enable_apis=args.enable_apis):
                cleanup_needed = True
                if infrastructure.deploy(context):
                    security.run(context)
                    pipeline.run(context)
                    bigquery_dbt.run(context)
                    monitoring.run(context)
    except KeyboardInterrupt:
        context.add("runner", "interrupted", "FAIL", "received Ctrl+C")
    except Exception as error:  # noqa: BLE001 - always reach cleanup/report
        context.add("runner", "unhandled error", "FAIL", repr(error))
    finally:
        if cleanup_needed and not args.keep_resources:
            try:
                cleanup_resources(
                    context,
                    execute=True,
                    confirmed_project_id=args.confirm_project_id,
                    delete_project=args.delete_project,
                )
            except Exception as error:  # noqa: BLE001
                context.add("cleanup", "unhandled cleanup error", "FAIL", repr(error))
        context.write_report()
        print(f"報告位置：{report_dir}")

    return 1 if any(result.status == "FAIL" for result in context.results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
