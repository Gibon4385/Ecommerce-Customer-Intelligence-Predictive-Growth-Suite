#!/usr/bin/env python3
"""Run a named load scenario against the production Kafka producer code path."""

import argparse
import datetime as dt
import os
import subprocess
import sys
import uuid
from pathlib import Path

import yaml


LOAD_DIR = Path(__file__).resolve().parent
REPO_ROOT = LOAD_DIR.parents[1]
DEFAULT_SCENARIOS = LOAD_DIR / "scenarios.yaml"
DEFAULT_RESULTS = LOAD_DIR / "results"


def load_scenario(path: Path, name: str) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    scenarios = document.get("scenarios", {})
    if name not in scenarios:
        available = ", ".join(sorted(scenarios)) or "none"
        raise ValueError(f"Unknown scenario '{name}'. Available scenarios: {available}")
    return scenarios[name]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--scenarios-file", type=Path, default=DEFAULT_SCENARIOS)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--no-validate", action="store_true")
    args = parser.parse_args()

    try:
        scenario = load_scenario(args.scenarios_file, args.scenario)
    except (OSError, ValueError, yaml.YAMLError) as error:
        parser.error(str(error))

    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{args.scenario}-{timestamp}-{uuid.uuid4().hex[:8]}"
    args.results_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.results_dir / f"{run_id}.json"

    command = [
        args.python,
        str(REPO_ROOT / "scripts" / "run_kafka_producer.py"),
        "--bootstrap-servers", args.bootstrap_servers,
        "--target-eps", str(scenario["target_eps"]),
        "--duration-seconds", str(scenario["duration_seconds"]),
        "--run-id", run_id,
        "--metrics-output", str(summary_path),
        "--seed", str(scenario.get("seed", 42)),
        "--log-level", "INFO",
    ]

    print(f"Running scenario '{args.scenario}' as {run_id}", flush=True)
    print(f"Summary: {summary_path}", flush=True)
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
    if completed.returncode != 0:
        return completed.returncode

    if args.no_validate:
        return 0

    return subprocess.run(
        [
            args.python,
            str(LOAD_DIR / "validate_results.py"),
            "--scenario", args.scenario,
            "--summary", str(summary_path),
            "--scenarios-file", str(args.scenarios_file),
        ],
        cwd=REPO_ROOT,
        check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
