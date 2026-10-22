#!/usr/bin/env python3
"""Validate producer results and optional end-to-end Cloud Monitoring observations."""

import argparse
import json
from pathlib import Path

import yaml


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--summary", required=True, type=Path)
    parser.add_argument("--scenarios-file", type=Path, default=Path(__file__).with_name("scenarios.yaml"))
    parser.add_argument(
        "--observations",
        type=Path,
        help="Optional JSON exported from Cloud Monitoring; see observations.example.json",
    )
    args = parser.parse_args()

    scenarios = yaml.safe_load(args.scenarios_file.read_text(encoding="utf-8"))["scenarios"]
    if args.scenario not in scenarios:
        parser.error(f"Scenario '{args.scenario}' is not defined")
    scenario = scenarios[args.scenario]
    thresholds = scenario.get("thresholds", {})
    summary = read_json(args.summary)
    observations = read_json(args.observations) if args.observations else None

    checks = []
    minimum_eps = scenario["target_eps"] * thresholds.get("min_eps_ratio", 0)
    checks.append(("producer actual EPS", summary["actual_eps"] >= minimum_eps, summary["actual_eps"], f">= {minimum_eps:.2f}"))
    checks.append(("producer delivery errors", summary["delivery_errors"] <= thresholds.get("max_delivery_errors", 0), summary["delivery_errors"], f"<= {thresholds.get('max_delivery_errors', 0)}"))

    observation_map = {
        "max_gcs_freshness_seconds": "gcs_freshness_seconds",
        "max_spark_failed_batches": "spark_failed_batches",
        "max_recovery_seconds": "recovery_seconds",
    }
    required_observations = [key for key in observation_map if key in thresholds]
    if observations:
        for threshold_key in required_observations:
            observed_key = observation_map[threshold_key]
            observed = observations.get(observed_key)
            limit = thresholds[threshold_key]
            checks.append((observed_key, observed is not None and observed <= limit, observed, f"<= {limit}"))

    failed = False
    for name, passed, actual, expected in checks:
        status = "PASS" if passed else "FAIL"
        print(f"{status:4}  {name}: actual={actual}, expected={expected}")
        failed = failed or not passed

    if required_observations and not observations:
        print("INFO  End-to-end checks were not evaluated; provide --observations after exporting the dashboard values.")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
