"""Shared command, result, and report utilities for cloud E2E tests."""

from __future__ import annotations

import json
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable


@dataclass
class CheckResult:
    phase: str
    name: str
    status: str
    detail: str
    duration_seconds: float = 0.0


class Context:
    def __init__(self, repo_root: Path, config: dict, report_dir: Path):
        self.repo_root = repo_root
        self.config = config
        self.report_dir = report_dir
        # Standalone cleanup writes terraform-destroy.log before the final
        # report is generated, so the run directory must exist immediately.
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.results: list[CheckResult] = []
        self.runtime: dict[str, str] = {}

    @property
    def project_id(self) -> str:
        return self.config["project_id"]

    @property
    def region(self) -> str:
        return self.config.get("region", "asia-east1")

    @property
    def zone(self) -> str:
        return self.config.get("zone", "asia-east1-b")

    def add(self, phase: str, name: str, status: str, detail: str, started: float = 0.0):
        duration = time.monotonic() - started if started else 0.0
        result = CheckResult(phase, name, status, detail, round(duration, 3))
        self.results.append(result)
        print(f"{status:4} [{phase}] {name}: {detail}", flush=True)
        return result

    def check(self, phase: str, name: str, operation: Callable[[], str]):
        started = time.monotonic()
        try:
            return self.add(phase, name, "PASS", operation(), started)
        except Exception as error:  # noqa: BLE001 - report every external failure
            return self.add(phase, name, "FAIL", str(error), started)

    def command(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        check: bool = True,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            args,
            cwd=cwd or self.repo_root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
        if check and completed.returncode != 0:
            output = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(args)}\n{output}")
        return completed

    def json_command(self, args: list[str], **kwargs):
        output = self.command(args, **kwargs).stdout.strip()
        return json.loads(output or "null")

    def write_report(self):
        self.report_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "project_id": self.project_id,
            "runtime": self.runtime,
            "summary": {
                status: sum(result.status == status for result in self.results)
                for status in ("PASS", "FAIL", "SKIP")
            },
            "results": [asdict(result) for result in self.results],
        }
        (self.report_dir / "report.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        lines = [
            f"# GCP E2E 測試報告：{self.project_id}",
            "",
            f"- PASS：{payload['summary']['PASS']}",
            f"- FAIL：{payload['summary']['FAIL']}",
            f"- SKIP：{payload['summary']['SKIP']}",
            "",
            "| 階段 | 檢查 | 狀態 | 說明 |",
            "|---|---|---|---|",
        ]
        for result in self.results:
            detail = result.detail.replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {result.phase} | {result.name} | {result.status} | {detail} |")
        (self.report_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def google_api_get(self, url: str):
        token = self.command(["gcloud", "auth", "print-access-token"]).stdout.strip()
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


def wait_until(description: str, timeout_seconds: int, interval_seconds: int, probe: Callable[[], bool]):
    deadline = time.monotonic() + timeout_seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            if probe():
                return description
        except Exception as error:  # noqa: BLE001 - retry transient cloud state
            last_error = error
        time.sleep(interval_seconds)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise TimeoutError(f"timed out waiting for {description}{suffix}")
