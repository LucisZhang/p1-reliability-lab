from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from harness.config import REPO_ROOT

STACK_VERSIONS = {
    "java": "11",
    "maven": "3.9",
    "python": "3.11",
    "node": "20",
    "mysql": "8.0.36",
    "flink": "1.20.4",
    "flink_cdc": "3.6.0",
    "iceberg": "1.10.x",
    "minio": "RELEASE.2025-04-22T22-12-26Z",
    "starrocks": "3.3.x (M3+)",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def git_sha() -> str:
    override = os.environ.get("P1_PROVENANCE_GIT_SHA", "").strip()
    if override:
        if not re.fullmatch(r"[0-9a-f]{7,40}", override):
            raise ValueError("P1_PROVENANCE_GIT_SHA must be a 7-40 character lowercase hex SHA")
        return override
    proc = subprocess.run(
        ["git", "rev-parse", "--short=12", "HEAD"],
        cwd=REPO_ROOT,
        check=False,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        return "NO_GIT_REPO"
    return proc.stdout.strip()


@dataclass(frozen=True)
class Provenance:
    run_id: str
    git_sha: str
    started_at: str
    finished_at: str
    stack_versions: dict[str, str]
    command: str
    logs: str

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "git_sha": self.git_sha,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stack_versions": self.stack_versions,
            "command": self.command,
            "logs": self.logs,
        }


def build_provenance(
    *,
    command: str,
    logs: str,
    started_at: str | None = None,
    finished_at: str | None = None,
    stack_versions: dict[str, str] | None = None,
) -> Provenance:
    return Provenance(
        run_id=f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}",
        git_sha=git_sha(),
        started_at=started_at or utc_now(),
        finished_at=finished_at or utc_now(),
        stack_versions=stack_versions or STACK_VERSIONS,
        command=command,
        logs=logs,
    )


def write_result(
    output: Path,
    *,
    payload: dict[str, object],
    command: str,
    logs: str,
    started_at: str | None = None,
    finished_at: str | None = None,
    stack_versions: dict[str, str] | None = None,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    provenance = build_provenance(
        command=command,
        logs=logs,
        started_at=started_at,
        finished_at=finished_at,
        stack_versions=stack_versions,
    )
    result = {**provenance.as_dict(), **payload}
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit a provenance JSON envelope.")
    parser.add_argument("--command", required=True)
    parser.add_argument("--logs", required=True)
    args = parser.parse_args()
    print(json.dumps(build_provenance(command=args.command, logs=args.logs).as_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
