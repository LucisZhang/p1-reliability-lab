from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import uuid
from collections.abc import Sequence
from typing import Any, cast
from urllib.request import urlopen

from harness.config import REPO_ROOT, Settings, load_settings

JOB_JAR = REPO_ROOT / "flink-jobs" / "target" / "cdc-to-iceberg.jar"
REMOTE_JOB_JAR = "/opt/flink/cdc-to-iceberg.jar"
JOB_MAIN_CLASS = "com.p1.reliability.cdc.CdcToIcebergJob"
KAFKA_JOB_MAIN_CLASS = "com.p1.reliability.cdc.KafkaToIcebergJob"
BATCH_SQL_CLASS = "com.p1.reliability.cdc.IcebergBatchSql"
ADMIN_CLASS = "com.p1.reliability.cdc.IcebergAdmin"
SNAPSHOT_LINK_CLASS = "com.p1.reliability.cdc.IcebergSnapshotLink"


class FlinkCommandError(RuntimeError):
    pass


def compose_base(settings: Settings) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(settings.env_file),
        "-f",
        str(settings.compose_file),
    ]


def container_job_args(
    settings: Settings, *, checkpoint_interval_ms: int | None = None
) -> list[str]:
    args = [
        "--mysql-host",
        "mysql",
        "--mysql-port",
        "3306",
        "--mysql-database",
        settings.mysql_database,
        "--mysql-user",
        settings.mysql_user,
        "--mysql-password",
        settings.mysql_password,
        "--iceberg-catalog-name",
        settings.iceberg_catalog_name,
        "--iceberg-catalog-database",
        settings.iceberg_catalog_database,
        "--iceberg-database",
        settings.mysql_database,
        "--iceberg-warehouse",
        settings.iceberg_warehouse,
        "--s3-endpoint",
        settings.minio_docker_endpoint,
        "--s3-access-key",
        settings.minio_root_user,
        "--s3-secret-key",
        settings.minio_root_password,
        "--s3-region",
        settings.minio_region,
    ]
    if checkpoint_interval_ms is not None:
        args.extend(["--checkpoint-interval-ms", str(checkpoint_interval_ms)])
    return args


def copy_job_jar(settings: Settings, remote_path: str) -> None:
    if not JOB_JAR.exists():
        raise FileNotFoundError(f"{JOB_JAR} does not exist; run make build-flink first")
    proc = subprocess.run(
        [*compose_base(settings), "cp", str(JOB_JAR), f"jobmanager:{remote_path}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())


def ensure_remote_job_jar(settings: Settings | None = None) -> None:
    active = settings or load_settings()
    copy_job_jar(active, REMOTE_JOB_JAR)


def run_jobmanager(
    args: list[str],
    *,
    settings: Settings | None = None,
    capture: bool = True,
    timeout_seconds: int | None = None,
) -> subprocess.CompletedProcess[str]:
    active = settings or load_settings()
    return subprocess.run(
        [*compose_base(active), "exec", "-T", "jobmanager", *args],
        check=False,
        stderr=subprocess.PIPE if capture else None,
        stdout=subprocess.PIPE if capture else None,
        text=True,
        timeout=timeout_seconds,
    )


def run_java_class(
    class_name: str,
    extra_args: list[str],
    *,
    settings: Settings | None = None,
) -> subprocess.CompletedProcess[str]:
    active = settings or load_settings()
    remote_jar = f"/tmp/cdc-to-iceberg-{uuid.uuid4().hex}.jar"
    copy_job_jar(active, remote_jar)
    return run_jobmanager(
        [
            "java",
            "-cp",
            f"{remote_jar}:/opt/flink/lib/*",
            class_name,
            *container_job_args(active),
            *extra_args,
        ],
        settings=active,
    )


def submit_job(
    *,
    settings: Settings | None = None,
    savepoint: str | None = None,
    checkpoint_interval_ms: int | None = None,
    extra_job_args: Sequence[str] | None = None,
    main_class: str = JOB_MAIN_CLASS,
) -> str:
    active = settings or load_settings()
    ensure_remote_job_jar(active)
    command = ["flink", "run"]
    if savepoint:
        command.extend(["-s", savepoint])
    command.extend(
        [
            "-d",
            "-c",
            main_class,
            REMOTE_JOB_JAR,
            *container_job_args(active, checkpoint_interval_ms=checkpoint_interval_ms),
            *(list(extra_job_args) if extra_job_args else []),
        ]
    )
    proc = run_jobmanager(command, settings=active)
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())
    match = re.search(r"JobID\s+([a-f0-9]{32})", proc.stdout)
    if not match:
        match = re.search(r"Job has been submitted with JobID\s+([a-f0-9]{32})", proc.stdout)
    if not match:
        raise FlinkCommandError(f"could not parse submitted job id from: {proc.stdout.strip()}")
    return match.group(1)


def submit_kafka_job(
    *,
    settings: Settings | None = None,
    checkpoint_interval_ms: int | None = None,
) -> str:
    active = settings or load_settings()
    return submit_job(
        settings=active,
        checkpoint_interval_ms=checkpoint_interval_ms,
        main_class=KAFKA_JOB_MAIN_CLASS,
        extra_job_args=[
            "--kafka-bootstrap-servers",
            active.kafka_bootstrap_servers,
            "--kafka-topic",
            active.kafka_topic,
            "--kafka-group-id",
            active.kafka_consumer_group,
            "--schema-registry-url",
            active.schema_registry_docker_url,
        ],
    )


def running_job_ids(*, settings: Settings | None = None) -> list[str]:
    active = settings or load_settings()
    proc = run_jobmanager(["flink", "list", "-r"], settings=active)
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())
    ids: list[str] = []
    for line in proc.stdout.splitlines():
        match = re.search(r": ([a-f0-9]{32}) : ", line)
        if match:
            ids.append(match.group(1))
    return ids


def cancel_job(job_id: str, *, settings: Settings | None = None) -> None:
    active = settings or load_settings()
    proc = run_jobmanager(["flink", "cancel", job_id], settings=active)
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())


def savepoint(
    job_id: str, *, settings: Settings | None = None, timeout_seconds: int | None = None
) -> str:
    active = settings or load_settings()
    proc = run_jobmanager(
        ["flink", "savepoint", job_id, "/opt/flink/savepoints"],
        settings=active,
        timeout_seconds=timeout_seconds,
    )
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())
    combined = f"{proc.stdout}\n{proc.stderr}"
    match = re.search(r"Path:\s*(\S+)", combined)
    if not match:
        print(combined, file=sys.stderr)
        raise FlinkCommandError("could not parse savepoint path")
    return match.group(1)


def flink_rest_json(path: str, *, settings: Settings | None = None) -> dict[str, Any]:
    active = settings or load_settings()
    url = f"http://{active.flink_jobmanager_host}:{active.flink_rest_port}{path}"
    with urlopen(url, timeout=10) as response:
        return cast(dict[str, Any], json.loads(response.read().decode("utf-8")))


def latest_completed_checkpoint(
    job_id: str, *, settings: Settings | None = None
) -> dict[str, Any] | None:
    payload = flink_rest_json(f"/jobs/{job_id}/checkpoints", settings=settings)
    latest = payload.get("latest")
    if isinstance(latest, dict):
        completed = latest.get("completed")
        if isinstance(completed, dict) and completed.get("external_path"):
            return completed

    history = payload.get("history")
    if isinstance(history, list):
        for item in history:
            if (
                isinstance(item, dict)
                and item.get("status") == "COMPLETED"
                and item.get("external_path")
            ):
                return item
    return None


def reset_iceberg_tables(*, settings: Settings | None = None) -> None:
    active = settings or load_settings()
    proc = run_java_class(ADMIN_CLASS, ["reset-tables"], settings=active)
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flink job helpers for the reliability lab.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit = subparsers.add_parser("submit", description="Submit the CDC to Iceberg job.")
    submit.add_argument("--savepoint")
    submit.add_argument("--checkpoint-interval-ms", type=int)

    subparsers.add_parser("reset-iceberg", description="Drop Phase 1.2 Iceberg tables.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "submit":
            job_id = submit_job(
                savepoint=args.savepoint,
                checkpoint_interval_ms=args.checkpoint_interval_ms,
            )
            print(job_id)
            return 0
        if args.command == "reset-iceberg":
            reset_iceberg_tables()
            return 0
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
