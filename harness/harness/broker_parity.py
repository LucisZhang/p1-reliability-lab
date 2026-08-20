from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

from harness.config import REPO_ROOT, Settings, load_settings
from harness.flink import (
    BATCH_SQL_CLASS,
    SNAPSHOT_LINK_CLASS,
    FlinkCommandError,
    cancel_job,
    compose_base,
    latest_completed_checkpoint,
    reset_iceberg_tables,
    run_java_class,
    running_job_ids,
    submit_job,
    submit_kafka_job,
)
from harness.generator import insert_events
from harness.provenance import STACK_VERSIONS, utc_now, write_result
from harness.sql import clean_sql_output, run_mysql_script

CURRENT_TABLE = "cdc_lab.orders_current"
CHANGELOG_TABLE = "cdc_lab.orders_changelog"
RESULT_PATH = REPO_ROOT / "showcase" / "results" / "broker_parity.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b1-broker-parity.log"
BROKER_STACK_VERSIONS = {
    **STACK_VERSIONS,
    "kafka": "3.9.2 (single-node KRaft)",
    "schema_registry": "Confluent 7.9.8",
    "debezium_connect": "3.2.7.Final",
    "flink_kafka_connector": "3.4.0-1.20",
    "broker_serialization": "Kafka Connect JSON schema envelope (B1)",
}

Row = dict[str, str]


def parse_topic_end_offsets(output: str, *, topic: str) -> list[dict[str, int]]:
    offsets: list[dict[str, int]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith(f"{topic}:"):
            continue
        fields = line.rsplit(":", 2)
        if len(fields) != 3:
            raise ValueError(f"malformed Kafka end-offset row: {line}")
        offsets.append({"partition": int(fields[1]), "log_end_offset": int(fields[2])})
    if not offsets:
        raise ValueError(f"no end offsets found for topic {topic!r}")
    return sorted(offsets, key=lambda item: item["partition"])


def parse_consumer_group_offsets(
    output: str,
    *,
    group: str,
    topic: str,
) -> list[dict[str, int]]:
    offsets: list[dict[str, int]] = []
    for raw_line in output.splitlines():
        fields = raw_line.split()
        if len(fields) < 6 or fields[0] != group or fields[1] != topic:
            continue
        if fields[3] == "-":
            raise ValueError(
                f"consumer group {group!r} has no committed offset for partition {fields[2]}"
            )
        offsets.append(
            {
                "partition": int(fields[2]),
                "current_offset": int(fields[3]),
                "log_end_offset": int(fields[4]),
                "lag": int(fields[5]),
            }
        )
    if not offsets:
        raise ValueError(f"no committed offsets found for group {group!r} and topic {topic!r}")
    return sorted(offsets, key=lambda item: item["partition"])


def row_diff(expected: Sequence[Row], actual: Sequence[Row]) -> dict[str, list[Row]]:
    expected_set = {json.dumps(row, sort_keys=True) for row in expected}
    actual_set = {json.dumps(row, sort_keys=True) for row in actual}
    return {
        "missing_from_actual": [
            cast(Row, json.loads(row)) for row in sorted(expected_set - actual_set)
        ],
        "unexpected_in_actual": [
            cast(Row, json.loads(row)) for row in sorted(actual_set - expected_set)
        ],
    }


def diff_count(diff: dict[str, list[Row]]) -> int:
    return sum(len(rows) for rows in diff.values())


def snapshot_digest(rows: Sequence[Row]) -> str:
    payload = json.dumps(list(rows), separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def linkage_complete(linkage: dict[str, object]) -> bool:
    offsets = linkage.get("kafka_offsets")
    checkpoint = linkage.get("flink_checkpoint")
    snapshots = linkage.get("iceberg_snapshot_ids")
    return (
        isinstance(offsets, list)
        and len(offsets) > 0
        and all(isinstance(item, dict) and item.get("lag") == 0 for item in offsets)
        and isinstance(checkpoint, dict)
        and isinstance(checkpoint.get("id"), int)
        and isinstance(snapshots, dict)
        and isinstance(snapshots.get("orders_current"), int)
        and isinstance(snapshots.get("orders_changelog"), int)
    )


def run_broker_parity(
    *,
    events: int,
    seed: int,
    timeout_seconds: int,
    checkpoint_interval_ms: int,
) -> dict[str, object]:
    if events < 1:
        raise ValueError("--events must be positive")
    if RESULT_PATH.exists():
        raise FileExistsError(
            f"append-only result already exists: {RESULT_PATH}; use a fresh checkout/run volume"
        )

    settings = load_settings()
    started_at = utc_now()
    log_lines: list[str] = []
    active_jobs: list[str] = []
    external_log = os.environ.get("P1_RESULT_LOGS", "").strip()
    logs_path = external_log or DEFAULT_LOG_PATH

    def log(message: str) -> None:
        line = f"{utc_now()} {message}"
        log_lines.append(line)
        print(line, flush=True)

    try:
        log("checking broker profile services and fresh topic state")
        _require_broker_services(settings)
        initial_offsets = _topic_end_offsets(settings)
        if sum(item["log_end_offset"] for item in initial_offsets) != 0:
            raise RuntimeError(
                "broker parity requires a fresh Kafka topic; run `make down` before broker-up"
            )

        _cancel_existing_jobs(settings, log=log)
        _mysql("TRUNCATE TABLE orders;", settings)
        reset_iceberg_tables(settings=settings)

        log("Path A: submitting the unchanged embedded MySQL CDC job")
        path_a_job = submit_job(
            settings=settings,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        active_jobs.append(path_a_job)
        _wait_for_job(path_a_job, settings=settings, timeout_seconds=120)

        log(f"generating deterministic workload events={events} seed={seed}")
        insert_events(events, seed, batch_size=min(1000, events), reset=False)
        _wait_until(
            lambda: len(_iceberg_rows(settings)) == events,
            description=f"Path A Iceberg row count {events}",
            timeout_seconds=timeout_seconds,
            interval=3,
        )
        source_rows = _mysql_rows(settings)
        path_a_rows = _iceberg_rows(settings)
        source_path_a_diff = row_diff(source_rows, path_a_rows)
        if diff_count(source_path_a_diff) != 0:
            raise RuntimeError(f"Path A source/Iceberg diff is non-zero: {source_path_a_diff}")
        path_a_checkpoint = _require_checkpoint(
            path_a_job, settings=settings, timeout_seconds=timeout_seconds
        )
        path_a_snapshots = _snapshot_ids(settings)
        path_a_changelog_count = _iceberg_scalar(
            f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings
        )
        if path_a_changelog_count != events:
            raise RuntimeError(
                f"Path A changelog count {path_a_changelog_count}, expected {events}"
            )
        log(
            "Path A converged: "
            f"checkpoint={path_a_checkpoint['id']} "
            f"snapshot={path_a_snapshots['orders_current']} rows={len(path_a_rows)}"
        )

        _wait_until(
            lambda: sum(item["log_end_offset"] for item in _topic_end_offsets(settings)) == events,
            description=f"Debezium topic end offset total {events}",
            timeout_seconds=timeout_seconds,
            interval=2,
        )
        topic_offsets_before_path_b = _topic_end_offsets(settings)

        cancel_job(path_a_job, settings=settings)
        active_jobs.remove(path_a_job)
        _wait_until(
            lambda: path_a_job not in running_job_ids(settings=settings),
            description=f"Path A job {path_a_job} cancellation",
            timeout_seconds=120,
        )
        reset_iceberg_tables(settings=settings)

        log("Path B: submitting the Kafka Debezium consumer from earliest offsets")
        path_b_job = submit_kafka_job(
            settings=settings,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        active_jobs.append(path_b_job)
        _wait_for_job(path_b_job, settings=settings, timeout_seconds=120)
        _wait_until(
            lambda: diff_count(row_diff(path_a_rows, _iceberg_rows(settings))) == 0,
            description="Path B final state to match the captured Path A final state",
            timeout_seconds=timeout_seconds,
            interval=3,
        )
        path_b_rows = _iceberg_rows(settings)
        path_a_path_b_diff = row_diff(path_a_rows, path_b_rows)
        source_path_b_diff = row_diff(source_rows, path_b_rows)
        path_b_checkpoint = _require_checkpoint(
            path_b_job, settings=settings, timeout_seconds=timeout_seconds
        )
        group_offsets = _wait_for_zero_group_lag(settings, timeout_seconds=timeout_seconds)
        path_b_snapshots = _snapshot_ids(settings)
        path_b_changelog_count = _iceberg_scalar(
            f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings
        )

        linkage: dict[str, object] = {
            "topic": settings.kafka_topic,
            "consumer_group": settings.kafka_consumer_group,
            "kafka_offsets": group_offsets,
            "flink_checkpoint": {
                "id": int(path_b_checkpoint["id"]),
                "status": path_b_checkpoint.get("status"),
                "external_path": path_b_checkpoint.get("external_path"),
            },
            "iceberg_snapshot_ids": path_b_snapshots,
            "observation": (
                "Kafka group offsets were read after this completed Flink checkpoint and the "
                "Iceberg snapshot IDs were read after row-level convergence."
            ),
        }

        parity_diff_count = diff_count(path_a_path_b_diff)
        source_path_b_diff_count = diff_count(source_path_b_diff)
        passed = (
            parity_diff_count == 0
            and source_path_b_diff_count == 0
            and len(path_a_rows) == events
            and len(path_b_rows) == events
            and path_a_changelog_count == events
            and path_b_changelog_count == events
            and linkage_complete(linkage)
        )
        if not passed:
            raise RuntimeError("broker parity assertions did not all pass")

        environment = collect_environment()
        payload: dict[str, object] = {
            "phase": "B1",
            "reader": "Flink SQL batch (Iceberg v2 equality-delete aware)",
            "claim_boundary": (
                "Parity only: unchanged Path A versus broker Path B; no broker fault claim."
            ),
            "environment": environment,
            "scenario": {
                "events": events,
                "seed": seed,
                "checkpoint_interval_ms": checkpoint_interval_ms,
                "topic": settings.kafka_topic,
                "topic_partitions": settings.kafka_topic_partitions,
                "topic_key": "orders.order_id primary key",
                "initial_topic_end_offsets": initial_offsets,
                "topic_end_offsets_before_path_b": topic_offsets_before_path_b,
            },
            "path_a": {
                "job_class": "com.p1.reliability.cdc.CdcToIcebergJob",
                "job_id": path_a_job,
                "delivery_chain": "MySQL binlog -> embedded Debezium -> Flink -> Iceberg",
                "checkpoint": {
                    "id": int(path_a_checkpoint["id"]),
                    "external_path": path_a_checkpoint.get("external_path"),
                },
                "iceberg_snapshot_ids": path_a_snapshots,
                "row_count": len(path_a_rows),
                "changelog_row_count": path_a_changelog_count,
                "snapshot_sha256": snapshot_digest(path_a_rows),
                "source_diff_count": diff_count(source_path_a_diff),
            },
            "path_b": {
                "job_class": "com.p1.reliability.cdc.KafkaToIcebergJob",
                "job_id": path_b_job,
                "delivery_chain": (
                    "MySQL GTID -> standalone Debezium at-least-once -> Kafka offsets -> "
                    "Flink checkpoint -> Iceberg keyed upsert"
                ),
                "row_count": len(path_b_rows),
                "changelog_row_count": path_b_changelog_count,
                "snapshot_sha256": snapshot_digest(path_b_rows),
            },
            "parity": {
                "row_level_diff_count": parity_diff_count,
                "path_a_path_b_diff": path_a_path_b_diff,
                "source_path_b_diff_count": source_path_b_diff_count,
                "source_path_b_diff": source_path_b_diff,
                "snapshot_digests_match": snapshot_digest(path_a_rows)
                == snapshot_digest(path_b_rows),
            },
            "offset_checkpoint_snapshot_linkage": linkage,
            "summary": {
                "passed": passed,
                "path_a_path_b_row_level_diff_zero": parity_diff_count == 0,
                "source_path_b_row_level_diff_zero": source_path_b_diff_count == 0,
                "kafka_lag_zero": all(item["lag"] == 0 for item in group_offsets),
                "linkage_complete": linkage_complete(linkage),
            },
        }
        log(
            "broker parity assertions passed: "
            f"row_level_diff_count={parity_diff_count} linkage_complete=true"
        )
        _write_internal_log(log_lines, logs_path=logs_path, external=bool(external_log))
        write_result(
            RESULT_PATH,
            payload=payload,
            command=os.environ.get("P1_RESULT_COMMAND", "make broker-verify"),
            logs=logs_path,
            started_at=started_at,
            finished_at=utc_now(),
            stack_versions=BROKER_STACK_VERSIONS,
        )
        return {**payload, "result_path": str(RESULT_PATH.relative_to(REPO_ROOT))}
    except Exception:
        _write_internal_log(log_lines, logs_path=logs_path, external=bool(external_log))
        raise
    finally:
        for job_id in list(active_jobs):
            try:
                cancel_job(job_id, settings=settings)
            except Exception as exc:
                print(f"warning: failed to cancel Flink job {job_id}: {exc}", file=sys.stderr)


def _require_broker_services(settings: Settings) -> None:
    proc = subprocess.run(
        [*compose_base(settings), "ps", "--status", "running", "--services"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())
    services = set(proc.stdout.split())
    required = {
        "mysql",
        "minio",
        "jobmanager",
        "taskmanager",
        "kafka",
        "schema-registry",
        "debezium",
    }
    missing = sorted(required - services)
    if missing:
        raise FlinkCommandError(f"broker services are not running: {', '.join(missing)}")


def _cancel_existing_jobs(settings: Settings, *, log: Callable[[str], None]) -> None:
    for job_id in running_job_ids(settings=settings):
        log(f"cancelling pre-existing Flink job {job_id}")
        cancel_job(job_id, settings=settings)


def _wait_for_job(job_id: str, *, settings: Settings, timeout_seconds: int) -> None:
    _wait_until(
        lambda: job_id in running_job_ids(settings=settings),
        description=f"Flink job {job_id} running",
        timeout_seconds=timeout_seconds,
    )


def _require_checkpoint(
    job_id: str,
    *,
    settings: Settings,
    timeout_seconds: int,
) -> dict[str, Any]:
    checkpoint: dict[str, Any] | None = None

    def ready() -> bool:
        nonlocal checkpoint
        checkpoint = latest_completed_checkpoint(job_id, settings=settings)
        return checkpoint is not None and isinstance(checkpoint.get("id"), int)

    _wait_until(
        ready,
        description=f"completed checkpoint for job {job_id}",
        timeout_seconds=timeout_seconds,
    )
    if checkpoint is None:
        raise TimeoutError(f"no completed checkpoint for job {job_id}")
    return checkpoint


def _topic_end_offsets(settings: Settings) -> list[dict[str, int]]:
    proc = subprocess.run(
        [
            *compose_base(settings),
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            settings.kafka_topic,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return parse_topic_end_offsets(proc.stdout, topic=settings.kafka_topic)


def _consumer_group_offsets(settings: Settings) -> list[dict[str, int]]:
    proc = subprocess.run(
        [
            *compose_base(settings),
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--describe",
            "--group",
            settings.kafka_consumer_group,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return parse_consumer_group_offsets(
        proc.stdout,
        group=settings.kafka_consumer_group,
        topic=settings.kafka_topic,
    )


def _wait_for_zero_group_lag(
    settings: Settings,
    *,
    timeout_seconds: int,
) -> list[dict[str, int]]:
    offsets: list[dict[str, int]] = []

    def ready() -> bool:
        nonlocal offsets
        offsets = _consumer_group_offsets(settings)
        return len(offsets) == settings.kafka_topic_partitions and all(
            item["lag"] == 0 for item in offsets
        )

    _wait_until(
        ready,
        description="Kafka consumer-group committed lag to reach zero",
        timeout_seconds=timeout_seconds,
        interval=2,
    )
    return offsets


def _mysql(script: str, settings: Settings) -> None:
    proc = run_mysql_script(script, settings=settings, capture=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())


def _mysql_rows(settings: Settings) -> list[Row]:
    proc = run_mysql_script(
        """
        SELECT order_id, business_key, event_id, customer_id, status, amount_cents,
               DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f'), seed
          FROM orders
         ORDER BY order_id;
        """,
        settings=settings,
        capture=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return [_row_dict(line.split("\t")) for line in proc.stdout.splitlines() if line.strip()]


def _iceberg_rows(settings: Settings) -> list[Row]:
    output = _iceberg_query(
        f"""
        SELECT order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed
          FROM {CURRENT_TABLE}
         ORDER BY order_id
        """,
        settings,
    )
    return [_row_dict(line.split("\t")) for line in output.splitlines() if line.strip()]


def _iceberg_scalar(query: str, settings: Settings) -> int:
    output = _iceberg_query(query, settings)
    lines = [line for line in output.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"query returned no rows: {query}")
    return int(lines[-1].split("\t")[0])


def _iceberg_query(query: str, settings: Settings) -> str:
    proc = run_java_class(BATCH_SQL_CLASS, ["--query", query], settings=settings)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return clean_sql_output(proc.stdout)


def _snapshot_ids(settings: Settings) -> dict[str, int]:
    proc = run_java_class(SNAPSHOT_LINK_CLASS, [], settings=settings)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    lines = [line for line in clean_sql_output(proc.stdout).splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"could not parse Iceberg snapshot IDs: {proc.stdout}")
    payload = json.loads(lines[-1])
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected snapshot-link payload: {payload}")
    return {
        "orders_current": int(payload["orders_current"]),
        "orders_changelog": int(payload["orders_changelog"]),
    }


def _row_dict(fields: list[str]) -> Row:
    if len(fields) != 8:
        raise ValueError(f"expected 8 fields, got {len(fields)}: {fields}")
    return {
        "order_id": fields[0],
        "business_key": fields[1],
        "event_id": fields[2],
        "customer_id": fields[3],
        "status": fields[4],
        "amount_cents": fields[5],
        "updated_at": _normalize_timestamp(fields[6]),
        "seed": fields[7],
    }


def _normalize_timestamp(value: str) -> str:
    if "." not in value:
        return f"{value}.000"
    head, fraction = value.split(".", 1)
    return f"{head}.{fraction[:3].ljust(3, '0')}"


def _wait_until(
    predicate: Callable[[], bool],
    *,
    description: str,
    timeout_seconds: int,
    interval: float = 2,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    if last_error is not None:
        raise TimeoutError(f"timed out waiting for {description}; last error: {last_error}")
    raise TimeoutError(f"timed out waiting for {description}")


def collect_environment() -> dict[str, object]:
    cpu_model = platform.processor() or "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name") and ":" in line:
                cpu_model = line.split(":", 1)[1].strip()
                break

    total_memory_bytes = 0
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                total_memory_bytes = int(line.split()[1]) * 1024
                break
    if total_memory_bytes == 0:
        total_memory_bytes = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))

    os_name = platform.platform()
    os_release = Path("/etc/os-release")
    if os_release.exists():
        for line in os_release.read_text(encoding="utf-8").splitlines():
            if line.startswith("PRETTY_NAME="):
                os_name = line.split("=", 1)[1].strip().strip('"')
                break

    docker = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if docker.returncode != 0:
        raise RuntimeError(docker.stderr.strip() or docker.stdout.strip())
    return {
        "hostname": socket.gethostname(),
        "cpu": {"model": cpu_model, "logical_count": os.cpu_count()},
        "ram_bytes": total_memory_bytes,
        "os": os_name,
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "docker_version": docker.stdout.strip(),
        "load_average_at_capture": list(os.getloadavg()),
        "execution": (
            "dedicated Docker-capable Linux VM; CPU-only; " "no GPU or co-tenant workloads"
        ),
    }


def _write_internal_log(lines: Sequence[str], *, logs_path: str, external: bool) -> None:
    if external:
        return
    path = REPO_ROOT / logs_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run Phase B1 Path A/Path B broker parity.")
    parser.add_argument("--events", type=int, default=100 if smoke else 1000)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000 if smoke else 5000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_broker_parity(
            events=args.events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
        )
    except Exception as exc:
        print(f"broker-verify failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
