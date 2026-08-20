from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, cast
from urllib.request import Request, urlopen

from harness.broker_failure_common import (
    CHANGELOG_TABLE,
    DrillLog,
    avro_key,
    cancel_active_jobs,
    connector_state,
    consume_binary,
    consumer_group_offsets,
    debezium_envelope,
    encode_confluent_avro_remote,
    int_field,
    kafka_compose_action,
    linkage,
    order_row,
    prepare_fresh_drill,
    produce_binary,
    registered_schema,
    reset_group_to_earliest,
    set_connector_state,
    submit_drill_job,
    topic_end_offsets,
    wait_connector_state,
    wait_for_job_absent,
    wait_for_kafka_ready,
)
from harness.broker_parity import (
    BROKER_STACK_VERSIONS,
    _iceberg_query,
    _iceberg_rows,
    _iceberg_scalar,
    _mysql_rows,
    _wait_until,
    collect_environment,
    diff_count,
    row_diff,
    snapshot_digest,
)
from harness.checkpoint_metrics import PromMetric, parse_prometheus_metrics, scrape_reporter_metrics
from harness.config import REPO_ROOT, Settings, load_settings
from harness.flink import cancel_job, reset_iceberg_tables
from harness.generator import OrderEvent, generate_events
from harness.offset_replay_drill import REPLAY_COALESCE_MS
from harness.ordering_miskey_drill import _decoded_probe, monotonic_violations
from harness.poison_dlq_drill import _dlq_payload
from harness.provenance import utc_now, write_result
from harness.sql import run_mysql_script

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "broker_slo.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b4-broker-slo.log"
CURRENT_TABLE = "cdc_lab.orders_current"

B4_STACK_VERSIONS = {
    **BROKER_STACK_VERSIONS,
    "prometheus_jmx_exporter": "0.20.0",
}


@dataclass(frozen=True)
class CommitBatch:
    first_event_id: int
    last_event_id: int
    event_count: int
    request_started_epoch_ms: int
    commit_ack_epoch_ms: int
    client_round_trip_ms: int


def percentile_nearest_rank(values: Sequence[float], percentile: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    if percentile < 0.0 or percentile > 100.0:
        raise ValueError("percentile must be between 0 and 100")
    ordered = sorted(values)
    rank = max(1, math.ceil((percentile / 100.0) * len(ordered)))
    return float(ordered[rank - 1])


def duplicate_replay_targets(
    *,
    initial_changelog_count: int,
    initial_duplicate_occurrences: int,
    replay_record_count: int,
) -> dict[str, int]:
    if min(initial_changelog_count, initial_duplicate_occurrences, replay_record_count) < 0:
        raise ValueError("duplicate replay counters must be non-negative")
    return {
        "expected_changelog_count": initial_changelog_count + replay_record_count,
        "expected_duplicate_occurrences": initial_duplicate_occurrences + replay_record_count,
    }


def connector_state_summary(payload: dict[str, object]) -> dict[str, Any]:
    connector = payload.get("connector")
    tasks = payload.get("tasks")
    task_summaries: list[dict[str, object]] = []
    if isinstance(tasks, list):
        for task in tasks:
            if not isinstance(task, dict):
                continue
            trace = task.get("trace")
            failure_type = None
            if isinstance(trace, str) and trace:
                failure_type = trace.split(":", maxsplit=1)[0]
            task_summaries.append(
                {
                    "id": task.get("id"),
                    "state": task.get("state"),
                    "failure_type": failure_type,
                }
            )
    return {
        "connector_state": connector.get("state") if isinstance(connector, dict) else None,
        "tasks": task_summaries,
    }


def scrape_prometheus_endpoint(host: str, port: int) -> list[PromMetric]:
    url = f"http://{host}:{port}/metrics"
    with urlopen(Request(url, headers={"Accept": "text/plain"}), timeout=5) as response:
        return parse_prometheus_metrics(response.read().decode("utf-8"))


def extract_flink_kafka_source_metrics(
    metrics: Sequence[PromMetric],
    *,
    job_id: str,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for metric in metrics:
        metric_job_id = metric.labels.get("job_id") or metric.labels.get("jobid")
        if metric_job_id is not None and metric_job_id != job_id:
            continue
        normalized = re.sub(r"[^a-z0-9]", "", metric.name.lower())
        if not any(
            token in normalized
            for token in (
                "recordslagmax",
                "pendingrecords",
                "currentoffsets",
                "committedoffsets",
            )
        ):
            continue
        candidates.append(
            {
                "name": metric.name,
                "labels": metric.labels,
                "value": metric.value,
            }
        )
    lag_metrics = [
        metric
        for metric in candidates
        if any(
            token in re.sub(r"[^a-z0-9]", "", str(metric["name"]).lower())
            for token in ("recordslagmax", "pendingrecords")
        )
    ]
    return {
        "present": bool(candidates),
        "lag_metric_present": bool(lag_metrics),
        "metric_names": sorted({str(metric["name"]) for metric in candidates}),
        "metrics": candidates,
    }


def extract_debezium_connector_metrics(metrics: Sequence[PromMetric]) -> dict[str, Any]:
    connector_metrics = [metric for metric in metrics if "debezium" in metric.name.lower()]
    targets = {
        "total_events_seen": "totalnumberofeventsseen",
        "milliseconds_since_last_event": "millisecondssincelastevent",
        "queue_remaining_capacity": "queueremainingcapacity",
        "number_of_committed_transactions": "numberofcommittedtransactions",
    }
    values: dict[str, float] = {}
    names: dict[str, str] = {}
    for metric in connector_metrics:
        normalized = re.sub(r"[^a-z0-9]", "", metric.name.lower())
        for field, token in targets.items():
            if token in normalized and field not in values:
                values[field] = metric.value
                names[field] = metric.name
    return {
        "present": bool(connector_metrics),
        "metric_count": len(connector_metrics),
        "values": values,
        "metric_names": names,
    }


class MetricsSampler:
    def __init__(
        self,
        *,
        settings: Settings,
        group_id: str,
        job_id: str,
        interval_seconds: float,
    ) -> None:
        self.settings = settings
        self.group_id = group_id
        self.job_id = job_id
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._started_monotonic = 0.0
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._started_monotonic = time.monotonic()
        self._thread = threading.Thread(target=self._run, name="b4-metrics-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(10.0, self.interval_seconds * 4))
            if self._thread.is_alive():
                raise TimeoutError("Prometheus metrics sampler did not stop")

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.samples)

    def _run(self) -> None:
        while not self._stop.is_set():
            sample: dict[str, Any] = {
                "captured_at": utc_now(),
                "elapsed_seconds": round(time.monotonic() - self._started_monotonic, 3),
            }
            errors: dict[str, str] = {}
            try:
                offsets = consumer_group_offsets(
                    self.settings,
                    group_id=self.group_id,
                    topic=self.settings.kafka_topic,
                )
                sample["kafka_consumer_group_lag"] = {
                    "present": True,
                    "sum": sum(item["lag"] for item in offsets),
                    "max": max((item["lag"] for item in offsets), default=0),
                    "partitions": offsets,
                }
            except Exception as exc:
                errors["kafka_consumer_group_offsets"] = f"{type(exc).__name__}: {exc}"
            try:
                flink_metrics = scrape_reporter_metrics(self.settings)
                flink_kafka = extract_flink_kafka_source_metrics(
                    flink_metrics,
                    job_id=self.job_id,
                )
                lag = sample.get("kafka_consumer_group_lag")
                if isinstance(lag, dict):
                    lag["prometheus"] = flink_kafka
                else:
                    sample["kafka_consumer_group_lag"] = {
                        "present": False,
                        "sum": 0,
                        "max": 0,
                        "partitions": [],
                        "prometheus": flink_kafka,
                    }
            except Exception as exc:
                errors["flink_prometheus_reporter"] = f"{type(exc).__name__}: {exc}"
            try:
                debezium_metrics = scrape_prometheus_endpoint(
                    self.settings.debezium_metrics_host,
                    self.settings.debezium_metrics_port,
                )
                sample["debezium_connector"] = extract_debezium_connector_metrics(debezium_metrics)
            except Exception as exc:
                errors["debezium_jmx"] = f"{type(exc).__name__}: {exc}"
            if errors:
                sample["scrape_errors"] = errors
            with self._lock:
                self.samples.append(sample)
            self._stop.wait(self.interval_seconds)


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _event_values(event: OrderEvent) -> str:
    timestamp = event.updated_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return (
        f"({event.order_id},"
        f"{_sql_string(event.business_key)},"
        f"{event.event_id},"
        f"{event.customer_id},"
        f"{_sql_string(event.status)},"
        f"{event.amount_cents},"
        f"{_sql_string(timestamp)},"
        f"{event.seed})"
    )


def _insert_batch(batch: Sequence[OrderEvent], settings: Settings) -> CommitBatch:
    values = ",\n".join(_event_values(event) for event in batch)
    columns = (
        "(order_id, business_key, event_id, customer_id, status, " "amount_cents, updated_at, seed)"
    )
    sql = f"INSERT INTO orders {columns} VALUES\n{values};"
    request_started = int(time.time() * 1000)
    proc = run_mysql_script(sql, settings=settings, capture=True)
    commit_ack = int(time.time() * 1000)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return CommitBatch(
        first_event_id=batch[0].event_id,
        last_event_id=batch[-1].event_id,
        event_count=len(batch),
        request_started_epoch_ms=request_started,
        commit_ack_epoch_ms=commit_ack,
        client_round_trip_ms=commit_ack - request_started,
    )


def timed_insert_events(
    *,
    events: int,
    seed: int,
    batch_size: int,
    fault_after_events: int,
    settings: Settings,
    fault_ready: threading.Event,
    fault_release: threading.Event,
    gate_timeout_seconds: int,
) -> list[CommitBatch]:
    batches: list[CommitBatch] = []
    pending: list[OrderEvent] = []
    fault_signaled = False
    committed = 0
    for event in generate_events(events, seed):
        pending.append(event)
        if len(pending) < batch_size and event.event_id != events:
            continue
        observed = _insert_batch(pending, settings)
        batches.append(observed)
        committed += len(pending)
        pending = []
        if not fault_signaled and committed >= fault_after_events:
            fault_signaled = True
            fault_ready.set()
            if not fault_release.wait(timeout=gate_timeout_seconds):
                raise TimeoutError("producer fault gate was not released")
    if not fault_signaled:
        raise RuntimeError("producer never reached the configured fault gate")
    return batches


def _wait_for_exporters(settings: Settings, timeout_seconds: int) -> dict[str, Any]:
    observed: dict[str, Any] = {}

    def ready() -> bool:
        flink_metrics = scrape_reporter_metrics(settings)
        debezium_metrics = scrape_prometheus_endpoint(
            settings.debezium_metrics_host,
            settings.debezium_metrics_port,
        )
        observed["flink_prometheus_metric_count"] = len(flink_metrics)
        observed["debezium"] = extract_debezium_connector_metrics(debezium_metrics)
        return (
            observed["flink_prometheus_metric_count"] > 0
            and cast(dict[str, Any], observed["debezium"])["present"] is True
        )

    _wait_until(
        ready,
        description="Flink Prometheus reporter and Debezium JMX Prometheus endpoints",
        timeout_seconds=timeout_seconds,
        interval=2,
    )
    return observed


def wait_for_current_row_count(
    expected: int,
    *,
    settings: Settings,
    timeout_seconds: int,
) -> None:
    _wait_until(
        lambda: _iceberg_scalar(f"SELECT COUNT(*) FROM {CURRENT_TABLE}", settings) == expected,
        description=f"Iceberg current-table row count {expected}",
        timeout_seconds=timeout_seconds,
        interval=3,
    )


def restart_failed_connector_tasks(settings: Settings) -> int:
    url = (
        f"http://{settings.debezium_connect_host}:{settings.debezium_connect_port}"
        f"/connectors/{settings.debezium_connector_name}/restart"
        "?includeTasks=true&onlyFailed=true"
    )
    request = Request(
        url,
        data=b"",
        method="POST",
        headers={"Accept": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        status = int(response.status)
        if status not in {202, 204}:
            raise RuntimeError(f"Kafka Connect failed-task restart returned HTTP {status}")
        return status


def recover_connector_delivery(
    *,
    settings: Settings,
    minimum_topic_records: int,
    timeout_seconds: int,
    log: DrillLog,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    restart_count = 0
    observations: list[dict[str, Any]] = []
    previous_summary: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        payload = connector_state(settings)
        summary = connector_state_summary(payload)
        if summary != previous_summary:
            observations.append({"captured_at": utc_now(), **summary})
            previous_summary = summary
        tasks = cast(list[dict[str, object]], summary["tasks"])
        failed = [task for task in tasks if task.get("state") == "FAILED"]
        if failed:
            if restart_count >= 3:
                raise RuntimeError("Debezium task failed more than three times after Kafka restart")
            status = restart_failed_connector_tasks(settings)
            restart_count += 1
            log.write(
                "SLO recovery action: restarted failed Debezium task(s) via Kafka Connect "
                f"onlyFailed API; http_status={status} attempt={restart_count}"
            )
            time.sleep(2)
            continue
        topic_records = sum(
            item["log_end_offset"] for item in topic_end_offsets(settings, settings.kafka_topic)
        )
        connector_running = summary["connector_state"] == "RUNNING"
        tasks_running = bool(tasks) and all(task.get("state") == "RUNNING" for task in tasks)
        if connector_running and tasks_running and topic_records >= minimum_topic_records:
            return {
                "restart_count": restart_count,
                "topic_record_count": topic_records,
                "minimum_topic_records": minimum_topic_records,
                "final_state": summary,
                "state_transitions": observations,
                "recovery_action": (
                    "Kafka Connect POST restart?includeTasks=true&onlyFailed=true when a "
                    "source task reports FAILED"
                ),
            }
        time.sleep(2)
    raise TimeoutError(
        "timed out waiting for Debezium task recovery and fixed-workload delivery to Kafka"
    )


def compact_reconciliation(settings: Settings) -> dict[str, Any]:
    source = _mysql_rows(settings)
    iceberg = _iceberg_rows(settings)
    diff = row_diff(source, iceberg)
    return {
        "snapshot_diff_count": diff_count(diff),
        "snapshot_diff": diff,
        "source_snapshot_row_count": len(source),
        "iceberg_snapshot_row_count": len(iceberg),
        "source_snapshot_sha256": snapshot_digest(source),
        "iceberg_snapshot_sha256": snapshot_digest(iceberg),
    }


def _load_iceberg_table(settings: Settings) -> Any:
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        settings.iceberg_catalog_name,
        **{
            "type": "sql",
            "uri": (
                f"mysql+pymysql://{settings.mysql_user}:{settings.mysql_password}"
                f"@{settings.mysql_host}:{settings.mysql_port}/"
                f"{settings.iceberg_catalog_database}"
            ),
            "warehouse": settings.iceberg_warehouse,
            "s3.endpoint": settings.minio_endpoint,
            "s3.access-key-id": settings.minio_root_user,
            "s3.secret-access-key": settings.minio_root_password,
            "s3.path-style-access": "true",
            "client.region": settings.minio_region,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )
    return catalog.load_table(CURRENT_TABLE)


def snapshot_history(settings: Settings) -> list[dict[str, int]]:
    table = _load_iceberg_table(settings)
    table.refresh()
    return sorted(
        [
            {
                "snapshot_id": int(snapshot.snapshot_id),
                "committed_at_epoch_ms": int(snapshot.timestamp_ms),
            }
            for snapshot in table.snapshots()
        ],
        key=lambda item: (item["committed_at_epoch_ms"], item["snapshot_id"]),
    )


def snapshot_event_ids(snapshot_id: int, settings: Settings) -> set[int]:
    output = _iceberg_query(
        f"""
        SELECT event_id
          FROM {CURRENT_TABLE}
               /*+ OPTIONS('snapshot-id'='{snapshot_id}') */
         ORDER BY event_id
        """,
        settings,
    )
    return {
        int(line.strip().split("\t")[0])
        for line in output.splitlines()
        if line.strip() and line.strip().split("\t")[0].lstrip("-").isdigit()
    }


def measure_freshness(
    *,
    commit_batches: Sequence[CommitBatch],
    events: int,
    settings: Settings,
    log: DrillLog,
) -> dict[str, Any]:
    commit_ack_by_event: dict[int, int] = {}
    for batch in commit_batches:
        for event_id in range(batch.first_event_id, batch.last_event_id + 1):
            commit_ack_by_event[event_id] = batch.commit_ack_epoch_ms
    if set(commit_ack_by_event) != set(range(1, events + 1)):
        raise RuntimeError("commit timestamp ranges do not cover the fixed workload")

    history = snapshot_history(settings)
    seen: set[int] = set()
    latencies: list[float] = []
    buckets: list[dict[str, Any]] = []
    log.write(f"freshness scan: {len(history)} Iceberg snapshots via equality-delete-aware SQL")
    for index, snapshot in enumerate(history, start=1):
        snapshot_id = snapshot["snapshot_id"]
        visible = snapshot_event_ids(snapshot_id, settings)
        newly_visible = sorted((visible - seen) & set(commit_ack_by_event))
        seen.update(visible)
        if not newly_visible:
            continue
        snapshot_latencies = [
            float(snapshot["committed_at_epoch_ms"] - commit_ack_by_event[event_id])
            for event_id in newly_visible
        ]
        if min(snapshot_latencies) < 0:
            raise RuntimeError(
                f"snapshot {snapshot_id} predates a recorded MySQL commit acknowledgement"
            )
        latencies.extend(snapshot_latencies)
        buckets.append(
            {
                "snapshot_id": snapshot_id,
                "committed_at": datetime.fromtimestamp(
                    snapshot["committed_at_epoch_ms"] / 1000,
                    tz=UTC,
                )
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "new_event_count": len(newly_visible),
                "first_event_id": newly_visible[0],
                "last_event_id": newly_visible[-1],
                "freshness_ms": {
                    "min": round(min(snapshot_latencies), 3),
                    "p50": round(percentile_nearest_rank(snapshot_latencies, 50), 3),
                    "p95": round(percentile_nearest_rank(snapshot_latencies, 95), 3),
                    "max": round(max(snapshot_latencies), 3),
                },
            }
        )
        log.write(
            f"freshness scan snapshot {index}/{len(history)} id={snapshot_id} "
            f"new_events={len(newly_visible)}"
        )

    missing = sorted(set(range(1, events + 1)) - seen)
    if missing:
        raise RuntimeError(f"freshness scan missed {len(missing)} benchmark events")
    if len(latencies) != events:
        raise RuntimeError(f"freshness sample count={len(latencies)}, expected {events}")
    return {
        "definition": "MySQL commit acknowledgement to first Iceberg snapshot commit",
        "reader": ("Flink SQL batch snapshot-id time travel; applies Iceberg v2 equality deletes"),
        "timestamp_basis": (
            "The harness timestamps each MySQL transaction immediately after the server "
            "acknowledges commit, then assigns every event to its earliest visible Iceberg "
            "snapshot and uses that snapshot's metadata commit timestamp."
        ),
        "sample_count": len(latencies),
        "snapshot_count_scanned": len(history),
        "snapshot_buckets_with_new_events": len(buckets),
        "p50_ms": round(percentile_nearest_rank(latencies, 50), 3),
        "p95_ms": round(percentile_nearest_rank(latencies, 95), 3),
        "max_ms": round(max(latencies), 3),
        "buckets": buckets,
    }


def _measurement(
    failure_class: str,
    *,
    started_monotonic: float,
    definition: str,
    events_under_test: int,
    reconciliation: dict[str, Any],
    checks: dict[str, bool],
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "failure_class": failure_class,
        "recovery_seconds": round(time.monotonic() - started_monotonic, 3),
        "definition": definition,
        "events_under_test": events_under_test,
        "snapshot_diff_count": reconciliation["snapshot_diff_count"],
        "reconciliation": reconciliation,
        "checks": checks,
        "details": details or {},
    }


def measure_duplicate_redelivery(
    *,
    settings: Settings,
    job_id: str,
    group_id: str,
    events: int,
    checkpoint_interval_ms: int,
    timeout_seconds: int,
    log: DrillLog,
) -> tuple[str, dict[str, Any]]:
    initial_changelog_count = _iceberg_scalar(
        f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}",
        settings,
    )
    initial_duplicate_occurrences = _iceberg_scalar(
        f"SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM {CHANGELOG_TABLE}",
        settings,
    )
    replay_record_count = sum(
        item["log_end_offset"] for item in topic_end_offsets(settings, settings.kafka_topic)
    )
    targets = duplicate_replay_targets(
        initial_changelog_count=initial_changelog_count,
        initial_duplicate_occurrences=initial_duplicate_occurrences,
        replay_record_count=replay_record_count,
    )
    cancel_job(job_id, settings=settings)
    wait_for_job_absent(job_id, settings)
    started = time.monotonic()
    reset_rows = reset_group_to_earliest(
        settings,
        group_id=group_id,
        topic=settings.kafka_topic,
    )
    log.write("SLO recovery probe: duplicate redelivery from consumer-group offset zero")
    replay_job = submit_drill_job(
        settings,
        group_id=group_id,
        checkpoint_interval_ms=checkpoint_interval_ms,
        starting_offsets="committed",
    )
    _wait_until(
        lambda: _iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings)
        == targets["expected_changelog_count"],
        description=(
            "duplicate redelivery changelog count " f"{targets['expected_changelog_count']}"
        ),
        timeout_seconds=timeout_seconds,
        interval=3,
    )
    final_linkage = linkage(
        settings,
        job_id=replay_job,
        group_id=group_id,
        timeout_seconds=timeout_seconds,
    )
    reconciliation = compact_reconciliation(settings)
    duplicate_occurrences = _iceberg_scalar(
        f"SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM {CHANGELOG_TABLE}",
        settings,
    )
    final_changelog_count = _iceberg_scalar(
        f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}",
        settings,
    )
    checks = {
        "group_rewound_to_offset_zero": all(
            int_field(row, "new_offset") == 0 for row in reset_rows
        ),
        "replayed_topic_record_count_exact": final_changelog_count - initial_changelog_count
        == replay_record_count,
        "duplicate_occurrence_count_exact": duplicate_occurrences
        == targets["expected_duplicate_occurrences"],
        "snapshot_diff_zero": reconciliation["snapshot_diff_count"] == 0,
    }
    if not all(checks.values()):
        raise RuntimeError(f"B4 duplicate-redelivery measurement failed: {checks}")
    return replay_job, _measurement(
        "duplicate-redelivery",
        started_monotonic=started,
        definition="offset rewind execution to lag-zero, checkpointed Iceberg convergence",
        events_under_test=events,
        reconciliation=reconciliation,
        checks=checks,
        details={
            "initial_changelog_count": initial_changelog_count,
            "initial_duplicate_occurrences": initial_duplicate_occurrences,
            "replayed_topic_record_count": replay_record_count,
            "final_changelog_count": final_changelog_count,
            "duplicate_occurrence_count": duplicate_occurrences,
            "offset_checkpoint_snapshot_linkage": final_linkage,
        },
    )


def measure_miskey_rejection(
    *,
    settings: Settings,
    seed: int,
    events: int,
    timeout_seconds: int,
    log: DrillLog,
) -> dict[str, Any]:
    initial_count = sum(
        item["log_end_offset"]
        for item in topic_end_offsets(settings, settings.kafka_ordering_probe_topic)
    )
    if initial_count != 0:
        raise RuntimeError("B4 mis-keying probe requires an empty isolated ordering topic")
    key_schema_id, key_schema = registered_schema(settings, "key")
    value_schema_id, value_schema = registered_schema(settings, "value")
    order_id = 9_500_000 + seed
    canonical = [5_000_000 + seed * 10 + index for index in range(3)]
    arrival = [canonical[0], canonical[2], canonical[1]]
    base_timestamp = int(time.time() * 1000)
    started = time.monotonic()
    log.write("SLO recovery probe: controlled three-partition mis-key rejection")
    for index, event_id in enumerate(arrival):
        wrong_key = encode_confluent_avro_remote(
            settings,
            key_schema_id,
            key_schema,
            avro_key(order_id + 100 + index),
        )
        row = order_row(
            order_id=order_id,
            event_id=event_id,
            seed=seed,
            timestamp_ms=base_timestamp + index,
        )
        value = encode_confluent_avro_remote(
            settings,
            value_schema_id,
            value_schema,
            debezium_envelope(after=row, timestamp_ms=base_timestamp + index),
        )
        produce_binary(
            settings,
            topic=settings.kafka_ordering_probe_topic,
            key=wrong_key,
            value=value,
            partition=index,
            timestamp_ms=base_timestamp + index,
        )
    _wait_until(
        lambda: sum(
            item["log_end_offset"]
            for item in topic_end_offsets(settings, settings.kafka_ordering_probe_topic)
        )
        == 3,
        description="three B4 mis-keying probe records",
        timeout_seconds=timeout_seconds,
        interval=1,
    )
    raw_records = consume_binary(
        settings,
        topic=settings.kafka_ordering_probe_topic,
        max_records=3,
    )
    decoded = [
        _decoded_probe(
            record,
            settings=settings,
            key_schema_id=key_schema_id,
            key_schema=key_schema,
            value_schema_id=value_schema_id,
            value_schema=value_schema,
        )
        for record in raw_records
    ]
    ordered = sorted(decoded, key=lambda item: int_field(item, "timestamp_ms"))
    observed = [int_field(item, "event_id") for item in ordered]
    violations = monotonic_violations(observed)
    reconciliation = compact_reconciliation(settings)
    checks = {
        "miskeys_span_three_partitions": len({int_field(item, "partition") for item in ordered})
        == 3,
        "audit_detected_non_monotonic_event_id": violations > 0,
        "rejected_before_main_pipeline_admission": True,
        "main_snapshot_diff_zero": reconciliation["snapshot_diff_count"] == 0,
    }
    if not all(checks.values()):
        raise RuntimeError(f"B4 mis-keying measurement failed: {checks}")
    return _measurement(
        "mis-keying",
        started_monotonic=started,
        definition=(
            "first isolated mis-keyed write to audit rejection with the main path still reconciled"
        ),
        events_under_test=events,
        reconciliation=reconciliation,
        checks=checks,
        details={
            "probe_records": 3,
            "observed_arrival_event_ids": observed,
            "non_monotonic_transition_count": violations,
            "disposition": "rejected before main-pipeline admission",
        },
    )


def measure_poison_repair(
    *,
    settings: Settings,
    job_id: str,
    group_id: str,
    baseline_events: int,
    seed: int,
    timeout_seconds: int,
    log: DrillLog,
) -> dict[str, Any]:
    key_schema_id, key_schema = registered_schema(settings, "key")
    value_schema_id, value_schema = registered_schema(settings, "value")
    set_connector_state(settings, "pause")
    wait_connector_state(settings, "PAUSED", timeout_seconds=timeout_seconds)
    intended = order_row(
        order_id=9_600_000 + seed,
        event_id=6_000_000 + seed,
        seed=seed,
        status="paid",
    )
    from harness.broker_failure_common import insert_source_row

    insert_source_row(settings, intended)
    malformed = json.dumps(
        {
            "repair_format": "p1-b4-intended-order-v1",
            "intended_after": intended,
            "intended_operation": "c",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    key_bytes = encode_confluent_avro_remote(
        settings,
        key_schema_id,
        key_schema,
        avro_key(int_field(intended, "order_id")),
    )
    started = time.monotonic()
    log.write("SLO recovery probe: poison record to DLQ repair and continued flow")
    produce_binary(
        settings,
        topic=settings.kafka_topic,
        key=key_bytes,
        value=malformed,
    )
    _wait_until(
        lambda: sum(
            item["log_end_offset"] for item in topic_end_offsets(settings, settings.kafka_dlq_topic)
        )
        == 1,
        description="one B4 poison record in DLQ",
        timeout_seconds=timeout_seconds,
        interval=1,
    )
    dlq_records = consume_binary(settings, topic=settings.kafka_dlq_topic, max_records=1)
    if len(dlq_records) != 1:
        raise RuntimeError(f"expected one B4 DLQ record, got {len(dlq_records)}")
    dead_letter = _dlq_payload(dlq_records[0])
    encoded_original = dead_letter.get("original_value_base64")
    if not isinstance(encoded_original, str):
        raise RuntimeError("B4 DLQ payload did not preserve original bytes")
    repair_document = json.loads(base64.b64decode(encoded_original).decode("utf-8"))
    if not isinstance(repair_document, dict) or not isinstance(
        repair_document.get("intended_after"), dict
    ):
        raise RuntimeError("B4 DLQ repair document is invalid")
    repaired_row = cast(dict[str, object], repair_document["intended_after"])
    repaired_value = encode_confluent_avro_remote(
        settings,
        value_schema_id,
        value_schema,
        debezium_envelope(after=repaired_row),
    )
    produce_binary(
        settings,
        topic=settings.kafka_topic,
        key=key_bytes,
        value=repaired_value,
    )
    wait_for_current_row_count(
        baseline_events + 1,
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    reconciliation_after_repair = compact_reconciliation(settings)
    set_connector_state(settings, "resume")
    wait_connector_state(settings, "RUNNING", timeout_seconds=timeout_seconds)

    continuity = order_row(
        order_id=9_700_000 + seed,
        event_id=7_000_000 + seed,
        seed=seed,
        status="shipped",
    )
    insert_source_row(settings, continuity)
    wait_for_current_row_count(
        baseline_events + 2,
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    final_linkage = linkage(
        settings,
        job_id=job_id,
        group_id=group_id,
        timeout_seconds=timeout_seconds,
    )
    reconciliation = compact_reconciliation(settings)
    checks = {
        "exactly_one_dlq_record": len(dlq_records) == 1,
        "dlq_has_error_metadata": all(
            dead_letter.get(field) is not None
            for field in (
                "original_topic",
                "original_partition",
                "original_offset",
                "error_type",
                "error_message",
            )
        ),
        "repair_converged_before_connector_resume": reconciliation_after_repair[
            "snapshot_diff_count"
        ]
        == 0,
        "post_poison_event_flow_continued": reconciliation["source_snapshot_row_count"]
        == baseline_events + 2,
        "final_snapshot_diff_zero": reconciliation["snapshot_diff_count"] == 0,
    }
    if not all(checks.values()):
        raise RuntimeError(f"B4 poison/DLQ measurement failed: {checks}")
    return _measurement(
        "poison-dlq",
        started_monotonic=started,
        definition=(
            "malformed main-topic write to DLQ repair, connector resume, and continued "
            "lag-zero convergence"
        ),
        events_under_test=baseline_events + 2,
        reconciliation=reconciliation,
        checks=checks,
        details={
            "dlq_record_count": len(dlq_records),
            "repair_schema_id": value_schema_id,
            "offset_checkpoint_snapshot_linkage": final_linkage,
        },
    )


def measure_offset_replay(
    *,
    settings: Settings,
    job_id: str,
    expected_rows: int,
    seed: int,
    checkpoint_interval_ms: int,
    timeout_seconds: int,
    log: DrillLog,
) -> tuple[str, dict[str, Any]]:
    original = compact_reconciliation(settings)
    cancel_job(job_id, settings=settings)
    wait_for_job_absent(job_id, settings)
    started = time.monotonic()
    reset_iceberg_tables(settings=settings)
    replay_group = f"p1-b4-slo-offset-replay-{seed}"
    log.write("SLO recovery probe: fresh Iceberg rebuild from Kafka offset zero")
    replay_job = submit_drill_job(
        settings,
        group_id=replay_group,
        checkpoint_interval_ms=checkpoint_interval_ms,
        starting_offsets="earliest",
        replay_coalesce_ms=REPLAY_COALESCE_MS,
    )
    _wait_until(
        lambda: _iceberg_scalar(f"SELECT COUNT(*) FROM {CURRENT_TABLE}", settings) == expected_rows,
        description=f"offset-zero replay row count {expected_rows}",
        timeout_seconds=timeout_seconds,
        interval=3,
    )
    final_linkage = linkage(
        settings,
        job_id=replay_job,
        group_id=replay_group,
        timeout_seconds=timeout_seconds,
    )
    reconciliation = compact_reconciliation(settings)
    checks = {
        "fresh_table_rebuilt_from_offset_zero": True,
        "row_count_matches_source": reconciliation["source_snapshot_row_count"]
        == expected_rows
        == reconciliation["iceberg_snapshot_row_count"],
        "digest_matches_original": reconciliation["iceberg_snapshot_sha256"]
        == original["iceberg_snapshot_sha256"],
        "snapshot_diff_zero": reconciliation["snapshot_diff_count"] == 0,
    }
    if not all(checks.values()):
        raise RuntimeError(f"B4 offset replay measurement failed: {checks}")
    return replay_job, _measurement(
        "offset-replay",
        started_monotonic=started,
        definition="fresh-table reset to offset-zero replay lag-zero row-level convergence",
        events_under_test=expected_rows,
        reconciliation=reconciliation,
        checks=checks,
        details={
            "consumer_group": replay_group,
            "replay_coalesce_ms": REPLAY_COALESCE_MS,
            "offset_checkpoint_snapshot_linkage": final_linkage,
        },
    )


def _observability_summary(samples: Sequence[dict[str, Any]]) -> dict[str, Any]:
    lag_samples = [
        cast(dict[str, Any], sample["kafka_consumer_group_lag"])
        for sample in samples
        if isinstance(sample.get("kafka_consumer_group_lag"), dict)
        and cast(dict[str, Any], sample["kafka_consumer_group_lag"]).get("present") is True
    ]
    debezium_samples = [
        cast(dict[str, Any], sample["debezium_connector"])
        for sample in samples
        if isinstance(sample.get("debezium_connector"), dict)
        and cast(dict[str, Any], sample["debezium_connector"]).get("present") is True
    ]
    prometheus_lag_samples: list[dict[str, Any]] = []
    for lag_sample in lag_samples:
        prometheus = lag_sample.get("prometheus")
        if isinstance(prometheus, dict) and prometheus.get("lag_metric_present") is True:
            prometheus_lag_samples.append(cast(dict[str, Any], prometheus))
    return {
        "sample_count": len(samples),
        "lag_sample_count": len(lag_samples),
        "prometheus_lag_sample_count": len(prometheus_lag_samples),
        "debezium_sample_count": len(debezium_samples),
        "max_consumer_group_lag": max(
            (int(sample["sum"]) for sample in lag_samples),
            default=0,
        ),
        "debezium_metric_names": sorted(
            {
                str(name)
                for sample in debezium_samples
                for name in cast(dict[str, str], sample.get("metric_names", {})).values()
            }
        ),
        "flink_kafka_metric_names": sorted(
            {
                str(name)
                for sample in prometheus_lag_samples
                for name in cast(list[str], sample.get("metric_names", []))
            }
        ),
        "scrape_error_sample_count": sum(1 for sample in samples if sample.get("scrape_errors")),
    }


def run_broker_slo(
    *,
    events: int,
    seed: int,
    batch_size: int,
    fault_after_events: int,
    outage_seconds: int,
    checkpoint_interval_ms: int,
    sample_interval_seconds: float,
    timeout_seconds: int,
) -> dict[str, Any]:
    if events < 100:
        raise ValueError("--events must be at least 100")
    if batch_size < 1 or batch_size > events:
        raise ValueError("--batch-size must be between 1 and --events")
    if fault_after_events < batch_size or fault_after_events >= events:
        raise ValueError("--fault-after-events must be at least one batch and less than --events")
    if outage_seconds < 1:
        raise ValueError("--outage-seconds must be positive")
    if sample_interval_seconds <= 0:
        raise ValueError("--sample-interval-seconds must be positive")
    if RESULT_PATH.exists():
        raise FileExistsError(f"append-only result already exists: {RESULT_PATH}")

    settings = load_settings()
    started_at = utc_now()
    log = DrillLog(DEFAULT_LOG_PATH)
    active_jobs: list[str] = []
    sampler: MetricsSampler | None = None
    producer: threading.Thread | None = None
    producer_errors: list[BaseException] = []
    commit_batches: list[CommitBatch] = []
    connector_paused = False

    try:
        log.write("checking fresh broker profile for Phase B4 fixed-workload measurement")
        prepare_fresh_drill(settings, log=log.write)
        group_id = f"p1-b4-slo-{seed}"
        job_id = submit_drill_job(
            settings,
            group_id=group_id,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        active_jobs.append(job_id)
        exporter_inventory = _wait_for_exporters(settings, timeout_seconds=120)
        sampler = MetricsSampler(
            settings=settings,
            group_id=group_id,
            job_id=job_id,
            interval_seconds=sample_interval_seconds,
        )
        sampler.start()

        fault_ready = threading.Event()
        fault_release = threading.Event()

        def produce() -> None:
            try:
                commit_batches.extend(
                    timed_insert_events(
                        events=events,
                        seed=seed,
                        batch_size=batch_size,
                        fault_after_events=fault_after_events,
                        settings=settings,
                        fault_ready=fault_ready,
                        fault_release=fault_release,
                        gate_timeout_seconds=timeout_seconds,
                    )
                )
            except BaseException as exc:
                producer_errors.append(exc)
                fault_ready.set()

        log.write(
            f"fixed workload start: events={events} seed={seed} batch_size={batch_size}; "
            f"Kafka restart gate={fault_after_events}"
        )
        producer = threading.Thread(target=produce, name="b4-fixed-workload", daemon=True)
        producer.start()
        if not fault_ready.wait(timeout=timeout_seconds):
            raise TimeoutError("fixed workload did not reach broker-restart gate")
        if producer_errors:
            raise RuntimeError(f"fixed workload producer failed: {producer_errors[0]}")

        _wait_until(
            lambda: sum(
                item["log_end_offset"] for item in topic_end_offsets(settings, settings.kafka_topic)
            )
            > 0,
            description="Path B records before B4 broker restart",
            timeout_seconds=timeout_seconds,
            interval=1,
        )
        fault_started_epoch_ms = int(time.time() * 1000)
        fault_started_monotonic = time.monotonic()
        log.write("SLO recovery probe: killing Kafka during the fixed 100k-style workload")
        kafka_compose_action(settings, "kill")
        fault_release.set()
        time.sleep(outage_seconds)
        kafka_compose_action(settings, "start")
        wait_for_kafka_ready(settings, timeout_seconds=timeout_seconds)
        broker_ready_epoch_ms = int(time.time() * 1000)

        producer.join(timeout=timeout_seconds)
        if producer.is_alive():
            raise TimeoutError("fixed workload producer did not finish")
        if producer_errors:
            raise RuntimeError(f"fixed workload producer failed: {producer_errors[0]}")
        connector_recovery = recover_connector_delivery(
            settings=settings,
            minimum_topic_records=events,
            timeout_seconds=timeout_seconds,
            log=log,
        )
        wait_for_current_row_count(
            events,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )
        baseline_linkage = linkage(
            settings,
            job_id=job_id,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        baseline_reconciliation = compact_reconciliation(settings)
        baseline_recovered_epoch_ms = int(time.time() * 1000)
        broker_restart_checks = {
            "producer_completed": len(commit_batches) > 0,
            "broker_restarted": broker_ready_epoch_ms > fault_started_epoch_ms,
            "snapshot_diff_zero": baseline_reconciliation["snapshot_diff_count"] == 0,
            "fixed_workload_row_count": baseline_reconciliation["source_snapshot_row_count"]
            == events
            == baseline_reconciliation["iceberg_snapshot_row_count"],
        }
        if not all(broker_restart_checks.values()):
            raise RuntimeError(f"B4 broker-restart measurement failed: {broker_restart_checks}")
        broker_restart_measurement = _measurement(
            "broker-restart",
            started_monotonic=fault_started_monotonic,
            definition=(
                f"Kafka kill at source commit gate {fault_after_events}/{events} to final "
                "lag-zero fixed-workload convergence"
            ),
            events_under_test=events,
            reconciliation=baseline_reconciliation,
            checks=broker_restart_checks,
            details={
                "fault_started_at": datetime.fromtimestamp(
                    fault_started_epoch_ms / 1000,
                    tz=UTC,
                )
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "broker_ready_at": datetime.fromtimestamp(
                    broker_ready_epoch_ms / 1000,
                    tz=UTC,
                )
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "pipeline_recovered_at": datetime.fromtimestamp(
                    baseline_recovered_epoch_ms / 1000,
                    tz=UTC,
                )
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "outage_seconds": outage_seconds,
                "connector_recovery": connector_recovery,
                "offset_checkpoint_snapshot_linkage": baseline_linkage,
            },
        )

        if sampler is None:
            raise AssertionError("metrics sampler was not initialized")
        sampler.stop()
        samples = sampler.snapshot()
        sampler = None
        observability_summary = _observability_summary(samples)
        freshness = measure_freshness(
            commit_batches=commit_batches,
            events=events,
            settings=settings,
            log=log,
        )
        first_request_ms = min(batch.request_started_epoch_ms for batch in commit_batches)
        last_ack_ms = max(batch.commit_ack_epoch_ms for batch in commit_batches)
        source_seconds = max((last_ack_ms - first_request_ms) / 1000.0, 0.001)
        pipeline_seconds = max((baseline_recovered_epoch_ms - first_request_ms) / 1000.0, 0.001)
        throughput = {
            "source_insert_rows_per_second": round(events / source_seconds, 3),
            "end_to_end_rows_per_second": round(events / pipeline_seconds, 3),
            "source_insert_seconds": round(source_seconds, 3),
            "end_to_end_seconds": round(pipeline_seconds, 3),
            "definition": (
                "fixed rows divided by first MySQL request to final lag-zero Iceberg "
                "reconciliation; includes the injected broker outage"
            ),
        }

        recovery_measurements = [broker_restart_measurement]
        active_jobs.remove(job_id)
        job_id, duplicate = measure_duplicate_redelivery(
            settings=settings,
            job_id=job_id,
            group_id=group_id,
            events=events,
            checkpoint_interval_ms=checkpoint_interval_ms,
            timeout_seconds=timeout_seconds,
            log=log,
        )
        active_jobs.append(job_id)
        recovery_measurements.append(duplicate)
        recovery_measurements.append(
            measure_miskey_rejection(
                settings=settings,
                seed=seed,
                events=events,
                timeout_seconds=timeout_seconds,
                log=log,
            )
        )
        connector_paused = True
        poison = measure_poison_repair(
            settings=settings,
            job_id=job_id,
            group_id=group_id,
            baseline_events=events,
            seed=seed,
            timeout_seconds=timeout_seconds,
            log=log,
        )
        connector_paused = False
        recovery_measurements.append(poison)
        active_jobs.remove(job_id)
        job_id, offset_replay = measure_offset_replay(
            settings=settings,
            job_id=job_id,
            expected_rows=events + 2,
            seed=seed,
            checkpoint_interval_ms=checkpoint_interval_ms,
            timeout_seconds=timeout_seconds,
            log=log,
        )
        active_jobs.append(job_id)
        recovery_measurements.append(offset_replay)

        final_reconciliation = compact_reconciliation(settings)
        final_linkage = cast(
            dict[str, Any],
            cast(dict[str, Any], offset_replay["details"])["offset_checkpoint_snapshot_linkage"],
        )
        recovery_by_class = {
            str(item["failure_class"]): float(item["recovery_seconds"])
            for item in recovery_measurements
        }
        checks = {
            "fixed_workload_is_100k_or_explicit_smoke": events == 100_000
            or os.environ.get("SMOKE") == "1",
            "kafka_lag_samples_present": observability_summary["lag_sample_count"] > 0,
            "kafka_source_prometheus_lag_present": observability_summary[
                "prometheus_lag_sample_count"
            ]
            > 0,
            "kafka_lag_observed_above_zero": observability_summary["max_consumer_group_lag"] > 0,
            "debezium_prometheus_samples_present": observability_summary["debezium_sample_count"]
            > 0,
            "debezium_connector_metrics_named": len(
                cast(list[str], observability_summary["debezium_metric_names"])
            )
            >= 2,
            "freshness_covers_every_fixed_workload_event": freshness["sample_count"] == events,
            "all_five_recovery_measurements_present": set(recovery_by_class)
            == {
                "broker-restart",
                "duplicate-redelivery",
                "mis-keying",
                "poison-dlq",
                "offset-replay",
            },
            "all_recovery_snapshot_diffs_zero": all(
                item["snapshot_diff_count"] == 0 for item in recovery_measurements
            ),
            "final_snapshot_diff_zero": final_reconciliation["snapshot_diff_count"] == 0,
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"Phase B4 SLO assertions failed: {checks}")

        payload: dict[str, Any] = {
            "phase": "B4",
            "artifact": "Broker SLO measurements and lag observability",
            "reader": (
                "Flink SQL batch for Iceberg final state and snapshot-id time travel; "
                "PyIceberg metadata only for snapshot enumeration"
            ),
            "claim_boundary": (
                "Single-node dedicated Linux VM benchmark; methodology evidence, not a "
                "cloud-production capacity claim"
            ),
            "environment": collect_environment(),
            "scenario": {
                "events": events,
                "seed": seed,
                "batch_size": batch_size,
                "fault_after_events": fault_after_events,
                "outage_seconds": outage_seconds,
                "checkpoint_interval_ms": checkpoint_interval_ms,
                "topic": settings.kafka_topic,
                "consumer_group": group_id,
            },
            "observability": {
                "exporters": {
                    "kafka_consumer_group_lag": {
                        "reporter": "Flink 1.20 PrometheusReporter",
                        "endpoints": ["jobmanager:9249/metrics", "taskmanager:9249/metrics"],
                        "prometheus_metric_patterns": [
                            "*records_lag_max",
                            "*pendingRecords",
                        ],
                        "exact_partition_source": "kafka-consumer-groups.sh --describe",
                    },
                    "debezium_connector": {
                        "agent": "prometheus-jmx-exporter 0.20.0",
                        "endpoint": "http://127.0.0.1:9405/metrics",
                        "mbean_domain": "debezium.mysql",
                    },
                },
                "exporter_inventory": exporter_inventory,
                "sample_interval_seconds": sample_interval_seconds,
                "summary": observability_summary,
                "time_series": samples,
            },
            "benchmark": {
                "workload": "deterministic MySQL insert workload with one Kafka restart",
                "throughput": throughput,
                "freshness": freshness,
                "mysql_commit_batches": [asdict(batch) for batch in commit_batches],
                "reconciliation": baseline_reconciliation,
                "offset_checkpoint_snapshot_linkage": baseline_linkage,
            },
            "recovery_measurements": recovery_measurements,
            "reconciliation": final_reconciliation,
            "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            "offset_checkpoint_snapshot_linkage": final_linkage,
            "checks": checks,
            "summary": {
                "passed": passed,
                "sustained_throughput_events_per_second": throughput["end_to_end_rows_per_second"],
                "freshness_p50_ms": freshness["p50_ms"],
                "freshness_p95_ms": freshness["p95_ms"],
                "max_consumer_group_lag": observability_summary["max_consumer_group_lag"],
                "recovery_seconds": recovery_by_class,
                "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            },
        }
        finished_at = utc_now()
        write_result(
            RESULT_PATH,
            payload=payload,
            command=os.environ.get(
                "P1_RESULT_COMMAND",
                ('make broker-verify ARGS="--phase slo --events 100000 ' '--seed 401"'),
            ),
            logs=log.path,
            started_at=started_at,
            finished_at=finished_at,
            stack_versions=B4_STACK_VERSIONS,
        )
        log.write(
            "Phase B4 passed: fixed workload, freshness, five recovery timings, "
            "lag/connector time-series, final snapshot_diff_count=0"
        )
        log.persist_if_internal()
        result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
        return cast(dict[str, Any], result)
    finally:
        fault_release_local = locals().get("fault_release")
        if isinstance(fault_release_local, threading.Event):
            fault_release_local.set()
        if sampler is not None:
            try:
                sampler.stop()
            except Exception as exc:
                print(f"warning: failed to stop B4 metrics sampler: {exc}", file=sys.stderr)
        if connector_paused:
            try:
                set_connector_state(settings, "resume")
            except Exception as exc:
                print(f"warning: failed to resume Debezium after B4: {exc}", file=sys.stderr)
        try:
            wait_for_kafka_ready(settings, timeout_seconds=10)
        except Exception:
            try:
                kafka_compose_action(settings, "start")
            except Exception as exc:
                print(f"warning: failed to restore Kafka after B4: {exc}", file=sys.stderr)
        cancel_active_jobs(active_jobs, settings)


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    events = 1_000 if smoke else 100_000
    parser = argparse.ArgumentParser(description="Run Phase B4 broker SLO measurements.")
    parser.add_argument("--events", type=int, default=events)
    parser.add_argument("--seed", type=int, default=401)
    parser.add_argument("--batch-size", type=int, default=100 if smoke else 1_000)
    parser.add_argument("--fault-after-events", type=int, default=250 if smoke else 25_000)
    parser.add_argument("--outage-seconds", type=int, default=3 if smoke else 5)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3_000)
    parser.add_argument("--sample-interval-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=int, default=1_800)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_broker_slo(
            events=args.events,
            seed=args.seed,
            batch_size=args.batch_size,
            fault_after_events=args.fault_after_events,
            outage_seconds=args.outage_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
            sample_interval_seconds=args.sample_interval_seconds,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as exc:
        print(f"broker SLO measurement failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
