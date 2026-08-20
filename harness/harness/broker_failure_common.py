from __future__ import annotations

import base64
import io
import json
import os
import struct
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote
from urllib.request import Request, urlopen

from avro import io as avro_io
from avro import schema as avro_schema

from harness.broker_parity import (
    BROKER_STACK_VERSIONS,
    Row,
    _cancel_existing_jobs,
    _iceberg_query,
    _iceberg_rows,
    _iceberg_scalar,
    _mysql,
    _mysql_rows,
    _require_broker_services,
    _require_checkpoint,
    _snapshot_ids,
    _wait_for_job,
    _wait_until,
    collect_environment,
    diff_count,
    parse_consumer_group_offsets,
    parse_topic_end_offsets,
    row_diff,
    snapshot_digest,
)
from harness.config import REPO_ROOT, Settings
from harness.flink import (
    KAFKA_JOB_MAIN_CLASS,
    REMOTE_JOB_JAR,
    cancel_job,
    compose_base,
    ensure_remote_job_jar,
    run_jobmanager,
    running_job_ids,
    submit_kafka_job,
)
from harness.provenance import utc_now, write_result

CURRENT_TABLE = "cdc_lab.orders_current"
CHANGELOG_TABLE = "cdc_lab.orders_changelog"
KAFKA_BINARY_ADMIN_CLASS = "com.p1.reliability.cdc.KafkaBinaryAdmin"

B3_STACK_VERSIONS = {
    **BROKER_STACK_VERSIONS,
    "path_b_dlq": "Flink KafkaSink 3.4.0-1.20 (AT_LEAST_ONCE)",
    "b3_wire_tool": "Kafka clients from pinned Flink connector 3.4.0-1.20",
}

JsonObject = dict[str, object]


@dataclass
class DrillLog:
    default_path: str
    lines: list[str] = field(default_factory=list)

    @property
    def path(self) -> str:
        return os.environ.get("P1_RESULT_LOGS", "").strip() or self.default_path

    @property
    def external(self) -> bool:
        return bool(os.environ.get("P1_RESULT_LOGS", "").strip())

    def write(self, message: str) -> None:
        line = f"{utc_now()} {message}"
        self.lines.append(line)
        print(line, flush=True)

    def persist_if_internal(self) -> None:
        if self.external:
            return
        output = REPO_ROOT / self.path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("\n".join(self.lines) + "\n", encoding="utf-8")


def require_new_result(path: Path) -> None:
    if path.exists():
        raise FileExistsError(
            f"append-only result already exists: {path}; use a fresh checkout/run volume"
        )


def prepare_fresh_drill(settings: Settings, *, log: Callable[[str], None]) -> None:
    _require_broker_services(settings)
    for topic in (
        settings.kafka_topic,
        settings.kafka_dlq_topic,
        settings.kafka_ordering_probe_topic,
    ):
        offsets = topic_end_offsets(settings, topic)
        if sum(item["log_end_offset"] for item in offsets) != 0:
            raise RuntimeError(
                f"Phase B3 requires a fresh topic {topic!r}; "
                'run remote-broker-up ARGS="--phase failures --fresh"'
            )
    _cancel_existing_jobs(settings, log=log)
    _mysql("TRUNCATE TABLE orders;", settings)
    from harness.flink import reset_iceberg_tables

    reset_iceberg_tables(settings=settings)


def submit_drill_job(
    settings: Settings,
    *,
    group_id: str,
    checkpoint_interval_ms: int,
    starting_offsets: str = "earliest",
    start_timestamp_ms: int | None = None,
) -> str:
    job_id = submit_kafka_job(
        settings=settings,
        checkpoint_interval_ms=checkpoint_interval_ms,
        group_id=group_id,
        starting_offsets=starting_offsets,
        start_timestamp_ms=start_timestamp_ms,
    )
    _wait_for_job(job_id, settings=settings, timeout_seconds=120)
    return job_id


def cancel_active_jobs(job_ids: list[str], settings: Settings) -> None:
    for job_id in list(reversed(job_ids)):
        try:
            if job_id in running_job_ids(settings=settings):
                cancel_job(job_id, settings=settings)
        except Exception as exc:
            print(f"warning: failed to cancel Flink job {job_id}: {exc}", file=sys.stderr)


def wait_for_job_absent(job_id: str, settings: Settings, timeout_seconds: int = 120) -> None:
    _wait_until(
        lambda: job_id not in running_job_ids(settings=settings),
        description=f"Flink job {job_id} cancellation",
        timeout_seconds=timeout_seconds,
    )


def wait_for_iceberg_rows(
    expected: int,
    settings: Settings,
    *,
    timeout_seconds: int,
) -> None:
    _wait_until(
        lambda: len(_iceberg_rows(settings)) == expected,
        description=f"Iceberg current-table row count {expected}",
        timeout_seconds=timeout_seconds,
        interval=2,
    )


def topic_end_offsets(settings: Settings, topic: str) -> list[dict[str, int]]:
    proc = kafka_command(
        settings,
        "kafka-get-offsets.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        topic,
    )
    return parse_topic_end_offsets(proc.stdout, topic=topic)


def consumer_group_offsets(
    settings: Settings,
    *,
    group_id: str,
    topic: str,
) -> list[dict[str, int]]:
    proc = kafka_command(
        settings,
        "kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--describe",
        "--group",
        group_id,
    )
    parsed = parse_consumer_group_offsets(proc.stdout, group=group_id, topic=topic)
    by_partition = {item["partition"]: item for item in parsed}
    for end in topic_end_offsets(settings, topic):
        partition = end["partition"]
        if partition not in by_partition and end["log_end_offset"] == 0:
            by_partition[partition] = {
                "partition": partition,
                "current_offset": 0,
                "log_end_offset": 0,
                "lag": 0,
            }
    return [by_partition[index] for index in sorted(by_partition)]


def wait_for_zero_lag(
    settings: Settings,
    *,
    group_id: str,
    topic: str,
    timeout_seconds: int,
) -> list[dict[str, int]]:
    offsets: list[dict[str, int]] = []

    def ready() -> bool:
        nonlocal offsets
        offsets = consumer_group_offsets(settings, group_id=group_id, topic=topic)
        ends = topic_end_offsets(settings, topic)
        return (
            len(offsets) == len(ends)
            and sum(item["log_end_offset"] for item in ends) > 0
            and all(item["lag"] == 0 for item in offsets)
        )

    _wait_until(
        ready,
        description=f"Kafka group {group_id} lag to reach zero",
        timeout_seconds=timeout_seconds,
        interval=2,
    )
    return offsets


def linkage(
    settings: Settings,
    *,
    job_id: str,
    group_id: str,
    timeout_seconds: int,
) -> dict[str, object]:
    checkpoint = _require_checkpoint(
        job_id,
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    offsets = wait_for_zero_lag(
        settings,
        group_id=group_id,
        topic=settings.kafka_topic,
        timeout_seconds=timeout_seconds,
    )
    return {
        "topic": settings.kafka_topic,
        "consumer_group": group_id,
        "kafka_offsets": offsets,
        "flink_checkpoint": {
            "id": int(checkpoint["id"]),
            "status": checkpoint.get("status"),
            "external_path": checkpoint.get("external_path"),
        },
        "iceberg_snapshot_ids": _snapshot_ids(settings),
    }


def reconciliation(settings: Settings) -> dict[str, object]:
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
        "mysql_rows": source,
        "iceberg_rows": iceberg,
    }


def event_id_audit(settings: Settings) -> dict[str, object]:
    source = _mysql_rows(settings)
    iceberg = _iceberg_rows(settings)
    source_ids = sorted(int(row["event_id"]) for row in source)
    iceberg_ids = sorted(int(row["event_id"]) for row in iceberg)
    changelog_count = _iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings)
    distinct_count = _iceberg_scalar(
        f"SELECT COUNT(DISTINCT event_id) FROM {CHANGELOG_TABLE}", settings
    )
    duplicate_rows = duplicate_event_counts(settings)
    duplicate_occurrences = sum(item["count"] - 1 for item in duplicate_rows)
    return {
        "source_current_event_ids": source_ids,
        "iceberg_current_event_ids": iceberg_ids,
        "current_event_ids_match": source_ids == iceberg_ids,
        "changelog_row_count": changelog_count,
        "changelog_distinct_event_id_count": distinct_count,
        "duplicate_event_ids": duplicate_rows,
        "duplicate_occurrence_count": duplicate_occurrences,
        "consistent": source_ids == iceberg_ids,
    }


def duplicate_event_counts(settings: Settings) -> list[dict[str, int]]:
    output = _iceberg_query(
        f"""
        SELECT event_id, COUNT(*)
          FROM {CHANGELOG_TABLE}
         GROUP BY event_id
        HAVING COUNT(*) > 1
         ORDER BY event_id
        """,
        settings,
    )
    rows: list[dict[str, int]] = []
    for line in output.splitlines():
        fields = line.split("\t")
        if len(fields) == 2 and fields[0].lstrip("-").isdigit() and fields[1].isdigit():
            rows.append({"event_id": int(fields[0]), "count": int(fields[1])})
    return rows


def kafka_command(
    settings: Settings,
    script_name: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [
            *compose_base(settings),
            "exec",
            "-T",
            "kafka",
            f"/opt/kafka/bin/{script_name}",
            *args,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc


def reset_group_to_earliest(
    settings: Settings,
    *,
    group_id: str,
    topic: str,
) -> list[dict[str, object]]:
    proc = kafka_command(
        settings,
        "kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        group_id,
        "--topic",
        topic,
        "--reset-offsets",
        "--to-earliest",
        "--execute",
    )
    return parse_reset_offsets(proc.stdout, group_id=group_id, topic=topic)


def parse_reset_offsets(
    output: str,
    *,
    group_id: str,
    topic: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw_line in output.splitlines():
        fields = raw_line.split()
        if len(fields) == 4 and fields[0] == group_id and fields[1] == topic:
            rows.append(
                {
                    "group": fields[0],
                    "topic": fields[1],
                    "partition": int(fields[2]),
                    "new_offset": int(fields[3]),
                }
            )
    if not rows:
        raise ValueError(f"could not parse consumer-group reset output: {output}")
    return sorted(rows, key=lambda row: int_field(row, "partition"))


def offsets_for_timestamp(
    settings: Settings,
    *,
    topic: str,
    timestamp_ms: int,
) -> list[dict[str, int]]:
    proc = kafka_command(
        settings,
        "kafka-get-offsets.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        topic,
        "--time",
        str(timestamp_ms),
    )
    requested = parse_topic_end_offsets(proc.stdout, topic=topic)
    ends = {
        item["partition"]: item["log_end_offset"] for item in topic_end_offsets(settings, topic)
    }
    return [
        {
            "partition": item["partition"],
            "requested_offset": item["log_end_offset"],
            "resolved_start_offset": (
                ends[item["partition"]] if item["log_end_offset"] < 0 else item["log_end_offset"]
            ),
            "log_end_offset": ends[item["partition"]],
        }
        for item in requested
    ]


def produce_binary(
    settings: Settings,
    *,
    topic: str,
    key: bytes | None,
    value: bytes | None,
    partition: int | None = None,
    timestamp_ms: int | None = None,
) -> dict[str, object]:
    args = [
        "produce",
        "--bootstrap-servers",
        settings.kafka_bootstrap_servers,
        "--topic",
        topic,
        "--key-base64",
        "-" if key is None else base64.b64encode(key).decode("ascii"),
        "--value-base64",
        "-" if value is None else base64.b64encode(value).decode("ascii"),
    ]
    if partition is not None:
        args.extend(["--partition", str(partition)])
    if timestamp_ms is not None:
        args.extend(["--timestamp-ms", str(timestamp_ms)])
    output = run_binary_admin(settings, args)
    rows = json_lines(output)
    if len(rows) != 1:
        raise RuntimeError(f"expected one producer metadata row, got {rows}")
    return rows[0]


def consume_binary(
    settings: Settings,
    *,
    topic: str,
    max_records: int,
    timeout_ms: int = 10_000,
) -> list[dict[str, object]]:
    output = run_binary_admin(
        settings,
        [
            "consume",
            "--bootstrap-servers",
            settings.kafka_bootstrap_servers,
            "--topic",
            topic,
            "--max-records",
            str(max_records),
            "--timeout-ms",
            str(timeout_ms),
        ],
    )
    return json_lines(output)


def run_binary_admin(settings: Settings, args: Sequence[str]) -> str:
    ensure_remote_job_jar(settings)
    proc = run_jobmanager(
        [
            "java",
            "-cp",
            f"{REMOTE_JOB_JAR}:/opt/flink/lib/*",
            KAFKA_BINARY_ADMIN_CLASS,
            *args,
        ],
        settings=settings,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc.stdout


def json_lines(output: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("{"):
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(cast(dict[str, object], payload))
    return rows


def registered_schema(settings: Settings, kind: str) -> tuple[int, dict[str, object]]:
    if kind not in {"key", "value"}:
        raise ValueError("schema kind must be key or value")
    subject = quote(f"{settings.kafka_topic}-{kind}", safe="")
    url = (
        f"http://{settings.schema_registry_host}:{settings.schema_registry_port}"
        f"/subjects/{subject}/versions/latest"
    )
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("id"), int):
        raise RuntimeError(f"unexpected Registry response for {kind}: {payload}")
    schema_id = payload.get("id")
    if not isinstance(schema_id, int):
        raise RuntimeError(f"Registry response lacks integer schema id for {kind}")
    raw_schema = payload.get("schema")
    if not isinstance(raw_schema, str):
        raise RuntimeError(f"Registry response lacks schema text for {kind}")
    parsed = json.loads(raw_schema)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Registry {kind} schema is not an object")
    return schema_id, cast(dict[str, object], parsed)


def encode_confluent_avro(
    schema_id: int,
    schema_payload: dict[str, object],
    datum: dict[str, object],
) -> bytes:
    parsed = avro_schema.parse(json.dumps(schema_payload))
    buffer = io.BytesIO()
    buffer.write(b"\x00")
    buffer.write(struct.pack(">I", schema_id))
    encoder = avro_io.BinaryEncoder(buffer)
    avro_io.DatumWriter(parsed).write(datum, encoder)
    return buffer.getvalue()


def decode_confluent_avro(
    payload: bytes,
    expected_schema_id: int,
    schema_payload: dict[str, object],
) -> dict[str, object]:
    if len(payload) < 5 or payload[0] != 0:
        raise ValueError("payload is not Confluent Avro wire format")
    schema_id = struct.unpack(">I", payload[1:5])[0]
    if schema_id != expected_schema_id:
        raise ValueError(f"schema id {schema_id} does not match expected {expected_schema_id}")
    parsed = avro_schema.parse(json.dumps(schema_payload))
    decoder = avro_io.BinaryDecoder(io.BytesIO(payload[5:]))
    decoded = avro_io.DatumReader(parsed).read(decoder)
    if not isinstance(decoded, dict):
        raise ValueError("decoded Avro datum is not an object")
    return cast(dict[str, object], decoded)


def order_row(
    *,
    order_id: int,
    event_id: int,
    seed: int,
    status: str = "paid",
    timestamp_ms: int | None = None,
) -> dict[str, object]:
    observed_timestamp = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    return {
        "order_id": order_id,
        "business_key": f"b3-order-{order_id}",
        "event_id": event_id,
        "customer_id": 1 + seed % 1000,
        "status": status,
        "amount_cents": 10_000 + event_id % 10_000,
        "updated_at": observed_timestamp,
        "seed": seed,
    }


def debezium_envelope(
    *,
    after: dict[str, object] | None,
    before: dict[str, object] | None = None,
    operation: str = "c",
    timestamp_ms: int | None = None,
) -> dict[str, object]:
    observed_timestamp = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    return {
        "before": before,
        "after": after,
        "source": {
            "version": "3.2.4.Final",
            "connector": "mysql",
            "name": "broker",
            "ts_ms": observed_timestamp,
            "snapshot": "false",
            "db": "cdc_lab",
            "sequence": None,
            "ts_us": None,
            "ts_ns": None,
            "table": "orders",
            "server_id": 1,
            "gtid": "b3-probe:1",
            "file": "mysql-bin.b3-probe",
            "pos": 4,
            "row": 0,
            "thread": None,
            "query": None,
        },
        "transaction": None,
        "op": operation,
        "ts_ms": observed_timestamp,
        "ts_us": None,
        "ts_ns": None,
    }


def avro_key(order_id: int) -> dict[str, object]:
    return {"order_id": order_id}


def insert_source_row(settings: Settings, row: dict[str, object]) -> None:
    def sql_string(value: object) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

    timestamp_ms = int_field(row, "updated_at")
    timestamp_text = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp_ms / 1000))
    milliseconds = timestamp_ms % 1000
    _mysql(
        """
        INSERT INTO orders
            (order_id, business_key, event_id, customer_id, status,
             amount_cents, updated_at, seed)
        VALUES
            ({order_id}, {business_key}, {event_id}, {customer_id}, {status},
             {amount_cents}, {updated_at}, {seed});
        """.format(
            order_id=int_field(row, "order_id"),
            business_key=sql_string(row["business_key"]),
            event_id=int_field(row, "event_id"),
            customer_id=int_field(row, "customer_id"),
            status=sql_string(row["status"]),
            amount_cents=int_field(row, "amount_cents"),
            updated_at=sql_string(f"{timestamp_text}.{milliseconds:03d}"),
            seed=int_field(row, "seed"),
        ),
        settings,
    )


def connector_state(settings: Settings) -> dict[str, object]:
    url = (
        f"http://{settings.debezium_connect_host}:{settings.debezium_connect_port}"
        f"/connectors/{settings.debezium_connector_name}/status"
    )
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected connector status: {payload}")
    return cast(dict[str, object], payload)


def set_connector_state(settings: Settings, action: str) -> None:
    if action not in {"pause", "resume"}:
        raise ValueError("connector action must be pause or resume")
    url = (
        f"http://{settings.debezium_connect_host}:{settings.debezium_connect_port}"
        f"/connectors/{settings.debezium_connector_name}/{action}"
    )
    with urlopen(Request(url, data=b"", method="PUT"), timeout=10):
        pass


def wait_connector_state(
    settings: Settings,
    expected: str,
    *,
    timeout_seconds: int,
) -> dict[str, object]:
    observed: dict[str, object] = {}

    def ready() -> bool:
        nonlocal observed
        observed = connector_state(settings)
        connector = observed.get("connector")
        tasks = observed.get("tasks")
        connector_ok = isinstance(connector, dict) and connector.get("state") == expected
        tasks_ok = isinstance(tasks, list) and all(
            isinstance(task, dict) and task.get("state") == expected for task in tasks
        )
        return connector_ok and tasks_ok

    _wait_until(
        ready,
        description=f"Debezium connector/tasks state {expected}",
        timeout_seconds=timeout_seconds,
        interval=2,
    )
    return observed


def kafka_container_state(settings: Settings) -> dict[str, object]:
    compose = compose_base(settings)
    ps = subprocess.run(
        [*compose, "ps", "-q", "kafka"],
        check=False,
        capture_output=True,
        text=True,
    )
    if ps.returncode != 0 or not ps.stdout.strip():
        raise RuntimeError(ps.stderr.strip() or "Kafka container id is unavailable")
    container_id = ps.stdout.strip()
    inspect = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{json .State}}",
            container_id,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspect.returncode != 0:
        raise RuntimeError(inspect.stderr.strip() or inspect.stdout.strip())
    state = json.loads(inspect.stdout)
    if not isinstance(state, dict):
        raise RuntimeError(f"unexpected Kafka container state: {state}")
    return {"container_id": container_id, **cast(dict[str, object], state)}


def kafka_compose_action(settings: Settings, action: str) -> None:
    if action not in {"kill", "start"}:
        raise ValueError("Kafka action must be kill or start")
    proc = subprocess.run(
        [*compose_base(settings), action, "kafka"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())


def wait_for_kafka_ready(settings: Settings, timeout_seconds: int) -> None:
    _wait_until(
        lambda: kafka_command(
            settings,
            "kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--list",
            check=False,
        ).returncode
        == 0,
        description="Kafka broker readiness after restart",
        timeout_seconds=timeout_seconds,
        interval=2,
    )


def finalize_result(
    path: Path,
    *,
    payload: dict[str, object],
    log: DrillLog,
    started_at: str,
    default_command: str,
) -> dict[str, object]:
    finished_at = utc_now()
    write_result(
        path,
        payload=payload,
        command=os.environ.get("P1_RESULT_COMMAND", default_command),
        logs=log.path,
        started_at=started_at,
        finished_at=finished_at,
        stack_versions=B3_STACK_VERSIONS,
    )
    log.persist_if_internal()
    result = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise RuntimeError(f"written result is not an object: {path}")
    return {
        **cast(dict[str, object], result),
        "result_path": str(path.relative_to(REPO_ROOT)),
    }


def b3_base_payload(failure_class: str) -> dict[str, object]:
    return {
        "phase": "B3",
        "failure_class": failure_class,
        "reader": "Flink SQL batch (Iceberg v2 equality-delete aware)",
        "claim_boundary": (
            "MySQL GTID -> Debezium at-least-once -> Kafka -> Flink checkpoint -> "
            "Iceberg keyed upsert; single-node remote lab only"
        ),
        "path_b_job_class": KAFKA_JOB_MAIN_CLASS,
        "environment": collect_environment(),
    }


def rows_match(left: Sequence[Row], right: Sequence[Row]) -> bool:
    return diff_count(row_diff(left, right)) == 0


def int_field(payload: dict[str, object], name: str) -> int:
    value: Any = payload.get(name)
    if not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def object_field(payload: dict[str, object], name: str) -> dict[str, object]:
    value = payload.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], value)


def object_list_field(payload: dict[str, object], name: str) -> list[dict[str, object]]:
    value = payload.get(name)
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{name} must be a list of objects")
    return [cast(dict[str, object], item) for item in value]
