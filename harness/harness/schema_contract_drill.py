from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections.abc import Sequence
from typing import cast
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from harness.broker_parity import (
    CHANGELOG_TABLE,
    _cancel_existing_jobs,
    _consumer_group_offsets,
    _iceberg_rows,
    _iceberg_scalar,
    _mysql,
    _mysql_rows,
    _require_broker_services,
    _require_checkpoint,
    _snapshot_ids,
    _topic_end_offsets,
    _wait_for_job,
    _wait_until,
    _write_internal_log,
    collect_environment,
    diff_count,
    row_diff,
)
from harness.config import REPO_ROOT, Settings, load_settings
from harness.flink import (
    cancel_job,
    flink_rest_json,
    latest_completed_checkpoint,
    reset_iceberg_tables,
    running_job_ids,
)
from harness.generator import insert_events
from harness.provenance import STACK_VERSIONS, utc_now, write_result

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "schema_contract_drill.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b2-schema-contract.log"
CONTRACT_ROOT = REPO_ROOT / "contracts" / "avro"
CONTRACT_STACK_VERSIONS = {
    **STACK_VERSIONS,
    "kafka": "3.9.2 (single-node KRaft)",
    "schema_registry": "Confluent 7.9.8",
    "debezium_connect": "3.2.4.Final",
    "confluent_avro_converter": "7.9.8",
    "jackson": "2.18.6 (BOM)",
    "java_avro": "1.12.0 (Flink job)",
    "flink_kafka_connector": "3.4.0-1.20",
    "python_avro": "1.12.1",
    "broker_serialization": "Confluent Avro with Schema Registry",
}

JsonObject = dict[str, object]


def registry_subject(topic: str, kind: str) -> str:
    if kind not in {"key", "value"}:
        raise ValueError("subject kind must be key or value")
    return f"{topic}-{kind}"


def mutate_event_id_type(schema_text: str) -> tuple[str, int]:
    payload = json.loads(schema_text)
    mutations = 0

    def visit(value: object) -> None:
        nonlocal mutations
        if isinstance(value, dict):
            if value.get("name") == "event_id" and "type" in value:
                value["type"] = "string"
                mutations += 1
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    if mutations < 1:
        raise ValueError("registered Debezium schema has no event_id field to mutate")
    return json.dumps(payload, separators=(",", ":"), sort_keys=True), mutations


def schema_sha256(schema_text: str) -> str:
    parsed = json.loads(schema_text)
    canonical = json.dumps(parsed, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def run_schema_contract_drill(
    *,
    baseline_events: int,
    seed: int,
    timeout_seconds: int,
    checkpoint_interval_ms: int,
) -> dict[str, object]:
    if baseline_events < 1:
        raise ValueError("--baseline-events must be positive")
    if RESULT_PATH.exists():
        raise FileExistsError(
            f"append-only result already exists: {RESULT_PATH}; use a fresh checkout/run volume"
        )

    settings = load_settings()
    started_at = utc_now()
    log_lines: list[str] = []
    external_log = os.environ.get("P1_RESULT_LOGS", "").strip()
    logs_path = external_log or DEFAULT_LOG_PATH
    active_job: str | None = None

    def log(message: str) -> None:
        line = f"{utc_now()} {message}"
        log_lines.append(line)
        print(line, flush=True)

    try:
        log("checking broker services, connector, and fresh Avro topic state")
        _require_broker_services(settings)
        connector_before = _connector_status(settings)
        if not _connector_running(connector_before):
            raise RuntimeError(f"Debezium connector is not RUNNING: {connector_before}")

        initial_offsets = _topic_end_offsets(settings)
        if sum(item["log_end_offset"] for item in initial_offsets) != 0:
            raise RuntimeError(
                "schema-contract drill requires a fresh Kafka topic; use "
                'remote-broker-up ARGS="--phase contracts --fresh"'
            )
        initial_rows = _mysql_rows(settings)
        if initial_rows:
            raise RuntimeError("schema-contract drill requires an empty MySQL orders table")

        _cancel_existing_jobs(settings, log=log)
        reset_iceberg_tables(settings=settings)

        log("submitting the Registry-backed Avro Kafka consumer")
        active_job = _submit_contract_job(
            settings=settings,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        _wait_for_job(active_job, settings=settings, timeout_seconds=120)

        log(f"writing baseline flow through Debezium Avro events={baseline_events} seed={seed}")
        insert_events(
            baseline_events,
            seed,
            batch_size=min(1000, baseline_events),
            reset=False,
        )
        _wait_until(
            lambda: sum(item["log_end_offset"] for item in _topic_end_offsets(settings))
            == baseline_events,
            description=f"baseline Kafka end offsets total {baseline_events}",
            timeout_seconds=timeout_seconds,
        )
        _wait_for_iceberg_rows_while_job_active(
            active_job,
            expected_rows=baseline_events,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )
        baseline_source = _mysql_rows(settings)
        baseline_iceberg = _iceberg_rows(settings)
        baseline_diff = row_diff(baseline_source, baseline_iceberg)
        if diff_count(baseline_diff) != 0:
            raise RuntimeError(f"baseline source/Iceberg diff is non-zero: {baseline_diff}")
        checkpoint_before = _require_checkpoint(
            active_job,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )
        offsets_before = _wait_for_contract_zero_group_lag(
            settings,
            timeout_seconds=timeout_seconds,
        )

        key_subject = registry_subject(settings.kafka_topic, "key")
        value_subject = registry_subject(settings.kafka_topic, "value")
        _wait_until(
            lambda: _subject_exists(settings, key_subject)
            and _subject_exists(settings, value_subject),
            description="Debezium key/value Avro subjects to be registered",
            timeout_seconds=timeout_seconds,
        )
        global_compatibility = _get_global_compatibility(settings)
        if global_compatibility != "BACKWARD":
            raise RuntimeError(
                f"global Schema Registry compatibility is {global_compatibility!r}, "
                "expected 'BACKWARD'"
            )
        for subject in (key_subject, value_subject):
            _set_subject_compatibility(settings, subject, "BACKWARD")

        key_schema_before = _latest_schema(settings, key_subject)
        value_schema_before = _latest_schema(settings, value_subject)
        key_compatibility = _get_subject_compatibility(settings, key_subject)
        value_compatibility = _get_subject_compatibility(settings, value_subject)
        if key_compatibility != "BACKWARD" or value_compatibility != "BACKWARD":
            raise RuntimeError("key/value subjects are not both configured BACKWARD")

        connector_config = _connector_converter_config(settings)
        expected_converter = "io.confluent.connect.avro.AvroConverter"
        if (
            connector_config.get("key.converter") != expected_converter
            or connector_config.get("value.converter") != expected_converter
        ):
            raise RuntimeError(f"connector is not using AvroConverter: {connector_config}")

        incompatible_schema, mutation_count = mutate_event_id_type(
            _required_string(value_schema_before, "schema")
        )
        compatibility_status, compatibility_payload = _request_json(
            "POST",
            _registry_url(
                settings,
                f"/compatibility/subjects/{quote(value_subject, safe='')}/versions/latest"
                "?verbose=true",
            ),
            {"schemaType": "AVRO", "schema": incompatible_schema},
        )
        compatibility_object = _as_object(compatibility_payload, "compatibility response")
        if compatibility_status != 200 or compatibility_object.get("is_compatible") is not False:
            raise RuntimeError(
                "registry did not classify the type change as incompatible: "
                f"HTTP {compatibility_status} {compatibility_payload}"
            )

        log(
            "pushing incompatible value schema mutation event_id long->string; "
            "expecting HTTP 409"
        )
        versions_before = _subject_versions(settings, value_subject)
        registration_status, registration_payload = _request_json(
            "POST",
            _registry_url(
                settings,
                f"/subjects/{quote(value_subject, safe='')}/versions",
            ),
            {"schemaType": "AVRO", "schema": incompatible_schema},
        )
        if registration_status != 409:
            raise RuntimeError(
                "incompatible schema registration was not rejected: "
                f"HTTP {registration_status} {registration_payload}"
            )

        versions_after_rejection = _subject_versions(settings, value_subject)
        value_schema_after_rejection = _latest_schema(settings, value_subject)
        if versions_after_rejection != versions_before:
            raise RuntimeError(
                "rejected schema unexpectedly changed subject versions: "
                f"before={versions_before} after={versions_after_rejection}"
            )
        if _schema_identity(value_schema_after_rejection) != _schema_identity(value_schema_before):
            raise RuntimeError("rejected schema unexpectedly changed the latest registered schema")
        job_running_after_rejection = active_job in running_job_ids(settings=settings)
        if not job_running_after_rejection:
            raise RuntimeError("Flink job stopped after the registry rejection")
        connector_after_rejection = _connector_status(settings)
        if not _connector_running(connector_after_rejection):
            raise RuntimeError(
                f"Debezium connector stopped after registry rejection: {connector_after_rejection}"
            )

        post_order_id = 9_000_000 + seed
        post_event_id = 19_000_000 + seed
        log(
            "writing one deterministic post-rejection event with the previously registered schema "
            f"order_id={post_order_id}"
        )
        _mysql(
            f"""
            INSERT INTO orders
                (order_id, business_key, event_id, customer_id, status,
                 amount_cents, updated_at, seed)
            VALUES
                ({post_order_id}, 'contract-order-{post_order_id}', {post_event_id},
                 {1 + seed % 1000}, 'paid', {10_000 + seed},
                 '2026-02-01 00:00:00.000', {seed});
            """,
            settings,
        )
        expected_rows = baseline_events + 1
        _wait_until(
            lambda: sum(item["log_end_offset"] for item in _topic_end_offsets(settings))
            == expected_rows,
            description=f"post-rejection Kafka end offsets total {expected_rows}",
            timeout_seconds=timeout_seconds,
        )
        _wait_for_iceberg_rows_while_job_active(
            active_job,
            expected_rows=expected_rows,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )
        checkpoint_before_id = int(checkpoint_before["id"])
        _wait_until(
            lambda: _latest_checkpoint_id(active_job, settings) > checkpoint_before_id,
            description="a completed Flink checkpoint after the rejected schema attempt",
            timeout_seconds=timeout_seconds,
        )
        checkpoint_after = _require_checkpoint(
            active_job,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )
        offsets_after = _wait_for_contract_zero_group_lag(
            settings,
            timeout_seconds=timeout_seconds,
        )
        final_source = _mysql_rows(settings)
        final_iceberg = _iceberg_rows(settings)
        final_diff = row_diff(final_source, final_iceberg)
        final_diff_count = diff_count(final_diff)
        if final_diff_count != 0:
            raise RuntimeError(f"post-rejection source/Iceberg diff is non-zero: {final_diff}")

        connector_final = _connector_status(settings)
        job_running_final = active_job in running_job_ids(settings=settings)
        value_schema_final = _latest_schema(settings, value_subject)
        versions_final = _subject_versions(settings, value_subject)
        latest_schema_unchanged = (
            _schema_identity(value_schema_final) == _schema_identity(value_schema_before)
            and versions_final == versions_before
        )
        post_event_visible = any(
            row["order_id"] == str(post_order_id) and row["event_id"] == str(post_event_id)
            for row in final_iceberg
        )
        changelog_count = _iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings)
        snapshots = _snapshot_ids(settings)
        consumer_registry_avro_verified = (
            job_running_after_rejection
            and isinstance(value_schema_before.get("id"), int)
            and diff_count(baseline_diff) == 0
            and post_event_visible
            and final_diff_count == 0
        )

        checks = {
            "global_compatibility_backward": global_compatibility == "BACKWARD",
            "key_subject_backward": key_compatibility == "BACKWARD",
            "value_subject_backward": value_compatibility == "BACKWARD",
            "producer_uses_registry_avro": (
                connector_config.get("key.converter") == expected_converter
                and connector_config.get("value.converter") == expected_converter
                and connector_config.get("key.converter.schema.registry.url")
                == settings.schema_registry_docker_url
                and connector_config.get("value.converter.schema.registry.url")
                == settings.schema_registry_docker_url
            ),
            "consumer_uses_registry_avro": consumer_registry_avro_verified,
            "compatibility_probe_rejected_type_change": (
                compatibility_status == 200 and compatibility_object.get("is_compatible") is False
            ),
            "registration_rejected_http_409": registration_status == 409,
            "subject_versions_unchanged": versions_final == versions_before,
            "latest_schema_unchanged": latest_schema_unchanged,
            "connector_running_after_rejection": _connector_running(connector_final),
            "flink_job_running_after_rejection": job_running_after_rejection,
            "post_rejection_event_visible": post_event_visible,
            "post_rejection_source_iceberg_diff_zero": final_diff_count == 0,
            "kafka_lag_zero": all(item["lag"] == 0 for item in offsets_after),
            "checkpoint_advanced": int(checkpoint_after["id"]) > checkpoint_before_id,
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"schema-contract checks did not all pass: {checks}")

        fixture_paths = [
            "order-change-v1.avsc",
            "order-change-compatible-add-v2.avsc",
            "order-change-incompatible-remove-v2.avsc",
            "order-change-incompatible-type-v2.avsc",
        ]
        key_contract_evidence = _schema_evidence(key_schema_before)
        key_contract_evidence["compatibility"] = key_compatibility
        value_contract_evidence = _schema_evidence(value_schema_before)
        value_contract_evidence["compatibility"] = value_compatibility
        payload: dict[str, object] = {
            "phase": "B2",
            "claim_boundary": (
                "Schema Registry rejection and uninterrupted old-schema flow only; "
                "no broker-failure claim."
            ),
            "environment": collect_environment(),
            "scenario": {
                "baseline_events": baseline_events,
                "post_rejection_events": 1,
                "seed": seed,
                "checkpoint_interval_ms": checkpoint_interval_ms,
                "topic": settings.kafka_topic,
                "topic_partitions": settings.kafka_topic_partitions,
                "consumer_group": settings.kafka_consumer_group,
                "initial_topic_end_offsets": initial_offsets,
            },
            "contracts": {
                "format": "AVRO",
                "global_compatibility": global_compatibility,
                "key": key_contract_evidence,
                "value": value_contract_evidence,
                "consumer_projection_fixtures": [
                    {
                        "path": f"contracts/avro/{filename}",
                        "sha256": schema_sha256(
                            (CONTRACT_ROOT / filename).read_text(encoding="utf-8")
                        ),
                    }
                    for filename in fixture_paths
                ],
            },
            "producer": {
                "runtime": "single-worker Debezium Connect",
                "connector": settings.debezium_connector_name,
                "converter_config": connector_config,
                "state_before": connector_before,
                "state_after_rejection": connector_after_rejection,
                "state_final": connector_final,
            },
            "consumer": {
                "job_class": "com.p1.reliability.cdc.KafkaToIcebergJob",
                "deserializer_class": (
                    "com.p1.reliability.cdc." "KafkaDebeziumAvroRecordDeserializationSchema"
                ),
                "schema_registry_url": settings.schema_registry_docker_url,
                "job_id": active_job,
                "wire_resolution_evidence": (
                    "The only configured Path B deserializer resolved Confluent-wire Avro "
                    "records through the captured value subject; baseline and post-rejection "
                    "rows both reconciled to Iceberg."
                ),
            },
            "incompatible_schema_attempt": {
                "subject": value_subject,
                "mutation": "nested event_id field type long -> string",
                "mutated_field_occurrences": mutation_count,
                "candidate_schema_sha256": schema_sha256(incompatible_schema),
                "compatibility_check": {
                    "http_status": compatibility_status,
                    **compatibility_object,
                },
                "registration": {
                    "http_status": registration_status,
                    "rejected": registration_status == 409,
                    "response": registration_payload,
                },
                "versions_before": versions_before,
                "versions_after_rejection": versions_after_rejection,
                "versions_final": versions_final,
                "latest_schema_unchanged": latest_schema_unchanged,
            },
            "flow_continuity": {
                "before_rejection": {
                    "source_row_count": len(baseline_source),
                    "iceberg_row_count": len(baseline_iceberg),
                    "source_iceberg_diff_count": diff_count(baseline_diff),
                    "kafka_offsets": offsets_before,
                    "flink_checkpoint_id": checkpoint_before_id,
                },
                "after_rejection": {
                    "source_row_count": len(final_source),
                    "iceberg_row_count": len(final_iceberg),
                    "orders_changelog_row_count": changelog_count,
                    "source_iceberg_diff_count": final_diff_count,
                    "source_iceberg_diff": final_diff,
                    "post_event_order_id": post_order_id,
                    "post_event_event_id": post_event_id,
                    "post_event_visible": post_event_visible,
                    "kafka_offsets": offsets_after,
                    "flink_checkpoint": {
                        "id": int(checkpoint_after["id"]),
                        "status": checkpoint_after.get("status"),
                        "external_path": checkpoint_after.get("external_path"),
                    },
                    "iceberg_snapshot_ids": snapshots,
                },
            },
            "checks": checks,
            "summary": {
                "passed": passed,
                "incompatible_schema_rejected": registration_status == 409,
                "old_schema_pipeline_continued": (
                    latest_schema_unchanged
                    and post_event_visible
                    and final_diff_count == 0
                    and _connector_running(connector_final)
                    and job_running_final
                ),
            },
        }
        log(
            "schema-contract drill passed: registration_status=409 "
            f"post_rejection_diff_count={final_diff_count} kafka_lag=0"
        )
        _write_internal_log(log_lines, logs_path=logs_path, external=bool(external_log))
        write_result(
            RESULT_PATH,
            payload=payload,
            command=os.environ.get(
                "P1_RESULT_COMMAND",
                'make broker-verify ARGS="--phase contracts"',
            ),
            logs=logs_path,
            started_at=started_at,
            finished_at=utc_now(),
            stack_versions=CONTRACT_STACK_VERSIONS,
        )
        result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            raise RuntimeError("written schema-contract result is not a JSON object")
        return {
            **cast(dict[str, object], result),
            "result_path": str(RESULT_PATH.relative_to(REPO_ROOT)),
        }
    except Exception:
        _write_internal_log(log_lines, logs_path=logs_path, external=bool(external_log))
        raise
    finally:
        if active_job is not None:
            try:
                if active_job in running_job_ids(settings=settings):
                    cancel_job(active_job, settings=settings)
            except Exception as exc:
                print(f"warning: failed to cancel Flink job {active_job}: {exc}", file=sys.stderr)


def _submit_contract_job(*, settings: Settings, checkpoint_interval_ms: int) -> str:
    from harness.flink import submit_kafka_job

    return submit_kafka_job(
        settings=settings,
        checkpoint_interval_ms=checkpoint_interval_ms,
    )


def _latest_checkpoint_id(job_id: str, settings: Settings) -> int:
    checkpoint = latest_completed_checkpoint(job_id, settings=settings)
    if checkpoint is None or not isinstance(checkpoint.get("id"), int):
        return -1
    return int(checkpoint["id"])


def _wait_for_iceberg_rows_while_job_active(
    job_id: str,
    *,
    expected_rows: int,
    settings: Settings,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    terminal_states = {"CANCELED", "FAILED", "FINISHED", "SUSPENDED"}
    while time.monotonic() < deadline:
        job = flink_rest_json(f"/jobs/{job_id}", settings=settings)
        state = job.get("state")
        if state in terminal_states:
            exceptions = flink_rest_json(f"/jobs/{job_id}/exceptions", settings=settings)
            root_exception = exceptions.get("root-exception")
            detail = (
                root_exception[:2000]
                if isinstance(root_exception, str) and root_exception
                else "root exception unavailable"
            )
            raise RuntimeError(
                f"Flink job {job_id} entered terminal state {state} while waiting for "
                f"Iceberg row count {expected_rows}: {detail}"
            )
        if len(_iceberg_rows(settings)) == expected_rows:
            return
        time.sleep(3)
    raise TimeoutError(
        f"timed out waiting for Iceberg row count {expected_rows} while Flink job "
        f"{job_id} remained active"
    )


def normalize_contract_group_offsets(
    described_offsets: list[dict[str, int]],
    topic_end_offsets: list[dict[str, int]],
) -> list[dict[str, int]]:
    described_by_partition = {item["partition"]: item for item in described_offsets}
    normalized: list[dict[str, int]] = []
    for topic_offset in sorted(topic_end_offsets, key=lambda item: item["partition"]):
        partition = topic_offset["partition"]
        log_end_offset = topic_offset["log_end_offset"]
        described = described_by_partition.get(partition)
        if described is None:
            if log_end_offset != 0:
                return []
            normalized.append(
                {
                    "partition": partition,
                    "current_offset": 0,
                    "log_end_offset": 0,
                    "lag": 0,
                }
            )
            continue
        if described["log_end_offset"] != log_end_offset:
            return []
        normalized.append(described)
    return normalized


def _wait_for_contract_zero_group_lag(
    settings: Settings,
    *,
    timeout_seconds: int,
) -> list[dict[str, int]]:
    offsets: list[dict[str, int]] = []

    def ready() -> bool:
        nonlocal offsets
        offsets = normalize_contract_group_offsets(
            _consumer_group_offsets(settings),
            _topic_end_offsets(settings),
        )
        return len(offsets) == settings.kafka_topic_partitions and all(
            item["lag"] == 0 for item in offsets
        )

    _wait_until(
        ready,
        description="B2 Kafka consumer-group committed lag to reach zero",
        timeout_seconds=timeout_seconds,
        interval=2,
    )
    return offsets


def _registry_url(settings: Settings, path: str) -> str:
    return f"http://{settings.schema_registry_host}:{settings.schema_registry_port}{path}"


def _connect_url(settings: Settings, path: str) -> str:
    return f"http://{settings.debezium_connect_host}:{settings.debezium_connect_port}{path}"


def _request_json(
    method: str,
    url: str,
    payload: JsonObject | None = None,
) -> tuple[int, object]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=data,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, json.loads(body) if body else {}


def _as_object(payload: object, description: str) -> JsonObject:
    if not isinstance(payload, dict):
        raise RuntimeError(f"{description} is not a JSON object: {payload}")
    return cast(JsonObject, payload)


def _subject_exists(settings: Settings, subject: str) -> bool:
    status, _ = _request_json(
        "GET",
        _registry_url(settings, f"/subjects/{quote(subject, safe='')}/versions/latest"),
    )
    return status == 200


def _latest_schema(settings: Settings, subject: str) -> JsonObject:
    status, payload = _request_json(
        "GET",
        _registry_url(settings, f"/subjects/{quote(subject, safe='')}/versions/latest"),
    )
    if status != 200:
        raise RuntimeError(f"latest schema lookup failed for {subject}: HTTP {status} {payload}")
    schema = _as_object(payload, f"latest schema for {subject}")
    for field in ("id", "version", "schema"):
        if field not in schema:
            raise RuntimeError(f"latest schema for {subject} is missing {field}")
    return schema


def _subject_versions(settings: Settings, subject: str) -> list[int]:
    status, payload = _request_json(
        "GET",
        _registry_url(settings, f"/subjects/{quote(subject, safe='')}/versions"),
    )
    if status != 200 or not isinstance(payload, list):
        raise RuntimeError(f"subject versions lookup failed for {subject}: HTTP {status} {payload}")
    if not all(isinstance(version, int) for version in payload):
        raise RuntimeError(f"subject versions are not integers for {subject}: {payload}")
    return [int(version) for version in payload]


def _get_global_compatibility(settings: Settings) -> str:
    status, payload = _request_json("GET", _registry_url(settings, "/config"))
    if status != 200:
        raise RuntimeError(f"global compatibility lookup failed: HTTP {status} {payload}")
    return _required_string(_as_object(payload, "global compatibility"), "compatibilityLevel")


def _set_subject_compatibility(settings: Settings, subject: str, level: str) -> None:
    status, payload = _request_json(
        "PUT",
        _registry_url(settings, f"/config/{quote(subject, safe='')}"),
        {"compatibility": level},
    )
    if status not in {200, 201}:
        raise RuntimeError(
            f"subject compatibility update failed for {subject}: HTTP {status} {payload}"
        )


def _get_subject_compatibility(settings: Settings, subject: str) -> str:
    status, payload = _request_json(
        "GET",
        _registry_url(
            settings,
            f"/config/{quote(subject, safe='')}?defaultToGlobal=true",
        ),
    )
    if status != 200:
        raise RuntimeError(
            f"subject compatibility lookup failed for {subject}: HTTP {status} {payload}"
        )
    return _required_string(
        _as_object(payload, f"compatibility for {subject}"), "compatibilityLevel"
    )


def _connector_status(settings: Settings) -> JsonObject:
    status, payload = _request_json(
        "GET",
        _connect_url(
            settings,
            f"/connectors/{quote(settings.debezium_connector_name, safe='')}/status",
        ),
    )
    if status != 200:
        raise RuntimeError(f"connector status lookup failed: HTTP {status} {payload}")
    return _as_object(payload, "connector status")


def _connector_running(status: JsonObject) -> bool:
    connector = status.get("connector")
    tasks = status.get("tasks")
    return (
        isinstance(connector, dict)
        and connector.get("state") == "RUNNING"
        and isinstance(tasks, list)
        and len(tasks) == 1
        and isinstance(tasks[0], dict)
        and tasks[0].get("state") == "RUNNING"
    )


def _connector_converter_config(settings: Settings) -> dict[str, str]:
    status, payload = _request_json(
        "GET",
        _connect_url(
            settings,
            f"/connectors/{quote(settings.debezium_connector_name, safe='')}/config",
        ),
    )
    if status != 200:
        raise RuntimeError(f"connector config lookup failed: HTTP {status} {payload}")
    config = _as_object(payload, "connector config")
    keys = (
        "key.converter",
        "key.converter.schema.registry.url",
        "key.converter.enhanced.avro.schema.support",
        "value.converter",
        "value.converter.schema.registry.url",
        "value.converter.enhanced.avro.schema.support",
    )
    filtered: dict[str, str] = {}
    for key in keys:
        value = config.get(key)
        if not isinstance(value, str):
            raise RuntimeError(f"connector config is missing string {key}")
        filtered[key] = value
    return filtered


def _required_string(payload: JsonObject, field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"JSON object is missing non-empty string {field}: {payload}")
    return value


def _schema_identity(schema: JsonObject) -> tuple[int, int, str]:
    schema_id = schema.get("id")
    version = schema.get("version")
    text = schema.get("schema")
    if not isinstance(schema_id, int) or not isinstance(version, int) or not isinstance(text, str):
        raise RuntimeError(f"invalid schema identity: {schema}")
    return schema_id, version, schema_sha256(text)


def _schema_evidence(schema: JsonObject) -> JsonObject:
    schema_id, version, digest = _schema_identity(schema)
    subject = schema.get("subject")
    return {
        "subject": subject,
        "id": schema_id,
        "version": version,
        "schema_type": schema.get("schemaType", "AVRO"),
        "schema_sha256": digest,
        "schema": json.loads(_required_string(schema, "schema")),
    }


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run the Phase B2 incompatible-schema drill.")
    parser.add_argument("--baseline-events", type=int, default=1 if smoke else 2)
    parser.add_argument("--seed", type=int, default=211)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000 if smoke else 5000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_schema_contract_drill(
            baseline_events=args.baseline_events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
        )
    except Exception as exc:
        print(f"broker contract verify failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
