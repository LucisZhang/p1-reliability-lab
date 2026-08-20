from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from collections.abc import Sequence
from typing import cast

from harness.broker_failure_common import (
    DrillLog,
    avro_key,
    b3_base_payload,
    cancel_active_jobs,
    consume_binary,
    debezium_envelope,
    encode_confluent_avro_remote,
    event_id_audit,
    finalize_result,
    insert_source_row,
    int_field,
    linkage,
    order_row,
    prepare_fresh_drill,
    produce_binary,
    reconciliation,
    registered_schema,
    require_new_result,
    set_connector_state,
    submit_drill_job,
    topic_end_offsets,
    wait_connector_state,
    wait_for_iceberg_rows,
)
from harness.broker_parity import _require_checkpoint, _wait_until
from harness.config import REPO_ROOT, load_settings
from harness.flink import latest_completed_checkpoint, running_job_ids
from harness.generator import insert_events
from harness.provenance import utc_now

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "poison_dlq_drill.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b3-poison-dlq.log"
FAILURE_CLASS = "poison-dlq"


def _dlq_payload(record: dict[str, object]) -> dict[str, object]:
    encoded = record.get("value_base64")
    if not isinstance(encoded, str):
        raise ValueError(f"DLQ Kafka record lacks value bytes: {record}")
    payload = json.loads(base64.b64decode(encoded).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"DLQ payload is not an object: {payload}")
    return cast(dict[str, object], payload)


def run_poison_dlq(
    *,
    baseline_events: int,
    seed: int,
    timeout_seconds: int,
    checkpoint_interval_ms: int,
) -> dict[str, object]:
    if baseline_events < 1:
        raise ValueError("--baseline-events must be positive")
    require_new_result(RESULT_PATH)
    settings = load_settings()
    started_at = utc_now()
    log = DrillLog(DEFAULT_LOG_PATH)
    active_jobs: list[str] = []
    connector_paused = False

    try:
        log.write("preparing fresh Path B baseline for poison-message quarantine and repair")
        prepare_fresh_drill(settings, log=log.write)
        group_id = f"p1-b3-poison-dlq-{seed}"
        job_id = submit_drill_job(
            settings,
            group_id=group_id,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        active_jobs.append(job_id)
        insert_events(
            baseline_events,
            seed,
            batch_size=min(100, baseline_events),
            reset=False,
        )
        wait_for_iceberg_rows(baseline_events, settings, timeout_seconds=timeout_seconds)
        baseline_linkage = linkage(
            settings,
            job_id=job_id,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        checkpoint_before = _require_checkpoint(
            job_id,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )
        key_schema_id, key_schema = registered_schema(settings, "key")
        value_schema_id, value_schema = registered_schema(settings, "value")

        log.write(
            "pausing Debezium only, writing the intended source row, then injecting its "
            "malformed non-Avro envelope into the main topic"
        )
        set_connector_state(settings, "pause")
        connector_paused = True
        paused_status = wait_connector_state(
            settings,
            "PAUSED",
            timeout_seconds=timeout_seconds,
        )
        intended_row = order_row(
            order_id=9_300_000 + seed,
            event_id=3_000_000 + seed,
            seed=seed,
            status="paid",
        )
        insert_source_row(settings, intended_row)
        malformed_object = {
            "repair_format": "p1-b3-intended-order-v1",
            "intended_after": intended_row,
            "intended_operation": "c",
        }
        malformed_bytes = json.dumps(
            malformed_object,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        key_bytes = encode_confluent_avro_remote(
            settings,
            key_schema_id,
            key_schema,
            avro_key(int_field(intended_row, "order_id")),
        )
        poison_metadata = produce_binary(
            settings,
            topic=settings.kafka_topic,
            key=key_bytes,
            value=malformed_bytes,
        )

        _wait_until(
            lambda: sum(
                item["log_end_offset"]
                for item in topic_end_offsets(settings, settings.kafka_dlq_topic)
            )
            == 1,
            description="one quarantined DLQ record",
            timeout_seconds=timeout_seconds,
            interval=1,
        )
        dlq_records = consume_binary(
            settings,
            topic=settings.kafka_dlq_topic,
            max_records=1,
        )
        if len(dlq_records) != 1:
            raise RuntimeError(f"expected one DLQ record, got {len(dlq_records)}")
        dead_letter = _dlq_payload(dlq_records[0])
        job_running_after_poison = job_id in running_job_ids(settings=settings)

        checkpoint_after_poison: dict[str, object] = {}

        def checkpoint_advanced() -> bool:
            nonlocal checkpoint_after_poison
            observed = latest_completed_checkpoint(job_id, settings=settings)
            if observed is None:
                return False
            checkpoint_after_poison = cast(dict[str, object], observed)
            return int(observed["id"]) > int(checkpoint_before["id"])

        _wait_until(
            checkpoint_advanced,
            description="completed checkpoint after poison quarantine",
            timeout_seconds=timeout_seconds,
            interval=1,
        )

        encoded_original = dead_letter.get("original_value_base64")
        if not isinstance(encoded_original, str):
            raise RuntimeError(f"DLQ metadata lacks original value: {dead_letter}")
        repair_document = json.loads(base64.b64decode(encoded_original).decode("utf-8"))
        if not isinstance(repair_document, dict) or not isinstance(
            repair_document.get("intended_after"), dict
        ):
            raise RuntimeError(f"DLQ repair document is invalid: {repair_document}")
        repaired_row = cast(dict[str, object], repair_document["intended_after"])
        repaired_value = encode_confluent_avro_remote(
            settings,
            value_schema_id,
            value_schema,
            debezium_envelope(after=repaired_row),
        )
        log.write(
            "operator repair: decode intended row from DLQ metadata, encode with registered "
            f"Avro schema id {value_schema_id}, replay with the primary-key key"
        )
        repair_metadata = produce_binary(
            settings,
            topic=settings.kafka_topic,
            key=key_bytes,
            value=repaired_value,
        )
        wait_for_iceberg_rows(
            baseline_events + 1,
            settings,
            timeout_seconds=timeout_seconds,
        )
        repair_linkage = linkage(
            settings,
            job_id=job_id,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        reconciliation_after_repair = reconciliation(settings)
        if reconciliation_after_repair["snapshot_diff_count"] != 0:
            raise RuntimeError(f"repair replay did not converge: {reconciliation_after_repair}")

        set_connector_state(settings, "resume")
        connector_paused = False
        resumed_status = wait_connector_state(
            settings,
            "RUNNING",
            timeout_seconds=timeout_seconds,
        )
        continuity_row = order_row(
            order_id=9_400_000 + seed,
            event_id=3_100_000 + seed,
            seed=seed,
            status="shipped",
        )
        insert_source_row(settings, continuity_row)
        wait_for_iceberg_rows(
            baseline_events + 2,
            settings,
            timeout_seconds=timeout_seconds,
        )
        final_linkage = linkage(
            settings,
            job_id=job_id,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        final_reconciliation = reconciliation(settings)
        audit = event_id_audit(settings)
        final_iceberg_rows = final_reconciliation["iceberg_rows"]
        continuity_visible = isinstance(final_iceberg_rows, list) and any(
            isinstance(row, dict)
            and row.get("event_id") == str(int_field(continuity_row, "event_id"))
            for row in final_iceberg_rows
        )

        resumed_connector = resumed_status.get("connector")
        checks = {
            "poison_was_non_avro": not malformed_bytes.startswith(b"\x00"),
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
            "dlq_preserved_original_value": encoded_original
            == base64.b64encode(malformed_bytes).decode("ascii"),
            "main_job_survived_poison": job_running_after_poison,
            "checkpoint_advanced_after_poison": int_field(checkpoint_after_poison, "id")
            > int_field(checkpoint_before, "id"),
            "repair_replayed_with_registered_avro": int_field(repair_metadata, "offset") >= 0,
            "repair_converged_before_connector_resume": reconciliation_after_repair[
                "snapshot_diff_count"
            ]
            == 0,
            "connector_resumed": (
                isinstance(resumed_connector, dict) and resumed_connector.get("state") == "RUNNING"
            ),
            "post_poison_event_flow_continued": continuity_visible,
            "final_snapshot_diff_zero": final_reconciliation["snapshot_diff_count"] == 0,
            "final_event_id_audit_consistent": audit["consistent"] is True,
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"poison/DLQ assertions failed: {checks}")

        payload = {
            **b3_base_payload(FAILURE_CLASS),
            "scenario": {
                "baseline_events": baseline_events,
                "seed": seed,
                "main_topic": settings.kafka_topic,
                "dlq_topic": settings.kafka_dlq_topic,
                "consumer_group": group_id,
            },
            "fault": {
                "mechanism": "malformed UTF-8 JSON bytes on an Avro order topic",
                "producer_metadata": poison_metadata,
                "connector_status_during_injection": paused_status,
                "malformed_sha256_not_recorded_as_plaintext": True,
            },
            "quarantine": {
                "dlq_kafka_record": dlq_records[0],
                "error_metadata": dead_letter,
                "state": "retained with repair replay recorded",
            },
            "recovery": {
                "mode": "decode intended row, re-encode registered Avro, replay with PK key",
                "repair_producer_metadata": repair_metadata,
                "connector_status_after_resume": resumed_status,
                "reconciliation_before_connector_resume": reconciliation_after_repair,
            },
            "reconciliation": final_reconciliation,
            "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            "event_id_audit": audit,
            "baseline_offset_checkpoint_snapshot_linkage": baseline_linkage,
            "repair_offset_checkpoint_snapshot_linkage": repair_linkage,
            "offset_checkpoint_snapshot_linkage": final_linkage,
            "checks": checks,
            "summary": {
                "passed": passed,
                "dlq_record_count": len(dlq_records),
                "repair_replayed": True,
                "main_pipeline_continued": continuity_visible,
                "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            },
        }
        log.write(
            "poison/DLQ passed: one record quarantined with metadata, repaired Avro replay "
            "converged, main job continued, snapshot_diff_count=0"
        )
        return finalize_result(
            RESULT_PATH,
            payload=payload,
            log=log,
            started_at=started_at,
            default_command='make broker-verify ARGS="--failure poison-dlq"',
        )
    finally:
        if connector_paused:
            try:
                set_connector_state(settings, "resume")
            except Exception as exc:
                print(f"warning: failed to resume Debezium connector: {exc}", file=sys.stderr)
        cancel_active_jobs(active_jobs, settings)


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run the Phase B3 poison-message DLQ drill.")
    parser.add_argument("--baseline-events", type=int, default=4 if smoke else 12)
    parser.add_argument("--seed", type=int, default=304)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_poison_dlq(
            baseline_events=args.baseline_events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
        )
    except Exception as exc:
        print(f"poison/DLQ drill failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
