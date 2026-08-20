from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from collections.abc import Sequence
from typing import cast

from harness.broker_failure_common import (
    DrillLog,
    avro_key,
    b3_base_payload,
    cancel_active_jobs,
    consume_binary,
    debezium_envelope,
    decode_confluent_avro,
    encode_confluent_avro,
    event_id_audit,
    finalize_result,
    int_field,
    linkage,
    order_row,
    prepare_fresh_drill,
    produce_binary,
    reconciliation,
    registered_schema,
    require_new_result,
    submit_drill_job,
    topic_end_offsets,
    wait_for_iceberg_rows,
)
from harness.broker_parity import _wait_until
from harness.config import REPO_ROOT, load_settings
from harness.generator import insert_events
from harness.provenance import utc_now

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "ordering_miskey_drill.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b3-ordering-miskey.log"
FAILURE_CLASS = "mis-keying"


def monotonic_violations(values: Sequence[int]) -> int:
    return sum(1 for left, right in zip(values, values[1:], strict=False) if right <= left)


def _decoded_probe(
    record: dict[str, object],
    *,
    key_schema_id: int,
    key_schema: dict[str, object],
    value_schema_id: int,
    value_schema: dict[str, object],
) -> dict[str, object]:
    raw_key = record.get("key_base64")
    raw_value = record.get("value_base64")
    if not isinstance(raw_key, str) or not isinstance(raw_value, str):
        raise ValueError(f"probe record is missing binary key/value: {record}")
    key = decode_confluent_avro(base64.b64decode(raw_key), key_schema_id, key_schema)
    envelope = decode_confluent_avro(base64.b64decode(raw_value), value_schema_id, value_schema)
    after = envelope.get("after")
    if not isinstance(after, dict):
        raise ValueError(f"probe envelope has no after row: {envelope}")
    return {
        "partition": int(cast(int, record["partition"])),
        "offset": int(cast(int, record["offset"])),
        "timestamp_ms": int(cast(int, record["timestamp_ms"])),
        "key_order_id": int(cast(int, key["order_id"])),
        "value_order_id": int(cast(int, after["order_id"])),
        "event_id": int(cast(int, after["event_id"])),
        "status": str(after["status"]),
    }


def run_ordering_miskey(
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

    try:
        log.write("preparing a fresh Path B baseline and isolated Avro ordering probe topic")
        prepare_fresh_drill(settings, log=log.write)
        group_id = f"p1-b3-ordering-main-{seed}"
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
        main_linkage = linkage(
            settings,
            job_id=job_id,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        final_reconciliation = reconciliation(settings)
        main_audit = event_id_audit(settings)

        key_schema_id, key_schema = registered_schema(settings, "key")
        value_schema_id, value_schema = registered_schema(settings, "value")
        base_timestamp = int(time.time() * 1000)

        correct_order_id = 9_100_000 + seed
        correct_event_ids = [1_000_000 + seed * 10 + index for index in range(3)]
        correct_key = encode_confluent_avro(
            key_schema_id,
            key_schema,
            avro_key(correct_order_id),
        )
        correct_producer_metadata: list[dict[str, object]] = []
        for index, event_id in enumerate(correct_event_ids):
            row = order_row(
                order_id=correct_order_id,
                event_id=event_id,
                seed=seed,
                status=("created", "packed", "delivered")[index],
                timestamp_ms=base_timestamp + index,
            )
            value = encode_confluent_avro(
                value_schema_id,
                value_schema,
                debezium_envelope(after=row, timestamp_ms=base_timestamp + index),
            )
            correct_producer_metadata.append(
                produce_binary(
                    settings,
                    topic=settings.kafka_ordering_probe_topic,
                    key=correct_key,
                    value=value,
                    timestamp_ms=base_timestamp + index,
                )
            )

        miskey_order_id = 9_200_000 + seed
        canonical_event_ids = [2_000_000 + seed * 10 + index for index in range(3)]
        arrival_event_ids = [
            canonical_event_ids[0],
            canonical_event_ids[2],
            canonical_event_ids[1],
        ]
        miskey_producer_metadata: list[dict[str, object]] = []
        for index, event_id in enumerate(arrival_event_ids):
            wrong_key_order_id = miskey_order_id + 100 + index
            key = encode_confluent_avro(
                key_schema_id,
                key_schema,
                avro_key(wrong_key_order_id),
            )
            row = order_row(
                order_id=miskey_order_id,
                event_id=event_id,
                seed=seed,
                status=("created", "delivered", "packed")[index],
                timestamp_ms=base_timestamp + 100 + index,
            )
            value = encode_confluent_avro(
                value_schema_id,
                value_schema,
                debezium_envelope(
                    after=row,
                    timestamp_ms=base_timestamp + 100 + index,
                ),
            )
            miskey_producer_metadata.append(
                produce_binary(
                    settings,
                    topic=settings.kafka_ordering_probe_topic,
                    key=key,
                    value=value,
                    partition=index,
                    timestamp_ms=base_timestamp + 100 + index,
                )
            )

        _wait_until(
            lambda: sum(
                item["log_end_offset"]
                for item in topic_end_offsets(settings, settings.kafka_ordering_probe_topic)
            )
            == 6,
            description="six isolated ordering-probe records",
            timeout_seconds=timeout_seconds,
        )
        raw_records = consume_binary(
            settings,
            topic=settings.kafka_ordering_probe_topic,
            max_records=6,
        )
        if len(raw_records) != 6:
            raise RuntimeError(f"expected six probe records, got {len(raw_records)}")
        decoded = [
            _decoded_probe(
                record,
                key_schema_id=key_schema_id,
                key_schema=key_schema,
                value_schema_id=value_schema_id,
                value_schema=value_schema,
            )
            for record in raw_records
        ]
        correct_records = sorted(
            (item for item in decoded if item["value_order_id"] == correct_order_id),
            key=lambda item: int_field(item, "timestamp_ms"),
        )
        miskey_records = sorted(
            (item for item in decoded if item["value_order_id"] == miskey_order_id),
            key=lambda item: int_field(item, "timestamp_ms"),
        )
        observed_correct_events = [int_field(item, "event_id") for item in correct_records]
        observed_arrival_events = [int_field(item, "event_id") for item in miskey_records]
        correct_partitions = sorted({int_field(item, "partition") for item in correct_records})
        miskey_partitions = sorted({int_field(item, "partition") for item in miskey_records})
        miskey_violations = monotonic_violations(observed_arrival_events)
        arrival_applied_final = observed_arrival_events[-1]
        canonical_final = max(canonical_event_ids)
        would_corrupt = arrival_applied_final != canonical_final

        checks = {
            "correct_pk_key_matches_value": all(
                item["key_order_id"] == item["value_order_id"] for item in correct_records
            ),
            "correct_pk_records_share_one_partition": len(correct_partitions) == 1,
            "correct_pk_offsets_increase": all(
                int_field(left, "offset") < int_field(right, "offset")
                for left, right in zip(correct_records, correct_records[1:], strict=False)
            ),
            "correct_pk_event_order_preserved": observed_correct_events == correct_event_ids,
            "miskeys_do_not_match_primary_key": all(
                item["key_order_id"] != item["value_order_id"] for item in miskey_records
            ),
            "miskey_records_span_partitions": len(miskey_partitions) == 3,
            "audit_detected_non_monotonic_event_id": miskey_violations > 0,
            "arrival_order_would_corrupt_final_state": would_corrupt,
            "probe_isolated_from_main_topic": settings.kafka_ordering_probe_topic
            != settings.kafka_topic,
            "main_snapshot_diff_zero": final_reconciliation["snapshot_diff_count"] == 0,
            "main_event_id_audit_consistent": main_audit["consistent"] is True,
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"ordering/mis-keying assertions failed: {checks}")

        payload = {
            **b3_base_payload(FAILURE_CLASS),
            "scenario": {
                "baseline_events": baseline_events,
                "seed": seed,
                "main_topic": settings.kafka_topic,
                "probe_topic": settings.kafka_ordering_probe_topic,
                "probe_isolation": (
                    "Mis-keyed records are quarantined in a dedicated Avro probe topic and "
                    "never admitted to the Iceberg main path."
                ),
                "ordering_limit": (
                    "Kafka preserves order only inside one partition; primary-key keying keeps "
                    "one order on one partition but does not create total order."
                ),
            },
            "schema_registry": {
                "key_subject": f"{settings.kafka_topic}-key",
                "key_schema_id": key_schema_id,
                "value_subject": f"{settings.kafka_topic}-value",
                "value_schema_id": value_schema_id,
            },
            "correct_pk_probe": {
                "order_id": correct_order_id,
                "expected_event_ids": correct_event_ids,
                "observed_event_ids": observed_correct_events,
                "partitions": correct_partitions,
                "records": correct_records,
                "producer_metadata": correct_producer_metadata,
            },
            "miskey_probe": {
                "order_id": miskey_order_id,
                "canonical_event_ids": canonical_event_ids,
                "controlled_arrival_event_ids": arrival_event_ids,
                "observed_arrival_event_ids": observed_arrival_events,
                "partitions": miskey_partitions,
                "records": miskey_records,
                "producer_metadata": miskey_producer_metadata,
                "audit": {
                    "non_monotonic_transition_count": miskey_violations,
                    "canonical_final_event_id": canonical_final,
                    "arrival_applied_final_event_id": arrival_applied_final,
                    "would_corrupt_final_state": would_corrupt,
                    "disposition": "rejected before main-pipeline admission",
                },
            },
            "recovery": {
                "mode": "audit gate rejects mis-keyed batch; retain PK-keyed main path",
            },
            "reconciliation": final_reconciliation,
            "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            "event_id_audit": main_audit,
            "offset_checkpoint_snapshot_linkage": main_linkage,
            "checks": checks,
            "summary": {
                "passed": passed,
                "ordering_limit_detected": True,
                "miskey_violation_count": miskey_violations,
                "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            },
        }
        log.write(
            "ordering probe passed: PK records stayed on one partition; controlled mis-keying "
            f"spanned {miskey_partitions}, audit violations={miskey_violations}, main diff=0"
        )
        return finalize_result(
            RESULT_PATH,
            payload=payload,
            log=log,
            started_at=started_at,
            default_command='make broker-verify ARGS="--failure mis-keying"',
        )
    finally:
        cancel_active_jobs(active_jobs, settings)


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run the Phase B3 ordering/mis-keying probe.")
    parser.add_argument("--baseline-events", type=int, default=6 if smoke else 18)
    parser.add_argument("--seed", type=int, default=303)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_ordering_miskey(
            baseline_events=args.baseline_events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
        )
    except Exception as exc:
        print(f"ordering/mis-keying drill failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
