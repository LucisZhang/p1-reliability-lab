from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence

from harness.broker_failure_common import (
    CHANGELOG_TABLE,
    DrillLog,
    b3_base_payload,
    cancel_active_jobs,
    event_id_audit,
    finalize_result,
    int_field,
    linkage,
    object_list_field,
    prepare_fresh_drill,
    reconciliation,
    require_new_result,
    reset_group_to_earliest,
    submit_drill_job,
    wait_for_iceberg_rows,
    wait_for_job_absent,
)
from harness.broker_parity import _iceberg_scalar, _wait_until
from harness.config import REPO_ROOT, load_settings
from harness.flink import cancel_job
from harness.generator import insert_events
from harness.provenance import utc_now

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "duplicate_redelivery_drill.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b3-duplicate-redelivery.log"
FAILURE_CLASS = "duplicate-redelivery"


def run_duplicate_redelivery(
    *,
    events: int,
    seed: int,
    timeout_seconds: int,
    checkpoint_interval_ms: int,
) -> dict[str, object]:
    if events < 1:
        raise ValueError("--events must be positive")
    require_new_result(RESULT_PATH)
    settings = load_settings()
    started_at = utc_now()
    log = DrillLog(DEFAULT_LOG_PATH)
    active_jobs: list[str] = []

    try:
        log.write("checking fresh broker profile and preparing duplicate-redelivery workload")
        prepare_fresh_drill(settings, log=log.write)
        group_id = f"p1-b3-duplicate-redelivery-{seed}"
        first_job = submit_drill_job(
            settings,
            group_id=group_id,
            checkpoint_interval_ms=checkpoint_interval_ms,
            starting_offsets="committed",
        )
        active_jobs.append(first_job)
        insert_events(events, seed, batch_size=min(events, 100), reset=False)
        wait_for_iceberg_rows(events, settings, timeout_seconds=timeout_seconds)
        first_linkage = linkage(
            settings,
            job_id=first_job,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        first_changelog_count = _iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings)
        if first_changelog_count != events:
            raise RuntimeError(
                f"first delivery changelog count={first_changelog_count}, expected {events}"
            )

        log.write(f"forcing redelivery: cancel job {first_job}, rewind consumer group to earliest")
        cancel_job(first_job, settings=settings)
        active_jobs.remove(first_job)
        wait_for_job_absent(first_job, settings)
        reset_rows = reset_group_to_earliest(
            settings,
            group_id=group_id,
            topic=settings.kafka_topic,
        )
        if any(int_field(row, "new_offset") != 0 for row in reset_rows):
            raise RuntimeError(f"consumer group did not rewind to offset 0: {reset_rows}")

        second_job = submit_drill_job(
            settings,
            group_id=group_id,
            checkpoint_interval_ms=checkpoint_interval_ms,
            starting_offsets="committed",
        )
        active_jobs.append(second_job)
        _wait_until(
            lambda: _iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings)
            == events * 2,
            description=f"redelivered changelog row count {events * 2}",
            timeout_seconds=timeout_seconds,
            interval=2,
        )
        second_linkage = linkage(
            settings,
            job_id=second_job,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        final_reconciliation = reconciliation(settings)
        audit = event_id_audit(settings)
        duplicate_rows = audit["duplicate_event_ids"]
        duplicate_count = int_field(audit, "duplicate_occurrence_count")
        duplicated_distinct = len(duplicate_rows) if isinstance(duplicate_rows, list) else -1

        second_offsets = object_list_field(second_linkage, "kafka_offsets")
        checks = {
            "group_rewound_to_offset_zero": all(
                int_field(row, "new_offset") == 0 for row in reset_rows
            ),
            "all_event_ids_redelivered": duplicated_distinct == events,
            "duplicate_occurrence_count_exact": duplicate_count == events,
            "changelog_contains_both_deliveries": audit["changelog_row_count"] == events * 2,
            "current_table_remains_unique": final_reconciliation["iceberg_snapshot_row_count"]
            == events,
            "snapshot_diff_zero": final_reconciliation["snapshot_diff_count"] == 0,
            "event_id_audit_consistent": audit["consistent"] is True,
            "kafka_lag_zero": all(item.get("lag") == 0 for item in second_offsets),
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"duplicate redelivery assertions failed: {checks}")

        payload = {
            **b3_base_payload(FAILURE_CLASS),
            "scenario": {
                "events": events,
                "seed": seed,
                "checkpoint_interval_ms": checkpoint_interval_ms,
                "topic": settings.kafka_topic,
                "consumer_group": group_id,
            },
            "fault": {
                "mechanism": "consumer-group offset rewind",
                "command": ("kafka-consumer-groups.sh --reset-offsets --to-earliest --execute"),
                "reset_offsets": reset_rows,
                "first_delivery_linkage": first_linkage,
            },
            "recovery": {
                "mode": "restart Path B from rewound committed offsets",
                "first_job_id": first_job,
                "redelivery_job_id": second_job,
            },
            "reconciliation": final_reconciliation,
            "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            "event_id_audit": audit,
            "duplicates_detected": {
                "distinct_event_ids": duplicated_distinct,
                "duplicate_occurrence_count": duplicate_count,
                "expected_duplicate_occurrence_count": events,
            },
            "offset_checkpoint_snapshot_linkage": second_linkage,
            "checks": checks,
            "summary": {
                "passed": passed,
                "duplicates_detected": duplicate_count,
                "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            },
        }
        log.write(
            f"duplicate redelivery passed: duplicates_detected={duplicate_count}, "
            "current-table snapshot_diff_count=0"
        )
        return finalize_result(
            RESULT_PATH,
            payload=payload,
            log=log,
            started_at=started_at,
            default_command='make broker-verify ARGS="--failure duplicate-redelivery"',
        )
    finally:
        cancel_active_jobs(active_jobs, settings)


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run the Phase B3 duplicate-redelivery drill.")
    parser.add_argument("--events", type=int, default=12 if smoke else 36)
    parser.add_argument("--seed", type=int, default=302)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_duplicate_redelivery(
            events=args.events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
        )
    except Exception as exc:
        print(f"duplicate redelivery drill failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
