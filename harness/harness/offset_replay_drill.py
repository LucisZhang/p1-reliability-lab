from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from typing import cast

from harness.broker_failure_common import (
    DrillLog,
    b3_base_payload,
    cancel_active_jobs,
    event_id_audit,
    finalize_result,
    linkage,
    object_list_field,
    offsets_for_timestamp,
    prepare_fresh_drill,
    reconciliation,
    require_new_result,
    rows_match,
    submit_drill_job,
    wait_for_iceberg_rows,
    wait_for_job_absent,
)
from harness.broker_parity import (
    Row,
    _iceberg_rows,
    _mysql,
    _wait_until,
    row_diff,
    snapshot_digest,
)
from harness.config import REPO_ROOT, load_settings
from harness.flink import cancel_job, reset_iceberg_tables
from harness.generator import insert_events
from harness.provenance import utc_now

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "offset_replay_drill.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b3-offset-replay.log"
FAILURE_CLASS = "offset-replay"
REPLAY_COALESCE_MS = 10_000


def _wait_for_convergence(events: int, settings: object, timeout_seconds: int) -> None:
    # The runtime object is a concrete Settings instance; keeping this helper local avoids
    # exposing replay-only policy in the shared B1 utilities.
    from harness.config import Settings

    active = cast(Settings, settings)

    def converged() -> bool:
        observed = reconciliation(active)
        return (
            observed["source_snapshot_row_count"] == events
            and observed["iceberg_snapshot_row_count"] == events
            and observed["snapshot_diff_count"] == 0
        )

    _wait_until(
        converged,
        description=f"source/Iceberg convergence for {events} replayed rows",
        timeout_seconds=timeout_seconds,
        interval=2,
    )


def run_offset_replay(
    *,
    events: int,
    seed: int,
    timeout_seconds: int,
    checkpoint_interval_ms: int,
) -> dict[str, object]:
    if events < 6:
        raise ValueError("--events must be at least 6 for a partitioned timestamp replay")
    require_new_result(RESULT_PATH)
    settings = load_settings()
    started_at = utc_now()
    log = DrillLog(DEFAULT_LOG_PATH)
    active_jobs: list[str] = []

    try:
        log.write("preparing fresh topic and original Path B table for replay/backfill")
        prepare_fresh_drill(settings, log=log.write)
        original_group = f"p1-b3-replay-original-{seed}"
        original_job = submit_drill_job(
            settings,
            group_id=original_group,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        active_jobs.append(original_job)
        insert_events(events, seed, batch_size=min(100, events), reset=False)
        wait_for_iceberg_rows(events, settings, timeout_seconds=timeout_seconds)
        baseline_linkage = linkage(
            settings,
            job_id=original_job,
            group_id=original_group,
            timeout_seconds=timeout_seconds,
        )

        chosen_timestamp_ms = int(time.time() * 1000)
        log.write(
            "chosen timestamp captured after baseline lag=0; applying a complete update sweep "
            "so records at/after the timestamp contain one current image for every key"
        )
        _mysql(
            """
            UPDATE orders
               SET event_id = event_id + 1000000,
                   status = 'replayed',
                   amount_cents = amount_cents + 7,
                   updated_at = DATE_ADD(updated_at, INTERVAL 1 DAY);
            """,
            settings,
        )
        _wait_for_convergence(events, settings, timeout_seconds)
        original_linkage = linkage(
            settings,
            job_id=original_job,
            group_id=original_group,
            timeout_seconds=timeout_seconds,
        )
        original_reconciliation = reconciliation(settings)
        original_rows = cast(list[Row], original_reconciliation["iceberg_rows"])
        original_digest = snapshot_digest(original_rows)
        timestamp_offsets = offsets_for_timestamp(
            settings,
            topic=settings.kafka_topic,
            timestamp_ms=chosen_timestamp_ms,
        )

        cancel_job(original_job, settings=settings)
        active_jobs.remove(original_job)
        wait_for_job_absent(original_job, settings)
        reset_iceberg_tables(settings=settings)

        log.write("rebuilding freshly recreated Iceberg tables from topic offset 0")
        offset_zero_group = f"p1-b3-replay-offset-zero-{seed}"
        offset_zero_job = submit_drill_job(
            settings,
            group_id=offset_zero_group,
            checkpoint_interval_ms=checkpoint_interval_ms,
            starting_offsets="earliest",
            replay_coalesce_ms=REPLAY_COALESCE_MS,
        )
        active_jobs.append(offset_zero_job)
        _wait_for_convergence(events, settings, timeout_seconds)
        offset_zero_linkage = linkage(
            settings,
            job_id=offset_zero_job,
            group_id=offset_zero_group,
            timeout_seconds=timeout_seconds,
        )
        offset_zero_rows = _iceberg_rows(settings)
        offset_zero_diff = row_diff(original_rows, offset_zero_rows)
        offset_zero_digest = snapshot_digest(offset_zero_rows)

        cancel_job(offset_zero_job, settings=settings)
        active_jobs.remove(offset_zero_job)
        wait_for_job_absent(offset_zero_job, settings)
        reset_iceberg_tables(settings=settings)

        log.write(
            f"rebuilding freshly recreated Iceberg tables from timestamp {chosen_timestamp_ms}"
        )
        timestamp_group = f"p1-b3-replay-timestamp-{seed}"
        timestamp_job = submit_drill_job(
            settings,
            group_id=timestamp_group,
            checkpoint_interval_ms=checkpoint_interval_ms,
            starting_offsets="timestamp",
            start_timestamp_ms=chosen_timestamp_ms,
            replay_coalesce_ms=REPLAY_COALESCE_MS,
        )
        active_jobs.append(timestamp_job)
        _wait_for_convergence(events, settings, timeout_seconds)
        timestamp_linkage = linkage(
            settings,
            job_id=timestamp_job,
            group_id=timestamp_group,
            timeout_seconds=timeout_seconds,
        )
        timestamp_rows = _iceberg_rows(settings)
        timestamp_diff = row_diff(original_rows, timestamp_rows)
        timestamp_digest = snapshot_digest(timestamp_rows)
        final_reconciliation = reconciliation(settings)
        audit = event_id_audit(settings)

        nonempty_timestamp_partitions = [
            item for item in timestamp_offsets if item["log_end_offset"] > 0
        ]
        baseline_offsets = object_list_field(baseline_linkage, "kafka_offsets")
        checks = {
            "baseline_converged_before_timestamp": all(
                item.get("lag") == 0 for item in baseline_offsets
            ),
            "complete_update_sweep_changed_every_event_id": all(
                int(row["event_id"]) > 1_000_000 for row in original_rows
            ),
            "timestamp_resolved_after_offset_zero": bool(nonempty_timestamp_partitions)
            and all(item["resolved_start_offset"] > 0 for item in nonempty_timestamp_partitions),
            "offset_zero_rebuild_matches_original": rows_match(original_rows, offset_zero_rows),
            "offset_zero_digest_matches": offset_zero_digest == original_digest,
            "timestamp_rebuild_matches_original": rows_match(original_rows, timestamp_rows),
            "timestamp_digest_matches": timestamp_digest == original_digest,
            "fresh_snapshot_lineages_recorded": (
                original_linkage["iceberg_snapshot_ids"]
                != offset_zero_linkage["iceberg_snapshot_ids"]
                and offset_zero_linkage["iceberg_snapshot_ids"]
                != timestamp_linkage["iceberg_snapshot_ids"]
            ),
            "final_source_snapshot_diff_zero": final_reconciliation["snapshot_diff_count"] == 0,
            "final_event_id_audit_consistent": audit["consistent"] is True,
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"offset replay assertions failed: {checks}")

        payload = {
            **b3_base_payload(FAILURE_CLASS),
            "scenario": {
                "events": events,
                "seed": seed,
                "topic": settings.kafka_topic,
                "chosen_timestamp_ms": chosen_timestamp_ms,
                "timestamp_precondition": (
                    "A complete update sweep after the chosen timestamp carries a current image "
                    "for every primary key, so timestamp-only rebuild is complete."
                ),
                "fresh_table_method": (
                    "drop/recreate both logical Iceberg tables between runs, producing new "
                    "snapshot lineages while retaining the captured original rows/digest"
                ),
                "current_table_replay_policy": (
                    "Path B replay-only latest-per-primary-key coalescing with a "
                    f"{REPLAY_COALESCE_MS} ms quiet period; normal streaming remains unchanged"
                ),
            },
            "original": {
                "job_id": original_job,
                "snapshot_sha256": original_digest,
                "rows": original_rows,
                "reconciliation": original_reconciliation,
                "offset_checkpoint_snapshot_linkage": original_linkage,
            },
            "offset_zero_replay": {
                "job_id": offset_zero_job,
                "consumer_group": offset_zero_group,
                "starting_offsets": "earliest (offset 0)",
                "snapshot_sha256": offset_zero_digest,
                "row_level_diff_count": sum(len(rows) for rows in offset_zero_diff.values()),
                "row_level_diff": offset_zero_diff,
                "offset_checkpoint_snapshot_linkage": offset_zero_linkage,
            },
            "timestamp_replay": {
                "job_id": timestamp_job,
                "consumer_group": timestamp_group,
                "requested_timestamp_ms": chosen_timestamp_ms,
                "resolved_partition_offsets": timestamp_offsets,
                "snapshot_sha256": timestamp_digest,
                "row_level_diff_count": sum(len(rows) for rows in timestamp_diff.values()),
                "row_level_diff": timestamp_diff,
                "offset_checkpoint_snapshot_linkage": timestamp_linkage,
            },
            "recovery": {
                "mode": "fresh-table rebuild from offset 0 and chosen timestamp",
            },
            "reconciliation": final_reconciliation,
            "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            "event_id_audit": audit,
            "offset_checkpoint_snapshot_linkage": timestamp_linkage,
            "checks": checks,
            "summary": {
                "passed": passed,
                "offset_zero_diff_count": sum(len(rows) for rows in offset_zero_diff.values()),
                "timestamp_diff_count": sum(len(rows) for rows in timestamp_diff.values()),
                "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            },
        }
        log.write(
            "offset replay passed: fresh offset-0 and post-sweep timestamp rebuilds both "
            "matched the captured original row-for-row and by digest"
        )
        return finalize_result(
            RESULT_PATH,
            payload=payload,
            log=log,
            started_at=started_at,
            default_command='make broker-verify ARGS="--failure offset-replay"',
        )
    finally:
        cancel_active_jobs(active_jobs, settings)


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run the Phase B3 offset/timestamp replay drill.")
    parser.add_argument("--events", type=int, default=12 if smoke else 36)
    parser.add_argument("--seed", type=int, default=305)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_offset_replay(
            events=args.events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
        )
    except Exception as exc:
        print(f"offset replay drill failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
