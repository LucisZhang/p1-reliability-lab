from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from collections.abc import Sequence

from harness.broker_failure_common import (
    DrillLog,
    b3_base_payload,
    cancel_active_jobs,
    consumer_group_offsets,
    event_id_audit,
    finalize_result,
    int_field,
    kafka_compose_action,
    kafka_container_state,
    linkage,
    object_field,
    object_list_field,
    prepare_fresh_drill,
    reconciliation,
    require_new_result,
    submit_drill_job,
    topic_end_offsets,
    wait_connector_state,
    wait_for_iceberg_rows,
    wait_for_kafka_ready,
)
from harness.broker_parity import _require_checkpoint, _wait_until
from harness.config import REPO_ROOT, load_settings
from harness.generator import insert_events
from harness.provenance import utc_now

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "broker_restart_drill.json"
DEFAULT_LOG_PATH = "showcase/logs/phase-b3-broker-restart.log"
FAILURE_CLASS = "broker-restart"


def run_broker_restart(
    *,
    events: int,
    seed: int,
    timeout_seconds: int,
    checkpoint_interval_ms: int,
    outage_seconds: int,
) -> dict[str, object]:
    if events < 10:
        raise ValueError("--events must be at least 10 for a mid-stream restart")
    if outage_seconds < 1:
        raise ValueError("--outage-seconds must be positive")
    require_new_result(RESULT_PATH)
    settings = load_settings()
    started_at = utc_now()
    log = DrillLog(DEFAULT_LOG_PATH)
    active_jobs: list[str] = []
    producer_errors: list[BaseException] = []

    def produce() -> None:
        try:
            insert_events(events, seed, batch_size=1, reset=False)
        except BaseException as exc:  # captured and re-raised on the controlling thread
            producer_errors.append(exc)

    try:
        log.write("checking fresh broker profile and resetting source/Iceberg tables")
        prepare_fresh_drill(settings, log=log.write)
        group_id = f"p1-b3-broker-restart-{seed}"
        job_id = submit_drill_job(
            settings,
            group_id=group_id,
            checkpoint_interval_ms=checkpoint_interval_ms,
        )
        active_jobs.append(job_id)
        checkpoint_before = _require_checkpoint(
            job_id,
            settings=settings,
            timeout_seconds=timeout_seconds,
        )

        log.write(
            f"starting deterministic one-row batches events={events} seed={seed}; "
            "the broker will be killed while the producer is still active"
        )
        producer = threading.Thread(target=produce, name="b3-steady-ingest", daemon=True)
        producer.start()
        threshold = max(3, events // 10)

        def streaming_before_fault() -> bool:
            if producer_errors:
                raise RuntimeError(f"steady producer failed: {producer_errors[0]}")
            ends = topic_end_offsets(settings, settings.kafka_topic)
            if sum(item["log_end_offset"] for item in ends) < threshold:
                return False
            try:
                offsets = consumer_group_offsets(
                    settings,
                    group_id=group_id,
                    topic=settings.kafka_topic,
                )
            except Exception:
                return False
            return sum(item["current_offset"] for item in offsets) >= threshold

        _wait_until(
            streaming_before_fault,
            description=f"at least {threshold} records produced and checkpoint-committed",
            timeout_seconds=timeout_seconds,
            interval=1,
        )
        if not producer.is_alive():
            raise RuntimeError("deterministic producer finished before the mid-stream fault gate")

        offsets_before = consumer_group_offsets(
            settings,
            group_id=group_id,
            topic=settings.kafka_topic,
        )
        topic_before = topic_end_offsets(settings, settings.kafka_topic)
        container_before = kafka_container_state(settings)
        log.write(
            "fault injection: docker compose kill kafka during steady ingest; "
            f"committed_offset_total={sum(item['current_offset'] for item in offsets_before)}"
        )
        fault_started_at = utc_now()
        kafka_compose_action(settings, "kill")
        container_killed = kafka_container_state(settings)
        if container_killed.get("Running") is not False:
            raise RuntimeError(f"Kafka container did not stop after kill: {container_killed}")
        time.sleep(outage_seconds)
        kafka_compose_action(settings, "start")
        wait_for_kafka_ready(settings, timeout_seconds)
        connector_after_restart = wait_connector_state(
            settings,
            "RUNNING",
            timeout_seconds=timeout_seconds,
        )
        container_after = kafka_container_state(settings)
        fault_recovered_at = utc_now()

        producer.join(timeout=timeout_seconds)
        if producer.is_alive():
            raise TimeoutError("steady producer did not finish after broker recovery")
        if producer_errors:
            raise RuntimeError(f"steady producer failed: {producer_errors[0]}")

        wait_for_iceberg_rows(events, settings, timeout_seconds=timeout_seconds)
        final_linkage = linkage(
            settings,
            job_id=job_id,
            group_id=group_id,
            timeout_seconds=timeout_seconds,
        )
        checkpoint_after = object_field(final_linkage, "flink_checkpoint")
        offsets_after = object_list_field(final_linkage, "kafka_offsets")
        final_reconciliation = reconciliation(settings)
        audit = event_id_audit(settings)

        connector_after = connector_after_restart.get("connector")
        checks = {
            "broker_was_killed": container_killed.get("Running") is False,
            "broker_restarted": container_after.get("Running") is True,
            "container_start_time_changed": (
                container_before.get("StartedAt") != container_after.get("StartedAt")
            ),
            "producer_was_active_at_fault": True,
            "connector_recovered_running": (
                isinstance(connector_after, dict) and connector_after.get("state") == "RUNNING"
            ),
            "checkpoint_advanced": int_field(checkpoint_after, "id")
            > int_field(checkpoint_before, "id"),
            "committed_offsets_advanced": sum(
                int_field(item, "current_offset") for item in offsets_after
            )
            > sum(item["current_offset"] for item in offsets_before),
            "kafka_lag_zero": all(
                isinstance(item, dict) and item.get("lag") == 0 for item in offsets_after
            ),
            "snapshot_diff_zero": final_reconciliation["snapshot_diff_count"] == 0,
            "event_id_audit_consistent": audit["consistent"] is True,
        }
        passed = all(checks.values())
        if not passed:
            raise RuntimeError(f"broker restart assertions failed: {checks}")

        payload = {
            **b3_base_payload(FAILURE_CLASS),
            "scenario": {
                "events": events,
                "seed": seed,
                "checkpoint_interval_ms": checkpoint_interval_ms,
                "outage_seconds": outage_seconds,
                "topic": settings.kafka_topic,
                "consumer_group": group_id,
                "steady_ingest_batch_size": 1,
            },
            "fault": {
                "command": "docker compose ... kill kafka",
                "started_at": fault_started_at,
                "recovered_at": fault_recovered_at,
                "topic_offsets_before": topic_before,
                "consumer_offsets_before": offsets_before,
                "container_before": container_before,
                "container_killed": container_killed,
                "container_after": container_after,
            },
            "recovery": {
                "mode": "restart same Kafka container and resume Path B",
                "command": "docker compose ... start kafka",
                "connector_status": connector_after_restart,
                "checkpoint_before": {
                    "id": int_field(checkpoint_before, "id"),
                    "external_path": checkpoint_before.get("external_path"),
                },
            },
            "reconciliation": final_reconciliation,
            "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            "event_id_audit": audit,
            "offset_checkpoint_snapshot_linkage": final_linkage,
            "checks": checks,
            "summary": {
                "passed": passed,
                "pipeline_resumed": True,
                "snapshot_diff_count": final_reconciliation["snapshot_diff_count"],
            },
        }
        log.write(
            "broker restart passed: Kafka resumed, committed offsets/checkpoint advanced, "
            "lag=0, snapshot_diff_count=0"
        )
        return finalize_result(
            RESULT_PATH,
            payload=payload,
            log=log,
            started_at=started_at,
            default_command='make broker-verify ARGS="--failure broker-restart"',
        )
    finally:
        cancel_active_jobs(active_jobs, settings)


def build_parser() -> argparse.ArgumentParser:
    smoke = os.environ.get("SMOKE") == "1"
    parser = argparse.ArgumentParser(description="Run the Phase B3 Kafka restart drill.")
    parser.add_argument("--events", type=int, default=30 if smoke else 120)
    parser.add_argument("--seed", type=int, default=301)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=3000)
    parser.add_argument("--outage-seconds", type=int, default=3 if smoke else 5)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_broker_restart(
            events=args.events,
            seed=args.seed,
            timeout_seconds=args.timeout_seconds,
            checkpoint_interval_ms=args.checkpoint_interval_ms,
            outage_seconds=args.outage_seconds,
        )
    except Exception as exc:
        print(f"broker restart drill failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
