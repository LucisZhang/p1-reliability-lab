from __future__ import annotations

import pytest

from harness.broker_slo import (
    connector_state_summary,
    duplicate_replay_targets,
    extract_debezium_connector_metrics,
    extract_flink_kafka_source_metrics,
    percentile_nearest_rank,
)
from harness.checkpoint_metrics import PromMetric


def test_flink_kafka_source_metrics_filter_job_and_keep_lag_gauges() -> None:
    metrics = [
        PromMetric(
            name="flink_taskmanager_job_task_operator_KafkaSourceReader_"
            "KafkaConsumer_records_lag_max",
            labels={"job_id": "abc", "subtask_index": "0"},
            value=7.0,
        ),
        PromMetric(
            name="flink_taskmanager_job_task_operator_KafkaSourceReader_" "pendingRecords",
            labels={"job_id": "abc", "subtask_index": "0"},
            value=3.0,
        ),
        PromMetric(
            name="flink_taskmanager_job_task_operator_KafkaSourceReader_"
            "KafkaConsumer_records_lag_max",
            labels={"job_id": "another-job"},
            value=99.0,
        ),
    ]
    observed = extract_flink_kafka_source_metrics(metrics, job_id="abc")
    assert observed == {
        "present": True,
        "lag_metric_present": True,
        "metric_names": [
            "flink_taskmanager_job_task_operator_KafkaSourceReader_"
            "KafkaConsumer_records_lag_max",
            "flink_taskmanager_job_task_operator_KafkaSourceReader_pendingRecords",
        ],
        "metrics": [
            {
                "name": "flink_taskmanager_job_task_operator_KafkaSourceReader_"
                "KafkaConsumer_records_lag_max",
                "labels": {"job_id": "abc", "subtask_index": "0"},
                "value": 7.0,
            },
            {
                "name": "flink_taskmanager_job_task_operator_KafkaSourceReader_pendingRecords",
                "labels": {"job_id": "abc", "subtask_index": "0"},
                "value": 3.0,
            },
        ],
    }


def test_debezium_metrics_are_named_from_jmx_exporter_output() -> None:
    metrics = [
        PromMetric(
            name="debezium_mysql_connector_metrics_streaming_totalnumberofeventsseen",
            labels={"server": "broker"},
            value=100_000.0,
        ),
        PromMetric(
            name="debezium_mysql_connector_metrics_streaming_millisecondssincelastevent",
            labels={"server": "broker"},
            value=12.0,
        ),
    ]
    observed = extract_debezium_connector_metrics(metrics)
    assert observed["present"] is True
    assert observed["metric_count"] == 2
    assert observed["values"] == {
        "total_events_seen": 100_000.0,
        "milliseconds_since_last_event": 12.0,
    }


def test_nearest_rank_percentile_is_deterministic() -> None:
    values = [float(value) for value in range(1, 101)]
    assert percentile_nearest_rank(values, 50) == 50.0
    assert percentile_nearest_rank(values, 95) == 95.0
    with pytest.raises(ValueError, match="at least one"):
        percentile_nearest_rank([], 95)


def test_duplicate_replay_targets_include_preexisting_broker_restart_redelivery() -> None:
    assert duplicate_replay_targets(
        initial_changelog_count=100_252,
        initial_duplicate_occurrences=252,
        replay_record_count=100_252,
    ) == {
        "expected_changelog_count": 200_504,
        "expected_duplicate_occurrences": 100_504,
    }
    with pytest.raises(ValueError, match="non-negative"):
        duplicate_replay_targets(
            initial_changelog_count=-1,
            initial_duplicate_occurrences=0,
            replay_record_count=1,
        )


def test_connector_state_summary_keeps_states_without_leaking_trace_config() -> None:
    observed = connector_state_summary(
        {
            "connector": {"state": "RUNNING"},
            "tasks": [
                {
                    "id": 0,
                    "state": "FAILED",
                    "trace": ("java.lang.IllegalStateException: config={database.password=secret}"),
                }
            ],
        }
    )
    assert observed == {
        "connector_state": "RUNNING",
        "tasks": [
            {
                "id": 0,
                "state": "FAILED",
                "failure_type": "java.lang.IllegalStateException",
            }
        ],
    }
    assert "secret" not in str(observed)
