from __future__ import annotations

import json

import pytest

from harness.schema_contract_drill import (
    mutate_event_id_type,
    normalize_contract_group_offsets,
    registry_subject,
    schema_sha256,
)


def test_registry_subject_uses_topic_name_strategy() -> None:
    assert registry_subject("broker.cdc_lab.orders", "key") == "broker.cdc_lab.orders-key"
    assert registry_subject("broker.cdc_lab.orders", "value") == "broker.cdc_lab.orders-value"
    with pytest.raises(ValueError, match="key or value"):
        registry_subject("broker.cdc_lab.orders", "headers")


def test_contract_lag_normalizes_only_omitted_empty_partitions() -> None:
    described = [
        {"partition": 0, "current_offset": 1, "log_end_offset": 1, "lag": 0},
        {"partition": 1, "current_offset": 1, "log_end_offset": 1, "lag": 0},
    ]
    topic_ends = [
        {"partition": 0, "log_end_offset": 1},
        {"partition": 1, "log_end_offset": 1},
        {"partition": 2, "log_end_offset": 0},
    ]

    assert normalize_contract_group_offsets(described, topic_ends) == [
        described[0],
        described[1],
        {"partition": 2, "current_offset": 0, "log_end_offset": 0, "lag": 0},
    ]


def test_contract_lag_rejects_omitted_nonempty_partition() -> None:
    described = [
        {"partition": 0, "current_offset": 1, "log_end_offset": 1, "lag": 0},
    ]
    topic_ends = [
        {"partition": 0, "log_end_offset": 1},
        {"partition": 1, "log_end_offset": 1},
    ]

    assert normalize_contract_group_offsets(described, topic_ends) == []


def test_mutation_changes_nested_event_id_and_is_canonical() -> None:
    original = json.dumps(
        {
            "type": "record",
            "name": "Envelope",
            "fields": [
                {
                    "name": "after",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "Order",
                            "fields": [
                                {"name": "event_id", "type": "long"},
                                {"name": "status", "type": "string"},
                            ],
                        },
                    ],
                }
            ],
        }
    )

    mutated, count = mutate_event_id_type(original)
    parsed = json.loads(mutated)
    assert count == 1
    assert parsed["fields"][0]["type"][1]["fields"][0]["type"] == "string"
    assert schema_sha256(mutated) == schema_sha256(json.dumps(parsed, indent=2))


def test_mutation_refuses_schema_without_event_id() -> None:
    with pytest.raises(ValueError, match="no event_id"):
        mutate_event_id_type('{"type":"record","name":"R","fields":[]}')
