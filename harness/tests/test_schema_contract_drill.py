from __future__ import annotations

import json

import pytest

from harness.schema_contract_drill import mutate_event_id_type, registry_subject, schema_sha256


def test_registry_subject_uses_topic_name_strategy() -> None:
    assert registry_subject("broker.cdc_lab.orders", "key") == "broker.cdc_lab.orders-key"
    assert registry_subject("broker.cdc_lab.orders", "value") == "broker.cdc_lab.orders-value"
    with pytest.raises(ValueError, match="key or value"):
        registry_subject("broker.cdc_lab.orders", "headers")


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
