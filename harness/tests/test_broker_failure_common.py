from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from harness.broker_failure_common import (
    decode_confluent_avro,
    encode_confluent_avro,
    parse_reset_offsets,
    set_connector_state,
)
from harness.config import Settings
from harness.ordering_miskey_drill import monotonic_violations


def test_parse_reset_offsets_is_partition_sorted() -> None:
    output = """
GROUP                         TOPIC                    PARTITION  NEW-OFFSET
p1-b3-duplicate-redelivery   broker.cdc_lab.orders    2          0
p1-b3-duplicate-redelivery   broker.cdc_lab.orders    0          0
p1-b3-duplicate-redelivery   broker.cdc_lab.orders    1          0
"""
    assert parse_reset_offsets(
        output,
        group_id="p1-b3-duplicate-redelivery",
        topic="broker.cdc_lab.orders",
    ) == [
        {
            "group": "p1-b3-duplicate-redelivery",
            "topic": "broker.cdc_lab.orders",
            "partition": 0,
            "new_offset": 0,
        },
        {
            "group": "p1-b3-duplicate-redelivery",
            "topic": "broker.cdc_lab.orders",
            "partition": 1,
            "new_offset": 0,
        },
        {
            "group": "p1-b3-duplicate-redelivery",
            "topic": "broker.cdc_lab.orders",
            "partition": 2,
            "new_offset": 0,
        },
    ]


def test_parse_reset_offsets_accepts_kafka_392_single_line_output() -> None:
    output = (
        "GROUP TOPIC PARTITION NEW-OFFSET "
        "p1-b3-duplicate-redelivery-302 broker.cdc_lab.orders 0 0 "
        "p1-b3-duplicate-redelivery-302 broker.cdc_lab.orders 1 0 "
        "p1-b3-duplicate-redelivery-302 broker.cdc_lab.orders 2 0"
    )
    assert parse_reset_offsets(
        output,
        group_id="p1-b3-duplicate-redelivery-302",
        topic="broker.cdc_lab.orders",
    ) == [
        {
            "group": "p1-b3-duplicate-redelivery-302",
            "topic": "broker.cdc_lab.orders",
            "partition": 0,
            "new_offset": 0,
        },
        {
            "group": "p1-b3-duplicate-redelivery-302",
            "topic": "broker.cdc_lab.orders",
            "partition": 1,
            "new_offset": 0,
        },
        {
            "group": "p1-b3-duplicate-redelivery-302",
            "topic": "broker.cdc_lab.orders",
            "partition": 2,
            "new_offset": 0,
        },
    ]


def test_connector_pause_uses_json_media_type() -> None:
    settings = cast(
        Settings,
        SimpleNamespace(
            debezium_connect_host="127.0.0.1",
            debezium_connect_port=8083,
            debezium_connector_name="p1-orders-connector",
        ),
    )
    with patch("harness.broker_failure_common.urlopen") as open_url:
        set_connector_state(settings, "pause")

    request = open_url.call_args.args[0]
    assert request.get_method() == "PUT"
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("Accept") == "application/json"


def test_confluent_avro_wire_round_trip_preserves_schema_id() -> None:
    schema: dict[str, object] = {
        "type": "record",
        "name": "Probe",
        "fields": [{"name": "event_id", "type": "long"}],
    }
    payload = encode_confluent_avro(42, schema, {"event_id": 99})
    assert payload[:5] == b"\x00\x00\x00\x00*"
    assert decode_confluent_avro(payload, 42, schema) == {"event_id": 99}


def test_ordering_audit_counts_non_monotonic_transitions() -> None:
    assert monotonic_violations([100, 101, 102]) == 0
    assert monotonic_violations([100, 102, 101]) == 1
