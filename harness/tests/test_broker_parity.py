from __future__ import annotations

import pytest

from harness.broker_parity import (
    diff_count,
    linkage_complete,
    parse_consumer_group_offsets,
    parse_topic_end_offsets,
    row_diff,
    snapshot_digest,
)


def test_parse_topic_end_offsets() -> None:
    output = "\n".join(
        [
            "broker.cdc_lab.orders:2:5",
            "broker.cdc_lab.orders:0:7",
            "broker.cdc_lab.orders:1:3",
        ]
    )
    assert parse_topic_end_offsets(output, topic="broker.cdc_lab.orders") == [
        {"partition": 0, "log_end_offset": 7},
        {"partition": 1, "log_end_offset": 3},
        {"partition": 2, "log_end_offset": 5},
    ]


def test_parse_consumer_group_offsets_and_linkage() -> None:
    output = """
GROUP                 TOPIC                   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
p1-broker-parity-b1  broker.cdc_lab.orders   1          3               3               0
p1-broker-parity-b1  broker.cdc_lab.orders   0          7               7               0
"""
    offsets = parse_consumer_group_offsets(
        output,
        group="p1-broker-parity-b1",
        topic="broker.cdc_lab.orders",
    )
    assert offsets == [
        {"partition": 0, "current_offset": 7, "log_end_offset": 7, "lag": 0},
        {"partition": 1, "current_offset": 3, "log_end_offset": 3, "lag": 0},
    ]
    assert linkage_complete(
        {
            "kafka_offsets": offsets,
            "flink_checkpoint": {"id": 12},
            "iceberg_snapshot_ids": {"orders_current": 101, "orders_changelog": 102},
        }
    )


def test_parse_consumer_group_offsets_rejects_uncommitted_partition() -> None:
    with pytest.raises(ValueError, match="no committed offset"):
        parse_consumer_group_offsets(
            "p1-broker-parity-b1 broker.cdc_lab.orders 0 - 4 - - - -",
            group="p1-broker-parity-b1",
            topic="broker.cdc_lab.orders",
        )


def test_row_diff_and_digest_are_deterministic() -> None:
    first = [{"order_id": "1", "status": "paid"}]
    second = [{"status": "paid", "order_id": "1"}]
    assert diff_count(row_diff(first, second)) == 0
    assert snapshot_digest(first) == snapshot_digest(second)
