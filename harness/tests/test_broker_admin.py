from __future__ import annotations

from pathlib import Path

from harness.broker_admin import connector_config, topic_prefix
from harness.config import REPO_ROOT, load_settings


def test_connector_config_pins_primary_key_and_registry_backed_avro() -> None:
    settings = load_settings(REPO_ROOT / ".env.example")
    config = connector_config(settings)

    assert topic_prefix(settings) == "broker"
    assert config["message.key.columns"] == "cdc_lab.orders:order_id"
    assert config["topic.prefix"] == "broker"
    assert config["snapshot.mode"] == "initial"
    assert config["tombstones.on.delete"] == "false"
    assert config["topic.creation.default.partitions"] == "3"
    assert config["key.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert config["value.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert config["key.converter.schema.registry.url"] == "http://schema-registry:8081"
    assert config["value.converter.schema.registry.url"] == "http://schema-registry:8081"
    assert config["field.name.adjustment.mode"] == "avro"


def test_example_env_is_a_real_file() -> None:
    assert Path(REPO_ROOT / ".env.example").is_file()
