from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Sequence
from typing import cast
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from harness.config import Settings, load_settings
from harness.flink import compose_base


def topic_prefix(settings: Settings) -> str:
    suffix = f".{settings.mysql_database}.orders"
    if not settings.kafka_topic.endswith(suffix):
        raise ValueError(
            f"KAFKA_TOPIC must end with {suffix!r} for Debezium topic routing; "
            f"got {settings.kafka_topic!r}"
        )
    return settings.kafka_topic[: -len(suffix)]


def connector_config(settings: Settings) -> dict[str, str]:
    return {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": settings.mysql_user,
        "database.password": settings.mysql_password,
        "database.server.id": "184054",
        "database.include.list": settings.mysql_database,
        "table.include.list": f"{settings.mysql_database}.orders",
        "topic.prefix": topic_prefix(settings),
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        "schema.history.internal.kafka.topic": "p1-schema-history.orders",
        "snapshot.mode": "initial",
        "include.schema.changes": "false",
        "tombstones.on.delete": "false",
        "message.key.columns": f"{settings.mysql_database}.orders:order_id",
        "database.connectionTimeZone": "UTC",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": str(settings.kafka_topic_partitions),
        "topic.creation.default.cleanup.policy": "delete",
        "topic.creation.default.retention.ms": "86400000",
    }


def _request_json(
    method: str,
    url: str,
    payload: dict[str, object] | None = None,
) -> tuple[int, object]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=data,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        parsed: object = json.loads(body) if body else {}
        return exc.code, parsed


def _create_topic(settings: Settings) -> None:
    proc = subprocess.run(
        [
            *compose_base(settings),
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            settings.kafka_topic,
            "--partitions",
            str(settings.kafka_topic_partitions),
            "--replication-factor",
            "1",
            "--config",
            "cleanup.policy=delete",
            "--config",
            "retention.ms=86400000",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())


def configure_broker(settings: Settings) -> dict[str, object]:
    _create_topic(settings)

    registry_url = f"http://{settings.schema_registry_host}:{settings.schema_registry_port}/config"
    status, registry_payload = _request_json("PUT", registry_url, {"compatibility": "BACKWARD"})
    if status not in {200, 201}:
        raise RuntimeError(f"Schema Registry compatibility update failed: HTTP {status}")

    connect_base = f"http://{settings.debezium_connect_host}:{settings.debezium_connect_port}"
    connector_url = f"{connect_base}/connectors/{settings.debezium_connector_name}"
    existing_status, _ = _request_json("GET", connector_url)
    config = connector_config(settings)
    if existing_status == 404:
        status, _ = _request_json(
            "POST",
            f"{connect_base}/connectors",
            {"name": settings.debezium_connector_name, "config": config},
        )
        expected = {200, 201}
    elif existing_status == 200:
        status, _ = _request_json("PUT", f"{connector_url}/config", cast(dict[str, object], config))
        expected = {200}
    else:
        raise RuntimeError(f"Debezium connector lookup failed: HTTP {existing_status}")
    if status not in expected:
        raise RuntimeError(f"Debezium connector configuration failed: HTTP {status}")

    connector_state = _wait_for_connector(settings, timeout_seconds=120)
    compatibility_status, compatibility = _request_json("GET", registry_url)
    if compatibility_status != 200:
        raise RuntimeError(
            f"Schema Registry compatibility verification failed: HTTP {compatibility_status}"
        )
    compatibility_value = cast(dict[str, object], compatibility).get("compatibilityLevel")
    if compatibility_value != "BACKWARD":
        raise RuntimeError(
            f"Schema Registry compatibility is {compatibility_value!r}, expected 'BACKWARD'"
        )

    return {
        "kafka_topic": settings.kafka_topic,
        "partitions": settings.kafka_topic_partitions,
        "topic_key": f"{settings.mysql_database}.orders.order_id",
        "schema_registry_compatibility": compatibility_value,
        "debezium_connector": settings.debezium_connector_name,
        "debezium_state": connector_state,
        "serialization": "Kafka Connect JSON with schema envelope (Phase B1 parity only)",
    }


def _wait_for_connector(settings: Settings, *, timeout_seconds: int) -> dict[str, object]:
    url = (
        f"http://{settings.debezium_connect_host}:{settings.debezium_connect_port}"
        f"/connectors/{settings.debezium_connector_name}/status"
    )
    deadline = time.monotonic() + timeout_seconds
    last_payload: object = {}
    while time.monotonic() < deadline:
        status, payload = _request_json("GET", url)
        last_payload = payload
        if status == 200 and isinstance(payload, dict):
            connector = payload.get("connector")
            tasks = payload.get("tasks")
            if (
                isinstance(connector, dict)
                and isinstance(tasks, list)
                and len(tasks) == 1
                and isinstance(tasks[0], dict)
            ):
                task = tasks[0]
                if connector.get("state") == "RUNNING" and task.get("state") == "RUNNING":
                    return {
                        "connector": connector.get("state"),
                        "task_0": task.get("state"),
                    }
        time.sleep(2)
    raise TimeoutError(f"Debezium connector did not reach RUNNING: {last_payload}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Configure the Phase B broker ingress services.")
    parser.add_argument("command", choices=["configure"])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "configure":
            result = configure_broker(load_settings())
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
    except Exception as exc:
        print(f"broker admin failed: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
