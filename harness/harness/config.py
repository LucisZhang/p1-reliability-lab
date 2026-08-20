from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = REPO_ROOT / ".env"
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_env_file(path: Path = DEFAULT_ENV_FILE) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _strip_quotes(value.strip())
    return values


def env_value(key: str, default: str, values: dict[str, str] | None = None) -> str:
    file_values = values if values is not None else load_env_file()
    return os.environ.get(key, file_values.get(key, default))


@dataclass(frozen=True)
class Settings:
    env_file: Path
    compose_file: Path
    mysql_host: str
    mysql_port: int
    mysql_container: str
    mysql_database: str
    mysql_user: str
    mysql_password: str
    iceberg_catalog_database: str
    iceberg_catalog_name: str
    iceberg_warehouse: str
    minio_endpoint: str
    minio_docker_endpoint: str
    minio_bucket: str
    minio_root_user: str
    minio_root_password: str
    minio_region: str
    flink_jobmanager_host: str
    flink_rest_port: int
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_topic_partitions: int
    kafka_consumer_group: str
    kafka_dlq_topic: str
    kafka_ordering_probe_topic: str
    debezium_connect_host: str
    debezium_connect_port: int
    debezium_connector_name: str
    debezium_metrics_host: str
    debezium_metrics_port: int
    schema_registry_host: str
    schema_registry_port: int
    schema_registry_docker_url: str
    starrocks_host: str
    starrocks_port: int


def configured_env_file() -> Path:
    raw = os.environ.get("P1_ENV_FILE")
    if raw is None or raw == "":
        return DEFAULT_ENV_FILE
    path = Path(raw)
    return path if path.is_absolute() else REPO_ROOT / path


def load_settings(env_file: Path | None = None) -> Settings:
    active_env_file = env_file or configured_env_file()
    values = load_env_file(active_env_file)
    return Settings(
        env_file=active_env_file,
        compose_file=COMPOSE_FILE,
        mysql_host=env_value("MYSQL_HOST", "127.0.0.1", values),
        mysql_port=int(env_value("MYSQL_PORT", "3306", values)),
        mysql_container=env_value("MYSQL_CONTAINER", "mysql", values),
        mysql_database=env_value("MYSQL_DATABASE", "cdc_lab", values),
        mysql_user=env_value("MYSQL_USER", "cdc", values),
        mysql_password=env_value("MYSQL_PASSWORD", "cdc_pw", values),
        iceberg_catalog_database=env_value("ICEBERG_CATALOG_DATABASE", "iceberg_catalog", values),
        iceberg_catalog_name=env_value("ICEBERG_CATALOG_NAME", "lab_iceberg", values),
        iceberg_warehouse=env_value("ICEBERG_WAREHOUSE", "s3://warehouse/iceberg", values),
        minio_endpoint=env_value("MINIO_ENDPOINT", "http://127.0.0.1:9000", values),
        minio_docker_endpoint=env_value("MINIO_DOCKER_ENDPOINT", "http://minio:9000", values),
        minio_bucket=env_value("MINIO_BUCKET", "warehouse", values),
        minio_root_user=env_value("MINIO_ROOT_USER", "minioadmin", values),
        minio_root_password=env_value("MINIO_ROOT_PASSWORD", "minioadmin", values),
        minio_region=env_value("MINIO_REGION", "us-east-1", values),
        flink_jobmanager_host=env_value("FLINK_JOBMANAGER_HOST", "127.0.0.1", values),
        flink_rest_port=int(env_value("FLINK_REST_PORT", "8081", values)),
        kafka_bootstrap_servers=env_value("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092", values),
        kafka_topic=env_value("KAFKA_TOPIC", "broker.cdc_lab.orders", values),
        kafka_topic_partitions=int(env_value("KAFKA_TOPIC_PARTITIONS", "3", values)),
        kafka_consumer_group=env_value("KAFKA_CONSUMER_GROUP", "p1-broker-parity-b1", values),
        kafka_dlq_topic=env_value("KAFKA_DLQ_TOPIC", "broker.cdc_lab.orders.dlq", values),
        kafka_ordering_probe_topic=env_value(
            "KAFKA_ORDERING_PROBE_TOPIC",
            "broker.cdc_lab.orders.ordering-probe",
            values,
        ),
        debezium_connect_host=env_value("DEBEZIUM_CONNECT_HOST", "127.0.0.1", values),
        debezium_connect_port=int(env_value("DEBEZIUM_CONNECT_PORT", "8083", values)),
        debezium_connector_name=env_value("DEBEZIUM_CONNECTOR_NAME", "p1-orders-connector", values),
        debezium_metrics_host=env_value("DEBEZIUM_METRICS_HOST", "127.0.0.1", values),
        debezium_metrics_port=int(env_value("DEBEZIUM_METRICS_PORT", "9405", values)),
        schema_registry_host=env_value("SCHEMA_REGISTRY_HOST", "127.0.0.1", values),
        schema_registry_port=int(env_value("SCHEMA_REGISTRY_PORT", "8085", values)),
        schema_registry_docker_url=env_value(
            "SCHEMA_REGISTRY_DOCKER_URL", "http://schema-registry:8081", values
        ),
        starrocks_host=env_value("STARROCKS_HOST", "127.0.0.1", values),
        starrocks_port=int(env_value("STARROCKS_PORT", "9030", values)),
    )
