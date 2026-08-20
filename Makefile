SHELL := /bin/bash

ROOT := $(CURDIR)
ENV_FILE ?= .env
SYSTEM_PYTHON := $(shell command -v python3.11 2>/dev/null || command -v python3 2>/dev/null)
PYTHON ?= $(if $(wildcard $(ROOT)/.venv/bin/python),$(ROOT)/.venv/bin/python,$(SYSTEM_PYTHON))
MAVEN ?= mvn
MAVEN_REPO ?= $(ROOT)/.m2/repository
RESOURCE_PROFILE ?= small
COMPOSE := docker compose --env-file $(ENV_FILE) -f infra/docker-compose.yml
BROKER_LONG_RUNNING_SERVICES := mysql minio kafka schema-registry debezium jobmanager taskmanager
PYTHONPATH := $(ROOT)/harness
export PYTHONPATH
export P1_ENV_FILE := $(ENV_FILE)

.PHONY: ensure-env doctor local-verify artifact-verify preflight-heavy preflight-broker
.PHONY: up-core up-olap ps down build-flink submit-flink savepoint restore
.PHONY: gen eo-verify small-file-rewrite ckpt-metrics import-starrocks smoke-starrocks-catalog
.PHONY: compaction-bench dq backfill test test-cdc lint sql-mysql sql-iceberg sql-iceberg-meta
.PHONY: sql-starrocks dashboard-build dashboard-preview broker-up broker-verify
.PHONY: build-debezium-avro-plugin build-debezium-jmx-agent
.PHONY: sync-up sync-down remote-broker-up remote-broker-verify

ensure-env:
	@if [ ! -f "$(ENV_FILE)" ]; then cp .env.example "$(ENV_FILE)"; fi

doctor:
	$(PYTHON) -m harness.doctor

local-verify:
	$(MAKE) test
	$(MAKE) lint
	$(MAKE) dashboard-build

artifact-verify:
	scripts/sync-results-to-dashboard.sh

preflight-heavy:
	P1_REPO_ROOT=$(ROOT) $(PYTHON) scripts/preflight-heavy.py

preflight-broker:
	P1_REPO_ROOT=$(ROOT) P1_PREFLIGHT_PROFILE=broker $(PYTHON) scripts/preflight-heavy.py

up-core: ensure-env preflight-heavy
	RESOURCE_PROFILE=$(RESOURCE_PROFILE) $(COMPOSE) --profile core up -d --build

up-olap: ensure-env preflight-heavy
	RESOURCE_PROFILE=$(RESOURCE_PROFILE) $(COMPOSE) --profile olap up -d --build

build-debezium-avro-plugin:
	$(MAVEN) -q -Dmaven.repo.local=$(MAVEN_REPO) -f infra/debezium/avro-converter-pom.xml \
		clean dependency:copy-dependencies -DincludeScope=runtime \
		-DoutputDirectory=$(ROOT)/infra/debezium/target/avro-plugin
	@test -f infra/debezium/target/avro-plugin/kafka-connect-avro-converter-7.9.8.jar

build-debezium-jmx-agent:
	$(MAVEN) -q -Dmaven.repo.local=$(MAVEN_REPO) \
		org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
		-Dartifact=io.prometheus.jmx:jmx_prometheus_javaagent:0.20.0 \
		-DoutputDirectory=$(ROOT)/infra/debezium/target/jmx-exporter
	@test -f infra/debezium/target/jmx-exporter/jmx_prometheus_javaagent-0.20.0.jar

broker-up: ensure-env preflight-broker build-debezium-avro-plugin build-debezium-jmx-agent
	RESOURCE_PROFILE=$(RESOURCE_PROFILE) $(COMPOSE) --profile broker up -d --build --wait --wait-timeout 240 $(BROKER_LONG_RUNNING_SERVICES)
	RESOURCE_PROFILE=$(RESOURCE_PROFILE) $(COMPOSE) --profile broker run --rm minio-init
	$(PYTHON) -m harness.broker_admin configure

broker-verify: ensure-env preflight-broker build-flink
	$(PYTHON) -m harness.broker_verify $(ARGS)

ps: ensure-env
	$(COMPOSE) ps

down: ensure-env
	$(COMPOSE) --profile core --profile olap --profile broker down -v

build-flink:
	$(MAVEN) -q -Dmaven.repo.local=$(MAVEN_REPO) -f flink-jobs/pom.xml clean package

submit-flink: ensure-env
	$(PYTHON) -m harness.flink submit

savepoint:
	@test -n "$(JOB)" || (echo "JOB=<flink-job-id> is required" >&2; exit 2)
	$(COMPOSE) exec -T jobmanager flink savepoint $(JOB)

restore:
	@test -n "$(SP)" || (echo "SP=<savepoint-path> is required" >&2; exit 2)
	$(PYTHON) -m harness.flink submit --savepoint "$(SP)"

gen: ensure-env
	$(PYTHON) -m harness.generator $(ARGS)

eo-verify: ensure-env preflight-heavy build-flink
	$(PYTHON) -m harness.eo_verify $(ARGS)

small-file-rewrite: ensure-env preflight-heavy build-flink
	$(PYTHON) -m harness.small_file_rewrite $(ARGS)

ckpt-metrics: ensure-env preflight-heavy build-flink
	$(PYTHON) -m harness.checkpoint_metrics $(ARGS)

import-starrocks:
	@echo "StarRocks is intentionally out of core and starts in M3."
	@exit 2

smoke-starrocks-catalog:
	@echo "StarRocks catalog smoke test is intentionally out of Phase 1.1/core."
	@exit 2

compaction-bench:
	@echo "StarRocks compaction bench is intentionally out of Phase 1.1/core."
	@exit 2

dq:
	@echo "Data-quality harness is introduced in a later phase."
	@exit 2

backfill:
	@echo "Backfill harness is introduced in a later phase."
	@exit 2

test:
	$(PYTHON) -m pytest -q harness/tests

test-cdc: ensure-env preflight-heavy build-flink
	CDC_INTEGRATION=1 $(PYTHON) -m pytest harness/tests/cdc_correctness -v

lint:
	$(PYTHON) -m ruff check harness
	$(PYTHON) -m black --check harness
	$(PYTHON) -m mypy harness
	@if [ -f flink-jobs/pom.xml ]; then \
		$(MAVEN) -q -Dmaven.repo.local=$(MAVEN_REPO) -f flink-jobs/pom.xml verify; \
	else \
		echo "Skipping Maven verify: Flink job is introduced in Phase 1.2."; \
	fi

sql-mysql: ensure-env
	$(PYTHON) -m harness.sql mysql $(if $(Q),--query "$(Q)",$(ARGS))

sql-iceberg: ensure-env
	$(PYTHON) -m harness.sql iceberg $(if $(Q),--query "$(Q)",$(ARGS))

sql-iceberg-meta: ensure-env
	$(PYTHON) -m harness.sql iceberg-meta $(ARGS)

sql-starrocks: ensure-env
	$(PYTHON) -m harness.sql starrocks $(if $(Q),--query "$(Q)",$(ARGS))

dashboard-build:
	scripts/sync-results-to-dashboard.sh
	npm --prefix dashboard ci
	npm --prefix dashboard run build

dashboard-preview:
	npm --prefix dashboard run preview

sync-up:
	P1_REMOTE_ROOT="$(P1_REMOTE_ROOT)" scripts/remote/broker-remote.sh sync-up

sync-down:
	P1_REMOTE_ROOT="$(P1_REMOTE_ROOT)" scripts/remote/broker-remote.sh sync-down

remote-broker-up:
	P1_REMOTE_ROOT="$(P1_REMOTE_ROOT)" RESOURCE_PROFILE="$(RESOURCE_PROFILE)" \
		scripts/remote/broker-remote.sh run broker-up "$(ARGS)"

remote-broker-verify:
	P1_REMOTE_ROOT="$(P1_REMOTE_ROOT)" RESOURCE_PROFILE="$(RESOURCE_PROFILE)" \
		scripts/remote/broker-remote.sh run broker-verify "$(ARGS)"
