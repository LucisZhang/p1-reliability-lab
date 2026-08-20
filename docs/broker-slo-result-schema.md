# Phase B4 Broker SLO Result Schema

`showcase/results/broker_slo.json` is the single append-only certification artifact for Phase B4.
It is produced only by the guarded remote command recorded in `command`; the local dashboard build
rejects an artifact that does not satisfy this contract.

## Fixed workload

- `phase` is `B4` and `environment` records the remote host, CPU, RAM, OS, kernel, architecture,
  Docker version, and load at capture.
- `scenario.events` is `100000`; `scenario.seed` and all fault/checkpoint parameters are explicit.
- `benchmark.throughput.end_to_end_rows_per_second` divides the fixed row count by the observed
  first-MySQL-request to final lag-zero, row-reconciled window. The window includes the broker
  outage and is not a peak-capacity claim.
- `benchmark.freshness` assigns every event to the earliest Iceberg snapshot in which Flink SQL
  time travel can read it. The event's start timestamp is the acknowledgement of its MySQL
  transaction commit; the end timestamp is Iceberg snapshot metadata `timestamp_ms`. The result
  retains commit batches and per-snapshot aggregates, then reports p50/p95/max across 100,000
  events. PyIceberg enumerates metadata only and never reads final-state rows.

## Prometheus-format observability

`observability.time_series` is an ordered list sampled during the fixed workload:

- `kafka_consumer_group_lag`: exact partition values, sum, and max from Kafka's group-offset
  command, paired in the same sample with the KafkaSource lag gauges exposed by Flink's existing
  Prometheus reporter. An endpoint error remains explicit rather than becoming a zero.
- `debezium_connector`: selected numeric values and their emitted names from Debezium's
  `debezium.mysql` MBeans, exposed by the pinned Prometheus JMX Java agent `0.20.0`.
- `scrape_errors`: explicit endpoint failures, expected to be possible while Kafka is killed;
  errors are retained rather than converted to zero.

The validator requires multiple samples, at least one named Debezium connector sample, and a
positive observed Kafka lag during the restart workload.

## Recovery measurements

`recovery_measurements` contains exactly one entry for each B3 failure class:
`broker-restart`, `duplicate-redelivery`, `mis-keying`, `poison-dlq`, and `offset-replay`.
Every entry records:

- `recovery_seconds` and the exact start/end `definition`;
- `events_under_test`;
- compact source/Iceberg row counts, SHA-256 digests, and `snapshot_diff_count=0`;
- scenario-specific `checks`, all `true`;
- linkage or audit details appropriate to that failure.

The mis-keying timer ends at audit rejection while the isolated probe remains outside the main
topic; it is not presented as recovery from admitted corruption. The top-level final linkage must
contain zero-lag Kafka offsets, a completed Flink checkpoint, and current/changelog Iceberg
snapshot IDs. `summary.passed` is true only when every B4 check passes.

The broker-restart measurement continuously checks connector and task state until the fixed
workload is present in Kafka. Kafka Connect can report a running connector while its source task
is failed; when that occurs, B4 invokes the standard `includeTasks=true&onlyFailed=true` restart
endpoint and includes the action, restart count, sanitized state transitions, and delivered topic
record count in `recovery_measurements[0].details.connector_recovery`. Connector traces and
configuration values are never copied into the result.
