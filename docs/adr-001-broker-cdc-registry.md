# ADR-001: Path B broker, CDC runtime, and schema registry

- Status: Accepted
- Date: 2026-08-21
- Scope: Exactly Once Stream broker upgrade, Phases B1–B5

## Context

The broker upgrade preserves direct embedded CDC as Path A and adds Path B without changing the
Iceberg correctness boundary. The upgrade plan's component decisions are locked; Phase B5 records
what was actually certified and the measurements that determined whether the Kafka default fit the
host. See [`UPGRADE_PLAN_BROKER.md`](engineering-log/UPGRADE_PLAN_BROKER.md) and the certified Path A/Path B
parity result in [`broker_parity.json`](../showcase/results/broker_parity.json).

## Decision

| Concern | Accepted choice | Recorded basis |
| --- | --- | --- |
| Broker | Apache Kafka `3.9.2`, one combined broker/controller in single-node KRaft mode | Kafka was the locked default. Both the B1 and B4 broker preflights passed the 16 GiB total / 8 GiB available-memory gate, so the Redpanda fallback was not triggered. The B1 parity result linked Kafka offsets to a completed Flink checkpoint and Iceberg snapshots with row-level diff `0`. |
| CDC runtime | One-worker Debezium Connect `3.2.4.Final` | Connect keeps Kafka-backed connector state and the Confluent Avro converter in one existing worker. The initially pinned `3.2.7.Final` image did not exist in the selected registry; the preserved [B1 pull failure](../showcase/logs/phase-b1-broker-up-20260820T093844Z.log) forced the certified `3.2.4.Final` pin. No Debezium Server comparison was run, so this is an operational choice, not a measured head-to-head memory claim. |
| Contract format and registry | Confluent Avro converter and Schema Registry `7.9.8`, global and subject-level `BACKWARD` compatibility | It matches the pinned Kafka line and provides the required producer/consumer wire contract. The certified [B2 contract result](../showcase/results/schema_contract_drill.json) records Registry-backed producer and consumer paths, HTTP `409` rejection of the incompatible change, unchanged subject version `1`, continued checkpoints, lag `0`, and final row-level diff `0`. |

Path A remains the embedded Debezium/Flink CDC route. Path B is therefore:

```text
MySQL GTID/binlog
  -> Debezium Connect at-least-once delivery
  -> Registry-backed Avro
  -> Kafka partition offsets
  -> checkpointed Flink Kafka source
  -> Iceberg v2 keyed-upsert snapshots
```

The broker does not turn Debezium delivery into end-to-end exactly-once delivery. Duplicate
delivery remains visible in the append-only changelog, while primary-key keying and the Iceberg
current-state upsert provide idempotent convergence. The certified duplicate-redelivery evidence
is [`duplicate_redelivery_drill.json`](../showcase/results/duplicate_redelivery_drill.json).

## Measured memory envelope

The preflight logs report host total and available memory rounded to 0.1 GiB. They do **not**
contain per-container RSS, so the only defensible footprint is the approximate decrease in
host-available memory between the broker-up preflight and the subsequent verification preflight.
It is a whole `broker` profile footprint—MySQL, MinIO, Kafka, Schema Registry, Debezium Connect,
Flink JobManager, and Flink TaskManager—not a broker-only or per-component measurement.

| Certified phase/profile | Broker-up preflight | Verification preflight | Approx. available-memory decrease | Evidence |
| --- | ---: | ---: | ---: | --- |
| B1, `RESOURCE_PROFILE=small`, JSON-wire parity stack | 30.4 GiB total / 29.5 GiB available | 30.4 GiB total / 26.7 GiB available | 2.8 GiB | [B1 broker-up](../showcase/logs/phase-b1-broker-up-20260820T101225Z.log) · [B1 verify](../showcase/logs/phase-b1-broker-verify-20260820T101332Z.log) |
| B4, `RESOURCE_PROFILE=small`, Avro + Debezium JMX measurement stack | 30.4 GiB total / 29.5 GiB available | 30.4 GiB total / 26.7 GiB available | 2.8 GiB | [B4 broker-up](../showcase/logs/phase-b4-broker-up-20260820T162222Z.log) · [B4 verify](../showcase/logs/phase-b4-broker-verify-20260820T162338Z.log) |

Both observed verification points retained 18.7 GiB more available memory than the 8 GiB launch
floor. On this 30.4 GiB host, that evidence keeps Kafka KRaft within the locked memory gate. It
does not prove that the same profile fits a 16 GiB host, and it does not allocate the 2.8 GiB
delta among services.

## Consequences

- Path B has an explicit at-least-once segment. Correctness claims require primary-key Kafka
  keys, completed-checkpoint/offset/snapshot linkage, equality-delete-aware reconciliation, and
  the duplicate event-ID audit.
- Single-node KRaft is reproducible lab infrastructure, not an HA or replicated production
  topology. The fixed benchmark and disclaimer are recorded in [`SLO.md`](SLO.md) and
  [`broker_slo.json`](../showcase/results/broker_slo.json).
- The Confluent converter adds a pinned JVM dependency closure. The certified stack records
  Jackson `2.18.6`, Flink-side Avro `1.12.0`, and the Confluent-managed Connect plugin closure;
  these pins are documented in [`version-matrix.md`](version-matrix.md) and the B2 result.
- The Redpanda fallback remains dormant. Crossing the memory gate on a future target host would
  require new measured evidence and a superseding ADR rather than silently changing the broker.
