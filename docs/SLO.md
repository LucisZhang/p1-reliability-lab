# Path B measured SLOs

## Scope and evidence

These are regression budgets for the pinned single-node Path B lab, measured by run
`20260820T162859Z-0828bdbb`. The authoritative artifact is
[`showcase/results/broker_slo.json`](../showcase/results/broker_slo.json), and the complete
guarded transcript is
[`showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`](../showcase/logs/phase-b4-broker-verify-20260820T162338Z.log).
The exact recorded command was:

```text
make broker-verify ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000 --fault-after-events 25000 --outage-seconds 5"
```

The run started at `2026-08-21 00:23:53 +08:00` (`2026-08-20T16:23:53Z`) and finished at
`2026-08-21 00:28:59 +08:00` (`2026-08-20T16:28:59Z`). Every recovery measurement ended with
`snapshot_diff_count=0`, and the final Kafka offsets were at lag `0` on all three partitions.
Those claims map to `started_at`, `finished_at`, `recovery_measurements[*].snapshot_diff_count`,
and `offset_checkpoint_snapshot_linkage.kafka_offsets[*].lag` in the result.

## Regression budgets and certified observations

The budgets below are rounded outward from this one certified run. A future run on the same
pinned profile fails the regression gate if it crosses a budget or loses zero-diff convergence.
They are not availability commitments.

| Statement | Regression budget | Certified observation | Result field |
| --- | ---: | ---: | --- |
| Fixed 100,000-event workload, including the five-second Kafka outage, reaches lag-zero Iceberg reconciliation | at least 1,790 rows/s and within 56 s | 1,791.665 rows/s in 55.814 s | `benchmark.throughput.end_to_end_rows_per_second`, `end_to_end_seconds` |
| MySQL commit acknowledgement to first readable Iceberg snapshot | p50 at most 15.3 s; p95 at most 20.7 s | p50 15.201 s; p95 20.614 s; max 21.277 s across 100,000 events | `benchmark.freshness` |
| Kafka broker restart at the 25,000-event gate recovers to lag-zero, zero-diff state | within 48 s | 47.688 s; 100,000 rows; diff 0 | `recovery_measurements[0]` |
| Full consumer-group rewind and duplicate redelivery reconverges idempotently | within 51 s | 50.696 s; exactly 100,000 topic records replayed; diff 0 | `recovery_measurements[1]` |
| Controlled three-partition mis-keying is detected and rejected before main-path admission | within 25 s | 24.456 s; one non-monotonic transition; main diff 0 | `recovery_measurements[2]` |
| Poison record reaches the DLQ, is repaired with registered Avro, and normal flow resumes | within 53 s | 52.414 s; one DLQ record; 100,002 final rows; diff 0 | `recovery_measurements[3]` |
| Fresh Iceberg tables rebuild from Kafka offset zero | within 39 s | 38.008 s; 100,002 rows; digest match; diff 0 | `recovery_measurements[4]` |

The sustained-throughput number belongs to the fixed broker-restart workload window. It is
context for the run, not a claim that every later replay or repair step sustained that rate.
Mis-keying time ends at audit rejection; it is not recovery from corruption admitted to the
main topic.

## Lag and connector observability

The fixed workload captured 16 ordered samples. Eight contained exact Kafka consumer-group lag
and matching Flink Prometheus KafkaSource lag gauges; all 16 contained named Debezium JMX samples.
The maximum exact group lag was 75,000, followed by lag zero. Early samples without committed
group offsets remain explicit scrape errors rather than synthetic zeroes. These values map to
`observability.summary` and `observability.time_series` in the result and are rendered by the
dashboard's Phase B4 lag chart.

Debezium metrics come from the pinned Prometheus JMX Exporter Java agent `0.20.0`. Kafka lag uses
the existing Flink 1.20 Prometheus reporter for exported KafkaSource gauges and Kafka's group
offset command for exact per-partition values. The result records both metric-name sets under
`observability.summary`.

## Measurement environment and disclaimer

The certified environment in `environment` was one dedicated, CPU-only `x86_64` Linux VM:

- 8 logical AMD EPYC CPUs;
- 32,687,382,528 bytes (about 30.4 GiB) RAM;
- Ubuntu 24.04.4 LTS, kernel `6.8.0-137-generic`;
- Docker 29.7.2;
- no GPU and no co-tenant workload;
- `RESOURCE_PROFILE=small`; preflight recorded 54.8 GiB free disk and 26.7 GiB available RAM.

This is single-node, laptop-scale methodology evidence. The run was executed on a rented
single-node VM rather than the local Mac laptop, and it is **not** a cloud-production topology or
an HA/availability claim. There is no multi-node broker, replicated fault domain, managed
service, or statistically repeated capacity study. Re-run the pinned workload multiple times on
the intended production-like topology before turning these regression budgets into operational
SLOs.
