# Phase B1 Broker Parity Result Schema

`showcase/results/broker_parity.json` is added only by a passing remote run. It extends the
existing provenance envelope with:

- `environment`: remote hostname, CPU, RAM, OS/kernel, architecture, Docker version, and load
  average at evidence capture.
- `scenario`: fixed event count/seed, checkpoint interval, three-partition Kafka topic, primary
  key rule, and topic end offsets before Path B starts.
- `path_a` and `path_b`: unchanged embedded-CDC job versus the new Kafka-source job, with job IDs,
  final row/changelog counts, deterministic snapshot digests, and Iceberg snapshot IDs.
- `parity`: row-level Path A/Path B and source/Path B diffs. Both counts must be zero.
- `offset_checkpoint_snapshot_linkage`: per-partition committed Kafka offsets and lag, the
  completed Flink checkpoint ID/path, and current/changelog Iceberg snapshot IDs observed after
  convergence.
- `summary`: publishable only when parity, source reconciliation, zero Kafka lag, and linkage
  completeness all pass.

`dashboard/scripts/sync-results.mjs` enforces these B1-specific fields in the light-path build.
