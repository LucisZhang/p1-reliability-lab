# Phase B3 broker-failure result schema

Phase B3 adds exactly five append-only artifacts. A drill writes its artifact only after every
machine-checkable assertion passes:

| `--failure` value | Result artifact |
| --- | --- |
| `broker-restart` | `showcase/results/broker_restart_drill.json` |
| `duplicate-redelivery` | `showcase/results/duplicate_redelivery_drill.json` |
| `mis-keying` | `showcase/results/ordering_miskey_drill.json` |
| `poison-dlq` | `showcase/results/poison_dlq_drill.json` |
| `offset-replay` | `showcase/results/offset_replay_drill.json` |

Every artifact includes the global provenance envelope, remote `environment`, `phase=B3`,
`failure_class`, the deterministic `scenario`, `fault`, `recovery`, `reconciliation`,
`event_id_audit`, `checks`, and `summary`. The root `snapshot_diff_count` and the nested final
reconciliation count must both be zero. `offset_checkpoint_snapshot_linkage` always records the
final per-partition Kafka committed offsets and lag, completed Flink checkpoint, and both Iceberg
snapshot IDs.

Scenario-specific evidence:

- Broker restart records the Kafka container state before kill, while stopped, and after start,
  plus offsets/checkpoints before and after recovery.
- Duplicate redelivery records the executed group-offset rewind and the exact positive duplicate
  occurrence count from the changelog event-ID audit; final-table reconciliation remains the
  correctness proof.
- Mis-keying records Registry-backed Avro probe records. Correct PK keys stay on one partition;
  controlled wrong keys span three partitions and produce a non-monotonic arrival sequence. The
  probe is isolated and rejected before main-topic admission, so the main path still ends at diff
  zero.
- Poison/DLQ records the original Kafka metadata and base64 bytes, deserialization error metadata,
  registered-Avro repair replay, post-poison continuity event, and final convergence. The DLQ
  remains an audit record after repair.
- Offset replay records the captured original rows/digest and two new Iceberg snapshot lineages:
  one rebuilt from offset 0 and one from a chosen timestamp following a complete update sweep.
  Both rebuilds must match the original row-for-row and by digest.

The static dashboard sync rejects a B3 file if any `checks` value is not `true`, provenance/log
linkage is missing, Kafka lag is non-zero, scenario-specific evidence is incomplete, or final
snapshot diff is non-zero.
