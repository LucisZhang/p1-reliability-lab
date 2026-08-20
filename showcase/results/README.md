# Results JSON Contract

Every exported result JSON must include:

- `run_id`
- `git_sha`
- `started_at`
- `finished_at`
- `stack_versions`
- `command`
- `logs`

Additional phase-specific fields should be stable, typed, and documented in the phase that introduces them.

`scripts/sync-results-to-dashboard.sh` validates these fields before copying result artifacts
into `dashboard/public/results/`. The sync also writes `dashboard/public/results/index.json`,
which is the static dashboard manifest. The dashboard reads that manifest and the copied JSON
only; it does not call MySQL, Flink, Iceberg, MinIO, StarRocks, or any backend service.

For deployed dashboard builds, the default results location is Vite's static `/results/` path.
Set `BASE_RESULTS_URL` at build time to point the dashboard at a separately hosted immutable
results directory with the same files and `index.json` manifest.

## Synced Results Manifest

`dashboard/public/results/index.json` is generated, not hand-authored:

- `generated_at`: ISO-8601 sync timestamp.
- `source`: source glob used by the sync step.
- `artifacts`: one entry per validated JSON artifact.
- Per artifact: `filename`, `phase`, `run_id`, `git_sha`, `started_at`, `finished_at`,
  `command`, and `logs`.

## Phase 1.2 CDC Smoke

`phase-1.2-cdc-smoke.json` adds:

- `reader`: data reader used for reconciliation; must be `Flink SQL batch`.
- `job_id` and `savepoint`: Flink job and controlled restart provenance.
- `source_iceberg_diff_count`: final source snapshot vs `orders_current` diff count.
- `deleted_key_absent` and `updated_key_current`: delete/update smoke assertions.
- `orders_changelog_change_count`: audit-table CDC row count for the known sequence.
- `mysql_rows`, `iceberg_rows`, and `final_rows`: normalized final-state snapshots.
- `delete_file_smoke`: Iceberg metadata-only evidence for delete and equality-delete files.

## Phase 2.1 Failure Reconciliation

`eo_reconciliation.json` adds:

- `reader`: data reader used for final-state reconciliation; must be `Flink SQL batch`.
- `claim_boundary`: the verified pipeline segment, `MySQL CDC -> Flink -> Iceberg`.
- `results`: one object per induced failure class.
- Per result, `failure_class`, `trigger`, `recovery`, job/checkpoint IDs, `snapshot_diff_count`,
  `snapshot_diff`, source/Iceberg snapshot row counts, normalized `mysql_rows` and
  `iceberg_rows`, and `event_id_audit`.
- `event_id_audit`: final current-table event-id set comparison plus changelog distinct-event-id
  and row-count checks. This is an audit supplement; snapshot reconciliation is the proof.
- `summary`: aggregate pass/fail flags for snapshot diffs and event-id audits.
- Phase 2.1 extends the matrix to five failure classes: `task-crash`,
  `checkpoint-restore`, `jobmanager-restart`, `savepoint-restore`, and
  `sink-commit-fault`.
- `sink-commit-fault.fault_injection`: documents the test-only
  `--checkpoint-complete-fault-event-id` mechanism, marker path, marker text, and trigger event.

## Phase 2.2 Iceberg Small-File Rewrite

`iceberg_small_file_rewrite.json` adds:

- `maintenance_scope`: states that this is Iceberg small-file management only; StarRocks serving
  imports and Primary Key table maintenance are outside the artifact.
- `scenario`: resource-profile-scaled batch count, rows per batch, checkpoint interval, low write
  target file size, rewrite target file size, manifest merge setting, and planning repetitions.
- `before` and `after`: Iceberg metadata metrics for `orders_current`: data-file count, median
  file size, distinct live data-file manifest count, snapshot data/delete manifest counts,
  manifest bytes, and timed `TableScan.planFiles()` planning latency.
- `rewrite_data_files`: result counts from Iceberg `rewrite_data_files`, including rewritten
  data files, added data files, target file size, and rewritten bytes.
- `rewrite_manifests`: whether a data-manifest rewrite ran, with live data-file manifest counts
  and raw snapshot-manifest counts before/after that maintenance step.
- `deltas`, `checks`, and `summary`: machine-checkable proof that data-file count and manifest
  count decreased, median file size increased, and planning latency decreased.
- `chart`: path to the captured before/after SVG in `showcase/media/`.

## Phase 2.3 Checkpoint Metrics Under Load

`checkpoint_metrics.json` adds:

- `reader`: states that Flink checkpoint and backpressure metrics come from the real Prometheus
  metrics reporter, while Iceberg commit lag is read with Flink SQL batch.
- `scenario`: resource-profile-scaled input spike size, checkpoint interval, test-only sleep
  gates, sampling cadence, timeout, and minimum backpressure threshold.
- `metric_names`: documents the Prometheus metric patterns and REST fallback fields used for
  checkpoint duration, alignment time, checkpoint failure count, backpressure indicator, and
  Iceberg commit lag.
- `time_series`: ordered samples with `checkpoint`, `backpressure`, and
  `iceberg_commit_lag` sections. Each sample records checkpoint duration, alignment time,
  checkpoint failure count, backpressure indicator, source progress, Iceberg changelog progress,
  event-count lag, and max-event-id gap.
- `checkpoint_records`: deduplicated completed-checkpoint records from the Flink REST checkpoint
  detail endpoint.
- `summary`: baseline-vs-load maxima and checks proving checkpoint duration/alignment rose,
  backpressure was observed, lag appeared during the spike, and lag recovered to zero.
- `chart`: path to the captured Phase 2.3 SVG in `showcase/media/`.

## Phase B3 Broker Failure Drills

Phase B3 adds five separate append-only JSON files, one for each `make broker-verify
ARGS="--failure <name>"` run: `broker_restart_drill.json`,
`duplicate_redelivery_drill.json`, `ordering_miskey_drill.json`, `poison_dlq_drill.json`, and
`offset_replay_drill.json`.

- Every file includes `phase`, `failure_class`, remote `environment`, deterministic `scenario`,
  `fault`, `recovery`, final `reconciliation`, `snapshot_diff_count`, `event_id_audit`,
  `offset_checkpoint_snapshot_linkage`, `checks`, and `summary`.
- Final linkage contains per-partition committed Kafka offsets/lag, a completed Flink checkpoint,
  and both Iceberg snapshot IDs. Final lag and final source/Iceberg snapshot diff must be zero.
- Duplicate redelivery additionally reports the exact duplicate occurrence count; the ordering
  probe reports the detected non-monotonic transition and its pre-admission rejection; poison/DLQ
  reports quarantine metadata and repair replay; offset replay reports independent offset-0 and
  timestamp rebuild diffs.

See [`docs/broker-failure-result-schema.md`](../../docs/broker-failure-result-schema.md) for the
stable per-drill fields and validation rules.
