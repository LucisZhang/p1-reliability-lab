# RUNBOOK

This runbook is the durable incident record for the reliability lab. Phase 1.1 creates the skeleton only; failure phases append real incidents, observed symptoms, commands, recovery steps, and links to artifacts immediately after verification.

## Operating Envelope

- Single-node Docker Compose.
- `core` profile: MySQL, Flink JobManager/TaskManager, MinIO, and the Iceberg JDBC catalog database schema in MySQL.
- `olap` profile is reserved for StarRocks in M3+.
- `broker` profile adds Apache Kafka 3.9.2 in single-node KRaft mode, Schema Registry 7.9.8,
  and one Debezium Connect 3.2.4.Final worker while retaining all `core` services.
- Use repo-root `make` targets only.
- Space-constrained laptops are **local lite** environments: run `make local-verify` and the
  static dashboard; do not treat them as the default place for the heavy failure-reproduction
  run.
- Heavy Docker targets (`make up-core`, `make eo-verify`, `make test-cdc`, `make
  small-file-rewrite`, `make ckpt-metrics`) run `make preflight-heavy` first. The default guard
  refuses to start when the repository volume has less than 25 GiB free or Docker does not
  respond within 10 seconds.
- Full five-failure-class reproduction should run on a workstation with at least 40 GiB free
  disk and enough Docker memory for Flink, MySQL, MinIO, and the Iceberg catalog. Preserve the
  evidence bundle described in `docs/local-lite-and-workstation.md`.
- Broker integration runs are remote-only for the current Mac workflow. Use `make sync-up`,
  `make remote-broker-up`, `make remote-broker-verify`, then `make sync-down`; the remote
  launchers always record load average, record GPU occupancy when `nvidia-smi` exists, and
  explicitly allow its absence on the dedicated CPU-only VM. Runs execute under tmux. Phase B1
  is a parity run, not a fault drill, so it does not add a synthetic incident entry.

## Incident Log

Append one section per induced or observed failure.

### Incident Template

- Phase:
- Run ID:
- Trigger:
- User-visible symptom:
- Detection command:
- Recovery command:
- Validation:
- Artifacts:
- Notes for next run:

### Phase 1.3 - Flink Task Crash

- Phase: 1.3
- Run ID: `20260527T141635Z-904e0208`
- Trigger: The verifier submitted the CDC job with a one-shot task-crash hook and inserted
  `event_id=1303`, causing a controlled Flink operator exception mid-stream.
- User-visible symptom: The streaming job stayed under the same Flink job id
  `02984c321fbe1c4ac3166c203664fffd`, entered recovery, and resumed processing after the
  automatic task restart.
- Detection command: `make eo-verify ARGS="--failure task-crash,checkpoint-restore"`
- Recovery command: No manual job restart. The job's fixed-delay restart strategy recovered the
  failed task; the verifier then continued with update and delete events.
- Validation: `snapshot_diff_count=0`; source and Iceberg final snapshots each had 3 rows; the
  event-id audit matched current-table ids `[1303, 1304, 2302]` and 9 changelog rows.
- Artifacts: `showcase/results/eo_reconciliation.json` and
  `showcase/logs/phase-1.3-eo-verify.log`
- Notes for next run: Keep the crash marker path unique per run so a previous local marker cannot
  suppress the induced failure.

### Phase 1.3 - Checkpoint Restore

- Phase: 1.3
- Run ID: `20260527T141635Z-904e0208`
- Trigger: The verifier inserted baseline rows, waited for checkpoint `5`, canceled job
  `72e23d19d5a02ffccfbcc6bb3eff7354`, and restored from the retained checkpoint directory.
- User-visible symptom: The original job stopped and a new Flink job
  `844a8099db3e014b5013c85491794222` started from
  `file:///opt/flink/checkpoints/72e23d19d5a02ffccfbcc6bb3eff7354/chk-5`.
- Detection command: `make eo-verify ARGS="--failure task-crash,checkpoint-restore"`
- Recovery command: `flink run -s <retained checkpoint path>` through the repo-root
  `make eo-verify` harness.
- Validation: `snapshot_diff_count=0`; source and Iceberg final snapshots each had 3 rows; the
  event-id audit matched current-table ids `[3303, 3304, 4302]` and 9 changelog rows.
- Artifacts: `showcase/results/eo_reconciliation.json` and
  `showcase/logs/phase-1.3-eo-verify.log`
- Notes for next run: Resolve the retained checkpoint path after cancellation; a REST-reported
  checkpoint can be superseded by a later retained checkpoint during shutdown.

### Phase 2.1 - JobManager Restart

- Phase: 2.1
- Run ID: `20260527T151754Z-ef73a5a5`
- Trigger: The verifier inserted baseline rows, waited for checkpoint `7`, then restarted the
  Flink JobManager container for job `0d5ef97a6af890188267e5eab681a535`.
- User-visible symptom: The session job was no longer active after the JobManager restart; the
  verifier brought the TaskManager service back, restored from
  `file:///opt/flink/checkpoints/0d5ef97a6af890188267e5eab681a535/chk-7`, and continued as job
  `6d2b471d567b56fe5211105861a7de73`.
- Detection command: `make eo-verify ARGS="--failure all"`
- Recovery command: JobManager restart plus TaskManager re-registration, then `flink run -s
  <latest checkpoint path>` through the repo-root verifier.
- Validation: `snapshot_diff_count=0`; source and Iceberg final snapshots each had 3 rows; the
  event-id audit matched current-table ids `[5303, 5304, 6302]` and 9 changelog rows.
- Artifacts: `showcase/results/eo_reconciliation.json` and
  `showcase/logs/phase-2.1-eo-verify.log`
- Notes for next run: In this single-node Compose setup, restart the TaskManager after a
  JobManager container restart before waiting for a registered worker.

### Phase 2.1 - Savepoint Restore

- Phase: 2.1
- Run ID: `20260527T151754Z-ef73a5a5`
- Trigger: The verifier inserted baseline rows for job `4838f8c175f327962bd3366a35cf140d`,
  required a completed checkpoint, created
  `file:/opt/flink/savepoints/savepoint-4838f8-e7a91c2a50d2`, canceled the job, and restored from
  that explicit savepoint.
- User-visible symptom: The original job stopped and replacement job
  `2fb90d4411d257d8548bc2c34e1a0dda` resumed from the savepoint before update/delete/insert
  events were applied.
- Detection command: `make eo-verify ARGS="--failure all"`
- Recovery command: `flink savepoint <job> /opt/flink/savepoints`, `flink cancel <job>`, then
  `flink run -s <savepoint path>` through the repo-root verifier.
- Validation: `snapshot_diff_count=0`; source and Iceberg final snapshots each had 3 rows; the
  event-id audit matched current-table ids `[7303, 7304, 8302]` and 9 changelog rows.
- Artifacts: `showcase/results/eo_reconciliation.json` and
  `showcase/logs/phase-2.1-eo-verify.log`
- Notes for next run: Wait for at least one completed checkpoint before creating the savepoint so
  a missing or restarting TaskManager fails early instead of hanging the savepoint command.

### Phase 2.1 - Sink Commit Fault

- Phase: 2.1
- Run ID: `20260527T151754Z-ef73a5a5`
- Trigger: The verifier submitted the job with the test-only
  `--checkpoint-complete-fault-event-id=9303` flag. After event `9303` was observed, the injected
  operator threw once from `CheckpointListener.notifyCheckpointComplete` at checkpoint `7`.
- User-visible symptom: The job failed during the checkpoint-complete commit phase, wrote marker
  `/tmp/p1-phase-2-1-sink-commit-abd5a4f5ede24eafa73ec1b053d1c7bf.marker`, and recovered through
  the normal Flink restart strategy.
- Detection command: `make eo-verify ARGS="--failure all"`
- Recovery command: No manual restart. The job's fixed-delay restart strategy recovered after the
  checkpoint-complete callback failure.
- Validation: `snapshot_diff_count=0`; source and Iceberg final snapshots each had 3 rows; the
  event-id audit matched current-table ids `[9303, 9304, 10302]` and 9 changelog rows.
- Artifacts: `showcase/results/eo_reconciliation.json` and
  `showcase/logs/phase-2.1-eo-verify.log`
- Notes for next run: The fault is commit-time by construction: it is not thrown while mapping a
  record; it is thrown from the checkpoint-complete callback that drives Iceberg sink commits.

### Phase 2.3 - Checkpoint Backpressure Thresholds

- Phase: 2.3
- Run ID: `20260527T233135Z-0b65b846`
- Trigger: The metrics harness inserted a 320-event MySQL spike while the CDC job ran with
  test-only 40 ms main-path and 120 ms alignment-probe sleep gates.
- User-visible symptom: Checkpoint duration rose from a 55 ms baseline max to 19,022 ms under
  load; reporter alignment time rose from 5.008125 ms to 16,882.2503 ms; the reporter-derived
  pressure indicator peaked at 0.649; Iceberg changelog lag peaked at 320 events and recovered
  to 0.
- Detection command: `make ckpt-metrics`
- Recovery command: No manual recovery. The job drained the backlog after the spike and the
  harness canceled the job after three zero-lag recovery samples.
- Validation: `summary.passed=true`; checkpoint duration, alignment time, pressure indicator,
  checkpoint failure count recording, lag observation, and lag recovery checks all passed.
- Artifacts: `showcase/results/checkpoint_metrics.json`,
  `showcase/logs/phase-2.3-checkpoint-metrics.log`, and
  `showcase/media/phase-2.3-checkpoint-metrics.svg`
- Notes for next run: In this single-node CDC topology, explicit hard/soft backpressured-time
  stayed at 0, so the documented pressure indicator uses Flink reporter busy-time saturation
  when explicit backpressured time is zero.

### Phase B2 - Incompatible Avro Schema Rejection

- Phase: B2
- Run ID: `20260820T120104Z-7e40acd6`
- Trigger: The guarded verifier read value subject `broker.cdc_lab.orders-value` version `1`,
  changed the nested `event_id` type from `long` to `string`, checked compatibility, and then
  attempted to register the incompatible schema under subject-level `BACKWARD` compatibility.
- User-visible symptom: Schema Registry returned HTTP `409`; the value subject versions stayed
  `[1]` and its latest schema was unchanged. Debezium connector/task and the Flink job stayed
  `RUNNING`, so no incompatible record entered the topic and old-schema traffic continued.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--phase contracts --baseline-events 2 --seed 211"`
- Recovery command: No service or job restart. Keep Registry version `1`, reject the candidate,
  and continue producing with the previously registered schema; the verifier wrote deterministic
  post-rejection event `event_id=19000211` to prove continuity.
- Validation: All 15 contract checks passed; registration HTTP status was `409`; subject versions
  were `[1]` before and after rejection; checkpoint ID advanced from `5` to `7`; the
  post-rejection event was visible; all three recorded partition lags were `0`; source and
  Iceberg each ended with 3 rows and row-level diff `0`.
- Artifacts: `showcase/results/schema_contract_drill.json`,
  `showcase/logs/phase-b2-broker-up-20260820T115832Z.log`, and
  `showcase/logs/phase-b2-broker-verify-20260820T115942Z.log`
- Notes for next run: Start with `remote-broker-up ARGS="--phase contracts --fresh"`. Kafka's
  group report can omit a partition that has never received a record; treat it as zero lag only
  when the independently queried topic end offset for that partition is also zero.

## Recovery Procedures

### Core Stack Reset

Use only when a phase explicitly allows a clean reset:

```bash
make preflight-heavy
make down
make up-core
make ps
```

If Docker is unresponsive because the host disk is full, do not loop on Docker commands. Free
disk first, restart/recover Docker if needed, then run `make down` once to remove the lab
containers and volumes.

### Source Data Regeneration

Generator runs are deterministic by seed. For a clean database, the same `--events` and `--seed` produce the same event-id range and logical stream.

```bash
make gen ARGS="--events 10000 --seed 1"
make sql-mysql Q="SELECT MIN(event_id), MAX(event_id), COUNT(*) FROM orders"
```

## Artifact Rules

- Raw command transcripts belong in `showcase/logs/`.
- Result JSON belongs in `showcase/results/` and must include provenance.
- Screenshots, recordings, and diagrams belong in `showcase/media/`.
