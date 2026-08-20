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

### Phase B3 - Kafka Broker Restart Mid-Stream

- Phase: B3
- Run ID: `20260820T130027Z-0b79472b`
- Trigger: With the deterministic 120-event, seed-301 producer still active, the verifier ran
  `docker compose ... kill kafka` after the Path B consumer had committed 63 offsets. The Kafka
  container exited with code `137`.
- User-visible symptom: Kafka became unavailable during steady ingest. Debezium and the Flink
  Kafka source paused until the same container was restarted and healthy, then resumed without a
  replacement Flink job.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--failure broker-restart"`
- Recovery command: The drill ran `docker compose ... start kafka`, waited for Kafka readiness
  and Debezium connector/task state `RUNNING`, then let the existing consumer resume from its
  committed offsets.
- Validation: All 10 checks passed. The Flink checkpoint advanced from `1` to `113`; final
  partition offsets were `35/35/50`, each with lag `0`; the source and Iceberg snapshots each had
  120 rows and `snapshot_diff_count=0`; the event-id audit was consistent.
- Artifacts: `showcase/results/broker_restart_drill.json`,
  `showcase/logs/phase-b3-broker-up-20260820T125100Z.log`, and
  `showcase/logs/phase-b3-broker-restart-broker-verify-20260820T125335Z.log`
- Notes for next run: Compose omits stopped containers from `ps -q` unless `--all` is used. Keep
  the container lookup stop-aware so cleanup can restart the exact Kafka container after a kill.

### Phase B3 - Forced Duplicate Redelivery

- Phase: B3
- Run ID: `20260820T130839Z-063ce577`
- Trigger: After 36 seed-302 events reached Iceberg and checkpoint `5` completed, the verifier
  canceled the first Path B job and executed `kafka-consumer-groups.sh --reset-offsets
  --to-earliest --execute` for all three partitions.
- User-visible symptom: A replacement Path B job consumed every Kafka event a second time. The
  append-only changelog grew from 36 to 72 rows while the keyed current table stayed at 36 rows.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--failure duplicate-redelivery"`
- Recovery command: Rewind consumer group `p1-b3-duplicate-redelivery-302` to offset `0` on
  partitions `0`, `1`, and `2`, then submit a new Path B job from committed offsets.
- Validation: All 8 checks passed. The event-id audit found exactly 36 duplicate occurrences
  across 36 distinct event IDs, the current table remained unique, final Kafka lag was `0`, and
  source-vs-Iceberg `snapshot_diff_count=0`.
- Artifacts: `showcase/results/duplicate_redelivery_drill.json`,
  `showcase/logs/phase-b3-broker-up-20260820T130522Z.log`, and
  `showcase/logs/phase-b3-duplicate-redelivery-broker-verify-20260820T130645Z.log`
- Notes for next run: Kafka 3.9.2 can print all offset-reset rows on one physical line. Parse the
  repeated group/topic/partition/offset tokens, not newline layout.

### Phase B3 - Out-of-Order Mis-Keying Boundary

- Phase: B3
- Run ID: `20260820T131228Z-8531bebf`
- Trigger: The isolated Avro probe first produced event IDs `1003030..1003032` with the real
  primary-key key, then produced a controlled sequence `2003030, 2003032, 2003031` for one value
  key using three deliberately incorrect Kafka keys and explicit partitions `0`, `1`, and `2`.
- User-visible symptom: Correctly keyed events stayed on partition `2` at offsets `0,1,2` and
  remained ordered. The mis-keyed sequence crossed partitions and contained one non-monotonic
  transition; applying arrival order would leave event `2003031` instead of canonical final event
  `2003032`.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--failure mis-keying"`
- Recovery command: Reject the audited mis-keyed batch before main-pipeline admission and retain
  primary-key keying. The probe topic is isolated, so no repair write was made to the main topic.
- Validation: All 11 checks passed. The audit detected exactly one ordering violation, proved the
  arrival-applied final state would be wrong, and kept the 18-row main Path B workload at
  `snapshot_diff_count=0` with a consistent event-id audit.
- Artifacts: `showcase/results/ordering_miskey_drill.json`,
  `showcase/logs/phase-b3-broker-up-20260820T130935Z.log`, and
  `showcase/logs/phase-b3-ordering-miskey-broker-verify-20260820T131048Z.log`
- Notes for next run: Kafka ordering is per partition, not global. Keep the controlled mis-keyed
  records out of the main topic; their purpose is to demonstrate and detect the boundary, not to
  manufacture a successful total-order claim.

### Phase B3 - Poison Message Quarantine and Repair

- Phase: B3
- Run ID: `20260820T131949Z-2aea4b71`
- Trigger: With Debezium paused, the verifier wrote the intended source row and injected its
  malformed non-Avro JSON bytes into main topic partition `2`, offset `5`.
- User-visible symptom: The Path B deserializer rejected the value as
  `org.apache.kafka.connect.errors.DataException` but kept the main Flink job running. Exactly one
  record appeared in `broker.cdc_lab.orders.dlq` with source topic/partition/offset, error type and
  message, and the original key/value bytes.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--failure poison-dlq"`
- Recovery command: Decode the intended row from the DLQ metadata, encode it with registered Avro
  value schema ID `2`, replay it with the primary-key key, wait for zero-diff convergence, resume
  Debezium, and send one additional source event to prove continued flow.
- Validation: All 12 checks passed. DLQ count was exactly `1`; the original bytes were preserved;
  checkpoint ID advanced from `2` to `22`; the connector returned to `RUNNING`; the post-poison
  event became visible; and the final 14-row source/Iceberg snapshots had diff `0`.
- Artifacts: `showcase/results/poison_dlq_drill.json`,
  `showcase/logs/phase-b3-broker-up-20260820T131620Z.log`, and
  `showcase/logs/phase-b3-poison-dlq-broker-verify-20260820T131737Z.log`
- Notes for next run: Debezium Connect 3.2 rejects urllib's default form media type on pause/resume
  requests. Send explicit `Accept: application/json` and `Content-Type: application/json`.

### Phase B3 - Offset-Zero and Timestamp Replay

- Phase: B3
- Run ID: `20260820T135530Z-411754b4`
- Trigger: After the original 36-row seed-305 workload reached lag `0`, the verifier recorded
  timestamp `1787233988707`, applied a complete update sweep, captured the original Iceberg
  snapshot, and twice dropped/recreated both logical Iceberg tables.
- User-visible symptom: An initial development run exposed stale final rows when a Path B decode
  rebalance merged two channels and broke Kafka's per-key order. The certified job preserves the
  ordered decode/current segment and uses replay-only latest-per-key coalescing before rebuilding
  first from offset `0`, then from the chosen timestamp.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--failure offset-replay"`
- Recovery command: Recreate the Iceberg current/changelog tables, submit the replay job from
  `earliest`, validate it, recreate the tables again, then submit from timestamp
  `1787233988707`; preserve one ordered channel until keyed routing and apply the replay-only
  10-second latest-per-key quiet window.
- Validation: All 10 checks passed. Offset-zero and timestamp row-level diff counts were both
  `0`; both rebuilt digests equaled the original
  `17ed71ec943ec3a57a8635ba90ab6e2bcae0a7a3db34c146adc6c8b36305487e`; timestamp starts
  resolved to offsets `9/15/12`; all final partition lags were `0`; and original, offset-zero,
  and timestamp runs recorded distinct Iceberg snapshot lineages.
- Artifacts: `showcase/results/offset_replay_drill.json`,
  `showcase/logs/phase-b3-broker-up-20260820T135104Z.log`, and
  `showcase/logs/phase-b3-offset-replay-broker-verify-20260820T135221Z.log`
- Notes for next run: Preserve the Path B ordered segment. The failed pre-fix timeout remains in
  `showcase/logs/phase-b3-offset-replay-broker-verify-20260820T132144Z.log`; do not treat Kafka
  lag `0` as a substitute for final row-level reconciliation.

### Phase B4 - Measured Kafka Broker Restart Recovery

- Phase: B4
- Run ID: `20260820T162859Z-0828bdbb`
- Trigger: During the fixed 100,000-event seed-401 workload, the verifier killed Kafka after
  25,000 committed source events, held the outage for five seconds, and restarted the same
  container.
- User-visible symptom: Exact consumer-group lag peaked at 75,000 while the source finished;
  Flink then drained all three partitions back to lag `0`.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000
  --fault-after-events 25000 --outage-seconds 5"`
- Recovery command: `docker compose ... start kafka`; then continuously require both the
  connector and task to be `RUNNING` and at least 100,000 topic records. If a task reports
  `FAILED`, call Kafka Connect `POST .../restart?includeTasks=true&onlyFailed=true`. The certified
  run required zero task-restart calls.
- Validation: Recovery took 47.688 s; source and Iceberg each had 100,000 rows; every broker
  restart check passed; `snapshot_diff_count=0`; final partition lag was `0/0/0`.
- Artifacts: `showcase/results/broker_slo.json`,
  `showcase/logs/phase-b4-broker-up-20260820T162222Z.log`, and
  `showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`
- Notes for next run: Do not accept connector state alone. A connector can be `RUNNING` while a
  source task has failed; require task state plus delivered topic progress. The earlier timeout
  remains in `showcase/logs/phase-b4-broker-verify-20260820T154554Z.log`.

### Phase B4 - Measured Duplicate Redelivery Recovery

- Phase: B4
- Run ID: `20260820T162859Z-0828bdbb`
- Trigger: After the fixed workload converged, the verifier canceled the Path B job, rewound all
  partitions of consumer group `p1-b4-slo-401` to offset `0`, and started a replacement job from
  those committed offsets.
- User-visible symptom: All 100,000 Kafka records were deliberately delivered again to the
  append-only changelog while the keyed current table remained at 100,000 rows.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000
  --fault-after-events 25000 --outage-seconds 5"`
- Recovery command: `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`, followed
  by a new Path B job from committed offsets and lag-zero checkpoint convergence.
- Validation: Recovery took 50.696 s; the replay increment was exactly 100,000 records; duplicate
  occurrence accounting was exact; source and Iceberg each had 100,000 rows and diff `0`.
- Artifacts: `showcase/results/broker_slo.json` and
  `showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`
- Notes for next run: Use the actual pre-replay topic record count and changelog baseline. A
  broker fault may add auditable redelivery before the deliberate rewind, so a hard-coded
  `events * 2` changelog target is not a safe completion condition.

### Phase B4 - Measured Mis-Keying Rejection

- Phase: B4
- Run ID: `20260820T162859Z-0828bdbb`
- Trigger: The verifier wrote event IDs `5004010, 5004012, 5004011` for one logical order with
  deliberately wrong keys to explicit partitions `0, 1, 2` on the isolated ordering probe topic.
- User-visible symptom: Arrival order contained one non-monotonic transition, demonstrating the
  boundary of Kafka's per-partition ordering guarantee.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000
  --fault-after-events 25000 --outage-seconds 5"`
- Recovery command: Reject the audited three-record batch before main-topic admission and retain
  primary-key keying; no repair write enters the main pipeline.
- Validation: Detection and rejection took 24.456 s; the probe spanned three partitions, the
  audit found the violation, and the 100,000-row main source/Iceberg snapshots retained diff `0`.
- Artifacts: `showcase/results/broker_slo.json` and
  `showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`
- Notes for next run: This timer ends at pre-admission rejection. Do not describe it as recovery
  from corruption that entered the current-state table.

### Phase B4 - Measured Poison DLQ Repair

- Phase: B4
- Run ID: `20260820T162859Z-0828bdbb`
- Trigger: With Debezium paused, the verifier inserted the intended source row and wrote malformed
  non-Avro bytes to the main topic.
- User-visible symptom: Exactly one record entered the DLQ with error metadata and original
  bytes; the main job stayed available for repair and subsequent traffic.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000
  --fault-after-events 25000 --outage-seconds 5"`
- Recovery command: Decode the preserved repair document, encode it with registered Avro schema
  ID `2`, replay it with the primary-key key, resume Debezium, and insert a continuity row.
- Validation: Quarantine, repair, resume, and continued lag-zero convergence took 52.414 s; the
  DLQ count was exactly `1`; source and Iceberg ended at 100,002 rows with diff `0`.
- Artifacts: `showcase/results/broker_slo.json` and
  `showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`
- Notes for next run: Require zero-diff convergence before resuming the connector, then prove
  continued flow with a distinct source event rather than treating DLQ arrival as recovery.

### Phase B4 - Measured Offset-Zero Rebuild

- Phase: B4
- Run ID: `20260820T162859Z-0828bdbb`
- Trigger: After the poison repair and continuity event, the verifier canceled the active job,
  dropped/recreated both Iceberg tables, and submitted a fresh consumer group from offset `0`.
- User-visible symptom: Serving state was empty until the 100,002-record topic history rebuilt
  the current and changelog tables.
- Detection command: `make remote-broker-verify
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills
  ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000
  --fault-after-events 25000 --outage-seconds 5"`
- Recovery command: Recreate the Iceberg tables and start consumer group
  `p1-b4-slo-offset-replay-401` from `earliest` with the existing replay-only 10-second
  latest-per-key quiet window.
- Validation: The offset-zero rebuild took 38.008 s; all 100,002 rows returned; the rebuilt
  digest matched the pre-reset digest; checkpoint `6` completed; all partition lags and the final
  row-level diff were `0`.
- Artifacts: `showcase/results/broker_slo.json` and
  `showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`
- Notes for next run: Keep the replay quiet-window semantics unchanged and require both digest
  equality and row-level reconciliation; lag `0` alone is not a rebuild proof.

## Recovery Procedures

### Path B DLQ Triage and Repair

Use this standing procedure only for a Path B record already quarantined in
`broker.cdc_lab.orders.dlq`. The certified sequence and its stop conditions come from the
[Phase B3 poison-message incident](#phase-b3---poison-message-quarantine-and-repair),
[`poison_dlq_drill.json`](showcase/results/poison_dlq_drill.json), and the
[raw transcript](showcase/logs/phase-b3-poison-dlq-broker-verify-20260820T131737Z.log).

1. Keep the Flink main job running. If the intended source row is also pending in
   MySQL/Debezium, pause only the Debezium connector and record its connector/task state; this
   prevents an operator repair and the source producer from racing each other.
2. Read the scoped DLQ record without deleting it. Preserve the source topic, partition, offset,
   error type/message, original key bytes, and original value bytes in the incident record.
   Stop if any of these fields is absent or if the number of records cannot be explained.
3. Decode the preserved repair document and verify the intended primary key and row against the
   source of truth. Do not send the malformed bytes back to the main topic.
4. Resolve the active registered key/value schemas, encode the repaired Debezium envelope as
   Confluent-wire Avro, and publish it to the main topic with the MySQL primary key as the Kafka
   record key. Record the schema ID plus the produced topic/partition/offset.
5. Before resuming Debezium, require a completed Flink checkpoint after the repair, zero committed
   lag on every partition, and equality-delete-aware source-vs-Iceberg row reconciliation with
   `snapshot_diff_count=0`. Treat DLQ arrival alone as detection, not recovery.
6. Resume Debezium, require both connector and task state `RUNNING`, then observe a distinct
   post-repair source event reach Iceberg at lag zero. Retain the DLQ record and repair linkage as
   evidence; do not silently purge it.

The isolated rehearsal remains:

```bash
make remote-broker-verify \
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills \
  ARGS="--failure poison-dlq"
```

The certified rehearsal quarantined exactly one record, replayed it with registered Avro schema
ID `2`, advanced checkpoint `2 → 22`, resumed normal flow, and ended with a 14-row zero-diff
reconciliation. Those values describe that recorded run, not a universal incident threshold.

### Path B Offset Replay and Fresh-Table Rebuild

This is a standing rebuild procedure, not an in-place offset experiment. The certified basis is
the [Phase B3 replay incident](#phase-b3---offset-zero-and-timestamp-replay),
[`offset_replay_drill.json`](showcase/results/offset_replay_drill.json), and its
[raw transcript](showcase/logs/phase-b3-offset-replay-broker-verify-20260820T135221Z.log).

1. Declare the rebuild target and consumer group. Prefer a fresh table namespace; in this lab the
   procedure drops/recreates both logical Iceberg tables, so run it only in an isolated drill or
   an explicitly approved maintenance window. Preserve the original source snapshot, Iceberg
   rows/digest, Kafka end offsets, completed checkpoint, and Iceberg snapshot IDs first.
2. Verify topic retention covers the requested history. Choose one start policy:
   - **Offset zero:** start a new consumer group from `earliest`; this is the default complete
     rebuild.
   - **Timestamp:** resolve one start offset per partition for the chosen timestamp. Use this only
     when records at or after that timestamp contain a complete current image for every primary
     key. The certified drill created that precondition with a complete update sweep; without it,
     fall back to offset zero.
3. Preserve Path B's ordered decode/current-state segment and enable only the certified replay
   policy: latest-per-primary-key coalescing with a 10-second quiet period. Normal streaming does
   not use this replay-only coalescing policy.
4. Recreate the target current and changelog tables, submit the replay job with the new consumer
   group/start policy, and record the resolved partition offsets.
5. Require a completed post-replay checkpoint, committed lag `0` for every partition, distinct new
   Iceberg snapshot lineage, row-level diff `0`, and a snapshot digest equal to the preserved
   original. Kafka lag `0` alone is not acceptance.
6. Cut readers over only after all gates pass. On any missing key, unexpected row, digest mismatch,
   or incomplete timestamp coverage, keep the rebuilt tables isolated, retain the failure
   transcript, and restart from offset zero after correcting the cause.

The isolated rehearsal remains:

```bash
make remote-broker-verify \
  P1_REMOTE_ROOT=exactly-once-workstation:/root/autodl-tmp/exactly-once-drills \
  ARGS="--failure offset-replay"
```

The recorded timestamp `1787233988707` resolved to partition offsets `9/15/12`; both it and the
offset-zero rebuild matched the preserved digest and row set. Those offsets belong only to run
`20260820T135530Z-411754b4` and must never be copied into another incident.

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
