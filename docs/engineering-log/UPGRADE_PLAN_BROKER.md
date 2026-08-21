# Upgrade Plan: Broker Ingress, Data Contracts & Replay Drills

Status: PHASE B5 DONE — 2026-08-21.
Evidence (2026-08-21): `../../README.md`, `../../RUNBOOK.md`, and
`../adr-001-broker-cdc-registry.md`; `make local-verify` passed with 52 tests passed / 1 skipped,
lint, Maven verify, results-contract validation, and the dashboard build green; the relative-link
check passed for all links in the merged README and the ADR. Phase B5 generated no result artifact and
left `showcase/results/` unchanged.
Scope owner: exactly-once-drills (data-platform half of the cloud-native gap).
Explicit non-goals: Kubernetes, Grafana, cloud deployment — those belong to the
frontier-forge serving stack, not this repo. Do not add them here.

---

## Goal

Strengthen this repo's core identity — *reproducible correctness evidence under
failure* — by putting a real message broker between CDC and Flink, enforcing
data contracts, and adding the failure classes that only exist once a broker is
in the path: redelivery, offset replay, poison messages, and schema breaks.
Finish with SLO-style measurements (throughput, freshness, recovery time) so the
project can make quantified reliability statements, not just zero-diff claims.

Why this makes the project stronger rather than broader: Debezium→Kafka delivery
is at-least-once. Moving to a broker therefore makes end-to-end exactly-once
*harder and more honest* — correctness now depends on keyed idempotent upserts
plus event-id auditing, and the reconciliation framework this repo already has
is exactly the right instrument to prove it.

## Context

- Current architecture (call it **Path A**): MySQL 8.0 (ROW binlog, GTID)
  → Flink CDC 3.6.0 connector with embedded Debezium → Flink 1.20.4
  → Iceberg 1.10.x (JDBC catalog + MinIO). Single-node Docker Compose.
- Verified evidence: five failure classes (task crash, checkpoint restore,
  JM restart, savepoint restore, sink-commit fault), all with
  `snapshot_diff_count=0` reconciliation in `showcase/results/`, incidents in
  `RUNBOOK.md`, Prometheus reporter on :9249, static evidence dashboard.
- Conventions that are hard rules (see also `AGENTS.md`):
  - Results are append-only JSON in `showcase/results/` with provenance fields
    (`run_id`, `git_sha`, timestamps, exact command, logs path). Never edit or
    regenerate old records.
  - Every failure drill gets an incident entry in `RUNBOOK.md` using the
    existing template (Phase, Run ID, Trigger, Symptom, Detection command,
    Recovery command, Validation, Artifacts, Notes).
  - CI stays light-path (lint, unit tests, Maven verify, dashboard
    results-contract validation). Heavy Docker integration runs are manual;
    their outputs are committed as auditable JSON.
  - All tool and image versions pinned (mise.toml, pom.xml, compose pins).
  - Make targets guard with preflight checks (disk/memory) before heavy runs.

## Design decisions (already made — do not relitigate)

1. **Broker: Apache Kafka, single-node KRaft mode**, pinned image version.
   Fallback: if the preflight memory check shows the full `broker` profile
   cannot run alongside the core profile on a 16 GB machine, switch to
   **Redpanda** (Kafka-API compatible, single binary, built-in schema
   registry) and record the decision + measured footprints in an ADR under
   `docs/`. Whichever runs, the README must name it precisely.
2. **CDC producer: standalone Debezium** publishing to Kafka topics — choose
   Debezium Server or a single-worker Kafka Connect, whichever is lighter to
   operate in compose; justify the choice in the same ADR.
3. **Serialization & contracts: Avro + Schema Registry** (Confluent community
   image or Apicurio; Redpanda's built-in registry if the fallback triggers).
   Compatibility mode: BACKWARD.
4. **Path A is preserved.** The new broker pipeline (**Path B**) lives behind a
   compose profile (`broker`). Existing certified evidence stays valid and
   Path A stays runnable. The README presents both paths and their different
   delivery-semantics chains.
5. **Topic keying = primary key** so Kafka preserves per-key order within a
   partition. The ordering guarantee and its limits must be stated explicitly
   in the README.
6. **The semantics chain is a first-class deliverable.** Document and test the
   full chain: binlog GTID → Debezium delivery (at-least-once) → Kafka offsets
   → Flink checkpoint (exactly-once within Flink) → Iceberg snapshot
   (idempotent keyed upsert). Reconciliation records must tie Kafka offsets ↔
   Flink checkpoint IDs ↔ Iceberg snapshot IDs together in the results JSON.

## Phases

### Phase B1 — Broker ingress path (parity first, no faults yet)

Requirements:
1. Compose profile `broker`: Kafka (KRaft) + Schema Registry + Debezium,
   pinned versions, added to preflight guards.
2. New Flink job variant consuming from Kafka (keep the Path A job untouched).
3. Parity check: run the same deterministic generator workload
   (fixed `--seed`, fixed `--events`) through Path A and Path B; both must
   converge to identical Iceberg final state (row-level diff = 0).
4. Results: `showcase/results/broker_parity.json` with provenance + the
   offset↔checkpoint↔snapshot linkage fields.

Acceptance: `make broker-verify` green on a fresh environment; parity JSON
committed; README architecture section updated with the Path A/B diagram.

### Phase B2 — Data contracts

Requirements:
1. Avro schemas for the order-change topic(s) registered with BACKWARD
   compatibility; producer and Flink consumer both go through the registry.
2. Unit-level contract tests in `harness/tests/` (schema evolution cases:
   compatible add-with-default passes; field removal / type change fails).
3. Drill: push an incompatible schema. Expected behavior: rejected at the
   registry (or quarantined), pipeline keeps running on the old schema, event
   flow unaffected. Record as a RUNBOOK incident + results JSON.

Acceptance: contract tests in CI; incompatible-schema drill has script,
incident entry, and results artifact.

### Phase B3 — New failure drills

Each drill follows the existing pattern: a reproducible script (deterministic
seed), a reconciliation run, a results JSON with provenance, and a RUNBOOK
incident. All drills must end with `snapshot_diff_count=0` (or the documented
expected quarantine state for the poison drill).

1. **Broker restart mid-stream** — kill/restart the Kafka container during
   steady ingest; pipeline resumes from committed offsets.
2. **Duplicate redelivery** — force redelivery (rewind consumer-group offsets
   or re-produce a batch with identical event ids); prove idempotent
   convergence and report the number of duplicates detected by the event-id
   audit, not just the final zero-diff.
3. **Out-of-order / mis-keying probe** — demonstrate the ordering guarantee's
   boundary: correct per-key order under PK partitioning, then a controlled
   mis-keyed producer run showing how cross-partition interleaving would
   corrupt order, and how the audit detects it. This drill documents a
   *limit*, honestly, rather than pretending total ordering.
4. **Poison message → DLQ** — inject a malformed/undeserializable record; it
   routes to a dead-letter topic with error metadata, the main pipeline
   continues, and an operator procedure (documented in RUNBOOK) repairs and
   replays it to convergence.
5. **Replay / backfill from offset** — rebuild a fresh Iceberg table by
   replaying the topic from offset 0 (and from a chosen timestamp); prove the
   rebuilt table matches the original snapshot row-for-row.

Acceptance: five scripts runnable via `make broker-verify ARGS="--failure <name>"`
(mirroring the existing `eo-verify` interface), five incidents appended to
RUNBOOK, five results JSONs, dashboard panels rendering them via the existing
results contract.

### Phase B4 — SLO measurements & lag observability

Requirements:
1. Export Kafka consumer-group lag and Debezium connector metrics into the
   existing Prometheus setup; capture time-series into results JSON the same
   way `checkpoint_metrics.json` does today.
2. Fixed-workload benchmark (e.g., 100k generated events, fixed seed):
   sustained throughput, end-to-end freshness (MySQL commit → Iceberg commit)
   p50/p95, and recovery time for each Phase B3 drill.
3. `docs/SLO.md`: explicit SLO statements ("after fault X, pipeline recovers
   within Ys with zero reconciliation diff at Z events/s"), the exact hardware
   they were measured on, and a plain disclaimer that this is a single-node
   laptop environment, not cloud production.

Acceptance: SLO.md with measured numbers traceable to results JSONs; lag
metrics visible in the dashboard.

### Phase B5 — Documentation closure

1. README (EN + zh-CN): updated architecture, the delivery-semantics chain for
   both paths, the drill catalog table (now 10 failure classes), and a short
   "production story" paragraph — what broke during this upgrade, what was
   measured, what was traded off.
2. RUNBOOK: DLQ triage procedure and offset-replay procedure as standing
   operational procedures (not just incidents).
3. ADR in `docs/` recording the broker/CDC-runtime/registry choices and
   measured memory footprints.

## Execution environment (local agent, remote execution over SSH)

- The agent executing this plan runs **locally on the Mac**, in the local
  clone. The rented remote box is treated as untrusted for credentials: no
  agent auth, no git push credentials, and no secrets ever land on it — it
  holds only a synced working copy and Docker.
- All Docker/integration execution — smoke runs, the full Phase B3 drill
  suite, and every Phase B4 benchmark — happens on a **dedicated Docker-capable
  Linux VM** rented for this upgrade (NOT the frontier-forge GPU container:
  that box is a restricted container without cap_sys_admin and cannot run a
  Docker daemon — verified 2026-08-19, see
  showcase/logs/phase-b1-remote-preflight-blocked.log). Minimum spec:
  4–8 vCPU, 16 GB RAM, 60 GB disk, Ubuntu 22.04+, root, native Docker CE.
  Never run docker compose on the Mac.
  Rationale: native-Linux numbers on an uncontended machine are the only
  credible basis for SLO.md.
- The current endpoint, project-local SSH config, provisioning state, and
  co-tenancy launch gate are recorded locally in the gitignored
  `.remote/connection.md`. Use the project-local command
  `ssh -F .remote/ssh_config exactly-once-workstation` instead of assuming a
  user-global alias still points at the live AutoDL port. If the local file is
  absent, re-verify the AutoDL console rather than committing an ephemeral root
  endpoint.
- **Phase B1 must first build the remote-execution harness**, mirroring the
  pattern frontier-forge already uses: `make sync-up` (rsync the working tree
  to the remote), `make remote-broker-up` / `make remote-broker-verify [ARGS=…]`
  (ssh wrappers that run the corresponding target on the remote under
  nohup/tmux so an SSH drop cannot kill a long run, streaming/tailing logs
  back), and `make sync-down` (pull results JSON and logs back into the local
  tree). Results are committed **from the Mac** after sync-down; provenance
  still records the remote environment.
- Every results JSON adds an `environment` provenance field (hostname, CPU,
  RAM, OS, docker version). SLO.md's hardware section describes this box.
- The VM is dedicated to this project: no GPU, no co-tenant workloads. The
  preflight load guard stays (refuse certification runs when load1 is already
  high — e.g. a previous run didn't clean up), but the nvidia-smi check must
  degrade gracefully when the binary is absent. The VM may be provisioned
  per-phase and released after Phase B4; SLO.md pins its exact specs.
- Design decision #1's Redpanda fallback remains only as a preflight escape
  hatch; on this box the default is Kafka KRaft.

## Verification (global)

- Fresh-clone bring-up on the remote box: `make broker-up` (with preflight
  guards) then `make broker-verify` completes green; `SMOKE=1` variants exist
  for fast iteration during development.
- CI additions stay light-path only: new harness unit tests (dedup audit,
  contract checks, offset-linkage parsing) + dashboard results-contract
  validation for the new JSON kinds.
- Every headline claim in the updated README traces to a committed results
  file, matching the repo's existing standard.

## Constraints / guardrails

- Do not modify Path A job code, existing drill scripts' behavior, or any file
  under `showcase/results/` (append-only).
- No Kubernetes, no Grafana, no OpenTelemetry, no multi-node claims. Honest
  labeling throughout: single-node, laptop-scale, methodology over scale.
- Memory/disk budget is a real constraint: preflight must measure and refuse
  rather than thrash; the Redpanda fallback rule exists for this reason.
- If any phase cannot produce reproducible evidence within scope, shrink the
  phase — do not add components to compensate.

## Suggested execution order

Sequential, single thread: B1 → B2 → B3 → B4 → B5. The phases share compose
files and harness code; parallel threads would conflict. Estimated effort:
1–2 weeks. B1+B2 alone are a shippable increment (broker ingress + contracts);
stop-and-ship there is acceptable if time pressure requires.

## Narrative targets (for the final README, resume-facing)

- "Extended an exactly-once CDC→Flink→Iceberg pipeline with a Kafka ingress
  and schema-registry contracts; proved end-to-end idempotent convergence
  under broker restarts, forced redelivery, poison messages, and full-topic
  replay — 10 failure classes, each with reproducible scripts and row-level
  reconciliation evidence."
- "Defined and measured SLOs (recovery time, end-to-end freshness p95,
  sustained throughput) on pinned hardware with append-only, provenance-linked
  result artifacts."

## Execution log

- Phase B1 — BLOCKED (acceptance FAILED) 2026-08-19, evidence:
  `showcase/logs/phase-b1-remote-preflight-blocked.log`. Local unit/lint/Maven/dashboard
  gates passed, but the assigned endpoint is a capability-restricted container with no Docker
  daemon/socket/CLI and no `CAP_SYS_ADMIN`; `make broker-up` and `make broker-verify` were not
  launched. No `showcase/results/broker_parity.json` was created, and the phase was not committed
  or pushed.
- Phase B1 — DONE 2026-08-20, evidence: `showcase/results/broker_parity.json`,
  `showcase/logs/phase-b1-broker-up-20260820T101225Z.log`, and
  `showcase/logs/phase-b1-broker-verify-20260820T101332Z.log`. On the dedicated CPU-only Linux VM,
  the guarded broker profile came up healthy and the fixed 1,000-event, seed-17 workload produced
  Path A/Path B Iceberg final-state row-level diff `0`, source/Path B diff `0`, Kafka lag `0`, and
  complete offset↔checkpoint↔snapshot linkage.
- Phase B2 — DONE 2026-08-20, evidence: `showcase/results/schema_contract_drill.json`,
  `showcase/logs/phase-b2-broker-up-20260820T115832Z.log`, and
  `showcase/logs/phase-b2-broker-verify-20260820T115942Z.log`. Run
  `20260820T120104Z-7e40acd6` registered Debezium key/value Avro subjects under `BACKWARD`,
  rejected an `event_id long -> string` value-schema mutation with HTTP `409` without creating
  a new subject version, then proved the old schema continued through checkpoint `5 -> 7`,
  Kafka lag `0`, and final source/Iceberg row-level diff `0`.
- Phase B3 — DONE 2026-08-20, evidence: `showcase/results/broker_restart_drill.json`,
  `showcase/results/duplicate_redelivery_drill.json`,
  `showcase/results/ordering_miskey_drill.json`, `showcase/results/poison_dlq_drill.json`,
  `showcase/results/offset_replay_drill.json`, and the five Phase B3 incidents in `RUNBOOK.md`.
  Five guarded, fresh-volume remote runs certified restart recovery, 36 audited duplicate
  occurrences with idempotent current-state convergence, a detected mis-key ordering violation,
  one-record DLQ quarantine followed by registered-Avro repair, and row-identical offset-zero plus
  timestamp rebuilds. Every required final reconciliation had `snapshot_diff_count=0`; the raw
  certified and failed pre-fix transcripts are retained under `showcase/logs/phase-b3-*.log`.
- Phase B4 — DONE 2026-08-21, evidence: `showcase/results/broker_slo.json`,
  `docs/SLO.md`, `showcase/logs/phase-b4-broker-up-20260820T162222Z.log`, and
  `showcase/logs/phase-b4-broker-verify-20260820T162338Z.log`. The guarded 100,000-event,
  seed-401 run measured 1,791.665 rows/s end-to-end throughput, 15.201 s/20.614 s freshness
  p50/p95, peak exact consumer-group lag 75,000, and recovery times of 47.688/50.696/24.456/
  52.414/38.008 seconds for broker restart, duplicate redelivery, mis-key rejection, poison/DLQ
  repair, and offset-zero rebuild. All five recovery records and the final 100,002-row state had
  `snapshot_diff_count=0`; Flink KafkaSource lag gauges and 16 Debezium JMX samples are retained
  in the result and rendered by the dashboard.
