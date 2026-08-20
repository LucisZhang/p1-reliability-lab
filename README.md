# Reliability Lab — MySQL CDC → Flink → Iceberg

[![ci](https://github.com/LucisZhang/p1-reliability-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/LucisZhang/p1-reliability-lab/actions/workflows/ci.yml)

A single-node **reliability lab** for a real-time data pipeline:
`MySQL CDC → Flink 1.20 → Apache Iceberg v2 (upsert)`.

The point is not wiring components together — it is **reproducible evidence of
correctness under failure**. A Python harness induces real failures in the running
pipeline and proves exactly-once delivery by reconciling the final MySQL source
snapshot against the final Iceberg snapshot, row by row, through a correctness-safe
reader. Every claim is backed by a committed, machine-checkable JSON artifact with
full provenance (`run_id`, `git_sha`, exact command, logs).

## Verified claims

Claims are **gated**: a claim is added to
[`docs/resume-claims-after-verification.md`](docs/resume-claims-after-verification.md)
only after the phase that proves it has passed and produced auditable JSON under
[`showcase/results/`](showcase/results/).

| Claim | Evidence |
| --- | --- |
| Exactly-once final-state reconciliation across **five induced failure classes** — task crash, retained-checkpoint restore, JobManager restart, savepoint restore, and a deterministic checkpoint-complete sink-commit fault — with **zero snapshot diff** in every class. | [`showcase/results/eo_reconciliation.json`](showcase/results/eo_reconciliation.json) (run `20260527T151754Z-ef73a5a5`), incident log in [`RUNBOOK.md`](RUNBOOK.md) |
| CDC correctness smoke: source-vs-Iceberg final-state parity including updates and deletes, changelog audit counts, and equality-delete file metadata evidence. | [`showcase/results/phase-1.2-cdc-smoke.json`](showcase/results/phase-1.2-cdc-smoke.json) |
| Iceberg small-file maintenance: `rewrite_data_files` + manifest rewrite measurably reduced data-file and manifest counts, raised median file size, and lowered `planFiles()` planning latency. | [`showcase/results/iceberg_small_file_rewrite.json`](showcase/results/iceberg_small_file_rewrite.json), chart in [`showcase/media/`](showcase/media/) |
| Checkpoint behavior under load: real Prometheus-reporter metrics show checkpoint duration/alignment rising under a deterministic input spike, backpressure appearing, Iceberg commit lag growing and **recovering to zero**. | [`showcase/results/checkpoint_metrics.json`](showcase/results/checkpoint_metrics.json), chart in [`showcase/media/`](showcase/media/) |
| Broker ingress parity: the same deterministic 1,000-event, seed-17 workload through preserved Path A and Kafka Path B converged to the same Iceberg final-state digest with row-level diff `0`; the recorded Path B offsets have lag `0` and are linked to a completed Flink checkpoint and Iceberg snapshot IDs. | [`showcase/results/broker_parity.json`](showcase/results/broker_parity.json) (run `20260820T102311Z-5bbec087`), raw log in [`showcase/logs/phase-b1-broker-verify-20260820T101332Z.log`](showcase/logs/phase-b1-broker-verify-20260820T101332Z.log) |

## Current captured run

Run `20260711T034018Z-local-mac` at evidence commit `7eab9c3` is one recorded
Apple Silicon macOS run with 16 GiB host RAM. Its Docker Desktop VM reported
10 CPUs and about 7.65 GiB memory. All five induced failure classes recovered
with zero snapshot differences and consistent event-ID audits.

| Failure class | Recorded result | Primary evidence |
| --- | --- | --- |
| Task crash | Pass | [`eo_reconciliation-all.json`](docs/workstation-run/20260711T034018Z-local-mac/eo_reconciliation-all.json) |
| Checkpoint restore | Pass | Same captured-run JSON, `results[1]` |
| JobManager restart | Pass | Same captured-run JSON, `results[2]` |
| Savepoint restore | Pass | Same captured-run JSON, `results[3]` |
| Sink commit fault | Pass | Same captured-run JSON, `results[4]` |

Read the [captured-run summary](docs/workstation-run/20260711T034018Z-local-mac/SUMMARY.md)
for the exact commands and environment. This result proves that one run; it does not
establish universal hardware compatibility or one-command reproducibility.

## Path A / Path B architecture

![Path A and broker Path B architecture](showcase/media/phase-b1-path-a-b.svg)

- **Path A (preserved):** MySQL GTID/binlog → the existing Flink CDC job with embedded
  Debezium → Flink checkpoint state → Iceberg v2 keyed upsert. Its code and certified failure
  artifacts remain unchanged.
- **Path B (Phase B1):** MySQL GTID/binlog → standalone Debezium Connect
  (**at-least-once**) → Apache Kafka 3.9.2 offsets → a separate Flink Kafka-source job
  (checkpointed source state) → the same Iceberg v2 keyed upsert model.

The Kafka record key is the MySQL `order_id` primary key. Kafka therefore preserves order for
one key inside its assigned topic partition; it does **not** provide total order across keys or
partitions, and a mis-keyed producer would fall outside that guarantee. The Phase B1 parity
verifier records the committed per-partition offsets, completed Flink checkpoint ID, and current
Iceberg snapshot IDs as one linkage object. The verified linkage and row-level reconciliation are
committed in [`showcase/results/broker_parity.json`](showcase/results/broker_parity.json).

Schema Registry 7.9.8 is part of the `broker` profile with global `BACKWARD` compatibility.
The current Phase B2 implementation uses Registry-backed Avro for Debezium key/value production
and Flink Path B consumption; the committed B1 parity artifact remains an immutable record of
the earlier JSON-wire run. The incompatible-schema runtime drill is not claimed until its remote
`schema_contract_drill.json` evidence is committed. See
[`docs/data-contracts.md`](docs/data-contracts.md) for the contract boundary.

## How the evidence works

- **Correctness-safe reading.** Iceberg v2 upsert tables contain equality deletes;
  pyiceberg is not a correctness reader for them. The lab splits the paths:
  `make sql-iceberg` reads data through **Flink SQL batch**; `make sql-iceberg-meta`
  uses pyiceberg for **metadata only** (files, manifests, snapshots).
- **Results contract.** Every artifact must carry `run_id`, `git_sha`, `started_at`,
  `finished_at`, `stack_versions`, `command`, and `logs`
  ([contract](showcase/results/README.md)); the dashboard sync step validates this
  before an artifact is publishable.
- **Incident log.** [`RUNBOOK.md`](RUNBOOK.md) records each induced failure as an
  incident: trigger, observed symptom, detection/recovery commands, validation,
  artifact links.

## Evidence dashboard (deployable slice)

The heavy pipeline is not a public live demo. The deployable slice is a **static
dashboard** ([`dashboard/`](dashboard/)) built over the exported result JSON — it
renders the artifacts and their provenance and calls no backend.

```bash
make dashboard-build     # validates results contract, then vite build
make dashboard-preview   # serve the built dashboard locally
```

![Recorded evidence dashboard](showcase/media/phase-1.4-dashboard.jpg)

The public portfolio adds an interactive captured-run replay over the same JSON package.
Open the public [Portfolio Phase 2 Review](https://portfolio-site-gpt-review.vercel.app/engineering/p1-reliability-lab).
The isolated Review deployment requires no Vercel login or query secret.

## Local lite mode

On a space-constrained laptop, use the no-Docker path:

```bash
make local-verify
```

This runs harness unit tests, lint/type checks, Maven verification, and the static dashboard
build with results-contract validation. It is the recommended local command for reviewing
the project and building the portfolio evidence dashboard. It does **not** reproduce the live
Flink/MySQL/Iceberg failure run on demand.

## Remote heavy reproduction path

Pinned toolchain: Java 11 (Temurin), Maven 3.9, Python 3.11, Node 20
(see [`docs/version-matrix.md`](docs/version-matrix.md) and `.tool-versions`).

The Mac remains a light-path and Git/evidence machine. Docker runs on the dedicated Linux VM
through disconnect-safe tmux wrappers. No `.git`, `.env`, SSH agent, token, or Git credential
is synced.

```bash
export P1_REMOTE_ROOT='exactly-once-workstation:/root/autodl-tmp/exactly-once-drills'
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--events 1000 --seed 17"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

Every new remote launch records load average and refuses to run above half the logical CPU
count. On a host with `nvidia-smi`, it also records GPU state and refuses active compute;
on the dedicated CPU-only VM, an absent binary is explicitly recorded and skipped.
`make preflight-broker` also checks disk, Docker, and at least 16 GiB total / 8 GiB available
RAM before selecting Kafka KRaft.
See [`docs/broker-remote-execution.md`](docs/broker-remote-execution.md) for reconnect and
append-only sync behavior.

The heavy path should run on a workstation with at least 40 GiB free disk and enough Docker
memory for Flink, MySQL, MinIO, and the Iceberg catalog. The Makefile refuses to start heavy
targets when the repository volume has less than 25 GiB free or Docker does not respond
promptly. See
[`docs/local-lite-and-workstation.md`](docs/local-lite-and-workstation.md) for the full split
between laptop-friendly verification and workstation reproduction.

Lightweight checks (no Docker): `make test` (harness unit tests), `make lint`
(ruff, black, mypy, Maven verify), `make dashboard-build`, or the combined
`make local-verify`.

## CI

GitHub Actions runs the light paths on every push: Python lint + unit tests,
the Flink job Maven build, and the dashboard build with results-contract
validation. The heavy Docker integration (`make eo-verify`, `make test-cdc`)
is intentionally **not** in CI — it runs manually on a single node and its
outputs are committed as auditable artifacts.

## Scope and status

- Verified through **Phase 2.3** plus **Phase B1 broker ingress parity**; the B1 claim is bounded
  to the parity run in [`showcase/results/broker_parity.json`](showcase/results/broker_parity.json),
  not the broker-fault drills. Phase B2 Avro code and light contract tests are present, but no B2
  runtime claim is made until the remote result is committed.
- **StarRocks (M3+) has not been started** — the `olap` compose profile,
  serving-table imports, and the compaction benchmark are reserved future work.
- Single-node Docker Compose only; no cloud, no multi-node, no GPU.
- Local laptops are treated as evidence-review machines, not the default heavy reproduction
  environment. Preserve workstation evidence before making any "reproduced on demand" claim.
