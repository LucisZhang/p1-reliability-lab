# AGENTS.md

> Durable project knowledge base. Individual Codex Phase prompts point here for stack,
> structure, commands, guardrails, and showcase rules — they do not re-explain them.
> If anything here is ambiguous for a given task, ask before expanding scope.

## Project Overview

A narrow, deep **reliability lab** for a real-time data pipeline. Spine:
`MySQL CDC → Flink → Apache Iceberg (v2, upsert)`, with **StarRocks** importing Iceberg
into internal Primary Key tables for serving and for a compaction-pressure benchmark. The
point is **not** wiring components together — it is **reproducible evidence of correctness
under failure**: exactly-once verified by final source-snapshot vs Iceberg-snapshot
reconciliation (including updates/deletes), CDC correctness, Iceberg small-file maintenance,
and the StarRocks ingestion/compaction tradeoff. Differentiation = failure-mode artifacts
(reconciliation reports, tuning curves, postmortems), not the technology list.

## Project Structure

```
.
├── Makefile                # repo-root entrypoint for ALL commands (see Development Commands)
├── mise.toml               # pinned Java/Python/Node toolchain (also .tool-versions)
├── .env.example            # DB hosts/users/passwords/endpoints for harness + sql wrappers
├── docs/
│   └── version-matrix.md   # chosen versions + date + compat rationale + known incompat
├── infra/
│   ├── docker-compose.yml   # Compose PROFILES: `core` and `olap`
│   ├── mysql/               # init SQL (source DB + iceberg_catalog DB), binlog ROW + GTID
│   ├── flink/               # flink-conf (checkpoints/savepoints + metrics reporter), libs
│   └── starrocks/           # FE/BE config + memory limits; external-catalog + internal-table init
├── flink-jobs/              # Java/Maven: CDC source → Iceberg v2 upsert sink; operator UIDs
├── harness/                 # Python: generator, sql wrappers, eo_verify, cdc tests,
│   │                        #   small-file rewrite driver, metrics scrape, compaction_bench,
│   │                        #   dq, backfill, provenance
│   ├── harness/
│   ├── tests/
│   └── requirements.txt
├── dashboard/               # static read-only evidence dashboard (deployable slice)
│   ├── public/results/      # JSON synced here pre-build (Vite serves public/ as-is)
│   └── src/
├── scripts/
│   └── sync-results-to-dashboard.sh  # copies validated showcase/results/*.json -> dashboard/public/results/
├── showcase/
│   ├── results/*.json       # exported results (each carries provenance — see below)
│   ├── logs/                # raw command transcripts referenced by results JSON
│   ├── media/               # screenshots, recordings, architecture diagram
│   └── NOTES.md             # one appended line per Phase: what was produced + why it matters
├── RUNBOOK.md               # created as a SKELETON in Phase 1.1; appended after each failure phase
├── README.md               # visitor-facing; OPENS with failure evidence, not architecture
└── docs/resume-claims-after-verification.md  # resume lines, each gated on a Phase passing
```

## Tech Stack (pinned 2026-05-26 — rationale + links in docs/version-matrix.md; change only with a recorded reason)

- **Apache Flink 1.20.x** (e.g. 1.20.4) — current 1.x line; required because Flink CDC 3.x latest only supports 1.19+.
- **Flink CDC 3.6.0** (build for Flink 1.20.x), MySQL connector — **latest stable; CDC 3.3+ dropped Flink 1.17/1.18, so 1.18 is NOT an option.** Requires JDK 11+.
- **Apache Iceberg 1.10.x** via `iceberg-flink-runtime-1.20` (1.11.0 acceptable) — **v2 table format** (required for upsert/row-level deletes).
- **StarRocks 3.3.x** (3.5.x acceptable) — reads Iceberg via external JDBC catalog, imports into **internal Primary Key tables**. PK **size-tiered compaction** toggle `enable_pk_size_tiered_compaction_strategy` (BE config) is a benchmark dimension. *(Note: PK size-tiered compaction landed across 3.1–3.3; we use ≥3.3 for currency, not because 3.3 introduced it.)*
- **MySQL 8.0** — CDC source; binlog `ROW`, GTID on. Also hosts the **Iceberg JDBC catalog** schema.
- **MinIO** — local S3 (path-style) for Iceberg warehouse.
- **pyiceberg** — **metadata-only** reads (file counts, manifests, snapshots, small-file metrics). **NOT** used to materialize table data for reconciliation — PyIceberg does not apply Iceberg **equality deletes**, and Flink CDC upsert writes equality deletes, so a PyIceberg data read of `orders_current` would crash or (worse) silently return a stale view and falsely report "diff = 0".
- **Iceberg final-state data reader = Flink SQL batch** (Table API/SQL in batch mode against the same JDBC catalog + MinIO warehouse) — Flink correctly applies v2 equality deletes (it is the writer). A small tested Java Iceberg verification CLI, or a local Spark verifier with the matching Iceberg runtime, are acceptable alternatives. This reader backs `make sql-iceberg`, `eo-verify`, and `test-cdc`.
- **Java 11**, **Maven 3.9**, **Python 3.11**, **Node 20** — pinned in `mise.toml`/`.tool-versions`.

No GPU. Single node. No managed cloud. See Resource Profiles below.

## Iceberg catalog (single concrete choice — JDBC)

One catalog, shared by Flink (writer) and StarRocks (reader):
- **JDBC catalog** backed by a dedicated `iceberg_catalog` schema in the same MySQL instance.
- Warehouse on MinIO, path-style S3: endpoint `http://minio:9000`, bucket `warehouse`.
- Flink catalog props and the StarRocks `CREATE EXTERNAL CATALOG` statement (with
  `iceberg.catalog.type=jdbc`, `iceberg.catalog.uri`, warehouse, and S3 path-style + keys)
  are committed under `infra/` and `docs/version-matrix.md`. Both sides must use identical
  warehouse path and S3 credentials.
- **StarRocks JDBC-driver requirement (do not omit):** for a MySQL-backed Iceberg JDBC catalog,
  StarRocks requires the MySQL driver JAR placed in the StarRocks **`fe/lib` and
  `be/lib/jni-packages`** directories. Pin **`mysql-connector-j`** to a known version and copy it
  into both paths via the StarRocks Dockerfile / init step. The catalog will not open without it.
- **Catalog smoke test (gates import):** `make smoke-starrocks-catalog` must pass —
  `SHOW DATABASES FROM <iceberg_catalog>` and `SELECT COUNT(*) FROM <iceberg_catalog>.<db>.orders_current`
  — before any internal PK-table import (Phase 3.1) proceeds.

## Development Commands (MOST IMPORTANT — how Codex verifies its own work; ALL run from repo root)

> Fragile `cd`-chained paths are forbidden. Use the Makefile. Each target is a real,
> repo-root-relative command.

```bash
make doctor            # verify Java/Maven/Python/Node versions match mise.toml; fail loudly if not
make local-verify      # laptop-safe: unit tests + lint/Maven verify + dashboard contract/build
make preflight-heavy   # fail-fast guard for heavy Docker work: free disk + Docker responsiveness
make up-core           # docker compose --profile core up -d   (MySQL, Flink, MinIO, JDBC catalog)
make up-olap           # docker compose --profile olap up -d    (adds StarRocks; M3+ only)
make ps                # compose ps; expect healthy
make down              # compose down -v

make build-flink       # mvn -q -f flink-jobs/pom.xml clean package  -> flink-jobs/target/cdc-to-iceberg.jar
make submit-flink      # copy jar into jobmanager + flink run -d (correct absolute container paths)
make savepoint JOB=<id># trigger savepoint
make restore SP=<path> # flink run -s <path> -d

make gen ARGS="--events 100000 --seed 1"   # deterministic event stream into MySQL
make eo-verify ARGS="--failure all"          # induce failures, emit eo_reconciliation.json
make small-file-rewrite                       # generate small files, rewrite_data_files, emit json
make ckpt-metrics                             # induce backpressure, scrape, emit checkpoint_metrics.json
make import-starrocks ARGS="--mode incremental"  # Iceberg -> StarRocks internal PK tables (requires smoke-starrocks-catalog first)
make smoke-starrocks-catalog                   # SHOW DATABASES FROM <iceberg_cat> + SELECT COUNT(*) FROM <iceberg_cat>.<db>.orders_current
make compaction-bench ARGS="--sweep default" # StarRocks import-compaction sweep
make dq ARGS="--inject dirty"                # data-quality gate demo
make backfill ARGS="--reconcile"             # backfill + stream/batch merge + drift check

make test                                     # pytest -q (all harness suites)
make test-cdc                                 # pytest harness/tests/cdc_correctness -v
make lint                                      # ruff check + black --check + mypy harness; mvn -q -f flink-jobs/pom.xml verify
make sql-mysql Q="SELECT COUNT(*) FROM orders"      # wrapper -> python -m harness.sql mysql --query
make sql-iceberg Q="SELECT count(*) FROM db.orders_current"  # DATA read via Flink SQL batch (applies v2 equality deletes correctly) — NOT pyiceberg
make sql-iceberg-meta ARGS="--table db.orders_current"       # METADATA only via pyiceberg (file/manifest/snapshot counts)
make sql-starrocks Q="SELECT count(*) FROM internal.orders_current"

make dashboard-build   # scripts/sync-results-to-dashboard.sh && (cd dashboard && npm ci && npm run build)
make dashboard-preview # serve dashboard/dist locally
```

Success for any Phase = its stated `make` target(s) exit 0 and produce the named artifact in
`showcase/results/` (with provenance) when applicable.

## Phase Completion Workflow

- After a Phase passes its required verification target(s), create a Phase-scoped git commit
  that includes the implementation, docs, and generated evidence artifacts for that Phase.
  The commit message must name the Phase and summarize the verified outcome.
- Push the Phase commit to the configured GitHub remote for the active repository and branch,
  unless the user explicitly asks to keep it local. If the working tree contains unrelated or
  unfinished changes, ask before including them in the Phase commit.
- The final Phase handoff must explicitly state the verification result, produced artifacts,
  commit hash, pushed branch, and whether anything remains uncommitted.
- End every Phase handoff with Codex's readiness judgment for the next Phase: state either
  "Ready to proceed to the next Phase" or "Not ready to proceed to the next Phase", and list
  the concrete blockers when it is not ready.

## Resource Profiles

Default operating split:
- **local lite** — use on constrained laptops. Run `make local-verify`, inspect committed
  evidence, and build the static dashboard. Do not start Docker or claim on-demand heavy
  reproduction from this path.
- **workstation reproduction** — use for `make up-core`, `make eo-verify ARGS="--failure all"`,
  `make test-cdc`, `make small-file-rewrite`, and `make ckpt-metrics`. These targets run
  `make preflight-heavy` first and should have at least 40 GiB free disk on the host.
- **remote connection inventory** — the current project-local SSH endpoint, provisioning
  state, and shared-host launch gates live in the gitignored `.remote/connection.md`; use
  `.remote/ssh_config` rather than relying on a possibly stale global SSH alias. If the local
  file is absent, re-verify the AutoDL console instead of guessing. Recording an endpoint does
  not itself authorize a remote phase or relax the guardrails below.

Set `RESOURCE_PROFILE=small|default` (env, read by Makefile + harness):
- **small** — reduced row counts, longer checkpoint intervals, StarRocks FE/BE memory capped;
  intended for ~8–12 GB Docker memory (M1) / 12–16 GB (M3 with `olap`). Benchmarks reproduce
  the *shape* and the failure *threshold*, not absolute throughput.
- **default** — fuller volumes; assumes ≥16 GB Docker memory.
Minimum tested machine and expected per-phase runtimes are recorded in `docs/version-matrix.md`.

## Coding Standards

- **Determinism in the harness is non-negotiable.** Every event has a monotonic unique
  `event_id` and a stable business key. Reconciliation is **final source-snapshot vs Iceberg
  snapshot** (covers updates/deletes); event-id set comparison is an **audit supplement**, not
  the proof.
- **Reconciliation reader correctness:** the Iceberg final-state read for `orders_current` MUST
  use a reader that applies v2 **equality deletes** (Flink SQL batch / tested Java CLI / local
  Spark). PyIceberg is **forbidden** for this read (it cannot apply equality deletes and would
  yield a false "diff = 0"); PyIceberg is allowed only for metadata/file-count/manifest metrics.
- **Two Iceberg tables:** `orders_current` (v2 upsert, PK uniqueness applies) and
  `orders_changelog` (append/audit, multiple rows per key expected). Uniqueness checks apply
  ONLY to `orders_current`.
- Java: explicit **operator UID on every stateful operator** (savepoint restore depends on it);
  no swallowed exceptions in sink/commit paths.
- Python: type hints, `ruff`+`black` clean, `mypy` clean.
- **Provenance:** every `showcase/results/*.json` MUST include `run_id`, `git_sha`,
  `started_at`, `finished_at`, `stack_versions`, `command`, and a `logs` pointer into
  `showcase/logs/`. The dashboard surfaces provenance; resume claims read the same JSON.
- Commit messages name the Phase.

## Guardrails

- **Feasibility (hard):** single-node Docker Compose, **no GPU**, no multi-node, no PB-scale,
  no paid cloud. If a task seems to need more, stop and ask.
- **Single OLAP engine:** StarRocks only. **No Doris** as implemented code/infra (Doris may
  appear only as cited background in docs).
- **EO claim boundary is Iceberg-first.** Exactly-once is claimed/tested ONLY for
  `MySQL CDC → Flink → Iceberg`. The Iceberg→StarRocks import is a serving-load step,
  **explicitly outside** the EO claim — never configure or describe it as exactly-once.
- **"Exactly-Once" word embargo:** the phrase must not appear in `README.md` or any
  resume-facing text until `make eo-verify ARGS="--failure all"` passes with final-snapshot
  reconciliation showing no loss and no incorrect rows across all failure classes. Resume lines
  live in `docs/resume-claims-after-verification.md`, each gated on its Phase.
- **No DolphinScheduler / no lineage / no compaction claims** in README/resume until their
  Phase passes.
- **Do not touch:** committed secrets, `infra/*/data` volumes during a run, pinned versions
  (without recording a reason in `docs/version-matrix.md`).
- **Scope discipline:** stay within the current Phase's requirements; defer out-of-scope items
  to their Phase. **Ask first if ambiguous.**

## Display & Deployment Requirements

- **Demo tier (decided, honest):** the full pipeline is **NOT** a public live demo — heavy
  multi-component infra. Show it via a **screen recording of the real pipeline running locally**
  (induced failure → recovery) + architecture narrative. The **deployable interactive slice is
  the read-only evidence dashboard** (`dashboard/`), rendering **real exported results** from
  `dashboard/public/results/*.json`. Honestly labeled as an explorer of recorded real runs —
  never dressed up as live, never fed fake/canned data. Provenance fields make every chart
  auditable.
- **Visitor README** opens with failure evidence, then links dashboard, recording, diagram, repo.
- **Architecture diagram** committed under `showcase/media/`.
- **Showcase-capture convention (DURING execution):** after each Phase passes verification,
  capture portfolio-worthy material into `showcase/` (you have Computer Use — screenshots,
  terminal recordings, exported charts, benchmark JSON, diagrams; choose what fits, no fixed
  list) and **append one or two lines to `showcase/NOTES.md`** stating what was produced and why
  it is worth showing. Material accumulates automatically, not reconstructed afterward.
- **RUNBOOK is incremental:** the skeleton is created in Phase 1.1; **append the real incident +
  recovery immediately after each failure-related Phase**, not retroactively at the end.

## Deployable-slice interface contract (decoupled from any portfolio host)

- The dashboard is a **static build** (`dashboard/dist`), **no backend**, **no live connection**
  to the heavy stack. It reads JSON that `scripts/sync-results-to-dashboard.sh` copied into
  `dashboard/public/results/` before build (Vite only serves `public/` reliably). Optionally
  honor a `BASE_RESULTS_URL` for a deployed results location; document local vs deployed modes.
- The **results-JSON schema** (in `showcase/results/README.md`) is the integration seam: a
  portfolio site later embeds via iframe or links the deployed URL (Vercel/Netlify/Pages).
  `infra/docker-compose.yml` remains the full local-repro path.
