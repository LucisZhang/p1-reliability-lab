# Version Matrix

Selected on 2026-05-26 for a single-node local reliability lab. Phase B1 broker additions were
selected on 2026-08-19 for the remote Linux workstation reproduction path.

| Component | Pinned family | Current use | Rationale / compatibility note |
| --- | --- | --- | --- |
| Java | 11 | Host toolchain and Flink CDC job runtime | Flink CDC 3.x requires JDK 11+; Java 11 keeps compatibility broad for Flink 1.20. |
| Maven | 3.9 | Java build for `flink-jobs/target/cdc-to-iceberg.jar` | Current Maven 3.x line used for reproducible Flink job packaging. |
| Python | 3.11 | Harness, generator, SQL wrappers, CDC smoke | Stable runtime for typed harness code and pyiceberg metadata tooling. |
| Node | 20 | Future static dashboard | LTS line for Vite-based dashboard builds. |
| MySQL | 8.0 | Core source DB and Iceberg JDBC catalog DB | CDC source uses ROW binlog, full row image, and GTID. |
| Apache Flink | 1.20.x | Core JobManager/TaskManager, CDC job, and batch SQL reader | Required by the selected Flink CDC 3.x line; the Iceberg data reader must be Flink SQL batch or an equivalent equality-delete-aware engine. |
| Flink CDC | 3.6.0 | MySQL CDC source in the Phase 1.2 Flink job | Latest selected CDC line; CDC 3.3+ dropped older Flink 1.17/1.18 support. The Maven artifact is the Flink-1.20 build. |
| Flink Kafka connector | 3.4.0-1.20 | Path B Kafka source | Last connector line published for Flink 1.20; it keeps the existing Flink runtime unchanged. |
| Apache Iceberg | 1.10.x | JDBC catalog, v2 `orders_current` upsert table, and v2 `orders_changelog` table | v2 tables are required for upsert/equality-delete behavior. |
| MinIO | RELEASE.2025-04-22T22-12-26Z | Core object store | Local S3-compatible warehouse with path-style access. |
| Apache Kafka | 3.9.2 | `broker` profile, one combined broker/controller in KRaft mode | Final 3.x line with the 2026 security/bug-fix patch; the official `apache/kafka:3.9.2` image is used. Single-node combined mode is intentionally lab-only. |
| Debezium Connect | 3.2.4.Final | One-worker standalone CDC producer for Path B | Corrected on 2026-08-20 after the remote pull proved the earlier `quay.io/debezium/connect:3.2.7.Final` pin did not exist; the official Quay catalog and manifest expose `3.2.4.Final` for amd64. One Connect worker is operationally lighter here than adding Debezium Server plus separate offset/config persistence. Evidence: `showcase/logs/phase-b1-broker-up-20260820T093844Z.log`. |
| Confluent Schema Registry | 7.9.8 | `broker` profile registry, global compatibility `BACKWARD` | Confluent Platform 7.9 is the Kafka 3.9-compatible line. B1 deploys and health-checks the registry; Registry-backed Avro schemas and evolution drills remain Phase B2. |
| StarRocks | 3.3.x | M3+ only | Kept out of `core`; later used for internal Primary Key table serving and compaction benchmark. |
| pyiceberg | 0.9.x | Metadata-only wrapper | It is allowed for table metadata, file, manifest, and snapshot inspection only. It must not materialize current table data for reconciliation. |

## Known Incompatibilities

- PyIceberg is not a correctness reader for v2 upsert tables that contain equality deletes. Phase 1.1 fixes the split: `make sql-iceberg` identifies the Flink SQL batch data path, while `make sql-iceberg-meta` identifies the pyiceberg metadata-only path.
- StarRocks is excluded from `core`. Catalog smoke tests and internal table imports start in M3.
- No multi-node, GPU, or managed cloud assumptions are part of this stack.
- The full Docker reproduction is not laptop-default. Heavy targets are guarded by
  `make preflight-heavy` and should run on a workstation with at least 40 GiB free disk; use
  `make local-verify` for no-Docker laptop review.
- Phase B1 uses Kafka Connect JSON with its schema envelope for parity only. This is an explicit
  phase boundary, not the final contract format: Avro producer/consumer integration and schema
  evolution acceptance belong to Phase B2.
- `make preflight-broker` additionally requires at least 16 GiB total and 8 GiB currently
  available RAM before starting Kafka, Schema Registry, Debezium Connect, and the core stack.
  A failure activates the locked Redpanda+ADR decision gate; lowering the guard is not allowed.
