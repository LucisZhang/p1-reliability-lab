# Showcase Notes

Append one or two lines after each verified phase describing what was produced and why it is worth showing.

- Phase 1.1 produced a strict toolchain/core-stack smoke artifact plus deterministic generator proof (`showcase/results/phase-1.1-smoke.json`), establishing the reproducible base later failure phases depend on.
- Phase 1.2 produced a real CDC-to-Iceberg v2 smoke artifact (`showcase/results/phase-1.2-cdc-smoke.json`) with source-vs-Iceberg final-state parity, update/delete proof, changelog count, and equality-delete metadata.
- Phase 1.3 produced `showcase/results/eo_reconciliation.json` plus `showcase/logs/phase-1.3-eo-verify.log`, showing task-crash recovery and retained-checkpoint restore with zero final-snapshot diff and consistent event-id audits.
- Phase 1.4 produced the static evidence dashboard and `showcase/media/phase-1.4-dashboard.jpg`, showing the Phase 1.3 reconciliation JSON with visible provenance and no live stack dependency.
- Phase 2.1 refreshed `showcase/results/eo_reconciliation.json` and added `showcase/logs/phase-2.1-eo-verify.log`, covering all five EO failure classes including JobManager restart, explicit savepoint restore, and a checkpoint-complete sink-commit fault with zero final-snapshot diff.
- Phase 2.2 produced `showcase/results/iceberg_small_file_rewrite.json` plus `showcase/media/phase-2.2-small-file-rewrite.svg`, showing Iceberg `rewrite_data_files` reducing live data files and manifests while improving planning latency.
- Phase 2.3 produced `showcase/results/checkpoint_metrics.json` plus `showcase/media/phase-2.3-checkpoint-metrics.svg`, showing real Flink reporter checkpoint duration, alignment, pressure, and Iceberg lag under a deterministic input spike.
- Phase B1 produced `showcase/results/broker_parity.json` and `showcase/media/phase-b1-path-a-b.svg`, proving a fixed Path A/Path B workload converged row-for-row while preserving the offset↔checkpoint↔snapshot evidence chain.
