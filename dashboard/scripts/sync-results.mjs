import { copyFile, mkdir, readdir, readFile, stat, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const dashboardDir = path.resolve(scriptDir, "..");
const rootDir = path.resolve(dashboardDir, "..");
const sourceDir = path.join(rootDir, "showcase", "results");
const targetDir = path.join(dashboardDir, "public", "results");

const requiredProvenanceFields = [
  "run_id",
  "git_sha",
  "started_at",
  "finished_at",
  "stack_versions",
  "command",
  "logs",
];

function validateProvenance(filename, artifact) {
  const missing = requiredProvenanceFields.filter((field) => artifact[field] === undefined);
  if (missing.length > 0) {
    throw new Error(`${filename} is missing required provenance fields: ${missing.join(", ")}`);
  }
  if (artifact.stack_versions === null || typeof artifact.stack_versions !== "object") {
    throw new Error(`${filename} must include stack_versions as an object`);
  }
}

function validateEoReconciliation(filename, artifact) {
  if (filename !== "eo_reconciliation.json") {
    return;
  }
  if (!Array.isArray(artifact.results) || artifact.results.length === 0) {
    throw new Error(`${filename} must include a non-empty results array`);
  }
  for (const [index, result] of artifact.results.entries()) {
    if (!result.failure_class) {
      throw new Error(`${filename} results[${index}] is missing failure_class`);
    }
    if (typeof result.snapshot_diff_count !== "number") {
      throw new Error(`${filename} results[${index}] must include numeric snapshot_diff_count`);
    }
    if (!result.snapshot_diff || typeof result.snapshot_diff !== "object") {
      throw new Error(`${filename} results[${index}] is missing snapshot_diff`);
    }
  }
}

function validateSmallFileRewrite(filename, artifact) {
  if (filename !== "iceberg_small_file_rewrite.json") {
    return;
  }
  for (const field of ["before", "after", "rewrite_data_files", "checks", "summary"]) {
    if (!artifact[field] || typeof artifact[field] !== "object") {
      throw new Error(`${filename} must include ${field} as an object`);
    }
  }
  const requiredChecks = [
    "data_file_count_decreased",
    "manifest_count_decreased",
    "median_file_size_increased",
    "planning_latency_decreased",
  ];
  for (const check of requiredChecks) {
    if (artifact.checks[check] !== true) {
      throw new Error(`${filename} check failed or missing: ${check}`);
    }
  }
}

function validateBrokerParity(filename, artifact) {
  if (filename !== "broker_parity.json") {
    return;
  }
  if (!artifact.environment || typeof artifact.environment !== "object") {
    throw new Error(`${filename} must include remote environment provenance`);
  }
  if (!artifact.parity || artifact.parity.row_level_diff_count !== 0) {
    throw new Error(`${filename} must prove Path A/Path B row_level_diff_count=0`);
  }
  if (!artifact.summary || artifact.summary.passed !== true) {
    throw new Error(`${filename} summary.passed must be true`);
  }
  const linkage = artifact.offset_checkpoint_snapshot_linkage;
  if (!linkage || typeof linkage !== "object") {
    throw new Error(`${filename} is missing offset/checkpoint/snapshot linkage`);
  }
  if (!Array.isArray(linkage.kafka_offsets) || linkage.kafka_offsets.length === 0) {
    throw new Error(`${filename} linkage must include Kafka partition offsets`);
  }
  if (linkage.kafka_offsets.some((item) => item.lag !== 0)) {
    throw new Error(`${filename} linkage contains non-zero Kafka lag`);
  }
  if (typeof linkage.flink_checkpoint?.id !== "number") {
    throw new Error(`${filename} linkage must include a numeric Flink checkpoint id`);
  }
  if (
    typeof linkage.iceberg_snapshot_ids?.orders_current !== "number" ||
    typeof linkage.iceberg_snapshot_ids?.orders_changelog !== "number"
  ) {
    throw new Error(`${filename} linkage must include both Iceberg snapshot ids`);
  }
}

function validateSchemaContractDrill(filename, artifact) {
  if (filename !== "schema_contract_drill.json") {
    return;
  }
  if (!artifact.environment || typeof artifact.environment !== "object") {
    throw new Error(`${filename} must include remote environment provenance`);
  }
  if (artifact.contracts?.format !== "AVRO") {
    throw new Error(`${filename} contracts.format must be AVRO`);
  }
  if (artifact.contracts?.global_compatibility !== "BACKWARD") {
    throw new Error(`${filename} must record global BACKWARD compatibility`);
  }
  for (const kind of ["key", "value"]) {
    const contract = artifact.contracts?.[kind];
    if (
      contract?.compatibility !== "BACKWARD" ||
      typeof contract?.id !== "number" ||
      typeof contract?.version !== "number" ||
      !contract?.schema ||
      typeof contract.schema !== "object"
    ) {
      throw new Error(`${filename} must capture a registered BACKWARD ${kind} Avro schema`);
    }
  }
  const attempt = artifact.incompatible_schema_attempt;
  if (
    attempt?.compatibility_check?.is_compatible !== false ||
    attempt?.registration?.http_status !== 409 ||
    attempt?.registration?.rejected !== true ||
    attempt?.latest_schema_unchanged !== true
  ) {
    throw new Error(`${filename} must prove incompatible registration was rejected unchanged`);
  }
  const continuity = artifact.flow_continuity?.after_rejection;
  if (
    continuity?.source_iceberg_diff_count !== 0 ||
    continuity?.post_event_visible !== true ||
    typeof continuity?.flink_checkpoint?.id !== "number" ||
    !Array.isArray(continuity?.kafka_offsets) ||
    continuity.kafka_offsets.some((item) => item.lag !== 0)
  ) {
    throw new Error(`${filename} must prove post-rejection flow and zero Kafka lag`);
  }
  if (
    !artifact.checks ||
    Object.values(artifact.checks).some((value) => value !== true) ||
    artifact.summary?.passed !== true ||
    artifact.summary?.old_schema_pipeline_continued !== true
  ) {
    throw new Error(`${filename} summary and every contract check must pass`);
  }
}

const b3FailureArtifacts = new Map([
  ["broker_restart_drill.json", "broker-restart"],
  ["duplicate_redelivery_drill.json", "duplicate-redelivery"],
  ["ordering_miskey_drill.json", "mis-keying"],
  ["poison_dlq_drill.json", "poison-dlq"],
  ["offset_replay_drill.json", "offset-replay"],
]);

function validateB3FailureDrill(filename, artifact) {
  const expectedFailure = b3FailureArtifacts.get(filename);
  if (!expectedFailure) {
    return;
  }
  if (artifact.phase !== "B3" || artifact.failure_class !== expectedFailure) {
    throw new Error(`${filename} must identify Phase B3 ${expectedFailure}`);
  }
  if (!artifact.environment || typeof artifact.environment !== "object") {
    throw new Error(`${filename} must include remote environment provenance`);
  }
  if (
    artifact.snapshot_diff_count !== 0 ||
    artifact.reconciliation?.snapshot_diff_count !== 0
  ) {
    throw new Error(`${filename} must prove final snapshot_diff_count=0`);
  }
  const linkage = artifact.offset_checkpoint_snapshot_linkage;
  if (
    !linkage ||
    !Array.isArray(linkage.kafka_offsets) ||
    linkage.kafka_offsets.length === 0 ||
    linkage.kafka_offsets.some((item) => item.lag !== 0) ||
    typeof linkage.flink_checkpoint?.id !== "number" ||
    typeof linkage.iceberg_snapshot_ids?.orders_current !== "number" ||
    typeof linkage.iceberg_snapshot_ids?.orders_changelog !== "number"
  ) {
    throw new Error(`${filename} must link zero-lag Kafka offsets, checkpoint, and snapshots`);
  }
  if (
    !artifact.checks ||
    Object.values(artifact.checks).some((value) => value !== true) ||
    artifact.summary?.passed !== true
  ) {
    throw new Error(`${filename} summary and every B3 check must pass`);
  }

  if (
    expectedFailure === "broker-restart" &&
    (artifact.fault?.container_killed?.Running !== false ||
      artifact.fault?.container_after?.Running !== true ||
      artifact.summary?.pipeline_resumed !== true)
  ) {
    throw new Error(`${filename} must prove an actual broker stop/restart and resumed flow`);
  }
  if (
    expectedFailure === "duplicate-redelivery" &&
    (!(artifact.duplicates_detected?.duplicate_occurrence_count > 0) ||
      artifact.duplicates_detected?.duplicate_occurrence_count !==
        artifact.duplicates_detected?.expected_duplicate_occurrence_count)
  ) {
    throw new Error(`${filename} must report the exact positive duplicate count`);
  }
  if (
    expectedFailure === "mis-keying" &&
    (artifact.miskey_probe?.audit?.non_monotonic_transition_count < 1 ||
      artifact.miskey_probe?.audit?.would_corrupt_final_state !== true ||
      artifact.miskey_probe?.audit?.disposition !== "rejected before main-pipeline admission")
  ) {
    throw new Error(`${filename} must expose and reject the cross-partition ordering limit`);
  }
  if (
    expectedFailure === "poison-dlq" &&
    (artifact.summary?.dlq_record_count !== 1 ||
      artifact.summary?.repair_replayed !== true ||
      artifact.summary?.main_pipeline_continued !== true)
  ) {
    throw new Error(`${filename} must prove one DLQ quarantine, repair, and continued flow`);
  }
  if (
    expectedFailure === "offset-replay" &&
    (artifact.summary?.offset_zero_diff_count !== 0 ||
      artifact.summary?.timestamp_diff_count !== 0 ||
      !Array.isArray(artifact.timestamp_replay?.resolved_partition_offsets))
  ) {
    throw new Error(`${filename} must prove offset-0 and timestamp rebuild parity`);
  }
}

async function readJson(filePath) {
  const raw = await readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fileExists(filePath) {
  try {
    await stat(filePath);
    return true;
  } catch {
    return false;
  }
}

async function clearSyncedJson() {
  await mkdir(targetDir, { recursive: true });
  const existing = await readdir(targetDir);
  await Promise.all(
    existing
      .filter((filename) => filename.endsWith(".json"))
      .map((filename) => unlink(path.join(targetDir, filename))),
  );
}

async function main() {
  await clearSyncedJson();

  const sourceFiles = (await readdir(sourceDir)).filter((filename) => filename.endsWith(".json"));
  sourceFiles.sort((left, right) => left.localeCompare(right));

  if (sourceFiles.length === 0) {
    throw new Error("No JSON result artifacts found in showcase/results");
  }

  const artifacts = [];

  for (const filename of sourceFiles) {
    const sourcePath = path.join(sourceDir, filename);
    const targetPath = path.join(targetDir, filename);
    const artifact = await readJson(sourcePath);

    validateProvenance(filename, artifact);
    validateEoReconciliation(filename, artifact);
    validateSmallFileRewrite(filename, artifact);
    validateBrokerParity(filename, artifact);
    validateSchemaContractDrill(filename, artifact);
    validateB3FailureDrill(filename, artifact);

    if (artifact.logs) {
      const logPath = path.join(rootDir, artifact.logs);
      if (!(await fileExists(logPath))) {
        throw new Error(`${filename} references missing log file: ${artifact.logs}`);
      }
    }

    await copyFile(sourcePath, targetPath);
    artifacts.push({
      filename,
      phase: artifact.phase ?? null,
      run_id: artifact.run_id,
      git_sha: artifact.git_sha,
      started_at: artifact.started_at,
      finished_at: artifact.finished_at,
      command: artifact.command,
      logs: artifact.logs,
    });
  }

  const index = {
    generated_at: new Date().toISOString(),
    source: "showcase/results/*.json",
    artifacts,
  };
  await writeFile(path.join(targetDir, "index.json"), `${JSON.stringify(index, null, 2)}\n`);

  console.log(`Synced ${artifacts.length} validated result artifact(s) to dashboard/public/results/`);
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
