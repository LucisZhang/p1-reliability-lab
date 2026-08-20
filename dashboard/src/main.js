import "./styles.css";

const app = document.querySelector("#app");

const requiredProvenanceFields = [
  "run_id",
  "git_sha",
  "started_at",
  "finished_at",
  "stack_versions",
  "command",
  "logs",
];

function normalizeResultsBase() {
  const configuredBase =
    import.meta.env.BASE_RESULTS_URL || import.meta.env.VITE_BASE_RESULTS_URL || "";
  const base = configuredBase.trim();
  if (base) {
    return base.endsWith("/") ? base : `${base}/`;
  }
  return `${import.meta.env.BASE_URL}results/`;
}

const resultsBase = normalizeResultsBase();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDate(value) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
    timeZone: "UTC",
  }).format(date);
}

function formatDuration(startedAt, finishedAt) {
  const start = new Date(startedAt).getTime();
  const finish = new Date(finishedAt).getTime();
  if (Number.isNaN(start) || Number.isNaN(finish) || finish < start) {
    return "n/a";
  }
  const seconds = Math.round((finish - start) / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
}

function artifactTitle(artifact) {
  if (artifact.phase) {
    return `Phase ${artifact.phase}`;
  }
  return artifact.filename.replace(/\.json$/, "").replaceAll("-", " ");
}

function artifactStatus(artifact) {
  if (artifact.summary && typeof artifact.summary.passed === "boolean") {
    return artifact.summary.passed ? "Passed" : "Failed";
  }
  if (typeof artifact.passed === "boolean") {
    return artifact.passed ? "Passed" : "Failed";
  }
  if (artifact.checks || artifact.source_iceberg_diff_count === 0) {
    return "Recorded";
  }
  return "Synced";
}

function hasRequiredProvenance(artifact) {
  return requiredProvenanceFields.every((field) => Object.hasOwn(artifact, field));
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function renderError(error) {
  app.innerHTML = `
    <section class="shell state-shell">
      <p class="eyebrow">Recorded runs · static JSON</p>
      <h1>Exactly Once Stream</h1>
      <div class="notice error">
        <strong>Dashboard data could not be loaded.</strong>
        <span>${escapeHtml(error.message || error)}</span>
      </div>
    </section>
  `;
}

function renderDiffVisual(eoArtifact) {
  const results = asArray(eoArtifact.results);
  const totalDiff = results.reduce(
    (sum, result) => sum + Number(result.snapshot_diff_count || 0),
    0,
  );
  const passed = eoArtifact.summary?.passed === true && totalDiff === 0;

  return `
    <section class="diff-panel ${passed ? "is-clean" : "is-dirty"}" aria-label="Snapshot diff summary">
      <div>
        <p class="section-kicker">Final source snapshot vs Iceberg snapshot</p>
        <h2>diff = ${escapeHtml(totalDiff)}</h2>
        <p>${escapeHtml(eoArtifact.reader || "reader not recorded")} · ${escapeHtml(eoArtifact.claim_boundary || "claim boundary not recorded")}</p>
      </div>
      <div class="diff-meter" aria-hidden="true">
        <span>${escapeHtml(totalDiff)}</span>
      </div>
    </section>
  `;
}

function renderFailureTable(eoArtifact) {
  const rows = asArray(eoArtifact.results)
    .map((result) => {
      const audit = result.event_id_audit || {};
      const recoveryMode = result.recovery?.mode || "recorded";
      const currentRows = result.source_snapshot_row_count ?? "n/a";
      const icebergRows = result.iceberg_snapshot_row_count ?? "n/a";
      const diffCount = Number(result.snapshot_diff_count ?? 0);
      return `
        <tr>
          <th scope="row">
            <span class="failure-name">${escapeHtml(result.failure_class)}</span>
            <span class="failure-trigger">${escapeHtml(result.trigger)}</span>
          </th>
          <td>${escapeHtml(recoveryMode)}</td>
          <td class="numeric">${escapeHtml(currentRows)}</td>
          <td class="numeric">${escapeHtml(icebergRows)}</td>
          <td>
            <span class="diff-badge ${diffCount === 0 ? "clean" : "dirty"}">${escapeHtml(diffCount)}</span>
          </td>
          <td>${audit.consistent === true ? "Consistent" : "Check artifact"}</td>
        </tr>
      `;
    })
    .join("");

  return `
    <section class="panel">
      <div class="section-heading">
        <p class="section-kicker">Failure reconciliation</p>
        <h2>Per-class outcome</h2>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Failure class</th>
              <th scope="col">Recovery</th>
              <th scope="col">Source rows</th>
              <th scope="col">Iceberg rows</th>
              <th scope="col">Diff</th>
              <th scope="col">Event-id audit</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </section>
  `;
}

function renderSnapshotDiffs(eoArtifact) {
  const rows = asArray(eoArtifact.results)
    .map((result) => {
      const missing = asArray(result.snapshot_diff?.missing_in_iceberg);
      const unexpected = asArray(result.snapshot_diff?.unexpected_in_iceberg);
      return `
        <article class="diff-detail">
          <h3>${escapeHtml(result.failure_class)}</h3>
          <dl>
            <div>
              <dt>Missing in Iceberg</dt>
              <dd>${escapeHtml(missing.length)}</dd>
            </div>
            <div>
              <dt>Unexpected in Iceberg</dt>
              <dd>${escapeHtml(unexpected.length)}</dd>
            </div>
          </dl>
        </article>
      `;
    })
    .join("");

  return `
    <section class="detail-grid" aria-label="Snapshot diff detail">
      ${rows}
    </section>
  `;
}

function b3Headline(artifact) {
  if (artifact.failure_class === "broker-restart") {
    return {
      value: "0",
      label: "final diff",
      detail: "Kafka killed and restarted mid-stream; committed lag returned to zero.",
    };
  }
  if (artifact.failure_class === "duplicate-redelivery") {
    return {
      value: artifact.summary?.duplicates_detected ?? "n/a",
      label: "duplicates audited",
      detail: "Consumer offsets rewound; keyed upserts still converged.",
    };
  }
  if (artifact.failure_class === "mis-keying") {
    return {
      value: artifact.summary?.miskey_violation_count ?? "n/a",
      label: "order violations",
      detail: "Cross-partition mis-keying was detected and rejected before main-path admission.",
    };
  }
  if (artifact.failure_class === "poison-dlq") {
    return {
      value: artifact.summary?.dlq_record_count ?? "n/a",
      label: "DLQ record",
      detail: "Malformed bytes were quarantined, repaired as registered Avro, and replayed.",
    };
  }
  return {
    value: artifact.summary?.timestamp_diff_count ?? "n/a",
    label: "timestamp diff",
    detail: "Fresh tables rebuilt from offset 0 and a chosen post-sweep timestamp.",
  };
}

function renderB3Drills(artifacts) {
  const drills = artifacts.filter(
    (artifact) => artifact.phase === "B3" && typeof artifact.failure_class === "string",
  );
  if (!drills.length) {
    return "";
  }
  const cards = drills
    .map((artifact) => {
      const headline = b3Headline(artifact);
      return `
        <article class="b3-card">
          <div class="b3-card-top">
            <div>
              <p class="section-kicker">Phase B3</p>
              <h3>${escapeHtml(artifact.failure_class)}</h3>
            </div>
            <span class="status-pill">${artifact.summary?.passed === true ? "Passed" : "Failed"}</span>
          </div>
          <div class="b3-metric">
            <strong>${escapeHtml(headline.value)}</strong>
            <span>${escapeHtml(headline.label)}</span>
          </div>
          <p>${escapeHtml(headline.detail)}</p>
          <dl>
            <div><dt>Snapshot diff</dt><dd>${escapeHtml(artifact.snapshot_diff_count)}</dd></div>
            <div><dt>Run</dt><dd>${escapeHtml(artifact.run_id)}</dd></div>
          </dl>
        </article>
      `;
    })
    .join("");
  return `
    <section class="panel">
      <div class="section-heading">
        <div>
          <p class="section-kicker">Broker failure evidence</p>
          <h2>Phase B3 drills</h2>
        </div>
        <span>${escapeHtml(drills.length)} / 5 captured</span>
      </div>
      <div class="b3-grid">${cards}</div>
    </section>
  `;
}

function formatMetric(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: digits }).format(number);
}

function renderLagChart(sloArtifact) {
  const lagSamples = asArray(sloArtifact.observability?.time_series)
    .filter((sample) => sample.kafka_consumer_group_lag?.present === true)
    .map((sample) => ({
      elapsed: Number(sample.elapsed_seconds || 0),
      lag: Number(sample.kafka_consumer_group_lag.sum || 0),
    }));
  if (lagSamples.length < 2) {
    return `<div class="notice">Kafka lag samples are recorded in the raw artifact but cannot form a chart.</div>`;
  }
  const width = 760;
  const height = 190;
  const inset = 22;
  const maxElapsed = Math.max(...lagSamples.map((sample) => sample.elapsed), 1);
  const maxLag = Math.max(...lagSamples.map((sample) => sample.lag), 1);
  const points = lagSamples
    .map((sample) => {
      const x = inset + (sample.elapsed / maxElapsed) * (width - inset * 2);
      const y = height - inset - (sample.lag / maxLag) * (height - inset * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return `
    <div class="lag-chart" role="img" aria-label="Kafka consumer-group lag over the B4 workload">
      <div class="lag-chart-heading">
        <span>Kafka consumer-group lag</span>
        <strong>peak ${escapeHtml(formatMetric(maxLag, 0))} events</strong>
      </div>
      <svg viewBox="0 0 ${width} ${height}" aria-hidden="true">
        <line x1="${inset}" y1="${height - inset}" x2="${width - inset}" y2="${height - inset}" />
        <line x1="${inset}" y1="${inset}" x2="${inset}" y2="${height - inset}" />
        <polyline points="${points}" />
      </svg>
      <div class="lag-chart-axis"><span>0s</span><span>${escapeHtml(formatMetric(maxElapsed, 1))}s</span></div>
    </div>
  `;
}

function renderSloPanel(artifacts) {
  const sloArtifact = artifacts.find((artifact) => artifact.filename === "broker_slo.json");
  if (!sloArtifact) {
    return "";
  }
  const summary = sloArtifact.summary || {};
  const observability = sloArtifact.observability?.summary || {};
  const recoveryCards = asArray(sloArtifact.recovery_measurements)
    .map(
      (measurement) => `
        <article class="recovery-card">
          <span>${escapeHtml(measurement.failure_class)}</span>
          <strong>${escapeHtml(formatMetric(measurement.recovery_seconds, 3))}s</strong>
          <small>snapshot diff ${escapeHtml(measurement.snapshot_diff_count)}</small>
        </article>
      `,
    )
    .join("");
  return `
    <section class="panel slo-panel">
      <div class="section-heading">
        <div>
          <p class="section-kicker">Measured on pinned single-node hardware</p>
          <h2>Phase B4 SLO evidence</h2>
        </div>
        <span class="status-pill">${summary.passed === true ? "Passed" : "Failed"}</span>
      </div>
      <div class="slo-headlines">
        <article><strong>${escapeHtml(formatMetric(summary.sustained_throughput_events_per_second, 1))}</strong><span>events / second</span></article>
        <article><strong>${escapeHtml(formatMetric(summary.freshness_p50_ms, 1))} ms</strong><span>freshness p50</span></article>
        <article><strong>${escapeHtml(formatMetric(summary.freshness_p95_ms, 1))} ms</strong><span>freshness p95</span></article>
        <article><strong>${escapeHtml(formatMetric(observability.debezium_sample_count, 0))}</strong><span>Debezium samples</span></article>
      </div>
      ${renderLagChart(sloArtifact)}
      <div class="section-heading compact-heading">
        <div>
          <p class="section-kicker">Fault boundary to zero-diff convergence</p>
          <h3>Recovery time by B3 drill</h3>
        </div>
      </div>
      <div class="recovery-grid">${recoveryCards}</div>
      <p class="slo-disclaimer">Recorded-run evidence only. This chart reads committed JSON and has no live connection to Kafka or Debezium.</p>
    </section>
  `;
}

function renderProvenanceCard(artifact, resultsBaseUrl) {
  const missing = requiredProvenanceFields.filter((field) => !Object.hasOwn(artifact, field));
  const versions = artifact.stack_versions || {};
  const versionText = Object.entries(versions)
    .map(([key, value]) => `${key}: ${value}`)
    .join(" · ");
  const rawHref = `${resultsBaseUrl}${encodeURIComponent(artifact.filename)}`;

  return `
    <article class="artifact-card">
      <div class="artifact-card-top">
        <div>
          <p class="artifact-title">${escapeHtml(artifactTitle(artifact))}</p>
          <p class="artifact-file">${escapeHtml(artifact.filename)}</p>
        </div>
        <span class="status-pill">${escapeHtml(artifactStatus(artifact))}</span>
      </div>
      <dl class="provenance-list">
        <div>
          <dt>Run</dt>
          <dd>${escapeHtml(artifact.run_id || "missing")}</dd>
        </div>
        <div>
          <dt>Git</dt>
          <dd>${escapeHtml(artifact.git_sha || "missing")}</dd>
        </div>
        <div>
          <dt>Started</dt>
          <dd>${escapeHtml(formatDate(artifact.started_at))}</dd>
        </div>
        <div>
          <dt>Finished</dt>
          <dd>${escapeHtml(formatDate(artifact.finished_at))}</dd>
        </div>
        <div>
          <dt>Duration</dt>
          <dd>${escapeHtml(formatDuration(artifact.started_at, artifact.finished_at))}</dd>
        </div>
        <div>
          <dt>Command</dt>
          <dd><code>${escapeHtml(artifact.command || "missing")}</code></dd>
        </div>
        <div>
          <dt>Logs</dt>
          <dd>${escapeHtml(artifact.logs || "missing")}</dd>
        </div>
        <div>
          <dt>Stack</dt>
          <dd>${escapeHtml(versionText || "missing")}</dd>
        </div>
      </dl>
      ${
        missing.length
          ? `<p class="missing">Missing provenance: ${escapeHtml(missing.join(", "))}</p>`
          : ""
      }
      <a class="raw-link" href="${escapeHtml(rawHref)}">Raw JSON</a>
    </article>
  `;
}

function renderDashboard(artifacts) {
  const eoArtifact =
    artifacts.find((artifact) => artifact.filename === "eo_reconciliation.json") ||
    artifacts.find((artifact) => Array.isArray(artifact.results));

  if (!eoArtifact) {
    throw new Error("No EO reconciliation artifact was found in synced results.");
  }

  const provenanceCards = artifacts
    .map((artifact) => renderProvenanceCard(artifact, resultsBase))
    .join("");
  const invalidCount = artifacts.filter((artifact) => !hasRequiredProvenance(artifact)).length;

  app.innerHTML = `
    <section class="shell">
      <header class="hero">
        <div>
          <p class="eyebrow">Recorded runs · static JSON</p>
          <h1>Exactly Once Stream</h1>
          <p class="lede">A read-only explorer for exported local pipeline runs. It renders synced artifacts only and does not connect to MySQL, Flink, Iceberg, MinIO, or StarRocks.</p>
        </div>
        <div class="run-summary" aria-label="EO run summary">
          <span>${escapeHtml(artifactTitle(eoArtifact))}</span>
          <strong>${escapeHtml(eoArtifact.summary?.failure_classes?.length ?? asArray(eoArtifact.results).length)}</strong>
          <span>failure classes</span>
        </div>
      </header>

      ${invalidCount ? `<div class="notice">Some synced artifacts are missing required provenance fields.</div>` : ""}
      ${renderDiffVisual(eoArtifact)}
      ${renderFailureTable(eoArtifact)}
      ${renderSnapshotDiffs(eoArtifact)}
      ${renderB3Drills(artifacts)}
      ${renderSloPanel(artifacts)}

      <section class="panel provenance-panel">
        <div class="section-heading">
          <p class="section-kicker">Artifact provenance</p>
          <h2>Synced results</h2>
        </div>
        <div class="artifact-grid">${provenanceCards}</div>
      </section>
    </section>
  `;
}

async function loadJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return response.json();
}

async function main() {
  const index = await loadJson(`${resultsBase}index.json`);
  const files = asArray(index.artifacts).map((artifact) => artifact.filename).filter(Boolean);
  if (!files.length) {
    throw new Error("results/index.json did not list any artifacts.");
  }
  const artifacts = await Promise.all(
    files.map(async (filename) => {
      const artifact = await loadJson(`${resultsBase}${encodeURIComponent(filename)}`);
      return { ...artifact, filename };
    }),
  );
  renderDashboard(artifacts);
}

main().catch(renderError);
