#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: launch-broker-target.sh <repo-root> broker-up|broker-verify <args> <git-sha> <resource-profile>" >&2
  exit 2
fi

repo_root="$1"
target="$2"
args="$3"
git_sha="$4"
resource_profile="$5"
if [[ "${target}" != "broker-up" && "${target}" != "broker-verify" ]]; then
  echo "unsupported target: ${target}" >&2
  exit 2
fi
if [[ "${repo_root}" != /* || "${repo_root}" == "/" || "${repo_root##*/}" != "exactly-once-drills" ]]; then
  echo "refusing unsafe repo root: ${repo_root}" >&2
  exit 2
fi
if [[ ! "${git_sha}" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "invalid provenance SHA: ${git_sha}" >&2
  exit 2
fi
if [[ "${resource_profile}" != "small" && "${resource_profile}" != "default" ]]; then
  echo "RESOURCE_PROFILE must be small or default" >&2
  exit 2
fi
if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for disconnect-safe remote runs" >&2
  exit 2
fi

cd "${repo_root}"
mkdir -p .remote-runs showcase/logs
active_file=".remote-runs/${target}.active"
resume=0
if [[ -f "${active_file}" ]]; then
  run_id="$(<"${active_file}")"
  status_file=".remote-runs/${run_id}.status"
  session="p1-${run_id}"
  if [[ -f "${status_file}" ]] && [[ "$(<"${status_file}")" == "RUNNING" ]] \
      && tmux has-session -t "${session}" 2>/dev/null; then
    resume=1
  fi
fi

if [[ "${resume}" == "0" ]]; then
  run_id="${target}-$(date -u +%Y%m%dT%H%M%SZ)"
  status_file=".remote-runs/${run_id}.status"
  session="p1-${run_id}"
  log_file="showcase/logs/phase-b1-${run_id}.log"
  printf '%s\n' "${run_id}" > "${active_file}"
  printf '%s\n' "RUNNING" > "${status_file}"

  set +e
  ./scripts/remote/shared-host-guard.sh 2>&1 | tee "${log_file}"
  guard_rc=${PIPESTATUS[0]}
  set -e
  if [[ "${guard_rc}" -ne 0 ]]; then
    printf '%s\n' "${guard_rc}" > "${status_file}"
    echo "remote launch refused by shared-host guard (exit ${guard_rc})" >&2
    exit "${guard_rc}"
  fi

  line=$(($(wc -l < "${log_file}") + 1))

  if ! tmux new-session -d -s "${session}" \
      env \
      "P1_PROVENANCE_GIT_SHA=${git_sha}" \
      "RESOURCE_PROFILE=${resource_profile}" \
      ./scripts/remote/run-broker-target.sh \
      "${target}" "${args}" "${status_file}" "${log_file}"; then
    printf '%s\n' "2" > "${status_file}"
    echo "failed to create tmux session ${session}" >&2
    exit 2
  fi
  echo "remote launch: run_id=${run_id} session=${session} log=${log_file}"
else
  log_file="showcase/logs/phase-b1-${run_id}.log"
  line=1
  echo "remote resume: run_id=${run_id} session=${session} log=${log_file}"
fi

while [[ "$(<"${status_file}")" == "RUNNING" ]]; do
  if [[ -f "${log_file}" ]]; then
    total="$(wc -l < "${log_file}")"
    if (( total >= line )); then
      sed -n "${line},${total}p" "${log_file}"
      line=$((total + 1))
    fi
  fi
  sleep 2
done

if [[ -f "${log_file}" ]]; then
  total="$(wc -l < "${log_file}")"
  if (( total >= line )); then
    sed -n "${line},${total}p" "${log_file}"
  fi
fi
rc="$(<"${status_file}")"
if [[ ! "${rc}" =~ ^[0-9]+$ ]]; then
  echo "remote run has invalid status: ${rc}" >&2
  exit 2
fi
exit "${rc}"
