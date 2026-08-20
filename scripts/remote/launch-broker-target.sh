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
phase_tag="b1"
expected_result="showcase/results/broker_parity.json"
if [[ " ${args} " == *" --phase contracts "* ]]; then
  phase_tag="b2"
  expected_result="showcase/results/schema_contract_drill.json"
elif [[ " ${args} " == *" --phase failures "* ]]; then
  phase_tag="b3"
  expected_result=""
elif [[ " ${args} " == *" --failure broker-restart "* ]]; then
  phase_tag="b3-broker-restart"
  expected_result="showcase/results/broker_restart_drill.json"
elif [[ " ${args} " == *" --failure duplicate-redelivery "* ]]; then
  phase_tag="b3-duplicate-redelivery"
  expected_result="showcase/results/duplicate_redelivery_drill.json"
elif [[ " ${args} " == *" --failure mis-keying "* ]]; then
  phase_tag="b3-ordering-miskey"
  expected_result="showcase/results/ordering_miskey_drill.json"
elif [[ " ${args} " == *" --failure poison-dlq "* ]]; then
  phase_tag="b3-poison-dlq"
  expected_result="showcase/results/poison_dlq_drill.json"
elif [[ " ${args} " == *" --failure offset-replay "* ]]; then
  phase_tag="b3-offset-replay"
  expected_result="showcase/results/offset_replay_drill.json"
fi
active_file=".remote-runs/${target}-${phase_tag}.active"
resume=0
completed=0
force_launch=0
if [[ " ${args} " == *" --fresh "* ]]; then
  force_launch=1
fi
if [[ -f "${active_file}" ]]; then
  run_id="$(<"${active_file}")"
  status_file=".remote-runs/${run_id}.status"
  session="p1-${run_id}"
  log_file="showcase/logs/phase-${phase_tag}-${run_id}.log"
  if [[ -f "${status_file}" ]] && [[ "$(<"${status_file}")" == "RUNNING" ]] \
      && tmux has-session -t "${session}" 2>/dev/null; then
    resume=1
  elif [[ "${force_launch}" == "0" ]] \
      && [[ -f "${status_file}" ]] && [[ "$(<"${status_file}")" == "0" ]] \
      && [[ -f "${log_file}" ]] \
      && grep -Fq \
        "remote target provenance: git_sha=${git_sha} resource_profile=${resource_profile}" \
        "${log_file}"; then
    if [[ "${target}" != "broker-verify" ]] || [[ -n "${expected_result}" && -f "${expected_result}" ]]; then
      completed=1
    fi
  fi
fi

if [[ "${resume}" == "0" && "${completed}" == "0" ]]; then
  run_id="${target}-$(date -u +%Y%m%dT%H%M%SZ)"
  status_file=".remote-runs/${run_id}.status"
  session="p1-${run_id}"
  log_file="showcase/logs/phase-${phase_tag}-${run_id}.log"
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
elif [[ "${resume}" == "1" ]]; then
  line=1
  echo "remote resume: run_id=${run_id} session=${session} log=${log_file}"
else
  line=1
  echo "remote completed: run_id=${run_id} exit_code=0 log=${log_file}"
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
