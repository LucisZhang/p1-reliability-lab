#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "shared-host guard requires remote Linux" >&2
  exit 2
fi
if command -v nvidia-smi >/dev/null 2>&1; then
  echo "shared-host guard: nvidia-smi GPU snapshot"
  nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total \
    --format=csv,noheader,nounits

  gpu_processes="$(nvidia-smi --query-compute-apps=pid,process_name,used_memory \
    --format=csv,noheader,nounits 2>/dev/null || true)"
  if [[ -n "${gpu_processes//[[:space:]]/}" ]]; then
    echo "shared-host guard: GPU compute processes are active; refusing co-tenancy" >&2
    echo "${gpu_processes}" >&2
    exit 3
  fi
  echo "shared-host guard: GPU compute process list is empty"
else
  echo "shared-host guard: nvidia-smi unavailable; dedicated CPU-only VM, GPU check skipped"
fi

read -r load1 load5 load15 _ < /proc/loadavg
logical_cpus="$(getconf _NPROCESSORS_ONLN)"
echo "shared-host guard: load_average=${load1},${load5},${load15} logical_cpus=${logical_cpus}"

if ! awk -v load_value="${load1}" -v cpu_count="${logical_cpus}" \
  'BEGIN { exit !(load_value <= cpu_count / 2.0) }'; then
  echo "shared-host guard: load1 exceeds half of logical CPU count; refusing benchmark contamination" >&2
  exit 3
fi
echo "shared-host guard: PASS"
