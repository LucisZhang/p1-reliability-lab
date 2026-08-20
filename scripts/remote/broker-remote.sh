#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh sync-up
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh sync-down
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-up ""
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-verify "--events 1000 --seed 17"
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

: "${P1_REMOTE_ROOT:?Set P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills}"
if [[ "${P1_REMOTE_ROOT}" != *:/* ]]; then
  echo "P1_REMOTE_ROOT must use user@host:/absolute/path form" >&2
  exit 2
fi

remote_host="${P1_REMOTE_ROOT%%:*}"
remote_path="${P1_REMOTE_ROOT#*:}"
remote_path="${remote_path%/}"
if [[ -z "${remote_host}" || "${remote_path}" != /* || "${remote_path}" == "/" ]]; then
  echo "refusing unsafe P1_REMOTE_ROOT=${P1_REMOTE_ROOT}" >&2
  exit 2
fi
if [[ "${remote_path##*/}" != "exactly-once-drills" ]]; then
  echo "remote path must end in /exactly-once-drills: ${remote_path}" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
local_root="$(cd "${script_dir}/../.." && pwd)/"
operation="$1"
ssh_config="${P1_SSH_CONFIG:-${local_root}.remote/ssh_config}"
ssh_args=()
rsync_ssh="ssh"
if [[ -f "${ssh_config}" ]]; then
  ssh_args=(-F "${ssh_config}")
  printf -v rsync_ssh 'ssh -F %q' "${ssh_config}"
fi

sync_up() {
  ssh "${ssh_args[@]}" "${remote_host}" mkdir -p "${remote_path}"
  rsync --archive --compress --delete-delay --prune-empty-dirs -e "${rsync_ssh}" \
    --exclude=/.git/ \
    --exclude=/.env \
    --exclude=/.venv/ \
    --exclude=/.m2/ \
    --exclude=/.DS_Store \
    --exclude='**/.DS_Store' \
    --exclude=/.pytest_cache/ \
    --exclude=/.mypy_cache/ \
    --exclude=/.ruff_cache/ \
    --exclude=/dashboard/node_modules/ \
    --exclude=/dashboard/dist/ \
    --exclude=/flink-jobs/target/ \
    --exclude='/infra/*/data/' \
    --exclude='/showcase/results/*.json' \
    --exclude='/showcase/logs/*.log' \
    --exclude=/.remote-runs/ \
    "${local_root}" "${remote_host}:${remote_path}/"
  echo "sync-up complete: ${remote_host}:${remote_path} (no .git or .env transferred)"
}

sync_down() {
  local_result="${local_root}showcase/results/broker_parity.json"
  if [[ -e "${local_result}" ]]; then
    echo "append-only result already exists locally: ${local_result}" >&2
    exit 2
  fi
  ssh "${ssh_args[@]}" "${remote_host}" test -f "${remote_path}/showcase/results/broker_parity.json"
  rsync --archive --compress --prune-empty-dirs -e "${rsync_ssh}" \
    --include=/showcase/ \
    --include=/showcase/results/ \
    --include=/showcase/results/broker_parity.json \
    --include=/showcase/logs/ \
    --include='/showcase/logs/phase-b1-*.log' \
    --exclude='*' \
    "${remote_host}:${remote_path}/" "${local_root}"
  echo "sync-down complete: added broker_parity.json and Phase B1 logs"
}

run_remote() {
  if [[ $# -ne 2 ]]; then
    usage
    exit 2
  fi
  target="$1"
  args="$2"
  if [[ "${target}" != "broker-up" && "${target}" != "broker-verify" ]]; then
    echo "unsupported remote target: ${target}" >&2
    exit 2
  fi
  git_sha="$(git -C "${local_root}" rev-parse --short=12 HEAD)"
  resource_profile="${RESOURCE_PROFILE:-small}"

  printf -v remote_command '%q ' \
    "${remote_path}/scripts/remote/launch-broker-target.sh" \
    "${remote_path}" \
    "${target}" \
    "${args}" \
    "${git_sha}" \
    "${resource_profile}"
  ssh "${ssh_args[@]}" "${remote_host}" "${remote_command}"
}

case "${operation}" in
  sync-up)
    [[ $# -eq 1 ]] || { usage; exit 2; }
    sync_up
    ;;
  sync-down)
    [[ $# -eq 1 ]] || { usage; exit 2; }
    sync_down
    ;;
  run)
    shift
    run_remote "$@"
    ;;
  *)
    usage
    exit 2
    ;;
esac
