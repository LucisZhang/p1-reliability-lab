#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh sync-up
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh sync-down
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-up ""
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-verify "--events 1000 --seed 17"
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-up "--phase contracts --fresh"
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-verify "--phase contracts --seed 211"
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-up "--phase failures --fresh"
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-verify "--failure broker-restart"
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-up "--phase slo --fresh"
  P1_REMOTE_ROOT=user@host:/absolute/exactly-once-drills scripts/remote/broker-remote.sh run broker-verify "--phase slo --events 100000 --seed 401"
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
    --exclude=/infra/debezium/target/ \
    --exclude='/infra/*/data/' \
    --exclude='/showcase/results/*.json' \
    --exclude='/showcase/logs/*.log' \
    --exclude=/.remote-runs/ \
    "${local_root}" "${remote_host}:${remote_path}/"
  echo "sync-up complete: ${remote_host}:${remote_path} (no .git or .env transferred)"
}

sync_down() {
  sync_tmp="$(mktemp -d)"
  trap 'rm -rf -- "${sync_tmp}"' EXIT
  rsync --archive --compress --prune-empty-dirs -e "${rsync_ssh}" \
    --include=/showcase/ \
    --include=/showcase/results/ \
    --include=/showcase/results/broker_parity.json \
    --include=/showcase/results/schema_contract_drill.json \
    --include=/showcase/results/broker_restart_drill.json \
    --include=/showcase/results/duplicate_redelivery_drill.json \
    --include=/showcase/results/ordering_miskey_drill.json \
    --include=/showcase/results/poison_dlq_drill.json \
    --include=/showcase/results/offset_replay_drill.json \
    --include=/showcase/results/broker_slo.json \
    --include=/showcase/logs/ \
    --include='/showcase/logs/phase-b1-*.log' \
    --include='/showcase/logs/phase-b2-*.log' \
    --include='/showcase/logs/phase-b3-*.log' \
    --include='/showcase/logs/phase-b4-*.log' \
    --exclude='*' \
    "${remote_host}:${remote_path}/" "${sync_tmp}/"

  shopt -s nullglob
  sources=(
    "${sync_tmp}/showcase/results/"*.json
    "${sync_tmp}/showcase/logs/"*.log
  )
  if [[ "${#sources[@]}" -eq 0 ]]; then
    echo "remote contains no known broker result or log artifacts" >&2
    exit 2
  fi

  added=0
  unchanged=0
  for source in "${sources[@]}"; do
    relative="${source#${sync_tmp}/}"
    destination="${local_root}${relative}"
    if [[ -e "${destination}" ]]; then
      if ! cmp -s "${source}" "${destination}"; then
        echo "append-only artifact differs and will not be overwritten: ${destination}" >&2
        exit 2
      fi
      unchanged=$((unchanged + 1))
      continue
    fi
    mkdir -p "$(dirname "${destination}")"
    cp "${source}" "${destination}"
    added=$((added + 1))
  done
  echo "sync-down complete: added=${added} unchanged=${unchanged} append-only artifacts"
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
