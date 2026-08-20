#!/usr/bin/env bash
set -uo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: run-broker-target.sh broker-up|broker-verify <args> <status-file> <log-file>" >&2
  exit 2
fi

target="$1"
args="$2"
status_file="$3"
log_file="$4"
mkdir -p "$(dirname "${status_file}")" "$(dirname "${log_file}")"
exec > >(tee -a "${log_file}") 2>&1

echo "remote target started: target=${target} utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "remote target provenance: git_sha=${P1_PROVENANCE_GIT_SHA} resource_profile=${RESOURCE_PROFILE}"

set +e
if [[ "${target}" == "broker-up" ]]; then
  env P1_RESULT_COMMAND="make broker-up" \
    make broker-up ENV_FILE=.env.example RESOURCE_PROFILE="${RESOURCE_PROFILE}"
  rc=$?
elif [[ "${target}" == "broker-verify" ]]; then
  exact_command="make broker-verify"
  if [[ -n "${args}" ]]; then
    exact_command="make broker-verify ARGS=\"${args}\""
  fi
  env P1_RESULT_COMMAND="${exact_command}" P1_RESULT_LOGS="${log_file}" \
    make broker-verify ENV_FILE=.env.example RESOURCE_PROFILE="${RESOURCE_PROFILE}" ARGS="${args}"
  rc=$?
else
  echo "unsupported remote target: ${target}" >&2
  rc=2
fi
set -e

echo "remote target finished: target=${target} exit_code=${rc} utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
status_tmp="${status_file}.tmp.$$"
printf '%s\n' "${rc}" > "${status_tmp}"
mv "${status_tmp}" "${status_file}"
exit "${rc}"
