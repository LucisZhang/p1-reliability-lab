# Phase B Remote Execution

Docker stays on the remote Linux workstation. The Mac keeps the authoritative Git checkout,
performs light verification, and commits artifacts only after they are pulled back.

## One-time shell input

Set the remote working-copy location for each invocation; the value contains only an SSH host
alias and a path, never a password, token, agent socket, or Git credential:

```bash
export P1_REMOTE_ROOT='exactly-once-workstation:/root/autodl-tmp/exactly-once-drills'
```

When `.remote/ssh_config` exists, the wrappers pass it to both SSH and rsync, so a stale global
alias cannot select the wrong endpoint. `P1_SSH_CONFIG` can name a different local config file.
The public `.env.example` values are used inside the isolated Compose lab. `sync-up` explicitly
excludes `.git`, `.env`, caches, build outputs, prior result JSON, and prior logs.

## Phase B1 sequence

Start from a fresh remote Compose project/volume set. The parity verifier intentionally refuses
a non-empty order topic so old offsets cannot be mistaken for the fixed B1 workload.

```bash
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--events 1000 --seed 17"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

## Phase B2 sequence

Phase B2 also requires a fresh topic because historical B1 records used the prior JSON wire
format. The `--fresh` mode runs `make down` only on the remote lab before the guarded broker
bring-up; the phase selector keeps the B2 tmux session, result expectation, and logs separate
from B1.

```bash
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase contracts --fresh"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase contracts --baseline-events 2 --seed 211"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

Both remote targets launch in a target-specific tmux session. If SSH drops, rerun the same
`remote-broker-*` command: the launcher detects the active session and resumes log streaming.
Before a new session starts, `shared-host-guard.sh` records `/proc/loadavg`; a one-minute load
above half the logical CPU count refuses the run. When `nvidia-smi` exists, the same guard
records GPU state and refuses active compute. Its absence on the dedicated CPU-only VM is
recorded and allowed.

`sync-down` is append-only: it stages known B1/B2 results and logs in a temporary local directory,
compares any existing path byte-for-byte, and refuses a differing artifact instead of overwriting
it. New `broker_parity.json` or `schema_contract_drill.json` files and their phase logs are then
copied into the authoritative Mac checkout.

## Phase B3 failure runs

Each B3 drill starts from fresh broker/core volumes. Repeat the guarded bring-up before each
failure command; the `--fresh` launcher is intentionally not cached. The five accepted failure
names are `broker-restart`, `duplicate-redelivery`, `mis-keying`, `poison-dlq`, and
`offset-replay`.

```bash
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase failures --fresh"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--failure broker-restart"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

Run the same fresh bring-up/verify pair for each remaining name. The remote launcher uses a
separate tmux session/status key and append-only result path per drill, so reconnecting to one
long run cannot accidentally reuse a different drill's completion. `sync-down` accepts only the
five known B3 JSON filenames and `phase-b3-*.log` transcripts in addition to the B1/B2 artifacts.

## Phase B4 SLO run

Phase B4 is one disconnect-safe remote benchmark. The guarded bring-up must be fresh, and the
certification command must retain the locked 100,000-event workload and seed in its exact command
provenance.

```bash
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase slo --fresh"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase slo --events 100000 --seed 401 --batch-size 1000 \
  --fault-after-events 25000 --outage-seconds 5"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

The launcher records the GPU/process and load-average gate before both bring-up and measurement.
The verifier emits only the new append-only `broker_slo.json` and its `phase-b4-*.log`; it does
not rewrite the B3 artifacts. A failed exploratory run may leave only a remote log. Certification
requires a fresh run and a newly created B4 result. On a freshly synchronized VM, the B4 runner
creates a repo-local Python 3.11 environment and installs the exact pins from
`harness/requirements.txt` when PyIceberg 0.9.1 is absent; it does not modify the host Python
installation.
