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

Both remote targets launch in a target-specific tmux session. If SSH drops, rerun the same
`remote-broker-*` command: the launcher detects the active session and resumes log streaming.
Before a new session starts, `shared-host-guard.sh` records `/proc/loadavg`; a one-minute load
above half the logical CPU count refuses the run. When `nvidia-smi` exists, the same guard
records GPU state and refuses active compute. Its absence on the dedicated CPU-only VM is
recorded and allowed.

`sync-down` is append-only: it refuses to overwrite a local `broker_parity.json` and pulls only
that new artifact plus `showcase/logs/phase-b1-*.log`.
