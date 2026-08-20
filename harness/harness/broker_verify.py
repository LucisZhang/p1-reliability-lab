from __future__ import annotations

import sys
from collections.abc import Sequence

from harness import (
    broker_parity,
    broker_restart_drill,
    duplicate_redelivery_drill,
    offset_replay_drill,
    ordering_miskey_drill,
    poison_dlq_drill,
    schema_contract_drill,
)

FAILURE_RUNNERS = {
    "broker-restart": broker_restart_drill.main,
    "duplicate-redelivery": duplicate_redelivery_drill.main,
    "mis-keying": ordering_miskey_drill.main,
    "poison-dlq": poison_dlq_drill.main,
    "offset-replay": offset_replay_drill.main,
}


def split_phase(argv: Sequence[str]) -> tuple[str, list[str]]:
    remaining = list(argv)
    phase = "parity"
    if "--phase" in remaining:
        index = remaining.index("--phase")
        if index + 1 >= len(remaining):
            raise ValueError("--phase requires parity or contracts")
        phase = remaining[index + 1]
        del remaining[index : index + 2]
    if phase not in {"parity", "contracts"}:
        raise ValueError("--phase must be parity or contracts")
    return phase, remaining


def split_failure(argv: Sequence[str]) -> tuple[str | None, list[str]]:
    remaining = list(argv)
    if "--failure" not in remaining:
        return None, remaining
    if "--phase" in remaining:
        raise ValueError("--phase and --failure are mutually exclusive")
    index = remaining.index("--failure")
    if index + 1 >= len(remaining):
        raise ValueError("--failure requires a Phase B3 failure name")
    failure = remaining[index + 1]
    del remaining[index : index + 2]
    if failure not in FAILURE_RUNNERS:
        choices = ", ".join(FAILURE_RUNNERS)
        raise ValueError(f"--failure must be one of: {choices}")
    return failure, remaining


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(argv) if argv is not None else sys.argv[1:]
    try:
        failure, remaining = split_failure(raw)
        if failure is not None:
            return FAILURE_RUNNERS[failure](remaining)
        phase, remaining = split_phase(remaining)
    except ValueError as exc:
        print(f"broker-verify failed: {exc}", file=sys.stderr)
        return 2
    if phase == "contracts":
        return schema_contract_drill.main(remaining)
    return broker_parity.main(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
