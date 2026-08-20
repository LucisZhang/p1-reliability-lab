from __future__ import annotations

import sys
from collections.abc import Sequence

from harness import broker_parity, schema_contract_drill


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


def main(argv: Sequence[str] | None = None) -> int:
    try:
        phase, remaining = split_phase(list(argv) if argv is not None else sys.argv[1:])
    except ValueError as exc:
        print(f"broker-verify failed: {exc}", file=sys.stderr)
        return 2
    if phase == "contracts":
        return schema_contract_drill.main(remaining)
    return broker_parity.main(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
