from __future__ import annotations

import pytest

from harness.broker_verify import split_phase


def test_broker_verify_defaults_to_unchanged_parity_mode() -> None:
    assert split_phase(["--events", "10", "--seed", "17"]) == (
        "parity",
        ["--events", "10", "--seed", "17"],
    )


def test_broker_verify_selects_contract_mode_without_forwarding_phase() -> None:
    assert split_phase(["--phase", "contracts", "--seed", "211"]) == (
        "contracts",
        ["--seed", "211"],
    )


def test_broker_verify_rejects_unknown_phase() -> None:
    with pytest.raises(ValueError, match="parity or contracts"):
        split_phase(["--phase", "faults"])
