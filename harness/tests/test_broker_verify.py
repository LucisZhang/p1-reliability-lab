from __future__ import annotations

import pytest

from harness.broker_verify import FAILURE_RUNNERS, split_failure, split_phase


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


def test_broker_verify_selects_slo_mode_without_forwarding_phase() -> None:
    assert split_phase(["--phase", "slo", "--seed", "401"]) == (
        "slo",
        ["--seed", "401"],
    )


def test_broker_verify_rejects_unknown_phase() -> None:
    with pytest.raises(ValueError, match="parity, contracts, or slo"):
        split_phase(["--phase", "faults"])


def test_broker_verify_routes_each_phase_b3_failure_without_forwarding_flag() -> None:
    assert set(FAILURE_RUNNERS) == {
        "broker-restart",
        "duplicate-redelivery",
        "mis-keying",
        "poison-dlq",
        "offset-replay",
    }
    for failure in FAILURE_RUNNERS:
        assert split_failure(["--failure", failure, "--seed", "999"]) == (
            failure,
            ["--seed", "999"],
        )


def test_broker_verify_rejects_mixed_phase_and_failure_modes() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        split_failure(["--phase", "contracts", "--failure", "broker-restart"])
