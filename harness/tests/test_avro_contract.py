from __future__ import annotations

from pathlib import Path

from harness.avro_contract import contract_sha256, evaluate_evolution, load_contract
from harness.config import REPO_ROOT

CONTRACTS = REPO_ROOT / "contracts" / "avro"


def _contract(filename: str) -> dict[str, object]:
    return load_contract(Path(CONTRACTS / filename))


def test_add_with_default_is_backward_compatible() -> None:
    evaluation = evaluate_evolution(
        _contract("order-change-v1.avsc"),
        _contract("order-change-compatible-add-v2.avsc"),
    )

    assert evaluation.passed is True
    assert evaluation.backward_compatible is True
    assert evaluation.added_fields_have_defaults is True
    assert evaluation.removed_fields == ()


def test_required_field_removal_is_rejected_by_contract_policy() -> None:
    evaluation = evaluate_evolution(
        _contract("order-change-v1.avsc"),
        _contract("order-change-incompatible-remove-v2.avsc"),
    )

    assert evaluation.passed is False
    assert evaluation.backward_compatible is True
    assert evaluation.required_fields_preserved is False
    assert evaluation.removed_fields == ("event_id",)


def test_type_change_is_rejected_by_avro_and_contract_policy() -> None:
    evaluation = evaluate_evolution(
        _contract("order-change-v1.avsc"),
        _contract("order-change-incompatible-type-v2.avsc"),
    )

    assert evaluation.passed is False
    assert evaluation.backward_compatible is False
    assert evaluation.changed_field_types == ("event_id",)
    assert any("not compatible" in message for message in evaluation.avro_messages)


def test_contract_hash_is_canonical_and_stable() -> None:
    contract = _contract("order-change-v1.avsc")
    assert contract_sha256(contract) == contract_sha256(dict(reversed(list(contract.items()))))
