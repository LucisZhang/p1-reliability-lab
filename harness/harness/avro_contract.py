from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from avro import schema as avro_schema
from avro.compatibility import (
    ReaderWriterCompatibilityChecker,
    SchemaCompatibilityType,
)

JsonObject = dict[str, object]


@dataclass(frozen=True)
class ContractEvaluation:
    backward_compatible: bool
    required_fields_preserved: bool
    added_fields_have_defaults: bool
    removed_fields: tuple[str, ...]
    changed_field_types: tuple[str, ...]
    added_fields_without_defaults: tuple[str, ...]
    avro_messages: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return (
            self.backward_compatible
            and self.required_fields_preserved
            and self.added_fields_have_defaults
            and not self.changed_field_types
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "backward_compatible": self.backward_compatible,
            "required_fields_preserved": self.required_fields_preserved,
            "added_fields_have_defaults": self.added_fields_have_defaults,
            "removed_fields": list(self.removed_fields),
            "changed_field_types": list(self.changed_field_types),
            "added_fields_without_defaults": list(self.added_fields_without_defaults),
            "avro_messages": list(self.avro_messages),
        }


def load_contract(path: Path) -> JsonObject:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Avro contract must be a JSON object: {path}")
    _record_fields(cast(JsonObject, payload))
    return cast(JsonObject, payload)


def contract_sha256(contract: JsonObject) -> str:
    canonical = json.dumps(contract, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def evaluate_evolution(previous: JsonObject, candidate: JsonObject) -> ContractEvaluation:
    previous_fields = _record_fields(previous)
    candidate_fields = _record_fields(candidate)

    previous_by_name = {_field_name(field): field for field in previous_fields}
    candidate_by_name = {_field_name(field): field for field in candidate_fields}

    removed = tuple(sorted(previous_by_name.keys() - candidate_by_name.keys()))
    added = tuple(sorted(candidate_by_name.keys() - previous_by_name.keys()))
    changed_types = tuple(
        sorted(
            name
            for name in previous_by_name.keys() & candidate_by_name.keys()
            if _canonical_type(previous_by_name[name]) != _canonical_type(candidate_by_name[name])
        )
    )
    added_without_defaults = tuple(
        name for name in added if "default" not in candidate_by_name[name]
    )

    reader = avro_schema.parse(json.dumps(candidate))
    writer = avro_schema.parse(json.dumps(previous))
    compatibility = ReaderWriterCompatibilityChecker().get_compatibility(  # type: ignore[no-untyped-call]
        reader, writer
    )
    backward_compatible = compatibility.compatibility == SchemaCompatibilityType.compatible

    return ContractEvaluation(
        backward_compatible=backward_compatible,
        required_fields_preserved=not removed,
        added_fields_have_defaults=not added_without_defaults,
        removed_fields=removed,
        changed_field_types=changed_types,
        added_fields_without_defaults=added_without_defaults,
        avro_messages=tuple(sorted(str(message) for message in compatibility.messages)),
    )


def _record_fields(contract: JsonObject) -> list[JsonObject]:
    if contract.get("type") != "record":
        raise ValueError("order-change contract root must be an Avro record")
    raw_fields = contract.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ValueError("order-change contract must contain fields")
    fields: list[JsonObject] = []
    for raw_field in raw_fields:
        if not isinstance(raw_field, dict):
            raise ValueError("Avro record fields must be JSON objects")
        field = cast(JsonObject, raw_field)
        _field_name(field)
        if "type" not in field:
            raise ValueError(f"Avro field {_field_name(field)!r} has no type")
        fields.append(field)
    return fields


def _field_name(field: JsonObject) -> str:
    name = field.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("Avro field must have a non-empty string name")
    return name


def _canonical_type(field: JsonObject) -> str:
    return json.dumps(field["type"], separators=(",", ":"), sort_keys=True)
