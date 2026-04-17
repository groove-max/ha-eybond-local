"""Load declarative register schemas from JSON files."""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from ..models import (
    BinarySensorDescription,
    MeasurementDescription,
    RegisterValueSpec,
    decimals_for_divisor,
)
from .register_schema_models import RegisterBlockLayout, RegisterSchemaMetadata
from ..const import BUILTIN_SCHEMA_PREFIX

REGISTER_SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "register_schemas"
_EXTERNAL_REGISTER_SCHEMA_ROOTS: tuple[Path, ...] = ()


@lru_cache(maxsize=None)
def load_register_schema(schema_name: str) -> RegisterSchemaMetadata:
    """Load one declarative register schema from the register_schemas directory."""

    schema_path = _resolve_schema_path(schema_name)
    raw = _load_raw_schema(schema_path)

    enum_tables = {
        key: {_parse_enum_key(enum_key): str(label) for enum_key, label in value.items()}
        for key, value in raw.get("enum_tables", {}).items()
    }
    bit_labels = {
        key: {int(bit_key): str(label) for bit_key, label in value.items()}
        for key, value in raw.get("bit_labels", {}).items()
    }
    spec_sets = {
        key: tuple(_parse_spec(item, enum_tables) for item in value)
        for key, value in raw.get("spec_sets", {}).items()
    }
    measurement_precisions = _build_measurement_display_precisions(spec_sets)

    schema = RegisterSchemaMetadata(
        key=str(raw.get("schema_key", schema_path.stem)),
        title=str(raw.get("title", schema_path.stem)),
        driver_key=str(raw.get("driver_key", schema_path.stem)),
        protocol_family=str(raw.get("protocol_family", schema_path.stem)),
        source_name=str(schema_name),
        source_path=str(schema_path),
        source_scope=_register_schema_source_scope(schema_path),
        blocks=tuple(_parse_block(item) for item in raw.get("blocks", [])),
        spec_sets=spec_sets,
        enum_tables=enum_tables,
        bit_labels=bit_labels,
        scalar_registers={
            key: int(value) for key, value in raw.get("scalar_registers", {}).items()
        },
        measurement_descriptions=tuple(
            _parse_measurement_description(item, measurement_precisions)
            for item in raw.get("measurement_descriptions", [])
        ),
        binary_sensor_descriptions=tuple(
            _parse_binary_sensor_description(item)
            for item in raw.get("binary_sensor_descriptions", [])
        ),
    )
    _validate_schema(schema)
    return schema


@lru_cache(maxsize=None)
def _load_raw_schema(schema_path: Path) -> dict[str, Any]:
    raw = json.loads(schema_path.read_text(encoding="utf-8"))
    parent_ref = raw.pop("extends", None)
    if not parent_ref:
        return raw

    parent_ref_str = str(parent_ref)
    if parent_ref_str.startswith(("./", "../")):
        parent_path = _resolve_relative_parent_schema_path(schema_path, parent_ref_str)
    else:
        parent_path = _resolve_schema_path(parent_ref_str)
    parent_raw = _load_raw_schema(parent_path)
    return _merge_raw_schema(parent_raw, raw)


def set_external_register_schema_roots(roots: tuple[Path, ...] | list[Path]) -> None:
    """Configure additional search roots for declarative register schemas."""

    global _EXTERNAL_REGISTER_SCHEMA_ROOTS
    normalized = tuple(Path(root).resolve() for root in roots if root)
    if normalized == _EXTERNAL_REGISTER_SCHEMA_ROOTS:
        return
    _EXTERNAL_REGISTER_SCHEMA_ROOTS = normalized
    load_register_schema.cache_clear()
    _load_raw_schema.cache_clear()


def clear_register_schema_loader_cache() -> None:
    """Clear cached declarative register schemas."""

    load_register_schema.cache_clear()
    _load_raw_schema.cache_clear()


def _register_schema_search_dirs() -> tuple[Path, ...]:
    return (*_EXTERNAL_REGISTER_SCHEMA_ROOTS, REGISTER_SCHEMAS_DIR)


def _resolve_schema_path(schema_name: str) -> Path:
    schema_path = Path(schema_name)
    if schema_path.is_absolute():
        return schema_path

    if schema_name.startswith(BUILTIN_SCHEMA_PREFIX):
        return builtin_register_schema_path(schema_name.removeprefix(BUILTIN_SCHEMA_PREFIX))

    for root in _register_schema_search_dirs():
        candidate = (root / schema_name).resolve()
        if not _is_within_root(candidate, root):
            continue
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"register_schema_not_found:{schema_name}")


def builtin_register_schema_path(schema_name: str) -> Path:
    """Return the built-in register schema path, bypassing external overrides."""

    return (REGISTER_SCHEMAS_DIR / schema_name).resolve()


def _resolve_relative_parent_schema_path(schema_path: Path, parent_ref: str) -> Path:
    candidate = (schema_path.parent / parent_ref).resolve()
    if candidate.is_file():
        return candidate

    resolved_schema_path = schema_path.resolve()
    for root in _EXTERNAL_REGISTER_SCHEMA_ROOTS:
        resolved_root = root.resolve()
        if not _is_within_root(resolved_schema_path, resolved_root):
            continue
        relative_schema_path = resolved_schema_path.relative_to(resolved_root)
        builtin_schema_path = (REGISTER_SCHEMAS_DIR / relative_schema_path).resolve()
        if not _is_within_root(builtin_schema_path, REGISTER_SCHEMAS_DIR):
            continue
        builtin_candidate = (builtin_schema_path.parent / parent_ref).resolve()
        if _is_within_root(builtin_candidate, REGISTER_SCHEMAS_DIR) and builtin_candidate.is_file():
            return builtin_candidate

    return candidate


def _register_schema_source_scope(schema_path: Path) -> str:
    resolved = schema_path.resolve()
    if _is_within_root(resolved, REGISTER_SCHEMAS_DIR):
        return "builtin"
    for root in _EXTERNAL_REGISTER_SCHEMA_ROOTS:
        if _is_within_root(resolved, root):
            return "external"
    return "absolute"


def _parse_block(raw: Mapping[str, Any]) -> RegisterBlockLayout:
    return RegisterBlockLayout(
        key=str(raw["key"]),
        start=int(raw["start"]),
        count=int(raw["count"]),
    )


def _parse_spec(
    raw: Mapping[str, Any],
    enum_tables: Mapping[str, dict[int | str, str]],
) -> RegisterValueSpec:
    enum_map = None
    enum_table = raw.get("enum_table")
    if enum_table is not None:
        enum_map = enum_tables[str(enum_table)]
    elif isinstance(raw.get("enum_map"), Mapping):
        enum_map = {_parse_enum_key(key): str(value) for key, value in raw["enum_map"].items()}

    return RegisterValueSpec(
        key=str(raw["key"]),
        register=int(raw["register"]),
        word_count=int(raw.get("word_count", 1)),
        signed=bool(raw.get("signed", False)),
        combine=str(raw.get("combine", "u16")),
        divisor=_optional_int(raw.get("divisor")),
        decimals=_optional_int(raw.get("decimals")),
        enum_map=enum_map,
    )


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _parse_enum_key(value: Any) -> int | str:
    text = str(value)
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return int(text)
    return text


def _build_measurement_display_precisions(
    spec_sets: Mapping[str, tuple[RegisterValueSpec, ...]],
) -> dict[str, int]:
    precisions: dict[str, int] = {}
    for specs in spec_sets.values():
        for spec in specs:
            precision = _measurement_display_precision(spec)
            if precision is None:
                continue
            current = precisions.get(spec.key)
            if current is None or precision > current:
                precisions[spec.key] = precision
    return precisions


def _measurement_display_precision(spec: RegisterValueSpec) -> int | None:
    if spec.decimals is not None:
        return max(spec.decimals, 0)
    if spec.divisor is None:
        return None
    precision = decimals_for_divisor(spec.divisor)
    if 10**precision != spec.divisor:
        return None
    return precision


def _parse_measurement_description(
    raw: Mapping[str, Any],
    inferred_precisions: Mapping[str, int] | None = None,
) -> MeasurementDescription:
    key = str(raw["key"])
    suggested_display_precision = _optional_int(raw.get("suggested_display_precision"))
    if suggested_display_precision is None and inferred_precisions is not None:
        suggested_display_precision = inferred_precisions.get(key)

    return MeasurementDescription(
        key=key,
        name=str(raw["name"]),
        unit=_optional_str(raw.get("unit")),
        device_class=_optional_str(raw.get("device_class")),
        state_class=_optional_str(raw.get("state_class")),
        icon=_optional_str(raw.get("icon")),
        diagnostic=bool(raw.get("diagnostic", False)),
        enabled_default=bool(raw.get("enabled_default", True)),
        live=bool(raw.get("live", True)),
        suggested_display_precision=suggested_display_precision,
    )


def _parse_binary_sensor_description(raw: Mapping[str, Any]) -> BinarySensorDescription:
    return BinarySensorDescription(
        key=str(raw["key"]),
        name=str(raw["name"]),
        device_class=_optional_str(raw.get("device_class")),
        icon=_optional_str(raw.get("icon")),
        diagnostic=bool(raw.get("diagnostic", False)),
        enabled_default=bool(raw.get("enabled_default", True)),
        live=bool(raw.get("live", True)),
    )


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _merge_raw_schema(
    base: Mapping[str, Any],
    overlay: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(base)
    merged["blocks"] = _merge_keyed_list(
        base.get("blocks", []),
        overlay.get("blocks", []),
        key_field="key",
    )
    merged["measurement_descriptions"] = _merge_keyed_list(
        base.get("measurement_descriptions", []),
        overlay.get("measurement_descriptions", []),
        key_field="key",
    )
    merged["binary_sensor_descriptions"] = _merge_keyed_list(
        base.get("binary_sensor_descriptions", []),
        overlay.get("binary_sensor_descriptions", []),
        key_field="key",
    )
    merged["spec_sets"] = _merge_spec_sets(
        base.get("spec_sets", {}),
        overlay.get("spec_sets", {}),
    )
    merged["enum_tables"] = _merge_nested_maps(
        base.get("enum_tables", {}),
        overlay.get("enum_tables", {}),
    )
    merged["bit_labels"] = _merge_nested_maps(
        base.get("bit_labels", {}),
        overlay.get("bit_labels", {}),
    )
    merged["scalar_registers"] = {
        **base.get("scalar_registers", {}),
        **overlay.get("scalar_registers", {}),
    }

    for key, value in overlay.items():
        if key in {
            "blocks",
            "measurement_descriptions",
            "binary_sensor_descriptions",
            "spec_sets",
            "enum_tables",
            "bit_labels",
            "scalar_registers",
        }:
            continue
        merged[key] = value
    return merged


def _merge_spec_sets(
    base: Mapping[str, list[dict[str, Any]]],
    overlay: Mapping[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    merged = {
        key: [dict(item) for item in value]
        for key, value in base.items()
    }
    for set_key, overlay_items in overlay.items():
        base_items = merged.get(set_key, [])
        merged[set_key] = _merge_keyed_list(base_items, overlay_items, key_field="key")
    return merged


def _merge_nested_maps(
    base: Mapping[str, Mapping[str, Any]],
    overlay: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {key: dict(value) for key, value in base.items()}
    for key, value in overlay.items():
        merged[key] = {**merged.get(key, {}), **value}
    return merged


def _merge_keyed_list(
    base: list[Mapping[str, Any]],
    overlay: list[Mapping[str, Any]],
    *,
    key_field: str,
) -> list[dict[str, Any]]:
    result = [dict(item) for item in base]
    positions = {
        str(item[key_field]): index
        for index, item in enumerate(result)
        if key_field in item
    }
    for item in overlay:
        item_key = str(item[key_field])
        if item_key in positions:
            index = positions[item_key]
            result[index] = {**result[index], **item}
            continue
        positions[item_key] = len(result)
        result.append(dict(item))
    return result


def _validate_schema(schema: RegisterSchemaMetadata) -> None:
    block_keys = _unique_or_raise(
        items=(block.key for block in schema.blocks),
        kind="block",
        schema_key=schema.key,
    )
    _unique_or_raise(
        items=(key for key in schema.spec_sets),
        kind="spec_set",
        schema_key=schema.key,
    )
    _unique_or_raise(
        items=(key for key in schema.enum_tables),
        kind="enum_table",
        schema_key=schema.key,
    )
    _unique_or_raise(
        items=(key for key in schema.bit_labels),
        kind="bit_labels",
        schema_key=schema.key,
    )
    _unique_or_raise(
        items=(key for key in schema.scalar_registers),
        kind="scalar_register",
        schema_key=schema.key,
    )
    _unique_or_raise(
        items=(item.key for item in schema.measurement_descriptions),
        kind="measurement_description",
        schema_key=schema.key,
    )
    _unique_or_raise(
        items=(item.key for item in schema.binary_sensor_descriptions),
        kind="binary_sensor_description",
        schema_key=schema.key,
    )

    required_blocks = {"status", "serial", "live", "config"}
    missing_blocks = sorted(required_blocks - block_keys)
    if missing_blocks:
        raise ValueError(f"register_schema:{schema.key}:missing_blocks:{','.join(missing_blocks)}")


def _unique_or_raise(*, items, kind: str, schema_key: str) -> set[str]:
    seen: set[str] = set()
    for item in items:
        if item in seen:
            raise ValueError(f"register_schema:{schema_key}:duplicate_{kind}:{item}")
        seen.add(item)
    return seen
