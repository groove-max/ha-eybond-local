"""Generate device-scoped local overlay drafts from shadow-learning evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

from ..metadata.local_metadata import (
    _dump_json,
    _ensure_can_write,
    ensure_local_metadata_dirs,
    local_profile_path,
    local_profiles_root,
    local_register_schema_path,
    local_register_schemas_root,
)
from ..metadata.device_catalog_loader import force_unsupported_models
from ..metadata.profile_loader import builtin_base_profile_name, load_driver_profile
from ..metadata.semantic_titles_loader import resolve_semantic_title
from ..metadata.register_schema_loader import builtin_base_schema_name, load_register_schema
from ..eybond_g_ascii_settings import G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD
from .read_learning_binder import match_enum_bindings
from .shadow_learning import (
    coerce_optional_int as _to_int,
    deterministic_evidence_hash,
    shadow_learning_slug as _slugify,
)
from .shadow_learning_review_model import (
    attach_learned_read_review_model,
    build_learned_control_review_model,
)


_LEARNED_PROFILE_TITLE_SUFFIX = " (Local Shadow Learned Draft)"
_LEARNED_SCHEMA_TITLE_SUFFIX = " (Local Shadow Learned Draft)"

# Capability group the learned controls are assigned to. The generated overlay
# must also *define* this group (base profiles do not), otherwise activating the
# overlay fails profile validation with ``unknown_group_for_capability``.
_LEARNED_CAPABILITY_GROUP_KEY = "config"

# TEMPORARY VALIDATION TOGGLE -- KEEP False in committed code.
# When True, the overlay generator emits a learned control for EVERY scan-correlated register,
# including ones that duplicate a built-in control (which are normally deduplicated by
# register/title). Use it to add and validate ALL discovered controls (e.g. 23 instead of 6),
# then set it back to False and re-scan. WARNING while it is True: activating the overlay
# creates duplicate entities -- a learned control alongside the built-in one for the same
# register -- so it is for validation only, not normal use.
_EMIT_BUILTIN_DUPLICATE_CONTROLS = False

_ACTION_KEYWORDS = (
    "reset",
    "reboot",
    "restart",
    "clear",
    "sync",
    "turn on",
    "turn off",
)
_DESTRUCTIVE_KEYWORDS = (
    "factory",
    "erase",
    "clear",
    "delete",
    "reset",
)


@dataclass(frozen=True, slots=True)
class ShadowLearningOverlayDraftResult:
    """Result payload for one learned overlay draft generation run."""

    profile_path: Path
    schema_path: Path
    generated_capability_count: int
    skipped_duplicate_count: int
    manifest: dict[str, Any]
    generated_read_count: int = 0


def generate_shadow_learning_overlay_drafts(
    *,
    config_dir: Path,
    source_profile_name: str,
    source_schema_name: str,
    session_manifest: dict[str, Any],
    correlation: dict[str, Any],
    read_map: dict[str, Any] | None = None,
    read_bindings: dict[str, Any] | None = None,
    output_profile_name: str | None = None,
    output_schema_name: str | None = None,
    overwrite: bool = False,
) -> ShadowLearningOverlayDraftResult:
    """Generate one inactive learned profile/schema overlay pair from correlation evidence."""

    ensure_local_metadata_dirs(config_dir)
    # A previously activated learned overlay surfaces as the runtime's effective
    # profile/schema name. Re-running discovery must extend the built-in base the
    # overlay derives from: extending the overlay itself would re-wrap its name in
    # ``builtin:`` (resolving to a non-existent install-dir path) and accumulate the
    # overlay's session token into the new output names. Rebase to the built-in base.
    source_profile_name = builtin_base_profile_name(source_profile_name)
    source_schema_name = builtin_base_schema_name(source_schema_name)
    profile = load_driver_profile(source_profile_name)
    schema = load_register_schema(_builtin_schema_ref(source_schema_name))

    normalized_manifest = _normalize_session_manifest(session_manifest)
    profile_output_name, schema_output_name = _resolve_output_names(
        source_profile_name=source_profile_name,
        source_schema_name=source_schema_name,
        session_manifest=normalized_manifest,
        output_profile_name=output_profile_name,
        output_schema_name=output_schema_name,
    )
    profile_destination = local_profile_path(config_dir, profile_output_name)
    schema_destination = local_register_schema_path(config_dir, schema_output_name)
    _ensure_can_write(profile_destination, local_profiles_root(config_dir), overwrite=overwrite)
    _ensure_can_write(schema_destination, local_register_schemas_root(config_dir), overwrite=overwrite)

    capabilities, learned_summary = _build_learned_capabilities(
        source_profile=profile,
        correlation=correlation,
        session_manifest=normalized_manifest,
    )
    read_enum_bindings = match_enum_bindings(
        read_bindings=read_bindings,
        registers=(read_map or {}).get("registers", {}) if isinstance(read_map, dict) else {},
        enum_tables=dict(schema.enum_tables) if schema.enum_tables else {},
    )
    learned_read = _build_learned_read_overlay(
        schema=schema,
        read_bindings=read_bindings,
        read_enum_bindings=read_enum_bindings,
    )
    review_model = attach_learned_read_review_model(
        build_learned_control_review_model(capabilities),
        learned_read_sensors=list(learned_read["generated"]),
        skipped_read_sensors=list(learned_read["skipped"]),
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "kind": "shadow_learning_device_overlay",
        "generated_at": generated_at,
        "source": "cloud_shadow_learning",
        "scope": "device",
        "session": normalized_manifest,
        "source_profile_name": str(source_profile_name),
        "source_schema_name": str(source_schema_name),
        "correlation_summary": {
            "matched_count": _safe_matched_count(correlation),
            "unmatched_attempt_count": int(correlation.get("unmatched_attempt_count", 0)),
            "unmatched_write_count": int(correlation.get("unmatched_write_count", 0)),
        },
        # The cloud's observed read map (authoritative poll addresses; values are
        # the session seed snapshot). Evidence for read-sensor learning and for
        # catalog contributions; not consumed by write capabilities.
        "read_map": _normalize_read_map(read_map),
        # Cloud label ↔ register correlation verdicts (read-sensor evidence for
        # the schema generator and for catalog contributions).
        "read_bindings": read_bindings if isinstance(read_bindings, dict) else {},
        # Enum-string sensors matched by inverting the source schema's known
        # enum tables against the seed snapshot (single-session evidence; the
        # raw observations accumulate across sessions for table learning).
        "read_enum_bindings": read_enum_bindings,
        "learned_capabilities": list(learned_summary["generated"]),
        "skipped_duplicates": list(learned_summary["skipped"]),
        # Read sensors materialized from unique value/enum correlations.
        "learned_read_sensors": list(learned_read["generated"]),
        "skipped_read_sensors": list(learned_read["skipped"]),
        "review_model": review_model,
        "output": {
            "profile_name": profile_output_name,
            "schema_name": schema_output_name,
            "profile_path": str(profile_destination),
            "schema_path": str(schema_destination),
        },
    }

    profile_raw: dict[str, Any] = {
        "extends": str(source_profile_name),
        "profile_key": f"local_shadow_learning_{_slugify(normalized_manifest.get('session_id') or 'session')}",
        "title": _append_suffix(profile.title, _LEARNED_PROFILE_TITLE_SUFFIX),
        "driver_key": profile.driver_key,
        "protocol_family": profile.protocol_family,
        "draft_of": str(source_profile_name),
        "experimental": True,
        "shadow_learning_overlay": manifest,
        "groups": [
            {
                "key": _LEARNED_CAPABILITY_GROUP_KEY,
                "title": "Learned controls",
                "order": 900,
                "description": "Controls discovered from cloud shadow-learning.",
                "icon": "mdi:cog-outline",
            }
        ],
        "capabilities": capabilities,
    }

    schema_raw: dict[str, Any] = {
        "extends": _builtin_schema_ref(source_schema_name),
        "schema_key": f"local_shadow_learning_{_slugify(normalized_manifest.get('session_id') or 'session')}",
        "title": _append_suffix(schema.title, _LEARNED_SCHEMA_TITLE_SUFFIX),
        "driver_key": schema.driver_key,
        "protocol_family": schema.protocol_family,
        "draft_of": str(source_schema_name),
        "experimental": True,
        "shadow_learning_overlay": manifest,
        "learned_write_registers": sorted(
            {
                int(item.get("register", 0))
                for item in learned_summary["generated"]
                # Admit register 0 (a valid Modbus address): _group_matched_records
                # admits it (rejects only register < 0), so excluding it here left
                # the schema's write-register list inconsistent with the controls.
                if int(item.get("register", 0)) >= 0
            }
        ),
        "learned_write_commands": sorted(
            {
                str(item.get("command") or "")
                for item in learned_summary["generated"]
                if str(item.get("command") or "") and not item.get("command_map")
            }
            | {
                str(command or "")
                for item in learned_summary["generated"]
                for command in (item.get("command_map") or {}).values()
                if str(command or "")
            }
        ),
        **learned_read["schema_fragment"],
    }

    profile_destination.parent.mkdir(parents=True, exist_ok=True)
    schema_destination.parent.mkdir(parents=True, exist_ok=True)
    profile_destination.write_text(_dump_json(profile_raw), encoding="utf-8")
    schema_destination.write_text(_dump_json(schema_raw), encoding="utf-8")

    return ShadowLearningOverlayDraftResult(
        profile_path=profile_destination,
        schema_path=schema_destination,
        generated_capability_count=len(capabilities),
        skipped_duplicate_count=len(learned_summary["skipped"]),
        generated_read_count=len(learned_read["generated"]),
        manifest=manifest,
    )


# Polled spec-set per builtin block key; registers outside all polled blocks are
# read one-by-one via the aux_config optional-spec path (same as learned controls).
_READ_BLOCK_SPEC_SETS = ("status", "live", "config")
_READ_OUT_OF_BLOCK_SPEC_SET = "aux_config"
_READ_UNIT_DEVICE_CLASS = {
    "v": "voltage",
    "a": "current",
    "hz": "frequency",
    "w": "power",
    "va": "apparent_power",
    "var": "reactive_power",
    "°c": "temperature",
    "c": "temperature",
    "wh": "energy",
    "kwh": "energy",
    "%": None,
}


def _build_learned_read_overlay(
    *,
    schema,
    read_bindings: dict[str, Any] | None,
    read_enum_bindings: dict[str, Any] | None,
) -> dict[str, Any]:
    """Turn unique read correlations into schema sensor definitions.

    Numeric and enum sensors whose register/title is already decoded by the
    builtin schema are skipped. Surviving ones are emitted into the right
    polled spec-set (or aux_config for out-of-block registers) plus a
    measurement description, so they materialize as HA sensors through the
    existing decode + read-back path — no driver change required.
    """

    if force_unsupported_models():
        # Validation mode: don't deduplicate against the builtin schema so every
        # correlated read sensor materializes (mirrors the duplicate-controls
        # toggle). Activating then adds learned reads alongside any builtin ones.
        existing_registers: set[int] = set()
        existing_titles: set[str] = set()
    else:
        existing_registers = {
            int(spec.register)
            for specs in schema.spec_sets.values()
            for spec in specs
        }
        existing_titles = {
            _normalize_title(measurement.name or measurement.key)
            for measurement in schema.measurement_descriptions
        }
    block_for_register = _polled_block_lookup(schema)

    spec_set_additions: dict[str, list[dict[str, Any]]] = {}
    measurements: list[dict[str, Any]] = []
    generated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    used_keys: set[str] = set()

    def _accept(
        *,
        register: int,
        title: str,
        spec: dict[str, Any],
        measurement: dict[str, Any],
        kind: str,
    ) -> None:
        set_name = block_for_register(register) or _READ_OUT_OF_BLOCK_SPEC_SET
        spec_set_additions.setdefault(set_name, []).append(spec)
        measurements.append(measurement)
        generated.append(
            {"key": spec["key"], "register": register, "title": title, "kind": kind, "spec_set": set_name}
        )

    for binding in _unique_read_bindings(read_bindings):
        register = binding["register"]
        title = binding["title"]
        title_key = _normalize_title(title)
        skip_reason = _read_skip_reason(register, title_key, existing_registers, existing_titles)
        if skip_reason is not None:
            skipped.append({"register": register, "title": title, "kind": "numeric", "reason": skip_reason})
            continue
        key = _unique_read_key(f"learned_read_{register}", used_keys)
        existing_registers.add(register)
        existing_titles.add(title_key)
        spec: dict[str, Any] = {"key": key, "register": register}
        if binding["signed"]:
            spec["signed"] = True
        if binding["divisor"] > 1:
            spec["divisor"] = binding["divisor"]
            spec["decimals"] = binding["decimals"]
        measurement = _read_measurement(
            key=key, title=title, unit=binding["unit"], decimals=binding["decimals"], divisor=binding["divisor"]
        )
        _accept(register=register, title=title, spec=spec, measurement=measurement, kind="numeric")

    for binding in _unique_enum_bindings(read_enum_bindings):
        register = binding["register"]
        title = binding["title"]
        title_key = _normalize_title(title)
        skip_reason = _read_skip_reason(register, title_key, existing_registers, existing_titles)
        if skip_reason is not None:
            skipped.append({"register": register, "title": title, "kind": "enum", "reason": skip_reason})
            continue
        key = _unique_read_key(f"learned_read_{register}", used_keys)
        existing_registers.add(register)
        existing_titles.add(title_key)
        spec = {"key": key, "register": register, "enum_table": binding["enum_table"]}
        measurement = {"key": key, "name": title, "enabled_default": True, "learned": True}
        semantic = resolve_semantic_title(title)
        if semantic is not None:
            measurement["translation_key"] = semantic.semantic_key
        _accept(register=register, title=title, spec=spec, measurement=measurement, kind="enum")

    fragment: dict[str, Any] = {}
    if spec_set_additions:
        fragment["spec_sets"] = {
            set_name: specs for set_name, specs in spec_set_additions.items()
        }
    if measurements:
        fragment["measurement_descriptions"] = measurements
        fragment["learned_read_registers"] = sorted({int(item["register"]) for item in generated})

    return {"schema_fragment": fragment, "generated": generated, "skipped": skipped}


def _polled_block_lookup(schema):
    ranges: list[tuple[int, int, str]] = []
    for block in schema.blocks:
        if block.key in _READ_BLOCK_SPEC_SETS:
            ranges.append((int(block.start), int(block.start) + int(block.count), block.key))

    def _lookup(register: int) -> str | None:
        for start, stop, set_name in ranges:
            if start <= register < stop:
                return set_name
        return None

    return _lookup


def _read_skip_reason(
    register: int,
    title_key: str,
    existing_registers: set[int],
    existing_titles: set[str],
) -> str | None:
    if register in existing_registers:
        return "register_already_decoded"
    if title_key and title_key in existing_titles:
        return "title_already_mapped"
    return None


def _read_measurement(
    *,
    key: str,
    title: str,
    unit: str,
    decimals: int,
    divisor: int,
) -> dict[str, Any]:
    measurement: dict[str, Any] = {
        "key": key,
        "name": title,
        "state_class": "measurement",
        "enabled_default": True,
        "learned": True,
    }
    # The cross-vendor semantic catalog is the authority for presentation when
    # it knows this title; the cloud unit and a unit→class guess are fallbacks.
    semantic = resolve_semantic_title(title)
    normalized_unit = str(unit or "").strip()
    if semantic is not None:
        measurement["translation_key"] = semantic.semantic_key
        if semantic.unit:
            normalized_unit = semantic.unit
        if semantic.state_class:
            measurement["state_class"] = semantic.state_class
        if semantic.device_class:
            measurement["device_class"] = semantic.device_class
    if normalized_unit:
        measurement["unit"] = normalized_unit
        measurement.setdefault(
            "device_class", _READ_UNIT_DEVICE_CLASS.get(normalized_unit.lower())
        )
        if measurement.get("device_class") is None:
            measurement.pop("device_class")
    if divisor > 1 and decimals > 0:
        measurement["suggested_display_precision"] = decimals
    return measurement


def _unique_read_bindings(read_bindings: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(read_bindings, dict):
        return []
    rows: list[dict[str, Any]] = []
    for item in read_bindings.get("bindings", []):
        if not isinstance(item, dict) or item.get("status") != "unique":
            continue
        candidates = item.get("candidates") or []
        if not candidates:
            continue
        candidate = candidates[0]
        rows.append(
            {
                "register": int(candidate.get("register", 0)),
                "title": str(item.get("title") or ""),
                "unit": str(item.get("unit") or ""),
                "divisor": int(candidate.get("divisor", 1) or 1),
                "decimals": int(item.get("decimals", 0) or 0),
                "signed": bool(candidate.get("signed")),
            }
        )
    return rows


def _unique_enum_bindings(read_enum_bindings: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(read_enum_bindings, dict):
        return []
    rows: list[dict[str, Any]] = []
    for item in read_enum_bindings.get("bindings", []):
        if not isinstance(item, dict) or item.get("status") != "unique":
            continue
        candidates = item.get("candidates") or []
        if not candidates:
            continue
        candidate = candidates[0]
        rows.append(
            {
                "register": int(candidate.get("register", 0)),
                "title": str(item.get("title") or ""),
                "enum_table": str(candidate.get("enum_table") or ""),
            }
        )
    return rows


def _unique_read_key(base_key: str, used_keys: set[str]) -> str:
    key = base_key
    suffix = 2
    while key in used_keys:
        key = f"{base_key}_{suffix}"
        suffix += 1
    used_keys.add(key)
    return key


def _build_learned_capabilities(
    *,
    source_profile,
    correlation: dict[str, Any],
    session_manifest: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    existing_keys = {capability.key for capability in source_profile.capabilities}
    if _EMIT_BUILTIN_DUPLICATE_CONTROLS:
        # Validation mode: do not deduplicate against built-in controls, so every discovered
        # register becomes a learned control. (existing_keys is kept so learned keys stay
        # unique; learned keys are prefixed and never collide with built-in keys anyway.)
        existing_titles: set[str] = set()
        existing_registers: set[int] = set()
        existing_commands: set[str] = set()
    else:
        existing_titles = {_normalize_title(capability.title or capability.key) for capability in source_profile.capabilities}
        existing_registers = {int(capability.register) for capability in source_profile.capabilities}
        existing_commands = {
            str(getattr(capability, "command", "") or "").strip().upper()
            for capability in source_profile.capabilities
            if str(getattr(capability, "command", "") or "").strip()
        }

    grouped = [*_group_matched_records(correlation), *_group_matched_command_records(correlation)]
    generated: list[dict[str, Any]] = []
    generated_summary: list[dict[str, Any]] = []
    skipped_summary: list[dict[str, Any]] = []

    next_order = 9000
    for group in grouped:
        register = int(group.get("register", -1))
        command = str(group.get("command") or "").strip().upper()
        command_map = {
            int(key): str(value).strip().upper()
            for key, value in dict(group.get("command_map") or {}).items()
            if str(value or "").strip()
        }
        field_name = str(group["field_name"])
        field_id = str(group["field_id"])
        title_key = _normalize_title(field_name)

        if register >= 0 and register in existing_registers:
            skipped_summary.append(
                {
                    "field_id": field_id,
                    "field_name": field_name,
                    "register": register,
                    "reason": "register_already_mapped",
                }
            )
            continue
        if command and not command_map and command in existing_commands:
            skipped_summary.append(
                {
                    "field_id": field_id,
                    "field_name": field_name,
                    "command": command,
                    "reason": "command_already_mapped",
                }
            )
            continue
        if title_key and title_key in existing_titles:
            skipped_summary.append(
                {
                    "field_id": field_id,
                    "field_name": field_name,
                    "register": register,
                    "command": command,
                    "reason": "title_already_mapped",
                }
            )
            continue

        base_key = (
            _capability_key(field_id=field_id, field_name=field_name, register=register)
            if register >= 0
            else _command_capability_key(field_id=field_id, field_name=field_name, command=command)
        )
        capability_key = _unique_capability_key(base_key, existing_keys)
        existing_keys.add(capability_key)

        read_key = _g_ascii_read_key_for_group(group)
        classification = _classify_learned_control(group)
        classification, command_map = _normalize_g_ascii_feature_flag_capability(
            group=group,
            classification=classification,
            command_map=command_map,
            read_key=read_key,
        )
        provenance = {
            "source": "cloud_shadow_learning",
            "scope": "device",
            "cloud_field_id": field_id,
            "write_command": command,
            "evidence_hash": str(group["evidence_hash"]),
            "learned_at": str(group["learned_at"]),
            "confidence": str(classification["confidence"]),
            "safety_class": str(classification["safety_class"]),
            "session_id": str(session_manifest.get("session_id") or ""),
        }

        capability: dict[str, Any] = {
            "key": capability_key,
            "title": field_name,
            "register": register,
            "value_kind": str(classification["value_kind"]),
            "note": "Learned from shadow-learning write correlation.",
            "word_count": int(classification["word_count"]),
            "combine": str(classification["combine"]),
            "tested": False,
            "provenance": "cloud_hint",
            "support_tier": "conditional",
            "support_notes": "Generated from local shadow-learning evidence. Draft is inactive until explicitly activated.",
            "enabled_default": False,
            "advanced": bool(classification["advanced"]),
            "requires_confirm": bool(classification["requires_confirm"]),
            "unsafe_while_running": bool(classification["unsafe_while_running"]),
            "group": _LEARNED_CAPABILITY_GROUP_KEY,
            "order": next_order,
            "learned_provenance": provenance,
        }
        next_order += 1

        minimum = classification.get("minimum")
        maximum = classification.get("maximum")
        action_value = classification.get("action_value")
        enum_map = classification.get("enum_map")
        divisor = classification.get("divisor")
        unit = classification.get("unit")
        if minimum is not None:
            capability["minimum"] = int(minimum)
        if maximum is not None:
            capability["maximum"] = int(maximum)
        if action_value is not None:
            capability["action_value"] = int(action_value)
        if divisor is not None:
            capability["divisor"] = int(divisor)
        if unit:
            capability["unit"] = str(unit)
        if isinstance(enum_map, dict):
            capability["enum_map"] = {int(key): str(value) for key, value in enum_map.items()}
        if command:
            capability["command"] = command
        if command_map:
            capability["command_map"] = {int(key): value for key, value in sorted(command_map.items())}
        if read_key:
            capability["read_key"] = read_key

        generated.append(capability)
        generated_summary.append(
            {
                "key": capability_key,
                "field_id": field_id,
                "field_name": field_name,
                "register": register,
                "command": command,
                "command_map": capability.get("command_map", {}),
                "read_key": read_key,
                "value_kind": capability["value_kind"],
                "safety_class": provenance["safety_class"],
                "confidence": provenance["confidence"],
                "evidence_hash": provenance["evidence_hash"],
            }
        )

    return generated, {"generated": generated_summary, "skipped": skipped_summary}


def _group_matched_records(correlation: dict[str, Any]) -> list[dict[str, Any]]:
    matched = correlation.get("matched")
    if not isinstance(matched, list):
        return []

    grouped: dict[str, dict[str, Any]] = {}
    for item in matched:
        if not isinstance(item, dict):
            continue
        observation = item.get("observation")
        if not isinstance(observation, dict):
            continue
        register = _to_int(observation.get("register"))
        if register is None or register < 0:
            continue

        field_id = str(item.get("field_id") or "").strip()
        field_name = str(item.get("field_name") or field_id or f"register_{register}").strip()
        key = (
            f"{field_id}::register_{register}"
            if field_id
            else f"register_{register}_{_slugify(field_name)}"
        )
        requested_value = str(item.get("requested_value") or "").strip()

        entry = grouped.setdefault(
            key,
            {
                "field_id": field_id,
                "field_name": field_name,
                "register": register,
                "samples": [],
            },
        )
        entry["samples"].append(
            {
                "sequence_index": _to_int(item.get("sequence_index")) or 0,
                "requested_value": requested_value,
                "value_label": str(item.get("value_label") or "").strip(),
                "value_source": str(item.get("value_source") or "").strip(),
                "requested_at": str(item.get("requested_at") or "").strip(),
                "observation": {
                    "register": register,
                    "function_code": _to_int(observation.get("function_code")) or 16,
                    "values": [
                        int(value)
                        for value in list(observation.get("values") or [])
                        if _to_int(value) is not None
                    ],
                    "devcode": _to_int(observation.get("devcode")),
                    "devaddr": _to_int(observation.get("devaddr")),
                    "timestamp": str(observation.get("timestamp") or "").strip(),
                },
            }
        )

    output: list[dict[str, Any]] = []
    for item in grouped.values():
        samples = sorted(item["samples"], key=lambda sample: int(sample.get("sequence_index", 0)))
        if not samples:
            continue
        learned_at = _first_non_empty(
            [
                str(sample["observation"].get("timestamp") or "")
                for sample in samples
            ]
        ) or _first_non_empty([str(sample.get("requested_at") or "") for sample in samples])
        sample_payload = {
            "field_id": str(item["field_id"]),
            "field_name": str(item["field_name"]),
            "register": int(item["register"]),
            "samples": samples,
        }
        output.append(
            {
                "field_id": str(item["field_id"]),
                "field_name": str(item["field_name"]),
                "register": int(item["register"]),
                "samples": samples,
                "learned_at": learned_at or datetime.now(timezone.utc).isoformat(),
                "evidence_hash": deterministic_evidence_hash(sample_payload),
            }
        )
    return sorted(output, key=lambda item: (int(item["register"]), str(item["field_name"])))


def _group_matched_command_records(correlation: dict[str, Any]) -> list[dict[str, Any]]:
    matched = correlation.get("matched")
    if not isinstance(matched, list):
        return []

    grouped: dict[str, dict[str, Any]] = {}
    for item in matched:
        if not isinstance(item, dict):
            continue
        observation = item.get("observation")
        if not isinstance(observation, dict):
            continue
        protocol = str(observation.get("protocol") or "").strip()
        command = str(observation.get("command") or "").strip().upper()
        if protocol != "eybond_g_ascii" or not command:
            continue

        field_id = str(item.get("field_id") or "").strip()
        field_name = str(item.get("field_name") or field_id or command).strip()
        key = f"{field_id}::command_field" if field_id else f"command_{command}_{_slugify(field_name)}"
        requested_value = str(item.get("requested_value") or "").strip()

        entry = grouped.setdefault(
            key,
            {
                "field_id": field_id,
                "field_name": field_name,
                "register": -1,
                "command": "",
                "read_key": str(item.get("read_key") or "").strip(),
                "samples": [],
            },
        )
        entry["samples"].append(
            {
                "sequence_index": _to_int(item.get("sequence_index")) or 0,
                "requested_value": requested_value,
                "value_label": str(item.get("value_label") or "").strip(),
                "value_source": str(item.get("value_source") or "").strip(),
                "requested_at": str(item.get("requested_at") or "").strip(),
                "observation": {
                    "register": -1,
                    "function_code": 0,
                    "values": [],
                    "protocol": protocol,
                    "command": command,
                    "value": str(observation.get("value") or "").strip(),
                    "devcode": _to_int(observation.get("devcode")),
                    "devaddr": _to_int(observation.get("devaddr")),
                    "timestamp": str(observation.get("timestamp") or "").strip(),
                },
            }
        )

    output: list[dict[str, Any]] = []
    for item in grouped.values():
        samples = sorted(item["samples"], key=lambda sample: int(sample.get("sequence_index", 0)))
        if not samples:
            continue
        commands = {
            str(sample.get("observation", {}).get("command") or "").strip().upper()
            for sample in samples
            if str(sample.get("observation", {}).get("command") or "").strip()
        }
        common_command = next(iter(commands)) if len(commands) == 1 else ""
        command_map = _command_map_for_samples(samples, common_command=common_command)
        learned_at = _first_non_empty(
            [str(sample["observation"].get("timestamp") or "") for sample in samples]
        ) or _first_non_empty([str(sample.get("requested_at") or "") for sample in samples])
        sample_payload = {
            "field_id": str(item["field_id"]),
            "field_name": str(item["field_name"]),
            "command": common_command,
            "command_map": command_map,
            "read_key": str(item.get("read_key") or ""),
            "samples": samples,
        }
        output.append(
            {
                "field_id": str(item["field_id"]),
                "field_name": str(item["field_name"]),
                "register": -1,
                "command": common_command,
                "command_map": command_map,
                "read_key": str(item.get("read_key") or ""),
                "samples": samples,
                "learned_at": learned_at or datetime.now(timezone.utc).isoformat(),
                "evidence_hash": deterministic_evidence_hash(sample_payload),
            }
        )
    return sorted(output, key=lambda item: (str(item["field_name"]), str(item["command"])))


def _command_map_for_samples(
    samples: list[dict[str, Any]],
    *,
    common_command: str,
) -> dict[int, str]:
    """Return raw UI value -> full ASCII command when prefix+value is insufficient.

    ValueCloud legacy controls can expose cloud-side option values that do not match the
    inverter command suffix (for example 12336 -> OPR00, 48 -> TBAT0). The learned profile
    must preserve the actual captured command line instead of rebuilding it from the cloud value.
    """

    mapped: dict[int, str] = {}
    needs_map = not common_command
    for sample in samples:
        raw_value = _to_int(sample.get("requested_value"))
        if raw_value is None:
            continue
        observation = sample.get("observation", {})
        if not isinstance(observation, dict):
            continue
        observed = _observed_ascii_command_line(observation)
        if not observed:
            continue
        mapped[raw_value] = observed
        expected = f"{common_command}{raw_value}" if common_command else ""
        if observed != expected:
            needs_map = True
    return mapped if needs_map else {}


def _g_ascii_read_key_for_group(group: dict[str, Any]) -> str:
    read_key = str(group.get("read_key") or "").strip()
    if read_key:
        return read_key
    field_id = str(group.get("field_id") or "").strip()
    definition = G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.get(field_id)
    return definition.read_key if definition is not None else ""


def _normalize_g_ascii_feature_flag_capability(
    *,
    group: dict[str, Any],
    classification: dict[str, Any],
    command_map: dict[int, str],
    read_key: str,
) -> tuple[dict[str, Any], dict[int, str]]:
    """Normalize G-ASCII feature toggles from cloud ASCII codes to bool 0/1.

    ValueCloud exposes some feature flags as option values 68/69 (ASCII D/E),
    while the inverter command lines are ``TDx``/``TEx``. Our runtime bool
    capabilities are intentionally keyed by 0/1, so the generated profile must
    keep 68/69 only as cloud evidence, not as capability values.
    """

    definition = None
    if read_key:
        for candidate in G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.values():
            if candidate.read_key == read_key:
                definition = candidate
                break
    if definition is None:
        definition = G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.get(
            str(group.get("field_id") or "").strip()
        )
    if definition is None or definition.readback_kind != "feature_flag":
        return classification, command_map
    if not command_map:
        return classification, command_map

    normalized_command_map: dict[int, str] = {}
    disabled_label = "Disabled"
    enabled_label = "Enabled"
    enum_map = classification.get("enum_map")
    if isinstance(enum_map, dict):
        for raw_value, label in enum_map.items():
            command = command_map.get(int(raw_value)) if _to_int(raw_value) is not None else ""
            command = str(command or "").strip().upper()
            if command.startswith("TD"):
                disabled_label = str(label)
            elif command.startswith("TE"):
                enabled_label = str(label)

    for command in command_map.values():
        normalized = str(command or "").strip().upper()
        if normalized.startswith("TD"):
            normalized_command_map[0] = normalized
        elif normalized.startswith("TE"):
            normalized_command_map[1] = normalized

    if set(normalized_command_map) != {0, 1}:
        return classification, command_map

    normalized_classification = dict(classification)
    normalized_classification["value_kind"] = "bool"
    normalized_classification["enum_map"] = {0: disabled_label, 1: enabled_label}
    return normalized_classification, normalized_command_map


def _observed_ascii_command_line(observation: dict[str, Any]) -> str:
    command = str(observation.get("command") or "").strip().upper()
    if not command:
        return ""
    value = str(observation.get("value") or "").strip()
    return f"{command}{value}" if value else command


_ON_OFF_OFF_RE = re.compile(r"\boff\b")
_ON_OFF_ON_RE = re.compile(r"\bon\b")


def _looks_like_on_off(labeled_options: list[tuple[int, str]]) -> bool:
    """Whether a 2-option set reads as a disable/enable (off/on) pair -> render as a switch."""

    has_off = has_on = False
    for _value, label in labeled_options:
        lowered = label.lower()
        if "disable" in lowered or _ON_OFF_OFF_RE.search(lowered):
            has_off = True
        if "enable" in lowered or _ON_OFF_ON_RE.search(lowered):
            has_on = True
    return has_off and has_on


def _classify_learned_control(group: dict[str, Any]) -> dict[str, Any]:
    field_name = str(group.get("field_name") or "")
    field_id = str(group.get("field_id") or "")
    normalized_text = _normalize_title(f"{field_name} {field_id}")
    samples = list(group.get("samples") or [])

    requested_values = [str(sample.get("requested_value") or "").strip() for sample in samples]
    unique_requested = _unique_ordered(requested_values)
    requested_ints = [_to_int(value) for value in unique_requested]
    all_requested_int = all(value is not None for value in requested_ints) and bool(requested_ints)
    requested_int_values = [int(value) for value in requested_ints if value is not None]

    max_word_count = 1
    first_observed_value = 1
    for sample in samples:
        values = list(sample.get("observation", {}).get("values") or [])
        if values:
            max_word_count = max(max_word_count, len(values))
            first_observed_value = int(values[0])

    is_destructive = any(keyword in normalized_text for keyword in _DESTRUCTIVE_KEYWORDS)

    # Authoritative path: a SmartESS "choice" field carries discrete labeled options. Map each
    # option's first observed register value (the value the runtime read-back reads, and the one
    # we write back) to its SmartESS label, in sweep order.
    is_choice = any(str(sample.get("value_source") or "") == "choice" for sample in samples)
    option_label_by_value: dict[int, str] = {}
    option_order: list[int] = []
    for sample in samples:
        label = str(sample.get("value_label") or "").strip()
        values = list(sample.get("observation", {}).get("values") or [])
        if not label:
            continue
        if values:
            observed = int(values[0])
        else:
            observed = _to_int(sample.get("requested_value"))
            if observed is None:
                continue
        if observed not in option_label_by_value:
            option_label_by_value[observed] = label
            option_order.append(observed)
    labeled_options = [(value, option_label_by_value[value]) for value in option_order]

    # Authoritative: a single-option choice is a momentary trigger -- a button ("Forced EQ
    # Charging Once", "Exit Fault Mode", "Clear Record"). A DESTRUCTIVE keyword (clear/reset/...)
    # also forces a button as a safety net even without that signal. A non-destructive name
    # keyword like "restart" must NOT: "Auto Restart When Overload" is a 2-option Disable/Enable
    # setting (switch), not a button.
    if is_destructive or (is_choice and len(labeled_options) == 1):
        return {
            "value_kind": "action",
            "word_count": 1,
            "combine": "u16",
            "action_value": labeled_options[0][0] if labeled_options else first_observed_value,
            "advanced": True,
            "requires_confirm": True,
            "unsafe_while_running": is_destructive,
            "safety_class": "destructive_action" if is_destructive else "action",
            "confidence": "high" if labeled_options else ("medium" if len(samples) == 1 else "high"),
        }

    # Labeled multi-option choice: a 2-option off/on pair is a switch; anything else is a SELECT
    # keyed by the register value with the real SmartESS labels (fixes "enums are bare ordinals"
    # and "Output Voltage/Frequency render as numeric fields").
    if len(labeled_options) >= 2:
        enum_map = {value: label for value, label in labeled_options}
        value_kind = (
            "bool"
            if len(labeled_options) == 2 and _looks_like_on_off(labeled_options)
            else "enum"
        )
        return {
            "value_kind": value_kind,
            "word_count": 1,
            "combine": "u16",
            "enum_map": enum_map,
            "advanced": False,
            "requires_confirm": False,
            "unsafe_while_running": False,
            "safety_class": "setting",
            "confidence": "high",
        }

    # --- Fallbacks for label-less evidence (older runs / genuinely numeric fields) ---
    if all_requested_int and set(requested_int_values) == {0, 1}:
        return {
            "value_kind": "bool",
            "word_count": 1,
            "combine": "u16",
            "enum_map": {0: "0", 1: "1"},
            "advanced": False,
            "requires_confirm": False,
            "unsafe_while_running": False,
            "safety_class": "setting",
            "confidence": "high" if len(samples) >= 2 else "medium",
        }

    if all_requested_int and len(requested_int_values) >= 2:
        return {
            "value_kind": "enum",
            "word_count": 1,
            "combine": "u16",
            "enum_map": {
                int(value): str(value)
                for value in requested_int_values
            },
            "advanced": False,
            "requires_confirm": False,
            "unsafe_while_running": False,
            "safety_class": "setting",
            "confidence": "high",
        }

    # Numeric field: derive the display divisor from the observed write. The cloud writes the
    # register in raw units for the displayed value we sent (e.g. wrote 56.0 -> register 560 ->
    # divisor 10), so divisor = observed_register / written_value, validated to a power of ten.
    # No min/max is recorded -- per design the DEVICE validates the value, not the front-end.
    divisor = 1
    for sample in samples:
        try:
            written = float(str(sample.get("requested_value") or "").strip())
        except ValueError:
            continue
        observed = list(sample.get("observation", {}).get("values") or [])
        if not observed or written == 0:
            continue
        ratio = abs(int(observed[0])) / abs(written)
        candidate = int(round(ratio))
        if candidate in (10, 100, 1000) and abs(ratio - candidate) < 0.05:
            divisor = candidate
        break

    if max_word_count == 2:
        value_kind = "u32_high_first"
    elif divisor > 1:
        value_kind = "scaled_u16"
    else:
        value_kind = "u16"
    if max_word_count == 1 and not any(
        list(sample.get("observation", {}).get("values") or []) for sample in samples
    ):
        requested_numeric = [str(value).strip() for value in requested_values if str(value).strip()]
        decimal_places = [
            len(value.split(".", 1)[1])
            for value in requested_numeric
            if "." in value and _looks_numeric(value)
        ]
        if decimal_places:
            divisor = 10 ** min(max(decimal_places), 3)
            value_kind = "scaled_u16"
    classification = {
        "value_kind": value_kind,
        "word_count": 2 if max_word_count == 2 else 1,
        "combine": "u32_high_first" if max_word_count == 2 else "u16",
        "advanced": False,
        "requires_confirm": False,
        "unsafe_while_running": False,
        "safety_class": "setting",
        "confidence": "medium" if len(samples) == 1 else "high",
    }
    if divisor > 1:
        classification["divisor"] = divisor
    # SmartESS shows a unit for numeric settings; derive the common ones from the field name so
    # the number entity carries V/A/Hz/°C/W/% (ambiguous ones like "Time" are left unitless).
    lowered_name = field_name.lower()
    for keyword, unit in (
        ("voltage", "V"),
        ("current", "A"),
        ("frequency", "Hz"),
        ("temperature", "°C"),
        ("power", "W"),
        ("soc", "%"),
        ("capacity", "Ah"),
        ("time", "min"),
    ):
        if keyword in lowered_name:
            classification["unit"] = unit
            break
    return classification


def _looks_numeric(value: str) -> bool:
    try:
        float(str(value))
    except ValueError:
        return False
    return True


def _normalize_read_map(read_map: dict[str, Any] | None) -> dict[str, Any]:
    """Bound and sanitize one session read map for manifest embedding."""

    if not isinstance(read_map, dict):
        return {}
    blocks = []
    for item in read_map.get("read_blocks", []) or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            try:
                address, count = int(item[0]), int(item[1])
            except (TypeError, ValueError):
                continue
            occurrences = int(item[2]) if len(item) > 2 else 0
            blocks.append([address, count, occurrences])
    registers: dict[str, list[int]] = {}
    raw_registers = read_map.get("registers")
    if isinstance(raw_registers, dict):
        for key, samples in raw_registers.items():
            if not isinstance(samples, (list, tuple)):
                continue
            try:
                register = int(key)
            except (TypeError, ValueError):
                continue
            registers[str(register)] = [int(value) for value in samples][:8]
    ascii_commands = []
    for item in read_map.get("ascii_commands", []) or []:
        if isinstance(item, (list, tuple)) and len(item) >= 1:
            command = str(item[0] or "").strip().upper()
            if not command:
                continue
            occurrences = int(item[1]) if len(item) > 1 else 0
            ascii_commands.append([command, occurrences])
    ascii_fields: dict[str, list[str]] = {}
    raw_ascii_fields = read_map.get("ascii_fields")
    if isinstance(raw_ascii_fields, dict):
        for key, samples in raw_ascii_fields.items():
            command = str(key or "").strip().upper()
            if not command or not isinstance(samples, (list, tuple)):
                continue
            ascii_fields[command] = [str(value) for value in samples][:8]

    if not blocks and not registers and not ascii_commands and not ascii_fields:
        return {}
    normalized = {
        "read_blocks": blocks,
        "registers": registers,
        "read_event_count": int(read_map.get("read_event_count", 0) or 0),
        "value_source": str(read_map.get("value_source") or ""),
    }
    if ascii_commands:
        normalized["ascii_commands"] = ascii_commands
    if ascii_fields:
        normalized["ascii_fields"] = ascii_fields
    return normalized


def _safe_matched_count(correlation: dict[str, Any]) -> int:
    matched_count = _to_int(correlation.get("matched_count"))
    if matched_count is not None:
        return matched_count
    matched = correlation.get("matched")
    return len(matched) if isinstance(matched, list) else 0


def _normalize_session_manifest(session_manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": str(session_manifest.get("session_id") or "").strip(),
        "collector_pn": str(session_manifest.get("collector_pn") or "").strip(),
        "cloud_pn": str(session_manifest.get("cloud_pn") or "").strip(),
        "cloud_sn": str(session_manifest.get("cloud_sn") or "").strip(),
        "devcode": _to_int(session_manifest.get("devcode")),
        "devaddr": _to_int(session_manifest.get("devaddr")),
        "write_response_mode": str(session_manifest.get("write_response_mode") or "").strip(),
    }


def _resolve_output_names(
    *,
    source_profile_name: str,
    source_schema_name: str,
    session_manifest: dict[str, Any],
    output_profile_name: str | None,
    output_schema_name: str | None,
) -> tuple[str, str]:
    if output_profile_name and output_schema_name:
        return str(output_profile_name), str(output_schema_name)

    device_token = _slugify(
        session_manifest.get("cloud_sn")
        or session_manifest.get("cloud_pn")
        or session_manifest.get("collector_pn")
        or "device"
    )
    session_token = _slugify(session_manifest.get("session_id") or "session")
    source_profile_stem = Path(source_profile_name).stem
    source_schema_stem = Path(source_schema_name).stem
    profile_name = output_profile_name or (
        f"learned/shadow_learning/{device_token}/{source_profile_stem}_{session_token}.json"
    )
    schema_name = output_schema_name or (
        f"learned/shadow_learning/{device_token}/{source_schema_stem}_{session_token}.json"
    )
    return str(profile_name), str(schema_name)


def _builtin_schema_ref(source_schema_name: str) -> str:
    normalized = str(source_schema_name or "").strip()
    if normalized.startswith("builtin:"):
        return normalized
    return f"builtin:{normalized}"


def _append_suffix(text: str, suffix: str) -> str:
    return text if text.endswith(suffix) else f"{text}{suffix}"


def _capability_key(*, field_id: str, field_name: str, register: int) -> str:
    base = _slugify(field_id or field_name)
    if not base.startswith("learned_"):
        base = f"learned_{base}"
    return f"{base}_{int(register)}"


def _command_capability_key(*, field_id: str, field_name: str, command: str) -> str:
    base = _slugify(field_id or field_name)
    if not base.startswith("learned_"):
        base = f"learned_{base}"
    command_key = _slugify(command) or "command"
    return f"{base}_{command_key}"


def _unique_capability_key(base_key: str, existing_keys: set[str]) -> str:
    if base_key not in existing_keys:
        return base_key
    index = 2
    while f"{base_key}_{index}" in existing_keys:
        index += 1
    return f"{base_key}_{index}"



def _normalize_title(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").split())


def _first_non_empty(values: list[str]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _unique_ordered(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        output.append(text)
        seen.add(text)
    return output


__all__ = [
    "ShadowLearningOverlayDraftResult",
    "generate_shadow_learning_overlay_drafts",
]
