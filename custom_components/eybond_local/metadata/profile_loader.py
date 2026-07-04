"""Load declarative driver profile metadata from JSON files."""

from __future__ import annotations

from copy import deepcopy
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from ..models import (
    CapabilityChoice,
    CapabilityCondition,
    CapabilityGroup,
    CapabilityPreset,
    CapabilityPresetItem,
    CapabilityRecommendation,
    WriteCapability,
)

PROFILES_DIR = Path(__file__).resolve().parents[1] / "protocol_catalogs" / "profiles"
_EXTERNAL_PROFILE_ROOTS: tuple[Path, ...] = ()
_BUILTIN_PROFILE_ALIASES = {
    "smg_modbus.json": "modbus_smg/default.json",
}
_ALLOWED_CAPABILITY_PROVENANCE: frozenset[str] = frozenset(
    {"verified", "doc_backed", "inferred", "cloud_hint"}
)


@dataclass(frozen=True, slots=True)
class DriverProfileMetadata:
    """Declarative capability metadata loaded from a JSON profile."""

    key: str
    title: str
    driver_key: str
    protocol_family: str
    source_name: str
    source_path: str
    source_scope: str
    groups: tuple[CapabilityGroup, ...]
    capabilities: tuple[WriteCapability, ...]
    presets: tuple[CapabilityPreset, ...]

    def get_capability(self, capability_key: str) -> WriteCapability:
        """Return one capability from the loaded profile."""

        for capability in self.capabilities:
            if capability.key == capability_key:
                return capability
        raise KeyError(capability_key)

    def enum_map_for(self, capability_key: str) -> dict[int, str]:
        """Return the effective enum map for one capability."""

        return self.get_capability(capability_key).enum_value_map


def canonical_driver_profile_name(profile_name: str) -> str:
    """Return the canonical built-in profile name for persisted metadata."""

    normalized = str(profile_name or "").strip()
    return _BUILTIN_PROFILE_ALIASES.get(normalized, normalized)


@lru_cache(maxsize=None)
def load_driver_profile(profile_name: str) -> DriverProfileMetadata:
    """Load one declarative driver profile from the profiles directory."""

    profile_path = _resolve_profile_path(profile_name)
    raw = _load_raw_profile(profile_path)
    profile_key = str(raw.get("profile_key", profile_path.stem))
    driver_key = str(raw.get("driver_key", profile_key))
    protocol_family = str(raw.get("protocol_family", driver_key))
    capability_defaults = _parse_capability_defaults(raw.get("capability_defaults", {}))
    capability_templates = _parse_capability_template_map(raw.get("capability_templates", {}))

    named_conditions = {
        key: _parse_condition(value)
        for key, value in raw.get("conditions", {}).items()
    }

    metadata = DriverProfileMetadata(
        key=profile_key,
        title=str(raw.get("title", profile_path.stem)),
        driver_key=driver_key,
        protocol_family=protocol_family,
        source_name=str(profile_name),
        source_path=str(profile_path),
        source_scope=_profile_source_scope(profile_path),
        groups=tuple(_parse_group(item) for item in raw.get("groups", [])),
        capabilities=tuple(
            _parse_capability(item, named_conditions, capability_defaults, capability_templates)
            for item in raw.get("capabilities", [])
        ),
        presets=tuple(
            _parse_preset(item, named_conditions)
            for item in raw.get("presets", [])
        ),
    )
    _validate_profile(metadata)
    return metadata


def load_driver_profile_raw(profile_name: str) -> dict[str, Any]:
    """Load one declarative driver profile as fully resolved raw JSON data."""

    profile_path = _resolve_profile_path(profile_name)
    return deepcopy(_load_raw_profile(profile_path))


@lru_cache(maxsize=None)
def _load_raw_profile(profile_path: Path) -> dict[str, Any]:
    raw = json.loads(profile_path.read_text(encoding="utf-8"))
    parent_ref = raw.pop("extends", None)
    if not parent_ref:
        return raw

    parent_ref_str = str(parent_ref)
    if parent_ref_str.startswith(("./", "../")):
        parent_path = _resolve_relative_parent_profile_path(profile_path, parent_ref_str)
    else:
        parent_path = _resolve_profile_path(parent_ref_str)
    parent_raw = _load_raw_profile(parent_path)
    return _merge_raw_profile(parent_raw, raw)


def profile_file_names() -> tuple[str, ...]:
    """Return the available declarative profile file names."""

    names: set[str] = set()
    for root in _profile_search_dirs():
        if not root.exists():
            continue
        names.update(path.name for path in root.glob("*.json"))
    return tuple(sorted(names))


def set_external_profile_roots(roots: tuple[Path, ...] | list[Path]) -> None:
    """Configure additional search roots for declarative driver profiles."""

    global _EXTERNAL_PROFILE_ROOTS
    normalized = tuple(Path(root).resolve() for root in roots if root)
    if normalized == _EXTERNAL_PROFILE_ROOTS:
        return
    _EXTERNAL_PROFILE_ROOTS = normalized
    load_driver_profile.cache_clear()
    _load_raw_profile.cache_clear()
    builtin_base_profile_name.cache_clear()


def clear_profile_loader_cache() -> None:
    """Clear cached declarative profile metadata."""

    load_driver_profile.cache_clear()
    _load_raw_profile.cache_clear()
    builtin_base_profile_name.cache_clear()


def _profile_search_dirs() -> tuple[Path, ...]:
    return (*_EXTERNAL_PROFILE_ROOTS, PROFILES_DIR)


def _resolve_profile_path(profile_name: str) -> Path:
    profile_path = Path(profile_name)
    if profile_path.is_absolute():
        return profile_path

    for root in _profile_search_dirs():
        candidate = (root / profile_name).resolve()
        if not _is_within_root(candidate, root):
            continue
        if candidate.is_file():
            return candidate
    alias = _BUILTIN_PROFILE_ALIASES.get(str(profile_name))
    if alias:
        return (PROFILES_DIR / alias).resolve()
    raise FileNotFoundError(f"profile_not_found:{profile_name}")


def builtin_profile_path(profile_name: str) -> Path:
    """Return the built-in profile path, bypassing external overrides."""

    resolved_name = _BUILTIN_PROFILE_ALIASES.get(profile_name, profile_name)
    return (PROFILES_DIR / resolved_name).resolve()


@lru_cache(maxsize=None)
def builtin_base_profile_name(profile_name: str) -> str:
    """Return the built-in base profile name underlying ``profile_name``.

    Local overlays (e.g. activated shadow-learning drafts) live in an external
    root and ``extends`` a built-in profile. Tooling that derives a new draft must
    start from that built-in base, not the overlay -- otherwise output names and
    parent references accumulate the overlay's own session token. Walk the
    ``extends``/``draft_of`` chain until the name resolves to a built-in profile.

    Cached for the same reason as builtin_base_schema_name: it is reached from
    metadata resolution on every coordinator refresh and must not walk the
    extends chain on disk in the event loop each time. Cleared on root changes.
    """

    current = str(profile_name or "").strip()
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        try:
            profile_path = _resolve_profile_path(current)
        except FileNotFoundError:
            break
        if _profile_source_scope(profile_path) != "external":
            return current
        raw = json.loads(profile_path.read_text(encoding="utf-8"))
        parent = str(raw.get("extends") or raw.get("draft_of") or "").strip()
        if not parent:
            break
        current = parent
    return current


def _resolve_relative_parent_profile_path(profile_path: Path, parent_ref: str) -> Path:
    candidate = (profile_path.parent / parent_ref).resolve()
    if candidate.is_file():
        return candidate

    resolved_profile_path = profile_path.resolve()
    for root in _EXTERNAL_PROFILE_ROOTS:
        resolved_root = root.resolve()
        if not _is_within_root(resolved_profile_path, resolved_root):
            continue
        relative_profile_path = resolved_profile_path.relative_to(resolved_root)
        builtin_profile = (PROFILES_DIR / relative_profile_path).resolve()
        if not _is_within_root(builtin_profile, PROFILES_DIR):
            continue
        builtin_candidate = (builtin_profile.parent / parent_ref).resolve()
        if _is_within_root(builtin_candidate, PROFILES_DIR) and builtin_candidate.is_file():
            return builtin_candidate

    return candidate


def _profile_source_scope(profile_path: Path) -> str:
    resolved = profile_path.resolve()
    if _is_within_root(resolved, PROFILES_DIR):
        return "builtin"
    for root in _EXTERNAL_PROFILE_ROOTS:
        if _is_within_root(resolved, root):
            return "external"
    return "absolute"


def _parse_group(raw: Mapping[str, Any]) -> CapabilityGroup:
    return CapabilityGroup(
        key=str(raw["key"]),
        title=str(raw["title"]),
        order=int(raw.get("order", 1000)),
        description=str(raw.get("description", "")),
        icon=_optional_str(raw.get("icon")),
        advanced=bool(raw.get("advanced", False)),
    )


def _parse_capability(
    raw: Mapping[str, Any],
    named_conditions: Mapping[str, CapabilityCondition],
    capability_defaults: Mapping[str, Any],
    capability_templates: Mapping[str, Mapping[str, Any]],
) -> WriteCapability:
    resolved_raw = _resolve_capability_raw(raw, capability_defaults, capability_templates)
    learned_provenance = resolved_raw.get("learned_provenance")
    learned_scope = ""
    if isinstance(learned_provenance, Mapping):
        learned_scope = str(learned_provenance.get("scope") or "").strip()
    metadata_scope = str(resolved_raw.get("metadata_scope") or learned_scope or "").strip()
    experimental = bool(resolved_raw.get("experimental", False) or bool(learned_scope))
    choices = tuple(_parse_choice(item) for item in raw.get("choices", []))
    if not choices:
        choices = tuple(_parse_choice(item) for item in resolved_raw.get("choices", []))
    enum_map = _parse_enum_map(resolved_raw.get("enum_map"))
    command_map = _parse_command_map(resolved_raw.get("command_map"))
    tested = bool(resolved_raw.get("tested", False))
    provenance = _parse_capability_provenance(resolved_raw.get("provenance"), tested=tested)
    return WriteCapability(
        key=str(resolved_raw["key"]),
        register=int(resolved_raw.get("register", -1)),
        value_kind=str(resolved_raw["value_kind"]),
        note=str(resolved_raw.get("note", "")),
        command=str(resolved_raw.get("command", "")),
        command_map=command_map,
        word_count=int(resolved_raw.get("word_count", 1)),
        combine=str(resolved_raw.get("combine", "u16")),
        bitmask=_optional_bitmask(
            resolved_raw.get("bitmask"),
            capability_key=str(resolved_raw["key"]),
            word_count=int(resolved_raw.get("word_count", 1)),
        ),
        tested=tested,
        provenance=provenance,
        support_tier=str(resolved_raw.get("support_tier", "")),
        support_notes=str(resolved_raw.get("support_notes", "")),
        action_value=_optional_int(resolved_raw.get("action_value")),
        divisor=_optional_int(resolved_raw.get("divisor")),
        minimum=_optional_int(resolved_raw.get("minimum")),
        maximum=_optional_int(resolved_raw.get("maximum")),
        command_width=_optional_int(resolved_raw.get("command_width")),
        command_precision=_optional_int(resolved_raw.get("command_precision")),
        enum_map=enum_map,
        choices=choices,
        recommendations=tuple(
            _parse_recommendation(item, named_conditions)
            for item in resolved_raw.get("recommendations", [])
        ),
        title=str(resolved_raw.get("title", "")),
        group=str(resolved_raw.get("group", "config")),
        order=int(resolved_raw.get("order", 1000)),
        unit=_optional_str(resolved_raw.get("unit")),
        device_class=_optional_str(resolved_raw.get("device_class")),
        step=_optional_float(resolved_raw.get("step")),
        enabled_default=bool(resolved_raw.get("enabled_default", False)),
        advanced=bool(resolved_raw.get("advanced", False)),
        requires_confirm=bool(resolved_raw.get("requires_confirm", False)),
        reboot_required=bool(resolved_raw.get("reboot_required", False)),
        read_key=str(resolved_raw.get("read_key", "")),
        depends_on=tuple(str(item) for item in resolved_raw.get("depends_on", [])),
        affects=tuple(str(item) for item in resolved_raw.get("affects", [])),
        exclusive_with=tuple(str(item) for item in resolved_raw.get("exclusive_with", [])),
        change_summary=str(resolved_raw.get("change_summary", "")),
        unsafe_while_running=bool(resolved_raw.get("unsafe_while_running", False)),
        safe_operating_modes=tuple(
            str(item)
            for item in resolved_raw.get("safe_operating_modes", ("Power On", "Standby", "Fault"))
        ),
        visible_if=_resolve_conditions(resolved_raw.get("visible_if", []), named_conditions),
        editable_if=_resolve_conditions(resolved_raw.get("editable_if", []), named_conditions),
        experimental=experimental,
        metadata_scope=metadata_scope,
        write_function=_optional_int(resolved_raw.get("write_function")),
    )


def _resolve_capability_raw(
    raw: Mapping[str, Any],
    capability_defaults: Mapping[str, Any],
    capability_templates: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    template_name = _optional_str(raw.get("template"))
    if not template_name:
        return {**capability_defaults, **raw}

    template_raw = capability_templates.get(template_name)
    if template_raw is None:
        raise ValueError(f"unknown_capability_template:{template_name}")

    resolved = {**capability_defaults, **template_raw, **raw}
    resolved.setdefault("key", template_name)
    return resolved


def _parse_capability_defaults(raw_defaults: Any) -> dict[str, Any]:
    if not raw_defaults:
        return {}
    if not isinstance(raw_defaults, Mapping):
        raise ValueError("invalid_capability_defaults")
    return {str(key): value for key, value in raw_defaults.items()}


def _parse_capability_template_map(raw_templates: Any) -> dict[str, dict[str, Any]]:
    if not raw_templates:
        return {}
    if not isinstance(raw_templates, Mapping):
        raise ValueError("invalid_capability_templates")

    templates: dict[str, dict[str, Any]] = {}
    for template_name, raw in raw_templates.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"invalid_capability_template:{template_name}")
        template = dict(raw)
        template.setdefault("key", str(template_name))
        templates[str(template_name)] = template
    return templates


def _parse_choice(raw: Mapping[str, Any]) -> CapabilityChoice:
    return CapabilityChoice(
        value=int(raw["value"]),
        label=str(raw["label"]),
        description=str(raw.get("description", "")),
        order=int(raw.get("order", 1000)),
        advanced=bool(raw.get("advanced", False)),
    )


def _parse_command_map(raw: Any) -> dict[int, str] | None:
    if not isinstance(raw, Mapping):
        return None
    parsed: dict[int, str] = {}
    for key, value in raw.items():
        command = str(value or "").strip().upper()
        if not command:
            continue
        parsed[int(key)] = command
    return parsed or None


def _parse_recommendation(
    raw: Mapping[str, Any],
    named_conditions: Mapping[str, CapabilityCondition],
) -> CapabilityRecommendation:
    return CapabilityRecommendation(
        value=raw["value"],
        reason=str(raw["reason"]),
        conditions=_resolve_conditions(raw.get("conditions", []), named_conditions),
        label=str(raw.get("label", "")),
        priority=int(raw.get("priority", 1000)),
    )


def _parse_preset(
    raw: Mapping[str, Any],
    named_conditions: Mapping[str, CapabilityCondition],
) -> CapabilityPreset:
    return CapabilityPreset(
        key=str(raw["key"]),
        title=str(raw["title"]),
        description=str(raw.get("description", "")),
        items=tuple(_parse_preset_item(item) for item in raw.get("items", [])),
        conditions=_resolve_conditions(raw.get("conditions", []), named_conditions),
        group=str(raw.get("group", "recommended")),
        order=int(raw.get("order", 1000)),
        icon=_optional_str(raw.get("icon")),
        advanced=bool(raw.get("advanced", False)),
        requires_confirm=bool(raw.get("requires_confirm", True)),
    )


def _parse_preset_item(raw: Mapping[str, Any]) -> CapabilityPresetItem:
    return CapabilityPresetItem(
        capability_key=str(raw["capability_key"]),
        value=raw["value"],
        reason=str(raw.get("reason", "")),
        order=int(raw.get("order", 1000)),
    )


def _resolve_conditions(
    raw_items: list[Any],
    named_conditions: Mapping[str, CapabilityCondition],
) -> tuple[CapabilityCondition, ...]:
    resolved: list[CapabilityCondition] = []
    for raw in raw_items:
        if isinstance(raw, str):
            if raw not in named_conditions:
                raise KeyError(f"unknown_condition:{raw}")
            resolved.append(named_conditions[raw])
            continue
        if isinstance(raw, Mapping):
            resolved.append(_parse_condition(raw))
            continue
        raise ValueError(f"unsupported_condition_reference:{raw!r}")
    return tuple(resolved)


def _parse_condition(raw: Mapping[str, Any]) -> CapabilityCondition:
    return CapabilityCondition(
        key=str(raw["key"]),
        operator=str(raw.get("operator", "eq")),
        value=raw.get("value", True),
        reason=str(raw.get("reason", "")),
        effect=str(raw.get("effect", "warning")),
    )


def _parse_enum_map(raw: Any) -> dict[int, str] | None:
    if not isinstance(raw, Mapping):
        return None
    return {int(key): str(value) for key, value in raw.items()}


def _parse_capability_provenance(value: Any, *, tested: bool) -> str:
    if value is None or value == "":
        return "verified" if tested else "inferred"

    parsed = str(value).strip().lower()
    if parsed not in _ALLOWED_CAPABILITY_PROVENANCE:
        raise ValueError(f"invalid_capability_provenance:{value}")
    return parsed


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _optional_bitmask(value: Any, *, capability_key: str, word_count: int) -> int | None:
    """Parse one capability bitmask (int, or hex string like "0x0001")."""

    if value is None or value == "":
        return None
    mask = int(value.strip(), 0) if isinstance(value, str) else int(value)
    if not 1 <= mask <= 0xFFFF:
        raise ValueError(f"invalid_capability_bitmask:{capability_key}:{value}")
    if word_count != 1:
        raise ValueError(f"bitmask_requires_single_word:{capability_key}")
    return mask


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


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


def _merge_raw_profile(
    base: Mapping[str, Any],
    overlay: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(base)
    merged["capability_defaults"] = {
        **base.get("capability_defaults", {}),
        **overlay.get("capability_defaults", {}),
    }
    merged["capability_templates"] = _merge_named_mapping(
        base.get("capability_templates", {}),
        overlay.get("capability_templates", {}),
    )
    merged["groups"] = _merge_keyed_list(
        base.get("groups", []),
        overlay.get("groups", []),
        key_field="key",
    )
    merged["capabilities"] = _merge_keyed_list(
        base.get("capabilities", []),
        overlay.get("capabilities", []),
        key_field="key",
    )
    merged["presets"] = _merge_keyed_list(
        base.get("presets", []),
        overlay.get("presets", []),
        key_field="key",
    )
    merged["conditions"] = {
        **base.get("conditions", {}),
        **overlay.get("conditions", {}),
    }

    for key, value in overlay.items():
        if key in {
            "capability_defaults",
            "capability_templates",
            "groups",
            "capabilities",
            "presets",
            "conditions",
        }:
            continue
        merged[key] = value
    return merged


def _merge_keyed_list(
    base: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    overlay: list[dict[str, Any]] | tuple[dict[str, Any], ...],
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


def _merge_named_mapping(
    base: Mapping[str, Any],
    overlay: Mapping[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        str(key): (dict(value) if isinstance(value, Mapping) else value)
        for key, value in base.items()
    }
    for key, value in overlay.items():
        mapped_key = str(key)
        if mapped_key in result and isinstance(result[mapped_key], Mapping) and isinstance(value, Mapping):
            result[mapped_key] = {**result[mapped_key], **value}
            continue
        result[mapped_key] = dict(value) if isinstance(value, Mapping) else value
    return result


def _validate_profile(profile: DriverProfileMetadata) -> None:
    """Validate a loaded declarative profile and fail early on schema issues."""

    group_keys = _unique_or_raise(
        items=(group.key for group in profile.groups),
        kind="group",
        profile_key=profile.key,
    )
    capability_keys = _unique_or_raise(
        items=(capability.key for capability in profile.capabilities),
        kind="capability",
        profile_key=profile.key,
    )
    _unique_or_raise(
        items=(preset.key for preset in profile.presets),
        kind="preset",
        profile_key=profile.key,
    )

    allowed_support_tiers = {"standard", "conditional", "blocked"}

    for capability in profile.capabilities:
        if capability.group not in group_keys:
            raise ValueError(
                f"profile:{profile.key}:unknown_group_for_capability:"
                f"{capability.key}:{capability.group}"
            )
        if capability.register < 0 and not capability.command and not capability.command_map:
            raise ValueError(
                f"profile:{profile.key}:capability_requires_register_or_command:{capability.key}"
            )
        if capability.word_count < 1:
            raise ValueError(
                f"profile:{profile.key}:capability_requires_positive_word_count:{capability.key}"
            )
        if capability.value_kind == "enum" and not capability.enum_value_map:
            raise ValueError(
                f"profile:{profile.key}:enum_capability_without_choices:{capability.key}"
            )
        if capability.value_kind == "action" and capability.action_value is None:
            raise ValueError(
                f"profile:{profile.key}:action_capability_requires_action_value:{capability.key}"
            )
        if capability.value_kind not in {"enum", "bool"} and capability.enum_value_map:
            raise ValueError(
                f"profile:{profile.key}:non_enum_capability_has_enum_map:{capability.key}"
            )
        if capability.value_kind == "bool":
            raw_values = set(capability.enum_value_map) if capability.enum_value_map else {0, 1}
            if raw_values != {0, 1}:
                raise ValueError(
                    f"profile:{profile.key}:bool_capability_requires_0_1:{capability.key}"
                )
        if capability.support_tier and capability.support_tier not in allowed_support_tiers:
            raise ValueError(
                f"profile:{profile.key}:unsupported_support_tier:"
                f"{capability.key}:{capability.support_tier}"
            )
        if capability.support_tier == "blocked" and capability.tested:
            raise ValueError(
                f"profile:{profile.key}:blocked_capability_cannot_be_tested:{capability.key}"
            )
        _unique_or_raise(
            items=(str(choice.value) for choice in capability.enum_choices),
            kind=f"choice_value:{capability.key}",
            profile_key=profile.key,
        )
        for recommendation in capability.recommendations:
            if capability.value_kind in {"enum", "bool"} and recommendation.value not in capability.enum_value_map:
                raise ValueError(
                    f"profile:{profile.key}:unknown_recommendation_enum_value:"
                    f"{capability.key}:{recommendation.value}"
                )

    for preset in profile.presets:
        for item in preset.items:
            if item.capability_key not in capability_keys:
                raise ValueError(
                    f"profile:{profile.key}:preset_references_unknown_capability:"
                    f"{preset.key}:{item.capability_key}"
                )


def _unique_or_raise(
    *,
    items,
    kind: str,
    profile_key: str,
) -> set[str]:
    """Return a set of values and raise if duplicates are present."""

    seen: set[str] = set()
    for item in items:
        if item in seen:
            raise ValueError(f"profile:{profile_key}:duplicate_{kind}:{item}")
        seen.add(item)
    return seen
