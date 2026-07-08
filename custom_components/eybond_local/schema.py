"""Helpers that build a runtime UI schema from inverter capability metadata."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .control_policy import (
    CONTROL_MODE_READ_ONLY,
    can_expose_capability,
    can_expose_preset,
    controls_reason,
)
from .models import CapabilityPreset, CapabilityPresetItem, DetectedInverter, WriteCapability

UI_SCHEMA_VERSION = 4


def capability_write_exposure_allowed(
    capability: WriteCapability,
    *,
    control_mode: str,
    detection_confidence: str,
    variant_key: str = "",
    profile_source_scope: str = "",
    schema_source_scope: str = "",
    profile_name: str = "",
    device_scoped_overlay_active: bool = False,
    selected_control_keys: frozenset[str] | None = None,
) -> bool:
    """Return whether runtime should expose one capability as writable."""

    overlay_exposes_learned = device_scoped_overlay_active and _is_device_scoped_learned_capability(
        capability
    )
    # A device-scoped learned control is exposed only when the user selected it. When the
    # activation declares a selection (``selected_control_keys`` is a set), only those keys
    # pass and an empty selection exposes none; ``None`` means a legacy activation with no
    # selection, which keeps the prior behavior of exposing every learned control.
    if (
        overlay_exposes_learned
        and selected_control_keys is not None
        and capability.key not in selected_control_keys
    ):
        return False
    if _is_family_fallback_variant(variant_key=variant_key, profile_name=profile_name) and not overlay_exposes_learned:
        return False
    if not capability.allows_runtime_write_without_local_proof and not overlay_exposes_learned:
        return False
    if (
        capability.provenance == "verified"
        and not device_scoped_overlay_active
        and not _has_confirmed_local_metadata_proof(
            profile_source_scope=profile_source_scope,
            schema_source_scope=schema_source_scope,
        )
    ):
        # ``_has_confirmed_local_metadata_proof`` requires a built-in profile + schema. When
        # a device-scoped overlay is active the effective metadata is "external", so this
        # would suppress every verified built-in control and the whole inverter's settings
        # would go unavailable the moment one learned control is activated. A device-scoped
        # overlay *extends* the proven built-in base, so its built-in verified controls
        # remain proven -- treat an active overlay as satisfying the proof.
        return False
    if overlay_exposes_learned:
        # A device-scoped learned control the user explicitly discovered, selected, and
        # activated is exposed under auto/full regardless of the base control-mode gate.
        # That gate withholds *untested* controls under "auto" (learned controls are
        # observed from cloud traffic, never write-tested, so ``capability.tested`` is
        # False) -- but the activation itself is the deliberate per-control opt-in that
        # gate is meant to require, so demanding control_mode="full" on top would hide
        # the very control the user just added. Read-only still suppresses all writes.
        return control_mode != CONTROL_MODE_READ_ONLY
    return can_expose_capability(
        capability,
        control_mode=control_mode,
        detection_confidence=detection_confidence,
    )


def preset_write_exposure_allowed(
    preset: CapabilityPreset,
    *,
    capabilities_by_key: dict[str, WriteCapability],
    control_mode: str,
    detection_confidence: str,
    variant_key: str = "",
    profile_source_scope: str = "",
    schema_source_scope: str = "",
    profile_name: str = "",
    device_scoped_overlay_active: bool = False,
    selected_control_keys: frozenset[str] | None = None,
) -> bool:
    """Return whether runtime should expose one preset as writable."""

    if _is_family_fallback_variant(variant_key=variant_key, profile_name=profile_name) and not device_scoped_overlay_active:
        return False
    if not can_expose_preset(
        preset,
        capabilities_by_key=capabilities_by_key,
        control_mode=control_mode,
        detection_confidence=detection_confidence,
    ):
        return False
    for item in preset.items:
        capability = capabilities_by_key.get(item.capability_key)
        if capability is None:
            return False
        if not capability_write_exposure_allowed(
            capability,
            control_mode=control_mode,
            detection_confidence=detection_confidence,
            variant_key=variant_key,
            profile_source_scope=profile_source_scope,
            schema_source_scope=schema_source_scope,
            profile_name=profile_name,
            device_scoped_overlay_active=device_scoped_overlay_active,
            selected_control_keys=selected_control_keys,
        ):
            return False
    return True


def build_runtime_ui_schema(
    inverter: DetectedInverter,
    values: Mapping[str, Any],
) -> dict[str, object]:
    """Build a grouped, runtime-aware UI schema for the detected inverter."""

    by_group: dict[str, list[dict[str, object]]] = {}

    for capability in inverter.capabilities:
        capability_payload = serialize_capability(capability, inverter, values)
        by_group.setdefault(capability.group, []).append(capability_payload)

    groups: list[dict[str, object]] = []
    for group in inverter.capability_groups:
        serialized_capabilities = sorted(
            by_group.pop(group.key, []),
            key=lambda item: (int(item["order"]), str(item["title"])),
        )
        groups.append(
            _serialize_group(
                key=group.key,
                title=group.title,
                order=group.order,
                description=group.description,
                icon=group.icon,
                advanced=group.advanced,
                capabilities=serialized_capabilities,
                values=values,
            )
        )

    for group_key, capabilities in sorted(by_group.items()):
        serialized_capabilities = sorted(
            capabilities,
            key=lambda item: (int(item["order"]), str(item["title"])),
        )
        groups.append(
            _serialize_group(
                key=group_key,
                title=group_key.replace("_", " ").title(),
                order=9999,
                description="",
                icon=None,
                advanced=False,
                capabilities=serialized_capabilities,
                values=values,
            )
        )

    return {
        "version": UI_SCHEMA_VERSION,
        "driver_key": inverter.driver_key,
        "protocol_family": inverter.protocol_family,
        "model_name": inverter.model_name,
        "serial_number": inverter.serial_number,
        "overview": _build_overview(values),
        "access": {
            "detection_confidence": values.get("detection_confidence"),
            "control_mode": values.get("control_mode"),
            "controls_enabled": values.get("controls_enabled"),
            "control_policy_reason": values.get("control_policy_reason"),
            "blocked_write_count": values.get("blocked_write_count", 0),
            "blocked_write_summary": values.get("blocked_write_summary"),
        },
        "groups": groups,
        "presets": [
            serialize_preset(preset, inverter, values)
            for preset in sorted(
                inverter.capability_presets,
                key=lambda preset: (preset.group, preset.order, preset.title),
            )
        ],
    }


def _build_overview(values: Mapping[str, Any]) -> dict[str, object]:
    """Build a compact top-level overview for future dashboards/custom UIs."""

    return {
        "site_mode": values.get("site_mode_state"),
        "power_flow": values.get("power_flow_summary"),
        "system_status": values.get("operational_state"),
        "protection_status": values.get("protection_state"),
        "alarm_context": values.get("alarm_context_state"),
        "roles": {
            "load": values.get("load_supply_state"),
            "battery": values.get("battery_role_state"),
            "pv": values.get("pv_role_state"),
            "utility": values.get("utility_role_state"),
        },
        "warnings_active": bool(values.get("warning_active")),
        "fault_active": bool(values.get("fault_active")),
    }


def _serialize_group(
    *,
    key: str,
    title: str,
    order: int,
    description: str,
    icon: str | None,
    advanced: bool,
    capabilities: list[dict[str, object]],
    values: Mapping[str, Any],
) -> dict[str, object]:
    """Serialize one capability group together with runtime summary metadata."""

    status_counts = _group_status_counts(capabilities)
    reasons = _dedupe_strings(
        reason
        for capability in capabilities
        for reason in capability.get("reasons", [])
    )
    warnings = _dedupe_strings(
        warning
        for capability in capabilities
        for warning in capability.get("warnings", [])
    )
    return {
        "key": key,
        "title": title,
        "order": order,
        "description": description,
        "icon": icon,
        "advanced": advanced,
        "capabilities": capabilities,
        "status": _group_status(status_counts),
        "status_counts": status_counts,
        "summary": _group_summary(key, status_counts, values),
        "reasons": reasons,
        "warnings": warnings,
    }


def serialize_capability(
    capability: WriteCapability,
    inverter: DetectedInverter,
    values: Mapping[str, Any],
) -> dict[str, object]:
    """Serialize one capability with its current runtime state."""

    runtime_state = capability.runtime_state(values)
    policy_active = _policy_is_active(values)
    policy_allowed = True
    if policy_active:
        policy_allowed = capability_write_exposure_allowed(
            capability,
            control_mode=str(values.get("control_mode") or ""),
            detection_confidence=str(values.get("detection_confidence") or ""),
            variant_key=str(values.get("effective_variant_key") or ""),
            profile_source_scope=str(values.get("effective_profile_source_scope") or ""),
            schema_source_scope=str(values.get("effective_schema_source_scope") or ""),
            profile_name=str(values.get("effective_profile_name") or ""),
            device_scoped_overlay_active=bool(values.get("effective_device_scoped_overlay_active")),
            selected_control_keys=_selected_control_keys_from_values(values),
        )
    effective_editable = runtime_state.editable and policy_allowed
    entity_kind = entity_kind_for_capability(capability)
    blocked_reason = values.get(f"capability_block_reason_{capability.key}")
    blocked_code = values.get(f"capability_block_code_{capability.key}")
    blocked_action = values.get(f"capability_block_action_{capability.key}")
    blocked_exception_code = values.get(f"capability_block_exception_{capability.key}")
    reasons = list(runtime_state.reasons)
    if policy_active and runtime_state.visible and not policy_allowed:
        reasons.append(f"Control policy blocks editing: {controls_reason(control_mode=str(values.get('control_mode') or ''), detection_confidence=str(values.get('detection_confidence') or ''))}.")
    status = "hidden"
    if runtime_state.visible:
        status = "blocked" if blocked_reason else ("editable" if effective_editable else "read_only")
    return {
        "key": capability.key,
        "register": capability.register,
        "command": capability.command or None,
        "command_map": capability.command_map or None,
        "status": status,
        "entity_kind": entity_kind,
        "value_kind": capability.value_kind,
        "title": capability.display_name,
        "group": capability.group,
        "order": capability.order,
        "current_value": values.get(capability.value_key),
        "minimum": capability.native_minimum if entity_kind == "number" else None,
        "maximum": capability.native_maximum if entity_kind == "number" else None,
        "step": capability.native_step if entity_kind == "number" else None,
        "unit": capability.unit,
        "device_class": capability.device_class,
        "enum_options": capability.enum_value_map or None,
        "choices": [
            {
                "value": choice.value,
                "label": choice.label,
                "description": choice.description,
                "order": choice.order,
                "advanced": choice.advanced,
            }
            for choice in capability.enum_choices
        ],
        "enabled_default": capability.enabled_default,
        "advanced": capability.advanced,
        "requires_confirm": capability.requires_confirm,
        "reboot_required": capability.reboot_required,
        "depends_on": list(capability.depends_on),
        "depends_on_details": _serialize_related_capabilities(inverter, capability.depends_on, values),
        "affects": list(capability.affects),
        "affects_details": _serialize_related_capabilities(inverter, capability.affects, values),
        "exclusive_with": list(capability.exclusive_with),
        "exclusive_with_details": _serialize_related_capabilities(
            inverter,
            capability.exclusive_with,
            values,
        ),
        "change_summary": capability.change_summary,
        "unsafe_while_running": capability.unsafe_while_running,
        "tested": capability.tested,
        "validation_state": capability.validation_state,
        "support_tier": capability.resolved_support_tier,
        "support_notes": capability.support_notes,
        "support": {
            "validation_state": capability.validation_state,
            "tier": capability.resolved_support_tier,
            "notes": capability.support_notes,
        },
        "experimental": capability.experimental,
        "metadata_scope": capability.metadata_scope,
        "device_scoped": capability.is_device_scoped_experimental,
        "blocked_reason": blocked_reason,
        "blocked_code": blocked_code,
        "blocked_suggested_action": blocked_action,
        "blocked_exception_code": blocked_exception_code,
        "blocked_by_inverter": bool(blocked_reason),
        "blocked": {
            "active": bool(blocked_reason),
            "code": blocked_code,
            "reason": blocked_reason,
            "suggested_action": blocked_action,
            "exception_code": blocked_exception_code,
        },
        "visible": runtime_state.visible,
        "editable": effective_editable,
        "policy_editable": policy_allowed,
        "reasons": reasons,
        "warnings": list(runtime_state.warnings),
        "recommendations": [
            {
                "value": recommendation.value,
                "label": recommendation.label,
                "reason": recommendation.reason,
                "priority": recommendation.priority,
                "matches_current": recommendation.matches_current,
            }
            for recommendation in runtime_state.recommendations
        ],
        "note": capability.note,
    }


def serialize_preset(
    preset: CapabilityPreset,
    inverter: DetectedInverter,
    values: Mapping[str, Any],
) -> dict[str, object]:
    """Serialize one declarative preset with runtime applicability details."""

    runtime_state = preset.runtime_state(inverter, values)
    capabilities_by_key = {capability.key: capability for capability in inverter.capabilities}
    policy_active = _policy_is_active(values)
    policy_allowed = True
    if policy_active:
        policy_allowed = preset_write_exposure_allowed(
            preset,
            capabilities_by_key=capabilities_by_key,
            control_mode=str(values.get("control_mode") or ""),
            detection_confidence=str(values.get("detection_confidence") or ""),
            variant_key=str(values.get("effective_variant_key") or ""),
            profile_source_scope=str(values.get("effective_profile_source_scope") or ""),
            schema_source_scope=str(values.get("effective_schema_source_scope") or ""),
            profile_name=str(values.get("effective_profile_name") or ""),
            device_scoped_overlay_active=bool(values.get("effective_device_scoped_overlay_active")),
            selected_control_keys=_selected_control_keys_from_values(values),
        )
    reasons = list(runtime_state.reasons)
    if policy_active and runtime_state.visible and not policy_allowed:
        reasons.append(f"Control policy blocks preset apply: {controls_reason(control_mode=str(values.get('control_mode') or ''), detection_confidence=str(values.get('detection_confidence') or ''))}.")
    return {
        "key": preset.key,
        "title": preset.title,
        "description": preset.description,
        "group": preset.group,
        "order": preset.order,
        "icon": preset.icon,
        "advanced": preset.advanced,
        "requires_confirm": preset.requires_confirm,
        "visible": runtime_state.visible,
        "applicable": runtime_state.applicable and policy_allowed,
        "policy_applicable": policy_allowed,
        "reasons": reasons,
        "warnings": list(runtime_state.warnings),
        "matches_current": runtime_state.matches_current,
        "changes": [
            _serialize_preset_item(item, inverter, values)
            for item in sorted(preset.items, key=lambda item: (item.order, item.capability_key))
        ],
    }


def entity_kind_for_capability(capability: WriteCapability) -> str:
    """Suggest the best UI control type for a capability."""

    if capability.value_kind == "bool":
        return "switch"
    if capability.value_kind == "enum":
        return "select"
    if capability.value_kind in {"scaled_u16", "u16", "u32"}:
        return "number"
    if capability.value_kind == "action":
        return "button"
    return "unknown"


def _serialize_related_capabilities(
    inverter: DetectedInverter,
    capability_keys: tuple[str, ...],
    values: Mapping[str, Any],
) -> list[dict[str, object]]:
    """Resolve relation keys into UI-friendly related descriptors."""

    details: list[dict[str, object]] = []
    for capability_key in capability_keys:
        try:
            related = inverter.get_capability(capability_key)
        except KeyError:
            if capability_key in values:
                details.append(
                    {
                        "key": capability_key,
                        "title": capability_key.replace("_", " ").title(),
                        "group": "runtime",
                        "entity_kind": "sensor",
                        "current_value": values.get(capability_key),
                        "visible": True,
                        "editable": False,
                        "warnings": [],
                        "change_summary": "",
                        "supported": True,
                    }
                )
            else:
                details.append(
                    {
                        "key": capability_key,
                        "supported": False,
                    }
                )
            continue

        runtime_state = related.runtime_state(values)
        details.append(
            {
                "key": related.key,
                "title": related.display_name,
                "group": related.group,
                "entity_kind": entity_kind_for_capability(related),
                "current_value": values.get(related.value_key),
                "visible": runtime_state.visible,
                "editable": runtime_state.editable,
                "warnings": list(runtime_state.warnings),
                "change_summary": related.change_summary,
                "supported": True,
            }
        )
    return details


def _group_status_counts(capabilities: list[dict[str, object]]) -> dict[str, int]:
    """Count runtime availability states inside one group."""

    counts = {
        "total": len(capabilities),
        "editable": 0,
        "read_only": 0,
        "blocked": 0,
        "hidden": 0,
    }
    for capability in capabilities:
        status = str(capability.get("status") or "hidden")
        if status in counts:
            counts[status] += 1
    return counts


def _group_status(counts: Mapping[str, int]) -> str:
    """Collapse capability counts into one high-level group status."""

    if counts.get("blocked", 0):
        return "attention"
    if counts.get("editable", 0) and not counts.get("read_only", 0):
        return "ready"
    if counts.get("editable", 0):
        return "mixed"
    if counts.get("read_only", 0):
        return "read_only"
    return "hidden"


def _group_summary(
    group_key: str,
    counts: Mapping[str, int],
    values: Mapping[str, Any],
) -> str:
    """Return a concise summary of the group's current editing context."""

    if group_key == "charging":
        summary = values.get("charging_settings_state")
        if summary:
            return str(summary)
    elif group_key == "battery":
        summary = values.get("battery_settings_state")
        if summary:
            return str(summary)
    elif group_key == "output":
        summary = values.get("output_settings_state")
        if summary:
            return str(summary)
    elif group_key == "system":
        if not values.get("remote_control_enabled"):
            return "Remote control disabled"
        if not values.get("configuration_safe_mode"):
            return "Some system settings are locked in the current mode"
        return "System settings available"

    editable = counts.get("editable", 0)
    blocked = counts.get("blocked", 0)
    read_only = counts.get("read_only", 0)
    hidden = counts.get("hidden", 0)
    if editable and not read_only and not blocked:
        return "All visible settings are editable"
    if blocked:
        return "Some settings are blocked by the inverter"
    if read_only and not editable:
        return "Visible settings are read-only"
    if hidden and not editable and not read_only and not blocked:
        return "Settings are currently hidden"
    return "Mixed availability"


def _dedupe_strings(items: Any) -> list[str]:
    """Deduplicate strings while preserving order."""

    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item:
            continue
        text = str(item)
        if text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _serialize_preset_item(
    item: CapabilityPresetItem,
    inverter: DetectedInverter,
    values: Mapping[str, Any],
) -> dict[str, object]:
    """Resolve one preset item against the detected inverter capabilities."""

    try:
        capability = inverter.get_capability(item.capability_key)
    except KeyError:
        return {
            "key": item.capability_key,
            "target_value": item.value,
            "reason": item.reason,
            "supported": False,
        }

    runtime_state = capability.runtime_state(values)
    current_value = values.get(capability.value_key)
    target_label = _target_label(capability, item.value)
    matches_current = current_value == item.value or current_value == target_label
    return {
        "key": capability.key,
        "title": capability.display_name,
        "group": capability.group,
        "entity_kind": entity_kind_for_capability(capability),
        "current_value": current_value,
        "target_value": item.value,
        "target_label": target_label,
        "matches_current": matches_current,
        "visible": runtime_state.visible,
        "editable": runtime_state.editable,
        "reasons": list(runtime_state.reasons),
        "warnings": list(runtime_state.warnings),
        "item_reason": item.reason,
        "change_summary": capability.change_summary,
        "supported": True,
    }


def _target_label(capability: WriteCapability, value: object) -> str:
    """Return the user-facing label for one preset target value."""

    enum_map = capability.enum_value_map
    if enum_map and value in enum_map:
        return enum_map[value]
    return str(value)


def _has_confirmed_local_metadata_proof(
    *,
    profile_source_scope: str,
    schema_source_scope: str,
) -> bool:
    trusted_scopes = {"builtin", "external"}
    return profile_source_scope in trusted_scopes and schema_source_scope in trusted_scopes


def _is_family_fallback_variant(*, variant_key: str, profile_name: str) -> bool:
    normalized_variant_key = str(variant_key or "").strip()
    if normalized_variant_key == "family_fallback":
        return True
    return str(profile_name or "").strip().endswith("/family_fallback.json")


def _is_device_scoped_learned_capability(capability: WriteCapability) -> bool:
    return capability.is_device_scoped_experimental


def _selected_control_keys_from_values(values: Mapping[str, Any]) -> frozenset[str] | None:
    """Read the device-scoped overlay selected-control keys from a runtime value set.

    Returns ``None`` when the activation declared no selection (legacy activation), so the
    serializer preserves the prior exposure behavior. Returns a frozenset (possibly empty)
    when a selection is present so only the selected learned controls remain editable.
    """

    raw = values.get("effective_device_scoped_overlay_selected_control_keys")
    if raw is None:
        return None
    if isinstance(raw, frozenset):
        return raw
    if isinstance(raw, (list, tuple, set)):
        return frozenset(str(key).strip() for key in raw if str(key or "").strip())
    return None


def _policy_is_active(values: Mapping[str, Any]) -> bool:
    """Return whether entry-level access policy is present in this value set."""

    return any(
        key in values
        for key in (
            "control_mode",
            "detection_confidence",
            "controls_enabled",
            "control_policy_reason",
        )
    )
