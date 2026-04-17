"""Helpers for generating SmartESS-backed SMG bridge drafts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from .local_metadata import (
    _dump_json,
    _ensure_can_write,
    ensure_local_metadata_dirs,
    local_profile_path,
    local_profiles_root,
    local_register_schema_path,
    local_register_schemas_root,
)
from .profile_loader import builtin_profile_path, load_driver_profile
from .register_schema_loader import builtin_register_schema_path, load_register_schema


_SMARTESS_SMG_OWNER_KEYS = frozenset({"modbus_smg"})
_BRIDGE_PROFILE_TITLE_SUFFIX = " (Local SmartESS SMG Bridge)"
_BRIDGE_SCHEMA_TITLE_SUFFIX = " (Local SmartESS SMG Bridge)"

_SMARTESS_SMG_BRIDGE_MAPPINGS: dict[str, dict[str, str]] = {
    "output mode": {
        "profile_key": "output_mode",
        "measurement_key": "output_mode",
    },
    "output priority": {
        "profile_key": "output_source_priority",
        "measurement_key": "output_source_priority",
    },
    "input voltage range": {
        "profile_key": "input_voltage_range",
        "measurement_key": "input_voltage_range",
    },
    "backlight control": {
        "profile_key": "lcd_backlight_mode",
        "measurement_key": "lcd_backlight_mode",
    },
    "auto return to default display screen": {
        "profile_key": "lcd_auto_return_mode",
        "measurement_key": "lcd_auto_return_mode",
    },
    "power saving mode": {
        "profile_key": "power_saving_mode",
        "measurement_key": "power_saving_mode",
    },
    "auto restart when overload occurs": {
        "profile_key": "overload_restart_mode",
        "measurement_key": "overload_restart_mode",
    },
    "auto restart when over temperature occurs": {
        "profile_key": "over_temperature_restart_mode",
        "measurement_key": "over_temperature_restart_mode",
    },
    "overload bypass": {
        "profile_key": "overload_bypass_mode",
        "measurement_key": "overload_bypass_mode",
    },
    "battery eq mode": {
        "profile_key": "battery_equalization_mode",
        "measurement_key": "battery_equalization_mode",
    },
    "output voltage": {
        "profile_key": "output_rating_voltage",
        "measurement_key": "output_rating_voltage",
    },
    "output frequency": {
        "profile_key": "output_rating_frequency",
        "measurement_key": "output_rating_frequency",
    },
    "high dc protection voltage": {
        "profile_key": "battery_overvoltage_protection_voltage",
        "measurement_key": "battery_overvoltage_protection_voltage",
    },
    "bulk charging voltage (c.v voltage)": {
        "profile_key": "battery_bulk_voltage",
        "measurement_key": "battery_bulk_voltage",
    },
    "floating charging voltage": {
        "profile_key": "battery_float_voltage",
        "measurement_key": "battery_float_voltage",
    },
    "low dc protection voltage in mains mode": {
        "profile_key": "battery_under_voltage",
        "measurement_key": "battery_under_voltage",
    },
    "low dc protection voltage in off-grid mode": {
        "profile_key": "battery_under_voltage_off_grid",
        "measurement_key": "battery_under_voltage_off_grid",
    },
    "charger source priority": {
        "profile_key": "charge_source_priority",
        "measurement_key": "charge_source_priority",
    },
    "max.charging current": {
        "profile_key": "max_charge_current",
        "measurement_key": "max_charge_current",
    },
    "max.ac.charging current": {
        "profile_key": "max_ac_charge_current",
        "measurement_key": "max_ac_charge_current",
    },
    "eq charing voltage": {
        "profile_key": "battery_equalization_voltage",
        "measurement_key": "battery_equalization_voltage",
    },
    "eq charing time": {
        "profile_key": "battery_equalization_time",
        "measurement_key": "battery_equalization_time",
    },
    "eq timeout exit time": {
        "profile_key": "battery_equalization_timeout",
        "measurement_key": "battery_equalization_timeout",
    },
    "eq interval time": {
        "profile_key": "battery_equalization_interval",
        "measurement_key": "battery_equalization_interval",
    },
    "low dc protection soc in grid mode": {
        "profile_key": "low_dc_protection_soc_grid_mode",
        "measurement_key": "low_dc_protection_soc_grid_mode",
    },
    "off grid mode battery discharge soc protection value": {
        "profile_key": "low_dc_cutoff_soc",
        "measurement_key": "low_dc_cutoff_soc",
    },
    "boot method": {
        "profile_key": "turn_on_mode",
        "measurement_key": "turn_on_mode",
    },
    "exit fault mode": {
        "profile_key": "exit_fault_mode",
        "measurement_key": "exit_fault_mode",
    },
}


@dataclass(frozen=True, slots=True)
class SmartEssSmgBridgeMatch:
    """One SmartESS cloud field mapped onto an existing SMG surface."""

    cloud_title: str
    normalized_title: str
    bucket: str
    profile_key: str = ""
    measurement_key: str = ""
    profile_enable: bool = False
    measurement_enable: bool = False


@dataclass(frozen=True, slots=True)
class SmartEssSmgBridgePlan:
    """Resolved SmartESS-backed SMG bridge plan."""

    source_profile_name: str
    source_schema_name: str
    source_profile_path: str
    source_schema_path: str
    bridge_label: str
    reason: str
    matches: tuple[SmartEssSmgBridgeMatch, ...]
    blocked_field_titles: tuple[str, ...] = ()
    skipped_field_titles: tuple[str, ...] = ()

    @property
    def profile_enable_keys(self) -> tuple[str, ...]:
        return _unique_values(
            match.profile_key
            for match in self.matches
            if match.profile_enable and match.profile_key
        )

    @property
    def measurement_enable_keys(self) -> tuple[str, ...]:
        return _unique_values(
            match.measurement_key
            for match in self.matches
            if match.measurement_enable and match.measurement_key
        )


def resolve_smartess_smg_bridge_plan(
    *,
    effective_owner_key: str = "",
    source_profile_name: str = "",
    source_schema_name: str = "",
    source_profile_path: str = "",
    source_schema_path: str = "",
    cloud_evidence: dict[str, Any] | None = None,
) -> SmartEssSmgBridgePlan | None:
    """Resolve one safe SmartESS-to-SMG bridge plan from cloud evidence."""

    if str(effective_owner_key or "").strip() not in _SMARTESS_SMG_OWNER_KEYS:
        return None
    if not source_profile_name or not source_schema_name:
        return None

    source_profile_ref = source_profile_path or str(builtin_profile_path(source_profile_name))
    source_schema_ref = source_schema_path or str(builtin_register_schema_path(source_schema_name))
    profile = load_driver_profile(source_profile_ref)
    schema = load_register_schema(source_schema_ref)
    fields = _normalized_setting_fields(cloud_evidence)
    if not fields:
        return None

    capabilities_by_key = {
        capability.key: capability
        for capability in profile.capabilities
    }
    measurements_by_key = {
        measurement.key: measurement
        for measurement in schema.measurement_descriptions
    }

    matches: list[SmartEssSmgBridgeMatch] = []
    blocked_field_titles: list[str] = []
    skipped_field_titles: list[str] = []
    seen_titles: set[str] = set()

    for field in fields:
        title = str(field.get("title") or "").strip()
        if not title:
            continue
        normalized_title = _normalize_setting_title(title)
        if not normalized_title or normalized_title in seen_titles:
            continue
        seen_titles.add(normalized_title)

        mapping = _SMARTESS_SMG_BRIDGE_MAPPINGS.get(normalized_title)
        if mapping is None:
            skipped_field_titles.append(title)
            continue

        profile_key = str(mapping.get("profile_key") or "")
        measurement_key = str(mapping.get("measurement_key") or "")
        capability = capabilities_by_key.get(profile_key) if profile_key else None
        measurement = measurements_by_key.get(measurement_key) if measurement_key else None
        if capability is None and measurement is None:
            skipped_field_titles.append(title)
            continue
        if capability is not None and capability.support_tier == "blocked":
            blocked_field_titles.append(title)
            continue

        profile_enable = capability is not None and not capability.enabled_default
        measurement_enable = measurement is not None and not measurement.enabled_default
        if not profile_enable and not measurement_enable:
            continue

        matches.append(
            SmartEssSmgBridgeMatch(
                cloud_title=title,
                normalized_title=normalized_title,
                bucket=str(field.get("bucket") or ""),
                profile_key=profile_key,
                measurement_key=measurement_key,
                profile_enable=profile_enable,
                measurement_enable=measurement_enable,
            )
        )

    if not matches:
        return None

    profile_enable_count = len(
        _unique_values(match.profile_key for match in matches if match.profile_enable and match.profile_key)
    )
    measurement_enable_count = len(
        _unique_values(
            match.measurement_key
            for match in matches
            if match.measurement_enable and match.measurement_key
        )
    )
    reason = (
        "SmartESS cloud settings matched existing SMG controls and config readbacks "
        f"({profile_enable_count} controls, {measurement_enable_count} readbacks) "
        "without changing the modbus_smg runtime owner."
    )
    return SmartEssSmgBridgePlan(
        source_profile_name=source_profile_name,
        source_schema_name=source_schema_name,
        source_profile_path=str(source_profile_ref),
        source_schema_path=str(source_schema_ref),
        bridge_label="SmartESS SMG bridge",
        reason=reason,
        matches=tuple(matches),
        blocked_field_titles=tuple(blocked_field_titles),
        skipped_field_titles=tuple(skipped_field_titles),
    )


def create_smartess_smg_bridge_draft(
    *,
    config_dir: Path,
    plan: SmartEssSmgBridgePlan,
    cloud_evidence: dict[str, Any],
    output_profile_name: str | None = None,
    output_schema_name: str | None = None,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Create one SmartESS-backed SMG bridge draft pair."""

    ensure_local_metadata_dirs(config_dir)

    profile_output_name = output_profile_name or plan.source_profile_name
    schema_output_name = output_schema_name or plan.source_schema_name
    profile_destination = local_profile_path(config_dir, profile_output_name)
    schema_destination = local_register_schema_path(config_dir, schema_output_name)
    _ensure_can_write(
        profile_destination,
        local_profiles_root(config_dir),
        overwrite=overwrite,
    )
    _ensure_can_write(
        schema_destination,
        local_register_schemas_root(config_dir),
        overwrite=overwrite,
    )

    profile_raw = _read_json(Path(plan.source_profile_path))
    schema_raw = _read_json(Path(plan.source_schema_path))
    _apply_enabled_default_flags(
        profile_raw,
        list_key="capabilities",
        enabled_keys=plan.profile_enable_keys,
    )
    _apply_enabled_default_flags(
        schema_raw,
        list_key="measurement_descriptions",
        enabled_keys=plan.measurement_enable_keys,
    )

    annotation = _bridge_annotation(plan=plan, cloud_evidence=cloud_evidence)
    profile_raw.setdefault("draft_of", plan.source_profile_name)
    profile_raw.setdefault("experimental", True)
    profile_raw["title"] = _append_title_suffix(
        str(profile_raw.get("title", Path(plan.source_profile_name).stem)),
        _BRIDGE_PROFILE_TITLE_SUFFIX,
    )
    profile_raw["smartess_bridge"] = annotation

    schema_raw.setdefault("draft_of", plan.source_schema_name)
    schema_raw.setdefault("experimental", True)
    schema_raw["title"] = _append_title_suffix(
        str(schema_raw.get("title", Path(plan.source_schema_name).stem)),
        _BRIDGE_SCHEMA_TITLE_SUFFIX,
    )
    schema_raw["smartess_bridge"] = annotation

    profile_destination.parent.mkdir(parents=True, exist_ok=True)
    schema_destination.parent.mkdir(parents=True, exist_ok=True)
    profile_destination.write_text(_dump_json(profile_raw), encoding="utf-8")
    schema_destination.write_text(_dump_json(schema_raw), encoding="utf-8")
    return profile_destination, schema_destination


def _normalized_setting_fields(cloud_evidence: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(cloud_evidence, dict):
        return []
    payload = cloud_evidence.get("payload")
    if not isinstance(payload, dict):
        return []
    normalized = payload.get("normalized")
    if not isinstance(normalized, dict):
        return []
    device_settings = normalized.get("device_settings")
    if not isinstance(device_settings, dict):
        return []
    fields = device_settings.get("fields")
    if not isinstance(fields, list):
        return []
    return [field for field in fields if isinstance(field, dict)]


def _normalize_setting_title(value: str) -> str:
    return " ".join(str(value).strip().lower().replace("_", " ").split())


def _apply_enabled_default_flags(
    raw: dict[str, Any],
    *,
    list_key: str,
    enabled_keys: tuple[str, ...],
) -> None:
    if not enabled_keys:
        return
    raw_items = raw.get(list_key)
    enabled = set(enabled_keys)
    if not isinstance(raw_items, list):
        raw[list_key] = [
            {
                "key": key,
                "enabled_default": True,
            }
            for key in enabled_keys
        ]
        return
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "")
        if key in enabled:
            item["enabled_default"] = True
            enabled.discard(key)
    for key in enabled:
        raw_items.append(
            {
                "key": key,
                "enabled_default": True,
            }
        )


def _append_title_suffix(title: str, suffix: str) -> str:
    return title if title.endswith(suffix) else f"{title}{suffix}"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _bridge_annotation(
    *,
    plan: SmartEssSmgBridgePlan,
    cloud_evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "kind": "smg_bridge",
        "bridge_label": plan.bridge_label,
        "reason": plan.reason,
        "source_profile_name": plan.source_profile_name,
        "source_schema_name": plan.source_schema_name,
        "profile_enable_keys": list(plan.profile_enable_keys),
        "measurement_enable_keys": list(plan.measurement_enable_keys),
        "matches": [
            {
                "cloud_title": match.cloud_title,
                "bucket": match.bucket,
                "profile_key": match.profile_key,
                "measurement_key": match.measurement_key,
                "profile_enable": match.profile_enable,
                "measurement_enable": match.measurement_enable,
            }
            for match in plan.matches
        ],
        "blocked_field_titles": list(plan.blocked_field_titles),
        "skipped_field_titles": list(plan.skipped_field_titles),
        "cloud_source": str(cloud_evidence.get("source") or ""),
        "cloud_match": dict(cloud_evidence.get("match") or {}),
        "cloud_identity": dict(cloud_evidence.get("device_identity") or {}),
        "cloud_summary": dict(cloud_evidence.get("summary") or {}),
    }


def _unique_values(values) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return tuple(ordered)