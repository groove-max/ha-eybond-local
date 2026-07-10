"""Support bundle export helpers for troubleshooting and experimental onboarding."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ..const import LOCAL_METADATA_DIR, LOCAL_SUPPORT_PACKAGES_DIR


_SMG_FAMILY_FALLBACK_VARIANT = "family_fallback"
_SMG_READ_ONLY_PROFILE_NAME = "modbus_smg/family_fallback.json"
_COLLECTOR_VALUE_PREFIXES = (
    "collector_",
    "proxy_capture_",
    "smartess_",
)
_RUNTIME_DIAGNOSTIC_VALUE_PREFIXES = (
    "integration_",
    "runtime_",
    "support_workflow_",
)
_RUNTIME_DIAGNOSTIC_VALUE_KEYS = frozenset(
    {
        "control_mode",
        "cloud_evidence_path",
        "last_error",
        "local_metadata_status",
        "support_bundle_path",
        "support_package_download_path",
        "support_package_download_relative_url",
        "support_package_download_url",
        "support_package_path",
    }
)


def _split_runtime_values_by_role(values: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return runtime values grouped by collector, inverter, and integration role."""

    grouped: dict[str, dict[str, Any]] = {
        "collector": {},
        "inverter": {},
        "integration": {},
    }
    for key, value in values.items():
        normalized_key = str(key)
        if normalized_key.startswith(_COLLECTOR_VALUE_PREFIXES):
            grouped["collector"][normalized_key] = value
            continue
        if (
            normalized_key in _RUNTIME_DIAGNOSTIC_VALUE_KEYS
            or normalized_key.startswith(_RUNTIME_DIAGNOSTIC_VALUE_PREFIXES)
        ):
            grouped["integration"][normalized_key] = value
            continue
        grouped["inverter"][normalized_key] = value
    return grouped


def _build_role_payloads(
    *,
    collector: dict[str, Any] | None,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
    data: dict[str, Any],
    options: dict[str, Any],
    source_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Build an explicit role split while preserving the legacy runtime payload."""

    grouped_values = _split_runtime_values_by_role(values)
    collector_identity = {
        "collector_ip": data.get("collector_ip", ""),
        "collector_pn": (collector or {}).get("collector_pn") or data.get("collector_pn", ""),
        "cloud_family": data.get("collector_cloud_family", ""),
        "operation_mode": options.get("collector_operation_mode") or data.get("collector_operation_mode", ""),
    }
    inverter_identity = {
        "driver_key": source_metadata.get("effective_owner_key") or (inverter or {}).get("driver_key", ""),
        "model_name": (inverter or {}).get("model_name") or data.get("detected_model", ""),
        "serial_number": (inverter or {}).get("serial_number") or data.get("detected_serial", ""),
        "variant_key": source_metadata.get("variant_key", ""),
        "profile_name": source_metadata.get("profile_name", ""),
        "register_schema_name": source_metadata.get("register_schema_name", ""),
    }
    return {
        "collector": {
            "present": collector is not None,
            "payload_ref": "runtime.collector",
            "identity": collector_identity,
            "values": grouped_values["collector"],
        },
        "inverter": {
            "present": inverter is not None,
            "payload_ref": "runtime.inverter",
            "identity": inverter_identity,
            "values": grouped_values["inverter"],
        },
        "integration": {
            "payload_ref": "runtime.values",
            "values": grouped_values["integration"],
        },
    }


def _descriptor_decision_shadow_payload(
    *,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
) -> dict[str, Any] | None:
    """Return descriptor-tree shadow diagnostics from runtime payloads, if present."""

    if isinstance(inverter, dict):
        direct = inverter.get("descriptor_decision_shadow")
        if isinstance(direct, dict):
            return direct
        details = inverter.get("details")
        if isinstance(details, dict):
            nested = details.get("descriptor_decision_shadow")
            if isinstance(nested, dict):
                return nested
            device_catalog = details.get("device_catalog")
            if isinstance(device_catalog, dict):
                catalog_report = device_catalog.get("descriptor_decision")
                if isinstance(catalog_report, dict):
                    return catalog_report
    value_report = values.get("descriptor_decision_shadow")
    if isinstance(value_report, dict):
        return value_report
    return None


def _catalog_detection_payload(
    *,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
) -> dict[str, Any] | None:
    """Return the canonical compiled-catalog resolution payload."""

    if isinstance(inverter, dict):
        details = inverter.get("details")
        if isinstance(details, dict):
            direct = details.get("catalog_detection")
            if isinstance(direct, dict):
                return direct
            device_catalog = details.get("device_catalog")
            if isinstance(device_catalog, dict):
                compiled = device_catalog.get("compiled_resolution")
                if isinstance(compiled, dict):
                    return compiled
    direct = values.get("catalog_detection")
    if isinstance(direct, dict):
        return direct
    device_catalog = values.get("device_catalog")
    if isinstance(device_catalog, dict):
        compiled = device_catalog.get("compiled_resolution")
        if isinstance(compiled, dict):
            return compiled
    return None


def is_read_only_unverified_smg_family(
    *,
    variant_key: str = "",
    profile_name: str = "",
    effective_owner_key: str = "",
) -> bool:
    """Return whether one runtime path is the read-only unverified SMG family state."""

    normalized_variant_key = str(variant_key or "").strip()
    normalized_profile_name = str(profile_name or "").strip()
    normalized_owner_key = str(effective_owner_key or "").strip()
    if normalized_owner_key and normalized_owner_key != "modbus_smg":
        return False
    return (
        normalized_variant_key == _SMG_FAMILY_FALLBACK_VARIANT
        or normalized_profile_name == _SMG_READ_ONLY_PROFILE_NAME
    )


def build_support_marker(
    *,
    variant_key: str = "",
    profile_name: str = "",
    effective_owner_key: str = "",
) -> dict[str, Any] | None:
    """Return one machine-readable support marker for special runtime states."""

    if not is_read_only_unverified_smg_family(
        variant_key=variant_key,
        profile_name=profile_name,
        effective_owner_key=effective_owner_key,
    ):
        return None
    return {
        "key": "read_only_unverified_smg_family",
        "label": "Read-only unverified SMG family",
        "read_only": True,
        "verification": "unverified",
        "summary": (
            "Read-only SMG-family metadata is active. "
            "Built-in writes are intentionally disabled until a verified model-specific mapping exists."
        ),
    }


def build_support_bundle_payload(
    *,
    entry_id: str,
    entry_title: str,
    connected: bool,
    collector: dict[str, Any] | None,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
    data: dict[str, Any],
    options: dict[str, Any],
    profile_name: str,
    register_schema_name: str,
    variant_key: str = "",
    effective_owner_key: str = "",
    effective_owner_name: str = "",
    smartess_family_name: str = "",
    raw_profile_name: str = "",
    raw_register_schema_name: str = "",
    smartess_protocol_asset_id: str = "",
    smartess_profile_key: str = "",
    cloud_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one machine-readable support bundle payload."""

    support_marker = build_support_marker(
        variant_key=variant_key,
        profile_name=profile_name,
        effective_owner_key=effective_owner_key,
    )

    source_metadata = {
        "profile_name": profile_name,
        "register_schema_name": register_schema_name,
        "variant_key": variant_key,
        "support_marker": support_marker,
        "effective_owner_key": effective_owner_key,
        "effective_owner_name": effective_owner_name,
        "smartess_family_name": smartess_family_name,
        "raw_profile_name": raw_profile_name,
        "raw_register_schema_name": raw_register_schema_name,
        "smartess_protocol_asset_id": smartess_protocol_asset_id,
        "smartess_profile_key": smartess_profile_key,
    }
    runtime_payload = {
        "connected": connected,
        "collector": collector,
        "inverter": inverter,
        "values": values,
    }
    descriptor_decision_shadow = _descriptor_decision_shadow_payload(
        inverter=inverter,
        values=values,
    )
    catalog_detection = _catalog_detection_payload(
        inverter=inverter,
        values=values,
    )

    return {
        "bundle_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entry": {
            "entry_id": entry_id,
            "title": entry_title,
            "data": data,
            "options": options,
        },
        "source_metadata": source_metadata,
        "runtime": runtime_payload,
        "roles": _build_role_payloads(
            collector=collector,
            inverter=inverter,
            values=values,
            data=data,
            options=options,
            source_metadata=source_metadata,
        ),
        "evidence": {
            "cloud": cloud_evidence,
            "catalog_detection": catalog_detection,
            "descriptor_decision_shadow": descriptor_decision_shadow,
        },
    }


def export_support_bundle(
    *,
    config_dir: Path,
    entry_id: str,
    entry_title: str,
    connected: bool,
    collector: dict[str, Any] | None,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
    data: dict[str, Any],
    options: dict[str, Any],
    profile_name: str,
    register_schema_name: str,
    variant_key: str = "",
    effective_owner_key: str = "",
    effective_owner_name: str = "",
    smartess_family_name: str = "",
    raw_profile_name: str = "",
    raw_register_schema_name: str = "",
    smartess_protocol_asset_id: str = "",
    smartess_profile_key: str = "",
    cloud_evidence: dict[str, Any] | None = None,
    overwrite: bool = False,
) -> Path:
    """Build and export one JSON support bundle payload for the current entry."""

    payload = build_support_bundle_payload(
        entry_id=entry_id,
        entry_title=entry_title,
        connected=connected,
        collector=collector,
        inverter=inverter,
        values=values,
        data=data,
        options=options,
        profile_name=profile_name,
        register_schema_name=register_schema_name,
        variant_key=variant_key,
        effective_owner_key=effective_owner_key,
        effective_owner_name=effective_owner_name,
        smartess_family_name=smartess_family_name,
        raw_profile_name=raw_profile_name,
        raw_register_schema_name=raw_register_schema_name,
        smartess_protocol_asset_id=smartess_protocol_asset_id,
        smartess_profile_key=smartess_profile_key,
        cloud_evidence=cloud_evidence,
    )

    output_root = config_dir / LOCAL_METADATA_DIR / LOCAL_SUPPORT_PACKAGES_DIR
    output_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = output_root / f"{entry_id}_{timestamp}_support_bundle.json"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)

    destination.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination
