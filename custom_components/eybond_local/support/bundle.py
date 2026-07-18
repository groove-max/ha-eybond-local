"""Support bundle export helpers for troubleshooting and experimental onboarding."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ..const import LOCAL_METADATA_DIR, LOCAL_SUPPORT_PACKAGES_DIR


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


def _present(source: dict[str, Any], *keys: str) -> dict[str, Any]:
    """Return only the present, non-None keys (so devcode/addr 0x0000/0 survive)."""

    return {key: source[key] for key in keys if source.get(key) is not None}


def _redact_recovery_contract(data: dict[str, Any]) -> dict[str, Any]:
    """Strip the raw recovery records from the bundle's verbatim entry data.

    The RecoveryContract's callback proof AND the strategy-transition recovery
    state both carry opaque route/endpoint snapshots (trigger target, advertised
    HA endpoint, local bind host). A support bundle shows only the STRUCTURE
    (booleans, kind, timestamps, route-completeness) -- never the network values
    themselves -- so each raw record is replaced with a pointer, never copied.
    """

    from ..connection.recovery_contract import RECOVERY_CONTRACT_KEY
    from ..const import CONF_STRATEGY_TRANSITION_STATE

    redacted = dict(data)
    if RECOVERY_CONTRACT_KEY in redacted:
        redacted[RECOVERY_CONTRACT_KEY] = (
            "**redacted: see roles.diagnostics.recovery**"
        )
    if CONF_STRATEGY_TRANSITION_STATE in redacted:
        redacted[CONF_STRATEGY_TRANSITION_STATE] = (
            "**redacted: see roles.diagnostics.collector_identity"
            ".connection_strategy_transition_state**"
        )
    return redacted


def _strategy_transition_state_diagnostics(data: dict[str, Any]) -> dict[str, Any]:
    """The redacted diagnostics view of the strategy-transition recovery state.

    Returns ``{}`` when no state is persisted; otherwise ONLY the typed
    ``StrategyTransitionRecoveryState.diagnostics()`` (kind / timestamps /
    route-completeness), never the raw route/endpoint. A malformed persisted
    record is reported as an opaque flag, never echoed back.
    """

    from ..connection.strategy_transition_recovery import (
        StrategyTransitionRecoveryState,
    )
    from ..const import CONF_STRATEGY_TRANSITION_STATE

    record = data.get(CONF_STRATEGY_TRANSITION_STATE)
    if not record:
        return {}
    state = StrategyTransitionRecoveryState.from_record(record)
    if state is None:
        return {"present": True, "malformed": True}
    return state.diagnostics()


def _build_diagnostics_split(
    values: dict[str, Any],
    data: dict[str, Any],
    options: dict[str, Any],
) -> dict[str, Any]:
    """Return the split collector/session/frame/route diagnostics.

    This makes the support package clearly separate STABLE identity from volatile
    last-frame / route metadata, so a reader never mistakes the last frame's
    devcode for the collector identity. Everything is projected from the runtime
    snapshot values; nothing here mutates runtime state.
    """

    try:
        from ..connection.connection_policy import entry_axis_diagnostics

        axes = entry_axis_diagnostics(data, options)
    except Exception:  # pragma: no cover - defensive: never fail the bundle
        axes = {}

    return {
        "collector_identity": {
            "collector_pn": values.get("collector_pn") or data.get("collector_pn", ""),
            "collector_kind": (
                "esp_eybond_bridge"
                if values.get("collector_virtual_bridge") or data.get("collector_virtual_bridge")
                else data.get("collector_kind", "")
            ),
            "collector_virtual_bridge": bool(
                values.get("collector_virtual_bridge") or data.get("collector_virtual_bridge")
            ),
            "collector_bridge_version": values.get("collector_bridge_version", ""),
            "collector_firmware_version": values.get("smartess_collector_version", ""),
            "collector_cloud_family": values.get("collector_cloud_family")
            or data.get("collector_cloud_family", ""),
            "collector_cloud_profile_key": values.get("collector_cloud_profile_key")
            or data.get("collector_cloud_profile_key", ""),
            "connection_strategy": axes.get("connection_strategy", ""),
            "connection_strategy_evidence": axes.get("connection_strategy_evidence", ""),
            "endpoint_control_policy": axes.get("endpoint_control_policy", ""),
            # Recovery-required status, REDACTED: only the typed diagnostics
            # view (kind / timestamps / route-completeness), NEVER the raw
            # route/endpoint values the persisted record carries.
            "connection_strategy_transition_state": (
                _strategy_transition_state_diagnostics(data)
            ),
            # Stable identity devcode (heartbeat), NOT the volatile last frame.
            "devcode": values.get("collector_devcode", ""),
        },
        "session": _present(
            values,
            "collector_remote_ip",
            "collector_callback_session_protocol",
            "collector_callback_observed_session_protocol",
            "collector_callback_wire_framing",
            "collector_callback_identity_sources",
            "collector_callback_collector_management_adapter",
            "collector_callback_inverter_forward_adapter",
            "collector_callback_proxy_adapter",
            "collector_callback_adapter_conflict",
            "collector_connection_count",
            "collector_disconnect_count",
            "collector_last_disconnect_reason",
            "collector_heartbeat_age_seconds",
            "runtime_session_state",
        ),
        "last_frame": _present(values, "collector_last_frame_devcode"),
        "heartbeat": _present(
            values,
            "collector_heartbeat_devcode",
            "collector_heartbeat_payload_len",
            "collector_heartbeat_payload",
            "collector_heartbeat_format",
            "collector_heartbeat_age_seconds",
        ),
        "inverter_route": {
            "driver_key": values.get("driver_key") or data.get("driver_hint", ""),
            "protocol_family": values.get("protocol_family", ""),
            "model": values.get("model_name") or data.get("detected_model", ""),
            "serial": values.get("serial_number") or data.get("detected_serial", ""),
            "inverter_forward_adapter": values.get(
                "collector_callback_inverter_forward_adapter", ""
            ),
            **_present(
                values,
                "inverter_route_devcode",
                "inverter_route_collector_addr",
                "inverter_route_device_addr",
            ),
        },
        "collector_management_route": {
            "collector_management_adapter": values.get(
                "collector_callback_collector_management_adapter", ""
            ),
            **_present(
                values,
                "collector_management_adapter_id",
                "collector_management_adapter_provenance",
                "collector_management_can_read_endpoint_state",
                "collector_management_can_write_endpoint",
                "collector_management_can_apply_changes",
                "collector_management_can_reboot",
                "collector_management_last_operation",
                "collector_management_last_status",
                "collector_management_last_error_class",
                "collector_management_last_error_code",
                "collector_management_last_duration_ms",
                "collector_management_last_timestamp",
                "smartess_device_address",
            ),
        },
        "collector_metadata": {
            **_present(
                values,
                "collector_metadata_route_channels",
                "collector_metadata_route_provenance",
                "collector_metadata_session_generation",
                "collector_metadata_identity_known",
                "collector_metadata_identity_transitions",
                "collector_metadata_last_read_fresh",
                "collector_metadata_channel_status",
                "collector_metadata_channel_duration_ms",
                "collector_metadata_channel_errors",
                "collector_metadata_channel_commands",
                "collector_metadata_channel_failures",
                "collector_metadata_partial_channels",
                "collector_metadata_cache_dirty",
                "collector_metadata_framed_cache_keys",
                "collector_metadata_at_cache_keys",
                "collector_metadata_framed_age_seconds",
                "collector_metadata_at_age_seconds",
                "collector_metadata_dead_channels",
                "collector_metadata_dead_channel_detail",
            ),
        },
        "migration": {
            "migration_status": axes.get("migration_status", ""),
            "migration_warning": axes.get("migration_warning", ""),
            "migration_axes_source": axes.get("migration_axes_source", ""),
            "connection_strategy_source": axes.get("connection_strategy_source", ""),
            "endpoint_control_policy_source": axes.get(
                "endpoint_control_policy_source", ""
            ),
        },
        # Proof STRUCTURE only: booleans, methods and timestamps. The raw
        # trigger target / advertised endpoint snapshots never enter a bundle.
        "recovery": {
            "recovery_contract_version": axes.get("recovery_contract_version", 0),
            "recovery_contract_valid": axes.get("recovery_contract_valid", False),
            "recovery_contract_identity_strong": axes.get(
                "recovery_contract_identity_strong", False
            ),
            "recovery_contract_pn_bound": axes.get("recovery_contract_pn_bound", False),
            "inbound_recovery_verified": axes.get("inbound_recovery_verified", False),
            "inbound_recovery_method": axes.get("inbound_recovery_method", ""),
            "inbound_recovery_verified_at": axes.get("inbound_recovery_verified_at", ""),
            "callback_recovery_verified": axes.get("callback_recovery_verified", False),
            "callback_recovery_method": axes.get("callback_recovery_method", ""),
            "callback_recovery_verified_at": axes.get(
                "callback_recovery_verified_at", ""
            ),
            "callback_route_bound": axes.get("callback_route_bound", False),
            "advertised_endpoint_bound": axes.get("advertised_endpoint_bound", False),
        },
    }


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
        "diagnostics": _build_diagnostics_split(values, data, options),
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
    support_marker: dict[str, Any] | None = None,
    cloud_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one machine-readable support bundle payload.

    ``support_marker`` is the authoritative, driver-produced marker payload (or
    ``None``). This layer only embeds it -- it never infers a special runtime
    state from driver key, variant key or profile path.
    """

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
            "data": _redact_recovery_contract(data),
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
    support_marker: dict[str, Any] | None = None,
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
        support_marker=support_marker,
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
