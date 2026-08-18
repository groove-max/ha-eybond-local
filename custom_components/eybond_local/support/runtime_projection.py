"""Pure support-artifact projections from typed runtime state.

This module deliberately owns no coordinator, Home Assistant, transport or
persistence state.  It converts an already-selected runtime snapshot into the
stable payload shapes consumed by support bundles and fixtures.
"""

from __future__ import annotations

from typing import Any

from ..fixtures.utils import build_command_fixture_responses
from ..models import CollectorCloudProfile


def build_collector_support_payload(
    collector: object | None,
    cloud_profile: CollectorCloudProfile,
) -> dict[str, Any] | None:
    """Project collector runtime metadata into the support payload shape."""

    if collector is None:
        return None
    if type(cloud_profile) is not CollectorCloudProfile:
        raise TypeError("collector_cloud_profile_invalid")
    return {
        "remote_ip": collector.remote_ip,
        "remote_port": collector.remote_port,
        "connection_count": collector.connection_count,
        "connection_replace_count": collector.connection_replace_count,
        "disconnect_count": collector.disconnect_count,
        "pending_request_drop_count": collector.pending_request_drop_count,
        "last_disconnect_reason": collector.last_disconnect_reason,
        "discovery_restart_count": collector.discovery_restart_count,
        "last_discovery_reason": collector.last_discovery_reason,
        "collector_pn": collector.collector_pn,
        "profile_key": collector.profile_key,
        "profile_name": collector.profile_name,
        "last_udp_reply": collector.last_udp_reply,
        "last_udp_reply_from": collector.last_udp_reply_from,
        "last_devcode": collector.last_devcode,
        "smartess_collector_version": collector.smartess_collector_version,
        "smartess_protocol_raw_id": collector.smartess_protocol_raw_id,
        "smartess_protocol_asset_id": collector.smartess_protocol_asset_id,
        "smartess_protocol_asset_name": collector.smartess_protocol_asset_name,
        "smartess_protocol_suffix": collector.smartess_protocol_suffix,
        "smartess_protocol_profile_key": collector.smartess_protocol_profile_key,
        "smartess_protocol_name": collector.smartess_protocol_name,
        "smartess_device_address": collector.smartess_device_address,
        "collector_cloud_profile_key": cloud_profile.key,
        "collector_cloud_profile_label": cloud_profile.label,
        "collector_cloud_profile_source": cloud_profile.source,
        "collector_cloud_profile_confidence": cloud_profile.confidence,
    }


def build_inverter_support_payload(inverter: object) -> dict[str, Any]:
    """Project one detected inverter into the support payload shape."""

    return {
        "driver_key": inverter.driver_key,
        "protocol_family": inverter.protocol_family,
        "model_name": inverter.model_name,
        "variant_key": inverter.variant_key,
        "serial_number": inverter.serial_number,
        "profile_name": inverter.profile_name,
        "register_schema_name": inverter.register_schema_name,
        "probe_target": {
            "devcode": inverter.probe_target.devcode,
            "collector_addr": inverter.probe_target.collector_addr,
            "device_addr": inverter.probe_target.device_addr,
        },
        "details": dict(inverter.details),
    }


def _best_generic_capture(raw_capture: dict[str, Any]) -> dict[str, Any] | None:
    captures = list(raw_capture.get("captures") or [])
    if not captures:
        return None
    return max(
        captures,
        key=lambda capture: (
            len(capture.get("fixture_ranges") or []),
            -len(capture.get("range_failures") or []),
        ),
    )


def build_support_fixture(
    raw_capture: dict[str, Any],
    *,
    inverter: object | None,
    collector_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Build a reusable protocol fixture from one support capture."""

    ranges = list(raw_capture.get("fixture_ranges") or [])
    command_responses = build_command_fixture_responses(raw_capture)
    probe_target = None
    fixture_name = ""
    if inverter is not None:
        probe_target = {
            "devcode": inverter.probe_target.devcode,
            "collector_addr": inverter.probe_target.collector_addr,
            "device_addr": inverter.probe_target.device_addr,
        }
        fixture_name = f"{inverter.driver_key}_support_capture"
    elif raw_capture.get("capture_kind") == "generic_register_dump":
        best_capture = _best_generic_capture(raw_capture)
        if best_capture is not None:
            ranges = list(best_capture.get("fixture_ranges") or ranges)
            probe_target = dict(best_capture.get("probe_target") or {})
            fixture_name = f"{best_capture.get('driver_key', 'unknown')}_support_capture"
    if not ranges and not command_responses:
        return None

    collector = collector_payload or {}
    fixture: dict[str, Any] = {
        "fixture_version": 1,
        "name": fixture_name or "unknown_driver_support_capture",
        "collector": {
            "remote_ip": collector.get("remote_ip"),
            "collector_pn": collector.get("collector_pn"),
            "last_devcode": collector.get("last_devcode"),
            "profile_key": collector.get("profile_key"),
            "profile_name": collector.get("profile_name"),
        },
        "probe_target": probe_target,
    }
    if ranges:
        fixture["ranges"] = ranges
    if command_responses:
        fixture["command_responses"] = command_responses
    return fixture


def metadata_source_payload(metadata: object | None) -> dict[str, Any] | None:
    """Project one metadata source descriptor without interpreting it."""

    if metadata is None:
        return None
    return {
        "name": getattr(metadata, "source_name", ""),
        "scope": getattr(metadata, "source_scope", ""),
        "path": getattr(metadata, "source_path", ""),
    }


__all__ = [
    "build_collector_support_payload",
    "build_inverter_support_payload",
    "build_support_fixture",
    "metadata_source_payload",
]
