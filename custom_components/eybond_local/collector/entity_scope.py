"""Device-scope routing helpers for collector-facing entities."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TypeVar


_EXPLICIT_COLLECTOR_ENTITY_KEYS: frozenset[str] = frozenset(
    {
        "configured_collector_ip",
        "smartess_collector_version",
        "smartess_protocol_raw_id",
        "smartess_protocol_asset_id",
        "smartess_protocol_asset_name",
        "smartess_protocol_suffix",
        "smartess_protocol_profile_key",
        "smartess_protocol_name",
        "smartess_device_address",
        "runtime_driver_state",
        # Poll-pipeline debugging sensors: they describe the collector's
        # polling machinery, not the inverter.
        "runtime_refresh_phase_breakdown",
        "driver_slow_requests",
        "driver_unsupported_commands",
    }
)

_COLLECTOR_TOOLING_KEYS: frozenset[str] = frozenset(
    {
        "bind_collector_to_home_assistant",
        "apply_collector_changes",
        "rediscover_collector",
        "reboot_collector",
        "rollback_collector_server_endpoint",
        "start_proxy_capture",
        "stop_proxy_capture",
    }
)

# These diagnostics are populated exclusively by the framed EyeBond heartbeat
# and frame header.  A confirmed plain AT callback stream has no such envelope,
# so registering them there can only create permanently-unavailable entities.
_FRAMED_HEARTBEAT_ENTITY_KEYS: frozenset[str] = frozenset(
    {
        "collector_profile",
        "collector_profile_key",
        "collector_heartbeat_devcode",
        "collector_heartbeat_payload",
        "collector_heartbeat_age_seconds",
        "collector_heartbeat_ascii",
        "collector_heartbeat_payload_len",
        "collector_heartbeat_format",
        "collector_heartbeat_suffix",
        "collector_heartbeat_suffix_kind",
        "collector_heartbeat_suffix_uint",
        "collector_devcode_major",
        "collector_devcode_minor",
        "collector_devcode",
        "collector_last_frame_devcode",
    }
)

_DescriptionT = TypeVar("_DescriptionT")


def is_collector_entity_key(key: str) -> bool:
    """Return whether one entity key belongs to the collector device scope."""

    normalized = str(key or "").strip()
    return normalized.startswith("collector_") or normalized in _EXPLICIT_COLLECTOR_ENTITY_KEYS


def is_collector_tooling_key(key: str) -> bool:
    """Return whether one tooling action belongs to the collector device scope."""

    return str(key or "").strip() in _COLLECTOR_TOOLING_KEYS


def filter_measurements_for_collector_session(
    descriptions: Iterable[_DescriptionT],
    session_protocol: object,
) -> tuple[_DescriptionT, ...]:
    """Drop diagnostics that cannot exist on the confirmed collector wire.

    Unknown or malformed protocols deliberately retain the full inventory: UI
    reconciliation must never delete an entity merely because wire authority is
    temporarily unavailable.  Only the exact confirmed ``at_text`` protocol has
    a closed set of impossible framed-heartbeat fields.
    """

    items = tuple(descriptions)
    if type(session_protocol) is not str or session_protocol != "at_text":
        return items
    return tuple(
        item
        for item in items
        if getattr(item, "key", None) not in _FRAMED_HEARTBEAT_ENTITY_KEYS
    )
