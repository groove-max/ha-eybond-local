"""Device-scope routing helpers for collector-facing entities."""

from __future__ import annotations


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


def is_collector_entity_key(key: str) -> bool:
    """Return whether one entity key belongs to the collector device scope."""

    normalized = str(key or "").strip()
    return normalized.startswith("collector_") or normalized in _EXPLICIT_COLLECTOR_ENTITY_KEYS


def is_collector_tooling_key(key: str) -> bool:
    """Return whether one tooling action belongs to the collector device scope."""

    return str(key or "").strip() in _COLLECTOR_TOOLING_KEYS
