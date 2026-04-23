"""Runtime helpers for integration-level tooling entities."""

from __future__ import annotations

from collections.abc import Collection

_ALWAYS_AVAILABLE_TOOLING_KEYS = (
    "create_support_package",
    "export_support_bundle",
    "reload_local_metadata",
    "create_local_profile_draft",
    "create_local_schema_draft",
)
_CLOCK_SYNC_PROFILE_NAMES = frozenset({"modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"})
_CLOCK_SYNC_CAPABILITY_KEYS = frozenset({"inverter_date_write", "inverter_time_write"})


def supports_clock_sync(capability_keys: Collection[str], profile_name: str = "") -> bool:
    """Return whether the current runtime exposes clock-write capabilities."""

    if profile_name and profile_name not in _CLOCK_SYNC_PROFILE_NAMES:
        return False
    return _CLOCK_SYNC_CAPABILITY_KEYS.issubset(set(capability_keys))


def tooling_button_keys_for_runtime(
    capability_keys: Collection[str],
    profile_name: str = "",
) -> tuple[str, ...]:
    """Return integration-level tooling buttons that belong to the current runtime."""

    keys = list(_ALWAYS_AVAILABLE_TOOLING_KEYS)
    if supports_clock_sync(capability_keys, profile_name):
        keys.append("sync_inverter_clock")
    return tuple(keys)


def default_enabled_tooling_button_keys_for_runtime(
    capability_keys: Collection[str],
    profile_name: str = "",
) -> tuple[str, ...]:
    """Return tooling buttons that should be enabled by default for the current runtime."""

    keys = ["create_support_package"]
    if supports_clock_sync(capability_keys, profile_name):
        keys.append("sync_inverter_clock")
    return tuple(keys)