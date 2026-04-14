"""Branch-aware helpers for serializing connection settings into entries/options."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .branch_registry import get_connection_branch
from .ui import ConnectionFormField
from ..const import CONF_COLLECTOR_IP, CONF_DRIVER_HINT


def _ordered_unique_field_keys(fields: tuple[ConnectionFormField, ...]) -> tuple[str, ...]:
    keys: list[str] = []
    seen: set[str] = set()
    for field in fields:
        if field.key in seen:
            continue
        seen.add(field.key)
        keys.append(field.key)
    return tuple(keys)


def persisted_branch_setting_keys(connection_type: str) -> tuple[str, ...]:
    """Return the persisted branch settings for one connection type."""

    branch = get_connection_branch(connection_type)
    return _ordered_unique_field_keys(
        branch.form_layout.manual_fields
        + branch.form_layout.manual_advanced_fields
        + branch.form_layout.runtime_fields
    )


def runtime_option_setting_keys(connection_type: str) -> tuple[str, ...]:
    """Return runtime-option keys for one connection type."""

    branch = get_connection_branch(connection_type)
    return _ordered_unique_field_keys(branch.form_layout.runtime_fields)


def build_detected_entry_settings(
    connection_type: str,
    *,
    server_ip: str,
    collector_ip: str = "",
    default_broadcast: str,
    overrides: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    """Build persisted branch settings for one autodetected entry."""

    branch = get_connection_branch(connection_type)
    values = branch.build_auto_values(
        server_ip=server_ip,
        default_broadcast=default_broadcast,
    )
    if collector_ip and CONF_COLLECTOR_IP in persisted_branch_setting_keys(connection_type):
        values[CONF_COLLECTOR_IP] = collector_ip
    for key in persisted_branch_setting_keys(connection_type):
        if overrides is not None and key in overrides:
            values[key] = overrides[key]
    return {
        key: values[key]
        for key in persisted_branch_setting_keys(connection_type)
        if key in values
    }


def build_manual_entry_settings(
    connection_type: str,
    values: Mapping[str, object],
) -> dict[str, Any]:
    """Build persisted branch settings for one manual entry."""

    return {
        key: values[key]
        for key in persisted_branch_setting_keys(connection_type)
        if key in values
    }


def build_runtime_option_settings(
    connection_type: str,
    values: Mapping[str, object],
) -> dict[str, Any]:
    """Build persisted runtime option settings for one branch."""

    return {
        key: values[key]
        for key in runtime_option_setting_keys(connection_type)
        if key in values
    }


def with_driver_hint(
    values: Mapping[str, object],
    *,
    driver_hint: str,
) -> dict[str, Any]:
    """Return a copy with the persisted driver hint overridden."""

    merged = dict(values)
    if CONF_DRIVER_HINT in merged or driver_hint:
        merged[CONF_DRIVER_HINT] = driver_hint
    return merged
