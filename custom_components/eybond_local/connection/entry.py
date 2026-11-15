"""Branch-aware helpers for serializing connection settings into entries/options."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .branch_registry import get_connection_branch
from .ui import ConnectionFormField
from ..const import (
    CONF_COLLECTOR_IP,
    CONF_DEVICE_CATALOG_ENTRY,
    CONF_DRIVER_HINT,
    DRIVER_HINT_AUTO,
)


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
            if (
                key == CONF_COLLECTOR_IP
                and collector_ip
                and not str(overrides[key] or "").strip()
            ):
                continue
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


def resolve_driver_hint(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> str:
    """Return the driver intent the entry really acts on.

    Both dicts are read because a created entry carries the hint in data
    while an edited one carries it in options. options outranks data; this
    ordering is defined here once so no reader can drift from it, and so a
    value left behind in the losing source stays harmless.
    """

    raw = options.get(CONF_DRIVER_HINT, data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO))
    hint = str(raw or "").strip()
    return hint or DRIVER_HINT_AUTO


def resolve_catalog_entry_key(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> str:
    """Return the explicitly selected experimental map key, if any."""

    raw = options.get(
        CONF_DEVICE_CATALOG_ENTRY,
        data.get(CONF_DEVICE_CATALOG_ENTRY, ""),
    )
    return str(raw or "").strip()


def resolve_effective_catalog_choice(
    data: Mapping[str, object],
    options: Mapping[str, object],
    *,
    driver_hint: str,
) -> str:
    """Return the explicit map that still belongs to this driver choice.

    A stored map key is not a fact on its own: an entry marked
    explicit-selection-only may bind only with the driver its surface
    declares, and startup refuses the pair outright when the effective hint
    names another driver (and unbinds the inverter). Callers that rebuild an
    entry's options therefore keep the map only while this returns it, and
    release it from both sources otherwise. An empty result means no map can
    bind with this driver: nothing is stored, the key no longer resolves in
    the catalog, or it belongs to a different driver.
    """

    key = resolve_catalog_entry_key(data, options)
    if not key:
        return ""
    intent = str(driver_hint or "").strip()
    if not intent or intent == DRIVER_HINT_AUTO:
        # Automatic detection is the opposite of a named map; keeping the key
        # next to it would let a later detection re-bind a map the user had
        # effectively released.
        return ""
    try:
        from ..metadata.compiled_detection_catalog import (
            resolve_explicitly_selected_model_surface,
        )

        resolution = resolve_explicitly_selected_model_surface(key)
    except Exception:
        return ""
    if resolution is None:
        return ""
    _descriptor, surface = resolution
    return key if surface.driver_key == intent else ""


def apply_driver_choice(
    data: dict[str, Any],
    options: dict[str, Any],
    *,
    driver_hint: str,
) -> None:
    """Persist one driver choice in BOTH entry sources.

    Every reader resolves the hint options-then-data, so a choice written to
    only one dict silently re-applies the other copy after a reload: an
    options step could delete its own driver_hint while entry.data still
    pinned a driver, and startup would then run the stale driver instead of
    the automatic default. A real choice therefore writes both copies, and
    the automatic default clears both copies, so auto means the same thing
    to every reader.
    """

    hint = str(driver_hint or "").strip()
    if hint and hint != DRIVER_HINT_AUTO:
        data[CONF_DRIVER_HINT] = hint
        options[CONF_DRIVER_HINT] = hint
    else:
        data.pop(CONF_DRIVER_HINT, None)
        options.pop(CONF_DRIVER_HINT, None)


def apply_catalog_choice(
    data: dict[str, Any],
    options: dict[str, Any],
    *,
    catalog_entry_key: str,
) -> None:
    """Persist, or release, the explicitly selected experimental map.

    The map is an options-level choice, but readers also accept a ``data``
    copy of the key, so releasing it must remove it from both dicts:
    otherwise that documented fallback quietly re-binds a map the user had
    just released.
    """

    key = str(catalog_entry_key or "").strip()
    if key:
        options[CONF_DEVICE_CATALOG_ENTRY] = key
    else:
        options.pop(CONF_DEVICE_CATALOG_ENTRY, None)
    data.pop(CONF_DEVICE_CATALOG_ENTRY, None)
