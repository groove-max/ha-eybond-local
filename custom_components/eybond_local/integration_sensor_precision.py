"""Late sensor display-precision reconciliation after runtime values exist."""

from __future__ import annotations

from math import isfinite
from typing import TYPE_CHECKING

from .platform_context import entity_setup_context

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant


_FLOAT_PRECISION_DEVICE_CLASSES = {
    "current",
    "frequency",
    "temperature",
    "voltage",
}


def _infer_sensor_display_precision(value: float) -> int | None:
    """Infer a stable display precision for one float-like sensor value."""

    if not isfinite(value):
        return None
    if value.is_integer():
        return 1
    text = format(value, ".6f").rstrip("0")
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1])


async def _async_self_heal_sensor_display_precision(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Repair stale zero-precision sensor overrides after runtime values are known."""

    coordinator = getattr(entry, "runtime_data", None)
    if coordinator is None:
        return

    from homeassistant.helpers import entity_registry as er

    from .drivers.registry import measurements_for_runtime

    registry = er.async_get(hass)
    update_entity_options = getattr(registry, "async_update_entity_options", None)
    if not callable(update_entity_options):
        return

    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    driver_key = driver.key if driver is not None else None
    register_schema_name = (
        getattr(inverter, "register_schema_name", "") if inverter is not None else ""
    )
    write_capabilities = (
        inverter.capabilities
        if inverter is not None
        else (driver.write_capabilities if driver is not None else ())
    )
    descriptions_by_key = {
        description.key: description
        for description in measurements_for_runtime(
            driver_key=driver_key,
            register_schema_name=register_schema_name,
            variant_key=(getattr(inverter, "variant_key", "") or None)
            if inverter is not None
            else None,
            write_capabilities=write_capabilities,
            include_all_drivers_when_unknown=False,
            collector_only_mode=not has_inverter_identity,
        )
    }
    unique_id_prefix = f"{entry.entry_id}_"

    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        entity_id = getattr(entity_entry, "entity_id", None)
        unique_id = str(getattr(entity_entry, "unique_id", "") or "")
        if not entity_id or not unique_id.startswith(unique_id_prefix):
            continue

        description = descriptions_by_key.get(unique_id[len(unique_id_prefix) :])
        if description is None:
            continue

        desired_precision = description.suggested_display_precision
        if (
            desired_precision is None
            and description.device_class in _FLOAT_PRECISION_DEVICE_CLASSES
        ):
            native_value = coordinator.data.runtime_value(description.key)
            if isinstance(native_value, float):
                desired_precision = _infer_sensor_display_precision(native_value)
        if desired_precision is None:
            continue

        options = dict(getattr(entity_entry, "options", {}) or {})
        sensor_options = dict(options.get("sensor") or {})
        current_precision = sensor_options.get("suggested_display_precision")
        if current_precision == desired_precision:
            continue
        if current_precision not in (None, 0):
            continue

        sensor_options["suggested_display_precision"] = desired_precision
        update_entity_options(entity_id, "sensor", sensor_options)
