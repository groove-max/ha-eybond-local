"""EyeBond Local integration."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import homeassistant.helpers.config_validation as cv
except ModuleNotFoundError:  # Local tooling imports the package without Home Assistant installed.
    cv = None

from .const import CONF_CONNECTION_TYPE, CONF_SERVER_IP, CONNECTION_TYPE_EYBOND, PLATFORMS
from .metadata.profile_loader import set_external_profile_roots
from .metadata.register_schema_loader import set_external_register_schema_roots
from .runtime.link import resolve_server_ip
from .services import async_setup_services

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

CONFIG_SCHEMA: Any = (
    cv.config_entry_only_config_schema("eybond_local")
    if cv is not None
    else None
)


def _configure_local_metadata_roots(hass: HomeAssistant) -> None:
    """Configure external profile/schema roots under the HA config directory."""

    custom_root = Path(hass.config.path("eybond_local")).resolve()
    set_external_profile_roots((custom_root / "profiles",))
    set_external_register_schema_roots((custom_root / "register_schemas",))


def _prime_metadata_caches() -> None:
    """Warm metadata loaders so async startup paths do not hit disk directly."""

    from .drivers.registry import prime_metadata_caches

    prime_metadata_caches()


async def async_setup(hass: HomeAssistant, _config: dict) -> bool:
    """Initialize shared loader state for the integration."""

    _configure_local_metadata_roots(hass)
    await hass.async_add_executor_job(_prime_metadata_caches)
    await async_setup_services(hass)
    return True


async def _async_self_heal_server_ip(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Persist a valid local listener IP if the stored one has gone stale."""

    if entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND) != CONNECTION_TYPE_EYBOND:
        return

    configured_server_ip = entry.options.get(
        CONF_SERVER_IP,
        entry.data.get(CONF_SERVER_IP, ""),
    )
    resolved_server_ip = await hass.async_add_executor_job(
        resolve_server_ip,
        configured_server_ip,
    )
    if not resolved_server_ip or resolved_server_ip == configured_server_ip:
        return

    data = dict(entry.data)
    options = dict(entry.options)
    changed = False

    if data.get(CONF_SERVER_IP) != resolved_server_ip:
        data[CONF_SERVER_IP] = resolved_server_ip
        changed = True
    if CONF_SERVER_IP in options and options.get(CONF_SERVER_IP) != resolved_server_ip:
        options[CONF_SERVER_IP] = resolved_server_ip
        changed = True

    if not changed:
        return

    logger.warning(
        "Healing stale EyeBond server_ip from %s to %s for entry %s",
        configured_server_ip,
        resolved_server_ip,
        entry.entry_id,
    )
    hass.config_entries.async_update_entry(
        entry,
        data=data,
        options=options,
    )


def _entity_unique_id(entry_id: str, domain: str, key: str) -> str:
    """Return the unique_id format used by one HA entity platform."""

    if domain == "sensor":
        return f"{entry_id}_{key}"
    return f"{entry_id}_{domain}_{key}"


def _preset_unique_id(entry_id: str, key: str) -> str:
    """Return the unique_id format used by preset buttons."""

    return f"{entry_id}_preset_{key}"


def _tool_unique_id(entry_id: str, key: str) -> str:
    """Return the unique_id format used by tooling buttons."""

    return f"{entry_id}_tool_{key}"


def _default_enabled_unique_ids(entry_id: str) -> set[str]:
    """Return all entity unique_ids that should be enabled by default."""

    from .derived_energy import default_enabled_derived_energy_keys
    from .drivers.registry import (
        all_binary_sensors,
        all_capability_presets,
        all_measurements,
        all_write_capabilities,
    )
    from .schema import entity_kind_for_capability

    expected: set[str] = set()
    for measurement in all_measurements():
        if measurement.enabled_default:
            expected.add(_entity_unique_id(entry_id, "sensor", measurement.key))

    for key in default_enabled_derived_energy_keys():
        expected.add(_entity_unique_id(entry_id, "sensor", key))

    for description in all_binary_sensors():
        if description.enabled_default:
            expected.add(_entity_unique_id(entry_id, "binary_sensor", description.key))

    for capability in all_write_capabilities():
        if not capability.enabled_default:
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button"}:
            expected.add(_entity_unique_id(entry_id, entity_kind, capability.key))

    for preset in all_capability_presets():
        if not preset.advanced:
            expected.add(_preset_unique_id(entry_id, preset.key))

    return expected


async def _async_self_heal_enabled_defaults(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Re-enable newly default-enabled entities that were previously auto-disabled."""

    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers.entity_registry import RegistryEntryDisabler

    registry = er.async_get(hass)
    expected_unique_ids = await hass.async_add_executor_job(
        _default_enabled_unique_ids,
        entry.entry_id,
    )
    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in expected_unique_ids:
            continue
        if entity_entry.disabled_by != RegistryEntryDisabler.INTEGRATION:
            continue
        logger.warning(
            "Re-enabling newly default-enabled entity %s for entry %s",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_update_entity(entity_entry.entity_id, disabled_by=None)


async def _async_cleanup_obsolete_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> None:
    """Remove entity-registry entries that no longer belong to this entry's driver."""

    from homeassistant.helpers import entity_registry as er

    from .button import _tooling_button_specs
    from .derived_energy import (
        derived_energy_cycle_descriptions_for_keys,
        derived_energy_descriptions_for_keys,
        derived_energy_entity_descriptions_for_keys,
    )
    from .drivers.registry import binary_sensors_for_driver, measurements_for_driver
    from .schema import entity_kind_for_capability

    registry = er.async_get(hass)
    driver = coordinator.current_driver
    driver_key = driver.key if driver is not None else None
    measurement_descriptions = measurements_for_driver(driver_key)
    measurement_keys = {description.key for description in measurement_descriptions}
    runtime_keys = measurement_keys | {
        description.key for description in binary_sensors_for_driver(driver_key)
    }
    derived_energy_source_descriptions = derived_energy_descriptions_for_keys(
        measurement_keys
    )
    derived_energy_descriptions = derived_energy_entity_descriptions_for_keys(
        measurement_keys
    )
    expected_unique_ids: set[str] = {
        _entity_unique_id(entry.entry_id, "sensor", description.key)
        for description in measurement_descriptions
    }
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "sensor", description.key)
        for description in derived_energy_descriptions
    )
    derived_energy_keys = {
        description.key
        for description in derived_energy_source_descriptions
    }
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "sensor", description.key)
        for description in derived_energy_cycle_descriptions_for_keys(
            runtime_keys | derived_energy_keys
        )
    )
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "binary_sensor", description.key)
        for description in binary_sensors_for_driver(driver_key)
    )
    expected_unique_ids.update(
        _tool_unique_id(entry.entry_id, spec.key)
        for spec in _tooling_button_specs()
    )

    inverter = coordinator.data.inverter
    capabilities = inverter.capabilities if inverter is not None else (driver.write_capabilities if driver is not None else ())
    presets = inverter.capability_presets if inverter is not None else (driver.capability_presets if driver is not None else ())
    for capability in capabilities:
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button"}:
            expected_unique_ids.add(_entity_unique_id(entry.entry_id, entity_kind, capability.key))
    for preset in presets:
        expected_unique_ids.add(_preset_unique_id(entry.entry_id, preset.key))

    removable = []
    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id in expected_unique_ids:
            continue
        removable.append(entity_entry.entity_id)

    for entity_id in removable:
        logger.warning(
            "Removing obsolete entity %s for entry %s after driver-specific metadata refresh",
            entity_id,
            entry.entry_id,
        )
        registry.async_remove(entity_id)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up EyeBond Local from a config entry."""

    from .runtime.coordinator import EybondLocalCoordinator

    _configure_local_metadata_roots(hass)
    await hass.async_add_executor_job(_prime_metadata_caches)
    await _async_self_heal_server_ip(hass, entry)
    await _async_self_heal_enabled_defaults(hass, entry)
    coordinator = EybondLocalCoordinator(hass, entry)
    await coordinator.async_setup()
    entry.runtime_data = coordinator
    await coordinator.async_refresh()
    await _async_cleanup_obsolete_entities(hass, entry, coordinator)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    coordinator.async_sync_device_registry()
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""

    from .runtime.coordinator import EybondLocalCoordinator

    coordinator: EybondLocalCoordinator = entry.runtime_data
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        await coordinator.async_shutdown()
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry after options changes."""

    await hass.config_entries.async_reload(entry.entry_id)
