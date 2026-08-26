"""Entity-registry reconciliation for one configured runtime."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .collector.signal import is_legacy_disabled_signal_entity_key
from .collector.entity_scope import filter_measurements_for_collector_session
from .const import (
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_CONTROL_MODE,
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    CONTROL_MODE_FULL,
)
from .device_scoped_overlay import filter_learned_read_measurements_for_activation
from .integration_metadata import (
    _collector_cloud_family_for_entity_filter,
    _coordinator_has_inverter_identity,
    _entity_unique_id,
    _preset_unique_id,
    _text_unique_id,
    _tool_unique_id,
)
from .platform_context import entity_setup_context
from .integration_sensor_precision import (
    _async_self_heal_sensor_display_precision,
    _infer_sensor_display_precision,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_DEFAULT_ENABLED_RUNTIME_SELECT_KEYS: tuple[str, ...] = ()

def _is_integration_disabled(disabled_by: object, integration_disabler: object) -> bool:
    """Return whether one entity-registry disabled_by marker means integration-disabled."""

    if disabled_by is None:
        return False

    normalized_disabled_by = str(disabled_by).strip().lower()
    expected = {"integration"}

    normalized_disabler = str(integration_disabler).strip().lower()
    if normalized_disabler:
        expected.add(normalized_disabler)

    disabler_value = getattr(integration_disabler, "value", None)
    if disabler_value is not None:
        normalized_value = str(disabler_value).strip().lower()
        if normalized_value:
            expected.add(normalized_value)

    return normalized_disabled_by in expected


def _default_enabled_unique_ids(entry_id: str) -> set[str]:
    """Return all entity unique_ids that should be enabled by default."""

    from .derived_energy import default_enabled_derived_energy_keys
    from .text import default_enabled_collector_text_keys_for_runtime
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

    for key in default_enabled_collector_text_keys_for_runtime():
        expected.add(_text_unique_id(entry_id, key))

    for key in _DEFAULT_ENABLED_RUNTIME_SELECT_KEYS:
        expected.add(_entity_unique_id(entry_id, "select", key))

    for capability in all_write_capabilities():
        if not capability.enabled_default:
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button", "time"}:
            expected.add(_entity_unique_id(entry_id, entity_kind, capability.key))

    for preset in all_capability_presets():
        if not preset.advanced:
            expected.add(_preset_unique_id(entry_id, preset.key))

    return expected


def _default_enabled_unique_ids_for_current_runtime(
    entry_id: str,
    coordinator,
    driver,
    inverter,
    can_expose_capability,
    can_expose_preset,
    has_inverter_identity: bool | None = None,
) -> set[str]:
    """Return default-enabled unique_ids for the currently detected runtime metadata."""

    from .derived_energy import default_enabled_derived_energy_keys
    from .drivers.registry import binary_sensors_for_runtime, measurements_for_runtime
    from .select import default_enabled_runtime_select_keys_for_runtime
    from .schema import entity_kind_for_capability
    from .text import default_enabled_collector_text_keys_for_runtime
    from .tooling import default_enabled_tooling_button_keys_for_runtime

    driver_key = driver.key if driver is not None else None
    register_schema_name = getattr(inverter, "register_schema_name", "") if inverter is not None else ""
    capabilities = (
        inverter.capabilities
        if inverter is not None
        else (driver.write_capabilities if driver is not None else ())
    )
    capability_keys = {capability.key for capability in capabilities}
    profile_name = getattr(inverter, "profile_name", "") if inverter is not None else ""
    if has_inverter_identity is None:
        has_inverter_identity = _coordinator_has_inverter_identity(coordinator, inverter)
    presets = (
        inverter.capability_presets
        if inverter is not None
        else (driver.capability_presets if driver is not None else ())
    )
    measurement_descriptions = measurements_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        variant_key=(getattr(inverter, "variant_key", "") or None) if inverter is not None else None,
        write_capabilities=capabilities,
        include_all_drivers_when_unknown=False,
        collector_only_mode=not has_inverter_identity,
    )
    measurement_descriptions = filter_learned_read_measurements_for_activation(
        measurement_descriptions,
        entry_data=getattr(getattr(coordinator, "config_entry", None), "data", None),
        entry_options=getattr(getattr(coordinator, "config_entry", None), "options", None),
    )
    measurement_descriptions = filter_measurements_for_collector_session(
        measurement_descriptions,
        getattr(coordinator, "collector_session_protocol", ""),
    )
    binary_sensor_descriptions = binary_sensors_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        include_all_drivers_when_unknown=False,
    )

    expected: set[str] = set()
    collector_cloud_family = _collector_cloud_family_for_entity_filter(
        getattr(coordinator, "config_entry", None),
        coordinator,
    )
    for measurement in measurement_descriptions:
        if is_legacy_disabled_signal_entity_key(measurement.key, collector_cloud_family):
            continue
        if measurement.enabled_default:
            expected.add(_entity_unique_id(entry_id, "sensor", measurement.key))

    for key in default_enabled_derived_energy_keys():
        expected.add(_entity_unique_id(entry_id, "sensor", key))

    for description in binary_sensor_descriptions:
        if description.enabled_default:
            expected.add(_entity_unique_id(entry_id, "binary_sensor", description.key))

    for key in default_enabled_collector_text_keys_for_runtime():
        expected.add(_text_unique_id(entry_id, key))

    for key in default_enabled_runtime_select_keys_for_runtime(
        has_inverter_identity=has_inverter_identity,
    ):
        expected.add(_entity_unique_id(entry_id, "select", key))

    for key in default_enabled_tooling_button_keys_for_runtime(
        capability_keys,
        profile_name,
        has_inverter_identity=has_inverter_identity,
        collector_proxy_capture_allowed=False,
    ):
        expected.add(_tool_unique_id(entry_id, key))

    for capability in capabilities:
        if not capability.enabled_default:
            continue
        if not can_expose_capability(capability):
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button", "time"}:
            expected.add(_entity_unique_id(entry_id, entity_kind, capability.key))

    for preset in presets:
        if preset.advanced:
            continue
        if not can_expose_preset(preset):
            continue
        expected.add(_preset_unique_id(entry_id, preset.key))

    return expected


async def _async_self_heal_enabled_defaults(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> None:
    """Re-enable newly default-enabled entities that were previously auto-disabled."""

    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers.entity_registry import RegistryEntryDisabler

    registry = er.async_get(hass)
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    expected_unique_ids = await hass.async_add_executor_job(
        _default_enabled_unique_ids_for_current_runtime,
        entry.entry_id,
        coordinator,
        driver,
        inverter,
        coordinator.can_expose_capability,
        coordinator.can_expose_preset,
        has_inverter_identity,
    )
    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in expected_unique_ids:
            continue
        if not _is_integration_disabled(
            entity_entry.disabled_by,
            RegistryEntryDisabler.INTEGRATION,
        ):
            continue
        logger.warning(
            "Re-enabling newly default-enabled entity %s for entry %s",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_update_entity(entity_entry.entity_id, disabled_by=None)


async def _async_self_heal_expert_defaults(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Reconcile expert-only entities against the current control mode."""

    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers.entity_registry import RegistryEntryDisabler

    registry = er.async_get(hass)
    coordinator = getattr(entry, "runtime_data", None)
    expose_expert_entities = getattr(coordinator, "control_mode", "") == CONTROL_MODE_FULL
    expert_only_unique_ids: set[str] = {
        _text_unique_id(entry.entry_id, "collector_callback_endpoint"),
    }
    if not expert_only_unique_ids:
        return

    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in expert_only_unique_ids:
            continue
        if expose_expert_entities:
            if not _is_integration_disabled(
                entity_entry.disabled_by,
                RegistryEntryDisabler.INTEGRATION,
            ):
                continue
            logger.warning(
                "Re-enabling full-control expert entity %s for entry %s",
                entity_entry.entity_id,
                entry.entry_id,
            )
            registry.async_update_entity(entity_entry.entity_id, disabled_by=None)
            continue
        if entity_entry.disabled_by is not None:
            continue
        logger.warning(
            "Disabling newly expert-only entity %s for entry %s",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_update_entity(
            entity_entry.entity_id,
            disabled_by=RegistryEntryDisabler.INTEGRATION,
        )


async def _async_remove_legacy_runtime_select_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Remove runtime select entities that were migrated into config flow options."""

    from homeassistant.helpers import entity_registry as er

    registry = er.async_get(hass)
    legacy_unique_ids = {
        _entity_unique_id(entry.entry_id, "select", CONF_CONTROL_MODE),
        # CP2A: the writable collector operation-mode select was removed. The
        # connection strategy is now the single user authority for the transport
        # method; the mode is a read-only projection of it. Remove the already
        # registered entity by its integration-owned unique_id regardless of the
        # gated obsolete-entity cleanup, touching no other entity.
        _entity_unique_id(entry.entry_id, "select", CONF_COLLECTOR_OPERATION_MODE),
    }

    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in legacy_unique_ids:
            continue
        logger.warning(
            "Removing legacy runtime select %s for entry %s after config-flow migration",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_remove(entity_entry.entity_id)


async def _async_finalize_expert_entity_migration(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Run expert-only entity migration after platform setup finishes."""
    await _async_self_heal_expert_defaults(hass, entry)
    if getattr(entry, "runtime_data", None) is not None:
        await _async_remove_legacy_runtime_select_entities(hass, entry)
    await _async_self_heal_sensor_display_precision(hass, entry)


async def _async_cleanup_obsolete_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> None:
    """Remove entity-registry entries that no longer belong to this entry's driver."""
    from homeassistant.helpers import entity_registry as er

    registry = er.async_get(hass)
    menu_owned_proxy_unique_ids = {
        _entity_unique_id(
            entry.entry_id,
            "number",
            CONF_PROXY_CAPTURE_DURATION_MINUTES,
        ),
        _tool_unique_id(entry.entry_id, "start_proxy_capture"),
        _tool_unique_id(entry.entry_id, "stop_proxy_capture"),
    }
    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in menu_owned_proxy_unique_ids:
            continue
        logger.info(
            "Removing obsolete proxy control entity %s for entry %s; "
            "proxy actions and duration are owned by the options flow",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_remove(entity_entry.entity_id)

    cleanup_allowed, cleanup_reason = _cleanup_obsolete_entities_allowed(coordinator)
    if not cleanup_allowed:
        logger.debug(
            "Skipping obsolete entity cleanup for entry %s: %s",
            entry.entry_id,
            cleanup_reason,
        )
        return

    from .button import _tooling_button_specs
    from .derived_energy import (
        derived_energy_cycle_descriptions_for_keys,
        derived_energy_descriptions_for_keys,
        derived_energy_entity_descriptions_for_keys,
    )
    from .drivers.registry import binary_sensors_for_runtime, measurements_for_runtime
    from .select import runtime_select_keys_for_runtime
    from .schema import entity_kind_for_capability
    from .text import collector_text_keys_for_runtime
    from .tooling import tooling_button_keys_for_runtime

    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    driver_key = driver.key if driver is not None else None
    register_schema_name = getattr(inverter, "register_schema_name", "") if inverter is not None else ""
    capabilities = (
        inverter.capabilities
        if inverter is not None
        else (driver.write_capabilities if driver is not None else ())
    )
    capability_keys = {capability.key for capability in capabilities}
    profile_name = getattr(inverter, "profile_name", "") if inverter is not None else ""
    presets = (
        inverter.capability_presets
        if inverter is not None
        else (driver.capability_presets if driver is not None else ())
    )
    measurement_descriptions = measurements_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        variant_key=(getattr(inverter, "variant_key", "") or None) if inverter is not None else None,
        write_capabilities=capabilities,
        include_all_drivers_when_unknown=False,
        collector_only_mode=not has_inverter_identity,
    )
    measurement_descriptions = filter_learned_read_measurements_for_activation(
        measurement_descriptions,
        entry_data=getattr(entry, "data", None),
        entry_options=getattr(entry, "options", None),
    )
    measurement_descriptions = filter_measurements_for_collector_session(
        measurement_descriptions,
        getattr(coordinator, "collector_session_protocol", ""),
    )
    binary_sensor_descriptions = binary_sensors_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        include_all_drivers_when_unknown=False,
    )
    measurement_keys = {description.key for description in measurement_descriptions}
    runtime_keys = measurement_keys | {
        description.key for description in binary_sensor_descriptions
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
        if not is_legacy_disabled_signal_entity_key(
            description.key,
            _collector_cloud_family_for_entity_filter(entry, coordinator),
        )
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
        for description in binary_sensor_descriptions
    )
    expected_unique_ids.update(
        _tool_unique_id(entry.entry_id, key)
        for key in tooling_button_keys_for_runtime(
            capability_keys,
            profile_name,
            has_inverter_identity=has_inverter_identity,
            collector_proxy_capture_allowed=False,
        )
    )
    expected_unique_ids.update(
        _text_unique_id(entry.entry_id, key)
        for key in collector_text_keys_for_runtime()
    )
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "select", key)
        for key in runtime_select_keys_for_runtime(
            has_inverter_identity=has_inverter_identity,
        )
    )
    for capability in capabilities:
        if not coordinator.can_expose_capability(capability):
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button", "time"}:
            expected_unique_ids.add(_entity_unique_id(entry.entry_id, entity_kind, capability.key))
    for preset in presets:
        if not coordinator.can_expose_preset(preset):
            continue
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


def _cleanup_obsolete_entities_allowed(coordinator) -> tuple[bool, str]:
    """Return whether destructive cleanup can safely run for current metadata state."""

    if getattr(coordinator, "identified_inverter", None) is not None:
        return True, "live_inverter_identity"

    snapshot = getattr(coordinator, "effective_metadata_snapshot", None)
    if snapshot is None or not bool(getattr(snapshot, "is_valid", False)):
        return False, "missing_valid_effective_metadata_snapshot"

    effective_metadata = getattr(coordinator, "effective_metadata", None)
    if effective_metadata is None:
        return False, "effective_metadata_unavailable"

    effective_owner_key = str(getattr(effective_metadata, "effective_owner_key", "") or "").strip()
    effective_profile_name = str(getattr(effective_metadata, "profile_name", "") or "").strip()
    effective_register_schema_name = str(
        getattr(effective_metadata, "register_schema_name", "") or ""
    ).strip()
    if not (effective_owner_key and effective_profile_name and effective_register_schema_name):
        return False, "effective_metadata_incomplete"

    snapshot_owner_key = str(getattr(snapshot, "effective_owner_key", "") or "").strip()
    snapshot_profile_name = str(getattr(snapshot, "profile_name", "") or "").strip()
    snapshot_register_schema_name = str(
        getattr(snapshot, "register_schema_name", "") or ""
    ).strip()
    if (
        effective_owner_key != snapshot_owner_key
        or effective_profile_name != snapshot_profile_name
        or effective_register_schema_name != snapshot_register_schema_name
    ):
        return False, "effective_metadata_mismatch_from_snapshot"

    profile_metadata = getattr(effective_metadata, "profile_metadata", None)
    register_schema_metadata = getattr(effective_metadata, "register_schema_metadata", None)
    if profile_metadata is None or register_schema_metadata is None:
        return False, "effective_metadata_assets_unresolved"

    return True, "snapshot_metadata_consistent"
