"""Number platform for writable inverter capabilities."""

from __future__ import annotations

from typing import Any

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .runtime.coordinator import EybondLocalCoordinator
from .models import WriteCapability
from .schema import serialize_capability


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create number entities for numeric write capabilities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver = coordinator.current_driver
    inverter = coordinator.data.inverter
    capabilities = (
        inverter.capabilities if inverter is not None else (driver.write_capabilities if driver is not None else ())
    )
    async_add_entities(
        EybondCapabilityNumber(coordinator, capability)
        for capability in capabilities
        if capability.value_kind in {"scaled_u16", "u16", "u32"}
        if coordinator.can_expose_capability(capability)
    )


class EybondCapabilityNumber(CoordinatorEntity[EybondLocalCoordinator], NumberEntity):
    """One writable numeric capability backed by the driver capability map."""

    _attr_has_entity_name = True
    _attr_mode = NumberMode.BOX
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        capability: WriteCapability,
    ) -> None:
        super().__init__(coordinator)
        self._capability = capability

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_number_{capability.key}"
        self._attr_name = capability.display_name
        self._attr_native_min_value = capability.native_minimum if capability.native_minimum is not None else 0.0
        self._attr_native_max_value = capability.native_maximum if capability.native_maximum is not None else 65535.0
        self._attr_native_step = capability.native_step
        self._attr_entity_registry_enabled_default = capability.enabled_default
        self._attr_native_unit_of_measurement = capability.unit

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if not snapshot.connected or inverter is None:
            return False
        if not any(cap.key == self._capability.key for cap in inverter.capabilities):
            return False
        return self._capability.runtime_state(snapshot.values).visible

    @property
    def native_value(self) -> Any:
        return self.coordinator.data.values.get(self._capability.value_key)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_capability(self._capability, inverter, snapshot.values)

    async def async_set_native_value(self, value: float) -> None:
        await self.coordinator.async_write_capability(self._capability.key, value)
