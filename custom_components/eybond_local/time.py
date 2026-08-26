"""Time platform for typed inverter schedule capabilities."""

from __future__ import annotations

from datetime import time

from homeassistant.components.time import TimeEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .models import WriteCapability
from .platform_context import entity_setup_context
from .runtime.coordinator import EybondLocalCoordinator
from .schema import serialize_capability


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create time entities for typed HH:MM inverter settings."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    capabilities = (
        (
            inverter.capabilities
            if inverter is not None
            else (driver.write_capabilities if driver is not None else ())
        )
        if has_inverter_identity
        else ()
    )
    async_add_entities(
        EybondCapabilityTime(coordinator, capability)
        for capability in capabilities
        if capability.value_kind == "time_hhmm"
        if coordinator.can_expose_capability(capability)
    )


class EybondCapabilityTime(CoordinatorEntity[EybondLocalCoordinator], TimeEntity):
    """One HH:MM capability backed by a packed integer register."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        capability: WriteCapability,
    ) -> None:
        super().__init__(coordinator)
        self._capability = capability
        self._attr_unique_id = (
            f"{coordinator.config_entry.entry_id}_time_{capability.key}"
        )
        self._attr_name = capability.display_name
        self._attr_entity_registry_enabled_default = (
            coordinator.capability_enabled_by_default(capability)
        )

    @property
    def device_info(self):
        return self.coordinator.inverter_device_info()

    @property
    def available(self) -> bool:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if not snapshot.connected or inverter is None:
            return False
        if not any(cap.key == self._capability.key for cap in inverter.capabilities):
            return False
        return self._capability.runtime_state(snapshot.runtime_values()).visible

    @property
    def native_value(self) -> time | None:
        value = self.coordinator.data.runtime_value(self._capability.value_key)
        if type(value) is not str:
            return None
        try:
            return time.fromisoformat(value)
        except ValueError:
            return None

    @property
    def extra_state_attributes(self) -> dict[str, object]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_capability(
            self._capability,
            inverter,
            snapshot.runtime_values(),
        )

    async def async_set_value(self, value: time) -> None:
        await self.coordinator.async_write_capability(
            self._capability.key,
            value.strftime("%H:%M"),
        )
