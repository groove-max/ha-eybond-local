"""Switch platform for writable binary inverter capabilities."""

from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
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
    """Create switch entities for binary write capabilities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver = coordinator.current_driver
    inverter = coordinator.data.inverter
    capabilities = (
        inverter.capabilities if inverter is not None else (driver.write_capabilities if driver is not None else ())
    )
    async_add_entities(
        EybondCapabilitySwitch(coordinator, capability)
        for capability in capabilities
        if capability.value_kind == "bool"
        if coordinator.can_expose_capability(capability)
    )


class EybondCapabilitySwitch(CoordinatorEntity[EybondLocalCoordinator], SwitchEntity):
    """One writable binary capability backed by the driver capability map."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        capability: WriteCapability,
    ) -> None:
        super().__init__(coordinator)
        self._capability = capability

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_switch_{capability.key}"
        self._attr_name = capability.display_name
        self._attr_entity_registry_enabled_default = capability.enabled_default

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
    def is_on(self) -> bool | None:
        value = self.coordinator.data.values.get(self._capability.value_key)
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            enum_map = self._capability.enum_value_map
            on_label = enum_map.get(1)
            off_label = enum_map.get(0)
            if on_label and value == on_label:
                return True
            if off_label and value == off_label:
                return False
            lowered = value.strip().lower()
            if lowered in {"on", "true", "enabled", "enable", "yes"}:
                return True
            if lowered in {"off", "false", "disabled", "disable", "no"}:
                return False
        return None

    @property
    def extra_state_attributes(self) -> dict[str, object]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_capability(self._capability, inverter, snapshot.values)

    async def async_turn_on(self, **kwargs) -> None:
        await self.coordinator.async_write_capability(self._capability.key, True)

    async def async_turn_off(self, **kwargs) -> None:
        await self.coordinator.async_write_capability(self._capability.key, False)
