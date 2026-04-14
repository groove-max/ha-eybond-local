"""Select platform for writable enum inverter capabilities."""

from __future__ import annotations

from homeassistant.components.select import SelectEntity
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
    """Create select entities for enum write capabilities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver = coordinator.current_driver
    inverter = coordinator.data.inverter
    capabilities = (
        inverter.capabilities if inverter is not None else (driver.write_capabilities if driver is not None else ())
    )
    async_add_entities(
        EybondCapabilitySelect(coordinator, capability)
        for capability in capabilities
        if capability.value_kind == "enum" and capability.enum_value_map
        if coordinator.can_expose_capability(capability)
    )


class EybondCapabilitySelect(CoordinatorEntity[EybondLocalCoordinator], SelectEntity):
    """One writable enum capability backed by the driver capability map."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG
    _attr_entity_registry_enabled_default = False

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        capability: WriteCapability,
    ) -> None:
        super().__init__(coordinator)
        self._capability = capability

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_select_{capability.key}"
        self._attr_name = capability.display_name
        self._attr_options = capability.enum_options
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
    def current_option(self) -> str | None:
        value = self.coordinator.data.values.get(self._capability.value_key)
        return value if isinstance(value, str) else None

    @property
    def extra_state_attributes(self) -> dict[str, object]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_capability(self._capability, inverter, snapshot.values)

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_write_capability(self._capability.key, option)
