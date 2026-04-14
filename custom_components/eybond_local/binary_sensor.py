"""Binary sensor platform for EyeBond Local runtime states."""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .runtime.coordinator import EybondLocalCoordinator
from .drivers.registry import binary_sensors_for_driver
from .models import BinarySensorDescription

_DEVICE_CLASS_MAP: dict[str, BinarySensorDeviceClass] = {
    "battery_charging": BinarySensorDeviceClass.BATTERY_CHARGING,
    "power": BinarySensorDeviceClass.POWER,
    "problem": BinarySensorDeviceClass.PROBLEM,
    "running": BinarySensorDeviceClass.RUNNING,
}


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create all known binary sensors."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver = coordinator.current_driver
    driver_key = driver.key if driver is not None else None
    async_add_entities(
        EybondBinaryValueSensor(coordinator, description)
        for description in binary_sensors_for_driver(driver_key)
    )


class EybondBinaryValueSensor(
    CoordinatorEntity[EybondLocalCoordinator],
    BinarySensorEntity,
):
    """One boolean runtime value exposed as a Home Assistant binary sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        description: BinarySensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self._description = description

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_binary_sensor_{description.key}"
        self._attr_name = description.name
        self._attr_icon = description.icon
        self._attr_entity_registry_enabled_default = description.enabled_default

        if description.diagnostic:
            self._attr_entity_category = EntityCategory.DIAGNOSTIC

        if description.device_class:
            device_class = _DEVICE_CLASS_MAP.get(description.device_class)
            if device_class:
                self._attr_device_class = device_class

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        snapshot = self.coordinator.data
        if self._description.key not in snapshot.values:
            return False
        if self._description.live and not snapshot.connected:
            return False
        return True

    @property
    def is_on(self) -> bool | None:
        value = self.coordinator.data.values.get(self._description.key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"on", "true", "yes", "active", "connected"}:
                return True
            if lowered in {"off", "false", "no", "idle", "inactive", "disconnected"}:
                return False
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {
            "value_key": self._description.key,
            "live": self._description.live,
            "diagnostic": self._description.diagnostic,
        }
