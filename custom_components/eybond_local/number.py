"""Number platform for writable inverter capabilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    MAX_PROXY_CAPTURE_DURATION_MINUTES,
    MIN_PROXY_CAPTURE_DURATION_MINUTES,
)
from .runtime.coordinator import EybondLocalCoordinator
from .models import WriteCapability
from .platform_context import entity_setup_context
from .schema import serialize_capability


@dataclass(frozen=True, slots=True)
class _RuntimeNumberSpec:
    key: str
    name: str
    minimum: float
    maximum: float
    step: float
    unit: str
    icon: str
    enabled_default: bool = True


def _runtime_number_specs(coordinator: EybondLocalCoordinator) -> tuple[_RuntimeNumberSpec, ...]:
    if not hasattr(coordinator, "async_set_proxy_capture_duration_minutes"):
        return ()
    collector_capabilities = getattr(coordinator, "collector_capabilities", None)
    if not bool(getattr(collector_capabilities, "proxy_capture", True)):
        return ()
    return (
        _RuntimeNumberSpec(
            key=CONF_PROXY_CAPTURE_DURATION_MINUTES,
            name="Proxy Mode Duration",
            minimum=float(MIN_PROXY_CAPTURE_DURATION_MINUTES),
            maximum=float(MAX_PROXY_CAPTURE_DURATION_MINUTES),
            step=1.0,
            unit="min",
            icon="mdi:timer-cog-outline",
        ),
    )


def default_enabled_runtime_number_keys_for_runtime(
    coordinator: EybondLocalCoordinator,
) -> set[str]:
    """Return default-enabled runtime number keys for entity self-healing."""

    return {spec.key for spec in _runtime_number_specs(coordinator) if spec.enabled_default}


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create number entities for numeric write capabilities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    # Without an inverter identity, capability entities would describe a
    # phantom inverter on the collector device (manual driver hint, nothing
    # attached yet). The entry reloads once detection persists an identity.
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
        [
            *(
                EybondCapabilityNumber(coordinator, capability)
                for capability in capabilities
                if capability.value_kind in {"scaled_u16", "u16", "u32"}
                if coordinator.can_expose_capability(capability)
            ),
            *(EybondRuntimeSettingNumber(coordinator, spec) for spec in _runtime_number_specs(coordinator)),
        ]
    )


class EybondRuntimeSettingNumber(CoordinatorEntity[EybondLocalCoordinator], NumberEntity):
    """One writable numeric runtime setting owned by the collector."""

    _attr_has_entity_name = True
    _attr_mode = NumberMode.BOX
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator: EybondLocalCoordinator, spec: _RuntimeNumberSpec) -> None:
        super().__init__(coordinator)
        self._spec = spec
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_number_{spec.key}"
        self._attr_name = spec.name
        self._attr_translation_key = spec.key
        self._attr_native_min_value = spec.minimum
        self._attr_native_max_value = spec.maximum
        self._attr_native_step = spec.step
        self._attr_native_unit_of_measurement = spec.unit
        self._attr_icon = spec.icon
        self._attr_entity_registry_enabled_default = spec.enabled_default

    @property
    def device_info(self):
        return self.coordinator.collector_device_info()

    @property
    def available(self) -> bool:
        return self.coordinator.proxy_capture_duration_availability_reason() is None

    @property
    def native_value(self) -> float:
        return float(self.coordinator.proxy_capture_display_duration_minutes)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {
            "availability_reason": self.coordinator.proxy_capture_duration_availability_reason(),
            "configured_duration_minutes": self.coordinator.proxy_capture_configured_duration_minutes,
            "remaining_seconds": self.coordinator.proxy_capture_remaining_seconds,
            "remaining_minutes": self.coordinator.proxy_capture_remaining_minutes,
        }

    async def async_set_native_value(self, value: float) -> None:
        await self.coordinator.async_set_proxy_capture_duration_minutes(value)


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
        self._attr_entity_registry_enabled_default = coordinator.capability_enabled_by_default(
            capability
        )
        self._attr_native_unit_of_measurement = capability.unit

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
