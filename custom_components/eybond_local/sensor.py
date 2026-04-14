"""Sensor platform for EyeBond Local."""

from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .runtime.coordinator import EybondLocalCoordinator
from .derived_energy import (
    DerivedEnergyCycleDescription,
    DerivedEnergyDescription,
    compute_derived_power,
    derived_energy_cycle_descriptions_for_keys,
    derived_energy_descriptions_for_keys,
    derived_energy_entity_descriptions_for_keys,
)
from .drivers.registry import measurements_for_driver
from .drivers.registry import binary_sensors_for_driver
from .energy import CyclingEnergyAccumulator, EnergyAccumulator
from .models import MeasurementDescription

_DEVICE_CLASS_MAP: dict[str, SensorDeviceClass] = {
    "battery": SensorDeviceClass.BATTERY,
    "current": SensorDeviceClass.CURRENT,
    "energy": SensorDeviceClass.ENERGY,
    "frequency": SensorDeviceClass.FREQUENCY,
    "power": SensorDeviceClass.POWER,
    "temperature": SensorDeviceClass.TEMPERATURE,
    "voltage": SensorDeviceClass.VOLTAGE,
}

_STATE_CLASS_MAP: dict[str, SensorStateClass] = {
    "measurement": SensorStateClass.MEASUREMENT,
    "total": SensorStateClass.TOTAL,
    "total_increasing": SensorStateClass.TOTAL_INCREASING,
}

_FLOAT_PRECISION_DEVICE_CLASSES = {
    "current",
    "frequency",
    "temperature",
    "voltage",
}

_SUMMARY_ATTRIBUTE_MAP: dict[str, tuple[tuple[str, str], ...]] = {
    "operational_state": (
        ("site_mode", "site_mode_state"),
        ("power_flow", "power_flow_summary"),
        ("charging_policy", "charge_source_policy_state"),
        ("protection_status", "protection_state"),
        ("alarm_context", "alarm_context_state"),
        ("load_supply", "load_supply_state"),
        ("grid_assist", "grid_assist_state"),
        ("warnings_active", "warning_active"),
        ("fault_active", "fault_active"),
    ),
    "site_mode_state": (
        ("system_status", "operational_state"),
        ("power_flow", "power_flow_summary"),
        ("charging_policy", "charge_source_policy_state"),
        ("protection_status", "protection_state"),
        ("alarm_context", "alarm_context_state"),
        ("grid_assist", "grid_assist_state"),
        ("warnings_active", "warning_active"),
        ("fault_active", "fault_active"),
    ),
    "power_flow_summary": (
        ("load_supply", "load_supply_state"),
        ("battery_role", "battery_role_state"),
        ("pv_role", "pv_role_state"),
        ("utility_role", "utility_role_state"),
        ("charging_policy", "charge_source_policy_state"),
        ("charging_source", "charging_source_state"),
        ("grid_direction", "grid_power_direction"),
        ("battery_direction", "battery_power_direction"),
        ("output_state", "output_state"),
        ("pv_state", "pv_state"),
    ),
    "protection_state": (
        ("alarm_context", "alarm_context_state"),
        ("active_warning_count", "warning_count"),
        ("active_fault_count", "fault_count"),
        ("warning_code", "warning_code"),
        ("fault_code", "fault_code"),
        ("warning_details", "warning_descriptions"),
        ("fault_details", "fault_descriptions"),
        ("battery_protection_active", "battery_protection_active"),
        ("grid_warning_active", "grid_warning_active"),
        ("pv_warning_active", "pv_warning_active"),
        ("thermal_warning_active", "thermal_warning_active"),
        ("load_protection_active", "load_protection_active"),
    ),
    "charge_source_policy_state": (
        ("charging_source", "charging_source_state"),
        ("charging_settings", "charging_settings_state"),
        ("charging_active", "charging_active"),
        ("utility_charging_allowed", "utility_charging_allowed"),
        ("pv_only_charging", "pv_only_charging"),
    ),
    "charging_settings_state": (
        ("charging_policy", "charge_source_policy_state"),
        ("charging_source", "charging_source_state"),
        ("charging_active", "charging_active"),
        ("utility_charging_allowed", "utility_charging_allowed"),
        ("pv_only_charging", "pv_only_charging"),
    ),
    "battery_settings_state": (
        ("battery_connected", "battery_connected"),
        ("battery_equalization_enabled", "battery_equalization_enabled"),
        ("configuration_safe_mode", "configuration_safe_mode"),
    ),
    "output_settings_state": (
        ("operating_mode", "operating_mode"),
        ("configuration_safe_mode", "configuration_safe_mode"),
        ("remote_control_enabled", "remote_control_enabled"),
    ),
}


def _infer_float_display_precision(value: float) -> int | None:
    if not isfinite(value):
        return None
    if value.is_integer():
        return 1
    text = format(value, ".6f").rstrip("0")
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1])

async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create all known sensor entities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
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
    sensor_entities = [
        EybondValueSensor(coordinator, description)
        for description in measurement_descriptions
    ]
    derived_energy_entities = [
        EybondDerivedEnergySensor(coordinator, description)
        for description in derived_energy_descriptions
    ]
    derived_energy_by_key = {
        description.key: description
        for description in derived_energy_source_descriptions
    }
    derived_energy_cycle_entities = [
        EybondDerivedEnergyCycleSensor(
            coordinator,
            cycle_description,
            derived_energy_by_key[cycle_description.source_key],
        )
        for cycle_description in derived_energy_cycle_descriptions_for_keys(
            runtime_keys | set(derived_energy_by_key)
        )
        if cycle_description.source_key in derived_energy_by_key
    ]
    async_add_entities([
        *sensor_entities,
        *derived_energy_entities,
        *derived_energy_cycle_entities,
    ])


class EybondValueSensor(CoordinatorEntity[EybondLocalCoordinator], SensorEntity):
    """A single decoded transport or inverter value."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        description: MeasurementDescription,
    ) -> None:
        super().__init__(coordinator)
        self._description = description

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_{description.key}"
        self._attr_name = description.name
        self._attr_native_unit_of_measurement = description.unit
        self._attr_icon = description.icon
        self._attr_entity_registry_enabled_default = description.enabled_default

        if description.diagnostic:
            self._attr_entity_category = EntityCategory.DIAGNOSTIC

        if description.device_class:
            device_class = _DEVICE_CLASS_MAP.get(description.device_class)
            if device_class:
                self._attr_device_class = device_class

        if description.state_class:
            state_class = _STATE_CLASS_MAP.get(description.state_class)
            if state_class:
                self._attr_state_class = state_class

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
    def native_value(self) -> Any:
        return self.coordinator.data.values.get(self._description.key)

    @property
    def suggested_display_precision(self) -> int | None:
        if self._description.suggested_display_precision is not None:
            return self._description.suggested_display_precision
        if self._description.device_class not in _FLOAT_PRECISION_DEVICE_CLASSES:
            return None
        value = self.native_value
        if not isinstance(value, float):
            return None
        return _infer_float_display_precision(value)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        values = self.coordinator.data.values
        fields = _SUMMARY_ATTRIBUTE_MAP.get(self._description.key)
        if not fields:
            return None
        attributes: dict[str, Any] = {}
        for attribute_key, value_key in fields:
            value = values.get(value_key)
            if value is None or value == "":
                continue
            attributes[attribute_key] = value
        return attributes or None

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()


class EybondDerivedEnergySensor(
    RestoreEntity,
    CoordinatorEntity[EybondLocalCoordinator],
    SensorEntity,
):
    """A total_increasing energy sensor estimated from runtime power values."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_native_unit_of_measurement = "kWh"
    _attr_suggested_display_precision = 4

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        description: DerivedEnergyDescription,
    ) -> None:
        super().__init__(coordinator)
        self._description = description
        self._accumulator = EnergyAccumulator()
        self._native_value: float | None = None

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_{description.key}"
        self._attr_name = description.name
        self._attr_icon = description.icon
        self._attr_entity_registry_enabled_default = description.enabled_default

    async def async_added_to_hass(self) -> None:
        """Restore the previous accumulated total after startup."""

        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is None or last_state.state in {"unknown", "unavailable"}:
            return
        try:
            restored_total = float(last_state.state)
        except (TypeError, ValueError):
            return
        self._accumulator.total_kwh = restored_total
        self._native_value = self._accumulator.total_kwh

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        return self._native_value is not None

    @property
    def native_value(self) -> float | None:
        return self._native_value

    @callback
    def _handle_coordinator_update(self) -> None:
        snapshot = self.coordinator.data
        if not snapshot.connected:
            self._accumulator.reset_sample()
            self.async_write_ha_state()
            return

        power_w = compute_derived_power(snapshot.values, self._description)
        if power_w is None:
            self._accumulator.reset_sample()
            self.async_write_ha_state()
            return

        now = datetime.now(timezone.utc)
        self._native_value = self._accumulator.accumulate(power_w, now)
        self.async_write_ha_state()


class EybondDerivedEnergyCycleSensor(
    RestoreEntity,
    CoordinatorEntity[EybondLocalCoordinator],
    SensorEntity,
):
    """A day/month helper sensor derived from one estimated energy source."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_native_unit_of_measurement = "kWh"
    _attr_suggested_display_precision = 4

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        description: DerivedEnergyCycleDescription,
        source_description: DerivedEnergyDescription,
    ) -> None:
        super().__init__(coordinator)
        self._description = description
        self._source_description = source_description
        self._accumulator = CyclingEnergyAccumulator(cycle=description.cycle)
        self._native_value: float | None = None

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_{description.key}"
        self._attr_name = description.name
        self._attr_icon = description.icon
        self._attr_entity_registry_enabled_default = description.enabled_default

    async def async_added_to_hass(self) -> None:
        """Restore the previous cycle total when it belongs to the active period."""

        await super().async_added_to_hass()
        now = dt_util.now()
        last_state = await self.async_get_last_state()
        if last_state is None or last_state.state in {"unknown", "unavailable"}:
            self._accumulator.restore(total_kwh=0.0, period_key="", now=now)
            self._native_value = self._accumulator.total_kwh
            return
        try:
            restored_total = float(last_state.state)
        except (TypeError, ValueError):
            restored_total = 0.0
        period_key = str(last_state.attributes.get("period_key", ""))
        self._accumulator.restore(total_kwh=restored_total, period_key=period_key, now=now)
        self._native_value = self._accumulator.total_kwh

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        return self._native_value is not None

    @property
    def native_value(self) -> float | None:
        return self._native_value

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {
            "cycle": self._description.cycle,
            "period_key": self._accumulator.period_key,
            "source_key": self._source_description.key,
        }

    @callback
    def _handle_coordinator_update(self) -> None:
        snapshot = self.coordinator.data
        if not snapshot.connected:
            self._accumulator.reset_sample()
            self.async_write_ha_state()
            return

        power_w = compute_derived_power(snapshot.values, self._source_description)
        if power_w is None:
            self._accumulator.reset_sample()
            self.async_write_ha_state()
            return

        now = dt_util.now()
        self._native_value = self._accumulator.accumulate(power_w, now)
        self.async_write_ha_state()
