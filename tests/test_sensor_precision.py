from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types
import unittest

from helpers.homeassistant_stubs import ensure_module


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_sensor_stubs() -> None:
    ha = ensure_module("homeassistant")
    components = ensure_module("homeassistant.components")
    sensor = ensure_module("homeassistant.components.sensor")
    config_entries = ensure_module("homeassistant.config_entries")
    core = ensure_module("homeassistant.core")
    helpers = ensure_module("homeassistant.helpers")
    entity = ensure_module("homeassistant.helpers.entity")
    entity_registry = ensure_module("homeassistant.helpers.entity_registry")
    entity_platform = ensure_module("homeassistant.helpers.entity_platform")
    restore_state = ensure_module("homeassistant.helpers.restore_state")
    update_coordinator = ensure_module("homeassistant.helpers.update_coordinator")
    util = ensure_module("homeassistant.util")
    dt = ensure_module("homeassistant.util.dt")

    class SensorDeviceClass:
        BATTERY = "battery"
        CURRENT = "current"
        ENERGY = "energy"
        ENUM = "enum"
        FREQUENCY = "frequency"
        POWER = "power"
        TEMPERATURE = "temperature"
        VOLTAGE = "voltage"

    class SensorEntity:
        async def async_added_to_hass(self):
            return None

    class SensorStateClass:
        MEASUREMENT = "measurement"
        TOTAL = "total"
        TOTAL_INCREASING = "total_increasing"

    class ConfigEntry:
        pass

    class EntityCategory:
        DIAGNOSTIC = "diagnostic"

    class AddEntitiesCallback:
        pass

    class RestoreEntity:
        pass

    class CoordinatorEntity:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, coordinator):
            self.coordinator = coordinator

        async def async_added_to_hass(self):
            return None

    def callback(func):
        return func

    sensor.SensorDeviceClass = SensorDeviceClass
    sensor.SensorEntity = SensorEntity
    sensor.SensorStateClass = SensorStateClass
    config_entries.ConfigEntry = ConfigEntry
    core.callback = callback
    entity.EntityCategory = EntityCategory
    entity_platform.AddEntitiesCallback = AddEntitiesCallback
    restore_state.RestoreEntity = RestoreEntity
    update_coordinator.CoordinatorEntity = CoordinatorEntity
    dt.now = lambda: None
    util.dt = dt

    ha.components = components
    ha.config_entries = config_entries
    ha.core = core
    ha.helpers = helpers
    ha.util = util
    components.sensor = sensor
    helpers.entity = entity
    helpers.entity_registry = entity_registry
    helpers.entity_platform = entity_platform
    helpers.restore_state = restore_state
    helpers.update_coordinator = update_coordinator

    def async_get(hass):
        return hass.entity_registry

    entity_registry.async_get = async_get

    if "custom_components.eybond_local.runtime.coordinator" not in sys.modules:
        runtime_coordinator = types.ModuleType(
            "custom_components.eybond_local.runtime.coordinator"
        )

        class EybondLocalCoordinator:
            pass

        runtime_coordinator.EybondLocalCoordinator = EybondLocalCoordinator
        sys.modules[
            "custom_components.eybond_local.runtime.coordinator"
        ] = runtime_coordinator


_install_sensor_stubs()


from custom_components.eybond_local.models import MeasurementDescription, RuntimeSnapshot
from custom_components.eybond_local.sensor import EybondValueSensor


class _FakeCoordinator:
    def __init__(self, key: str, value: object, *, collector_cloud_family: str = "") -> None:
        self.config_entry = types.SimpleNamespace(entry_id="entry123")
        self.data = RuntimeSnapshot(values={key: value}, connected=True)
        self.collector_cloud_family = collector_cloud_family

    def device_info(self) -> dict[str, str]:
        return {}


class _FakeRegistry:
    def __init__(self, options: dict[str, object] | None = None) -> None:
        self._entries = {
            "sensor.battery_voltage": types.SimpleNamespace(options=options or {})
        }

    def async_get(self, entity_id: str):
        return self._entries.get(entity_id)

    def async_update_entity_options(self, entity_id: str, domain: str, options: dict[str, object]) -> None:
        entry = self._entries[entity_id]
        merged = dict(entry.options)
        merged[domain] = dict(options)
        entry.options = merged


class SensorPrecisionTests(unittest.TestCase):
    def test_sensor_prefers_typed_telemetry_over_legacy_compatibility_value(self) -> None:
        from custom_components.eybond_local.telemetry import (
            TypedTelemetryFrame,
            fold_driver_telemetry,
        )

        coordinator = _FakeCoordinator("battery_voltage", 24.0)
        coordinator.data.telemetry = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"battery_voltage": 51.2},
            replace=True,
        )
        description = MeasurementDescription(
            key="battery_voltage",
            name="Battery Voltage",
            unit="V",
            device_class="voltage",
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertTrue(sensor.available)
        self.assertEqual(sensor.native_value, 51.2)

    def test_callback_identity_attributes_separate_live_confirmed_and_expected_wire(self) -> None:
        coordinator = _FakeCoordinator("collector_callback_identity_status", "idle")
        coordinator.data.values.update(
            {
                "collector_callback_identity_summary": "No unresolved sessions.",
                "collector_callback_pending_session_count": 0,
                "collector_callback_recent_session_count": 3,
                "collector_callback_identified_session_count": 3,
                "collector_callback_unresolved_session_count": 0,
                "collector_callback_duplicate_peer_ip_count": 1,
                # Deliberately different configured vs live values: diagnostics
                # must not flatten them into one misleading projection.
                "collector_callback_identity_strategy": "at_dtupn",
                "collector_configured_session_protocol": "at_text",
                "collector_current_live_session": "active",
                "collector_live_session_protocol": "eybond_framed",
                "collector_confirmed_session_protocol": "eybond_framed",
                "collector_confirmed_wire_binding": "eybond_framed",
                "collector_callback_wire_framing": "eybond_framed",
                "collector_callback_identity_sources": "fc2_parameter_2",
                "collector_callback_inverter_forward_adapter": "framed_fc4",
                "collector_callback_adapter_conflict": False,
            }
        )
        description = MeasurementDescription(
            key="collector_callback_identity_status",
            name="Collector Callback Identity Status",
            diagnostic=True,
        )

        sensor = EybondValueSensor(coordinator, description)
        attributes = sensor.extra_state_attributes

        self.assertIsNotNone(attributes)
        assert attributes is not None
        self.assertEqual(attributes["live_session_protocol"], "eybond_framed")
        self.assertEqual(attributes["confirmed_session_protocol"], "eybond_framed")
        self.assertEqual(attributes["effective_wire_framing"], "eybond_framed")
        self.assertEqual(attributes["inverter_forward_adapter"], "framed_fc4")
        self.assertEqual(attributes["identity_sources"], "fc2_parameter_2")
        self.assertEqual(attributes["configured_session_protocol"], "at_text")
        self.assertEqual(attributes["configured_identity_strategy"], "at_dtupn")
        self.assertFalse(attributes["adapter_conflict"])
        self.assertNotIn("session_protocol", attributes)
        self.assertNotIn("identity_strategy", attributes)

    def test_summary_attributes_prefer_typed_state_and_keep_legacy_metadata(self) -> None:
        from custom_components.eybond_local.telemetry import (
            TypedTelemetryFrame,
            fold_driver_telemetry,
        )

        coordinator = _FakeCoordinator("output_settings_state", "Configured")
        coordinator.data.values.update(
            {
                "operating_mode": "Fault",
                "configuration_safe_mode": True,
                "remote_control_enabled": False,
            }
        )
        coordinator.data.telemetry = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"operating_mode": "Power On"},
            replace=True,
        )
        description = MeasurementDescription(
            key="output_settings_state",
            name="Output Settings",
        )

        attributes = EybondValueSensor(coordinator, description).extra_state_attributes

        self.assertEqual(
            attributes,
            {
                "operating_mode": "Power On",
                "configuration_safe_mode": True,
                "remote_control_enabled": False,
            },
        )

    def test_explicit_precision_overrides_float_fallback(self) -> None:
        coordinator = _FakeCoordinator("battery_voltage", 52.0)
        description = MeasurementDescription(
            key="battery_voltage",
            name="Battery Voltage",
            unit="V",
            device_class="voltage",
            suggested_display_precision=3,
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertEqual(sensor.suggested_display_precision, 3)

    def test_voltage_sensor_falls_back_to_single_decimal_for_integer_like_floats(self) -> None:
        coordinator = _FakeCoordinator("battery_voltage", 52.0)
        description = MeasurementDescription(
            key="battery_voltage",
            name="Battery Voltage",
            unit="V",
            device_class="voltage",
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertEqual(sensor.suggested_display_precision, 1)

    def test_frequency_sensor_falls_back_to_fractional_digits_in_native_value(self) -> None:
        coordinator = _FakeCoordinator("sync_frequency", 49.95)
        description = MeasurementDescription(
            key="sync_frequency",
            name="Sync Frequency",
            unit="Hz",
            device_class="frequency",
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertEqual(sensor.suggested_display_precision, 2)

    def test_power_sensor_does_not_add_float_precision_without_metadata(self) -> None:
        coordinator = _FakeCoordinator("battery_power", 614.4)
        description = MeasurementDescription(
            key="battery_power",
            name="Battery Power",
            unit="W",
            device_class="power",
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertIsNone(sensor.suggested_display_precision)

    def test_enum_sensor_exposes_translation_key_and_options(self) -> None:
        coordinator = _FakeCoordinator("collector_signal_quality", "excellent")
        description = MeasurementDescription(
            key="collector_signal_quality",
            name="Collector Signal Quality",
            translation_key="collector_signal_quality",
            device_class="enum",
            options=("unknown", "excellent", "good", "fair", "weak"),
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertEqual(sensor._attr_translation_key, "collector_signal_quality")
        self.assertEqual(sensor._attr_options, ["unknown", "excellent", "good", "fair", "weak"])
        self.assertEqual(sensor.native_value, "excellent")

    def test_collector_signal_sensors_are_inactive_for_legacy_collectors(self) -> None:
        coordinator = _FakeCoordinator(
            "collector_signal_strength",
            -67,
            collector_cloud_family="legacy_binary",
        )
        description = MeasurementDescription(
            key="collector_signal_strength",
            name="Collector Signal Strength",
            unit="dBm",
            device_class="signal_strength",
            enabled_default=True,
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertFalse(sensor._attr_entity_registry_enabled_default)
        self.assertFalse(sensor.available)

    def test_collector_signal_quality_is_inactive_for_legacy_collectors(self) -> None:
        coordinator = _FakeCoordinator(
            "collector_signal_quality",
            "excellent",
            collector_cloud_family="legacy_binary",
        )
        description = MeasurementDescription(
            key="collector_signal_quality",
            name="Collector Signal Quality",
            translation_key="collector_signal_quality",
            device_class="enum",
            options=("unknown", "excellent", "good", "fair", "weak"),
            enabled_default=True,
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertFalse(sensor._attr_entity_registry_enabled_default)
        self.assertFalse(sensor.available)

    def test_collector_signal_sensors_stay_active_for_smartess_at_collectors(self) -> None:
        coordinator = _FakeCoordinator(
            "collector_signal_strength",
            -67,
            collector_cloud_family="smartess_at",
        )
        description = MeasurementDescription(
            key="collector_signal_strength",
            name="Collector Signal Strength",
            unit="dBm",
            device_class="signal_strength",
            enabled_default=True,
        )

        sensor = EybondValueSensor(coordinator, description)

        self.assertTrue(sensor._attr_entity_registry_enabled_default)
        self.assertTrue(sensor.available)

    def test_added_to_hass_repairs_stale_zero_precision_override(self) -> None:
        coordinator = _FakeCoordinator("battery_voltage", 52.0)
        description = MeasurementDescription(
            key="battery_voltage",
            name="Battery Voltage",
            unit="V",
            device_class="voltage",
            suggested_display_precision=1,
        )
        registry = _FakeRegistry(options={"sensor": {"suggested_display_precision": 0}})
        sensor = EybondValueSensor(coordinator, description)
        sensor.entity_id = "sensor.battery_voltage"
        sensor.hass = types.SimpleNamespace(entity_registry=registry)

        asyncio.run(sensor.async_added_to_hass())

        self.assertEqual(
            registry.async_get("sensor.battery_voltage").options,
            {"sensor": {"suggested_display_precision": 1}},
        )


if __name__ == "__main__":
    unittest.main()
