from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_sensor_stubs() -> None:
    def _ensure_module(name: str) -> types.ModuleType:
        module = sys.modules.get(name)
        if module is None:
            module = types.ModuleType(name)
            sys.modules[name] = module
        return module

    ha = _ensure_module("homeassistant")
    components = _ensure_module("homeassistant.components")
    sensor = _ensure_module("homeassistant.components.sensor")
    config_entries = _ensure_module("homeassistant.config_entries")
    core = _ensure_module("homeassistant.core")
    helpers = _ensure_module("homeassistant.helpers")
    entity = _ensure_module("homeassistant.helpers.entity")
    entity_platform = _ensure_module("homeassistant.helpers.entity_platform")
    restore_state = _ensure_module("homeassistant.helpers.restore_state")
    update_coordinator = _ensure_module("homeassistant.helpers.update_coordinator")
    util = _ensure_module("homeassistant.util")
    dt = _ensure_module("homeassistant.util.dt")

    class SensorDeviceClass:
        BATTERY = "battery"
        CURRENT = "current"
        ENERGY = "energy"
        FREQUENCY = "frequency"
        POWER = "power"
        TEMPERATURE = "temperature"
        VOLTAGE = "voltage"

    class SensorEntity:
        pass

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
    helpers.entity_platform = entity_platform
    helpers.restore_state = restore_state
    helpers.update_coordinator = update_coordinator

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


from custom_components.eybond_local.models import MeasurementDescription
from custom_components.eybond_local.sensor import EybondValueSensor


class _FakeCoordinator:
    def __init__(self, key: str, value: object) -> None:
        self.config_entry = types.SimpleNamespace(entry_id="entry123")
        self.data = types.SimpleNamespace(values={key: value}, connected=True)

    def device_info(self) -> dict[str, str]:
        return {}


class SensorPrecisionTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()