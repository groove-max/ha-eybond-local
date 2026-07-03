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


from custom_components.eybond_local.collector.entity_scope import is_collector_entity_key  # noqa: E402
from custom_components.eybond_local.models import MeasurementDescription, RuntimeSnapshot  # noqa: E402
from custom_components.eybond_local.sensor import EybondValueSensor  # noqa: E402


class CollectorEntityScopeTests(unittest.TestCase):
    def test_is_collector_entity_key_matches_current_namespace_and_future_smartess_keys(self) -> None:
        self.assertTrue(is_collector_entity_key("collector_pn"))
        self.assertTrue(is_collector_entity_key("collector_poll_utilization_percent"))
        # Poll-debugging sensors live on the collector device, not the inverter.
        self.assertTrue(is_collector_entity_key("runtime_refresh_phase_breakdown"))
        self.assertTrue(is_collector_entity_key("driver_slow_requests"))
        self.assertTrue(is_collector_entity_key("driver_unsupported_commands"))
        self.assertTrue(is_collector_entity_key("configured_collector_ip"))
        self.assertTrue(is_collector_entity_key("smartess_protocol_asset_id"))
        self.assertTrue(is_collector_entity_key("runtime_driver_state"))
        self.assertFalse(is_collector_entity_key("model_name"))
        self.assertFalse(is_collector_entity_key("driver_key"))


class RuntimeSensorRoutingTests(unittest.TestCase):
    def test_runtime_sensor_uses_key_based_device_routing(self) -> None:
        coordinator = types.SimpleNamespace(
            config_entry=types.SimpleNamespace(entry_id="entry-1"),
            data=RuntimeSnapshot(values={"collector_pn": "E5000020000000"}),
            device_info_for_key=lambda key: {"scope": key},
        )
        description = MeasurementDescription(key="collector_pn", name="Collector PN", live=False)

        entity = EybondValueSensor(coordinator, description)

        self.assertEqual(entity.device_info, {"scope": "collector_pn"})

    def test_runtime_sensor_keeps_inverter_keys_on_main_scope(self) -> None:
        coordinator = types.SimpleNamespace(
            config_entry=types.SimpleNamespace(entry_id="entry-1"),
            data=RuntimeSnapshot(values={"model_name": "SMG 6200"}),
            device_info_for_key=lambda key: {"scope": key},
        )
        description = MeasurementDescription(key="model_name", name="Model", live=False)

        entity = EybondValueSensor(coordinator, description)

        self.assertEqual(entity.device_info, {"scope": "model_name"})


if __name__ == "__main__":
    unittest.main()
