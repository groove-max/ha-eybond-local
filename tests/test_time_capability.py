from __future__ import annotations

from datetime import time
from pathlib import Path
import sys
import types
import unittest

from helpers.homeassistant_stubs import ensure_module, ensure_package


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_STUBBED_MODULE_NAMES = (
    "homeassistant",
    "homeassistant.components",
    "homeassistant.components.time",
    "homeassistant.config_entries",
    "homeassistant.helpers",
    "homeassistant.helpers.entity",
    "homeassistant.helpers.entity_platform",
    "homeassistant.helpers.update_coordinator",
    "custom_components.eybond_local.runtime.coordinator",
)
_STUBBED_MODULES_SNAPSHOT = {
    name: sys.modules.get(name) for name in _STUBBED_MODULE_NAMES
}


def _install_stubs() -> None:
    ha = ensure_module("homeassistant")
    components = ensure_module("homeassistant.components")
    time_component = ensure_module("homeassistant.components.time")
    config_entries = ensure_module("homeassistant.config_entries")
    helpers = ensure_module("homeassistant.helpers")
    entity = ensure_module("homeassistant.helpers.entity")
    entity_platform = ensure_module("homeassistant.helpers.entity_platform")
    update_coordinator = ensure_module("homeassistant.helpers.update_coordinator")

    class TimeEntity:
        pass

    class ConfigEntry:
        pass

    class EntityCategory:
        CONFIG = "config"

    class AddEntitiesCallback:
        pass

    class CoordinatorEntity:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, coordinator):
            self.coordinator = coordinator

    time_component.TimeEntity = TimeEntity
    config_entries.ConfigEntry = ConfigEntry
    entity.EntityCategory = EntityCategory
    entity_platform.AddEntitiesCallback = AddEntitiesCallback
    update_coordinator.CoordinatorEntity = CoordinatorEntity

    ha.components = components
    ha.config_entries = config_entries
    ha.helpers = helpers
    components.time = time_component
    helpers.entity = entity
    helpers.entity_platform = entity_platform
    helpers.update_coordinator = update_coordinator

    if "custom_components.eybond_local.runtime.coordinator" not in sys.modules:
        runtime_coordinator = ensure_package(
            "custom_components.eybond_local.runtime.coordinator",
            REPO_ROOT / "custom_components/eybond_local/runtime/coordinator",
        )

        class EybondLocalCoordinator:
            pass

        runtime_coordinator.EybondLocalCoordinator = EybondLocalCoordinator


_install_stubs()


def tearDownModule() -> None:
    for name, original in _STUBBED_MODULES_SNAPSHOT.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


from custom_components.eybond_local.models import (  # noqa: E402
    DetectedInverter,
    ProbeTarget,
    RuntimeSnapshot,
    WriteCapability,
)
from custom_components.eybond_local.time import EybondCapabilityTime  # noqa: E402


class _CoordinatorStub:
    def __init__(self, capability: WriteCapability) -> None:
        self.config_entry = types.SimpleNamespace(entry_id="entry-1")
        inverter = DetectedInverter(
            driver_key="modbus_catalog",
            protocol_family="modbus_catalog",
            model_name="Kevolt",
            serial_number="",
            probe_target=ProbeTarget(devcode=1, collector_addr=1, device_addr=1),
            capabilities=(capability,),
        )
        self.data = RuntimeSnapshot(
            connected=True,
            inverter=inverter,
            values={capability.value_key: "08:30"},
        )
        self.writes: list[tuple[str, object]] = []

    def capability_enabled_by_default(self, _capability: WriteCapability) -> bool:
        return False

    def inverter_device_info(self) -> dict[str, str]:
        return {"scope": "inverter"}

    async def async_write_capability(self, key: str, value: object) -> None:
        self.writes.append((key, value))


class TimeCapabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_time_entity_reads_and_writes_typed_hhmm_value(self) -> None:
        capability = WriteCapability(
            key="time_of_use_period_1_time",
            register=148,
            value_kind="time_hhmm",
            note="",
            read_key="time_of_use_period_1_time",
            enabled_default=False,
        )
        coordinator = _CoordinatorStub(capability)
        entity = EybondCapabilityTime(coordinator, capability)

        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value, time(8, 30))
        self.assertFalse(entity._attr_entity_registry_enabled_default)
        self.assertEqual(entity.device_info, {"scope": "inverter"})

        await entity.async_set_value(time(21, 5))
        self.assertEqual(
            coordinator.writes,
            [("time_of_use_period_1_time", "21:05")],
        )

        coordinator.data.values[capability.value_key] = "25:99"
        self.assertIsNone(entity.native_value)


if __name__ == "__main__":
    unittest.main()
