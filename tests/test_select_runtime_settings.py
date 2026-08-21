from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types
import unittest

from helpers.homeassistant_stubs import ensure_module, ensure_package


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_select_stubs() -> None:
    ha = ensure_module("homeassistant")
    components = ensure_module("homeassistant.components")
    select = ensure_module("homeassistant.components.select")
    config_entries = ensure_module("homeassistant.config_entries")
    helpers = ensure_module("homeassistant.helpers")
    entity = ensure_module("homeassistant.helpers.entity")
    entity_platform = ensure_module("homeassistant.helpers.entity_platform")
    update_coordinator = ensure_module("homeassistant.helpers.update_coordinator")

    class SelectEntity:
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

    select.SelectEntity = SelectEntity
    config_entries.ConfigEntry = ConfigEntry
    entity.EntityCategory = EntityCategory
    entity_platform.AddEntitiesCallback = AddEntitiesCallback
    update_coordinator.CoordinatorEntity = CoordinatorEntity

    ha.components = components
    ha.config_entries = config_entries
    ha.helpers = helpers
    components.select = select
    helpers.entity = entity
    helpers.entity_platform = entity_platform
    helpers.update_coordinator = update_coordinator

    if "custom_components.eybond_local.runtime.coordinator" not in sys.modules:
        runtime_coordinator = ensure_package(
            "custom_components.eybond_local.runtime.coordinator",
            REPO_ROOT
            / "custom_components/eybond_local/runtime/coordinator",
        )

        class EybondLocalCoordinator:
            pass

        runtime_coordinator.EybondLocalCoordinator = EybondLocalCoordinator


_install_select_stubs()


from custom_components.eybond_local.select import (  # noqa: E402
    EybondRuntimeSettingSelect,
    _RuntimeSelectSpec,
    async_setup_entry,
    default_enabled_runtime_select_keys_for_runtime,
    runtime_select_keys_for_runtime,
)
from custom_components.eybond_local.collector.capabilities import (  # noqa: E402
    collector_capability_profile,
)


class _CoordinatorStub:
    def __init__(self) -> None:
        self.config_entry = types.SimpleNamespace(entry_id="entry-1")
        self.data = types.SimpleNamespace(connected=True, values={})
        self.control_mode = "auto"
        self.controls_enabled = True
        self.controls_reason = "autodetected_high_confidence"
        self.controls_summary = "Controls are enabled automatically."
        self.collector_capabilities = collector_capability_profile()
        self.calls: list[tuple[str, str]] = []

    def collector_device_info(self):
        return {"scope": "collector"}

    def inverter_device_info(self):
        return {"scope": "inverter"}

    async def async_set_control_mode(self, option: str) -> None:
        self.calls.append(("control_mode", option))
        self.control_mode = option


class RuntimeSelectInventoryTests(unittest.TestCase):
    """CP2A Test A: the writable collector operation-mode select is gone."""

    def test_collector_operation_mode_is_not_a_runtime_select_key(self) -> None:
        # The connection-strategy transition is the only user authority for the
        # connection method; the operation mode is a read-only projection, never
        # a writable runtime select.
        for keys in (
            runtime_select_keys_for_runtime(),
            default_enabled_runtime_select_keys_for_runtime(),
            runtime_select_keys_for_runtime(has_inverter_identity=False),
            default_enabled_runtime_select_keys_for_runtime(has_inverter_identity=False),
        ):
            self.assertNotIn("collector_operation_mode", keys)

    def test_runtime_setup_creates_no_writable_operation_mode_select(self) -> None:
        async def _run() -> list[object]:
            coordinator = _CoordinatorStub()
            entry = types.SimpleNamespace(data={}, options={}, runtime_data=coordinator)
            entities: list[object] = []
            await async_setup_entry(None, entry, entities.extend)
            return entities

        entities = asyncio.run(_run())
        self.assertFalse(
            any(
                isinstance(entity, EybondRuntimeSettingSelect)
                and entity._spec.key == "collector_operation_mode"
                for entity in entities
            )
        )

    def test_setup_for_local_bridge_creates_no_operation_mode_select(self) -> None:
        async def _run() -> list[object]:
            coordinator = _CoordinatorStub()
            coordinator.collector_capabilities = collector_capability_profile(
                virtual_bridge=True
            )
            entry = types.SimpleNamespace(
                data={"collector_virtual_bridge": True},
                options={},
                runtime_data=coordinator,
            )
            entities: list[object] = []
            await async_setup_entry(None, entry, entities.extend)
            return entities

        entities = asyncio.run(_run())
        self.assertFalse(
            any(
                isinstance(entity, EybondRuntimeSettingSelect)
                and entity._spec.key == "collector_operation_mode"
                for entity in entities
            )
        )

    def test_select_module_has_no_writable_mode_path(self) -> None:
        # Static guard: the select platform holds no writable operation-mode
        # surface — no spec, no setter call, no change-reason availability gate.
        import custom_components.eybond_local.select as select_module

        source = Path(select_module.__file__).read_text(encoding="utf-8")
        self.assertNotIn("async_set_collector_operation_mode", source)
        self.assertNotIn('key="collector_operation_mode"', source)
        self.assertNotIn("collector_operation_mode_change_reason", source)


class RuntimeSettingSelectGenericPathTests(unittest.TestCase):
    """The runtime-setting select class survives as generic scaffolding.

    CP2A removed only the operation-mode-specific branches; these tests pin the
    remaining generic control-mode path so that removal did not break it.
    """

    def _control_mode_entity(self, coordinator) -> EybondRuntimeSettingSelect:
        return EybondRuntimeSettingSelect(
            coordinator,
            _RuntimeSelectSpec(
                key="control_mode",
                translation_key="control_mode",
                name="Control Mode",
                options=("auto", "read_only", "full"),
                device_scope="integration",
            ),
        )

    def test_control_mode_select_reads_integration_scope(self) -> None:
        coordinator = _CoordinatorStub()
        entity = self._control_mode_entity(coordinator)
        self.assertEqual(entity.device_info, {"scope": "inverter"})
        self.assertEqual(entity.current_option, "auto")
        self.assertTrue(entity.available)
        self.assertEqual(
            entity.extra_state_attributes,
            {
                "setting_scope": "integration",
                "write_enabled": True,
                "controls_enabled": True,
                "control_policy_reason": "autodetected_high_confidence",
                "control_policy_summary": "Controls are enabled automatically.",
            },
        )

    def test_control_mode_select_routes_write_to_coordinator(self) -> None:
        coordinator = _CoordinatorStub()
        entity = self._control_mode_entity(coordinator)
        asyncio.run(entity.async_select_option("read_only"))
        self.assertEqual(coordinator.calls, [("control_mode", "read_only")])

    def test_unknown_runtime_select_key_raises(self) -> None:
        coordinator = _CoordinatorStub()
        entity = EybondRuntimeSettingSelect(
            coordinator,
            _RuntimeSelectSpec(
                key="not_a_setting",
                translation_key="not_a_setting",
                name="Not A Setting",
                options=("a", "b"),
                device_scope="integration",
            ),
        )
        with self.assertRaises(ValueError):
            asyncio.run(entity.async_select_option("a"))


if __name__ == "__main__":
    unittest.main()
