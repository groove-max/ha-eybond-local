from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_select_stubs() -> None:
    def _ensure_module(name: str) -> types.ModuleType:
        module = sys.modules.get(name)
        if module is None:
            module = types.ModuleType(name)
            sys.modules[name] = module
        return module

    ha = _ensure_module("homeassistant")
    components = _ensure_module("homeassistant.components")
    select = _ensure_module("homeassistant.components.select")
    config_entries = _ensure_module("homeassistant.config_entries")
    helpers = _ensure_module("homeassistant.helpers")
    entity = _ensure_module("homeassistant.helpers.entity")
    entity_platform = _ensure_module("homeassistant.helpers.entity_platform")
    update_coordinator = _ensure_module("homeassistant.helpers.update_coordinator")

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
        runtime_coordinator = types.ModuleType(
            "custom_components.eybond_local.runtime.coordinator"
        )

        class EybondLocalCoordinator:
            pass

        runtime_coordinator.EybondLocalCoordinator = EybondLocalCoordinator
        sys.modules[
            "custom_components.eybond_local.runtime.coordinator"
        ] = runtime_coordinator


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
        self.data = types.SimpleNamespace(
            connected=True,
            values={
                "collector_server_endpoint": "47.91.67.66,18899,TCP",
                "collector_operation_endpoint_sync_status": "aligned",
            },
        )
        self.collector_operation_mode = "smartess_cloud_home_assistant"
        self.control_mode = "auto"
        self.controls_enabled = True
        self.controls_reason = "autodetected_high_confidence"
        self.controls_summary = "Controls are enabled automatically."
        self.collector_callback_target_endpoint = "192.168.1.50,18899,TCP"
        self.collector_server_endpoint_rollback_target = "47.91.67.66,18899,TCP"
        self.proxy_capture_upstream_endpoint = "47.91.67.66,18899,TCP"
        self.collector_capabilities = collector_capability_profile()
        self.calls: list[tuple[str, str]] = []

    def collector_device_info(self):
        return {"scope": "collector"}

    def inverter_device_info(self):
        return {"scope": "inverter"}

    def collector_operation_mode_change_reason(self, *, target_mode: str = "") -> str | None:
        return None

    async def async_set_collector_operation_mode(self, option: str) -> None:
        self.calls.append(("collector_operation_mode", option))
        self.collector_operation_mode = option

    async def async_set_control_mode(self, option: str) -> None:
        self.calls.append(("control_mode", option))
        self.control_mode = option


class RuntimeSelectTests(unittest.TestCase):
    def test_runtime_select_keys_are_exposed_and_default_enabled(self) -> None:
        self.assertEqual(
            runtime_select_keys_for_runtime(),
            ("collector_operation_mode",),
        )
        self.assertEqual(
            default_enabled_runtime_select_keys_for_runtime(),
            ("collector_operation_mode",),
        )

    def test_runtime_select_keys_can_be_limited_to_collector_only_mode(self) -> None:
        self.assertEqual(
            runtime_select_keys_for_runtime(has_inverter_identity=False),
            ("collector_operation_mode",),
        )
        self.assertEqual(
            default_enabled_runtime_select_keys_for_runtime(has_inverter_identity=False),
            ("collector_operation_mode",),
        )

    def test_setup_skips_collector_operation_mode_select_for_local_bridge(self) -> None:
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

    def test_collector_operation_mode_select_routes_to_collector_device(self) -> None:
        coordinator = _CoordinatorStub()
        entity = EybondRuntimeSettingSelect(
            coordinator,
            _RuntimeSelectSpec(
                key="collector_operation_mode",
                translation_key="collector_operation_mode",
                name="Collector Operation Mode",
                options=(
                    "smartess_cloud_home_assistant",
                    "home_assistant_only",
                ),
                device_scope="collector",
            ),
        )

        self.assertEqual(entity.device_info, {"scope": "collector"})
        self.assertEqual(entity.current_option, "smartess_cloud_home_assistant")
        self.assertTrue(entity.available)
        self.assertEqual(
            entity.extra_state_attributes,
            {
                "setting_scope": "collector",
                "write_enabled": True,
                "availability_reason": "Ready",
                "current_callback_endpoint": "47.91.67.66,18899,TCP",
                "target_callback_endpoint": "192.168.1.50,18899,TCP",
                "rollback_callback_endpoint": "47.91.67.66,18899,TCP",
                "upstream_callback_endpoint": "47.91.67.66,18899,TCP",
                "callback_sync_status": "aligned",
                "mode_change_effect": "ha_only_enforces_home_assistant_callback; smartess_cloud_home_assistant_restores_upstream_callback_when_available",
            },
        )

    def test_collector_operation_mode_select_is_unavailable_during_proxy_transition(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.collector_operation_mode_change_reason = lambda *, target_mode="": (
            "collector_operation_mode_proxy_transition_active"
        )
        entity = EybondRuntimeSettingSelect(
            coordinator,
            _RuntimeSelectSpec(
                key="collector_operation_mode",
                translation_key="collector_operation_mode",
                name="Collector Operation Mode",
                options=(
                    "smartess_cloud_home_assistant",
                    "home_assistant_only",
                ),
                device_scope="collector",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(entity.current_option, "smartess_cloud_home_assistant")
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Proxy capture is changing the collector callback. Wait for the transition to finish.",
        )
        self.assertFalse(entity.extra_state_attributes["write_enabled"])

    def test_collector_operation_mode_select_is_unavailable_while_mode_change_applies(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.collector_operation_mode_change_reason = lambda *, target_mode="": (
            "collector_operation_mode_apply_pending"
        )
        entity = EybondRuntimeSettingSelect(
            coordinator,
            _RuntimeSelectSpec(
                key="collector_operation_mode",
                translation_key="collector_operation_mode",
                name="Collector Operation Mode",
                options=(
                    "smartess_cloud_home_assistant",
                    "home_assistant_only",
                ),
                device_scope="collector",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(entity.current_option, "smartess_cloud_home_assistant")
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Collector is applying the new operation mode. Wait for the collector to restart and reconnect.",
        )
        self.assertFalse(entity.extra_state_attributes["write_enabled"])

    def test_collector_operation_mode_select_reports_missing_upstream_endpoint(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.collector_operation_mode_change_reason = lambda *, target_mode="": (
            "collector_operation_mode_rollback_endpoint_unavailable"
        )
        entity = EybondRuntimeSettingSelect(
            coordinator,
            _RuntimeSelectSpec(
                key="collector_operation_mode",
                translation_key="collector_operation_mode",
                name="Collector Operation Mode",
                options=(
                    "smartess_cloud_home_assistant",
                    "home_assistant_only",
                ),
                device_scope="collector",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(entity.current_option, "smartess_cloud_home_assistant")
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "No upstream callback endpoint is available yet.",
        )
        self.assertFalse(entity.extra_state_attributes["write_enabled"])

if __name__ == "__main__":
    unittest.main()
