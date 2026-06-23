from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_STUBBED_MODULE_NAMES: tuple[str, ...] = (
    "homeassistant",
    "homeassistant.components",
    "homeassistant.components.text",
    "homeassistant.config_entries",
    "homeassistant.helpers",
    "homeassistant.helpers.entity",
    "homeassistant.helpers.entity_platform",
    "homeassistant.helpers.entity_registry",
    "homeassistant.helpers.update_coordinator",
    "custom_components.eybond_local.runtime.coordinator",
)
_STUBBED_MODULES_SNAPSHOT: dict[str, types.ModuleType | None] = {
    name: sys.modules.get(name) for name in _STUBBED_MODULE_NAMES
}


def _install_text_stubs() -> None:
    def _ensure_module(name: str) -> types.ModuleType:
        module = sys.modules.get(name)
        if module is None:
            module = types.ModuleType(name)
            sys.modules[name] = module
        return module

    ha = _ensure_module("homeassistant")
    components = _ensure_module("homeassistant.components")
    text = _ensure_module("homeassistant.components.text")
    config_entries = _ensure_module("homeassistant.config_entries")
    helpers = _ensure_module("homeassistant.helpers")
    entity = _ensure_module("homeassistant.helpers.entity")
    entity_platform = _ensure_module("homeassistant.helpers.entity_platform")
    entity_registry = _ensure_module("homeassistant.helpers.entity_registry")
    update_coordinator = _ensure_module("homeassistant.helpers.update_coordinator")

    class TextEntity:
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

    text.TextEntity = TextEntity
    config_entries.ConfigEntry = ConfigEntry
    entity.EntityCategory = EntityCategory
    entity_platform.AddEntitiesCallback = AddEntitiesCallback
    update_coordinator.CoordinatorEntity = CoordinatorEntity

    if not hasattr(entity_registry, "async_get"):
        entity_registry.async_get = lambda _hass: None
    if not hasattr(entity_registry, "async_entries_for_config_entry"):
        entity_registry.async_entries_for_config_entry = lambda _registry, _entry_id: []
    if not hasattr(entity_registry, "RegistryEntryDisabler"):
        class _RegistryEntryDisabler:
            INTEGRATION = "integration"

        entity_registry.RegistryEntryDisabler = _RegistryEntryDisabler

    ha.components = components
    ha.config_entries = config_entries
    ha.helpers = helpers
    components.text = text
    helpers.entity = entity
    helpers.entity_platform = entity_platform
    helpers.entity_registry = entity_registry
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


_install_text_stubs()


def tearDownModule() -> None:
    """Restore sys.modules so other test files see a clean import landscape."""

    for name, original in _STUBBED_MODULES_SNAPSHOT.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


from custom_components.eybond_local.models import RuntimeSnapshot  # noqa: E402
from custom_components.eybond_local.text import (  # noqa: E402
    EybondCollectorText,
    _CollectorTextSpec,
)


class _CoordinatorStub:
    def __init__(self) -> None:
        self.config_entry = types.SimpleNamespace(entry_id="entry-1")
        self.data = RuntimeSnapshot(
            connected=True,
            values={
                "collector_server_endpoint": "192.168.1.193,18899,TCP",
            },
        )
        self.control_mode = "full"
        self.collector_callback_target_endpoint = "203.0.113.7,2223,TCP"
        self.calls: list[dict[str, object]] = []

    def collector_device_info(self):
        return {"scope": "collector"}

    async def async_set_collector_server_endpoint(self, **kwargs):
        self.calls.append(dict(kwargs))

    async def async_set_raw_collector_server_endpoint(self, **kwargs):
        self.calls.append(dict(kwargs))


class CollectorTextTests(unittest.TestCase):
    def test_collector_text_routes_to_collector_device(self) -> None:
        coordinator = _CoordinatorStub()
        entity = EybondCollectorText(
            coordinator,
            _CollectorTextSpec(
                key="collector_callback_endpoint",
                translation_key="collector_callback_endpoint",
                name="Collector Callback Endpoint",
                icon="mdi:lan-pending",
                enabled_default=True,
            ),
        )

        self.assertEqual(entity.device_info, {"scope": "collector"})
        self.assertEqual(entity._attr_translation_key, "collector_callback_endpoint")
        self.assertTrue(entity._attr_entity_registry_enabled_default)
        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value, "192.168.1.193,18899,TCP")
        self.assertEqual(
            entity.extra_state_attributes["current_callback_endpoint"],
            "192.168.1.193,18899,TCP",
        )
        self.assertIsNone(entity.extra_state_attributes["pending_callback_endpoint"])
        self.assertFalse(entity.extra_state_attributes["pending_apply_required"])
        self.assertTrue(entity.extra_state_attributes["apply_required"])
        self.assertTrue(entity.extra_state_attributes["expert_action"])
        self.assertFalse(entity.extra_state_attributes["read_only"])
        self.assertTrue(entity.extra_state_attributes["write_enabled"])
        self.assertEqual(
            entity.extra_state_attributes["pending_apply_action"],
            "apply_collector_changes",
        )

    def test_collector_text_requires_full_control_in_auto_mode(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.control_mode = "auto"
        entity = EybondCollectorText(
            coordinator,
            _CollectorTextSpec(
                key="collector_callback_endpoint",
                translation_key="collector_callback_endpoint",
                name="Collector Callback Endpoint",
                icon="mdi:lan-pending",
                enabled_default=True,
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Requires Full Control.",
        )
        self.assertFalse(entity.extra_state_attributes["read_only"])
        self.assertFalse(entity.extra_state_attributes["write_enabled"])

    def test_collector_text_reports_auto_or_full_requirement(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.control_mode = "read_only"
        entity = EybondCollectorText(
            coordinator,
            _CollectorTextSpec(
                key="collector_callback_endpoint",
                translation_key="collector_callback_endpoint",
                name="Collector Callback Endpoint",
                icon="mdi:lan-pending",
                enabled_default=True,
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Requires Full Control.",
        )

    def test_collector_text_write_requires_full_control(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            coordinator.control_mode = "auto"
            entity = EybondCollectorText(
                coordinator,
                _CollectorTextSpec(
                    key="collector_callback_endpoint",
                    translation_key="collector_callback_endpoint",
                    name="Collector Callback Endpoint",
                    icon="mdi:lan-pending",
                    enabled_default=True,
                ),
            )

            with self.assertRaisesRegex(PermissionError, "Requires Full Control for write access"):
                await entity.async_set_value("10.0.0.25,18899")

            self.assertEqual(coordinator.calls, [])

        asyncio.run(_run())

    def test_collector_text_stages_endpoint_without_apply(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            entity = EybondCollectorText(
                coordinator,
                _CollectorTextSpec(
                    key="collector_callback_endpoint",
                    translation_key="collector_callback_endpoint",
                    name="Collector Callback Endpoint",
                    icon="mdi:lan-pending",
                    enabled_default=True,
                ),
            )

            await entity.async_set_value("10.0.0.25,18899")

            self.assertEqual(
                coordinator.calls,
                [
                    {
                        "endpoint": "10.0.0.25,18899",
                        "apply_changes": False,
                        "confirm_redirect": True,
                    }
                ],
            )

        asyncio.run(_run())

    def test_collector_text_preserves_host_only_shape(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            entity = EybondCollectorText(
                coordinator,
                _CollectorTextSpec(
                    key="collector_callback_endpoint",
                    translation_key="collector_callback_endpoint",
                    name="Collector Callback Endpoint",
                    icon="mdi:lan-pending",
                    enabled_default=True,
                ),
            )

            await entity.async_set_value("ess.eybond.com")

            self.assertEqual(
                coordinator.calls,
                [
                    {
                        "endpoint": "ess.eybond.com",
                        "apply_changes": False,
                        "confirm_redirect": True,
                    }
                ],
            )

        asyncio.run(_run())

    def test_collector_text_shows_pending_endpoint_override(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.data.values["collector_callback_endpoint_pending"] = "10.0.0.25,18899"
        coordinator.data.values["collector_callback_endpoint_pending_apply_required"] = True
        entity = EybondCollectorText(
            coordinator,
            _CollectorTextSpec(
                key="collector_callback_endpoint",
                translation_key="collector_callback_endpoint",
                name="Collector Callback Endpoint",
                icon="mdi:lan-pending",
                enabled_default=True,
            ),
        )

        self.assertEqual(entity.native_value, "10.0.0.25,18899")
        self.assertEqual(
            entity.extra_state_attributes["current_callback_endpoint"],
            "192.168.1.193,18899,TCP",
        )
        self.assertEqual(
            entity.extra_state_attributes["pending_callback_endpoint"],
            "10.0.0.25,18899",
        )
        self.assertTrue(entity.extra_state_attributes["pending_apply_required"])


if __name__ == "__main__":
    unittest.main()
