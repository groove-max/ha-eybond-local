from __future__ import annotations

from datetime import datetime
from pathlib import Path
import asyncio
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_button_stubs() -> None:
    def _ensure_module(name: str) -> types.ModuleType:
        module = sys.modules.get(name)
        if module is None:
            module = types.ModuleType(name)
            sys.modules[name] = module
        return module

    ha = _ensure_module("homeassistant")
    components = _ensure_module("homeassistant.components")
    button = _ensure_module("homeassistant.components.button")
    config_entries = _ensure_module("homeassistant.config_entries")
    helpers = _ensure_module("homeassistant.helpers")
    entity = _ensure_module("homeassistant.helpers.entity")
    entity_platform = _ensure_module("homeassistant.helpers.entity_platform")
    update_coordinator = _ensure_module("homeassistant.helpers.update_coordinator")
    util = _ensure_module("homeassistant.util")
    dt = _ensure_module("homeassistant.util.dt")

    class ButtonEntity:
        pass

    class ConfigEntry:
        pass

    class EntityCategory:
        DIAGNOSTIC = "diagnostic"
        CONFIG = "config"

    class AddEntitiesCallback:
        pass

    class CoordinatorEntity:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, coordinator):
            self.coordinator = coordinator

    button.ButtonEntity = ButtonEntity
    config_entries.ConfigEntry = ConfigEntry
    entity.EntityCategory = EntityCategory
    entity_platform.AddEntitiesCallback = AddEntitiesCallback
    update_coordinator.CoordinatorEntity = CoordinatorEntity
    dt.now = lambda: datetime(2026, 4, 28, 12, 0, 0)
    util.dt = dt

    ha.components = components
    ha.config_entries = config_entries
    ha.helpers = helpers
    ha.util = util
    components.button = button
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


_install_button_stubs()


from custom_components.eybond_local.button import (  # noqa: E402
    EybondToolingButton,
    _ToolingButtonSpec,
    _tooling_button_specs_for_runtime,
)
from custom_components.eybond_local.models import RuntimeSnapshot  # noqa: E402


class _CoordinatorStub:
    def __init__(self) -> None:
        self.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"collector_ip": "192.168.1.55"},
        )
        self.data = RuntimeSnapshot(
            connected=True,
            values={
                "collector_server_endpoint": "192.168.1.193,18899,TCP",
                "collector_cloud_family": "smartess_at",
                "collector_cloud_family_source": "explicit_endpoint_port",
                "collector_cloud_family_confidence": "medium",
                "collector_reboot_required": "1",
                "proxy_capture_status": "running",
                "proxy_capture_status_label": "Running",
                "proxy_capture_summary": "Collector proxy capture is active.",
                "proxy_capture_blocking_reason": "",
                "proxy_capture_can_start": False,
                "proxy_capture_can_stop": True,
                "proxy_capture_redirect_required": True,
                "proxy_capture_collector_cloud_family": "smartess_at",
                "proxy_trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
                "proxy_trace_manifest_path": "/config/eybond_local/proxy_traces/session.json",
                "proxy_trace_line_count": 7,
                "proxy_trace_kind_summary": "chunk=4, frame=2, masked_endpoint_response=1",
                "proxy_trace_recent_kinds": "chunk -> frame -> masked_endpoint_response",
                "proxy_trace_last_timestamp": "2026-04-28T12:00:03Z",
            },
        )
        self.effective_profile_name = "builtin:profiles/modbus_smg/default.json"
        self.effective_register_schema_name = "builtin:register_schemas/modbus_smg/models/smg_6200.json"
        self.control_mode = "full"
        self.controls_enabled = True
        self.controls_reason = "manual_full_override"
        self.collector_callback_target_endpoint = "203.0.113.7,2223,TCP"
        self.proxy_capture_target_endpoint = "203.0.113.7,18899,TCP"
        self.collector_server_endpoint_rollback_target = "47.91.67.66,18899,TCP"
        self.support_package_export_running = False
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.proxy_capture_overview = types.SimpleNamespace(
            can_start=True,
            can_stop=False,
            status="ready",
            blocking_reason="",
            redirect_required=False,
        )

    @property
    def collector_actions_enabled(self) -> bool:
        return self.control_mode in {"auto", "full"}

    def inverter_device_info(self):
        return {"scope": "inverter"}

    def collector_device_info(self):
        return {"scope": "collector"}

    def collector_configuration_lock_reason(self) -> str | None:
        status = str(getattr(self.proxy_capture_overview, "status", "") or "").strip()
        if status in {"starting", "stopping", "restoring"}:
            return "Proxy capture is changing the collector callback. Wait for the transition to finish."
        if status == "running":
            return "Stop proxy capture before changing collector callback actions."
        return self.collector_operation_mode_apply_lock_reason()

    def collector_operation_mode_apply_lock_reason(self) -> str | None:
        sync_status = str(
            self.data.values.get("collector_operation_endpoint_sync_status") or ""
        ).strip()
        if sync_status in {"applied", "waiting_for_collector", "cooldown"}:
            return (
                "Collector is applying the new operation mode. "
                "Wait for the collector to restart and reconnect."
            )
        return None

    async def async_apply_collector_changes(self, **kwargs):
        self.calls.append(("apply", dict(kwargs)))

    async def async_trigger_collector_rediscovery(self, **kwargs):
        self.calls.append(("rediscover", dict(kwargs)))

    async def async_bind_collector_to_home_assistant(self, **kwargs):
        self.calls.append(("bind", dict(kwargs)))

    async def async_reboot_collector(self, **kwargs):
        self.calls.append(("reboot", dict(kwargs)))

    async def async_rollback_collector_server_endpoint(self, **kwargs):
        self.calls.append(("rollback", dict(kwargs)))

    async def async_start_proxy_capture(self, **kwargs):
        self.calls.append(("start_proxy_capture", dict(kwargs)))

    async def async_stop_proxy_capture(self, **kwargs):
        self.calls.append(("stop_proxy_capture", dict(kwargs)))

    async def async_request_refresh(self):
        self.calls.append(("request_refresh", {}))

    async def async_export_support_package(self):
        self.calls.append(("create_support_package", {}))
        return "/config/eybond_local/support_packages/entry-1.zip"


class ToolingButtonTests(unittest.TestCase):
    def test_collector_tooling_specs_are_enabled_by_default(self) -> None:
        specs = {
            spec.key: spec
            for spec in _tooling_button_specs_for_runtime(set(), "smg_modbus.json")
        }

        self.assertNotIn("bind_collector_to_home_assistant", specs)
        self.assertTrue(specs["apply_collector_changes"].enabled_default)
        self.assertTrue(specs["rediscover_collector"].enabled_default)
        self.assertTrue(specs["reboot_collector"].enabled_default)
        self.assertNotIn("rollback_collector_server_endpoint", specs)
        self.assertTrue(specs["start_proxy_capture"].enabled_default)
        self.assertTrue(specs["stop_proxy_capture"].enabled_default)
        self.assertEqual(specs["apply_collector_changes"].entity_category, "diagnostic")

    def test_collector_only_runtime_skips_metadata_draft_buttons(self) -> None:
        specs = {
            spec.key: spec
            for spec in _tooling_button_specs_for_runtime(
                set(),
                "smg_modbus.json",
                has_inverter_identity=False,
            )
        }

        self.assertIn("create_support_package", specs)
        self.assertIn("apply_collector_changes", specs)
        self.assertIn("rediscover_collector", specs)
        self.assertNotIn("reload_local_metadata", specs)
        self.assertNotIn("create_local_profile_draft", specs)
        self.assertNotIn("create_local_schema_draft", specs)

    def test_runtime_without_proxy_capture_skips_proxy_buttons(self) -> None:
        specs = {
            spec.key: spec
            for spec in _tooling_button_specs_for_runtime(
                set(),
                "smg_modbus.json",
                collector_proxy_capture_allowed=False,
            )
        }

        self.assertIn("create_support_package", specs)
        self.assertIn("reboot_collector", specs)
        self.assertNotIn("start_proxy_capture", specs)
        self.assertNotIn("stop_proxy_capture", specs)

    def test_create_support_package_button_is_disabled_while_export_running(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.support_package_export_running = True
        coordinator.data.values["support_package_export_running"] = True
        coordinator.data.values["support_package_export_status"] = "running"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="create_support_package",
                name="Create Support Archive",
                icon="mdi:package-variant-closed",
            ),
        )

        self.assertFalse(entity.available)
        self.assertTrue(entity.extra_state_attributes["support_package_export_running"])
        self.assertEqual(
            entity.extra_state_attributes["support_package_export_status"],
            "running",
        )

    def test_create_support_package_button_press_rejects_duplicate_export(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            coordinator.support_package_export_running = True
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="create_support_package",
                    name="Create Support Archive",
                    icon="mdi:package-variant-closed",
                ),
            )

            with self.assertRaisesRegex(RuntimeError, "support_package_export_in_progress"):
                await entity.async_press()

            self.assertEqual(coordinator.calls, [])

        asyncio.run(_run())

    def test_rediscover_collector_button_stays_available_while_waiting_for_reconnect(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.control_mode = "read_only"
        coordinator.data = RuntimeSnapshot(
            connected=False,
            values={
                **coordinator.data.values,
                "collector_operation_endpoint_sync_status": "waiting_for_collector",
            },
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="rediscover_collector",
                name="Re-discover Collector",
                icon="mdi:radar",
                entity_category="diagnostic",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(entity.device_info, {"scope": "collector"})
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")
        self.assertEqual(entity.extra_state_attributes["requires_control_mode"], "none")
        self.assertEqual(entity.extra_state_attributes["target_callback_owner"], "bootstrap")

    def test_rediscover_collector_button_is_disabled_while_proxy_capture_is_running(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=False,
            can_stop=True,
            status="running",
            blocking_reason="session_active",
            redirect_required=False,
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="rediscover_collector",
                name="Re-discover Collector",
                icon="mdi:radar",
                entity_category="diagnostic",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Stop proxy capture before requesting collector callback.",
        )

    def test_collector_tooling_button_routes_to_collector_device(self) -> None:
        coordinator = _CoordinatorStub()
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertEqual(entity.device_info, {"scope": "collector"})
        self.assertEqual(entity._attr_translation_key, "bind_collector_to_home_assistant")
        self.assertTrue(entity.available)
        self.assertEqual(entity.extra_state_attributes["action_scope"], "collector")
        self.assertEqual(
            entity.extra_state_attributes["requires_control_mode"],
            "auto_or_full",
        )
        self.assertEqual(entity.extra_state_attributes["confirmation_mode"], "baked_in")
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")
        self.assertFalse(entity.extra_state_attributes["expert_action"])
        self.assertEqual(entity.extra_state_attributes["collector_cloud_family"], "smartess_at")
        self.assertEqual(
            entity.extra_state_attributes["collector_cloud_family_source"],
            "explicit_endpoint_port",
        )
        self.assertEqual(
            entity.extra_state_attributes["proxy_capture_collector_cloud_family"],
            "smartess_at",
        )
        self.assertEqual(
            entity.extra_state_attributes["target_callback_owner"],
            "home_assistant",
        )
        self.assertEqual(
            entity.extra_state_attributes["collector_rollback_endpoint"],
            "47.91.67.66,18899,TCP",
        )

    def test_bind_collector_button_reports_when_home_assistant_is_already_active_target(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.data.values["collector_server_endpoint"] = "203.0.113.7,2223,TCP"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Collector already points to Home Assistant.",
        )

    def test_bind_collector_button_is_available_in_auto_when_controls_are_enabled(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.control_mode = "auto"
        coordinator.controls_reason = "autodetected_high_confidence"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")

    def test_bind_collector_button_is_disabled_while_proxy_capture_is_starting(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=False,
            can_stop=False,
            status="starting",
            blocking_reason="session_active",
            redirect_required=True,
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Proxy capture is changing the collector callback. Wait for the transition to finish.",
        )

    def test_bind_collector_button_stays_available_when_proxy_target_is_active(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.data.values["collector_server_endpoint"] = "203.0.113.7,18899,TCP"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")

    def test_bind_collector_button_stays_available_in_auto_when_inverter_controls_are_disabled(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.control_mode = "auto"
        coordinator.controls_enabled = False
        coordinator.controls_reason = "autodetected_below_write_threshold"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Ready",
        )

    def test_bind_collector_button_requires_auto_or_full_control(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.control_mode = "read_only"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="bind_collector_to_home_assistant",
                name="Bind Collector to Home Assistant",
                icon="mdi:home-import-outline",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Requires Auto or Full Control.",
        )

    def test_collector_rollback_button_reports_missing_cached_endpoint(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.collector_server_endpoint_rollback_target = ""
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="rollback_collector_server_endpoint",
                name="Rollback Collector Callback",
                icon="mdi:backup-restore",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertFalse(entity.extra_state_attributes["rollback_ready"])
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "No cached callback endpoint is available yet. Rollback becomes available after one collector redirect change.",
        )

    def test_collector_rollback_button_is_disabled_when_current_endpoint_matches_source_of_truth(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.data.values["collector_server_endpoint"] = "47.91.67.66,18899,TCP"
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="rollback_collector_server_endpoint",
                name="Rollback Collector Callback",
                icon="mdi:backup-restore",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertFalse(entity.extra_state_attributes["rollback_ready"])
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Collector already matches the source-of-truth callback endpoint.",
        )

    def test_collector_rollback_button_is_disabled_while_proxy_capture_is_running(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=False,
            can_stop=True,
            status="running",
            blocking_reason="session_active",
            redirect_required=True,
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="rollback_collector_server_endpoint",
                name="Rollback Collector Callback",
                icon="mdi:backup-restore",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Stop proxy capture before changing collector callback actions.",
        )

    def test_reboot_collector_button_is_disabled_while_proxy_capture_is_running(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=False,
            can_stop=True,
            status="running",
            blocking_reason="session_active",
            redirect_required=True,
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="reboot_collector",
                name="Restart Collector",
                icon="mdi:restart",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Stop proxy capture before changing collector callback actions.",
        )

    def test_reboot_collector_button_is_available_for_virtual_bridge_without_reboot_feature(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.data = RuntimeSnapshot(
            connected=True,
            values={
                **coordinator.data.values,
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
            },
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="reboot_collector",
                name="Restart Collector",
                icon="mdi:restart",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)

    def test_reboot_collector_button_is_available_for_virtual_bridge(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.data = RuntimeSnapshot(
            connected=True,
            values={
                **coordinator.data.values,
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
            },
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="reboot_collector",
                name="Restart Collector",
                icon="mdi:restart",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")

    def test_apply_collector_changes_button_is_disabled_while_mode_change_applies(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.collector_configuration_lock_reason = lambda: (
            "Collector is applying the new operation mode. Wait for the collector to restart and reconnect."
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="apply_collector_changes",
                name="Apply Collector Changes",
                icon="mdi:check-network-outline",
                entity_category="diagnostic",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Collector is applying the new operation mode. Wait for the collector to restart and reconnect.",
        )

    def test_start_proxy_capture_button_uses_proxy_overview_availability(self) -> None:
        coordinator = _CoordinatorStub()
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="start_proxy_capture",
                name="Start Proxy Capture",
                icon="mdi:transit-connection-variant",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(entity.device_info, {"scope": "collector"})
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")
        self.assertEqual(entity.extra_state_attributes["proxy_trace_line_count"], 7)
        self.assertEqual(
            entity.extra_state_attributes["proxy_trace_recent_kinds"],
            "chunk -> frame -> masked_endpoint_response",
        )
        self.assertEqual(
            entity.extra_state_attributes["proxy_trace_last_timestamp"],
            "2026-04-28T12:00:03Z",
        )

    def test_start_proxy_capture_button_reports_missing_upstream_endpoint(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=False,
            can_stop=False,
            status="blocked",
            blocking_reason="upstream_endpoint_unavailable",
            redirect_required=False,
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="start_proxy_capture",
                name="Start Proxy Capture",
                icon="mdi:transit-connection-variant",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "No upstream callback endpoint is available yet. Restore cloud access first or wait for one external callback endpoint to be detected.",
        )

    def test_start_proxy_capture_button_is_disabled_while_mode_change_applies(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.collector_operation_mode_apply_lock_reason = lambda: (
            "Collector is applying the new operation mode. Wait for the collector to restart and reconnect."
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="start_proxy_capture",
                name="Start Proxy Capture",
                icon="mdi:transit-connection-variant",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "Collector is applying the new operation mode. Wait for the collector to restart and reconnect.",
        )

    def test_stop_proxy_capture_button_reports_when_session_not_active(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=False,
            can_stop=False,
            status="ready",
            blocking_reason="",
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="stop_proxy_capture",
                name="Stop Proxy Capture",
                icon="mdi:stop-circle-outline",
                entity_category="config",
            ),
        )

        self.assertFalse(entity.available)
        self.assertEqual(
            entity.extra_state_attributes["availability_reason"],
            "No proxy capture session is active.",
        )

    def test_collector_rollback_button_press_uses_explicit_flags(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="rollback_collector_server_endpoint",
                    name="Rollback Collector Callback",
                    icon="mdi:backup-restore",
                    entity_category="config",
                ),
            )

            await entity.async_press()

            self.assertEqual(
                coordinator.calls,
                [
                    (
                        "rollback",
                        {
                            "apply_changes": True,
                            "confirm_redirect": True,
                        },
                    )
                ],
            )

        asyncio.run(_run())

    def test_bind_collector_button_press_uses_explicit_flags(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="bind_collector_to_home_assistant",
                    name="Bind Collector to Home Assistant",
                    icon="mdi:home-import-outline",
                    entity_category="config",
                ),
            )

            await entity.async_press()

            self.assertEqual(
                coordinator.calls,
                [("bind", {"confirm_redirect": True})],
            )

        asyncio.run(_run())

    def test_rediscover_collector_button_press_calls_coordinator(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="rediscover_collector",
                    name="Re-discover Collector",
                    icon="mdi:radar",
                    entity_category="diagnostic",
                ),
            )

            await entity.async_press()

            self.assertEqual(coordinator.calls, [("rediscover", {})])

        asyncio.run(_run())

    def test_start_proxy_capture_button_press_calls_coordinator(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="start_proxy_capture",
                    name="Start Proxy Capture",
                    icon="mdi:transit-connection-variant",
                    entity_category="config",
                ),
            )

            await entity.async_press()

            self.assertEqual(coordinator.calls, [("start_proxy_capture", {"confirm_redirect": False})])

        asyncio.run(_run())

    def test_start_proxy_capture_button_stays_available_when_redirect_is_required(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.proxy_capture_overview = types.SimpleNamespace(
            can_start=True,
            can_stop=False,
            status="ready",
            blocking_reason="",
            redirect_required=True,
        )
        entity = EybondToolingButton(
            coordinator,
            _ToolingButtonSpec(
                key="start_proxy_capture",
                name="Start Proxy Capture",
                icon="mdi:transit-connection-variant",
                entity_category="config",
            ),
        )

        self.assertTrue(entity.available)
        self.assertEqual(entity.extra_state_attributes["availability_reason"], "Ready")

    def test_start_proxy_capture_button_press_confirms_redirect_when_required(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            coordinator.proxy_capture_overview = types.SimpleNamespace(
                can_start=True,
                can_stop=False,
                status="ready",
                blocking_reason="",
                redirect_required=True,
            )
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="start_proxy_capture",
                    name="Start Proxy Capture",
                    icon="mdi:transit-connection-variant",
                    entity_category="config",
                ),
            )

            await entity.async_press()

            self.assertEqual(coordinator.calls, [("start_proxy_capture", {"confirm_redirect": True})])

        asyncio.run(_run())

    def test_stop_proxy_capture_button_press_calls_coordinator(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            coordinator.proxy_capture_overview = types.SimpleNamespace(
                can_start=False,
                can_stop=True,
                status="running",
                blocking_reason="session_active",
            )
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="stop_proxy_capture",
                    name="Stop Proxy Capture",
                    icon="mdi:stop-circle-outline",
                    entity_category="config",
                ),
            )

            await entity.async_press()

            self.assertEqual(coordinator.calls, [("stop_proxy_capture", {})])

        asyncio.run(_run())

    def test_stop_proxy_capture_button_swallows_stale_not_running_and_refreshes(self) -> None:
        async def _run() -> None:
            coordinator = _CoordinatorStub()
            coordinator.proxy_capture_overview = types.SimpleNamespace(
                can_start=False,
                can_stop=True,
                status="running",
                blocking_reason="session_active",
            )

            async def _stop_proxy_capture(**kwargs):
                coordinator.calls.append(("stop_proxy_capture", dict(kwargs)))
                raise RuntimeError("proxy_capture_not_running")

            coordinator.async_stop_proxy_capture = _stop_proxy_capture
            entity = EybondToolingButton(
                coordinator,
                _ToolingButtonSpec(
                    key="stop_proxy_capture",
                    name="Stop Proxy Capture",
                    icon="mdi:stop-circle-outline",
                    entity_category="config",
                ),
            )

            await entity.async_press()

            self.assertEqual(
                coordinator.calls,
                [("stop_proxy_capture", {}), ("request_refresh", {})],
            )

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
