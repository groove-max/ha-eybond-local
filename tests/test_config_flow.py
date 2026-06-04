from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch, sentinel


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_homeassistant_stubs() -> None:
    voluptuous = types.ModuleType("voluptuous")
    ha = sys.modules.get("homeassistant") or types.ModuleType("homeassistant")
    config_entries = types.ModuleType("homeassistant.config_entries")
    core = types.ModuleType("homeassistant.core")
    data_entry_flow = types.ModuleType("homeassistant.data_entry_flow")
    helpers = types.ModuleType("homeassistant.helpers")
    selector = types.ModuleType("homeassistant.helpers.selector")

    class ConfigFlow:
        def __init_subclass__(cls, **kwargs):
            return super().__init_subclass__()

        def async_show_menu(self, *, step_id, menu_options, description_placeholders=None):
            return {
                "type": "menu",
                "step_id": step_id,
                "menu_options": list(menu_options),
                "description_placeholders": description_placeholders or {},
            }

        def async_show_form(self, *, step_id, data_schema=None, errors=None, description_placeholders=None):
            return {
                "type": "form",
                "step_id": step_id,
                "data_schema": data_schema,
                "errors": errors or {},
                "description_placeholders": description_placeholders or {},
            }

        def async_show_progress(self, *, step_id, progress_action, progress_task, description_placeholders=None):
            return {
                "type": "progress",
                "step_id": step_id,
                "progress_action": progress_action,
                "progress_task": progress_task,
                "description_placeholders": description_placeholders or {},
            }

        def async_show_progress_done(self, *, next_step_id):
            return {
                "type": "progress_done",
                "next_step_id": next_step_id,
            }

        def async_update_progress(self, progress):
            self._test_progress = progress

        async def async_set_unique_id(self, unique_id):
            self._test_unique_id = unique_id

        def _abort_if_unique_id_configured(self):
            return None

        def async_create_entry(self, *, title, data, options=None):
            result = {"type": "create_entry", "title": title, "data": data}
            if options is not None:
                result["options"] = options
            return result

        def async_abort(self, *, reason):
            return {"type": "abort", "reason": reason}

    class OptionsFlow:
        def async_show_menu(self, *, step_id, menu_options, description_placeholders=None):
            return {
                "type": "menu",
                "step_id": step_id,
                "menu_options": list(menu_options),
                "description_placeholders": description_placeholders or {},
            }

        def async_show_form(self, *, step_id, data_schema=None, errors=None, description_placeholders=None):
            return {
                "type": "form",
                "step_id": step_id,
                "data_schema": data_schema,
                "errors": errors or {},
                "description_placeholders": description_placeholders or {},
            }

        def async_create_entry(self, *, data):
            return {"type": "create_entry", "data": data}

    def callback(func):
        return func

    class HomeAssistant:
        pass

    def split_entity_id(entity_id):
        return tuple(str(entity_id).split(".", 1))

    class SupportsResponse:
        ONLY = "only"

    def section(schema, _options=None):
        return schema

    class _SelectorConfig:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class _Selector:
        def __init__(self, config=None):
            self.config = config

    class SelectOptionDict(dict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

    class Schema:
        def __init__(self, schema):
            self.schema = schema

    def Required(key, default=None):
        return key

    def Optional(key, default=None):
        return key

    def All(*validators):
        return validators

    def Range(**kwargs):
        return kwargs

    def In(value):
        return value

    config_entries.ConfigFlow = ConfigFlow
    config_entries.ConfigFlowResult = dict
    config_entries.OptionsFlow = OptionsFlow
    core.HomeAssistant = HomeAssistant
    core.callback = callback
    core.split_entity_id = split_entity_id
    core.SupportsResponse = SupportsResponse
    data_entry_flow.section = section

    selector.BooleanSelector = _Selector
    selector.NumberSelector = _Selector
    selector.NumberSelectorConfig = _SelectorConfig
    selector.NumberSelectorMode = types.SimpleNamespace(BOX="box", SLIDER="slider")
    selector.SelectOptionDict = SelectOptionDict
    selector.SelectSelector = _Selector
    selector.SelectSelectorConfig = _SelectorConfig
    selector.SelectSelectorMode = types.SimpleNamespace(DROPDOWN="dropdown")
    selector.TextSelector = _Selector
    selector.TextSelectorConfig = _SelectorConfig

    voluptuous.Schema = Schema
    voluptuous.Required = Required
    voluptuous.Optional = Optional
    voluptuous.All = All
    voluptuous.Range = Range
    voluptuous.In = In

    sys.modules["voluptuous"] = voluptuous
    sys.modules["homeassistant"] = ha
    sys.modules["homeassistant.config_entries"] = config_entries
    sys.modules["homeassistant.core"] = core
    sys.modules["homeassistant.data_entry_flow"] = data_entry_flow
    sys.modules["homeassistant.helpers"] = helpers
    sys.modules["homeassistant.helpers.selector"] = selector


_install_homeassistant_stubs()


import custom_components.eybond_local.config_flow as config_flow_module
from custom_components.eybond_local.config_flow import (
    BLE_ACTION_APPLY,
    BLE_ACTION_RESCAN,
    BLE_ACTION_REFRESH_WIFI,
    COLLECTOR_WIFI_ACTION_APPLY,
    COLLECTOR_WIFI_ACTION_REFRESH,
    COLLECTOR_NETWORK_ALREADY_CONNECTED,
    COLLECTOR_NETWORK_NEEDS_BLUETOOTH,
    COLLECTOR_OPERATION_HA_ONLY,
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
    CONF_BLE_ACTION,
    CONF_COLLECTOR_WIFI_ACTION,
    CONF_COLLECTOR_NETWORK_STATUS,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_CONFIRM_COLLECTOR_WIFI_APPLY,
    CONF_CONFIRM_COLLECTOR_ENDPOINT_RISK,
    CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE,
    CONF_SETUP_MODE,
    CONF_WIFI_PASSWORD,
    CONF_WIFI_SSID,
    CONF_RESULT_KEY,
    EybondLocalConfigFlow,
    EybondLocalOptionsFlow,
    SETUP_MODE_DEEP_SCAN,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
    _get_ipv4_interfaces,
    _flatten_sections,
)
from custom_components.eybond_local.collector.smartess_ble import SmartEssBleCandidate
from custom_components.eybond_local.collector.smartess_ble import (
    SmartEssBleError,
    SmartEssBleProvisionBranch,
    SmartEssBleProvisioningInfo,
    SmartEssBleProvisionOutcome,
    SmartEssBleProvisionResult,
    SmartEssBleWifiNetwork,
)
from custom_components.eybond_local.collector.smartess_local import (
    SET_REBOOT_OR_APPLY,
    SET_TARGET_PASSWORD,
    SET_TARGET_SSID,
)
from custom_components.eybond_local.const import (
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_DRIVER_HINT,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
)
from custom_components.eybond_local.metadata.local_metadata import (
    local_profile_path,
    local_register_schema_path,
)
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import (
    CollectorCandidate,
    CollectorInfo,
    DriverMatch,
    OnboardingResult,
    ProbeTarget,
)
from custom_components.eybond_local.onboarding.detection import DiscoveryTarget
from custom_components.eybond_local.support.workflow import build_support_workflow_state
from custom_components.eybond_local.support.cloud_evidence import CloudEvidenceRecord, build_cloud_evidence_payload


class _FakeEntry:
    def __init__(self, entry_id: str, *, server_ip: str, tcp_port: int) -> None:
        self.entry_id = entry_id
        self.data = {"server_ip": server_ip, "tcp_port": tcp_port}
        self.options = {}


class _FakeConfigEntries:
    def __init__(self, entries=None) -> None:
        self._entries = list(entries or [])
        self.unloaded: list[str] = []
        self.reloaded: list[str] = []

    def async_entries(self, _domain):
        return list(self._entries)

    async def async_unload(self, entry_id: str):
        self.unloaded.append(entry_id)
        return True

    async def async_reload(self, entry_id: str):
        self.reloaded.append(entry_id)
        return True


class _FakeHass:
    def __init__(self, entries=None) -> None:
        class _Services:
            def __init__(self) -> None:
                self.registered: list[tuple[str, str]] = []

            def async_register(self, domain, service, _handler, **_kwargs) -> None:
                self.registered.append((domain, service))

        self.config_entries = _FakeConfigEntries(entries)
        self.config = types.SimpleNamespace(language="en", config_dir="/config", time_zone="UTC")
        self.data: dict[str, object] = {}
        self.services = _Services()
        self.executor_job_calls: list[tuple[object, tuple[object, ...]]] = []

    async def async_add_executor_job(self, func, *args):
        self.executor_job_calls.append((func, args))
        return func(*args)

    def async_create_task(self, coro):
        return asyncio.create_task(coro)


class _DoneTask:
    def __init__(self, exception=None) -> None:
        self._exception = exception

    def done(self) -> bool:
        return True

    def exception(self):
        return self._exception


class _PendingTask:
    def done(self) -> bool:
        return False


@dataclass(frozen=True)
class _SmartEssDraftPlan:
    source_profile_name: str
    source_schema_name: str
    driver_label: str
    reason: str


@dataclass(frozen=True)
class _SmartEssSmgBridgePlan:
    source_profile_name: str
    source_schema_name: str
    bridge_label: str
    reason: str
    profile_enable_keys: tuple[str, ...] = ()
    measurement_enable_keys: tuple[str, ...] = ()
    blocked_field_titles: tuple[str, ...] = ()
    skipped_field_titles: tuple[str, ...] = ()


class ConfigFlowTests(unittest.IsolatedAsyncioTestCase):
    def _make_flow(self, *, entries=None) -> EybondLocalConfigFlow:
        flow = EybondLocalConfigFlow()
        flow.hass = _FakeHass(entries)
        flow.context = {}
        flow._local_ip = "192.168.1.50"
        flow._auto_config = {"server_ip": "192.168.1.50"}
        flow._interface_options = [
            {
                "name": "eth0",
                "ip": "192.168.1.50",
                "label": "eth0 - 192.168.1.50",
                "network": "192.168.0.0/16",
                "broadcast": "192.168.255.255",
            },
        ]
        return flow

    def _make_options_flow(self) -> EybondLocalOptionsFlow:
        entry = type("_Entry", (), {})()
        entry.data = {
            "connection_type": "eybond",
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
            "detected_model": "SMG 6200",
            "detected_serial": "12345",
            "detection_confidence": "high",
            "control_mode": "auto",
        }
        entry.options = {}
        entry.runtime_data = {}
        options = EybondLocalOptionsFlow(entry)
        options.hass = _FakeHass()
        options.context = {}
        return options

    async def test_scanning_without_results_routes_to_scan_results(self) -> None:
        flow = self._make_flow()
        flow._scan_task = _DoneTask()
        flow._scan_progress_visible = True
        flow._autodetect_results = {}

        result = await flow.async_step_scanning()

        self.assertEqual(result["type"], "progress_done")
        self.assertEqual(result["next_step_id"], "scan_results")
        self.assertTrue(flow._scan_error)

    async def test_scanning_progress_shows_estimated_progress_bar(self) -> None:
        flow = self._make_flow()
        flow._scan_task = _PendingTask()
        flow._scan_started_monotonic = 100.0
        flow._scan_progress_stage = "discovering"

        with patch(
            "custom_components.eybond_local.config_flow.time.monotonic",
            return_value=112.0,
        ):
            result = await flow.async_step_scanning()

        self.assertEqual(result["type"], "progress")
        placeholders = result["description_placeholders"]
        self.assertEqual(placeholders["scan_progress_phase"], "Sending discovery probes")
        self.assertIn("[", placeholders["scan_progress_bar"])
        self.assertIn("%", placeholders["scan_progress_bar"])
        self.assertIn("12s elapsed", placeholders["scan_progress_detail"])
        self.assertNotIn("remaining", placeholders["scan_progress_detail"])

    def test_get_ipv4_interfaces_parses_busybox_oneline_output(self) -> None:
        output = (
            "1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever preferred_lft forever\n"
            "2: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0\\       valid_lft forever preferred_lft forever\n"
            "3: wlan0    inet 192.168.1.50/24 brd 192.168.1.255 scope global dynamic noprefixroute wlan0\\       valid_lft 42620sec preferred_lft 42620sec\n"
            "4: hassio    inet 172.30.32.1/23 brd 172.30.33.255 scope global hassio\\       valid_lft forever preferred_lft forever\n"
        )

        with patch(
            "custom_components.eybond_local.config_flow.subprocess.check_output",
            side_effect=[subprocess.CalledProcessError(1, ["ip"]), output],
        ):
            interfaces = _get_ipv4_interfaces()

        wlan0 = next(interface for interface in interfaces if interface["name"] == "wlan0")
        self.assertEqual(wlan0["ip"], "192.168.1.50")
        self.assertEqual(wlan0["network"], "192.168.1.0/24")
        self.assertEqual(wlan0["broadcast"], "192.168.1.255")
        self.assertFalse(any(interface["name"] == "docker0" for interface in interfaces))
        self.assertFalse(any(interface["name"] == "hassio" for interface in interfaces))

    async def test_scanning_shows_progress_once_even_if_task_finishes_immediately(self) -> None:
        flow = self._make_flow()

        def _done_task(coro):
            coro.close()
            return _DoneTask()

        flow.hass.async_create_task = _done_task

        first = await flow.async_step_scanning()
        second = await flow.async_step_scanning()

        self.assertEqual(first["type"], "progress")
        self.assertEqual(second["type"], "progress_done")

    async def test_async_ensure_network_defaults_heals_stale_auto_server_ip(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.2.50"}

        with patch(
            "custom_components.eybond_local.config_flow._get_ipv4_interfaces",
            return_value=[
                {
                    "name": "eth0",
                    "ip": "192.168.1.50",
                    "label": "eth0 - 192.168.1.50",
                    "network": "192.168.0.0/16",
                    "broadcast": "192.168.255.255",
                },
            ],
        ), patch(
            "custom_components.eybond_local.config_flow._get_local_ip",
            return_value="192.168.1.50",
        ):
            await flow._async_ensure_network_defaults()

        self.assertEqual(flow._auto_config["server_ip"], "192.168.1.50")
        self.assertEqual(flow._scan_discovery_targets()[0].ip, "192.168.255.255")
        self.assertEqual(flow._deep_scan_plan()["network_cidr"], "192.168.0.0/16")

    async def test_user_step_shows_connection_type_selector_only(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_user()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "user")
        self.assertIn("connection_type", result["data_schema"].schema)
        self.assertNotIn("server_ip", result["data_schema"].schema)
        self.assertNotIn("setup_mode", result["data_schema"].schema)

    async def test_user_step_preloads_translation_bundle_via_executor(self) -> None:
        flow = self._make_flow()

        await flow.async_step_user()

        self.assertIn(
            "_load_translation_bundle",
            [getattr(func, "__name__", "") for func, _args in flow.hass.executor_job_calls],
        )

    async def test_user_step_single_interface_welcome_hint_does_not_mention_interface(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_user()

        self.assertNotIn("192.168.1.50", result["description_placeholders"]["welcome_hint"])
        self.assertNotIn("wlan0", result["description_placeholders"]["welcome_hint"])

    async def test_user_step_multi_interface_welcome_hint_does_not_mention_specific_interface(self) -> None:
        flow = self._make_flow()
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "192.168.2.50", "label": "wlan0 - 192.168.2.50"},
        ]

        result = await flow.async_step_user()

        self.assertNotIn("192.168.1.50", result["description_placeholders"]["welcome_hint"])
        self.assertNotIn("192.168.2.50", result["description_placeholders"]["welcome_hint"])
        self.assertNotIn("wlan0", result["description_placeholders"]["welcome_hint"])

    async def test_user_step_routes_to_interface_selection_when_multiple_interfaces(self) -> None:
        flow = self._make_flow()
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "192.168.2.50", "label": "wlan0 - 192.168.2.50"},
        ]

        result = await flow.async_step_user({"connection_type": "eybond"})

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "collector_network")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")

    async def test_collector_network_is_shown_as_menu(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_collector_network()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "collector_network")
        self.assertEqual(result["menu_options"], ["auto", "bluetooth_setup"])

    async def test_collector_network_routes_to_bluetooth_setup_when_collector_is_not_connected(self) -> None:
        flow = self._make_flow()

        menu_result = await flow.async_step_collector_network()

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(flow, "_async_discover_smartess_ble_candidates", new=AsyncMock(return_value=())):
            result = await flow.async_step_bluetooth_setup()

        self.assertEqual(menu_result["type"], "menu")
        self.assertIn("bluetooth_setup", menu_result["menu_options"])
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")

    async def test_collector_network_stays_put_when_ble_host_is_unavailable(self) -> None:
        flow = self._make_flow()

        menu_result = await flow.async_step_collector_network()

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(
                return_value=types.SimpleNamespace(
                    available=False,
                    reason="adapter_not_found",
                    detail="No Bluetooth adapters found",
                )
            ),
        ), patch.object(flow, "_async_discover_smartess_ble_candidates", new=AsyncMock(return_value=())) as discover:
            result = await flow.async_step_bluetooth_setup()

        discover.assert_not_awaited()
        self.assertEqual(menu_result["type"], "menu")
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")
        self.assertEqual(result["errors"], {"base": "ble_unavailable"})
        self.assertEqual(flow._ble_last_error, "No Bluetooth adapters found")

    async def test_collector_network_accepts_home_assistant_bluetooth_proxy_without_local_adapter(self) -> None:
        flow = self._make_flow()
        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        bluetooth_module.async_scanner_count = Mock(return_value=1)
        bluetooth_module.async_discovered_service_info = Mock(return_value=())
        bluetooth_module.async_scanner_devices_by_address = Mock(return_value={})

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(
                return_value=types.SimpleNamespace(
                    available=False,
                    reason="adapter_not_found",
                    detail="No Bluetooth adapters found",
                )
            ),
        ), patch.object(flow, "_async_discover_smartess_ble_candidates", new=AsyncMock(return_value=())):
            result = await flow.async_step_bluetooth_setup()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")

    async def test_collector_network_routes_to_scan_interface_when_collector_is_connected(self) -> None:
        flow = self._make_flow()

        menu_result = await flow.async_step_collector_network()
        result = await flow.async_step_auto()

        self.assertEqual(menu_result["type"], "menu")
        self.assertIn("auto", menu_result["menu_options"])
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "auto")

    async def test_auto_step_routes_directly_to_manual_when_setup_mode_is_manual(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        result = await flow.async_step_auto({"server_ip": "192.168.1.50", CONF_SETUP_MODE: "manual"})

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "manual")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")

    async def test_user_step_routes_to_auto_when_one_interface(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_user({"connection_type": "eybond"})

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "collector_network")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")
        self.assertEqual(flow._auto_config["server_ip"], "192.168.1.50")

    async def test_auto_step_shows_setup_mode_selector(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        result = await flow.async_step_auto()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "auto")
        self.assertIn("setup_mode", result["data_schema"].schema)

    async def test_auto_step_uses_localized_setup_mode_labels(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "ru"
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        result = await flow.async_step_auto()

        selector = result["data_schema"].schema["setup_mode"]
        labels = [option["label"] for option in selector.config.kwargs["options"]]
        self.assertIn("Запустить автопоиск", labels)
        self.assertIn("Запустить глубокое сканирование", labels)
        self.assertIn("Пропустить и перейти к ручной настройке", labels)
        self.assertNotIn("Подключить коллектор к Wi-Fi через Bluetooth", labels)
        self.assertNotIn("Auto scan first", labels)
        self.assertNotIn("Manual setup now", labels)

    async def test_auto_step_uses_localized_interface_hint(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "ru"
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        result = await flow.async_step_auto()

        hint = result["description_placeholders"]["interface_hint"]
        self.assertIn("автоматически", hint)
        self.assertNotIn("Home Assistant will use", hint)

    async def test_auto_step_starts_scanning_when_setup_mode_is_auto(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        async def _fake_scanning(user_input=None):
            return {"type": "progress", "step_id": "scanning"}

        flow.async_step_scanning = _fake_scanning

        result = await flow.async_step_auto({"server_ip": "192.168.1.50", CONF_SETUP_MODE: "auto"})

        self.assertEqual(result["type"], "progress")
        self.assertEqual(flow._auto_config["server_ip"], "192.168.1.50")

    async def test_auto_step_heals_stale_submitted_server_ip(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.104"}

        async def _fake_scanning(user_input=None):
            return {"type": "progress", "step_id": "scanning"}

        flow.async_step_scanning = _fake_scanning

        result = await flow.async_step_auto({"server_ip": "192.168.1.104", CONF_SETUP_MODE: "auto"})

        self.assertEqual(result["type"], "progress")
        self.assertEqual(flow._auto_config["server_ip"], "192.168.1.50")

    async def test_auto_step_routes_to_deep_scan_when_setup_mode_is_deep(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        result = await flow.async_step_auto(
            {"server_ip": "192.168.1.50", CONF_SETUP_MODE: SETUP_MODE_DEEP_SCAN}
        )

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "deep_scan")
        self.assertIn("start_deep_scan", result["menu_options"])
        self.assertEqual(result["description_placeholders"]["deep_scan_network"], "192.168.0.0/16")
        self.assertEqual(result["description_placeholders"]["deep_scan_target_count"], "65533")
        self.assertNotIn("deep_scan_duration", result["description_placeholders"])
        self.assertIn("larger than /24", result["description_placeholders"]["deep_scan_warning"])

    async def test_bluetooth_setup_shows_capability_error_when_host_is_unavailable(self) -> None:
        flow = self._make_flow()

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=False)),
        ):
            result = await flow.async_step_bluetooth_setup(
                {"ble_address": "AA:BB:CC:DD:EE:FF"}
            )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")
        self.assertEqual(result["errors"], {"base": "ble_unavailable"})

    async def test_bluetooth_setup_uses_discovered_collectors_selector(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="BB:CC:DD:EE:FF:00",
                local_pn="A1234567890123",
                local_name="Zulu Collector",
            ),
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="A0000000000001",
                local_name="Alpha Collector",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=()),
        ):
            result = await flow.async_step_bluetooth_setup()

        ble_selector = result["data_schema"].schema["ble_address"]
        options = ble_selector.config.kwargs["options"]
        self.assertEqual(
            [option["value"] for option in options],
            ["AA:BB:CC:DD:EE:FF", "BB:CC:DD:EE:FF:00"],
        )
        self.assertEqual(
            [option["label"] for option in options],
            [
                "Alpha Collector - A0000000000001 - AA:BB:CC:DD:EE:FF",
                "Zulu Collector - A1234567890123 - BB:CC:DD:EE:FF:00",
            ],
        )

    async def test_bluetooth_setup_uses_home_assistant_bluetooth_cache(self) -> None:
        flow = self._make_flow()

        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        bluetooth_module.async_discovered_service_info = Mock(
            return_value=(
                types.SimpleNamespace(
                    address="E8:88:6C:43:C2:47",
                    name="E50000253884199645\u200b",
                    manufacturer_data={0x3545: b"0000253884199645"},
                    service_uuids=(),
                    device=object(),
                ),
            )
        )

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleScanner",
        ) as scanner_cls, patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=()),
        ):
            result = await flow.async_step_bluetooth_setup()

        scanner_cls.assert_not_called()
        ble_selector = result["data_schema"].schema["ble_address"]
        options = ble_selector.config.kwargs["options"]
        self.assertEqual(
            [option["value"] for option in options],
            ["E8:88:6C:43:C2:47"],
        )
        self.assertIn("E50000253884199645", options[0]["label"])

    async def test_bluetooth_setup_uses_home_assistant_bluetooth_advertisement_callback(self) -> None:
        flow = self._make_flow()

        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        bluetooth_module.async_discovered_service_info = Mock(return_value=())
        bluetooth_module.async_scanner_devices_by_address = Mock(return_value=())
        bluetooth_module.BluetoothScanningMode = types.SimpleNamespace(ACTIVE=sentinel.active_scan)
        service_info = types.SimpleNamespace(
            address="E8:88:6C:43:C2:47",
            name="E50000253884199645\u200b",
            manufacturer_data={0x3545: b"0000253884199645"},
            service_uuids=(),
            device=object(),
        )

        def async_register_callback(hass, callback, matcher, mode):
            self.assertIs(hass, flow.hass)
            self.assertEqual(mode, sentinel.active_scan)
            self.assertIn(matcher["connectable"], (False, True))
            callback(service_info, sentinel.bluetooth_change)
            return Mock()

        bluetooth_module.async_register_callback = Mock(side_effect=async_register_callback)

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleScanner",
        ) as scanner_cls, patch(
            "custom_components.eybond_local.config_flow.asyncio.sleep",
            new=AsyncMock(),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=()),
        ):
            result = await flow.async_step_bluetooth_setup()

        scanner_cls.assert_not_called()
        self.assertEqual(bluetooth_module.async_register_callback.call_count, 6)
        ble_selector = result["data_schema"].schema["ble_address"]
        options = ble_selector.config.kwargs["options"]
        self.assertEqual(
            [option["value"] for option in options],
            ["E8:88:6C:43:C2:47"],
        )
        self.assertIn("E50000253884199645", options[0]["label"])

    async def test_bluetooth_setup_skips_raw_bleak_fallback_when_only_ha_proxy_scanners_exist(self) -> None:
        flow = self._make_flow()

        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        bluetooth_module.async_scanner_count = Mock(return_value=1)
        bluetooth_module.async_discovered_service_info = Mock(return_value=())
        bluetooth_module.async_scanner_devices_by_address = Mock(return_value={})

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=False)),
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleScanner",
        ) as scanner_cls:
            result = await flow.async_step_bluetooth_setup()

        scanner_cls.assert_not_called()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")

    async def test_bluetooth_setup_uses_collector_wifi_selector_when_scan_returns_networks(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Collector PN",
            ),
        )
        wifi_networks = (
            SmartEssBleWifiNetwork(ssid="Neighbor", signal=-75),
            SmartEssBleWifiNetwork(ssid="GRooVE", signal=-44),
            SmartEssBleWifiNetwork(ssid="Office", signal=-58),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=wifi_networks),
        ):
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "AA:BB:CC:DD:EE:FF",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        options = wifi_selector.config.kwargs["options"]
        self.assertTrue(wifi_selector.config.kwargs["custom_value"])
        self.assertEqual(
            set(result["data_schema"].schema),
            {"ble_address", "wifi_ssid", "wifi_password", CONF_BLE_ACTION},
        )
        self.assertEqual(
            [option["value"] for option in options],
            ["Neighbor", "GRooVE", "Office"],
        )
        self.assertEqual(
            [option["label"] for option in options],
            ["Neighbor (-75 dBm)", "GRooVE (-44 dBm)", "Office (-58 dBm)"],
        )
        self.assertEqual(result["errors"], {})

    async def test_bluetooth_setup_scans_default_collector_wifi_on_first_entry(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Collector PN",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)),
        ) as wifi_scan:
            result = await flow.async_step_bluetooth_setup()

        wifi_scan.assert_awaited_once_with("AA:BB:CC:DD:EE:FF", ble_device=None)
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        options = wifi_selector.config.kwargs["options"]
        self.assertEqual(options[0]["value"], "GRooVE")
        self.assertEqual(options[0]["label"], "GRooVE (98%)")
        self.assertTrue(wifi_selector.config.kwargs["custom_value"])
        self.assertEqual(
            set(result["data_schema"].schema),
            {"ble_address", "wifi_ssid", "wifi_password", CONF_BLE_ACTION},
        )
        self.assertEqual(result["errors"], {})

    async def test_bluetooth_setup_scans_wifi_for_newly_selected_collector(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Alpha Collector",
            ),
            SmartEssBleCandidate(
                address="11:22:33:44:55:66",
                local_pn="E50000253884199777",
                local_name="Bravo Collector",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                side_effect=(
                    (SmartEssBleWifiNetwork(ssid="Alpha WiFi", signal=92),),
                    (SmartEssBleWifiNetwork(ssid="Bravo WiFi", signal=88),),
                )
            ),
        ) as wifi_scan:
            await flow.async_step_bluetooth_setup()
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "11:22:33:44:55:66",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        self.assertEqual(
            [call.args[0] for call in wifi_scan.await_args_list],
            ["AA:BB:CC:DD:EE:FF", "11:22:33:44:55:66"],
        )
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        options = wifi_selector.config.kwargs["options"]
        self.assertEqual(options[0]["value"], "Bravo WiFi")

    async def test_bluetooth_setup_switching_collectors_ignores_stale_wifi_submission(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Alpha Collector",
            ),
            SmartEssBleCandidate(
                address="11:22:33:44:55:66",
                local_pn="E50000253884199777",
                local_name="Bravo Collector",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                side_effect=(
                    (SmartEssBleWifiNetwork(ssid="Alpha WiFi", signal=92),),
                    (SmartEssBleWifiNetwork(ssid="Bravo WiFi", signal=88),),
                )
            ),
        ) as wifi_scan, patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(return_value=None),
        ) as bootstrap:
            await flow.async_step_bluetooth_setup()
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "11:22:33:44:55:66",
                    "wifi_ssid": "Alpha WiFi",
                    "wifi_password": "Secret123",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        self.assertEqual(
            [call.args[0] for call in wifi_scan.await_args_list],
            ["AA:BB:CC:DD:EE:FF", "11:22:33:44:55:66"],
        )
        bootstrap.assert_not_awaited()
        self.assertEqual(result["errors"], {})
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        options = wifi_selector.config.kwargs["options"]
        self.assertEqual(options[0]["value"], "Bravo WiFi")

    async def test_bluetooth_setup_marks_and_rejects_already_added_ble_candidate(self) -> None:
        existing_entry = types.SimpleNamespace(
            entry_id="existing",
            unique_id="collector:E50000253884199645",
            data={"collector_pn": "E50000253884199645"},
            options={},
        )
        flow = self._make_flow(entries=[existing_entry])
        flow.context = {"entry_id": "existing"}
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Collector PN",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=()),
        ) as wifi_scan:
            first_result = await flow.async_step_bluetooth_setup()
            submit_result = await flow.async_step_bluetooth_setup(
                {"ble_address": "AA:BB:CC:DD:EE:FF"}
            )

        wifi_scan.assert_not_awaited()
        ble_selector = first_result["data_schema"].schema["ble_address"]
        options = ble_selector.config.kwargs["options"]
        self.assertIn("Already added", options[0]["label"])
        self.assertEqual(submit_result["errors"], {"ble_address": "already_added_candidate"})

    async def test_bluetooth_setup_reports_unstable_link_when_collector_wifi_scan_fails(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Collector PN",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(side_effect=SmartEssBleError("ble_wifi_scan_failed:timeout")),
        ) as wifi_scan, patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(return_value=None),
        ) as bootstrap:
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "AA:BB:CC:DD:EE:FF",
                    "wifi_ssid": "Home WiFi",
                    "wifi_password": "Secret123",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        bootstrap.assert_not_awaited()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")
        self.assertEqual(result["errors"], {"base": "ble_wifi_scan_failed"})
        self.assertEqual(flow._ble_last_error, "ble_wifi_scan_failed:timeout")

    async def test_bluetooth_setup_reports_unstable_link_on_first_entry_scan_failure(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Collector PN",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(side_effect=SmartEssBleError("ble_wifi_scan_failed:timeout")),
        ):
            result = await flow.async_step_bluetooth_setup()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "ble_wifi_scan_failed"})
        self.assertEqual(flow._ble_last_error, "ble_wifi_scan_failed:timeout")

    async def test_smartess_ble_wifi_scan_times_out(self) -> None:
        flow = self._make_flow()
        session = Mock()

        async def wait_forever() -> None:
            await asyncio.Event().wait()

        session.connect = AsyncMock(side_effect=wait_forever)
        session.disconnect = AsyncMock(return_value=None)

        with patch(
            "custom_components.eybond_local.config_flow._BLE_CONNECT_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ):
            with self.assertRaisesRegex(SmartEssBleError, "ble_wifi_scan_failed:timeout"):
                await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        session.disconnect.assert_awaited_once()

    async def test_smartess_ble_wifi_scan_times_out_after_connect(self) -> None:
        flow = self._make_flow()
        session = Mock()
        provisioner = Mock()

        async def wait_forever() -> None:
            await asyncio.Event().wait()

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(side_effect=wait_forever)

        with patch(
            "custom_components.eybond_local.config_flow._BLE_WIFI_SCAN_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            with self.assertRaisesRegex(SmartEssBleError, "ble_wifi_scan_failed:timeout"):
                await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        session.disconnect.assert_awaited_once()

    async def test_smartess_ble_wifi_scan_maps_notification_timeout_to_scan_failure(self) -> None:
        flow = self._make_flow()
        session = Mock()
        provisioner = Mock()

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(side_effect=SmartEssBleError("ble_notification_timeout"))

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=()),
        ):
            with self.assertRaisesRegex(SmartEssBleError, "ble_wifi_scan_failed:notification_timeout"):
                await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        self.assertEqual(session.connect.await_count, 3)
        self.assertEqual(provisioner.scan_wifi_networks.await_count, 3)
        self.assertEqual(session.disconnect.await_count, 3)

    async def test_smartess_ble_wifi_scan_retries_once_after_transient_not_connected(self) -> None:
        flow = self._make_flow()
        session = Mock()
        provisioner = Mock()

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            side_effect=(
                SmartEssBleError("ble_not_connected"),
                (SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),),
            )
        )

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=()),
        ):
            result = await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        self.assertEqual(result, (SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),))
        self.assertEqual(session.connect.await_count, 2)
        self.assertEqual(provisioner.scan_wifi_networks.await_count, 2)
        self.assertEqual(session.disconnect.await_count, 2)

    async def test_smartess_ble_wifi_scan_retries_once_after_transient_gatt_error(self) -> None:
        flow = self._make_flow()
        first_session = Mock()
        second_session = Mock()
        first_provisioner = Mock()
        second_provisioner = Mock()

        first_session.connect = AsyncMock(return_value=None)
        first_session.disconnect = AsyncMock(return_value=None)
        first_provisioner.scan_wifi_networks = AsyncMock(
            side_effect=RuntimeError(
                "Bluetooth GATT Error address=AA:BB:CC:DD:EE:FF handle=30 error=133 description=Error"
            )
        )

        second_session.connect = AsyncMock(return_value=None)
        second_session.disconnect = AsyncMock(return_value=None)
        second_provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )
        refreshed_candidate = SmartEssBleCandidate(
            address="AA:BB:CC:DD:EE:FF",
            local_pn="E50000253884199645",
            local_name="Collector",
            device=sentinel.refreshed_ble_device,
        )
        discover = AsyncMock(return_value=(refreshed_candidate,))

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            side_effect=(sentinel.ble_link_first, sentinel.ble_link_second),
        ) as link_cls, patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            side_effect=(first_session, second_session),
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            side_effect=(first_provisioner, second_provisioner),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=discover,
        ):
            result = await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        self.assertEqual(result, (SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),))
        self.assertEqual(link_cls.call_count, 2)
        self.assertIsNone(link_cls.call_args_list[0].kwargs["device"])
        self.assertIs(link_cls.call_args_list[1].kwargs["device"], sentinel.refreshed_ble_device)
        discover.assert_awaited_once_with(force_active_scan=True)
        first_session.disconnect.assert_awaited_once()
        second_session.disconnect.assert_awaited_once()

    async def test_smartess_ble_wifi_scan_uses_home_assistant_device_lookup_for_manual_address(self) -> None:
        flow = self._make_flow()
        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        resolved_device = object()
        bluetooth_module.async_ble_device_from_address = Mock(return_value=resolved_device)

        session = Mock()
        provisioner = Mock()
        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
        ) as link_cls, patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            result = await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        link_cls.assert_called_once_with("AA:BB:CC:DD:EE:FF", device=resolved_device)
        self.assertEqual(result[0].ssid, "GRooVE")

    async def test_smartess_ble_wifi_scan_prefers_home_assistant_device_lookup_over_candidate_device(self) -> None:
        flow = self._make_flow()
        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        candidate_device = object()
        resolved_device = types.SimpleNamespace(name="Collector BLE")
        bluetooth_module.async_ble_device_from_address = Mock(return_value=resolved_device)

        session = Mock()
        provisioner = Mock()
        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
        ) as link_cls, patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            await flow._async_scan_smartess_ble_wifi_networks(
                "AA:BB:CC:DD:EE:FF",
                ble_device=candidate_device,
            )

        link_cls.assert_called_once_with("AA:BB:CC:DD:EE:FF", device=resolved_device)

    async def test_smartess_ble_wifi_scan_still_uses_home_assistant_device_when_name_is_missing(
        self,
    ) -> None:
        flow = self._make_flow()
        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        candidate_device = object()
        resolved_device = types.SimpleNamespace(name=None)
        bluetooth_module.async_ble_device_from_address = Mock(return_value=resolved_device)

        session = Mock()
        provisioner = Mock()
        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
        ) as link_cls, patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            await flow._async_scan_smartess_ble_wifi_networks(
                "AA:BB:CC:DD:EE:FF",
                ble_device=candidate_device,
            )

        link_cls.assert_called_once_with("AA:BB:CC:DD:EE:FF", device=resolved_device)

    async def test_smartess_ble_wifi_scan_uses_connectable_home_assistant_lookup_only(self) -> None:
        flow = self._make_flow()
        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        candidate_device = object()
        bluetooth_module.async_ble_device_from_address = Mock(return_value=None)

        session = Mock()
        provisioner = Mock()
        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )

        with patch.dict(
            sys.modules,
            {
                "homeassistant.components": components_module,
                "homeassistant.components.bluetooth": bluetooth_module,
            },
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
        ) as link_cls, patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            await flow._async_scan_smartess_ble_wifi_networks(
                "AA:BB:CC:DD:EE:FF",
                ble_device=candidate_device,
            )

        bluetooth_module.async_ble_device_from_address.assert_called_once_with(
            flow.hass,
            "AA:BB:CC:DD:EE:FF",
            connectable=True,
        )
        link_cls.assert_called_once_with("AA:BB:CC:DD:EE:FF", device=None)

    async def test_smartess_ble_wifi_scan_falls_back_to_candidate_device_without_home_assistant_lookup(self) -> None:
        flow = self._make_flow()
        candidate_device = object()

        session = Mock()
        provisioner = Mock()
        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )

        with patch.object(
            config_flow_module,
            "BleakSmartEssBleLink",
        ) as link_cls, patch.object(
            config_flow_module.importlib,
            "import_module",
            side_effect=ImportError,
        ), patch.object(
            config_flow_module,
            "SmartEssBleSession",
            return_value=session,
        ), patch.object(
            config_flow_module,
            "SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            await flow._async_scan_smartess_ble_wifi_networks(
                "AA:BB:CC:DD:EE:FF",
                ble_device=candidate_device,
            )

        link_cls.assert_called_once_with("AA:BB:CC:DD:EE:FF", device=candidate_device)

    async def test_smartess_ble_bootstrap_times_out(self) -> None:
        flow = self._make_flow()
        session = Mock()
        provisioner = Mock()

        async def wait_forever(*args, **kwargs) -> None:
            await asyncio.Event().wait()

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.provision_wifi = AsyncMock(side_effect=wait_forever)

        with patch(
            "custom_components.eybond_local.config_flow._BLE_PROVISION_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            with self.assertRaisesRegex(SmartEssBleError, "ble_provision_failed:timeout"):
                await flow._async_run_smartess_ble_bootstrap(
                    ble_address="AA:BB:CC:DD:EE:FF",
                    ssid="Home WiFi",
                    password="Secret123",
                )

        session.disconnect.assert_awaited_once()

    async def test_smartess_ble_bootstrap_maps_notification_timeout_to_provision_failure(self) -> None:
        flow = self._make_flow()
        session = Mock()
        provisioner = Mock()

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.provision_wifi = AsyncMock(side_effect=SmartEssBleError("ble_notification_timeout"))

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            with self.assertRaisesRegex(SmartEssBleError, "ble_provision_failed:notification_timeout"):
                await flow._async_run_smartess_ble_bootstrap(
                    ble_address="AA:BB:CC:DD:EE:FF",
                    ssid="Home WiFi",
                    password="Secret123",
                )

        session.disconnect.assert_awaited_once()

    async def test_smartess_ble_wifi_scan_caches_firmware_version_from_preflight(self) -> None:
        flow = self._make_flow()
        session = Mock()
        provisioner = Mock()

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.scan_wifi_networks = AsyncMock(
            return_value=(SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),)
        )
        provisioner.last_firmware_version = "8.50.8.18"

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            result = await flow._async_scan_smartess_ble_wifi_networks("AA:BB:CC:DD:EE:FF")

        self.assertEqual(result, (SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),))
        self.assertEqual(flow._ble_fw_version_by_address["AA:BB:CC:DD:EE:FF"], "8.50.8.18")

    async def test_smartess_ble_bootstrap_reuses_cached_firmware_version_for_branch_probe(self) -> None:
        flow = self._make_flow()
        flow._ble_fw_version_by_address["AA:BB:CC:DD:EE:FF"] = "8.50.8.18"
        session = Mock()
        provisioner = Mock()
        resolved_info = SmartEssBleProvisioningInfo(
            fw_version="8.50.8.18",
            at_version="1.11",
            branch=SmartEssBleProvisionBranch.WFLKAP,
            requires_restart=False,
        )

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.query_device_info = AsyncMock(return_value=resolved_info)
        provisioner.provision_wifi = AsyncMock(
            return_value=SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.WFLKAP,
                outcome=SmartEssBleProvisionOutcome.SUCCESS,
                status_code="W000",
                raw_response="AT+LINK:W000",
                details=None,
            )
        )
        provisioner.last_firmware_version = "8.50.8.18"

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            await flow._async_run_smartess_ble_bootstrap(
                ble_address="AA:BB:CC:DD:EE:FF",
                ssid="Home WiFi",
                password="Secret123",
            )

        provisioner.query_device_info.assert_awaited_once_with(known_fw_version="8.50.8.18")
        provisioner.provision_wifi.assert_awaited_once_with(
            ssid="Home WiFi",
            password="Secret123",
            info=resolved_info,
        )
        self.assertEqual(flow._ble_fw_version_by_address["AA:BB:CC:DD:EE:FF"], "8.50.8.18")

    async def test_smartess_ble_bootstrap_reuses_selected_result_firmware_when_cache_is_empty(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="manual",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="PN123",
                    smartess_collector_version="8.50.12.3",
                ),
            )
        )
        session = Mock()
        provisioner = Mock()
        resolved_info = SmartEssBleProvisioningInfo(
            fw_version="8.50.12.3",
            at_version="1.10",
            branch=SmartEssBleProvisionBranch.WFLKAP,
            requires_restart=False,
        )

        session.connect = AsyncMock(return_value=None)
        session.disconnect = AsyncMock(return_value=None)
        provisioner.query_device_info = AsyncMock(return_value=resolved_info)
        provisioner.provision_wifi = AsyncMock(
            return_value=SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.WFLKAP,
                outcome=SmartEssBleProvisionOutcome.SUCCESS,
                status_code="W000",
                raw_response="AT+LINK:W000",
                details=None,
            )
        )
        provisioner.last_firmware_version = ""

        with patch(
            "custom_components.eybond_local.config_flow.BleakSmartEssBleLink",
            return_value=sentinel.ble_link,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleSession",
            return_value=session,
        ), patch(
            "custom_components.eybond_local.config_flow.SmartEssBleProvisioner",
            return_value=provisioner,
        ):
            await flow._async_run_smartess_ble_bootstrap(
                ble_address="AA:BB:CC:DD:EE:FF",
                ssid="Home WiFi",
                password="Secret123",
            )

        provisioner.query_device_info.assert_awaited_once_with(known_fw_version="8.50.12.3")
        provisioner.provision_wifi.assert_awaited_once_with(
            ssid="Home WiFi",
            password="Secret123",
            info=resolved_info,
        )
        self.assertNotIn("AA:BB:CC:DD:EE:FF", flow._ble_fw_version_by_address)

    async def test_bluetooth_setup_falls_back_to_manual_address_when_scan_is_empty(self) -> None:
        flow = self._make_flow()

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=()),
        ):
            result = await flow.async_step_bluetooth_setup({CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI})

        ble_selector = result["data_schema"].schema["ble_address"]
        self.assertNotIn("options", ble_selector.config.kwargs)
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        self.assertEqual(wifi_selector.config.kwargs["options"], [])
        self.assertTrue(wifi_selector.config.kwargs["custom_value"])

    async def test_bluetooth_setup_refresh_action_refreshes_candidates_without_bootstrap(self) -> None:
        flow = self._make_flow()

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(
                return_value=(
                    SmartEssBleCandidate(
                        address="11:22:33:44:55:66",
                        local_pn="A9999999999999",
                        local_name="Rescanned Collector",
                    ),
                )
            ),
        ) as discover, patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                return_value=(
                    SmartEssBleWifiNetwork(ssid="GRooVE", signal=-42),
                )
            ),
        ) as wifi_scan, patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(return_value=None),
        ) as bootstrap:
            result = await flow.async_step_bluetooth_setup(
                {
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        discover.assert_awaited_once_with(force_active_scan=True)
        wifi_scan.assert_awaited_once_with("11:22:33:44:55:66", ble_device=None)
        bootstrap.assert_not_awaited()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")
        ble_selector = result["data_schema"].schema["ble_address"]
        options = ble_selector.config.kwargs["options"]
        self.assertEqual([option["value"] for option in options], ["11:22:33:44:55:66"])
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        self.assertEqual(wifi_selector.config.kwargs["options"][0]["value"], "GRooVE")

    async def test_bluetooth_setup_rescan_action_refreshes_collectors_without_wifi_scan(self) -> None:
        flow = self._make_flow()

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(
                return_value=(
                    SmartEssBleCandidate(
                        address="11:22:33:44:55:66",
                        local_pn="A9999999999999",
                        local_name="Rescanned Collector",
                    ),
                )
            ),
        ) as discover, patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=()),
        ) as wifi_scan, patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(return_value=None),
        ) as bootstrap:
            result = await flow.async_step_bluetooth_setup(
                {
                    CONF_BLE_ACTION: BLE_ACTION_RESCAN,
                }
            )

        discover.assert_awaited_once_with(force_active_scan=True)
        wifi_scan.assert_not_awaited()
        bootstrap.assert_not_awaited()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "bluetooth_setup")
        action_selector = result["data_schema"].schema[CONF_BLE_ACTION]
        self.assertEqual(
            [option["value"] for option in action_selector.config.kwargs["options"]],
            [BLE_ACTION_RESCAN, BLE_ACTION_REFRESH_WIFI, BLE_ACTION_APPLY],
        )

    async def test_bluetooth_setup_refresh_action_keeps_selected_collector_when_still_available(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Alpha Collector",
            ),
            SmartEssBleCandidate(
                address="11:22:33:44:55:66",
                local_pn="E50000253884199777",
                local_name="Bravo Collector",
            ),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ) as discover, patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                side_effect=(
                    (SmartEssBleWifiNetwork(ssid="Alpha WiFi", signal=92),),
                    (SmartEssBleWifiNetwork(ssid="Bravo WiFi", signal=88),),
                    (SmartEssBleWifiNetwork(ssid="Bravo WiFi Refreshed", signal=86),),
                )
            ),
        ) as wifi_scan:
            await flow.async_step_bluetooth_setup()
            await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "11:22:33:44:55:66",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "11:22:33:44:55:66",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        self.assertEqual(discover.await_count, 3)
        self.assertEqual(
            [call.args[0] for call in wifi_scan.await_args_list],
            ["AA:BB:CC:DD:EE:FF", "11:22:33:44:55:66", "11:22:33:44:55:66"],
        )
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        self.assertEqual(
            wifi_selector.config.kwargs["options"][0]["value"],
            "Bravo WiFi Refreshed",
        )

    async def test_bluetooth_setup_keeps_cached_wifi_networks_when_refresh_scan_fails(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000253884199645",
                local_name="Collector PN",
            ),
        )
        cached_networks = (
            SmartEssBleWifiNetwork(ssid="GRooVE", signal=92),
            SmartEssBleWifiNetwork(ssid="Office", signal=58),
        )

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(return_value=candidates),
        ) as discover, patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                side_effect=(
                    cached_networks,
                    SmartEssBleError("ble_wifi_scan_failed:timeout"),
                )
            ),
        ) as wifi_scan:
            first_result = await flow.async_step_bluetooth_setup()
            refreshed_result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "AA:BB:CC:DD:EE:FF",
                    CONF_BLE_ACTION: BLE_ACTION_REFRESH_WIFI,
                }
            )

        self.assertEqual(discover.await_count, 2)
        self.assertEqual(wifi_scan.await_count, 2)
        self.assertEqual(first_result["errors"], {})
        self.assertEqual(refreshed_result["errors"], {})
        refreshed_wifi_selector = refreshed_result["data_schema"].schema["wifi_ssid"]
        refreshed_options = refreshed_wifi_selector.config.kwargs["options"]
        self.assertEqual([option["value"] for option in refreshed_options], ["GRooVE", "Office"])
        self.assertEqual(refreshed_result["description_placeholders"]["ble_last_error"], "ble_wifi_scan_failed:timeout")

    async def test_bluetooth_setup_keeps_detailed_provision_failure_code(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(
                return_value=(
                    SmartEssBleCandidate(
                        address="AA:BB:CC:DD:EE:FF",
                        local_pn="E50000253884199645",
                        local_name="Collector PN",
                    ),
                )
            ),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(return_value=()),
        ), patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(side_effect=SmartEssBleError("ble_provision_failed:wflkap:W008")),
        ):
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "AA:BB:CC:DD:EE:FF",
                    "wifi_ssid": "GRooVE",
                    "wifi_password": "55555555",
                    CONF_BLE_ACTION: BLE_ACTION_APPLY,
                }
            )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "ble_provision_failed"})
        self.assertEqual(flow._ble_last_error, "ble_provision_failed:wflkap:W008")

    async def test_bluetooth_setup_runs_bootstrap_then_returns_to_scan_interface(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(
                return_value=(
                    SmartEssBleCandidate(
                        address="AA:BB:CC:DD:EE:FF",
                        local_pn="A0000000000001",
                        local_name="Alpha Collector",
                    ),
                )
            ),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                return_value=(
                    SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-42),
                )
            ),
        ), patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(return_value=None),
        ) as bootstrap:
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "AA:BB:CC:DD:EE:FF",
                    "wifi_ssid": "Manual WiFi",
                    "wifi_password": "Secret123",
                    CONF_BLE_ACTION: BLE_ACTION_APPLY,
                }
            )

        bootstrap.assert_awaited_once_with(
            ble_address="AA:BB:CC:DD:EE:FF",
            ssid="Manual WiFi",
            password="Secret123",
            ble_device=None,
        )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "auto")

    async def test_bluetooth_setup_accepts_hidden_wifi_name_with_single_custom_selector(
        self,
    ) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        with patch(
            "custom_components.eybond_local.config_flow.async_probe_ble_host_capability",
            new=AsyncMock(return_value=types.SimpleNamespace(available=True)),
        ), patch.object(
            flow,
            "_async_discover_smartess_ble_candidates",
            new=AsyncMock(
                return_value=(
                    SmartEssBleCandidate(
                        address="AA:BB:CC:DD:EE:FF",
                        local_pn="A0000000000001",
                        local_name="Alpha Collector",
                    ),
                )
            ),
        ), patch.object(
            flow,
            "_async_scan_smartess_ble_wifi_networks",
            new=AsyncMock(
                return_value=(
                    SmartEssBleWifiNetwork(ssid="GRooVE", signal=-42),
                    SmartEssBleWifiNetwork(ssid="Office", signal=-58),
                )
            ),
        ), patch.object(
            flow,
            "_async_run_smartess_ble_bootstrap",
            new=AsyncMock(return_value=None),
        ) as bootstrap:
            result = await flow.async_step_bluetooth_setup(
                {
                    "ble_address": "AA:BB:CC:DD:EE:FF",
                    "wifi_ssid": "Hidden WiFi",
                    "wifi_password": "Secret123",
                    CONF_BLE_ACTION: BLE_ACTION_APPLY,
                }
            )

        bootstrap.assert_awaited_once_with(
            ble_address="AA:BB:CC:DD:EE:FF",
            ssid="Hidden WiFi",
            password="Secret123",
            ble_device=None,
        )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "auto")

    async def test_deep_scan_placeholders_do_not_expose_duration_estimates(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}
        flow._interface_options = [
            {
                "name": "wlan0",
                "ip": "192.168.1.50",
                "label": "wlan0 - 192.168.1.50",
                "network": "192.168.1.0/24",
                "broadcast": "192.168.1.255",
            },
        ]

        result = await flow.async_step_deep_scan()

        self.assertEqual(result["description_placeholders"]["deep_scan_target_count"], "253")
        self.assertNotIn("deep_scan_duration", result["description_placeholders"])
        self.assertIn("unicast-запитами", result["description_placeholders"]["deep_scan_warning"])

    async def test_change_scan_interface_preserves_connection_type(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}

        async def _fake_scanning(user_input=None):
            return {"type": "progress", "step_id": "scanning"}

        flow.async_step_scanning = _fake_scanning

        result = await flow.async_step_change_scan_interface({"server_ip": "192.168.2.50"})

        self.assertEqual(result["type"], "progress")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")
        self.assertEqual(flow._auto_config["server_ip"], "192.168.2.50")

    async def test_scan_results_without_results_still_offers_manual(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {}
        flow._scan_error = True

        result = await flow.async_step_scan_results()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "scan_results")
        self.assertEqual(result["menu_options"][:2], ["refresh_scan", "deep_scan"])
        self.assertIn("deep_scan", result["menu_options"])
        self.assertIn("refresh_scan", result["menu_options"])
        self.assertIn("manual", result["menu_options"])
        self.assertNotIn("choose", result["menu_options"])

    async def test_scan_results_with_multiple_interfaces_offers_change_interface(self) -> None:
        flow = self._make_flow()
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "192.168.2.50", "label": "wlan0 - 192.168.2.50"},
        ]
        flow._autodetect_results = {}

        result = await flow.async_step_scan_results()

        self.assertIn("change_scan_interface", result["menu_options"])

    async def test_scan_results_after_deep_scan_hides_deep_scan_action(self) -> None:
        flow = self._make_flow()
        flow._scan_mode = SETUP_MODE_DEEP_SCAN

        result = await flow.async_step_scan_results()

        self.assertIn("deep_scan", result["menu_options"])

    async def test_scan_results_with_available_results_shows_menu(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(target_ip="192.168.1.14", source="udp", ip="192.168.1.14", connected=True),
                match=DriverMatch(
                    driver_key="pi30",
                    protocol_family="pi30",
                    model_name="PowMr 4.2kW",
                    serial_number="553555355535552",
                    probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                ),
                connection_mode="known_ip",
            )
        }

        result = await flow.async_step_scan_results()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "scan_results")
        self.assertEqual(result["menu_options"][:2], ["choose", "refresh_scan"])
        self.assertIn("scan_summary", result["description_placeholders"])
        self.assertIn("choose", result["menu_options"])
        self.assertIn("deep_scan", result["menu_options"])

    async def test_scan_results_udp_only_candidate_still_shows_choose(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.14",
                    source="subnet_unicast",
                    ip="192.168.1.14",
                    udp_reply="rsp>server=1;",
                    connected=False,
                ),
                connection_mode="subnet_unicast",
                next_action="manual_input",
                last_error="collector_not_connected",
            )
        }

        result = await flow.async_step_scan_results()

        self.assertEqual(result["type"], "menu")
        self.assertIn("choose", result["menu_options"])

    def test_scan_discovery_targets_use_selected_broadcast_only(self) -> None:
        flow = self._make_flow()

        targets = flow._scan_discovery_targets()

        self.assertEqual(
            targets,
            (DiscoveryTarget(ip="192.168.255.255", source="broadcast"),),
        )

    async def test_choose_step_shows_selector_form(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(target_ip="192.168.1.14", source="udp", ip="192.168.1.14", connected=True),
                match=DriverMatch(
                    driver_key="pi30",
                    protocol_family="pi30",
                    model_name="PowMr 4.2kW",
                    serial_number="553555355535552",
                    probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                ),
                connection_mode="known_ip",
            ),
            "1": OnboardingResult(
                collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
                match=DriverMatch(
                    driver_key="modbus_smg",
                    protocol_family="modbus_smg",
                    model_name="SMG 6200",
                    serial_number="92632511100118",
                    probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                ),
                connection_mode="known_ip",
            ),
        }

        result = await flow.async_step_choose()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "choose")

    async def test_confirm_step_exposes_poll_interval_field(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertIn("poll_interval", result["data_schema"].schema)
        self.assertIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)

    async def test_confirm_step_placeholders_render_split_collector_and_inverter_tables(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="PN123"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                details={
                    "rated_power": 6200,
                    "collector_signal_strength": -67,
                    "battery_connected": True,
                    "battery_percent": 78,
                },
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        placeholders = result["description_placeholders"]
        self.assertIn("**Collector**", placeholders["collector_confirm_table"])
        self.assertIn("| Collector PN | PN123 |", placeholders["collector_confirm_table"])
        self.assertIn("| Collector IP | 192.168.1.55 |", placeholders["collector_confirm_table"])
        self.assertNotIn("Collector Signal Strength", placeholders["collector_confirm_table"])
        self.assertIn("**Inverter**", placeholders["inverter_confirm_table"])
        self.assertIn("| Model | SMG 6200 |", placeholders["inverter_confirm_table"])
        self.assertIn("| Rated Power | 6200 W |", placeholders["inverter_confirm_table"])
        self.assertIn(
            "| Serial Number | 92632511100118 |",
            placeholders["inverter_confirm_table"],
        )
        self.assertIn(
            "| Detection Confidence | High confidence |",
            placeholders["inverter_confirm_table"],
        )
        self.assertIn(
            "| Protocol Family | modbus_smg |",
            placeholders["inverter_confirm_table"],
        )
        self.assertNotIn("Battery Connection", placeholders["inverter_confirm_table"])
        self.assertNotIn("Battery Percent", placeholders["inverter_confirm_table"])

    async def test_confirm_step_placeholders_keep_rated_power_missing_visible(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="PN123"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        placeholders = result["description_placeholders"]
        self.assertNotIn("Collector Signal Strength", placeholders["collector_confirm_table"])
        self.assertIn(
            "| Rated Power | Not available yet |",
            placeholders["inverter_confirm_table"],
        )
        self.assertNotIn("Battery Connection", placeholders["inverter_confirm_table"])
        self.assertNotIn("Battery Percent", placeholders["inverter_confirm_table"])

    async def test_confirm_step_uses_collector_pn_from_enriched_match_details(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                details={
                    "collector_pn": "PN999",
                },
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        placeholders = result["description_placeholders"]
        self.assertIn("| Collector PN | PN999 |", placeholders["collector_confirm_table"])

    async def test_confirm_step_does_not_refresh_runtime_details_for_autodetected_result(self) -> None:
        flow = self._make_flow()
        selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="PN123"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="broadcast",
        )
        flow._autodetect_results = {"0": selected_result}
        flow._selected_result = selected_result
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=object(),
        ) as create_manager:
            result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        placeholders = result["description_placeholders"]
        self.assertNotIn("Collector Signal Strength", placeholders["collector_confirm_table"])
        self.assertNotIn("Battery Connection", placeholders["inverter_confirm_table"])
        self.assertNotIn("Battery Percent", placeholders["inverter_confirm_table"])
        self.assertIn("| Rated Power | Not available yet |", placeholders["inverter_confirm_table"])
        create_manager.assert_not_called()

    async def test_confirm_step_skips_smartess_cloud_assist_for_low_confidence_result(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000025388419"),
            ),
            match=DriverMatch(
                driver_key="pi30",
                protocol_family="pi30",
                model_name="PowMr 4.2kW",
                serial_number="553555355535552",
                probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                confidence="medium",
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")

    async def test_confirm_step_skips_smartess_cloud_assist_for_collector_only_result(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000025388419"),
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")

    async def test_choose_step_selects_specific_result(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(target_ip="192.168.1.14", source="udp", ip="192.168.1.14", connected=True),
                match=DriverMatch(
                    driver_key="pi30",
                    protocol_family="pi30",
                    model_name="PowMr 4.2kW",
                    serial_number="553555355535552",
                    probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                ),
                connection_mode="known_ip",
            ),
            "1": OnboardingResult(
                collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
                match=DriverMatch(
                    driver_key="modbus_smg",
                    protocol_family="modbus_smg",
                    model_name="SMG 6200",
                    serial_number="92632511100118",
                    probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                ),
                connection_mode="known_ip",
            ),
        }

        result = await flow.async_step_choose({CONF_RESULT_KEY: "1"})

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertIsNotNone(flow._selected_result)
        self.assertEqual(flow._selected_result.match.model_name, "SMG 6200")

    async def test_choose_step_udp_only_candidate_can_create_pending_entry(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {
            "server_ip": "192.168.1.104",
            "collector_ip": "",
            "driver_hint": "auto",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.14",
                    source="subnet_unicast",
                    ip="192.168.1.14",
                    udp_reply="rsp>server=1;",
                    connected=False,
                ),
                connection_mode="subnet_unicast",
                next_action="manual_input",
                last_error="collector_not_connected",
            )
        }

        await flow.async_step_choose({CONF_RESULT_KEY: "0"})
        result = await flow._async_create_entry_from_result({"poll_interval": 30})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["title"], "Collector 192.168.1.14")
        self.assertEqual(result["data"]["collector_ip"], "192.168.1.14")
        self.assertEqual(result["data"]["connection_mode"], "known_ip")

    async def test_collector_operation_smartess_and_ha_routes_to_confirm(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_collector_operation(
            {CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_SMARTESS_AND_HA}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertEqual(flow._collector_operation_mode, COLLECTOR_OPERATION_SMARTESS_AND_HA)

    async def test_collector_operation_ha_only_binds_silently_then_routes_to_confirm(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )

        with patch.object(
            flow,
            "_async_bind_selected_collector_to_home_assistant",
            new=AsyncMock(),
        ) as bind:
            result = await flow.async_step_collector_operation(
                {CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_HA_ONLY}
            )

        bind.assert_awaited_once()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertEqual(flow._collector_operation_mode, COLLECTOR_OPERATION_HA_ONLY)
        self.assertTrue(flow._collector_endpoint_bind_applied)

    async def test_collector_endpoint_confirmation_requires_acknowledgement(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )
        flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"
        flow._collector_original_server_endpoint = "collector-cloud.smartess.example,18899,TCP"

        result = await flow.async_step_collector_endpoint_confirm(
            {CONF_CONFIRM_COLLECTOR_ENDPOINT_RISK: False}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "collector_endpoint_confirm")
        self.assertEqual(
            result["errors"],
            {CONF_CONFIRM_COLLECTOR_ENDPOINT_RISK: "collector_endpoint_risk_not_confirmed"},
        )

    async def test_collector_endpoint_confirmation_binds_then_routes_to_confirm(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )
        flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"
        flow._collector_original_server_endpoint = "collector-cloud.smartess.example,18899,TCP"

        with patch.object(flow, "_async_bind_selected_collector_to_home_assistant", new=AsyncMock()) as bind:
            result = await flow.async_step_collector_endpoint_confirm(
                {CONF_CONFIRM_COLLECTOR_ENDPOINT_RISK: True}
            )

        bind.assert_awaited_once()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertTrue(flow._collector_endpoint_bind_applied)

    async def test_confirm_step_persists_poll_interval_in_entry_options(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="PN123",
                    smartess_collector_version="1.2.3",
                    smartess_protocol_asset_id="0925",
                    smartess_protocol_profile_key="smartess_0925",
                    smartess_device_address=5,
                ),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                details={
                    "smartess_collector_version": "1.2.3",
                    "smartess_protocol_asset_id": "0925",
                    "smartess_profile_key": "smartess_0925",
                    "smartess_device_address": 5,
                },
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm(
            {
                CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_SMARTESS_AND_HA,
                "poll_interval": 15,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["options"]["poll_interval"], 15)
        self.assertEqual(result["data"][CONF_SMARTESS_COLLECTOR_VERSION], "1.2.3")
        self.assertEqual(result["data"][CONF_SMARTESS_PROTOCOL_ASSET_ID], "0925")
        self.assertEqual(result["data"][CONF_SMARTESS_PROFILE_KEY], "smartess_0925")
        self.assertEqual(result["data"][CONF_SMARTESS_DEVICE_ADDRESS], 5)

    async def test_confirm_step_remembers_original_endpoint_after_ha_only_binding(self) -> None:
        flow = self._make_flow()
        flow._collector_operation_mode = COLLECTOR_OPERATION_HA_ONLY
        flow._collector_endpoint_bind_applied = True
        flow._collector_original_server_endpoint = "collector-cloud.smartess.example,18899,TCP"
        flow._collector_target_server_endpoint = "192.168.1.50,18899,TCP"
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="PN123"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm(
            {
                CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_HA_ONLY,
                "poll_interval": 15,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["options"][CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT],
            "collector-cloud.smartess.example,18899,TCP",
        )

    async def test_collector_callback_target_uses_legacy_cloud_port(self) -> None:
        flow = self._make_flow()
        flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"

        self.assertEqual(
            flow._collector_callback_target_endpoint(),
            "192.168.1.50,18899,TCP",
        )

    async def test_collector_callback_target_preserves_host_only_shape(self) -> None:
        flow = self._make_flow()
        flow._collector_current_server_endpoint = "ess.eybond.com"

        self.assertEqual(
            flow._collector_callback_target_endpoint(),
            "192.168.1.50",
        )

    async def test_endpoint_originality_hint_uses_catalog_host_match(self) -> None:
        flow = self._make_flow()

        hint = flow._endpoint_originality_hint("dtu_ess.eybond.com")

        self.assertIn("original cloud endpoint", hint)

    async def test_endpoint_originality_hint_uses_catalog_port_match(self) -> None:
        flow = self._make_flow()

        hint = flow._endpoint_originality_hint("collector.example,502,TCP")

        self.assertIn("original cloud endpoint", hint)

    async def test_endpoint_originality_hint_reports_custom_for_unknown_endpoint(self) -> None:
        flow = self._make_flow()

        hint = flow._endpoint_originality_hint("collector.example,65535,TCP")

        self.assertIn("does not look like the stock cloud address", hint)

    async def test_do_scan_keeps_matching_entries_loaded(self) -> None:
        matching = _FakeEntry("match", server_ip="192.168.1.50", tcp_port=8899)
        other = _FakeEntry("other", server_ip="192.168.1.60", tcp_port=8899)
        flow = self._make_flow(entries=[matching, other])

        class _FakeDetector:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

            async def async_auto_detect(self, **kwargs):
                return (OnboardingResult(),)

        with patch("custom_components.eybond_local.config_flow.create_onboarding_manager", return_value=_FakeDetector()):
            await flow._async_do_scan()

        self.assertEqual(flow.hass.config_entries.unloaded, [])
        self.assertEqual(flow.hass.config_entries.reloaded, [])

    async def test_do_scan_builds_connection_spec_through_generic_builder(self) -> None:
        flow = self._make_flow()

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                return ()

        with patch(
            "custom_components.eybond_local.config_flow.build_connection_spec_from_values",
            return_value=sentinel.connection_spec,
        ) as build_spec, patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ) as create_manager:
            await flow._async_do_scan()

        build_spec.assert_called_once()
        create_manager.assert_called_once_with(
            sentinel.connection_spec,
            driver_hint="auto",
        )

    async def test_do_scan_uses_single_attempt_for_quick_scan(self) -> None:
        flow = self._make_flow()
        captured_kwargs: dict[str, object] = {}

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                captured_kwargs.update(kwargs)
                return ()

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_do_scan()

        self.assertEqual(captured_kwargs["attempts"], 1)
        self.assertFalse(captured_kwargs["enrich_runtime_details"])

    async def test_do_scan_keeps_runtime_enrichment_for_deep_scan(self) -> None:
        flow = self._make_flow()
        flow._set_scan_mode(SETUP_MODE_DEEP_SCAN)
        captured_kwargs: dict[str, object] = {}

        class _FakeDetector:
            async def async_deep_detect(self, **kwargs):
                captured_kwargs.update(kwargs)
                return ()

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_do_scan()

        self.assertTrue(captured_kwargs["enrich_runtime_details"])

    async def test_do_scan_preserves_new_collector_only_result_alongside_existing_matched_entry(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=8899)
        existing.data.update(
            {
                "collector_ip": "192.168.1.55",
                "collector_pn": "E5000025388419",
                "detected_serial": "92632511100118",
            }
        )
        existing.unique_id = "collector:E5000025388419"
        flow = self._make_flow(entries=[existing])

        matched_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000025388419"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=1),
            ),
            connection_mode="broadcast",
        )
        collector_only_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.193",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000099990002"),
            ),
            connection_mode="broadcast",
            next_action="manual_driver_selection",
            last_error="no_supported_driver_matched",
        )

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                return (matched_result, collector_only_result)

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_do_scan()

        self.assertEqual(
            {result.collector.ip for result in flow._autodetect_results.values() if result.collector is not None},
            {"192.168.1.55", "192.168.1.193"},
        )
        self.assertEqual(
            {result.collector.ip for result in flow._available_autodetect_results().values() if result.collector is not None},
            {"192.168.1.193"},
        )

    async def test_do_scan_publishes_determinate_progress_updates(self) -> None:
        flow = self._make_flow()
        seen_progress: list[float] = []
        flow.async_update_progress = seen_progress.append

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                await asyncio.sleep(0.4)
                return (
                    OnboardingResult(
                        collector=CollectorCandidate(
                            target_ip="192.168.1.55",
                            source="udp",
                            ip="192.168.1.55",
                            connected=True,
                        ),
                        connection_mode="known_ip",
                    ),
                )

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_do_scan()

        self.assertTrue(seen_progress)
        self.assertEqual(seen_progress[-1], 1.0)
        self.assertGreaterEqual(max(seen_progress), 0.99)

    def test_scan_progress_fraction_starts_near_zero_for_discovery(self) -> None:
        flow = self._make_flow()
        flow._scan_progress_stage = "preparing"
        self.assertEqual(flow._scan_progress_fraction(0.0), 0.0)

        flow._scan_progress_stage = "discovering"
        self.assertLessEqual(flow._scan_progress_fraction(0.0), 0.02)

    async def test_do_scan_timeout_returns_without_hanging(self) -> None:
        flow = self._make_flow()

        class _SlowDetector:
            async def async_auto_detect(self, **kwargs):
                await asyncio.sleep(0.05)
                return ()

        with patch(
            "custom_components.eybond_local.config_flow._AUTO_SCAN_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_SlowDetector(),
        ):
            await flow._async_do_scan()

        self.assertEqual(flow._autodetect_results, {})

    async def test_probe_manual_target_builds_connection_spec_through_generic_builder(self) -> None:
        flow = self._make_flow()
        user_input = {
            "server_ip": "192.168.1.50",
            "tcp_port": 8899,
            "udp_port": 58899,
            "collector_ip": "192.168.1.55",
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                return ()

        with patch(
            "custom_components.eybond_local.config_flow.build_connection_spec_from_values",
            return_value=sentinel.connection_spec,
        ) as build_spec, patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ) as create_manager:
            result = await flow._async_probe_manual_target(user_input)

        self.assertEqual(result.next_action, "create_pending_entry")
        build_spec.assert_called_once()
        create_manager.assert_called_once_with(
            sentinel.connection_spec,
            driver_hint="auto",
        )

    async def test_probe_manual_target_skips_broadcast_when_collector_ip_is_set(self) -> None:
        flow = self._make_flow()
        user_input = {
            "server_ip": "192.168.1.50",
            "tcp_port": 8899,
            "udp_port": 58899,
            "collector_ip": "192.168.1.14",
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }
        captured_kwargs: dict[str, object] = {}

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                captured_kwargs.update(kwargs)
                return ()

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_probe_manual_target(user_input)

        self.assertEqual(captured_kwargs["collector_ip"], "192.168.1.14")
        self.assertEqual(captured_kwargs["discovery_target"], "")

    async def test_probe_manual_target_timeout_returns_pending_result(self) -> None:
        flow = self._make_flow()
        user_input = {
            "server_ip": "192.168.1.50",
            "tcp_port": 8899,
            "udp_port": 58899,
            "collector_ip": "192.168.1.55",
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }

        class _SlowDetector:
            async def async_auto_detect(self, **kwargs):
                await asyncio.sleep(0.05)
                return ()

        with patch(
            "custom_components.eybond_local.config_flow._MANUAL_PROBE_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_SlowDetector(),
        ):
            result = await flow._async_probe_manual_target(user_input)

        self.assertEqual(result.connection_mode, "manual")
        self.assertEqual(result.next_action, "create_pending_entry")
        self.assertEqual(result.last_error, "manual_probe_timeout")

    async def test_manual_confirm_step_exposes_retry_edit_and_create_actions(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "tcp_port": 8899,
        }
        flow._manual_result = OnboardingResult(connection_mode="manual")

        result = await flow.async_step_manual_confirm()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "manual_confirm")
        self.assertEqual(
            result["menu_options"],
            ["manual_probe_again", "manual_edit_settings", "manual_create_pending"],
        )

    async def test_manual_confirm_skips_smartess_cloud_assist_for_collector_only_result(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "tcp_port": 8899,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="manual",
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="manual",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000025388419"),
            ),
        )

        result = await flow.async_step_manual_confirm()

        self.assertNotIn("manual_smartess_cloud_assist", result["menu_options"])

    async def test_manual_confirm_skips_smartess_cloud_assist_for_low_confidence_inverter_match(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "tcp_port": 8899,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="manual",
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="manual",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000025388419"),
            ),
            match=DriverMatch(
                driver_key="pi30",
                protocol_family="pi30",
                model_name="PowMr 4.2kW",
                serial_number="553555355535552",
                probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                confidence="medium",
            ),
        )

        result = await flow.async_step_manual_confirm()

        self.assertNotIn("manual_smartess_cloud_assist", result["menu_options"])

    async def test_manual_confirm_surfaces_smartess_hint_when_local_driver_is_unconfirmed(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "tcp_port": 8899,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="manual",
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="manual",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="PN123",
                    smartess_collector_version="8.50.12.3",
                    smartess_protocol_asset_id="0000",
                ),
            ),
        )

        result = await flow.async_step_manual_confirm()
        placeholders = result["description_placeholders"]

        self.assertIn("SmartESS metadata", placeholders["probe_summary"])
        self.assertIn("cloud identity", placeholders["control_summary"])

    async def test_manual_edit_settings_returns_to_manual_form_with_previous_values(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "driver_hint": "auto",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(connection_mode="manual")

        result = await flow.async_step_manual_edit_settings()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "manual")
        self.assertEqual(flow._manual_defaults["collector_ip"], "192.168.1.55")
        self.assertIsNone(flow._manual_result)

    async def test_manual_step_localizes_driver_selector_labels(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"

        result = await flow.async_step_manual()

        selector = result["data_schema"].schema["driver_hint"]
        labels = [option["label"] for option in selector.config.kwargs["options"]]
        self.assertEqual(labels, ["Авто", "SMG / Modbus", "PI30"])

    async def test_manual_step_recovers_when_auto_config_is_missing(self) -> None:
        flow = self._make_flow()
        flow._auto_config = None

        result = await flow.async_step_manual()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "manual")
        self.assertEqual(flow._auto_config["server_ip"], "192.168.1.50")

    async def test_manual_step_heals_stale_submitted_server_ip_before_probe(self) -> None:
        flow = self._make_flow()
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}
        captured_input: dict[str, object] = {}

        async def _fake_probe(user_input):
            captured_input.update(user_input)
            return OnboardingResult(connection_mode="manual")

        with patch.object(flow, "_async_probe_manual_target", side_effect=_fake_probe):
            result = await flow.async_step_manual(
                {
                    "server_ip": "192.168.1.104",
                    "tcp_port": 8899,
                    "udp_port": 58899,
                    "collector_ip": "192.168.1.14",
                    "discovery_target": "192.168.1.255",
                    "discovery_interval": 3,
                    "heartbeat_interval": 60,
                    "driver_hint": "auto",
                }
            )

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "manual_confirm")
        self.assertEqual(captured_input["server_ip"], "192.168.1.50")
        self.assertEqual(flow._manual_config["server_ip"], "192.168.1.50")

    async def test_manual_probe_again_retries_with_stored_settings(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "driver_hint": "auto",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }

        with patch.object(
            flow,
            "_async_probe_manual_target",
            return_value=OnboardingResult(connection_mode="manual", next_action="create_pending_entry"),
        ) as probe_manual_target:
            result = await flow.async_step_manual_probe_again()

        probe_manual_target.assert_awaited_once_with(flow._manual_config)
        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "manual_confirm")

    async def test_manual_create_pending_uses_stored_manual_config(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "driver_hint": "auto",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(connection_mode="manual", next_action="create_pending_entry")

        result = await flow.async_step_manual_create_pending()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["title"], "EyeBond Setup Pending")
        self.assertEqual(result["data"]["collector_ip"], "192.168.1.55")

    async def test_manual_create_pending_drops_default_broadcast_collector_ip(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.255",
            "driver_hint": "auto",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(connection_mode="broadcast", next_action="create_pending_entry")

        result = await flow.async_step_manual_create_pending()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["collector_ip"], "")
        self.assertEqual(flow._test_unique_id, "listener:192.168.1.50:8899")

    async def test_manual_high_confidence_entry_defaults_to_auto_control_mode(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "driver_hint": "auto",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="manual",
                ip="192.168.1.55",
                connected=True,
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                confidence="high",
            ),
            connection_mode="manual",
        )

        result = await flow.async_step_manual_create_pending()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["control_mode"], "auto")
        self.assertEqual(result["data"]["detection_confidence"], "high")

    async def test_smartess_cloud_assist_persists_inferred_metadata_on_pending_entry(self) -> None:
        flow = self._make_flow()
        with tempfile.TemporaryDirectory() as tempdir:
            flow.hass.config.config_dir = tempdir
            flow._selected_result = OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.55",
                    source="udp",
                    ip="192.168.1.55",
                    connected=True,
                    collector=CollectorInfo(
                        collector_pn="E5000025388419",
                        smartess_protocol_asset_id="0000",
                    ),
                ),
                connection_mode="known_ip",
            )

            evidence = build_cloud_evidence_payload(
                source="smartess_cloud_onboarding",
                payload={
                    "normalized": {
                        "device_list": {
                            "device_count": 1,
                            "devices": [
                                {
                                    "pn": "E50000253884199645",
                                    "sn": "E50000253884199645094801",
                                    "devcode": 2376,
                                    "devaddr": 5,
                                    "devName": "SD-HYM-4862HWP",
                                    "devalias": "Garage inverter",
                                    "status": "online",
                                    "brand": "SmartESS",
                                }
                            ],
                        },
                        "device_detail": {
                            "section_counts": {
                                "bc_": 1,
                                "bt_": 1,
                                "gd_": 1,
                                "pv_": 1,
                                "sy_": 1,
                            }
                        },
                        "device_settings": {
                            "field_count": 39,
                            "mapped_field_count": 28,
                            "fields_with_current_value": 2,
                            "fields": [
                                {
                                    "title": "Output priority",
                                    "bucket": "exact_0925",
                                    "has_current_value": True,
                                    "current_value": 2,
                                    "choices": [
                                        {"value": 0, "raw_value": "0", "label": "UTI"},
                                        {"value": 1, "raw_value": "1", "label": "SOL"},
                                        {"value": 2, "raw_value": "2", "label": "SBU"},
                                    ],
                                    "binding": {"register": 4537},
                                },
                                {
                                    "title": "Battery Type",
                                    "bucket": "exact_0925",
                                    "has_current_value": True,
                                    "current_value": 6,
                                    "choices": [
                                        {"value": 2, "raw_value": "2", "label": "USER"},
                                        {"value": 6, "raw_value": "6", "label": "Li4"},
                                    ],
                                    "binding": {"register": 4539},
                                },
                                {
                                    "title": "Boot method",
                                    "bucket": "cloud_only",
                                    "has_current_value": False,
                                },
                            ],
                        }
                    }
                },
                collector_pn="E5000025388419",
                pn="E50000253884199645",
                sn="E50000253884199645094801",
                devcode=2376,
                devaddr=5,
                summary={
                    "detail_sections": ["bc_", "bt_", "gd_", "pv_", "sy_"],
                    "settings_field_count": 39,
                    "settings_mapped_field_count": 28,
                    "settings_exact_0925_field_count": 28,
                    "settings_probable_0925_field_count": 5,
                    "settings_cloud_only_field_count": 6,
                    "settings_current_values_included": True,
                },
            )

            await flow.async_step_smartess_cloud_assist_choice()
            with patch(
                "custom_components.eybond_local.config_flow.fetch_and_export_smartess_device_bundle_cloud_evidence",
                return_value=CloudEvidenceRecord(
                    path=Path("/config/eybond_local/cloud_evidence/onboarding.json"),
                    payload=evidence,
                ),
            ):
                assist_result = await flow.async_step_smartess_cloud_assist(
                    {"username": "groove", "password": "secret"}
                )

            self.assertEqual(assist_result["type"], "menu")
            self.assertEqual(assist_result["step_id"], "smartess_cloud_assist_summary")
            self.assertEqual(assist_result["menu_options"], ["confirm"])

            placeholders = assist_result["description_placeholders"]
            self.assertIn("SmartESS 0925", placeholders["smartess_cloud_mapping_table"])
            self.assertIn("E50000253884199645", placeholders["smartess_cloud_identity_table"])
            self.assertIn("Garage inverter", placeholders["smartess_cloud_identity_table"])
            self.assertIn("bc_ (1)", placeholders["smartess_cloud_detail_summary"])
            self.assertIn("39", placeholders["smartess_cloud_settings_table"])
            self.assertIn("Output priority", placeholders["smartess_cloud_highlights_table"])
            self.assertIn("SBU", placeholders["smartess_cloud_highlights_table"])
            self.assertIn("reg 4537", placeholders["smartess_cloud_highlights_table"])

            created = await flow.async_step_confirm({"poll_interval": 15})

            self.assertEqual(created["type"], "create_entry")
            self.assertEqual(created["data"][CONF_SMARTESS_PROTOCOL_ASSET_ID], "0925")
            self.assertEqual(created["data"][CONF_SMARTESS_PROFILE_KEY], "smartess_0925")
            self.assertEqual(created["data"][CONF_DRIVER_HINT], "pi30")

    async def test_scan_results_placeholders_use_localized_choose_label(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "ru"
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(target_ip="192.168.1.14", source="udp", ip="192.168.1.14", connected=True),
                match=DriverMatch(
                    driver_key="pi30",
                    protocol_family="pi30",
                    model_name="PowMr 4.2kW",
                    serial_number="553555355535552",
                    probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                ),
                connection_mode="known_ip",
            )
        }

        await flow._async_ensure_translation_bundle()

        placeholders = flow._scan_results_placeholders()

        self.assertIn("Добавить обнаруженное устройство", placeholders["scan_next_hint"])
        self.assertNotIn("Add detected device", placeholders["scan_next_hint"])

    async def test_scan_results_placeholders_use_localized_retry_actions(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.14",
                    source="udp",
                    ip="192.168.1.14",
                    udp_reply="rsp>server=1;",
                ),
                connection_mode="known_ip",
            )
        }

        await flow._async_ensure_translation_bundle()

        placeholders = flow._scan_results_placeholders()

        self.assertIn("Повторити сканування", placeholders["scan_next_hint"])
        self.assertIn("Ручне налаштування", placeholders["scan_next_hint"])
        self.assertNotIn("Запустити глибоке сканування", placeholders["scan_next_hint"])
        self.assertNotIn("Refresh scan", placeholders["scan_next_hint"])
        self.assertNotIn("Manual setup", placeholders["scan_next_hint"])

    async def test_scan_results_placeholders_surface_localized_smartess_pending_state(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "ru"
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.57",
                    source="udp",
                    ip="192.168.1.57",
                    connected=True,
                    collector=CollectorInfo(
                        collector_pn="PN789",
                        smartess_collector_version="8.50.12.3",
                        smartess_protocol_asset_id="0000",
                    ),
                ),
                connection_mode="known_ip",
            )
        }

        await flow._async_ensure_translation_bundle()

        placeholders = flow._scan_results_placeholders()
        result_label = flow._result_label(flow._autodetect_results["0"])

        self.assertIn("локальное сопоставление инвертора пока не подтверждено", placeholders["scan_summary"])
        self.assertIn("сохранить ожидающее устройство", placeholders["scan_next_hint"])
        self.assertIn("Есть признаки SmartESS", result_label)

    async def test_options_runtime_step_renders_branch_aware_connection_section(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "runtime")

    async def test_options_init_menu_exposes_collector_wifi(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_init()

        self.assertEqual(result["menu_options"], ["runtime", "collector_wifi", "diagnostics"])

    async def test_options_collector_wifi_step_renders_current_status(self) -> None:
        options = self._make_options_flow()

        async def refresh_status() -> None:
            options._collector_wifi_current_ssid = "GRooVE"
            options._collector_wifi_network_diagnostics = "1,0,0"
            options._collector_wifi_networks = (
                SmartEssBleWifiNetwork(ssid="GRooVE", signal=98),
                SmartEssBleWifiNetwork(ssid="Other", signal=42),
            )

        options._async_refresh_collector_wifi_status = refresh_status

        result = await options.async_step_collector_wifi()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "collector_wifi")
        self.assertEqual(result["description_placeholders"]["current_ssid"], "GRooVE")
        self.assertEqual(result["description_placeholders"]["status_updates"], "")
        self.assertNotIn("network_diagnostics", result["description_placeholders"])
        self.assertIn(CONF_WIFI_SSID, result["data_schema"].schema)
        self.assertIn(CONF_WIFI_PASSWORD, result["data_schema"].schema)
        self.assertIn(CONF_COLLECTOR_WIFI_ACTION, result["data_schema"].schema)
        self.assertIn(CONF_CONFIRM_COLLECTOR_WIFI_APPLY, result["data_schema"].schema)

    async def test_options_collector_wifi_step_shows_only_non_empty_status_updates(self) -> None:
        options = self._make_options_flow()
        options._collector_wifi_current_ssid = "GRooVE"
        options._collector_wifi_last_result = "Saved."
        options._collector_wifi_last_error = "collector_timeout"

        result = await options.async_step_collector_wifi(
            {
                CONF_COLLECTOR_WIFI_ACTION: COLLECTOR_WIFI_ACTION_APPLY,
                CONF_WIFI_SSID: "NewWiFi",
                CONF_WIFI_PASSWORD: "Secret123",
            }
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(
            result["errors"],
            {CONF_CONFIRM_COLLECTOR_WIFI_APPLY: "collector_wifi_apply_not_confirmed"},
        )
        self.assertIn("**Last action:** Saved.", result["description_placeholders"]["status_updates"])
        self.assertIn(
            "**Last error:** collector_timeout",
            result["description_placeholders"]["status_updates"],
        )

    async def test_options_collector_wifi_refresh_keeps_flow_open(self) -> None:
        options = self._make_options_flow()
        apply_mock = AsyncMock()

        async def refresh_status() -> None:
            options._collector_wifi_current_ssid = "GRooVE"

        options._async_refresh_collector_wifi_status = refresh_status
        options._async_apply_collector_wifi_settings = apply_mock

        result = await options.async_step_collector_wifi(
            {
                CONF_COLLECTOR_WIFI_ACTION: COLLECTOR_WIFI_ACTION_REFRESH,
                CONF_WIFI_SSID: "Ignored",
                CONF_WIFI_PASSWORD: "Ignored",
            }
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {})
        apply_mock.assert_not_called()

    async def test_options_collector_wifi_apply_preserves_existing_options(self) -> None:
        options = self._make_options_flow()
        options._config_entry.options = {"poll_interval": 15}
        options._async_apply_collector_wifi_settings = AsyncMock()

        result = await options.async_step_collector_wifi(
            {
                CONF_COLLECTOR_WIFI_ACTION: COLLECTOR_WIFI_ACTION_APPLY,
                CONF_WIFI_SSID: "NewWiFi",
                CONF_WIFI_PASSWORD: "Secret123",
                CONF_CONFIRM_COLLECTOR_WIFI_APPLY: True,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"], {"poll_interval": 15})
        options._async_apply_collector_wifi_settings.assert_awaited_once_with(
            ssid="NewWiFi",
            password="Secret123",
        )

    async def test_options_collector_wifi_apply_writes_without_password_readback(self) -> None:
        options = self._make_options_flow()
        transport = AsyncMock()
        session = AsyncMock()
        writes: list[tuple[int, str]] = []
        reads: list[int] = []

        async def set_collector(parameter: int, value: str):
            writes.append((parameter, value))
            return type("_SetResponse", (), {"status": 0, "parameter": parameter})()

        async def query_collector(parameter: int):
            reads.append(parameter)
            return type(
                "_QueryResponse",
                (),
                {"code": 0, "parameter": parameter, "text": "NewWiFi", "data": b"NewWiFi"},
            )()

        async def with_session():
            return transport, session

        session.set_collector.side_effect = set_collector
        session.query_collector.side_effect = query_collector
        options._async_with_options_collector_session = with_session

        await options._async_apply_collector_wifi_settings(ssid="NewWiFi", password="Secret123")

        self.assertEqual(
            writes,
            [
                (SET_TARGET_SSID, "NewWiFi"),
                (SET_TARGET_PASSWORD, "Secret123"),
                (SET_REBOOT_OR_APPLY, "1"),
            ],
        )
        self.assertEqual(reads, [SET_TARGET_SSID])
        transport.stop.assert_awaited_once()

    async def test_options_runtime_step_preloads_translation_bundle_via_executor(self) -> None:
        options = self._make_options_flow()

        await options.async_step_runtime()

        self.assertIn(
            "_load_translation_bundle",
            [getattr(func, "__name__", "") for func, _args in options.hass.executor_job_calls],
        )

    async def test_options_runtime_step_localizes_control_mode_labels(self) -> None:
        options = self._make_options_flow()
        options.hass.config.language = "ru"

        result = await options.async_step_runtime()

        selector = result["data_schema"].schema["control_mode"]
        labels = [option["label"] for option in selector.config.kwargs["options"]]
        self.assertEqual(labels, ["Авто", "Только чтение", "Полный контроль"])

    async def test_options_runtime_step_serializes_branch_aware_option_payload(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_runtime(
            {
                "poll_interval": 15,
                "control_mode": "full",
                "connection": {
                    "server_ip": "192.168.1.60",
                    "collector_ip": "192.168.1.56",
                    "tcp_port": 8899,
                    "advertised_server_ip": "203.0.113.10",
                    "advertised_tcp_port": "9443",
                    "udp_port": 58899,
                    "discovery_target": "192.168.1.255",
                    "discovery_interval": 4,
                    "heartbeat_interval": 30,
                    "driver_hint": "modbus_smg",
                },
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["poll_interval"], 15)
        self.assertEqual(result["data"]["control_mode"], "full")
        self.assertEqual(result["data"]["advertised_server_ip"], "203.0.113.10")
        self.assertEqual(result["data"]["advertised_tcp_port"], 9443)
        self.assertEqual(result["data"]["driver_hint"], "modbus_smg")
        self.assertNotIn("connection", result["data"])

    async def test_advanced_metadata_offers_smartess_drafts_from_effective_metadata(self) -> None:
        options = self._make_options_flow()
        profile_metadata = load_driver_profile("pi30_ascii/models/smartess_0925_compat.json")
        schema_metadata = load_register_schema("pi30_ascii/models/smartess_0925_compat.json")

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="PI30-family runtime",
                effective_owner_key="pi30",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="pi30_ascii/models/smartess_0925_compat.json",
                effective_register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
                effective_profile_metadata=profile_metadata,
                effective_register_schema_metadata=schema_metadata,
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_advanced_metadata()

        self.assertEqual(result["type"], "menu")
        self.assertIn("create_profile_draft", result["menu_options"])
        self.assertIn("create_schema_draft", result["menu_options"])

    async def test_advanced_metadata_offers_known_family_draft_without_routine_actions(self) -> None:
        options = self._make_options_flow()
        profile_metadata = load_driver_profile("pi30_ascii/models/smartess_0925_compat.json")
        schema_metadata = load_register_schema("pi30_ascii/models/smartess_0925_compat.json")

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="PI30-family runtime",
                effective_owner_key="pi30",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="pi30_ascii/models/smartess_0925_compat.json",
                effective_register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
                effective_profile_metadata=profile_metadata,
                effective_register_schema_metadata=schema_metadata,
                smartess_cloud_export_available=True,
                smartess_known_family_draft_plan=_SmartEssDraftPlan(
                    source_profile_name="pi30_ascii/models/smartess_0925_compat.json",
                    source_schema_name="pi30_ascii/models/smartess_0925_compat.json",
                    driver_label="SmartESS 0925",
                    reason="Known-family inference matched the verified SmartESS 0925 detail-section signature bc_/bt_/gd_/pv_/sy_.",
                ),
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_advanced_metadata()

        self.assertEqual(result["type"], "menu")
        self.assertIn("export_smartess_cloud_evidence", result["menu_options"])
        self.assertIn("create_smartess_draft", result["menu_options"])
        self.assertNotIn("create_support_package", result["menu_options"])

    async def test_diagnostics_menu_exposes_reload_and_advanced_metadata_actions(self) -> None:
        options = self._make_options_flow()
        workflow = {
            f"support_workflow_{key}": value
            for key, value in build_support_workflow_state(
                has_inverter=True,
                effective_owner_key="modbus_smg",
                effective_owner_name="SMG-family runtime",
                detection_confidence="high",
                profile_source_scope="external",
                schema_source_scope="builtin",
            ).items()
        }

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                smartess_cloud_export_available=True,
                smartess_known_family_draft_plan=None,
                smartess_smg_bridge_plan=None,
                data=types.SimpleNamespace(values=workflow),
            )

            result = await options.async_step_diagnostics()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(
            result["menu_options"],
            [
                "create_support_package",
                "reload_local_metadata",
                "advanced_metadata",
            ],
        )
        self.assertEqual(
            result["description_placeholders"]["support_archive_action_label"],
            "Create support archive",
        )
        self.assertEqual(
            options._tr("options.step.diagnostics.menu_options.reload_local_metadata", ""),
            "Reload local metadata",
        )
        self.assertEqual(
            options._tr("options.step.advanced_metadata.menu_options.export_smartess_cloud_evidence", ""),
            "Export SmartESS cloud evidence",
        )
        self.assertNotIn("proxy_capture", result["menu_options"])
        self.assertIn(
            "visible entity count may stay the same",
            result["description_placeholders"]["smartess_cloud_diagnostics_hint"],
        )

    async def test_advanced_metadata_hides_routine_diagnostics_actions(self) -> None:
        options = self._make_options_flow()
        workflow = {
            f"support_workflow_{key}": value
            for key, value in build_support_workflow_state(
                has_inverter=True,
                effective_owner_key="modbus_smg",
                effective_owner_name="SMG-family runtime",
                detection_confidence="high",
                profile_source_scope="external",
                schema_source_scope="builtin",
            ).items()
        }

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                smartess_cloud_export_available=True,
                smartess_known_family_draft_plan=None,
                smartess_smg_bridge_plan=None,
                data=types.SimpleNamespace(values=workflow),
            )

            result = await options.async_step_advanced_metadata()

        self.assertEqual(result["type"], "menu")
        self.assertIn("export_smartess_cloud_evidence", result["menu_options"])
        self.assertIn("proxy_capture", result["menu_options"])
        self.assertNotIn("create_support_package", result["menu_options"])
        self.assertNotIn("reload_local_metadata", result["menu_options"])
        self.assertNotIn("rollback_local_metadata", result["menu_options"])

    async def test_proxy_capture_step_shows_planner_status(self) -> None:
        options = self._make_options_flow()

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False),
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="/config/eybond_local/proxy_traces/session.json",
                data=types.SimpleNamespace(
                    values={
                        "proxy_capture_status_label": "Ready",
                        "proxy_capture_summary": "Collector proxy capture is ready.",
                        "proxy_capture_blocking_reason": "",
                        "proxy_capture_current_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_target_endpoint": "192.168.1.50,18899,TCP",
                        "proxy_capture_masked_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_redirect_required": True,
                        "proxy_capture_can_stop": False,
                        "proxy_capture_status": "ready",
                        "proxy_trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
                        "proxy_trace_manifest_path": "/config/eybond_local/proxy_traces/session.json",
                        "proxy_trace_line_count": 7,
                        "proxy_trace_kind_summary": "chunk=4, frame=2, masked_endpoint_response=1",
                        "proxy_trace_recent_kinds": "chunk -> frame -> masked_endpoint_response",
                        "proxy_trace_recent_events": "2026-04-28T12:00:03Z cloud_to_collector: masked AT+CLDSRVHOST1 response as collector-cloud.smartess.example,18899,TCP",
                        "proxy_trace_last_timestamp": "2026-04-28T12:00:03Z",
                    }
                ),
            )

            result = await options.async_step_proxy_capture()

        self.assertEqual(result["step_id"], "proxy_capture")
        self.assertEqual(result["type"], "form")
        self.assertEqual(
            list(result["data_schema"].schema.keys())[:2],
            ["proxy_capture_live_log_view", "proxy_capture_action"],
        )
        self.assertEqual(
            list(result["data_schema"].schema.keys()),
            ["proxy_capture_live_log_view", "proxy_capture_action"],
        )
        self.assertIn("proxy_capture_action", result["data_schema"].schema)
        self.assertIn("proxy_capture_live_log_view", result["data_schema"].schema)
        self.assertTrue(
            result["data_schema"].schema["proxy_capture_live_log_view"].config.kwargs.get("read_only")
        )
        self.assertIn("Collector proxy capture is ready.", result["description_placeholders"]["proxy_capture_summary"])
        self.assertEqual(result["description_placeholders"]["proxy_trace_line_count"], "7")
        self.assertEqual(
            result["description_placeholders"]["proxy_trace_recent_kinds"],
            "chunk -> frame -> masked_endpoint_response",
        )
        self.assertIn("The live log is empty.", result["description_placeholders"]["proxy_capture_live_log"])
        self.assertIn(
            "accept collector traffic on the proxy endpoint",
            result["description_placeholders"]["proxy_capture_user_plan"],
        )
        self.assertEqual(result["description_placeholders"]["proxy_capture_saved_result_section"], "")

    async def test_show_proxy_capture_status_step_renders_current_status(self) -> None:
        options = self._make_options_flow()

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False),
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="/config/eybond_local/proxy_traces/session.json",
                data=types.SimpleNamespace(
                    values={
                        "proxy_capture_status_label": "Ready",
                        "proxy_capture_summary": "Collector proxy capture is ready.",
                        "proxy_capture_blocking_reason": "",
                        "proxy_capture_current_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_target_endpoint": "192.168.1.50,18899,TCP",
                        "proxy_capture_masked_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_redirect_required": True,
                        "proxy_capture_can_stop": False,
                        "proxy_capture_status": "ready",
                        "proxy_trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
                        "proxy_trace_manifest_path": "/config/eybond_local/proxy_traces/session.json",
                        "proxy_trace_line_count": 7,
                        "proxy_trace_kind_summary": "chunk=4, frame=2, masked_endpoint_response=1",
                        "proxy_trace_recent_kinds": "chunk -> frame -> masked_endpoint_response",
                        "proxy_trace_recent_events": "2026-04-28T12:00:03Z cloud_to_collector: masked AT+CLDSRVHOST1 response as collector-cloud.smartess.example,18899,TCP",
                        "proxy_trace_last_timestamp": "2026-04-28T12:00:03Z",
                    }
                ),
            )

            result = await options.async_step_proxy_capture()

        self.assertEqual(result["step_id"], "proxy_capture")
        self.assertEqual(result["type"], "form")
        self.assertEqual(
            result["description_placeholders"]["proxy_capture_current_endpoint"],
            "collector-cloud.smartess.example,18899,TCP",
        )
        self.assertEqual(result["description_placeholders"]["proxy_trace_line_count"], "7")
        self.assertEqual(
            result["description_placeholders"]["proxy_trace_recent_kinds"],
            "chunk -> frame -> masked_endpoint_response",
        )
        self.assertIn("The live log is empty.", result["description_placeholders"]["proxy_capture_live_log"])
        self.assertIn(
            "accept collector traffic on the proxy endpoint",
            result["description_placeholders"]["proxy_capture_user_plan"],
        )

    async def test_proxy_capture_prefers_full_live_log_and_relative_download_url(self) -> None:
        options = self._make_options_flow()

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False),
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="/config/eybond_local/proxy_traces/session.json",
                data=types.SimpleNamespace(
                    values={
                        "proxy_capture_status_label": "Ready",
                        "proxy_capture_summary": "Collector proxy capture is ready.",
                        "proxy_capture_blocking_reason": "",
                        "proxy_capture_current_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_target_endpoint": "192.168.1.50,18899,TCP",
                        "proxy_capture_masked_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_redirect_required": True,
                        "proxy_capture_can_stop": True,
                        "proxy_capture_status": "running",
                        "proxy_trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
                        "proxy_trace_manifest_path": "/config/eybond_local/proxy_traces/session.json",
                        "proxy_trace_saved_result_path": "/config/eybond_local/proxy_traces/session.zip",
                        "proxy_trace_saved_result_download_url": "/local/eybond_local/proxy_traces/session.zip",
                        "proxy_trace_line_count": 7,
                        "proxy_trace_kind_summary": "chunk=4, frame=2, masked_endpoint_response=1",
                        "proxy_trace_recent_kinds": "chunk -> frame -> masked_endpoint_response",
                        "proxy_trace_recent_events": "recent only",
                        "proxy_trace_live_log": "line one\nline two",
                        "proxy_trace_last_timestamp": "2026-04-28T12:00:03Z",
                    }
                ),
            )

            result = await options.async_step_proxy_capture()

        self.assertEqual(result["description_placeholders"]["proxy_capture_live_log"], "line one\nline two")
        self.assertEqual(result["description_placeholders"]["proxy_capture_saved_result_section"], "")

    async def test_proxy_capture_running_plan_surfaces_safety_lease_deadline(self) -> None:
        options = self._make_options_flow()
        options.hass.config.time_zone = "Europe/Kyiv"

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False),
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="/config/eybond_local/proxy_traces/session.json",
                data=types.SimpleNamespace(
                    values={
                        "proxy_capture_status_label": "Running",
                        "proxy_capture_summary": "Collector proxy capture is active.",
                        "proxy_capture_blocking_reason": "",
                        "proxy_capture_current_endpoint": "192.168.1.50,18899,TCP",
                        "proxy_capture_target_endpoint": "192.168.1.50,18899,TCP",
                        "proxy_capture_masked_endpoint": "collector-cloud.smartess.example,18899,TCP",
                        "proxy_capture_redirect_required": True,
                        "proxy_capture_can_stop": True,
                        "proxy_capture_status": "running",
                        "proxy_capture_session_expires_at": "2026-04-29T12:10:00+00:00",
                    }
                ),
            )

            result = await options.async_step_proxy_capture()

        self.assertIn(
            "29.04.2026 15:10 EEST",
            result["description_placeholders"]["proxy_capture_user_plan"],
        )
        self.assertNotIn(
            "2026-04-29T12:10:00+00:00",
            result["description_placeholders"]["proxy_capture_user_plan"],
        )
        self.assertNotIn(
            "29.04.2026 12:10 UTC",
            result["description_placeholders"]["proxy_capture_user_plan"],
        )
        self.assertNotIn(
            "lease",
            result["description_placeholders"]["proxy_capture_user_plan"].lower(),
        )

    async def test_proxy_capture_shows_saved_zip_when_session_is_finished(self) -> None:
        options = self._make_options_flow()

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False),
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="/config/eybond_local/proxy_traces/session.json",
                data=types.SimpleNamespace(
                    values={
                        "proxy_capture_status_label": "Ready",
                        "proxy_capture_summary": "Collector proxy capture is ready.",
                        "proxy_capture_status": "ready",
                        "proxy_trace_saved_result_path": "/config/eybond_local/proxy_traces/session.zip",
                        "proxy_trace_saved_result_download_url": "http://195.191.72.37:8123/local/eybond_local/proxy_traces/session.zip",
                    }
                ),
            )

            result = await options.async_step_proxy_capture()

        self.assertIn(
            "](http://195.191.72.37:8123/local/eybond_local/proxy_traces/session.zip)",
            result["description_placeholders"]["proxy_capture_saved_result_section"],
        )
        self.assertIn(
            "previous capture is complete",
            result["description_placeholders"]["proxy_capture_user_plan"].lower(),
        )
        self.assertNotIn(
            "/config/eybond_local/proxy_traces/session.zip",
            result["description_placeholders"]["proxy_capture_saved_result_section"],
        )

    async def test_start_proxy_capture_step_invokes_coordinator(self) -> None:
        options = self._make_options_flow()

        async def _start_proxy_capture(**kwargs):
            self.assertEqual(kwargs, {"anonymized": True, "confirm_redirect": False})
            return {
                "status": "running",
                "trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
            }

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False),
                async_start_proxy_capture=_start_proxy_capture,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="",
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_proxy_capture({"proxy_capture_action": "start"})

        self.assertEqual(result["step_id"], "proxy_capture")
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["description_placeholders"]["proxy_capture_action_result"], "Capture started.")

    async def test_start_proxy_capture_step_auto_confirms_redirect_when_required(self) -> None:
        options = self._make_options_flow()

        async def _start_proxy_capture(**kwargs):
            self.assertEqual(kwargs, {"anonymized": True, "confirm_redirect": True})
            return {
                "status": "running",
                "trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
            }

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False, redirect_required=True),
                async_start_proxy_capture=_start_proxy_capture,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="",
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_proxy_capture({"proxy_capture_action": "start"})

        self.assertEqual(result["step_id"], "proxy_capture")
        self.assertEqual(result["description_placeholders"]["proxy_capture_action_result"], "Capture started.")

    async def test_stop_proxy_capture_step_invokes_coordinator(self) -> None:
        options = self._make_options_flow()

        async def _stop_proxy_capture():
            return {
                "status": "stopped",
                "trace_path": "/config/eybond_local/proxy_traces/session.jsonl",
                "manifest_path": "/config/eybond_local/proxy_traces/session.json",
                "saved_result_path": "/config/eybond_local/proxy_traces/session.zip",
            }

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                proxy_capture_overview=types.SimpleNamespace(can_start=False, can_stop=True),
                async_stop_proxy_capture=_stop_proxy_capture,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=None,
                effective_register_schema_metadata=None,
                latest_proxy_trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                latest_proxy_trace_manifest_path="/config/eybond_local/proxy_traces/session.json",
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_proxy_capture({"proxy_capture_action": "stop"})

        self.assertEqual(result["step_id"], "proxy_capture")
        self.assertEqual(result["description_placeholders"]["proxy_capture_action_result"], "Capture stopped.")

    async def test_create_support_package_uses_absolute_download_link_in_result(self) -> None:
        options = self._make_options_flow()

        async def _export_support_package_with_cloud_refresh(
            *,
            smartess_username: str,
            smartess_password: str,
            wants_refresh: bool | None = None,
        ) -> str:
            return "/config/support/support_archive.zip"

        options._config_entry.runtime_data = types.SimpleNamespace(
            async_export_support_package_with_cloud_refresh=_export_support_package_with_cloud_refresh,
            smartess_cloud_export_available=True,
            smartess_collector_pn="E5000025388419",
            data=types.SimpleNamespace(
                values={
                    "support_package_download_url": "http://192.168.1.50:8123/local/eybond_local/support/support_archive.zip",
                    "support_package_download_relative_url": "/local/eybond_local/support/support_archive.zip",
                }
            ),
        )

        result = await options.async_step_create_support_package(
            {
                CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                "username": "groove",
                "password": "usa2000",
            }
        )

        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertIn(
            "](http://192.168.1.50:8123/local/eybond_local/support/support_archive.zip)",
            result["description_placeholders"]["download_markdown"],
        )

    async def test_proxy_capture_defaults_to_start_when_session_is_not_running(self) -> None:
        options = self._make_options_flow()
        coordinator = types.SimpleNamespace(
            proxy_capture_overview=types.SimpleNamespace(can_start=True, can_stop=False)
        )

        action = options._default_proxy_capture_action(
            coordinator,
            [
                {"value": "start", "label": "Start"},
                {"value": "refresh", "label": "Refresh"},
            ],
        )

        self.assertEqual(action, "start")

    async def test_proxy_capture_defaults_to_refresh_when_session_is_running(self) -> None:
        options = self._make_options_flow()
        coordinator = types.SimpleNamespace(
            proxy_capture_overview=types.SimpleNamespace(can_start=False, can_stop=True)
        )

        action = options._default_proxy_capture_action(
            coordinator,
            [
                {"value": "stop", "label": "Stop"},
                {"value": "refresh", "label": "Refresh"},
            ],
        )

        self.assertEqual(action, "refresh")

    async def test_diagnostics_menu_exposes_rollback_for_active_local_override(self) -> None:
        options = self._make_options_flow()
        workflow = {
            f"support_workflow_{key}": value
            for key, value in build_support_workflow_state(
                has_inverter=True,
                effective_owner_key="modbus_smg",
                effective_owner_name="SMG-family runtime",
                detection_confidence="high",
                profile_source_scope="external",
                schema_source_scope="external",
            ).items()
        }

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            profile_path = local_profile_path(Path(tempdir), "smg_modbus.json")
            schema_path = local_register_schema_path(
                Path(tempdir),
                "modbus_smg/models/smg_6200.json",
            )
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            profile_path.write_text("{}\n", encoding="utf-8")
            schema_path.write_text("{}\n", encoding="utf-8")
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=types.SimpleNamespace(
                    source_scope="external",
                    source_path=str(profile_path),
                ),
                effective_register_schema_metadata=types.SimpleNamespace(
                    source_scope="external",
                    source_path=str(schema_path),
                ),
                smartess_cloud_export_available=True,
                smartess_known_family_draft_plan=None,
                smartess_smg_bridge_plan=None,
                data=types.SimpleNamespace(values=workflow),
            )

            result = await options.async_step_diagnostics()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["menu_options"][0], "create_support_package")
        self.assertIn("reload_local_metadata", result["menu_options"])
        self.assertIn("rollback_local_metadata", result["menu_options"])
        self.assertNotIn("proxy_capture", result["menu_options"])

    async def test_rollback_local_metadata_runs_coordinator_action(self) -> None:
        options = self._make_options_flow()
        captured: dict[str, object] = {}

        async def _rollback_local_metadata() -> tuple[str, str]:
            captured["called"] = True
            return (
                "/config/eybond_local/profiles/smg_modbus.json",
                "/config/eybond_local/register_schemas/modbus_smg/models/smg_6200.json",
            )

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            profile_path = local_profile_path(Path(tempdir), "smg_modbus.json")
            schema_path = local_register_schema_path(
                Path(tempdir),
                "modbus_smg/models/smg_6200.json",
            )
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            profile_path.write_text("{}\n", encoding="utf-8")
            schema_path.write_text("{}\n", encoding="utf-8")
            options._config_entry.runtime_data = types.SimpleNamespace(
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=types.SimpleNamespace(
                    source_scope="external",
                    source_path=str(profile_path),
                ),
                effective_register_schema_metadata=types.SimpleNamespace(
                    source_scope="external",
                    source_path=str(schema_path),
                ),
                async_rollback_local_metadata=_rollback_local_metadata,
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_rollback_local_metadata({})

        self.assertTrue(captured["called"])
        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertIn(
            "removed",
            result["description_placeholders"]["status"].lower(),
        )
        self.assertIn(
            "/config/eybond_local/profiles/smg_modbus.json",
            result["description_placeholders"]["path"],
        )

    async def test_advanced_metadata_offers_smartess_smg_bridge_for_active_smg_entry(self) -> None:
        options = self._make_options_flow()
        profile_metadata = load_driver_profile("smg_modbus.json")
        schema_metadata = load_register_schema("modbus_smg/models/smg_6200.json")

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="SMG-family runtime",
                effective_owner_key="modbus_smg",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="smg_modbus.json",
                effective_register_schema_name="modbus_smg/models/smg_6200.json",
                effective_profile_metadata=profile_metadata,
                effective_register_schema_metadata=schema_metadata,
                smartess_cloud_export_available=True,
                smartess_smg_bridge_plan=_SmartEssSmgBridgePlan(
                    source_profile_name="smg_modbus.json",
                    source_schema_name="modbus_smg/models/smg_6200.json",
                    bridge_label="SmartESS SMG bridge",
                    reason="SmartESS cloud settings matched existing SMG controls and config readbacks.",
                    profile_enable_keys=("output_mode", "turn_on_mode"),
                    measurement_enable_keys=("output_mode", "turn_on_mode", "low_dc_cutoff_soc"),
                ),
                data=types.SimpleNamespace(values={}),
            )

            result = await options.async_step_advanced_metadata()

        self.assertEqual(result["type"], "menu")
        self.assertIn("create_smartess_smg_bridge", result["menu_options"])

    async def test_export_smartess_cloud_evidence_runs_coordinator_action(self) -> None:
        options = self._make_options_flow()
        captured: dict[str, str] = {}

        async def _export_smartess_cloud_evidence(*, username: str, password: str) -> str:
            captured["username"] = username
            captured["password"] = password
            return "/config/eybond_local/cloud_evidence/entry123.json"

        options._config_entry.runtime_data = types.SimpleNamespace(
            async_export_smartess_cloud_evidence=_export_smartess_cloud_evidence,
            smartess_collector_pn="E5000025388419",
            data=types.SimpleNamespace(values={}),
        )

        result = await options.async_step_export_smartess_cloud_evidence(
            {"username": "groove", "password": "usa2000"}
        )

        self.assertEqual(captured["username"], "groove")
        self.assertEqual(captured["password"], "usa2000")
        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertEqual(
            result["description_placeholders"]["path"],
            "/config/eybond_local/cloud_evidence/entry123.json",
        )
        self.assertIn(
            "SmartESS cloud bundle",
            result["description_placeholders"]["status"],
        )

    async def test_export_smartess_cloud_evidence_points_to_bridge_and_reload_when_available(self) -> None:
        options = self._make_options_flow()

        async def _export_smartess_cloud_evidence(*, username: str, password: str) -> str:
            return "/config/eybond_local/cloud_evidence/entry123.json"

        options._config_entry.runtime_data = types.SimpleNamespace(
            async_export_smartess_cloud_evidence=_export_smartess_cloud_evidence,
            smartess_collector_pn="E5000025388419",
            smartess_smg_bridge_plan=_SmartEssSmgBridgePlan(
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                bridge_label="SmartESS SMG bridge",
                reason="SmartESS cloud settings matched existing SMG controls and config readbacks.",
            ),
            smartess_known_family_draft_plan=None,
            data=types.SimpleNamespace(values={}),
        )

        result = await options.async_step_export_smartess_cloud_evidence(
            {"username": "groove", "password": "usa2000"}
        )

        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertIn(
            "create the SmartESS SMG bridge",
            result["description_placeholders"]["next_step"],
        )
        self.assertIn(
            "reload local metadata",
            result["description_placeholders"]["next_step"].lower(),
        )

    async def test_create_support_package_shows_guided_form_with_saved_cloud_evidence(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            smartess_cloud_export_available=True,
            smartess_cloud_evidence_path="/config/eybond_local/cloud_evidence/entry123.json",
            smartess_collector_pn="E5000025388419",
            data=types.SimpleNamespace(values={}),
        )

        result = await options.async_step_create_support_package()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "create_support_package")
        self.assertIn(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE, result["data_schema"].schema)
        self.assertEqual(
            result["description_placeholders"]["cloud_evidence_path"],
            "/config/eybond_local/cloud_evidence/entry123.json",
        )
        self.assertIn(
            "included automatically",
            result["description_placeholders"]["smartess_archive_plan_summary"],
        )
        selector = result["data_schema"].schema[CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE]
        option_values = [
            option["value"]
            for option in selector.config.kwargs["options"]
        ]
        self.assertEqual(
            option_values,
            [
                SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
            ],
        )

    async def test_create_support_package_refresh_requires_credentials(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            smartess_cloud_export_available=True,
            smartess_collector_pn="E5000025388419",
            data=types.SimpleNamespace(values={}),
        )

        result = await options.async_step_create_support_package(
            {
                CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                "username": "",
                "password": "",
            }
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "create_support_package")
        self.assertEqual(
            result["errors"],
            {"username": "required", "password": "required"},
        )

    async def test_create_support_package_refresh_exports_archive_inline(self) -> None:
        options = self._make_options_flow()
        captured: dict[str, object] = {}

        async def _export_support_package_with_cloud_refresh(
            *,
            smartess_username: str,
            smartess_password: str,
            wants_refresh: bool | None = None,
        ) -> str:
            captured["username"] = smartess_username
            captured["password"] = smartess_password
            captured["wants_refresh"] = wants_refresh
            return "/config/support/support_archive.zip"

        options._config_entry.runtime_data = types.SimpleNamespace(
            async_export_support_package_with_cloud_refresh=_export_support_package_with_cloud_refresh,
            smartess_cloud_export_available=True,
            smartess_collector_pn="E5000025388419",
            data=types.SimpleNamespace(
                values={
                    "support_package_download_url": "/api/diagnostics/support_archive.zip",
                }
            ),
        )

        result = await options.async_step_create_support_package(
            {
                CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                "username": "groove",
                "password": "usa2000",
            }
        )

        self.assertEqual(captured["username"], "groove")
        self.assertEqual(captured["password"], "usa2000")
        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertEqual(
            result["description_placeholders"]["path"],
            "/config/support/support_archive.zip",
        )
        self.assertIn(
            "Fresh SmartESS cloud evidence was fetched",
            result["description_placeholders"]["status"],
        )

    async def test_create_smartess_draft_runs_known_family_generator(self) -> None:
        options = self._make_options_flow()
        captured: dict[str, object] = {}
        plan = _SmartEssDraftPlan(
            source_profile_name="pi30_ascii/models/smartess_0925_compat.json",
            source_schema_name="pi30_ascii/models/smartess_0925_compat.json",
            driver_label="SmartESS 0925",
            reason="Known-family inference matched the verified SmartESS 0925 detail-section signature bc_/bt_/gd_/pv_/sy_.",
        )

        async def _create_smartess_known_family_draft_named(
            output_profile_name: str | None = None,
            output_schema_name: str | None = None,
            *,
            overwrite: bool = True,
        ) -> tuple[str, str]:
            captured["output_profile_name"] = output_profile_name
            captured["output_schema_name"] = output_schema_name
            captured["overwrite"] = overwrite
            return (
                "/config/eybond_local/profiles/pi30_ascii/models/smartess_0925_compat.json",
                "/config/eybond_local/register_schemas/pi30_ascii/models/smartess_0925_compat.json",
            )

        options._config_entry.runtime_data = types.SimpleNamespace(
            smartess_known_family_draft_plan=plan,
            async_create_smartess_known_family_draft_named=_create_smartess_known_family_draft_named,
            data=types.SimpleNamespace(
                values={"cloud_evidence_path": "/config/eybond_local/cloud_evidence/entry123.json"}
            ),
        )

        result = await options.async_step_create_smartess_draft(
            {
                "output_profile": "pi30_ascii/models/smartess_0925_compat.json",
                "output_schema": "pi30_ascii/models/smartess_0925_compat.json",
                "overwrite": True,
            }
        )

        self.assertEqual(
            captured["output_profile_name"],
            "pi30_ascii/models/smartess_0925_compat.json",
        )
        self.assertEqual(
            captured["output_schema_name"],
            "pi30_ascii/models/smartess_0925_compat.json",
        )
        self.assertTrue(captured["overwrite"])
        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertIn("SmartESS-derived local profile", result["description_placeholders"]["status"])

    async def test_create_smartess_smg_bridge_runs_bridge_generator(self) -> None:
        options = self._make_options_flow()
        captured: dict[str, object] = {}
        plan = _SmartEssSmgBridgePlan(
            source_profile_name="smg_modbus.json",
            source_schema_name="modbus_smg/models/smg_6200.json",
            bridge_label="SmartESS SMG bridge",
            reason="SmartESS cloud settings matched existing SMG controls and config readbacks.",
            profile_enable_keys=("output_mode", "turn_on_mode"),
            measurement_enable_keys=("output_mode", "turn_on_mode", "low_dc_cutoff_soc"),
            blocked_field_titles=("Power Saving Mode",),
            skipped_field_titles=("Output control",),
        )

        async def _create_smartess_smg_bridge_named(
            output_profile_name: str | None = None,
            output_schema_name: str | None = None,
            *,
            overwrite: bool = True,
        ) -> tuple[str, str]:
            captured["output_profile_name"] = output_profile_name
            captured["output_schema_name"] = output_schema_name
            captured["overwrite"] = overwrite
            return (
                "/config/eybond_local/profiles/smg_modbus.json",
                "/config/eybond_local/register_schemas/modbus_smg/models/smg_6200.json",
            )

        options._config_entry.runtime_data = types.SimpleNamespace(
            smartess_smg_bridge_plan=plan,
            async_create_smartess_smg_bridge_named=_create_smartess_smg_bridge_named,
            data=types.SimpleNamespace(
                values={"cloud_evidence_path": "/config/eybond_local/cloud_evidence/entry123.json"}
            ),
        )

        result = await options.async_step_create_smartess_smg_bridge(
            {
                "output_profile": "smg_modbus.json",
                "output_schema": "modbus_smg/models/smg_6200.json",
                "overwrite": True,
            }
        )

        self.assertEqual(captured["output_profile_name"], "smg_modbus.json")
        self.assertEqual(
            captured["output_schema_name"],
            "modbus_smg/models/smg_6200.json",
        )
        self.assertTrue(captured["overwrite"])
        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertIn("SmartESS-backed SMG bridge draft", result["description_placeholders"]["status"])

    async def test_diagnostics_placeholders_use_effective_smartess_metadata_without_driver(self) -> None:
        options = self._make_options_flow()
        profile_metadata = load_driver_profile("pi30_ascii/models/smartess_0925_compat.json")
        schema_metadata = load_register_schema("pi30_ascii/models/smartess_0925_compat.json")

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            options._config_entry.runtime_data = types.SimpleNamespace(
                current_driver=None,
                effective_owner_name="PI30-family runtime",
                effective_owner_key="pi30",
                smartess_family_name="SmartESS 0925",
                effective_profile_name="pi30_ascii/models/smartess_0925_compat.json",
                effective_register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
                effective_profile_metadata=profile_metadata,
                effective_register_schema_metadata=schema_metadata,
                data=types.SimpleNamespace(values={}),
            )

            placeholders = options._diagnostics_placeholders()

        self.assertEqual(placeholders["effective_owner_name"], "PI30-family runtime")
        self.assertEqual(placeholders["effective_owner_key"], "pi30")
        self.assertEqual(placeholders["smartess_family_name"], "SmartESS 0925")
        self.assertEqual(placeholders["smartess_family_line"], "\n**SmartESS family:** SmartESS 0925")
        self.assertEqual(placeholders["profile_name"], "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(
            placeholders["register_schema_name"],
            "pi30_ascii/models/smartess_0925_compat.json",
        )
        self.assertIn(
            "profiles/pi30_ascii/models/smartess_0925_compat.json",
            placeholders["effective_profile_source"],
        )
        self.assertIn(
            "register_schemas/pi30_ascii/models/smartess_0925_compat.json",
            placeholders["effective_schema_source"],
        )

    def test_validate_connection_inputs_uses_field_validation_metadata(self) -> None:
        flow = self._make_flow()
        errors = flow._validate_connection_inputs(
            {
                "server_ip": "not-an-ip",
                "advertised_server_ip": "still-not-an-ip",
                "advertised_tcp_port": "70000",
                "collector_ip": "",
                "discovery_target": "also-not-an-ip",
            },
            fields=flow._connection_branch().form_layout.manual_fields
            + flow._connection_branch().form_layout.manual_advanced_fields,
        )

        self.assertEqual(errors["server_ip"], "invalid_ip")
        self.assertEqual(errors["advertised_server_ip"], "invalid_ip")
        self.assertEqual(errors["advertised_tcp_port"], "invalid_port")
        self.assertEqual(errors["discovery_target"], "invalid_ip")
        self.assertNotIn("collector_ip", errors)


    def test_flatten_sections_coerces_numeric_selector_values_to_ints(self) -> None:
        flattened = _flatten_sections(
            {
                "server_ip": "192.168.1.50",
                "advanced_connection": {
                    "tcp_port": 8899.0,
                    "udp_port": 58899.0,
                    "discovery_interval": 10.0,
                    "heartbeat_interval": 60.0,
                    "advertised_tcp_port": "9443",
                },
            }
        )

        self.assertEqual(flattened["advertised_tcp_port"], 9443)
        self.assertEqual(flattened["tcp_port"], 8899)
        self.assertEqual(flattened["udp_port"], 58899)
        self.assertEqual(flattened["discovery_interval"], 10)
        self.assertEqual(flattened["heartbeat_interval"], 60)


if __name__ == "__main__":
    unittest.main()
