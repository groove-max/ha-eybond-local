from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch, sentinel


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_homeassistant_stubs() -> None:
    if "homeassistant" in sys.modules:
        return

    voluptuous = types.ModuleType("voluptuous")
    ha = types.ModuleType("homeassistant")
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
    core.callback = callback
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


from custom_components.eybond_local.config_flow import (
    CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE,
    CONF_SETUP_MODE,
    CONF_RESULT_KEY,
    EybondLocalConfigFlow,
    EybondLocalOptionsFlow,
    SETUP_MODE_DEEP_SCAN,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
    _get_ipv4_interfaces,
    _flatten_sections,
)
from custom_components.eybond_local.const import (
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
        self.config_entries = _FakeConfigEntries(entries)
        self.config = types.SimpleNamespace(language="en", config_dir="")
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

    def test_get_ipv4_interfaces_parses_busybox_oneline_output(self) -> None:
        output = (
            "1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever preferred_lft forever\n"
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

    async def test_user_step_single_interface_welcome_hint_mentions_selected_interface_only(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_user()

        self.assertIn("192.168.1.50", result["description_placeholders"]["welcome_hint"])
        self.assertNotIn("wizard will then ask", result["description_placeholders"]["welcome_hint"])

    async def test_user_step_multi_interface_welcome_hint_mentions_interface_selection(self) -> None:
        flow = self._make_flow()
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "192.168.2.50", "label": "wlan0 - 192.168.2.50"},
        ]

        result = await flow.async_step_user()

        self.assertIn("wizard will then ask", result["description_placeholders"]["welcome_hint"])
        self.assertNotIn("192.168.1.50", result["description_placeholders"]["welcome_hint"])

    async def test_user_step_routes_to_interface_selection_when_multiple_interfaces(self) -> None:
        flow = self._make_flow()
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "192.168.2.50", "label": "wlan0 - 192.168.2.50"},
        ]

        result = await flow.async_step_user({"connection_type": "eybond"})

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "auto")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")

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

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "auto")
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

        self.assertNotIn("deep_scan", result["menu_options"])

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
        self.assertEqual(result["menu_options"][:2], ["refresh_scan", "deep_scan"])
        self.assertIn("scan_summary", result["description_placeholders"])
        self.assertIn("choose", result["menu_options"])
        self.assertIn("deep_scan", result["menu_options"])

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

    async def test_confirm_step_routes_to_smartess_cloud_assist_choice_for_low_confidence_result(self) -> None:
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

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "smartess_cloud_assist_choice")
        self.assertIn("smartess_cloud_assist", result["menu_options"])
        self.assertIn("confirm_without_cloud_assist", result["menu_options"])

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

        result = await flow.async_step_confirm({"poll_interval": 15})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["options"]["poll_interval"], 15)
        self.assertEqual(result["data"][CONF_SMARTESS_COLLECTOR_VERSION], "1.2.3")
        self.assertEqual(result["data"][CONF_SMARTESS_PROTOCOL_ASSET_ID], "0925")
        self.assertEqual(result["data"][CONF_SMARTESS_PROFILE_KEY], "smartess_0925")
        self.assertEqual(result["data"][CONF_SMARTESS_DEVICE_ADDRESS], 5)

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

    async def test_manual_confirm_offers_smartess_cloud_assist_when_collector_pn_is_known(self) -> None:
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

        self.assertIn("manual_smartess_cloud_assist", result["menu_options"])

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

        self.assertIn("Оновити результати сканування", placeholders["scan_next_hint"])
        self.assertIn("Ручне налаштування", placeholders["scan_next_hint"])
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
        self.assertIn("сохранить ожидающую запись", placeholders["scan_next_hint"])
        self.assertIn("Есть признаки SmartESS", result_label)

    async def test_options_runtime_step_renders_branch_aware_connection_section(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "runtime")

    async def test_options_runtime_step_preloads_translation_bundle_via_executor(self) -> None:
        options = self._make_options_flow()

        await options.async_step_runtime()

        self.assertIn(
            "_load_translation_bundle",
            [getattr(func, "__name__", "") for func, _args in options.hass.executor_job_calls],
        )

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
        self.assertIn("export_support_bundle", result["menu_options"])
        self.assertIn("create_smartess_draft", result["menu_options"])
        self.assertNotIn("export_smartess_cloud_evidence", result["menu_options"])

    async def test_diagnostics_menu_exposes_reload_and_smartess_cloud_actions(self) -> None:
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
                "export_smartess_cloud_evidence",
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
            options._tr("options.step.diagnostics.menu_options.export_smartess_cloud_evidence", ""),
            "Export SmartESS cloud evidence",
        )
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
        self.assertIn("export_support_bundle", result["menu_options"])
        self.assertNotIn("create_support_package", result["menu_options"])
        self.assertNotIn("reload_local_metadata", result["menu_options"])
        self.assertNotIn("export_smartess_cloud_evidence", result["menu_options"])

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
        captured: dict[str, str] = {}

        async def _export_support_package_with_cloud_refresh(
            *,
            smartess_username: str,
            smartess_password: str,
        ) -> str:
            captured["username"] = smartess_username
            captured["password"] = smartess_password
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
