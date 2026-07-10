from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch, sentinel
import zipfile


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
    entity_registry = types.ModuleType("homeassistant.helpers.entity_registry")
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
    selector.SelectSelectorMode = types.SimpleNamespace(DROPDOWN="dropdown", LIST="list")
    selector.TextSelector = _Selector
    selector.TextSelectorConfig = _SelectorConfig

    entity_registry.async_get = lambda _hass: None
    entity_registry.async_entries_for_config_entry = lambda *_args, **_kwargs: []
    helpers.entity_registry = entity_registry

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
    sys.modules["homeassistant.helpers.entity_registry"] = entity_registry
    sys.modules["homeassistant.helpers.selector"] = selector


_install_homeassistant_stubs()


import custom_components.eybond_local.config_flow as config_flow_module
from custom_components.eybond_local.config_flow import (
    BLE_ACTION_APPLY,
    BLE_ACTION_RESCAN,
    BLE_ACTION_REFRESH_WIFI,
    COLLECTOR_UART_ACTION_APPLY,
    COLLECTOR_UART_ACTION_REFRESH,
    COLLECTOR_WIFI_ACTION_APPLY,
    COLLECTOR_WIFI_ACTION_REFRESH,
    COLLECTOR_NETWORK_ALREADY_CONNECTED,
    COLLECTOR_NETWORK_NEEDS_BLUETOOTH,
    COLLECTOR_OPERATION_HA_ONLY,
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
    CONF_BLE_ACTION,
    CONF_COLLECTOR_UART_ACTION,
    CONF_COLLECTOR_UART_BAUDRATE,
    CONF_COLLECTOR_WIFI_ACTION,
    CONF_COLLECTOR_NETWORK_STATUS,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_CONFIRM_COLLECTOR_UART_APPLY,
    CONF_CONFIRM_COLLECTOR_WIFI_APPLY,
    CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE,
    CONF_SETUP_MODE,
    CONF_WIFI_PASSWORD,
    CONF_WIFI_SSID,
    CONF_RESULT_KEY,
    EybondLocalConfigFlow,
    EybondLocalOptionsFlow,
    SHADOW_LEARNING_ACTION_EXPORT_SUPPORT_ONLY,
    SHADOW_LEARNING_ACTION_GENERATE_OVERLAY,
    SHADOW_LEARNING_ACTION_REFRESH,
    SHADOW_LEARNING_ACTION_RUN_LEARNING,
    SHADOW_LEARNING_MODE_ENUM_SWEEP,
    SHADOW_LEARNING_MODE_MANUAL,
    SHADOW_LEARNING_MODE_SUPPORT_ONLY,
    SETUP_MODE_DEEP_SCAN,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
    _get_ipv4_interfaces,
    _flatten_sections,
)
from custom_components.eybond_local.support.bundle import build_support_bundle_payload
from custom_components.eybond_local.support.package import (
    build_shadow_learning_runtime_values,
    export_support_package,
)
from custom_components.eybond_local.support.shadow_learning_review_model import (
    attach_learned_read_review_model,
    build_learned_control_review_model,
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
    QUERY_HARDWARE_VERSION,
    QUERY_SERIAL_BAUDRATE,
    SET_REBOOT_OR_APPLY,
    SET_SERVER_ENDPOINT,
    SET_SERIAL_BAUDRATE,
    SET_TARGET_PASSWORD,
    SET_TARGET_SSID,
)
from custom_components.eybond_local.const import (
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
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


def _schema_select_options(data_schema, field_name: str) -> list[str]:
    """Extract SelectSelector option values for one schema field."""

    for key, validator in data_schema.schema.items():
        if str(key) != field_name:
            continue
        config = getattr(validator, "config", None)
        # The stubbed SelectSelectorConfig keeps kwargs; real HA uses a dict.
        if hasattr(config, "kwargs"):
            options = config.kwargs.get("options", [])
        else:
            options = (config or {}).get("options", [])
        values = []
        for option in options:
            if isinstance(option, dict):
                values.append(str(option["value"]))
            else:
                values.append(str(option))
        return values
    raise AssertionError(f"field {field_name} not found in schema")


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

    def test_selected_result_with_driver_choice_promotes_selected_match(self) -> None:
        flow = self._make_flow()
        pi30 = DriverMatch(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            serial_number="VMII-NXPW5KW",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=255, device_addr=0),
        )
        smg = DriverMatch(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG-compatible",
            serial_number="VMII-NXPW5KW",
            probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
        )
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="deep_scan",
                ip="192.168.1.55",
            ),
            match=pi30,
            alternative_matches=(smg,),
        )

        self.assertEqual(
            [match.driver_key for match in flow._driver_choice_candidates(result)],
            ["pi30", "modbus_smg"],
        )
        selected = flow._selected_result_with_match(result, smg)

        self.assertEqual(selected.match.driver_key, "modbus_smg")
        self.assertEqual([match.driver_key for match in selected.alternative_matches], ["pi30"])

    def test_driver_choice_presentation_is_human_readable(self) -> None:
        flow = self._make_flow()
        pi30 = DriverMatch(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PI30 4200",
            serial_number="X1",
            probe_target=ProbeTarget(devcode=0x0102, collector_addr=255, device_addr=0),
            details={"probe_elapsed_ms": 4086},
        )
        smartess = DriverMatch(
            driver_key="smartess_local",
            protocol_family="smartess_local",
            model_name="PowMr 4.2kW (SmartESS 0925)",
            serial_number="X1",
            probe_target=ProbeTarget(devcode=1, collector_addr=5, device_addr=5),
            details={"probe_elapsed_ms": 6220},
        )
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="deep_scan",
                ip="192.168.1.14",
            ),
            match=pi30,
            alternative_matches=(smartess,),
        )
        candidates = flow._driver_choice_candidates(result)

        primary_label = flow._driver_choice_label(candidates[0], recommended=True)
        alternative_label = flow._driver_choice_label(candidates[1])
        self.assertEqual(primary_label, "PI30 4200 (recommended)")
        self.assertEqual(alternative_label, "PowMr 4.2kW (SmartESS 0925)")
        # No raw route digits anywhere in the labels.
        self.assertNotIn("255", primary_label + alternative_label)
        self.assertNotIn("258", primary_label + alternative_label)

        placeholders = flow._driver_choice_placeholders(result)
        lines = placeholders["driver_choice_candidates"].splitlines()
        self.assertIn("answered in 4.1s", lines[0])
        self.assertIn("recommended", lines[0])
        self.assertIn("answered in 6.2s", lines[1])
        self.assertNotIn("recommended", lines[1])
        self.assertNotIn("255", placeholders["driver_choice_candidates"])

    def test_driver_choice_shows_device_address_only_for_same_driver_duplicates(self) -> None:
        flow = self._make_flow()
        first = DriverMatch(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="X1",
            probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
        )
        second = DriverMatch(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="X1",
            probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=4),
        )
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="deep_scan",
                ip="192.168.1.14",
            ),
            match=first,
            alternative_matches=(second,),
        )
        candidates = flow._driver_choice_candidates(result)
        self.assertTrue(flow._driver_choice_needs_address(candidates))

        label = flow._driver_choice_label(candidates[1], include_address=True)
        self.assertEqual(label, "SMG 6200 (device address 4)")

    def test_driver_choice_appends_driver_name_only_when_model_lacks_it(self) -> None:
        flow = self._make_flow()
        match = DriverMatch(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji 4200",
            serial_number="X1",
            probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
        )

        self.assertEqual(
            flow._driver_choice_base_label(match),
            "Anenji 4200 — SMG / Modbus",
        )

    async def test_driver_choice_submit_updates_autodetect_registry_and_refresh_state(self) -> None:
        from custom_components.eybond_local.config_flow import CONF_DRIVER_MATCH_KEY

        flow = self._make_flow()
        pi30 = DriverMatch(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            serial_number="VMII-NXPW5KW",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=255, device_addr=0),
        )
        smg = DriverMatch(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG-compatible",
            serial_number="VMII-NXPW5KW",
            probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
        )
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="deep_scan",
                ip="192.168.1.55",
            ),
            match=pi30,
            alternative_matches=(smg,),
        )
        flow._autodetect_results = {"result_1": result}
        flow._set_selected_result(result)
        flow._selected_result_runtime_details_attempted = True
        flow._selected_result_collector_capabilities_attempted = True

        with patch.object(
            flow,
            "async_step_detection_summary",
            new=AsyncMock(return_value={"type": "form", "step_id": "detection_summary"}),
        ):
            step_result = await flow.async_step_driver_choice(
                {CONF_DRIVER_MATCH_KEY: flow._driver_choice_key(smg)}
            )

        self.assertEqual(step_result["step_id"], "detection_summary")
        self.assertEqual(flow._selected_result.match.driver_key, "modbus_smg")
        self.assertIs(flow._autodetect_results["result_1"], flow._selected_result)
        self.assertFalse(flow._selected_result_runtime_details_attempted)
        self.assertFalse(flow._selected_result_collector_capabilities_attempted)

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

    async def test_user_step_skips_welcome_for_single_connection_type(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_user()

        # One supported connection type: no welcome form, straight to readiness.
        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "collector_network")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")

    async def test_user_step_preloads_translation_bundle_via_executor(self) -> None:
        flow = self._make_flow()

        await flow.async_step_user()

        self.assertIn(
            "_load_translation_bundle",
            [getattr(func, "__name__", "") for func, _args in flow.hass.executor_job_calls],
        )

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

    async def test_collector_network_auto_advances_to_scanning_with_one_interface(self) -> None:
        flow = self._make_flow()

        async def _fake_scanning(user_input=None):
            return {"type": "progress", "step_id": "scanning"}

        flow.async_step_scanning = _fake_scanning

        menu_result = await flow.async_step_collector_network()
        result = await flow.async_step_auto()

        self.assertEqual(menu_result["type"], "menu")
        self.assertIn("auto", menu_result["menu_options"])
        # One interface: the interface-picker form is skipped entirely.
        self.assertEqual(result["type"], "progress")
        self.assertEqual(result["step_id"], "scanning")

    async def test_user_step_routes_to_auto_when_one_interface(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_user({"connection_type": "eybond"})

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "collector_network")
        self.assertEqual(flow._auto_config["connection_type"], "eybond")
        self.assertEqual(flow._auto_config["server_ip"], "192.168.1.50")

    async def test_auto_step_uses_localized_interface_hint(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "ru"
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}
        # Two interfaces => the picker form is shown (no auto-advance).
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "10.0.0.2", "label": "wlan0 - 10.0.0.2"},
        ]

        result = await flow.async_step_auto()

        self.assertEqual(result["type"], "form")
        hint = result["description_placeholders"]["interface_hint"]
        self.assertIn("Выберите", hint)
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
                    address="AA:BB:CC:DD:EE:47",
                    name="E50000200000000001\u200b",
                    manufacturer_data={0x3545: b"0000200000000001"},
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
            ["AA:BB:CC:DD:EE:47"],
        )
        self.assertIn("E50000200000000001", options[0]["label"])

    async def test_bluetooth_setup_uses_home_assistant_bluetooth_advertisement_callback(self) -> None:
        flow = self._make_flow()

        components_module = types.ModuleType("homeassistant.components")
        bluetooth_module = types.ModuleType("homeassistant.components.bluetooth")
        bluetooth_module.async_discovered_service_info = Mock(return_value=())
        bluetooth_module.async_scanner_devices_by_address = Mock(return_value=())
        bluetooth_module.BluetoothScanningMode = types.SimpleNamespace(ACTIVE=sentinel.active_scan)
        service_info = types.SimpleNamespace(
            address="AA:BB:CC:DD:EE:47",
            name="E50000200000000001\u200b",
            manufacturer_data={0x3545: b"0000200000000001"},
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
        self.assertEqual(bluetooth_module.async_register_callback.call_count, 8)
        registered_matchers = [
            call.args[2] for call in bluetooth_module.async_register_callback.call_args_list
        ]
        self.assertIn({"local_name": "E50*", "connectable": False}, registered_matchers)
        self.assertIn({"local_name": "E50*", "connectable": True}, registered_matchers)
        self.assertIn({"local_name": "V00*", "connectable": False}, registered_matchers)
        self.assertIn({"local_name": "V00*", "connectable": True}, registered_matchers)
        ble_selector = result["data_schema"].schema["ble_address"]
        options = ble_selector.config.kwargs["options"]
        self.assertEqual(
            [option["value"] for option in options],
            ["AA:BB:CC:DD:EE:47"],
        )
        self.assertIn("E50000200000000001", options[0]["label"])

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
                local_pn="E50000200000000001",
                local_name="Collector PN",
            ),
        )
        wifi_networks = (
            SmartEssBleWifiNetwork(ssid="Neighbor", signal=-75),
            SmartEssBleWifiNetwork(ssid="HomeNet", signal=-44),
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
            ["Neighbor", "HomeNet", "Office"],
        )
        self.assertEqual(
            [option["label"] for option in options],
            ["Neighbor (-75 dBm)", "HomeNet (-44 dBm)", "Office (-58 dBm)"],
        )
        self.assertEqual(result["errors"], {})

    async def test_bluetooth_setup_scans_default_collector_wifi_on_first_entry(self) -> None:
        flow = self._make_flow()
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000200000000001",
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
            new=AsyncMock(return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)),
        ) as wifi_scan:
            result = await flow.async_step_bluetooth_setup()

        wifi_scan.assert_awaited_once_with("AA:BB:CC:DD:EE:FF", ble_device=None)
        wifi_selector = result["data_schema"].schema["wifi_ssid"]
        options = wifi_selector.config.kwargs["options"]
        self.assertEqual(options[0]["value"], "HomeNet")
        self.assertEqual(options[0]["label"], "HomeNet (98%)")
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
                local_pn="E50000200000000001",
                local_name="Alpha Collector",
            ),
            SmartEssBleCandidate(
                address="11:22:33:44:55:66",
                local_pn="E50000200000009777",
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
                local_pn="E50000200000000001",
                local_name="Alpha Collector",
            ),
            SmartEssBleCandidate(
                address="11:22:33:44:55:66",
                local_pn="E50000200000009777",
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
            unique_id="collector:E50000200000000001",
            data={"collector_pn": "E50000200000000001"},
            options={},
        )
        flow = self._make_flow(entries=[existing_entry])
        flow.context = {"entry_id": "existing"}
        candidates = (
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000200000000001",
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
                local_pn="E50000200000000001",
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
                local_pn="E50000200000000001",
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
                (SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),),
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

        self.assertEqual(result, (SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),))
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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
        )
        refreshed_candidate = SmartEssBleCandidate(
            address="AA:BB:CC:DD:EE:FF",
            local_pn="E50000200000000001",
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

        self.assertEqual(result, (SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),))
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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
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
        self.assertEqual(result[0].ssid, "HomeNet")

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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
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
            return_value=(SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),)
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

        self.assertEqual(result, (SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),))
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
                    SmartEssBleWifiNetwork(ssid="HomeNet", signal=-42),
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
        self.assertEqual(wifi_selector.config.kwargs["options"][0]["value"], "HomeNet")

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
                local_pn="E50000200000000001",
                local_name="Alpha Collector",
            ),
            SmartEssBleCandidate(
                address="11:22:33:44:55:66",
                local_pn="E50000200000009777",
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
                local_pn="E50000200000000001",
                local_name="Collector PN",
            ),
        )
        cached_networks = (
            SmartEssBleWifiNetwork(ssid="HomeNet", signal=92),
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
        self.assertEqual([option["value"] for option in refreshed_options], ["HomeNet", "Office"])
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
                        local_pn="E50000200000000001",
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
                    "wifi_ssid": "HomeNet",
                    "wifi_password": "55555555",
                    CONF_BLE_ACTION: BLE_ACTION_APPLY,
                }
            )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "ble_provision_failed"})
        self.assertEqual(flow._ble_last_error, "ble_provision_failed:wflkap:W008")

    async def test_bluetooth_setup_runs_bootstrap_then_returns_to_scan_interface(self) -> None:
        flow = self._make_flow()
        async def _fake_scanning(user_input=None):
            return {"type": "progress", "step_id": "scanning"}

        flow.async_step_scanning = _fake_scanning
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
        # One interface: provisioning returns to auto, which auto-advances to scan.
        self.assertEqual(result["type"], "progress")
        self.assertEqual(result["step_id"], "scanning")

    async def test_bluetooth_setup_accepts_hidden_wifi_name_with_single_custom_selector(
        self,
    ) -> None:
        flow = self._make_flow()
        async def _fake_scanning(user_input=None):
            return {"type": "progress", "step_id": "scanning"}

        flow.async_step_scanning = _fake_scanning
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
                    SmartEssBleWifiNetwork(ssid="HomeNet", signal=-42),
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
        self.assertEqual(result["type"], "progress")
        self.assertEqual(result["step_id"], "scanning")

    async def test_deep_scan_autostarts_for_known_normal_network(self) -> None:
        flow = self._make_flow()
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

        with patch.object(
            flow,
            "async_step_start_deep_scan",
            new=AsyncMock(return_value={"type": "progress", "step_id": "scanning"}),
        ) as start:
            result = await flow.async_step_deep_scan()

        start.assert_awaited_once()
        self.assertEqual(result["step_id"], "scanning")

    async def test_deep_scan_large_subnet_shows_confirm_menu_without_duration_estimates(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"
        flow._auto_config = {"connection_type": "eybond", "server_ip": "192.168.1.50"}
        flow._interface_options = [
            {
                "name": "wlan0",
                "ip": "192.168.1.50",
                "label": "wlan0 - 192.168.1.50",
                "network": "192.168.0.0/16",
                "broadcast": "192.168.255.255",
            },
        ]

        result = await flow.async_step_deep_scan()

        self.assertEqual(result["step_id"], "deep_scan")
        self.assertEqual(result["description_placeholders"]["deep_scan_target_count"], "65533")
        self.assertNotIn("deep_scan_duration", result["description_placeholders"])
        self.assertTrue(result["description_placeholders"]["deep_scan_warning"])

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

    async def test_scan_results_without_results_offers_advanced_setup(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {}
        flow._scan_error = True

        result = await flow.async_step_scan_results()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "scan_results")
        options = _schema_select_options(result["data_schema"], "result_key")
        self.assertEqual(
            list(options),
            ["action:refresh_scan", "action:advanced_setup"],
        )

    async def test_advanced_setup_submenu_exposes_deep_and_manual(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_advanced_setup()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "advanced_setup")
        self.assertIn("deep_scan", result["menu_options"])
        self.assertIn("manual", result["menu_options"])
        self.assertIn("refresh_scan", result["menu_options"])

    async def test_advanced_setup_offers_change_interface_with_multiple(self) -> None:
        flow = self._make_flow()
        flow._interface_options = [
            {"name": "eth0", "ip": "192.168.1.50", "label": "eth0 - 192.168.1.50"},
            {"name": "wlan0", "ip": "192.168.2.50", "label": "wlan0 - 192.168.2.50"},
        ]

        result = await flow.async_step_advanced_setup()

        self.assertIn("change_scan_interface", result["menu_options"])

    async def test_scan_results_always_offers_advanced_setup(self) -> None:
        flow = self._make_flow()
        flow._scan_mode = SETUP_MODE_DEEP_SCAN

        result = await flow.async_step_scan_results()

        options = _schema_select_options(result["data_schema"], "result_key")
        self.assertIn("action:advanced_setup", options)

    def test_collapse_merges_skip_marker_with_pn_result_for_same_collector(self) -> None:
        flow = self._make_flow()
        skip_marker = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="subnet_unicast",
                ip="192.168.1.14",
            ),
            connection_mode="subnet_unicast",
            last_error="already_configured",
        )
        inventory_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.14",
                collector=CollectorInfo(
                    remote_ip="192.168.1.14",
                    collector_pn="Q0000000000001",
                ),
            ),
            connection_mode="broadcast",
            next_action="manual_driver_selection",
            last_error="collector_detected_without_driver",
        )
        other = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="subnet_unicast",
                ip="192.168.1.51",
                collector=CollectorInfo(remote_ip="192.168.1.51", collector_pn="V0000000000001"),
            ),
            connection_mode="subnet_unicast",
        )

        collapsed = flow._collapse_scan_results([skip_marker, inventory_result, other])

        self.assertEqual(len(collapsed), 2)
        merged = next(r for r in collapsed if r.collector.ip == "192.168.1.14")
        # The PN-carrying duplicate wins so the line shows the identity.
        self.assertEqual(merged.collector.collector.collector_pn, "Q0000000000001")

    async def test_scan_results_refresh_label_names_deep_scan_after_deep_scan(self) -> None:
        flow = self._make_flow()
        flow._scan_mode = SETUP_MODE_DEEP_SCAN
        flow._autodetect_results = {}

        self.assertEqual(flow._refresh_scan_action_label(), "Repeat deep scan")
        placeholders = flow._scan_results_placeholders()
        self.assertIn("Repeat deep scan", placeholders["scan_next_hint"])

        flow._scan_mode = "auto"
        self.assertEqual(
            flow._refresh_scan_action_label(),
            "Refresh scan results",
        )

    async def test_scan_results_with_available_results_offers_direct_selection(self) -> None:
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

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "scan_results")
        options = _schema_select_options(result["data_schema"], "result_key")
        self.assertEqual(
            list(options),
            ["0", "action:refresh_scan", "action:advanced_setup"],
        )
        self.assertIn("scan_summary", result["description_placeholders"])

        with (
            patch.object(
                flow,
                "async_step_detection_summary",
                new=AsyncMock(return_value={"type": "form", "step_id": "detection_summary"}),
            ),
            patch.object(flow, "_existing_entry_for_result", return_value=None),
        ):
            submit = await flow.async_step_scan_results({"result_key": "0"})

        self.assertEqual(submit["step_id"], "detection_summary")
        self.assertIs(flow._selected_result, flow._autodetect_results["0"])

    async def test_scan_results_udp_only_candidate_is_still_selectable(self) -> None:
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

        self.assertEqual(result["type"], "form")
        options = _schema_select_options(result["data_schema"], "result_key")
        self.assertIn("0", options)

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
                    serial_number="92632500000001",
                    probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                ),
                connection_mode="known_ip",
            ),
        }

        result = await flow.async_step_choose()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "choose")

    async def test_confirm_step_exposes_poll_mode_field(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
            ),
            connection_mode="known_ip",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertIn("poll_mode", result["data_schema"].schema)
        self.assertNotIn("poll_interval", result["data_schema"].schema)
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)

        manual_result = await flow.async_step_confirm({"poll_mode": "manual"})

        self.assertEqual(manual_result["type"], "form")
        self.assertEqual(manual_result["step_id"], "confirm_poll_interval")
        self.assertIn("poll_interval", manual_result["data_schema"].schema)

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
                serial_number="92632500000001",
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
            "| Serial Number | 92632500000001 |",
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
                serial_number="92632500000001",
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
                serial_number="92632500000001",
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
                serial_number="92632500000001",
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
                collector=CollectorInfo(collector_pn="E5000020000000"),
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
                collector=CollectorInfo(collector_pn="E5000020000000"),
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
                    serial_number="92632500000001",
                    probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                ),
                connection_mode="known_ip",
            ),
        }

        result = await flow.async_step_choose({CONF_RESULT_KEY: "1"})

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "detection_summary")
        self.assertIsNotNone(flow._selected_result)
        self.assertEqual(flow._selected_result.match.model_name, "SMG 6200")

        result = await flow.async_step_detection_summary({})
        self.assertEqual(result["step_id"], "confirm")

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

    async def test_create_entry_persists_collector_cloud_family_from_onboarding(self) -> None:
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
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="broadcast",
                ip="192.168.1.14",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="E5000099990001",
                    collector_cloud_family="valuecloud_at",
                    collector_cloud_family_source="endpoint_host",
                    collector_cloud_family_confidence="high",
                    collector_server_endpoint="iot.eybond.com,18899,TCP",
                ),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                details={},
            ),
            connection_mode="known_ip",
        )

        result = await flow._async_create_entry_from_result({"poll_interval": 30})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"][CONF_COLLECTOR_CLOUD_FAMILY], "valuecloud_at")

    async def test_choose_step_link_down_result_shows_retryable_error(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.55",
                    source="udp",
                    ip="192.168.1.55",
                    connected=True,
                ),
                connection_mode="known_ip",
                next_action="manual_driver_selection",
                last_error="inverter_link_down",
            )
        }

        result = await flow.async_step_choose({CONF_RESULT_KEY: "0"})

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "choose")
        self.assertEqual(result["errors"], {"base": "inverter_link_down"})
        self.assertIsNone(flow._selected_result)

    async def test_choose_step_single_link_down_result_does_not_auto_advance(self) -> None:
        flow = self._make_flow()
        flow._autodetect_results = {
            "0": OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.55",
                    source="udp",
                    ip="192.168.1.55",
                    connected=True,
                ),
                connection_mode="known_ip",
                last_error="inverter_link_down",
            )
        }

        result = await flow.async_step_choose()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "choose")
        self.assertEqual(result["errors"], {"base": "inverter_link_down"})

    def _result_with_catalog_details(self, catalog: dict | None) -> OnboardingResult:
        details = {}
        if catalog is not None:
            details["device_catalog"] = catalog
        return OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                details=details,
            ),
            connection_mode="known_ip",
        )

    async def test_detection_summary_full_tier_placeholders(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._result_with_catalog_details(
            {"kind": "device", "tier": "full", "entry_key": "smg_6200"}
        )

        result = await flow.async_step_detection_summary()

        self.assertEqual(result["step_id"], "detection_summary")
        placeholders = result["description_placeholders"]
        self.assertEqual(placeholders["model"], "SMG 6200")
        self.assertIn("Full support", placeholders["tier_headline"])

    async def test_detection_summary_offers_cloud_assist_only_as_optional_menu(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._result_with_catalog_details(
            {"kind": "family", "tier": "partial"}
        )
        flow._detection_summary_context = "auto"

        # Default: cloud assist is not offered -> plain info form, no auto-pop.
        plain = await flow.async_step_detection_summary()
        self.assertEqual(plain["type"], "form")

        # When it can be offered, it appears as an explicit choice, not before confirm.
        flow._can_offer_smartess_cloud_assist = lambda _result: True
        menu = await flow.async_step_detection_summary()
        self.assertEqual(menu["type"], "menu")
        self.assertEqual(menu["menu_options"], ["confirm", "smartess_cloud_assist"])

    async def test_confirm_does_not_auto_pop_cloud_assist(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._result_with_catalog_details(
            {"kind": "device", "tier": "full"}
        )
        flow._can_offer_smartess_cloud_assist = lambda _result: True

        result = await flow.async_step_confirm()

        # confirm shows its own form directly; cloud assist never interrupts it.
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")

    async def test_detection_summary_partial_tier_mentions_learning(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._result_with_catalog_details(
            {"kind": "family", "tier": "partial"}
        )

        result = await flow.async_step_detection_summary()

        placeholders = result["description_placeholders"]
        self.assertIn("Partial support", placeholders["tier_headline"])
        self.assertIn("learning", placeholders["tier_details"])

    async def test_detection_summary_collector_only_does_not_suggest_learning(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="udp",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="ESP32COLLECTOR"),
            ),
            connection_mode="known_ip",
            next_action="create_pending_entry",
        )

        result = await flow.async_step_detection_summary()

        placeholders = result["description_placeholders"]
        self.assertIn("Device not recognized", placeholders["tier_headline"])
        self.assertIn("no inverter was detected", placeholders["tier_details"])
        self.assertIn("Support Archive", placeholders["tier_details"])
        self.assertNotIn("Add controls", placeholders["tier_details"])
        self.assertNotIn("device learning", placeholders["tier_details"])

    async def test_detection_summary_without_catalog_details_uses_driver_text(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._result_with_catalog_details(None)

        result = await flow.async_step_detection_summary()

        placeholders = result["description_placeholders"]
        self.assertIn("driver", placeholders["tier_headline"].lower())

    async def test_detection_summary_submit_continues_to_confirm(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._result_with_catalog_details(
            {"kind": "device", "tier": "full"}
        )

        result = await flow.async_step_detection_summary({})

        self.assertEqual(result["step_id"], "confirm")

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
                serial_number="92632500000001",
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

        interval_form = await flow.async_step_confirm({"poll_mode": "manual"})
        self.assertEqual(interval_form["step_id"], "confirm_poll_interval")
        result = await flow.async_step_confirm_poll_interval({"poll_interval": 15})

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
                serial_number="92632500000001",
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
        self.assertEqual(
            result["options"][CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY],
            "smartess_at",
        )
        self.assertEqual(
            result["options"][CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE],
            "config_flow_pre_bind",
        )
        self.assertTrue(result["options"][CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT])

    def _bridge_confirm_result(self, *, is_bridge: bool) -> OnboardingResult:
        details = {"collector_virtual_bridge": True} if is_bridge else {}
        return OnboardingResult(
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
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                details=details,
            ),
            connection_mode="known_ip",
        )

    def _bridge_confirm_result_from_collector_info(self) -> OnboardingResult:
        result = self._bridge_confirm_result(is_bridge=False)
        result.collector.collector.collector_virtual_bridge = True
        result.collector.collector.collector_bridge_kind = "esp-collector"
        return result

    def _bridge_confirm_result_from_hardware_token(self) -> OnboardingResult:
        result = self._bridge_confirm_result(is_bridge=False)
        result.match.details["collector_hardware_version"] = "esp-collector/0.1.2/ESP32"
        result.match.details["collector_virtual_bridge"] = True
        result.match.details["collector_bridge_kind"] = "esp-collector"
        result.match.details["collector_bridge_version"] = "0.1.2"
        return result

    def _collector_only_bridge_result(self) -> OnboardingResult:
        return OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="udp",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="ESP32COLLECTOR",
                    collector_virtual_bridge=True,
                    collector_bridge_kind="esp-collector",
                    collector_bridge_version="dev",
                ),
            ),
            connection_mode="known_ip",
            next_action="create_pending_entry",
        )

    async def test_confirm_step_hides_operation_mode_selector_for_detected_bridge(self) -> None:
        # Item 1: a detected bridge forces HA-only and hides the SmartESS+HA /
        # HA-only choice, showing an informational note instead.
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result(is_bridge=True)

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertIn("poll_mode", result["data_schema"].schema)
        self.assertNotIn("poll_interval", result["data_schema"].schema)
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertTrue(
            result["description_placeholders"]["collector_operation_mode_note"].strip()
        )

    async def test_confirm_step_hides_operation_mode_selector_for_bridge_collector_info(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result_from_collector_info()

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)

    async def test_confirm_step_hides_operation_mode_selector_for_hardware_token_bridge(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result_from_hardware_token()

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)

    async def test_confirm_step_hides_operation_mode_selector_for_collector_only_bridge(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._collector_only_bridge_result()

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "confirm")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertTrue(
            result["description_placeholders"]["collector_operation_mode_note"].strip()
        )

    async def test_confirm_step_refreshes_collector_only_bridge_capability_from_hardware_token(self) -> None:
        flow = self._make_flow()
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="udp",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="ESP32COLLECTOR"),
            ),
            connection_mode="known_ip",
            next_action="create_pending_entry",
        )
        flow._auto_config = {
            "server_ip": "192.168.1.50",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }
        flow._autodetect_results = {"0": result}
        flow._selected_result = result

        class _FakeTransport:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs
                self.started = False

            async def start(self) -> None:
                self.started = True

            async def stop(self) -> None:
                self.started = False

        with (
            patch(
                "custom_components.eybond_local.config_flow.SharedEybondTransport",
                _FakeTransport,
            ),
            patch(
                "custom_components.eybond_local.config_flow.SharedCollectorAtTransport",
                _FakeTransport,
            ),
            patch(
                "custom_components.eybond_local.config_flow.query_runtime_collector_values",
                new=AsyncMock(
                    return_value={
                        "collector_hardware_version": "esp-collector/0.1.2/ESP32",
                        "collector_server_endpoint": "192.168.1.50,8899,TCP",
                    }
                ),
            ),
            patch(
                "custom_components.eybond_local.config_flow.query_runtime_collector_at_values",
                new=AsyncMock(
                    return_value={}
                ),
            ),
        ):
            form = await flow.async_step_confirm()

        self.assertEqual(form["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, form["data_schema"].schema)
        assert flow._selected_result is not None
        assert flow._selected_result.collector is not None
        assert flow._selected_result.collector.collector is not None
        self.assertTrue(flow._selected_result.collector.collector.collector_virtual_bridge)

    async def test_confirm_step_refreshes_bridge_capability_for_merged_auto_result(self) -> None:
        flow = self._make_flow()
        scanned_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="udp",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="ESP32COLLECTOR"),
            ),
            connection_mode="known_ip",
            next_action="create_pending_entry",
        )
        selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="udp",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="ESP32COLLECTOR"),
            ),
            connection_mode="known_ip",
            next_action="create_pending_entry",
        )
        flow._auto_config = {
            "server_ip": "192.168.1.50",
            "tcp_port": 8899,
            "udp_port": 58899,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }
        flow._autodetect_results = {"0": scanned_result}
        flow._selected_result = selected_result

        class _FakeTransport:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

        with (
            patch(
                "custom_components.eybond_local.config_flow.SharedEybondTransport",
                _FakeTransport,
            ),
            patch(
                "custom_components.eybond_local.config_flow.SharedCollectorAtTransport",
                _FakeTransport,
            ),
            patch(
                "custom_components.eybond_local.config_flow.query_runtime_collector_values",
                new=AsyncMock(
                    return_value={
                        "collector_hardware_version": "esp-collector/0.1.2/ESP32",
                    }
                ),
            ),
            patch(
                "custom_components.eybond_local.config_flow.query_runtime_collector_at_values",
                new=AsyncMock(return_value={}),
            ),
        ):
            form = await flow.async_step_confirm()

        self.assertEqual(form["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, form["data_schema"].schema)
        assert flow._selected_result is not None
        assert flow._selected_result.collector is not None
        assert flow._selected_result.collector.collector is not None
        self.assertTrue(flow._selected_result.collector.collector.collector_virtual_bridge)

    async def test_confirm_step_persists_ha_only_for_collector_only_bridge(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._collector_only_bridge_result()
        flow._collector_endpoint_bind_applied = True

        result = await flow.async_step_confirm({"poll_mode": "auto"})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        self.assertEqual(
            result["options"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        self.assertTrue(result["data"]["collector_virtual_bridge"])

    async def test_confirm_step_hides_operation_mode_selector_for_factory_collector(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result(is_bridge=False)

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertEqual(
            result["description_placeholders"]["collector_operation_mode_note"], ""
        )

    async def test_confirm_step_bridge_refused_endpoint_write_does_not_hard_fail(self) -> None:
        # Older bridge firmware may refuse the FC=3 param-21 endpoint write.
        # For a detected bridge that refusal remains non-fatal — the flow forces
        # HA-only and creates the entry instead of surfacing a hard error.
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result(is_bridge=True)

        transport = AsyncMock()
        session = AsyncMock()

        async def set_collector(parameter: int, value: str):
            # The bridge refuses the endpoint write with a non-zero status.
            status = 1 if parameter == SET_SERVER_ENDPOINT else 0
            return type("_SetResponse", (), {"status": status, "parameter": parameter})()

        session.set_collector.side_effect = set_collector

        async def with_session():
            return transport, session

        # The current endpoint differs from the HA target, so the explicit write
        # path runs (skipping the early current==target return). The bind resets
        # cached endpoint state first, so the differing value must come from the
        # read, not a pre-set attribute.
        async def read_endpoint():
            flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"
            return "collector-cloud.smartess.example,18899,TCP"

        flow._async_with_selected_collector_session = with_session
        flow._async_read_selected_collector_server_endpoint = read_endpoint

        result = await flow.async_step_confirm(
            {
                CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_HA_ONLY,
                "poll_interval": 15,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE], COLLECTOR_OPERATION_HA_ONLY
        )
        # The reboot/apply write must NOT run after a refused endpoint write.
        applied_parameters = [
            call.args[0] for call in session.set_collector.await_args_list
        ]
        self.assertEqual(applied_parameters, [SET_SERVER_ENDPOINT])

    async def test_confirm_step_bridge_successful_endpoint_write_is_applied(self) -> None:
        # Current bridge firmware accepts and persists the FC=3 param-21 endpoint
        # write, followed by the standard FC=3 param-29 apply command.
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result(is_bridge=True)

        transport = AsyncMock()
        session = AsyncMock()

        async def set_collector(parameter: int, value: str):
            return type("_SetResponse", (), {"status": 0, "parameter": parameter})()

        async def query_collector(parameter: int):
            text = "192.168.1.50,18899,TCP" if parameter == SET_SERVER_ENDPOINT else "0"
            return type("_QueryResponse", (), {"code": 0, "parameter": parameter, "text": text})()

        session.set_collector.side_effect = set_collector
        session.query_collector.side_effect = query_collector

        async def with_session():
            return transport, session

        async def read_endpoint():
            flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"
            return "collector-cloud.smartess.example,18899,TCP"

        flow._async_with_selected_collector_session = with_session
        flow._async_read_selected_collector_server_endpoint = read_endpoint

        result = await flow.async_step_confirm(
            {
                CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_HA_ONLY,
                "poll_interval": 15,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE], COLLECTOR_OPERATION_HA_ONLY
        )
        self.assertEqual(
            [call.args[0] for call in session.set_collector.await_args_list],
            [SET_SERVER_ENDPOINT, SET_REBOOT_OR_APPLY],
        )

    async def test_confirm_step_ignores_stale_operation_mode_for_factory_collector(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._bridge_confirm_result(is_bridge=False)

        transport = AsyncMock()
        session = AsyncMock()

        async def set_collector(parameter: int, value: str):
            status = 1 if parameter == SET_SERVER_ENDPOINT else 0
            return type("_SetResponse", (), {"status": status, "parameter": parameter})()

        session.set_collector.side_effect = set_collector

        async def with_session():
            return transport, session

        async def read_endpoint():
            flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"
            return "collector-cloud.smartess.example,18899,TCP"

        flow._async_with_selected_collector_session = with_session
        flow._async_read_selected_collector_server_endpoint = read_endpoint

        result = await flow.async_step_confirm(
            {
                CONF_COLLECTOR_OPERATION_MODE: COLLECTOR_OPERATION_HA_ONLY,
                "poll_interval": 15,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_SMARTESS_AND_HA,
        )
        session.set_collector.assert_not_awaited()

    async def test_collector_callback_target_uses_listener_port_not_cloud_port(self) -> None:
        # The HA-only callback target must point at OUR listener port. The
        # collector's cloud endpoint port (18899) is the vendor cloud /
        # proxy-capture port: mirroring it aimed collectors at the proxy
        # listener while the runtime announcer advertised the real one, and
        # the two endpoints then fought on every reconnect.
        flow = self._make_flow()
        flow._collector_current_server_endpoint = "collector-cloud.smartess.example,18899,TCP"

        self.assertEqual(
            flow._collector_callback_target_endpoint(),
            "192.168.1.50,8899,TCP",
        )

    async def test_collector_callback_target_preserves_host_only_shape(self) -> None:
        flow = self._make_flow()
        flow._collector_current_server_endpoint = "ess.eybond.com"

        self.assertEqual(
            flow._collector_callback_target_endpoint(),
            "192.168.1.50",
        )

    async def test_collector_callback_target_uses_listener_port_for_valuecloud_shape(self) -> None:
        flow = self._make_flow()
        flow._collector_current_server_endpoint = "iot.eybond.com,18899,TCP"

        self.assertEqual(
            flow._collector_callback_target_endpoint(),
            "192.168.1.50,8899,TCP",
        )

    async def test_collector_original_endpoint_options_use_valuecloud_host_profile_before_18899_port_fallback(self) -> None:
        flow = self._make_flow()

        options = flow._collector_original_endpoint_options(
            "iot.eybond.com,18899,TCP"
        )

        self.assertEqual(
            options["collector_original_server_endpoint_profile_key"],
            "valuecloud_at",
        )

    async def test_endpoint_originality_hint_uses_catalog_host_match(self) -> None:
        flow = self._make_flow()

        hint = flow._endpoint_originality_hint("dtu_ess.eybond.com")

        self.assertIn("original cloud endpoint", hint)

    async def test_endpoint_originality_hint_uses_valuecloud_host_match_before_18899_port_fallback(self) -> None:
        flow = self._make_flow()

        hint = flow._endpoint_originality_hint("iot.eybond.com,18899,TCP")

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
                "collector_pn": "E5000020000000",
                "detected_serial": "92632500000001",
            }
        )
        existing.unique_id = "collector:E5000020000000"
        flow = self._make_flow(entries=[existing])

        matched_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000020000000"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
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

    async def test_do_scan_collapses_prefix_and_full_collector_pn_duplicates(self) -> None:
        flow = self._make_flow()
        matched_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="Q0000000000001"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=1),
            ),
            connection_mode="broadcast",
        )
        collector_only_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="Q00000000000010001"),
            ),
            connection_mode="broadcast",
            next_action="manual_driver_selection",
            last_error="collector_detected_without_driver",
        )

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                return (collector_only_result, matched_result)

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_do_scan()

        self.assertEqual(len(flow._autodetect_results), 1)
        result = next(iter(flow._autodetect_results.values()))
        self.assertIsNotNone(result.match)
        self.assertEqual(result.match.model_name, "SMG 6200")

    async def test_existing_entry_does_not_claim_different_collector_pn_on_same_nat_ip(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=8899)
        existing.data.update(
            {
                "collector_ip": "192.168.1.193",
                "collector_pn": "E5000099990001",
            }
        )
        existing.unique_id = "collector:E5000099990001"
        flow = self._make_flow(entries=[existing])
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.193",
                connected=True,
                collector=CollectorInfo(collector_pn="E5000099990002"),
            ),
            connection_mode="broadcast",
        )

        self.assertIsNone(flow._existing_entry_for_result(result))

    async def test_existing_entry_matches_prefix_and_full_collector_pn(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=8899)
        existing.data.update({"collector_pn": "Q00000000000010001"})
        existing.unique_id = "collector:Q00000000000010001"
        flow = self._make_flow(entries=[existing])
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.51",
                connected=True,
                collector=CollectorInfo(collector_pn="Q0000000000001"),
            ),
            connection_mode="broadcast",
        )

        self.assertIs(flow._existing_entry_for_result(result), existing)

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
                collector=CollectorInfo(collector_pn="E5000020000000"),
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
                collector=CollectorInfo(collector_pn="E5000020000000"),
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
        self.assertEqual(
            labels,
            [
                "Авто",
                "SMG / Modbus",
                "SRNE / Modbus",
                "MUST PV/PH18",
                "Каталог пристроїв / Modbus (Aohai FSA…)",
                "PI30",
                "EyeBond G-ASCII",
                "SmartESS 0925 / Modbus",
                "PI18",
            ],
        )

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
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                confidence="high",
            ),
            connection_mode="manual",
        )

        result = await flow.async_step_manual_create_pending()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["control_mode"], "auto")
        self.assertEqual(result["data"]["detection_confidence"], "high")

    async def test_manual_high_confidence_routes_via_detection_summary(self) -> None:
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
        probe_result = OnboardingResult(
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
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                confidence="high",
                details={
                    "device_catalog": {
                        "kind": "device",
                        "tier": "full",
                        "entry_key": "smg_6200",
                    }
                },
            ),
            connection_mode="manual",
        )

        async def _fake_probe(values):
            return probe_result

        with patch.object(flow, "_async_probe_manual_target", side_effect=_fake_probe):
            result = await flow.async_step_manual_probe_again()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "detection_summary")
        placeholders = result["description_placeholders"]
        self.assertEqual(placeholders["model"], "SMG 6200")
        self.assertIn("Full support", placeholders["tier_headline"])

        created = await flow.async_step_detection_summary({})

        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["device_catalog_kind"], "device")
        self.assertEqual(created["data"]["device_catalog_tier"], "full")
        self.assertEqual(created["data"]["device_catalog_entry_key"], "smg_6200")

    async def test_auto_entry_persists_device_catalog_metadata(self) -> None:
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
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55", source="udp", ip="192.168.1.55", connected=True
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG family 4200 variant",
                serial_number="15573400000004",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x01, device_addr=1),
                confidence="medium",
                details={
                    "device_catalog": {
                        "kind": "family",
                        "tier": "partial",
                    }
                },
            ),
            connection_mode="known_ip",
        )

        result = await flow._async_create_entry_from_result({"poll_interval": 30})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["device_catalog_kind"], "family")
        self.assertEqual(result["data"]["device_catalog_tier"], "partial")
        self.assertEqual(result["data"]["device_catalog_entry_key"], "")

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
                        collector_pn="E5000020000000",
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
                                    "pn": "E50000200000000001",
                                    "sn": "E50000200000000001000001",
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
                collector_pn="E5000020000000",
                pn="E50000200000000001",
                sn="E50000200000000001000001",
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

            flow._smartess_cloud_assist_mode = "auto"
            with patch(
                "custom_components.eybond_local.config_flow.fetch_and_export_smartess_device_bundle_cloud_evidence",
                return_value=CloudEvidenceRecord(
                    path=Path("/config/eybond_local/cloud_evidence/onboarding.json"),
                    payload=evidence,
                ),
            ):
                assist_result = await flow.async_step_smartess_cloud_assist(
                    {"username": "test-user", "password": "secret"}
                )

            self.assertEqual(assist_result["type"], "menu")
            self.assertEqual(assist_result["step_id"], "smartess_cloud_assist_summary")
            self.assertEqual(assist_result["menu_options"], ["confirm"])

            placeholders = assist_result["description_placeholders"]
            self.assertIn("SmartESS 0925", placeholders["smartess_cloud_mapping_table"])
            self.assertIn("E50000200000000001", placeholders["smartess_cloud_identity_table"])
            self.assertIn("Garage inverter", placeholders["smartess_cloud_identity_table"])
            self.assertIn("bc_ (1)", placeholders["smartess_cloud_detail_summary"])
            self.assertIn("39", placeholders["smartess_cloud_settings_table"])
            self.assertIn("Output priority", placeholders["smartess_cloud_highlights_table"])
            self.assertIn("SBU", placeholders["smartess_cloud_highlights_table"])
            self.assertIn("reg 4537", placeholders["smartess_cloud_highlights_table"])

            created = await flow.async_step_confirm({"poll_mode": "auto"})

            self.assertEqual(created["type"], "create_entry")
            self.assertEqual(created["data"][CONF_SMARTESS_PROTOCOL_ASSET_ID], "0925")
            self.assertEqual(created["data"][CONF_SMARTESS_PROFILE_KEY], "smartess_0925")
            self.assertEqual(created["data"][CONF_DRIVER_HINT], "pi30")

    async def test_scan_results_placeholders_use_localized_select_hint(self) -> None:
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

        self.assertIn("Выберите в списке ниже инвертор", placeholders["scan_next_hint"])
        self.assertNotIn("Pick the inverter", placeholders["scan_next_hint"])

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
        self.assertIn("Ввести адресу вручну", placeholders["scan_next_hint"])
        self.assertNotIn("Запустити глибоке сканування", placeholders["scan_next_hint"])
        self.assertNotIn("Refresh scan", placeholders["scan_next_hint"])
        self.assertNotIn("Enter address manually", placeholders["scan_next_hint"])

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
        self.assertIn("сохранить его как ожидающее", placeholders["scan_next_hint"])
        self.assertIn("Есть признаки SmartESS", result_label)

    async def test_options_runtime_step_renders_branch_aware_connection_section(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "runtime")

    async def test_options_init_menu_exposes_collector_wifi(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_init()

        self.assertEqual(
            result["menu_options"],
            ["runtime", "shadow_learning", "collector_wifi", "diagnostics"],
        )

    async def test_options_init_menu_hides_shadow_learning_for_virtual_bridge(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )

        result = await options.async_step_init()

        self.assertEqual(
            result["menu_options"],
            ["runtime", "collector_wifi", "collector_uart", "diagnostics"],
        )
        self.assertNotIn("shadow_learning", result["menu_options"])
        self.assertIn("collector_uart", result["menu_options"])
        self.assertTrue(
            result["description_placeholders"]["bridge_note"].strip()
        )

    async def test_options_init_menu_keeps_shadow_learning_for_factory_collector(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=False),
                values={},
            ),
        )

        result = await options.async_step_init()

        self.assertIn("shadow_learning", result["menu_options"])
        self.assertNotIn("collector_uart", result["menu_options"])
        self.assertEqual(result["description_placeholders"]["bridge_note"], "")

    async def test_options_collector_wifi_step_renders_current_status(self) -> None:
        options = self._make_options_flow()

        async def refresh_status() -> None:
            options._collector_wifi_current_ssid = "HomeNet"
            options._collector_wifi_network_diagnostics = "1,0,0"
            options._collector_wifi_networks = (
                SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),
                SmartEssBleWifiNetwork(ssid="Other", signal=42),
            )

        options._async_refresh_collector_wifi_status = refresh_status

        result = await options.async_step_collector_wifi()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "collector_wifi")
        self.assertEqual(result["description_placeholders"]["current_ssid"], "HomeNet")
        self.assertEqual(result["description_placeholders"]["status_updates"], "")
        self.assertNotIn("network_diagnostics", result["description_placeholders"])
        self.assertIn(CONF_WIFI_SSID, result["data_schema"].schema)
        self.assertIn(CONF_WIFI_PASSWORD, result["data_schema"].schema)
        self.assertIn(CONF_COLLECTOR_WIFI_ACTION, result["data_schema"].schema)
        self.assertIn(CONF_CONFIRM_COLLECTOR_WIFI_APPLY, result["data_schema"].schema)

    async def test_options_collector_wifi_step_shows_only_non_empty_status_updates(self) -> None:
        options = self._make_options_flow()
        options._collector_wifi_current_ssid = "HomeNet"
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
            options._collector_wifi_current_ssid = "HomeNet"

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

    async def test_options_collector_uart_step_renders_current_status_for_bridge(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )

        async def refresh_status() -> None:
            options._collector_uart_current_settings = "2400"
            options._collector_uart_current_baudrate = "2400"

        options._async_refresh_collector_uart_status = refresh_status

        result = await options.async_step_collector_uart()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "collector_uart")
        self.assertEqual(result["description_placeholders"]["current_uart"], "2400")
        self.assertIn(CONF_COLLECTOR_UART_BAUDRATE, result["data_schema"].schema)
        self.assertIn(CONF_COLLECTOR_UART_ACTION, result["data_schema"].schema)
        self.assertIn(CONF_CONFIRM_COLLECTOR_UART_APPLY, result["data_schema"].schema)

    async def test_options_collector_uart_refresh_reads_parameter_34(self) -> None:
        options = self._make_options_flow()
        transport = AsyncMock()
        session = AsyncMock()
        reads: list[int] = []

        async def query_collector(parameter: int):
            reads.append(parameter)
            return type(
                "_QueryResponse",
                (),
                {
                    "code": 0,
                    "parameter": parameter,
                    "text": "ESP32" if parameter == QUERY_HARDWARE_VERSION else "9600",
                    "data": b"ESP32" if parameter == QUERY_HARDWARE_VERSION else b"9600",
                },
            )()

        async def with_session():
            return transport, session

        session.query_collector.side_effect = query_collector
        options._async_with_options_collector_session = with_session

        await options._async_refresh_collector_uart_status()

        self.assertEqual(reads, [QUERY_HARDWARE_VERSION, QUERY_SERIAL_BAUDRATE])
        self.assertEqual(options._collector_uart_hardware_version, "ESP32")
        self.assertEqual(options._collector_uart_current_baudrate, "9600")
        self.assertEqual(options._collector_uart_current_settings, "9600")
        transport.stop.assert_awaited_once()

    async def test_options_collector_uart_step_blocks_runtime_change_for_bk72xx(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )

        async def refresh_status() -> None:
            options._collector_uart_hardware_version = "BK72xx/RTL87xx"
            options._collector_uart_current_settings = "2400"
            options._collector_uart_current_baudrate = "2400"

        options._async_refresh_collector_uart_status = refresh_status

        result = await options.async_step_collector_uart()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "collector_uart")
        self.assertEqual(result["description_placeholders"]["hardware_version"], "BK72xx/RTL87xx")
        self.assertIn("BK72xx", result["description_placeholders"]["runtime_unavailable_note"])
        self.assertIn(CONF_COLLECTOR_UART_ACTION, result["data_schema"].schema)
        self.assertNotIn(CONF_COLLECTOR_UART_BAUDRATE, result["data_schema"].schema)
        self.assertNotIn(CONF_CONFIRM_COLLECTOR_UART_APPLY, result["data_schema"].schema)

    async def test_options_collector_uart_apply_writes_parameter_34_only(self) -> None:
        options = self._make_options_flow()
        snapshot = types.SimpleNamespace(
            values={
                "collector_virtual_bridge": True,
                "collector_serial_baudrate": "2400,8,1,NONE",
            }
        )
        coordinator = types.SimpleNamespace(
            data=snapshot,
            invalidate_collector_runtime_values=Mock(),
            async_request_refresh=AsyncMock(),
        )
        options._config_entry.runtime_data = coordinator
        transport = AsyncMock()
        session = AsyncMock()
        writes: list[tuple[int, str]] = []

        async def set_collector(parameter: int, value: str):
            writes.append((parameter, value))
            return type("_SetResponse", (), {"status": 0, "parameter": parameter})()

        async def with_session():
            return transport, session

        session.set_collector.side_effect = set_collector
        options._async_with_options_collector_session = with_session

        await options._async_apply_collector_uart_baudrate("9600")

        self.assertEqual(writes, [(SET_SERIAL_BAUDRATE, "9600")])
        self.assertEqual(snapshot.values["collector_serial_baudrate"], "2400,8,1,NONE")
        coordinator.invalidate_collector_runtime_values.assert_called_once_with()
        coordinator.async_request_refresh.assert_awaited_once_with()
        transport.stop.assert_awaited_once()

    async def test_options_collector_uart_apply_refuses_bk72xx_runtime_change(self) -> None:
        options = self._make_options_flow()
        options._collector_uart_hardware_version = "BK72xx/RTL87xx"
        options._async_with_options_collector_session = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "collector_uart_runtime_unavailable"):
            await options._async_apply_collector_uart_baudrate("9600")

        options._async_with_options_collector_session.assert_not_called()

    async def test_options_collector_uart_apply_requires_confirmation(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )
        options._async_apply_collector_uart_baudrate = AsyncMock()

        result = await options.async_step_collector_uart(
            {
                CONF_COLLECTOR_UART_ACTION: COLLECTOR_UART_ACTION_APPLY,
                CONF_COLLECTOR_UART_BAUDRATE: "9600",
            }
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(
            result["errors"],
            {CONF_CONFIRM_COLLECTOR_UART_APPLY: "collector_uart_apply_not_confirmed"},
        )
        options._async_apply_collector_uart_baudrate.assert_not_called()

    async def test_options_collector_uart_apply_preserves_existing_options(self) -> None:
        options = self._make_options_flow()
        options._config_entry.options = {"poll_interval": 15}
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )
        options._async_apply_collector_uart_baudrate = AsyncMock()

        result = await options.async_step_collector_uart(
            {
                CONF_COLLECTOR_UART_ACTION: COLLECTOR_UART_ACTION_APPLY,
                CONF_COLLECTOR_UART_BAUDRATE: "9600",
                CONF_CONFIRM_COLLECTOR_UART_APPLY: True,
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"], {"poll_interval": 15})
        options._async_apply_collector_uart_baudrate.assert_awaited_once_with("9600")

    async def test_options_collector_uart_step_returns_init_for_factory_collector(self) -> None:
        options = self._make_options_flow()

        result = await options.async_step_collector_uart()

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "init")
        self.assertNotIn("collector_uart", result["menu_options"])

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

        form = await options.async_step_runtime()

        self.assertIn("poll_mode", form["data_schema"].schema)
        self.assertIn("poll_interval", form["data_schema"].schema)

        result = await options.async_step_runtime(
            {
                "poll_mode": "manual",
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
        self.assertEqual(result["data"]["poll_mode"], "manual")
        self.assertEqual(result["data"]["poll_interval"], 15)
        self.assertEqual(result["data"]["control_mode"], "full")
        self.assertEqual(result["data"]["advertised_server_ip"], "203.0.113.10")
        self.assertEqual(result["data"]["advertised_tcp_port"], 9443)
        self.assertEqual(result["data"]["driver_hint"], "modbus_smg")
        self.assertNotIn("connection", result["data"])

    async def test_options_runtime_auto_mode_hides_poll_interval_and_preserves_fallback(self) -> None:
        options = self._make_options_flow()
        options._config_entry.options = {"poll_interval": 15, "poll_mode": "auto"}

        form = await options.async_step_runtime()

        self.assertIn("poll_mode", form["data_schema"].schema)
        self.assertNotIn("poll_interval", form["data_schema"].schema)

        result = await options.async_step_runtime(
            {
                "poll_mode": "auto",
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
        self.assertEqual(result["data"]["poll_mode"], "auto")
        self.assertEqual(result["data"]["poll_interval"], 15)

    async def test_options_runtime_switching_auto_to_manual_requests_interval(self) -> None:
        options = self._make_options_flow()
        options._config_entry.options = {"poll_interval": 15, "poll_mode": "auto"}

        result = await options.async_step_runtime(
            {
                "poll_mode": "manual",
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

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "runtime_poll_interval")
        self.assertIn("poll_interval", result["data_schema"].schema)

        created = await options.async_step_runtime_poll_interval({"poll_interval": 20})

        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["poll_mode"], "manual")
        self.assertEqual(created["data"]["poll_interval"], 20)

    async def test_diagnostics_menu_exposes_reload_and_capture_actions(self) -> None:
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
        # The diagnostic command runner is gated behind Home Assistant Advanced
        # Mode (off by default), so it is not in the standard diagnostics menu.
        self.assertEqual(
            result["menu_options"],
            [
                "create_support_package",
                "reload_local_metadata",
                "proxy_capture",
            ],
        )
        self.assertNotIn("diagnostic_commands", result["menu_options"])
        self.assertEqual(
            result["description_placeholders"]["support_archive_action_label"],
            "Create support archive",
        )
        self.assertEqual(
            options._tr("options.step.diagnostics.menu_options.reload_local_metadata", ""),
            "Reload local metadata",
        )
        self.assertNotIn("advanced_metadata", result["menu_options"])

    async def test_diagnostics_menu_shows_command_runner_in_advanced_mode(self) -> None:
        options = self._make_options_flow()
        options.show_advanced_options = True
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
        self.assertIn("diagnostic_commands", result["menu_options"])

    async def test_diagnostic_commands_step_runs_and_displays_result(self) -> None:
        options = self._make_options_flow()
        calls: list[dict[str, object]] = []

        async def _run_diagnostic_commands(**kwargs):
            calls.append(dict(kwargs))
            return {
                "success": True,
                "output": "[1] read 171\nstatus: ok\ndecimal: 8960\n",
                "results": [],
                "context": {},
                "started_at": "2026-06-19T00:00:00+00:00",
                "finished_at": "2026-06-19T00:00:01+00:00",
                "result_path": "/config/eybond_local/diagnostic_runs/result.json",
                "download_url": "/local/eybond_local/diagnostic_runs/result.share.json",
            }

        options._config_entry.runtime_data = types.SimpleNamespace(
            async_run_diagnostic_commands=_run_diagnostic_commands,
        )

        initial = await options.async_step_diagnostic_commands()
        self.assertEqual(initial["type"], "form")
        self.assertEqual(initial["step_id"], "diagnostic_commands")
        commands_selector = initial["data_schema"].schema["diagnostic_commands"]
        self.assertTrue(commands_selector.config.kwargs.get("multiline"))
        self.assertNotIn("diagnostic_result", initial["data_schema"].schema)

        result = await options.async_step_diagnostic_commands(
            {
                "diagnostic_commands": "driver modbus_smg\nread 171\n",
                "diagnostic_stop_on_error": False,
                "diagnostic_publish_download_copy": True,
            }
        )

        self.assertEqual(
            calls,
            [
                {
                    "commands": "driver modbus_smg\nread 171\n",
                    "stop_on_error": False,
                    "confirm_write": False,
                    "publish_download_copy": True,
                }
            ],
        )
        self.assertEqual(result["type"], "form")
        self.assertIn("diagnostic_result", result["data_schema"].schema)
        result_selector = result["data_schema"].schema["diagnostic_result"]
        self.assertTrue(result_selector.config.kwargs.get("multiline"))
        self.assertTrue(result_selector.config.kwargs.get("read_only"))
        self.assertIn(
            "/local/eybond_local/diagnostic_runs/result.share.json",
            result["description_placeholders"]["diagnostic_download_markdown"],
        )

    async def test_diagnostic_commands_step_requires_commands(self) -> None:
        options = self._make_options_flow()
        result = await options.async_step_diagnostic_commands(
            {
                "diagnostic_commands": " \n",
                "diagnostic_stop_on_error": True,
            }
        )

        self.assertEqual(
            result["errors"],
            {"diagnostic_commands": "diagnostic_commands_required"},
        )

    async def test_diagnostics_menu_omits_proxy_capture_for_detected_bridge(self) -> None:
        # Item 3: a detected bridge has no SmartESS cloud side, so proxy capture
        # (which has nothing to capture) is omitted from the diagnostics menu.
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            menu_options = options._diagnostics_menu_options("create_support_package")

        self.assertNotIn("proxy_capture", menu_options)
        self.assertIn("create_support_package", menu_options)

    async def test_diagnostics_menu_keeps_proxy_capture_for_factory_collector(self) -> None:
        # Item 3 fail-safe: a factory collector keeps proxy capture.
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=False),
                values={},
            ),
        )

        with tempfile.TemporaryDirectory() as tempdir:
            options.hass.config.config_dir = tempdir
            menu_options = options._diagnostics_menu_options("create_support_package")

        self.assertIn("proxy_capture", menu_options)

    async def test_options_runtime_step_hides_operation_mode_selector_for_bridge(self) -> None:
        # Item 1: the runtime options flow forces/hides the collector operation
        # mode for a detected bridge, exactly like the onboarding confirm step.
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertTrue(
            result["description_placeholders"]["collector_operation_mode_note"].strip()
        )

    async def test_options_runtime_step_hides_operation_mode_selector_for_bridge_entry_data(self) -> None:
        options = self._make_options_flow()
        options._config_entry.data = {
            **dict(options._config_entry.data),
            "collector_virtual_bridge": True,
        }
        options._config_entry.runtime_data = None

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertTrue(
            result["description_placeholders"]["collector_operation_mode_note"].strip()
        )

    async def test_options_runtime_step_keeps_operation_mode_selector_for_factory(self) -> None:
        # Item 1 fail-safe: a factory collector keeps the selector and empty note.
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=False),
                values={},
            ),
        )

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertEqual(
            result["description_placeholders"]["collector_operation_mode_note"], ""
        )

    async def test_options_runtime_step_forces_ha_only_for_bridge_on_submit(self) -> None:
        # Item 1: submitting the bridge runtime form (no operation-mode field)
        # still persists HA-only.
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )

        result = await options.async_step_runtime(
            {
                "poll_interval": 15,
                "control_mode": "auto",
                "connection": {
                    "server_ip": "192.168.1.50",
                    "collector_ip": "192.168.1.55",
                    "tcp_port": 8899,
                    "udp_port": 58899,
                    "discovery_target": "192.168.1.255",
                    "discovery_interval": 3,
                    "heartbeat_interval": 60,
                    "driver_hint": "auto",
                },
            }
        )

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE], COLLECTOR_OPERATION_HA_ONLY
        )

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
                        "proxy_trace_saved_result_download_url": "http://203.0.113.7:8123/local/eybond_local/proxy_traces/session.zip",
                    }
                ),
            )

            result = await options.async_step_proxy_capture()

        self.assertIn(
            "](http://203.0.113.7:8123/local/eybond_local/proxy_traces/session.zip)",
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
            smartess_collector_pn="E5000020000000",
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
                "username": " test-user ",
                "password": " pw-test-0000 ",
            }
        )

        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertIn(
            'href="http://192.168.1.50:8123/local/eybond_local/support/support_archive.zip"',
            result["description_placeholders"]["download_markdown"],
        )
        self.assertIn(
            'target="_blank"',
            result["description_placeholders"]["download_markdown"],
        )
        self.assertIn(
            "download",
            result["description_placeholders"]["download_markdown"],
        )
        self.assertNotIn(
            "\n\n`",
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
        self.assertIn("proxy_capture", result["menu_options"])
        self.assertNotIn("advanced_metadata", result["menu_options"])

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

    async def test_create_support_package_shows_guided_form_with_saved_cloud_evidence(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            smartess_cloud_export_available=True,
            smartess_cloud_evidence_path="/config/eybond_local/cloud_evidence/entry123.json",
            smartess_collector_pn="E5000020000000",
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

    async def test_create_support_package_shows_refresh_for_valuecloud_evidence(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            cloud_evidence_export_available=True,
            smartess_cloud_export_available=False,
            smartess_cloud_evidence_path="",
            smartess_collector_pn="A0000000000001",
            data=types.SimpleNamespace(values={"collector_cloud_family": "valuecloud_at"}),
        )

        result = await options.async_step_create_support_package()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "create_support_package")
        selector = result["data_schema"].schema[CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE]
        option_values = [
            option["value"]
            for option in selector.config.kwargs["options"]
        ]
        self.assertEqual(
            option_values,
            [
                SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
            ],
        )

    async def test_create_support_package_refresh_requires_credentials(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            smartess_cloud_export_available=True,
            smartess_collector_pn="E5000020000000",
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
            smartess_collector_pn="E5000020000000",
            data=types.SimpleNamespace(
                values={
                    "support_package_download_url": "/api/diagnostics/support_archive.zip",
                }
            ),
        )

        result = await options.async_step_create_support_package(
            {
                CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                "username": " test-user ",
                "password": " pw-test-0000 ",
            }
        )

        self.assertEqual(captured["username"], "test-user")
        self.assertEqual(captured["password"], "pw-test-0000")
        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertEqual(
            result["description_placeholders"]["path"],
            "/config/support/support_archive.zip",
        )
        self.assertIn(
            "Fresh cloud evidence was fetched",
            result["description_placeholders"]["status"],
        )

    async def test_create_support_package_for_bridge_does_not_refresh_cloud_evidence(self) -> None:
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

        options._config_entry.data = {
            **dict(options._config_entry.data),
            "collector_virtual_bridge": True,
        }
        options._config_entry.runtime_data = types.SimpleNamespace(
            async_export_support_package_with_cloud_refresh=_export_support_package_with_cloud_refresh,
            smartess_cloud_export_available=True,
            smartess_cloud_evidence_path="",
            smartess_collector_pn="ESP32COLLECTOR",
            data=types.SimpleNamespace(
                values={
                    "collector_virtual_bridge": True,
                    "support_package_download_url": "/api/diagnostics/support_archive.zip",
                }
            ),
        )

        result = await options.async_step_create_support_package(
            {
                CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                "username": "should-not-be-used",
                "password": "should-not-be-used",
            }
        )

        self.assertEqual(result["step_id"], "diagnostics_result")
        self.assertEqual(captured["username"], "")
        self.assertEqual(captured["password"], "")
        self.assertIs(captured["wants_refresh"], False)
        self.assertIn(
            "No cloud evidence was included",
            result["description_placeholders"]["status"],
        )

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

    def test_shadow_learning_route_rejects_control_when_collector_off_proxy(self) -> None:
        # SAFETY-CRITICAL: control is allowed ONLY while the collector's main link is on our
        # proxy right now (collector_connected). A sticky "reached us once" signal
        # (collector_protocol_ingress) plus residual route activity must NOT grant control --
        # after a mid-scan revert the collector is back on the real server and a ctrlDevice
        # would reach the inverter (it turned off the user's output).
        options = object.__new__(config_flow_module.EybondLocalOptionsFlow)

        def _coordinator(collector_connected: bool) -> object:
            return types.SimpleNamespace(
                _runtime=types.SimpleNamespace(
                    shadow_learning_route_status=lambda: {
                        "running": True,
                        "collector_connected": collector_connected,
                        # Stale/sticky signals that must never override the live-socket check.
                        "collector_protocol_ingress": True,
                        "route_protocol_activity": True,
                        "upstream_connected": False,
                        "ready": False,
                        "upstream_error": "",
                    }
                )
            )

        # Reverted to the real server (no live collector socket) -> blocked despite stale flags.
        self.assertFalse(
            options._shadow_learning_route_accepts_control(_coordinator(False))
        )
        # Collector on our proxy but upstream down -> blocked for writes.
        self.assertFalse(
            options._shadow_learning_route_accepts_control(_coordinator(True))
        )

        self.assertTrue(
            options._shadow_learning_route_accepts_control(
                types.SimpleNamespace(
                    _runtime=types.SimpleNamespace(
                        shadow_learning_route_status=lambda: {
                            "running": True,
                            "collector_connected": True,
                            "collector_protocol_ingress": True,
                            "route_protocol_activity": True,
                            "upstream_connected": True,
                            "ready": True,
                            "upstream_error": "",
                        }
                    )
                )
            )
        )

    def test_shadow_learning_placeholders_prefer_runtime_session_state(self) -> None:
        options = self._make_options_flow()
        options._shadow_learning_state = {
            "session": {"status": "learning"},
        }
        options._config_entry.runtime_data = types.SimpleNamespace(
            _runtime=types.SimpleNamespace(
                shadow_learning_route_status=lambda: {
                    "running": False,
                    "collector_connected": False,
                    "upstream_connected": False,
                    "ready": False,
                    "upstream_error": "",
                }
            ),
            data=types.SimpleNamespace(values={}),
        )

        placeholders = options._shadow_learning_placeholders(options._coordinator())

        self.assertEqual(placeholders["shadow_learning_session_state"], "stopped")

    def test_shadow_learning_placeholders_surface_restore_failed_state(self) -> None:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            _runtime=types.SimpleNamespace(
                shadow_learning_route_status=lambda: {
                    "running": False,
                    "collector_connected": False,
                    "collector_protocol_ingress": False,
                    "upstream_connected": False,
                    "ready": False,
                    "upstream_error": "",
                }
            ),
            data=types.SimpleNamespace(values={"shadow_learning_session_status": "restore_failed"}),
        )

        placeholders = options._shadow_learning_placeholders(options._coordinator())

        self.assertEqual(placeholders["shadow_learning_session_state"], "restore_failed")

    def _wizard_options_flow(self) -> EybondLocalOptionsFlow:
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(values={}),
        )
        return options

    async def test_control_discovery_entry_shows_consent_not_action_dropdown(self) -> None:
        options = self._wizard_options_flow()

        result = await options.async_step_shadow_learning()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning")
        self.assertIn(
            "shadow_learning_confirm_cloud_write", result["data_schema"].schema
        )
        # The long technical action/mode dropdown must be gone from the normal path.
        self.assertNotIn("shadow_learning_action", result["data_schema"].schema)
        self.assertNotIn("shadow_learning_mode", result["data_schema"].schema)
        self.assertNotIn("shadow_learning_field_ids", result["data_schema"].schema)

    async def test_control_discovery_intro_requires_consent(self) -> None:
        options = self._wizard_options_flow()

        result = await options.async_step_shadow_learning(
            {"shadow_learning_confirm_cloud_write": False}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning")
        self.assertEqual(
            result["errors"], {"shadow_learning_confirm_cloud_write": "required"}
        )
        self.assertNotIn("wizard_consent", options._shadow_learning_state)

    async def test_control_discovery_consent_advances_to_credentials(self) -> None:
        options = self._wizard_options_flow()

        result = await options.async_step_shadow_learning(
            {"shadow_learning_confirm_cloud_write": True}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_credentials")
        self.assertTrue(options._shadow_learning_state["wizard_consent"])
        # Credentials step asks only for cloud username/password.
        self.assertEqual(set(result["data_schema"].schema), {"username", "password"})

    async def test_control_discovery_credentials_require_username_and_password(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["wizard_consent"] = True

        result = await options.async_step_shadow_learning_credentials(
            {"username": "", "password": ""}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_credentials")
        self.assertEqual(
            result["errors"], {"username": "required", "password": "required"}
        )
        self.assertNotIn("wizard_credentials", options._shadow_learning_state)

    async def test_control_discovery_credentials_unreachable_without_consent(self) -> None:
        options = self._wizard_options_flow()

        result = await options.async_step_shadow_learning_credentials(
            {"username": "demo", "password": "secret"}
        )

        # Falls back to the intro/consent step; credentials are not accepted.
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning")
        self.assertNotIn("wizard_credentials", options._shadow_learning_state)

    async def test_control_discovery_credentials_advance_through_progress(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["wizard_consent"] = True
        options._shadow_learning_state["wizard_progress_task"] = _DoneTask()

        result = await options.async_step_shadow_learning_credentials(
            {"username": " demo ", "password": " secret "}
        )

        self.assertEqual(result["type"], "progress_done")
        self.assertEqual(result["next_step_id"], "shadow_learning_review")
        self.assertEqual(
            options._shadow_learning_state["wizard_credentials"],
            {"username": "demo", "password": "secret"},
        )

    async def test_control_discovery_progress_creates_task_and_shows_progress(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["wizard_consent"] = True
        options._shadow_learning_state["wizard_credentials"] = {
            "username": "demo",
            "password": "secret",
        }

        result = await options.async_step_shadow_learning_progress()

        self.assertEqual(result["type"], "progress")
        self.assertEqual(result["step_id"], "shadow_learning_progress")
        self.assertEqual(result["progress_action"], "shadow_learning")
        task = options._shadow_learning_state["wizard_progress_task"]
        self.assertIsNotNone(task)
        # The placeholder runner performs no live operation; let it finish cleanly.
        await task

    async def test_control_discovery_progress_completes_to_review(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["wizard_consent"] = True
        options._shadow_learning_state["wizard_credentials"] = {
            "username": "demo",
            "password": "secret",
        }
        options._shadow_learning_state["wizard_progress_task"] = _DoneTask()

        result = await options.async_step_shadow_learning_progress()

        self.assertEqual(result["type"], "progress_done")
        self.assertEqual(result["next_step_id"], "shadow_learning_review")
        self.assertIsNone(options._shadow_learning_state["wizard_progress_task"])

    async def test_control_discovery_progress_unreachable_without_credentials(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["wizard_consent"] = True

        result = await options.async_step_shadow_learning_progress()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning")

    def test_set_control_discovery_progress_records_stage_and_clamps(self) -> None:
        options = self._wizard_options_flow()

        options._set_control_discovery_progress(0.45, "testing", done=10, total=23)
        progress = options._shadow_learning_state["progress"]
        self.assertEqual(progress["stage"], "testing")
        self.assertAlmostEqual(progress["fraction"], 0.45)
        self.assertEqual(progress["done"], 10)
        self.assertEqual(progress["total"], 23)

        # Fractions are clamped into [0, 1] for the determinate progress bar.
        options._set_control_discovery_progress(1.5, "finalizing")
        self.assertEqual(options._shadow_learning_state["progress"]["fraction"], 1.0)
        options._set_control_discovery_progress(-0.5, "preflight")
        self.assertEqual(options._shadow_learning_state["progress"]["fraction"], 0.0)

    async def test_control_discovery_review_forwards_to_result(self) -> None:
        options = self._wizard_options_flow()

        # An empty review (nothing found / failed run) skips the redundant
        # intermediate "nothing found" page and forwards straight to the result.
        shown = await options.async_step_shadow_learning_review()
        self.assertEqual(shown["type"], "form")
        self.assertEqual(shown["step_id"], "shadow_learning_result")

    @staticmethod
    def _review_capabilities() -> list[dict[str, Any]]:
        """A normal-risk control plus a high-risk (reset/destructive) control."""

        return [
            {
                "key": "learned_backlight_700",
                "title": "Backlight Control",
                "register": 700,
                "value_kind": "bool",
                "learned_provenance": {
                    "cloud_field_id": "sys_backlight_700",
                    "confidence": "high",
                    "safety_class": "setting",
                    "evidence_hash": "aaaa",
                },
            },
            {
                "key": "learned_reset_690",
                "title": "Reset user parameters",
                "register": 690,
                "value_kind": "action",
                "learned_provenance": {
                    "cloud_field_id": "sys_reset_690",
                    "confidence": "high",
                    "safety_class": "destructive_action",
                    "evidence_hash": "bbbb",
                },
            },
        ]

    def _seed_control_discovery_review(
        self,
        options,
        capabilities=None,
        *,
        phase="edit",
        skipped=None,
        learned_reads=None,
        skipped_reads=None,
    ) -> dict[str, Any]:
        """Embed a real review model in flow state the way the runner would.

        Defaults to the ``edit`` review page (where rename/enable fields live);
        pass ``phase="overview"`` to exercise the read-only overview page, and
        ``skipped`` to seed the already-supported control list.
        """

        review_model = attach_learned_read_review_model(
            build_learned_control_review_model(
                capabilities if capabilities is not None else self._review_capabilities()
            ),
            learned_read_sensors=list(learned_reads or []),
            skipped_read_sensors=list(skipped_reads or []),
        )
        manifest: dict[str, Any] = {"review_model": review_model}
        if skipped is not None:
            manifest["skipped_duplicates"] = list(skipped)
        options._shadow_learning_state["overlay"] = {"manifest": manifest}
        if phase is not None:
            options._shadow_learning_state["review_phase"] = phase
        return review_model

    async def test_control_discovery_review_edit_lists_controls_as_checkboxes(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)  # phase="edit" by default

        result = await options.async_step_shadow_learning_review()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_review")
        # A single multi-select field — no per-control rename/enable fields.
        schema = result["data_schema"].schema
        self.assertEqual({str(key) for key in schema}, {"enabled_controls"})
        selector = next(
            value for key, value in schema.items() if str(key) == "enabled_controls"
        )
        labels = [option["label"] for option in selector.config.kwargs["options"]]
        # Each option is labelled with the control's friendly name (no field IDs).
        self.assertIn("Backlight Control", labels)
        self.assertIn("Reset user parameters", labels)
        self.assertNotIn("sys_reset_690", labels)
        placeholders = result["description_placeholders"]
        self.assertEqual(placeholders["control_discovery_count"], "2")
        on_count = int(placeholders["control_discovery_on_count"])
        off_count = int(placeholders["control_discovery_off_count"])
        self.assertEqual(on_count + off_count, 2)
        self.assertGreaterEqual(off_count, 1)
        # Descriptions/types live on the overview page, not here.
        self.assertNotIn("control_discovery_table", placeholders)

    async def test_control_discovery_review_overview_lists_new_and_existing(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(
            options,
            phase="overview",
            skipped=[
                {"field_id": "bse_eybond_ctrl_48", "field_name": "Output Mode", "register": 300},
                {"field_id": "bse_eybond_ctrl_49", "field_name": "Output priority", "register": 301},
            ],
        )

        result = await options.async_step_shadow_learning_review()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_review")
        # Overview is read-only: no rename/enable fields on this page.
        self.assertEqual(dict(result["data_schema"].schema), {})
        placeholders = result["description_placeholders"]
        self.assertEqual(placeholders["control_discovery_new_count"], "2")
        self.assertEqual(placeholders["control_discovery_existing_count"], "2")
        overview = placeholders["control_discovery_overview"]
        # New controls and already-supported controls both appear, marked, with
        # friendly types and a suggested-state note (no field IDs / risk codes).
        self.assertIn("Backlight Control", overview)
        self.assertIn("Output Mode", overview)
        self.assertIn("Output priority", overview)
        self.assertIn("Switch", overview)
        self.assertIn("Button", overview)
        self.assertIn("Risky", overview)
        self.assertNotIn("destructive_action", overview)
        self.assertNotIn("sys_reset_690", overview)

    async def test_control_discovery_review_overview_continues_to_edit(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options, phase="overview")

        result = await options.async_step_shadow_learning_review({})

        # Continuing from the overview lands on the edit page with the selection.
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_review")
        self.assertIn("enabled_controls", {str(key) for key in result["data_schema"].schema})

    async def test_control_discovery_review_defaults_disable_risky_controls(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)
        controls = options._control_discovery_review_controls()

        default_enabled = options._control_discovery_default_enabled_keys(controls, {})

        # Normal control is pre-selected; risky control is not.
        self.assertIn("learned_backlight_700", default_enabled)
        self.assertNotIn("learned_reset_690", default_enabled)

    async def test_control_discovery_review_stores_user_choices(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)

        # User flips the defaults: enable the risky control, leave the normal off.
        forwarded = await options.async_step_shadow_learning_review(
            {"enabled_controls": ["learned_reset_690"]}
        )

        # On submit the wizard advances to the result step.
        self.assertEqual(forwarded["type"], "form")
        self.assertEqual(forwarded["step_id"], "shadow_learning_result")

        selections = options._shadow_learning_state["review_selections"]
        controls = selections["controls"]
        # The friendly discovered name is used as-is (there is no rename field).
        self.assertEqual(controls["learned_backlight_700"]["label"], "Backlight Control")
        self.assertFalse(controls["learned_backlight_700"]["enabled"])
        self.assertTrue(controls["learned_reset_690"]["enabled"])
        self.assertEqual(selections["enabled_by_user"], ["learned_reset_690"])
        self.assertEqual(selections["excluded_by_user"], ["learned_backlight_700"])

    async def test_control_discovery_review_lists_read_sensors_as_checkboxes(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(
            options,
            capabilities=[],
            learned_reads=[
                {
                    "key": "learned_read_344",
                    "register": 344,
                    "title": "Output 2 Cut-Off SOC Status",
                    "kind": "numeric",
                    "spec_set": "config",
                },
                {
                    "key": "learned_read_239",
                    "register": 239,
                    "title": "Output 2 Apparent Power",
                    "kind": "numeric",
                    "spec_set": "live",
                },
            ],
        )

        result = await options.async_step_shadow_learning_review()

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_review")
        schema = result["data_schema"].schema
        self.assertEqual({str(key) for key in schema}, {"enabled_read_sensors"})
        selector = next(
            value for key, value in schema.items() if str(key) == "enabled_read_sensors"
        )
        labels = [option["label"] for option in selector.config.kwargs["options"]]
        self.assertIn("Output 2 Cut-Off SOC Status", labels)
        self.assertIn("Output 2 Apparent Power", labels)

    async def test_control_discovery_review_stores_read_sensor_choices(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(
            options,
            capabilities=[],
            learned_reads=[
                {
                    "key": "learned_read_344",
                    "register": 344,
                    "title": "Output 2 Cut-Off SOC Status",
                    "kind": "numeric",
                    "spec_set": "config",
                },
                {
                    "key": "learned_read_239",
                    "register": 239,
                    "title": "Output 2 Apparent Power",
                    "kind": "numeric",
                    "spec_set": "live",
                },
            ],
        )

        await options.async_step_shadow_learning_review(
            {"enabled_read_sensors": ["learned_read_344"]}
        )

        selections = options._shadow_learning_state["review_selections"]
        self.assertEqual(selections["read_enabled_by_user"], ["learned_read_344"])
        self.assertEqual(selections["read_excluded_by_user"], ["learned_read_239"])
        self.assertTrue(selections["read_sensors"]["learned_read_344"]["enabled"])
        self.assertFalse(selections["read_sensors"]["learned_read_239"]["enabled"])

    async def test_control_discovery_review_keeps_disabled_controls_in_evidence(self) -> None:
        options = self._wizard_options_flow()
        review_model = self._seed_control_discovery_review(options)

        # Add nothing: both controls left off.
        await options.async_step_shadow_learning_review({"enabled_controls": []})

        # The discovered evidence is untouched: every control (including the ones
        # the user left disabled) is still present in learned_all...
        learned_keys = {
            entry["key"]
            for entry in options._shadow_learning_state["overlay"]["manifest"][
                "review_model"
            ]["learned_all"]
        }
        self.assertEqual(learned_keys, {"learned_backlight_700", "learned_reset_690"})
        # ...and the developer field name / default label is captured as evidence.
        reset_entry = next(
            entry
            for entry in review_model["learned_all"]
            if entry["key"] == "learned_reset_690"
        )
        self.assertEqual(reset_entry["field_name"], "Reset user parameters")
        self.assertEqual(reset_entry["default_label"], "Reset user parameters")
        # Both controls were recorded as excluded by the user.
        self.assertEqual(
            set(options._shadow_learning_state["review_selections"]["excluded_by_user"]),
            {"learned_backlight_700", "learned_reset_690"},
        )

    async def test_control_discovery_review_preserves_prior_selection_on_revisit(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)

        # First pass: flip both controls (enable risky, disable normal).
        await options.async_step_shadow_learning_review(
            {"enabled_controls": ["learned_reset_690"]}
        )

        # Revisiting reflects the user's prior choice, not the defaults.
        controls = options._control_discovery_review_controls()
        default_enabled = options._control_discovery_default_enabled_keys(
            controls, options._control_discovery_prior_selections()
        )
        self.assertNotIn("learned_backlight_700", default_enabled)
        self.assertIn("learned_reset_690", default_enabled)

    async def test_control_discovery_review_empty_uses_empty_copy(self) -> None:
        options = self._wizard_options_flow()

        # Empty review forwards to the result screen, which carries the detailed
        # "nothing found" copy directly (no intermediate empty page).
        shown = await options.async_step_shadow_learning_review()

        self.assertEqual(shown["type"], "form")
        self.assertEqual(shown["step_id"], "shadow_learning_result")
        self.assertIn(
            "No controls were found",
            shown["description_placeholders"]["control_discovery_hint"],
        )

    async def test_control_discovery_result_drops_credentials_and_returns_to_menu(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["wizard_credentials"] = {
            "username": "demo",
            "password": "secret",
        }

        shown = await options.async_step_shadow_learning_result()
        self.assertEqual(shown["type"], "form")
        self.assertEqual(shown["step_id"], "shadow_learning_result")
        # Credentials are dropped as soon as the result step is reached.
        self.assertNotIn("wizard_credentials", options._shadow_learning_state)

        done = await options.async_step_shadow_learning_result({})
        self.assertEqual(done["type"], "menu")
        self.assertEqual(done["step_id"], "init")

    async def test_control_discovery_result_failed_run_shows_failure_copy(self) -> None:
        options = self._wizard_options_flow()
        # Discovery ran but failed (e.g. the device never reconnected in time):
        # the copy must say so, not claim that nothing was found.
        options._shadow_learning_state["discovery"] = {
            "status": "error",
            "reason": "shadow_learning_session_not_ready",
        }

        shown = await options.async_step_shadow_learning_result()

        self.assertEqual(shown["type"], "form")
        self.assertEqual(shown["step_id"], "shadow_learning_result")
        hint = shown["description_placeholders"]["control_discovery_hint"]
        self.assertIn("couldn't finish", hint)
        self.assertNotIn("No controls were found", hint)

    async def test_control_discovery_empty_result_can_create_support_package(self) -> None:
        options = self._wizard_options_flow()
        options._shadow_learning_state["session"] = {
            "session_id": "empty-run",
            "trace_path": "/config/eybond_local/shadow_learning_traces/empty.jsonl",
        }
        options._shadow_learning_state["orchestration"] = {
            "planned_write_count": 1,
            "executed_result_count": 1,
            "sent_count": 1,
            "degraded_count": 1,
            "results": [{"field_id": "sys_eybond_ctrl_53", "reason": "session_not_ready"}],
            "correlation": {
                "matched_count": 0,
                "unmatched_attempt_count": 0,
                "degraded_attempt_count": 1,
            },
        }
        exported: dict[str, Any] = {}
        published: list[dict[str, Any]] = []

        async def _fake_export(**kwargs):
            exported.update(kwargs)
            return "/config/eybond_local/support/empty.zip"

        def _fake_publish(**kwargs):
            published.append(kwargs)
            values = build_shadow_learning_runtime_values(**kwargs)
            options._config_entry.runtime_data.data.values.update(values)
            return dict(values["shadow_learning_artifacts"])

        options._config_entry.runtime_data.publish_shadow_learning_artifacts = (
            _fake_publish
        )
        options._config_entry.runtime_data.async_export_support_package_with_cloud_refresh = (
            _fake_export
        )

        shown = await options.async_step_shadow_learning_result()
        self.assertEqual(shown["type"], "form")
        self.assertEqual(shown["step_id"], "shadow_learning_result")
        self.assertIn("result_action", shown["data_schema"].schema)

        result = await options.async_step_shadow_learning_result(
            {"result_action": "create_support_package"}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_result")
        self.assertEqual(exported.get("wants_refresh"), False)
        self.assertEqual(
            options._shadow_learning_state["support_package_path"],
            "/config/eybond_local/support/empty.zip",
        )
        self.assertTrue(published)
        self.assertEqual(
            options._config_entry.runtime_data.data.values[
                "shadow_learning_orchestration"
            ]["degraded_count"],
            1,
        )

    async def test_control_discovery_full_path_never_persists_credentials(self) -> None:
        options = self._wizard_options_flow()

        await options.async_step_shadow_learning(
            {"shadow_learning_confirm_cloud_write": True}
        )
        progress = await options.async_step_shadow_learning_credentials(
            {"username": "demo", "password": "secret"}
        )
        self.assertEqual(progress["type"], "progress")
        # Let the placeholder runner finish, then complete the progress step.
        await options._shadow_learning_state["wizard_progress_task"]
        done = await options.async_step_shadow_learning_progress()
        self.assertEqual(done["next_step_id"], "shadow_learning_review")

        await options.async_step_shadow_learning_review({})
        await options.async_step_shadow_learning_result({})

        self.assertNotIn("username", options._config_entry.options)
        self.assertNotIn("password", options._config_entry.options)
        self.assertNotIn("username", options._config_entry.data)
        self.assertNotIn("password", options._config_entry.data)
        self.assertNotIn("wizard_credentials", options._shadow_learning_state)

    async def test_control_discovery_result_offers_apply_for_learned_reads_only(self) -> None:
        # Read-learning closes the loop: when the session learned read sensors
        # but no controls were selected, the result screen must still offer
        # Apply (the schema overlay carrying the reads activates regardless).
        options = self._wizard_options_flow()
        options._shadow_learning_state["overlay"] = {
            "manifest": {"review_model": build_learned_control_review_model([])},
            "profile_name": "learned/p.json",
            "schema_name": "learned/s.json",
            "generated_read_count": 4,
        }

        recorded: dict[str, Any] = {}

        async def _fake_activate(*, profile_name, register_schema_name, selection=None):
            recorded["called"] = True
            recorded["selection"] = selection
            return {"scope": "device", "profile_name": profile_name}

        options._config_entry.runtime_data.async_activate_device_scoped_overlay = (
            _fake_activate
        )

        shown = await options.async_step_shadow_learning_result()
        self.assertEqual(shown["type"], "form")
        # The reads-only body reports the learned read count.
        self.assertIn("4", shown["description_placeholders"]["control_discovery_hint"])

        done = await options.async_step_shadow_learning_result(
            {"result_action": "activate_selected"}
        )
        # Activate is reachable with zero controls: the overlay schema (reads)
        # was activated, and the confirmation mentions the read sensors.
        self.assertTrue(recorded.get("called"))
        self.assertIn("4", done["description_placeholders"]["control_discovery_hint"])

    async def test_control_discovery_result_activates_selected_controls(self) -> None:
        # EYB-REF-047 (closes F1): the guided result step must actually activate
        # exactly the controls the user selected on the review screen — not just
        # store them in flow state and discard them at the end of the wizard.
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)
        # The automatic runner records the generated overlay's profile/schema
        # names; the seed helper only embeds the review model, so add them.
        options._shadow_learning_state["overlay"].update(
            {"profile_name": "learned/p.json", "schema_name": "learned/s.json"}
        )
        # User keeps the normal control and leaves the risky one off.
        await options.async_step_shadow_learning_review(
            {"enabled_controls": ["learned_backlight_700"]}
        )

        recorded: dict[str, Any] = {}

        async def _fake_activate(*, profile_name, register_schema_name, selection=None):
            recorded["profile_name"] = profile_name
            recorded["register_schema_name"] = register_schema_name
            recorded["selection"] = selection
            return {
                "scope": "device",
                "profile_name": profile_name,
                **(selection or {}),
            }

        options._config_entry.runtime_data.async_activate_device_scoped_overlay = (
            _fake_activate
        )

        # The result screen offers the activate / support / close actions.
        shown = await options.async_step_shadow_learning_result()
        self.assertEqual(shown["type"], "form")
        self.assertEqual(shown["step_id"], "shadow_learning_result")
        self.assertIn("result_action", shown["data_schema"].schema)

        done = await options.async_step_shadow_learning_result(
            {"result_action": "activate_selected"}
        )

        # The guided flow activated the device-scoped overlay with exactly the
        # user's selection: only the enabled control, carrying its user label.
        self.assertEqual(recorded["profile_name"], "learned/p.json")
        self.assertEqual(recorded["register_schema_name"], "learned/s.json")
        self.assertEqual(
            recorded["selection"]["selected_control_keys"], ["learned_backlight_700"]
        )
        selected = {c["key"]: c for c in recorded["selection"]["selected_controls"]}
        self.assertEqual(selected["learned_backlight_700"]["label"], "Backlight Control")
        excluded_keys = {c["key"] for c in recorded["selection"]["excluded_controls"]}
        self.assertIn("learned_reset_690", excluded_keys)
        # The activation is recorded, and applying confirms on the same screen
        # instead of bouncing back to the menu (the user leaves deliberately).
        self.assertEqual(
            options._shadow_learning_state["activation"]["scope"], "device"
        )
        self.assertEqual(done["type"], "form")
        self.assertEqual(done["step_id"], "shadow_learning_result")
        self.assertIn(
            "added to Home Assistant",
            done["description_placeholders"]["control_discovery_hint"],
        )

    async def test_control_discovery_result_activates_selected_read_sensors(self) -> None:
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(
            options,
            capabilities=[],
            learned_reads=[
                {
                    "key": "learned_read_344",
                    "register": 344,
                    "title": "Output 2 Cut-Off SOC Status",
                    "kind": "numeric",
                    "spec_set": "config",
                },
                {
                    "key": "learned_read_239",
                    "register": 239,
                    "title": "Output 2 Apparent Power",
                    "kind": "numeric",
                    "spec_set": "live",
                },
            ],
        )
        options._shadow_learning_state["overlay"].update(
            {"profile_name": "learned/p.json", "schema_name": "learned/s.json"}
        )
        await options.async_step_shadow_learning_review(
            {"enabled_read_sensors": ["learned_read_344"]}
        )

        recorded: dict[str, Any] = {}

        async def _fake_activate(*, profile_name, register_schema_name, selection=None):
            recorded["selection"] = selection
            return {"scope": "device", "profile_name": profile_name, **(selection or {})}

        options._config_entry.runtime_data.async_activate_device_scoped_overlay = (
            _fake_activate
        )

        await options.async_step_shadow_learning_result(
            {"result_action": "activate_selected"}
        )

        self.assertEqual(
            recorded["selection"]["selected_read_sensor_keys"], ["learned_read_344"]
        )
        self.assertEqual(recorded["selection"]["selected_control_keys"], [])
        excluded = {
            item["key"] for item in recorded["selection"]["excluded_read_sensors"]
        }
        self.assertEqual(excluded, {"learned_read_239"})

    async def test_control_discovery_result_creates_support_package(self) -> None:
        # The secondary result action exports a support package without a live
        # SmartESS refresh, preserves the reviewed selection for support evidence,
        # and keeps the user on the result screen without activating runtime controls.
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)
        options._shadow_learning_state["overlay"].update(
            {"profile_name": "learned/p.json", "schema_name": "learned/s.json"}
        )
        await options.async_step_shadow_learning_review(
            {"enabled_controls": ["learned_backlight_700"]}
        )

        exported: dict[str, Any] = {}

        async def _fake_export(**kwargs):
            exported.update(kwargs)
            return "/config/eybond_local/support/eybond_support.zip"

        published: list[dict[str, Any]] = []

        def _fake_publish(**kwargs):
            published.append(kwargs)
            values = build_shadow_learning_runtime_values(**kwargs)
            options._config_entry.runtime_data.data.values.update(values)
            return dict(values["shadow_learning_artifacts"])

        async def _unexpected_activate(**_kwargs):
            raise AssertionError("support export must not activate learned controls")

        options._config_entry.runtime_data.publish_shadow_learning_artifacts = (
            _fake_publish
        )
        options._config_entry.runtime_data.async_activate_device_scoped_overlay = (
            _unexpected_activate
        )
        options._config_entry.runtime_data.async_export_support_package_with_cloud_refresh = (
            _fake_export
        )

        result = await options.async_step_shadow_learning_result(
            {"result_action": "create_support_package"}
        )

        self.assertEqual(
            options._shadow_learning_state["support_package_path"],
            "/config/eybond_local/support/eybond_support.zip",
        )
        # No live SmartESS operation: the export is requested without a refresh.
        self.assertEqual(exported.get("wants_refresh"), False)
        self.assertEqual(exported.get("smartess_username"), "")
        self.assertTrue(published)
        activation = options._config_entry.runtime_data.data.values[
            "shadow_learning_activation"
        ]
        self.assertEqual(activation["status"], "review_selected")
        self.assertFalse(activation["active"])
        self.assertEqual(
            activation["selected_control_keys"], ["learned_backlight_700"]
        )
        selected = {item["key"]: item for item in activation["selected_controls"]}
        excluded = {item["key"]: item for item in activation["excluded_controls"]}
        self.assertEqual(selected["learned_backlight_700"]["label"], "Backlight Control")
        self.assertIn("learned_reset_690", excluded)
        # The result screen is re-rendered so the user can still enable controls.
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_result")

    async def test_control_discovery_result_activation_failure_surfaces_error(self) -> None:
        # When activation cannot proceed (here: the overlay has no generated
        # profile/schema), the failure is surfaced as a plain form error and the
        # wizard stays on the result screen instead of raising or silently
        # returning to the menu.
        options = self._wizard_options_flow()
        self._seed_control_discovery_review(options)
        await options.async_step_shadow_learning_review(
            {"enabled_controls": ["learned_backlight_700"]}
        )

        result = await options.async_step_shadow_learning_result(
            {"result_action": "activate_selected"}
        )

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "shadow_learning_result")
        self.assertEqual(result["errors"], {"base": "shadow_learning_failed"})

    async def test_control_discovery_intro_carries_friendly_hint_placeholder(self) -> None:
        # The intro screen is rendered from the plain-language hint placeholder,
        # not the legacy technical status table. Guards the translations wiring
        # (translations/*.json shadow_learning.description == {control_discovery_hint}).
        options = self._wizard_options_flow()

        result = await options.async_step_shadow_learning()

        self.assertEqual(result["type"], "form")
        hint = result["description_placeholders"].get("control_discovery_hint")
        self.assertTrue(hint)
        self.assertNotIn("{", hint)

    # ---- Automatic control-discovery runner (EYB-REF-041) ----

    class _RunnerCoordinator:
        smartess_collector_pn = "E5000020000000"
        cloud_evidence_provider = "smartess"
        collector_cloud_family = "dtu_ess"
        effective_profile_name = "smg_modbus.json"
        effective_register_schema_name = "modbus_smg/models/smg_6200.json"

        def __init__(self, *, ready: bool = True) -> None:
            self.data = types.SimpleNamespace(values={})
            self._runtime = types.SimpleNamespace(
                shadow_learning_route_status=lambda: {
                    "running": True,
                    "collector_connected": True,
                    "collector_protocol_ingress": True,
                    "upstream_connected": True,
                    "ready": ready,
                    "upstream_error": "",
                }
            )
            self.started: list[dict] = []
            self.stopped: list[dict] = []
            self.published: list[dict] = []

        async def async_start_shadow_learning(self, **kwargs):
            self.started.append(kwargs)
            return {
                "status": "ready",
                "session_id": "auto-session",
                "trace_path": "/config/eybond_local/shadow_learning_traces/auto.jsonl",
            }

        async def async_stop_shadow_learning(self, **kwargs):
            self.stopped.append(kwargs)
            return {"status": "stopped", "restore_confirmed": True}

        def publish_shadow_learning_artifacts(self, **kwargs):
            self.published.append(kwargs)
            return {}

    def _runner_options_flow(self, coordinator):
        """Build an options flow wired to run the automatic discovery pipeline.

        The expensive preflight/identity/observation helpers are stubbed the
        same way the advanced-path tests stub them, so each test focuses on the
        runner's orchestration, plan shape, and fail-closed cleanup.
        """

        options = self._make_options_flow()
        options._config_entry.runtime_data = coordinator

        async def _fake_preflight(_coordinator):
            return {"can_start": True, "blockers": []}

        options._build_shadow_learning_preflight_snapshot = _fake_preflight
        options._shadow_learning_cloud_identity = lambda _coordinator: {
            "pn": "E50000200000000001",
            "sn": "E50000200000000001000001",
            "devcode": 2376,
            "devaddr": 1,
        }
        options._shadow_learning_observation_source = lambda _coordinator: None
        options._shadow_learning_state["wizard_credentials"] = {
            "username": "demo@example.com",
            "password": "cloud-secret",
        }
        return options

    def _runner_cloud_patches(
        self,
        *,
        captured: dict,
        fetch_side_effect=None,
        orchestration_override: dict | None = None,
    ):
        bundle = {
            "request": {
                "params": {
                    "pn": "E50000200000000001",
                    "sn": "E50000200000000001000001",
                    "devcode": 2376,
                    "devaddr": 1,
                }
            },
            "responses": {
                "device_settings": {
                    "dat": {
                        "field": [
                            {"id": "sys_eybond_ctrl_53", "item": [{"key": "0"}]}
                        ]
                    }
                }
            },
        }
        orchestration = orchestration_override or {
            "planned_write_count": 1,
            "executed_result_count": 1,
            "sent_count": 1,
            "error_count": 0,
            "degraded_count": 0,
            "leaked_count": 0,
            "unknown_field_count": 0,
            "results": [],
            "correlation": {"matched_count": 1, "unmatched_attempt_count": 0},
        }
        fetch_kwargs = {}
        if fetch_side_effect is not None:
            fetch_kwargs["side_effect"] = fetch_side_effect
        else:
            fetch_kwargs["return_value"] = bundle
        return (
            patch.object(
                config_flow_module,
                "login_with_password",
                return_value=(
                    object(),
                    types.SimpleNamespace(
                        token="token",
                        secret="secret",
                        uid="uid",
                        usr="usr",
                        role=1,
                        expire=1,
                    ),
                ),
            ),
            patch.object(config_flow_module, "fetch_device_bundle_for_collector", **fetch_kwargs),
            patch.object(
                config_flow_module,
                "async_orchestrate_shadow_learning_settings",
                side_effect=lambda **kwargs: captured.update(kwargs) or dict(orchestration),
            ),
            patch.object(
                config_flow_module,
                "generate_shadow_learning_overlay_drafts",
                return_value=types.SimpleNamespace(
                    profile_path=Path("/config/eybond_local/profiles/learned/p.json"),
                    schema_path=Path("/config/eybond_local/register_schemas/learned/s.json"),
                    generated_capability_count=2,
                    skipped_duplicate_count=0,
                    generated_read_count=3,
                    manifest={
                        "output": {
                            "profile_name": "learned/p.json",
                            "schema_name": "learned/s.json",
                        }
                    },
                ),
            ),
        )

    async def test_control_discovery_runner_aborts_before_cloud_when_route_not_ready(self) -> None:
        # SAFETY: if the proxy route is not write-ready (no live upstream and
        # not ready), the runner must stop the session and never reach SmartESS
        # cloud login / writes — the route gate that replaced the deleted
        # advanced-step gating.
        coordinator = self._RunnerCoordinator(ready=False)
        coordinator._runtime.shadow_learning_route_status = lambda: {
            "running": True,
            "collector_connected": True,
            "collector_protocol_ingress": True,
            "upstream_connected": False,
            "ready": False,
            "upstream_error": "",
        }
        options = self._runner_options_flow(coordinator)
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(captured=captured)

        with login_p as login_mock, fetch_p as fetch_mock, orchestrate_p as orchestrate_mock, overlay_p:
            await options._async_run_control_discovery()

        login_mock.assert_not_called()
        fetch_mock.assert_not_called()
        orchestrate_mock.assert_not_called()
        # Session was started fail-closed then stopped; no overlay drafted.
        self.assertEqual(len(coordinator.stopped), 1)

    async def test_control_discovery_runner_uses_valuecloud_provider_runner(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        coordinator.cloud_evidence_provider = "valuecloud"
        coordinator.collector_cloud_family = "valuecloud_at"
        coordinator.effective_profile_name = "eybond_g_ascii/base.json"
        coordinator.effective_register_schema_name = "eybond_g_ascii/base.json"
        options = self._runner_options_flow(coordinator)
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(captured=captured)
        valuecloud_session = types.SimpleNamespace(token="vc-token", secret="vc-secret", auth="")
        valuecloud_bundle = {
            "request": {
                "params": {
                    "pn": "A0000000000001",
                    "sn": "DEV19E27F1B2345DA3",
                    "devcode": 2506,
                    "devaddr": 1,
                }
            },
            "normalized": {
                "batch_control": {
                    "groups": [
                        {
                            "controlItemId": 10,
                            "parameters": [
                                {
                                    "id": "cltd_lcd_backlight",
                                    "detailsId": 20,
                                    "order": 3,
                                    "name": "LCD Backlight",
                                    "readwrite": "RW",
                                    "item": {"1": "On"},
                                }
                            ],
                        }
                    ]
                }
            },
        }

        with (
            login_p as smartess_login_mock,
            fetch_p as smartess_fetch_mock,
            orchestrate_p as smartess_orchestrate_mock,
            overlay_p as overlay_mock,
            patch.object(
                config_flow_module.valuecloud_cloud_module,
                "login_with_password",
                return_value=(object(), valuecloud_session),
            ) as valuecloud_login_mock,
            patch.object(
                config_flow_module.valuecloud_cloud_module,
                "fetch_device_bundle_for_collector_with_session",
                return_value=valuecloud_bundle,
            ) as valuecloud_fetch_mock,
            patch.object(
                config_flow_module,
                "async_orchestrate_valuecloud_shadow_learning",
                side_effect=lambda **kwargs: captured.update(kwargs)
                or {
                    "planned_write_count": 1,
                    "executed_result_count": 1,
                    "sent_count": 1,
                    "captured_not_applied_count": 1,
                    "error_count": 0,
                    "degraded_count": 0,
                    "leaked_count": 0,
                    "unknown_field_count": 0,
                    "results": [],
                    "correlation": {
                        "matched_count": 1,
                        "matched": [
                            {
                                "field_id": "cltd_lcd_backlight",
                                "field_name": "LCD Backlight",
                                "requested_value": "1",
                                "value_label": "On",
                                "value_source": "choice",
                                "observation": {
                                    "register": -1,
                                    "values": [],
                                    "protocol": "eybond_g_ascii",
                                    "command": "PBL",
                                    "value": "1",
                                },
                            }
                        ],
                        "unmatched_attempt_count": 0,
                        "unmatched_write_count": 0,
                    },
                    "read_map": {},
                },
            ) as valuecloud_orchestrate_mock,
        ):
            await options._async_run_control_discovery()

        self.assertEqual(len(coordinator.started), 1)
        smartess_login_mock.assert_not_called()
        smartess_fetch_mock.assert_not_called()
        smartess_orchestrate_mock.assert_not_called()
        valuecloud_login_mock.assert_called_once()
        valuecloud_fetch_mock.assert_called_once()
        valuecloud_orchestrate_mock.assert_called_once()
        overlay_mock.assert_called_once()
        self.assertEqual(captured["session"], valuecloud_session)
        self.assertEqual(captured["batch_control"], valuecloud_bundle["normalized"]["batch_control"])
        self.assertEqual(captured["pn"], "A0000000000001")
        self.assertEqual(captured["devcode"], 2506)
        self.assertEqual(options._shadow_learning_state["discovery"]["status"], "ok")

    async def test_control_discovery_runner_runs_full_pipeline_without_preview_plan(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(captured=captured)

        with login_p, fetch_p, orchestrate_p as orchestrate_mock, overlay_p as overlay_mock:
            await options._async_run_control_discovery()

        # One automatic pass: session started fail-closed, learning run, overlay
        # drafted, session stopped — with no preview-plan/action step in between.
        self.assertEqual(len(coordinator.started), 1)
        self.assertEqual(coordinator.started[0].get("allow_ack_writes"), False)
        orchestrate_mock.assert_called_once()
        overlay_mock.assert_called_once()
        self.assertEqual(len(coordinator.stopped), 1)

        # The plan is built internally and is bounded: all fields, every choice value swept (so
        # the overlay learns each control's value set) AND numeric fields included (one
        # observe-only write each to learn their register + display divisor), capped field count.
        self.assertEqual(list(captured["field_ids"]), [])
        self.assertTrue(captured["include_numeric"])
        self.assertTrue(captured["all_choice_values"])
        self.assertEqual(
            captured["max_fields"],
            config_flow_module.CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS,
        )
        self.assertGreater(config_flow_module.CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS, 0)

        self.assertEqual(options._shadow_learning_state["discovery"]["status"], "ok")
        self.assertEqual(
            options._shadow_learning_state["overlay"]["generated_capability_count"], 2
        )

    async def test_control_discovery_runner_uses_live_bundle_identity_without_saved_evidence(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)
        options._shadow_learning_cloud_identity = lambda _coordinator: None
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(captured=captured)

        with login_p, fetch_p as bundle_mock, orchestrate_p as orchestrate_mock, overlay_p:
            await options._async_run_control_discovery()

        bundle_mock.assert_called_once_with(
            username="demo@example.com",
            password="cloud-secret",
            collector_pn="E5000020000000",
        )
        orchestrate_mock.assert_called_once()
        self.assertEqual(captured["pn"], "E50000200000000001")
        self.assertEqual(captured["sn"], "E50000200000000001000001")
        self.assertEqual(captured["devcode"], 2376)
        self.assertEqual(captured["devaddr"], 1)
        self.assertEqual(
            options._shadow_learning_state["identity"]["sn"],
            "E50000200000000001000001",
        )

    async def test_control_discovery_runner_surfaces_trace_path(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(captured=captured)

        with login_p, fetch_p, orchestrate_p, overlay_p:
            await options._async_run_control_discovery()

        # Acceptance: the trace path created for the session is visible afterwards.
        placeholders = options._shadow_learning_placeholders(coordinator)
        self.assertEqual(
            placeholders["shadow_learning_trace_path"],
            "/config/eybond_local/shadow_learning_traces/auto.jsonl",
        )

    async def test_control_discovery_runner_is_fail_closed_on_failure(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(
            captured=captured,
            fetch_side_effect=RuntimeError("settings_fetch_boom"),
        )

        with login_p, fetch_p, orchestrate_p as orchestrate_mock, overlay_p as overlay_mock:
            await options._async_run_control_discovery()

        # The session was started, the cloud fetch failed, and cleanup still ran:
        # the runner never raises and records the failure in flow state.
        self.assertEqual(len(coordinator.started), 1)
        orchestrate_mock.assert_not_called()
        overlay_mock.assert_not_called()
        # Fail-closed: a stop+restore was attempted, tolerant of an already-stopped
        # session (raise_when_not_running=False).
        self.assertEqual(len(coordinator.stopped), 1)
        self.assertFalse(coordinator.stopped[0].get("raise_when_not_running", True))
        self.assertEqual(options._shadow_learning_state["discovery"]["status"], "error")
        self.assertIn("settings_fetch_boom", options._shadow_learning_state["discovery"]["reason"])

    async def test_control_discovery_runner_treats_leaked_write_as_failure(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)
        captured: dict = {}
        leaked_orchestration = {
            "planned_write_count": 62,
            "executed_result_count": 30,
            "sent_count": 0,
            "error_count": 29,
            "degraded_count": 0,
            "leaked_count": 1,
            "unknown_field_count": 0,
            "results": [{"status": "leaked", "reason": "control_leaked_unproxied"}],
            "correlation": {"matched_count": 29, "unmatched_attempt_count": 1},
        }
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(
            captured=captured,
            orchestration_override=leaked_orchestration,
        )

        with login_p, fetch_p, orchestrate_p as orchestrate_mock, overlay_p as overlay_mock:
            await options._async_run_control_discovery()

        orchestrate_mock.assert_called_once()
        overlay_mock.assert_not_called()
        self.assertEqual(len(coordinator.stopped), 1)
        self.assertFalse(coordinator.stopped[0].get("raise_when_not_running", True))
        self.assertEqual(options._shadow_learning_state["discovery"]["status"], "error")
        self.assertIn(
            "SAFETY STOP",
            options._shadow_learning_state["discovery"]["reason"],
        )
        self.assertNotIn("overlay", options._shadow_learning_state)

    async def test_control_discovery_runner_requires_credentials(self) -> None:
        # No live SmartESS operation may start without the transient credentials
        # gathered earlier in the wizard.
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)
        options._shadow_learning_state.pop("wizard_credentials", None)

        await options._async_run_control_discovery()

        self.assertEqual(len(coordinator.started), 0)
        self.assertEqual(len(coordinator.stopped), 0)
        self.assertEqual(
            options._shadow_learning_state["discovery"]["reason"], "credentials_required"
        )

    async def test_control_discovery_runner_blocks_when_preflight_not_ready(self) -> None:
        coordinator = self._RunnerCoordinator(ready=True)
        options = self._runner_options_flow(coordinator)

        async def _blocked_preflight(_coordinator):
            return {"can_start": False, "blockers": ["collector_not_connected"]}

        options._build_shadow_learning_preflight_snapshot = _blocked_preflight
        captured: dict = {}
        login_p, fetch_p, orchestrate_p, overlay_p = self._runner_cloud_patches(captured=captured)

        with login_p as login_mock, fetch_p, orchestrate_p, overlay_p:
            await options._async_run_control_discovery()

        # Preflight gate prevents the session from starting and any cloud login.
        self.assertEqual(len(coordinator.started), 0)
        login_mock.assert_not_called()
        self.assertEqual(options._shadow_learning_state["discovery"]["status"], "error")
        self.assertIn(
            "shadow_learning_preflight_blocked",
            options._shadow_learning_state["discovery"]["reason"],
        )


class PreflightEffectiveMetadataTests(unittest.TestCase):
    # The fallback logic (persisted snapshot, else live base schema) now lives on
    # coordinator.shadow_learning_effective_metadata so the preview preflight and
    # the actual start path (async_start_shadow_learning) share ONE implementation
    # and cannot drift. This method is now a thin delegation; the fallback itself
    # is covered in test_coordinator_device_hierarchy.

    def test_preflight_delegates_live_fallback(self) -> None:
        fallback = {
            "effective_owner_key": "modbus_smg",
            "profile_name": "",
            "register_schema_name": "modbus_smg/base.json",
        }
        coordinator = types.SimpleNamespace(shadow_learning_effective_metadata=fallback)

        self.assertIs(
            EybondLocalOptionsFlow._preflight_effective_metadata(coordinator), fallback
        )

    def test_preflight_delegates_persisted_snapshot(self) -> None:
        snapshot = types.SimpleNamespace(
            register_schema_name="modbus_smg/models/smg_6200.json"
        )
        coordinator = types.SimpleNamespace(shadow_learning_effective_metadata=snapshot)

        self.assertIs(
            EybondLocalOptionsFlow._preflight_effective_metadata(coordinator), snapshot
        )


if __name__ == "__main__":
    unittest.main()
