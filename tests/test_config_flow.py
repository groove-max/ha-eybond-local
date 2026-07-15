from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import json
from pathlib import Path
import subprocess
import itertools
import sys
import tempfile
import types
import unittest
from contextlib import contextmanager, suppress
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
    util = types.ModuleType("homeassistant.util")
    util_ulid = types.ModuleType("homeassistant.util.ulid")

    _ulid_counter = itertools.count(1)

    def ulid_now() -> str:
        """Deterministic ULID-shaped id for tests (26 chars, Crockford base32)."""

        return f"01TEST{next(_ulid_counter):020d}"[:26].upper()

    util_ulid.ulid_now = ulid_now
    util.ulid = util_ulid

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

        def async_update_reload_and_abort(
            self, entry, *, unique_id=None, title=None, data=None, options=None, **_kwargs
        ):
            if data is not None:
                entry.data = data
            if options is not None:
                entry.options = options
            if unique_id is not None:
                entry.unique_id = unique_id
                self._test_unique_id = unique_id
            if title is not None:
                entry.title = title
            return {
                "type": "abort",
                "reason": "reconfigure_successful",
                "entry": entry,
                "unique_id": unique_id,
                "data": data,
            }

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
    sys.modules["homeassistant.util"] = util
    sys.modules["homeassistant.util.ulid"] = util_ulid


_install_homeassistant_stubs()


import custom_components.eybond_local.config_flow as config_flow_module
import custom_components.eybond_local.support.cloud_control_discovery as cloud_control_discovery_module
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
    _poll_interval_selector,
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
    CONF_CONNECTION_STRATEGY,
    CONF_PROXY_ENABLED,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
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


class _FakeSetupEntry:
    """Minimal ConfigEntry double for exercising the runtime setup claim path."""

    def __init__(self, entry_id: str, data: dict, options: dict | None = None) -> None:
        self.entry_id = entry_id
        self.data = dict(data)
        self.options = dict(options or {})
        self.unique_id = None
        self.title = None
        self._unloads: list = []

    def async_on_unload(self, callback):
        self._unloads.append(callback)
        return callback


class _FakeConfigEntries:
    def __init__(self, entries=None) -> None:
        self._entries = list(entries or [])
        self.unloaded: list[str] = []
        self.reloaded: list[str] = []
        # Every async_update_entry(**kwargs) recorded, so tests can assert that
        # data+options are committed in ONE atomic update (one reload).
        self.updates: list[dict] = []

    def async_update_entry(self, entry, **kwargs):
        """Mirror HA: apply the update and report whether anything changed."""

        self.updates.append(dict(kwargs))
        changed = False
        for key in ("data", "options"):
            if key in kwargs and kwargs[key] is not None:
                new = dict(kwargs[key])
                if dict(getattr(entry, key, {}) or {}) != new:
                    changed = True
                setattr(entry, key, new)
        for key, value in kwargs.items():
            if key in ("data", "options"):
                continue
            if getattr(entry, key, None) != value:
                changed = True
            setattr(entry, key, value)
        return changed

    def async_entries(self, _domain):
        return list(self._entries)

    def async_get_entry(self, entry_id: str):
        for entry in self._entries:
            if getattr(entry, "entry_id", None) == entry_id:
                return entry
        return None

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

    def test_pi30_manual_poll_selector_uses_driver_policy_floor(self) -> None:
        selector = _poll_interval_selector("pi30")

        self.assertEqual(selector.config.kwargs["min"], 2)

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
        self.assertEqual(
            result["menu_options"],
            ["auto", "bluetooth_setup", "listener"],
        )

    async def test_listener_menu_option_creates_bootstrap_entry(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_listener()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["title"], "EyeBond Local — Discovery")
        self.assertEqual(result["data"], {"entry_role": "listener"})
        self.assertEqual(flow._test_unique_id, "eybond_local:listener")

    async def test_listener_import_uses_same_unique_bootstrap_entry(self) -> None:
        flow = self._make_flow()

        result = await flow.async_step_import({"entry_role": "listener"})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"], {"entry_role": "listener"})
        self.assertEqual(flow._test_unique_id, "eybond_local:listener")

    async def test_listener_entry_exposes_friendly_rediscovery_action(self) -> None:
        entry = types.SimpleNamespace(
            data={"entry_role": "listener"},
            options={},
        )

        options_flow = EybondLocalConfigFlow.async_get_options_flow(entry)
        result = await options_flow.async_step_init()

        self.assertEqual(type(options_flow).__name__, "ListenerOptionsFlow")
        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "listener")
        self.assertEqual(result["menu_options"], ["rediscover_devices"])
        self.assertTrue(callable(getattr(options_flow, "async_step_listener", None)))

    async def test_listener_rediscovery_requires_confirmation_and_reports_result(self) -> None:
        entry = types.SimpleNamespace(
            data={"entry_role": "listener"},
            options={},
        )
        options_flow = EybondLocalConfigFlow.async_get_options_flow(entry)
        options_flow.hass = _FakeHass()
        service = types.SimpleNamespace(
            async_show_discovered_devices_again=AsyncMock(
                return_value=types.SimpleNamespace(
                    connected_unclaimed_count=3,
                    suppressed_candidate_count=2,
                )
            )
        )

        unconfirmed = await options_flow.async_step_rediscover_devices(
            {"confirm_rediscover_devices": False}
        )
        self.assertEqual(
            unconfirmed["errors"],
            {"confirm_rediscover_devices": "required"},
        )

        with patch(
            "custom_components.eybond_local.passive_discovery.get_passive_callback_discovery",
            return_value=service,
        ):
            completed = await options_flow.async_step_rediscover_devices(
                {"confirm_rediscover_devices": True}
            )

        service.async_show_discovered_devices_again.assert_awaited_once_with()
        self.assertEqual(completed["type"], "form")
        self.assertEqual(completed["step_id"], "rediscover_devices_done")
        self.assertEqual(
            completed["description_placeholders"],
            {"connected_count": "3", "released_count": "2"},
        )
        closed = await options_flow.async_step_rediscover_devices_done({})
        self.assertEqual(closed["type"], "create_entry")

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

    async def test_confirm_step_passive_callback_without_match_defers_inverter_table(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )

        result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        placeholders = result["description_placeholders"]
        self.assertIn("**Колектор**", placeholders["collector_confirm_table"])
        self.assertIn("**Інвертор**", placeholders["inverter_confirm_table"])
        self.assertIn("після створення запису", placeholders["inverter_confirm_table"])
        self.assertNotIn("| Модель |", placeholders["inverter_confirm_table"])
        self.assertNotIn("Непідтверджений інвертор", placeholders["inverter_confirm_table"])
        self.assertEqual(placeholders["control_summary"], "")

    async def test_confirm_step_passive_callback_does_not_probe_collector_capabilities(self) -> None:
        flow = self._make_flow()
        selected = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )
        flow._autodetect_results = {"0": selected}
        flow._selected_result = selected

        with (
            patch(
                "custom_components.eybond_local.config_flow.SharedEybondTransport",
                side_effect=AssertionError("passive confirm must not start payload transport"),
            ),
            patch(
                "custom_components.eybond_local.config_flow.SharedCollectorAtTransport",
                side_effect=AssertionError("passive confirm must not start AT transport"),
            ),
            patch(
                "custom_components.eybond_local.config_flow.query_runtime_collector_values",
                new=AsyncMock(
                    side_effect=AssertionError("passive confirm must not query FC values")
                ),
            ),
            patch(
                "custom_components.eybond_local.config_flow.query_runtime_collector_at_values",
                new=AsyncMock(
                    side_effect=AssertionError("passive confirm must not query AT values")
                ),
            ),
        ):
            result = await flow.async_step_confirm()

        self.assertEqual(result["type"], "form")
        self.assertFalse(flow._selected_result_collector_capabilities_attempted)

    async def test_confirm_step_passive_callback_submit_creates_ha_only_entry_without_binding(self) -> None:
        flow = self._make_flow()
        selected = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )
        flow._selected_result = selected
        bind = AsyncMock()
        flow._async_bind_selected_collector_to_home_assistant = bind

        result = await flow.async_step_confirm({"poll_mode": "auto"})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["connection_mode"], "callback_listener")
        self.assertNotIn("collector_ip", result["data"])
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        self.assertEqual(
            result["options"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        bind.assert_not_awaited()

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

        # Item 1: a UDP-only candidate that never connected has NO collector PN,
        # so no normal collector entry is created (no IP fallback).
        self.assertEqual(result["type"], "abort")
        self.assertEqual(result["reason"], "collector_identity_required")

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

    async def test_detection_summary_passive_callback_defers_inverter_detection(self) -> None:
        flow = self._make_flow()
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="0.0.0.0",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )

        result = await flow.async_step_detection_summary()

        placeholders = result["description_placeholders"]
        self.assertIn("Collector connected", placeholders["tier_headline"])
        self.assertIn("runtime owns this session", placeholders["tier_details"])
        self.assertNotIn("no inverter was detected", placeholders["tier_details"])
        self.assertNotIn("Device not recognized", placeholders["tier_headline"])

    async def test_detection_summary_passive_callback_uses_localized_text(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"
        flow._selected_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="0.0.0.0",
                source="callback_listener",
                ip="192.168.1.1",
                connected=True,
                collector=CollectorInfo(collector_pn="V001107SYN8229"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )

        result = await flow.async_step_detection_summary()

        placeholders = result["description_placeholders"]
        self.assertIn("Колектор підключений", placeholders["tier_headline"])
        self.assertIn("вхідним підключенням", placeholders["tier_details"])
        self.assertNotIn("Collector connected", placeholders["tier_headline"])

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
        self.assertEqual(result["data"]["connection_mode"], "callback_listener")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        self.assertEqual(
            result["options"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        self.assertTrue(result["data"]["collector_virtual_bridge"])

    async def test_confirm_step_does_not_persist_original_endpoint_for_bridge(self) -> None:
        flow = self._make_flow()
        flow._selected_result = self._collector_only_bridge_result()
        flow._collector_operation_mode = COLLECTOR_OPERATION_HA_ONLY
        flow._collector_endpoint_bind_applied = True
        flow._collector_original_server_endpoint = "ess.eybond.com"
        flow._collector_target_server_endpoint = "192.168.1.50,8899,TCP"

        with tempfile.TemporaryDirectory() as tempdir:
            flow.hass.config.config_dir = tempdir

            result = await flow.async_step_confirm({"poll_mode": "auto"})

            self.assertEqual(result["type"], "create_entry")
            self.assertNotIn(
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
                result["options"],
            )
            self.assertFalse(
                (Path(tempdir) / ".storage" / "eybond_local.collectors").exists()
            )

    async def test_confirm_step_does_not_rewrite_endpoint_for_passive_callback_bridge(self) -> None:
        flow = self._make_flow()
        selected = self._collector_only_bridge_result()
        selected.collector.source = "callback_listener"
        selected = replace(selected, connection_mode="callback_listener")
        flow._selected_result = selected
        bind = AsyncMock()
        flow._async_bind_selected_collector_to_home_assistant = bind

        result = await flow.async_step_confirm({"poll_mode": "auto"})

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_COLLECTOR_OPERATION_MODE],
            COLLECTOR_OPERATION_HA_ONLY,
        )
        bind.assert_not_awaited()

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

    async def test_do_scan_scopes_active_probe_against_passive_discovery(self) -> None:
        flow = self._make_flow()
        events: list[str] = []

        class _FakePassiveDiscovery:
            def begin_active_probe_scope(self, scope_id: str) -> None:
                self.scope_id = scope_id
                events.append("begin")

            def end_active_probe_scope(self, scope_id: str) -> None:
                self.end_scope_id = scope_id
                events.append("end")

        class _FakeDetector:
            async def async_auto_detect(self, **kwargs):
                events.append("detect")
                return ()

        passive = _FakePassiveDiscovery()
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ), patch(
            "custom_components.eybond_local.passive_discovery.get_passive_callback_discovery",
            return_value=passive,
        ):
            await flow._async_do_scan()

        self.assertEqual(events, ["begin", "detect", "end"])
        self.assertEqual(passive.scope_id, passive.end_scope_id)

    async def test_do_scan_uses_addable_passive_callback_without_active_udp_probe(self) -> None:
        flow = self._make_flow()
        passive_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V000405SYN94677058"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )

        class _FakeDetector:
            def __init__(self) -> None:
                self.auto_called = False

            async def async_passive_detect(self, **kwargs):
                return (passive_result,)

            async def async_auto_detect(self, **kwargs):
                self.auto_called = True
                return ()

        detector = _FakeDetector()
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=detector,
        ):
            await flow._async_do_scan()

        self.assertFalse(detector.auto_called)
        self.assertEqual(len(flow._autodetect_results), 1)
        result = next(iter(flow._autodetect_results.values()))
        self.assertEqual(result.connection_mode, "callback_listener")
        self.assertEqual(result.collector.collector.collector_pn, "V000405SYN94677058")

    async def test_do_scan_merges_passive_callback_with_active_results_when_passive_is_existing(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=8899)
        existing.data.update({"collector_pn": "V000405SYN94677058"})
        existing.unique_id = "collector:V000405SYN94677058"
        flow = self._make_flow(entries=[existing])
        passive_existing = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V000405SYN94677058"),
            ),
            connection_mode="callback_listener",
        )
        active_new = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.255.255",
                source="broadcast",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V000405SYN94677059"),
            ),
            connection_mode="broadcast",
            next_action="manual_driver_selection",
        )

        class _FakeDetector:
            async def async_passive_detect(self, **kwargs):
                return (passive_existing,)

            async def async_auto_detect(self, **kwargs):
                return (active_new,)

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            await flow._async_do_scan()

        self.assertEqual(
            {
                result.collector.collector.collector_pn
                for result in flow._autodetect_results.values()
                if result.collector is not None and result.collector.collector is not None
            },
            {"V000405SYN94677058", "V000405SYN94677059"},
        )
        self.assertEqual(
            {
                result.collector.collector.collector_pn
                for result in flow._available_autodetect_results().values()
                if result.collector is not None and result.collector.collector is not None
            },
            {"V000405SYN94677059"},
        )

    async def test_integration_discovery_selects_passive_callback_candidate(self) -> None:
        flow = self._make_flow()
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            side_effect=AssertionError(
                "integration_discovery must use the concrete discovery_info session"
            ),
        ):
            result = await flow.async_step_integration_discovery(
                {
                    "tcp_port": 18899,
                    "collector_pn": "V000405SYN94677058",
                    "peer_ip": "195.138.86.175",
                }
            )

        self.assertEqual(flow._test_unique_id, "collector:V000405SYN94677058")
        self.assertEqual(
            flow.context["title_placeholders"],
            {"name": "Collector PN V000405SYN94677058"},
        )
        assert flow._selected_result is not None
        assert flow._selected_result.collector is not None
        self.assertIsNone(flow._selected_result.match)
        self.assertEqual(flow._selected_result.connection_mode, "callback_listener")
        self.assertEqual(flow._selected_result.collector.source, "callback_listener")
        self.assertEqual(flow._selected_result.collector.ip, "195.138.86.175")
        self.assertEqual(
            flow._selected_result.collector.collector.collector_pn,
            "V000405SYN94677058",
        )
        self.assertIn(result["type"], {"form", "menu"})

    async def test_integration_discovery_does_not_runtime_enrich_passive_callback_candidate(self) -> None:
        flow = self._make_flow()
        factory_specs: list[object] = []

        def _fake_create_onboarding_manager(spec, **kwargs):
            factory_specs.append(spec)
            raise AssertionError("passive discovery preview must not create a detector")

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            side_effect=_fake_create_onboarding_manager,
        ):
            await flow.async_step_integration_discovery(
                {
                    "tcp_port": 18899,
                    "collector_pn": "V001020SYN62344022",
                    "peer_ip": "195.138.86.175",
                    "collector_session_protocol": "at_text",
                }
            )

        self.assertEqual(factory_specs, [])
        assert flow._selected_result is not None
        self.assertIsNone(flow._selected_result.match)
        self.assertEqual(flow._selected_result.connection_mode, "callback_listener")
        assert flow._selected_result.collector is not None
        self.assertEqual(flow._selected_result.collector.source, "callback_listener")
        self.assertEqual(flow._selected_result.collector.session_protocol, "at_text")

    async def test_integration_discovery_aborts_existing_passive_collector(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=18899)
        existing.data.update({"collector_pn": "V000405SYN94677058"})
        existing.unique_id = "collector:V000405SYN94677058"
        flow = self._make_flow(entries=[existing])

        class _FakeDetector:
            async def async_passive_detect(self, **kwargs):
                return ()

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            result = await flow.async_step_integration_discovery(
                {
                    "tcp_port": 18899,
                    "collector_pn": "V000405SYN94677058",
                    "peer_ip": "195.138.86.175",
                }
            )

        self.assertEqual(result, {"type": "abort", "reason": "already_configured"})

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

    async def test_existing_entry_with_pn_does_not_claim_unknown_candidate_on_same_nat_ip(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=8899)
        existing.data.update(
            {
                "collector_ip": "195.138.86.175",
                "collector_pn": "V000405SYN94677058",
            }
        )
        existing.unique_id = "manual:195.138.86.175"
        flow = self._make_flow(entries=[existing])
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="195.138.86.175",
                source="manual",
                ip="195.138.86.175",
                connected=True,
            ),
            connection_mode="manual",
            next_action="create_pending_entry",
        )

        self.assertIsNone(flow._existing_entry_for_result(result))

    async def test_existing_pending_entry_claims_unknown_candidate_on_same_nat_ip(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=8899)
        existing.data.update(
            {
                "collector_ip": "195.138.86.175",
                "collector_pn": "",
                "detected_serial": "",
            }
        )
        existing.unique_id = "manual_pending:192.168.1.50:18899:195.138.86.175"
        flow = self._make_flow(entries=[existing])
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="195.138.86.175",
                source="manual",
                ip="195.138.86.175",
                connected=True,
            ),
            connection_mode="manual",
            next_action="create_pending_entry",
        )

        self.assertIs(flow._existing_entry_for_result(result), existing)

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

    async def test_probe_manual_target_never_accepts_a_passive_candidate(self) -> None:
        flow = self._make_flow()
        user_input = {
            "server_ip": "192.168.1.50",
            "tcp_port": 18899,
            "udp_port": 58899,
            "collector_ip": "195.138.86.175",
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }
        passive_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="195.138.86.175",
                connected=True,
                collector=CollectorInfo(collector_pn="V000405SYN94677058"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )

        class _FakeDetector:
            def __init__(self) -> None:
                self.auto_called = False

            async def async_passive_detect(self, **kwargs):
                return (passive_result,)

            async def async_auto_detect(self, **kwargs):
                self.auto_called = True
                return ()

        detector = _FakeDetector()
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=detector,
        ):
            result = await flow._async_probe_manual_target(user_input)

        # BLOCKER 1 regression: the listener's session inventory is NOT bound to
        # collector_ip (the probe target is only carried as a source/label), so a
        # lone passive candidate says nothing about the collector the user typed
        # an address for. A callback attempt must ALWAYS ask actively and prove
        # the answer, never short-circuit on a stranger's session.
        self.assertIsNot(result, passive_result)
        self.assertTrue(detector.auto_called)
        # And the attempt declares its own trigger expectation up front.
        self.assertEqual(flow._manual_expected_own_triggers, 1)

    async def test_probe_manual_target_ignores_ambiguous_passive_callback_candidates(self) -> None:
        flow = self._make_flow()
        user_input = {
            "server_ip": "192.168.1.50",
            "tcp_port": 18899,
            "udp_port": 58899,
            "collector_ip": "195.138.86.175",
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }
        passive_results = (
            OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.50",
                    source="callback_listener",
                    ip="195.138.86.175",
                    connected=True,
                    collector=CollectorInfo(collector_pn="V000405SYN94677058"),
                ),
                connection_mode="callback_listener",
                next_action="manual_driver_selection",
            ),
            OnboardingResult(
                collector=CollectorCandidate(
                    target_ip="192.168.1.50",
                    source="callback_listener",
                    ip="195.138.86.175",
                    connected=True,
                    collector=CollectorInfo(collector_pn="V000405SYN94677059"),
                ),
                connection_mode="callback_listener",
                next_action="manual_driver_selection",
            ),
        )
        active_result = OnboardingResult(connection_mode="manual", next_action="create_pending_entry")

        class _FakeDetector:
            async def async_passive_detect(self, **kwargs):
                return passive_results

            async def async_auto_detect(self, **kwargs):
                return (active_result,)

        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_FakeDetector(),
        ):
            result = await flow._async_probe_manual_target(user_input)

        self.assertIs(result, active_result)

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
            "custom_components.eybond_local.config_flow._MANUAL_PROBE_WATCHDOG_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=_SlowDetector(),
        ):
            result = await flow._async_probe_manual_target(user_input)

        self.assertEqual(result.connection_mode, "manual")
        self.assertEqual(result.next_action, "create_pending_entry")
        self.assertEqual(result.last_error, "manual_probe_timeout")

    async def test_manual_probe_preserves_partial_result_after_work_deadline(self) -> None:
        flow = self._make_flow()
        user_input = {
            "server_ip": "192.168.1.50",
            "tcp_port": 8899,
            "udp_port": 58899,
            "collector_ip": "192.168.1.55",
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
        }
        partial = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="manual",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="I300002SYN3387"),
            ),
            connection_mode="manual",
            next_action="manual_driver_selection",
            last_error="target_detection_timeout",
        )

        class _FinalizingDetector:
            async def async_auto_detect(self, **kwargs):
                self.total_timeout = kwargs["total_timeout"]
                await asyncio.sleep(0.01)
                return (partial,)

        detector = _FinalizingDetector()
        with patch(
            "custom_components.eybond_local.config_flow._MANUAL_PROBE_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow._MANUAL_PROBE_WATCHDOG_TIMEOUT",
            0.1,
        ), patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=detector,
        ):
            result = await flow._async_probe_manual_target(user_input)

        self.assertIs(result, partial)
        self.assertEqual(detector.total_timeout, 0.001)
        self.assertEqual(result.collector.collector.collector_pn, "I300002SYN3387")

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
                    # This test asserts the ACTIVE probe runs, which only
                    # callback_on_demand is allowed to do.
                    "connection_strategy": "callback_on_demand",
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

        # Part 2: no NORMAL collector entry is created without a durable PN --
        # the user gets an explicit PENDING entry instead, identified by a
        # synthetic pending:<ULID> (never by an address).
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertEqual(result["data"]["collector_pn"], "")
        self.assertTrue(flow._test_unique_id.startswith("pending:"))

    async def test_manual_callback_verified_pn_is_persisted_into_entry(self) -> None:
        # Scenario 1: a manual/known-IP callback verification observed the strong
        # full collector PN (via fc2_parameter_2 on the listener). The created
        # entry MUST persist that durable PN + callback_on_demand + callback_trigger
        # evidence -- so it can own the inbound session it triggers rather than
        # sitting at collector_offline forever.
        from custom_components.eybond_local.const import (
            CONF_COLLECTOR_PN,
            CONF_CONNECTION_STRATEGY_EVIDENCE,
            CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
        )
        from custom_components.eybond_local.connection.connection_policy import (
            collector_identity_binding_required,
        )

        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "driver_hint": "auto",
            "tcp_port": 18899,
            "udp_port": 58899,
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        # A callback-only collector: the manual probe to the IP returns no
        # collector object, so collector_pn would otherwise be dropped.
        flow._manual_result = OnboardingResult(
            connection_mode="known_ip", next_action="create_pending_entry"
        )
        flow._manual_verified_full_pn = "V001020SYN62344022"
        flow._verified_connection_strategy = CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        flow._verified_strategy_evidence = CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER

        result = await flow.async_step_manual_create_pending()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"][CONF_COLLECTOR_PN], "V001020SYN62344022")
        self.assertEqual(
            result["data"][CONF_CONNECTION_STRATEGY],
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertEqual(
            result["data"][CONF_CONNECTION_STRATEGY_EVIDENCE],
            CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
        )
        # The durable PN drives the entry unique id (durable identity, not IP).
        self.assertEqual(flow._test_unique_id, "collector:V001020SYN62344022")
        # The persisted entry is identity-bound: NOT an offline PN-less entry.
        self.assertFalse(
            collector_identity_binding_required(result["data"], result.get("options") or {})
        )

    async def test_callback_verification_unconfirmed_creates_no_normal_entry(self) -> None:
        # Item 5: a callback verification was in play (a passive-discovery
        # verification context exists) but produced no registry-certified strong
        # PN. Entry creation MUST fail closed -- no normal create_entry doomed to
        # collector_offline -- and instead re-prompt the manual verification form
        # with an error so the user can retry.
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "192.168.1.55",
            "driver_hint": "auto",
            "tcp_port": 18899,
            "udp_port": 58899,
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="known_ip", next_action="create_pending_entry"
        )
        # Verification context present, but no verified full PN was ever bound.
        flow._verification_expected_pn = "V001020SYN62344022"
        flow._manual_verified_full_pn = ""

        result = await flow.async_step_manual_create_pending()

        # No NORMAL collector entry is created: it is saved as an explicit
        # PENDING entry instead, so nothing runs a doomed PN-less runtime.
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertEqual(result["data"]["collector_pn"], "")
        # The unverified/expected PN never became a durable collector identity.
        self.assertNotEqual(
            getattr(flow, "_test_unique_id", None), "collector:V001020SYN62344022"
        )
        self.assertTrue(flow._test_unique_id.startswith("pending:"))

    async def test_manual_create_pending_resolves_bridge_profile_before_entry_creation(self) -> None:
        flow = self._make_flow()
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "195.138.86.175",
            "driver_hint": "auto",
            "tcp_port": 18899,
            "udp_port": 58899,
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="manual",
            next_action="create_pending_entry",
            last_error="manual_probe_timeout",
        )

        class _BridgeProfileTransport:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            async def async_query_bridge_hardware_version(self):
                return (None, b"\x00\x06esp-collector/0.1.8/ESP8266")

        with patch(
            "custom_components.eybond_local.config_flow.SharedCollectorAtTransport",
            _BridgeProfileTransport,
        ):
            result = await flow.async_step_manual_create_pending()

        # Item 1 + Part 2: virtual-bridge metadata is NOT a session identity, so
        # no NORMAL entry is created; it is saved as an explicit PENDING entry.
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertEqual(result["data"]["collector_pn"], "")

    async def test_detected_inverter_without_collector_pn_is_not_created(self) -> None:
        # Item 1: a detected model + serial does NOT substitute for the collector
        # PN (registry ownership is by PN only). No normal entry is created; the
        # detection stays in the flow and the user is re-prompted to verify.
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
                target_ip="192.168.1.55", source="manual", ip="192.168.1.55", connected=True
            ),  # detected inverter, but NO collector PN
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

        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            result = await flow.async_step_manual_create_pending()

        # A detected model/serial is NOT a session identity: no NORMAL entry.
        # It is saved as an explicit PENDING entry instead.
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertEqual(result["data"]["collector_pn"], "")
        self.assertTrue(flow._test_unique_id.startswith("pending:"))

    async def test_full_pn_with_model_serial_is_created(self) -> None:
        # Item 1: a full PN alongside model/serial DOES create a normal entry.
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
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
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

        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            result = await flow.async_step_manual_create_pending()

        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["collector_pn"], "V001020SYN62344022")
        self.assertEqual(flow._test_unique_id, "collector:V001020SYN62344022")

    async def test_listener_entry_is_created_without_collector_pn(self) -> None:
        # Item 1 exception: the integration listener/bootstrap entry owns no
        # collector session, so it is created without a PN.
        flow = self._make_flow()
        result = await flow.async_step_listener()
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"].get("entry_role"), "listener")

    async def test_manual_create_pending_allows_same_nat_ip_when_existing_entry_has_pn(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=18899)
        existing.data.update(
            {
                "collector_ip": "195.138.86.175",
                "collector_pn": "V000405SYN94677058",
                "detected_serial": "",
            }
        )
        existing.unique_id = "manual:195.138.86.175"
        flow = self._make_flow(entries=[existing])
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "195.138.86.175",
            "driver_hint": "auto",
            "tcp_port": 18899,
            "udp_port": 58899,
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="manual",
            next_action="create_pending_entry",
            last_error="manual_probe_timeout",
        )

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            new=AsyncMock(return_value=flow._manual_result),
        ):
            result = await flow.async_step_manual_create_pending()

        # Part 2: saved as an explicit PENDING entry. Its identity is a synthetic
        # pending:<ULID>, so it never aliases with the same-NAT-IP collector entry.
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertTrue(flow._test_unique_id.startswith("pending:"))

    async def test_manual_create_pending_allows_same_nat_ip_as_another_pending(self) -> None:
        existing = _FakeEntry("existing", server_ip="192.168.1.50", tcp_port=18899)
        existing.data.update(
            {
                "collector_ip": "195.138.86.175",
                "collector_pn": "",
                "detected_serial": "",
            }
        )
        existing.unique_id = "manual_pending:192.168.1.50:18899:195.138.86.175"
        flow = self._make_flow(entries=[existing])
        flow._manual_config = {
            "server_ip": "192.168.1.50",
            "collector_ip": "195.138.86.175",
            "driver_hint": "auto",
            "tcp_port": 18899,
            "udp_port": 58899,
            "discovery_target": "",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        }
        flow._manual_result = OnboardingResult(
            connection_mode="manual",
            next_action="create_pending_entry",
            last_error="manual_probe_timeout",
        )

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            new=AsyncMock(return_value=flow._manual_result),
        ):
            result = await flow.async_step_manual_create_pending()

        # An address is NOT an identity: a second collector behind the same NAT
        # must still be addable. It gets its own synthetic pending:<ULID>.
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertTrue(flow._test_unique_id.startswith("pending:"))

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

        # Part 2: saved as an explicit PENDING entry (inbound by default, so the
        # broadcast address is only a hint and is sanitized away).
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertEqual(result["data"]["collector_ip"], "")
        # Pending identity is synthetic, never derived from an address.
        self.assertTrue(flow._test_unique_id.startswith("pending:"))

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
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
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
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
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
                target_ip="192.168.1.55",
                source="udp",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="V001020SYN62344022"),
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
                "custom_components.eybond_local.support.cloud_evidence_providers.fetch_and_export_smartess_device_bundle_cloud_evidence",
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

    async def test_scan_result_labels_name_passive_callback_peer_address_explicitly(self) -> None:
        flow = self._make_flow()
        flow.hass.config.language = "uk"
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="192.168.1.1",
                connected=True,
                collector=CollectorInfo(collector_pn="V001107SYN8229"),
            ),
            connection_mode="callback_listener",
            next_action="manual_driver_selection",
        )
        flow._autodetect_results = {"0": result}

        await flow._async_ensure_translation_bundle()

        result_label = flow._result_label(result)
        scan_line = flow._scan_result_line(1, result)

        self.assertIn("PN V001107SYN8229", result_label)
        self.assertIn("з’єднання від 192.168.1.1", result_label)
        self.assertIn("з’єднання від 192.168.1.1", scan_line)
        self.assertNotIn("колектор 192.168.1.1", scan_line)

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
        # Item 3: a detected bridge has no upstream provider side, so proxy
        # capture (which has nothing to capture) is omitted from diagnostics.
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

    async def test_options_runtime_step_shows_connection_strategy_selector_for_factory(self) -> None:
        # Phase 4: the primary user choice is the connection-strategy selector,
        # not the legacy Cloud+HA / HA-only operation mode.
        options = self._make_options_flow()
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=False),
                values={},
            ),
        )

        result = await options.async_step_runtime()

        self.assertEqual(result["type"], "form")
        self.assertIn(CONF_CONNECTION_STRATEGY, result["data_schema"].schema)
        # The legacy operation-mode selector is no longer the primary choice.
        self.assertNotIn(CONF_COLLECTOR_OPERATION_MODE, result["data_schema"].schema)
        self.assertEqual(
            result["description_placeholders"]["collector_operation_mode_note"], ""
        )

    async def test_options_runtime_step_forces_inbound_for_bridge_on_submit(self) -> None:
        # Phase 4: a bridge dials Home Assistant on its own -> inbound. The
        # strategy selector is hidden for it and inbound is persisted.
        options = self._make_options_flow()
        options._config_entry.options = {CONF_PROXY_ENABLED: True}
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
        # Canonical (v4): the strategy is committed to entry.DATA, never options.
        self.assertEqual(
            options._config_entry.data[CONF_CONNECTION_STRATEGY],
            CONNECTION_STRATEGY_INBOUND,
        )
        self.assertNotIn(CONF_CONNECTION_STRATEGY, result["data"])
        # Capability-gated proxy must fail closed instead of carrying a stale
        # True from older options/capability snapshots.
        self.assertFalse(result["data"][CONF_PROXY_ENABLED])

    async def test_options_runtime_step_hides_retired_proxy_toggle(self) -> None:
        # The steady cloud-proxy flag never had a runtime consumer. Do not expose
        # it for either factory collectors or community bridges.
        proxy_capable = self._make_options_flow()
        proxy_capable._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(collector=None, values={}),
        )
        proxy_result = await proxy_capable.async_step_runtime()
        self.assertEqual(proxy_result["type"], "form")
        self.assertNotIn(CONF_PROXY_ENABLED, proxy_result["data_schema"].schema)

        bridge = self._make_options_flow()
        bridge._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=True),
                values={"collector_virtual_bridge": True},
            ),
        )
        bridge_result = await bridge.async_step_runtime()
        self.assertEqual(bridge_result["type"], "form")
        self.assertNotIn(CONF_PROXY_ENABLED, bridge_result["data_schema"].schema)

    async def test_options_runtime_step_persists_connection_strategy_and_disables_proxy(self) -> None:
        # The compatibility proxy axis remains explicit and fail-closed.
        options = self._make_options_flow()
        options._config_entry.options = {CONF_PROXY_ENABLED: True}
        options._config_entry.runtime_data = types.SimpleNamespace(
            data=types.SimpleNamespace(
                collector=types.SimpleNamespace(collector_virtual_bridge=False),
                values={},
            ),
        )

        result = await options.async_step_runtime(
            {
                "poll_interval": 15,
                "control_mode": "auto",
                "connection_strategy": CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
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
        # Canonical (v4): the submitted strategy lands in entry.DATA ...
        self.assertEqual(
            options._config_entry.data[CONF_CONNECTION_STRATEGY],
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        # ... and is NEVER written to options, where it could shadow a later
        # HA-only / Cloud+HA / bind / rollback action.
        self.assertNotIn(CONF_CONNECTION_STRATEGY, result["data"])
        self.assertNotIn(CONF_CONNECTION_STRATEGY, options._config_entry.options)
        self.assertFalse(result["data"][CONF_PROXY_ENABLED])
        self.assertIn(CONF_COLLECTOR_OPERATION_MODE, result["data"])
        # data+options are committed in ONE atomic update -> exactly one reload.
        updates = options.hass.config_entries.updates
        self.assertEqual(len(updates), 1)
        self.assertIn("data", updates[0])
        self.assertIn("options", updates[0])

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

    async def test_create_support_package_uses_current_origin_download_link_in_result(self) -> None:
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
            'href="/local/eybond_local/support/support_archive.zip"',
            result["description_placeholders"]["download_markdown"],
        )
        self.assertNotIn(
            "http://192.168.1.50:8123",
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
                    "support_package_download_url": "https://ha.example/api/diagnostics/support_archive.zip",
                    "support_package_download_relative_url": "/api/diagnostics/support_archive.zip?authSig=current-origin",
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
        self.assertIn(
            "/api/diagnostics/support_archive.zip?authSig=current-origin",
            result["description_placeholders"]["download_markdown"],
        )
        self.assertNotIn(
            "https://ha.example",
            result["description_placeholders"]["download_markdown"],
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
                cloud_control_discovery_module,
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
            patch.object(
                cloud_control_discovery_module,
                "fetch_device_bundle_for_collector",
                **fetch_kwargs,
            ),
            patch.object(
                cloud_control_discovery_module,
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
                cloud_control_discovery_module.valuecloud_cloud_module,
                "login_with_password",
                return_value=(object(), valuecloud_session),
            ) as valuecloud_login_mock,
            patch.object(
                cloud_control_discovery_module.valuecloud_cloud_module,
                "fetch_device_bundle_for_collector_with_session",
                return_value=valuecloud_bundle,
            ) as valuecloud_fetch_mock,
            patch.object(
                cloud_control_discovery_module,
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


class ConnectionStrategyVerificationFlowTests(unittest.IsolatedAsyncioTestCase):
    """Behavioral connection-strategy verification wired into passive discovery."""

    FULL_PN = "V001020SYN62344022"
    OTHER_FULL_PN = "V000405SYN94677058"
    OLD_SESSION = "listener-18899-1"
    NEW_SESSION = "listener-18899-2"
    PEER_IP = "203.0.113.10"

    def _make_flow(self) -> EybondLocalConfigFlow:
        flow = EybondLocalConfigFlow()
        flow.hass = _FakeHass()
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

    def _assert_callback_failure_menu(
        self,
        flow: EybondLocalConfigFlow,
        result: dict[str, object],
        reason: str,
    ) -> None:
        """A failed manual callback remains actionable, never form-blocking."""

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "manual_confirm")
        self.assertIn("manual_probe_again", result["menu_options"])
        self.assertIn("manual_edit_settings", result["menu_options"])
        self.assertIn("manual_create_pending", result["menu_options"])
        self.assertIsNotNone(flow._manual_result)
        self.assertEqual(flow._manual_result.last_error, reason)

    def _discovery_info(self, **overrides) -> dict[str, object]:
        info = {
            "tcp_port": 18899,
            "collector_pn": self.FULL_PN,
            "peer_ip": self.PEER_IP,
            "session_id": self.OLD_SESSION,
        }
        info.update(overrides)
        return info

    def _install_registry(self, flow, inventory: list[dict[str, object]]):
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        flow.hass.data["eybond_local"] = {"callback_session_registry": registry}
        return registry

    @staticmethod
    def _inventory_session(
        session_id: str,
        pn: str,
        state: str = "identified",
        *,
        identity_source: str = "at_dtupn",
    ) -> dict[str, object]:
        return {
            "session_id": session_id,
            "peer_ip": "203.0.113.10",
            "listener_port": 18899,
            "collector_pn": pn,
            "state": state,
            "collector_identity_source": identity_source,
        }

    @staticmethod
    def _schema_default(schema, field: str):
        for key in schema.schema:
            if str(getattr(key, "schema", key)) == field:
                default = getattr(key, "default", None)
                return default() if callable(default) else default
        return None

    def _manual_input(
        self, collector_ip: str, *, connection_strategy: str = "callback_on_demand"
    ) -> dict[str, object]:
        # The manual form now REQUIRES an explicit strategy: it decides whether
        # Home Assistant may reach out at all. These verification tests exercise
        # the callback path, so they state callback_on_demand.
        return {
            "server_ip": "192.168.1.50",
            "tcp_port": 18899,
            "udp_port": 58899,
            "collector_ip": collector_ip,
            "discovery_target": "192.168.1.255",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
            "connection_strategy": connection_strategy,
        }

    def _manual_result_with_pn(self, pn: str) -> OnboardingResult:
        return OnboardingResult(
            connection_mode="manual",
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="manual",
                ip="192.168.1.60",
                connected=True,
                collector=CollectorInfo(collector_pn=pn),
            ),
        )

    async def _drive_verification(self, flow) -> dict[str, object]:
        """Consent -> progress -> completed task -> result step routing."""

        consent = await flow.async_step_verify_connection()
        self.assertEqual(consent["type"], "form")
        self.assertEqual(consent["step_id"], "verify_connection")

        progress = await flow.async_step_verify_connection_progress()
        self.assertEqual(progress["type"], "progress")
        await flow._verification_task
        done = await flow.async_step_verify_connection_progress()
        self.assertEqual(done["type"], "progress_done")
        self.assertEqual(done["next_step_id"], "verify_connection_result")
        return await flow.async_step_verify_connection_result()

    # Discovery with an observed session id must ask for verification consent
    # in the SAME flow (no second flow is initialized anywhere).
    async def test_discovery_with_session_id_shows_verification_consent(self) -> None:
        flow = self._make_flow()
        result = await flow.async_step_integration_discovery(self._discovery_info())

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "verify_connection")
        self.assertEqual(
            flow._strategy_verification_context,
            {
                "collector_pn": self.FULL_PN,
                "session_id": self.OLD_SESSION,
                "port": 18899,
                "peer_ip": self.PEER_IP,
                "identity_source": "",
            },
        )
        # Payloads without a session id can never become inbound: reboot
        # verification is impossible, so the SAME flow continues on the manual
        # callback step (peer IP prefilled as an editable hint) and the entry is
        # created only after the callback proof.
        legacy_flow = self._make_flow()
        legacy = await legacy_flow.async_step_integration_discovery(
            self._discovery_info(session_id="", collector_pn=self.OTHER_FULL_PN)
        )
        self.assertEqual(legacy["type"], "form")
        self.assertEqual(legacy["step_id"], "manual")
        self.assertIsNone(legacy_flow._strategy_verification_context)
        self.assertEqual(legacy_flow._verification_expected_pn, self.OTHER_FULL_PN)
        self.assertEqual(legacy_flow._manual_defaults.get("collector_ip"), self.PEER_IP)
        self.assertEqual(legacy_flow._verified_connection_strategy, "")

    # Verification failure stays retryable in THIS discovery flow. Manual setup
    # remains an explicit choice and keeps the peer IP as an editable hint.
    async def test_verification_failure_falls_through_to_manual_with_peer_prefill(self) -> None:
        from custom_components.eybond_local.onboarding.strategy_verification import (
            StrategyVerificationResult,
        )

        flow = self._make_flow()
        await flow.async_step_integration_discovery(self._discovery_info())

        async def _fake_run() -> None:
            flow._verification_result = StrategyVerificationResult(
                failure_reason="restart_not_supported",
                collector_pn=self.FULL_PN,
            )

        with patch.object(flow, "_async_run_strategy_verification", side_effect=_fake_run):
            result = await self._drive_verification(flow)

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "verify_connection_failed")
        self.assertEqual(
            result["menu_options"],
            ["verify_connection_retry", "manual"],
        )
        retry = await flow.async_step_verify_connection_retry()
        self.assertEqual(retry["type"], "form")
        self.assertEqual(retry["step_id"], "verify_connection")

        result = await flow.async_step_manual()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "manual")
        # Peer IP is prefilled purely as an editable hint...
        self.assertEqual(flow._manual_defaults.get("collector_ip"), self.PEER_IP)
        # ...with honest labeling about router/VPN/port-forward setups.
        note = result["description_placeholders"]["verification_note"]
        self.assertIn("router", note.lower())
        # No strategy was guessed and no entry was created.
        self.assertEqual(flow._verified_connection_strategy, "")

    # 1./13. Verified inbound: entry data gets inbound + external + evidence,
    # and no unverified peer address is persisted as collector_ip.
    async def test_verification_success_stamps_inbound_external(self) -> None:
        from custom_components.eybond_local.connection import connection_policy as cp
        from custom_components.eybond_local.onboarding import strategy_verification as sv

        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(self._discovery_info())

        claim_owner_during_restart: list[str] = []
        test = self
        new_session = self._inventory_session(self.NEW_SESSION, self.FULL_PN)

        class _FakeChannel:
            def __init__(self, **_kwargs) -> None:
                self.closed = 0

            async def async_send_restart(self) -> None:
                # The temporary registry claim must already be held here.
                claim_owner_during_restart.append(registry.owner_for_pn(test.FULL_PN))
                # Collector reboots: old session drops, then a NEW one dials in.
                inventory.clear()
                inventory.append(new_session)

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                self.closed += 1

        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FakeChannel):
            result = await self._drive_verification(flow)

        # Success continues the normal passive-candidate routing.
        self.assertIn(result["type"], {"form", "menu"})
        self.assertIn(result["step_id"], {"driver_choice", "detection_summary"})
        self.assertEqual(flow._verified_connection_strategy, "inbound")
        self.assertEqual(flow._verified_strategy_evidence, sv.EVIDENCE_REBOOT_RECONNECT)
        # Item 2: the claim existed during the restart AND is HELD after a
        # successful inbound proof -- it is handed off at entry creation, not
        # released, so the runtime owns the session before it starts.
        self.assertTrue(claim_owner_during_restart[0].startswith("strategy_verification:"))
        self.assertTrue(
            registry.owner_for_pn(self.FULL_PN).startswith("strategy_verification:")
        )

        # Entry-data stamping: explicit inbound + evidence, no peer IP persisted.
        data = {
            "connection_mode": "callback_listener",
            "collector_ip": self.PEER_IP,
            "collector_operation_mode": "smartess_cloud_home_assistant",
        }
        flow._apply_verified_connection_strategy(data)
        data.update(cp.migrate_entry_axes(data, {}))
        self.assertEqual(data["connection_strategy"], "inbound")
        self.assertEqual(data["connection_strategy_evidence"], "reboot_reconnect")
        self.assertEqual(data["endpoint_control_policy"], "external")
        self.assertEqual(data["collector_ip"], "")
        # The behaviorally-verified inbound entry is exempt from the legacy
        # cloud-primary migration correction.
        self.assertIsNone(cp.correct_migrated_connection_strategy(data, {}))

    async def test_verification_rebinds_stale_flow_to_current_session(self) -> None:
        """An OTA/reboot between discovery and consent must not stale the flow."""

        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(
            self._discovery_info(collector_identity_source="at_dtupn")
        )

        # The discovery-time socket disappears while the consent form is open;
        # the same durable collector dials back on a new socket.
        inventory[:] = [self._inventory_session(self.NEW_SESSION, self.FULL_PN)]
        claimed_session_ids: list[str] = []
        post_restart = self._inventory_session(
            "listener-18899-3", self.FULL_PN
        )

        class _FakeChannel:
            def __init__(self, **kwargs) -> None:
                claimed_session_ids.append(kwargs["session_id"])

            async def async_send_restart(self) -> None:
                inventory[:] = [post_restart]

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                return None

        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FakeChannel):
            await self._drive_verification(flow)

        self.assertEqual(claimed_session_ids, [self.NEW_SESSION])
        self.assertEqual(flow._verification_old_session_id, self.NEW_SESSION)
        # Item 2: a successful inbound proof HOLDS the claim for handoff.
        self.assertTrue(
            registry.owner_for_pn(self.FULL_PN).startswith("strategy_verification:")
        )
        self.assertTrue(flow._verification_result.inbound_verified)

    # 2. Zero UDP callback triggers are sent while inbound verification runs.
    async def test_inbound_verification_sends_zero_udp_triggers(self) -> None:
        from custom_components.eybond_local.onboarding import strategy_verification as sv

        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(self._discovery_info())
        new_session = self._inventory_session(self.NEW_SESSION, self.FULL_PN)

        class _FakeChannel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                inventory.clear()
                inventory.append(new_session)

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                return None

        probe = AsyncMock(side_effect=AssertionError("UDP trigger sent during inbound verification"))
        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FakeChannel), patch.object(
            config_flow_module, "async_send_callback_trigger", probe
        ):
            await flow.async_step_verify_connection({})
            await flow._verification_task

        probe.assert_not_called()
        result = flow._verification_result
        assert result is not None
        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.evidence, sv.EVIDENCE_REBOOT_RECONNECT)

    async def test_inbound_verification_blocks_concurrent_callback_trigger(self) -> None:
        """A pending/runtime callback cannot contaminate the reboot proof."""

        from custom_components.eybond_local.collector.discovery import (
            async_send_callback_trigger,
        )
        from custom_components.eybond_local.connection.callback_ledger import (
            CallbackTriggerInhibitedError,
            get_callback_trigger_ledger,
        )

        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(self._discovery_info())
        new_session = self._inventory_session(self.NEW_SESSION, self.FULL_PN)
        blocked: list[str] = []
        generation_before = get_callback_trigger_ledger().snapshot_generation()

        class _FakeChannel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                try:
                    await async_send_callback_trigger(
                        bind_ip="127.0.0.1",
                        advertised_server_ip="127.0.0.1",
                        advertised_server_port=8899,
                        target_ip="192.0.2.55",
                        udp_port=58899,
                        timeout=0.01,
                        source="concurrent_pending_entry",
                    )
                except CallbackTriggerInhibitedError as exc:
                    blocked.append(str(exc))
                inventory[:] = [new_session]

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                return None

        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FakeChannel):
            await flow.async_step_verify_connection({})
            await flow._verification_task

        self.assertEqual(
            blocked,
            ["callback_trigger_inhibited_by_inbound_verification"],
        )
        self.assertEqual(
            get_callback_trigger_ledger().snapshot_generation(),
            generation_before,
        )
        result = flow._verification_result
        assert result is not None
        self.assertTrue(result.inbound_verified)

    # A session/identity already claimed by another owner is a typed failure --
    # never hijacked -- and the flow continues on the manual callback step.
    async def test_already_claimed_session_is_not_hijacked(self) -> None:
        from custom_components.eybond_local.onboarding import strategy_verification as sv

        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        registry.claim("entry-other", collector_pn=self.FULL_PN, session_id=self.OLD_SESSION)
        await flow.async_step_integration_discovery(self._discovery_info())

        class _FailChannel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                raise AssertionError("restart must not be sent for a claimed session")

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                return None

        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FailChannel):
            result = await self._drive_verification(flow)

        verification = flow._verification_result
        assert verification is not None
        self.assertEqual(verification.failure_reason, sv.FAILURE_SESSION_CLAIMED)
        # No inbound classification; the same flow offers retry or manual setup.
        self.assertEqual(flow._verified_connection_strategy, "")
        self.assertEqual(result["step_id"], "verify_connection_failed")
        # The foreign claim is untouched.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "entry-other")

    # Cancel (flow removal) releases the temporary claim in every state.
    async def test_cancel_during_verification_releases_claim(self) -> None:
        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(self._discovery_info())

        restart_started = asyncio.Event()

        class _HangingChannel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                restart_started.set()
                await asyncio.sleep(30)

            def is_connected(self) -> bool:
                return True

            async def async_close(self) -> None:
                order.append("close")

        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        order: list[str] = []
        real_release = CallbackSessionRegistry.release

        def _tracking_release(registry_self, owner):
            order.append("release")
            return real_release(registry_self, owner)

        with patch.object(
            config_flow_module, "ObservedSessionRestartChannel", _HangingChannel
        ), patch.object(CallbackSessionRegistry, "release", _tracking_release):
            progress = await flow.async_step_verify_connection_progress()
            self.assertEqual(progress["type"], "progress")
            task = flow._verification_task
            await asyncio.wait_for(restart_started.wait(), timeout=5)
            # The claim is held while the verification is in flight (promotion
            # to the full PN already happened before the restart).
            self.assertTrue(
                registry.owner_for_pn(self.FULL_PN).startswith("strategy_verification:")
            )

            flow.async_remove()
            # async_remove must NOT release the claim early while the task is
            # alive; the task's finally releases it after the channel closes.
            with suppress(asyncio.CancelledError):
                await task

        self.assertTrue(task.cancelled())
        # Ordering: restart channel fully closed BEFORE the claim was released.
        self.assertEqual(order, ["close", "release"])
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(flow._verification_claim_owner, "")

    # 8. Manual callback success -> callback_on_demand + evidence; the persisted
    # address is the one the trigger was actually sent to.
    async def test_manual_callback_success_marks_callback_on_demand(self) -> None:
        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        # Pre-trigger: only the originally observed session exists (baseline).
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            # The collector answers OUR trigger with a NEW strong session.
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        with patch.object(
            flow,
            "_async_probe_manual_target",
            side_effect=_probe,
        ):
            result = await flow.async_step_manual(self._manual_input("192.168.1.60"))

        self.assertEqual(result["type"], "menu")
        self.assertEqual(result["step_id"], "manual_confirm")
        self.assertEqual(flow._verified_connection_strategy, "callback_on_demand")
        self.assertEqual(flow._verified_strategy_evidence, "callback_trigger")

        data = {"collector_ip": "192.168.1.60"}
        flow._apply_verified_connection_strategy(data)
        self.assertEqual(data["connection_strategy"], "callback_on_demand")
        self.assertEqual(data["connection_strategy_evidence"], "callback_trigger")
        # The callback target address is kept (it answered), not cleared.
        self.assertEqual(data["collector_ip"], "192.168.1.60")

    # 9. Identity mismatch / timeout show the flow error and create nothing;
    # the address stays editable for a retry.
    async def test_manual_callback_identity_mismatch_shows_error(self) -> None:
        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            # A DIFFERENT collector answered at this address.
            inventory.append(self._inventory_session(self.NEW_SESSION, self.OTHER_FULL_PN))
            return self._manual_result_with_pn(self.OTHER_FULL_PN)

        with patch.object(
            flow,
            "_async_probe_manual_target",
            side_effect=_probe,
        ):
            result = await flow.async_step_manual(self._manual_input("192.168.1.60"))

        self._assert_callback_failure_menu(
            flow, result, "callback_identity_mismatch"
        )
        self.assertIsNone(flow._manual_result.collector)
        self.assertIsNone(flow._manual_result.match)
        self.assertEqual(flow._verified_connection_strategy, "")
        # The entered address stays in flow state and the explicit Edit action
        # returns it to the editable form.
        self.assertEqual(flow._manual_config.get("collector_ip"), "192.168.1.60")
        self.assertIn("manual_edit_settings", result["menu_options"])

    async def test_manual_callback_timeout_offers_pending_entry(self) -> None:
        from custom_components.eybond_local.const import (
            CONF_CONNECTION_STRATEGY,
            CONF_ENTRY_ROLE,
            CONF_PENDING_LAST_ATTEMPT_RESULT,
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            ENTRY_ROLE_PENDING_COLLECTOR,
            PENDING_ATTEMPT_CALLBACK_TIMEOUT,
        )

        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        registry = self._install_registry(flow, [])

        with patch.object(
            flow,
            "_async_probe_manual_target",
            return_value=OnboardingResult(connection_mode="manual"),
        ):
            result = await flow.async_step_manual(self._manual_input("192.168.1.60"))

        self._assert_callback_failure_menu(flow, result, "callback_timeout")
        self.assertEqual(flow._verified_connection_strategy, "")

        async def _passthrough_enrich(_user_input, pending_result):
            return pending_result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()

        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"][CONF_ENTRY_ROLE], ENTRY_ROLE_PENDING_COLLECTOR)
        self.assertEqual(
            created["data"][CONF_CONNECTION_STRATEGY],
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertEqual(created["data"]["collector_ip"], "192.168.1.60")
        self.assertEqual(created["data"]["collector_pn"], "")
        self.assertEqual(
            created["data"][CONF_PENDING_LAST_ATTEMPT_RESULT],
            PENDING_ATTEMPT_CALLBACK_TIMEOUT,
        )
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    # 10. Two collectors behind one peer IP cannot confirm each other: a fresh
    # session of a DIFFERENT full PN does not satisfy the new-session check.
    async def test_same_peer_ip_other_collector_does_not_confirm_callback(self) -> None:
        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        self._install_registry(
            flow,
            [self._inventory_session(self.NEW_SESSION, self.OTHER_FULL_PN)],
        )
        # Probe "found" the right PN but the only fresh session is the other
        # collector behind the same NAT: verification must not pass.
        with patch.object(
            flow,
            "_async_probe_manual_target",
            return_value=self._manual_result_with_pn(self.FULL_PN),
        ):
            result = await flow.async_step_manual(self._manual_input(self.PEER_IP))

        self._assert_callback_failure_menu(flow, result, "callback_timeout")
        self.assertEqual(flow._verified_connection_strategy, "")

    # Weak new session never proves the callback: strong identity is required.
    async def test_manual_weak_new_session_does_not_confirm(self) -> None:
        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(
                self._inventory_session(
                    self.NEW_SESSION,
                    self.FULL_PN,
                    identity_source="framed_heartbeat",  # weak
                )
            )
            return self._manual_result_with_pn(self.FULL_PN)

        with patch.object(flow, "_async_probe_manual_target", side_effect=_probe):
            result = await flow.async_step_manual(self._manual_input("192.168.1.60"))

        self._assert_callback_failure_menu(flow, result, "callback_timeout")
        self.assertEqual(flow._verified_connection_strategy, "")

    # No-session-id discovery: a session that ALREADY existed before the trigger
    # is baseline and never counts as the callback answer.
    async def test_manual_no_session_id_preexisting_session_is_not_answer(self) -> None:
        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)
        # Discovery payload without session_id routes straight to manual.
        result = await flow.async_step_integration_discovery(
            self._discovery_info(session_id="")
        )
        self.assertEqual(result["step_id"], "manual")

        with patch.object(
            flow,
            "_async_probe_manual_target",
            return_value=self._manual_result_with_pn(self.FULL_PN),
        ):
            result = await flow.async_step_manual(self._manual_input(self.PEER_IP))

        # The pre-existing session is in the baseline: no proof, no entry.
        self._assert_callback_failure_menu(flow, result, "callback_timeout")
        self.assertEqual(flow._verified_connection_strategy, "")

    # Entry created after short->full enrichment carries ONE consistent full PN
    # in unique_id, CONF_COLLECTOR_PN, and the title.
    async def test_entry_after_short_to_full_enrichment_is_consistent(self) -> None:
        short_pn = "V001020SYN6234"
        flow = self._make_flow()
        # The observed session already reports the strong FULL PN; the discovery
        # payload still carried the short prefix.
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(
            self._discovery_info(collector_pn=short_pn)
        )
        self.assertEqual(flow._test_unique_id, f"collector:{short_pn}")
        new_session = self._inventory_session(self.NEW_SESSION, self.FULL_PN)

        class _FakeChannel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                inventory.clear()
                inventory.append(new_session)

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                return None

        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FakeChannel):
            await self._drive_verification(flow)

        # Every flow model adopted the enriched FULL PN.
        self.assertEqual(flow._verified_connection_strategy, "inbound")
        self.assertEqual(flow._test_unique_id, f"collector:{self.FULL_PN}")
        self.assertEqual(
            flow._selected_result.collector.collector.collector_pn, self.FULL_PN
        )
        self.assertEqual(
            flow.context["title_placeholders"],
            {"name": f"Collector PN {self.FULL_PN}"},
        )

        # Create the REAL entry through the confirm submit path and check its
        # data -- not just the strategy stamping helper.
        with (
            patch(
                "custom_components.eybond_local.config_flow.SharedEybondTransport",
                side_effect=AssertionError("passive confirm must not start payload transport"),
            ),
            patch(
                "custom_components.eybond_local.config_flow.SharedCollectorAtTransport",
                side_effect=AssertionError("passive confirm must not start AT transport"),
            ),
        ):
            created = await flow.async_step_confirm({"poll_mode": "auto"})

        self.assertEqual(created["type"], "create_entry")
        data = created["data"]
        self.assertEqual(data["collector_pn"], self.FULL_PN)
        self.assertEqual(data["connection_strategy"], "inbound")
        self.assertEqual(data["connection_strategy_evidence"], "reboot_reconnect")
        self.assertEqual(data["endpoint_control_policy"], "external")
        self.assertEqual(data["collector_ip"], "")
        self.assertIn(self.FULL_PN, created["title"])

    # 12. Cancel/error cleanup: removing the flow cancels the verification task.
    async def test_flow_removal_cancels_verification_task(self) -> None:
        flow = self._make_flow()
        await flow.async_step_integration_discovery(self._discovery_info())

        started = asyncio.Event()

        async def _hang() -> None:
            started.set()
            await asyncio.sleep(30)

        with patch.object(flow, "_async_run_strategy_verification", side_effect=_hang):
            progress = await flow.async_step_verify_connection_progress()
            self.assertEqual(progress["type"], "progress")
            task = flow._verification_task
            await started.wait()

        flow.async_remove()
        with suppress(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertIsNone(flow._verification_task)
        self.assertIsNone(flow._strategy_verification_context)

    # ---- ownership handoff: config flow -> entry (items 1, 2, 3, 7, 9) ----

    async def test_manual_callback_claims_session_and_setup_completes_handoff(self) -> None:
        # Item 1 (the real end-to-end lifecycle): a known-IP one-shot callback
        # reaches a NEW strong full-PN session; the PRODUCTION manual step claims
        # THAT exact session under a UNIQUE per-attempt owner; the user confirms;
        # the entry is created (prepare_handoff commits the claim); and PRODUCTION
        # setup completes the handoff to the durable entry_id -- no gap, no double
        # owner, no leaked claim, never keyed on peer IP.
        from custom_components.eybond_local import (
            _register_entry_callback_session_claim,
        )
        from custom_components.eybond_local.const import CONF_COLLECTOR_PN

        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow, "_async_probe_manual_target", side_effect=_probe
        ), patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            result = await flow.async_step_manual(self._manual_input("192.168.1.60"))
            self.assertEqual(result["step_id"], "manual_confirm")

            # The owner is a UNIQUE per-attempt id (never a PN-derived id).
            owner = registry.owner_for_pn(self.FULL_PN)
            self.assertTrue(owner.startswith("callback_verification:"))
            self.assertEqual(registry.claimed_session_id(owner), self.NEW_SESSION)
            self.assertEqual(registry.claimed_identity(owner), self.FULL_PN)

            created = await flow.async_step_manual_create_pending()

        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"][CONF_COLLECTOR_PN], self.FULL_PN)
        self.assertTrue(flow._callback_ownership_handed_off)

        # Flow cleanup must NOT release a committed handoff.
        flow.async_remove()
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), owner)

        # PRODUCTION setup completes the handoff to the durable entry_id.
        entry = _FakeSetupEntry("entry-xyz", created["data"])
        _register_entry_callback_session_claim(flow.hass, entry)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "entry-xyz")
        self.assertEqual(registry.claimed_session_id("entry-xyz"), self.NEW_SESSION)
        # No leaked/duplicate owner: the per-attempt owner is gone.
        self.assertEqual(registry.claimed_identity(owner), "")
        # Completing again is a no-op (the handoff transfers exactly once).
        self.assertFalse(registry.complete_handoff(self.FULL_PN, "entry-other"))

    def test_handoff_api_has_production_callers(self) -> None:
        # Item 9 guard: the handoff API must be invoked by production code, not
        # left as dead API. Setup completes the handoff (complete_handoff) with a
        # claim-by-PN fallback; the config flow prepares it (prepare_handoff).
        import ast
        import inspect
        import textwrap

        from custom_components.eybond_local import (
            _register_entry_callback_session_claim,
        )
        from custom_components.eybond_local.config_flow import (
            EybondLocalConfigFlow,
        )

        setup_src = textwrap.dedent(
            inspect.getsource(_register_entry_callback_session_claim)
        )
        setup_calls = {
            node.func.attr
            for node in ast.walk(ast.parse(setup_src))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("complete_handoff", setup_calls)
        self.assertIn("claim", setup_calls)  # restart / no-handoff fallback

        prepare_src = textwrap.dedent(
            inspect.getsource(EybondLocalConfigFlow._prepare_ownership_handoff)
        )
        prepare_calls = {
            node.func.attr
            for node in ast.walk(ast.parse(prepare_src))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("prepare_handoff", prepare_calls)

    def test_setup_without_pending_handoff_claims_by_durable_pn(self) -> None:
        # Item 3 fallback: after an HA restart the in-memory handoff claim is
        # gone; setup must still claim the durable identity by PN (never by IP) so
        # the next same-PN session binds to the entry.
        from custom_components.eybond_local import (
            _register_entry_callback_session_claim,
        )

        flow = self._make_flow()
        inventory = [self._inventory_session(self.NEW_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        entry = _FakeSetupEntry("entry-restart", {"collector_pn": self.FULL_PN})
        _register_entry_callback_session_claim(flow.hass, entry)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "entry-restart")
        self.assertEqual(registry.claimed_session_id("entry-restart"), self.NEW_SESSION)

    def test_setup_blocked_by_uncommitted_verification_claim_is_retryable(self) -> None:
        # Item 3: an in-flight verification (uncommitted) for the PN blocks setup
        # with a RETRYABLE error -- the runtime must not start without ownership,
        # and the verification claim is never stolen.
        from custom_components.eybond_local import (
            ConfigEntryNotReady,
            _register_entry_callback_session_claim,
        )

        flow = self._make_flow()
        inventory = [self._inventory_session(self.NEW_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        registry.claim_session("callback_verification:live", session_id=self.NEW_SESSION)
        registry.promote_claim_to_full_pn("callback_verification:live", self.FULL_PN)

        entry = _FakeSetupEntry("entry-racing", {"collector_pn": self.FULL_PN})
        with self.assertRaises(ConfigEntryNotReady):
            _register_entry_callback_session_claim(flow.hass, entry)

        # The uncommitted verification claim is untouched; the entry did not steal it.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "callback_verification:live")
        self.assertEqual(registry.claimed_identity("entry-racing"), "")

        # After the competing verification releases, a setup retry succeeds and the
        # entry owns the durable identity.
        registry.release("callback_verification:live")
        _register_entry_callback_session_claim(flow.hass, entry)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "entry-racing")

    async def test_two_concurrent_flows_same_pn_do_not_share_owner(self) -> None:
        # Item 1: two live flows verifying the SAME collector are distinct owners;
        # the second gets a conflict and cannot disturb the first.
        registry_inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]

        flow_a = self._make_flow()
        flow_a._verification_expected_pn = self.FULL_PN
        flow_a._verification_old_session_id = self.OLD_SESSION
        registry = self._install_registry(flow_a, registry_inventory)

        flow_b = self._make_flow()
        flow_b._verification_expected_pn = self.FULL_PN
        flow_b._verification_old_session_id = self.OLD_SESSION
        # Same shared domain registry (one per HA process).
        flow_b.hass.data["eybond_local"] = flow_a.hass.data["eybond_local"]

        async def _probe(_flat_input):
            registry_inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        with patch.object(flow_a, "_async_probe_manual_target", side_effect=_probe):
            result_a = await flow_a.async_step_manual(self._manual_input("192.168.1.60"))
        self.assertEqual(result_a["step_id"], "manual_confirm")
        owner_a = registry.owner_for_pn(self.FULL_PN)
        self.assertTrue(owner_a.startswith("callback_verification:"))

        # Flow B now verifies the same PN (same NEW session already present): it
        # fails closed with a conflict and never takes ownership.
        with patch.object(
            flow_b,
            "_async_probe_manual_target",
            return_value=self._manual_result_with_pn(self.FULL_PN),
        ):
            result_b = await flow_b.async_step_manual(self._manual_input("192.168.1.60"))
        self.assertEqual(result_b["type"], "menu")
        self.assertEqual(result_b["step_id"], "manual_confirm")
        self.assertIn(
            flow_b._manual_result.last_error,
            ("callback_identity_conflict", "callback_timeout"),
        )
        self.assertIn("manual_create_pending", result_b["menu_options"])
        # Flow A still owns it; flow B never became an owner.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), owner_a)
        self.assertEqual(flow_b._verification_claim_owner, "")

    async def test_manual_callback_strategy_is_canonical_in_data_without_expected_pn(
        self,
    ) -> None:
        # Item 2 regression: a generic manual callback_on_demand flow (NO
        # passive-discovery expected PN) whose attempt resolves a full PN must
        # persist the CHOSEN strategy in entry.data -- not fall back to legacy
        # connection_mode derivation -- and never write it to options.
        #
        # The attempt is driven through the REAL lifecycle (own trigger recorded,
        # collector answers on a new strong session) rather than by stubbing the
        # probe out. A callback entry now takes its identity from the VERIFIED
        # PN only, so a stubbed probe -- which declares no trigger and leaves no
        # session behind -- is a state production cannot reach and would (fail
        # closed) yield a pending entry instead of the normal entry under test.
        from custom_components.eybond_local.const import (
            CONF_CONNECTION_STRATEGY,
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

        flow = self._make_flow()
        inventory: list[dict[str, object]] = []
        self._install_registry(flow, inventory)
        assert not flow._verification_expected_pn

        def _answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))

        detector = self._recording_detector(
            results=(self._manual_result_with_pn(self.FULL_PN),), on_detect=_answers
        )

        async def _passthrough_enrich(_user_input, result):
            return result

        routed = await self._drive_generic_callback(flow, detector)
        self.assertEqual(routed["step_id"], "manual_confirm")
        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()

        # A NORMAL entry (the PN was found immediately), carrying the chosen
        # strategy canonically in data.
        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["collector_pn"], self.FULL_PN)
        self.assertEqual(created["data"].get("entry_role", ""), "")
        self.assertEqual(
            created["data"][CONF_CONNECTION_STRATEGY],
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertNotIn(CONF_CONNECTION_STRATEGY, created.get("options") or {})

    async def test_generic_manual_inbound_never_adopts_a_foreign_candidate(self) -> None:
        # Item 3 regression: ONE unclaimed strong collector exists globally, but
        # this generic manual inbound flow has no link to it (no expected PN). It
        # may belong to another pending flow, so it must NOT become this entry.
        from custom_components.eybond_local.const import CONF_CONNECTION_STRATEGY

        flow = self._make_flow()
        registry = self._install_registry(
            flow, [self._inventory_session(self.NEW_SESSION, self.FULL_PN)]
        )
        assert not flow._verification_expected_pn

        with patch.object(
            flow,
            "_async_probe_manual_target",
            side_effect=AssertionError("inbound must not probe"),
        ):
            created = await flow.async_step_manual(
                self._manual_input("", connection_strategy="inbound")
            )

        # A PENDING entry, never a normal entry wearing the stranger's PN.
        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["entry_role"], "pending_collector")
        self.assertEqual(created["data"]["collector_pn"], "")
        self.assertNotEqual(
            getattr(flow, "_test_unique_id", ""), f"collector:{self.FULL_PN}"
        )
        # The stranger's session was never claimed by this flow.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        # And no user_confirmed_session evidence was invented.
        self.assertNotIn("connection_strategy_evidence", created["data"])
        self.assertEqual(created["data"][CONF_CONNECTION_STRATEGY], "inbound")

    # ---- generic manual callback_on_demand: active attempt + shared matcher ----

    def _recording_detector(self, *, results=(), own_triggers=1, extra_triggers=0, on_detect=None):
        """A detector that records its trigger(s) in the shared ledger, like the real one.

        ``extra_triggers`` simulates a CONCURRENT trigger from another flow/entry.
        Passive inventory must never be consulted on a callback attempt.
        """

        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        class _Detector:
            async def async_passive_detect(self, **_kwargs):
                raise AssertionError(
                    "a callback attempt must never short-circuit on passive inventory"
                )

            async def async_auto_detect(self, **_kwargs):
                ledger = get_callback_trigger_ledger()
                for _ in range(own_triggers):
                    ledger.record(target="ours", source="test_attempt")
                for _ in range(extra_triggers):
                    ledger.record(target="other", source="concurrent_flow")
                if on_detect is not None:
                    on_detect()
                return results

        return _Detector()

    async def _drive_generic_callback(self, flow, detector, collector_ip="192.168.1.60"):
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=detector,
        ):
            return await flow.async_step_manual(
                self._manual_input(collector_ip, connection_strategy="callback_on_demand")
            )

    async def test_manual_callback_attempt_owns_one_passive_discovery_scope(self) -> None:
        flow = self._make_flow()
        inventory: list[dict] = []
        self._install_registry(flow, inventory)
        events: list[tuple[str, str]] = []

        @contextmanager
        def _scope(_hass, scope_id):
            events.append(("begin", scope_id))
            retained: set[str] = set()
            try:
                yield retained
            finally:
                events.append(("end", scope_id))

        def _answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))

        detector = self._recording_detector(
            results=(self._manual_result_with_pn(self.FULL_PN),),
            on_detect=_answers,
        )
        with patch(
            "custom_components.eybond_local.passive_discovery."
            "active_callback_probe_scope",
            new=_scope,
        ):
            routed = await self._drive_generic_callback(flow, detector)

        self.assertEqual(routed["step_id"], "manual_confirm")
        self.assertEqual([event for event, _scope_id in events], ["begin", "end"])
        self.assertEqual(events[0][1], events[1][1])
        self.assertTrue(events[0][1].startswith("manual_callback:"))

    async def test_slow_detection_keeps_proven_callback_identity(self) -> None:
        """Driver timeout may not erase the collector session/PN already proved."""

        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        flow = self._make_flow()
        inventory: list[dict] = []
        registry = self._install_registry(flow, inventory)

        class _Detector:
            async def async_auto_detect(_self, **kwargs):
                get_callback_trigger_ledger().record(
                    target="ours", source="slow_detection_test"
                )
                inventory.append(
                    self._inventory_session(self.NEW_SESSION, self.FULL_PN)
                )
                # Simulate detector finalization just after its work deadline.
                await asyncio.sleep(0.01)
                result = replace(
                    self._manual_result_with_pn(self.FULL_PN),
                    last_error="target_detection_timeout",
                    next_action="manual_driver_selection",
                )
                return (result,)

        with patch(
            "custom_components.eybond_local.config_flow._MANUAL_PROBE_TIMEOUT",
            0.001,
        ), patch(
            "custom_components.eybond_local.config_flow._MANUAL_PROBE_WATCHDOG_TIMEOUT",
            0.1,
        ):
            routed = await self._drive_generic_callback(flow, _Detector())

        self.assertEqual(routed["step_id"], "manual_confirm")
        self.assertEqual(flow._manual_result.last_error, "target_detection_timeout")
        self.assertEqual(flow._manual_verified_full_pn, self.FULL_PN)
        owner = registry.owner_for_pn(self.FULL_PN)
        self.assertTrue(owner.startswith("callback_verification:"))
        self.assertEqual(registry.claimed_session_id(owner), self.NEW_SESSION)

    async def test_generic_callback_rejects_pre_existing_foreign_session(self) -> None:
        # BLOCKER 1: a foreign strong session already exists; the user types a
        # DIFFERENT collector address and chooses callback_on_demand. The active
        # attempt reaches the target and a NEW matching session appears. The old
        # foreign session must never be adopted and must stay unclaimed.
        flow = self._make_flow()
        inventory = [self._inventory_session("foreign-1", self.OTHER_FULL_PN)]
        registry = self._install_registry(flow, inventory)

        def _target_answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))

        detector = self._recording_detector(
            results=(self._manual_result_with_pn(self.FULL_PN),),
            on_detect=_target_answers,
        )

        async def _passthrough_enrich(_user_input, result):
            return result

        routed = await self._drive_generic_callback(flow, detector)
        self.assertEqual(routed["step_id"], "manual_confirm")
        # Only the target identity was claimed ...
        self.assertEqual(flow._manual_verified_full_pn, self.FULL_PN)
        owner = registry.owner_for_pn(self.FULL_PN)
        self.assertTrue(owner.startswith("callback_verification:"))
        self.assertEqual(registry.claimed_session_id(owner), self.NEW_SESSION)
        # ... and the pre-existing stranger is untouched.
        self.assertEqual(registry.owner_for_pn(self.OTHER_FULL_PN), "")

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()
        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["collector_pn"], self.FULL_PN)

    async def test_generic_callback_without_detector_pn_creates_no_normal_entry(self) -> None:
        # BLOCKER 1: one pre-existing foreign session; the active attempt does NOT
        # confirm a PN. No normal entry, and the foreign PN is never assigned.
        flow = self._make_flow()
        registry = self._install_registry(
            flow, [self._inventory_session("foreign-1", self.OTHER_FULL_PN)]
        )
        detector = self._recording_detector(results=())

        routed = await self._drive_generic_callback(flow, detector)

        self._assert_callback_failure_menu(flow, routed, "callback_timeout")
        self.assertEqual(flow._manual_verified_full_pn, "")
        self.assertEqual(registry.owner_for_pn(self.OTHER_FULL_PN), "")
        self.assertNotEqual(
            getattr(flow, "_test_unique_id", ""), f"collector:{self.OTHER_FULL_PN}"
        )

    async def test_concurrent_trigger_during_active_attempt_is_interference(self) -> None:
        # BLOCKER 2 (A): our trigger fires AND a concurrent one does. Even though
        # the detector returns the right PN and a matching session appeared, the
        # answer is not attributable to us -> interference, and nothing is claimed.
        flow = self._make_flow()
        inventory: list[dict] = []
        registry = self._install_registry(flow, inventory)

        def _answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))

        detector = self._recording_detector(
            results=(self._manual_result_with_pn(self.FULL_PN),),
            own_triggers=1,
            extra_triggers=1,  # someone else triggered concurrently
            on_detect=_answers,
        )

        routed = await self._drive_generic_callback(flow, detector)

        self._assert_callback_failure_menu(
            flow, routed, "callback_trigger_interference"
        )
        self.assertEqual(flow._manual_verified_full_pn, "")
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(flow._verification_claim_owner, "")

    async def test_timeout_after_our_trigger_is_timeout_not_interference(self) -> None:
        # BLOCKER 2 (B): our one trigger DID go out, then nothing answered. The
        # provenance is correct, so this is a plain timeout -- not interference.
        flow = self._make_flow()
        registry = self._install_registry(flow, [])
        detector = self._recording_detector(results=(), own_triggers=1)

        routed = await self._drive_generic_callback(flow, detector)

        self._assert_callback_failure_menu(flow, routed, "callback_timeout")
        self.assertNotEqual(
            flow._manual_result.last_error, "callback_trigger_interference"
        )
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    async def test_generic_callback_success_claims_and_stamps_canonical_strategy(self) -> None:
        # BLOCKER 2 (C): exactly one trigger; detector PN == the new strong
        # session; claim + handoff exist; the entry carries callback_on_demand.
        from custom_components.eybond_local.const import (
            CONF_CONNECTION_STRATEGY,
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

        flow = self._make_flow()
        inventory: list[dict] = []
        registry = self._install_registry(flow, inventory)

        def _answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))

        detector = self._recording_detector(
            results=(self._manual_result_with_pn(self.FULL_PN),),
            own_triggers=1,
            on_detect=_answers,
        )

        async def _passthrough_enrich(_user_input, result):
            return result

        routed = await self._drive_generic_callback(flow, detector)
        self.assertEqual(routed["step_id"], "manual_confirm")

        owner = registry.owner_for_pn(self.FULL_PN)
        self.assertTrue(owner.startswith("callback_verification:"))

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()

        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(
            created["data"][CONF_CONNECTION_STRATEGY],
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertNotIn(CONF_CONNECTION_STRATEGY, created.get("options") or {})
        # The handoff was prepared for the certified identity.
        self.assertTrue(flow._callback_ownership_handed_off)
        self.assertEqual(
            registry.prepared_handoff_identity(owner, self.FULL_PN), self.FULL_PN
        )

    async def test_pre_existing_session_never_substitutes_for_the_answer(self) -> None:
        # BLOCKER 2 (D): the ONLY strong session existed BEFORE our trigger and
        # matches the detector's PN. Baseline still rules it out: it cannot be an
        # answer to a trigger sent after it.
        flow = self._make_flow()
        registry = self._install_registry(
            flow, [self._inventory_session("pre-existing", self.FULL_PN)]
        )
        detector = self._recording_detector(
            results=(self._manual_result_with_pn(self.FULL_PN),), own_triggers=1
        )

        routed = await self._drive_generic_callback(flow, detector)

        self._assert_callback_failure_menu(flow, routed, "callback_timeout")
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    async def test_manual_callback_timeout_leaves_no_registry_claim(self) -> None:
        # Item 2/5 cleanup: a timeout (no confirming strong session) must leave
        # nothing owned.
        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        registry = self._install_registry(flow, [])
        with patch.object(
            flow,
            "_async_probe_manual_target",
            return_value=OnboardingResult(connection_mode="manual"),
        ):
            result = await flow.async_step_manual(self._manual_input("192.168.1.60"))
        self._assert_callback_failure_menu(flow, result, "callback_timeout")
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(flow._verification_claim_owner, "")

    async def test_expected_short_pn_is_not_saved_as_durable_identity(self) -> None:
        # Item 4: a discovery-time expected/short PN is NOT durable evidence. With
        # no registry-certified strong session, no PN is persisted, no normal
        # entry is created, and the expected PN never leaks into a collector id.
        flow = self._make_flow()
        flow._manual_config = self._manual_input("192.168.1.60")
        flow._manual_result = OnboardingResult(
            connection_mode="known_ip", next_action="create_pending_entry"
        )
        flow._verification_expected_pn = self.FULL_PN
        flow._manual_verified_full_pn = ""

        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            result = await flow.async_step_manual_create_pending()

        # Saved as a PENDING entry; the expected/short PN never became durable.
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"]["entry_role"], "pending_collector")
        self.assertEqual(result["data"]["collector_pn"], "")
        self.assertNotEqual(
            getattr(flow, "_test_unique_id", None), f"collector:{self.FULL_PN}"
        )

    def _pn_less_entry(self, entry_id: str = "entry-broken"):
        from custom_components.eybond_local.const import CONF_COLLECTOR_PN

        return _FakeSetupEntry(
            entry_id,
            {
                "connection_type": "eybond",
                "connection_mode": "known_ip",
                "collector_operation_mode": "home_assistant_only",
                CONF_COLLECTOR_PN: "",
            },
        )

    async def test_reconfigure_binds_pn_and_updates_existing_pn_less_entry(self) -> None:
        # Item 8: reconfigure runs the SAME manual callback verification against an
        # existing PN-less entry, binds the strong full PN, and updates the entry
        # in place (no delete/re-add), keeping the verified strategy/evidence and
        # committing the ownership handoff under a unique per-attempt owner.
        from custom_components.eybond_local.const import (
            CONF_COLLECTOR_PN,
            CONF_CONNECTION_STRATEGY,
            CONF_CONNECTION_STRATEGY_EVIDENCE,
        )

        flow = self._make_flow()
        entry = self._pn_less_entry()
        flow.hass.config_entries._entries.append(entry)
        flow.context = {"entry_id": "entry-broken", "source": "reconfigure"}

        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        with patch.object(flow, "_async_probe_manual_target", side_effect=_probe):
            result = await flow.async_step_reconfigure(self._manual_input("192.168.1.60"))

        self.assertEqual(result["type"], "abort")
        self.assertEqual(result["reason"], "reconfigure_successful")
        self.assertEqual(entry.data[CONF_COLLECTOR_PN], self.FULL_PN)
        self.assertEqual(entry.data[CONF_CONNECTION_STRATEGY], "callback_on_demand")
        self.assertEqual(entry.data[CONF_CONNECTION_STRATEGY_EVIDENCE], "callback_trigger")
        self.assertEqual(entry.unique_id, f"collector:{self.FULL_PN}")
        self.assertTrue(flow._callback_ownership_handed_off)
        owner = registry.owner_for_pn(self.FULL_PN)
        self.assertTrue(owner.startswith("callback_verification:"))

    async def test_reconfigure_without_strong_pn_does_not_masquerade_as_normal(self) -> None:
        # Item 8: until repair actually binds a strong PN, reconfigure must NOT
        # silently "fix" the entry -- it re-prompts, leaving the entry PN-less.
        from custom_components.eybond_local.const import CONF_COLLECTOR_PN

        flow = self._make_flow()
        entry = self._pn_less_entry()
        flow.hass.config_entries._entries.append(entry)
        flow.context = {"entry_id": "entry-broken", "source": "reconfigure"}
        registry = self._install_registry(flow, [])

        with patch.object(
            flow,
            "_async_probe_manual_target",
            return_value=OnboardingResult(connection_mode="known_ip"),
        ):
            result = await flow.async_step_reconfigure(self._manual_input("192.168.1.60"))

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "reconfigure")
        self.assertIn(
            result["errors"]["base"],
            ("callback_timeout", "callback_identity_unverified"),
        )
        self.assertEqual(entry.data.get(CONF_COLLECTOR_PN, ""), "")
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    async def test_reconfigure_ambiguous_when_two_new_collectors_answer(self) -> None:
        # Item 6: bind-any repair must NEVER pick the first inventory element. Two
        # distinct new strong PNs -> ambiguity, and nothing is bound.
        from custom_components.eybond_local.const import CONF_COLLECTOR_PN

        flow = self._make_flow()
        entry = self._pn_less_entry()
        flow.hass.config_entries._entries.append(entry)
        flow.context = {"entry_id": "entry-broken", "source": "reconfigure"}
        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            inventory.append(self._inventory_session("listener-18899-3", self.OTHER_FULL_PN))
            return OnboardingResult(connection_mode="known_ip")  # no single probe PN

        with patch.object(flow, "_async_probe_manual_target", side_effect=_probe):
            result = await flow.async_step_reconfigure(self._manual_input("192.168.1.60"))

        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"]["base"], "callback_identity_ambiguous")
        self.assertEqual(entry.data.get(CONF_COLLECTOR_PN, ""), "")
        # Neither identity was claimed by this flow.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(registry.owner_for_pn(self.OTHER_FULL_PN), "")

    async def test_reconfigure_not_required_for_healthy_entry(self) -> None:
        # Item 5: a healthy PN-bound entry must not run identity repair; the flow
        # aborts without changing strategy or triggering a callback.
        from custom_components.eybond_local.const import (
            CONF_COLLECTOR_PN,
            CONF_CONNECTION_STRATEGY,
        )

        flow = self._make_flow()
        entry = _FakeSetupEntry(
            "entry-healthy",
            {
                "connection_type": "eybond",
                CONF_COLLECTOR_PN: self.FULL_PN,
                CONF_CONNECTION_STRATEGY: "inbound",
                "connection_strategy_evidence": "reboot_reconnect",
            },
        )
        flow.hass.config_entries._entries.append(entry)
        flow.context = {"entry_id": "entry-healthy", "source": "reconfigure"}

        with patch.object(
            flow,
            "_async_probe_manual_target",
            side_effect=AssertionError("healthy entry must not be probed/triggered"),
        ):
            result = await flow.async_step_reconfigure(self._manual_input("192.168.1.60"))

        self.assertEqual(result["type"], "abort")
        self.assertEqual(result["reason"], "reconfigure_not_required")
        # Strategy untouched: a healthy inbound entry is NOT flipped to callback.
        self.assertEqual(entry.data[CONF_CONNECTION_STRATEGY], "inbound")

    async def test_reconfigure_collision_with_unloaded_same_pn_entry(self) -> None:
        # Item 7: a DIFFERENT config entry already owns collector:{pn} (unloaded,
        # so it holds NO registry claim, but the unique id is taken). Repair must
        # abort already_configured, leave the broken entry PN-less, and release
        # the attempt's claim.
        from custom_components.eybond_local.const import CONF_COLLECTOR_PN

        flow = self._make_flow()
        broken = self._pn_less_entry("entry-broken")
        healthy = _FakeSetupEntry("entry-healthy", {CONF_COLLECTOR_PN: self.FULL_PN})
        healthy.unique_id = f"collector:{self.FULL_PN}"
        flow.hass.config_entries._entries.extend([broken, healthy])
        flow.context = {"entry_id": "entry-broken", "source": "reconfigure"}
        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        with patch.object(flow, "_async_probe_manual_target", side_effect=_probe):
            result = await flow.async_step_reconfigure(self._manual_input("192.168.1.60"))

        self.assertEqual(result["type"], "abort")
        self.assertEqual(result["reason"], "already_configured")
        self.assertEqual(broken.data.get(CONF_COLLECTOR_PN, ""), "")  # unchanged
        # The attempt's claim was released -> the other entry's identity is free.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    # ---- Path B symmetry: passive inbound restart/reconnect handoff (item 2) ----

    async def test_passive_inbound_verification_hands_off_to_entry_at_setup(self) -> None:
        # Item 2 -- the full production Path B: passive discovery -> restart/
        # reconnect verification -> confirm/create (prepare_handoff) -> setup
        # (complete_handoff) -> the runtime owns the session. Symmetric with the
        # manual callback e2e.
        from custom_components.eybond_local import (
            _register_entry_callback_session_claim,
        )

        flow = self._make_flow()
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)
        await flow.async_step_integration_discovery(self._discovery_info())
        new_session = self._inventory_session(self.NEW_SESSION, self.FULL_PN)

        class _FakeChannel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                inventory.clear()
                inventory.append(new_session)

            def is_connected(self) -> bool:
                return False

            async def async_close(self) -> None:
                return None

        with patch.object(config_flow_module, "ObservedSessionRestartChannel", _FakeChannel):
            await self._drive_verification(flow)

        # Success HELD the claim (item 2), under a unique per-attempt owner.
        owner = registry.owner_for_pn(self.FULL_PN)
        self.assertTrue(owner.startswith("strategy_verification:"))

        with (
            patch(
                "custom_components.eybond_local.config_flow.SharedEybondTransport",
                side_effect=AssertionError("passive confirm must not start payload transport"),
            ),
            patch(
                "custom_components.eybond_local.config_flow.SharedCollectorAtTransport",
                side_effect=AssertionError("passive confirm must not start AT transport"),
            ),
        ):
            created = await flow.async_step_confirm({"poll_mode": "auto"})

        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["collector_pn"], self.FULL_PN)
        # Confirm committed the handoff; flow cleanup must not release it.
        self.assertTrue(flow._callback_ownership_handed_off)
        flow.async_remove()
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), owner)

        # PRODUCTION setup completes the handoff to the durable entry_id. (The
        # inbound claim keeps its pre-restart session id; the runtime rebinds the
        # next same-PN session by durable PN, which is what ownership guarantees.)
        entry = _FakeSetupEntry("entry-inbound", created["data"])
        _register_entry_callback_session_claim(flow.hass, entry)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "entry-inbound")

    # ---- item 4: rollback after a terminal helper throws AFTER prepare_handoff ----

    async def test_create_terminal_exception_rolls_back_committed_handoff(self) -> None:
        flow = self._make_flow()
        flow._verification_expected_pn = self.FULL_PN
        flow._verification_old_session_id = self.OLD_SESSION
        inventory = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        async def _passthrough_enrich(_user_input, result):
            return result

        boom = RuntimeError("HA create failed")

        def _raise(*_a, **_k):
            raise boom

        with patch.object(
            flow, "_async_probe_manual_target", side_effect=_probe
        ), patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            await flow.async_step_manual(self._manual_input("192.168.1.60"))
            with patch.object(flow, "async_create_entry", side_effect=_raise):
                with self.assertRaises(RuntimeError):
                    await flow.async_step_manual_create_pending()

        # The committed handoff was rolled back: flag reset, owner released, so a
        # retry (or another flow) is not permanently blocked.
        self.assertFalse(flow._callback_ownership_handed_off)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(flow._verification_claim_owner, "")

    async def test_reconfigure_terminal_exception_rolls_back_committed_handoff(self) -> None:
        flow = self._make_flow()
        entry = self._pn_less_entry()
        flow.hass.config_entries._entries.append(entry)
        flow.context = {"entry_id": "entry-broken", "source": "reconfigure"}
        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        async def _probe(_flat_input):
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))
            return self._manual_result_with_pn(self.FULL_PN)

        boom = RuntimeError("HA update failed")

        def _raise(*_a, **_k):
            raise boom

        with patch.object(flow, "_async_probe_manual_target", side_effect=_probe):
            with patch.object(flow, "async_update_reload_and_abort", side_effect=_raise):
                with self.assertRaises(RuntimeError):
                    await flow.async_step_reconfigure(self._manual_input("192.168.1.60"))

        self.assertFalse(flow._callback_ownership_handed_off)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(entry.data.get("collector_pn", ""), "")  # entry untouched


    # --- retry must be a WHOLE new attempt, never a bare re-probe -------------
    #
    # async_step_manual_probe_again used to call the probe directly and route on,
    # keeping the previous attempt's baseline, ledger generation, verified PN and
    # registry claim. That let a second probe reaching collector B be combined
    # with the first attempt's proof/claim for collector A. Every active manual
    # callback path now runs the one shared lifecycle helper.

    async def _first_attempt_claiming(self, flow, inventory, pn, session_id):
        """Drive a successful first manual attempt; return its claim owner."""

        def _answers():
            inventory.append(self._inventory_session(session_id, pn))

        detector = self._recording_detector(
            results=(self._manual_result_with_pn(pn),), on_detect=_answers
        )
        routed = await self._drive_generic_callback(flow, detector)
        self.assertEqual(routed["step_id"], "manual_confirm")
        self.assertEqual(flow._manual_verified_full_pn, pn)
        return flow._verification_claim_owner

    async def _probe_again(self, flow, detector):
        with patch(
            "custom_components.eybond_local.config_flow.create_onboarding_manager",
            return_value=detector,
        ):
            return await flow.async_step_manual_probe_again()

    async def test_probe_again_to_other_collector_rebinds_wholly(self) -> None:
        # A. First attempt claims A; "probe again" reaches B on a new strong
        # session. The old claim on A must be gone, the new claim must belong to
        # B, and the entry may only ever be created as B.
        flow = self._make_flow()
        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        owner_a = await self._first_attempt_claiming(
            flow, inventory, self.FULL_PN, self.OLD_SESSION
        )
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), owner_a)

        def _b_answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.OTHER_FULL_PN))

        routed = await self._probe_again(
            flow,
            self._recording_detector(
                results=(self._manual_result_with_pn(self.OTHER_FULL_PN),),
                on_detect=_b_answers,
            ),
        )
        self.assertEqual(routed["step_id"], "manual_confirm")

        # The first attempt's claim on A is released, not merely overwritten.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(registry.claimed_identity(owner_a), "")
        # The new claim is a NEW owner, bound to B's new session.
        owner_b = registry.owner_for_pn(self.OTHER_FULL_PN)
        self.assertTrue(owner_b.startswith("callback_verification:"))
        self.assertNotEqual(owner_b, owner_a)
        self.assertEqual(registry.claimed_session_id(owner_b), self.NEW_SESSION)
        self.assertEqual(flow._manual_verified_full_pn, self.OTHER_FULL_PN)
        self.assertEqual(flow._verification_claim_owner, owner_b)

        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()

        # The entry is B, and ONLY B ...
        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["collector_pn"], self.OTHER_FULL_PN)
        self.assertEqual(getattr(flow, "_test_unique_id", ""), f"collector:{self.OTHER_FULL_PN}")
        # ... the handoff certifies B ...
        self.assertEqual(
            registry.prepared_handoff_identity(owner_b, self.OTHER_FULL_PN),
            self.OTHER_FULL_PN,
        )
        # ... and no owner is left holding A anywhere in the registry.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    async def test_probe_again_timeout_drops_first_attempt_proof(self) -> None:
        # B. First attempt claims A; the retry times out. The claim on A must be
        # released, the verified PN cleared, and no normal entry for A may be
        # creatable from what the flow still holds.
        flow = self._make_flow()
        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        owner_a = await self._first_attempt_claiming(
            flow, inventory, self.FULL_PN, self.OLD_SESSION
        )
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), owner_a)

        # Our trigger fires; nothing new answers it.
        routed = await self._probe_again(flow, self._recording_detector(results=()))

        self._assert_callback_failure_menu(flow, routed, "callback_timeout")
        self.assertEqual(flow._manual_verified_full_pn, "")
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(flow._verification_claim_owner, "")
        self.assertFalse(flow._callback_ownership_handed_off)

        # The stale identity cannot be turned into a normal entry for A.
        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()
        self.assertNotEqual(created.get("data", {}).get("collector_pn", ""), self.FULL_PN)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    async def test_probe_again_interference_drops_first_attempt_proof(self) -> None:
        # C. First attempt claims A; during the retry a CONCURRENT trigger fires,
        # so even a matching session is not attributable to us. Old claim gone,
        # no new claim, no stale entry.
        flow = self._make_flow()
        inventory: list[dict[str, object]] = []
        registry = self._install_registry(flow, inventory)

        owner_a = await self._first_attempt_claiming(
            flow, inventory, self.FULL_PN, self.OLD_SESSION
        )

        def _answers():
            inventory.append(self._inventory_session(self.NEW_SESSION, self.FULL_PN))

        routed = await self._probe_again(
            flow,
            self._recording_detector(
                results=(self._manual_result_with_pn(self.FULL_PN),),
                own_triggers=1,
                extra_triggers=1,
                on_detect=_answers,
            ),
        )

        self._assert_callback_failure_menu(
            flow, routed, "callback_trigger_interference"
        )
        self.assertEqual(flow._manual_verified_full_pn, "")
        # The old claim is released and NO new claim was taken.
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(registry.claimed_identity(owner_a), "")
        self.assertEqual(flow._verification_claim_owner, "")

        async def _passthrough_enrich(_user_input, result):
            return result

        with patch.object(
            flow,
            "_async_enrich_manual_pending_collector_profile",
            side_effect=_passthrough_enrich,
        ):
            created = await flow.async_step_manual_create_pending()
        self.assertNotEqual(created.get("data", {}).get("collector_pn", ""), self.FULL_PN)

    async def test_probe_again_uses_fresh_baseline_and_generation(self) -> None:
        # F. The retry must snapshot its OWN baseline and ledger generation, not
        # reuse the first attempt's. The proof: at retry time the first attempt's
        # session is already IN the baseline (so it can never be re-counted as the
        # answer), and the generation has advanced past the first trigger.
        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        flow = self._make_flow()
        inventory: list[dict[str, object]] = []
        self._install_registry(flow, inventory)
        seen: list[tuple[frozenset, int]] = []

        def _record_and_answer(session_id, pn):
            def _inner():
                seen.append(
                    (flow._manual_callback_baseline, flow._manual_trigger_generation_before)
                )
                inventory.append(self._inventory_session(session_id, pn))

            return _inner

        await self._drive_generic_callback(
            flow,
            self._recording_detector(
                results=(self._manual_result_with_pn(self.FULL_PN),),
                on_detect=_record_and_answer(self.OLD_SESSION, self.FULL_PN),
            ),
        )
        await self._probe_again(
            flow,
            self._recording_detector(
                results=(self._manual_result_with_pn(self.OTHER_FULL_PN),),
                on_detect=_record_and_answer(self.NEW_SESSION, self.OTHER_FULL_PN),
            ),
        )

        self.assertEqual(len(seen), 2)
        (baseline_1, gen_1), (baseline_2, gen_2) = seen
        # A fresh baseline: empty at first, and containing the first attempt's
        # own session at retry time -- not the stale first snapshot.
        self.assertEqual(baseline_1, frozenset())
        self.assertIn(self.OLD_SESSION, baseline_2)
        self.assertNotEqual(baseline_1, baseline_2)
        # A fresh generation, sampled after the first attempt's trigger.
        self.assertGreater(gen_2, gen_1)
        self.assertEqual(gen_2, gen_1 + 1)
        self.assertLessEqual(
            gen_2, get_callback_trigger_ledger().snapshot_generation()
        )


    async def test_handoff_flag_stays_false_when_nothing_was_prepared(self) -> None:
        # Item 5: the registry reports "no claim under this owner" (False). The
        # flow must NOT record a handoff it never made: _release_verification_claim
        # deliberately stops releasing once _callback_ownership_handed_off is set,
        # so a lying flag would suppress this flow's own cleanup and strand the
        # owner. No abort either -- entry setup re-claims and fails closed there.
        flow = self._make_flow()
        registry = self._install_registry(flow, [])
        flow._verification_registry = registry
        flow._verification_claim_owner = "callback_verification:vanished"

        self.assertIsNone(flow._prepare_ownership_handoff(self.FULL_PN))

        self.assertFalse(flow._callback_ownership_handed_off)
        self.assertEqual(flow._verification_claim_owner, "")
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")

    async def test_handoff_refusal_aborts_and_releases_own_claim(self) -> None:
        # Item 5 (other half): a REFUSED handoff (ValueError) must abort without
        # creating an entry and must release this flow's own claim.
        flow = self._make_flow()
        sessions = [self._inventory_session(self.OLD_SESSION, self.FULL_PN)]
        registry = self._install_registry(flow, sessions)
        registry.claim_session("callback_verification:mine", session_id=self.OLD_SESSION)
        registry.promote_claim_to_full_pn("callback_verification:mine", self.FULL_PN)
        flow._verification_registry = registry
        flow._verification_claim_owner = "callback_verification:mine"

        # The claim stands for A; handing off B is refused by the registry.
        aborted = flow._prepare_ownership_handoff(self.OTHER_FULL_PN)

        self.assertEqual(aborted["type"], "abort")
        self.assertEqual(aborted["reason"], "already_configured")
        self.assertFalse(flow._callback_ownership_handed_off)
        self.assertEqual(registry.owner_for_pn(self.FULL_PN), "")
        self.assertEqual(registry.owner_for_pn(self.OTHER_FULL_PN), "")


if __name__ == "__main__":
    unittest.main()
