from __future__ import annotations

import asyncio
import dataclasses
import importlib
import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import unittest


@dataclasses.dataclass
class _FakeInverter:
    """Minimal inverter stand-in supporting dataclasses.replace for overlay-merge tests."""

    capabilities: tuple = ()
    capability_groups: tuple = ()
    register_schema_name: str = ""
from unittest.mock import AsyncMock, PropertyMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _ensure_module(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    return module


def _install_coordinator_stubs() -> None:
    custom_components = _ensure_module("custom_components")
    eybond_local = _ensure_module("custom_components.eybond_local")
    runtime_package = _ensure_module("custom_components.eybond_local.runtime")
    homeassistant = _ensure_module("homeassistant")
    components = _ensure_module("homeassistant.components")
    components_network = _ensure_module("homeassistant.components.network")
    components_network_util = _ensure_module("homeassistant.components.network.util")
    persistent_notification = _ensure_module(
        "homeassistant.components.persistent_notification"
    )
    config_entries = _ensure_module("homeassistant.config_entries")
    helpers = _ensure_module("homeassistant.helpers")
    device_registry = _ensure_module("homeassistant.helpers.device_registry")
    network = _ensure_module("homeassistant.helpers.network")
    update_coordinator = _ensure_module("homeassistant.helpers.update_coordinator")
    util = _ensure_module("homeassistant.util")
    dt = _ensure_module("homeassistant.util.dt")
    util_logging = _ensure_module("homeassistant.util.logging")

    class ConfigEntry:
        pass

    class DeviceInfo(dict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

    class DataUpdateCoordinator:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, *args, **kwargs):
            del args, kwargs

    config_entries.ConfigEntry = ConfigEntry
    device_registry.DeviceInfo = DeviceInfo
    device_registry.async_get = lambda hass: None
    update_coordinator.DataUpdateCoordinator = DataUpdateCoordinator
    util.dt = dt
    util.logging = util_logging
    util_logging.log_exception = lambda *args, **kwargs: None

    custom_components.__path__ = [str(REPO_ROOT / "custom_components")]
    eybond_local.__path__ = [str(REPO_ROOT / "custom_components" / "eybond_local")]
    runtime_package.__path__ = [
        str(REPO_ROOT / "custom_components" / "eybond_local" / "runtime")
    ]

    custom_components.eybond_local = eybond_local
    eybond_local.runtime = runtime_package
    homeassistant.components = components
    homeassistant.config_entries = config_entries
    homeassistant.helpers = helpers
    homeassistant.util = util
    components.persistent_notification = persistent_notification
    components.network = components_network
    components_network.util = components_network_util
    components_network_util.async_get_source_ip = lambda *args, **kwargs: "10.10.10.10"
    helpers.device_registry = device_registry
    helpers.network = network
    helpers.update_coordinator = update_coordinator
    network.NoURLAvailableError = RuntimeError
    network.get_url = lambda *args, **kwargs: "http://127.0.0.1:8123"

    const = _ensure_module("custom_components.eybond_local.const")
    const.CONF_COLLECTOR_IP = "collector_ip"
    const.CONF_COLLECTOR_CLOUD_FAMILY = "collector_cloud_family"
    const.CONF_COLLECTOR_OPERATION_MODE = "collector_operation_mode"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT = "collector_original_server_endpoint"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT = "collector_original_server_endpoint_observed_at"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY = "collector_original_server_endpoint_profile_key"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE = "collector_original_server_endpoint_source"
    const.CONF_COLLECTOR_PN = "collector_pn"
    const.CONF_CONNECTION_TYPE = "connection_type"
    const.CONF_CONNECTION_MODE = "connection_mode"
    const.CONF_CONTROL_MODE = "control_mode"
    const.CONF_DETECTED_MODEL = "detected_model"
    const.CONF_DETECTED_SERIAL = "detected_serial"
    const.CONF_DETECTION_CONFIDENCE = "detection_confidence"
    const.CONF_DISCOVERY_INTERVAL = "discovery_interval"
    const.CONF_DISCOVERY_TARGET = "discovery_target"
    const.CONF_DRIVER_HINT = "driver_hint"
    const.CONF_HEARTBEAT_INTERVAL = "heartbeat_interval"
    const.CONF_POLL_INTERVAL = "poll_interval"
    const.CONF_POLL_MODE = "poll_mode"
    const.CONF_PROXY_CAPTURE_DURATION_MINUTES = "proxy_capture_duration_minutes"
    const.CONF_SERVER_IP = "server_ip"
    const.CONF_SMARTESS_COLLECTOR_VERSION = "smartess_collector_version"
    const.CONF_SMARTESS_DEVICE_ADDRESS = "smartess_device_address"
    const.CONF_SMARTESS_PROFILE_KEY = "smartess_profile_key"
    const.CONF_SMARTESS_PROTOCOL_ASSET_ID = "smartess_protocol_asset_id"
    const.CONF_TCP_PORT = "tcp_port"
    const.CONF_UDP_PORT = "udp_port"
    const.BUILTIN_SCHEMA_PREFIX = "builtin:"
    const.DEFAULT_COLLECTOR_IP = ""
    const.DEFAULT_COLLECTOR_OPERATION_MODE = "smartess_cloud_home_assistant"
    const.DEFAULT_CONTROL_MODE = "limited"
    const.DEFAULT_DISCOVERY_INTERVAL = 30
    const.DEFAULT_DISCOVERY_TARGET = ""
    const.DEFAULT_HEARTBEAT_INTERVAL = 30
    const.DEFAULT_POLL_INTERVAL = 30
    const.DEFAULT_POLL_MODE = "auto"
    const.DEFAULT_PROXY_CAPTURE_DURATION_MINUTES = 10
    const.DEFAULT_TCP_PORT = 8899
    const.DEFAULT_UDP_PORT = 48899
    const.COLLECTOR_OPERATION_SMARTESS_AND_HA = "smartess_cloud_home_assistant"
    const.COLLECTOR_OPERATION_HA_ONLY = "home_assistant_only"
    const.CONTROL_MODE_AUTO = "auto"
    const.CONTROL_MODE_FULL = "full"
    const.CONTROL_MODE_READ_ONLY = "read_only"
    const.DOMAIN = "eybond_local"
    const.DRIVER_HINT_AUTO = "auto"
    const.POLL_MODE_AUTO = "auto"
    const.POLL_MODE_MANUAL = "manual"
    const.LOCAL_DIAGNOSTIC_RUNS_DIR = "diagnostic_runs"
    const.LOCAL_METADATA_DIR = "eybond_local"
    const.COLLECTOR_OPERATION_MODES = (
        "smartess_cloud_home_assistant",
        "home_assistant_only",
    )
    const.MAX_PROXY_CAPTURE_DURATION_MINUTES = 120
    const.MIN_PROXY_CAPTURE_DURATION_MINUTES = 1
    const.LOCAL_METADATA_DIR = "eybond_local"

    connection_models = _ensure_module("custom_components.eybond_local.connection.models")
    connection_models.build_connection_spec = lambda *args, **kwargs: None

    entity_scope = importlib.import_module(
        "custom_components.eybond_local.collector.entity_scope"
    )

    control_policy = _ensure_module("custom_components.eybond_local.control_policy")
    control_policy.can_expose_capability = lambda *args, **kwargs: True
    control_policy.can_expose_preset = lambda *args, **kwargs: True
    control_policy.controls_enabled = lambda *args, **kwargs: True
    control_policy.controls_reason = lambda *args, **kwargs: ""
    control_policy.controls_summary = lambda *args, **kwargs: ""

    drivers_registry = _ensure_module("custom_components.eybond_local.drivers.registry")
    drivers_registry.get_driver = lambda *args, **kwargs: None
    drivers_registry.all_write_capabilities = lambda *args, **kwargs: []

    fixtures_utils = _ensure_module("custom_components.eybond_local.fixtures.utils")
    fixtures_utils.anonymize_fixture_json = lambda *args, **kwargs: None
    fixtures_utils.build_command_fixture_responses = lambda *args, **kwargs: None

    effective_metadata = _ensure_module(
        "custom_components.eybond_local.metadata.effective_metadata"
    )
    effective_metadata.resolve_effective_metadata_selection = (
        lambda *args, **kwargs: None
    )

    local_metadata = _ensure_module("custom_components.eybond_local.metadata.local_metadata")
    local_metadata.clear_local_metadata_loader_caches = lambda *args, **kwargs: None
    local_metadata.create_local_profile_draft = lambda *args, **kwargs: None
    local_metadata.create_local_schema_draft = lambda *args, **kwargs: None
    local_metadata.rollback_local_metadata_overrides = lambda *args, **kwargs: None

    smartess_draft = _ensure_module("custom_components.eybond_local.metadata.smartess_draft")

    class SmartEssKnownFamilyDraftPlan:
        pass

    smartess_draft.SmartEssKnownFamilyDraftPlan = SmartEssKnownFamilyDraftPlan
    smartess_draft.create_smartess_known_family_draft = lambda *args, **kwargs: None
    smartess_draft.resolve_smartess_known_family_draft_plan = (
        lambda *args, **kwargs: None
    )

    smartess_smg_bridge = _ensure_module(
        "custom_components.eybond_local.metadata.smartess_smg_bridge"
    )

    class SmartEssSmgBridgePlan:
        pass

    smartess_smg_bridge.SmartEssSmgBridgePlan = SmartEssSmgBridgePlan
    smartess_smg_bridge.create_smartess_smg_bridge_draft = lambda *args, **kwargs: None
    smartess_smg_bridge.resolve_smartess_smg_bridge_plan = (
        lambda *args, **kwargs: None
    )

    models = _ensure_module("custom_components.eybond_local.models")

    class CapabilityChoice:
        pass

    class CapabilityCondition:
        pass

    class CapabilityGroup:
        pass

    class CapabilityPreset:
        pass

    class CapabilityPresetItem:
        pass

    class CapabilityRecommendation:
        pass

    class BinarySensorDescription:
        pass

    class MeasurementDescription:
        pass

    class RegisterValueSpec:
        pass

    class WriteCapability:
        pass

    class DetectedInverter:
        pass

    class ProbeTarget:
        pass

    class RuntimeSnapshot:
        def __init__(self, values=None, inverter=None, collector=None, connected=True):
            self.values = values or {}
            self.inverter = inverter
            self.collector = collector
            self.connected = connected

    models.CapabilityChoice = CapabilityChoice
    models.CapabilityCondition = CapabilityCondition
    models.CapabilityGroup = CapabilityGroup
    models.CapabilityPreset = CapabilityPreset
    models.CapabilityPresetItem = CapabilityPresetItem
    models.CapabilityRecommendation = CapabilityRecommendation
    models.BinarySensorDescription = BinarySensorDescription
    models.MeasurementDescription = MeasurementDescription
    models.RegisterValueSpec = RegisterValueSpec
    models.RuntimeSnapshot = RuntimeSnapshot
    models.WriteCapability = WriteCapability
    models.DetectedInverter = DetectedInverter
    models.ProbeTarget = ProbeTarget
    models.decimals_for_divisor = lambda _divisor: 0

    runtime_factory = _ensure_module("custom_components.eybond_local.runtime.factory")
    runtime_factory.create_runtime_manager = lambda *args, **kwargs: None

    runtime_manager = _ensure_module("custom_components.eybond_local.runtime.manager")

    class RuntimeManager:
        pass

    runtime_manager.RuntimeManager = RuntimeManager

    schema = _ensure_module("custom_components.eybond_local.schema")
    schema.build_runtime_ui_schema = lambda *args, **kwargs: None
    schema.capability_write_exposure_allowed = lambda *args, **kwargs: True
    schema.preset_write_exposure_allowed = lambda *args, **kwargs: True

    support_bundle = _ensure_module("custom_components.eybond_local.support.bundle")
    support_bundle.build_support_bundle_payload = lambda *args, **kwargs: None
    support_bundle.export_support_bundle = lambda *args, **kwargs: None

    support_cloud = _ensure_module("custom_components.eybond_local.support.cloud_evidence")
    support_cloud.fetch_and_export_device_bundle_cloud_evidence = (
        lambda *args, **kwargs: None
    )
    support_cloud.fetch_and_export_smartess_device_bundle_cloud_evidence = (
        lambda *args, **kwargs: None
    )
    support_cloud.load_latest_cloud_evidence = lambda *args, **kwargs: None

    support_package = _ensure_module("custom_components.eybond_local.support.package")
    support_package.export_support_package = lambda *args, **kwargs: None
    support_package.support_packages_root = (
        lambda config_dir: Path(config_dir) / "eybond_local" / "support_packages"
    )

    support_proxy_capture = _ensure_module(
        "custom_components.eybond_local.support.proxy_capture"
    )
    support_proxy_capture.build_proxy_capture_overview = lambda *args, **kwargs: None

    support_proxy_session = _ensure_module(
        "custom_components.eybond_local.support.proxy_session"
    )
    support_proxy_session.build_proxy_capture_command = lambda *args, **kwargs: []
    support_proxy_session.build_proxy_capture_restore_trigger_path = (
        lambda *args, **kwargs: None
    )
    support_proxy_session.build_proxy_capture_trace_path = (
        lambda *args, **kwargs: None
    )
    support_proxy_session.inspect_proxy_capture_start_status = (
        lambda *args, **kwargs: {}
    )
    support_proxy_session.inspect_proxy_capture_trace = lambda *args, **kwargs: {}
    support_proxy_session.open_proxy_trace_output_file = lambda path: None
    support_proxy_session.summarize_proxy_capture_trace = (
        lambda *args, **kwargs: {}
    )

    support_proxy_trace = _ensure_module(
        "custom_components.eybond_local.support.proxy_trace"
    )
    support_proxy_trace.build_proxy_capture_lease_deadline = (
        lambda *args, **kwargs: "2026-04-28T12:10:00+00:00"
    )
    support_proxy_trace.build_proxy_capture_session_state = (
        lambda *args, **kwargs: None
    )
    support_proxy_trace.build_proxy_trace_manifest = lambda *args, **kwargs: {}
    support_proxy_trace.clear_proxy_capture_session_state = (
        lambda *args, **kwargs: None
    )
    support_proxy_trace.export_proxy_trace_bundle = lambda *args, **kwargs: None
    support_proxy_trace.export_proxy_trace_manifest = lambda *args, **kwargs: None
    support_proxy_trace.load_latest_proxy_trace_manifest = (
        lambda *args, **kwargs: None
    )
    support_proxy_trace.load_proxy_capture_session_state = (
        lambda *args, **kwargs: None
    )
    support_proxy_trace.parse_proxy_capture_session_timestamp = (
        lambda *args, **kwargs: None
    )
    support_proxy_trace.proxy_capture_restore_guard_reason = (
        lambda *args, **kwargs: ""
    )
    support_proxy_trace.proxy_capture_session_is_active = (
        lambda state: bool(state)
    )
    support_proxy_trace.proxy_capture_session_is_expired = (
        lambda *args, **kwargs: False
    )
    support_proxy_trace.publish_proxy_trace_download_copy = (
        lambda *args, **kwargs: None
    )
    support_proxy_trace.refresh_proxy_capture_session_lease = (
        lambda state, **kwargs: state
    )
    support_proxy_trace.save_proxy_capture_session_state = (
        lambda *args, **kwargs: None
    )

    support_shadow_backend = _ensure_module(
        "custom_components.eybond_local.support.shadow_learning_backend"
    )
    support_shadow_backend.build_shadow_learning_preflight = (
        lambda *args, **kwargs: types.SimpleNamespace(can_start=True, blockers=[])
    )
    support_shadow_backend.build_shadow_learning_seed = (
        lambda *args, **kwargs: (types.SimpleNamespace(write_response_mode="exception"), [])
    )
    support_shadow_backend.build_shadow_learning_trace_path = (
        lambda *args, **kwargs: Path("/tmp/shadow-learning.jsonl")
    )

    support_shadow_proxy = _ensure_module(
        "custom_components.eybond_local.support.shadow_learning_proxy"
    )
    support_shadow_proxy.route_status_indicates_control_ready = (
        lambda status: bool(status.get("collector_connected"))
        and (
            bool(status.get("ready"))
            or bool(status.get("route_protocol_activity"))
            or bool(status.get("collector_protocol_ingress"))
        )
    )

    support_shadow_session = _ensure_module(
        "custom_components.eybond_local.support.shadow_learning_session"
    )
    support_shadow_session.build_shadow_learning_lease_deadline = (
        lambda *args, **kwargs: "2026-06-05T12:20:00+00:00"
    )
    support_shadow_session.build_shadow_learning_session_state = (
        lambda **kwargs: types.SimpleNamespace(
            **{
                "route_owner_id": "",
                "expires_at": "",
                "restore_attempt_count": 0,
                "last_restore_attempt_at": "",
                "last_restore_error": "",
                "status": "",
                **kwargs,
            }
        )
    )
    support_shadow_session.clear_shadow_learning_session_state = (
        lambda *args, **kwargs: None
    )
    support_shadow_session.load_shadow_learning_session_state = (
        lambda *args, **kwargs: None
    )
    support_shadow_session.save_shadow_learning_session_state = (
        lambda *args, **kwargs: None
    )
    support_shadow_session.shadow_learning_session_is_active = (
        lambda state: bool(state) and str(getattr(state, "status", "")) in {
            "preflight",
            "starting",
            "waiting_for_collector",
            "connecting_upstream",
            "ready",
            "learning",
            "degraded",
            "restoring",
        }
    )
    support_shadow_session.shadow_learning_session_is_expired = (
        lambda *args, **kwargs: False
    )
    support_shadow_session.shadow_learning_session_timestamp = (
        lambda: "2026-06-05T12:00:00+00:00"
    )

    support_workflow = _ensure_module("custom_components.eybond_local.support.workflow")
    support_workflow.build_support_workflow_state = lambda *args, **kwargs: {}

    support_diagnostic_export = _ensure_module(
        "custom_components.eybond_local.support.diagnostic_export"
    )
    support_diagnostic_export.export_diagnostic_run = lambda *args, **kwargs: None

    support_diagnostic_runner = _ensure_module(
        "custom_components.eybond_local.support.diagnostic_runner"
    )

    @dataclasses.dataclass
    class DiagnosticRuntimeContext:
        transport: object | None = None

    @dataclasses.dataclass
    class DiagnosticRunResult:
        success: bool
        output: str
        results: list
        context: dict
        started_at: str
        finished_at: str
        error: str | None = None

    class DiagnosticSingleFlight:
        def __init__(self, **_kwargs) -> None:
            pass

        @property
        def running(self) -> bool:
            return False

        async def cancel(self) -> None:
            return None

        async def run(self, factory, **_kwargs):
            return await factory()

    async def run_scenario(*_args, **_kwargs):
        return DiagnosticRunResult(True, "", [], {}, "", "")

    support_diagnostic_runner.DiagnosticRuntimeContext = DiagnosticRuntimeContext
    support_diagnostic_runner.DiagnosticRunResult = DiagnosticRunResult
    support_diagnostic_runner.DiagnosticSingleFlight = DiagnosticSingleFlight
    support_diagnostic_runner.run_scenario = run_scenario

_STUBBED_MODULE_NAMES: tuple[str, ...] = (
    "custom_components",
    "custom_components.eybond_local",
    "custom_components.eybond_local.runtime",
    "custom_components.eybond_local.const",
    "custom_components.eybond_local.connection.models",
    "custom_components.eybond_local.collector.entity_scope",
    "custom_components.eybond_local.control_policy",
    "custom_components.eybond_local.drivers.registry",
    "custom_components.eybond_local.fixtures.utils",
    "custom_components.eybond_local.metadata.effective_metadata",
    "custom_components.eybond_local.metadata.local_metadata",
    "custom_components.eybond_local.metadata.smartess_draft",
    "custom_components.eybond_local.metadata.smartess_smg_bridge",
    "custom_components.eybond_local.models",
    "custom_components.eybond_local.runtime.factory",
    "custom_components.eybond_local.runtime.manager",
    "custom_components.eybond_local.schema",
    "custom_components.eybond_local.support.bundle",
    "custom_components.eybond_local.support.cloud_evidence",
    "custom_components.eybond_local.support.diagnostic_export",
    "custom_components.eybond_local.support.diagnostic_runner",
    "custom_components.eybond_local.support.package",
    "custom_components.eybond_local.support.proxy_capture",
    "custom_components.eybond_local.support.proxy_session",
    "custom_components.eybond_local.support.proxy_trace",
    "custom_components.eybond_local.support.shadow_learning_backend",
    "custom_components.eybond_local.support.shadow_learning_proxy",
    "custom_components.eybond_local.support.shadow_learning_session",
    "custom_components.eybond_local.support.workflow",
    "custom_components.eybond_local.runtime.coordinator",
    "homeassistant",
    "homeassistant.components",
    "homeassistant.components.network",
    "homeassistant.components.network.util",
    "homeassistant.components.persistent_notification",
    "homeassistant.config_entries",
    "homeassistant.helpers",
    "homeassistant.helpers.device_registry",
    "homeassistant.helpers.network",
    "homeassistant.helpers.update_coordinator",
    "homeassistant.util",
    "homeassistant.util.dt",
    "homeassistant.util.logging",
)


class FakeDevice:
    def __init__(self, device_id: str, identifiers: set[tuple[str, str]]) -> None:
        self.id = device_id
        self.identifiers = identifiers
        self.name = None
        self.model = None
        self.manufacturer = None
        self.serial_number = None
        self.sw_version = None
        self.hw_version = None
        self.via_device_id = None


class FakeRegistry:
    def __init__(self) -> None:
        self._devices_by_key: dict[frozenset[tuple[str, str]], FakeDevice] = {}
        self._counter = 0
        self.removed_device_ids: list[str] = []

    def async_get_device(self, identifiers=None, connections=None):
        del connections
        if not identifiers:
            return None
        return self._devices_by_key.get(frozenset(identifiers))

    def async_get_or_create(self, config_entry_id=None, **info):
        del config_entry_id
        identifiers = set(info.get("identifiers") or set())
        key = frozenset(identifiers)
        device = self._devices_by_key.get(key)
        if device is None:
            self._counter += 1
            device = FakeDevice(f"device-{self._counter}", identifiers)
            self._devices_by_key[key] = device

        device.name = info.get("name")
        device.model = info.get("model")
        device.manufacturer = info.get("manufacturer")
        device.serial_number = info.get("serial_number")
        device.sw_version = info.get("sw_version")
        device.hw_version = info.get("hw_version")

        via_device = info.get("via_device")
        if via_device is not None:
            parent = self.async_get_device(identifiers={via_device})
            device.via_device_id = None if parent is None else parent.id

        return device

    def async_remove_device(self, device_id: str) -> bool:
        for key, device in list(self._devices_by_key.items()):
            if device.id != device_id:
                continue
            self.removed_device_ids.append(device_id)
            del self._devices_by_key[key]
            return True
        return False


class CoordinatorDeviceHierarchyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._saved_modules = {
            name: sys.modules.pop(name, None) for name in _STUBBED_MODULE_NAMES
        }
        # Class cleanups run even when setUpClass fails partway, so a broken
        # stub import cannot leak stub modules into the rest of the test run.
        cls.addClassCleanup(cls._restore_stubbed_modules)
        _install_coordinator_stubs()

        coordinator_spec = importlib.util.spec_from_file_location(
            "custom_components.eybond_local.runtime.coordinator",
            REPO_ROOT / "custom_components" / "eybond_local" / "runtime" / "coordinator.py",
        )
        assert coordinator_spec is not None and coordinator_spec.loader is not None
        coordinator_module = importlib.util.module_from_spec(coordinator_spec)
        sys.modules[coordinator_spec.name] = coordinator_module
        coordinator_spec.loader.exec_module(coordinator_module)

        cls.coordinator_module = coordinator_module
        cls.platform_context_module = importlib.import_module(
            "custom_components.eybond_local.platform_context"
        )
        cls.RuntimeSnapshot = sys.modules[
            "custom_components.eybond_local.models"
        ].RuntimeSnapshot

    @classmethod
    def _restore_stubbed_modules(cls) -> None:
        for name in reversed(_STUBBED_MODULE_NAMES):
            original = cls._saved_modules.get(name)
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original

    def test_proxy_capture_notification_id_uses_bundle_stem(self) -> None:
        notification_id = self.coordinator_module._proxy_capture_notification_id(
            "entry-1",
            "/config/eybond_local/proxy_traces/session_bundle.zip",
        )

        self.assertEqual(
            notification_id,
            "eybond_local_proxy_capture_entry-1_session_bundle",
        )

    def test_smartess_cloud_export_available_requires_smartess_provider(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_pn": "A0000000000001",
                "collector_cloud_family": "valuecloud_at",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = ""

        self.assertFalse(coordinator.smartess_cloud_export_available)
        self.assertEqual(coordinator.cloud_evidence_provider, "valuecloud")
        self.assertTrue(coordinator.cloud_evidence_export_available)

    def test_smartess_cloud_export_available_keeps_smartess_profiles(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_pn": "E5000020000000",
                "collector_cloud_family": "smartess_at",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = ""

        self.assertTrue(coordinator.smartess_cloud_export_available)
        self.assertEqual(coordinator.cloud_evidence_provider, "smartess")
        self.assertTrue(coordinator.cloud_evidence_export_available)

    def test_cloud_evidence_export_available_rejects_unknown_provider(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_pn": "A9999999999999",
                "collector_cloud_family": "unknown",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = ""

        self.assertEqual(coordinator.cloud_evidence_provider, "")
        self.assertFalse(coordinator.cloud_evidence_export_available)

    def test_absolute_local_download_url_makes_signed_api_link_browser_safe(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()

        with patch.object(
            self.coordinator_module.network,
            "get_url",
            return_value="http://192.168.1.98:8123/",
        ):
            url = coordinator._absolute_local_download_url(
                "/api/eybond_local/support_package/entry-1?authSig=signed"
            )

        self.assertEqual(
            url,
            "http://192.168.1.98:8123/api/eybond_local/support_package/entry-1?authSig=signed",
        )
        self.assertNotIn("/lovelace/", url)

    def test_diagnostic_waits_for_in_progress_runtime_refresh(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator.data = types.SimpleNamespace()
            coordinator._diagnostic_active = False
            coordinator._runtime_operation_lock = asyncio.Lock()

            poll_started = asyncio.Event()
            release_poll = asyncio.Event()
            diagnostic_started = asyncio.Event()

            async def _poll_with_lock(**_kwargs):
                poll_started.set()
                await release_poll.wait()
                return coordinator.data

            async def _fake_run_scenario(_commands, _context):
                diagnostic_started.set()
                return types.SimpleNamespace(
                    success=True,
                    output="ok\n",
                    results=[],
                    context={},
                    started_at="start",
                    finished_at="finish",
                    error=None,
                )

            coordinator._async_update_data_with_runtime_lock = _poll_with_lock
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-1")

            async def _run_executor_job(job):
                return job()

            coordinator.hass = types.SimpleNamespace(
                config=types.SimpleNamespace(config_dir="/tmp"),
                async_add_executor_job=_run_executor_job,
            )
            export = types.SimpleNamespace(
                result_path=Path("/tmp/result.json"),
                download_url=None,
            )

            poll_task = asyncio.create_task(coordinator._async_update_data())
            await poll_started.wait()
            with patch.object(
                self.coordinator_module,
                "run_scenario",
                _fake_run_scenario,
            ), patch.object(
                self.coordinator_module,
                "export_diagnostic_run",
                return_value=export,
            ):
                diagnostic_task = asyncio.create_task(
                    coordinator._async_execute_diagnostic(
                        "read 1",
                        types.SimpleNamespace(),
                    )
                )
                await asyncio.sleep(0)
                self.assertFalse(diagnostic_started.is_set())

                release_poll.set()
                await poll_task
                await diagnostic_task
                self.assertTrue(diagnostic_started.is_set())

        asyncio.run(_run())

    def test_write_capability_serializes_against_runtime_refresh(self) -> None:
        # A control write must take the same _runtime_operation_lock the polling
        # loop holds, so a write and a refresh never interleave on the shared
        # transport (a mis-correlated read-back on a safety-critical write).
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator.data = types.SimpleNamespace(
                inverter=types.SimpleNamespace(
                    get_capability=lambda key: types.SimpleNamespace(key=key)
                )
            )
            coordinator._diagnostic_active = False
            coordinator._runtime_operation_lock = asyncio.Lock()
            coordinator.can_expose_capability = lambda _cap: True

            poll_started = asyncio.Event()
            release_poll = asyncio.Event()
            write_started = asyncio.Event()

            async def _poll_with_lock(**_kwargs):
                poll_started.set()
                await release_poll.wait()
                return coordinator.data

            coordinator._async_update_data_with_runtime_lock = _poll_with_lock

            async def _runtime_write(_key, value):
                write_started.set()
                # The write only runs while it holds the operation lock.
                assert coordinator._runtime_operation_lock.locked()
                return value

            coordinator._runtime = types.SimpleNamespace(
                async_write_capability=_runtime_write
            )

            async def _noop_refresh():
                return None

            coordinator.async_request_refresh = _noop_refresh

            poll_task = asyncio.create_task(coordinator._async_update_data())
            await poll_started.wait()  # poll now holds the lock

            write_task = asyncio.create_task(
                coordinator.async_write_capability("op2_enable", 1)
            )
            await asyncio.sleep(0)
            # The write must be blocked on the lock while the poll holds it.
            self.assertFalse(write_started.is_set())

            release_poll.set()
            await poll_task
            result = await write_task
            self.assertTrue(write_started.is_set())
            self.assertEqual(result, 1)

        asyncio.run(_run())

    def test_proxy_capture_notification_body_without_link_uses_saved_path(self) -> None:
        hass = types.SimpleNamespace(config=types.SimpleNamespace(language="uk"))

        message = self.coordinator_module._localized_runtime_text(
            hass,
            "proxy_capture_notification_body_no_link",
            saved_path="/config/eybond_local/proxy_traces/session_bundle.zip",
        )

        self.assertIn("/config/eybond_local/proxy_traces/session_bundle.zip", message)
        self.assertIn("Збережений архів", message)

    def test_capability_enabled_by_default_enables_exposed_learned_control(self) -> None:
        # The overlay generator bakes enabled_default=False onto every learned capability
        # so it stays inactive until activation. Once activated + selected (exposable), the
        # entity must be enabled by default -- otherwise it is registered disabled and stays
        # hidden under "disabled entities" on the device page. Built-ins keep their default.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        learned = types.SimpleNamespace(
            is_device_scoped_experimental=True, enabled_default=False
        )
        builtin = types.SimpleNamespace(
            is_device_scoped_experimental=False, enabled_default=True
        )

        coordinator.can_expose_capability = lambda _cap: True
        self.assertTrue(coordinator.capability_enabled_by_default(learned))
        self.assertTrue(coordinator.capability_enabled_by_default(builtin))

        coordinator.can_expose_capability = lambda _cap: False
        self.assertFalse(coordinator.capability_enabled_by_default(learned))

    def test_apply_device_overlay_merges_learned_capabilities(self) -> None:
        # Regression: the runtime detects the inverter against built-in bindings, so its
        # capabilities never include the activated learned overlay controls. Without
        # merging them in, the learned control exists only in effective metadata and
        # never becomes an entity (every entity/write path reads inverter.capabilities).
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})

        builtin_cap = types.SimpleNamespace(key="battery_float_voltage")
        inverter = _FakeInverter(
            capabilities=(builtin_cap,),
            capability_groups=(types.SimpleNamespace(key="battery"),),
            register_schema_name="modbus_smg/models/smg_6200.json",
        )

        learned_cap = types.SimpleNamespace(
            key="learned_x_304", is_device_scoped_experimental=True, group="config"
        )
        learned_schema_name = "learned/shadow_learning/dev/smg_6200_session.json"
        stub_metadata = types.SimpleNamespace(
            device_scoped_overlay_active=True,
            register_schema_name=learned_schema_name,
            profile_metadata=types.SimpleNamespace(
                capabilities=(learned_cap,),
                groups=(types.SimpleNamespace(key="config"),),
            ),
        )
        original = self.coordinator_module.resolve_effective_metadata_selection
        self.coordinator_module.resolve_effective_metadata_selection = (
            lambda **_kwargs: stub_metadata
        )
        try:
            result = coordinator._apply_device_overlay_to_inverter(inverter, None)
        finally:
            self.coordinator_module.resolve_effective_metadata_selection = original

        self.assertIn("learned_x_304", {cap.key for cap in result.capabilities})
        self.assertIn("battery_float_voltage", {cap.key for cap in result.capabilities})
        self.assertIn("config", {group.key for group in result.capability_groups})
        # CRITICAL: the overlay merge must NOT change register_schema_name. Pointing it at the
        # learned overlay schema flips the metadata scope to external and fails the
        # write-exposure proof for EVERY capability (builtin included) -- every control then
        # disappears. The builtin schema stays; learned-register read-back is done in the driver.
        self.assertEqual(result.register_schema_name, "modbus_smg/models/smg_6200.json")

    def test_entity_setup_merges_active_overlay_into_inverter(self) -> None:
        # The single place every platform reads at setup must apply the overlay merge,
        # so activated learned controls materialize regardless of detection timing.
        pc = self.platform_context_module
        inverter = _FakeInverter(capabilities=(types.SimpleNamespace(key="builtin"),))
        merged = _FakeInverter(
            capabilities=(
                types.SimpleNamespace(key="builtin"),
                types.SimpleNamespace(key="learned_x"),
            )
        )
        coordinator = types.SimpleNamespace(
            _apply_device_overlay_to_inverter=lambda inv, collector: merged,
            data=types.SimpleNamespace(collector=None),
        )

        self.assertIs(pc._merge_active_device_overlay(coordinator, inverter), merged)
        # No applier / no inverter -> unchanged, never raises.
        self.assertIs(
            pc._merge_active_device_overlay(types.SimpleNamespace(), inverter), inverter
        )
        self.assertIsNone(pc._merge_active_device_overlay(coordinator, None))

    def test_apply_device_overlay_returns_inverter_unchanged_when_inactive(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        inverter = _FakeInverter()

        stub_metadata = types.SimpleNamespace(device_scoped_overlay_active=False)
        original = self.coordinator_module.resolve_effective_metadata_selection
        self.coordinator_module.resolve_effective_metadata_selection = (
            lambda **_kwargs: stub_metadata
        )
        try:
            result = coordinator._apply_device_overlay_to_inverter(inverter, None)
        finally:
            self.coordinator_module.resolve_effective_metadata_selection = original

        self.assertIs(result, inverter)

    def test_write_exposure_context_uses_warmed_effective_metadata_cache(self) -> None:
        # Regression: after activating a learned overlay, write-exposure checks run in the
        # event loop. They must use the executor-warmed effective metadata selection instead
        # of re-resolving the overlay and reading external profile/schema JSON synchronously.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator.data = types.SimpleNamespace(
            collector=None,
            inverter=types.SimpleNamespace(model_name="SMG 6200", variant_key="smg_6200"),
        )
        coordinator._cached_effective_metadata = types.SimpleNamespace(
            profile_name="learned/shadow_learning/device/profile.json",
            profile_metadata=types.SimpleNamespace(source_scope="external"),
            register_schema_metadata=types.SimpleNamespace(source_scope="external"),
            device_scoped_overlay_active=True,
            device_scoped_overlay_scope="device",
            device_scoped_overlay_selected_control_keys={"learned_a"},
        )

        with patch.object(
            self.coordinator_module,
            "resolve_effective_metadata_selection",
            side_effect=AssertionError("sync resolver should not run after warm-up"),
        ):
            context = coordinator._write_exposure_context()

        self.assertEqual(context["variant_key"], "smg_6200")
        self.assertEqual(context["profile_source_scope"], "external")
        self.assertEqual(context["schema_source_scope"], "external")
        self.assertTrue(context["device_scoped_overlay_active"])
        self.assertEqual(context["selected_control_keys"], {"learned_a"})

    def test_shadow_learning_main_redirect_uses_real_server_not_additive_callback(self) -> None:
        # SAFETY regression: in "SmartESS + HA" the HA callback is additive and the live
        # endpoint can already look like HA. The scan must still rewrite the collector's main
        # param-21 endpoint to the proxy (driven off the REAL upstream/rollback target) and
        # restore to the real server -- otherwise the collector keeps a live link to the real
        # cloud and a mid-scan reconnect can push a real command to the inverter.
        resolve = self.coordinator_module._resolve_shadow_learning_main_redirect
        real = "dtu_ess.eybond.com,18899,TCP"
        ha = "192.168.1.50,18899,TCP"

        # Live endpoint already looks like HA (additive callback), but rollback target is real.
        restore_endpoint, redirect_required = resolve(
            home_assistant_primary=False,
            current_endpoint=ha, rollback_target=real, upstream_endpoint=real, callback_endpoint=ha
        )
        self.assertEqual(restore_endpoint, real)
        self.assertTrue(redirect_required)

        # No remembered rollback -> falls back to the upstream the proxy forwards to (real).
        restore_endpoint, redirect_required = resolve(
            home_assistant_primary=False,
            current_endpoint=ha, rollback_target="", upstream_endpoint=real, callback_endpoint=ha
        )
        self.assertEqual(restore_endpoint, real)
        self.assertTrue(redirect_required)

        # Nothing real known anywhere -> no redirect (can't move to proxy), restore stays put.
        restore_endpoint, redirect_required = resolve(
            home_assistant_primary=False,
            current_endpoint=ha, rollback_target="", upstream_endpoint="", callback_endpoint=ha
        )
        self.assertEqual(restore_endpoint, ha)
        self.assertFalse(redirect_required)

    def test_shadow_learning_main_redirect_noops_when_already_ha_only(self) -> None:
        # Regression: starting a scan already in HA-only must NOT switch or restore -- the
        # collector is already isolated on HA. Restoring to the real-server rollback target
        # would move an already-isolated collector ONTO the real server after the scan and
        # leave its control entities unavailable (mirrors proxy capture's no-op).
        resolve = self.coordinator_module._resolve_shadow_learning_main_redirect
        restore_endpoint, redirect_required = resolve(
            home_assistant_primary=True,
            current_endpoint="192.168.1.50,18899,TCP",
            rollback_target="dtu_ess.eybond.com,18899,TCP",
            upstream_endpoint="dtu_ess.eybond.com,18899,TCP",
            callback_endpoint="192.168.1.50,18899,TCP",
        )
        self.assertEqual(restore_endpoint, "")
        self.assertFalse(redirect_required)

    def test_sync_device_registry_sets_inverter_parent_to_collector(self) -> None:
        registry = FakeRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={},
            options={},
            title="SMG 6200",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={
                "collector_hardware_version": "HW-7",
                "collector_type": "Wi-Fi.DTU",
            },
            inverter=types.SimpleNamespace(model_name="SMG 6200", serial_number="INV-001"),
            collector=types.SimpleNamespace(
                collector_pn="COL-001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="1.2.3",
            ),
        )
        coordinator._last_synced_device_meta = ("", "", "", "", "")
        coordinator._last_synced_collector_device_meta = ("", "", "", "", "")

        coordinator.async_sync_device_registry()

        collector = registry.async_get_device(
            identifiers={("eybond_local", "entry-1:collector")}
        )
        inverter = registry.async_get_device(
            identifiers={("eybond_local", "entry-1")}
        )

        self.assertIsNotNone(collector)
        self.assertIsNotNone(inverter)
        self.assertEqual(collector.name, "Collector PN COL-001")
        self.assertEqual(collector.model, "Wi-Fi.DTU")
        self.assertEqual(collector.hw_version, "HW-7")
        self.assertEqual(inverter.via_device_id, collector.id)

    def test_pending_entry_uses_collector_scope_until_inverter_identity_exists(self) -> None:
        registry = FakeRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_pn": "COL-001",
                "detected_model": "",
                "detected_serial": "",
                "driver_hint": "modbus_smg",
            },
            options={},
            title="Collector PN COL-001",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_type": "Wi-Fi.DTU"},
            inverter=types.SimpleNamespace(
                model_name="",
                serial_number="",
                driver_key="modbus_smg",
                register_schema_name="smg_v1",
                capabilities=(),
                capability_presets=(),
            ),
            collector=types.SimpleNamespace(
                collector_pn="COL-001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="1.2.3",
            ),
        )
        coordinator._last_synced_device_meta = ("", "", "", "", "")
        coordinator._last_synced_collector_device_meta = ("", "", "", "", "", "")

        stale_inverter = registry.async_get_or_create(
            config_entry_id="entry-1",
            identifiers={("eybond_local", "entry-1")},
            name="Collector PN COL-001",
            manufacturer="OEM / EyeBond",
        )

        with patch.object(self.coordinator_module, "get_driver") as get_driver:
            self.assertIsNone(coordinator.identified_inverter)
            self.assertFalse(coordinator.has_inverter_identity)
            self.assertIsNone(coordinator.current_driver)
            get_driver.assert_not_called()
            self.assertEqual(
                coordinator.inverter_device_info()["identifiers"],
                {("eybond_local", "entry-1:collector")},
            )

        coordinator.async_sync_device_registry()

        collector = registry.async_get_device(
            identifiers={("eybond_local", "entry-1:collector")}
        )
        inverter = registry.async_get_device(
            identifiers={("eybond_local", "entry-1")}
        )

        self.assertIsNotNone(collector)
        self.assertIsNone(inverter)
        self.assertEqual(registry.removed_device_ids, [stale_inverter.id])

    def test_snapshot_backed_setup_uses_persisted_anenji_metadata_without_live_inverter(self) -> None:
        fake_driver = types.SimpleNamespace(key="modbus_smg", name="SMG / Modbus")
        fake_selection = types.SimpleNamespace(
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG-family runtime",
            profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            profile_metadata=types.SimpleNamespace(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                source_name="modbus_smg/models/anenji_4200_protocol_1.json",
                groups=(types.SimpleNamespace(key="config", title="Config", order=1),),
                capabilities=(types.SimpleNamespace(key="boot_method"),),
                presets=(types.SimpleNamespace(key="normal"),),
            ),
            register_schema_metadata=types.SimpleNamespace(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                source_name="modbus_smg/models/anenji_4200_protocol_1.json",
            ),
        )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"driver_hint": "auto"},
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "modbus_smg",
                    "effective_owner_name": "SMG-family runtime",
                    "profile_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                    "register_schema_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                    "confidence": "high",
                    "generation": 4,
                    "generated_at": "2026-06-03T19:00:00+00:00",
                }
            },
            title="SMG 6200",
        )
        coordinator.data = self.RuntimeSnapshot(values={}, inverter=None, collector=None)

        with patch.object(
            self.coordinator_module,
            "resolve_effective_metadata_selection",
            return_value=fake_selection,
        ), patch.object(
            self.platform_context_module,
            "get_driver",
            side_effect=lambda key: fake_driver if key == fake_driver.key else None,
        ):
            driver, inverter, has_inverter_identity = self.platform_context_module.entity_setup_context(
                coordinator.config_entry,
                coordinator,
            )

        self.assertIsNotNone(driver)
        self.assertEqual(getattr(driver, "key", ""), "modbus_smg")
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.profile_name, "modbus_smg/models/anenji_4200_protocol_1.json")
        self.assertEqual(
            inverter.register_schema_name,
            "modbus_smg/models/anenji_4200_protocol_1.json",
        )
        self.assertGreater(len(inverter.capabilities), 0)
        self.assertFalse(has_inverter_identity)

    def test_snapshot_backed_setup_is_not_synthesized_without_persisted_snapshot(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"driver_hint": "auto"},
            options={},
            title="SMG 6200",
        )
        coordinator.data = self.RuntimeSnapshot(values={}, inverter=None, collector=None)

        with patch.object(
            self.coordinator_module,
            "resolve_effective_metadata_selection",
            side_effect=AssertionError("resolver must not run without valid snapshot"),
        ), patch.object(
            self.platform_context_module,
            "get_driver",
            side_effect=AssertionError("driver lookup must not run without valid snapshot"),
        ):
            driver, inverter, has_inverter_identity = self.platform_context_module.entity_setup_context(
                coordinator.config_entry,
                coordinator,
            )

        self.assertIsNone(driver)
        self.assertIsNone(inverter)
        self.assertFalse(has_inverter_identity)

    def test_shadow_learning_effective_metadata_falls_back_to_live_for_partial_tier(self) -> None:
        # Partial-tier devices persist NO snapshot, so the learning start path
        # must fall back to the LIVE effective metadata (base schema) instead of
        # seeding with the empty persisted snapshot — otherwise it blocks with
        # missing_effective_metadata_snapshot on exactly the devices learning is
        # for. This property is the single source of truth shared with the
        # config-flow preflight.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"driver_hint": "auto"},
            options={},
            title="SMG Family",
        )
        coordinator.data = self.RuntimeSnapshot(values={}, inverter=None, collector=None)

        family_selection = types.SimpleNamespace(
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG-family runtime",
            profile_name="",
            register_schema_name="modbus_smg/base.json",
        )
        with patch.object(
            self.coordinator_module,
            "resolve_effective_metadata_selection",
            return_value=family_selection,
        ):
            result = coordinator.shadow_learning_effective_metadata

        self.assertEqual(result["register_schema_name"], "modbus_smg/base.json")
        self.assertEqual(result["profile_name"], "")

    def test_shadow_learning_effective_metadata_prefers_persisted_snapshot(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"driver_hint": "auto"},
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "modbus_smg",
                    "effective_owner_name": "SMG-family runtime",
                    "profile_name": "smg_modbus.json",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                    "confidence": "high",
                    "generation": 4,
                    "generated_at": "2026-06-03T19:00:00+00:00",
                }
            },
            title="SMG 6200",
        )
        coordinator.data = self.RuntimeSnapshot(values={}, inverter=None, collector=None)

        result = coordinator.shadow_learning_effective_metadata

        self.assertEqual(result.register_schema_name, "modbus_smg/models/smg_6200.json")

    def test_snapshot_backed_setup_is_not_synthesized_for_invalid_snapshot_payload(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"driver_hint": "auto"},
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "modbus_smg",
                    "profile_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                    "register_schema_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                    "confidence": "none",
                }
            },
            title="SMG 6200",
        )
        coordinator.data = self.RuntimeSnapshot(values={}, inverter=None, collector=None)

        with patch.object(
            self.coordinator_module,
            "resolve_effective_metadata_selection",
            side_effect=AssertionError("resolver must not run for invalid snapshot payload"),
        ), patch.object(
            self.platform_context_module,
            "get_driver",
            side_effect=AssertionError("driver lookup must not run for invalid snapshot payload"),
        ):
            driver, inverter, has_inverter_identity = self.platform_context_module.entity_setup_context(
                coordinator.config_entry,
                coordinator,
            )

        self.assertIsNone(driver)
        self.assertIsNone(inverter)
        self.assertFalse(has_inverter_identity)

    def test_remembered_external_endpoint_is_persisted_and_reused_for_rollback(self) -> None:
        updated_options: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                del title, data
                entry.options = dict(options or {})
                updated_options.append(dict(entry.options))

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={},
            options={},
            title="Collector PN COL-001",
        )
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = ""

        snapshot = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "47.91.67.66,18899,TCP"}
        )

        import asyncio

        asyncio.run(coordinator._async_remember_collector_server_endpoint(snapshot))

        self.assertEqual(
            coordinator.collector_server_endpoint_rollback_target,
            "47.91.67.66,18899,TCP",
        )
        self.assertEqual(len(updated_options), 1)
        self.assertEqual(
            updated_options[0]["collector_original_server_endpoint"],
            "47.91.67.66,18899,TCP",
        )
        self.assertEqual(
            updated_options[0]["collector_original_server_endpoint_profile_key"],
            "smartess_at",
        )
        self.assertEqual(
            updated_options[0]["collector_original_server_endpoint_source"],
            "runtime_observed",
        )
        self.assertTrue(updated_options[0]["collector_original_server_endpoint_observed_at"])

    def test_remember_collector_server_endpoint_does_not_replace_existing_original(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        updated_options: list[dict[str, str]] = []
        coordinator._async_update_entry_without_reload = lambda **kwargs: updated_options.append(
            kwargs["options"]
        )
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = "ess.eybond.com"
        coordinator.config_entry = types.SimpleNamespace(
            data={},
            options={
                "collector_original_server_endpoint": "ess.eybond.com",
                "collector_original_server_endpoint_profile_key": "legacy_binary",
            },
        )

        snapshot = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "dtu_ess.eybond.com,18899,TCP"}
        )

        import asyncio

        asyncio.run(coordinator._async_remember_collector_server_endpoint(snapshot))

        self.assertEqual(coordinator.collector_server_endpoint_rollback_target, "ess.eybond.com")
        self.assertEqual(updated_options, [])

    def test_restore_collector_original_endpoint_from_registry(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            remember_collector_original_endpoint,
        )

        async def _run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                remember_collector_original_endpoint(
                    config_dir=Path(tmp),
                    collector_pn="PN12345",
                    original_endpoint_raw="ess.eybond.com",
                    cloud_profile_key="legacy_binary",
                    source="test_registry",
                    observed_at="2026-06-22T10:00:00+00:00",
                    last_seen_ip="192.168.1.55",
                )
                updated_options: list[dict[str, str]] = []

                async def _async_add_executor_job(func, *args):
                    return func(*args)

                coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
                coordinator.hass = types.SimpleNamespace(
                    config=types.SimpleNamespace(config_dir=tmp),
                    async_add_executor_job=_async_add_executor_job,
                )
                coordinator._async_update_entry_without_reload = lambda **kwargs: updated_options.append(
                    kwargs["options"]
                )
                coordinator._connection_spec = types.SimpleNamespace(
                    effective_advertised_server_ip="192.168.1.50",
                    effective_advertised_tcp_port=8899,
                )
                coordinator._runtime = types.SimpleNamespace(
                    collector_server_endpoint_rollback_target="",
                )
                coordinator._remembered_collector_server_endpoint = ""
                coordinator.config_entry = types.SimpleNamespace(
                    data={"collector_pn": "PN12345"},
                    options={},
                )
                snapshot = self.RuntimeSnapshot(values={})

                await coordinator._async_restore_collector_original_endpoint_from_registry(
                    snapshot
                )

                self.assertEqual(
                    coordinator.collector_server_endpoint_rollback_target,
                    "ess.eybond.com",
                )
                self.assertEqual(len(updated_options), 1)
                self.assertEqual(
                    updated_options[0]["collector_original_server_endpoint"],
                    "ess.eybond.com",
                )
                self.assertEqual(
                    updated_options[0]["collector_original_server_endpoint_profile_key"],
                    "legacy_binary",
                )
                self.assertEqual(
                    updated_options[0]["collector_original_server_endpoint_source"],
                    "test_registry",
                )

        import asyncio

        asyncio.run(_run())

    def test_restore_collector_original_endpoint_from_registry_by_unique_last_seen_ip(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            remember_collector_original_endpoint,
        )

        async def _run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                remember_collector_original_endpoint(
                    config_dir=Path(tmp),
                    collector_pn="E50000200000000001",
                    original_endpoint_raw="iot.eybond.com,18899,TCP",
                    cloud_profile_key="valuecloud_at",
                    source="test_registry",
                    observed_at="2026-06-24T20:52:14+00:00",
                    last_seen_ip="192.168.8.110",
                )
                updated_options: list[dict[str, str]] = []

                async def _async_add_executor_job(func, *args):
                    return func(*args)

                coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
                coordinator.hass = types.SimpleNamespace(
                    config=types.SimpleNamespace(config_dir=tmp),
                    async_add_executor_job=_async_add_executor_job,
                )
                coordinator._async_update_entry_without_reload = lambda **kwargs: updated_options.append(
                    kwargs["options"]
                )
                coordinator._connection_spec = types.SimpleNamespace(
                    effective_advertised_server_ip="192.168.8.113",
                    effective_advertised_tcp_port=8899,
                )
                coordinator._runtime = types.SimpleNamespace(
                    collector_server_endpoint_rollback_target="",
                )
                coordinator._remembered_collector_server_endpoint = ""
                coordinator.config_entry = types.SimpleNamespace(
                    data={
                        "collector_pn": "E5000020000000",
                        "collector_ip": "192.168.8.110",
                    },
                    options={},
                )
                snapshot = self.RuntimeSnapshot(
                    collector=types.SimpleNamespace(remote_ip="192.168.8.110"),
                    values={},
                )

                await coordinator._async_restore_collector_original_endpoint_from_registry(
                    snapshot
                )

                self.assertEqual(
                    coordinator.collector_server_endpoint_rollback_target,
                    "iot.eybond.com,18899,TCP",
                )
                self.assertEqual(len(updated_options), 1)
                self.assertEqual(
                    updated_options[0]["collector_original_server_endpoint"],
                    "iot.eybond.com,18899,TCP",
                )
                self.assertEqual(
                    updated_options[0]["collector_original_server_endpoint_profile_key"],
                    "valuecloud_at",
                )

        import asyncio

        asyncio.run(_run())

    def test_restore_collector_original_endpoint_by_ip_fails_closed_when_ambiguous(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            remember_collector_original_endpoint,
        )

        async def _run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                for pn in ("PN12345", "PN67890"):
                    remember_collector_original_endpoint(
                        config_dir=Path(tmp),
                        collector_pn=pn,
                        original_endpoint_raw="iot.eybond.com,18899,TCP",
                        cloud_profile_key="valuecloud_at",
                        last_seen_ip="192.168.8.110",
                    )
                updated_options: list[dict[str, str]] = []

                async def _async_add_executor_job(func, *args):
                    return func(*args)

                coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
                coordinator.hass = types.SimpleNamespace(
                    config=types.SimpleNamespace(config_dir=tmp),
                    async_add_executor_job=_async_add_executor_job,
                )
                coordinator._async_update_entry_without_reload = lambda **kwargs: updated_options.append(
                    kwargs["options"]
                )
                coordinator._connection_spec = types.SimpleNamespace(
                    effective_advertised_server_ip="192.168.8.113",
                    effective_advertised_tcp_port=8899,
                )
                coordinator._runtime = types.SimpleNamespace(
                    collector_server_endpoint_rollback_target="",
                )
                coordinator._remembered_collector_server_endpoint = ""
                coordinator.config_entry = types.SimpleNamespace(
                    data={"collector_ip": "192.168.8.110"},
                    options={},
                )
                snapshot = self.RuntimeSnapshot(
                    collector=types.SimpleNamespace(remote_ip="192.168.8.110"),
                    values={},
                )

                await coordinator._async_restore_collector_original_endpoint_from_registry(
                    snapshot
                )

                self.assertEqual(coordinator.collector_server_endpoint_rollback_target, "")
                self.assertEqual(updated_options, [])

        import asyncio

        asyncio.run(_run())

    def test_remember_collector_server_endpoint_writes_registry_by_pn(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            get_collector_registry_record,
        )

        async def _run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                updated_options: list[dict[str, str]] = []

                async def _async_add_executor_job(func, *args):
                    return func(*args)

                coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
                coordinator.hass = types.SimpleNamespace(
                    config=types.SimpleNamespace(config_dir=tmp),
                    async_add_executor_job=_async_add_executor_job,
                )
                coordinator._async_update_entry_without_reload = lambda **kwargs: updated_options.append(
                    kwargs["options"]
                )
                coordinator._connection_spec = types.SimpleNamespace(
                    effective_advertised_server_ip="192.168.1.50",
                    effective_advertised_tcp_port=8899,
                )
                coordinator._runtime = types.SimpleNamespace(
                    collector_server_endpoint_rollback_target="",
                )
                coordinator._remembered_collector_server_endpoint = ""
                coordinator.config_entry = types.SimpleNamespace(
                    data={},
                    options={},
                )
                snapshot = self.RuntimeSnapshot(
                    collector=types.SimpleNamespace(
                        collector_pn="PN12345",
                        remote_ip="192.168.1.55",
                    ),
                    values={"collector_server_endpoint": "dtu_ess.eybond.com,18899,TCP"},
                )

                await coordinator._async_remember_collector_server_endpoint(snapshot)

                record = get_collector_registry_record(
                    config_dir=Path(tmp),
                    collector_pn="PN12345",
                )
                self.assertIsNotNone(record)
                assert record is not None
                self.assertEqual(record.original_endpoint_raw, "dtu_ess.eybond.com,18899,TCP")
                self.assertEqual(record.cloud_profile_key, "smartess_at")
                self.assertEqual(record.source, "runtime_observed")
                self.assertEqual(record.last_seen_ip, "192.168.1.55")

        import asyncio

        asyncio.run(_run())

    def test_host_only_external_endpoint_is_preserved_for_rollback_and_bind_shape(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="ess.eybond.com",
        )
        coordinator._remembered_collector_server_endpoint = ""
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "ess.eybond.com"}
        )

        self.assertEqual(coordinator.collector_server_endpoint_rollback_target, "ess.eybond.com")
        self.assertEqual(coordinator.collector_callback_target_endpoint, "192.168.1.50")
        self.assertEqual(coordinator.proxy_capture_target_endpoint, "192.168.1.50")

    def test_prepare_listener_uses_legacy_port_for_host_only_family(self) -> None:
        listener_ports: list[int] = []

        async def _ensure_listener(port: int) -> None:
            listener_ports.append(port)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            collector_server_endpoint_rollback_target="",
            async_ensure_callback_listener=_ensure_listener,
        )
        coordinator._remembered_collector_server_endpoint = ""
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "ess.eybond.com"}
        )

        asyncio.run(
            coordinator._async_prepare_home_assistant_callback_listener(
                coordinator.collector_callback_target_endpoint
            )
        )

        self.assertEqual(coordinator.collector_callback_target_endpoint, "192.168.1.50")
        self.assertEqual(listener_ports, [502])

    def test_ha_only_mode_uses_legacy_listener_for_host_only_endpoint(self) -> None:
        async def _run() -> None:
            listener_ports: list[int] = []
            endpoint_calls: list[tuple[str, bool]] = []
            reverse_discovery_flags: list[bool] = []
            refresh_calls: list[bool] = []

            async def _ensure_listener(port: int) -> None:
                listener_ports.append(port)

            async def _set_endpoint(endpoint: str, *, apply_changes: bool = True):
                self.assertEqual(
                    coordinator.data.values.get("collector_operation_endpoint_sync_status"),
                    "waiting_for_collector",
                )
                endpoint_calls.append((endpoint, apply_changes))
                return {"readback_endpoint": endpoint, "status": "applied"}

            async def _request_refresh() -> None:
                refresh_calls.append(True)

            def _async_update_entry(entry, **kwargs) -> None:
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])
                if "options" in kwargs:
                    entry.options = dict(kwargs["options"])

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                collector_server_endpoint_rollback_target="ess.eybond.com",
                async_ensure_callback_listener=_ensure_listener,
                async_set_collector_server_endpoint=_set_endpoint,
                set_reverse_discovery_enabled=reverse_discovery_flags.append,
            )
            coordinator._remembered_collector_server_endpoint = ""
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={},
                options={},
            )
            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "ess.eybond.com"},
            )
            coordinator._tooling_values = {}
            coordinator.collector_operation_mode_change_reason = lambda *, target_mode="": None
            coordinator.async_request_refresh = _request_refresh

            await coordinator.async_set_collector_operation_mode("home_assistant_only")

            self.assertEqual(listener_ports, [502])
            self.assertEqual(endpoint_calls, [("192.168.1.50", True)])
            self.assertEqual(reverse_discovery_flags, [False])
            self.assertEqual(refresh_calls, [True])
            self.assertEqual(
                coordinator.data.values["collector_operation_endpoint_sync_status"],
                "applied",
            )

        asyncio.run(_run())

    def test_legacy_mode_lock_clears_after_reconnect_without_endpoint_readback(self) -> None:
        async def _run() -> None:
            listener_ports: list[int] = []

            async def _ensure_listener(port: int) -> None:
                listener_ports.append(port)

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                collector_server_endpoint_rollback_target="ess.eybond.com",
                async_ensure_callback_listener=_ensure_listener,
            )
            coordinator._remembered_collector_server_endpoint = ""
            coordinator._collector_operation_pending_target_endpoint = "192.168.1.50"
            coordinator.config_entry = types.SimpleNamespace(
                data={},
                options={"collector_operation_mode": "home_assistant_only"},
            )

            disconnected_snapshot = self.RuntimeSnapshot(
                connected=False,
                values={"collector_server_endpoint": "192.168.1.50"},
            )
            coordinator.data = disconnected_snapshot

            await coordinator._async_reconcile_collector_operation_mode_endpoint(
                disconnected_snapshot
            )

            self.assertEqual(
                disconnected_snapshot.values["collector_operation_endpoint_sync_status"],
                "waiting_for_collector",
            )
            self.assertEqual(
                coordinator._collector_operation_pending_target_endpoint,
                "192.168.1.50",
            )

            connected_snapshot = self.RuntimeSnapshot(connected=True, values={})
            coordinator.data = connected_snapshot

            await coordinator._async_reconcile_collector_operation_mode_endpoint(
                connected_snapshot
            )

            self.assertEqual(connected_snapshot.values["collector_server_endpoint"], "192.168.1.50")
            self.assertEqual(
                connected_snapshot.values["collector_operation_endpoint_sync_status"],
                "aligned",
            )
            self.assertEqual(coordinator._collector_operation_pending_target_endpoint, "")
            self.assertEqual(listener_ports, [502, 502])

        asyncio.run(_run())

    def test_shadow_learning_blocks_cloud_mode_endpoint_restore_reconcile(self) -> None:
        async def _run() -> None:
            set_endpoint_calls: list[tuple[str, bool]] = []

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                set_endpoint_calls.append((endpoint, apply_changes))
                return {"readback_endpoint": endpoint, "status": "applied"}

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return types.SimpleNamespace(status="learning")

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                collector_server_endpoint_rollback_target="dtu_ess.eybond.com,18899,TCP",
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
            )
            coordinator._collector_operation_pending_target_endpoint = ""
            coordinator._remembered_collector_server_endpoint = (
                "dtu_ess.eybond.com,18899,TCP"
            )
            coordinator._shadow_learning_process_running = lambda: False
            coordinator._async_active_shadow_learning_state = (
                _async_active_shadow_learning_state
            )
            coordinator.config_entry = types.SimpleNamespace(
                data={
                    "collector_ip": "192.168.1.55",
                    "collector_operation_mode": "smartess_cloud_home_assistant",
                },
                options={
                    "collector_operation_mode": "smartess_cloud_home_assistant",
                    "collector_original_server_endpoint": "dtu_ess.eybond.com,18899,TCP",
                },
            )
            snapshot = self.RuntimeSnapshot(
                connected=True,
                values={
                    "collector_server_endpoint": "192.168.1.50,18899,TCP",
                    "collector_cloud_family": "smartess_at",
                },
            )
            coordinator.data = snapshot

            await coordinator._async_reconcile_collector_operation_mode_endpoint(snapshot)

            self.assertEqual(set_endpoint_calls, [])
            self.assertEqual(
                snapshot.values["collector_operation_endpoint_sync_status"],
                "shadow_learning_active",
            )
            self.assertEqual(
                snapshot.values["collector_server_endpoint"],
                "192.168.1.50,18899,TCP",
            )

        import asyncio

        asyncio.run(_run())

    def test_home_assistant_callback_target_pins_listener_port_over_cloud_port(self) -> None:
        # The callback target must always carry THIS entry's listener port:
        # inheriting the cloud/proxy port (18899) from the collector-reported
        # endpoint pointed collectors at the proxy-capture listener while the
        # UDP announcer advertised the real one, fighting on every reconnect.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = ""
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "47.91.67.66,18899,TCP"}
        )

        self.assertEqual(
            coordinator.collector_callback_target_endpoint,
            "192.168.1.50,8899,TCP",
        )

    def test_proxy_capture_upstream_endpoint_uses_default_smartess_fallback(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = ""
        coordinator.data = self.RuntimeSnapshot(
            values={
                "collector_server_endpoint": "192.168.1.50,18899,TCP",
                "collector_cloud_family": "smartess_at",
            }
        )

        self.assertEqual(
            coordinator.proxy_capture_upstream_endpoint,
            "dtu_ess.eybond.com,18899,TCP",
        )

    def test_proxy_capture_upstream_endpoint_ignores_stale_local_callback_after_ha_ip_change(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_ip": "192.168.1.55"},
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"}
        )

        self.assertEqual(
            coordinator.proxy_capture_upstream_endpoint,
            "47.91.67.66,18899,TCP",
        )

    def test_configure_reverse_discovery_turns_off_for_ha_only_mode(self) -> None:
        reverse_discovery_flags: list[bool] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            collector_server_endpoint_rollback_target="",
            set_reverse_discovery_enabled=reverse_discovery_flags.append,
        )
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "home_assistant_only"},
            options={"collector_operation_mode": "home_assistant_only"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"}
        )

        coordinator._configure_reverse_discovery_mode()

        self.assertEqual(reverse_discovery_flags, [False])

    def test_collector_operation_mode_forces_ha_only_for_runtime_bridge(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "smartess_cloud_home_assistant"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(collector_virtual_bridge=True),
        )

        self.assertEqual(coordinator.collector_operation_mode, "home_assistant_only")
        self.assertTrue(coordinator.collector_home_assistant_primary)

    def test_runtime_bridge_syncs_persisted_operation_mode_to_ha_only(self) -> None:
        updates: list[dict[str, object]] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "smartess_cloud_home_assistant"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(collector_virtual_bridge=True),
        )
        coordinator._async_update_entry_without_reload = lambda **kwargs: updates.append(kwargs)

        coordinator._sync_forced_collector_operation_mode()

        self.assertEqual(len(updates), 1)
        data = updates[0]["data"]
        options = updates[0]["options"]
        self.assertEqual(data["collector_operation_mode"], "home_assistant_only")
        self.assertEqual(options["collector_operation_mode"], "home_assistant_only")
        self.assertTrue(data["collector_virtual_bridge"])
        self.assertTrue(options["collector_virtual_bridge"])

    def test_configure_reverse_discovery_stays_on_for_ha_only_bridge(self) -> None:
        # Item 1: a detected bridge refuses the param-21 endpoint write and does
        # not persist the endpoint, so it relearns the HA server only from UDP
        # discovery. Forced to HA-only, it must KEEP reverse discovery enabled to
        # reconnect after a reboot — unlike a factory collector.
        reverse_discovery_flags: list[bool] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            collector_server_endpoint_rollback_target="",
            set_reverse_discovery_enabled=reverse_discovery_flags.append,
        )
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "home_assistant_only"},
            options={"collector_operation_mode": "home_assistant_only"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            collector=types.SimpleNamespace(collector_virtual_bridge=True),
        )

        coordinator._configure_reverse_discovery_mode()

        self.assertEqual(reverse_discovery_flags, [True])

    def test_configure_reverse_discovery_turns_off_when_endpoint_already_targets_ha(self) -> None:
        reverse_discovery_flags: list[bool] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            server_ip="192.168.1.50",
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            collector_server_endpoint_rollback_target="",
            set_reverse_discovery_enabled=reverse_discovery_flags.append,
        )
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "smartess_cloud_home_assistant"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"}
        )

        coordinator._configure_reverse_discovery_mode()

        self.assertEqual(reverse_discovery_flags, [False])

    def test_configure_reverse_discovery_keeps_on_when_endpoint_targets_cloud(self) -> None:
        reverse_discovery_flags: list[bool] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            server_ip="192.168.1.50",
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            collector_server_endpoint_rollback_target="",
            set_reverse_discovery_enabled=reverse_discovery_flags.append,
        )
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "smartess_cloud_home_assistant"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "dtu_ess.eybond.com,18899,TCP"}
        )

        coordinator._configure_reverse_discovery_mode()

        self.assertEqual(reverse_discovery_flags, [True])

    def test_async_trigger_collector_rediscovery_keeps_bootstrap_transport_separate(self) -> None:
        async def _run() -> None:
            reverse_discovery_calls: list[dict[str, float | int]] = []
            prepared_targets: list[str] = []
            refresh_calls: list[bool] = []

            async def _trigger_reverse_discovery(
                *,
                port: int = 0,
                timeout: float = 0.75,
            ) -> dict[str, object]:
                reverse_discovery_calls.append(
                    {"port": int(port), "timeout": float(timeout)}
                )
                return {
                    "status": "probe_sent",
                    "advertised_endpoint": "192.168.1.104:8899",
                }

            async def _prepare_listener(endpoint: str) -> None:
                prepared_targets.append(endpoint)

            async def _request_refresh() -> None:
                refresh_calls.append(True)

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.104",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.104",
                collector_server_endpoint_rollback_target="",
                async_trigger_reverse_discovery=_trigger_reverse_discovery,
            )
            coordinator.config_entry = types.SimpleNamespace(
                data={
                    "collector_ip": "192.168.1.55",
                    "collector_operation_mode": "home_assistant_only",
                },
                options={"collector_operation_mode": "home_assistant_only"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=False,
                values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            )
            coordinator._async_prepare_home_assistant_callback_listener = _prepare_listener
            coordinator.async_request_refresh = _request_refresh

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                result = await coordinator.async_trigger_collector_rediscovery()

            self.assertEqual(prepared_targets, ["192.168.1.104,8899,TCP"])
            self.assertEqual(
                reverse_discovery_calls,
                [{"port": 0, "timeout": 0.75}],
            )
            self.assertEqual(
                result["collector_callback_target_endpoint"],
                "192.168.1.104,8899,TCP",
            )
            self.assertEqual(result["target_role"], "bootstrap")
            self.assertEqual(refresh_calls, [True])

        asyncio.run(_run())

    def test_collector_server_endpoint_rollback_target_ignores_stale_runtime_local_callback(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.104",
            collector_server_endpoint_rollback_target="192.168.1.50,18899,TCP",
        )
        coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_ip": "192.168.1.55"},
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})

        self.assertEqual(
            coordinator.collector_server_endpoint_rollback_target,
            "47.91.67.66,18899,TCP",
        )

    def test_proxy_capture_overview_passes_upstream_endpoint(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            connected=True,
        )
        coordinator.config_entry = types.SimpleNamespace(
            data={"detection_confidence": "none"},
            options={"control_mode": "auto"},
        )
        coordinator._active_proxy_capture_state = lambda: None
        coordinator._proxy_capture_runtime_values = lambda: {}

        captured: dict[str, object] = {}
        original_builder = self.coordinator_module.build_proxy_capture_overview

        def _fake_build_proxy_capture_overview(**kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(
                can_start=bool(kwargs["upstream_endpoint"]),
                can_stop=False,
                blocking_reason="",
                redirect_required=True,
            )

        self.coordinator_module.build_proxy_capture_overview = _fake_build_proxy_capture_overview
        try:
            overview = coordinator.proxy_capture_overview
        finally:
            self.coordinator_module.build_proxy_capture_overview = original_builder

        self.assertEqual(captured["upstream_endpoint"], "47.91.67.66,18899,TCP")
        self.assertTrue(overview.can_start)

    def test_proxy_capture_duration_properties_follow_config_and_runtime_values(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"proxy_capture_duration_minutes": 10},
            options={"proxy_capture_duration_minutes": 15},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"proxy_capture_remaining_seconds": 125},
            connected=True,
        )
        coordinator._tooling_values = {}

        with patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            "proxy_capture_overview",
            new_callable=PropertyMock,
            return_value=types.SimpleNamespace(
                can_stop=True,
                critical_phase=False,
                can_start=False,
                blocking_reason="",
            ),
        ):
            self.assertEqual(coordinator.proxy_capture_configured_duration_minutes, 15)
            self.assertEqual(coordinator.proxy_capture_remaining_seconds, 125)
            self.assertEqual(coordinator.proxy_capture_remaining_minutes, 3)
            self.assertEqual(coordinator.proxy_capture_display_duration_minutes, 3)
            self.assertIsNone(coordinator.proxy_capture_duration_availability_reason())

    def test_proxy_capture_values_pass_upstream_endpoint(self) -> None:
        import asyncio

        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                collector_server_endpoint_rollback_target="",
            )
            coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"
            coordinator.data = self.RuntimeSnapshot(
                values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
                connected=True,
            )
            coordinator.config_entry = types.SimpleNamespace(
                data={"detection_confidence": "none"},
                options={"control_mode": "auto"},
            )

            async def _async_none(*args, **kwargs):
                del args, kwargs
                return None

            async def _async_add_executor_job(func):
                return func()

            async def _async_download_details(_manifest_path: str):
                return "", ""

            coordinator.hass = types.SimpleNamespace(
                async_add_executor_job=_async_add_executor_job,
            )
            coordinator._async_active_proxy_capture_state = _async_none
            coordinator._async_latest_proxy_trace_record = _async_none
            coordinator._async_proxy_trace_manifest_download_details = _async_download_details

            captured: dict[str, object] = {}
            original_builder = self.coordinator_module.build_proxy_capture_overview

            def _fake_build_proxy_capture_overview(**kwargs):
                captured.update(kwargs)
                return types.SimpleNamespace(
                    status="ready",
                    status_label="Ready",
                    summary="",
                    blocking_reason="",
                    can_start=bool(kwargs["upstream_endpoint"]),
                    can_stop=False,
                    critical_phase=False,
                    redirect_required=True,
                    current_endpoint=kwargs["current_endpoint"],
                    target_endpoint=kwargs["target_endpoint"],
                    masked_endpoint=kwargs["current_endpoint"],
                    latest_trace_path=kwargs["latest_trace_path"],
                    latest_manifest_path=kwargs["latest_manifest_path"],
                )

            self.coordinator_module.build_proxy_capture_overview = _fake_build_proxy_capture_overview
            try:
                values = await coordinator._proxy_capture_values()
            finally:
                self.coordinator_module.build_proxy_capture_overview = original_builder

            self.assertEqual(captured["upstream_endpoint"], "47.91.67.66,18899,TCP")
            self.assertTrue(values["proxy_capture_can_start"])

        asyncio.run(_run())

    def test_collector_device_info_prefers_more_complete_configured_pn(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_pn": "E50000200000000001",
                "collector_ip": "192.168.1.55",
            },
            options={},
            title="Collector PN E50000200000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(
                collector_pn="E5000020000000",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="1.2.3",
            ),
        )

        info = coordinator.collector_device_info()

        self.assertEqual(info["name"], "Collector PN E50000200000000001")
        self.assertEqual(info["serial_number"], "E50000200000000001")

    def test_collector_device_info_does_not_use_configured_firmware_fallback(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_pn": "V0000000000001",
                "collector_ip": "192.168.1.51",
                "smartess_collector_version": "8.50.12.3",
            },
            options={},
            title="Collector PN V0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(
                collector_pn="V0000000000001",
                profile_name="",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                collector_virtual_bridge=False,
            ),
        )

        info = coordinator.collector_device_info()

        self.assertNotIn("sw_version", info)

    def test_collector_device_info_uses_honest_identity_for_virtual_bridge(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-bridge",
            data={
                "collector_pn": "E50000200000000001",
                "collector_ip": "192.0.2.55",
            },
            options={},
            title="Collector PN E50000200000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_virtual_bridge": True},
            collector=types.SimpleNamespace(
                collector_pn="E50000200000000001",
                profile_name="",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                collector_virtual_bridge=True,
                collector_bridge_kind="esp-collector",
                collector_bridge_version="0.4.0",
            ),
        )

        info = coordinator.collector_device_info()

        self.assertEqual(info["manufacturer"], "ESP EyeBond Collector (community)")
        self.assertEqual(info["model"], "ESP EyeBond Collector")
        self.assertEqual(info["sw_version"], "0.4.0")
        self.assertEqual(
            info["configuration_url"],
            "https://github.com/groove-max/esp-eybond-collector",
        )
        self.assertEqual(info["serial_number"], "E50000200000000001")

    def test_remember_runtime_identity_strengthens_pending_entry_metadata(self) -> None:
        updated_entries: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                del options
                if data is not None:
                    entry.data = dict(data)
                if title is not None:
                    entry.title = title
                updated_entries.append(
                    {
                        "title": entry.title,
                        "data": dict(entry.data),
                    }
                )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-2",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "",
                "detected_model": "",
                "detected_serial": "",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="Collector 192.168.1.14",
        )
        coordinator.data = self.RuntimeSnapshot()

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
                smartess_protocol_profile_key="smartess_at",
            ),
        )

        import asyncio

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertEqual(
            coordinator.config_entry.data["collector_pn"],
            "Q0000000000001",
        )
        self.assertEqual(
            coordinator.config_entry.data["detected_model"],
            "PowMr 4.2kW",
        )
        self.assertEqual(
            coordinator.config_entry.data["detected_serial"],
            "55355535553555",
        )
        self.assertEqual(
            coordinator.config_entry.data["collector_cloud_profile_key"],
            "smartess_at",
        )
        self.assertEqual(
            coordinator.config_entry.data["collector_cloud_profile_source"],
            "runtime_observed",
        )
        self.assertEqual(
            coordinator.config_entry.data["collector_cloud_profile_confidence"],
            "high",
        )
        self.assertEqual(
            coordinator.config_entry.title,
            "Collector PN Q0000000000001",
        )
        self.assertEqual(len(updated_entries), 1)

    def test_remember_runtime_identity_requests_reload_after_platform_setup(self) -> None:
        updated_entries: list[dict[str, object]] = []
        reload_requests: list[str] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                del options
                if data is not None:
                    entry.data = dict(data)
                if title is not None:
                    entry.title = title
                updated_entries.append(
                    {
                        "title": entry.title,
                        "data": dict(entry.data),
                    }
                )

            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-3",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "",
                "detected_model": "",
                "detected_serial": "",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="Collector 192.168.1.14",
        )
        coordinator.data = self.RuntimeSnapshot()
        coordinator._entity_platforms_initialized = True
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = True

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        async def _run() -> None:
            await coordinator._async_remember_runtime_identity(snapshot)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(
            coordinator.config_entry.data["detected_model"],
            "PowMr 4.2kW",
        )
        self.assertEqual(len(updated_entries), 1)
        self.assertEqual(reload_requests, ["entry-3"])
        self.assertTrue(coordinator._entity_platform_reload_requested)

    def test_remember_runtime_identity_clears_stale_0925_register_serial(self) -> None:
        updated_entries: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title
                updated_entries.append({"title": entry.title, "data": dict(entry.data)})

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-0925",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)",
                "detected_serial": "55355535553555",
                "server_ip": "192.168.1.50",
            },
            options={},
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot()

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                driver_key="smartess_local",
                model_name="PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)",
                serial_number="",
                variant_key="smartess_0925",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="3.6.7.6",
            ),
        )

        import asyncio

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertEqual(coordinator.config_entry.data["detected_serial"], "")
        self.assertEqual(len(updated_entries), 1)

    def test_remember_runtime_identity_persists_effective_snapshot_in_options(self) -> None:
        updated_entries: list[dict[str, object]] = []
        from custom_components.eybond_local.metadata.compiled_detection_catalog import (
            load_compiled_detection_catalog,
        )

        catalog = load_compiled_detection_catalog()
        descriptor_revision = catalog.devices["smg_6200"].revision

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title
                updated_entries.append(
                    {
                        "title": entry.title,
                        "data": dict(entry.data),
                        "options": dict(entry.options),
                    }
                )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-6",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "",
                "detected_model": "",
                "detected_serial": "",
                "detection_confidence": "medium",
                "server_ip": "192.168.1.104",
                "driver_hint": "auto",
            },
            options={},
            title="Collector 192.168.1.14",
        )
        coordinator.data = self.RuntimeSnapshot()
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = True

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
                driver_key="modbus_smg",
                variant_key="default",
                profile_name="modbus_smg/models/smg_6200.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
                details={
                    "catalog_detection": {
                        "candidate_keys": ["smg_6200"],
                        "resolution": "exact",
                        "surface_key": "smg_6200_full",
                        "evidence_fingerprint": "fingerprint",
                        "catalog_version": catalog.catalog_version,
                        "descriptor_revisions": [
                            f"smg_6200:{descriptor_revision}"
                        ],
                    }
                },
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertEqual(coordinator.config_entry.data["detected_model"], "PowMr 4.2kW")
        self.assertEqual(coordinator.config_entry.data["detected_serial"], "55355535553555")
        self.assertEqual(coordinator.config_entry.data["detection_confidence"], "high")

        persisted_snapshot = coordinator.config_entry.options.get("effective_metadata_snapshot")
        self.assertIsInstance(persisted_snapshot, dict)
        assert isinstance(persisted_snapshot, dict)
        self.assertEqual(persisted_snapshot.get("effective_owner_key"), "modbus_smg")
        self.assertEqual(
            persisted_snapshot.get("profile_name"),
            "modbus_smg/models/smg_6200.json",
        )
        self.assertEqual(
            persisted_snapshot.get("register_schema_name"),
            "modbus_smg/models/smg_6200.json",
        )
        self.assertNotIn("collector_cloud_profile_key", persisted_snapshot)
        self.assertNotIn("collector_cloud_profile_label", persisted_snapshot)
        self.assertNotIn("collector_cloud_profile_source", persisted_snapshot)
        self.assertNotIn("collector_cloud_profile_confidence", persisted_snapshot)
        self.assertEqual(persisted_snapshot.get("confidence"), "high")
        self.assertEqual(persisted_snapshot.get("candidate_keys"), ["smg_6200"])
        self.assertEqual(persisted_snapshot.get("resolution_level"), "exact")
        self.assertEqual(persisted_snapshot.get("surface_key"), "smg_6200_full")
        self.assertEqual(
            persisted_snapshot.get("evidence_fingerprint"),
            "fingerprint",
        )
        self.assertEqual(
            persisted_snapshot.get("catalog_version"),
            catalog.catalog_version,
        )
        self.assertEqual(
            persisted_snapshot.get("descriptor_revisions"),
            [f"smg_6200:{descriptor_revision}"],
        )
        self.assertEqual(persisted_snapshot.get("generation"), 1)
        self.assertTrue(str(persisted_snapshot.get("generated_at") or ""))
        self.assertEqual(len(updated_entries), 1)

    def test_remember_runtime_identity_skips_snapshot_rewrite_when_unchanged(self) -> None:
        updated_entries: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title
                updated_entries.append(
                    {
                        "title": entry.title,
                        "data": dict(entry.data),
                        "options": dict(entry.options),
                    }
                )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-6b",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "",
                "detected_model": "",
                "detected_serial": "",
                "detection_confidence": "medium",
                "server_ip": "192.168.1.104",
                "driver_hint": "auto",
            },
            options={},
            title="Collector 192.168.1.14",
        )
        coordinator.data = self.RuntimeSnapshot()
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = True

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
                driver_key="modbus_smg",
                variant_key="powmr_4200_protocol_1",
                profile_name="modbus_smg/models/powmr_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/powmr_4200_protocol_1.json",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))
        first_snapshot = dict(
            coordinator.config_entry.options.get("effective_metadata_snapshot") or {}
        )
        self.assertTrue(first_snapshot)
        self.assertEqual(first_snapshot.get("generation"), 1)
        self.assertEqual(len(updated_entries), 1)

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        second_snapshot = dict(
            coordinator.config_entry.options.get("effective_metadata_snapshot") or {}
        )
        self.assertEqual(len(updated_entries), 1)
        self.assertEqual(second_snapshot, first_snapshot)
        self.assertEqual(second_snapshot.get("generation"), 1)

    def test_remember_runtime_identity_does_not_persist_snapshot_without_live_identity(self) -> None:
        update_calls: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title
                update_calls.append(
                    {
                        "title": entry.title,
                        "data": dict(entry.data),
                        "options": dict(entry.options),
                    }
                )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-7",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "",
                "detected_serial": "",
                "driver_hint": "modbus_smg",
                "detection_confidence": "none",
                "server_ip": "192.168.1.104",
            },
            options={"driver_hint": "modbus_smg"},
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot()
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = True

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="",
                serial_number="",
                driver_key="modbus_smg",
                variant_key="powmr_4200_protocol_1",
                profile_name="modbus_smg/models/powmr_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/powmr_4200_protocol_1.json",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertEqual(len(update_calls), 1)
        self.assertNotIn(
            "effective_metadata_snapshot",
            update_calls[0]["options"],
        )
        self.assertNotIn("effective_metadata_snapshot", coordinator.config_entry.options)

    def test_remember_runtime_identity_requests_reload_when_platforms_loaded_collector_only(self) -> None:
        reload_requests: list[str] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if title is not None:
                    entry.title = title

            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-5",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "PowMr 4.2kW",
                "detected_serial": "55355535553555",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot()
        coordinator._entity_platforms_initialized = True
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = False

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        async def _run() -> None:
            await coordinator._async_remember_runtime_identity(snapshot)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(reload_requests, ["entry-5"])
        self.assertTrue(coordinator._entity_platform_reload_requested)

    def test_mark_entity_platforms_initialized_requests_reload_when_identity_arrived_during_setup(self) -> None:
        reload_requests: list[str] = []

        class _ConfigEntries:
            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(entry_id="entry-4")
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
            )
        )
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False

        async def _run() -> None:
            coordinator.mark_entity_platforms_initialized(has_inverter_identity=False)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertTrue(coordinator._entity_platforms_initialized)
        self.assertFalse(coordinator._entity_platforms_loaded_with_inverter_identity)
        self.assertTrue(coordinator._entity_platform_reload_requested)
        self.assertEqual(reload_requests, ["entry-4"])

    def test_remember_runtime_identity_requests_reload_on_effective_metadata_drift(self) -> None:
        reload_requests: list[str] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title

            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-drift",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "SMG 6200",
                "detected_serial": "SMG-123",
                "detection_confidence": "high",
                "server_ip": "192.168.1.104",
                "driver_hint": "modbus_smg",
            },
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "modbus_smg",
                    "effective_owner_name": "modbus_smg",
                    "variant_key": "smg_default",
                    "profile_name": "modbus_smg/models/smg_default.json",
                    "register_schema_name": "modbus_smg/models/smg_default.json",
                    "confidence": "high",
                    "generation": 1,
                    "generated_at": "2026-06-01T00:00:00+00:00",
                }
            },
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="SMG 6200",
                serial_number="SMG-123",
            )
        )
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = False
        coordinator._platform_loaded_effective_metadata_signature = ("", "", "")

        coordinator.mark_entity_platforms_initialized(has_inverter_identity=True)

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="SMG 6200",
                serial_number="SMG-123",
                driver_key="modbus_smg",
                variant_key="anenji_4200_protocol_1",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        async def _run() -> None:
            await coordinator._async_remember_runtime_identity(snapshot)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(reload_requests, ["entry-drift"])
        self.assertTrue(coordinator._entity_platform_reload_requested)

    def test_remember_runtime_identity_requests_reload_for_first_runtime_signature_after_upgrade(self) -> None:
        reload_requests: list[str] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title

            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-upgrade-first-runtime-signature",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "SMG 6200",
                "detected_serial": "SMG-123",
                "detection_confidence": "high",
                "server_ip": "192.168.1.104",
                "driver_hint": "modbus_smg",
            },
            options={},
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="SMG 6200",
                serial_number="SMG-123",
            )
        )
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = False
        coordinator._entity_platforms_loaded_with_driver_fallback = False
        coordinator._platform_loaded_effective_metadata_signature = ("", "", "")

        coordinator.mark_entity_platforms_initialized(has_inverter_identity=True)

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="SMG 6200",
                serial_number="SMG-123",
                driver_key="modbus_smg",
                variant_key="anenji_4200_protocol_1",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="8.50.12.3",
            ),
        )

        async def _run() -> None:
            await coordinator._async_remember_runtime_identity(snapshot)
            await coordinator._async_remember_runtime_identity(snapshot)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(reload_requests, ["entry-upgrade-first-runtime-signature"])
        self.assertTrue(coordinator._entity_platform_reload_requested)

    def test_metadata_drift_reload_allows_first_runtime_signature_for_driver_fallback_setup(self) -> None:
        reload_requests: list[str] = []

        class _ConfigEntries:
            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(entry_id="entry-driver-fallback")
        coordinator._entity_platforms_initialized = True
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = False
        coordinator._entity_platforms_loaded_with_driver_fallback = True

        async def _run() -> None:
            coordinator._request_entry_reload_for_metadata_drift(
                setup_signature=("", "", ""),
                runtime_signature=(
                    "anenji_4200_protocol_1",
                    "modbus_smg/models/anenji_4200_protocol_1.json",
                    "modbus_smg/models/anenji_4200_protocol_1.json",
                ),
            )
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(reload_requests, ["entry-driver-fallback"])
        self.assertTrue(coordinator._entity_platform_reload_requested)

    def test_remember_runtime_identity_does_not_reload_on_identical_effective_metadata(self) -> None:
        reload_requests: list[str] = []

        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                if data is not None:
                    entry.data = dict(data)
                if options is not None:
                    entry.options = dict(options)
                if title is not None:
                    entry.title = title

            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-same",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "SMG 6200",
                "detected_serial": "SMG-123",
                "detection_confidence": "high",
                "server_ip": "192.168.1.104",
                "driver_hint": "modbus_smg",
            },
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "modbus_smg",
                    "effective_owner_name": "modbus_smg",
                    "variant_key": "anenji_4200_protocol_1",
                    "profile_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                    "register_schema_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                    "confidence": "high",
                    "generation": 2,
                    "generated_at": "2026-06-01T00:00:00+00:00",
                }
            },
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="SMG 6200",
                serial_number="SMG-123",
            )
        )
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platforms_loaded_with_inverter_identity = False
        coordinator._platform_loaded_effective_metadata_signature = ("", "", "")

        coordinator.mark_entity_platforms_initialized(has_inverter_identity=True)

        snapshot = self.RuntimeSnapshot(
            values={"smartess_profile_key": "hint-only-change"},
            inverter=types.SimpleNamespace(
                model_name="SMG 6200",
                serial_number="SMG-123",
                driver_key="modbus_smg",
                variant_key="anenji_4200_protocol_1",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            ),
            collector=types.SimpleNamespace(
                collector_pn="Q0000000000001",
                profile_name="EyeBond ASCII PN v1",
                smartess_protocol_name="changed-hint-only",
                smartess_protocol_asset_name="changed-hint-only",
                smartess_collector_version="8.50.12.3",
            ),
        )

        async def _run() -> None:
            await coordinator._async_remember_runtime_identity(snapshot)
            await coordinator._async_remember_runtime_identity(snapshot)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(reload_requests, [])
        self.assertFalse(coordinator._entity_platform_reload_requested)

    def test_clear_proxy_capture_session_runtime_values_drops_stale_session_keys(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.data = self.RuntimeSnapshot(
            values={
                "proxy_capture_session_status": "running",
                "proxy_capture_session_started_at": "2026-04-30T00:00:00+00:00",
                "proxy_capture_session_expires_at": "2026-04-30T00:10:00+00:00",
                "proxy_capture_session_anonymized": True,
                "proxy_trace_path": "/config/trace.jsonl",
            }
        )
        coordinator._tooling_values = {
            "proxy_capture_session_status": "running",
            "proxy_capture_session_started_at": "2026-04-30T00:00:00+00:00",
            "proxy_capture_session_expires_at": "2026-04-30T00:10:00+00:00",
            "proxy_capture_session_anonymized": True,
            "proxy_trace_path": "/config/trace.jsonl",
        }

        coordinator._clear_proxy_capture_session_runtime_values()

        self.assertNotIn("proxy_capture_session_status", coordinator.data.values)
        self.assertNotIn("proxy_capture_session_started_at", coordinator.data.values)
        self.assertNotIn("proxy_capture_session_expires_at", coordinator.data.values)
        self.assertNotIn("proxy_capture_session_anonymized", coordinator.data.values)
        self.assertEqual(coordinator.data.values["proxy_trace_path"], "/config/trace.jsonl")
        self.assertNotIn("proxy_capture_session_status", coordinator._tooling_values)

    def test_active_proxy_capture_state_ignores_stale_running_session_without_route(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id", data={})
        coordinator._runtime = types.SimpleNamespace(proxy_capture_route_running=lambda: False)
        coordinator.data = self.RuntimeSnapshot(
            values={
                "proxy_capture_session_status": "running",
                "proxy_capture_session_started_at": "2026-04-30T00:00:00+00:00",
                "proxy_capture_session_expires_at": "2026-04-30T00:10:00+00:00",
                "proxy_capture_session_anonymized": True,
                "proxy_capture_redirect_required": True,
                "proxy_capture_target_endpoint": "127.0.0.1:18899",
                "proxy_capture_masked_endpoint": "cloud.example:1883",
                "proxy_trace_path": "/config/trace.jsonl",
            }
        )
        coordinator._tooling_values = {}

        self.assertIsNone(coordinator._active_proxy_capture_state())

    def test_active_proxy_capture_state_prefers_cached_session_state(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        cached_state = types.SimpleNamespace(
            status="running",
            trace_path="/config/trace.jsonl",
            original_endpoint="cloud.example,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
        )
        coordinator._cached_proxy_capture_session_state = cached_state
        coordinator.data = self.RuntimeSnapshot(
            values={
                "proxy_capture_session_status": "running",
            }
        )
        coordinator._tooling_values = {}

        self.assertIs(coordinator._active_proxy_capture_state(), cached_state)

    def test_start_proxy_capture_fails_early_when_shadow_learning_owns_route(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            active_shadow_state = types.SimpleNamespace(status="ready")
            save_calls: list[bool] = []
            stop_shadow_calls: list[dict[str, object]] = []

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return active_shadow_state

            async def _async_save_proxy_capture_session_state(_state) -> None:
                save_calls.append(True)

            async def _async_stop_shadow_learning(**kwargs):
                stop_shadow_calls.append(dict(kwargs))

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator._async_save_proxy_capture_session_state = _async_save_proxy_capture_session_state
            coordinator.async_stop_shadow_learning = _async_stop_shadow_learning
            coordinator._shadow_learning_process_running = lambda: False
            coordinator._proxy_capture_process_running = lambda: False
            coordinator.collector_operation_mode_apply_lock_code = lambda: None

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(
                    can_start=True,
                    blocking_reason="",
                    redirect_required=False,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "shadow_learning_route_running"):
                    await coordinator.async_start_proxy_capture()

            self.assertEqual(save_calls, [])
            self.assertEqual(stop_shadow_calls, [])

        import asyncio

        asyncio.run(_run())

    def test_start_shadow_learning_fails_early_when_proxy_capture_owns_route(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            active_proxy_state = types.SimpleNamespace(status="running")
            save_calls: list[bool] = []
            start_shadow_calls: list[dict[str, object]] = []

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return active_proxy_state

            async def _async_save_shadow_learning_session_state(_state) -> None:
                save_calls.append(True)

            async def _async_start_shadow_learning_route(**kwargs) -> None:
                start_shadow_calls.append(dict(kwargs))

            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator._async_save_shadow_learning_session_state = (
                _async_save_shadow_learning_session_state
            )
            coordinator._runtime = types.SimpleNamespace(
                proxy_capture_route_running=lambda: False,
                async_start_shadow_learning_route=_async_start_shadow_learning_route,
            )
            coordinator._shadow_learning_process_running = lambda: False

            with self.assertRaisesRegex(RuntimeError, "proxy_capture_route_running"):
                await coordinator.async_start_shadow_learning(
                    output_path=Path("/tmp/shadow.jsonl"),
                    raw_capture={},
                )

            self.assertEqual(save_calls, [])
            self.assertEqual(start_shadow_calls, [])

        import asyncio

        asyncio.run(_run())

    def test_start_shadow_learning_fails_early_when_memory_is_low(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            start_shadow_calls: list[dict[str, object]] = []

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return None

            async def _async_start_shadow_learning_route(**kwargs) -> None:
                start_shadow_calls.append(dict(kwargs))

            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator._runtime = types.SimpleNamespace(
                proxy_capture_route_running=lambda: False,
                async_start_shadow_learning_route=_async_start_shadow_learning_route,
            )
            coordinator._shadow_learning_process_running = lambda: False

            with patch.object(
                self.coordinator_module,
                "read_available_memory_mib",
                return_value=128,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "shadow_learning_preflight_blocked:insufficient_memory:128MiB",
                ):
                    await coordinator.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow.jsonl"),
                        raw_capture={},
                    )

            self.assertEqual(start_shadow_calls, [])

        import asyncio

        asyncio.run(_run())

    def test_reconcile_expired_proxy_session_prefers_proxy_restore_trigger(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            calls: list[dict[str, object]] = []
            refreshed_snapshots: list[float] = []
            snapshot = self.RuntimeSnapshot(values={"collector_server_endpoint": "192.168.1.50,18899,TCP"})
            active_state = types.SimpleNamespace(status="running")
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-id",
                options={"poll_interval": 30},
            )

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return active_state

            async def _async_stop_proxy_capture(**kwargs):
                calls.append(dict(kwargs))

            async def _async_refresh(*, poll_interval: float):
                refreshed_snapshots.append(poll_interval)
                return snapshot

            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator.async_stop_proxy_capture = _async_stop_proxy_capture
            coordinator._runtime = types.SimpleNamespace(async_refresh=_async_refresh)

            with patch.object(
                self.coordinator_module,
                "proxy_capture_session_is_active",
                return_value=True,
            ), patch.object(
                self.coordinator_module,
                "proxy_capture_session_is_expired",
                return_value=True,
            ), patch.object(
                coordinator,
                "_proxy_capture_process_running",
                return_value=True,
            ):
                result = await coordinator._async_reconcile_proxy_capture_session(snapshot)

            self.assertIs(result, snapshot)
            self.assertEqual(
                calls,
                [
                    {
                        "reason": "expired_lease",
                        "prefer_proxy_restore_trigger": True,
                        "request_refresh": False,
                    }
                ],
            )
            self.assertEqual(refreshed_snapshots, [30.0])

        import asyncio

        asyncio.run(_run())

    def test_reconcile_expired_shadow_session_stops_with_expired_lease(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            calls: list[dict[str, object]] = []
            refreshed_snapshots: list[float] = []
            snapshot = self.RuntimeSnapshot(values={})
            active_state = types.SimpleNamespace(status="ready")
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-id",
                options={"poll_interval": 45},
            )

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return active_state

            async def _async_stop_shadow_learning(**kwargs):
                calls.append(dict(kwargs))

            async def _async_refresh(*, poll_interval: float):
                refreshed_snapshots.append(poll_interval)
                return snapshot

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator.async_stop_shadow_learning = _async_stop_shadow_learning
            coordinator._runtime = types.SimpleNamespace(async_refresh=_async_refresh)

            with patch.object(
                self.coordinator_module,
                "shadow_learning_session_is_active",
                return_value=True,
            ), patch.object(
                self.coordinator_module,
                "shadow_learning_session_is_expired",
                return_value=True,
            ):
                result = await coordinator._async_reconcile_shadow_learning_session(snapshot)

            self.assertIs(result, snapshot)
            self.assertEqual(
                calls,
                [
                    {
                        "reason": "expired_lease",
                        "request_refresh": False,
                        "raise_when_not_running": False,
                    }
                ],
            )
            self.assertEqual(refreshed_snapshots, [45.0])

        import asyncio

        asyncio.run(_run())

    def test_recover_shadow_learning_state_retries_restore_failed_session(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            calls: list[dict[str, object]] = []
            state = types.SimpleNamespace(status="restore_failed")
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return state

            async def _async_stop_shadow_learning(**kwargs):
                calls.append(dict(kwargs))

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator.async_stop_shadow_learning = _async_stop_shadow_learning

            with patch.object(
                self.coordinator_module,
                "shadow_learning_session_is_active",
                return_value=False,
            ), patch.object(
                self.coordinator_module,
                "shadow_learning_session_is_expired",
                return_value=False,
            ):
                await coordinator._async_recover_shadow_learning_state()

            self.assertEqual(
                calls,
                [
                    {
                        "reason": "recovered_after_restart",
                        "request_refresh": False,
                        "raise_when_not_running": False,
                    }
                ],
            )

        import asyncio

        asyncio.run(_run())

    def test_stop_shadow_learning_keeps_recoverable_state_when_restore_fails(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            saved_states: list[object] = []
            clear_calls: list[bool] = []
            notify_calls: list[bool] = []
            published: list[dict[str, object]] = []
            state = types.SimpleNamespace(
                entry_id="entry-id",
                collector_pn="E5000020000000",
                trace_path="/tmp/shadow.jsonl",
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                upstream_endpoint="eu.smartess.io,18899,TCP",
                restore_required=True,
                started_at="2026-06-05T12:00:00+00:00",
                expires_at="2026-06-05T12:20:00+00:00",
                restore_attempt_count=1,
                last_restore_attempt_at="",
                last_restore_error="",
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator._runtime = types.SimpleNamespace(
                async_stop_shadow_learning_route=lambda: asyncio.sleep(0)
            )

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return state

            async def _async_restore_proxy_capture_endpoint(_endpoint: str):
                raise RuntimeError("restore_failed")

            async def _async_save_shadow_learning_session_state(new_state):
                saved_states.append(new_state)

            async def _async_clear_shadow_learning_session_state():
                clear_calls.append(True)

            async def _async_request_refresh():
                return None

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator._async_restore_proxy_capture_endpoint = _async_restore_proxy_capture_endpoint
            coordinator._async_save_shadow_learning_session_state = _async_save_shadow_learning_session_state
            coordinator._async_clear_shadow_learning_session_state = _async_clear_shadow_learning_session_state
            coordinator.async_request_refresh = _async_request_refresh
            coordinator._notify_proxy_capture_restore_unconfirmed = lambda: notify_calls.append(True)
            coordinator._publish_tooling_values = lambda **kwargs: published.append(dict(kwargs))

            result = await coordinator.async_stop_shadow_learning()

            self.assertEqual(result["status"], "restore_unconfirmed")
            self.assertEqual(result["restore_confirmed"], False)
            self.assertFalse(clear_calls)
            self.assertEqual(saved_states[-1].status, "restore_failed")
            self.assertEqual(saved_states[-1].restore_attempt_count, 2)
            self.assertTrue(notify_calls)
            self.assertEqual(published[-1]["shadow_learning_session_status"], "restore_failed")

        import asyncio

        asyncio.run(_run())

    def test_stop_shadow_learning_clears_state_after_confirmed_restore(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            saved_states: list[object] = []
            clear_calls: list[bool] = []
            state = types.SimpleNamespace(
                entry_id="entry-id",
                collector_pn="E5000020000000",
                trace_path="/tmp/shadow.jsonl",
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                upstream_endpoint="eu.smartess.io,18899,TCP",
                restore_required=True,
                started_at="2026-06-05T12:00:00+00:00",
                expires_at="2026-06-05T12:20:00+00:00",
                restore_attempt_count=0,
                last_restore_attempt_at="",
                last_restore_error="",
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator._runtime = types.SimpleNamespace(
                async_stop_shadow_learning_route=lambda: asyncio.sleep(0)
            )

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return state

            async def _async_restore_proxy_capture_endpoint(endpoint: str):
                return endpoint

            async def _async_save_shadow_learning_session_state(new_state):
                saved_states.append(new_state)

            async def _async_clear_shadow_learning_session_state():
                clear_calls.append(True)

            async def _async_request_refresh():
                return None

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator._async_restore_proxy_capture_endpoint = _async_restore_proxy_capture_endpoint
            coordinator._async_save_shadow_learning_session_state = _async_save_shadow_learning_session_state
            coordinator._async_clear_shadow_learning_session_state = _async_clear_shadow_learning_session_state
            coordinator.async_request_refresh = _async_request_refresh
            coordinator._notify_proxy_capture_restore_unconfirmed = lambda: None
            coordinator._publish_tooling_values = lambda **kwargs: None

            result = await coordinator.async_stop_shadow_learning()

            self.assertEqual(result["status"], "stopped")
            self.assertEqual(result["restore_confirmed"], True)
            self.assertEqual(result["restored_endpoint"], "eu.smartess.io,18899,TCP")
            self.assertTrue(clear_calls)
            self.assertEqual(saved_states[0].status, "restoring")

        import asyncio

        asyncio.run(_run())

    def test_start_shadow_learning_keeps_recoverable_state_when_start_fails_after_redirect(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            saved_states: list[object] = []
            clear_calls: list[bool] = []
            route_stop_calls: list[bool] = []
            refresh_calls: list[bool] = []
            published: list[dict[str, object]] = []
            set_endpoint_calls: list[tuple[str, bool]] = []

            async def _async_start_shadow_learning_route(**kwargs):
                del kwargs
                return None

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                set_endpoint_calls.append((endpoint, apply_changes))
                return {"readback_endpoint": endpoint}

            async def _async_stop_shadow_learning_route(**kwargs) -> None:
                self.assertTrue(str(kwargs.get("owner_id") or "").startswith("shadow_learning:"))
                route_stop_calls.append(True)

            async def _async_preflight_proxy_capture_network(**kwargs) -> None:
                del kwargs
                return None

            async def _async_save_shadow_learning_session_state(state) -> None:
                saved_states.append(state)

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return None

            async def _async_wait_for_shadow_learning_ready(**kwargs) -> None:
                del kwargs
                raise RuntimeError("startup_timeout")

            async def _async_best_effort_restore_after_start_failure(_endpoint: str):
                return False, "restore_write_timeout"

            async def _async_clear_shadow_learning_session_state() -> None:
                clear_calls.append(True)

            async def _async_request_refresh() -> None:
                refresh_calls.append(True)

            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-id",
                data={},
                options={"proxy_capture_duration_minutes": 10},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=False,
                values={"collector_server_endpoint": "eu.smartess.io,18899,TCP"},
            )
            coordinator._runtime = types.SimpleNamespace(
                proxy_capture_route_running=lambda: False,
                async_start_shadow_learning_route=_async_start_shadow_learning_route,
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                async_stop_shadow_learning_route=_async_stop_shadow_learning_route,
            )
            coordinator._shadow_learning_process_running = lambda: False
            coordinator._async_preflight_proxy_capture_network = _async_preflight_proxy_capture_network
            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator._async_save_shadow_learning_session_state = (
                _async_save_shadow_learning_session_state
            )
            coordinator._async_wait_for_shadow_learning_ready = (
                _async_wait_for_shadow_learning_ready
            )
            coordinator._async_best_effort_restore_after_start_failure = (
                _async_best_effort_restore_after_start_failure
            )
            coordinator._async_clear_shadow_learning_session_state = (
                _async_clear_shadow_learning_session_state
            )
            coordinator.async_request_refresh = _async_request_refresh
            coordinator._publish_tooling_values = lambda **kwargs: published.append(dict(kwargs))

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "smartess_collector_pn",
                new_callable=PropertyMock,
                return_value="E5000020000000",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_callback_target_endpoint",
                new_callable=PropertyMock,
                return_value="192.168.1.50,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_upstream_endpoint",
                new_callable=PropertyMock,
                return_value="eu.smartess.io,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_key",
                new_callable=PropertyMock,
                return_value="smartess-default",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_label",
                new_callable=PropertyMock,
                return_value="SmartESS Default",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_source",
                new_callable=PropertyMock,
                return_value="runtime",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_confidence",
                new_callable=PropertyMock,
                return_value="high",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "effective_metadata_snapshot",
                new_callable=PropertyMock,
                return_value={},
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "shadow_learning_effective_metadata",
                new_callable=PropertyMock,
                return_value={"register_schema_name": "modbus_smg/base.json"},
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_family",
                new_callable=PropertyMock,
                return_value="smartess",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "_effective_callback_server_host",
                new_callable=PropertyMock,
                return_value="192.168.1.50",
            ), patch.object(
                self.coordinator_module,
                "build_shadow_learning_seed",
                return_value=(
                    types.SimpleNamespace(write_response_mode="exception"),
                    [],
                ),
            ), patch.object(
                self.coordinator_module,
                "build_shadow_learning_preflight",
                return_value=types.SimpleNamespace(can_start=True, blockers=[]),
            ):
                with self.assertRaisesRegex(RuntimeError, "startup_timeout"):
                    await coordinator.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow-start-failure.jsonl"),
                        raw_capture={},
                    )

            self.assertFalse(clear_calls)
            self.assertEqual(route_stop_calls, [True])
            self.assertEqual(refresh_calls, [True])
            self.assertEqual(set_endpoint_calls, [("192.168.1.50,18899,TCP", True)])
            self.assertEqual(saved_states[-1].status, "restore_failed")
            self.assertTrue(saved_states[-1].restore_required)
            self.assertEqual(saved_states[-1].original_endpoint, "eu.smartess.io,18899,TCP")
            self.assertEqual(saved_states[-1].restore_attempt_count, 1)
            self.assertEqual(saved_states[-1].last_restore_error, "restore_write_timeout")
            self.assertTrue(saved_states[-1].route_owner_id.startswith("shadow_learning:"))
            self.assertEqual(published[-1]["shadow_learning_session_status"], "restore_failed")

        import asyncio

        asyncio.run(_run())

    def test_start_shadow_learning_requires_post_redirect_shadow_connection(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            wait_kwargs: list[dict[str, object]] = []
            set_endpoint_calls: list[tuple[str, bool]] = []
            route_status = {
                "running": True,
                "collector_connected": True,
                "collector_connection_sequence": 7,
                "collector_protocol_ingress": True,
                "route_protocol_activity": True,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }

            async def _async_start_shadow_learning_route(**kwargs):
                del kwargs
                return None

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                set_endpoint_calls.append((endpoint, apply_changes))
                return {"readback_endpoint": endpoint, "status": "applied"}

            async def _async_stop_shadow_learning_route(**kwargs) -> None:
                raise AssertionError(f"route should not stop: {kwargs!r}")

            async def _async_preflight_proxy_capture_network(**kwargs) -> None:
                del kwargs
                return None

            async def _async_save_shadow_learning_session_state(_state) -> None:
                return None

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return None

            async def _async_wait_for_shadow_learning_ready(**kwargs) -> None:
                wait_kwargs.append(dict(kwargs))

            async def _async_request_refresh() -> None:
                return None

            async def _async_clear_shadow_learning_session_state() -> None:
                return None

            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-id",
                data={"collector_operation_mode": "smartess_cloud_home_assistant"},
                options={"proxy_capture_duration_minutes": 10},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=False,
                values={"collector_server_endpoint": "eu.smartess.io,18899,TCP"},
            )
            coordinator._runtime = types.SimpleNamespace(
                proxy_capture_route_running=lambda: False,
                shadow_learning_route_status=lambda: dict(route_status),
                async_start_shadow_learning_route=_async_start_shadow_learning_route,
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                async_stop_shadow_learning_route=_async_stop_shadow_learning_route,
            )
            coordinator._shadow_learning_process_running = lambda: False
            coordinator._proxy_capture_collector_ip = lambda: "192.168.1.55"
            coordinator._async_preflight_proxy_capture_network = _async_preflight_proxy_capture_network
            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator._async_save_shadow_learning_session_state = (
                _async_save_shadow_learning_session_state
            )
            coordinator._async_wait_for_shadow_learning_ready = (
                _async_wait_for_shadow_learning_ready
            )
            coordinator._async_clear_shadow_learning_session_state = (
                _async_clear_shadow_learning_session_state
            )
            coordinator.async_request_refresh = _async_request_refresh
            coordinator._publish_tooling_values = lambda **_kwargs: None

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "smartess_collector_pn",
                new_callable=PropertyMock,
                return_value="E5000020000000",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_callback_target_endpoint",
                new_callable=PropertyMock,
                return_value="192.168.1.50,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_upstream_endpoint",
                new_callable=PropertyMock,
                return_value="eu.smartess.io,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_key",
                new_callable=PropertyMock,
                return_value="smartess-default",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_label",
                new_callable=PropertyMock,
                return_value="SmartESS Default",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_source",
                new_callable=PropertyMock,
                return_value="runtime",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_confidence",
                new_callable=PropertyMock,
                return_value="high",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "effective_metadata_snapshot",
                new_callable=PropertyMock,
                return_value={},
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "shadow_learning_effective_metadata",
                new_callable=PropertyMock,
                return_value={"register_schema_name": "modbus_smg/base.json"},
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_family",
                new_callable=PropertyMock,
                return_value="smartess",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "_effective_callback_server_host",
                new_callable=PropertyMock,
                return_value="192.168.1.50",
            ), patch.object(
                self.coordinator_module,
                "build_shadow_learning_seed",
                return_value=(
                    types.SimpleNamespace(write_response_mode="exception"),
                    [],
                ),
            ), patch.object(
                self.coordinator_module,
                "build_shadow_learning_preflight",
                return_value=types.SimpleNamespace(can_start=True, blockers=[]),
            ):
                await coordinator.async_start_shadow_learning(
                    output_path=Path("/tmp/shadow-start-ready.jsonl"),
                    raw_capture={},
                )

            self.assertEqual(set_endpoint_calls, [("192.168.1.50,18899,TCP", True)])
            self.assertEqual(len(wait_kwargs), 1)
            self.assertEqual(
                wait_kwargs[0]["min_collector_connection_sequence"],
                7,
            )

        import asyncio

        asyncio.run(_run())

    def test_wait_for_shadow_learning_ready_rejects_stale_collector_connection(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            statuses = [
                {
                    "running": True,
                    "collector_connected": True,
                    "collector_connection_sequence": 7,
                    "collector_protocol_ingress": True,
                    "route_protocol_activity": True,
                    "upstream_connected": False,
                    "ready": False,
                    "upstream_error": "",
                },
                {
                    "running": True,
                    "collector_connected": True,
                    "collector_connection_sequence": 8,
                    "collector_protocol_ingress": True,
                    "route_protocol_activity": True,
                    "upstream_connected": False,
                    "ready": False,
                    "upstream_error": "",
                },
            ]
            sleeps: list[float] = []

            coordinator._shadow_learning_process_running = lambda: True
            coordinator._shadow_learning_route_status = lambda: statuses.pop(0)

            original_sleep = self.coordinator_module.asyncio.sleep

            async def _sleep(duration: float) -> None:
                sleeps.append(duration)

            self.coordinator_module.asyncio.sleep = _sleep
            try:
                await coordinator._async_wait_for_shadow_learning_ready(
                    trace_path=Path("/tmp/shadow-stale-connection.jsonl"),
                    timeout_seconds=5.0,
                    min_collector_connection_sequence=7,
                )
            finally:
                self.coordinator_module.asyncio.sleep = original_sleep

            self.assertEqual(sleeps, [1.0])
            self.assertEqual(statuses, [])

        import asyncio

        asyncio.run(_run())

    def test_start_shadow_learning_fails_when_redirect_readback_mismatches(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            route_stop_calls: list[bool] = []
            restore_calls: list[str] = []
            refresh_calls: list[bool] = []

            async def _async_start_shadow_learning_route(**kwargs):
                del kwargs
                return None

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                del endpoint, apply_changes
                return {"readback_endpoint": "eu.smartess.io,18899,TCP"}

            async def _async_stop_shadow_learning_route(**kwargs) -> None:
                self.assertTrue(str(kwargs.get("owner_id") or "").startswith("shadow_learning:"))
                route_stop_calls.append(True)

            async def _async_preflight_proxy_capture_network(**kwargs) -> None:
                del kwargs
                return None

            async def _async_save_shadow_learning_session_state(_state) -> None:
                return None

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return None

            async def _async_wait_for_shadow_learning_ready(**kwargs) -> None:
                raise AssertionError(f"must not wait after bad readback: {kwargs!r}")

            async def _async_best_effort_restore_after_start_failure(endpoint: str):
                restore_calls.append(endpoint)
                return True, ""

            async def _async_clear_shadow_learning_session_state() -> None:
                return None

            async def _async_request_refresh() -> None:
                refresh_calls.append(True)

            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-id",
                data={"collector_operation_mode": "smartess_cloud_home_assistant"},
                options={"proxy_capture_duration_minutes": 10},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=False,
                values={"collector_server_endpoint": "eu.smartess.io,18899,TCP"},
            )
            coordinator._runtime = types.SimpleNamespace(
                proxy_capture_route_running=lambda: False,
                shadow_learning_route_status=lambda: {
                    "running": True,
                    "collector_connected": False,
                    "collector_connection_sequence": 0,
                    "collector_protocol_ingress": False,
                    "route_protocol_activity": False,
                    "upstream_connected": False,
                    "ready": False,
                    "upstream_error": "",
                },
                async_start_shadow_learning_route=_async_start_shadow_learning_route,
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                async_stop_shadow_learning_route=_async_stop_shadow_learning_route,
            )
            coordinator._shadow_learning_process_running = lambda: False
            coordinator._proxy_capture_collector_ip = lambda: "192.168.1.55"
            coordinator._async_preflight_proxy_capture_network = _async_preflight_proxy_capture_network
            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator._async_save_shadow_learning_session_state = (
                _async_save_shadow_learning_session_state
            )
            coordinator._async_wait_for_shadow_learning_ready = (
                _async_wait_for_shadow_learning_ready
            )
            coordinator._async_best_effort_restore_after_start_failure = (
                _async_best_effort_restore_after_start_failure
            )
            coordinator._async_clear_shadow_learning_session_state = (
                _async_clear_shadow_learning_session_state
            )
            coordinator.async_request_refresh = _async_request_refresh
            coordinator._publish_tooling_values = lambda **_kwargs: None

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "smartess_collector_pn",
                new_callable=PropertyMock,
                return_value="E5000020000000",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_callback_target_endpoint",
                new_callable=PropertyMock,
                return_value="192.168.1.50,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_upstream_endpoint",
                new_callable=PropertyMock,
                return_value="eu.smartess.io,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_key",
                new_callable=PropertyMock,
                return_value="smartess-default",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_label",
                new_callable=PropertyMock,
                return_value="SmartESS Default",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_source",
                new_callable=PropertyMock,
                return_value="runtime",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_confidence",
                new_callable=PropertyMock,
                return_value="high",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "effective_metadata_snapshot",
                new_callable=PropertyMock,
                return_value={},
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "shadow_learning_effective_metadata",
                new_callable=PropertyMock,
                return_value={"register_schema_name": "modbus_smg/base.json"},
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_family",
                new_callable=PropertyMock,
                return_value="smartess",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "_effective_callback_server_host",
                new_callable=PropertyMock,
                return_value="192.168.1.50",
            ), patch.object(
                self.coordinator_module,
                "build_shadow_learning_seed",
                return_value=(
                    types.SimpleNamespace(write_response_mode="exception"),
                    [],
                ),
            ), patch.object(
                self.coordinator_module,
                "build_shadow_learning_preflight",
                return_value=types.SimpleNamespace(can_start=True, blockers=[]),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "shadow_learning_endpoint_redirect_not_confirmed",
                ):
                    await coordinator.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow-bad-readback.jsonl"),
                        raw_capture={},
                    )

            self.assertEqual(route_stop_calls, [True])
            self.assertEqual(restore_calls, ["eu.smartess.io,18899,TCP"])
            self.assertEqual(refresh_calls, [True])

        import asyncio

        asyncio.run(_run())

    def test_best_effort_restore_after_start_failure_reports_unconfirmed_restore(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            notifications: list[bool] = []
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")

            async def _async_restore_proxy_capture_endpoint(_endpoint: str):
                raise RuntimeError("write_timeout")

            coordinator._async_restore_proxy_capture_endpoint = _async_restore_proxy_capture_endpoint
            coordinator._notify_proxy_capture_restore_unconfirmed = lambda: notifications.append(True)

            confirmed, reason = await coordinator._async_best_effort_restore_after_start_failure(
                "eu.smartess.io,18899,TCP"
            )

            self.assertFalse(confirmed)
            self.assertEqual(reason, "write_timeout")
            self.assertTrue(notifications)

        import asyncio

        asyncio.run(_run())

    def test_restore_proxy_capture_endpoint_bypasses_transition_lock(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            calls: list[tuple[str, bool]] = []

            async def _async_set_collector_server_endpoint(endpoint: str, *, apply_changes: bool = True):
                calls.append((endpoint, apply_changes))
                return {"readback_endpoint": endpoint}

            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint
            )

            def _raise_if_high_level_collector_actions_disabled() -> None:
                raise AssertionError("restore should bypass high-level collector locks")

            coordinator._raise_if_high_level_collector_actions_disabled = (
                _raise_if_high_level_collector_actions_disabled
            )

            restored_endpoint = await coordinator._async_restore_proxy_capture_endpoint(
                "ess.eybond.com"
            )

            self.assertEqual(restored_endpoint, "ess.eybond.com")
            self.assertEqual(calls, [("ess.eybond.com", True)])

        import asyncio

        asyncio.run(_run())

    def test_collector_onboarding_values_publish_status_label(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={},
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"support_workflow_level_label": "Pending confirmation"}
        )
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
        )
        coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"

        values = coordinator._collector_onboarding_values(coordinator.data)

        self.assertEqual(values["collector_onboarding_status"], "Pending confirmation")
        self.assertTrue(values["collector_original_endpoint_known"])
        self.assertEqual(values["collector_original_endpoint_profile_key"], "")
        self.assertEqual(values["collector_original_endpoint_source"], "")
        self.assertEqual(values["collector_original_endpoint_observed_at"], "")

    def test_collector_onboarding_values_include_transport_profile_mismatch(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_cloud_family": "smartess_at",
                "driver_hint": "auto",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._connection_spec = types.SimpleNamespace(
            collector_cloud_family="smartess_at",
            collector_session_protocol="at_text",
            collector_identity_strategy="at_dtupn",
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
            listener_diagnostics=lambda: {
                "collector_callback_session_protocol": "",
                "collector_callback_identity_strategy": "",
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        values = coordinator._collector_transport_profile_runtime_values()

        self.assertEqual(values["collector_resolved_cloud_family"], "smartess_at")
        self.assertEqual(values["collector_resolved_session_protocol"], "at_text")
        self.assertEqual(values["collector_resolved_identity_strategy"], "at_dtupn")
        self.assertEqual(values["collector_connection_session_protocol"], "at_text")
        self.assertEqual(values["collector_connection_identity_strategy"], "at_dtupn")
        self.assertEqual(values["collector_runtime_link_session_protocol"], "")
        self.assertEqual(values["collector_runtime_link_identity_strategy"], "")

    def test_update_reconciles_transport_after_runtime_endpoint_discovery(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            refresh_count = 0
            reconcile_calls: list[tuple[str, str, str]] = []

            async def _async_refresh(*, poll_interval: float | None = None):
                nonlocal refresh_count
                del poll_interval
                refresh_count += 1
                return self.RuntimeSnapshot(
                    connected=True,
                    values={
                        "collector_server_endpoint": "dtu_ess.eybond.com,18899,TCP",
                        "refresh_count": refresh_count,
                    },
                )

            async def _async_reconcile_collector_session_profile(
                *,
                collector_session_protocol: str,
                collector_identity_strategy: str,
                collector_raw_passthrough_bootstrap: str = "",
                collector_raw_passthrough_frame_format: str = "",
                collector_raw_passthrough_min_interval_ms: int = 0,
                reason: str,
            ) -> bool:
                del (
                    collector_raw_passthrough_bootstrap,
                    collector_raw_passthrough_frame_format,
                    collector_raw_passthrough_min_interval_ms,
                )
                reconcile_calls.append(
                    (collector_session_protocol, collector_identity_strategy, reason)
                )
                return (
                    reason == "post_refresh_profile_discovery"
                    and collector_session_protocol == "at_text"
                    and collector_identity_strategy == "at_dtupn"
                )

            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={
                    "driver_hint": "auto",
                    "poll_interval": 10,
                },
                options={},
                title="Collector PN A0000000000001",
            )
            coordinator.hass = types.SimpleNamespace()
            coordinator.data = self.RuntimeSnapshot()
            coordinator._runtime = types.SimpleNamespace(
                async_refresh=_async_refresh,
                async_reconcile_collector_session_profile=(
                    _async_reconcile_collector_session_profile
                ),
                listener_diagnostics=lambda: {
                    "collector_callback_session_protocol": "",
                    "collector_callback_identity_strategy": "",
                },
            )
            coordinator._remembered_collector_server_endpoint = ""
            coordinator._device_overlay_merge_status = ""
            coordinator._tooling_values = {}
            coordinator._async_reconcile_network = AsyncMock(return_value=False)
            coordinator._async_reconcile_proxy_capture_session = AsyncMock(
                side_effect=lambda snapshot: snapshot
            )
            coordinator._async_reconcile_shadow_learning_session = AsyncMock(
                side_effect=lambda snapshot: snapshot
            )
            coordinator._async_restore_collector_original_endpoint_from_registry = AsyncMock()
            coordinator._async_remember_collector_server_endpoint = AsyncMock()
            coordinator._async_remember_runtime_identity = AsyncMock()
            coordinator._sync_forced_collector_operation_mode = lambda: None
            coordinator._configure_reverse_discovery_mode = lambda: None
            coordinator._async_warm_effective_metadata_cache = AsyncMock()
            coordinator._async_reconcile_collector_operation_mode_endpoint = AsyncMock()
            coordinator._write_exposure_context = lambda: {
                "variant_key": "",
                "profile_name": "",
                "profile_source_scope": "",
                "schema_source_scope": "",
                "device_scoped_overlay_active": False,
                "device_scoped_overlay_scope": "",
                "selected_control_keys": None,
                "effective_capabilities_experimental": False,
            }
            coordinator._support_workflow_values = lambda _snapshot: {}
            coordinator._collector_onboarding_values = lambda _snapshot: {}
            coordinator._proxy_capture_values = AsyncMock(return_value={})
            coordinator._prune_hidden_collector_values_for_mode = lambda _snapshot: None
            coordinator.async_sync_device_registry = lambda _snapshot: None

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_key",
                new_callable=PropertyMock,
                return_value="",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_label",
                new_callable=PropertyMock,
                return_value="",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_source",
                new_callable=PropertyMock,
                return_value="",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_profile_confidence",
                new_callable=PropertyMock,
                return_value="",
            ), patch(
                "custom_components.eybond_local._async_self_heal_sensor_display_precision",
                new_callable=AsyncMock,
                create=True,
            ):
                snapshot = await coordinator._async_update_data_with_runtime_lock()

            self.assertEqual(refresh_count, 2)
            self.assertIn(
                ("at_text", "at_dtupn", "post_refresh_profile_discovery"),
                reconcile_calls,
            )
            self.assertEqual(snapshot.values["collector_cloud_family"], "smartess_at")
            self.assertEqual(snapshot.values["refresh_count"], 2)

        asyncio.run(_run())

    def test_refresh_before_support_export_updates_snapshot_best_effort(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-1")
            refreshed = self.RuntimeSnapshot(values={"collector_resolved_session_protocol": "at_text"})
            coordinator.data = self.RuntimeSnapshot(values={})
            coordinator._async_update_data = AsyncMock(return_value=refreshed)

            await coordinator._async_refresh_before_support_export()

            self.assertIs(coordinator.data, refreshed)

        asyncio.run(_run())

    def test_refresh_before_support_export_is_fail_open(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-1")
            original = self.RuntimeSnapshot(values={"existing": True})
            coordinator.data = original
            coordinator._async_update_data = AsyncMock(side_effect=RuntimeError("boom"))

            await coordinator._async_refresh_before_support_export()

            self.assertIs(coordinator.data, original)

        asyncio.run(_run())

    def test_collector_original_endpoint_values_include_registry_summary(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            remember_collector_original_endpoint,
        )

        with tempfile.TemporaryDirectory() as tmp:
            remember_collector_original_endpoint(
                config_dir=Path(tmp),
                collector_pn="PN12345",
                original_endpoint_raw="dtu_ess.eybond.com,18899,TCP",
                cloud_profile_key="smartess_at",
                source="test_registry",
                observed_at="2026-06-22T10:00:00+00:00",
                last_seen_ip="192.168.2.209",
            )
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.hass = types.SimpleNamespace(
                config=types.SimpleNamespace(path=lambda: tmp),
            )
            coordinator.config_entry = types.SimpleNamespace(
                data={"collector_pn": "PN12345"},
                options={},
            )
            coordinator.data = self.RuntimeSnapshot(values={})
            coordinator._remembered_collector_server_endpoint = ""

            values = coordinator._collector_original_endpoint_runtime_values(
                include_registry=True
            )

        self.assertEqual(values["collector_registry_record_status"], "found")
        self.assertTrue(values["collector_registry_record_pn_known"])
        self.assertEqual(
            values["collector_registry_original_endpoint"],
            "dtu_ess.eybond.com,18899,TCP",
        )
        self.assertEqual(values["collector_registry_cloud_profile_key"], "smartess_at")
        self.assertEqual(values["collector_registry_source"], "test_registry")
        self.assertEqual(values["collector_registry_last_seen_ip"], "192.168.2.209")

    def test_integration_build_runtime_values_read_embedded_build_info(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package_dir = Path(tmp)
            (package_dir / "manifest.json").write_text(
                '{"version": "0.2.0-test"}',
                encoding="utf-8",
            )
            (package_dir / "BUILD_INFO.txt").write_text(
                "eybond_local build\n"
                "manifest_version: 0.2.0-test\n"
                "git_describe:     v0.2.0-test-1-gabcdef0\n"
                "git_commit:       abcdef0\n"
                "commit_date:      2026-06-23\n"
                "built_at:         20260623T194735Z\n",
                encoding="utf-8",
            )

            with patch.object(
                self.coordinator_module,
                "_package_dir",
                return_value=package_dir,
            ):
                values = self.coordinator_module._integration_build_runtime_values()

        self.assertEqual(values["integration_manifest_version"], "0.2.0-test")
        self.assertTrue(values["integration_build_info_present"])
        self.assertEqual(
            values["integration_build_git_describe"],
            "v0.2.0-test-1-gabcdef0",
        )
        self.assertEqual(values["integration_build_git_commit"], "abcdef0")
        self.assertEqual(values["integration_build_commit_date"], "2026-06-23")
        self.assertEqual(values["integration_build_built_at"], "20260623T194735Z")

    def test_async_set_collector_operation_mode_updates_runtime_endpoint_and_persists_mode(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={},
                options={},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "47.91.67.66,18899,TCP"},
            )
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
            )
            coordinator._runtime = types.SimpleNamespace(
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator._remembered_collector_server_endpoint = ""
            coordinator._tooling_values = {}
            calls: list[tuple[object, ...]] = []
            updates: list[dict[str, object]] = []

            async def _async_set_collector_server_endpoint(endpoint: str, *, apply_changes: bool = True):
                calls.append(("set_endpoint", endpoint, apply_changes))
                return {"readback_endpoint": endpoint, "status": "applied"}

            def _async_update_entry(entry, **kwargs) -> None:
                updates.append(dict(kwargs))
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])
                if "options" in kwargs:
                    entry.options = dict(kwargs["options"])

            async def _async_request_refresh() -> None:
                calls.append(("refresh",))

            coordinator._runtime.async_set_collector_server_endpoint = _async_set_collector_server_endpoint
            coordinator.async_request_refresh = _async_request_refresh
            coordinator.collector_operation_mode_change_reason = lambda *, target_mode="": None
            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
            )

            await coordinator.async_set_collector_operation_mode("home_assistant_only")

            coordinator.data.values["collector_server_endpoint"] = "192.168.1.50,18899,TCP"
            await coordinator.async_set_collector_operation_mode("smartess_cloud_home_assistant")

            self.assertEqual(
                calls,
                [
                    ("set_endpoint", "192.168.1.50,18899,TCP", True),
                    ("refresh",),
                    ("set_endpoint", "47.91.67.66,18899,TCP", True),
                    ("refresh",),
                ],
            )
            self.assertEqual(
                coordinator.config_entry.options.get("collector_operation_mode"),
                "smartess_cloud_home_assistant",
            )
            self.assertGreaterEqual(len(updates), 3)

        import asyncio

        asyncio.run(_run())

    def test_bridge_rejects_smartess_operation_mode_target(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_operation_mode": "home_assistant_only",
                "collector_virtual_bridge": True,
            },
            options={"collector_operation_mode": "home_assistant_only"},
        )
        coordinator.data = self.RuntimeSnapshot(
            connected=True,
            values={"collector_server_endpoint": "192.168.1.50,8899,TCP"},
        )

        with patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            "proxy_capture_overview",
            new_callable=PropertyMock,
            return_value=types.SimpleNamespace(status="ready"),
        ):
            reason = coordinator.collector_operation_mode_change_reason(
                target_mode="smartess_cloud_home_assistant"
            )

        self.assertEqual(reason, "collector_operation_mode_target_unavailable")

    def test_raw_collector_endpoint_stage_publishes_pending_override(self) -> None:
        async def _run() -> None:
            calls: list[tuple[str, bool]] = []
            refresh_calls: list[bool] = []

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                calls.append((endpoint, apply_changes))
                return {"requested_endpoint": endpoint, "status": "staged"}

            async def _async_request_refresh() -> None:
                refresh_calls.append(True)

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={"control_mode": "full"},
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "192.168.1.50,8899,TCP"},
            )
            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
            )
            coordinator._async_prepare_home_assistant_callback_listener = AsyncMock()
            coordinator.async_request_refresh = _async_request_refresh

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                await coordinator.async_set_raw_collector_server_endpoint(
                    endpoint="10.0.0.25,18899",
                    apply_changes=False,
                    confirm_redirect=True,
                )

            self.assertEqual(calls, [("10.0.0.25,18899", False)])
            self.assertEqual(refresh_calls, [True])
            self.assertEqual(
                coordinator.data.values["collector_callback_endpoint_pending"],
                "10.0.0.25,18899",
            )
            self.assertTrue(
                coordinator.data.values["collector_callback_endpoint_pending_apply_required"]
            )

        asyncio.run(_run())

    def test_raw_collector_endpoint_apply_clears_pending_override(self) -> None:
        async def _run() -> None:
            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                return {"requested_endpoint": endpoint, "status": "applied"}

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={"control_mode": "full"},
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={
                    "collector_server_endpoint": "192.168.1.50,8899,TCP",
                    "collector_callback_endpoint_pending": "10.0.0.25,18899",
                    "collector_callback_endpoint_pending_apply_required": True,
                },
            )
            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
            )
            coordinator._async_prepare_home_assistant_callback_listener = AsyncMock()

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                await coordinator.async_set_raw_collector_server_endpoint(
                    endpoint="10.0.0.25,18899",
                    apply_changes=True,
                    confirm_redirect=True,
                )

            self.assertNotIn("collector_callback_endpoint_pending", coordinator.data.values)
            self.assertNotIn(
                "collector_callback_endpoint_pending_apply_required",
                coordinator.data.values,
            )

        asyncio.run(_run())

    def test_bind_collector_to_home_assistant_clears_pending_endpoint_override(self) -> None:
        async def _run() -> None:
            calls: list[tuple[str, bool]] = []

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                calls.append((endpoint, apply_changes))
                return {"requested_endpoint": endpoint, "status": "applied"}

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={"control_mode": "full"},
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={
                    "collector_server_endpoint": "47.91.67.66,18899,TCP",
                    "collector_callback_endpoint_pending": "10.0.0.25,18899",
                    "collector_callback_endpoint_pending_apply_required": True,
                },
            )
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
            )
            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator._async_prepare_home_assistant_callback_listener = AsyncMock()

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                await coordinator.async_bind_collector_to_home_assistant(
                    confirm_redirect=True,
                )

            self.assertEqual(calls, [("192.168.1.50,18899,TCP", True)])
            self.assertNotIn("collector_callback_endpoint_pending", coordinator.data.values)
            self.assertNotIn(
                "collector_callback_endpoint_pending_apply_required",
                coordinator.data.values,
            )

        asyncio.run(_run())

    def test_rollback_collector_endpoint_stage_publishes_pending_override(self) -> None:
        async def _run() -> None:
            refresh_calls: list[bool] = []

            async def _async_set_collector_server_endpoint(
                endpoint: str, *, apply_changes: bool = True
            ) -> dict[str, object]:
                return {"requested_endpoint": endpoint, "status": "rollback_staged"}

            async def _async_request_refresh() -> None:
                refresh_calls.append(True)

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={"control_mode": "full"},
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            )
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
            )
            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator.async_request_refresh = _async_request_refresh

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                await coordinator.async_rollback_collector_server_endpoint(
                    apply_changes=False,
                    confirm_redirect=True,
                )

            self.assertEqual(refresh_calls, [True])
            self.assertEqual(
                coordinator.data.values["collector_callback_endpoint_pending"],
                "47.91.67.66,18899,TCP",
            )
            self.assertTrue(
                coordinator.data.values["collector_callback_endpoint_pending_apply_required"]
            )

        asyncio.run(_run())

    def test_async_set_control_mode_persists_mode_via_standard_entry_update(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={"control_mode": "auto"},
                options={"control_mode": "auto"},
            )
            calls: list[tuple[str, object]] = []

            def _async_update_entry(entry, **kwargs) -> None:
                calls.append(("update", dict(kwargs)))
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])
                if "options" in kwargs:
                    entry.options = dict(kwargs["options"])

            reloads: list[str] = []

            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(
                    async_update_entry=_async_update_entry,
                    async_schedule_reload=reloads.append,
                )
            )

            result = await coordinator.async_set_control_mode("full")

            self.assertEqual(result, "full")
            self.assertEqual(coordinator.config_entry.data["control_mode"], "full")
            self.assertEqual(coordinator.config_entry.options["control_mode"], "full")
            self.assertEqual(calls, [("update", {"data": {"control_mode": "full"}, "options": {"control_mode": "full"}})])
            # The capability-entity surface depends on the mode, and platforms
            # materialize entities once at setup: the switch must reload.
            self.assertEqual(reloads, ["entry-1"])

            # A no-op mode change must not reload.
            result = await coordinator.async_set_control_mode("full")
            self.assertEqual(result, "full")
            self.assertEqual(reloads, ["entry-1"])

        asyncio.run(_run())

    def test_poll_recommended_interval_keeps_headroom_after_overrun(self) -> None:
        recommended = self.coordinator_module._poll_recommended_interval_seconds(
            current_interval=10,
            observed_duration=11.2,
        )

        self.assertEqual(recommended, 16)

    def test_poll_metrics_reports_overrun_without_auto_adjusting_interval(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        entry = types.SimpleNamespace(
            entry_id="entry-poll",
            options={"poll_interval": 10},
        )
        updates: list[dict[str, object]] = []
        notifications: list[dict[str, object]] = []

        def _async_update_entry(config_entry, **kwargs) -> None:
            updates.append(dict(kwargs))
            if "options" in kwargs:
                config_entry.options = dict(kwargs["options"])

        def _async_create(hass, body, *, title, notification_id) -> None:
            del hass
            notifications.append(
                {
                    "body": body,
                    "title": title,
                    "notification_id": notification_id,
                }
            )

        coordinator.config_entry = entry
        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(language="en"),
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry),
        )
        coordinator._suppress_entry_reload_count = 0
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        self.coordinator_module.persistent_notification.async_create = _async_create

        async def _run() -> list[object]:
            snapshots = [
                self.RuntimeSnapshot(
                    values={
                        "collector_poll_duration_ms": 12000,
                        "runtime_driver_state": "driver_bound",
                    },
                    connected=True,
                    inverter=object(),
                )
                for _ in range(3)
            ]
            for snapshot in snapshots:
                coordinator._record_poll_cycle_metrics(
                    snapshot,
                    poll_interval_seconds=10,
                )
            return snapshots

        snapshots = asyncio.run(_run())

        self.assertEqual(entry.options["poll_interval"], 10)
        self.assertEqual(updates, [])
        self.assertEqual(len(notifications), 1)
        self.assertNotIn("collector_poll_interval_auto_adjusted", snapshots[-1].values)
        self.assertEqual(
            snapshots[-1].values["collector_poll_recommended_min_interval_seconds"],
            18,
        )

    def test_poll_metrics_can_use_full_coordinator_cycle_duration(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        snapshot = self.RuntimeSnapshot(
            values={"collector_poll_duration_ms": 700}
        )

        coordinator._record_poll_cycle_metrics(
            snapshot,
            poll_interval_seconds=10,
            duration_seconds=5.2,
            start_interval_seconds=10.1,
        )

        self.assertEqual(snapshot.values["collector_driver_poll_duration_ms"], 700)
        self.assertEqual(snapshot.values["collector_poll_duration_ms"], 5200)
        self.assertEqual(snapshot.values["collector_poll_utilization_percent"], 52)
        self.assertEqual(snapshot.values["collector_poll_start_interval_ms"], 10100)
        self.assertEqual(snapshot.values["collector_poll_target_start_interval_seconds"], 10)

    def test_poll_metrics_reports_scheduler_next_interval_as_target(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        coordinator.config_entry = types.SimpleNamespace(
            options={"poll_mode": "auto", "poll_interval": 10}
        )
        snapshot = self.RuntimeSnapshot(values={"collector_poll_duration_ms": 700})
        decision = self.coordinator_module.PollDecision(
            mode="auto",
            effective_interval=16,
            manual_interval=10,
            recommended_interval=16,
            utilization_percent=120,
            policy_min_interval=10,
            policy_max_interval=120,
            observed_duration=12,
            sample_count=1,
        )

        coordinator._record_poll_cycle_metrics(
            snapshot,
            poll_interval_seconds=10,
            duration_seconds=12.0,
            decision=decision,
        )

        self.assertEqual(snapshot.values["collector_poll_current_interval_seconds"], 10)
        self.assertEqual(snapshot.values["collector_poll_next_interval_seconds"], 16)
        self.assertEqual(snapshot.values["collector_poll_target_start_interval_seconds"], 16)

    def test_driver_unbound_manual_poll_suppresses_high_utilization_warning(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        entry = types.SimpleNamespace(
            entry_id="entry-poll",
            options={"poll_mode": "manual", "poll_interval": 10},
        )
        notifications: list[dict[str, object]] = []

        def _async_create(hass, body, *, title, notification_id) -> None:
            del hass
            notifications.append(
                {
                    "body": body,
                    "title": title,
                    "notification_id": notification_id,
                }
            )

        coordinator.config_entry = entry
        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(language="en"),
            config_entries=types.SimpleNamespace(async_update_entry=lambda *_args, **_kwargs: None),
        )
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        self.coordinator_module.persistent_notification.async_create = _async_create

        snapshot = self.RuntimeSnapshot(
            values={
                "collector_poll_duration_ms": 81533,
                "runtime_driver_state": "driver_unbound",
            },
            connected=True,
        )
        for _ in range(3):
            coordinator._record_poll_cycle_metrics(
                snapshot,
                poll_interval_seconds=10,
            )

        self.assertEqual(notifications, [])
        self.assertEqual(snapshot.values["collector_poll_context"], "detection")
        self.assertEqual(snapshot.values["collector_poll_utilization_percent"], 815)
        self.assertEqual(snapshot.values["collector_poll_high_utilization_streak"], 0)
        self.assertEqual(snapshot.values["collector_poll_overrun_streak"], 0)

    def test_driver_unbound_auto_uses_retry_interval_without_polluting_scheduler(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={},
            options={"poll_mode": "auto", "poll_interval": 10},
        )
        coordinator._poll_scheduler_driver_key = "auto"
        coordinator._poll_non_runtime_retry_interval_seconds = 0
        coordinator._ensure_poll_scheduler()

        current_interval = coordinator._current_poll_cycle_interval_seconds()
        decision = coordinator._poll_scheduler.observe(81.533, success=False)
        next_interval = coordinator._next_poll_cycle_interval_seconds(
            current_interval=current_interval,
            duration_seconds=81.533,
            poll_context="detection",
            decision=decision,
        )
        snapshot = self.RuntimeSnapshot(
            values={
                "collector_poll_duration_ms": 81533,
                "runtime_driver_state": "driver_unbound",
            },
            connected=True,
        )
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0

        coordinator._record_poll_cycle_metrics(
            snapshot,
            poll_interval_seconds=current_interval,
            duration_seconds=81.533,
            decision=decision,
            runtime_driver_state="driver_unbound",
            poll_context="detection",
            next_interval_seconds=next_interval,
        )

        self.assertEqual(decision.effective_interval, 10)
        self.assertEqual(coordinator._poll_scheduler.current_interval(), 10)
        self.assertEqual(next_interval, 106)
        self.assertEqual(coordinator._current_poll_cycle_interval_seconds(), 106)
        self.assertEqual(snapshot.values["collector_poll_context"], "detection")
        self.assertEqual(snapshot.values["collector_poll_next_interval_seconds"], 106)
        self.assertEqual(snapshot.values["collector_poll_detection_retry_interval_seconds"], 106)
        self.assertEqual(snapshot.values["collector_poll_high_utilization_streak"], 0)

    def test_collector_offline_poll_reports_collector_context(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-poll",
            options={"poll_mode": "manual", "poll_interval": 10},
        )
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        snapshot = self.RuntimeSnapshot(
            values={"runtime_driver_state": "collector_offline"},
            connected=False,
        )

        coordinator._record_poll_cycle_metrics(
            snapshot,
            poll_interval_seconds=10,
            duration_seconds=4.5,
        )

        self.assertEqual(snapshot.values["collector_poll_context"], "collector")
        self.assertEqual(snapshot.values["collector_poll_high_utilization_streak"], 0)
        self.assertEqual(snapshot.values["collector_poll_overrun_streak"], 0)

    def test_first_bound_cycle_after_unbound_does_not_train_auto_scheduler(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-poll",
                data={},
                options={"poll_mode": "auto", "poll_interval": 10},
            )
            coordinator.data = self.RuntimeSnapshot(
                values={"runtime_driver_state": "driver_unbound"},
                connected=True,
            )
            coordinator._diagnostic_active = False
            coordinator._runtime_operation_lock = asyncio.Lock()
            coordinator._poll_scheduler_driver_key = "auto"
            coordinator._poll_scheduler = self.coordinator_module.PollScheduler(
                policy=self.coordinator_module.poll_policy_for_driver("auto"),
                mode="auto",
                manual_interval=10,
            )
            observe_calls: list[dict[str, object]] = []
            original_observe = coordinator._poll_scheduler.observe

            def _observe(duration_seconds, *, success=True):
                observe_calls.append(
                    {
                        "duration_seconds": duration_seconds,
                        "success": success,
                    }
                )
                return original_observe(duration_seconds, success=success)

            coordinator._poll_scheduler.observe = _observe
            coordinator._poll_non_runtime_retry_interval_seconds = 0
            coordinator._poll_duration_ewma_seconds = 0.0
            coordinator._poll_duration_max_seconds = 0.0
            coordinator._poll_recent_durations_seconds = []
            coordinator._poll_last_cycle_started_monotonic = 0.0
            coordinator._collector_poll_overrun_streak = 0
            coordinator._collector_poll_high_utilization_streak = 0
            coordinator._poll_last_notification_monotonic = 0.0

            async def _poll_with_lock(**_kwargs):
                return self.RuntimeSnapshot(
                    values={
                        "runtime_driver_state": "driver_bound",
                        "collector_poll_duration_ms": 1000,
                    },
                    connected=True,
                    inverter=object(),
                )

            coordinator._async_update_data_with_runtime_lock = _poll_with_lock

            snapshot = await coordinator._async_update_data()

            self.assertEqual(snapshot.values["collector_poll_context"], "runtime")
            self.assertEqual(observe_calls[-1]["success"], False)
            self.assertEqual(coordinator._poll_scheduler.current_interval(), 10)
            self.assertNotIn(
                "collector_poll_detection_retry_interval_seconds",
                snapshot.values,
            )

        asyncio.run(_run())

    def test_unsupported_commands_persist_once_and_recheck_clears(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        entry = types.SimpleNamespace(
            entry_id="entry-1",
            options={},
        )
        updates: list[dict[str, object]] = []

        def _async_update_entry(config_entry, **kwargs) -> None:
            updates.append(dict(kwargs))
            if "options" in kwargs:
                config_entry.options = dict(kwargs["options"])

        coordinator.config_entry = entry
        coordinator.hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry),
        )
        coordinator._suppress_entry_reload_count = 0
        runtime_calls: list[tuple[str, object]] = []
        coordinator._runtime = types.SimpleNamespace(
            set_persistent_unsupported_commands=(
                lambda commands: runtime_calls.append(("set", commands))
            ),
            clear_unsupported_command_cache=(
                lambda: runtime_calls.append(("clear", None))
            ),
        )

        snapshot = self.RuntimeSnapshot(
            values={"driver_unsupported_commands": "QPIWS, Q1, QET"},
            connected=True,
            inverter=object(),
        )
        coordinator._maybe_persist_unsupported_commands(snapshot)
        self.assertEqual(entry.options["driver_unsupported_commands"], ["Q1", "QET", "QPIWS"])
        self.assertEqual(runtime_calls, [("set", ("Q1", "QET", "QPIWS"))])
        self.assertEqual(len(updates), 1)

        # Unchanged set: no second write.
        coordinator._maybe_persist_unsupported_commands(snapshot)
        self.assertEqual(len(updates), 1)

        async def _run() -> None:
            refreshes: list[bool] = []

            async def _request_refresh() -> None:
                refreshes.append(True)

            coordinator.async_request_refresh = _request_refresh
            await coordinator.async_recheck_supported_commands()
            self.assertEqual(refreshes, [True])

        asyncio.run(_run())
        self.assertNotIn("driver_unsupported_commands", entry.options)
        self.assertIn(("clear", None), runtime_calls)

    def test_collector_connection_watcher_refreshes_only_when_not_bound(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._shutdown_complete = False
        scheduled: list[object] = []

        def _create_task(coro):
            scheduled.append(coro)
            coro.close()
            return None

        async def _fake_refresh():
            return None

        coordinator.hass = types.SimpleNamespace(async_create_task=_create_task)
        coordinator.async_request_refresh = _fake_refresh

        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "driver_unbound"},
            connected=True,
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 1)

        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "collector_offline"},
            connected=False,
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 2)

        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "driver_bound"},
            connected=True,
            inverter=object(),
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 2)

        coordinator._shutdown_complete = True
        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "driver_unbound"},
            connected=True,
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 2)

    def test_is_clean_runtime_poll_cycle_matrix(self) -> None:
        clean = self.coordinator_module._is_clean_runtime_poll_cycle

        self.assertTrue(
            clean(
                previous_runtime_driver_state="driver_bound",
                runtime_driver_state="driver_bound",
                previous_reconnect_count=2,
                reconnect_count=2,
            )
        )
        # Recovery happened inside the cycle: reconnect counter advanced.
        self.assertFalse(
            clean(
                previous_runtime_driver_state="driver_bound",
                runtime_driver_state="driver_bound",
                previous_reconnect_count=2,
                reconnect_count=3,
            )
        )
        # Transition cycle: detection ran inside it.
        self.assertFalse(
            clean(
                previous_runtime_driver_state="driver_unbound",
                runtime_driver_state="driver_bound",
                previous_reconnect_count=0,
                reconnect_count=0,
            )
        )
        self.assertFalse(
            clean(
                previous_runtime_driver_state="driver_bound",
                runtime_driver_state="collector_offline",
                previous_reconnect_count=0,
                reconnect_count=0,
            )
        )

    def test_recovery_cycle_does_not_feed_scheduler_or_warning(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-poll",
                data={},
                options={"poll_mode": "manual", "poll_interval": 10},
            )
            coordinator.data = self.RuntimeSnapshot(
                values={
                    "runtime_driver_state": "driver_bound",
                    "runtime_reconnect_count": 1,
                },
                connected=True,
                inverter=object(),
            )
            coordinator._diagnostic_active = False
            coordinator._runtime_operation_lock = asyncio.Lock()
            coordinator._poll_scheduler_driver_key = "auto"
            coordinator._poll_scheduler = self.coordinator_module.PollScheduler(
                policy=self.coordinator_module.poll_policy_for_driver("auto"),
                mode="manual",
                manual_interval=10,
            )
            observe_calls: list[dict[str, object]] = []
            original_observe = coordinator._poll_scheduler.observe

            def _observe(duration_seconds, *, success=True):
                observe_calls.append({"success": success})
                return original_observe(duration_seconds, success=success)

            coordinator._poll_scheduler.observe = _observe
            coordinator._poll_non_runtime_retry_interval_seconds = 0
            coordinator._poll_duration_ewma_seconds = 0.0
            coordinator._poll_duration_max_seconds = 0.0
            coordinator._poll_recent_durations_seconds = []
            coordinator._poll_last_cycle_started_monotonic = 0.0
            coordinator._collector_poll_overrun_streak = 0
            coordinator._collector_poll_high_utilization_streak = 0
            coordinator._poll_last_notification_monotonic = 0.0

            async def _poll_with_lock(**_kwargs):
                return self.RuntimeSnapshot(
                    values={
                        "runtime_driver_state": "driver_bound",
                        "runtime_reconnect_count": 2,
                        "collector_poll_duration_ms": 66000,
                    },
                    connected=True,
                    inverter=object(),
                )

            coordinator._async_update_data_with_runtime_lock = _poll_with_lock

            snapshot = await coordinator._async_update_data()

            self.assertEqual(snapshot.values["collector_poll_context"], "runtime")
            self.assertEqual(observe_calls[-1]["success"], False)
            self.assertEqual(
                snapshot.values["collector_poll_high_utilization_streak"], 0
            )
            self.assertEqual(coordinator._poll_recent_durations_seconds, [])
            self.assertEqual(coordinator._poll_duration_max_seconds, 0.0)

        asyncio.run(_run())

    def test_recovery_cycles_do_not_trigger_high_utilization_notification(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-poll",
            options={"poll_mode": "manual", "poll_interval": 10},
        )
        notifications: list[str] = []

        def _async_create(hass, body, *, title, notification_id) -> None:
            del hass, title, notification_id
            notifications.append(body)

        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(language="en"),
        )
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        self.coordinator_module.persistent_notification.async_create = _async_create

        async def _run() -> None:
            for _ in range(3):
                snapshot = self.RuntimeSnapshot(
                    values={
                        "collector_poll_duration_ms": 66000,
                        "runtime_driver_state": "driver_bound",
                    },
                    connected=True,
                    inverter=object(),
                )
                coordinator._record_poll_cycle_metrics(
                    snapshot,
                    poll_interval_seconds=10,
                    duration_seconds=66.0,
                    clean_runtime_poll=False,
                )
                self.assertEqual(
                    snapshot.values["collector_poll_high_utilization_streak"], 0
                )

        asyncio.run(_run())

        self.assertEqual(notifications, [])
        self.assertEqual(coordinator._poll_recent_durations_seconds, [])
        self.assertEqual(coordinator._poll_duration_ewma_seconds, 0.0)

    def test_high_utilization_notification_dismissed_after_normalization(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-poll",
            options={"poll_mode": "manual", "poll_interval": 10},
        )
        notifications: list[str] = []
        dismissals: list[str] = []

        def _async_create(hass, body, *, title, notification_id) -> None:
            del hass, body, title
            notifications.append(notification_id)

        def _async_dismiss(hass, notification_id) -> None:
            del hass
            dismissals.append(notification_id)

        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(language="en"),
        )
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        self.coordinator_module.persistent_notification.async_create = _async_create
        self.coordinator_module.persistent_notification.async_dismiss = _async_dismiss

        def _bound_snapshot(duration_ms: int):
            return self.RuntimeSnapshot(
                values={
                    "collector_poll_duration_ms": duration_ms,
                    "runtime_driver_state": "driver_bound",
                },
                connected=True,
                inverter=object(),
            )

        async def _run() -> None:
            for _ in range(3):
                coordinator._record_poll_cycle_metrics(
                    _bound_snapshot(12000),
                    poll_interval_seconds=10,
                    duration_seconds=12.0,
                )
            self.assertEqual(len(notifications), 1)
            self.assertEqual(dismissals, [])

            for _ in range(3):
                coordinator._record_poll_cycle_metrics(
                    _bound_snapshot(1000),
                    poll_interval_seconds=10,
                    duration_seconds=1.0,
                )

        asyncio.run(_run())

        self.assertEqual(dismissals, notifications)
        self.assertFalse(coordinator._poll_notification_active)

    def test_poll_scheduler_policy_updates_from_detected_driver_key(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={},
            options={"poll_mode": "auto", "poll_interval": 10},
        )
        coordinator._poll_scheduler_driver_key = "auto"
        coordinator._ensure_poll_scheduler()

        self.assertEqual(coordinator._poll_scheduler.policy.min_auto_interval, 10)

        coordinator._update_poll_scheduler_policy_from_snapshot(
            self.RuntimeSnapshot(values={"driver_key": "modbus_smg"})
        )
        for _ in range(10):
            coordinator._poll_scheduler.observe(0.7)

        self.assertEqual(coordinator._poll_scheduler_driver_key, "modbus_smg")
        self.assertEqual(coordinator._poll_scheduler.policy.min_auto_interval, 3)
        self.assertEqual(coordinator._poll_scheduler.effective_interval, 3)

    def test_fixed_rate_poll_scheduler_sets_remaining_post_refresh_delay(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        snapshot = self.RuntimeSnapshot(values={})

        coordinator._sync_fixed_rate_poll_update_interval(
            snapshot,
            poll_interval_seconds=10,
            duration_seconds=5.2,
        )

        self.assertAlmostEqual(coordinator.update_interval.total_seconds(), 4.8)
        self.assertEqual(snapshot.values["collector_poll_scheduler_mode"], "fixed_rate")
        self.assertEqual(snapshot.values["collector_poll_effective_update_delay_ms"], 4800)

        coordinator._sync_fixed_rate_poll_update_interval(
            snapshot,
            poll_interval_seconds=10,
            duration_seconds=12.0,
        )

        self.assertEqual(coordinator.update_interval.total_seconds(), 1.0)
        self.assertEqual(snapshot.values["collector_poll_effective_update_delay_ms"], 1000)

    def test_old_entry_without_poll_mode_stays_manual_and_warns_high_utilization(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        entry = types.SimpleNamespace(
            entry_id="entry-poll",
            options={"poll_interval": 10},
        )
        updates: list[dict[str, object]] = []
        notifications: list[dict[str, object]] = []

        def _async_create(hass, body, *, title, notification_id) -> None:
            del hass
            notifications.append(
                {
                    "body": body,
                    "title": title,
                    "notification_id": notification_id,
                }
            )

        coordinator.config_entry = entry
        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(language="en"),
            config_entries=types.SimpleNamespace(
                async_update_entry=lambda *_args, **kwargs: updates.append(dict(kwargs))
            ),
        )
        coordinator._poll_duration_ewma_seconds = 0.0
        coordinator._poll_duration_max_seconds = 0.0
        coordinator._poll_recent_durations_seconds = []
        coordinator._collector_poll_overrun_streak = 0
        coordinator._collector_poll_high_utilization_streak = 0
        coordinator._poll_last_notification_monotonic = 0.0
        self.coordinator_module.persistent_notification.async_create = _async_create

        self.assertEqual(coordinator._configured_poll_mode(), "manual")

        async def _run() -> None:
            for _ in range(3):
                coordinator._record_poll_cycle_metrics(
                    self.RuntimeSnapshot(
                        values={
                            "collector_poll_duration_ms": 9200,
                            "runtime_driver_state": "driver_bound",
                        },
                        connected=True,
                        inverter=object(),
                    ),
                    poll_interval_seconds=10,
                )

        asyncio.run(_run())

        self.assertEqual(updates, [])
        self.assertEqual(entry.options["poll_interval"], 10)
        self.assertEqual(len(notifications), 1)
        self.assertIn("polling cycle is using", notifications[0]["body"])


if __name__ == "__main__":
    unittest.main()
