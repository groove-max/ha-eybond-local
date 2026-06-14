from __future__ import annotations

import asyncio
import dataclasses
import importlib
import importlib.util
from pathlib import Path
import sys
import types
import unittest


@dataclasses.dataclass
class _FakeInverter:
    """Minimal inverter stand-in supporting dataclasses.replace for overlay-merge tests."""

    capabilities: tuple = ()
    capability_groups: tuple = ()
    register_schema_name: str = ""
from unittest.mock import PropertyMock, patch


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

    const = _ensure_module("custom_components.eybond_local.const")
    const.CONF_COLLECTOR_IP = "collector_ip"
    const.CONF_COLLECTOR_CLOUD_FAMILY = "collector_cloud_family"
    const.CONF_COLLECTOR_OPERATION_MODE = "collector_operation_mode"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT = "collector_original_server_endpoint"
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
    support_cloud.fetch_and_export_smartess_device_bundle_cloud_evidence = (
        lambda *args, **kwargs: None
    )
    support_cloud.load_latest_cloud_evidence = lambda *args, **kwargs: None

    support_package = _ensure_module("custom_components.eybond_local.support.package")
    support_package.export_support_package = lambda *args, **kwargs: None

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
    def tearDownClass(cls) -> None:
        for name in reversed(_STUBBED_MODULE_NAMES):
            original = cls._saved_modules.get(name)
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        super().tearDownClass()

    def test_proxy_capture_notification_id_uses_bundle_stem(self) -> None:
        notification_id = self.coordinator_module._proxy_capture_notification_id(
            "entry-1",
            "/config/eybond_local/proxy_traces/session_bundle.zip",
        )

        self.assertEqual(
            notification_id,
            "eybond_local_proxy_capture_entry-1_session_bundle",
        )

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
        self.assertEqual(
            updated_options,
            [{"collector_original_server_endpoint": "47.91.67.66,18899,TCP"}],
        )

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

    def test_home_assistant_callback_target_uses_legacy_cloud_port_for_full_endpoints(self) -> None:
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
            "192.168.1.50,18899,TCP",
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

            self.assertEqual(prepared_targets, ["192.168.1.104,18899,TCP"])
            self.assertEqual(
                reverse_discovery_calls,
                [{"port": 0, "timeout": 0.75}],
            )
            self.assertEqual(
                result["collector_callback_target_endpoint"],
                "192.168.1.104,18899,TCP",
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
                collector_bridge_features=("local_only", "no_cloud", "wifi_params"),
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

    def test_remember_runtime_identity_persists_effective_snapshot_in_options(self) -> None:
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

        self.assertEqual(coordinator.config_entry.data["detected_model"], "PowMr 4.2kW")
        self.assertEqual(coordinator.config_entry.data["detected_serial"], "55355535553555")
        self.assertEqual(coordinator.config_entry.data["detection_confidence"], "high")

        persisted_snapshot = coordinator.config_entry.options.get("effective_metadata_snapshot")
        self.assertIsInstance(persisted_snapshot, dict)
        assert isinstance(persisted_snapshot, dict)
        self.assertEqual(persisted_snapshot.get("effective_owner_key"), "modbus_smg")
        self.assertEqual(
            persisted_snapshot.get("profile_name"),
            "modbus_smg/models/powmr_4200_protocol_1.json",
        )
        self.assertEqual(
            persisted_snapshot.get("register_schema_name"),
            "modbus_smg/models/powmr_4200_protocol_1.json",
        )
        self.assertNotIn("collector_cloud_profile_key", persisted_snapshot)
        self.assertNotIn("collector_cloud_profile_label", persisted_snapshot)
        self.assertNotIn("collector_cloud_profile_source", persisted_snapshot)
        self.assertNotIn("collector_cloud_profile_confidence", persisted_snapshot)
        self.assertEqual(persisted_snapshot.get("confidence"), "high")
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

        self.assertEqual(values, {"collector_onboarding_status": "Pending confirmation"})

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

            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(
                    async_update_entry=_async_update_entry,
                )
            )

            result = await coordinator.async_set_control_mode("full")

            self.assertEqual(result, "full")
            self.assertEqual(coordinator.config_entry.data["control_mode"], "full")
            self.assertEqual(coordinator.config_entry.options["control_mode"], "full")
            self.assertEqual(calls, [("update", {"data": {"control_mode": "full"}, "options": {"control_mode": "full"}})])

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
