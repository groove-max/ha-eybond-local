from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib
import json
from datetime import datetime
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

from helpers.homeassistant_stubs import ensure_module


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_coordinator_stubs() -> None:
    custom_components = ensure_module("custom_components")
    eybond_local = ensure_module("custom_components.eybond_local")
    runtime_package = ensure_module("custom_components.eybond_local.runtime")
    homeassistant = ensure_module("homeassistant")
    components = ensure_module("homeassistant.components")
    components_network = ensure_module("homeassistant.components.network")
    components_network_util = ensure_module("homeassistant.components.network.util")
    persistent_notification = ensure_module(
        "homeassistant.components.persistent_notification"
    )
    config_entries = ensure_module("homeassistant.config_entries")
    ha_const = ensure_module("homeassistant.const")
    helpers = ensure_module("homeassistant.helpers")
    device_registry = ensure_module("homeassistant.helpers.device_registry")
    entity_registry = ensure_module("homeassistant.helpers.entity_registry")
    network = ensure_module("homeassistant.helpers.network")
    update_coordinator = ensure_module("homeassistant.helpers.update_coordinator")
    util = ensure_module("homeassistant.util")
    dt = ensure_module("homeassistant.util.dt")
    util_logging = ensure_module("homeassistant.util.logging")

    class ConfigEntry:
        pass

    class ConfigEntryState:
        LOADED = "loaded"
        SETUP_IN_PROGRESS = "setup_in_progress"

    class DeviceInfo(dict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

    class DataUpdateCoordinator:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, *args, **kwargs):
            self.hass = args[0] if args else kwargs.get("hass")

    config_entries.ConfigEntry = ConfigEntry
    config_entries.ConfigEntryState = ConfigEntryState
    ha_const.EVENT_COMPONENT_LOADED = "component_loaded"
    device_registry.DeviceInfo = DeviceInfo
    device_registry.async_get = lambda hass: None
    entity_registry.async_get = lambda hass: None
    entity_registry.async_entries_for_device = lambda *args, **kwargs: ()
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
    homeassistant.const = ha_const
    homeassistant.helpers = helpers
    homeassistant.util = util
    components.persistent_notification = persistent_notification
    components.network = components_network
    components_network.util = components_network_util
    components_network_util.async_get_source_ip = lambda *args, **kwargs: "10.10.10.10"
    helpers.device_registry = device_registry
    helpers.entity_registry = entity_registry
    helpers.network = network
    helpers.update_coordinator = update_coordinator
    network.NoURLAvailableError = RuntimeError
    network.get_url = lambda *args, **kwargs: "http://127.0.0.1:8123"

    const = ensure_module("custom_components.eybond_local.const")
    const.CONF_COLLECTOR_IP = "collector_ip"
    const.CONF_ADVERTISED_SERVER_IP = "advertised_server_ip"
    const.CONF_ADVERTISED_TCP_PORT = "advertised_tcp_port"
    const.CONF_STRATEGY_TRANSITION_STATE = "connection_strategy_transition_state"
    const.CONF_COLLECTOR_CLOUD_FAMILY = "collector_cloud_family"
    const.CONF_COLLECTOR_OPERATION_MODE = "collector_operation_mode"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT = "collector_original_server_endpoint"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT = "collector_original_server_endpoint_observed_at"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY = "collector_original_server_endpoint_profile_key"
    const.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE = "collector_original_server_endpoint_source"
    const.CONF_COLLECTOR_PN = "collector_pn"
    const.CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL = "collector_confirmed_session_protocol"
    const.CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT = "collector_confirmed_session_protocol_observed_at"
    const.CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN = "collector_confirmed_session_protocol_pn"
    const.CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE = "collector_confirmed_session_protocol_source"
    const.COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE = "live_session"
    const.CONF_CONNECTION_TYPE = "connection_type"
    const.CONF_CONNECTION_MODE = "connection_mode"
    const.CONF_ENTRY_ROLE = "entry_role"
    const.ENTRY_ROLE_LISTENER = "listener"
    const.CONF_CONTROL_MODE = "control_mode"
    const.CONF_DETECTED_MODEL = "detected_model"
    const.CONF_DETECTED_DRIVER = "detected_driver"
    const.CONF_DETECTED_SERIAL = "detected_serial"
    const.CONF_DETECTION_CONFIDENCE = "detection_confidence"
    const.CONF_DISCOVERY_INTERVAL = "discovery_interval"
    const.CONF_DISCOVERY_TARGET = "discovery_target"
    const.CONF_DRIVER_HINT = "driver_hint"
    const.CONF_DRIVER_DETECTION_STRATEGY = "driver_detection_strategy"
    const.DEFAULT_DRIVER_DETECTION_STRATEGY = "first_match"
    const.DRIVER_DETECTION_STRATEGIES = frozenset({"first_match", "full_scan"})
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
    const.COLLECTOR_OPERATION_CLOUD_AND_HA = "smartess_cloud_home_assistant"
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
    const.CONF_CONNECTION_STRATEGY = "connection_strategy"
    const.CONF_CONNECTION_STRATEGY_EVIDENCE = "connection_strategy_evidence"
    const.CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT = "reboot_reconnect"
    const.CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER = "callback_trigger"
    const.CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION = "user_confirmed_session"
    const.CONNECTION_STRATEGY_INBOUND = "inbound"
    const.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND = "callback_on_demand"
    const.CONNECTION_STRATEGIES = {"inbound", "callback_on_demand"}
    const.DEFAULT_CONNECTION_STRATEGY = "inbound"
    const.CONF_ENDPOINT_CONTROL_POLICY = "endpoint_control_policy"
    const.ENDPOINT_CONTROL_EXTERNAL = "external"
    const.ENDPOINT_CONTROL_INTEGRATION_MANAGED = "integration_managed"
    const.ENDPOINT_CONTROL_POLICIES = {"external", "integration_managed"}
    const.DEFAULT_ENDPOINT_CONTROL_POLICY = "external"
    const.CONF_PROXY_ENABLED = "proxy_enabled"
    const.DEFAULT_PROXY_ENABLED = False
    const.CONF_ENDPOINT_WRITTEN_VALUE = "endpoint_written_value"
    const.CONF_ENDPOINT_WRITTEN_AT = "endpoint_written_at"
    const.MAX_PROXY_CAPTURE_DURATION_MINUTES = 120
    const.MIN_PROXY_CAPTURE_DURATION_MINUTES = 1
    const.LOCAL_METADATA_DIR = "eybond_local"

    connection_models = ensure_module("custom_components.eybond_local.connection.models")
    connection_spec_factory = ensure_module(
        "custom_components.eybond_local.connection.spec_factory"
    )
    connection_spec_factory.build_connection_spec = lambda *args, **kwargs: None

    entity_scope = importlib.import_module(
        "custom_components.eybond_local.collector.entity_scope"
    )

    control_policy = ensure_module("custom_components.eybond_local.control_policy")
    control_policy.can_expose_capability = lambda *args, **kwargs: True
    control_policy.can_expose_preset = lambda *args, **kwargs: True
    control_policy.controls_enabled = lambda *args, **kwargs: True
    control_policy.controls_reason = lambda *args, **kwargs: ""
    control_policy.controls_summary = lambda *args, **kwargs: ""

    drivers_registry = ensure_module("custom_components.eybond_local.drivers.registry")
    drivers_registry.get_driver = lambda *args, **kwargs: None
    drivers_registry.all_write_capabilities = lambda *args, **kwargs: []
    # A realistic test double for the neutral policy resolver: mirrors what each
    # real driver declares via ``poll_policy_for`` (the coordinator only consumes
    # the resolved policy, so the double encodes the expected driver mapping).
    # The concrete policies now live in the driver modules; the neutral contract
    # exports only PollPolicy + DEFAULT, so the double builds the envelopes here.
    from custom_components.eybond_local.poll_policy import (
        DEFAULT_POLL_POLICY as _DEFAULT_POLL_POLICY,
        PollPolicy as _PollPolicy,
    )

    _SMG_POLICY = _PollPolicy(min_auto_interval=3.0, max_auto_interval=60.0)
    _FAST_POLICY = _PollPolicy(min_auto_interval=5.0, max_auto_interval=90.0)
    _PI30_POLICY = _PollPolicy(
        min_auto_interval=2.0, max_auto_interval=120.0, min_manual_interval=2.0
    )
    _STUB_DRIVER_POLICIES = {
        "modbus_smg": _SMG_POLICY,
        "srne_modbus": _FAST_POLICY,
        "must_pv_ph18": _FAST_POLICY,
        "smartess_local": _FAST_POLICY,
        # eybond_g_ascii / pi18 inherit the neutral default (ASCII == DEFAULT).
        "eybond_g_ascii": _DEFAULT_POLL_POLICY,
        "pi18": _DEFAULT_POLL_POLICY,
        "pi30": _PI30_POLICY,
    }
    drivers_registry.poll_policy_for_driver_key = (
        lambda driver_key="", inverter=None: _STUB_DRIVER_POLICIES.get(
            str(driver_key or "").strip(), _DEFAULT_POLL_POLICY
        )
    )
    def _serial_is_stable(driver_key="", inverter=None):
        if str(getattr(inverter, "variant_key", "") or "").strip() == "smartess_0925":
            return False
        details = getattr(inverter, "details", {})
        if str(driver_key or "").strip() == "pi30" and isinstance(details, dict):
            trust = details.get("serial_identity_trust")
            if trust is not None:
                return bool(
                    str(getattr(inverter, "serial_number", "") or "").strip()
                    and trust == "trusted"
                )
        return True

    drivers_registry.serial_is_stable = _serial_is_stable
    # Neutral support-marker resolver double: the coordinator only projects the
    # driver's verdict, so the double returns no special marker.
    drivers_registry.support_marker = (
        lambda driver_key="", variant_key="", profile_name="": None
    )

    fixtures_utils = ensure_module("custom_components.eybond_local.fixtures.utils")
    fixtures_utils.anonymize_fixture_json = lambda *args, **kwargs: None
    fixtures_utils.build_command_fixture_responses = lambda *args, **kwargs: None

    effective_metadata = ensure_module(
        "custom_components.eybond_local.metadata.effective_metadata"
    )
    effective_metadata.resolve_effective_metadata_selection = (
        lambda *args, **kwargs: None
    )

    local_metadata = ensure_module("custom_components.eybond_local.metadata.local_metadata")
    local_metadata.clear_local_metadata_loader_caches = lambda *args, **kwargs: None
    local_metadata.create_local_profile_draft = lambda *args, **kwargs: None
    local_metadata.create_local_schema_draft = lambda *args, **kwargs: None
    local_metadata.rollback_local_metadata_overrides = lambda *args, **kwargs: None

    smartess_draft = ensure_module("custom_components.eybond_local.metadata.smartess_draft")

    class SmartEssKnownFamilyDraftPlan:
        pass

    smartess_draft.SmartEssKnownFamilyDraftPlan = SmartEssKnownFamilyDraftPlan
    smartess_draft.create_smartess_known_family_draft = lambda *args, **kwargs: None
    smartess_draft.resolve_smartess_known_family_draft_plan = (
        lambda *args, **kwargs: None
    )

    smartess_smg_bridge = ensure_module(
        "custom_components.eybond_local.metadata.smartess_smg_bridge"
    )

    class SmartEssSmgBridgePlan:
        pass

    smartess_smg_bridge.SmartEssSmgBridgePlan = SmartEssSmgBridgePlan
    smartess_smg_bridge.create_smartess_smg_bridge_draft = lambda *args, **kwargs: None
    smartess_smg_bridge.resolve_smartess_smg_bridge_plan = (
        lambda *args, **kwargs: None
    )

    models = ensure_module("custom_components.eybond_local.models")

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

    class ProbeTarget:
        def __init__(self, devcode=0, collector_addr=0, device_addr=0):
            self.devcode = devcode
            self.collector_addr = collector_addr
            self.device_addr = device_addr

        @property
        def link_route(self):
            from custom_components.eybond_local.link_models import EybondLinkRoute

            return EybondLinkRoute(
                devcode=self.devcode,
                collector_addr=self.collector_addr,
            )

    class DetectedInverter:
        def __init__(
            self,
            *,
            driver_key,
            protocol_family,
            model_name,
            serial_number,
            probe_target,
            variant_key="default",
            details=None,
            profile_name="",
            register_schema_name="",
            capability_groups=(),
            capabilities=(),
            capability_presets=(),
        ):
            self.driver_key = driver_key
            self.protocol_family = protocol_family
            self.model_name = model_name
            self.serial_number = serial_number
            self.probe_target = probe_target
            self.variant_key = variant_key
            self.details = details or {}
            self.profile_name = profile_name
            self.register_schema_name = register_schema_name
            self.capability_groups = capability_groups
            self.capabilities = capabilities
            self.capability_presets = capability_presets

    class RuntimeSnapshot:
        def __init__(self, values=None, inverter=None, collector=None, connected=True):
            self.values = values or {}
            self.inverter = inverter
            self.collector = collector
            self.connected = connected

        @property
        def collector_server_endpoint(self):
            candidate = getattr(self.collector, "collector_server_endpoint", "")
            return candidate or self.values.get("collector_server_endpoint", "")

        def set_collector_server_endpoint(self, endpoint):
            if self.collector is not None:
                self.collector.collector_server_endpoint = endpoint
            if endpoint:
                self.values["collector_server_endpoint"] = endpoint
            else:
                self.values.pop("collector_server_endpoint", None)

        @property
        def collector_cloud_profile(self):
            key = getattr(self.collector, "collector_cloud_profile_key", "")
            key = key or getattr(self.collector, "smartess_protocol_profile_key", "")
            if not key:
                key = self.values.get("collector_cloud_profile_key", "")
            if not key:
                key = self.values.get("smartess_protocol_profile_key", "")
            if not key:
                return CollectorCloudProfile()
            return CollectorCloudProfile(
                key=key,
                label=(
                    getattr(self.collector, "collector_cloud_profile_label", "")
                    or getattr(self.collector, "smartess_protocol_name", "")
                    or getattr(self.collector, "smartess_protocol_asset_name", "")
                    or self.values.get("collector_cloud_profile_label", "")
                    or self.values.get("smartess_protocol_name", "")
                ),
                source=(
                    getattr(self.collector, "collector_cloud_profile_source", "")
                    or self.values.get("collector_cloud_profile_source", "")
                    or "runtime_observed"
                ),
                confidence=(
                    getattr(self.collector, "collector_cloud_profile_confidence", "")
                    or self.values.get("collector_cloud_profile_confidence", "")
                    or "high"
                ),
            )

        def set_collector_cloud_profile(self, profile):
            if self.collector is not None:
                self.collector.collector_cloud_profile_key = profile.key
                self.collector.collector_cloud_profile_label = profile.label
                self.collector.collector_cloud_profile_source = profile.source
                self.collector.collector_cloud_profile_confidence = profile.confidence
            for key, value in {
                "collector_cloud_profile_key": profile.key,
                "collector_cloud_profile_label": profile.label,
                "collector_cloud_profile_source": profile.source,
                "collector_cloud_profile_confidence": profile.confidence,
            }.items():
                if value:
                    self.values[key] = value
                else:
                    self.values.pop(key, None)

    class CollectorInfo:
        collector_server_endpoint = ""

    class CollectorCloudProfile:
        def __init__(self, key="", label="", source="", confidence=""):
            self.key = key
            self.label = label
            self.source = source
            self.confidence = confidence

        @property
        def known(self):
            return bool(self.key)

    models.CollectorInfo = CollectorInfo
    models.CollectorCloudProfile = CollectorCloudProfile
    models.CapabilityChoice = CapabilityChoice
    models.CapabilityCondition = CapabilityCondition
    models.CapabilityGroup = CapabilityGroup
    models.CapabilityPreset = CapabilityPreset
    models.CapabilityPresetItem = CapabilityPresetItem
    models.CapabilityRecommendation = CapabilityRecommendation
    models.BinarySensorDescription = BinarySensorDescription
    models.DetectedInverter = DetectedInverter
    models.MeasurementDescription = MeasurementDescription
    models.ProbeTarget = ProbeTarget
    models.RegisterValueSpec = RegisterValueSpec
    models.RuntimeSnapshot = RuntimeSnapshot
    models.WriteCapability = WriteCapability
    models.decimals_for_divisor = lambda _divisor: 0

    runtime_factory = ensure_module("custom_components.eybond_local.runtime.factory")
    runtime_factory.create_runtime_manager = lambda *args, **kwargs: None

    runtime_manager = ensure_module("custom_components.eybond_local.runtime.manager")

    class RuntimeManager:
        pass

    runtime_manager.RuntimeManager = RuntimeManager

    schema = ensure_module("custom_components.eybond_local.schema")
    schema.build_runtime_ui_schema = lambda *args, **kwargs: None
    schema.capability_write_exposure_allowed = lambda *args, **kwargs: True
    schema.preset_write_exposure_allowed = lambda *args, **kwargs: True

    support_bundle = ensure_module("custom_components.eybond_local.support.bundle")
    support_bundle.build_support_bundle_payload = lambda *args, **kwargs: None
    support_bundle.export_support_bundle = lambda *args, **kwargs: None

    support_cloud = ensure_module("custom_components.eybond_local.support.cloud_evidence")
    support_cloud.infer_evidence_provider = lambda payload: (
        str((payload or {}).get("provider") or "").strip().lower()
        if isinstance(payload, dict)
        else ""
    )
    support_cloud.fetch_and_export_smartess_device_bundle_cloud_evidence = (
        lambda *args, **kwargs: None
    )
    support_cloud.fetch_and_export_valuecloud_device_bundle_cloud_evidence = (
        lambda *args, **kwargs: None
    )
    support_cloud.load_latest_cloud_evidence = lambda *args, **kwargs: None

    class _CloudEvidenceRecord:  # minimal stand-in for the neutral provider module
        def __init__(self, path="", payload=None) -> None:
            self.path = path
            self.payload = payload

    support_cloud.CloudEvidenceRecord = _CloudEvidenceRecord

    support_package = ensure_module("custom_components.eybond_local.support.package")
    support_package.export_support_package = lambda *args, **kwargs: None
    support_package.support_packages_root = (
        lambda config_dir: Path(config_dir) / "eybond_local" / "support_packages"
    )

    support_proxy_capture = ensure_module(
        "custom_components.eybond_local.support.proxy_capture"
    )
    support_proxy_capture.PROXY_WIRE_TRANSPARENT = "transparent"
    support_proxy_capture.build_proxy_capture_overview = lambda *args, **kwargs: None
    support_proxy_capture.resolve_proxy_wire_mode = (
        lambda collector, cloud: (
            "transparent" if collector and cloud in {"at_text", "eybond_framed"} else ""
        )
    )

    support_proxy_session = ensure_module(
        "custom_components.eybond_local.support.proxy_capture.session"
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

    support_proxy_trace = ensure_module(
        "custom_components.eybond_local.support.proxy_capture.trace"
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
    support_proxy_trace.proxy_trace_root = (
        lambda config_dir: Path(config_dir) / "eybond_local" / "proxy_traces"
    )
    support_proxy_trace.refresh_proxy_capture_session_lease = (
        lambda state, **kwargs: state
    )
    support_proxy_trace.save_proxy_capture_session_state = (
        lambda *args, **kwargs: None
    )

    support_shadow_backend = ensure_module(
        "custom_components.eybond_local.support.shadow_learning.backend"
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

    support_shadow_proxy = ensure_module(
        "custom_components.eybond_local.support.shadow_learning.proxy"
    )
    support_shadow_proxy.route_status_indicates_control_ready = (
        lambda status: bool(status.get("collector_connected"))
        and (
            bool(status.get("ready"))
            or bool(status.get("route_protocol_activity"))
            or bool(status.get("collector_protocol_ingress"))
        )
    )

    support_shadow_session = ensure_module(
        "custom_components.eybond_local.support.shadow_learning.session"
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

    support_workflow = ensure_module("custom_components.eybond_local.support.workflow")
    support_workflow.build_support_workflow_state = lambda *args, **kwargs: {}

    support_diagnostic_export = ensure_module(
        "custom_components.eybond_local.support.diagnostic_export"
    )
    support_diagnostic_export.export_diagnostic_run = lambda *args, **kwargs: None

    support_diagnostic_runner = ensure_module(
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
    "custom_components.eybond_local.runtime.shadow_learning_facade",
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
    "custom_components.eybond_local.support.proxy_capture.session",
    "custom_components.eybond_local.support.proxy_capture.trace",
    "custom_components.eybond_local.support.shadow_learning",
    "custom_components.eybond_local.support.shadow_learning.backend",
    "custom_components.eybond_local.support.shadow_learning.proxy",
    "custom_components.eybond_local.support.shadow_learning.runtime",
    "custom_components.eybond_local.support.shadow_learning.session",
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
        self.config_entries: set[str] = set()
        self.name_by_user = None


class FakeRegistry:
    def __init__(self) -> None:
        self._devices_by_key: dict[frozenset[tuple[str, str]], FakeDevice] = {}
        self._counter = 0
        self.removed_device_ids: list[str] = []

    @property
    def devices(self):
        return {device.id: device for device in self._devices_by_key.values()}

    def async_get_device(self, identifiers=None, connections=None):
        del connections
        if not identifiers:
            return None
        return self._devices_by_key.get(frozenset(identifiers))

    def async_get_or_create(self, config_entry_id=None, **info):
        identifiers = set(info.get("identifiers") or set())
        key = frozenset(identifiers)
        device = self._devices_by_key.get(key)
        if device is None:
            self._counter += 1
            device = FakeDevice(f"device-{self._counter}", identifiers)
            self._devices_by_key[key] = device

        if type(config_entry_id) is str and config_entry_id:
            device.config_entries.add(config_entry_id)

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

    def async_update_device(self, device_id, **kwargs):
        for device in self._devices_by_key.values():
            if device.id != device_id:
                continue
            for key, value in kwargs.items():
                if hasattr(device, key):
                    setattr(device, key, value)
            return device
        return None

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

        coordinator_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.root"
        )

        cls.coordinator_module = coordinator_module
        cls.coordinator_polling_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.polling"
        )
        cls.coordinator_inverter_profile_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.inverter_profile"
        )
        cls.coordinator_tooling_projection_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.tooling_projection"
        )
        cls.coordinator_snapshot_projection_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.snapshot_projection"
        )
        cls.coordinator_cloud_tools_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.cloud_tools"
        )
        cls.coordinator_diagnostics_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.diagnostics"
        )
        cls.coordinator_startup_module = importlib.import_module(
            "custom_components.eybond_local.runtime.coordinator.startup"
        )
        cls.platform_context_module = importlib.import_module(
            "custom_components.eybond_local.platform_context"
        )
        cls.RuntimeSnapshot = sys.modules[
            "custom_components.eybond_local.models"
        ].RuntimeSnapshot

    def _support_readiness(
        self,
        *,
        proxy_can_start: bool = True,
        active_can_start: bool = True,
    ):
        module = importlib.import_module(
            "custom_components.eybond_local.support.acquisition"
        )
        ready = module.SupportOperationReadiness(
            visible=True,
            can_start=True,
            blocker="",
        )

        def _route(can_start: bool):
            return (
                ready
                if can_start
                else module.SupportOperationReadiness(
                    visible=True,
                    can_start=False,
                    blocker="operating_profile_requires_cloud_and_ha",
                )
            )

        return module.SupportAcquisitionReadiness(
            collector_identified=True,
            inverter_identified=False,
            cloud_metadata_read=ready,
            proxy_capture=_route(proxy_can_start),
            active_control_learning=_route(active_can_start),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        for name in reversed(_STUBBED_MODULE_NAMES):
            original = cls._saved_modules.get(name)
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        # Restoring ``sys.modules`` directly does not recreate the child-module
        # attributes that normal import machinery installs on each parent
        # package. Other suites may already have loaded additional descendants
        # (for example ``support.shadow_learning.overlay_generator``) that this
        # coordinator harness never stubs. Rebind every surviving integration
        # child so a later dotted patch/import sees the same module topology it
        # had before this harness replaced the parent packages.
        integration_prefix = "custom_components.eybond_local."
        for name, module in sorted(
            tuple(sys.modules.items()), key=lambda item: item[0].count(".")
        ):
            if module is None or not name.startswith(integration_prefix):
                continue
            parent_name, separator, child_name = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if separator and parent is not None:
                setattr(parent, child_name, module)
        super().tearDownClass()

    def setUp(self) -> None:
        # CP2C: the endpoint-operation authority is a process-level singleton, so
        # any token a prior test intentionally leaves held (e.g. a failed-restore
        # test that must retain ownership) would otherwise leak into the next
        # test's entry. Reset it per test to keep bare-coordinator tests isolated.
        super().setUp()
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY,
        )

        COLLECTOR_ENDPOINT_OPERATION_AUTHORITY._held.clear()
        COLLECTOR_ENDPOINT_OPERATION_AUTHORITY._counter = 0

    # ---- Batch 1 CP1b: the REAL transition-facade _commit (DI boundary) ----
    PN = "V001020SYN62344022"
    TS = "2026-07-16T10:00:00+00:00"

    def _callback_terminal(self, advertised, *, pn=None):
        from custom_components.eybond_local.connection.recovery.terminal import (
            RecoveryTerminalInput,
        )
        from custom_components.eybond_local.connection.recovery_contract import (
            CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            CallbackRecoveryProof,
        )

        pn = pn or self.PN
        return RecoveryTerminalInput(
            collector_pn=pn,
            callback_proof=CallbackRecoveryProof(
                method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
                collector_pn=pn,
                identity_source="fc2_parameter_2",
                verified_at=self.TS,
                trigger_target="203.0.113.10:58899",
                advertised_ha_endpoint=advertised,
                listener_port=18899,
            ),
        )

    def _inbound_terminal(self, *, pn=None):
        from custom_components.eybond_local.connection.recovery.terminal import (
            RecoveryTerminalInput,
        )
        from custom_components.eybond_local.connection.recovery_contract import (
            INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
            InboundRecoveryProof,
        )

        pn = pn or self.PN
        return RecoveryTerminalInput(
            collector_pn=pn,
            inbound_proof=InboundRecoveryProof(
                method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
                collector_pn=pn,
                identity_source="fc2_parameter_2",
                verified_at=self.TS,
                session_protocol="eybond_framed",
            ),
        )

    def _commit_coordinator(self, *, options):
        """A bare real coordinator wired for ``_apply_transition_commit``."""

        entry = types.SimpleNamespace(
            data={
                "connection_type": "eybond",
                "collector_pn": self.PN,
                "connection_strategy": "callback_on_demand",
                "collector_ip": "203.0.113.10",
            },
            options=dict(options),
            entry_id="entry-cp1b",
            update_listeners=[],
        )
        updates: list[dict] = []
        reloads: list[str] = []

        def _upd(config_entry, **kw):
            if "options" in kw:
                self.assertIsNotNone(kw["options"])
            updates.append(dict(kw))
            if kw.get("data") is not None:
                config_entry.data = dict(kw["data"])
            if "options" in kw and kw["options"] is not None:
                config_entry.options = dict(kw["options"])
            return True

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(
                async_update_entry=_upd,
                async_schedule_reload=lambda eid: reloads.append(eid),
            ),
            data={},
        )
        c = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        c.config_entry = entry
        c.hass = hass
        return c, entry, updates, reloads

    def test_di_inbound_commit_persists_route_strategy_contract_one_update(self) -> None:
        c, entry, updates, reloads = self._commit_coordinator(
            options={"advertised_server_ip": "stale", "advertised_tcp_port": 1, "control_mode": "manual"},
        )
        refusal = c._apply_transition_commit(
            {"connection_strategy": "inbound"},
            self._inbound_terminal(),
            {},
            advertised_host="192.168.1.50",
            advertised_port=8899,
        )
        self.assertEqual(refusal, "")
        # route + strategy + contract land in the ONE data update.
        self.assertEqual(entry.data["advertised_server_ip"], "192.168.1.50")
        self.assertEqual(entry.data["advertised_tcp_port"], 8899)
        self.assertEqual(entry.data["connection_strategy"], "inbound")
        self.assertIn("recovery_contract", entry.data)
        # stale options route dropped; unrelated option preserved.
        self.assertNotIn("advertised_server_ip", entry.options)
        self.assertNotIn("advertised_tcp_port", entry.options)
        self.assertEqual(entry.options["control_mode"], "manual")
        # exactly one update + one reload.
        self.assertEqual(len(updates), 1)
        self.assertEqual(reloads, ["entry-cp1b"])

    def test_di_callback_commit_persists_route_from_proof(self) -> None:
        c, entry, updates, reloads = self._commit_coordinator(
            options={"advertised_server_ip": "stale", "advertised_tcp_port": 1},
        )
        refusal = c._apply_transition_commit(
            {"connection_strategy": "callback_on_demand"},
            self._callback_terminal("195.191.72.37:18899"),
            {},
            advertised_host="195.191.72.37",
            advertised_port=18899,
        )
        self.assertEqual(refusal, "")
        # persisted route == the callback proof's advertised endpoint.
        self.assertEqual(entry.data["advertised_server_ip"], "195.191.72.37")
        self.assertEqual(entry.data["advertised_tcp_port"], 18899)
        self.assertNotIn("advertised_server_ip", entry.options)
        self.assertEqual(len(updates), 1)
        self.assertEqual(len(reloads), 1)

    def test_di_callback_proof_mismatch_commits_nothing(self) -> None:
        c, entry, updates, reloads = self._commit_coordinator(options={})
        refusal = c._apply_transition_commit(
            {"connection_strategy": "callback_on_demand"},
            self._callback_terminal("1.2.3.4:9000"),  # proof != attempted
            {},
            advertised_host="195.191.72.37",
            advertised_port=18899,
        )
        self.assertEqual(refusal, "transition_callback_route_mismatch")
        # NOTHING committed: no route, no strategy change, no update, no reload.
        self.assertNotIn("advertised_server_ip", entry.data)
        self.assertEqual(entry.data["connection_strategy"], "callback_on_demand")
        self.assertEqual(updates, [])
        self.assertEqual(reloads, [])

    def test_di_inbound_missing_proof_commits_nothing(self) -> None:
        c, entry, updates, reloads = self._commit_coordinator(options={})
        refusal = c._apply_transition_commit(
            {"connection_strategy": "inbound"},
            self._callback_terminal("192.168.1.50:8899"),  # wrong proof type
            {},
            advertised_host="192.168.1.50",
            advertised_port=8899,
        )
        self.assertEqual(refusal, "transition_inbound_route_unproven")
        self.assertEqual(updates, [])
        self.assertEqual(reloads, [])

    def test_di_non_strategy_merge_persists_no_route(self) -> None:
        # An inbound_recovered_after_restore merge (no strategy in updates) earns
        # no advertised route, even with a valid inbound proof.
        c, entry, updates, reloads = self._commit_coordinator(options={})
        refusal = c._apply_transition_commit(
            {},  # no connection_strategy committed
            self._inbound_terminal(),
            {},
            advertised_host="192.168.1.50",
            advertised_port=8899,
        )
        self.assertEqual(refusal, "")
        self.assertNotIn("advertised_server_ip", entry.data)
        self.assertEqual(updates[0]["options"], {})

    def test_di_invalid_committed_strategy_commits_nothing(self) -> None:
        # A bogus committed strategy is refused BEFORE any write -- it is never a
        # harmless merge that could persist "connection_strategy=bogus".
        c, entry, updates, reloads = self._commit_coordinator(options={})
        refusal = c._apply_transition_commit(
            {"connection_strategy": "bogus"},
            self._inbound_terminal(),
            {},
            advertised_host="192.168.1.50",
            advertised_port=8899,
        )
        self.assertEqual(refusal, "transition_committed_strategy_invalid")
        self.assertEqual(updates, [])
        self.assertEqual(reloads, [])
        # entry is unchanged -- the bogus strategy never reached the entry.
        self.assertEqual(entry.data["connection_strategy"], "callback_on_demand")

    def test_proxy_capture_notification_id_uses_capture_trace_stem(self) -> None:
        notification_id = self.coordinator_module._proxy_capture_notification_id(
            "entry-1",
            "/config/eybond_local/proxy_traces/session_trace.jsonl",
        )

        self.assertEqual(
            notification_id,
            "eybond_local_proxy_capture_entry-1_session_trace",
        )

    def test_cloud_evidence_export_available_supports_valuecloud_provider(self) -> None:
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

        self.assertEqual(coordinator.cloud_evidence_provider, "valuecloud")
        self.assertTrue(coordinator.cloud_evidence_export_available)

    def test_init_discards_stale_unsupported_cache_without_reload_suppression(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={},
            options={
                "driver_unsupported_commands": ["GLINE"],
                "driver_unsupported_commands_version": 1,
            },
        )
        updates: list[dict[str, object]] = []

        def _async_update_entry(config_entry, **kwargs) -> bool:
            updates.append(dict(kwargs))
            if "options" in kwargs:
                config_entry.options = dict(kwargs["options"])
            return True

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry),
        )

        coordinator = self.coordinator_module.EybondLocalCoordinator(hass, entry)

        self.assertEqual(updates, [{"options": {}}])
        self.assertEqual(entry.options, {})
        self.assertEqual(coordinator._suppress_entry_reload_count, 0)
        self.assertFalse(coordinator.consume_entry_reload_suppression())

    def test_pre_listener_persistence_does_not_leave_reload_suppression(self) -> None:
        """A startup write before listener registration cannot consume a token."""

        entry = types.SimpleNamespace(data={}, options={}, update_listeners=[])

        def _async_update_entry(config_entry, **kwargs) -> bool:
            if "data" in kwargs:
                config_entry.data = dict(kwargs["data"])
            return True

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = entry
        coordinator.hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
        )
        coordinator._suppress_entry_reload_count = 0

        coordinator._async_update_entry_without_reload(data={"detected": True})

        self.assertEqual(entry.data, {"detected": True})
        self.assertEqual(coordinator._suppress_entry_reload_count, 0)
        self.assertFalse(coordinator.consume_entry_reload_suppression())

    def test_runtime_persistence_with_listener_arms_one_reload_suppression(self) -> None:
        """Once setup registered a listener, one changed write arms one token."""

        entry = types.SimpleNamespace(
            data={}, options={}, update_listeners=[object()]
        )

        def _async_update_entry(config_entry, **kwargs) -> bool:
            if "data" in kwargs:
                config_entry.data = dict(kwargs["data"])
            return True

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = entry
        coordinator.hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
        )
        coordinator._suppress_entry_reload_count = 0

        coordinator._async_update_entry_without_reload(data={"detected": True})

        self.assertEqual(coordinator._suppress_entry_reload_count, 1)
        self.assertTrue(coordinator.consume_entry_reload_suppression())
        self.assertFalse(coordinator.consume_entry_reload_suppression())

    def test_legacy_metadata_channel_migrates_out_of_driver_option(self) -> None:
        # A legacy entry persisted the AT-metadata dead-channel verdict inside the
        # driver negative-cache option under a ``collector:`` namespace. It must
        # migrate into the dedicated metadata option and leave the driver option.
        entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={},
            options={
                "driver_unsupported_commands": ["GLINE", "collector:at_metadata"],
                "driver_unsupported_commands_version": 2,
            },
        )
        updates: list[dict[str, object]] = []

        def _async_update_entry(config_entry, **kwargs) -> bool:
            updates.append(dict(kwargs))
            if "options" in kwargs:
                config_entry.options = dict(kwargs["options"])
            return True

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry),
        )

        coordinator = self.coordinator_module.EybondLocalCoordinator(hass, entry)

        # The runtime reports the migrated dead channel from its OWN health store
        # (a stub here; the real hub split is covered in test_hub).
        coordinator._runtime = types.SimpleNamespace(
            collector_metadata_dead_channels=lambda: ("collector:at_metadata",)
        )

        # The one-time option rewrite happens on the first persist pass: the
        # legacy ``collector:`` key leaves the driver option and lands in the
        # dedicated metadata option.
        coordinator._maybe_persist_metadata_dead_channels()
        self.assertEqual(entry.options.get("driver_unsupported_commands"), ["GLINE"])
        self.assertEqual(
            entry.options.get("collector_metadata_dead_channels"),
            ["collector:at_metadata"],
        )
        self.assertEqual(
            entry.options.get("collector_metadata_dead_channels_version"), 1
        )

        # Idempotent: a settled entry produces no further write.
        updates.clear()
        coordinator._maybe_persist_metadata_dead_channels()
        self.assertEqual(updates, [])

    def test_cloud_evidence_export_available_supports_smartess_provider(self) -> None:
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

    def test_cached_evidence_ignored_when_active_provider_changes(self) -> None:
        # The active provider is SmartESS, but the cached record was loaded for
        # ValueCloud: the cache is ignored so one provider's evidence never leaks
        # (also what keeps a support bundle from embedding foreign evidence).
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_pn": "E5000020000000", "collector_cloud_family": "smartess_at"},
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        cached_record = types.SimpleNamespace(path="/x.json", payload={"provider": "valuecloud"})
        coordinator._cached_smartess_cloud_evidence_record = cached_record
        coordinator._cached_cloud_evidence_provider = "valuecloud"

        self.assertEqual(coordinator.cloud_evidence_provider, "smartess")
        # Stale (foreign-provider) cache -> not returned.
        self.assertIsNone(coordinator._latest_smartess_cloud_evidence_record())

        # Once the cache belongs to the active provider it is returned again.
        coordinator._cached_cloud_evidence_provider = "smartess"
        self.assertIs(coordinator._latest_smartess_cloud_evidence_record(), cached_record)

    def test_warm_cache_race_stamps_fetching_provider_not_reread(self) -> None:
        # The active cloud family changes WHILE the (SmartESS) load runs: the
        # cache must be stamped with the fetching provider so the result stays
        # invisible to the new provider (no re-read of a dynamic value).
        async def _run() -> None:
            from custom_components.eybond_local.support.cloud_evidence_providers import (
                CloudEvidenceContext,
            )

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={"collector_pn": "E1", "collector_cloud_family": "smartess_at"},
                options={},
            )
            coordinator.data = self.RuntimeSnapshot(values={})
            coordinator._cached_smartess_cloud_evidence_record = None
            coordinator._cached_cloud_evidence_provider = ""
            coordinator._cached_smartess_cloud_evidence_warmed = False

            record = types.SimpleNamespace(path="/x.json", payload={"provider": "smartess"})

            def _load_latest(_context):
                coordinator.config_entry.data["collector_cloud_family"] = "valuecloud_at"
                return record

            coordinator._cloud_evidence_provider_impl = lambda: types.SimpleNamespace(
                provider_id="smartess", load_latest=_load_latest
            )
            coordinator._cloud_evidence_context = lambda: CloudEvidenceContext(
                config_dir=Path("."), entry_id="e", collector_pn="E1"
            )

            async def _executor(fn, *args):
                return fn(*args)

            coordinator.hass = types.SimpleNamespace(async_add_executor_job=_executor)

            await coordinator._async_warm_smartess_cloud_evidence_cache()

            self.assertEqual(coordinator._cached_cloud_evidence_provider, "smartess")
            self.assertIs(coordinator._cached_smartess_cloud_evidence_record, record)
            self.assertEqual(coordinator.cloud_evidence_provider, "valuecloud")
            self.assertIsNone(coordinator._latest_smartess_cloud_evidence_record())

        asyncio.run(_run())

    def test_export_provider_change_does_not_publish_foreign_tooling_path(self) -> None:
        async def _run() -> None:
            from custom_components.eybond_local.support.cloud_evidence_providers import (
                CloudEvidenceContext,
            )

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={"collector_pn": "E1", "collector_cloud_family": "smartess_at"},
                options={},
            )
            coordinator.data = self.RuntimeSnapshot(values={})
            coordinator._cached_smartess_cloud_evidence_record = None
            coordinator._cached_cloud_evidence_provider = ""
            coordinator._cached_smartess_cloud_evidence_warmed = False
            published: list[dict[str, object]] = []
            record = types.SimpleNamespace(path="/smartess.json", payload={"provider": "smartess"})

            def _export(_context, *, username, password):
                coordinator.config_entry.data["collector_cloud_family"] = "valuecloud_at"
                return record

            provider = types.SimpleNamespace(
                provider_id="smartess",
                export_available=lambda _context: True,
                export=_export,
                export_status_label="SmartESS cloud evidence exported",
            )
            coordinator._cloud_evidence_provider_impl = lambda: provider
            coordinator._cloud_evidence_context = lambda: CloudEvidenceContext(
                config_dir=Path("."), entry_id="e", collector_pn="E1"
            )
            coordinator._publish_tooling_values = lambda **kwargs: published.append(kwargs)

            async def _executor(fn, *args):
                return fn(*args)

            coordinator.hass = types.SimpleNamespace(async_add_executor_job=_executor)
            result = await coordinator.async_export_cloud_evidence(
                username="u", password="p"
            )

            self.assertEqual(result, "/smartess.json")
            self.assertEqual(coordinator._cached_cloud_evidence_provider, "smartess")
            self.assertEqual(coordinator.cloud_evidence_provider, "valuecloud")
            self.assertIsNone(coordinator._latest_smartess_cloud_evidence_record())
            self.assertEqual(published, [])

        asyncio.run(_run())

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
                shareable_path=Path("/tmp/result.share.json"),
            )

            poll_task = asyncio.create_task(coordinator._async_update_data())
            await poll_started.wait()
            with patch.object(
                self.coordinator_diagnostics_module,
                "run_scenario",
                _fake_run_scenario,
            ), patch.object(
                self.coordinator_diagnostics_module,
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

    def test_diagnostic_download_uses_signed_entry_scoped_api_url(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator._runtime_operation_lock = asyncio.Lock()
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-1")

            async def _run_executor_job(job):
                return job()

            coordinator.hass = types.SimpleNamespace(
                config=types.SimpleNamespace(config_dir="/tmp"),
                async_add_executor_job=_run_executor_job,
            )
            result = types.SimpleNamespace(
                success=True,
                output="ok\n",
                results=[],
                context={},
                started_at="start",
                finished_at="finish",
                error=None,
            )
            export = types.SimpleNamespace(
                result_path=Path("/tmp/result.json"),
                shareable_path=Path(
                    "/tmp/diagnostic_entry-1_20260823T125052279675Z.share.json"
                ),
            )
            signed = "https://ha.example/api/eybond_local/diagnostic_run/signed"

            with patch.object(
                self.coordinator_diagnostics_module,
                "run_scenario",
                return_value=result,
            ), patch.object(
                self.coordinator_diagnostics_module,
                "export_diagnostic_run",
                return_value=export,
            ), patch.object(
                self.coordinator_diagnostics_module,
                "sign_diagnostic_run_download_url",
                return_value=signed,
            ) as signer:
                payload = await coordinator._async_execute_diagnostic(
                    "read 1",
                    types.SimpleNamespace(transport=None),
                    publish_download_copy=True,
                )

            signer.assert_called_once_with(
                coordinator.hass,
                "entry-1",
                export.shareable_path.name,
            )
            self.assertEqual(payload["download_url"], signed)

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

    def test_local_register_snapshot_serializes_against_runtime_refresh(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator._runtime_operation_lock = asyncio.Lock()
            capture_started = asyncio.Event()

            async def _capture():
                self.assertTrue(coordinator._runtime_operation_lock.locked())
                capture_started.set()
                return None

            coordinator._runtime = types.SimpleNamespace(
                async_capture_local_register_snapshot=_capture
            )
            await coordinator._runtime_operation_lock.acquire()
            task = asyncio.create_task(
                coordinator.async_capture_local_register_snapshot()
            )
            await asyncio.sleep(0)
            self.assertFalse(capture_started.is_set())

            coordinator._runtime_operation_lock.release()
            self.assertIsNone(await task)
            self.assertTrue(capture_started.is_set())

        asyncio.run(_run())

    def test_local_register_collection_uses_public_capture_and_publishes_typed_series(
        self,
    ) -> None:
        async def _run() -> None:
            from custom_components.eybond_local.drivers.local_register_evidence import (
                LocalRegisterBlockObservation,
                LocalRegisterReadPlan,
                LocalRegisterSnapshot,
            )
            from custom_components.eybond_local.drivers.local_register_series import (
                LocalRegisterSeriesPlan,
                LocalRegisterSnapshotSeries,
            )
            import custom_components.eybond_local.support.local_register_collection as collection_module
            from custom_components.eybond_local.support.local_register_collection import (
                LocalRegisterCollectionManager,
            )

            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator._runtime_operation_lock = asyncio.Lock()
            coordinator._shutdown_complete = False
            coordinator._tooling_values = {}
            coordinator.data = self.RuntimeSnapshot(values={})
            snapshots: list[LocalRegisterSnapshot] = []
            plan = LocalRegisterReadPlan(
                devcode=2376,
                collector_addr=1,
                device_addr=1,
                function=3,
                start=100,
                count=1,
            )
            for index in range(3):
                snapshots.append(
                    LocalRegisterSnapshot(
                        collector_pn="E50000200000000001",
                        driver_key="smg_modbus",
                        started_at=f"2026-08-22T10:00:{index * 10:02d}+00:00",
                        completed_at=f"2026-08-22T10:00:{index * 10 + 1:02d}+00:00",
                        planned_block_count=1,
                        failed_block_count=0,
                        blocks=(
                            LocalRegisterBlockObservation(
                                plan=plan,
                                observed_at=(
                                    f"2026-08-22T10:00:{index * 10 + 1:02d}+00:00"
                                ),
                                values=(2300 + index,),
                            ),
                        ),
                    )
                )

            async def _capture():
                self.assertTrue(coordinator._runtime_operation_lock.locked())
                return snapshots.pop(0)

            coordinator._runtime = types.SimpleNamespace(
                async_capture_local_register_snapshot=_capture
            )
            coordinator._local_register_collection = LocalRegisterCollectionManager(
                capture_snapshot=coordinator.async_capture_local_register_snapshot,
                on_update=coordinator._publish_local_register_collection_update,
            )

            async def _immediate_series(**kwargs):
                captured = [
                    await kwargs["capture_snapshot"]() for _ in range(3)
                ]
                return LocalRegisterSnapshotSeries(
                    collector_pn="E50000200000000001",
                    driver_key="smg_modbus",
                    sample_interval_seconds=kwargs["sample_interval_seconds"],
                    snapshots=tuple(captured),
                )

            with patch.object(
                collection_module,
                "async_capture_local_register_series",
                side_effect=_immediate_series,
            ):
                started = coordinator.start_local_register_collection(
                    LocalRegisterSeriesPlan(3, 1)
                )
                self.assertTrue(started.active)
                await asyncio.sleep(0)

            self.assertTrue(coordinator.local_register_collection_status.series_available)
            self.assertIs(
                type(coordinator.latest_local_register_series),
                LocalRegisterSnapshotSeries,
            )
            status_record = coordinator.data.values["local_register_collection"]
            series_record = coordinator.data.values[
                "local_register_series_evidence"
            ]
            self.assertIs(status_record["read_only"], True)
            self.assertIs(status_record["activation_allowed"], False)
            self.assertIs(series_record["cloud_mapping_proven"], False)
            self.assertEqual(series_record["snapshot_count"], 3)

            previous = coordinator.local_register_collection_status
            with self.assertRaises(TypeError):
                coordinator.start_local_register_collection(object())
            self.assertIs(coordinator.local_register_collection_status, previous)

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
        original = self.coordinator_inverter_profile_module.resolve_effective_metadata_selection
        self.coordinator_inverter_profile_module.resolve_effective_metadata_selection = (
            lambda **_kwargs: stub_metadata
        )
        try:
            result = coordinator.apply_device_overlay_to_inverter(inverter, None)
        finally:
            self.coordinator_inverter_profile_module.resolve_effective_metadata_selection = original

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
            apply_device_overlay_to_inverter=lambda inv, collector: merged,
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
        original = self.coordinator_inverter_profile_module.resolve_effective_metadata_selection
        self.coordinator_inverter_profile_module.resolve_effective_metadata_selection = (
            lambda **_kwargs: stub_metadata
        )
        try:
            result = coordinator._apply_device_overlay_to_inverter(inverter, None)
        finally:
            self.coordinator_inverter_profile_module.resolve_effective_metadata_selection = original

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
            self.coordinator_inverter_profile_module,
            "resolve_effective_metadata_selection",
            side_effect=AssertionError("sync resolver should not run after warm-up"),
        ):
            context = coordinator.write_exposure_context

        self.assertEqual(context["variant_key"], "smg_6200")
        self.assertEqual(context["profile_source_scope"], "external")
        self.assertEqual(context["schema_source_scope"], "external")
        self.assertTrue(context["device_scoped_overlay_active"])
        self.assertEqual(context["selected_control_keys"], {"learned_a"})

    def test_sync_device_registry_sets_inverter_parent_to_collector(self) -> None:
        registry = FakeRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(
                async_get_entry=lambda entry_id: (
                    object() if entry_id == "entry-1" else None
                )
            )
        )
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

    def test_device_registry_diagnostics_exposes_duplicate_children_without_identifiers(self) -> None:
        registry = FakeRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry

        collector = registry.async_get_or_create(
            config_entry_id="entry-1",
            identifiers={("eybond_local", "entry-1:collector")},
            name="Collector PN SECRET",
        )
        inverter = registry.async_get_or_create(
            config_entry_id="entry-1",
            identifiers={("eybond_local", "entry-1")},
            name="Anenji SECRET",
            model="Anenji ANJ-11KW-48V-WIFI-P",
            serial_number="SECRET-SERIAL",
            via_device=("eybond_local", "entry-1:collector"),
        )
        duplicate = registry.async_get_or_create(
            config_entry_id="legacy-entry",
            identifiers={("eybond_local", "legacy-inverter")},
            name="User Secret Name",
            model="Anenji ANJ-11KW-48V-WIFI-P",
            via_device=("eybond_local", "entry-1:collector"),
        )

        class _EntityRegistry:
            entries_by_device = {
                inverter.id: (
                    types.SimpleNamespace(disabled_by=None),
                    types.SimpleNamespace(disabled_by="integration"),
                ),
                duplicate.id: (),
            }

        entity_registry_module = sys.modules["homeassistant.helpers.entity_registry"]
        entity_registry_module.async_get = lambda hass: _EntityRegistry()
        entity_registry_module.async_entries_for_device = (
            lambda entity_registry, device_id, **kwargs: (
                entity_registry.entries_by_device.get(device_id, ())
            )
        )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(
                async_get_entry=lambda entry_id: (
                    object() if entry_id == "entry-1" else None
                )
            )
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={},
            options={},
            title="Anenji",
        )
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="Anenji ANJ-11KW-48V-WIFI-P",
                serial_number="92632500000001",
            )
        )

        diagnostics = coordinator.device_registry_diagnostics()

        self.assertEqual(
            diagnostics["topology_status"],
            "duplicate_inverter_children",
        )
        self.assertEqual(diagnostics["direct_child_count"], 2)
        self.assertEqual(diagnostics["unexpected_direct_child_count"], 1)
        self.assertEqual(diagnostics["relevant_device_count"], 3)
        self.assertEqual(
            [record["role"] for record in diagnostics["devices"]],
            [
                "canonical_collector",
                "canonical_inverter",
                "unexpected_collector_child",
            ],
        )
        self.assertEqual(diagnostics["devices"][1]["entity_count"], 2)
        self.assertEqual(diagnostics["devices"][1]["disabled_entity_count"], 1)
        self.assertFalse(diagnostics["devices"][2]["belongs_to_current_entry"])
        self.assertEqual(diagnostics["devices"][2]["live_config_entry_count"], 0)
        self.assertEqual(diagnostics["devices"][2]["missing_config_entry_count"], 1)
        self.assertTrue(diagnostics["devices"][2]["safe_cleanup_candidate"])
        serialized = str(diagnostics)
        self.assertNotIn("SECRET", serialized)
        self.assertNotIn("entry-1", serialized)
        self.assertNotIn("legacy-entry", serialized)

    def test_sync_removes_only_empty_orphaned_inverter_children(self) -> None:
        registry = FakeRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry

        collector = registry.async_get_or_create(
            config_entry_id="entry-1",
            identifiers={("eybond_local", "entry-1:collector")},
            name="Collector",
        )
        orphan = registry.async_get_or_create(
            config_entry_id="removed-entry",
            identifiers={("eybond_local", "removed-entry")},
            name="Old inverter",
            via_device=("eybond_local", "entry-1:collector"),
        )
        live_foreign = registry.async_get_or_create(
            config_entry_id="live-entry",
            identifiers={("eybond_local", "live-entry")},
            name="Live inverter",
            via_device=("eybond_local", "entry-1:collector"),
        )
        orphan_with_entity = registry.async_get_or_create(
            config_entry_id="removed-with-entity",
            identifiers={("eybond_local", "removed-with-entity")},
            name="Old inverter with entity",
            via_device=("eybond_local", "entry-1:collector"),
        )
        customized_orphan = registry.async_get_or_create(
            config_entry_id="removed-customized",
            identifiers={("eybond_local", "removed-customized")},
            name="Customized old inverter",
            via_device=("eybond_local", "entry-1:collector"),
        )
        customized_orphan.name_by_user = "Keep me"

        class _EntityRegistry:
            entries_by_device = {
                orphan_with_entity.id: (types.SimpleNamespace(disabled_by=None),),
            }

        entity_registry_module = sys.modules["homeassistant.helpers.entity_registry"]
        entity_registry_module.async_get = lambda hass: _EntityRegistry()
        entity_registry_module.async_entries_for_device = (
            lambda entity_registry, device_id, **kwargs: (
                entity_registry.entries_by_device.get(device_id, ())
            )
        )

        live_entries = {"entry-1", "live-entry"}
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(
                async_get_entry=lambda entry_id: (
                    object() if entry_id in live_entries else None
                )
            )
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "detected_model": "Anenji ANJ-11KW-48V-WIFI-P",
                "detected_serial": "INV-001",
            },
            options={},
            title="Anenji",
        )
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="Anenji ANJ-11KW-48V-WIFI-P",
                serial_number="INV-001",
                details={},
            )
        )
        coordinator._last_synced_device_meta = ("", "", "", "", "")

        coordinator._async_sync_inverter_device_registry()

        inverter = registry.async_get_device(
            identifiers={("eybond_local", "entry-1")}
        )
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.via_device_id, collector.id)
        self.assertEqual(registry.removed_device_ids, [orphan.id])
        self.assertIsNone(
            registry.async_get_device(
                identifiers={("eybond_local", "removed-entry")}
            )
        )
        for retained in (live_foreign, orphan_with_entity, customized_orphan):
            retained_device = registry.async_get_device(
                identifiers=retained.identifiers
            )
            self.assertIsNotNone(retained_device)
            self.assertIsNone(retained_device.via_device_id)

    def test_sync_device_registry_clears_untrusted_pi30_placeholder(self) -> None:
        class _PreservingRegistry(FakeRegistry):
            def async_get_or_create(self, config_entry_id=None, **info):
                identifiers = set(info.get("identifiers") or set())
                existing = self.async_get_device(identifiers=identifiers)
                prior_serial = None if existing is None else existing.serial_number
                device = super().async_get_or_create(config_entry_id, **info)
                if "serial_number" not in info:
                    device.serial_number = prior_serial
                return device

        registry = _PreservingRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry
        inverter_device = registry.async_get_or_create(
            config_entry_id="entry-1",
            identifiers={("eybond_local", "entry-1")},
            name="PI30 4200",
            serial_number="55355535553555",
        )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "detected_model": "PI30 4200",
                "detected_serial": "55355535553555",
            },
            options={},
            title="Collector PN Q0000000000001",
        )
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                driver_key="pi30",
                model_name="PI30 4200",
                serial_number="",
                details={
                    "reported_serial_number": "55355535553555",
                    "serial_identity_source": "qid",
                    "serial_identity_trust": "untrusted",
                    "serial_identity_reason": "known_placeholder",
                },
            )
        )
        coordinator._last_synced_device_meta = ("", "", "", "", "")

        coordinator._async_sync_inverter_device_registry()

        self.assertIsNone(inverter_device.serial_number)

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
            self.coordinator_inverter_profile_module,
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
            self.coordinator_inverter_profile_module,
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
            self.coordinator_inverter_profile_module,
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
            self.coordinator_inverter_profile_module,
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

    def test_host_only_endpoint_shape_exposes_implicit_legacy_port(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator._connection_spec = types.SimpleNamespace(
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=8899,
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="ess.eybond.com",
        )
        coordinator._remembered_collector_server_endpoint = ""
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_cloud_family": "legacy_binary"}, options={}
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50"}
        )

        shape = coordinator.collector_endpoint_write_shape

        self.assertEqual(shape.write_format, "host_only")
        self.assertEqual(shape.fixed_port, 502)
        self.assertTrue(shape.port_is_fixed)

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
                options={
                    "collector_operation_mode": "home_assistant_only",
                    # The automatic endpoint reconcile only runs when the
                    # integration manages the endpoint (it previously wrote it).
                    "endpoint_control_policy": "integration_managed",
                },
            )

            disconnected_snapshot = self.RuntimeSnapshot(
                connected=False,
                values={"collector_server_endpoint": "192.168.1.50"},
            )
            coordinator.data = disconnected_snapshot

            await coordinator._async_reconcile_managed_collector_endpoint(
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

            await coordinator._async_reconcile_managed_collector_endpoint(
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

    def test_reconcile_integration_managed_aligns_to_ha_regardless_of_operation_mode(self) -> None:
        # Phase 5: the reconcile targets Home Assistant purely from
        # endpoint_control_policy=integration_managed. The legacy operation mode
        # (here the legacy cloud+HA mode) is no longer consulted, and the endpoint is never
        # auto-restored to the previous/cloud endpoint here.
        async def _run() -> None:
            endpoint_writes: list[str] = []

            async def _ensure_listener(port: int) -> None:
                return None

            async def _set_endpoint(endpoint: str, *, apply_changes: bool = True):
                endpoint_writes.append(endpoint)
                return {"readback_endpoint": endpoint, "status": "applied"}

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                collector_server_endpoint_rollback_target="203.0.113.9,18899,TCP",
                async_ensure_callback_listener=_ensure_listener,
                async_set_collector_server_endpoint=_set_endpoint,
            )
            coordinator._remembered_collector_server_endpoint = ""
            coordinator._collector_operation_pending_target_endpoint = ""
            coordinator._ha_primary_reconcile_last_signature = None
            coordinator._ha_primary_reconcile_last_attempt_monotonic = 0.0
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-reconcile",
                data={},
                options={
                    # Legacy cloud-primary mode: must NOT drive the reconcile.
                    "collector_operation_mode": "smartess_cloud_home_assistant",
                    "endpoint_control_policy": "integration_managed",
                },
            )
            snapshot = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "203.0.113.9,18899,TCP"},
            )
            coordinator.data = snapshot

            await coordinator._async_reconcile_managed_collector_endpoint(snapshot)

            # It wrote the Home Assistant endpoint, not restored to the previous.
            self.assertEqual(len(endpoint_writes), 1)
            self.assertIn("192.168.1.50", endpoint_writes[0])
            self.assertNotIn("203.0.113.9", endpoint_writes[0])

        asyncio.run(_run())

    def test_reconcile_external_never_writes_endpoint(self) -> None:
        # Phase 5: endpoint_control_policy=external must never auto-write/restore.
        async def _run() -> None:
            wrote = False

            async def _set_endpoint(endpoint: str, *, apply_changes: bool = True):
                nonlocal wrote
                wrote = True
                return {"readback_endpoint": endpoint, "status": "applied"}

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                collector_server_endpoint_rollback_target="203.0.113.9,18899,TCP",
                async_set_collector_server_endpoint=_set_endpoint,
            )
            coordinator._collector_operation_pending_target_endpoint = "stale"
            coordinator.config_entry = types.SimpleNamespace(
                data={},
                options={"endpoint_control_policy": "external"},
            )
            snapshot = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "192.168.1.50,8899,TCP"},
            )
            coordinator.data = snapshot

            await coordinator._async_reconcile_managed_collector_endpoint(snapshot)

            self.assertFalse(wrote)
            self.assertEqual(
                snapshot.values["collector_operation_endpoint_sync_status"],
                "external_not_managed",
            )
            self.assertEqual(coordinator._collector_operation_pending_target_endpoint, "")

        asyncio.run(_run())

    def test_setup_prepares_listener_from_connection_axes_not_operation_mode(self) -> None:
        # Phase 5: listener preparation is runtime behavior and must follow the
        # explicit connection axes. A legacy cloud-primary operation mode must
        # not suppress the listener for an inbound entry.
        async def _run() -> None:
            listener_ports: list[int] = []
            started: list[bool] = []

            async def _async_start() -> None:
                started.append(True)

            async def _ensure_listener(port: int) -> None:
                listener_ports.append(port)

            async def _noop_async(*_args, **_kwargs) -> None:
                return None

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=18899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                async_start=_async_start,
                async_ensure_callback_listener=_ensure_listener,
            )
            coordinator.config_entry = types.SimpleNamespace(
                data={},
                options={
                    "collector_operation_mode": "smartess_cloud_home_assistant",
                    "connection_strategy": "inbound",
                    "endpoint_control_policy": "external",
                },
            )
            coordinator.data = self.RuntimeSnapshot(
                values={"collector_server_endpoint": "203.0.113.9,18899,TCP"}
            )
            coordinator._configure_reverse_discovery_mode = lambda: None
            coordinator._configure_callback_ownership = lambda: None
            coordinator._async_recover_proxy_capture_state = _noop_async
            coordinator._async_recover_shadow_learning_state = _noop_async
            coordinator._async_warm_smartess_cloud_evidence_cache = _noop_async
            coordinator._async_warm_effective_metadata_cache = _noop_async

            await coordinator.async_setup()

            self.assertEqual(started, [True])
            self.assertEqual(listener_ports, [18899])

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

            await coordinator._async_reconcile_managed_collector_endpoint(snapshot)

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

    def test_bridge_with_legacy_cloud_axes_is_reported_custom(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_operation_mode": "smartess_cloud_home_assistant"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(collector_virtual_bridge=True),
        )

        self.assertEqual(coordinator.collector_operation_mode, "custom")
        self.assertTrue(coordinator.collector_uses_home_assistant_route)

    def test_unproven_external_inbound_is_reported_custom(self) -> None:
        # Inbound alone cannot claim the complete HA-only product profile when
        # the integration neither owns the endpoint nor has an inbound proof.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_kind": "factory_eybond",
                "connection_strategy": "inbound",
                "collector_operation_mode": "smartess_cloud_home_assistant",
            },
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.data = self.RuntimeSnapshot(values={}, collector=None)
        # Precondition: a factory collector never forces HA-only by capability,
        # so the projection is driven purely by the canonical strategy.
        self.assertFalse(coordinator.collector_capabilities.ha_only_required)
        self.assertEqual(coordinator.collector_operation_mode, "custom")
        self.assertTrue(coordinator.collector_uses_home_assistant_route)

    def test_operation_mode_projects_callback_ignoring_stale_ha_only_mode(self) -> None:
        # CP2A Test B: an entry that declares the canonical CALLBACK strategy
        # projects smartess_cloud_home_assistant, IGNORING a stale HA-only mode.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_kind": "factory_eybond",
                "connection_strategy": "callback_on_demand",
                "collector_operation_mode": "home_assistant_only",
            },
            options={"collector_operation_mode": "home_assistant_only"},
        )
        coordinator.data = self.RuntimeSnapshot(values={}, collector=None)
        self.assertFalse(coordinator.collector_capabilities.ha_only_required)
        self.assertEqual(
            coordinator.collector_operation_mode, "smartess_cloud_home_assistant"
        )
        self.assertFalse(coordinator.collector_uses_home_assistant_route)

    def test_runtime_route_uses_canonical_legacy_strategy_derivation(self) -> None:
        # A pre-schema manual entry with a stale HA-only compatibility value is
        # callback-on-demand. The coordinator must use the central resolver;
        # directly re-reading the old mode would incorrectly classify it inbound.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_kind": "factory_eybond",
                "connection_mode": "manual",
                "collector_operation_mode": "home_assistant_only",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={}, collector=None)

        self.assertEqual(coordinator.connection_strategy, "callback_on_demand")
        self.assertFalse(coordinator.collector_uses_home_assistant_route)

    def test_legacy_mode_without_proven_axes_is_reported_custom(self) -> None:
        # The legacy field can derive transport compatibility, but it is not
        # sufficient evidence for a normal user-facing operating profile.
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_kind": "factory_eybond",
                "collector_operation_mode": "home_assistant_only",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={}, collector=None)
        self.assertFalse(coordinator.collector_capabilities.ha_only_required)
        self.assertEqual(coordinator.collector_operation_mode, "custom")
        self.assertTrue(coordinator.collector_uses_home_assistant_route)

    def _rollback_boundary_coordinator(self, *, data, options, values):
        """Build a bare coordinator wired for the read-only rollback boundary.

        Any config-entry write is a test failure. The registry read is routed
        through a fake executor at a non-existent config dir, so the existing
        read-only registry API returns nothing without touching the filesystem.
        """

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data=dict(data), options=dict(options))
        coordinator.data = self.RuntimeSnapshot(values=dict(values), collector=None)
        coordinator._runtime = types.SimpleNamespace(effective_advertised_server_ip="192.168.1.50")
        coordinator._connection_spec = types.SimpleNamespace(effective_advertised_server_ip="192.168.1.50")

        def _forbidden_write(**kwargs):
            raise AssertionError("read-only rollback boundary must not write the entry")

        coordinator._async_update_entry_without_reload = _forbidden_write

        async def _run_executor(func, *args):
            return func(*args)

        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(config_dir="/nonexistent-cp2b1-test-config-dir"),
            async_add_executor_job=_run_executor,
        )
        return coordinator

    @staticmethod
    def _proof_backed_inbound_data(*, pn: str = "E5000025SYN0000000001"):
        timestamp = "2026-07-21T10:00:00+00:00"
        return {
            "collector_kind": "factory_eybond",
            "collector_pn": pn,
            "connection_strategy": "inbound",
            "advertised_server_ip": "192.168.1.50",
            "advertised_tcp_port": 8899,
            "recovery_contract": {
                "schema_version": 1,
                "collector_pn": pn,
                "collector_identity_source": "fc2_parameter_2",
                "updated_at": timestamp,
                "inbound": {
                    "method": "reboot_reconnect_no_trigger",
                    "collector_pn": pn,
                    "identity_source": "fc2_parameter_2",
                    "verified_at": timestamp,
                    "session_protocol": "eybond_framed",
                },
            },
        }

    @staticmethod
    def _proof_backed_callback_data(*, pn: str = "E5000025SYN0000000001"):
        timestamp = "2026-07-21T10:00:00+00:00"
        return {
            "collector_kind": "factory_eybond",
            "collector_pn": pn,
            "connection_strategy": "callback_on_demand",
            "advertised_server_ip": "195.191.72.37",
            "advertised_tcp_port": 18899,
            "recovery_contract": {
                "schema_version": 1,
                "collector_pn": pn,
                "collector_identity_source": "fc2_parameter_2",
                "updated_at": timestamp,
                "callback": {
                    "method": "reset_unicast_reconnect_same_pn",
                    "collector_pn": pn,
                    "identity_source": "fc2_parameter_2",
                    "verified_at": timestamp,
                    "trigger_target": "203.0.113.10:58899",
                    "advertised_ha_endpoint": "195.191.72.37:18899",
                    "listener_port": 8899,
                },
            },
        }

    def test_cloud_rollback_context_returns_durable_original_read_only(self) -> None:
        # CP2B.1 Test E: the read-only boundary returns a typed context from the
        # durable original endpoint and NEVER writes the entry/registry/runtime.
        from custom_components.eybond_local.connection.strategy_transition_context import (
            CloudRollbackEndpoint,
        )

        async def _run() -> None:
            coordinator = self._rollback_boundary_coordinator(
                data={"collector_kind": "factory_eybond", "collector_pn": "E5000025SYN0000000001"},
                options={"collector_original_server_endpoint": "ess.eybond.com,18899,TCP"},
                values={"collector_server_endpoint": "ess.eybond.com,18899,TCP"},
            )
            context = await coordinator.collector_cloud_rollback_context()
            self.assertIsInstance(context, CloudRollbackEndpoint)
            self.assertEqual(context.provenance, "original_cloud_endpoint")
            self.assertEqual(context.endpoint, "ess.eybond.com,18899,TCP")

        asyncio.run(_run())

    def test_cloud_rollback_context_none_when_no_facts(self) -> None:
        # No durable/registry/observed-external fact -> honest none, still no write.
        async def _run() -> None:
            coordinator = self._rollback_boundary_coordinator(
                data=self._proof_backed_inbound_data(),
                options={},
                # Complete endpoint equals the proof-backed HA endpoint.
                values={"collector_server_endpoint": "192.168.1.50,8899,TCP"},
            )
            context = await coordinator.collector_cloud_rollback_context()
            self.assertEqual(context.provenance, "none")
            self.assertEqual(context.endpoint, "")

        asyncio.run(_run())

    def test_cloud_rollback_context_observed_external_when_no_durable(self) -> None:
        # Durable/registry absent; the current endpoint differs from the HA host
        # -> observed external candidate. Still read-only.
        async def _run() -> None:
            coordinator = self._rollback_boundary_coordinator(
                data=self._proof_backed_inbound_data(),
                options={},
                values={"collector_server_endpoint": "dtu.example,18899,TCP"},
            )
            context = await coordinator.collector_cloud_rollback_context()
            self.assertEqual(context.provenance, "observed_current_external_endpoint")
            self.assertEqual(context.endpoint, "dtu.example,18899,TCP")

        asyncio.run(_run())

    def test_cloud_rollback_context_same_host_other_port_is_external(self) -> None:
        async def _run() -> None:
            coordinator = self._rollback_boundary_coordinator(
                data=self._proof_backed_inbound_data(),
                options={},
                values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            )
            context = await coordinator.collector_cloud_rollback_context()
            self.assertEqual(
                context.provenance, "observed_current_external_endpoint"
            )

        asyncio.run(_run())

    def test_cloud_rollback_context_does_not_coerce_observed_duck(self) -> None:
        class EndpointDuck:
            def __str__(self) -> str:
                return "dtu.example,18899,TCP"

        async def _run() -> None:
            coordinator = self._rollback_boundary_coordinator(
                data=self._proof_backed_inbound_data(),
                options={},
                values={"collector_server_endpoint": EndpointDuck()},
            )
            context = await coordinator.collector_cloud_rollback_context()
            self.assertEqual(context.provenance, "none")

        asyncio.run(_run())

    def test_cloud_rollback_context_uses_nat_proof_not_runtime_local_host(self) -> None:
        async def _run() -> None:
            coordinator = self._rollback_boundary_coordinator(
                data=self._proof_backed_callback_data(),
                options={},
                # This is exactly the public endpoint certified by the callback
                # proof, while the runtime stub still advertises 192.168.1.50.
                values={"collector_server_endpoint": "195.191.72.37,18899,TCP"},
            )
            context = await coordinator.collector_cloud_rollback_context()
            self.assertEqual(context.provenance, "none")

        asyncio.run(_run())

    # ---- CP2B.2: persistence-before-write for the typed rollback selection ----
    def _catalog_selection(self):
        from custom_components.eybond_local.connection.strategy_transition_context import (
            CLOUD_PROVENANCE_EXPLICIT_USER,
            CloudRollbackEndpoint,
            CloudRollbackSelection,
            ROLLBACK_SELECTION_CATALOG,
        )

        return CloudRollbackSelection(
            endpoint=CloudRollbackEndpoint("dtu.example,18899,TCP", CLOUD_PROVENANCE_EXPLICIT_USER),
            selection_kind=ROLLBACK_SELECTION_CATALOG,
            catalog_profile_key="smartess_at",
            user_confirmed=True,
        )

    def _persist_coordinator(self, *, data, options, config_dir, executor_raises=False):
        from custom_components.eybond_local.connection.recovery_contract import (
            RecoveryContract,
        )

        order: list[str] = []
        data = dict(data)
        durable_pn = data.get("collector_pn")
        if durable_pn and not data.get("collector_virtual_bridge"):
            RecoveryContract.empty_for_pn(
                durable_pn, identity_source="fc2_parameter_2"
            ).write_to(data)
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data=dict(data), options=dict(options))
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(
                remote_ip="192.168.1.55",
                collector_pn=str(data.get("collector_pn", "")),
                collector_virtual_bridge=bool(data.get("collector_virtual_bridge")),
            ),
        )

        def _update(**kwargs):
            order.append("entry")
            coordinator.config_entry = types.SimpleNamespace(
                data=kwargs.get("data", coordinator.config_entry.data),
                options=kwargs.get("options", coordinator.config_entry.options),
            )

        coordinator._async_update_entry_without_reload = _update

        async def _run_executor(func, *args):
            order.append("registry")
            if executor_raises:
                raise OSError("simulated registry failure")
            return func(*args)

        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(config_dir=str(config_dir)),
            async_add_executor_job=_run_executor,
        )
        return coordinator, order

    def test_persist_selection_writes_data_then_registry_in_order(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            get_collector_registry_record,
        )

        async def _run() -> None:
            tmp = Path(tempfile.mkdtemp())
            coordinator, order = self._persist_coordinator(
                data={"collector_kind": "factory_eybond", "collector_pn": "E5000025SYN0000000001"},
                # A stale options copy of the original endpoint must be dropped.
                options={"collector_original_server_endpoint": "old.example,18899,TCP"},
                config_dir=tmp,
            )
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection()
            )
            self.assertEqual(error, "")
            # §5 ordering: entry whole-record BEFORE the PN registry.
            self.assertEqual(order, ["entry", "registry"])
            # Canonical entry.data holds the honest whole record.
            new_data = coordinator.config_entry.data
            self.assertEqual(new_data["collector_original_server_endpoint"], "dtu.example,18899,TCP")
            self.assertEqual(new_data["collector_original_server_endpoint_source"], "user_selected_catalog")
            self.assertEqual(new_data["collector_original_server_endpoint_profile_key"], "smartess_at")
            self.assertTrue(new_data["collector_original_server_endpoint_observed_at"])
            # Stale options copy dropped so it cannot shadow.
            self.assertNotIn("collector_original_server_endpoint", coordinator.config_entry.options)
            # PN-bound registry holds the selected endpoint.
            record = get_collector_registry_record(
                config_dir=tmp, collector_pn="E5000025SYN0000000001"
            )
            self.assertIsNotNone(record)
            self.assertEqual(record.original_endpoint_raw, "dtu.example,18899,TCP")
            self.assertEqual(record.source, "user_selected_catalog")

        asyncio.run(_run())

    def test_persist_selection_registry_forces_over_prior_observed(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            get_collector_registry_record,
            remember_collector_original_endpoint,
        )

        async def _run() -> None:
            tmp = Path(tempfile.mkdtemp())
            # A prior auto-observed original already exists for this PN.
            remember_collector_original_endpoint(
                config_dir=tmp,
                collector_pn="E5000025SYN0000000001",
                original_endpoint_raw="observed.old,18899,TCP",
                source="runtime_observed",
            )
            coordinator, _order = self._persist_coordinator(
                data={"collector_kind": "factory_eybond", "collector_pn": "E5000025SYN0000000001"},
                options={},
                config_dir=tmp,
            )
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection()
            )
            self.assertEqual(error, "")
            record = get_collector_registry_record(
                config_dir=tmp, collector_pn="E5000025SYN0000000001"
            )
            # The explicit user choice replaced the prior observed record.
            self.assertEqual(record.original_endpoint_raw, "dtu.example,18899,TCP")

        asyncio.run(_run())

    def test_persist_selection_pn_required_when_missing(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED,
        )

        async def _run() -> None:
            coordinator, order = self._persist_coordinator(
                data={"collector_kind": "factory_eybond"},  # no collector_pn
                options={},
                config_dir="/nonexistent-cp2b2",
            )
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection()
            )
            self.assertEqual(error, TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED)
            self.assertEqual(order, [])  # zero writes before wire

        asyncio.run(_run())

    def test_persist_selection_rejects_entry_pn_without_strong_contract(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED,
        )

        async def _run() -> None:
            coordinator, order = self._persist_coordinator(
                data={"collector_kind": "factory_eybond", "collector_pn": "WEAK"},
                options={},
                config_dir="/nonexistent-cp2b2",
            )
            coordinator.config_entry.data.pop("recovery_contract", None)
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection(), collector_pn="WEAK"
            )
            self.assertEqual(error, TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED)
            self.assertEqual(order, [])

        asyncio.run(_run())

    def test_persist_selection_rejects_foreign_strong_contract(self) -> None:
        from custom_components.eybond_local.connection.recovery_contract import (
            RecoveryContract,
        )
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED,
        )

        async def _run() -> None:
            coordinator, order = self._persist_coordinator(
                data={
                    "collector_kind": "factory_eybond",
                    "collector_pn": "E5000025SYN0000000001",
                },
                options={},
                config_dir="/nonexistent-cp2b2",
            )
            RecoveryContract.empty_for_pn(
                "V001020SYN62344022", identity_source="fc2_parameter_2"
            ).write_to(coordinator.config_entry.data)
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection(),
                collector_pn="E5000025SYN0000000001",
            )
            self.assertEqual(error, TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED)
            self.assertEqual(order, [])

        asyncio.run(_run())

    def test_durable_transition_pn_accepts_exact_owned_strong_session_for_legacy_entry(self) -> None:
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        pn = "E5000025SYN0000000001"
        inventory = (
            {
                "session_id": "legacy-live-session",
                "collector_pn": pn,
                "collector_identity_source": "fc2_parameter_2",
                "state": "routed_framed",
                "protocol_shape": "eybond_framed",
            },
        )
        registry = CallbackSessionRegistry(sessions_source=lambda: inventory)
        registry.claim_session("entry-id", session_id="legacy-live-session")
        registry.promote_claim_to_full_pn("entry-id", pn)
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_pn": pn}, options={}
        )
        self.assertEqual(
            coordinator._durable_transition_collector_pn(
                identity_registry=registry, owner_id="entry-id"
            ),
            pn,
        )

    def test_durable_transition_pn_rejects_weak_owned_session(self) -> None:
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        pn = "E5000025SYN0000000001"
        inventory = (
            {
                "session_id": "weak-live-session",
                "collector_pn": pn,
                "collector_identity_source": "framed_heartbeat",
                "state": "routed_framed",
                "protocol_shape": "eybond_framed",
            },
        )
        registry = CallbackSessionRegistry(sessions_source=lambda: inventory)
        registry.claim_session("entry-id", session_id="weak-live-session")
        registry.promote_claim_to_full_pn("entry-id", pn)
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_pn": pn}, options={}
        )
        self.assertEqual(
            coordinator._durable_transition_collector_pn(
                identity_registry=registry, owner_id="entry-id"
            ),
            "",
        )

    def test_persist_selection_pn_required_for_bridge(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED,
        )

        async def _run() -> None:
            coordinator, order = self._persist_coordinator(
                data={"collector_virtual_bridge": True, "collector_pn": "E5000025SYN0000000001"},
                options={},
                config_dir="/nonexistent-cp2b2",
            )
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection()
            )
            self.assertEqual(error, TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED)
            self.assertEqual(order, [])

        asyncio.run(_run())

    def test_bridge_endpoint_relocation_needs_no_cloud_rollback_record(self) -> None:
        async def _run() -> None:
            pn = "E5000025SYN0000000001"
            coordinator, order = self._persist_coordinator(
                data={
                    "collector_virtual_bridge": True,
                    "collector_kind": "esp_eybond_bridge",
                    "collector_pn": pn,
                },
                options={},
                config_dir="/nonexistent-local-bridge-relocation",
            )
            coordinator._durable_transition_collector_pn = lambda **_kwargs: pn

            refusal = await coordinator._async_persist_inbound_rollback_endpoint(
                "192.168.1.50,8899,TCP",
                collector_pn=pn,
            )

            self.assertEqual(refusal, "")
            self.assertEqual(order, [])
            self.assertNotIn(
                "collector_original_server_endpoint", coordinator.config_entry.data
            )

        asyncio.run(_run())

    def test_persist_selection_registry_failure_keeps_entry_intent_no_wire(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_ROLLBACK_PERSIST_FAILED,
        )

        async def _run() -> None:
            coordinator, order = self._persist_coordinator(
                data={"collector_kind": "factory_eybond", "collector_pn": "E5000025SYN0000000001"},
                options={},
                config_dir="/nonexistent-cp2b2",
                executor_raises=True,
            )
            error = await coordinator._async_persist_cloud_rollback_selection(
                self._catalog_selection()
            )
            self.assertEqual(error, TRANSITION_ROLLBACK_PERSIST_FAILED)
            # The safe local entry intent was written (retryable); registry failed.
            self.assertEqual(order, ["entry", "registry"])
            self.assertEqual(
                coordinator.config_entry.data["collector_original_server_endpoint"],
                "dtu.example,18899,TCP",
            )

        asyncio.run(_run())

    def test_inbound_overwrite_without_live_endpoint_requires_saved_original(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_INBOUND_ROLLBACK_PERSIST_FAILED,
        )

        async def _run() -> None:
            tmp = Path(tempfile.mkdtemp())
            coordinator, order = self._persist_coordinator(
                data={
                    "collector_kind": "factory_eybond",
                    "collector_pn": "E5000025SYN0000000001",
                },
                options={},
                config_dir=tmp,
            )
            refusal = await coordinator._async_persist_inbound_rollback_endpoint(
                "", collector_pn="E5000025SYN0000000001"
            )
            self.assertEqual(
                refusal, TRANSITION_INBOUND_ROLLBACK_PERSIST_FAILED
            )
            self.assertEqual(order, ["registry"])

        asyncio.run(_run())

    def test_inbound_overwrite_accepts_existing_durable_original_when_live_is_empty(self) -> None:
        async def _run() -> None:
            tmp = Path(tempfile.mkdtemp())
            coordinator, order = self._persist_coordinator(
                data={
                    "collector_kind": "factory_eybond",
                    "collector_pn": "E5000025SYN0000000001",
                    "collector_original_server_endpoint": "saved.example,18899,TCP",
                    "collector_original_server_endpoint_source": "runtime_observed",
                },
                options={},
                config_dir=tmp,
            )
            refusal = await coordinator._async_persist_inbound_rollback_endpoint(
                "", collector_pn="E5000025SYN0000000001"
            )
            self.assertEqual(refusal, "")
            self.assertEqual(order, ["registry"])

        asyncio.run(_run())

    def test_inbound_remembers_external_endpoint_before_any_overwrite(self) -> None:
        # CP2B.2 §9 audit: the EXISTING continuous remember (run on every snapshot
        # prepare, BEFORE any inbound transition) durably saves the current
        # external endpoint into the entry (options whole-record) AND the PN
        # registry. This is the mechanism the inbound overwrite relies on -- it is
        # proven here so the switch never loses the original cloud endpoint.
        from custom_components.eybond_local.support.collector_registry import (
            get_collector_registry_record,
        )

        async def _run() -> None:
            tmp = Path(tempfile.mkdtemp())
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={
                    "collector_kind": "factory_eybond",
                    "collector_pn": "E5000025SYN0000000001",
                    "collector_ip": "192.168.1.55",
                },
                options={},
            )
            coordinator.data = self.RuntimeSnapshot(
                values={"collector_server_endpoint": "ess.eybond.com,18899,TCP"},
                collector=types.SimpleNamespace(
                    remote_ip="192.168.1.55",
                    collector_pn="E5000025SYN0000000001",
                    collector_virtual_bridge=False,
                ),
            )
            # HA's own advertised host (the endpoint we would OVERWRITE to) differs
            # from the external endpoint, so the external one is remembered.
            coordinator._runtime = types.SimpleNamespace(effective_advertised_server_ip="192.168.1.50")
            coordinator._connection_spec = types.SimpleNamespace(effective_advertised_server_ip="192.168.1.50")
            coordinator._remembered_collector_server_endpoint = ""

            def _update(**kwargs):
                if "options" in kwargs:
                    coordinator.config_entry = types.SimpleNamespace(
                        data=coordinator.config_entry.data, options=kwargs["options"]
                    )

            coordinator._async_update_entry_without_reload = _update

            async def _run_executor(func, *args):
                return func(*args)

            coordinator.hass = types.SimpleNamespace(
                config=types.SimpleNamespace(config_dir=str(tmp)),
                async_add_executor_job=_run_executor,
            )

            await coordinator._async_remember_collector_server_endpoint(coordinator.data)

            # Saved as the durable original in the entry (options whole-record).
            self.assertEqual(
                coordinator.config_entry.options.get("collector_original_server_endpoint"),
                "ess.eybond.com,18899,TCP",
            )
            # Saved PN-bound in the registry.
            record = get_collector_registry_record(
                config_dir=tmp, collector_pn="E5000025SYN0000000001"
            )
            self.assertIsNotNone(record)
            self.assertEqual(record.original_endpoint_raw, "ess.eybond.com,18899,TCP")

        asyncio.run(_run())

    # ---- CP2C: endpoint-operation authority mutual exclusion ----
    def _full_control_coordinator(self, entry_id: str):
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id=entry_id, data={}, options={"control_mode": "full"}
        )
        writes: list[tuple] = []

        async def _write(*args, **kwargs):
            writes.append((args, kwargs))
            return {"readback_endpoint": args[0] if args else ""}

        coordinator._runtime = types.SimpleNamespace(
            async_set_collector_server_endpoint=_write
        )
        return coordinator, writes

    def test_5_6_7_manual_write_refused_with_zero_wire_when_busy(self) -> None:
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
            OPERATION_PROXY_CAPTURE,
            OPERATION_SHADOW_LEARNING,
            OPERATION_STRATEGY_TRANSITION,
        )

        async def _run() -> None:
            for op in (
                OPERATION_PROXY_CAPTURE,
                OPERATION_SHADOW_LEARNING,
                OPERATION_STRATEGY_TRANSITION,
            ):
                entry_id = f"cp2c-manual-{op}"
                coordinator, writes = self._full_control_coordinator(entry_id)
                held = AUTH.acquire(entry_id, op)
                try:
                    with self.assertRaises(RuntimeError) as ctx:
                        await coordinator.async_set_raw_collector_server_endpoint(
                            endpoint="dtu.example,18899,TCP", confirm_redirect=True
                        )
                    self.assertEqual(str(ctx.exception), "collector_endpoint_operation_busy")
                    self.assertEqual(writes, [], f"{op} allowed a wire write")
                finally:
                    AUTH.release(entry_id, held.token)

        asyncio.run(_run())

    def test_7_transition_lease_is_blocked_by_a_foreign_endpoint_owner(self) -> None:
        # The transition facade acquires via STRATEGY_TRANSITION_LEASES, which
        # delegates to the ONE authority. While proxy/shadow (or a manual write)
        # owns the entry, that lease cannot be acquired -> the facade returns a
        # typed busy. (The full facade is not driven here because its
        # passive_discovery import needs the real const module, unavailable under
        # this stub harness; the exclusion itself is proven at the lease/authority
        # boundary the facade uses.)
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
            OPERATION_PROXY_CAPTURE,
            OPERATION_SHADOW_LEARNING,
            OPERATION_STRATEGY_TRANSITION,
        )
        from custom_components.eybond_local.connection.strategy_transition import (
            STRATEGY_TRANSITION_LEASES,
        )

        del OPERATION_STRATEGY_TRANSITION  # imported for documentation of the mapping
        for op in (OPERATION_PROXY_CAPTURE, OPERATION_SHADOW_LEARNING):
            entry_id = f"cp2c-transition-lease-{op}"
            held = AUTH.acquire(entry_id, op)
            try:
                # A foreign endpoint owner blocks the transition lease...
                self.assertFalse(STRATEGY_TRANSITION_LEASES.acquire(entry_id))
                # ...and the active owner the facade reads is that foreign op, so
                # the facade surfaces the neutral busy reason (not "already
                # running", which is reserved for a concurrent strategy op).
                self.assertEqual(AUTH.active_operation(entry_id), op)
            finally:
                AUTH.release(entry_id, held.token)

    def test_3_transient_operation_releases_lease_on_error_and_cancel(self) -> None:
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
            OPERATION_MANUAL_ENDPOINT_WRITE,
        )

        async def _run() -> None:
            entry_id = "cp2c-cm-lifecycle"
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)

            # An error inside the guarded block releases the lease (finally).
            with self.assertRaises(ValueError):
                async with coordinator._collector_endpoint_operation(
                    OPERATION_MANUAL_ENDPOINT_WRITE
                ):
                    raise ValueError("inner boom")
            self.assertFalse(AUTH.is_held(entry_id))

            # Cancellation inside the guarded block releases the lease too.
            async def _hold() -> None:
                async with coordinator._collector_endpoint_operation(
                    OPERATION_MANUAL_ENDPOINT_WRITE
                ):
                    await asyncio.sleep(10)

            task = asyncio.ensure_future(_hold())
            await asyncio.sleep(0)
            self.assertTrue(AUTH.is_held(entry_id))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(AUTH.is_held(entry_id))

            # Busy raise happens BEFORE the block body (zero side effects).
            other = AUTH.acquire(entry_id, OPERATION_MANUAL_ENDPOINT_WRITE)
            try:
                entered = False
                with self.assertRaises(RuntimeError):
                    async with coordinator._collector_endpoint_operation(
                        OPERATION_MANUAL_ENDPOINT_WRITE
                    ):
                        entered = True
                self.assertFalse(entered)
            finally:
                AUTH.release(entry_id, other.token)

        asyncio.run(_run())

    def test_runtime_identity_promotes_unknown_collector_to_factory(self) -> None:
        updates: list[dict[str, object]] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_kind": "unknown",
                "connection_strategy": "callback_on_demand",
                "collector_operation_mode": "home_assistant_only",
            },
            options={"collector_kind": "unknown"},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={
                "model_name": "SMG 6200",
                "serial_number": "SMGSYN240001",
            },
            collector=None,
        )
        coordinator._async_update_entry_without_reload = lambda **kwargs: updates.append(kwargs)

        self.assertTrue(coordinator.collector_capabilities.proxy_capture)
        self.assertTrue(coordinator.collector_capabilities.shadow_learning)
        self.assertEqual(
            coordinator.collector_operation_mode,
            "smartess_cloud_home_assistant",
        )

        coordinator._sync_collector_capability_profile()

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["data"]["collector_kind"], "factory_eybond")
        self.assertEqual(updates[0]["options"]["collector_kind"], "factory_eybond")
        # Capability enrichment never rewrites either architecture axis.
        self.assertEqual(
            updates[0]["data"]["connection_strategy"], "callback_on_demand"
        )
        self.assertEqual(
            updates[0]["data"]["collector_operation_mode"],
            "home_assistant_only",
        )

    def test_runtime_bridge_syncs_profile_without_persisting_operation_mode(self) -> None:
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

        coordinator._sync_collector_capability_profile()

        self.assertEqual(len(updates), 1)
        data = updates[0]["data"]
        options = updates[0]["options"]
        self.assertEqual(
            data["collector_operation_mode"], "smartess_cloud_home_assistant"
        )
        self.assertEqual(
            options["collector_operation_mode"], "smartess_cloud_home_assistant"
        )
        self.assertTrue(data["collector_virtual_bridge"])
        self.assertTrue(options["collector_virtual_bridge"])
        # A bridge combined with legacy cloud/callback axes is not silently
        # relabelled HA-only; the read-only profile reports the mismatch.
        self.assertEqual(coordinator.collector_operation_mode, "custom")

    def test_runtime_bridge_sync_requests_reload_after_platform_setup(self) -> None:
        updates: list[dict[str, object]] = []
        reloads: list[str] = []

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={"collector_operation_mode": "smartess_cloud_home_assistant"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
        )
        coordinator.hass = types.SimpleNamespace(
            async_create_task=lambda coroutine: reloads.append("scheduled"),
            config_entries=types.SimpleNamespace(
                async_reload=lambda entry_id: entry_id,
            ),
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(collector_virtual_bridge=True),
        )
        coordinator._entity_platforms_initialized = True
        coordinator._entity_platform_reload_requested = False
        coordinator._async_update_entry_without_reload = lambda **kwargs: updates.append(kwargs)

        coordinator._sync_collector_capability_profile()

        self.assertEqual(len(updates), 1)
        self.assertEqual(reloads, ["scheduled"])
        self.assertTrue(coordinator._entity_platform_reload_requested)

    def test_reverse_discovery_off_for_inbound_strategy_even_for_bridge(self) -> None:
        # Architecture invariant: the UDP callback gate is keyed purely on the
        # explicit connection_strategy, NOT on the collector type. An inbound
        # entry (the collector dials Home Assistant by itself) never runs reverse
        # discovery -- even a virtual bridge. This replaces the old collector-type
        # exception that kept reverse discovery on for HA-only bridges.
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
            data={"connection_strategy": "inbound"},
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            collector=types.SimpleNamespace(collector_virtual_bridge=True),
        )

        coordinator._configure_reverse_discovery_mode()

        self.assertEqual(reverse_discovery_flags, [False])

    def test_reverse_discovery_ignores_endpoint_hostname_for_callback_strategy(self) -> None:
        # Architecture invariant: the endpoint string is opaque and never drives
        # transport behavior. A callback_on_demand entry keeps reverse discovery
        # ENABLED regardless of whether the live endpoint hostname happens to
        # point at this Home Assistant host. This replaces the old
        # endpoint-hostname decision that turned discovery off.
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
            data={"connection_strategy": "callback_on_demand"},
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_server_endpoint": "192.168.1.50,18899,TCP"}
        )

        coordinator._configure_reverse_discovery_mode()

        self.assertEqual(reverse_discovery_flags, [True])

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
                entry_id="entry-rediscovery",
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
        original_builder = self.coordinator_cloud_tools_module.build_proxy_capture_overview

        def _fake_build_proxy_capture_overview(**kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(
                can_start=bool(kwargs["upstream_endpoint"]),
                can_stop=False,
                blocking_reason="",
                redirect_required=True,
            )

        self.coordinator_cloud_tools_module.build_proxy_capture_overview = (
            _fake_build_proxy_capture_overview
        )
        try:
            overview = coordinator.proxy_capture_overview
        finally:
            self.coordinator_cloud_tools_module.build_proxy_capture_overview = original_builder

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

    def test_cloud_tools_share_the_exact_live_endpoint_context(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )

            async def _live_endpoint_state():
                return {"current_endpoint": "eu.smartess.io,18899,TCP"}

            coordinator._runtime = types.SimpleNamespace(
                async_get_collector_server_endpoint_state=_live_endpoint_state,
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator.data = self.RuntimeSnapshot(
                values={
                    "collector_server_endpoint": "stale.example,18899,TCP",
                }
            )

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_family",
                new_callable=PropertyMock,
                return_value="smartess",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_upstream_endpoint",
                new_callable=PropertyMock,
                return_value="eu.smartess.io,18899,TCP",
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_target_endpoint",
                new_callable=PropertyMock,
                return_value="192.168.1.50,18899,TCP",
            ):
                context = (
                    await coordinator._async_prepare_cloud_tool_endpoint_context()
                )

            self.assertEqual(
                context.current_endpoint,
                "eu.smartess.io,18899,TCP",
            )
            self.assertEqual(
                context.upstream_endpoint,
                "eu.smartess.io,18899,TCP",
            )
            self.assertEqual(
                context.target_endpoint,
                "192.168.1.50,18899,TCP",
            )
            self.assertEqual(
                coordinator.data.values["collector_server_endpoint"],
                "eu.smartess.io,18899,TCP",
            )

        asyncio.run(_run())

    def test_cloud_tool_endpoint_context_never_falls_back_to_stale_snapshot(
        self,
    ) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )

            async def _live_endpoint_state():
                raise RuntimeError("collector_not_connected")

            coordinator._runtime = types.SimpleNamespace(
                async_get_collector_server_endpoint_state=_live_endpoint_state,
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator.data = self.RuntimeSnapshot(
                values={
                    "collector_server_endpoint": "stale.example,18899,TCP",
                }
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "cloud_tool_collector_not_connected",
            ):
                await coordinator._async_prepare_cloud_tool_endpoint_context()

            self.assertEqual(
                coordinator.data.values["collector_server_endpoint"],
                "stale.example,18899,TCP",
            )

        asyncio.run(_run())

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
            original_builder = (
                self.coordinator_snapshot_projection_module.build_proxy_capture_overview
            )

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

            self.coordinator_snapshot_projection_module.build_proxy_capture_overview = (
                _fake_build_proxy_capture_overview
            )
            try:
                values = await coordinator._proxy_capture_values()
            finally:
                self.coordinator_snapshot_projection_module.build_proxy_capture_overview = (
                    original_builder
                )

            self.assertEqual(captured["upstream_endpoint"], "47.91.67.66,18899,TCP")
            self.assertTrue(values["proxy_capture_can_start"])

        asyncio.run(_run())

    def test_proxy_trace_manifest_download_uses_signed_authenticated_api(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._proxy_trace_download_manifest_path = ""
            coordinator._proxy_trace_download_details = ("", "")
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")

            async def _async_add_executor_job(func):
                return func()

            with tempfile.TemporaryDirectory() as tmp:
                manifest_path = Path(tmp) / "manifest.json"
                manifest_path.write_text("{}", encoding="utf-8")
                coordinator.hass = types.SimpleNamespace(
                    config=types.SimpleNamespace(config_dir=tmp),
                    async_add_executor_job=_async_add_executor_job,
                )
                bundle_path = Path(tmp) / "capture.zip"
                signed_url = (
                    "/api/eybond_local/proxy_capture/entry-id/capture.zip"
                    "?authSig=signed"
                )
                with patch.object(
                    self.coordinator_cloud_tools_module,
                    "export_proxy_trace_bundle",
                    return_value=bundle_path,
                ), patch.object(
                    self.coordinator_cloud_tools_module,
                    "sign_proxy_capture_download_url",
                    return_value=signed_url,
                ):
                    result = await coordinator._async_proxy_trace_manifest_download_details(
                        str(manifest_path)
                    )

            self.assertEqual(result, (str(bundle_path), signed_url))

        asyncio.run(_run())

    def test_proxy_download_paths_do_not_call_removed_absolute_url_helper(self) -> None:
        self.assertNotIn(
            "_absolute_local_download_url",
            self.coordinator_module.EybondLocalCoordinator.async_stop_proxy_capture.__code__.co_names,
        )
        self.assertNotIn(
            "_absolute_local_download_url",
            self.coordinator_module.EybondLocalCoordinator._async_proxy_trace_manifest_download_details.__code__.co_names,
        )

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

    def test_collector_device_info_uses_persisted_bridge_profile(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-bridge",
            data={
                "collector_pn": "V000405SYN94677058",
                "collector_ip": "195.138.86.175",
                "collector_kind": "esp_eybond_bridge",
                "collector_hardware_version": "esp-collector/0.1.8/ESP8266",
                "collector_bridge_version": "0.1.8",
            },
            options={},
            title="Collector PN V000405SYN94677058",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"collector_hardware_version": "esp-collector/0.1.8/ESP8266"},
            collector=types.SimpleNamespace(
                collector_pn="V000405SYN94677058",
                profile_name="",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                collector_virtual_bridge=False,
                collector_bridge_kind="",
                collector_bridge_version="",
            ),
        )

        info = coordinator.collector_device_info()

        self.assertEqual(info["manufacturer"], "ESP EyeBond Collector (community)")
        self.assertEqual(info["model"], "ESP EyeBond Collector")
        self.assertEqual(info["sw_version"], "0.1.8")
        self.assertEqual(info["hw_version"], "esp-collector/0.1.8/ESP8266")

    def test_collector_device_info_does_not_use_oem_eybond_manufacturer_fallback(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-unknown",
            data={"collector_pn": "V001107SYN8229"},
            options={},
            title="Collector PN V001107SYN8229",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(
                collector_pn="V001107SYN8229",
                profile_name="Unknown Collector 0x0000",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                collector_virtual_bridge=False,
                collector_bridge_kind="",
                collector_bridge_version="",
            ),
        )

        info = coordinator.collector_device_info()

        self.assertNotIn("manufacturer", info)
        self.assertEqual(info["model"], "Unknown Collector 0x0000")

    def test_collector_device_registry_clears_stale_oem_eybond_fallback(self) -> None:
        registry = FakeRegistry()
        self.coordinator_module.dr.async_get = lambda hass: registry

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-unknown",
            data={"collector_pn": "V001107SYN8229"},
            options={},
            title="Collector PN V001107SYN8229",
        )
        coordinator.data = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(
                collector_pn="V001107SYN8229",
                profile_name="Unknown Collector 0x0000",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                collector_virtual_bridge=False,
                collector_bridge_kind="",
                collector_bridge_version="",
            ),
        )
        coordinator._last_synced_collector_device_meta = ("", "", "", "", "", "")
        stale = registry.async_get_or_create(
            config_entry_id="entry-unknown",
            identifiers={("eybond_local", "entry-unknown:collector")},
            name="Collector PN V001107SYN8229",
            manufacturer="OEM / EyeBond",
            model="Unknown Collector 0x0000",
        )
        self.assertEqual(stale.manufacturer, "OEM / EyeBond")

        coordinator._async_sync_collector_device_registry()

        device = registry.async_get_device(
            identifiers={("eybond_local", "entry-unknown:collector")}
        )
        self.assertIsNotNone(device)
        self.assertIsNone(device.manufacturer)

    def test_remember_runtime_identity_strengthens_pending_entry_metadata(self) -> None:
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
                "control_mode": "read_only",
            },
            options={"control_mode": "read_only"},
            title="Collector 192.168.1.14",
        )
        coordinator.data = self.RuntimeSnapshot()

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
                driver_key="pi30",
                variant_key="default",
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
        self.assertEqual(coordinator.config_entry.data["detected_driver"], "pi30")
        self.assertEqual(coordinator.config_entry.data["control_mode"], "read_only")
        self.assertEqual(coordinator.config_entry.options["control_mode"], "read_only")
        self.assertNotIn("driver_hint", coordinator.config_entry.data)
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

    def test_remember_runtime_identity_collector_only_does_not_erase_inverter(self) -> None:
        # Runtime state-machine invariant: a collector-only snapshot (no inverter)
        # must not erase a previously confirmed inverter identity.
        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                del title, options
                if data is not None:
                    entry.data = dict(data)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-3",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "PowMr 4.2kW",
                "detected_serial": "55355535553555",
                "detection_confidence": "high",
                "driver_hint": "pi30",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="PowMr 4.2kW (55355535553555)",
        )
        coordinator.data = self.RuntimeSnapshot()

        collector_only = self.RuntimeSnapshot(
            values={},
            inverter=None,
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

        asyncio.run(coordinator._async_remember_runtime_identity(collector_only))

        self.assertEqual(coordinator.config_entry.data["detected_model"], "PowMr 4.2kW")
        self.assertEqual(
            coordinator.config_entry.data["detected_serial"], "55355535553555"
        )

    def test_remember_runtime_identity_different_serial_keeps_durable(self) -> None:
        # Runtime state-machine invariant 6: a different confirmed serial is a
        # conflict, not a silent swap of the durable inverter identity.
        class _ConfigEntries:
            def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                del title, options
                if data is not None:
                    entry.data = dict(data)

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-4",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "PowMr 4.2kW",
                "detected_serial": "55355535553555",
                "detection_confidence": "high",
                "driver_hint": "pi30",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="PowMr 4.2kW (55355535553555)",
        )
        coordinator.data = self.RuntimeSnapshot()

        conflicting = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355599999999",
                driver_key="pi30",
                variant_key="default",
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

        asyncio.run(coordinator._async_remember_runtime_identity(conflicting))

        # Durable serial is kept; the different serial is not silently swapped in.
        self.assertEqual(
            coordinator.config_entry.data["detected_serial"], "55355535553555"
        )

    def test_remember_runtime_identity_does_not_persist_callback_peer_ip(self) -> None:
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
            entry_id="entry-callback",
            data={
                "connection_mode": "callback_listener",
                "collector_pn": "V001020SYN62344022",
                "detected_model": "",
                "detected_serial": "",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="Collector PN V001020SYN62344022",
        )
        coordinator.data = self.RuntimeSnapshot()

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
            ),
            collector=types.SimpleNamespace(
                remote_ip="195.138.86.175",
                collector_pn="V001020SYN62344022",
                profile_name="",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                smartess_protocol_profile_key="smartess_at",
            ),
        )

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertNotIn("collector_ip", coordinator.config_entry.data)
        self.assertEqual(
            coordinator.config_entry.data["collector_pn"],
            "V001020SYN62344022",
        )
        self.assertEqual(len(updated_entries), 1)

    def test_remember_runtime_identity_does_not_replace_routed_ip_with_nat_peer(
        self,
    ) -> None:
        """A live TCP peer is observation, not the configured collector route."""

        updated_entries: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(
                self,
                entry,
                *,
                title=None,
                data=None,
                options=None,
            ) -> None:
                del options
                if data is not None:
                    entry.data = dict(data)
                if title is not None:
                    entry.title = title
                updated_entries.append(dict(entry.data))

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-routed",
            data={
                "connection_mode": "known_ip",
                "collector_ip": "192.168.8.52",
                "collector_pn": "V001020SYN62344022",
                "detected_model": "",
                "detected_serial": "",
                "server_ip": "192.168.2.50",
            },
            options={},
            title="Collector PN V001020SYN62344022",
        )
        coordinator.data = self.RuntimeSnapshot()

        snapshot = self.RuntimeSnapshot(
            values={},
            inverter=types.SimpleNamespace(
                model_name="PowMr 6.2kW",
                serial_number="55355535553555",
            ),
            collector=types.SimpleNamespace(
                remote_ip="192.168.2.1",
                collector_pn="V001020SYN62344022",
                profile_name="",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                smartess_protocol_profile_key="smartess_at",
            ),
        )

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertEqual(
            coordinator.config_entry.data["collector_ip"],
            "192.168.8.52",
        )
        self.assertNotEqual(
            coordinator.config_entry.data["collector_ip"],
            snapshot.collector.remote_ip,
        )
        self.assertEqual(updated_entries[-1]["collector_ip"], "192.168.8.52")

    def test_remember_runtime_identity_upgrades_collector_unique_id_to_full_pn(self) -> None:
        updated_entries: list[dict[str, object]] = []

        class _ConfigEntries:
            def async_update_entry(
                self,
                entry,
                *,
                title=None,
                data=None,
                options=None,
                unique_id=None,
            ) -> None:
                del options
                if data is not None:
                    entry.data = dict(data)
                if title is not None:
                    entry.title = title
                if unique_id is not None:
                    entry.unique_id = unique_id
                updated_entries.append(
                    {
                        "title": entry.title,
                        "data": dict(entry.data),
                        "unique_id": entry.unique_id,
                    }
                )

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(config_entries=_ConfigEntries())
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-callback",
            unique_id="collector:V001107SYN8229",
            data={
                "connection_mode": "callback_listener",
                "collector_pn": "V001107SYN82291016",
                "detected_model": "",
                "detected_serial": "",
                "server_ip": "192.168.1.104",
            },
            options={},
            title="Collector PN V001107SYN82291016",
        )
        coordinator.data = self.RuntimeSnapshot()

        snapshot = self.RuntimeSnapshot(
            values={},
            collector=types.SimpleNamespace(
                remote_ip="192.168.1.1",
                collector_pn="V001107SYN82291016",
                profile_name="",
                smartess_protocol_name=None,
                smartess_protocol_asset_name=None,
                smartess_collector_version="",
                smartess_protocol_profile_key="smartess_at",
            ),
        )

        asyncio.run(coordinator._async_remember_runtime_identity(snapshot))

        self.assertEqual(
            coordinator.config_entry.data["collector_pn"],
            "V001107SYN82291016",
        )
        self.assertEqual(
            coordinator.config_entry.unique_id,
            "collector:V001107SYN82291016",
        )
        self.assertEqual(
            updated_entries[-1]["unique_id"],
            "collector:V001107SYN82291016",
        )

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

    def test_remember_runtime_identity_clears_stale_pi30_placeholder(self) -> None:
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
            entry_id="entry-pi30-placeholder",
            data={
                "collector_ip": "192.168.1.14",
                "collector_pn": "Q0000000000001",
                "detected_model": "PI30 4200",
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
                driver_key="pi30",
                model_name="PI30 4200",
                serial_number="",
                variant_key="default",
                details={
                    "reported_serial_number": "55355535553555",
                    "serial_identity_source": "qid",
                    "serial_identity_trust": "untrusted",
                    "serial_identity_reason": "known_placeholder",
                },
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

    def test_identity_reload_waits_until_config_entry_is_loaded(self) -> None:
        reload_requests: list[str] = []
        state_callbacks: list[object] = []
        config_entry_state = sys.modules[
            "homeassistant.config_entries"
        ].ConfigEntryState

        class _ConfigEntries:
            async def async_reload(self, entry_id: str) -> None:
                reload_requests.append(entry_id)

        entry = types.SimpleNamespace(
            entry_id="entry-setup-race",
            state=config_entry_state.SETUP_IN_PROGRESS,
        )

        def _on_state_change(callback):
            state_callbacks.append(callback)
            return lambda: state_callbacks.remove(callback)

        entry.async_on_state_change = _on_state_change
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = types.SimpleNamespace(
            config_entries=_ConfigEntries(),
            async_create_task=lambda coro: asyncio.create_task(coro),
            loop=types.SimpleNamespace(
                call_later=lambda delay, callback: asyncio.get_running_loop().call_later(
                    delay, callback
                )
            ),
        )
        coordinator.config_entry = entry
        coordinator.data = self.RuntimeSnapshot(
            inverter=types.SimpleNamespace(
                model_name="PowMr 4.2kW",
                serial_number="55355535553555",
            )
        )
        coordinator._entity_platforms_initialized = False
        coordinator._entity_platform_reload_requested = False
        coordinator._entity_platform_reload_dispatched = False
        coordinator._entry_loaded_reload_unsub = None
        coordinator._shutdown_complete = False

        async def _run() -> None:
            coordinator.mark_entity_platforms_initialized(
                has_inverter_identity=False
            )
            await asyncio.sleep(0)
            self.assertEqual(reload_requests, [])
            self.assertEqual(len(state_callbacks), 1)

            entry.state = config_entry_state.LOADED
            state_callbacks[0]()
            self.assertEqual(reload_requests, [])
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(reload_requests, ["entry-setup-race"])
        self.assertTrue(coordinator._entity_platform_reload_dispatched)

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

    def test_proxy_capture_deadline_scheduler_uses_trace_timestamp_parser(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        scheduled: list[tuple[float, object]] = []
        handle = types.SimpleNamespace(cancel=lambda: None)
        coordinator._proxy_capture_deadline_refresh_handle = None
        coordinator.hass = types.SimpleNamespace(
            loop=types.SimpleNamespace(
                call_later=lambda delay, callback: (
                    scheduled.append((delay, callback)) or handle
                )
            )
        )
        deadline = datetime.now().astimezone()

        with patch.object(
            self.coordinator_cloud_tools_module,
            "parse_proxy_capture_session_timestamp",
            return_value=deadline,
        ) as parser:
            coordinator._schedule_proxy_capture_deadline_refresh(
                "2026-08-21T12:00:00+00:00"
            )

        parser.assert_called_once_with("2026-08-21T12:00:00+00:00")
        self.assertEqual(len(scheduled), 1)
        self.assertIs(coordinator._proxy_capture_deadline_refresh_handle, handle)

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
            coordinator.collector_endpoint_sync_lock_code = lambda: None

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

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "support_acquisition_readiness",
                new_callable=PropertyMock,
                return_value=self._support_readiness(),
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_actions_enabled",
                new_callable=PropertyMock,
                return_value=True,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "proxy_capture_route_running",
                ):
                    await coordinator.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow.jsonl"),
                        raw_capture={},
                    )

            self.assertEqual(save_calls, [])
            self.assertEqual(start_shadow_calls, [])

        import asyncio

        asyncio.run(_run())

    def test_support_readiness_does_not_require_an_identified_inverter(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-unbound-support",
            data={
                "collector_pn": "E50000200000000001",
                "connection_strategy": "callback_on_demand",
                "endpoint_control_policy": "external",
                "detected_model": "",
                "detected_serial": "",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={"driver_key": "auto"})

        with patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            "smartess_collector_pn",
            new_callable=PropertyMock,
            return_value="E50000200000000001",
        ), patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            "cloud_evidence_provider",
            new_callable=PropertyMock,
            return_value="smartess",
        ), patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            "collector_capabilities",
            new_callable=PropertyMock,
            return_value=types.SimpleNamespace(virtual_bridge=False),
        ):
            readiness = coordinator.support_acquisition_readiness

        self.assertTrue(readiness.collector_identified)
        self.assertFalse(readiness.inverter_identified)
        self.assertTrue(readiness.cloud_metadata_read.can_start)
        self.assertTrue(readiness.proxy_capture.can_start)
        self.assertTrue(readiness.active_control_learning.can_start)

    def test_start_shadow_learning_requires_cloud_and_ha_profile(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            downstream_calls: list[bool] = []

            async def _async_active_proxy_capture_state(
                *,
                require_process: bool = True,
            ):
                downstream_calls.append(require_process)
                return None

            coordinator._async_active_proxy_capture_state = (
                _async_active_proxy_capture_state
            )

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "support_acquisition_readiness",
                new_callable=PropertyMock,
                return_value=self._support_readiness(active_can_start=False),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "shadow_learning_requires_cloud_and_ha_profile",
                ):
                    await coordinator.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow.jsonl"),
                        raw_capture={},
                    )

            self.assertEqual(downstream_calls, [])

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
                self.coordinator_module.EybondLocalCoordinator,
                "support_acquisition_readiness",
                new_callable=PropertyMock,
                return_value=self._support_readiness(),
            ), patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_actions_enabled",
                new_callable=PropertyMock,
                return_value=True,
            ), patch.object(
                self.coordinator_cloud_tools_module,
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
                self.coordinator_cloud_tools_module,
                "proxy_capture_session_is_active",
                return_value=True,
            ), patch.object(
                self.coordinator_cloud_tools_module,
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
                self.coordinator_cloud_tools_module,
                "shadow_learning_session_is_active",
                return_value=True,
            ), patch.object(
                self.coordinator_cloud_tools_module,
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

    def test_reconcile_does_not_terminalize_proxy_transitional_states(self) -> None:
        async def _run() -> None:
            snapshot = self.RuntimeSnapshot(values={})
            for status in ("starting", "stopping", "restoring"):
                coordinator = object.__new__(
                    self.coordinator_module.EybondLocalCoordinator
                )
                state = types.SimpleNamespace(status=status)
                stop_calls: list[dict[str, object]] = []
                coordinator._async_active_proxy_capture_state = (
                    lambda *, require_process=False, state=state: asyncio.sleep(
                        0,
                        result=state,
                    )
                )
                coordinator._proxy_capture_process_running = lambda: False
                coordinator.async_stop_proxy_capture = (
                    lambda **kwargs: asyncio.sleep(
                        0,
                        result=stop_calls.append(dict(kwargs)),
                    )
                )
                with patch.object(
                    self.coordinator_cloud_tools_module,
                    "proxy_capture_session_is_active",
                    return_value=True,
                ), patch.object(
                    self.coordinator_cloud_tools_module,
                    "proxy_capture_session_is_expired",
                    return_value=False,
                ):
                    result = (
                        await coordinator._async_reconcile_proxy_capture_session(
                            snapshot
                        )
                    )
                self.assertIs(result, snapshot)
                self.assertEqual(stop_calls, [], status)

        asyncio.run(_run())

    def test_reconcile_does_not_terminalize_shadow_transitional_states(self) -> None:
        async def _run() -> None:
            snapshot = self.RuntimeSnapshot(values={})
            for status in ("preflight", "starting", "restoring"):
                coordinator = object.__new__(
                    self.coordinator_module.EybondLocalCoordinator
                )
                state = types.SimpleNamespace(status=status)
                stop_calls: list[dict[str, object]] = []
                coordinator._async_active_shadow_learning_state = (
                    lambda *, require_process=False, state=state: asyncio.sleep(
                        0,
                        result=state,
                    )
                )
                coordinator._shadow_learning_process_running = lambda: False
                coordinator.async_stop_shadow_learning = (
                    lambda **kwargs: asyncio.sleep(
                        0,
                        result=stop_calls.append(dict(kwargs)),
                    )
                )
                with patch.object(
                    self.coordinator_cloud_tools_module,
                    "shadow_learning_session_is_active",
                    return_value=True,
                ), patch.object(
                    self.coordinator_cloud_tools_module,
                    "shadow_learning_session_is_expired",
                    return_value=False,
                ):
                    result = (
                        await coordinator._async_reconcile_shadow_learning_session(
                            snapshot
                        )
                    )
                self.assertIs(result, snapshot)
                self.assertEqual(stop_calls, [], status)

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
                self.coordinator_cloud_tools_module,
                "shadow_learning_session_is_active",
                return_value=False,
            ), patch.object(
                self.coordinator_cloud_tools_module,
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
                route_owner_id="shadow_learning:entry-id:1",
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator._runtime = types.SimpleNamespace(
                async_stop_shadow_learning_route=lambda **kwargs: asyncio.sleep(0)
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
                route_owner_id="shadow_learning:entry-id:1",
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator._runtime = types.SimpleNamespace(
                async_stop_shadow_learning_route=lambda **kwargs: asyncio.sleep(0)
            )

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                self.assertFalse(require_process)
                return state

            async def _async_restore_proxy_capture_endpoint(endpoint: str):
                return endpoint

            async def _async_verify_restored_collector_endpoint(endpoint: str):
                return {
                    "restore_confirmed": True,
                    "observed_endpoint": endpoint,
                    "restore_error": "",
                }

            async def _async_save_shadow_learning_session_state(new_state):
                saved_states.append(new_state)

            async def _async_clear_shadow_learning_session_state():
                clear_calls.append(True)

            async def _async_request_refresh():
                return None

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator._async_restore_proxy_capture_endpoint = _async_restore_proxy_capture_endpoint
            coordinator._async_verify_restored_collector_endpoint = (
                _async_verify_restored_collector_endpoint
            )
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

    # ---- CP2C blocker 9: production-level endpoint-operation guarantees ----

    def _acquire_foreign_owner(self, entry_id: str):
        """Acquire the ONE authority for ``entry_id`` under a foreign operation."""
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
            OPERATION_STRATEGY_TRANSITION,
        )

        outcome = AUTH.acquire(entry_id, OPERATION_STRATEGY_TRANSITION, owner_ref="foreign:1")
        self.assertTrue(outcome.acquired)
        return AUTH, outcome.token

    def test_public_system_actions_refuse_with_zero_wire_when_busy(self) -> None:
        # apply / reboot / rediscovery / rollback are route-affecting full-control
        # actions: when a FOREIGN operation owns the entry each must typed-refuse
        # BEFORE touching the wire (no apply, reboot, UDP, or endpoint write).
        async def _run() -> None:
            entry_id = "entry-busy-public"
            AUTH, token = self._acquire_foreign_owner(entry_id)
            self.addCleanup(lambda: AUTH.release(entry_id, token))
            wire_calls: list[str] = []

            def _fail(name):
                async def _f(*args, **kwargs):
                    wire_calls.append(name)
                    raise AssertionError(f"{name} must not run while busy")

                return _f

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)
            coordinator._raise_if_high_level_collector_actions_disabled = lambda: None
            coordinator.collector_configuration_lock_code = lambda: "collector_configuration_ready"
            coordinator._runtime = types.SimpleNamespace(
                async_apply_collector_changes=_fail("apply"),
                async_reboot_collector=_fail("reboot"),
                async_trigger_reverse_discovery=_fail("rediscovery"),
                async_set_collector_server_endpoint=_fail("set_endpoint"),
            )

            from custom_components.eybond_local.connection.collector_endpoint_operation import (
                COLLECTOR_ENDPOINT_OPERATION_BUSY,
            )

            for coro in (
                coordinator.async_apply_collector_changes(confirm_restart=True),
                coordinator.async_reboot_collector(confirm_restart=True),
                coordinator.async_trigger_collector_rediscovery(),
                coordinator.async_rollback_collector_server_endpoint(confirm_redirect=True),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    await coro
                self.assertEqual(str(ctx.exception), COLLECTOR_ENDPOINT_OPERATION_BUSY)

            self.assertEqual(wire_calls, [], "no wire action may run while the entry is busy")

        import asyncio

        asyncio.run(_run())

    def test_automatic_reconcile_silently_skips_write_when_entry_is_busy(self) -> None:
        # The best-effort operation-mode reconcile must NOT break the refresh when
        # another endpoint operation owns the entry: it records an honest
        # operation_busy status and performs ZERO endpoint writes (no cooldown
        # stamp either, so it retries once the owner frees the entry).
        async def _run() -> None:
            entry_id = "entry-busy-reconcile"
            AUTH, token = self._acquire_foreign_owner(entry_id)
            self.addCleanup(lambda: AUTH.release(entry_id, token))
            endpoint_writes: list[str] = []

            async def _ensure_listener(port: int) -> None:
                return None

            async def _set_endpoint(endpoint: str, *, apply_changes: bool = True):
                endpoint_writes.append(endpoint)
                return {"readback_endpoint": endpoint, "status": "applied"}

            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                effective_advertised_tcp_port=8899,
            )
            coordinator._runtime = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
                collector_server_endpoint_rollback_target="203.0.113.9,18899,TCP",
                async_ensure_callback_listener=_ensure_listener,
                async_set_collector_server_endpoint=_set_endpoint,
            )
            coordinator._remembered_collector_server_endpoint = ""
            coordinator._collector_operation_pending_target_endpoint = ""
            coordinator._ha_primary_reconcile_last_signature = None
            coordinator._ha_primary_reconcile_last_attempt_monotonic = 0.0
            coordinator.config_entry = types.SimpleNamespace(
                entry_id=entry_id,
                data={},
                options={
                    "collector_operation_mode": "smartess_cloud_home_assistant",
                    "endpoint_control_policy": "integration_managed",
                },
            )
            snapshot = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "203.0.113.9,18899,TCP"},
            )
            coordinator.data = snapshot

            await coordinator._async_reconcile_managed_collector_endpoint(snapshot)

            self.assertEqual(endpoint_writes, [], "busy reconcile must not write the endpoint")
            self.assertEqual(
                snapshot.values.get("collector_operation_endpoint_sync_status"),
                "operation_busy",
            )

        import asyncio

        asyncio.run(_run())

    def test_stop_proxy_capture_refuses_with_zero_mutation_under_foreign_owner(self) -> None:
        # A stop while a FOREIGN operation owns the entry (adopt cannot prove
        # ownership) refuses BEFORE the first state/route/restore mutation.
        async def _run() -> None:
            entry_id = "entry-foreign-proxy-stop"
            AUTH, token = self._acquire_foreign_owner(entry_id)
            self.addCleanup(lambda: AUTH.release(entry_id, token))
            saved_states: list[object] = []

            state = types.SimpleNamespace(
                entry_id=entry_id,
                route_owner_id=f"proxy_capture:{entry_id}:ts",
                collector_pn="E5000020000000",
                trace_path="/tmp/proxy.jsonl",
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                restore_required=True,
                anonymized=False,
                started_at="2026-06-05T12:00:00+00:00",
                expires_at="2026-06-05T12:20:00+00:00",
                status="running",
            )
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                return state

            async def _async_save_proxy_capture_session_state(new_state):
                saved_states.append(new_state)

            def _guarded_restore(*args, **kwargs):
                raise AssertionError("restore must not run under a foreign owner")

            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator._async_save_proxy_capture_session_state = (
                _async_save_proxy_capture_session_state
            )
            coordinator._async_guarded_proxy_capture_restore = _guarded_restore

            from custom_components.eybond_local.connection.collector_endpoint_operation import (
                COLLECTOR_ENDPOINT_OPERATION_BUSY,
            )

            with self.assertRaises(RuntimeError) as ctx:
                await coordinator.async_stop_proxy_capture()
            self.assertEqual(str(ctx.exception), COLLECTOR_ENDPOINT_OPERATION_BUSY)
            self.assertEqual(saved_states, [], "no state may be written under a foreign owner")
            # The foreign owner still holds the entry, untouched.
            self.assertEqual(AUTH.active_operation(entry_id), "strategy_transition")

        import asyncio

        asyncio.run(_run())

    def test_stop_shadow_learning_refuses_with_zero_mutation_under_foreign_owner(self) -> None:
        async def _run() -> None:
            entry_id = "entry-foreign-shadow-stop"
            AUTH, token = self._acquire_foreign_owner(entry_id)
            self.addCleanup(lambda: AUTH.release(entry_id, token))
            saved_states: list[object] = []
            route_stops: list[object] = []

            state = types.SimpleNamespace(
                entry_id=entry_id,
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
                route_owner_id=f"shadow_learning:{entry_id}:1",
            )
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)

            async def _route_stop(**kwargs):
                route_stops.append(kwargs)

            coordinator._runtime = types.SimpleNamespace(
                async_stop_shadow_learning_route=_route_stop
            )

            async def _async_active_shadow_learning_state(*, require_process: bool = True):
                return state

            async def _async_save_shadow_learning_session_state(new_state):
                saved_states.append(new_state)

            def _restore_must_not_run(*args, **kwargs):
                raise AssertionError("restore must not run under a foreign owner")

            coordinator._async_active_shadow_learning_state = _async_active_shadow_learning_state
            coordinator._async_save_shadow_learning_session_state = (
                _async_save_shadow_learning_session_state
            )
            coordinator._async_restore_proxy_capture_endpoint = _restore_must_not_run

            from custom_components.eybond_local.connection.collector_endpoint_operation import (
                COLLECTOR_ENDPOINT_OPERATION_BUSY,
            )

            with self.assertRaises(RuntimeError) as ctx:
                await coordinator.async_stop_shadow_learning()
            self.assertEqual(str(ctx.exception), COLLECTOR_ENDPOINT_OPERATION_BUSY)
            self.assertEqual(saved_states, [])
            self.assertEqual(route_stops, [])
            self.assertEqual(AUTH.active_operation(entry_id), "strategy_transition")

        import asyncio

        asyncio.run(_run())

    def test_startup_recovery_failure_keeps_proxy_state_and_token(self) -> None:
        # B4: when a recovery-stop raises, the recovery must NOT force-clear the
        # session -- the recoverable state stays and the authority stays owned, so
        # a later recovery/stop can finish the restore.
        async def _run() -> None:
            entry_id = "entry-recovery-fail"
            from custom_components.eybond_local.connection.collector_endpoint_operation import (
                COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
                OPERATION_PROXY_CAPTURE,
            )

            # The interrupted mode still owns the entry (adopted at recovery start).
            owner = AUTH.acquire(
                entry_id, OPERATION_PROXY_CAPTURE, owner_ref=f"proxy_capture:{entry_id}:ts"
            )
            self.assertTrue(owner.acquired)
            self.addCleanup(lambda: AUTH.release(entry_id, owner.token))
            clear_calls: list[bool] = []
            notify_calls: list[bool] = []

            state = types.SimpleNamespace(
                entry_id=entry_id,
                route_owner_id=f"proxy_capture:{entry_id}:ts",
                status="running",
            )
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)

            async def _async_active_proxy_capture_state(*, require_process: bool = True):
                return state

            async def _async_stop_proxy_capture(**kwargs):
                raise RuntimeError("restore_failed")

            async def _async_clear_proxy_capture_session_state():
                clear_calls.append(True)

            coordinator._async_active_proxy_capture_state = _async_active_proxy_capture_state
            coordinator.async_stop_proxy_capture = _async_stop_proxy_capture
            coordinator._async_clear_proxy_capture_session_state = (
                _async_clear_proxy_capture_session_state
            )
            coordinator._notify_proxy_capture_restore_unconfirmed = lambda: notify_calls.append(True)

            await coordinator._async_recover_proxy_capture_state()

            self.assertFalse(clear_calls, "recovery failure must NOT clear the session")
            self.assertTrue(notify_calls)
            # The authority stays owned by the interrupted route owner.
            self.assertEqual(AUTH.active_operation(entry_id), OPERATION_PROXY_CAPTURE)

        import asyncio

        asyncio.run(_run())

    # ---- CP2C final: REAL-method start/stop cancellation atomicity ----
    #
    # These drive the production async_start_* / async_stop_* on a bare
    # coordinator with controllable async seams (NOT a hand-rolled model of the
    # algorithm), so they prove the shielded finalization the methods actually run.

    @contextlib.contextmanager
    def _shadow_start_env(self, rec, **seams):
        """Bare-coordinator harness that drives the REAL async_start_shadow_learning.

        ``rec`` accumulates observable effects; ``seams`` overrides any async seam
        (save / route / redirect / wait / restore / stop_route / clear) so a test
        can inject a cancel or a rendezvous at an exact point.
        """

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)

        async def d_route(**kwargs):
            rec["route"].append(kwargs.get("owner_id"))

        async def d_redirect(endpoint, *, apply_changes=True):
            rec["redirect"].append((endpoint, apply_changes))
            return {"readback_endpoint": endpoint}

        async def d_stop_route(**kwargs):
            rec["stop_route"].append(kwargs.get("owner_id"))

        async def d_disconnect(*, reason):
            rec["disconnect"].append(reason)

        async def d_preflight(**kwargs):
            return None

        async def d_save(state):
            rec["saved"].append(state)
            rec["present"] = True

        async def d_active_proxy(*, require_process=True):
            return None

        async def d_wait(**kwargs):
            return None

        async def d_restore(_endpoint):
            return True, ""

        async def d_clear():
            rec["clear"].append(True)
            rec["present"] = False

        async def d_refresh():
            rec["refresh"].append(True)

        async def d_endpoint_context():
            return types.SimpleNamespace(
                current_endpoint="eu.smartess.io,18899,TCP",
                upstream_endpoint="eu.smartess.io,18899,TCP",
                target_endpoint="192.168.1.50,18899,TCP",
            )

        pick = lambda key, default: seams.get(key, default)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-cancel",
            data={"collector_kind": "factory_eybond"},
            options={"proxy_capture_duration_minutes": 10},
        )
        coordinator.data = self.RuntimeSnapshot(
            connected=False,
            values={"collector_server_endpoint": "eu.smartess.io,18899,TCP"},
        )
        coordinator._runtime = types.SimpleNamespace(
            proxy_capture_route_running=lambda: False,
            async_start_shadow_learning_route=pick("route", d_route),
            async_set_collector_server_endpoint=pick("redirect", d_redirect),
            async_stop_shadow_learning_route=pick("stop_route", d_stop_route),
            async_disconnect_collector_connections=pick("disconnect", d_disconnect),
        )
        coordinator._shadow_learning_process_running = lambda: False
        coordinator._async_preflight_proxy_capture_network = pick("preflight", d_preflight)
        coordinator._async_active_proxy_capture_state = d_active_proxy
        coordinator._async_save_shadow_learning_session_state = pick("save", d_save)
        coordinator._async_wait_for_shadow_learning_ready = pick("wait", d_wait)
        coordinator._async_best_effort_restore_after_start_failure = pick("restore", d_restore)
        coordinator._async_clear_shadow_learning_session_state = pick("clear", d_clear)
        coordinator._async_prepare_cloud_tool_endpoint_context = pick(
            "endpoint_context", d_endpoint_context
        )
        coordinator.async_request_refresh = d_refresh
        coordinator._publish_tooling_values = lambda **kwargs: rec["published"].append(dict(kwargs))
        coordinator._proxy_capture_collector_ip = lambda: "192.168.1.55"

        prop = lambda name, value: patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            name,
            new_callable=PropertyMock,
            return_value=value,
        )
        patchers = [
            prop("support_acquisition_readiness", self._support_readiness()),
            prop("smartess_collector_pn", "E5000020000000"),
            prop("collector_cloud_tools_allowed", True),
            prop("collector_actions_enabled", True),
            prop("collector_callback_target_endpoint", "192.168.1.50,18899,TCP"),
            prop("proxy_capture_target_endpoint", "192.168.1.50,18899,TCP"),
            prop("proxy_capture_upstream_endpoint", "eu.smartess.io,18899,TCP"),
            prop("collector_cloud_profile_key", "smartess-default"),
            prop("collector_cloud_profile_label", "SmartESS Default"),
            prop("collector_cloud_profile_source", "runtime"),
            prop("collector_cloud_profile_confidence", "high"),
            prop("effective_metadata_snapshot", {}),
            prop(
                "shadow_learning_effective_metadata",
                {"register_schema_name": "modbus_smg/base.json"},
            ),
            prop("collector_cloud_family", "smartess_at"),
            prop("_effective_callback_server_host", "192.168.1.50"),
            patch.object(
                self.coordinator_cloud_tools_module,
                "build_shadow_learning_seed",
                return_value=(types.SimpleNamespace(write_response_mode="exception"), []),
            ),
            patch.object(
                self.coordinator_cloud_tools_module,
                "build_shadow_learning_preflight",
                return_value=types.SimpleNamespace(can_start=True, blockers=[]),
            ),
        ]
        with contextlib.ExitStack() as stack:
            for patcher in patchers:
                stack.enter_context(patcher)
            yield coordinator

    @staticmethod
    def _fresh_rec() -> dict:
        return {
            "route": [],
            "redirect": [],
            "disconnect": [],
            "stop_route": [],
            "saved": [],
            "clear": [],
            "refresh": [],
            "published": [],
            "present": False,
        }

    def _authority(self):
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
        )

        return AUTH

    def _assert_state_token_consistent(self, rec: dict, entry_id: str) -> None:
        """The core invariant: a persisted session and a held token move together.

        Forbidden pairs: (record present AND authority free) and (record absent AND
        authority held). Either the mode owns both, or it owns neither.
        """

        held = self._authority().is_held(entry_id)
        self.assertFalse(
            rec["present"] and not held,
            "persisted session left with a FREE authority",
        )
        self.assertFalse(
            (not rec["present"]) and held,
            "authority held with NO recoverable session",
        )

    def test_shadow_start_cancel_during_first_persistence_never_restores_endpoint(self) -> None:
        # A cancel during the first persistence is still BEFORE any endpoint wire
        # mutation. Cleanup clears the tentative record and releases ownership;
        # issuing a speculative restore here could reboot/disconnect the collector.
        async def _run() -> None:
            rec = self._fresh_rec()
            calls = {"save": 0}

            async def _save(state):
                rec["saved"].append(state)
                rec["present"] = True
                calls["save"] += 1
                if calls["save"] == 1:
                    raise asyncio.CancelledError()

            async def _restore(_endpoint):
                raise AssertionError("endpoint restore must not run before endpoint mutation")

            with self._shadow_start_env(rec, save=_save, restore=_restore) as coord:
                with self.assertRaises(asyncio.CancelledError):
                    await coord.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow-cancel-persist.jsonl"),
                        raw_capture={},
                    )
            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertTrue(rec["clear"])
            self.assertFalse(rec["present"])
            self.assertFalse(self._authority().is_held("entry-cancel"))

        asyncio.run(_run())

    def test_shadow_start_cancel_during_route_start_stops_exact_route(self) -> None:
        # Blocker 5: a route-start await that is cancelled AFTER creating the route
        # (before route_started could be set) must still be stopped by its EXACT
        # owner id in the finalization.
        async def _run() -> None:
            rec = self._fresh_rec()

            async def _route(**kwargs):
                rec["route"].append(kwargs.get("owner_id"))  # the route now exists
                raise asyncio.CancelledError()

            async def _restore(_endpoint):
                raise AssertionError("endpoint restore must not run before endpoint mutation")

            with self._shadow_start_env(rec, route=_route, restore=_restore) as coord:
                with self.assertRaises(asyncio.CancelledError):
                    await coord.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow-cancel-route.jsonl"),
                        raw_capture={},
                    )
            owner = rec["route"][0]
            self.assertTrue(owner and owner.startswith("shadow_learning:"))
            self.assertEqual(
                rec["stop_route"],
                [owner],
                "the exact route owner must be stopped even when route_started was never set",
            )
            self.assertEqual(
                rec["redirect"],
                [],
                "a cancelled route start must not reach the endpoint write",
            )
            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertFalse(self._authority().is_held("entry-cancel"))

        asyncio.run(_run())

    def test_shadow_start_redirects_cloud_route_and_persists_restore_contract(self) -> None:
        async def _run() -> None:
            rec = self._fresh_rec()
            with self._shadow_start_env(rec) as coord:
                result = await coord.async_start_shadow_learning(
                    output_path=Path("/tmp/shadow-cloud-route.jsonl"),
                    raw_capture={},
                )

            self.assertEqual(
                rec["redirect"],
                [("192.168.1.50,18899,TCP", True)],
            )
            self.assertEqual(rec["disconnect"], ["shadow_learning_start"])
            self.assertTrue(result["restore_required"])
            self.assertEqual(rec["saved"][-1].status, "ready")
            self.assertEqual(
                rec["saved"][-1].original_endpoint,
                "eu.smartess.io,18899,TCP",
            )
            self.assertEqual(
                rec["saved"][-1].proxy_endpoint,
                "192.168.1.50,18899,TCP",
            )
            authority = self._authority()
            token = authority.adopt(
                "entry-cancel",
                "shadow_learning",
                str(result["session_id"]),
            )
            self.assertIsNotNone(token)
            self.assertTrue(authority.release("entry-cancel", token))

        asyncio.run(_run())

    def test_proxy_start_disconnects_runtime_after_redirect_before_wait(self) -> None:
        async def _run() -> None:
            rec = self._fresh_rec()
            order: list[str] = []

            async def _redirect(endpoint, *, apply_changes=True):
                order.append("redirect")
                rec["redirect"].append((endpoint, apply_changes))
                return {"readback_endpoint": endpoint}

            async def _disconnect(*, reason):
                order.append("disconnect")
                rec["disconnect"].append(reason)

            async def _wait(*_args, **_kwargs):
                order.append("wait")

            with self._proxy_start_env(
                rec,
                redirect=_redirect,
                disconnect=_disconnect,
                wait=_wait,
            ) as coord:
                result = await coord.async_start_proxy_capture(confirm_redirect=True)

            self.assertEqual(order, ["redirect", "disconnect", "wait"])
            self.assertEqual(rec["disconnect"], ["proxy_capture_start"])
            self.assertEqual(result["status"], "running")

            authority = self._authority()
            owner_ref = str(rec["saved"][-1].route_owner_id)
            token = authority.adopt("entry-cancel", "proxy_capture", owner_ref)
            self.assertIsNotNone(token)
            self.assertTrue(authority.release("entry-cancel", token))

        asyncio.run(_run())

    def test_proxy_start_routes_new_cloud_session_transparently(self) -> None:
        async def _run() -> None:
            rec = self._fresh_rec()
            route_kwargs: list[dict[str, object]] = []

            async def _route(**kwargs):
                route_kwargs.append(dict(kwargs))
                rec["route"].append(kwargs.get("owner_id"))

            with self._proxy_start_env(
                rec,
                route=_route,
                collector_session_protocol="eybond_framed",
            ) as coord:
                result = await coord.async_start_proxy_capture(
                    confirm_redirect=True
                )

            self.assertEqual(result["status"], "running")
            self.assertEqual(
                route_kwargs[0]["proxy_wire_mode"],
                "transparent",
            )
            self.assertEqual(
                route_kwargs[0]["expected_session_protocol"],
                "at_text",
            )
            self.assertNotIn("bridge_context", route_kwargs[0])
            self.assertEqual(
                rec["saved"][0].proxy_wire_mode,
                "transparent",
            )
            self.assertEqual(
                rec["saved"][-1].proxy_wire_mode,
                "transparent",
            )

            authority = self._authority()
            token = authority.adopt(
                "entry-cancel",
                "proxy_capture",
                str(rec["saved"][-1].route_owner_id),
            )
            self.assertIsNotNone(token)
            self.assertTrue(authority.release("entry-cancel", token))

        asyncio.run(_run())

    def test_shadow_start_failed_restore_keeps_state_and_authority(self) -> None:
        async def _run() -> None:
            rec = self._fresh_rec()

            async def _redirect(_endpoint, *, apply_changes=True):
                raise asyncio.CancelledError()

            async def _restore(_endpoint):
                return False, "restore_write_timeout"

            with self._shadow_start_env(
                rec,
                redirect=_redirect,
                restore=_restore,
            ) as coord:
                with self.assertRaises(asyncio.CancelledError):
                    await coord.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow-restore-failed.jsonl"),
                        raw_capture={},
                    )

            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertTrue(rec["present"])
            self.assertFalse(rec["clear"])
            self.assertTrue(self._authority().is_held("entry-cancel"))
            self.assertEqual(rec["saved"][-1].status, "restore_failed")
            self.assertEqual(
                rec["saved"][-1].last_restore_error,
                "restore_write_timeout",
            )

        asyncio.run(_run())

    def test_shadow_start_two_cancels_finalization_runs_to_completion(self) -> None:
        # Blocker 2: the first cancel enters the shielded finalization; a second
        # cancel arrives while it is blocked; the finalization still runs to the
        # end once the rendezvous releases; the caller then receives CancelledError
        # and the state/token pair is consistent.
        async def _run() -> None:
            rec = self._fresh_rec()
            reached_route = asyncio.Event()
            route_gate = asyncio.Event()
            reached_cleanup = asyncio.Event()
            cleanup_gate = asyncio.Event()

            async def _route(**kwargs):
                rec["route"].append(kwargs.get("owner_id"))
                reached_route.set()
                await route_gate.wait()  # cancel #1 lands here

            async def _stop_route(**kwargs):
                rec["stop_route"].append(kwargs.get("owner_id"))
                reached_cleanup.set()
                await cleanup_gate.wait()  # cancel #2 arrives while blocked here

            async def _restore(_endpoint):
                return True, ""

            with self._shadow_start_env(
                rec, route=_route, stop_route=_stop_route, restore=_restore
            ) as coord:
                task = asyncio.ensure_future(
                    coord.async_start_shadow_learning(
                        output_path=Path("/tmp/shadow-two-cancel.jsonl"),
                        raw_capture={},
                    )
                )
                await asyncio.wait_for(reached_route.wait(), 2.0)
                task.cancel()  # #1 -> finalization begins
                await asyncio.wait_for(reached_cleanup.wait(), 2.0)
                task.cancel()  # #2 -> absorbed by the shield; cleanup keeps running
                cleanup_gate.set()  # release the rendezvous -> cleanup completes
                with self.assertRaises(asyncio.CancelledError):
                    await task

            self.assertEqual(rec["stop_route"], [rec["route"][0]])
            self.assertTrue(rec["clear"])
            self.assertFalse(rec["present"])
            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertFalse(self._authority().is_held("entry-cancel"))

        asyncio.run(_run())

    def test_shadow_stop_atomic_clear_and_release_survives_cancel(self) -> None:
        # Blocker 3: in a stop success path the clear+release is ONE shielded
        # critical section. Even if clear removes the record then blocks and the
        # task is cancelled, after the boundary the record is absent AND the
        # authority is free (never absent + held), and the caller gets CancelledError.
        async def _run() -> None:
            AUTH = self._authority()
            entry_id = "entry-stop-clear"
            rec = self._fresh_rec()
            rec["present"] = True
            reached_clear = asyncio.Event()
            clear_gate = asyncio.Event()

            state = types.SimpleNamespace(
                entry_id=entry_id,
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
                route_owner_id=f"shadow_learning:{entry_id}:1",
            )
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)

            async def _stop_route(**kwargs):
                return None

            coordinator._runtime = types.SimpleNamespace(
                async_stop_shadow_learning_route=_stop_route
            )

            async def _active(*, require_process: bool = True):
                return state

            async def _save(new_state):
                rec["saved"].append(new_state)
                rec["present"] = True

            async def _restore(_endpoint):
                return "eu.smartess.io,18899,TCP"

            async def _verify(endpoint):
                return {
                    "restore_confirmed": True,
                    "observed_endpoint": endpoint,
                    "restore_error": "",
                }

            async def _clear():
                rec["present"] = False  # record actually removed
                reached_clear.set()
                await clear_gate.wait()  # blocks BEFORE returning
                rec["clear"].append(True)

            async def _refresh():
                rec["refresh"].append(True)

            coordinator._async_active_shadow_learning_state = _active
            coordinator._async_save_shadow_learning_session_state = _save
            coordinator._async_restore_proxy_capture_endpoint = _restore
            coordinator._async_verify_restored_collector_endpoint = _verify
            coordinator._async_clear_shadow_learning_session_state = _clear
            coordinator.async_request_refresh = _refresh
            coordinator._publish_tooling_values = lambda **kwargs: None
            coordinator._notify_proxy_capture_restore_unconfirmed = lambda: None

            task = asyncio.ensure_future(coordinator.async_stop_shadow_learning())
            await asyncio.wait_for(reached_clear.wait(), 2.0)
            # The record is already gone; the authority is still held until release.
            self.assertFalse(rec["present"])
            task.cancel()
            clear_gate.set()  # let the shielded clear+release finish
            with self.assertRaises(asyncio.CancelledError):
                await task

            # After the critical boundary: record absent AND authority free.
            self.assertFalse(rec["present"])
            self.assertFalse(AUTH.is_held(entry_id))
            self.assertTrue(rec["clear"])

        asyncio.run(_run())

    def test_proxy_stop_atomic_clear_and_release_survives_cancel(self) -> None:
        """The proxy stop has its own production branch and must be equally atomic."""

        async def _run() -> None:
            AUTH = self._authority()
            entry_id = "entry-proxy-stop-clear"
            rec = self._fresh_rec()
            rec["present"] = True
            reached_clear = asyncio.Event()
            clear_gate = asyncio.Event()
            tmp_dir = tempfile.mkdtemp(prefix="cp2c-proxy-stop-")
            trace_path = Path(tmp_dir) / "proxy.jsonl"
            state = types.SimpleNamespace(
                entry_id=entry_id,
                route_owner_id=f"proxy_capture:{entry_id}:1",
                collector_pn="E5000020000000",
                trace_path=str(trace_path),
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                restore_required=True,
                anonymized=False,
                started_at="2026-06-05T12:00:00+00:00",
                expires_at="2026-06-05T12:20:00+00:00",
                status="running",
            )
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id=entry_id)

            async def _executor(func, *args):
                return func(*args)

            coordinator.hass = types.SimpleNamespace(
                config=types.SimpleNamespace(config_dir=tmp_dir),
                async_add_executor_job=_executor,
            )

            async def _active(*, require_process: bool = True):
                return state

            async def _save(new_state):
                rec["saved"].append(new_state)
                rec["present"] = True

            async def _restore(**kwargs):
                return {
                    "restored_endpoint": state.original_endpoint,
                    "restore_confirmed": True,
                    "restore_mode": "direct",
                    "restore_skipped_reason": "",
                    "current_endpoint": state.original_endpoint,
                }

            async def _clear():
                rec["present"] = False
                reached_clear.set()
                await clear_gate.wait()
                rec["clear"].append(True)

            coordinator._async_active_proxy_capture_state = _active
            coordinator._async_save_proxy_capture_session_state = _save
            coordinator._async_guarded_proxy_capture_restore = _restore
            coordinator._async_clear_proxy_capture_session_state = _clear
            coordinator._proxy_capture_result_status = lambda *a, **k: "stopped"
            coordinator._proxy_capture_local_status = lambda *a, **k: "stopped"
            coordinator._proxy_capture_overview_runtime_values = lambda **kwargs: {}
            coordinator._publish_tooling_values = lambda **kwargs: None
            coordinator.async_request_refresh = lambda: asyncio.sleep(0)

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "smartess_collector_pn",
                new_callable=PropertyMock,
                return_value="E5000020000000",
            ), patch.object(
                self.coordinator_cloud_tools_module,
                "build_proxy_capture_session_state",
                lambda *a, **k: types.SimpleNamespace(**k),
            ), patch.object(
                self.coordinator_cloud_tools_module,
                "summarize_proxy_capture_trace",
                return_value={},
            ), patch.object(
                self.coordinator_cloud_tools_module,
                "export_proxy_trace_manifest",
                return_value=Path(tmp_dir) / "manifest.json",
            ), patch.object(
                self.coordinator_cloud_tools_module,
                "export_proxy_trace_bundle",
                return_value=Path(tmp_dir) / "bundle.zip",
            ), patch.object(
                self.coordinator_cloud_tools_module,
                "sign_proxy_capture_download_url",
                return_value=(
                    "/api/eybond_local/proxy_capture/entry-id/bundle.zip"
                    "?authSig=signed"
                ),
            ):
                task = asyncio.ensure_future(coordinator.async_stop_proxy_capture())
                await asyncio.wait_for(reached_clear.wait(), 2.0)
                self.assertFalse(rec["present"])
                task.cancel()
                clear_gate.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            self.assertFalse(rec["present"])
            self.assertFalse(AUTH.is_held(entry_id))
            self.assertTrue(rec["clear"])

        asyncio.run(_run())

    @contextlib.contextmanager
    def _proxy_start_env(self, rec, **seams):
        """Bare-coordinator harness that drives the REAL async_start_proxy_capture."""

        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        tmp_dir = tempfile.mkdtemp(prefix="cp2c-proxy-")

        async def _executor(func, *args):
            return func(*args)

        async def d_route(**kwargs):
            rec["route"].append(kwargs.get("owner_id"))

        async def d_redirect(endpoint, *, apply_changes=True):
            rec["redirect"].append((endpoint, apply_changes))
            return {"readback_endpoint": endpoint}

        async def d_stop_process(*, owner_id="", force=False):
            rec["stop_route"].append(owner_id)

        async def d_disconnect(*, reason):
            rec["disconnect"].append(reason)

        async def d_preflight(**kwargs):
            return None

        async def d_save(state):
            rec["saved"].append(state)
            rec["present"] = True

        async def d_active_shadow(*, require_process=True):
            return None

        async def d_wait(*args, **kwargs):
            return None

        async def d_restore(_endpoint):
            return True, ""

        async def d_clear():
            rec["clear"].append(True)
            rec["present"] = False

        async def d_refresh():
            rec["refresh"].append(True)

        async def d_endpoint_context():
            return types.SimpleNamespace(
                current_endpoint=overview.current_endpoint,
                upstream_endpoint="eu.smartess.io,18899,TCP",
                target_endpoint=overview.target_endpoint,
            )

        pick = lambda key, default: seams.get(key, default)
        overview = types.SimpleNamespace(
            can_start=True,
            blocking_reason=None,
            redirect_required=seams.get("redirect_required", True),
            target_endpoint="192.168.1.50,18899,TCP",
            current_endpoint="eu.smartess.io,18899,TCP",
            masked_endpoint="masked.example,18899,TCP",
        )
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-cancel", data={}, options={}
        )
        coordinator.hass = types.SimpleNamespace(
            config=types.SimpleNamespace(config_dir=tmp_dir),
            async_add_executor_job=_executor,
        )
        coordinator._runtime = types.SimpleNamespace(
            async_start_proxy_capture_route=pick("route", d_route),
            async_set_collector_server_endpoint=pick("redirect", d_redirect),
            async_disconnect_collector_connections=pick("disconnect", d_disconnect),
        )
        coordinator.collector_endpoint_sync_lock_code = lambda: None
        coordinator._async_active_shadow_learning_state = d_active_shadow
        coordinator._shadow_learning_process_running = lambda: False
        coordinator._proxy_capture_process_running = lambda: False
        coordinator._async_save_proxy_capture_session_state = pick("save", d_save)
        coordinator._async_preflight_proxy_capture_network = pick("preflight", d_preflight)
        coordinator._proxy_capture_collector_ip = lambda: "192.168.1.55"
        coordinator._async_wait_for_proxy_capture_reconnect = pick("wait", d_wait)
        coordinator._async_best_effort_restore_after_start_failure = pick("restore", d_restore)
        coordinator._async_stop_proxy_capture_process = pick("stop_route", d_stop_process)
        coordinator._async_clear_proxy_capture_session_state = pick("clear", d_clear)
        coordinator._async_prepare_cloud_tool_endpoint_context = pick(
            "endpoint_context", d_endpoint_context
        )
        coordinator._proxy_capture_overview_for_live_context = lambda _context: overview
        coordinator.async_request_refresh = d_refresh
        coordinator._publish_tooling_values = lambda **kwargs: rec["published"].append(dict(kwargs))
        coordinator._proxy_capture_overview_runtime_values = lambda **kwargs: {}

        prop = lambda name, value: patch.object(
            self.coordinator_module.EybondLocalCoordinator,
            name,
            new_callable=PropertyMock,
            return_value=value,
        )
        patchers = [
            prop("support_acquisition_readiness", self._support_readiness()),
            prop("proxy_capture_overview", overview),
            prop("smartess_collector_pn", "E5000020000000"),
            prop("collector_cloud_tools_allowed", True),
            prop(
                "collector_capabilities",
                types.SimpleNamespace(proxy_capture=True),
            ),
            prop("collector_actions_enabled", True),
            prop("proxy_capture_upstream_endpoint", "eu.smartess.io,18899,TCP"),
            prop("collector_cloud_family", "smartess"),
            prop(
                "collector_session_protocol",
                seams.get("collector_session_protocol", "at_text"),
            ),
            prop("proxy_capture_configured_duration_minutes", 10),
            # The stub harness returns None for these path builders; give the real
            # method usable paths (nothing is written -- the route seam is stubbed).
            patch.object(
                self.coordinator_cloud_tools_module,
                "build_proxy_capture_trace_path",
                lambda *a, **k: Path(tmp_dir) / "proxy-trace.jsonl",
            ),
            patch.object(
                self.coordinator_cloud_tools_module,
                "build_proxy_capture_restore_trigger_path",
                lambda trace_path, *a, **k: Path(str(trace_path) + ".restore"),
            ),
            patch.object(
                self.coordinator_cloud_tools_module,
                "resolve_collector_cloud_session_protocol",
                lambda _family: "at_text",
            ),
            # The stub harness returns None for the session-state builder; give the
            # real method a namespace that carries route_owner_id/status/etc.
            patch.object(
                self.coordinator_cloud_tools_module,
                "build_proxy_capture_session_state",
                lambda *a, **k: types.SimpleNamespace(**k),
            ),
        ]
        with contextlib.ExitStack() as stack:
            for patcher in patchers:
                stack.enter_context(patcher)
            yield coordinator

    def test_proxy_start_cancel_during_first_persistence_never_restores_endpoint(self) -> None:
        # Same pre-wire boundary for proxy capture: tentative persistence may be
        # cleared, but the collector endpoint must not be rewritten.
        async def _run() -> None:
            rec = self._fresh_rec()
            calls = {"save": 0}

            async def _save(state):
                rec["saved"].append(state)
                rec["present"] = True
                calls["save"] += 1
                if calls["save"] == 1:
                    raise asyncio.CancelledError()

            async def _restore(_endpoint):
                raise AssertionError("endpoint restore must not run before endpoint mutation")

            with self._proxy_start_env(rec, save=_save, restore=_restore) as coord:
                with self.assertRaises(asyncio.CancelledError):
                    await coord.async_start_proxy_capture(confirm_redirect=True)
            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertTrue(rec["clear"])
            self.assertFalse(self._authority().is_held("entry-cancel"))

        asyncio.run(_run())

    def test_proxy_start_cancel_during_endpoint_write_failed_restore_is_recoverable(self) -> None:
        # After the endpoint-write await begins, a failed restore must retain the
        # recoverable record and authority because the wire result is uncertain.
        async def _run() -> None:
            rec = self._fresh_rec()
            async def _save(state):
                rec["saved"].append(state)
                rec["present"] = True

            async def _redirect(_endpoint, *, apply_changes=True):
                raise asyncio.CancelledError()

            async def _restore(_endpoint):
                return False, "restore_write_timeout"

            with self._proxy_start_env(
                rec, save=_save, redirect=_redirect, restore=_restore
            ) as coord:
                with self.assertRaises(asyncio.CancelledError):
                    await coord.async_start_proxy_capture(confirm_redirect=True)
            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertTrue(rec["present"])
            self.assertFalse(rec["clear"])
            self.assertTrue(self._authority().is_held("entry-cancel"))
            self.assertEqual(rec["saved"][-1].status, "restoring")

        asyncio.run(_run())

    def test_proxy_start_cancel_during_route_start_stops_exact_route(self) -> None:
        # Blocker 5 for proxy: a cancelled route-start (route created, route_started
        # never set) is still stopped by its EXACT owner id in the finalization.
        async def _run() -> None:
            rec = self._fresh_rec()

            async def _route(**kwargs):
                rec["route"].append(kwargs.get("owner_id"))
                raise asyncio.CancelledError()

            async def _restore(_endpoint):
                raise AssertionError("endpoint restore must not run before endpoint mutation")

            with self._proxy_start_env(rec, route=_route, restore=_restore) as coord:
                with self.assertRaises(asyncio.CancelledError):
                    await coord.async_start_proxy_capture(confirm_redirect=True)
            owner = rec["route"][0]
            self.assertTrue(owner and owner.startswith("proxy_capture:"))
            self.assertEqual(rec["stop_route"], [owner])
            self._assert_state_token_consistent(rec, "entry-cancel")
            self.assertFalse(self._authority().is_held("entry-cancel"))

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

    def test_start_failure_restore_ack_without_live_postcondition_is_unconfirmed(
        self,
    ) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            notifications: list[bool] = []

            coordinator._async_restore_proxy_capture_endpoint = (
                lambda endpoint: asyncio.sleep(0, result=endpoint)
            )
            coordinator._async_verify_restored_collector_endpoint = (
                lambda endpoint: asyncio.sleep(
                    0,
                    result={
                        "restore_confirmed": False,
                        "observed_endpoint": "",
                        "restore_error": "restore_live_endpoint_unavailable",
                    },
                )
            )
            coordinator._notify_proxy_capture_restore_unconfirmed = (
                lambda: notifications.append(True)
            )

            confirmed, reason = (
                await coordinator._async_best_effort_restore_after_start_failure(
                    "eu.smartess.io,18899,TCP"
                )
            )

            self.assertFalse(confirmed)
            self.assertEqual(reason, "restore_live_endpoint_unavailable")
            self.assertEqual(notifications, [True])

        asyncio.run(_run())

    def test_restore_verification_requires_live_matching_endpoint(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            calls: list[float] = []

            async def _endpoint_state(
                *,
                timeout: float = 0.0,
                require_heartbeat: bool = True,
            ):
                calls.append(timeout)
                self.assertFalse(require_heartbeat)
                return {
                    "current_endpoint": "eu.smartess.io,18899,TCP",
                }

            coordinator._runtime = types.SimpleNamespace(
                async_get_collector_server_endpoint_state=_endpoint_state
            )
            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_family",
                new_callable=PropertyMock,
                return_value="",
            ):
                result = (
                    await coordinator._async_verify_restored_collector_endpoint(
                        "eu.smartess.io,18899,TCP"
                    )
                )

            self.assertTrue(result["restore_confirmed"])
            self.assertEqual(
                calls,
                [
                    self.coordinator_module.DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait
                ],
            )

        asyncio.run(_run())

    def test_restore_verification_fails_closed_on_mismatch_or_timeout(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")

            async def _mismatch(
                *,
                timeout: float = 0.0,
                require_heartbeat: bool = True,
            ):
                self.assertFalse(require_heartbeat)
                return {
                    "current_endpoint": "192.168.1.50,18899,TCP",
                }

            coordinator._runtime = types.SimpleNamespace(
                async_get_collector_server_endpoint_state=_mismatch
            )
            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "collector_cloud_family",
                new_callable=PropertyMock,
                return_value="",
            ):
                mismatch = (
                    await coordinator._async_verify_restored_collector_endpoint(
                        "eu.smartess.io,18899,TCP"
                    )
                )
                self.assertFalse(mismatch["restore_confirmed"])
                self.assertEqual(
                    mismatch["restore_error"],
                    "restore_live_endpoint_mismatch",
                )

                async def _timeout(
                    *,
                    timeout: float = 0.0,
                    require_heartbeat: bool = True,
                ):
                    self.assertFalse(require_heartbeat)
                    raise TimeoutError("collector_not_connected")

                coordinator._runtime = types.SimpleNamespace(
                    async_get_collector_server_endpoint_state=_timeout
                )
                timed_out = (
                    await coordinator._async_verify_restored_collector_endpoint(
                        "eu.smartess.io,18899,TCP"
                    )
                )
                self.assertFalse(timed_out["restore_confirmed"])
                self.assertIn(
                    "collector_not_connected",
                    timed_out["restore_error"],
                )

        asyncio.run(_run())

    def test_proxy_restore_ack_without_live_read_falls_back_to_direct_restore(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            state = types.SimpleNamespace(
                trace_path="/tmp/proxy.jsonl",
                route_owner_id="proxy_capture:entry-id:1",
                restore_required=True,
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
            )
            coordinator._async_read_live_collector_server_endpoint = (
                lambda: asyncio.sleep(
                    0,
                    result="192.168.1.50,18899,TCP",
                )
            )
            coordinator._proxy_capture_process_running = lambda: True
            coordinator._async_trigger_proxy_capture_restore = (
                lambda **kwargs: asyncio.sleep(0, result=True)
            )
            coordinator._async_verify_restored_collector_endpoint = (
                AsyncMock(
                    side_effect=(
                        {
                            "restore_confirmed": False,
                            "observed_endpoint": "",
                            "restore_error": "restore_live_endpoint_unavailable",
                        },
                        {
                            "restore_confirmed": True,
                            "observed_endpoint": "eu.smartess.io,18899,TCP",
                            "restore_error": "",
                        },
                    )
                )
            )
            stop_calls: list[str] = []
            coordinator._async_stop_proxy_capture_process = (
                lambda *, owner_id: asyncio.sleep(
                    0,
                    result=stop_calls.append(owner_id),
                )
            )
            direct_calls: list[str] = []
            coordinator._async_restore_proxy_capture_endpoint = (
                lambda endpoint: asyncio.sleep(
                    0,
                    result=(
                        direct_calls.append(endpoint)
                        or endpoint
                    ),
                )
            )

            result = await coordinator._async_guarded_proxy_capture_restore(
                state=state,
                prefer_proxy_restore_trigger=True,
            )

            self.assertTrue(result["restore_confirmed"])
            self.assertEqual(result["restore_mode"], "proxy_trigger_then_direct")
            self.assertEqual(direct_calls, ["eu.smartess.io,18899,TCP"])
            self.assertEqual(
                stop_calls,
                ["proxy_capture:entry-id:1"],
            )

        asyncio.run(_run())

    def test_unavailable_current_endpoint_runs_owned_direct_restore(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            state = types.SimpleNamespace(
                trace_path="/tmp/proxy.jsonl",
                route_owner_id="proxy_capture:entry-id:2",
                restore_required=True,
                original_endpoint="dtu_ess.eybond.com,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
            )
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-id")
            coordinator._async_read_live_collector_server_endpoint = (
                lambda: asyncio.sleep(0, result="")
            )
            coordinator._proxy_capture_process_running = lambda: False
            stop_calls: list[str] = []
            coordinator._async_stop_proxy_capture_process = (
                lambda *, owner_id: asyncio.sleep(
                    0,
                    result=stop_calls.append(owner_id),
                )
            )
            direct_calls: list[str] = []
            coordinator._async_restore_proxy_capture_endpoint = (
                lambda endpoint: asyncio.sleep(
                    0,
                    result=(
                        direct_calls.append(endpoint)
                        or endpoint
                    ),
                )
            )
            coordinator._async_verify_restored_collector_endpoint = (
                lambda endpoint: asyncio.sleep(
                    0,
                    result={
                        "restore_confirmed": True,
                        "observed_endpoint": endpoint,
                        "restore_error": "",
                    },
                )
            )

            with patch.object(
                self.coordinator_cloud_tools_module,
                "proxy_capture_restore_guard_reason",
                return_value="current_endpoint_unavailable",
            ):
                result = await coordinator._async_guarded_proxy_capture_restore(
                    state=state,
                    prefer_proxy_restore_trigger=True,
                )

            self.assertTrue(result["restore_confirmed"])
            self.assertEqual(result["restore_mode"], "direct")
            self.assertEqual(
                direct_calls,
                ["dtu_ess.eybond.com,18899,TCP"],
            )
            self.assertEqual(
                stop_calls,
                ["proxy_capture:entry-id:2"],
            )

        asyncio.run(_run())

    def test_endpoint_terminalization_is_serialized_across_same_owner_calls(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(
                self.coordinator_module.EybondLocalCoordinator
            )
            entered = asyncio.Event()
            release = asyncio.Event()
            active = 0
            max_active = 0

            async def _stop_once(**kwargs):
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                entered.set()
                await release.wait()
                active -= 1
                return {"status": "stopped"}

            coordinator._async_stop_proxy_capture_once = _stop_once
            first = asyncio.create_task(coordinator.async_stop_proxy_capture())
            await entered.wait()
            second = asyncio.create_task(coordinator.async_stop_proxy_capture())
            await asyncio.sleep(0)
            self.assertEqual(max_active, 1)
            release.set()
            await asyncio.gather(first, second)
            self.assertEqual(max_active, 1)

        asyncio.run(_run())

    def test_restore_proxy_capture_endpoint_bypasses_transition_lock(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            calls: list[tuple[str, bool, float, bool]] = []
            disconnect_reasons: list[str] = []

            async def _async_set_collector_server_endpoint(
                endpoint: str,
                *,
                apply_changes: bool = True,
                timeout: float = 0.0,
                require_heartbeat: bool = True,
            ):
                calls.append(
                    (endpoint, apply_changes, timeout, require_heartbeat)
                )
                return {"readback_endpoint": endpoint}

            async def _async_disconnect_collector_connections(*, reason: str):
                disconnect_reasons.append(reason)

            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                async_disconnect_collector_connections=(
                    _async_disconnect_collector_connections
                ),
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
            self.assertEqual(
                calls,
                [
                    (
                        "ess.eybond.com",
                        True,
                        self.coordinator_module.DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait,
                        False,
                    )
                ],
            )
            self.assertEqual(
                disconnect_reasons,
                ["collector_endpoint_restore"],
            )

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

    def test_publish_snapshot_endpoint_keeps_collector_and_legacy_in_sync(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        collector = types.SimpleNamespace(
            collector_server_endpoint="old.example,18899,TCP"
        )
        coordinator.data = self.RuntimeSnapshot(
            collector=collector,
            values={"collector_server_endpoint": "old.example,18899,TCP"},
        )
        published: list[object] = []
        coordinator.async_set_updated_data = published.append

        coordinator._publish_snapshot_values(
            collector_server_endpoint="new.example,18899,TCP"
        )

        self.assertEqual(
            collector.collector_server_endpoint,
            "new.example,18899,TCP",
        )
        self.assertEqual(
            coordinator.data.values["collector_server_endpoint"],
            "new.example,18899,TCP",
        )
        self.assertEqual(published, [coordinator.data])

    def test_prime_startup_snapshot_publishes_detection_pending_collector_state(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "connection_type": "eybond",
                "connection_mode": "manual",
                "collector_operation_mode": "home_assistant_only",
                "control_mode": "read_only",
                "detection_confidence": "none",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._connection_spec = types.SimpleNamespace(
            collector_ip="192.168.1.51",
            collector_pn="V0000000000001",
            collector_cloud_family="smartess_at",
            server_ip="192.168.1.50",
            tcp_port=18899,
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=18899,
        )

        primed = coordinator.prime_startup_snapshot()

        self.assertTrue(primed)
        self.assertTrue(coordinator.data.connected)
        self.assertEqual(coordinator.data.collector.remote_ip, "192.168.1.51")
        self.assertEqual(coordinator.data.values["collector_pn"], "V0000000000001")
        self.assertEqual(
            coordinator.data.collector.collector_server_endpoint,
            coordinator.data.values["collector_server_endpoint"],
        )
        self.assertEqual(coordinator.data.values["runtime_driver_state"], "driver_unbound")
        self.assertEqual(
            coordinator.data.values["runtime_detection_status"],
            "detecting_inverter",
        )
        self.assertEqual(coordinator.data.values["collector_poll_context"], "detection")
        self.assertEqual(coordinator.data.values["last_error"], "startup_detection_pending")
        collector_info = coordinator.collector_device_info()
        self.assertEqual(collector_info["model"], "EyeBond Collector")

    def test_driver_unbound_interlocks_writes_without_changing_user_mode(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        capability = types.SimpleNamespace(key="output_source_priority")
        inverter = types.SimpleNamespace(
            model_name="Bound inverter",
            serial_number="SERIAL-1",
            capabilities=(capability,),
        )
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "control_mode": "full",
                "detection_confidence": "high",
            },
            options={"control_mode": "full"},
        )
        coordinator.data = self.RuntimeSnapshot(
            connected=True,
            inverter=inverter,
            values={"runtime_driver_state": "driver_unbound"},
        )
        coordinator._write_exposure_context = lambda: {
            "variant_key": "",
            "profile_source_scope": "builtin",
            "schema_source_scope": "builtin",
            "profile_name": "",
            "device_scoped_overlay_active": False,
            "selected_control_keys": None,
        }

        self.assertEqual(coordinator.control_mode, "full")
        self.assertFalse(coordinator.controls_enabled)
        self.assertEqual(coordinator.controls_reason, "driver_unbound")
        self.assertFalse(coordinator.can_expose_capability(capability))

        coordinator.data.values["runtime_driver_state"] = "driver_bound"

        self.assertEqual(coordinator.control_mode, "full")
        self.assertTrue(coordinator.controls_enabled)
        self.assertTrue(coordinator.can_expose_capability(capability))

        coordinator.data.values["runtime_driver_state"] = "collector_offline"

        self.assertTrue(coordinator.controls_enabled)
        self.assertTrue(coordinator.can_expose_capability(capability))

    def test_prime_startup_snapshot_includes_persisted_inverter_capabilities(self) -> None:
        capability = types.SimpleNamespace(key="output_source_priority")
        group = types.SimpleNamespace(key="power_source")
        profile = types.SimpleNamespace(
            driver_key="pi30",
            protocol_family="pi30",
            groups=(group,),
            capabilities=(capability,),
            presets=(),
        )
        driver = types.SimpleNamespace(
            key="pi30",
            probe_targets=(
                self.coordinator_module.ProbeTarget(
                    devcode=0x0994,
                    collector_addr=0x01,
                    device_addr=0,
                ),
            ),
        )
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "connection_type": "eybond",
                "connection_mode": "manual",
                "collector_operation_mode": "home_assistant_only",
                "control_mode": "auto",
                "detection_confidence": "high",
                "detected_model": "PI30 3500",
                "detected_serial": "55355535553555",
                "driver_hint": "pi30",
            },
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "pi30",
                    "variant_key": "default",
                    "profile_name": "pi30_ascii/models/smartess_0925_compat.json",
                    "register_schema_name": "pi30_ascii/models/smartess_0925_compat.json",
                    "confidence": "high",
                }
            },
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._connection_spec = types.SimpleNamespace(
            collector_ip="192.168.1.51",
            collector_pn="V0000000000001",
            collector_cloud_family="smartess_at",
            server_ip="192.168.1.50",
            tcp_port=18899,
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=18899,
        )

        with (
            patch.object(self.coordinator_startup_module, "get_driver", return_value=driver),
            patch.object(
                self.coordinator_startup_module,
                "load_driver_profile",
                return_value=profile,
            ),
        ):
            primed = coordinator.prime_startup_snapshot()

        self.assertTrue(primed)
        self.assertIsNotNone(coordinator.data.inverter)
        self.assertEqual(coordinator.data.inverter.driver_key, "pi30")
        self.assertEqual(coordinator.data.inverter.model_name, "PI30 3500")
        self.assertEqual(coordinator.data.inverter.serial_number, "55355535553555")
        self.assertEqual(
            coordinator.data.inverter.profile_name,
            "pi30_ascii/models/smartess_0925_compat.json",
        )
        self.assertEqual(coordinator.data.inverter.capabilities, (capability,))
        self.assertEqual(coordinator.data.inverter.capability_groups, (group,))

    def test_prime_startup_restores_high_confidence_auto_model_from_catalog(self) -> None:
        capability = types.SimpleNamespace(key="output_source_priority")
        group = types.SimpleNamespace(key="power_source")
        profile = types.SimpleNamespace(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            groups=(group,),
            capabilities=(capability,),
            presets=(),
        )
        register_schema = types.SimpleNamespace(driver_key="modbus_smg")
        surface = types.SimpleNamespace(
            driver_key="modbus_smg",
            variant_key="anenji_anj_11kw_48v_wifi_p",
            profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        driver = types.SimpleNamespace(
            key="modbus_smg",
            probe_targets=(
                self.coordinator_module.ProbeTarget(
                    devcode=1,
                    collector_addr=255,
                    device_addr=1,
                ),
            ),
        )
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "connection_type": "eybond",
                "control_mode": "auto",
                "detected_model": "Anenji ANJ-11KW-48V-WIFI-P",
                "detected_serial": "92B32500004401",
                "detection_confidence": "high",
                "driver_hint": "auto",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._connection_spec = types.SimpleNamespace(
            collector_ip="192.0.2.11",
            collector_pn="E5000SYNTHETIC5507",
            collector_cloud_family="smartess_at",
            server_ip="192.0.2.56",
            tcp_port=8899,
            effective_advertised_server_ip="192.0.2.56",
            effective_advertised_tcp_port=8899,
        )

        with (
            patch.object(
                self.coordinator_startup_module,
                "resolve_unique_persisted_model_surface",
                return_value=(types.SimpleNamespace(), surface),
            ),
            patch.object(self.coordinator_startup_module, "get_driver", return_value=driver),
            patch.object(
                self.coordinator_startup_module,
                "load_driver_profile",
                return_value=profile,
            ),
            patch.object(
                self.coordinator_startup_module,
                "load_register_schema",
                return_value=register_schema,
            ),
        ):
            primed = coordinator.prime_startup_snapshot()

        self.assertTrue(primed)
        inverter = coordinator.data.inverter
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.driver_key, "modbus_smg")
        self.assertEqual(inverter.variant_key, "anenji_anj_11kw_48v_wifi_p")
        self.assertEqual(inverter.capabilities, (capability,))
        self.assertEqual(
            inverter.details["runtime_detection_status"],
            "persisted_model_probe_degraded",
        )
        self.assertEqual(
            inverter.details["identity_source"], "persisted_detected_model"
        )
        self.assertEqual(
            coordinator.data.values["runtime_driver_state"], "driver_bound"
        )
        self.assertEqual(
            coordinator.data.values["effective_profile_name"],
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(
            coordinator.data.values["effective_variant_key"],
            "anenji_anj_11kw_48v_wifi_p",
        )
        self.assertEqual(
            coordinator.data.values["effective_inverter_capability_count"], 1
        )

    def test_prime_startup_catalog_fallback_requires_high_confidence(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "detected_model": "Anenji ANJ-11KW-48V-WIFI-P",
                "detection_confidence": "medium",
                "driver_hint": "auto",
            },
            options={},
        )

        with patch.object(
            self.coordinator_startup_module, "resolve_unique_persisted_model_surface"
        ) as resolver:
            inverter = coordinator._prime_startup_inverter_from_persisted_metadata()

        self.assertIsNone(inverter)
        resolver.assert_not_called()

    def test_prime_startup_restores_exact_catalog_telemetry_and_controls(self) -> None:
        capability = types.SimpleNamespace(key="grid_charge_enable")
        profile = types.SimpleNamespace(
            driver_key="modbus_catalog",
            protocol_family="deye_3ph_high_80kw",
            groups=(),
            capabilities=(capability,),
            presets=(),
        )
        surface = types.SimpleNamespace(
            driver_key="modbus_catalog",
            variant_key="deye_3ph_high_80kw",
            profile_name="modbus_catalog/deye_3ph_high_80kw.json",
            register_schema_name="deye_3ph_high_80kw/base.json",
            read_only=False,
        )
        driver = types.SimpleNamespace(
            key="modbus_catalog",
            profile_name="",
            register_schema_name="",
            probe_targets=(
                self.coordinator_module.ProbeTarget(
                    devcode=1,
                    collector_addr=255,
                    device_addr=1,
                ),
            ),
        )
        register_schema = types.SimpleNamespace(driver_key="modbus_catalog")
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "detected_model": "Deye-Compatible Three-Phase Hybrid 80 kW (Modbus)",
                "detected_serial": "",
                "detection_confidence": "high",
                "detected_driver": "modbus_catalog",
                "driver_hint": "auto",
            },
            options={},
        )

        with (
            patch.object(
                self.coordinator_startup_module,
                "resolve_unique_persisted_model_surface",
                return_value=(types.SimpleNamespace(), surface),
            ),
            patch.object(self.coordinator_startup_module, "get_driver", return_value=driver),
            patch.object(
                self.coordinator_startup_module,
                "load_register_schema",
                return_value=register_schema,
            ),
            patch.object(
                self.coordinator_startup_module,
                "load_driver_profile",
                return_value=profile,
            ) as profile_loader,
        ):
            inverter = coordinator._prime_startup_inverter_from_persisted_metadata()

        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.driver_key, "modbus_catalog")
        self.assertEqual(inverter.variant_key, "deye_3ph_high_80kw")
        self.assertEqual(
            inverter.profile_name,
            "modbus_catalog/deye_3ph_high_80kw.json",
        )
        self.assertEqual(
            inverter.register_schema_name,
            "deye_3ph_high_80kw/base.json",
        )
        self.assertEqual(inverter.capabilities, (capability,))
        self.assertEqual(
            inverter.details["identity_source"],
            "persisted_detected_model",
        )
        profile_loader.assert_called_once_with(
            "modbus_catalog/deye_3ph_high_80kw.json"
        )

    def test_prime_startup_snapshot_adds_persisted_inverter_when_values_already_exist(self) -> None:
        capability = types.SimpleNamespace(key="output_source_priority")
        profile = types.SimpleNamespace(
            driver_key="pi30",
            protocol_family="pi30",
            groups=(),
            capabilities=(capability,),
            presets=(),
        )
        driver = types.SimpleNamespace(
            key="pi30",
            probe_targets=(
                self.coordinator_module.ProbeTarget(
                    devcode=0x0994,
                    collector_addr=0x01,
                    device_addr=0,
                ),
            ),
        )
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "connection_type": "eybond",
                "collector_operation_mode": "home_assistant_only",
                "control_mode": "auto",
                "detection_confidence": "high",
                "detected_model": "PI30 3500",
                "detected_serial": "55355535553555",
                "driver_hint": "pi30",
            },
            options={
                "effective_metadata_snapshot": {
                    "effective_owner_key": "pi30",
                    "variant_key": "default",
                    "profile_name": "pi30_ascii/models/smartess_0925_compat.json",
                    "register_schema_name": "pi30_ascii/models/smartess_0925_compat.json",
                    "confidence": "high",
                }
            },
        )
        coordinator.data = self.RuntimeSnapshot(
            values={"proxy_capture_status": "idle"},
            inverter=None,
        )
        coordinator._connection_spec = types.SimpleNamespace(
            collector_ip="192.168.1.51",
            collector_pn="V0000000000001",
            collector_cloud_family="smartess_at",
            server_ip="192.168.1.50",
            tcp_port=18899,
            effective_advertised_server_ip="192.168.1.50",
            effective_advertised_tcp_port=18899,
        )

        with (
            patch.object(self.coordinator_startup_module, "get_driver", return_value=driver),
            patch.object(
                self.coordinator_startup_module,
                "load_driver_profile",
                return_value=profile,
            ),
        ):
            primed = coordinator.prime_startup_snapshot()

        self.assertTrue(primed)
        self.assertEqual(coordinator.data.values["proxy_capture_status"], "idle")
        self.assertIsNotNone(coordinator.data.inverter)
        self.assertEqual(coordinator.data.inverter.capabilities, (capability,))

    def test_collector_onboarding_values_keep_cloud_metadata_wire_neutral(self) -> None:
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
            collector_configured_session_protocol="at_text",
            collector_identity_strategy="at_dtupn",
        )
        coordinator._runtime = types.SimpleNamespace(
            collector_server_endpoint_rollback_target="",
            listener_diagnostics=lambda: {
                "collector_configured_session_protocol": "",
                "collector_callback_identity_strategy": "",
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        values = coordinator._collector_transport_profile_runtime_values()

        self.assertEqual(values["collector_resolved_cloud_family"], "smartess_at")
        self.assertEqual(values["collector_resolved_session_protocol"], "")
        self.assertEqual(values["collector_resolved_identity_strategy"], "")
        self.assertEqual(values["collector_connection_session_protocol"], "at_text")
        self.assertEqual(values["collector_connection_identity_strategy"], "at_dtupn")
        self.assertEqual(values["collector_runtime_link_session_protocol"], "")
        self.assertEqual(values["collector_runtime_link_identity_strategy"], "")

    def test_live_framed_session_inventory_overrides_at_text_profile(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_cloud_family": "smartess_at",
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
                "collector_session_protocol": "eybond_framed",
                "driver_hint": "pi30",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            listener_diagnostics=lambda: {
                # This configured value is not the observation; the live
                # inventory below is. A framed ESP bridge must be allowed to
                # override an at_text cloud-family default.
                "collector_configured_session_protocol": "eybond_framed",
                "collector_callback_identity_strategy": "framed_heartbeat_then_fc2_pn",
                "collector_callback_session_inventory": [
                    {
                        "state": "routed_framed",
                        "protocol_shape": "eybond_framed_or_binary",
                        "collector_identity_masked": "V001…1016",
                    }
                ],
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        profile = coordinator.collector_transport_profile

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.runtime_owner_key, "pi30")
        self.assertEqual(profile.session_protocol, "eybond_framed")
        self.assertEqual(profile.identity_strategy, "framed_heartbeat_then_fc2_pn")

    def test_virtual_bridge_at_management_session_is_not_collector_kind_framed_override(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_cloud_family": "smartess_at",
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
                "collector_session_protocol": "eybond_framed",
                "driver_hint": "auto",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            listener_diagnostics=lambda: {
                "collector_configured_session_protocol": "eybond_framed",
                "collector_callback_observed_session_protocol": "at_text",
                "collector_callback_session_inventory": [
                    {
                        "state": "routed_at_text",
                        "protocol_shape": "eybond_framed_or_binary",
                        "collector_identity_masked": "V001…4022",
                    }
                ],
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        profile = coordinator.collector_transport_profile

        self.assertEqual(profile.cloud_family, "smartess_at")
        # collector_transport_profile is a legacy callback-profile hint, not
        # inverter payload authority. It must not hardcode "virtual bridge =>
        # framed"; payload routing is handled by SessionHandle adapters.
        self.assertEqual(profile.session_protocol, "at_text")
        self.assertEqual(profile.identity_strategy, "at_dtupn")
        self.assertEqual(profile.raw_passthrough_frame_format, "transparent")

    def test_live_session_inventory_overrides_configured_callback_protocol(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_cloud_family": "smartess_at",
                "collector_session_protocol": "eybond_framed",
                "driver_hint": "auto",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            listener_diagnostics=lambda: {
                # This is the link manager's configured protocol, not an
                # observation. It must not mask the live byte-shape inventory.
                "collector_configured_session_protocol": "eybond_framed",
                "collector_callback_identity_strategy": "framed_heartbeat_then_fc2_pn",
                "collector_callback_session_inventory": [
                    {
                        "state": "pending",
                        "protocol_shape": "at_text",
                        "collector_identity_masked": "V001…1016",
                    }
                ],
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        profile = coordinator.collector_transport_profile

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.session_protocol, "at_text")
        self.assertEqual(profile.identity_strategy, "at_dtupn")

    def test_global_session_inventory_does_not_override_entry_with_pn(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_cloud_family": "smartess_at",
                "collector_pn": "V001107SYN82291016",
                "driver_hint": "auto",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            listener_diagnostics=lambda: {
                "collector_configured_session_protocol": "at_text",
                "collector_callback_observed_session_protocol": "",
                "collector_callback_session_inventory": [
                    {
                        "state": "routed_framed",
                        "protocol_shape": "eybond_framed_or_binary",
                        "collector_identity_masked": "V000…7777",
                    }
                ],
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        profile = coordinator.collector_transport_profile

        self.assertEqual(profile.session_protocol, "")
        self.assertEqual(profile.identity_strategy, "")

    def test_owned_observed_session_protocol_overrides_entry_with_pn(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.config_entry = types.SimpleNamespace(
            entry_id="entry-1",
            data={
                "collector_cloud_family": "smartess_at",
                "collector_pn": "V001107SYN82291016",
                "driver_hint": "auto",
            },
            options={},
        )
        coordinator.data = self.RuntimeSnapshot(values={})
        coordinator._runtime = types.SimpleNamespace(
            listener_diagnostics=lambda: {
                "collector_configured_session_protocol": "at_text",
                "collector_callback_observed_session_protocol": "eybond_framed",
                "collector_callback_session_inventory": [
                    {
                        "state": "routed_framed",
                        "protocol_shape": "eybond_framed_or_binary",
                        "collector_identity_masked": "V001…1016",
                    }
                ],
            },
        )
        coordinator._remembered_collector_server_endpoint = ""

        profile = coordinator.collector_transport_profile

        self.assertEqual(profile.session_protocol, "eybond_framed")
        self.assertEqual(profile.identity_strategy, "framed_heartbeat_then_fc2_pn")

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
                    "collector_configured_session_protocol": "",
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
            coordinator._sync_collector_capability_profile = lambda: None
            coordinator._configure_reverse_discovery_mode = lambda: None
            coordinator._async_warm_effective_metadata_cache = AsyncMock()
            coordinator._async_reconcile_managed_collector_endpoint = AsyncMock()
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
            coordinator._prune_collector_values_for_connection = lambda _snapshot: None
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
            ):
                snapshot = await coordinator._async_update_data_with_runtime_lock()

            self.assertEqual(refresh_count, 1)
            self.assertEqual(reconcile_calls, [])
            self.assertEqual(snapshot.values["collector_cloud_family"], "smartess_at")
            self.assertEqual(snapshot.values["refresh_count"], 1)

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
                self.coordinator_tooling_projection_module,
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

    def test_integration_build_runtime_values_read_real_manifest(self) -> None:
        package_dir = self.coordinator_tooling_projection_module._package_dir()
        manifest_path = package_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        values = self.coordinator_module._integration_build_runtime_values()

        self.assertEqual(package_dir.name, "eybond_local")
        self.assertEqual(
            Path(values["integration_package_dir"]),
            package_dir,
        )
        self.assertEqual(
            values["integration_manifest_version"],
            manifest["version"],
        )

    def test_bind_apply_persists_inbound_integration_managed(self) -> None:
        # Item 2: a successful bind write makes the entry inbound +
        # integration_managed and records write provenance.
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={"control_mode": "full"},
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "47.91.67.66,18899,TCP"},
            )
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
            )

            async def _async_set_collector_server_endpoint(endpoint, *, apply_changes=True):
                return {"readback_endpoint": endpoint, "status": "applied"}

            def _async_update_entry(entry, **kwargs):
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])

            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator._async_prepare_home_assistant_callback_listener = AsyncMock()
            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
            )

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                await coordinator.async_bind_collector_to_home_assistant(
                    confirm_redirect=True,
                )

            data = coordinator.config_entry.data
            # Batch 8: a bind records the endpoint-write FACTS only. The
            # connection strategy changes exclusively through the verified
            # transition authority (a bind is not a reconnect proof).
            self.assertNotIn("connection_strategy", data)
            self.assertEqual(data.get("endpoint_control_policy"), "integration_managed")
            self.assertEqual(data.get("endpoint_written_value"), "192.168.1.50,18899,TCP")
            self.assertIn("endpoint_written_at", data)

        asyncio.run(_run())

    def test_bind_already_bound_does_not_claim_endpoint_ownership(self) -> None:
        # Item 1/2: already pointing at HA, no write -> inbound, but NOT
        # integration_managed and no write provenance.
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
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

            def _async_update_entry(entry, **kwargs):
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])

            coordinator._runtime = types.SimpleNamespace(
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator._async_prepare_home_assistant_callback_listener = AsyncMock()
            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
            )

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                result = await coordinator.async_bind_collector_to_home_assistant(
                    confirm_redirect=True,
                )

            self.assertEqual(result["status"], "already_bound")
            data = coordinator.config_entry.data
            # Nothing was written, so NOTHING was earned: no axis of any kind.
            self.assertNotIn("connection_strategy", data)
            self.assertNotIn("endpoint_control_policy", data)
            self.assertNotIn("endpoint_written_value", data)

        asyncio.run(_run())

    def test_rollback_apply_persists_callback_external_and_clears_written(self) -> None:
        # Item 2: a successful rollback hands control back -> callback_on_demand
        # + external, with write provenance cleared.
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={
                    "control_mode": "full",
                    "connection_strategy": "inbound",
                    "endpoint_control_policy": "integration_managed",
                    "endpoint_written_value": "192.168.1.50,18899,TCP",
                    "endpoint_written_at": "2026-01-01T00:00:00+00:00",
                },
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            )
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
            )
            coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"

            async def _async_set_collector_server_endpoint(endpoint, *, apply_changes=True):
                return {"readback_endpoint": endpoint, "status": "applied"}

            def _async_update_entry(entry, **kwargs):
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])

            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
            )

            with patch.object(
                self.coordinator_module.EybondLocalCoordinator,
                "proxy_capture_overview",
                new_callable=PropertyMock,
                return_value=types.SimpleNamespace(status="ready"),
            ):
                await coordinator.async_rollback_collector_server_endpoint(
                    apply_changes=True,
                    confirm_redirect=True,
                )

            data = coordinator.config_entry.data
            # Batch 8: the rollback records the restore FACT (external, cleared
            # provenance); the strategy stays what it was — only the verified
            # transition authority may change it, after a callback proof.
            self.assertEqual(data.get("connection_strategy"), "inbound")
            self.assertEqual(data.get("endpoint_control_policy"), "external")
            self.assertNotIn("endpoint_written_value", data)
            self.assertNotIn("endpoint_written_at", data)

        asyncio.run(_run())

    def test_rollback_staged_does_not_change_durable_axes(self) -> None:
        # Item 2: apply_changes=False stages nothing on the collector, so it must
        # not change the durable axes.
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                entry_id="entry-1",
                data={
                    "control_mode": "full",
                    "connection_strategy": "inbound",
                    "endpoint_control_policy": "integration_managed",
                    "endpoint_written_value": "192.168.1.50,18899,TCP",
                },
                options={"control_mode": "full"},
            )
            coordinator.data = self.RuntimeSnapshot(
                connected=True,
                values={"collector_server_endpoint": "192.168.1.50,18899,TCP"},
            )
            coordinator._connection_spec = types.SimpleNamespace(
                effective_advertised_server_ip="192.168.1.50",
            )
            coordinator._remembered_collector_server_endpoint = "47.91.67.66,18899,TCP"

            async def _async_set_collector_server_endpoint(endpoint, *, apply_changes=True):
                return {"requested_endpoint": endpoint, "status": "rollback_staged"}

            async def _async_request_refresh() -> None:
                return None

            def _async_update_entry(entry, **kwargs):
                if "data" in kwargs:
                    entry.data = dict(kwargs["data"])

            coordinator._runtime = types.SimpleNamespace(
                async_set_collector_server_endpoint=_async_set_collector_server_endpoint,
                collector_server_endpoint_rollback_target="47.91.67.66,18899,TCP",
            )
            coordinator.async_request_refresh = _async_request_refresh
            coordinator.hass = types.SimpleNamespace(
                config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
            )

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

            data = coordinator.config_entry.data
            self.assertEqual(data.get("connection_strategy"), "inbound")
            self.assertEqual(data.get("endpoint_control_policy"), "integration_managed")
            self.assertEqual(data.get("endpoint_written_value"), "192.168.1.50,18899,TCP")

        asyncio.run(_run())

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
                entry_id="cp2c-writer-guard",
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
                entry_id="cp2c-writer-guard",
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
                entry_id="cp2c-writer-guard",
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
                entry_id="cp2c-writer-guard",
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
                policy=self.coordinator_module.poll_policy_for_driver_key("auto"),
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
        self.assertEqual(entry.options["driver_unsupported_commands_version"], 2)
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
        self.assertNotIn("driver_unsupported_commands_version", entry.options)
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
        invalidations: list[str] = []
        coordinator._runtime = types.SimpleNamespace(
            invalidate_collector_runtime_values=lambda: invalidations.append("invalidate")
        )

        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "driver_unbound"},
            connected=True,
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(invalidations, ["invalidate"])

        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "collector_offline"},
            connected=False,
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 2)
        self.assertEqual(invalidations, ["invalidate", "invalidate"])

        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "driver_bound"},
            connected=True,
            inverter=object(),
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 2)
        self.assertEqual(invalidations, ["invalidate", "invalidate"])

        coordinator._shutdown_complete = True
        coordinator.data = self.RuntimeSnapshot(
            values={"runtime_driver_state": "driver_unbound"},
            connected=True,
        )
        coordinator._on_collector_connection_established("192.168.1.14")
        self.assertEqual(len(scheduled), 2)
        self.assertEqual(invalidations, ["invalidate", "invalidate"])

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
                policy=self.coordinator_module.poll_policy_for_driver_key("auto"),
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

    def test_poll_scheduler_applies_model_specific_policy_for_same_driver_key(self) -> None:
        # A catalog driver keeps the SAME driver key but resolves a different
        # policy once the model/variant is known. The scheduler must switch even
        # though the driver key did not change (the old early-return bug applied
        # only the family/default policy forever).
        from custom_components.eybond_local.poll_policy import PollPolicy

        fast = PollPolicy(min_auto_interval=2.0, max_auto_interval=30.0)
        slow = PollPolicy(min_auto_interval=20.0, max_auto_interval=200.0)

        def _model_aware_resolver(driver_key="", inverter=None):
            if str(driver_key or "").strip() != "modbus_catalog":
                return PollPolicy(min_auto_interval=10.0, max_auto_interval=120.0)
            return fast if getattr(inverter, "variant_key", "") == "fast" else slow

        original = self.coordinator_polling_module.poll_policy_for_driver_key
        self.coordinator_polling_module.poll_policy_for_driver_key = _model_aware_resolver
        try:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(
                data={},
                options={"poll_mode": "auto", "poll_interval": 10},
            )
            coordinator._poll_scheduler_driver_key = "auto"
            # Scheduler created BEFORE model identity is known.
            coordinator._runtime = types.SimpleNamespace(detected_inverter=None)
            coordinator._ensure_poll_scheduler()

            # First snapshot: driver known, no model yet -> family/default policy.
            coordinator._update_poll_scheduler_policy_from_snapshot(
                self.RuntimeSnapshot(values={"driver_key": "modbus_catalog"})
            )
            self.assertEqual(coordinator._poll_scheduler_driver_key, "modbus_catalog")
            self.assertEqual(coordinator._poll_scheduler.policy.min_auto_interval, 20)

            # Accumulate observations; they must survive the policy switch.
            for _ in range(5):
                coordinator._poll_scheduler.observe(0.7)
            samples_before = coordinator._poll_scheduler._durations[-1]

            # The SAME driver later resolves a model-specific (variant) policy.
            coordinator._runtime.detected_inverter = types.SimpleNamespace(
                variant_key="fast"
            )
            coordinator._update_poll_scheduler_policy_from_snapshot(
                self.RuntimeSnapshot(values={"driver_key": "modbus_catalog"})
            )

            # Switched despite the unchanged driver key, samples preserved.
            self.assertEqual(coordinator._poll_scheduler_driver_key, "modbus_catalog")
            self.assertEqual(coordinator._poll_scheduler.policy.min_auto_interval, 2)
            self.assertEqual(coordinator._poll_scheduler._durations[-1], samples_before)
        finally:
            self.coordinator_polling_module.poll_policy_for_driver_key = original

    def test_persist_confirmed_session_protocol_is_pn_validated_and_live_sourced(self) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()  # non-None gate
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_pn": "PNALPHA-FULL-0001"}
        )
        recorded: dict = {}
        coordinator._persist_connection_axes = (
            lambda updates=None, **kwargs: recorded.update(updates or {})
        )

        # Matching PN + live evidence -> persisted with live_session provenance.
        coordinator._runtime = types.SimpleNamespace(
            confirmed_session_protocol_evidence=lambda: (
                "eybond_framed",
                "PNALPHA-FULL-0001",
            )
        )
        coordinator._persist_confirmed_session_protocol_from_runtime()
        self.assertEqual(
            recorded.get("collector_confirmed_session_protocol"), "eybond_framed"
        )
        self.assertEqual(
            recorded.get("collector_confirmed_session_protocol_source"), "live_session"
        )
        self.assertEqual(
            recorded.get("collector_confirmed_session_protocol_pn"), "PNALPHA-FULL-0001"
        )

        # A DIFFERENT PN is never persisted.
        recorded.clear()
        coordinator._runtime = types.SimpleNamespace(
            confirmed_session_protocol_evidence=lambda: ("eybond_framed", "PNBETA-FULL-0002")
        )
        coordinator._persist_confirmed_session_protocol_from_runtime()
        self.assertEqual(recorded, {})

        # No confirmed evidence -> nothing persisted.
        coordinator._runtime = types.SimpleNamespace(
            confirmed_session_protocol_evidence=lambda: ("", "")
        )
        coordinator._persist_confirmed_session_protocol_from_runtime()
        self.assertEqual(recorded, {})

    def test_persist_confirmed_session_protocol_stamps_observed_at_only_on_new_evidence(
        self,
    ) -> None:
        coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
        coordinator.hass = object()
        coordinator.config_entry = types.SimpleNamespace(
            data={"collector_pn": "PNALPHA-FULL-0001"}
        )
        recorded: dict = {}
        coordinator._persist_connection_axes = (
            lambda updates=None, **kwargs: recorded.update(updates or {})
        )
        coordinator._runtime = types.SimpleNamespace(
            confirmed_session_protocol_evidence=lambda: (
                "eybond_framed",
                "PNALPHA-FULL-0001",
            )
        )

        # NEW evidence: an observed-at UTC timestamp is stamped exactly once.
        coordinator._persist_confirmed_session_protocol_from_runtime()
        observed_at = recorded.get("collector_confirmed_session_protocol_observed_at")
        self.assertTrue(observed_at)
        # A real ISO-8601 UTC timestamp (parseable, tz-aware).
        parsed = datetime.fromisoformat(observed_at)
        self.assertIsNotNone(parsed.tzinfo)

        # The evidence is now already persisted (unchanged): a later poll must be
        # a pure no-op -- the timestamp is NOT rewritten each refresh.
        coordinator.config_entry = types.SimpleNamespace(
            data={
                "collector_pn": "PNALPHA-FULL-0001",
                "collector_confirmed_session_protocol": "eybond_framed",
                "collector_confirmed_session_protocol_source": "live_session",
                "collector_confirmed_session_protocol_pn": "PNALPHA-FULL-0001",
                "collector_confirmed_session_protocol_observed_at": observed_at,
            }
        )
        recorded.clear()
        coordinator._persist_confirmed_session_protocol_from_runtime()
        self.assertEqual(recorded, {})

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

    def test_support_package_refreshes_snapshot_after_capture_reconnect(self) -> None:
        async def _run() -> None:
            coordinator = object.__new__(self.coordinator_module.EybondLocalCoordinator)
            coordinator.config_entry = types.SimpleNamespace(entry_id="entry-support")
            coordinator._runtime_operation_lock = asyncio.Lock()
            coordinator.data = self.RuntimeSnapshot(connected=False)
            events: list[str] = []
            snapshots = iter(
                (
                    self.RuntimeSnapshot(
                        connected=False,
                        values={"runtime_session_state": "offline"},
                    ),
                    self.RuntimeSnapshot(
                        connected=True,
                        values={"runtime_session_state": "online"},
                    ),
                )
            )

            async def _refresh():
                snapshot = next(snapshots)
                events.append(f"refresh:{snapshot.connected}")
                return snapshot

            async def _registry_lookup():
                events.append("registry")
                return ("found", object())

            async def _capture():
                events.append("capture")
                return {
                    "capture_kind": "modbus_register_dump",
                    "captured_ranges": [{"start": 100, "count": 10}],
                    "range_failures": [],
                }

            def _build_payload(**_kwargs):
                events.append("build")
                return {
                    "runtime": {
                        "connected": coordinator.data.connected,
                        "values": dict(coordinator.data.values),
                    }
                }

            coordinator._async_update_data_with_runtime_lock = _refresh
            coordinator._async_collector_registry_lookup = _registry_lookup
            coordinator._runtime = types.SimpleNamespace(
                async_capture_support_evidence=_capture
            )
            coordinator._build_support_bundle_payload = _build_payload

            payload, raw_capture = await coordinator._async_build_support_package_payloads(
                integration_build_values={}
            )

            self.assertEqual(
                events,
                ["refresh:False", "registry", "capture", "refresh:True", "build"],
            )
            self.assertTrue(payload["runtime"]["connected"])
            self.assertEqual(
                payload["runtime"]["values"]["runtime_session_state"],
                "online",
            )
            self.assertEqual(raw_capture["capture_kind"], "modbus_register_dump")

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
