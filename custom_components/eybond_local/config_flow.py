"""Config flow for EyeBond Local."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from html import escape as html_escape
import importlib
import ipaddress
import json
import logging
from functools import lru_cache, wraps
from pathlib import Path
import re
import socket
import subprocess
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.data_entry_flow import section
from homeassistant.helpers.selector import (
    BooleanSelector,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from .collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    DEFAULT_COLLECTOR_SERVER_PROTOCOL,
    format_collector_server_endpoint_for_cloud_profile,
    format_collector_server_endpoint,
    inspect_collector_server_endpoint,
    resolve_collector_server_endpoint,
    home_assistant_callback_endpoint,
)
from .connection.branch_registry import get_connection_branch, supported_connection_types
from .connection.entry import (
    build_detected_entry_settings,
    build_manual_entry_settings,
    build_runtime_option_settings,
    with_driver_hint,
)
from .connection.models import build_connection_spec, build_connection_spec_from_values
from .connection.ui import ConnectionFormField
from .const import (
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_TYPE,
    CONF_CONNECTION_MODE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_MODEL,
    CONF_DEVICE_CATALOG_ENTRY,
    CONF_DEVICE_CATALOG_KIND,
    CONF_DEVICE_CATALOG_TIER,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    CONTROL_MODE_AUTO,
    CONTROL_MODE_FULL,
    CONTROL_MODE_READ_ONLY,
    COLLECTOR_OPERATION_HA_ONLY,
    COLLECTOR_OPERATION_MODES,
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
    CONNECTION_TYPE_EYBOND,
    DEFAULT_COLLECTOR_OPERATION_MODE,
    DEFAULT_CONTROL_MODE,
    CONF_DISCOVERY_INTERVAL,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_HINT,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_MODE,
    DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DOMAIN,
    DRIVER_HINT_AUTO,
    MAX_PROXY_CAPTURE_DURATION_MINUTES,
    MIN_PROXY_CAPTURE_DURATION_MINUTES,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from .control_policy import control_mode_options
from .collector.discovery import async_probe_target
from .collector.capabilities import (
    CollectorCapabilityProfile,
    collector_capability_profile_from_runtime,
)
from .collector.smartess_local import (
    QUERY_HARDWARE_VERSION,
    QUERY_NETWORK_DIAGNOSTICS,
    QUERY_REBOOT_REQUIRED,
    QUERY_SERIAL_BAUDRATE,
    QUERY_WIFI_SCAN_LIST,
    SET_REBOOT_OR_APPLY,
    SET_SERVER_ENDPOINT,
    SET_SERIAL_BAUDRATE,
    SET_TARGET_PASSWORD,
    SET_TARGET_SSID,
    SmartEssLocalSession,
)
from .collector.smartess_ble import (
    BleakSmartEssBleScanner,
    BleakSmartEssBleLink,
    SmartEssBleCandidate,
    SmartEssBleError,
    SmartEssBleHostCapability,
    SmartEssBleProvisionOutcome,
    SmartEssBleProvisioner,
    SmartEssBleSession,
    SmartEssBleWifiNetwork,
    async_probe_ble_host_capability,
    normalize_discovered_candidate,
    parse_wifi_scan_response,
)
from .collector.at_runtime import query_runtime_collector_at_values
from .collector.capabilities import parse_esp_collector_hardware_token
from .collector.parameter_registry import (
    COLLECTOR_PARAMETER_DEFINITION_BY_ID,
    query_runtime_collector_values,
)
from .collector.transport import SharedCollectorAtTransport, SharedEybondTransport, _finish_cleanup_on_cancel
from .collector.cloud_family import collector_cloud_family_observation_from_endpoint
from .drivers.catalog_identity import ERROR_INVERTER_LINK_DOWN
from .drivers.registry import driver_options
from .metadata.local_metadata import (
    local_profile_override_details,
    local_register_schema_override_details,
    resolve_local_metadata_rollback_paths,
)
from .naming import installation_title
from .metadata.profile_loader import load_driver_profile
from .metadata.smartess_draft import resolve_smartess_known_family_draft_plan
from .models import OnboardingResult
from .onboarding.detection import DiscoveryTarget
from .onboarding.factory import create_onboarding_manager
from .onboarding.presentation import (
    confidence_sort_score,
    has_smartess_collector_hint,
    scan_result_sort_key,
    scan_result_status_code,
)
from .onboarding.timeouts import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    auto_scan_timeout_seconds as _onboarding_auto_scan_timeout_seconds,
    deep_scan_timeout_seconds as _onboarding_deep_scan_timeout_seconds,
    manual_probe_timeout_seconds as _onboarding_manual_probe_timeout_seconds,
)
from .smartess_cloud import classify_smartess_cloud_error
from .smartess_cloud import (
    DEFAULT_LEARN_NUMERIC_VALUE,
    SessionCredentials,
    build_device_detail_action,
    build_device_settings_action,
    build_learn_settings_plan,
    fetch_device_bundle_for_collector,
    fetch_signed_action,
    login_with_password,
)
from .support.cloud_evidence import fetch_and_export_smartess_device_bundle_cloud_evidence
from .support.collector_registry import remember_collector_original_endpoint
from .support.memory_guard import read_available_memory_mib, shadow_learning_memory_blocker
from .support.shadow_learning_backend import build_shadow_learning_preflight, build_shadow_learning_seed
from .support.read_learning_binder import bind_cloud_labels_to_registers
from .support.shadow_learning_orchestrator import (
    async_orchestrate_shadow_learning_settings,
    orchestrate_shadow_learning_settings,
)
from .support.valuecloud_shadow_learning_orchestrator import (
    async_orchestrate_valuecloud_shadow_learning,
)
from .support.shadow_learning_proxy import route_status_indicates_control_write_ready
from .support.shadow_learning_overlay_generator import generate_shadow_learning_overlay_drafts
from .support.shadow_learning_review_model import (
    build_activation_selection,
    default_learned_control_label,
)
from . import valuecloud_cloud as valuecloud_cloud_module

CONF_RESULT_KEY = "result_key"
_SCAN_RESULTS_ACTION_REFRESH = "action:refresh_scan"
_SCAN_RESULTS_ACTION_ADVANCED = "action:advanced_setup"
CONF_DRIVER_MATCH_KEY = "driver_match_key"
CONF_COLLECTOR_NETWORK_STATUS = "collector_network_status"
CONF_COLLECTOR_WIFI_ACTION = "collector_wifi_action"
CONF_CONFIRM_COLLECTOR_WIFI_APPLY = "confirm_collector_wifi_apply"
CONF_COLLECTOR_UART_ACTION = "collector_uart_action"
CONF_COLLECTOR_UART_BAUDRATE = "collector_uart_baudrate"
CONF_CONFIRM_COLLECTOR_UART_APPLY = "confirm_collector_uart_apply"
CONF_SETUP_MODE = "setup_mode"
CONF_BLE_ADDRESS = "ble_address"
CONF_BLE_ACTION = "ble_action"
CONF_WIFI_SSID = "wifi_ssid"
CONF_WIFI_PASSWORD = "wifi_password"
BLE_ADDRESS_RESCAN = "__rescan__"
BLE_ACTION_RESCAN = "rescan"
BLE_ACTION_REFRESH_WIFI = "refresh_wifi"
BLE_ACTION_APPLY = "apply"
COLLECTOR_WIFI_ACTION_REFRESH = "refresh"
COLLECTOR_WIFI_ACTION_APPLY = "apply"
COLLECTOR_UART_ACTION_REFRESH = "refresh"
COLLECTOR_UART_ACTION_APPLY = "apply"
COLLECTOR_UART_BAUDRATES = ("2400", "4800", "9600", "19200", "38400", "57600", "115200")
CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE = "smartess_cloud_mode"
SETUP_MODE_AUTO = "auto"
SETUP_MODE_BLUETOOTH = "bluetooth"
SETUP_MODE_DEEP_SCAN = "deep_scan"
SETUP_MODE_MANUAL = "manual"
COLLECTOR_NETWORK_ALREADY_CONNECTED = "already_connected"
COLLECTOR_NETWORK_NEEDS_BLUETOOTH = "needs_bluetooth"
MANUAL_CONFIRM_ACTION_PROBE_AGAIN = "manual_probe_again"
MANUAL_CONFIRM_ACTION_EDIT_SETTINGS = "manual_edit_settings"
MANUAL_CONFIRM_ACTION_CREATE_PENDING = "manual_create_pending"
PROXY_CAPTURE_ACTION_RESET_TIMER = "reset_timer"
SHADOW_LEARNING_ACTION_REFRESH = "refresh"
SHADOW_LEARNING_ACTION_PREFLIGHT = "preflight"
SHADOW_LEARNING_ACTION_START_SESSION = "start_session"
SHADOW_LEARNING_ACTION_STOP_SESSION = "stop_session"
SHADOW_LEARNING_ACTION_PREVIEW_PLAN = "preview_plan"
SHADOW_LEARNING_ACTION_RUN_LEARNING = "run_learning"
SHADOW_LEARNING_ACTION_GENERATE_OVERLAY = "generate_overlay"
SHADOW_LEARNING_ACTION_ACTIVATE_OVERLAY = "activate_overlay"
SHADOW_LEARNING_ACTION_EXPORT_SUPPORT_ONLY = "export_support_only"
SHADOW_LEARNING_MODE_MANUAL = "manual_selected_fields"
SHADOW_LEARNING_MODE_ENUM_SWEEP = "enum_sweep"
SHADOW_LEARNING_MODE_NUMERIC_OPT_IN = "numeric_opt_in"
SHADOW_LEARNING_MODE_SUPPORT_ONLY = "support_package_only"
# Upper bound on the number of settings the automatic guided control-discovery
# pipeline (EYB-REF-041) will probe in one run. The automatic plan already tests
# only choice/enum settings, one value each (no numeric writes, no enum sweep);
# this cap keeps the normal user path a bounded probe instead of an open-ended
# sweep even when a device exposes a very large settings table.
CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS = 40
# Guided control-discovery result-screen actions (EYB-REF-047). The final wizard
# screen offers three explicit choices: enable the controls the user turned on,
# create a support package, or close. Activation is always an explicit user
# action — guided discovery never silently enables writable controls.
CONTROL_DISCOVERY_RESULT_ACTION_ACTIVATE = "activate_selected"
CONTROL_DISCOVERY_RESULT_ACTION_SUPPORT = "create_support_package"
CONTROL_DISCOVERY_RESULT_ACTION_RETRY = "retry"
CONTROL_DISCOVERY_RESULT_ACTION_DONE = "done"
SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED = "use_saved"
SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH = "refresh"
SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY = "archive_only"
_LOCAL_METADATA_STATUS_TRANSLATION_KEYS = {
    "Starting collector proxy capture": "starting_proxy_capture",
    "Collector proxy capture failed to start": "proxy_capture_failed_to_start",
    "Collector proxy capture running": "proxy_capture_running",
    "Stopping collector proxy capture": "stopping_proxy_capture",
    "Collector proxy capture stopped": "proxy_capture_stopped",
    "Recovered interrupted collector proxy capture": "recovered_interrupted_proxy_capture",
    "SmartESS cloud evidence exported": "smartess_cloud_evidence_exported",
    "Cloud evidence exported": "cloud_evidence_exported",
    "Cloud evidence refresh failed; using last saved evidence": "cloud_evidence_refresh_failed_using_saved",
    "Support bundle exported": "support_bundle_exported",
    "Support archive exported": "support_archive_exported",
    "Local profile draft created": "local_profile_draft_created",
    "Local register schema draft created": "local_register_schema_draft_created",
    "Reloading local metadata": "reloading_local_metadata",
    "Rolling back local metadata": "rolling_back_local_metadata",
    "SmartESS local draft created": "smartess_local_draft_created",
    "SmartESS SMG bridge created": "smartess_smg_bridge_created",
}
_INT_FIELDS = {
    CONF_ADVERTISED_TCP_PORT,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONF_DISCOVERY_INTERVAL,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
}
logger = logging.getLogger(__name__)
_TRANSLATIONS_DIR = Path(__file__).with_name("translations")
_FLOW_TRANSLATIONS_DIR = Path(__file__).with_name("flow_translations")
_ONBOARDING_TIMEOUT_POLICY = DEFAULT_ONBOARDING_TIMEOUT_POLICY
_AUTO_SCAN_TIMEOUT = _onboarding_auto_scan_timeout_seconds(_ONBOARDING_TIMEOUT_POLICY)
_BLE_SCAN_TIMEOUT = 5.0
_BLE_CONNECT_TIMEOUT = 30.0
_BLE_WIFI_SCAN_TIMEOUT = 30.0
_BLE_WIFI_SCAN_ATTEMPTS = 3
_BLE_WIFI_SCAN_RETRY_DELAY = 1.0
_BLE_PROVISION_TIMEOUT = 45.0
_MANUAL_PROBE_TIMEOUT = _onboarding_manual_probe_timeout_seconds(_ONBOARDING_TIMEOUT_POLICY)
_CONFIRM_RUNTIME_DETAILS_TIMEOUT = 8.0
_SCAN_PROGRESS_BAR_WIDTH = 12
_INTERNAL_SCAN_INTERFACE_NAMES = frozenset({"docker0", "hassio"})
_INTERNAL_SCAN_INTERFACE_PREFIXES = (
    "br-",
    "cni",
    "docker",
    "flannel",
    "veth",
    "virbr",
)
_IP_ADDR_SHOW_ONELINE = re.compile(
    r"^\d+:\s+(?P<ifname>\S+)\s+inet\s+(?P<ip>\d+\.\d+\.\d+\.\d+)/(?P<prefixlen>\d+)"
    r"(?:\s+brd\s+(?P<broadcast>\d+\.\d+\.\d+\.\d+))?\s+scope\s+(?P<scope>\S+)"
)


def _exception_detail(exc: BaseException) -> str:
    return str(exc) or type(exc).__name__


def _is_user_selectable_scan_interface(ifname: str) -> bool:
    normalized = str(ifname or "").strip().lower()
    if not normalized:
        return True
    if normalized in _INTERNAL_SCAN_INTERFACE_NAMES:
        return False
    return not normalized.startswith(_INTERNAL_SCAN_INTERFACE_PREFIXES)


@dataclass(slots=True)
class _SmartEssCloudSettingHighlight:
    """Compact preview for one SmartESS cloud field shown during onboarding."""

    title: str
    bucket: str = ""
    current_value: str = ""
    register: int | None = None


@dataclass(slots=True)
class _SmartEssCloudAssistState:
    """One saved SmartESS cloud-assist result for the current onboarding flow."""

    collector_pn: str
    evidence_path: str = ""
    inferred_asset_id: str = ""
    inferred_profile_key: str = ""
    inferred_driver_key: str = ""
    inferred_family_label: str = ""
    inferred_reason: str = ""
    exact_field_count: int = 0
    probable_field_count: int = 0
    cloud_only_field_count: int = 0
    current_values_included: bool = False
    total_field_count: int = 0
    mapped_field_count: int = 0
    fields_with_current_value: int = 0
    device_pn: str = ""
    device_sn: str = ""
    device_name: str = ""
    device_alias: str = ""
    device_status: str = ""
    device_brand: str = ""
    device_devcode: int | None = None
    device_devaddr: int | None = None
    detail_sections: tuple[str, ...] = ()
    highlight_settings: tuple[_SmartEssCloudSettingHighlight, ...] = ()


@asynccontextmanager
async def _async_timeout(timeout_seconds: float):
    """Use asyncio.timeout when available, with a Python 3.10-compatible fallback."""

    native_timeout = getattr(asyncio, "timeout", None)
    if native_timeout is not None:
        async with native_timeout(timeout_seconds):
            yield
        return

    task = asyncio.current_task()
    if task is None:
        yield
        return

    loop = asyncio.get_running_loop()
    timed_out = False

    def _cancel_current_task() -> None:
        nonlocal timed_out
        timed_out = True
        task.cancel()

    handle = loop.call_later(timeout_seconds, _cancel_current_task)
    try:
        yield
    except asyncio.CancelledError as exc:
        if timed_out:
            raise TimeoutError from exc
        raise
    finally:
        handle.cancel()


def _translation_candidates(language: str) -> list[str]:
    candidates: list[str] = []
    normalized = (language or "").strip()
    if normalized:
        candidates.append(normalized)
        if "-" in normalized:
            candidates.append(normalized.split("-", 1)[0])
        if "_" in normalized:
            candidates.append(normalized.split("_", 1)[0])
    candidates.append("en")
    return candidates


def _load_translation_bundle_from_dir(directory: Path, language: str) -> dict[str, Any]:
    seen: set[str] = set()
    for candidate in _translation_candidates(language):
        if candidate in seen:
            continue
        seen.add(candidate)
        path = directory / f"{candidate}.json"
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to load translation bundle: %s", path)
            break
    return {}


def _merge_translation_bundle(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _merge_translation_bundle(existing, value)
        else:
            merged[key] = value
    return merged


def _collector_identity_matches(left: str, right: str) -> bool:
    """Return whether two collector PN values look like the same collector."""

    normalized_left = str(left or "").strip()
    normalized_right = str(right or "").strip()
    if not normalized_left or not normalized_right:
        return False
    if normalized_left == normalized_right:
        return True
    if min(len(normalized_left), len(normalized_right)) < 10:
        return False
    return bool(
        normalized_left.startswith(normalized_right)
        or normalized_right.startswith(normalized_left)
    )


@lru_cache(maxsize=16)
def _load_translation_bundle(language: str) -> dict[str, Any]:
    """Load one translation bundle for the requested language."""

    bundle = _load_translation_bundle_from_dir(_TRANSLATIONS_DIR, language)
    flow_bundle = _load_translation_bundle_from_dir(_FLOW_TRANSLATIONS_DIR, language)
    return _merge_translation_bundle(bundle, flow_bundle)


def _translation_lookup(bundle: dict[str, Any], key: str) -> Any:
    """Look up a nested translation key inside one bundle."""

    current: Any = bundle
    for part in key.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _selector_option_label(
    bundle: dict[str, Any] | None,
    selector_key: str,
    option_key: str,
    default: str,
) -> str:
    """Resolve one localized selector option label with an English fallback."""

    if not isinstance(bundle, dict):
        return default
    value = _translation_lookup(bundle, f"selector.{selector_key}.options.{option_key}")
    return value if isinstance(value, str) and value else default


def _with_translation_bundle(step):
    """Preload one flow translation bundle before rendering localized UI."""

    @wraps(step)
    async def _wrapped(self, *args, **kwargs):
        await self._async_ensure_translation_bundle()
        return await step(self, *args, **kwargs)

    return _wrapped


def _result_indicates_inverter_link_down(result: OnboardingResult | None) -> bool:
    """True when the collector answered but the inverter link was down."""

    return (
        result is not None
        and result.match is None
        and str(result.last_error or "") == ERROR_INVERTER_LINK_DOWN
    )


def _apply_device_catalog_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist the catalog identification verdict into the config entry."""

    if result is None or result.match is None:
        return
    catalog = result.match.details.get("device_catalog")
    if not isinstance(catalog, dict):
        return
    data[CONF_DEVICE_CATALOG_KIND] = str(catalog.get("kind") or "")
    data[CONF_DEVICE_CATALOG_TIER] = str(catalog.get("tier") or "")
    data[CONF_DEVICE_CATALOG_ENTRY] = str(catalog.get("entry_key") or "")


def _result_is_virtual_bridge(result: OnboardingResult | None) -> bool:
    """Return True when an onboarding result positively identified an ESP bridge."""

    return _result_collector_capabilities(result).virtual_bridge


def _result_collector_capabilities(
    result: OnboardingResult | None,
) -> CollectorCapabilityProfile:
    """Return collector capabilities inferred from one onboarding result."""

    if result is None:
        return collector_capability_profile_from_runtime()
    collector = getattr(result, "collector", None)
    collector_info = getattr(collector, "collector", None)
    match = getattr(result, "match", None)
    details = getattr(match, "details", None)
    values = details if isinstance(details, dict) else {}
    return collector_capability_profile_from_runtime(
        collector=collector_info,
        values=values,
        data={},
        options={},
    )


def _apply_smartess_detection_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist SmartESS onboarding metadata when the probe captured it."""

    if result is None:
        return

    collector_info = result.collector.collector if result.collector is not None else None
    match_details = result.match.details if result.match is not None else {}

    def _pick(detail_key: str, collector_attr: str) -> Any:
        value = match_details.get(detail_key)
        if value not in (None, ""):
            return value
        if collector_info is None:
            return None
        value = getattr(collector_info, collector_attr, None)
        if value in (None, ""):
            return None
        return value

    mapping = (
        (CONF_SMARTESS_COLLECTOR_VERSION, "smartess_collector_version", "smartess_collector_version"),
        (CONF_SMARTESS_PROTOCOL_ASSET_ID, "smartess_protocol_asset_id", "smartess_protocol_asset_id"),
        (CONF_SMARTESS_PROFILE_KEY, "smartess_profile_key", "smartess_protocol_profile_key"),
        (CONF_SMARTESS_DEVICE_ADDRESS, "smartess_device_address", "smartess_device_address"),
    )
    for config_key, detail_key, collector_attr in mapping:
        value = _pick(detail_key, collector_attr)
        if value is not None:
            data[config_key] = value


def _apply_detection_evidence_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist detection evidence so diagnostics can explain how the entry was made."""

    if result is None:
        return

    if result.match is not None:
        target = result.match.probe_target
        data["detected_probe_route"] = (
            f"{target.devcode}:{target.collector_addr}:{target.device_addr}"
        )

    detection = result.detection
    if detection is None:
        return
    data["detection_depth"] = detection.depth
    data["detection_status"] = detection.status
    if detection.budget_exhausted:
        data["detection_budget_exhausted"] = True
    candidate_drivers = detection.details.get("candidate_drivers")
    if isinstance(candidate_drivers, (list, tuple)) and len(candidate_drivers) > 1:
        data["detection_candidate_drivers"] = list(candidate_drivers)
    probe_log = detection.details.get("probe_log")
    if isinstance(probe_log, (list, tuple)) and probe_log:
        data["detection_probe_log"] = [dict(entry) for entry in probe_log if isinstance(entry, dict)]


def _apply_collector_cloud_family_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist the collector cloud family learned from CLDSRVHOST1/onboarding."""

    if result is None:
        return
    match_details = result.match.details if result.match is not None else {}
    family = str(match_details.get("collector_cloud_family") or "").strip()
    if not family and result.collector is not None and result.collector.collector is not None:
        family = str(result.collector.collector.collector_cloud_family or "").strip()
    if family:
        data[CONF_COLLECTOR_CLOUD_FAMILY] = family


def _smartess_collector_firmware_version_for_result(result: OnboardingResult | None) -> str:
    if result is None:
        return ""
    match_details = result.match.details if result.match is not None else {}
    value = str(match_details.get("smartess_collector_version") or "").strip()
    if value:
        return value
    collector_info = result.collector.collector if result.collector is not None else None
    if collector_info is None:
        return ""
    return str(collector_info.smartess_collector_version or "").strip()


def _apply_smartess_cloud_assist_metadata(
    data: dict[str, Any],
    assist_state: _SmartEssCloudAssistState | None,
) -> None:
    """Persist SmartESS cloud-assisted metadata hints for one onboarding entry."""

    if assist_state is None:
        return

    if assist_state.inferred_asset_id:
        data[CONF_SMARTESS_PROTOCOL_ASSET_ID] = assist_state.inferred_asset_id
    if assist_state.inferred_profile_key:
        data[CONF_SMARTESS_PROFILE_KEY] = assist_state.inferred_profile_key
    if (
        assist_state.inferred_driver_key
        and str(data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO) or DRIVER_HINT_AUTO) == DRIVER_HINT_AUTO
    ):
        data[CONF_DRIVER_HINT] = assist_state.inferred_driver_key


def _smartess_cloud_bundle_payload(evidence: dict[str, Any]) -> dict[str, Any]:
    payload = evidence.get("payload") if isinstance(evidence, dict) else None
    return payload if isinstance(payload, dict) else {}


def _smartess_cloud_device_preview(evidence: dict[str, Any]) -> dict[str, Any]:
    identity = evidence.get("device_identity") if isinstance(evidence, dict) else None
    identity = identity if isinstance(identity, dict) else {}
    normalized = _smartess_cloud_bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_list = normalized.get("device_list")
    normalized_list = normalized_list if isinstance(normalized_list, dict) else {}
    devices = normalized_list.get("devices")
    devices = devices if isinstance(devices, list) else []

    device_preview: dict[str, Any] = {}
    identity_pn = str(identity.get("pn") or "").strip()
    identity_sn = str(identity.get("sn") or "").strip()
    for item in devices:
        if not isinstance(item, dict):
            continue
        item_pn = str(item.get("pn") or "").strip()
        item_sn = str(item.get("sn") or "").strip()
        if identity_pn and item_pn == identity_pn:
            device_preview = item
            break
        if identity_sn and item_sn == identity_sn:
            device_preview = item
            break
    if not device_preview:
        for item in devices:
            if isinstance(item, dict):
                device_preview = item
                break

    return {
        "pn": identity_pn or str(device_preview.get("pn") or "").strip(),
        "sn": identity_sn or str(device_preview.get("sn") or "").strip(),
        "devcode": identity.get("devcode") if identity.get("devcode") not in (None, "") else device_preview.get("devcode"),
        "devaddr": identity.get("devaddr") if identity.get("devaddr") not in (None, "") else device_preview.get("devaddr"),
        "name": str(device_preview.get("devName") or "").strip(),
        "alias": str(device_preview.get("devalias") or "").strip(),
        "status": str(device_preview.get("status") or "").strip(),
        "brand": str(device_preview.get("brand") or "").strip(),
    }


def _smartess_cloud_detail_sections(evidence: dict[str, Any]) -> tuple[str, ...]:
    summary = evidence.get("summary") if isinstance(evidence, dict) else None
    summary = summary if isinstance(summary, dict) else {}
    normalized = _smartess_cloud_bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_detail = normalized.get("device_detail")
    normalized_detail = normalized_detail if isinstance(normalized_detail, dict) else {}
    section_counts = normalized_detail.get("section_counts")
    section_counts = section_counts if isinstance(section_counts, dict) else {}

    previews: list[str] = []
    if section_counts:
        for key in sorted(section_counts):
            previews.append(f"{key} ({section_counts[key]})")
    else:
        detail_sections = summary.get("detail_sections")
        if isinstance(detail_sections, list):
            previews.extend(str(item).strip() for item in detail_sections if str(item).strip())
    return tuple(previews)


def _smartess_cloud_highlight_settings(
    evidence: dict[str, Any],
    *,
    limit: int = 5,
) -> tuple[_SmartEssCloudSettingHighlight, ...]:
    normalized = _smartess_cloud_bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_settings = normalized.get("device_settings")
    normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
    fields = normalized_settings.get("fields")
    fields = fields if isinstance(fields, list) else []

    bucket_priority = {"exact_0925": 0, "probable_0925": 1, "cloud_only": 2}

    def _register_for_field(field: dict[str, Any]) -> int | None:
        binding = field.get("binding")
        if isinstance(binding, dict):
            register = binding.get("register")
            if isinstance(register, int):
                return register
        register = field.get("asset_register")
        if isinstance(register, int):
            return register
        return None

    def _choice_label(field: dict[str, Any], value: Any) -> str:
        choices = field.get("choices")
        if not isinstance(choices, list):
            return ""
        value_text = str(value)
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            if choice.get("value") == value:
                return str(choice.get("label") or "").strip()
            if str(choice.get("raw_value") or "") == value_text:
                return str(choice.get("label") or "").strip()
        return ""

    def _current_value_preview(field: dict[str, Any]) -> str:
        if not field.get("has_current_value"):
            return ""
        current_value = field.get("current_value")
        label = _choice_label(field, current_value)
        if label:
            return label
        text = str(current_value).strip()
        if not text:
            return ""
        unit = str(field.get("unit") or "").strip()
        return f"{text} {unit}".strip()

    candidates = [field for field in fields if isinstance(field, dict) and str(field.get("title") or "").strip()]
    candidates.sort(
        key=lambda field: (
            0 if field.get("has_current_value") else 1,
            bucket_priority.get(str(field.get("bucket") or ""), 9),
            0 if _register_for_field(field) is not None else 1,
            str(field.get("title") or "").lower(),
        )
    )

    highlights: list[_SmartEssCloudSettingHighlight] = []
    for field in candidates:
        highlights.append(
            _SmartEssCloudSettingHighlight(
                title=str(field.get("title") or "").strip(),
                bucket=str(field.get("bucket") or "").strip(),
                current_value=_current_value_preview(field),
                register=_register_for_field(field),
            )
        )
        if len(highlights) >= limit:
            break
    return tuple(highlights)


class _TranslationBundleMixin:
    """Shared translation loading helpers for config and options flows."""

    def _flow_language(self) -> str:
        language = str(getattr(self, "context", {}).get("language") or "")
        if not language:
            hass = getattr(self, "hass", None)
            language = str(getattr(getattr(hass, "config", None), "language", "") or "")
        return language or "en"

    async def _async_ensure_translation_bundle(self) -> None:
        language = self._flow_language()
        if getattr(self, "_translation_bundle_language", None) == language:
            cached_bundle = getattr(self, "_translation_bundle", None)
            if isinstance(cached_bundle, dict):
                return

        self._translation_bundle = await self.hass.async_add_executor_job(
            _load_translation_bundle,
            language,
        )
        self._translation_bundle_language = language

    def _tr(
        self,
        key: str,
        default: str,
        placeholders: dict[str, Any] | None = None,
    ) -> str:
        bundle: dict[str, Any] = {}
        if getattr(self, "_translation_bundle_language", None) == self._flow_language():
            cached_bundle = getattr(self, "_translation_bundle", None)
            if isinstance(cached_bundle, dict):
                bundle = cached_bundle
        value = _translation_lookup(bundle, key)
        text = value if isinstance(value, str) and value else default
        if placeholders:
            try:
                return text.format(**placeholders)
            except (KeyError, ValueError):
                try:
                    return default.format(**placeholders)
                except (KeyError, ValueError):
                    return default
        return text

# ---------------------------------------------------------------------------
# Selector helpers
# ---------------------------------------------------------------------------

_PORT_SELECTOR = NumberSelector(
    NumberSelectorConfig(min=1, max=65535, mode=NumberSelectorMode.BOX)
)

_DISCOVERY_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=1,
        max=60,
        step=1,
        unit_of_measurement="s",
        mode=NumberSelectorMode.SLIDER,
    )
)

_HEARTBEAT_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=5,
        max=3600,
        step=5,
        unit_of_measurement="s",
        mode=NumberSelectorMode.BOX,
    )
)

_POLL_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=2,
        max=3600,
        step=1,
        unit_of_measurement="s",
        mode=NumberSelectorMode.BOX,
    )
)

_PROXY_CAPTURE_DURATION_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=MIN_PROXY_CAPTURE_DURATION_MINUTES,
        max=MAX_PROXY_CAPTURE_DURATION_MINUTES,
        step=1,
        unit_of_measurement="min",
        mode=NumberSelectorMode.BOX,
    )
)

_IP_TEXT_SELECTOR = TextSelector(TextSelectorConfig())
_BLE_ADDRESS_TEXT_SELECTOR = TextSelector(TextSelectorConfig())


def _build_multiline_log_text_selector() -> TextSelector:
    try:
        return TextSelector(TextSelectorConfig(multiline=True, read_only=True))
    except TypeError:
        return TextSelector(TextSelectorConfig(multiline=True))


_MULTILINE_LOG_TEXT_SELECTOR = _build_multiline_log_text_selector()
_MULTILINE_TEXT_SELECTOR = TextSelector(TextSelectorConfig(multiline=True))
_PASSWORD_TEXT_SELECTOR = TextSelector(TextSelectorConfig(type="password"))

_BOOLEAN_SELECTOR = BooleanSelector()


def _coerce_int(value: Any) -> int | None:
    """Best-effort int coercion; returns ``None`` for empty/non-numeric values."""

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _smartess_credential_schema_fields(
    *,
    required: bool = True,
    username_default: str = "",
    password_default: str = "",
) -> dict:
    """Return one shared SmartESS-credential schema fragment for cloud-assist forms.

    Centralizes the username + password fields used in the cloud-assist step,
    the standalone evidence-export form, and the create-support-package form so
    selector wiring stays consistent across the three call sites.
    """

    marker = vol.Required if required else vol.Optional
    return {
        marker("username", default=username_default): _IP_TEXT_SELECTOR,
        marker("password", default=password_default): _PASSWORD_TEXT_SELECTOR,
    }


_DRIVER_DISPLAY_LABELS: dict[str, str] = {
    DRIVER_HINT_AUTO: "Auto",
    "modbus_smg": "SMG / Modbus",
    "srne_modbus": "SRNE / Modbus",
    "must_pv_ph18": "MUST PV/PH18",
    "modbus_catalog": "Device Catalog / Modbus",
    "smartess_local": "SmartESS 0925 / Modbus",
    "pi30": "PI30",
    "eybond_g_ascii": "EyeBond G-ASCII",
    "pi18": "PI18",
}


def _driver_selector(bundle: dict[str, Any] | None = None) -> SelectSelector:
    labels = _DRIVER_DISPLAY_LABELS
    options = [
        SelectOptionDict(
            value=opt,
            label=_selector_option_label(
                bundle,
                "driver_hint",
                opt,
                labels.get(opt, opt.replace("_", " ").title()),
            ),
        )
        for opt in driver_options()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _control_mode_selector(bundle: dict[str, Any] | None = None) -> SelectSelector:
    labels = {"auto": "Auto", "read_only": "Read only", "full": "Full control"}
    options = [
        SelectOptionDict(
            value=opt,
            label=_selector_option_label(bundle, "control_mode", opt, labels.get(opt, opt)),
        )
        for opt in control_mode_options()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _poll_mode_selector(bundle: dict[str, Any] | None = None) -> SelectSelector:
    labels = {POLL_MODE_AUTO: "Automatic", POLL_MODE_MANUAL: "Manual"}
    options = [
        SelectOptionDict(
            value=opt,
            label=_selector_option_label(bundle, "poll_mode", opt, labels.get(opt, opt)),
        )
        for opt in (POLL_MODE_AUTO, POLL_MODE_MANUAL)
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _interface_selector(interface_options: list[dict[str, str]]) -> SelectSelector:
    """Return a selector for known interfaces."""

    options = [
        SelectOptionDict(value=item["ip"], label=item["label"])
        for item in interface_options
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _result_selector(result_options: dict[str, str]) -> SelectSelector:
    """Return a selector for scan results."""

    options = [
        SelectOptionDict(value=key, label=label)
        for key, label in result_options.items()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_network_status_selector(
    already_connected_label: str,
    needs_bluetooth_label: str,
) -> SelectSelector:
    """Return a selector for choosing the collector network onboarding path."""

    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=COLLECTOR_NETWORK_ALREADY_CONNECTED, label=already_connected_label),
                SelectOptionDict(value=COLLECTOR_NETWORK_NEEDS_BLUETOOTH, label=needs_bluetooth_label),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_operation_mode_selector(
    smartess_and_ha_label: str,
    ha_only_label: str,
) -> SelectSelector:
    """Return a selector for choosing the collector callback ownership mode."""

    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=COLLECTOR_OPERATION_SMARTESS_AND_HA, label=smartess_and_ha_label),
                SelectOptionDict(value=COLLECTOR_OPERATION_HA_ONLY, label=ha_only_label),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _sort_ble_candidates(
    candidates: tuple[SmartEssBleCandidate, ...],
) -> tuple[SmartEssBleCandidate, ...]:
    return tuple(
        sorted(
            candidates,
            key=lambda candidate: (
                str(candidate.preferred_name or candidate.local_pn or candidate.address).lower(),
                str(candidate.address).lower(),
            ),
        )
    )


def _ble_candidate_label(
    candidate: SmartEssBleCandidate,
    *,
    already_added_label: str = "",
) -> str:
    parts: list[str] = []
    for part in (
        str(candidate.preferred_name or "").strip(),
        str(candidate.local_pn or "").strip(),
        str(candidate.address or "").strip(),
    ):
        if part and part not in parts:
            parts.append(part)
    label = " - ".join(parts)
    if already_added_label:
        label = f"{label} ({already_added_label})"
    return label


def _ble_candidate_by_address(
    candidates: tuple[SmartEssBleCandidate, ...],
    address: str,
) -> SmartEssBleCandidate | None:
    normalized_address = str(address or "").strip()
    return next((candidate for candidate in candidates if candidate.address == normalized_address), None)


def _ble_candidate_selector(
    candidates: tuple[SmartEssBleCandidate, ...],
    *,
    already_added_addresses: set[str] | None = None,
    already_added_label: str = "",
) -> SelectSelector:
    already_added_addresses = already_added_addresses or set()
    options = [
        *[
            SelectOptionDict(
                value=candidate.address,
                label=_ble_candidate_label(
                    candidate,
                    already_added_label=(
                        already_added_label if candidate.address in already_added_addresses else ""
                    ),
                ),
            )
            for candidate in _sort_ble_candidates(candidates)
        ],
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _ble_action_selector(
    *,
    rescan_label: str,
    refresh_label: str,
    apply_label: str,
) -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=BLE_ACTION_RESCAN, label=rescan_label),
                SelectOptionDict(value=BLE_ACTION_REFRESH_WIFI, label=refresh_label),
                SelectOptionDict(value=BLE_ACTION_APPLY, label=apply_label),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_wifi_action_selector(*, refresh_label: str, apply_label: str) -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=COLLECTOR_WIFI_ACTION_REFRESH, label=refresh_label),
                SelectOptionDict(value=COLLECTOR_WIFI_ACTION_APPLY, label=apply_label),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_uart_baudrate_selector() -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[SelectOptionDict(value=value, label=value) for value in COLLECTOR_UART_BAUDRATES],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_uart_action_selector(
    *,
    refresh_label: str,
    apply_label: str,
    include_apply: bool = True,
) -> SelectSelector:
    options = [SelectOptionDict(value=COLLECTOR_UART_ACTION_REFRESH, label=refresh_label)]
    if include_apply:
        options.append(SelectOptionDict(value=COLLECTOR_UART_ACTION_APPLY, label=apply_label))
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _coerce_proxy_capture_duration_minutes(
    value: object,
    *,
    default: int = DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    minimum: int = MIN_PROXY_CAPTURE_DURATION_MINUTES,
) -> int:
    try:
        minutes = int(round(float(value)))
    except (TypeError, ValueError):
        minutes = int(default)
    return max(minimum, min(MAX_PROXY_CAPTURE_DURATION_MINUTES, minutes))
def _ble_wifi_network_label(network: SmartEssBleWifiNetwork) -> str:
    signal_label = f"{network.signal}%" if 0 <= network.signal <= 100 else f"{network.signal} dBm"
    return f"{network.ssid} ({signal_label})"


def _ble_wifi_selector(networks: tuple[SmartEssBleWifiNetwork, ...]) -> SelectSelector:
    seen_ssids: set[str] = set()
    options: list[SelectOptionDict] = []
    for network in networks:
        ssid = str(network.ssid or "").strip()
        if not ssid or ssid in seen_ssids:
            continue
        seen_ssids.add(ssid)
        options.append(SelectOptionDict(value=ssid, label=_ble_wifi_network_label(network)))
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            custom_value=True,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _is_retryable_ble_wifi_scan_error(exc: SmartEssBleError) -> bool:
    code = str(exc)
    return code in {
        "ble_not_connected",
        "ble_notification_timeout",
    }


# ---------------------------------------------------------------------------
# Section helpers
# ---------------------------------------------------------------------------

def _flatten_sections(user_input: dict[str, Any]) -> dict[str, Any]:
    """Flatten section-nested user input into a flat dict."""

    flat: dict[str, Any] = {}
    for key, value in user_input.items():
        if isinstance(value, dict):
            flat.update(value)
        else:
            flat[key] = value
    for key in _INT_FIELDS:
        value = flat.get(key)
        if isinstance(value, (int, float)):
            flat[key] = int(value)
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                flat[key] = int(stripped)
    return flat


# ---------------------------------------------------------------------------
# Network utilities
# ---------------------------------------------------------------------------

def _get_local_ip() -> str:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except OSError:
        return ""


def _build_interface_entry(
    *,
    ifname: str,
    ip: str,
    prefixlen: int | None = None,
    broadcast: str = "",
) -> dict[str, str]:
    label = f"{ifname} — {ip}" if ifname else ip
    interface: dict[str, str] = {"name": ifname, "ip": ip, "label": label}
    if prefixlen is not None and 0 < prefixlen <= 32:
        try:
            network = ipaddress.ip_interface(f"{ip}/{prefixlen}").network
        except ValueError:
            network = None
        if network is not None:
            interface["prefixlen"] = str(prefixlen)
            interface["network"] = str(network)
            if prefixlen < 31:
                interface["broadcast"] = str(network.broadcast_address)
    if broadcast:
        interface["broadcast"] = broadcast
    return interface


def _dedupe_interfaces(interfaces: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[str, dict[str, str]] = {}
    for interface in interfaces:
        deduped.setdefault(interface["ip"], interface)
    return list(deduped.values())


def _parse_ipv4_interfaces_json(raw: list[dict[str, Any]]) -> list[dict[str, str]]:
    interfaces: list[dict[str, str]] = []
    for item in raw:
        ifname = str(item.get("ifname", "")).strip()
        if ifname and not _is_user_selectable_scan_interface(ifname):
            continue
        for addr in item.get("addr_info", []):
            ip = str(addr.get("local", "")).strip()
            if not ip:
                continue
            if addr.get("family") != "inet":
                continue
            if addr.get("scope") not in {"global", "site"}:
                continue
            if ip.startswith("127."):
                continue
            prefixlen_raw = addr.get("prefixlen")
            try:
                prefixlen = int(prefixlen_raw)
            except (TypeError, ValueError):
                prefixlen = None
            interfaces.append(
                _build_interface_entry(
                    ifname=ifname,
                    ip=ip,
                    prefixlen=prefixlen,
                    broadcast=str(addr.get("broadcast", "")).strip(),
                )
            )
    return _dedupe_interfaces(interfaces)


def _parse_ipv4_interfaces_oneline(output: str) -> list[dict[str, str]]:
    interfaces: list[dict[str, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _IP_ADDR_SHOW_ONELINE.match(line)
        if match is None:
            continue
        ip = str(match.group("ip") or "").strip()
        if not ip or ip.startswith("127."):
            continue
        ifname = str(match.group("ifname") or "").strip()
        if ifname and not _is_user_selectable_scan_interface(ifname):
            continue
        scope = str(match.group("scope") or "").strip()
        if scope not in {"global", "site"}:
            continue
        try:
            prefixlen = int(match.group("prefixlen"))
        except (TypeError, ValueError):
            prefixlen = None
        interfaces.append(
            _build_interface_entry(
                ifname=ifname,
                ip=ip,
                prefixlen=prefixlen,
                broadcast=str(match.group("broadcast") or "").strip(),
            )
        )
    return _dedupe_interfaces(interfaces)


def _get_ipv4_interfaces() -> list[dict[str, str]]:
    """Return active global IPv4 interfaces with human-friendly labels."""

    try:
        output = subprocess.check_output(
            ["ip", "-j", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        raw = json.loads(output)
        interfaces = _parse_ipv4_interfaces_json(raw)
        if interfaces:
            return interfaces
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        pass

    try:
        output = subprocess.check_output(
            ["ip", "-o", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        interfaces = _parse_ipv4_interfaces_oneline(output)
        if interfaces:
            return interfaces
    except (OSError, subprocess.SubprocessError):
        pass

    fallback_ip = _get_local_ip()
    if not fallback_ip:
        return []
    return [{"name": "default", "ip": fallback_ip, "label": fallback_ip}]


def _compute_broadcast_24(ip: str) -> str:
    parts = ip.split(".")
    if len(parts) != 4:
        return DEFAULT_DISCOVERY_TARGET
    return f"{parts[0]}.{parts[1]}.{parts[2]}.255"


def _sanitize_pending_collector_ip(
    collector_ip: str,
    *,
    server_ip: str = "",
    discovery_target: str = "",
) -> str:
    candidate = str(collector_ip).strip()
    if not candidate:
        return ""
    if candidate == DEFAULT_DISCOVERY_TARGET:
        return ""
    default_broadcast = _compute_broadcast_24(server_ip) if server_ip else ""
    if discovery_target and candidate == discovery_target and default_broadcast and candidate == default_broadcast:
        return ""
    return candidate


def _network_target_count(network_cidr: str, *, exclude: set[str] | None = None) -> int:
    try:
        network = ipaddress.ip_network(network_cidr, strict=False)
    except ValueError:
        return 0

    total = int(network.num_addresses)
    if network.prefixlen < 31:
        total = max(0, total - 2)

    excluded_count = 0
    for ip in exclude or set():
        if not ip:
            continue
        try:
            address = ipaddress.ip_address(ip)
        except ValueError:
            continue
        if address not in network:
            continue
        if network.prefixlen < 31 and address in {network.network_address, network.broadcast_address}:
            continue
        excluded_count += 1
    return max(0, total - excluded_count)


def _is_ipv4(ip: str) -> bool:
    try:
        socket.inet_aton(ip)
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Config flow
# ---------------------------------------------------------------------------

class EybondLocalConfigFlow(_TranslationBundleMixin, ConfigFlow, domain=DOMAIN):
    """Create a config entry for an inverter behind an EyeBond collector."""

    VERSION = 1

    def __init__(self) -> None:
        self._translation_bundle: dict[str, Any] = {}
        self._translation_bundle_language = ""
        self._local_ip = ""
        self._default_broadcast = DEFAULT_DISCOVERY_TARGET
        self._interface_options: list[dict[str, str]] = []
        self._auto_config: dict[str, Any] = {}
        self._manual_defaults: dict[str, Any] = {}
        self._manual_config: dict[str, Any] = {}
        self._manual_result: OnboardingResult | None = None
        self._autodetect_results: dict[str, OnboardingResult] = {}
        self._selected_result: OnboardingResult | None = None
        self._selected_result_runtime_details_attempted = False
        self._selected_result_collector_capabilities_attempted = False
        self._scan_task: asyncio.Task | None = None
        self._scan_error: bool = False
        self._scan_mode = SETUP_MODE_AUTO
        self._scan_timeout_seconds = _AUTO_SCAN_TIMEOUT
        self._scan_started_monotonic: float | None = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._ble_last_error = ""
        self._ble_local_adapter_available = False
        self._ble_ha_backend_available = False
        self._ble_selected_address = ""
        self._ble_wifi_networks_by_address: dict[str, tuple[SmartEssBleWifiNetwork, ...]] = {}
        self._ble_fw_version_by_address: dict[str, str] = {}
        self._ble_wifi_scan_attempted_addresses: set[str] = set()
        self._ble_wifi_scan_failed_addresses: set[str] = set()
        self._collector_operation_mode = ""
        self._collector_original_server_endpoint = ""
        self._collector_current_server_endpoint = ""
        self._collector_target_server_endpoint = ""
        self._collector_endpoint_error = ""
        self._collector_endpoint_bind_applied = False
        self._smartess_cloud_assist: _SmartEssCloudAssistState | None = None
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""
        self._smartess_cloud_assist_last_error_code = ""
        self._detection_summary_context = "auto"
        self._confirm_poll_interval_pending_input: dict[str, Any] = {}
        self._confirm_poll_interval_pending_step_id = "confirm"

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return EybondLocalOptionsFlow(config_entry)

    # ---- step: user (welcome) ----

    @_with_translation_bundle
    async def _async_refresh_force_unsupported_override(self) -> None:
        """Re-read the on-device force-unsupported sentinel for flow-time detection.

        The integration's async_setup only runs after the first entry exists, so
        on a fresh install the very first config flow would otherwise ignore the
        force_unsupported.flag sentinel. Refresh it once here (in an executor —
        it stats a file) so the validation toggle works during onboarding too.
        """

        if getattr(self, "_force_unsupported_refreshed", False):
            return
        self._force_unsupported_refreshed = True
        from .metadata.device_catalog_loader import refresh_force_unsupported_override

        with suppress(Exception):
            config_root = Path(self.hass.config.path("eybond_local")).resolve()
            await self.hass.async_add_executor_job(
                refresh_force_unsupported_override, config_root
            )

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_refresh_force_unsupported_override()
        await self._async_ensure_network_defaults()

        def _select_connection_type(connection_type: str) -> None:
            self._auto_config = {CONF_CONNECTION_TYPE: connection_type}
            if len(self._interface_options) == 1:
                self._auto_config[CONF_SERVER_IP] = self._local_ip

        if user_input is not None:
            _select_connection_type(
                str(
                    user_input.get(
                        CONF_CONNECTION_TYPE,
                        self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND),
                    )
                )
            )
            return await self.async_step_collector_network()

        supported = supported_connection_types()
        if len(supported) == 1:
            # One connection type: a welcome screen with a single-option
            # dropdown asks nothing — go straight to network readiness.
            _select_connection_type(str(supported[0]))
            return await self.async_step_collector_network()

        data_schema = vol.Schema(
            {
                vol.Required(
                    CONF_CONNECTION_TYPE,
                    default=self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND),
                ): self._connection_type_selector(),
            }
        )
        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            description_placeholders=self._welcome_description_placeholders(),
        )

    # ---- step: collector_network ----

    @_with_translation_bundle
    async def async_step_collector_network(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        return self.async_show_menu(
            step_id="collector_network",
            menu_options=["auto", "bluetooth_setup"],
            description_placeholders=self._collector_network_placeholders(),
        )

    # ---- step: auto (choose interface → trigger scan) ----

    @_with_translation_bundle
    async def async_step_auto(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if self._scan_error:
            errors = {"base": "cannot_autodetect"}
            self._scan_error = False

        single_interface = len(self._interface_options) == 1

        def _start_auto_scan() -> ConfigFlowResult:
            self._set_scan_mode(SETUP_MODE_AUTO)
            self._reset_scan_progress()
            return self.async_step_scanning()

        if user_input is not None:
            effective = dict(user_input)
            effective.setdefault(CONF_SERVER_IP, self._local_ip)
            self._normalize_current_server_ip(effective)
            input_errors = self._validate_connection_inputs(
                effective,
                fields=self._connection_branch().form_layout.auto_fields,
            )
            if input_errors:
                errors.update(input_errors)
            else:
                self._auto_config.update(effective)
                return await _start_auto_scan()
        elif single_interface and not errors:
            # One interface and nothing to ask: start scanning immediately so
            # the happy path is Welcome -> (collector ready) -> results.
            self._auto_config.setdefault(CONF_SERVER_IP, self._local_ip)
            self._normalize_current_server_ip(self._auto_config)
            return await _start_auto_scan()

        data_schema = vol.Schema(
            self._build_connection_fields_schema(
                self._current_connection_type(),
                fields=self._connection_branch().form_layout.auto_fields,
                values=self._auto_connection_defaults(),
            )
        )

        return self.async_show_form(
            step_id="auto",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._auto_description_placeholders(single_interface),
        )

    @_with_translation_bundle
    async def async_step_bluetooth_setup(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        ble_candidates: tuple[SmartEssBleCandidate, ...] = ()
        wifi_networks: tuple[SmartEssBleWifiNetwork, ...] = ()
        previous_ble_address = self._ble_selected_address
        defaults = dict(user_input or {})
        selected_ble_value = str(defaults.get(CONF_BLE_ADDRESS, "") or "").strip()
        selected_ble_action = str(
            defaults.get(CONF_BLE_ACTION, BLE_ACTION_APPLY) or BLE_ACTION_APPLY
        ).strip()
        if selected_ble_action not in {BLE_ACTION_RESCAN, BLE_ACTION_REFRESH_WIFI, BLE_ACTION_APPLY}:
            selected_ble_action = BLE_ACTION_APPLY
        rescan_requested = user_input is not None and selected_ble_action == BLE_ACTION_RESCAN
        refresh_requested = user_input is not None and selected_ble_action == BLE_ACTION_REFRESH_WIFI
        apply_requested = user_input is not None and selected_ble_action == BLE_ACTION_APPLY

        # Refreshing the Wi-Fi list should also clear stale Wi-Fi values and re-run
        # nearby collector discovery before the selected collector is queried again.
        if refresh_requested:
            defaults.pop(CONF_WIFI_SSID, None)
            defaults.pop(CONF_WIFI_PASSWORD, None)

        submitted_ssid = str(defaults.get(CONF_WIFI_SSID, "") or "").strip()
        submitted_password = str(defaults.get(CONF_WIFI_PASSWORD, "") or "")

        if refresh_requested or rescan_requested:
            self._ble_last_error = ""
            selected_ble_value = selected_ble_value or previous_ble_address
        if refresh_requested:
            self._ble_wifi_scan_attempted_addresses.clear()
            self._ble_wifi_scan_failed_addresses.clear()

        capability = await self._async_probe_ble_setup_capability()
        if not capability.available:
            self._ble_last_error = str(capability.detail or capability.reason or "").strip()
            errors["base"] = "ble_unavailable"
        else:
            try:
                ble_candidates = await self._async_discover_smartess_ble_candidates(
                    force_active_scan=rescan_requested or refresh_requested,
                )
            except SmartEssBleError as exc:
                errors["base"] = self._ble_flow_error_key(exc)

        default_ble_address = selected_ble_value
        if ble_candidates:
            candidate_addresses = {candidate.address for candidate in ble_candidates}
            if default_ble_address not in candidate_addresses:
                default_ble_address = ble_candidates[0].address
            ble_address_selector: SelectSelector | TextSelector = _ble_candidate_selector(
                ble_candidates,
                already_added_addresses=self._already_added_ble_candidate_addresses(ble_candidates),
                already_added_label=self._tr("common.dynamic.status_already_added", "Already added"),
            )
            ble_address_marker: vol.Marker = vol.Required(
                CONF_BLE_ADDRESS,
                default=default_ble_address,
            )
        else:
            ble_address_selector = _BLE_ADDRESS_TEXT_SELECTOR
            ble_address_marker = vol.Optional(CONF_BLE_ADDRESS, default=default_ble_address)

        self._ble_selected_address = str(default_ble_address or "").strip()
        already_added_addresses = self._already_added_ble_candidate_addresses(ble_candidates)

        selected_candidate = _ble_candidate_by_address(ble_candidates, default_ble_address)
        selected_already_added = default_ble_address in already_added_addresses
        if selected_already_added and user_input is not None:
            errors[CONF_BLE_ADDRESS] = "already_added_candidate"

        should_scan_selected_wifi = (
            default_ble_address
            and not errors
            and not selected_already_added
            and (user_input is None or refresh_requested)
        )
        if should_scan_selected_wifi:
            cached_wifi_networks = self._ble_wifi_networks_by_address.get(default_ble_address, ())
            try:
                wifi_networks = await self._async_scan_smartess_ble_wifi_networks(
                    default_ble_address,
                    ble_device=selected_candidate.device if selected_candidate is not None else None,
                )
                self._ble_wifi_networks_by_address[default_ble_address] = wifi_networks
                self._ble_wifi_scan_failed_addresses.discard(default_ble_address)
                self._ble_last_error = ""
            except SmartEssBleError as exc:
                self._ble_wifi_scan_failed_addresses.add(default_ble_address)
                self._ble_last_error = str(exc)
                if cached_wifi_networks:
                    wifi_networks = cached_wifi_networks
                else:
                    errors["base"] = self._ble_flow_error_key(exc)
                logger.info(
                    "SmartESS BLE Wi-Fi scan unavailable address=%s error=%s",
                    default_ble_address,
                    exc,
                )
            finally:
                self._ble_wifi_scan_attempted_addresses.add(default_ble_address)
        elif default_ble_address in self._ble_wifi_networks_by_address:
            wifi_networks = self._ble_wifi_networks_by_address[default_ble_address]

        if refresh_requested or rescan_requested:
            selected_ble_action = BLE_ACTION_APPLY

        if user_input is not None and not errors:
            if apply_requested:
                if not default_ble_address:
                    errors[CONF_BLE_ADDRESS] = "ble_address_invalid"
                if not submitted_ssid:
                    errors[CONF_WIFI_SSID] = "ble_wifi_ssid_invalid"
                if not submitted_password:
                    errors[CONF_WIFI_PASSWORD] = "ble_wifi_password_invalid"

            if apply_requested and not errors:
                selected_candidate = _ble_candidate_by_address(ble_candidates, default_ble_address)
                try:
                    await self._async_run_smartess_ble_bootstrap(
                        ble_address=default_ble_address,
                        ssid=submitted_ssid,
                        password=submitted_password,
                        ble_device=selected_candidate.device if selected_candidate is not None else None,
                    )
                except SmartEssBleError as exc:
                    self._ble_last_error = str(exc)
                    errors["base"] = self._ble_flow_error_key(exc)
                else:
                    self._ble_last_error = ""
                    return await self.async_step_auto()

        default_wifi_ssid = submitted_ssid
        if not default_wifi_ssid and wifi_networks:
            default_wifi_ssid = wifi_networks[0].ssid
        wifi_ssid_selector = _ble_wifi_selector(wifi_networks)

        data_schema: dict[vol.Marker, Any] = {
            ble_address_marker: ble_address_selector,
            vol.Optional(CONF_WIFI_SSID, default=default_wifi_ssid): wifi_ssid_selector,
        }
        data_schema[
            vol.Optional(CONF_WIFI_PASSWORD, default=str(defaults.get(CONF_WIFI_PASSWORD, "")))
        ] = _PASSWORD_TEXT_SELECTOR
        data_schema[
            vol.Required(CONF_BLE_ACTION, default=selected_ble_action)
        ] = _ble_action_selector(
            rescan_label=self._bluetooth_rescan_action_label(),
            refresh_label=self._bluetooth_refresh_wifi_action_label(),
            apply_label=self._bluetooth_apply_action_label(),
        )

        return self.async_show_form(
            step_id="bluetooth_setup",
            data_schema=vol.Schema(data_schema),
            errors=errors,
            description_placeholders=self._bluetooth_setup_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_deep_scan(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        self._set_scan_mode(SETUP_MODE_DEEP_SCAN)
        plan = self._deep_scan_plan()
        if plan["network_cidr"] and plan["target_count"] and not plan["large_subnet"]:
            # A known, normally sized network needs no confirmation step; the
            # menu remains only where the user must decide something: a large
            # subnet (long scan), or an unknown network (change interface /
            # go manual).
            return await self.async_step_start_deep_scan()
        menu_options = ["start_deep_scan"]
        if len(self._interface_options) > 1:
            menu_options.append("change_scan_interface")
        menu_options.append("manual")
        return self.async_show_menu(
            step_id="deep_scan",
            menu_options=menu_options,
            description_placeholders=self._deep_scan_placeholders(),
        )

    async def async_step_start_deep_scan(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        self._set_scan_mode(SETUP_MODE_DEEP_SCAN)
        self._reset_scan_progress()
        return await self.async_step_scanning()

    # ---- step: scanning (progress indicator) ----

    @_with_translation_bundle
    async def async_step_scanning(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._scan_task is None:
            self._scan_started_monotonic = time.monotonic()
            self._scan_progress_stage = "preparing"
            self._scan_progress_visible = False
            self.async_update_progress(0.0)
            self._scan_task = self.hass.async_create_task(
                self._async_do_scan()
            )

        selected_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        selected_label = self._selected_interface_label(selected_ip)

        if not self._scan_progress_visible:
            self._scan_progress_visible = True
            return self.async_show_progress(
                step_id="scanning",
                progress_action="scanning_network",
                progress_task=self._scan_task,
                description_placeholders=self._scan_progress_placeholders(selected_label),
            )

        if self._scan_task.done():
            self._scan_started_monotonic = None
            self._scan_progress_visible = False
            if self._scan_task.exception():
                self._scan_error = True
            elif not self._autodetect_results:
                self._scan_error = True
            return self.async_show_progress_done(next_step_id="scan_results")

        return self.async_show_progress(
            step_id="scanning",
            progress_action="scanning_network",
            progress_task=self._scan_task,
            description_placeholders=self._scan_progress_placeholders(selected_label),
        )

    async def _async_do_scan(self) -> None:
        """Run auto-detection in the background."""

        effective_input = self._auto_config
        server_ip = str(effective_input.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        discovery_targets = self._scan_discovery_targets()
        deep_scan_plan = self._deep_scan_plan()
        scan_timeout = self._scan_timeout_seconds
        detector_timeout = max(5.0, scan_timeout - 5.0)
        if self._scan_mode != SETUP_MODE_DEEP_SCAN:
            detector_timeout = min(detector_timeout, 40.0)
        else:
            # The deep-scan deadline extends itself for every connected
            # collector it admits; the admission headroom (policy hard
            # ceiling) sits on top of the discovery budget so a /16 sweep
            # does not consume the identification budget. The outer guard
            # only has to stop a runaway scan, not pace it.
            scan_timeout = (
                scan_timeout
                + _ONBOARDING_TIMEOUT_POLICY.deep_scan_hard_ceiling_seconds
                + 30.0
            )
        self._scan_progress_stage = "discovering"
        progress_updater = asyncio.create_task(self._async_update_scan_progress_loop())
        detector = create_onboarding_manager(
            build_connection_spec_from_values(
                self._current_connection_type(),
                dict(self._auto_connection_defaults(), **effective_input),
            ),
            driver_hint=DRIVER_HINT_AUTO,
        )
        skip_probe_ips = self._configured_collector_probe_skip_ips()
        try:
            async with _async_timeout(scan_timeout):
                if self._scan_mode == SETUP_MODE_DEEP_SCAN:
                    results = await detector.async_deep_detect(
                        discovery_targets=discovery_targets,
                        unicast_network_cidr=deep_scan_plan["network_cidr"],
                        enrich_runtime_details=True,
                        total_timeout=detector_timeout,
                        skip_probe_ips=skip_probe_ips,
                    )
                else:
                    results = await detector.async_auto_detect(
                        discovery_targets=discovery_targets,
                        attempts=1,
                        enrich_runtime_details=False,
                        total_timeout=detector_timeout,
                        skip_probe_ips=skip_probe_ips,
                    )
        except TimeoutError:
            logger.warning(
                "%s scan timed out after %.1fs server_ip=%s discovery_targets=%s network=%s",
                self._scan_mode,
                scan_timeout,
                server_ip,
                ",".join(target.ip for target in discovery_targets),
                deep_scan_plan["network_cidr"] or "-",
            )
            self._scan_progress_stage = "finalizing"
            self._autodetect_results = {}
            return
        finally:
            progress_updater.cancel()
            with suppress(asyncio.CancelledError):
                await progress_updater
        self._scan_progress_stage = "analyzing"
        self.async_update_progress(0.9)
        await asyncio.sleep(0.08)
        visible_results = self._collapse_scan_results(
            result
            for result in results
            if self._is_visible_scan_result(result)
        )

        if not visible_results:
            self._scan_progress_stage = "finalizing"
            self._autodetect_results = {}
            return

        connected_collectors = [
            result
            for result in visible_results
            if result.collector is not None and result.collector.connected
        ]
        matched = [result for result in visible_results if result.match is not None]

        self._autodetect_results = {
            str(index): result
            for index, result in enumerate(self._sort_scan_results(visible_results))
        }
        self._scan_progress_stage = "finalizing"
        self.async_update_progress(0.99)
        await asyncio.sleep(0.08)
        self._set_selected_result(None)

        if not matched and not connected_collectors:
            best_result = visible_results[0] if visible_results else None
            self._manual_defaults = self._build_manual_defaults(effective_input, best_result)
        self.async_update_progress(1.0)
        await asyncio.sleep(0.12)

    # ---- step: scan_results ----

    @_with_translation_bundle
    async def async_step_scan_results(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """One screen: pick a found device directly, or pick a follow-up action."""

        available_results = self._available_autodetect_results()

        errors: dict[str, str] = {}
        if user_input is not None:
            selection = str(user_input.get(CONF_RESULT_KEY) or "")
            if selection == _SCAN_RESULTS_ACTION_REFRESH:
                return await self.async_step_refresh_scan()
            if selection == _SCAN_RESULTS_ACTION_ADVANCED:
                return await self.async_step_advanced_setup()
            result = available_results.get(selection)
            if result is None:
                errors["base"] = "invalid_selection"
            elif self._existing_entry_for_result(result) is not None:
                errors["base"] = "already_added_candidate"
            elif _result_indicates_inverter_link_down(result):
                errors["base"] = "inverter_link_down"
            else:
                self._set_selected_result(result)
                self._detection_summary_context = "auto"
                if self._selected_result_needs_driver_choice():
                    return await self.async_step_driver_choice()
                return await self.async_step_detection_summary()

        options: dict[str, str] = {
            key: self._result_label(result)
            for key, result in available_results.items()
        }
        options[_SCAN_RESULTS_ACTION_REFRESH] = self._refresh_scan_action_label()
        options[_SCAN_RESULTS_ACTION_ADVANCED] = self._scan_action_label(
            "advanced_setup", "Device not found? Advanced setup"
        )
        data_schema = vol.Schema(
            {
                vol.Required(CONF_RESULT_KEY): _result_selector(options),
            }
        )
        return self.async_show_form(
            step_id="scan_results",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._scan_results_placeholders(),
        )

    # ---- step: advanced_setup (power-user fallbacks) ----

    @_with_translation_bundle
    async def async_step_advanced_setup(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Power-user fallbacks when auto-scan did not find the device."""

        menu_options: list[str] = ["deep_scan"]
        if len(self._interface_options) > 1:
            menu_options.append("change_scan_interface")
        menu_options.append("manual")
        menu_options.append("refresh_scan")
        return self.async_show_menu(
            step_id="advanced_setup",
            menu_options=menu_options,
            description_placeholders=self._scan_results_placeholders(),
        )

    # ---- step: change_scan_interface ----

    @_with_translation_bundle
    async def async_step_change_scan_interface(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if user_input is not None:
            effective = dict(self._auto_config)
            effective.update(user_input)
            input_errors = self._validate_connection_inputs(
                effective,
                fields=self._connection_branch().form_layout.auto_fields,
            )
            if input_errors:
                errors.update(input_errors)
            else:
                self._auto_config.update(user_input)
                self._set_scan_mode(self._scan_mode)
                self._reset_scan_progress()
                return await self.async_step_scanning()

        data_schema = vol.Schema(
            self._build_connection_fields_schema(
                self._current_connection_type(),
                fields=self._connection_branch().form_layout.auto_fields,
                values=self._auto_connection_defaults(),
            )
        )
        return self.async_show_form(
            step_id="change_scan_interface",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._auto_description_placeholders(False),
        )

    # ---- step: refresh_scan ----

    async def async_step_refresh_scan(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        if not self._auto_config:
            self._auto_config = self._auto_connection_defaults()
        self._set_scan_mode(self._scan_mode)
        self._reset_scan_progress()
        return await self.async_step_scanning()

    # ---- step: choose ----

    @_with_translation_bundle
    async def async_step_choose(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._autodetect_results:
            return await self.async_step_auto()

        available_results = self._available_autodetect_results()
        if not available_results:
            return await self.async_step_scan_results()

        errors: dict[str, str] = {}
        if user_input is None and len(available_results) == 1:
            candidate = next(iter(available_results.values()))
            if _result_indicates_inverter_link_down(candidate):
                # Collector answered but the inverter link is down: never
                # classify, let the user fix cabling/power and rescan.
                errors["base"] = "inverter_link_down"
            else:
                self._set_selected_result(candidate)
                self._detection_summary_context = "auto"
                if self._selected_result_needs_driver_choice():
                    return await self.async_step_driver_choice()
                return await self.async_step_detection_summary()

        if user_input is not None:
            selected_key = user_input[CONF_RESULT_KEY]
            result = available_results.get(selected_key)
            if result is None:
                errors["base"] = "invalid_selection"
            elif self._existing_entry_for_result(result) is not None:
                errors["base"] = "already_added_candidate"
            elif _result_indicates_inverter_link_down(result):
                errors["base"] = "inverter_link_down"
            else:
                self._set_selected_result(result)
                self._detection_summary_context = "auto"
                if self._selected_result_needs_driver_choice():
                    return await self.async_step_driver_choice()
                return await self.async_step_detection_summary()

        options = {
            key: self._result_label(result)
            for key, result in available_results.items()
        }
        data_schema = vol.Schema(
            {
                vol.Required(CONF_RESULT_KEY): _result_selector(options),
            }
        )
        return self.async_show_form(
            step_id="choose",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._choose_placeholders(),
        )

    # ---- step: driver_choice ----

    @_with_translation_bundle
    async def async_step_driver_choice(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Let the user choose between multiple successful deep-scan drivers."""

        if self._selected_result is None:
            return await self.async_step_auto()
        candidates = self._driver_choice_candidates(self._selected_result)
        if len(candidates) <= 1:
            return await self.async_step_detection_summary()

        errors: dict[str, str] = {}
        if user_input is not None:
            selected_key = str(user_input.get(CONF_DRIVER_MATCH_KEY) or "").strip()
            selected_match = self._driver_choice_match_by_key(
                self._selected_result,
                selected_key,
            )
            if selected_match is None:
                errors["base"] = "invalid_selection"
            else:
                updated_result = self._selected_result_with_match(
                    self._selected_result,
                    selected_match,
                )
                # Keep the autodetect registry pointing at the updated result:
                # the confirm-time runtime-detail refresh only runs for results
                # it can find there, and it must probe with the chosen driver.
                for result_key, stored in self._autodetect_results.items():
                    if stored is self._selected_result:
                        self._autodetect_results[result_key] = updated_result
                        break
                self._set_selected_result(updated_result)
                return await self.async_step_detection_summary()

        include_address = self._driver_choice_needs_address(candidates)
        options = {
            self._driver_choice_key(match): self._driver_choice_label(
                match,
                recommended=index == 0,
                include_address=include_address,
            )
            for index, match in enumerate(candidates)
        }
        data_schema = vol.Schema(
            {
                vol.Required(CONF_DRIVER_MATCH_KEY): _result_selector(options),
            }
        )
        return self.async_show_form(
            step_id="driver_choice",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._driver_choice_placeholders(self._selected_result),
        )

    # ---- step: detection_summary ----

    @_with_translation_bundle
    async def async_step_detection_summary(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Tell the user WHAT was identified and WHICH support tier applies."""

        if self._detection_summary_result() is None:
            if self._detection_summary_context == "manual":
                return await self.async_step_manual()
            return await self.async_step_auto()
        if user_input is not None:
            if self._detection_summary_context == "manual":
                if not self._manual_config:
                    return await self.async_step_manual()
                return await self._async_create_manual_entry(
                    self._manual_config, self._manual_result
                )
            return await self.async_step_confirm()

        # Auto path: when SmartESS cloud assist can refine the identification,
        # offer it here as an optional choice instead of forcing it on the user.
        if self._detection_summary_context == "auto" and self._can_offer_smartess_cloud_assist(
            self._detection_summary_result()
        ):
            self._smartess_cloud_assist_mode = "auto"
            return self.async_show_menu(
                step_id="detection_summary",
                menu_options=["confirm", "smartess_cloud_assist"],
                description_placeholders=self._detection_summary_placeholders(),
            )

        return self.async_show_form(
            step_id="detection_summary",
            data_schema=vol.Schema({}),
            description_placeholders=self._detection_summary_placeholders(),
        )

    def _detection_summary_result(self) -> OnboardingResult | None:
        if self._detection_summary_context == "manual":
            return self._manual_result
        return self._selected_result

    def _detection_summary_placeholders(self) -> dict[str, str]:
        result = self._detection_summary_result()
        if result is None:
            return {"model": "", "tier_headline": "", "tier_details": ""}

        match = result.match
        catalog: dict[str, Any] = {}
        if match is not None and isinstance(match.details.get("device_catalog"), dict):
            catalog = match.details["device_catalog"]
        kind = str(catalog.get("kind") or "")
        tier = str(catalog.get("tier") or "")

        if kind == "device" and tier == "full":
            headline = self._tr(
                "common.dynamic.detection_tier_full_headline",
                "Full support",
            )
            details = self._tr(
                "common.dynamic.detection_tier_full_details",
                "This model is in the built-in device catalog. Read sensors and "
                "controls will be added out of the box.",
            )
        elif kind in ("device", "family") and tier == "partial":
            headline = self._tr(
                "common.dynamic.detection_tier_partial_headline",
                "Partial support (family match)",
            )
            details = self._tr(
                "common.dynamic.detection_tier_partial_details",
                "The inverter family is recognized, but this exact model is not in "
                "the catalog yet. Base read sensors will be added; controls stay "
                "locked for safety.\n\n"
                "Next step: after you finish here, open this integration and choose "
                "**Configure → Expand device support** to discover extra "
                "controls and sensor evidence.",
            )
        elif match is not None:
            headline = self._tr(
                "common.dynamic.detection_tier_driver_headline",
                "Detected by protocol driver",
            )
            details = self._tr(
                "common.dynamic.detection_tier_driver_details",
                "The device was identified by its protocol driver. The standard "
                "sensor set for this driver will be added.",
            )
        else:
            headline = self._tr(
                "common.dynamic.detection_tier_unidentified_headline",
                "Device not recognized",
            )
            details = self._tr(
                "common.dynamic.detection_tier_unidentified_details",
                "The collector responds, but no inverter was detected through the "
                "selected driver/catalog path. This can mean the inverter is not "
                "connected, is powered off, uses an unsupported protocol, or only "
                "the collector is present.\n\n"
                "You can still add a Pending Device to keep diagnostics available. "
                "Device learning is useful only after an inverter has actually "
                "been detected.\n\n"
                "Next step: check the inverter connection, then create a Support "
                "Archive if the device still cannot be identified.",
            )

        model = ""
        if match is not None and match.model_name:
            model = match.model_name
        else:
            model = self._result_label(result)
        return {"model": model, "tier_headline": headline, "tier_details": details}

    # ---- step: confirm ----

    @_with_translation_bundle
    async def async_step_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        # Cloud assist is no longer an interstitial that auto-pops before the
        # confirm form; it is an explicit choice on the detection summary.
        return await self._async_show_confirm_form(step_id="confirm", user_input=user_input)

    @_with_translation_bundle
    async def async_step_confirm_without_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        return await self._async_show_confirm_form(
            step_id="confirm_without_cloud_assist",
            user_input=user_input,
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        result = self._smartess_cloud_assist_context_result()
        if result is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_auto()

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                self._smartess_cloud_assist = await self._async_run_smartess_cloud_assist(
                    result,
                    username=str(user_input.get("username") or "").strip(),
                    password=str(user_input.get("password") or ""),
                )
                self._smartess_cloud_assist_last_error = ""
                self._smartess_cloud_assist_last_error_code = ""
            except Exception as exc:
                self._smartess_cloud_assist_last_error = str(exc)
                self._smartess_cloud_assist_last_error_code = (
                    classify_smartess_cloud_error(exc)
                )
                errors["base"] = "smartess_cloud_assist_failed"
            else:
                return await self.async_step_smartess_cloud_assist_summary()

        return self.async_show_form(
            step_id="smartess_cloud_assist",
            data_schema=vol.Schema(_smartess_credential_schema_fields()),
            description_placeholders=self._smartess_cloud_assist_placeholders(result),
            errors=errors or None,
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist_summary(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        result = self._smartess_cloud_assist_context_result()
        if result is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_confirm()

        if self._smartess_cloud_assist_state_for_result(result) is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_confirm()

        menu_options = ["manual_confirm"] if self._smartess_cloud_assist_mode == "manual" else ["confirm"]
        return self.async_show_menu(
            step_id="smartess_cloud_assist_summary",
            menu_options=menu_options,
            description_placeholders=self._smartess_cloud_assist_summary_placeholders(result),
        )

    async def _async_show_confirm_form(
        self,
        *,
        step_id: str,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            return await self.async_step_auto()

        await self._async_refresh_selected_result_collector_capabilities()

        # First add is intentionally not the place to choose Cloud+HA vs HA-only.
        # Discovery evidence can be partial at this point, especially for local
        # bridges and collector-only candidates. Keep the confirm form stable:
        # only collect the poll interval here. Runtime/options flow owns the
        # operation-mode UX once the entry has had a chance to read endpoint and
        # capability metadata. A detected virtual bridge is still forced HA-only
        # internally because it has no SmartESS cloud side.
        selected_capabilities = _result_collector_capabilities(self._selected_result)
        is_bridge = selected_capabilities.virtual_bridge

        errors: dict[str, str] = {}
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            poll_mode = str(flat_input.get(CONF_POLL_MODE, DEFAULT_POLL_MODE) or DEFAULT_POLL_MODE)
            if poll_mode not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
                errors[CONF_POLL_MODE] = "invalid_selection"
            elif poll_mode == POLL_MODE_MANUAL and CONF_POLL_INTERVAL not in flat_input:
                self._confirm_poll_interval_pending_input = dict(flat_input)
                self._confirm_poll_interval_pending_step_id = step_id
                return await self.async_step_confirm_poll_interval()
            if is_bridge:
                mode = COLLECTOR_OPERATION_HA_ONLY
            else:
                # Ignore any stale/hidden operation-mode value posted by an old
                # form. The mode can be changed later in the options flow.
                mode = self._collector_operation_mode or DEFAULT_COLLECTOR_OPERATION_MODE
            if mode == COLLECTOR_OPERATION_HA_ONLY and not self._collector_endpoint_bind_applied:
                self._collector_operation_mode = mode
                self._reset_collector_endpoint_binding_state()
                try:
                    # For a bridge, writing the HA server endpoint is still how
                    # the bridge is told where to connect — keep the bind.
                    # Modern bridge firmware accepts and persists the FC=3
                    # param-21 endpoint write. Older bridge firmware may refuse
                    # it; keep that refusal non-fatal for bridge upgrades.
                    await self._async_bind_selected_collector_to_home_assistant(
                        allow_refused_endpoint_write=is_bridge,
                    )
                except Exception as exc:
                    self._collector_endpoint_error = _exception_detail(exc)
                    errors["base"] = "collector_endpoint_write_failed"
                else:
                    self._collector_endpoint_bind_applied = True
                    return await self._async_create_entry_from_result(flat_input)
            else:
                self._collector_operation_mode = mode
                return await self._async_create_entry_from_result(flat_input)

        description_placeholders = dict(self._collector_operation_placeholders())
        if is_bridge:
            description_placeholders["collector_operation_mode_note"] = self._tr(
                "common.dynamic.collector_operation_mode_bridge_note",
                "Local bridge — always Home Assistant only; it has no SmartESS "
                "cloud side.",
            )
        else:
            description_placeholders.setdefault("collector_operation_mode_note", "")
        schema: dict[Any, Any] = {
            vol.Required(CONF_POLL_MODE, default=DEFAULT_POLL_MODE): _poll_mode_selector(
                self._translation_bundle,
            ),
        }
        return self.async_show_form(
            step_id=step_id,
            data_schema=vol.Schema(schema),
            errors=errors,
            description_placeholders=description_placeholders,
        )

    @_with_translation_bundle
    async def async_step_confirm_poll_interval(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            return await self.async_step_auto()
        pending = dict(self._confirm_poll_interval_pending_input)
        step_id = self._confirm_poll_interval_pending_step_id or "confirm"
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            pending[CONF_POLL_INTERVAL] = flat_input.get(
                CONF_POLL_INTERVAL,
                DEFAULT_POLL_INTERVAL,
            )
            return await self._async_show_confirm_form(
                step_id=step_id,
                user_input=pending,
            )
        return self.async_show_form(
            step_id="confirm_poll_interval",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_POLL_INTERVAL,
                        default=pending.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL),
                    ): _POLL_INTERVAL_SELECTOR,
                }
            ),
            errors={},
        )

    # ---- step: manual ----

    @_with_translation_bundle
    async def async_step_manual(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            self._normalize_current_server_ip(flat_input)
            errors = self._validate_connection_inputs(
                flat_input,
                fields=self._connection_branch().form_layout.manual_fields
                + self._connection_branch().form_layout.manual_advanced_fields,
            )
            if not errors:
                self._manual_config = dict(flat_input)
                self._manual_result = await self._async_probe_manual_target(flat_input)
                if self._manual_result.match is not None and self._manual_result.confidence == "high":
                    self._detection_summary_context = "manual"
                    return await self.async_step_detection_summary()
                return await self.async_step_manual_confirm()

        defaults = self._build_manual_defaults(user_input, self._selected_result)
        data_schema = vol.Schema(
            {
                **self._build_connection_fields_schema(
                    self._current_connection_type(),
                    fields=self._connection_branch().form_layout.manual_fields,
                    values=defaults,
                ),
                vol.Required("advanced_connection"): section(
                    vol.Schema(
                        self._build_connection_fields_schema(
                            self._current_connection_type(),
                            fields=self._connection_branch().form_layout.manual_advanced_fields,
                            values=defaults,
                        )
                    ),
                    {"collapsed": True},
                ),
            }
        )

        return self.async_show_form(
            step_id="manual",
            data_schema=data_schema,
            errors=errors,
        )

    # ---- step: manual_confirm ----

    @_with_translation_bundle
    async def async_step_manual_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        menu_options = [
            MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
            MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
        ]
        if self._can_offer_smartess_cloud_assist(self._manual_result):
            menu_options.append("manual_smartess_cloud_assist")
        menu_options.append(MANUAL_CONFIRM_ACTION_CREATE_PENDING)

        return self.async_show_menu(
            step_id="manual_confirm",
            menu_options=menu_options,
            description_placeholders=self._manual_confirm_placeholders(
                self._manual_config,
                self._manual_result,
            ),
        )

    @_with_translation_bundle
    async def async_step_manual_smartess_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()
        self._smartess_cloud_assist_mode = "manual"
        return await self.async_step_smartess_cloud_assist()

    async def async_step_manual_probe_again(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        self._manual_result = await self._async_probe_manual_target(self._manual_config)
        if self._manual_result.match is not None and self._manual_result.confidence == "high":
            self._detection_summary_context = "manual"
            return await self.async_step_detection_summary()
        return await self.async_step_manual_confirm()

    async def async_step_manual_edit_settings(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        self._manual_defaults = dict(self._manual_config)
        self._manual_result = None
        return await self.async_step_manual()

    async def async_step_manual_create_pending(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()
        return await self._async_create_manual_entry(self._manual_config, self._manual_result)

    # ---- entry creation ----

    async def _async_create_entry_from_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            raise RuntimeError("no_selected_result")

        result = self._selected_result
        existing_entry = self._existing_entry_for_result(result)
        if existing_entry is not None:
            return self.async_abort(reason="already_configured")
        collector_ip = result.collector.ip if result.collector is not None else ""
        collector_pn = self._collector_pn_for_result(result)
        driver_hint = (
            result.match.driver_key
            if result.match is not None
            else self._auto_config.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO)
        )
        assist_state = self._smartess_cloud_assist_state_for_result(result)
        if result.match is None and driver_hint == DRIVER_HINT_AUTO and assist_state is not None and assist_state.inferred_driver_key:
            driver_hint = assist_state.inferred_driver_key

        unique_id = self._result_unique_id(result)
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        title = installation_title(
            collector_pn=collector_pn,
            collector_ip=collector_ip or self._auto_config.get(CONF_COLLECTOR_IP, ""),
            detected_model=result.match.model_name if result.match is not None else "",
            detected_serial=result.match.serial_number if result.match is not None else "",
        )

        connection_type = result.connection_type or self._current_connection_type()
        connection_settings = with_driver_hint(
            build_detected_entry_settings(
                connection_type,
                server_ip=self._auto_config[CONF_SERVER_IP],
                collector_ip=collector_ip or self._auto_config.get(CONF_COLLECTOR_IP, ""),
                default_broadcast=_compute_broadcast_24(self._auto_config[CONF_SERVER_IP]),
                overrides=self._auto_config,
            ),
            driver_hint=driver_hint,
        )
        collector_capabilities = _result_collector_capabilities(result)
        data = {
            CONF_CONNECTION_TYPE: connection_type,
            **connection_settings,
            CONF_CONNECTION_MODE: "known_ip" if collector_ip else result.connection_mode,
            CONF_CONTROL_MODE: DEFAULT_CONTROL_MODE,
            CONF_COLLECTOR_OPERATION_MODE: (
                COLLECTOR_OPERATION_HA_ONLY
                if collector_capabilities.ha_only_required
                else self._collector_operation_mode or DEFAULT_COLLECTOR_OPERATION_MODE
            ),
            CONF_COLLECTOR_PN: collector_pn,
            CONF_DETECTION_CONFIDENCE: result.confidence,
            CONF_DETECTED_MODEL: result.match.model_name if result.match is not None else "",
            CONF_DETECTED_SERIAL: result.match.serial_number if result.match is not None else "",
        }
        if collector_capabilities.virtual_bridge:
            data["collector_virtual_bridge"] = True
            data["collector_bridge_kind"] = "esp-collector"
        _apply_smartess_detection_metadata(data, result)
        _apply_collector_cloud_family_metadata(data, result)
        _apply_device_catalog_metadata(data, result)
        _apply_smartess_cloud_assist_metadata(data, assist_state)
        _apply_detection_evidence_metadata(data, result)
        poll_interval = int((user_input or {}).get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
        poll_mode = str((user_input or {}).get(CONF_POLL_MODE, DEFAULT_POLL_MODE) or DEFAULT_POLL_MODE)
        if poll_mode not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
            poll_mode = DEFAULT_POLL_MODE
        options = {
            CONF_POLL_INTERVAL: poll_interval,
            CONF_POLL_MODE: poll_mode,
            CONF_COLLECTOR_OPERATION_MODE: (
                COLLECTOR_OPERATION_HA_ONLY
                if collector_capabilities.ha_only_required
                else self._collector_operation_mode or DEFAULT_COLLECTOR_OPERATION_MODE
            ),
        }
        remembered_endpoint = str(self._collector_original_server_endpoint or "").strip()
        target_endpoint = str(self._collector_target_server_endpoint or self._collector_callback_target_endpoint()).strip()
        if (
            self._collector_operation_mode == COLLECTOR_OPERATION_HA_ONLY
            and remembered_endpoint
            and remembered_endpoint != target_endpoint
        ):
            original_endpoint_options = self._collector_original_endpoint_options(
                remembered_endpoint
            )
            options.update(original_endpoint_options)
            with suppress(Exception):
                await self._async_remember_collector_original_endpoint_in_registry(
                    collector_pn=collector_pn,
                    endpoint=remembered_endpoint,
                    options=original_endpoint_options,
                )
        return self.async_create_entry(
            title=title,
            data=data,
            options=options,
        )

    async def _async_create_manual_entry(
        self,
        user_input: dict[str, Any],
        result: OnboardingResult | None = None,
    ) -> ConfigFlowResult:
        result = result or self._manual_result
        if result is not None:
            existing_entry = self._existing_entry_for_result(result)
            if existing_entry is not None:
                return self.async_abort(reason="already_configured")
        collector_ip = user_input.get(CONF_COLLECTOR_IP, "")
        collector_pn = ""
        detected_model = ""
        detected_serial = ""
        driver_hint = user_input.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO)
        connection_mode = "manual"

        if result is not None:
            connection_mode = result.connection_mode or connection_mode
            if result.collector is not None:
                collector_ip = result.collector.ip or collector_ip
                collector_info = result.collector.collector
                if collector_info is not None and collector_info.collector_pn:
                    collector_pn = collector_info.collector_pn
            if result.match is not None:
                detected_model = result.match.model_name
                detected_serial = result.match.serial_number
                driver_hint = result.match.driver_key or driver_hint

        assist_state = self._smartess_cloud_assist_state_for_result(result)
        if result is not None and result.match is None and driver_hint == DRIVER_HINT_AUTO and assist_state is not None and assist_state.inferred_driver_key:
            driver_hint = assist_state.inferred_driver_key

        collector_ip = _sanitize_pending_collector_ip(
            collector_ip,
            server_ip=str(user_input.get(CONF_SERVER_IP, "")),
            discovery_target=str(user_input.get(CONF_DISCOVERY_TARGET, "")),
        )

        unique_id = (
            f"collector:{collector_pn}"
            if collector_pn
            else f"inverter:{detected_serial}"
            if detected_serial
            else f"manual:{collector_ip}"
            if collector_ip
            else f"listener:{user_input[CONF_SERVER_IP]}:{user_input[CONF_TCP_PORT]}"
        )
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        title = (
            "EyeBond Setup Pending"
            if not (collector_pn or detected_model or detected_serial)
            else installation_title(
                collector_pn=collector_pn,
                collector_ip=collector_ip,
                detected_model=detected_model,
                detected_serial=detected_serial,
            )
        )

        connection_type = result.connection_type if result is not None else self._current_connection_type()
        data = with_driver_hint(
            build_manual_entry_settings(connection_type, user_input),
            driver_hint=driver_hint,
        )
        data.setdefault(CONF_CONNECTION_TYPE, connection_type)
        default_control_mode = (
            DEFAULT_CONTROL_MODE
            if result is not None and result.confidence == "high"
            else CONTROL_MODE_READ_ONLY
        )
        data.setdefault(CONF_CONTROL_MODE, default_control_mode)
        collector_capabilities = _result_collector_capabilities(result)
        data[CONF_COLLECTOR_OPERATION_MODE] = (
            COLLECTOR_OPERATION_HA_ONLY
            if collector_capabilities.ha_only_required
            else DEFAULT_COLLECTOR_OPERATION_MODE
        )
        data[CONF_COLLECTOR_IP] = collector_ip
        data[CONF_DETECTION_CONFIDENCE] = result.confidence if result is not None else "none"
        data[CONF_CONNECTION_MODE] = connection_mode
        data[CONF_COLLECTOR_PN] = collector_pn
        data[CONF_DETECTED_MODEL] = detected_model
        data[CONF_DETECTED_SERIAL] = detected_serial
        if collector_capabilities.virtual_bridge:
            data["collector_virtual_bridge"] = True
            data["collector_bridge_kind"] = "esp-collector"
        _apply_smartess_detection_metadata(data, result)
        _apply_collector_cloud_family_metadata(data, result)
        _apply_device_catalog_metadata(data, result)
        _apply_smartess_cloud_assist_metadata(data, assist_state)
        options = {
            CONF_POLL_INTERVAL: DEFAULT_POLL_INTERVAL,
            CONF_POLL_MODE: DEFAULT_POLL_MODE,
            CONF_COLLECTOR_OPERATION_MODE: (
                COLLECTOR_OPERATION_HA_ONLY
                if collector_capabilities.ha_only_required
                else DEFAULT_COLLECTOR_OPERATION_MODE
            ),
        }
        return self.async_create_entry(title=title, data=data, options=options)

    # ---- probe ----

    async def _async_probe_manual_target(
        self,
        user_input: dict[str, Any],
    ) -> OnboardingResult:
        """Run one-shot detection using the manual settings before creating an entry."""

        detector = create_onboarding_manager(
            build_connection_spec_from_values(
                self._current_connection_type(),
                build_manual_entry_settings(self._current_connection_type(), user_input),
            ),
            driver_hint=user_input[CONF_DRIVER_HINT],
        )
        collector_ip = user_input.get(CONF_COLLECTOR_IP, "")
        discovery_target = "" if collector_ip else user_input.get(CONF_DISCOVERY_TARGET, "")
        try:
            async with _async_timeout(_MANUAL_PROBE_TIMEOUT):
                results = await detector.async_auto_detect(
                    collector_ip=collector_ip,
                    discovery_target=discovery_target,
                    attempts=1,
                    connect_timeout=3.5,
                    heartbeat_timeout=1.5,
                    total_timeout=_MANUAL_PROBE_TIMEOUT,
                )
        except TimeoutError:
            logger.warning(
                "Manual onboarding probe timed out after %.1fs server_ip=%s collector_ip=%s discovery_target=%s driver_hint=%s",
                _MANUAL_PROBE_TIMEOUT,
                user_input.get(CONF_SERVER_IP, ""),
                collector_ip or "-",
                discovery_target or "-",
                user_input.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            return OnboardingResult(
                connection_type=self._current_connection_type(),
                connection_mode="manual",
                next_action="create_pending_entry",
                last_error="manual_probe_timeout",
            )
        if results:
            return results[0]

        return OnboardingResult(
            connection_type=self._current_connection_type(),
            connection_mode="manual",
            next_action="create_pending_entry",
            last_error="manual_target_not_confirmed",
        )

    # ---- network defaults ----

    async def _async_ensure_network_defaults(self) -> None:
        if not self._local_ip or not self._interface_options:
            self._interface_options = await self.hass.async_add_executor_job(_get_ipv4_interfaces)
            detected_local_ip = await self.hass.async_add_executor_job(_get_local_ip)

            if self._interface_options:
                preferred = next(
                    (
                        item["ip"]
                        for item in self._interface_options
                        if item["ip"] == detected_local_ip
                    ),
                    self._interface_options[0]["ip"],
                )
                self._local_ip = preferred
            elif detected_local_ip:
                self._local_ip = detected_local_ip

        if self._local_ip:
            self._default_broadcast = self._selected_interface_broadcast(self._local_ip)

        if not isinstance(self._auto_config, dict):
            self._auto_config = {}

        interface_ips = {
            str(item.get("ip") or "").strip()
            for item in self._interface_options
            if str(item.get("ip") or "").strip()
        }
        configured_server_ip = str(self._auto_config.get(CONF_SERVER_IP, "") or "").strip()
        if self._local_ip and (not configured_server_ip or configured_server_ip not in interface_ips):
            self._auto_config[CONF_SERVER_IP] = self._local_ip

    def _home_assistant_bluetooth_module(self) -> object | None:
        try:
            return importlib.import_module("homeassistant.components.bluetooth")
        except Exception:
            return None

    def _hass_bluetooth_scanner_count(self, bluetooth: object | None = None) -> int:
        bluetooth = bluetooth or self._home_assistant_bluetooth_module()
        if bluetooth is None:
            return 0

        count = 0
        scanner_count = getattr(bluetooth, "async_scanner_count", None)
        if callable(scanner_count):
            for kwargs in ({"connectable": True}, {"connectable": False}, {}):
                try:
                    value = scanner_count(self.hass, **kwargs)
                except TypeError:
                    if kwargs:
                        continue
                    try:
                        value = scanner_count(self.hass)
                    except Exception:
                        continue
                except Exception:
                    continue
                try:
                    count = max(count, int(value))
                except (TypeError, ValueError):
                    continue

        current_scanners = getattr(bluetooth, "async_current_scanners", None)
        if callable(current_scanners):
            for kwargs in ({"connectable": True}, {"connectable": False}, {}):
                try:
                    value = current_scanners(self.hass, **kwargs)
                except TypeError:
                    if kwargs:
                        continue
                    try:
                        value = current_scanners(self.hass)
                    except Exception:
                        continue
                except Exception:
                    continue
                if isinstance(value, dict):
                    count = max(count, len(value))
                    continue
                if value is None:
                    continue
                try:
                    count = max(count, len(tuple(value)))
                except TypeError:
                    continue

        return count

    def _hass_bluetooth_backend_capability(self) -> SmartEssBleHostCapability | None:
        bluetooth = self._home_assistant_bluetooth_module()
        if bluetooth is None:
            return None

        scanner_count = self._hass_bluetooth_scanner_count(bluetooth)
        if scanner_count > 0:
            return SmartEssBleHostCapability(
                available=True,
                backend="home_assistant_bluetooth",
                reason="ha_bluetooth_scanners_available",
                detail=f"{scanner_count} Home Assistant Bluetooth scanner(s) available",
            )

        if self._hass_bluetooth_service_infos(bluetooth) or self._hass_bluetooth_devices(bluetooth):
            return SmartEssBleHostCapability(
                available=True,
                backend="home_assistant_bluetooth",
                reason="ha_bluetooth_cache_available",
                detail="Home Assistant Bluetooth already has cached devices",
            )

        return SmartEssBleHostCapability(
            available=False,
            backend="home_assistant_bluetooth",
            reason="ha_bluetooth_unavailable",
        )

    async def _async_probe_ble_setup_capability(self) -> SmartEssBleHostCapability:
        local_capability = await async_probe_ble_host_capability()
        self._ble_local_adapter_available = bool(getattr(local_capability, "available", False))

        ha_capability = self._hass_bluetooth_backend_capability()
        self._ble_ha_backend_available = bool(ha_capability is not None and ha_capability.available)

        if self._ble_local_adapter_available:
            if isinstance(local_capability, SmartEssBleHostCapability):
                return local_capability
            return SmartEssBleHostCapability(
                available=True,
                backend=str(getattr(local_capability, "backend", "bleak") or "bleak"),
                reason=str(getattr(local_capability, "reason", "backend_available") or "backend_available"),
                detail=str(getattr(local_capability, "detail", "") or ""),
            )

        if ha_capability is not None and ha_capability.available:
            return ha_capability

        if isinstance(local_capability, SmartEssBleHostCapability):
            return local_capability
        return SmartEssBleHostCapability(
            available=False,
            backend=str(getattr(local_capability, "backend", "bleak") or "bleak"),
            reason=str(getattr(local_capability, "reason", "ble_unavailable") or "ble_unavailable"),
            detail=str(getattr(local_capability, "detail", "") or ""),
        )

    def _hass_bluetooth_device_from_address(self, address: str) -> object | None:
        bluetooth = self._home_assistant_bluetooth_module()
        if bluetooth is None:
            return None

        resolve_device = getattr(bluetooth, "async_ble_device_from_address", None)
        if not callable(resolve_device):
            return None

        normalized_address = str(address or "").strip()
        if not normalized_address:
            return None

        try:
            return resolve_device(self.hass, normalized_address, connectable=True)
        except TypeError:
            try:
                return resolve_device(self.hass, normalized_address)
            except Exception:
                return None
        except Exception:
            return None

    def _build_manual_defaults(
        self,
        user_input: dict[str, Any] | None,
        result: OnboardingResult | None,
    ) -> dict[str, Any]:
        collector_ip = ""
        driver_hint = DRIVER_HINT_AUTO
        if result is not None and result.collector is not None:
            collector_ip = result.collector.ip
        if result is not None and result.match is not None:
            driver_hint = result.match.driver_key
        defaults = self._connection_branch().build_manual_base_values(
            server_ip=str(self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip),
            default_broadcast=self._selected_interface_broadcast(),
            stored_defaults=self._manual_defaults,
            collector_ip=collector_ip,
            driver_hint=driver_hint,
        )
        if self._auto_config:
            defaults[CONF_SERVER_IP] = self._auto_config.get(CONF_SERVER_IP, defaults[CONF_SERVER_IP])
        if user_input is not None:
            flat = _flatten_sections(user_input)
            self._normalize_current_server_ip(flat)
            defaults.update(flat)
        self._manual_defaults = defaults
        return defaults

    def _normalize_current_server_ip(self, values: MutableMapping[str, Any]) -> None:
        if not self._local_ip:
            return
        interface_ips = {
            str(item.get("ip") or "").strip()
            for item in self._interface_options
            if str(item.get("ip") or "").strip()
        }
        if not interface_ips:
            return
        configured_server_ip = str(values.get(CONF_SERVER_IP, "") or "").strip()
        if configured_server_ip and configured_server_ip in interface_ips:
            return
        values[CONF_SERVER_IP] = self._local_ip

    # ---- selector helpers ----

    def _server_ip_field(self) -> SelectSelector | TextSelector:
        """Return the most user-friendly selector for choosing the local server IP."""

        if not self._interface_options:
            return _IP_TEXT_SELECTOR
        return _interface_selector(self._interface_options)

    def _connection_type_selector(self) -> SelectSelector:
        """Return a selector for supported connection branches."""

        options = [
            SelectOptionDict(
                value=connection_type,
                label=get_connection_branch(connection_type).display.integration_name,
            )
            for connection_type in supported_connection_types()
        ]
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    def _collector_network_status_selector(self) -> SelectSelector:
        return _collector_network_status_selector(
            self._tr(
                "common.dynamic.collector_network_already_connected",
                "Yes, the collector is already on this network",
            ),
            self._tr(
                "common.dynamic.collector_network_needs_bluetooth",
                "No, connect the collector to Wi-Fi using Bluetooth first (test mode, only for collectors with Bluetooth support)",
            ),
        )

    def _collector_operation_mode_selector(self) -> SelectSelector:
        return _collector_operation_mode_selector(
            self._tr(
                "common.dynamic.collector_operation_smartess_and_ha",
                "SmartESS cloud + Home Assistant",
            ),
            self._tr(
                "common.dynamic.collector_operation_ha_only",
                "Home Assistant only",
            ),
        )

    def _selected_result_is_virtual_bridge(self) -> bool:
        """Return True when the selected onboarding result is a detected bridge.

        Reads the bridge verdict carried from the onboarding hardware token (see
        ``onboarding/eybond.py``). Positive-only and fail-safe: a factory
        collector or an unread token leaves no verdict, so the confirm step
        behaves exactly as before — the runtime path still corrects identity and
        menu gating after the entry runs.
        """

        return _result_is_virtual_bridge(self._selected_result)

    def _reset_scan_progress(self) -> None:
        """Reset scan-progress bookkeeping before one new scan attempt starts."""

        self._scan_task = None
        self._scan_started_monotonic = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._ble_last_error = ""
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""
        self._smartess_cloud_assist_last_error_code = ""

    def _set_selected_result(self, result: OnboardingResult | None) -> None:
        """Persist the selected onboarding result and reset lazy confirm refresh state."""

        self._selected_result = result
        self._selected_result_runtime_details_attempted = False
        self._selected_result_collector_capabilities_attempted = False

    @staticmethod
    def _driver_choice_candidates(result: OnboardingResult) -> tuple:
        candidates = tuple(
            match
            for match in (result.match, *result.alternative_matches)
            if match is not None
        )
        seen: set[str] = set()
        deduped = []
        for match in candidates:
            key = EybondLocalConfigFlow._driver_choice_key(match)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(match)
        return tuple(deduped)

    def _selected_result_needs_driver_choice(self) -> bool:
        result = self._selected_result
        return bool(result is not None and len(self._driver_choice_candidates(result)) > 1)

    @staticmethod
    def _driver_choice_key(match) -> str:
        target = match.probe_target
        return (
            f"{match.driver_key}|{match.variant_key}|"
            f"{target.devcode}:{target.collector_addr}:{target.device_addr}"
        )

    @staticmethod
    def _driver_choice_match_by_key(result: OnboardingResult, selected_key: str):
        for match in EybondLocalConfigFlow._driver_choice_candidates(result):
            if EybondLocalConfigFlow._driver_choice_key(match) == selected_key:
                return match
        return None

    @staticmethod
    def _selected_result_with_match(result: OnboardingResult, selected_match) -> OnboardingResult:
        alternatives = tuple(
            match
            for match in EybondLocalConfigFlow._driver_choice_candidates(result)
            if EybondLocalConfigFlow._driver_choice_key(match)
            != EybondLocalConfigFlow._driver_choice_key(selected_match)
        )
        detection = result.detection
        if detection is not None:
            detection = replace(
                detection,
                status="matched",
                reason=selected_match.driver_key,
                details={
                    **dict(detection.details),
                    "selected_driver": selected_match.driver_key,
                },
            )
        return replace(
            result,
            match=selected_match,
            alternative_matches=alternatives,
            detection=detection,
        )

    @staticmethod
    def _driver_choice_needs_address(candidates) -> bool:
        """Show the device address only when it is the distinguishing detail."""

        keys = {(match.driver_key, match.variant_key) for match in candidates}
        return len(keys) < len(candidates)

    def _driver_display_name(self, driver_key: str) -> str:
        """Human driver name (protocol family / wire protocol), not the raw key."""

        return self._tr(
            f"selector.driver_hint.options.{driver_key}",
            _DRIVER_DISPLAY_LABELS.get(driver_key, driver_key),
        )

    def _driver_choice_base_label(self, match) -> str:
        model_name = match.model_name or self._unconfirmed_inverter_label()
        driver_label = self._driver_display_name(match.driver_key)
        # Drivers qualify their model names with the protocol family already
        # ("PI30 4200", "... (SmartESS 0925)"); repeating the driver name next
        # to it reads as a duplicate, so only append it when it adds anything.
        family_token = driver_label.split("/", 1)[0].strip().lower()
        if family_token and family_token in model_name.lower():
            return model_name
        return self._tr(
            "common.dynamic.driver_choice_option",
            "{model_name} — {driver_key}",
            {
                "driver_key": driver_label,
                "model_name": model_name,
            },
        )

    def _driver_choice_label(
        self,
        match,
        *,
        recommended: bool = False,
        include_address: bool = False,
    ) -> str:
        """Short dropdown label; the details live in the numbered list above."""

        label = self._driver_choice_base_label(match)
        extras: list[str] = []
        if recommended:
            extras.append(
                self._tr("common.dynamic.driver_choice_recommended", "recommended")
            )
        if include_address:
            extras.append(
                self._tr(
                    "common.dynamic.driver_choice_device_address",
                    "device address {device_addr}",
                    {"device_addr": match.probe_target.device_addr},
                )
            )
        if extras:
            return f"{label} ({', '.join(extras)})"
        return label

    def _driver_choice_line(
        self,
        index: int,
        match,
        *,
        recommended: bool,
        include_address: bool,
    ) -> str:
        """One numbered description line with the human-readable details."""

        confidence = self._confidence_label(match.confidence)
        # The shared confidence label is capitalized for standalone use; here
        # it sits mid-sentence, so lowercase the first letter.
        confidence = confidence[:1].lower() + confidence[1:]
        parts = [f"**{self._driver_choice_base_label(match)}**", confidence]
        probe_elapsed_ms = match.details.get("probe_elapsed_ms")
        if isinstance(probe_elapsed_ms, (int, float)) and probe_elapsed_ms > 0:
            parts.append(
                self._tr(
                    "common.dynamic.driver_choice_probe_seconds",
                    "answered in {seconds}s",
                    {"seconds": f"{float(probe_elapsed_ms) / 1000.0:.1f}"},
                )
            )
        if include_address:
            parts.append(
                self._tr(
                    "common.dynamic.driver_choice_device_address",
                    "device address {device_addr}",
                    {"device_addr": match.probe_target.device_addr},
                )
            )
        line = f"{index}. " + " · ".join(parts)
        if recommended:
            line += " — " + self._tr(
                "common.dynamic.driver_choice_recommended",
                "recommended",
            )
        return line

    def _selected_collector_ip(self) -> str:
        result = self._selected_result
        if result is None or result.collector is None:
            return ""
        return str(result.collector.ip or result.collector.target_ip or "").strip()

    def _selected_result_needs_runtime_details(self, result: OnboardingResult) -> bool:
        """Return whether the selected auto-detected result is still missing confirm-time details."""

        match = result.match
        if match is None:
            return False

        details = match.details
        required_key_groups = (
            ("collector_signal_strength", "signal_strength_dbm"),
            ("rated_power", "output_rating_active_power"),
            ("battery_connected", "battery_connection_state"),
            ("battery_percent",),
        )
        return any(
            self._onboarding_first_present_value(details, *keys) in (None, "")
            for keys in required_key_groups
        )

    def _merge_selected_result_runtime_details(
        self,
        current_result: OnboardingResult,
        refreshed_result: OnboardingResult,
    ) -> OnboardingResult:
        """Merge confirm-time runtime details into the currently selected result."""

        current_match = current_result.match
        refreshed_match = refreshed_result.match
        if current_match is None or refreshed_match is None:
            return current_result
        if refreshed_match.driver_key != current_match.driver_key:
            return current_result
        if (
            current_match.serial_number
            and refreshed_match.serial_number
            and refreshed_match.serial_number != current_match.serial_number
        ):
            return current_result

        merged_details = dict(current_match.details)
        merged_details.update(refreshed_match.details)
        merged_match = replace(current_match, details=merged_details)
        merged_collector = refreshed_result.collector or current_result.collector
        return replace(current_result, collector=merged_collector, match=merged_match)

    async def _async_refresh_selected_result_runtime_details(self) -> None:
        """Fetch missing confirm-time runtime details for the selected auto-detected result."""

        selected_result = self._selected_result
        if selected_result is None or selected_result.match is None:
            return
        if self._selected_result_runtime_details_attempted:
            return
        if selected_result not in self._autodetect_results.values():
            return

        self._selected_result_runtime_details_attempted = True
        if not self._selected_result_needs_runtime_details(selected_result):
            return

        collector_ip = self._selected_collector_ip()
        if not collector_ip:
            return

        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(self._current_connection_type(), values)
        detector = create_onboarding_manager(
            spec,
            driver_hint=selected_result.match.driver_key or DRIVER_HINT_AUTO,
        )
        try:
            async with _async_timeout(_CONFIRM_RUNTIME_DETAILS_TIMEOUT):
                refreshed_result = await detector.async_handoff_detect(
                    collector_ip=collector_ip,
                    attempts=1,
                    connect_timeout=3.5,
                    heartbeat_timeout=1.5,
                    enrich_runtime_details=True,
                    cleanup_new_shared_connection=True,
                )
        except TimeoutError:
            logger.debug(
                "Selected-result runtime detail refresh timed out collector_ip=%s timeout=%.1fs",
                collector_ip,
                _CONFIRM_RUNTIME_DETAILS_TIMEOUT,
            )
            return
        except Exception as exc:
            logger.debug(
                "Selected-result runtime detail refresh failed collector_ip=%s error=%s",
                collector_ip,
                exc,
            )
            return

        if refreshed_result is None or refreshed_result.match is None:
            return

        self._selected_result = self._merge_selected_result_runtime_details(
            selected_result,
            refreshed_result,
        )

    async def _async_refresh_selected_result_collector_capabilities(self) -> None:
        """Fetch missing collector capability evidence before rendering confirm."""

        selected_result = self._selected_result
        if selected_result is None or selected_result.collector is None:
            return
        if self._selected_result_collector_capabilities_attempted:
            return
        if not self._autodetect_results or selected_result is self._manual_result:
            return
        if _result_collector_capabilities(selected_result).virtual_bridge:
            return

        collector_ip = self._selected_collector_ip()
        if not collector_ip:
            return

        self._selected_result_collector_capabilities_attempted = True

        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(self._current_connection_type(), values)
        collector_pn = self._collector_pn_for_result(selected_result)
        details: dict[str, object] = {}
        payload_transport = SharedEybondTransport(
            host="0.0.0.0",
            port=getattr(spec, "tcp_port", DEFAULT_TCP_PORT),
            request_timeout=min(float(getattr(spec, "request_timeout", DEFAULT_REQUEST_TIMEOUT)), 3.0),
            heartbeat_interval=float(getattr(spec, "heartbeat_interval", DEFAULT_HEARTBEAT_INTERVAL)),
            collector_ip=collector_ip,
            collector_pn=collector_pn,
        )
        at_transport = SharedCollectorAtTransport(
            host="0.0.0.0",
            port=getattr(spec, "tcp_port", DEFAULT_TCP_PORT),
            request_timeout=min(float(getattr(spec, "request_timeout", DEFAULT_REQUEST_TIMEOUT)), 3.0),
            collector_ip=collector_ip,
            collector_pn=collector_pn,
        )
        try:
            await payload_transport.start()
            await at_transport.start()
        except Exception as exc:
            logger.debug(
                "Selected-result collector capability transport unavailable collector_ip=%s error=%s",
                collector_ip,
                exc,
            )
            with suppress(Exception):
                await at_transport.stop()
            with suppress(Exception):
                await payload_transport.stop()
            return

        try:
            async with _async_timeout(4.0):
                collector_parameters = tuple(
                    COLLECTOR_PARAMETER_DEFINITION_BY_ID[parameter]
                    for parameter in (6, 21)
                    if parameter in COLLECTOR_PARAMETER_DEFINITION_BY_ID
                )
                details.update(
                    await query_runtime_collector_values(
                        SmartEssLocalSession(payload_transport),
                        parameters=collector_parameters,
                    )
                )
        except TimeoutError:
            logger.debug(
                "Selected-result collector FC capability refresh timed out collector_ip=%s",
                collector_ip,
            )
        except Exception as exc:
            logger.debug(
                "Selected-result collector FC capability refresh failed collector_ip=%s error=%s",
                collector_ip,
                exc,
            )

        try:
            async with _async_timeout(4.0):
                details.update(await query_runtime_collector_at_values(at_transport))
        except TimeoutError:
            logger.debug(
                "Selected-result collector AT capability refresh timed out collector_ip=%s",
                collector_ip,
            )
        except Exception as exc:
            logger.debug(
                "Selected-result collector AT capability refresh failed collector_ip=%s error=%s",
                collector_ip,
                exc,
            )
        finally:
            with suppress(Exception):
                await at_transport.stop()
            with suppress(Exception):
                await payload_transport.stop()

        hardware_token = parse_esp_collector_hardware_token(
            details.get("collector_hardware_version")
        )
        if not hardware_token.is_bridge:
            return

        collector = selected_result.collector
        collector_info = collector.collector
        if collector_info is None:
            collector_info = CollectorInfo(remote_ip=collector.ip or collector.target_ip)
            collector.collector = collector_info
        collector_info.collector_virtual_bridge = True
        collector_info.collector_bridge_kind = "esp-collector"
        if hardware_token.version:
            collector_info.collector_bridge_version = hardware_token.version
        endpoint = str(details.get("collector_server_endpoint") or "").strip()
        if endpoint:
            collector_info.collector_server_endpoint = endpoint
        for detail_key, attr_name in (
            ("collector_cloud_family", "collector_cloud_family"),
            ("collector_cloud_family_source", "collector_cloud_family_source"),
            ("collector_cloud_family_confidence", "collector_cloud_family_confidence"),
        ):
            value = str(details.get(detail_key) or "").strip()
            if value:
                setattr(collector_info, attr_name, value)

    def _collector_callback_target_endpoint(self) -> str:
        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(self._current_connection_type(), values)
        template_endpoint = str(
            self._collector_current_server_endpoint
            or self._collector_original_server_endpoint
            or ""
        ).strip()
        return home_assistant_callback_endpoint(
            server_host=spec.effective_advertised_server_ip,
            listener_port=int(
                getattr(spec, "effective_advertised_tcp_port", 0)
                or getattr(spec, "tcp_port", 0)
                or 0
            ),
            template_endpoint=template_endpoint,
        )

    def _collector_original_endpoint_options(self, endpoint: str) -> dict[str, str]:
        """Return sticky option fields for a preserved original collector endpoint."""

        normalized_endpoint = str(endpoint or "").strip()
        if not normalized_endpoint:
            return {}

        profile_key = ""
        try:
            parsed = inspect_collector_server_endpoint(
                normalized_endpoint,
                require_explicit_port=False,
                require_explicit_protocol=False,
            )
        except ValueError:
            parsed = None
        if parsed is not None:
            profile_key = collector_cloud_family_observation_from_endpoint(
                normalized_endpoint
            ).family
            if profile_key == "unknown":
                profile_key = ""

        return {
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: normalized_endpoint,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: profile_key,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: "config_flow_pre_bind",
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT: datetime.now(
                timezone.utc
            ).isoformat(),
        }

    async def _async_remember_collector_original_endpoint_in_registry(
        self,
        *,
        collector_pn: str,
        endpoint: str,
        options: dict[str, Any],
    ) -> None:
        """Persist the original collector endpoint outside the config entry."""

        normalized_pn = str(collector_pn or "").strip()
        normalized_endpoint = str(endpoint or "").strip()
        if not normalized_pn or not normalized_endpoint:
            return
        config_dir = self._config_dir_path()
        await self.hass.async_add_executor_job(
            lambda: remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn=normalized_pn,
                original_endpoint_raw=normalized_endpoint,
                cloud_profile_key=str(
                    options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY) or ""
                ),
                source=str(options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE) or ""),
                observed_at=str(
                    options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT) or ""
                ),
                last_seen_ip=self._selected_collector_ip(),
            )
        )

    async def _async_with_selected_collector_session(self):
        collector_ip = self._selected_collector_ip()
        if not collector_ip:
            raise RuntimeError("collector_ip_unavailable")

        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(self._current_connection_type(), values)
        transport = SharedEybondTransport(
            host=spec.server_ip,
            port=spec.tcp_port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            heartbeat_interval=float(spec.heartbeat_interval),
            collector_ip=collector_ip,
        )
        await transport.start()
        try:
            with suppress(Exception):
                await async_probe_target(
                    bind_ip=spec.server_ip,
                    advertised_server_ip=spec.effective_advertised_server_ip,
                    advertised_server_port=spec.effective_advertised_tcp_port,
                    target_ip=collector_ip,
                    udp_port=spec.udp_port,
                    timeout=1.0,
                )
            connected = await transport.wait_until_connected(timeout=5.0)
            if not connected:
                raise ConnectionError("collector_not_connected")
            await transport.wait_until_heartbeat(timeout=1.5)
            return transport, SmartEssLocalSession(transport)
        except Exception:
            await transport.stop()
            raise

    async def _async_query_selected_collector_text(self, parameter: int) -> str:
        transport, session = await self._async_with_selected_collector_session()
        try:
            response = await session.query_collector(parameter)
            if response.code != 0:
                raise RuntimeError(f"collector_query_failed:parameter={parameter}:code={response.code}")
            return str(response.text or "").strip().strip("\x00")
        finally:
            await transport.stop()

    async def _async_read_selected_collector_server_endpoint(self) -> str:
        endpoint = await self._async_query_selected_collector_text(SET_SERVER_ENDPOINT)
        self._collector_current_server_endpoint = endpoint
        if endpoint and not self._collector_original_server_endpoint:
            self._collector_original_server_endpoint = endpoint
        self._collector_target_server_endpoint = self._collector_callback_target_endpoint()
        return endpoint

    async def _async_bind_selected_collector_to_home_assistant(
        self,
        *,
        allow_refused_endpoint_write: bool = False,
    ) -> None:
        target_endpoint = self._collector_callback_target_endpoint()
        current_endpoint = self._collector_current_server_endpoint or await self._async_read_selected_collector_server_endpoint()
        self._collector_target_server_endpoint = target_endpoint
        if current_endpoint == target_endpoint:
            return

        transport, session = await self._async_with_selected_collector_session()
        try:
            set_response = await session.set_collector(SET_SERVER_ENDPOINT, target_endpoint)
            if set_response.status != 0 or set_response.parameter != SET_SERVER_ENDPOINT:
                # Modern virtual-bridge firmware accepts the FC=3 param-21
                # endpoint write. Older bridge firmware may refuse it; for a
                # detected bridge that refusal is non-fatal because the mode is
                # HA-only regardless. A factory collector keeps the original hard
                # failure.
                if allow_refused_endpoint_write:
                    logger.debug(
                        "Collector endpoint write refused by a detected bridge "
                        "(parameter=%s status=%s); treating as applied and continuing.",
                        SET_SERVER_ENDPOINT,
                        set_response.status,
                    )
                    return
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_SERVER_ENDPOINT}:status={set_response.status}"
                )
            readback = await session.query_collector(SET_SERVER_ENDPOINT)
            if readback.code == 0 and str(readback.text or "").strip().strip("\x00"):
                self._collector_current_server_endpoint = str(readback.text or "").strip().strip("\x00")
            with suppress(Exception):
                await session.query_collector(QUERY_REBOOT_REQUIRED)
            try:
                apply_response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
            except Exception as exc:
                # Applying a staged endpoint makes the collector drop this TCP
                # session (it reconnects to the new endpoint / reboots), and
                # bridge firmware before the deferred-apply fix closes the
                # socket before the FC=3 ack is flushed. The endpoint write
                # and readback already succeeded above, so for a bridge the
                # lost ack means "applying", not "failed".
                if allow_refused_endpoint_write:
                    logger.debug(
                        "Collector endpoint apply dropped the session on a "
                        "detected bridge (%s); treating as applied.",
                        exc,
                    )
                    return
                raise
            if apply_response.status != 0 or apply_response.parameter != SET_REBOOT_OR_APPLY:
                if allow_refused_endpoint_write:
                    logger.debug(
                        "Collector endpoint apply refused by a detected bridge "
                        "(parameter=%s status=%s); treating as applied.",
                        SET_REBOOT_OR_APPLY,
                        apply_response.status,
                    )
                    return
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_REBOOT_OR_APPLY}:status={apply_response.status}"
                )
        finally:
            await transport.stop()

    def _reset_collector_endpoint_binding_state(self) -> None:
        self._collector_original_server_endpoint = ""
        self._collector_current_server_endpoint = ""
        self._collector_endpoint_error = ""
        self._collector_endpoint_bind_applied = False

    def _bluetooth_setup_placeholders(self) -> dict[str, str]:
        return {
            "selected_scan_interface": self._selected_interface_label(
                self._auto_config.get(CONF_SERVER_IP, self._local_ip)
            ),
            "ble_last_error": self._ble_last_error or self._tr("common.dynamic.none", "None"),
        }

    def _bluetooth_rescan_action_label(self) -> str:
        return self._tr(
            "common.dynamic.bluetooth_action_rescan",
            "Refresh collector list",
        )

    def _bluetooth_refresh_wifi_action_label(self) -> str:
        return self._tr(
            "common.dynamic.bluetooth_action_refresh_wifi",
            "Refresh Wi-Fi list for current collector",
        )

    def _bluetooth_apply_action_label(self) -> str:
        return self._tr(
            "common.dynamic.bluetooth_action_apply",
            "Apply settings to current collector",
        )

    @staticmethod
    def _ble_device_name(device: object | None) -> str:
        return str(getattr(device, "name", None) or "").strip()

    @staticmethod
    def _ble_log_value(value: object, *, limit: int = 140) -> str:
        try:
            text = str(value)
        except Exception:
            text = f"<{type(value).__name__}>"
        text = " ".join(text.split())
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    @classmethod
    def _ble_device_log_summary(cls, device: object | None) -> str:
        if device is None:
            return "none"

        parts = [f"type={type(device).__name__}"]
        for attribute in ("address", "name", "rssi"):
            value = getattr(device, attribute, None)
            if value not in (None, ""):
                parts.append(f"{attribute}={cls._ble_log_value(value)}")

        details = getattr(device, "details", None)
        if details is not None:
            parts.append(f"details_type={type(details).__name__}")
            if isinstance(details, dict):
                keys = ",".join(sorted(str(key) for key in details)[:8])
                if keys:
                    parts.append(f"details_keys={keys}")

        metadata = getattr(device, "metadata", None)
        if isinstance(metadata, dict):
            keys = ",".join(sorted(str(key) for key in metadata)[:8])
            if keys:
                parts.append(f"metadata_keys={keys}")
            service_uuids = metadata.get("uuids") or metadata.get("service_uuids")
            if service_uuids:
                rendered = ",".join(str(value) for value in list(service_uuids)[:6])
                parts.append(f"metadata_uuids={cls._ble_log_value(rendered)}")
            manufacturer_data = metadata.get("manufacturer_data")
            if isinstance(manufacturer_data, dict):
                ids = ",".join(str(key) for key in sorted(manufacturer_data)[:8])
                if ids:
                    parts.append(f"manufacturer_ids={ids}")

        return " ".join(parts)

    def _resolve_ble_connect_device(self, address: str, ble_device: object | None = None) -> object | None:
        resolved_device = self._hass_bluetooth_device_from_address(address)
        if resolved_device is not None:
            if not self._ble_device_name(resolved_device):
                logger.info(
                    "SmartESS BLE Home Assistant connectable device lacks a usable name for address=%s; "
                    "still preferring it over the current discovery candidate ha_device=%s candidate_device=%s",
                    address,
                    self._ble_device_log_summary(resolved_device),
                    self._ble_device_log_summary(ble_device),
                )
            logger.info(
                "SmartESS BLE using Home Assistant connectable device address=%s selected_device=%s "
                "candidate_device=%s",
                address,
                self._ble_device_log_summary(resolved_device),
                self._ble_device_log_summary(ble_device),
            )
            return resolved_device
        bluetooth = self._home_assistant_bluetooth_module()
        if bluetooth is not None and callable(getattr(bluetooth, "async_ble_device_from_address", None)):
            logger.warning(
                "SmartESS BLE found no Home Assistant connectable device for address=%s; "
                "falling back to address-only connection candidate_device=%s",
                address,
                self._ble_device_log_summary(ble_device),
            )
            return None
        logger.info(
            "SmartESS BLE using discovery candidate without Home Assistant lookup address=%s candidate_device=%s",
            address,
            self._ble_device_log_summary(ble_device),
        )
        return ble_device

    @staticmethod
    def _ble_flow_error_key(exc: SmartEssBleError) -> str:
        code = str(exc)
        if code in {
            "adapter_not_found",
            "backend_missing",
            "backend_not_supported",
            "ble_backend_missing",
            "host_unavailable",
            "permission_denied",
            "probe_failed",
        }:
            return "ble_unavailable"
        if code == "ble_address_invalid":
            return "ble_address_invalid"
        if code == "ble_wifi_ssid_invalid":
            return "ble_wifi_ssid_invalid"
        if code == "ble_wifi_password_invalid":
            return "ble_wifi_password_invalid"
        if code == "ble_scan_failed" or code.startswith("ble_scan_failed:"):
            return "ble_scan_failed"
        if code == "ble_wifi_scan_failed" or code.startswith("ble_wifi_scan_failed:"):
            return "ble_wifi_scan_failed"
        if code == "ble_provision_failed" or code.startswith("ble_provision_failed:"):
            return "ble_provision_failed"
        return "ble_provision_failed"

    async def _async_discover_smartess_ble_candidates(
        self,
        *,
        force_active_scan: bool = False,
    ) -> tuple[SmartEssBleCandidate, ...]:
        if force_active_scan:
            ha_candidates = await self._async_discover_smartess_ble_candidates_from_hass_advertisements(
                timeout=_BLE_SCAN_TIMEOUT
            )
            if not ha_candidates:
                ha_candidates = self._async_discovered_smartess_ble_candidates_from_hass()
        else:
            ha_candidates = self._async_discovered_smartess_ble_candidates_from_hass()
            if not ha_candidates:
                ha_candidates = await self._async_discover_smartess_ble_candidates_from_hass_advertisements(
                    timeout=_BLE_SCAN_TIMEOUT
                )
            if not ha_candidates:
                ha_candidates = self._async_discovered_smartess_ble_candidates_from_hass()
        if ha_candidates:
            self._ble_last_error = ""
            return _sort_ble_candidates(ha_candidates)

        if self._ble_ha_backend_available and not self._ble_local_adapter_available:
            logger.info(
                "SmartESS BLE scan found no collector candidates in Home Assistant Bluetooth data; "
                "skipping raw Bleak fallback because no local adapter is available"
            )
            return ()

        scanner = BleakSmartEssBleScanner()
        try:
            candidates = _sort_ble_candidates(await scanner.discover_candidates(timeout=_BLE_SCAN_TIMEOUT))
            if candidates:
                self._ble_last_error = ""
            else:
                logger.info(
                    "SmartESS BLE scan found no collector candidates after %.1fs",
                    _BLE_SCAN_TIMEOUT,
                )
            return candidates
        except SmartEssBleError:
            raise
        except PermissionError as exc:
            raise SmartEssBleError("permission_denied") from exc
        except FileNotFoundError as exc:
            raise SmartEssBleError("adapter_not_found") from exc
        except NotImplementedError as exc:
            raise SmartEssBleError("backend_not_supported") from exc
        except OSError as exc:
            raise SmartEssBleError("host_unavailable") from exc
        except Exception as exc:
            detail = _exception_detail(exc)
            logger.warning("SmartESS BLE scan failed error=%s", detail)
            raise SmartEssBleError(f"ble_scan_failed:{detail}") from exc

    async def _async_refresh_ble_device_before_wifi_scan_retry(
        self,
        ble_address: str,
        *,
        attempt: int,
        error: str,
    ) -> object | None:
        try:
            candidates = await self._async_discover_smartess_ble_candidates(force_active_scan=True)
        except SmartEssBleError as exc:
            logger.info(
                "SmartESS BLE Wi-Fi scan active rediscovery failed before retry address=%s attempt=%d error=%s refresh_error=%s",
                ble_address,
                attempt,
                error,
                exc,
            )
            return None

        candidate = _ble_candidate_by_address(candidates, ble_address)
        if candidate is None:
            logger.info(
                "SmartESS BLE Wi-Fi scan active rediscovery did not find selected collector before retry address=%s attempt=%d error=%s",
                ble_address,
                attempt,
                error,
            )
            return None

        logger.info(
            "SmartESS BLE Wi-Fi scan refreshed selected collector before retry address=%s attempt=%d error=%s device=%s",
            ble_address,
            attempt,
            error,
            self._ble_device_log_summary(candidate.device),
        )
        return candidate.device

    async def _async_discover_smartess_ble_candidates_from_hass_advertisements(
        self,
        *,
        timeout: float,
    ) -> tuple[SmartEssBleCandidate, ...]:
        try:
            bluetooth = importlib.import_module("homeassistant.components.bluetooth")
        except Exception:
            return ()

        register_callback = getattr(bluetooth, "async_register_callback", None)
        scanning_mode = getattr(bluetooth, "BluetoothScanningMode", None)
        if not callable(register_callback) or scanning_mode is None:
            return ()

        active_mode = getattr(scanning_mode, "ACTIVE", None)
        if active_mode is None:
            return ()

        deduped: dict[str, SmartEssBleCandidate] = {}
        advertisement_count = 0
        registration_errors: list[str] = []
        advertisement_samples: list[str] = []
        advertisement_sample_keys: set[str] = set()

        def _handle_advertisement(service_info: object, _change: object) -> None:
            nonlocal advertisement_count
            advertisement_count += 1
            if len(advertisement_samples) < 12:
                sample = self._hass_bluetooth_service_info_summary(service_info)
                if sample and sample not in advertisement_sample_keys:
                    advertisement_sample_keys.add(sample)
                    advertisement_samples.append(sample)
            candidate = self._smartess_ble_candidate_from_hass_service_info(service_info)
            if candidate is not None:
                deduped[candidate.address] = candidate

        unload_callbacks: list[Callable[[], None]] = []
        for matcher in (
            {"manufacturer_id": 0x3545, "connectable": False},
            {"manufacturer_id": 0x3545, "connectable": True},
            {"local_name": "E50*", "connectable": False},
            {"local_name": "E50*", "connectable": True},
            {"local_name": "V00*", "connectable": False},
            {"local_name": "V00*", "connectable": True},
            {"connectable": False},
            {"connectable": True},
        ):
            try:
                unload = register_callback(self.hass, _handle_advertisement, matcher, active_mode)
            except Exception as exc:
                registration_errors.append(f"{matcher}: {exc}")
                logger.debug("SmartESS BLE HA callback registration failed matcher=%s error=%s", matcher, exc)
                continue
            if callable(unload):
                unload_callbacks.append(unload)

        if not unload_callbacks:
            return ()

        try:
            await asyncio.sleep(float(timeout))
        finally:
            for unload in unload_callbacks:
                try:
                    unload()
                except Exception as exc:
                    logger.debug("SmartESS BLE HA callback cleanup failed error=%s", exc)

        if not deduped:
            logger.warning(
                "SmartESS BLE HA advertisement scan found no collector candidates after %.1fs "
                "registered_callbacks=%d advertisements=%d registration_errors=%s samples=%s",
                timeout,
                len(unload_callbacks),
                advertisement_count,
                registration_errors or "none",
                advertisement_samples or "none",
            )

        return tuple(deduped.values())

    def _async_discovered_smartess_ble_candidates_from_hass(self) -> tuple[SmartEssBleCandidate, ...]:
        try:
            bluetooth = importlib.import_module("homeassistant.components.bluetooth")
        except Exception:
            return ()

        service_infos = self._hass_bluetooth_service_infos(bluetooth)
        devices = self._hass_bluetooth_devices(bluetooth)

        if not service_infos and not devices:
            return ()

        deduped: dict[str, SmartEssBleCandidate] = {}
        for service_info in service_infos or ():
            candidate = self._smartess_ble_candidate_from_hass_service_info(service_info)
            if candidate is not None:
                deduped[candidate.address] = candidate
        for device in devices:
            candidate = self._smartess_ble_candidate_from_hass_device(device)
            if candidate is not None and candidate.address not in deduped:
                deduped[candidate.address] = candidate
        return tuple(deduped.values())

    def _hass_bluetooth_service_infos(self, bluetooth: object) -> tuple[object, ...]:
        discovered_service_info = getattr(bluetooth, "async_discovered_service_info", None)
        if not callable(discovered_service_info):
            return ()

        service_infos: list[object] = []
        seen_keys: set[tuple[str, str]] = set()
        call_variants = (
            {"connectable": True},
            {"connectable": False},
            {},
        )
        for kwargs in call_variants:
            try:
                result = discovered_service_info(self.hass, **kwargs)
            except TypeError:
                if kwargs:
                    continue
                try:
                    result = discovered_service_info(self.hass)
                except Exception:
                    continue
            except Exception:
                continue
            for service_info in result or ():
                key = (
                    str(getattr(service_info, "address", "") or ""),
                    str(getattr(service_info, "name", "") or ""),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                service_infos.append(service_info)
        return tuple(service_infos)

    def _hass_bluetooth_devices(self, bluetooth: object) -> tuple[object, ...]:
        devices: list[object] = []
        seen_addresses: set[str] = set()
        for attr in ("async_scanner_devices", "async_scanner_devices_by_address"):
            provider = getattr(bluetooth, attr, None)
            if not callable(provider):
                continue
            for kwargs in ({"connectable": True}, {"connectable": False}, {}):
                try:
                    result = provider(self.hass, **kwargs)
                except TypeError:
                    if kwargs:
                        continue
                    try:
                        result = provider(self.hass)
                    except Exception:
                        continue
                except Exception:
                    continue
                values = result.values() if isinstance(result, dict) else result or ()
                for device in values:
                    address = str(getattr(device, "address", "") or "").strip()
                    if not address or address in seen_addresses:
                        continue
                    seen_addresses.add(address)
                    devices.append(device)
        return tuple(devices)

    @staticmethod
    def _smartess_ble_candidate_from_hass_service_info(service_info: object) -> SmartEssBleCandidate | None:
        advertisement = getattr(service_info, "advertisement", None)
        device = getattr(service_info, "device", None)
        service_name = str(getattr(service_info, "name", "") or "").strip()
        return normalize_discovered_candidate(
            address=str(getattr(service_info, "address", "") or "").strip(),
            device_name=str(getattr(device, "name", "") or service_name).strip(),
            advertisement_local_name=str(getattr(advertisement, "local_name", "") or service_name).strip(),
            manufacturer_data=getattr(service_info, "manufacturer_data", None)
            or getattr(advertisement, "manufacturer_data", None),
            service_uuids=getattr(service_info, "service_uuids", None)
            or getattr(advertisement, "service_uuids", None)
            or (),
            device=device,
        )

    @staticmethod
    def _hass_bluetooth_service_info_summary(service_info: object) -> str:
        advertisement = getattr(service_info, "advertisement", None)
        device = getattr(service_info, "device", None)
        manufacturer_data = (
            getattr(service_info, "manufacturer_data", None)
            or getattr(advertisement, "manufacturer_data", None)
            or {}
        )
        manufacturer_summary: list[str] = []
        if isinstance(manufacturer_data, dict):
            for key, value in list(manufacturer_data.items())[:4]:
                data = bytes(value or b"")
                ascii_preview = data.decode("ascii", errors="ignore")[:24]
                manufacturer_summary.append(
                    f"0x{int(key):04x}:{data[:12].hex()}:{ascii_preview}"
                )
        service_uuids = (
            getattr(service_info, "service_uuids", None)
            or getattr(advertisement, "service_uuids", None)
            or ()
        )
        uuid_summary = ",".join(str(value) for value in tuple(service_uuids)[:4])
        return (
            f"address={str(getattr(service_info, 'address', '') or '').strip()} "
            f"name={str(getattr(service_info, 'name', '') or '').strip()} "
            f"local_name={str(getattr(advertisement, 'local_name', '') or '').strip()} "
            f"device_name={str(getattr(device, 'name', '') or '').strip()} "
            f"rssi={str(getattr(service_info, 'rssi', '') or '').strip()} "
            f"source={str(getattr(service_info, 'source', '') or '').strip()} "
            f"connectable={str(getattr(service_info, 'connectable', '') or '').strip()} "
            f"manufacturer={manufacturer_summary or 'none'} "
            f"service_uuids={uuid_summary or 'none'}"
        )

    @staticmethod
    def _smartess_ble_candidate_from_hass_device(device: object) -> SmartEssBleCandidate | None:
        address = str(getattr(device, "address", "") or "").strip()
        if not address:
            return None
        device_name = str(getattr(device, "name", "") or "").strip()
        metadata = getattr(device, "metadata", None)
        if not isinstance(metadata, dict):
            metadata = {}
        return normalize_discovered_candidate(
            address=address,
            device_name=device_name,
            advertisement_local_name=str(metadata.get("local_name") or device_name).strip(),
            manufacturer_data=metadata.get("manufacturer_data"),
            service_uuids=metadata.get("uuids") or (),
            device=device,
        )

    async def _async_scan_smartess_ble_wifi_networks(
        self,
        ble_address: str,
        ble_device: object | None = None,
    ) -> tuple[SmartEssBleWifiNetwork, ...]:
        if not ble_address:
            return ()

        current_ble_device = ble_device
        for attempt in range(1, _BLE_WIFI_SCAN_ATTEMPTS + 1):
            resolved_device = self._resolve_ble_connect_device(ble_address, current_ble_device)
            session = SmartEssBleSession(BleakSmartEssBleLink(ble_address, device=resolved_device))
            try:
                async with _async_timeout(_BLE_CONNECT_TIMEOUT):
                    await session.connect()
                provisioner = SmartEssBleProvisioner(session)
                async with _async_timeout(_BLE_WIFI_SCAN_TIMEOUT):
                    networks = tuple(await provisioner.scan_wifi_networks())
                if provisioner.last_firmware_version:
                    self._ble_fw_version_by_address[ble_address] = provisioner.last_firmware_version
                return networks
            except TimeoutError as exc:
                timeout = _BLE_WIFI_SCAN_TIMEOUT if session.connected else _BLE_CONNECT_TIMEOUT
                logger.warning(
                    "SmartESS BLE Wi-Fi scan timed out address=%s timeout=%.1fs",
                    ble_address,
                    timeout,
                )
                raise SmartEssBleError("ble_wifi_scan_failed:timeout") from exc
            except SmartEssBleError as exc:
                if attempt < _BLE_WIFI_SCAN_ATTEMPTS and _is_retryable_ble_wifi_scan_error(exc):
                    logger.info(
                        "SmartESS BLE Wi-Fi scan retrying after BLE session error address=%s attempt=%d/%d error=%s",
                        ble_address,
                        attempt,
                        _BLE_WIFI_SCAN_ATTEMPTS,
                        exc,
                    )
                    current_ble_device = await self._async_refresh_ble_device_before_wifi_scan_retry(
                        ble_address,
                        attempt=attempt,
                        error=str(exc),
                    )
                    await asyncio.sleep(_BLE_WIFI_SCAN_RETRY_DELAY * attempt)
                    continue
                if str(exc) == "ble_notification_timeout":
                    raise SmartEssBleError("ble_wifi_scan_failed:notification_timeout") from exc
                raise
            except PermissionError as exc:
                raise SmartEssBleError("ble_unavailable") from exc
            except Exception as exc:
                detail = _exception_detail(exc)
                if attempt < _BLE_WIFI_SCAN_ATTEMPTS:
                    logger.info(
                        "SmartESS BLE Wi-Fi scan retrying address=%s attempt=%d/%d error=%s",
                        ble_address,
                        attempt,
                        _BLE_WIFI_SCAN_ATTEMPTS,
                        detail,
                    )
                    current_ble_device = await self._async_refresh_ble_device_before_wifi_scan_retry(
                        ble_address,
                        attempt=attempt,
                        error=detail,
                    )
                    await asyncio.sleep(_BLE_WIFI_SCAN_RETRY_DELAY * attempt)
                    continue
                logger.info("SmartESS BLE Wi-Fi scan failed address=%s error=%s", ble_address, detail)
                raise SmartEssBleError(f"ble_wifi_scan_failed:{detail}") from exc
            finally:
                with suppress(Exception):
                    await session.disconnect()

        raise SmartEssBleError("ble_wifi_scan_failed:retry_exhausted")

    async def _async_run_smartess_ble_bootstrap(
        self,
        *,
        ble_address: str,
        ssid: str,
        password: str,
        ble_device: object | None = None,
    ) -> None:
        if not ble_address:
            raise SmartEssBleError("ble_address_invalid")

        resolved_device = self._resolve_ble_connect_device(ble_address, ble_device)
        session = SmartEssBleSession(BleakSmartEssBleLink(ble_address, device=resolved_device))
        try:
            async with _async_timeout(_BLE_PROVISION_TIMEOUT):
                await session.connect()
                provisioner = SmartEssBleProvisioner(session)
                resolved_info = None
                cached_fw_version = self._known_smartess_ble_firmware_version(ble_address)
                if cached_fw_version:
                    resolved_info = await provisioner.query_device_info(known_fw_version=cached_fw_version)
                result = await provisioner.provision_wifi(
                    ssid=ssid,
                    password=password,
                    info=resolved_info,
                )
                if provisioner.last_firmware_version:
                    self._ble_fw_version_by_address[ble_address] = provisioner.last_firmware_version
        except TimeoutError as exc:
            logger.warning(
                "SmartESS BLE provisioning timed out address=%s timeout=%.1fs",
                ble_address,
                _BLE_PROVISION_TIMEOUT,
            )
            raise SmartEssBleError("ble_provision_failed:timeout") from exc
        except SmartEssBleError as exc:
            if str(exc) == "ble_notification_timeout":
                raise SmartEssBleError("ble_provision_failed:notification_timeout") from exc
            raise
        except PermissionError as exc:
            raise SmartEssBleError("ble_unavailable") from exc
        except Exception as exc:
            detail = _exception_detail(exc)
            logger.warning("SmartESS BLE provisioning failed address=%s error=%s", ble_address, detail)
            raise SmartEssBleError(f"ble_provision_failed:{detail}") from exc
        finally:
            with suppress(Exception):
                await session.disconnect()

        logger.info(
            "SmartESS BLE provisioning result address=%s branch=%s outcome=%s status=%s details=%s",
            ble_address,
            result.branch.value,
            result.outcome.value,
            result.status_code,
            result.details,
        )

        if result.outcome == SmartEssBleProvisionOutcome.FAILURE:
            detail = f"{result.branch.value}:{result.status_code}"
            if result.details is not None:
                detail = f"{detail}:{','.join(result.details)}"
            raise SmartEssBleError(f"ble_provision_failed:{detail}")

    def _collector_pn_for_result(self, result: OnboardingResult | None) -> str:
        if result is None:
            return ""

        collector_info = result.collector.collector if result.collector is not None else None
        if collector_info is not None:
            collector_pn = str(collector_info.collector_pn or "").strip()
            if collector_pn:
                return collector_pn

        match_details = result.match.details if result.match is not None else {}
        return str(match_details.get("collector_pn") or "").strip()

    def _known_smartess_ble_firmware_version(self, ble_address: str) -> str:
        cached_fw_version = str(self._ble_fw_version_by_address.get(ble_address, "") or "").strip()
        if cached_fw_version:
            return cached_fw_version
        for result in (self._selected_result, self._manual_result):
            fw_version = _smartess_collector_firmware_version_for_result(result)
            if fw_version:
                return fw_version
        return str(
            self._auto_config.get(CONF_SMARTESS_COLLECTOR_VERSION)
            or self._manual_config.get(CONF_SMARTESS_COLLECTOR_VERSION)
            or ""
        ).strip()

    def _smartess_detected_hint_values(self, result: OnboardingResult | None) -> tuple[str, str]:
        if result is None:
            return "", ""

        collector_info = result.collector.collector if result.collector is not None else None
        match_details = result.match.details if result.match is not None else {}
        asset_id = str(
            match_details.get("smartess_protocol_asset_id")
            or getattr(collector_info, "smartess_protocol_asset_id", "")
            or ""
        ).strip()
        profile_key = str(
            match_details.get("smartess_profile_key")
            or getattr(collector_info, "smartess_protocol_profile_key", "")
            or ""
        ).strip()
        return asset_id, profile_key

    def _smartess_cloud_assist_context_result(self) -> OnboardingResult | None:
        if self._smartess_cloud_assist_mode == "manual":
            return self._manual_result
        return self._selected_result

    def _smartess_cloud_assist_state_for_result(
        self,
        result: OnboardingResult | None,
    ) -> _SmartEssCloudAssistState | None:
        collector_pn = self._collector_pn_for_result(result)
        if not collector_pn or self._smartess_cloud_assist is None:
            return None
        if self._smartess_cloud_assist.collector_pn != collector_pn:
            return None
        return self._smartess_cloud_assist

    def _can_offer_smartess_cloud_assist(self, result: OnboardingResult | None) -> bool:
        return False

    def _smartess_cloud_summary(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        placeholders = {
            "family_label": state.inferred_family_label,
            "driver_key": state.inferred_driver_key or DRIVER_HINT_AUTO,
            "exact_count": state.exact_field_count,
            "probable_count": state.probable_field_count,
            "cloud_only_count": state.cloud_only_field_count,
        }
        if state.inferred_family_label:
            return self._tr(
                "common.dynamic.smartess_cloud_summary_known_family",
                "**SmartESS cloud:** suggests **{family_label}** and pre-fills local metadata hints for `{driver_key}`. Settings surface: exact {exact_count}, probable {probable_count}, cloud-only {cloud_only_count}. Local controls stay disabled until a high-confidence local detection is confirmed.",
                placeholders,
            )
        return self._tr(
            "common.dynamic.smartess_cloud_summary_generic",
            "**SmartESS cloud:** evidence was saved for this collector, but no safe local family mapping was resolved yet. Settings surface: exact {exact_count}, probable {probable_count}, cloud-only {cloud_only_count}.",
            placeholders,
        )

    def _smartess_cloud_offer_summary(self, result: OnboardingResult | None) -> str:
        collector_pn = self._collector_pn_for_result(result)
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is not None:
            return self._smartess_cloud_summary(result)
        return self._tr(
            "common.dynamic.smartess_cloud_offer_summary",
            "Local detection is not yet high-confidence for collector `{collector_pn}`. SmartESS cloud assist can fetch extra identity and settings evidence before the entry is created.",
            {"collector_pn": collector_pn or self._tr("common.dynamic.not_available", "Not available")},
        )

    def _smartess_cloud_identity_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        not_available = self._tr("common.dynamic.not_available", "Not available")
        lines = [
            self._tr("common.dynamic.smartess_cloud_identity_heading", "**Cloud identity**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_collector_pn_label', 'Collector PN')} | {self._collector_pn_for_result(result) or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_pn_label', 'Device PN')} | {state.device_pn or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_sn_label', 'Device SN')} | {state.device_sn or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_name_label', 'Device')} | {state.device_name or not_available} |",
        ]
        if state.device_alias:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_alias_label', 'Alias')} | {state.device_alias} |"
            )
        if state.device_status:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_status_label', 'Status')} | {state.device_status} |"
            )
        if state.device_brand:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_brand_label', 'Brand')} | {state.device_brand} |"
            )
        address_value = self._smartess_cloud_device_address_preview(state) or not_available
        lines.append(
            f"| {self._tr('common.dynamic.smartess_cloud_device_address_label', 'Cloud address')} | {address_value} |"
        )
        return "\n".join(lines)

    def _smartess_cloud_mapping_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        not_available = self._tr("common.dynamic.not_available", "Not available")
        reason = state.inferred_reason or self._tr(
            "common.dynamic.smartess_cloud_mapping_reason_missing",
            "No safe local family mapping was resolved yet. The evidence is still saved for later diagnostics and support work.",
        )
        lines = [
            self._tr("common.dynamic.smartess_cloud_mapping_heading", "**Local interpretation**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_family_label', 'Suggested family')} | {state.inferred_family_label or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_driver_label', 'Local driver hint')} | {state.inferred_driver_key or DRIVER_HINT_AUTO} |",
            f"| {self._tr('common.dynamic.smartess_cloud_mapping_reason_label', 'Reason')} | {reason} |",
        ]
        return "\n".join(lines)

    def _smartess_cloud_detail_summary(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        if state.detail_sections:
            return self._tr(
                "common.dynamic.smartess_cloud_detail_sections_found",
                "**Cloud detail sections:** {sections}",
                {"sections": ", ".join(state.detail_sections)},
            )
        return self._tr(
            "common.dynamic.smartess_cloud_detail_sections_missing",
            "**Cloud detail sections:** no normalized section breakdown was captured.",
        )

    def _smartess_cloud_settings_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        lines = [
            self._tr("common.dynamic.smartess_cloud_settings_heading", "**Settings digest**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_total_fields_label', 'Total fields')} | {state.total_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_mapped_fields_label', 'Mapped local fields')} | {state.mapped_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_current_values_label', 'Fields with current value')} | {state.fields_with_current_value} |",
            f"| {self._tr('common.dynamic.smartess_cloud_exact_fields_label', 'Exact local matches')} | {state.exact_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_probable_fields_label', 'Probable local matches')} | {state.probable_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_cloud_only_fields_label', 'Cloud-only fields')} | {state.cloud_only_field_count} |",
        ]
        return "\n".join(lines)

    def _smartess_cloud_highlights_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        if not state.highlight_settings:
            return self._tr(
                "common.dynamic.smartess_cloud_highlights_empty",
                "**Highlighted SmartESS fields:** no compact field preview was captured.",
            )

        def _escape_cell(value: str) -> str:
            return str(value).replace("|", "\\|").replace("\n", " ")

        lines = [
            self._tr("common.dynamic.smartess_cloud_highlights_heading", "**Highlighted SmartESS fields**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_highlight_field_label', 'Field')} | {self._tr('common.dynamic.smartess_cloud_highlight_value_label', 'Value')} | {self._tr('common.dynamic.smartess_cloud_highlight_local_use_label', 'Local use')} |",
            "|---|---|---|",
        ]
        not_available = self._tr("common.dynamic.not_available", "Not available")
        for highlight in state.highlight_settings:
            lines.append(
                f"| {_escape_cell(highlight.title)} | {_escape_cell(highlight.current_value or not_available)} | {_escape_cell(self._smartess_cloud_local_use_preview(highlight))} |"
            )
        return "\n".join(lines)

    def _smartess_cloud_device_address_preview(
        self,
        state: _SmartEssCloudAssistState,
    ) -> str:
        if state.device_devcode in (None, "") and state.device_devaddr in (None, ""):
            return ""

        devcode = ""
        if isinstance(state.device_devcode, int):
            devcode = self._tr(
                "common.dynamic.smartess_cloud_device_devcode_value",
                "devcode {devcode} (0x{devcode_hex})",
                {"devcode": state.device_devcode, "devcode_hex": f"{state.device_devcode:04X}"},
            )
        devaddr = ""
        if isinstance(state.device_devaddr, int):
            devaddr = self._tr(
                "common.dynamic.smartess_cloud_device_devaddr_value",
                "devaddr {devaddr}",
                {"devaddr": state.device_devaddr},
            )
        return ", ".join(part for part in (devcode, devaddr) if part)

    def _smartess_cloud_bucket_label(self, bucket: str) -> str:
        if bucket == "exact_0925":
            return self._tr("common.dynamic.smartess_cloud_bucket_exact", "Exact local match")
        if bucket == "probable_0925":
            return self._tr("common.dynamic.smartess_cloud_bucket_probable", "Probable local match")
        if bucket == "cloud_only":
            return self._tr("common.dynamic.smartess_cloud_bucket_cloud_only", "Cloud-only")
        return self._tr("common.dynamic.unknown", "Unknown")

    def _smartess_cloud_local_use_preview(
        self,
        highlight: _SmartEssCloudSettingHighlight,
    ) -> str:
        bucket_label = self._smartess_cloud_bucket_label(highlight.bucket)
        if highlight.register is None:
            return bucket_label
        return self._tr(
            "common.dynamic.smartess_cloud_local_use_register",
            "{bucket_label}, reg {register}",
            {"bucket_label": bucket_label, "register": highlight.register},
        )

    def _smartess_cloud_status_line(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is not None and state.evidence_path:
            return self._tr(
                "common.dynamic.smartess_cloud_status_saved",
                "Last SmartESS cloud evidence: {path}",
                {"path": state.evidence_path},
            )
        if self._smartess_cloud_assist_last_error:
            error_code = getattr(self, "_smartess_cloud_assist_last_error_code", "") or "unexpected"
            translation_key = f"common.dynamic.smartess_cloud_status_failed_{error_code}"
            fallback = "Last SmartESS cloud assist attempt failed: {error}"
            return self._tr(
                translation_key,
                fallback,
                {"error": self._smartess_cloud_assist_last_error},
            )
        return ""

    def _smartess_cloud_assist_placeholders(
        self,
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        state = self._smartess_cloud_assist_state_for_result(result)
        return {
            "collector_pn": self._collector_pn_for_result(result)
            or self._tr("common.dynamic.not_available", "Not available"),
            "cloud_evidence_path": (
                state.evidence_path
                if state is not None and state.evidence_path
                else self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "smartess_cloud_offer_summary": self._smartess_cloud_offer_summary(result),
            "smartess_cloud_status_line": self._smartess_cloud_status_line(result),
        }

    def _smartess_cloud_assist_summary_placeholders(
        self,
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        placeholders = self._smartess_cloud_assist_placeholders(result)
        placeholders.update(
            {
                "smartess_cloud_identity_table": self._smartess_cloud_identity_table(result),
                "smartess_cloud_mapping_table": self._smartess_cloud_mapping_table(result),
                "smartess_cloud_detail_summary": self._smartess_cloud_detail_summary(result),
                "smartess_cloud_settings_table": self._smartess_cloud_settings_table(result),
                "smartess_cloud_highlights_table": self._smartess_cloud_highlights_table(result),
            }
        )
        return placeholders

    def _config_dir_path(self) -> Path:
        config_dir = str(getattr(getattr(self.hass, "config", None), "config_dir", "") or "").strip()
        if not config_dir:
            raise RuntimeError("config_dir_not_available")
        return Path(config_dir)

    async def _async_run_smartess_cloud_assist(
        self,
        result: OnboardingResult,
        *,
        username: str,
        password: str,
    ) -> _SmartEssCloudAssistState:
        if _result_is_virtual_bridge(result):
            raise RuntimeError("smartess_cloud_unavailable_for_virtual_bridge")

        collector_pn = self._collector_pn_for_result(result)
        if not collector_pn:
            raise RuntimeError("smartess_collector_pn_not_available")

        record = await self.hass.async_add_executor_job(
            lambda: fetch_and_export_smartess_device_bundle_cloud_evidence(
                config_dir=self._config_dir_path(),
                username=username,
                password=password,
                collector_pn=collector_pn,
                source="smartess_cloud_onboarding",
            )
        )
        asset_id, profile_key = self._smartess_detected_hint_values(result)
        plan = resolve_smartess_known_family_draft_plan(
            smartess_protocol_asset_id=asset_id,
            smartess_profile_key=profile_key,
            cloud_evidence=record.payload,
        )
        inferred_driver_key = ""
        if plan is not None and plan.source_profile_name:
            try:
                inferred_driver_key = str(
                    load_driver_profile(plan.source_profile_name).driver_key or ""
                ).strip()
            except Exception:
                inferred_driver_key = ""

        summary = dict(record.payload.get("summary") or {})
        device_preview = _smartess_cloud_device_preview(record.payload)
        normalized_settings = _smartess_cloud_bundle_payload(record.payload).get("normalized")
        normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
        normalized_settings = normalized_settings.get("device_settings") if isinstance(normalized_settings, dict) else {}
        normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
        return _SmartEssCloudAssistState(
            collector_pn=collector_pn,
            evidence_path=str(record.path),
            inferred_asset_id=plan.asset_id if plan is not None else "",
            inferred_profile_key=plan.profile_key if plan is not None else "",
            inferred_driver_key=inferred_driver_key,
            inferred_family_label=plan.driver_label if plan is not None else "",
            inferred_reason=plan.reason if plan is not None else "",
            exact_field_count=int(summary.get("settings_exact_0925_field_count") or 0),
            probable_field_count=int(summary.get("settings_probable_0925_field_count") or 0),
            cloud_only_field_count=int(summary.get("settings_cloud_only_field_count") or 0),
            current_values_included=bool(summary.get("settings_current_values_included", False)),
            total_field_count=int(normalized_settings.get("field_count") or summary.get("settings_field_count") or 0),
            mapped_field_count=int(normalized_settings.get("mapped_field_count") or summary.get("settings_mapped_field_count") or 0),
            fields_with_current_value=int(normalized_settings.get("fields_with_current_value") or 0),
            device_pn=str(device_preview.get("pn") or "").strip(),
            device_sn=str(device_preview.get("sn") or "").strip(),
            device_name=str(device_preview.get("name") or "").strip(),
            device_alias=str(device_preview.get("alias") or "").strip(),
            device_status=str(device_preview.get("status") or "").strip(),
            device_brand=str(device_preview.get("brand") or "").strip(),
            device_devcode=device_preview.get("devcode") if device_preview.get("devcode") not in ("", None) else None,
            device_devaddr=device_preview.get("devaddr") if device_preview.get("devaddr") not in ("", None) else None,
            detail_sections=_smartess_cloud_detail_sections(record.payload),
            highlight_settings=_smartess_cloud_highlight_settings(record.payload),
        )

    def _current_connection_type(self) -> str:
        """Return the active connection type for the current setup branch."""

        if self._selected_result is not None and self._selected_result.connection_type:
            return self._selected_result.connection_type
        if self._manual_result is not None and self._manual_result.connection_type:
            return self._manual_result.connection_type
        return str(self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND) or CONNECTION_TYPE_EYBOND)

    def _connection_branch(self):
        """Return branch metadata for the active connection type."""

        return get_connection_branch(self._current_connection_type())

    def _connection_display(self):
        """Return branch-aware display metadata for the active connection type."""

        return self._connection_branch().display

    def _selected_interface_option(self, server_ip: str | None = None) -> dict[str, str] | None:
        selected_ip = str(server_ip or self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        return next(
            (item for item in self._interface_options if item.get("ip") == selected_ip),
            None,
        )

    def _selected_interface_label(self, server_ip: str | None = None) -> str:
        interface = self._selected_interface_option(server_ip)
        if interface is not None:
            return interface.get("label") or interface.get("ip") or self._tr("common.dynamic.unknown", "Unknown")
        selected_ip = str(server_ip or self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        return selected_ip or self._tr("common.dynamic.unknown", "Unknown")

    def _selected_interface_network(self, server_ip: str | None = None) -> str:
        interface = self._selected_interface_option(server_ip)
        return str(interface.get("network", "") if interface is not None else "").strip()

    def _selected_interface_broadcast(self, server_ip: str | None = None) -> str:
        interface = self._selected_interface_option(server_ip)
        broadcast = str(interface.get("broadcast", "") if interface is not None else "").strip()
        if broadcast:
            return broadcast
        selected_ip = str(server_ip or self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        if selected_ip:
            return _compute_broadcast_24(selected_ip)
        return DEFAULT_DISCOVERY_TARGET

    def _scan_discovery_targets(self) -> tuple[DiscoveryTarget, ...]:
        selected_broadcast = self._selected_interface_broadcast()
        addresses = [selected_broadcast] if selected_broadcast else [DEFAULT_DISCOVERY_TARGET]
        return tuple(DiscoveryTarget(ip=address, source="broadcast") for address in addresses if address)

    def _deep_scan_plan(self) -> dict[str, Any]:
        network_cidr = self._selected_interface_network()
        server_ip = str(self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        target_count = _network_target_count(network_cidr, exclude={server_ip}) if network_cidr else 0
        return {
            "network_cidr": network_cidr,
            "target_count": target_count,
            "large_subnet": target_count > 253,
            "timeout_seconds": _onboarding_deep_scan_timeout_seconds(
                target_count,
                policy=_ONBOARDING_TIMEOUT_POLICY,
            ),
        }

    def _set_scan_mode(self, mode: str) -> None:
        self._scan_mode = mode
        if mode == SETUP_MODE_DEEP_SCAN:
            self._scan_timeout_seconds = self._deep_scan_plan()["timeout_seconds"]
            return
        self._scan_timeout_seconds = _AUTO_SCAN_TIMEOUT

    def _auto_connection_defaults(self) -> dict[str, Any]:
        """Return branch-aware defaults for the auto-scan flow."""

        server_ip = str(self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        defaults = self._connection_branch().build_auto_values(
            server_ip=server_ip,
            default_broadcast=self._selected_interface_broadcast(server_ip) if server_ip else self._default_broadcast,
        )
        defaults.update(self._auto_config)
        return defaults

    def _refresh_scan_action_label(self) -> str:
        """Context-aware label: repeating after a deep scan repeats the deep scan."""

        if self._scan_mode == SETUP_MODE_DEEP_SCAN:
            return self._tr(
                "common.dynamic.scan_results_action_refresh_deep",
                "Repeat deep scan",
            )
        return self._scan_action_label("refresh_scan", "Refresh scan results")

    def _scan_action_label(self, action: str, default: str) -> str:
        # deep_scan / change_scan_interface / manual moved under the
        # advanced_setup submenu; resolve their labels from either step.
        label = self._tr(f"config.step.scan_results.menu_options.{action}", "")
        if not label:
            label = self._tr(f"config.step.advanced_setup.menu_options.{action}", "")
        return label or default

    def _manual_confirm_action_label(self, action: str, default: str) -> str:
        return self._tr(
            f"config.step.manual_confirm.menu_options.{action}",
            default,
        )

    async def _async_update_scan_progress_loop(self) -> None:
        """Periodically publish determinate progress updates while one scan runs."""

        while True:
            started = self._scan_started_monotonic
            now = time.monotonic()
            elapsed_seconds = max(0.0, now - started) if started is not None else 0.0
            self.async_update_progress(self._scan_progress_fraction(elapsed_seconds))
            await asyncio.sleep(0.35)

    def _scan_progress_fraction(self, elapsed_seconds: float) -> float:
        scan_timeout = self._scan_timeout_seconds if self._scan_timeout_seconds > 0 else _AUTO_SCAN_TIMEOUT
        bounded_elapsed = min(max(elapsed_seconds, 0.0), scan_timeout)
        time_fraction = bounded_elapsed / scan_timeout if scan_timeout > 0 else 0.0
        if self._scan_progress_stage == "preparing":
            return 0.0
        if self._scan_progress_stage == "discovering":
            return min(0.82, 0.02 + (time_fraction * 0.8))
        if self._scan_progress_stage == "analyzing":
            return 0.9
        if self._scan_progress_stage == "finalizing":
            return 0.97
        return min(0.82, 0.02 + (time_fraction * 0.8))

    def _scan_progress_placeholders(self, selected_label: str) -> dict[str, str]:
        now = time.monotonic()
        started = self._scan_started_monotonic if self._scan_started_monotonic is not None else now
        elapsed_seconds_float = max(0.0, now - started)
        scan_timeout = self._scan_timeout_seconds if self._scan_timeout_seconds > 0 else _AUTO_SCAN_TIMEOUT
        bounded_elapsed = min(elapsed_seconds_float, scan_timeout)
        elapsed_seconds = int(round(bounded_elapsed))
        progress_fraction = self._scan_progress_fraction(elapsed_seconds_float)
        percent = max(0, min(99, int(round(progress_fraction * 100))))
        filled = max(0, min(_SCAN_PROGRESS_BAR_WIDTH, int(round(progress_fraction * _SCAN_PROGRESS_BAR_WIDTH))))
        progress_bar = (
            "["
            + ("#" * filled)
            + ("-" * (_SCAN_PROGRESS_BAR_WIDTH - filled))
            + f"] {percent}%"
        )
        stage_label = self._tr(
            f"common.dynamic.scan_progress_stage_{self._scan_progress_stage}",
            "Preparing scan",
        )
        return {
            "selected_scan_interface": selected_label,
            "scan_progress_phase": stage_label,
            "scan_progress_bar": progress_bar,
            "scan_progress_detail": self._tr(
                "common.dynamic.scan_progress_detail",
                "{elapsed_seconds}s elapsed.",
                {
                    "elapsed_seconds": elapsed_seconds,
                },
            ),
            "scan_progress_hint": self._tr(
                (
                    "common.dynamic.scan_progress_hint_deep"
                    if self._scan_mode == SETUP_MODE_DEEP_SCAN
                    else "common.dynamic.scan_progress_hint"
                ),
                (
                    "Deep scan keeps the same discovery flow and also probes the rest of the selected IPv4 network directly. If the subnet is larger than /24, this can take a while."
                    if self._scan_mode == SETUP_MODE_DEEP_SCAN
                    else "Quick scan sends the initial discovery probe and waits for collectors on the selected local network to answer."
                ),
            ),
        }

    def _peer_label(self) -> str:
        return self._tr(
            "common.dynamic.peer_label",
            self._connection_display().peer_label,
        )

    def _peer_label_plural(self) -> str:
        return self._tr(
            "common.dynamic.peer_label_plural",
            self._connection_display().peer_label_plural,
        )

    def _unconfirmed_inverter_label(self) -> str:
        return self._tr(
            "common.dynamic.unconfirmed_inverter",
            self._connection_display().unconfirmed_inverter_label,
        )

    def _selector_for_connection_field(self, field: ConnectionFormField):
        """Resolve the concrete HA selector for one branch-aware connection field."""

        if field.selector_kind == "server_ip":
            return self._server_ip_field()
        if field.selector_kind == "ip":
            return _IP_TEXT_SELECTOR
        if field.selector_kind == "port":
            return _PORT_SELECTOR
        if field.selector_kind == "optional_port":
            return _IP_TEXT_SELECTOR
        if field.selector_kind == "discovery_interval":
            return _DISCOVERY_INTERVAL_SELECTOR
        if field.selector_kind == "heartbeat_interval":
            return _HEARTBEAT_INTERVAL_SELECTOR
        if field.selector_kind == "driver_hint":
            return _driver_selector(self._translation_bundle)
        raise ValueError(f"unsupported_connection_selector:{field.selector_kind}")

    def _build_connection_fields_schema(
        self,
        connection_type: str,
        *,
        fields: tuple[ConnectionFormField, ...],
        values: dict[str, Any],
    ) -> dict[Any, Any]:
        """Build a voluptuous schema mapping for branch-aware connection fields."""

        get_connection_branch(connection_type)
        schema: dict[Any, Any] = {}
        for field in fields:
            marker = vol.Required if field.required else vol.Optional
            schema[marker(field.key, default=values.get(field.key, ""))] = self._selector_for_connection_field(field)
        return schema

    # ---- description placeholders ----

    def _collector_network_placeholders(self) -> dict[str, str]:
        return {
            "selected_scan_interface": self._selected_interface_label(),
            "peer_label": self._peer_label(),
        }

    def _collector_operation_placeholders(self) -> dict[str, str]:
        if self._selected_result is None:
            return {}
        placeholders = self._result_placeholders(self._selected_result)
        placeholders.update(
            {
                "collector_callback_target_endpoint": self._collector_callback_target_endpoint(),
            }
        )
        return placeholders

    def _endpoint_originality_hint(self, endpoint: str) -> str:
        normalized = str(endpoint or "").strip().lower()
        if not normalized:
            return self._tr(
                "common.dynamic.collector_endpoint_unknown_hint",
                "The current collector callback endpoint could not be read yet.",
            )

        host = normalized.split(",", 1)[0]
        family = ""
        try:
            parsed = inspect_collector_server_endpoint(
                normalized,
                require_explicit_port=False,
                require_explicit_protocol=False,
            )
        except ValueError:
            parsed = None
        if parsed is not None:
            host = str(parsed.host or "").strip().lower()
            family = collector_cloud_family_observation_from_endpoint(normalized).family
            if family == "unknown":
                family = ""

        if family or "eybond" in host or "smartess" in host:
            return self._tr(
                "common.dynamic.collector_endpoint_original_hint",
                "This looks like the original cloud endpoint. Write it down before continuing; the integration will remember it, but keeping your own copy is safer.",
            )
        return self._tr(
            "common.dynamic.collector_endpoint_custom_hint",
            "This endpoint does not look like the stock cloud address. Make sure you know how to restore it before continuing.",
        )

    def _auto_description_placeholders(self, single_interface: bool) -> dict[str, str]:
        if single_interface and self._interface_options:
            item = self._interface_options[0]
            return {
                "interface_hint": self._tr(
                    "common.dynamic.auto_interface_hint_single",
                    "Home Assistant will use **{selected_interface}** automatically.",
                    {"selected_interface": item["label"]},
                ),
            }
        return {
            "interface_hint": self._tr(
                "common.dynamic.auto_interface_hint_multi",
                "Choose which Home Assistant interface the {peer_label} should connect back to.",
                {"peer_label": self._peer_label()},
            ),
        }

    def _deep_scan_placeholders(self) -> dict[str, str]:
        plan = self._deep_scan_plan()
        network_cidr = plan["network_cidr"]
        target_count = plan["target_count"]
        if not network_cidr:
            warning = self._tr(
                "common.dynamic.deep_scan_warning_unknown_network",
                "Home Assistant did not report the subnet mask for this interface. Deep scan will fall back to the currently reachable local subnet only.",
            )
        elif target_count <= 0:
            warning = self._tr(
                "common.dynamic.deep_scan_warning_empty_network",
                "The selected interface does not expose any additional IPv4 addresses to probe beyond Home Assistant itself.",
            )
        elif plan["large_subnet"]:
            warning = self._tr(
                "common.dynamic.deep_scan_warning_long",
                "Deep scan keeps the initial broadcast probe, then checks the remaining addresses directly. If the selected subnet is larger than /24, this can take a while.",
            )
        else:
            warning = self._tr(
                "common.dynamic.deep_scan_warning_short",
                "The deep scan keeps the initial broadcast probe and then checks the rest of this IPv4 network directly.",
            )
        return {
            "selected_scan_interface": self._selected_interface_label(),
            "deep_scan_network": network_cidr or self._tr("common.dynamic.unknown", "Unknown"),
            "deep_scan_target_count": str(target_count),
            "deep_scan_warning": warning,
        }

    def _welcome_description_placeholders(self) -> dict[str, str]:
        display = self._connection_display()
        if len(self._interface_options) > 1:
            return {
                "welcome_hint": self._tr(
                    "common.dynamic.welcome_connection_type_multi",
                    "Choose the connection type first. The wizard will then continue with collector network setup and the next onboarding steps.",
                    {
                        "integration_name": display.integration_name,
                    },
                ),
            }
        return {
            "welcome_hint": self._tr(
                "common.dynamic.welcome_connection_type_single",
                "Choose the connection type first. The wizard will then continue with collector network setup and the next onboarding steps.",
                {
                    "integration_name": display.integration_name,
                },
            ),
        }

    def _manual_confirm_placeholders(
        self,
        manual_config: dict[str, Any],
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        collector_ip = ""
        collector_pn = ""
        smartess_collector_version = ""
        smartess_protocol_asset_id = ""
        model_name = self._unconfirmed_inverter_label()
        serial_number = self._tr("common.dynamic.not_available_yet", "Not available yet")

        if result is not None and result.collector is not None:
            collector_ip = result.collector.ip
            collector = result.collector.collector
            if collector is not None:
                smartess_collector_version = collector.smartess_collector_version or ""
                smartess_protocol_asset_id = collector.smartess_protocol_asset_id or ""
        collector_pn = self._collector_pn_for_result(result)
        if not collector_ip:
            collector_ip = manual_config.get(CONF_COLLECTOR_IP) or manual_config.get(CONF_DISCOVERY_TARGET, "")

        smartess_hint_available = bool(
            smartess_collector_version or smartess_protocol_asset_id
        )

        if result is not None and result.match is not None:
            model_name = result.match.model_name
            serial_number = result.match.serial_number or serial_number

        if result is not None and result.match is not None:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_confirmed",
                "{peer_label_capitalized} and inverter were confirmed with the manual settings.",
                {"peer_label_capitalized": self._peer_label().capitalize()},
            )
        elif (
            result is not None
            and result.collector is not None
            and result.collector.connected
            and smartess_hint_available
        ):
            probe_summary = self._tr(
                "common.dynamic.manual_probe_smartess_hint",
                "The {peer_label} responded and exposed SmartESS metadata, but the local inverter model is still unconfirmed.",
                {"peer_label": self._peer_label()},
            )
        elif result is not None and result.collector is not None and result.collector.connected:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_unconfirmed_model",
                "The {peer_label} responded, but the inverter model is still unconfirmed.",
                {"peer_label": self._peer_label()},
            )
        else:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_none",
                "No {peer_label} or inverter was confirmed yet.",
                {"peer_label": self._peer_label()},
            )

        return {
            "probe_summary": probe_summary,
            "collector_ip": collector_ip or self._tr("common.dynamic.unknown", "Unknown"),
            "collector_pn": collector_pn or self._tr("common.dynamic.unknown", "Unknown"),
            "model_name": model_name,
            "serial_number": serial_number,
            "smartess_cloud_summary": self._smartess_cloud_summary(result),
            "control_summary": self._tr(
                (
                    "common.dynamic.manual_control_summary_smartess_hint"
                    if smartess_hint_available
                    else "common.dynamic.manual_control_summary"
                ),
                (
                    "If you continue, a **read-only Pending Device** will be created. In Home Assistant it appears as **EyeBond Setup Pending**. Sensors may stay unavailable until a local driver match is confirmed. This local probe does not rule out SmartESS app support; the app may still use a separate cloud identity."
                    if smartess_hint_available
                    else "If you continue, a **read-only Pending Device** will be created. In Home Assistant it appears as **EyeBond Setup Pending**. Sensors may stay unavailable until the {peer_label} connects and detection completes."
                ),
                {"peer_label": self._peer_label()},
            ),
            "next_actions_hint": self._tr(
                "common.dynamic.manual_probe_next_actions",
                "Choose **{probe_again_action_label}** to test again, **{edit_settings_action_label}** to change the values, or **{create_pending_action_label}** to save the read-only Pending Device now.",
                {
                    "probe_again_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
                        "Probe again",
                    ),
                    "edit_settings_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
                        "Edit settings",
                    ),
                    "create_pending_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_CREATE_PENDING,
                        "Save Pending Device",
                    ),
                },
            ),
        }

    @staticmethod
    def _validate_connection_inputs(
        user_input: dict[str, Any],
        *,
        fields: tuple[ConnectionFormField, ...],
    ) -> dict[str, str]:
        errors: dict[str, str] = {}
        for field in fields:
            raw_value = str(user_input.get(field.key, "") or "").strip()
            if field.validation_kind == "ipv4":
                if not raw_value:
                    if field.required:
                        errors[field.key] = "invalid_ip"
                    continue
                if not _is_ipv4(raw_value):
                    errors[field.key] = "invalid_ip"
                continue
            if field.validation_kind == "port_optional":
                if not raw_value:
                    continue
                if not raw_value.isdigit() or not 1 <= int(raw_value) <= 65535:
                    errors[field.key] = "invalid_port"
        return errors

    # ---- scan result helpers ----

    def _result_label(self, result: OnboardingResult) -> str:
        match = result.match
        collector = result.collector
        collector_ip = collector.ip if collector is not None else self._tr("common.dynamic.unknown", "Unknown")
        status_label = self._result_status_label(result)
        if match is None:
            suffix = (
                self._tr(
                    "common.dynamic.suffix_smartess_hint",
                    "SmartESS metadata",
                )
                if has_smartess_collector_hint(result)
                else self._tr(
                    "common.dynamic.suffix_peer_connected",
                    "{peer_label} connected",
                    {"peer_label": self._peer_label()},
                )
                if collector is not None and collector.connected
                else self._tr(
                    "common.dynamic.suffix_peer_only",
                    "{peer_label} only",
                    {"peer_label": self._peer_label()},
                )
            )
            return self._tr(
                "common.dynamic.result_label_unmatched",
                "{status_label}: {collector_ip} ({suffix})",
                {
                    "status_label": status_label,
                    "collector_ip": collector_ip,
                    "suffix": suffix,
                },
            )
        serial = match.serial_number or self._tr("common.dynamic.unknown_serial", "unknown serial")
        return self._tr(
            "common.dynamic.result_label_matched",
            "{status_label}: {model_name} ({serial_number}) on {collector_ip} — {confidence_label}",
            {
                "status_label": status_label,
                "model_name": match.model_name,
                "serial_number": serial,
                "collector_ip": collector_ip,
                "confidence_label": self._confidence_label(result.confidence),
            },
        )

    @staticmethod
    def _escape_markdown_table_cell(value: object) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    @staticmethod
    def _onboarding_first_present_value(details: dict[str, Any], *keys: str) -> object | None:
        for key in keys:
            value = details.get(key)
            if value not in (None, ""):
                return value
        return None

    def _onboarding_confirm_table(
        self,
        heading_key: str,
        heading_fallback: str,
        rows: list[tuple[str, str, str]],
    ) -> str:
        lines = [
            self._tr(heading_key, heading_fallback),
            "",
            f"| {self._tr('common.dynamic.onboarding_confirm_table_label', 'Detail')} | {self._tr('common.dynamic.onboarding_confirm_table_value', 'Value')} |",
            "|---|---|",
        ]
        for label_key, label_fallback, value in rows:
            lines.append(
                f"| {self._tr(label_key, label_fallback)} | {self._escape_markdown_table_cell(value)} |"
            )
        return "\n".join(lines)

    def _onboarding_confirm_measurement(
        self,
        value: object,
        *,
        unit_key: str,
        unit_fallback: str,
    ) -> str:
        if value in (None, ""):
            return self._tr("common.dynamic.not_available_yet", "Not available yet")
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        if isinstance(value, (int, float)):
            return self._tr(unit_key, unit_fallback, {"value": value})
        return str(value)

    def _onboarding_confirm_battery_connection(self, value: object) -> str:
        if value in (None, ""):
            return self._tr("common.dynamic.not_available_yet", "Not available yet")
        if isinstance(value, bool):
            return self._tr(
                "common.dynamic.onboarding_confirm_battery_connected"
                if value
                else "common.dynamic.onboarding_confirm_battery_disconnected",
                "Connected" if value else "Not connected",
            )
        if isinstance(value, (int, float)) and value in (0, 1):
            return self._tr(
                "common.dynamic.onboarding_confirm_battery_connected"
                if int(value) == 1
                else "common.dynamic.onboarding_confirm_battery_disconnected",
                "Connected" if int(value) == 1 else "Not connected",
            )
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "connected", "present"}:
                return self._tr(
                    "common.dynamic.onboarding_confirm_battery_connected",
                    "Connected",
                )
            if normalized in {"0", "false", "no", "not connected", "disconnected", "absent"}:
                return self._tr(
                    "common.dynamic.onboarding_confirm_battery_disconnected",
                    "Not connected",
                )
        return str(value)

    def _result_placeholders(self, result: OnboardingResult) -> dict[str, str]:
        collector = result.collector
        match = result.match
        assist_state = self._smartess_cloud_assist_state_for_result(result)
        collector_ip = (
            collector.ip if collector is not None and collector.ip else ""
        ) or (
            collector.target_ip if collector is not None and collector.target_ip else ""
        ) or self._tr("common.dynamic.unknown", "Unknown")
        not_available_yet = self._tr("common.dynamic.not_available_yet", "Not available yet")
        collector_pn = self._collector_pn_for_result(result)
        collector_info = collector.collector if collector is not None else None
        driver_key = match.driver_key if match is not None else DRIVER_HINT_AUTO
        if match is None and assist_state is not None and assist_state.inferred_driver_key:
            driver_key = f"{assist_state.inferred_driver_key} (cloud-assisted)"
        match_details = match.details if match is not None else {}
        rated_power = self._onboarding_confirm_measurement(
            self._onboarding_first_present_value(
                match_details,
                "rated_power",
                "output_rating_active_power",
            ),
            unit_key="common.dynamic.onboarding_confirm_power_value",
            unit_fallback="{value} W",
        )
        collector_confirm_table = self._onboarding_confirm_table(
            "common.dynamic.onboarding_confirm_collector_heading",
            "**Collector**",
            [
                (
                    "common.dynamic.onboarding_confirm_collector_pn_label",
                    "Collector PN",
                    collector_pn or not_available_yet,
                ),
                (
                    "common.dynamic.onboarding_confirm_collector_ip_label",
                    "Collector IP",
                    collector_ip,
                ),
            ],
        )
        inverter_confirm_table = self._onboarding_confirm_table(
            "common.dynamic.onboarding_confirm_inverter_heading",
            "**Inverter**",
            [
                (
                    "common.dynamic.onboarding_confirm_model_label",
                    "Model",
                    match.model_name if match is not None else self._unconfirmed_inverter_label(),
                ),
                (
                    "common.dynamic.onboarding_confirm_rated_power_label",
                    "Rated Power",
                    rated_power,
                ),
                (
                    "common.dynamic.onboarding_confirm_serial_number_label",
                    "Serial Number",
                    match.serial_number if match is not None else not_available_yet,
                ),
                (
                    "common.dynamic.onboarding_confirm_detection_confidence_label",
                    "Detection Confidence",
                    self._confidence_label(result.confidence),
                ),
                (
                    "common.dynamic.onboarding_confirm_protocol_family_label",
                    "Protocol Family",
                    match.protocol_family if match is not None and match.protocol_family else not_available_yet,
                ),
            ],
        )
        return {
            "model_name": match.model_name if match is not None else self._unconfirmed_inverter_label(),
            "serial_number": match.serial_number if match is not None else not_available_yet,
            "driver_key": driver_key,
            "collector_ip": collector_ip,
            "collector_pn": collector_pn or self._tr("common.dynamic.unknown", "Unknown"),
            "confidence": self._confidence_label(result.confidence),
            "collector_confirm_table": collector_confirm_table,
            "inverter_confirm_table": inverter_confirm_table,
            "smartess_cloud_summary": self._smartess_cloud_summary(result),
            "control_summary": self._default_control_summary(result.confidence),
        }

    def _confidence_label(self, confidence: str) -> str:
        return {
            "high": self._tr("common.dynamic.confidence_high", "High confidence"),
            "medium": self._tr("common.dynamic.confidence_medium", "Medium confidence"),
            "low": self._tr("common.dynamic.confidence_low", "Low confidence"),
            "none": self._tr("common.dynamic.confidence_none", "No confidence"),
        }.get(confidence, confidence)

    def _default_control_summary(self, confidence: str) -> str:
        if confidence == "high":
            return self._tr(
                "common.dynamic.control_auto",
                "Tested controls are enabled automatically.",
            )
        return self._tr(
            "common.dynamic.control_waiting",
            "Monitoring only until a high-confidence detection is confirmed.",
        )

    def _result_unique_id(self, result: OnboardingResult) -> str:
        collector_ip = result.collector.ip if result.collector is not None else ""
        collector_pn = self._collector_pn_for_result(result)
        server_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        return (
            f"collector:{collector_pn}"
            if collector_pn
            else f"inverter:{result.match.serial_number}"
            if result.match is not None and result.match.serial_number
            else f"collector_ip:{collector_ip}"
            if collector_ip
            else f"listener:{server_ip}:{DEFAULT_TCP_PORT}"
        )

    def _configured_collector_probe_skip_ips(self) -> frozenset[str]:
        """Collector IPs owned by existing entries: scans must not probe them.

        Probing would steal the collector's callback session from the running
        entry; the scan lists them as already added instead.
        """

        ips: set[str] = set()
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.entry_id == self.context.get("entry_id"):
                continue
            ip = str(entry.data.get(CONF_COLLECTOR_IP, "") or "").strip()
            if ip:
                ips.add(ip)
        return frozenset(ips)

    def _existing_entry_for_result(self, result: OnboardingResult):
        collector = result.collector
        collector_pn = self._collector_pn_for_result(result)
        collector_ip = collector.ip if collector is not None else ""
        serial_number = result.match.serial_number if result.match is not None else ""
        candidate_unique_id = self._result_unique_id(result)

        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.entry_id == self.context.get("entry_id"):
                continue
            if entry.unique_id and entry.unique_id == candidate_unique_id:
                return entry
            entry_collector_pn = entry.data.get(CONF_COLLECTOR_PN, "")
            entry_serial = entry.data.get(CONF_DETECTED_SERIAL, "")
            entry_collector_ip = entry.data.get(CONF_COLLECTOR_IP, "")
            if collector_pn and _collector_identity_matches(entry_collector_pn, collector_pn):
                return entry
            if serial_number and entry_serial == serial_number:
                return entry
            if not collector_pn and not serial_number and collector_ip and entry_collector_ip == collector_ip:
                return entry
        return None

    def _already_added_ble_candidate_addresses(
        self,
        candidates: tuple[SmartEssBleCandidate, ...],
    ) -> set[str]:
        if not candidates:
            return set()

        existing_pns: set[str] = set()
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            entry_collector_pn = str(entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
            if entry_collector_pn:
                existing_pns.add(entry_collector_pn)
            entry_unique_id = str(getattr(entry, "unique_id", "") or "").strip()
            if entry_unique_id.startswith("collector:"):
                existing_pns.add(entry_unique_id.split(":", 1)[1])

        return {
            candidate.address
            for candidate in candidates
            if str(candidate.local_pn or "").strip() in existing_pns
        }

    @staticmethod
    def _is_visible_scan_result(result: OnboardingResult) -> bool:
        collector = result.collector
        if result.last_error == "already_configured":
            # Configured collectors are not probed, but the user must still
            # see that the scan accounted for them.
            return True
        if result.match is not None:
            return True
        if collector is None:
            return False
        collector_info = collector.collector
        return bool(
            collector.connected
            or collector.udp_reply
            or (collector_info is not None and collector_info.collector_pn)
        )

    @staticmethod
    def _is_addable_scan_result(result: OnboardingResult) -> bool:
        collector = result.collector
        collector_info = collector.collector if collector is not None else None
        return bool(
            result.match is not None
            or (
                collector is not None
                and (
                    collector.connected
                    or bool(collector.udp_reply)
                    or (collector_info is not None and collector_info.collector_pn)
                )
            )
        )

    def _available_autodetect_results(self) -> dict[str, OnboardingResult]:
        return {
            key: result
            for key, result in self._sorted_autodetect_items()
            if self._is_addable_scan_result(result)
            if self._existing_entry_for_result(result) is None
        }

    def _scan_result_key(self, result: OnboardingResult) -> str:
        collector = result.collector
        collector_pn = self._collector_pn_for_result(result)
        if collector_pn:
            return f"collector:{collector_pn}"
        if collector is not None and collector.ip:
            return f"ip:{collector.ip}"
        if collector is not None and collector.target_ip:
            return f"target:{collector.target_ip}"
        if result.match is not None and result.match.serial_number:
            return f"serial:{result.match.serial_number}"
        return "unknown"

    @staticmethod
    def _scan_result_priority(result: OnboardingResult) -> tuple[int, int, int, int, int]:
        collector = result.collector
        collector_info = collector.collector if collector is not None else None
        return (
            1 if result.match is not None else 0,
            1 if collector is not None and collector.connected else 0,
            1 if collector is not None and collector.udp_reply else 0,
            confidence_sort_score(result.confidence),
            1 if collector_info is not None and collector_info.collector_pn else 0,
        )

    def _sorted_autodetect_items(self) -> list[tuple[str, OnboardingResult]]:
        return sorted(
            self._autodetect_results.items(),
            key=lambda item: scan_result_sort_key(
                item[1],
                already_added=self._existing_entry_for_result(item[1]) is not None,
            ),
        )

    def _sort_scan_results(self, results: list[OnboardingResult]) -> list[OnboardingResult]:
        return sorted(
            results,
            key=lambda result: scan_result_sort_key(
                result,
                already_added=self._existing_entry_for_result(result) is not None,
            ),
        )

    @staticmethod
    def _scan_result_status_code(result: OnboardingResult, already_added: bool = False) -> str:
        return scan_result_status_code(result, already_added)

    @classmethod
    def _scan_result_sort_key(
        cls,
        result: OnboardingResult,
        *,
        already_added: bool = False,
    ) -> tuple[int, int, str, str, str]:
        return scan_result_sort_key(result, already_added=already_added)

    def _collapse_scan_results(
        self,
        results: Any,
    ) -> list[OnboardingResult]:
        collapsed: dict[str, OnboardingResult] = {}
        for result in results:
            key = self._scan_result_key(result)
            collector_pn = self._collector_pn_for_result(result)
            collector_ip = result.collector.ip if result.collector is not None else ""
            for existing_key, existing in collapsed.items():
                existing_pn = self._collector_pn_for_result(existing)
                if collector_pn and _collector_identity_matches(existing_pn, collector_pn):
                    key = existing_key
                    break
                # One collector seen through two sources (e.g. the skip-probe
                # marker for a configured IP plus a PN-carrying session-inventory
                # result) must collapse into one line. Same-IP merging applies
                # only when at least one side lacks a PN: two different
                # collectors behind one NAT IP both carry PNs and stay apart.
                if (
                    collector_ip
                    and existing.collector is not None
                    and existing.collector.ip == collector_ip
                    and (not collector_pn or not existing_pn)
                ):
                    key = existing_key
                    break
            current = collapsed.get(key)
            if current is None or self._scan_result_priority(result) > self._scan_result_priority(current):
                collapsed[key] = result
        return list(collapsed.values())

    def _scan_results_placeholders(self) -> dict[str, str]:
        results = self._sorted_autodetect_items()
        available_count = 0
        already_added_count = 0
        selected_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        refresh_action_label = self._refresh_scan_action_label()
        deep_scan_action_label = self._scan_action_label("deep_scan", "Run deep scan")
        manual_action_label = self._scan_action_label("manual", "Manual setup")
        selected_label = self._selected_interface_label(selected_ip)
        deep_scan_available = True
        for _, result in results:
            existing_entry = self._existing_entry_for_result(result)
            if existing_entry is not None:
                already_added_count += 1
            elif self._is_addable_scan_result(result):
                available_count += 1

        detected_count = len(results)
        ready_models = [
            result.match.model_name
            for result in self._available_autodetect_results().values()
            if result.match is not None and result.match.model_name
        ]
        candidate_list = "\n".join(
            self._scan_result_line(index, result)
            for index, (_, result) in enumerate(results, start=1)
        )
        if detected_count == 0:
            scan_summary = self._tr(
                "common.dynamic.scan_no_results_summary",
                "No reachable {peer_label_plural} or inverters were found.",
                {"peer_label_plural": self._peer_label_plural()},
            )
            if deep_scan_available:
                next_hint = self._tr(
                    "common.dynamic.scan_no_results_next_with_deep",
                    "Use **{refresh_action_label}** to try again, **{deep_scan_action_label}** to scan the full local network, or **{manual_action_label}** to switch to manual setup.",
                    {
                        "refresh_action_label": refresh_action_label,
                        "deep_scan_action_label": deep_scan_action_label,
                        "manual_action_label": manual_action_label,
                    },
                )
            else:
                next_hint = self._tr(
                    "common.dynamic.scan_no_results_next",
                    "Use **{refresh_action_label}** to try again, or **{manual_action_label}** to switch to manual setup.",
                    {
                        "refresh_action_label": refresh_action_label,
                        "manual_action_label": manual_action_label,
                    },
                )
        elif available_count == 0 and already_added_count == detected_count:
            scan_summary = self._tr(
                "common.dynamic.scan_all_added_summary",
                "Found **{detected_count}** device candidate(s), but all of them are already configured.",
                {"detected_count": detected_count},
            )
            if deep_scan_available:
                next_hint = self._tr(
                    "common.dynamic.scan_all_added_next_with_deep",
                    "Use **{refresh_action_label}** to look again, **{deep_scan_action_label}** to search the full local network, or **{manual_action_label}** if you intentionally need a different connection path.",
                    {
                        "refresh_action_label": refresh_action_label,
                        "deep_scan_action_label": deep_scan_action_label,
                        "manual_action_label": manual_action_label,
                    },
                )
            else:
                next_hint = self._tr(
                    "common.dynamic.scan_all_added_next",
                    "Use **{refresh_action_label}** to look again, or **{manual_action_label}** if you intentionally need a different connection path.",
                    {
                        "refresh_action_label": refresh_action_label,
                        "manual_action_label": manual_action_label,
                    },
                )
        elif available_count == 0:
            scan_summary = self._tr(
                "common.dynamic.scan_none_addable_summary",
                "Found **{detected_count}** device candidate(s), but none are ready to add yet.",
                {"detected_count": detected_count},
            )
            if deep_scan_available:
                next_hint = self._tr(
                    "common.dynamic.scan_none_addable_next_with_deep",
                    "Use **{refresh_action_label}** to try again, **{deep_scan_action_label}** to check the full local network, or **{manual_action_label}** to override the connection settings.",
                    {
                        "refresh_action_label": refresh_action_label,
                        "deep_scan_action_label": deep_scan_action_label,
                        "manual_action_label": manual_action_label,
                    },
                )
            else:
                next_hint = self._tr(
                    "common.dynamic.scan_none_addable_next",
                    "Use **{refresh_action_label}** to try again, or **{manual_action_label}** to override the connection settings.",
                    {
                        "refresh_action_label": refresh_action_label,
                        "manual_action_label": manual_action_label,
                    },
                )
        elif not ready_models:
            scan_summary = self._tr(
                "common.dynamic.scan_pending_summary",
                "Found **{detected_count}** device candidate(s). **{available_count}** collector candidate(s) can be added now, but local inverter matching is still pending.",
                {
                    "detected_count": detected_count,
                    "available_count": available_count,
                },
            )
            next_hint = self._tr(
                "common.dynamic.scan_pending_next_select",
                "Pick a device below to save the Pending Device now, or use **{refresh_action_label}** or **{manual_action_label}** to retry the local match.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        else:
            ready_summary = (
                ", ".join(dict.fromkeys(ready_models[:5]))
                or self._tr("common.dynamic.scan_ready_fallback", "detected inverters")
            )
            scan_summary = self._tr(
                "common.dynamic.scan_ready_summary",
                "Found **{detected_count}** device candidate(s). **{available_count}** can be added now, **{already_added_count}** already configured. Ready now: {ready_summary}.",
                {
                    "detected_count": detected_count,
                    "available_count": available_count,
                    "already_added_count": already_added_count,
                    "ready_summary": ready_summary,
                },
            )
            next_hint = self._tr(
                "common.dynamic.scan_ready_next_select",
                "Pick the inverter you want to add from the list below.",
            )

        return {
            "scan_summary": scan_summary,
            "scan_next_hint": next_hint,
            "selected_scan_interface": selected_label,
            "candidate_list": candidate_list,
        }

    def _choose_placeholders(self) -> dict[str, str]:
        return {
            "choose_summary": self._tr(
                "common.dynamic.choose_summary",
                "**{available_count}** detected device candidate(s) can be added right now. Already configured devices are excluded.",
                {"available_count": len(self._available_autodetect_results())},
            )
        }

    def _driver_choice_placeholders(self, result: OnboardingResult) -> dict[str, str]:
        candidates = self._driver_choice_candidates(result)
        include_address = self._driver_choice_needs_address(candidates)
        candidate_lines = "\n".join(
            self._driver_choice_line(
                index,
                match,
                recommended=index == 1,
                include_address=include_address,
            )
            for index, match in enumerate(candidates, start=1)
        )
        return {
            "driver_choice_summary": self._tr(
                "common.dynamic.driver_choice_summary",
                "This inverter answered over **{count}** different protocols during the deep scan, so you can choose which driver Home Assistant will use. Protocols can differ in polling speed — the response time below is a good hint. If unsure, keep the recommended option.",
                {"count": len(candidates)},
            ),
            "driver_choice_candidates": candidate_lines,
        }

    def _scan_result_line(self, index: int, result: OnboardingResult) -> str:
        collector = result.collector
        collector_ip = collector.ip if collector is not None else self._tr("common.dynamic.unknown", "Unknown")
        existing_entry = self._existing_entry_for_result(result)
        collector_pn = self._collector_pn_for_result(result)
        status_label = self._result_status_label(result, existing_entry is not None)

        status_code = scan_result_status_code(result, existing_entry is not None)
        if result.match is not None:
            confidence = self._confidence_label(result.confidence)
            # Lowercase mid-line, same as the driver-choice presentation.
            confidence = confidence[:1].lower() + confidence[1:]
            details = [
                result.match.model_name or self._unconfirmed_inverter_label(),
                f"{self._peer_label()} {collector_ip}",
            ]
            if result.match.serial_number:
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_serial",
                        "serial {serial_number}",
                        {"serial_number": result.match.serial_number},
                    )
                )
            details.append(confidence)
            line = f"{index}. **{status_label}** — " + " · ".join(details)
        else:
            # The status chip already names the situation (SmartESS hint,
            # collector connected, already added, ...): the details carry only
            # what the chip does not say.
            details = [f"{self._peer_label()} {collector_ip}"]
            if collector_pn:
                details.append(f"PN {collector_pn}")
            if (
                status_code not in ("smartess_hint", "already_added")
                and has_smartess_collector_hint(result)
            ):
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_smartess_hint",
                        "SmartESS metadata",
                    )
                )
            if (
                status_code in ("detection_timeout", "unknown")
                and collector is not None
                and collector.connected
            ):
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_peer_connected",
                        "{peer_label} connected",
                        {"peer_label": self._peer_label()},
                    )
                )
            elif status_code == "unknown" and collector is not None and collector.udp_reply:
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_peer_replied",
                        "{peer_label} replied, waiting for reverse connection",
                        {"peer_label": self._peer_label()},
                    )
                )
            line = f"{index}. **{status_label}** — " + " · ".join(details)

        if existing_entry is not None:
            line += " " + self._tr(
                "common.dynamic.scan_line_already_added",
                '*(already added as "{entry_title}")*',
                {"entry_title": existing_entry.title},
            )
        return line

    def _result_status_label(self, result: OnboardingResult, already_added: bool = False) -> str:
        status_code = scan_result_status_code(result, already_added)
        return {
            "ready": self._tr("common.dynamic.status_ready", "Ready"),
            "driver_choice": self._tr("common.dynamic.status_driver_choice", "Driver choice"),
            "review": self._tr("common.dynamic.status_review", "Review"),
            "already_added": self._tr("common.dynamic.status_already_added", "Already added"),
            "detection_timeout": self._tr("common.dynamic.status_detection_timeout", "Detection ran out of time"),
            "smartess_hint": self._tr("common.dynamic.status_smartess_hint", "SmartESS hint"),
            "collector_only": self._tr("common.dynamic.status_collector_only", "Collector only"),
            "collector_replied": self._tr("common.dynamic.status_collector_replied", "Collector replied"),
            "unknown": self._tr("common.dynamic.status_unknown", "Unknown"),
        }.get(status_code, self._tr("common.dynamic.status_unknown", "Unknown"))


# ---------------------------------------------------------------------------
# Options flow
# ---------------------------------------------------------------------------

class EybondLocalOptionsFlow(_TranslationBundleMixin, OptionsFlow):
    """Config entry options."""

    def __init__(self, config_entry) -> None:
        self._config_entry = config_entry
        self._translation_bundle: dict[str, Any] = {}
        self._translation_bundle_language = ""
        self._interface_options: list[dict[str, str]] = []
        self._diagnostics_result: dict[str, str] = {}
        self._diagnostic_commands_text = ""
        self._diagnostic_commands_output = ""
        self._diagnostic_commands_download_url = ""
        self._diagnostic_commands_result_path = ""
        self._diagnostic_publish_download_copy = False
        self._runtime_poll_interval_pending_input: dict[str, Any] = {}
        self._collector_wifi_current_ssid = ""
        self._collector_wifi_network_diagnostics = ""
        self._collector_wifi_last_error = ""
        self._collector_wifi_last_result = ""
        self._collector_wifi_networks: tuple[SmartEssBleWifiNetwork, ...] = ()
        self._collector_uart_current_settings = ""
        self._collector_uart_current_baudrate = ""
        self._collector_uart_hardware_version = ""
        self._collector_uart_last_error = ""
        self._collector_uart_last_result = ""
        self._shadow_learning_state: dict[str, Any] = {}

    def _server_ip_field(self) -> SelectSelector | TextSelector:
        """Return the user-friendly selector for one local server IP."""

        if not self._interface_options:
            return _IP_TEXT_SELECTOR
        return _interface_selector(self._interface_options)

    def _selector_for_connection_field(self, field: ConnectionFormField):
        """Resolve one selector for branch-aware connection fields."""

        return EybondLocalConfigFlow._selector_for_connection_field(self, field)

    def _build_connection_fields_schema(
        self,
        connection_type: str,
        *,
        fields: tuple[ConnectionFormField, ...],
        values: dict[str, Any],
    ) -> dict[Any, Any]:
        """Build one schema mapping for options-flow connection sections."""

        return EybondLocalConfigFlow._build_connection_fields_schema(
            self,
            connection_type,
            fields=fields,
            values=values,
        )

    def _collector_is_virtual_bridge(self) -> bool:
        """Return True when the entry's collector is a detected virtual bridge.

        Detection is positive-only: it reads the runtime snapshot's parsed
        hardware-version token. When the coordinator/snapshot is unavailable (older
        firmware, factory collector, or a missed query) this returns False, so
        the menu behaves exactly as before — the gate only ever removes
        cloud-only options, never adds restrictions to factory collectors.
        """

        return self._collector_capabilities().virtual_bridge

    def _collector_capabilities(self) -> CollectorCapabilityProfile:
        """Return current collector capability profile for options-flow gating."""

        coordinator = self._coordinator()
        data = getattr(coordinator, "data", None)
        collector = getattr(data, "collector", None)
        values = getattr(data, "values", None)
        return collector_capability_profile_from_runtime(
            collector=collector,
            values=values if isinstance(values, dict) else {},
            data=dict(getattr(self._config_entry, "data", {}) or {}),
            options=dict(getattr(self._config_entry, "options", {}) or {}),
            hardware_version=self._collector_uart_hardware_version,
        )

    @_with_translation_bundle
    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        capabilities = self._collector_capabilities()
        menu_options = ["runtime", "shadow_learning", "collector_wifi", "diagnostics"]
        bridge_note = ""
        if capabilities.virtual_bridge:
            # A local bridge has no SmartESS cloud side, so cloud-only control
            # discovery (shadow learning) is meaningless against it. Wi-Fi,
            # UART, runtime, and diagnostics stay — the bridge implements them.
            menu_options = ["runtime", "collector_wifi", "diagnostics"]
            if capabilities.uart_management:
                menu_options.insert(2, "collector_uart")
            bridge_note = self._tr(
                "common.dynamic.collector_virtual_bridge_note",
                "\n\nThis collector is a local ESP EyeBond Collector bridge with no "
                "SmartESS cloud side. Cloud-only actions (control discovery / "
                "shadow learning) are hidden; Wi-Fi, UART, and runtime settings remain "
                "available.",
            )
        return self.async_show_menu(
            step_id="init",
            menu_options=menu_options,
            description_placeholders={"bridge_note": bridge_note},
        )

    @_with_translation_bundle
    async def async_step_collector_wifi(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        selected_action = str(
            defaults.get(CONF_COLLECTOR_WIFI_ACTION, COLLECTOR_WIFI_ACTION_APPLY)
            or COLLECTOR_WIFI_ACTION_APPLY
        ).strip()
        if selected_action not in {COLLECTOR_WIFI_ACTION_REFRESH, COLLECTOR_WIFI_ACTION_APPLY}:
            selected_action = COLLECTOR_WIFI_ACTION_APPLY

        refresh_requested = user_input is not None and selected_action == COLLECTOR_WIFI_ACTION_REFRESH
        apply_requested = user_input is not None and selected_action == COLLECTOR_WIFI_ACTION_APPLY
        submitted_ssid = str(defaults.get(CONF_WIFI_SSID, "") or "").strip()
        submitted_password = str(defaults.get(CONF_WIFI_PASSWORD, "") or "")

        if user_input is None or refresh_requested:
            try:
                await self._async_refresh_collector_wifi_status()
            except Exception as exc:
                self._collector_wifi_last_error = _exception_detail(exc)
                errors["base"] = "collector_wifi_read_failed"
            else:
                self._collector_wifi_last_error = ""
                if refresh_requested:
                    self._collector_wifi_last_result = self._tr(
                        "common.dynamic.collector_wifi_refresh_done",
                        "Wi-Fi status refreshed.",
                    )
                    selected_action = COLLECTOR_WIFI_ACTION_APPLY

        if apply_requested:
            if not submitted_ssid:
                errors[CONF_WIFI_SSID] = "collector_wifi_ssid_required"
            elif not submitted_ssid.isascii():
                errors[CONF_WIFI_SSID] = "collector_wifi_ssid_not_ascii"
            if not submitted_password:
                errors[CONF_WIFI_PASSWORD] = "collector_wifi_password_required"
            elif not submitted_password.isascii():
                errors[CONF_WIFI_PASSWORD] = "collector_wifi_password_not_ascii"
            if not bool(defaults.get(CONF_CONFIRM_COLLECTOR_WIFI_APPLY)):
                errors[CONF_CONFIRM_COLLECTOR_WIFI_APPLY] = "collector_wifi_apply_not_confirmed"

            if not errors:
                try:
                    await self._async_apply_collector_wifi_settings(
                        ssid=submitted_ssid,
                        password=submitted_password,
                    )
                except Exception as exc:
                    self._collector_wifi_last_error = _exception_detail(exc)
                    errors["base"] = "collector_wifi_write_failed"
                else:
                    self._collector_wifi_last_error = ""
                    self._collector_wifi_last_result = self._tr(
                        "common.dynamic.collector_wifi_apply_done",
                        "Wi-Fi settings were accepted by the collector.",
                    )
                    return self.async_create_entry(data=dict(self._config_entry.options))

        default_wifi_ssid = submitted_ssid or self._collector_wifi_current_ssid
        password_default = submitted_password if errors and apply_requested else ""
        return self.async_show_form(
            step_id="collector_wifi",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_WIFI_SSID, default=default_wifi_ssid): _ble_wifi_selector(
                        self._collector_wifi_networks,
                    ),
                    vol.Optional(CONF_WIFI_PASSWORD, default=password_default): _PASSWORD_TEXT_SELECTOR,
                    vol.Required(CONF_COLLECTOR_WIFI_ACTION, default=selected_action): _collector_wifi_action_selector(
                        refresh_label=self._collector_wifi_refresh_action_label(),
                        apply_label=self._collector_wifi_apply_action_label(),
                    ),
                    vol.Required(CONF_CONFIRM_COLLECTOR_WIFI_APPLY, default=False): BooleanSelector(),
                }
            ),
            errors=errors,
            description_placeholders=self._collector_wifi_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_collector_uart(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._collector_capabilities().uart_management:
            return await self.async_step_init()

        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        selected_action = str(
            defaults.get(CONF_COLLECTOR_UART_ACTION, COLLECTOR_UART_ACTION_APPLY)
            or COLLECTOR_UART_ACTION_APPLY
        ).strip()
        if selected_action not in {COLLECTOR_UART_ACTION_REFRESH, COLLECTOR_UART_ACTION_APPLY}:
            selected_action = COLLECTOR_UART_ACTION_APPLY

        refresh_requested = user_input is not None and selected_action == COLLECTOR_UART_ACTION_REFRESH
        apply_requested = user_input is not None and selected_action == COLLECTOR_UART_ACTION_APPLY
        submitted_baudrate = self._normalize_collector_uart_baudrate(
            defaults.get(CONF_COLLECTOR_UART_BAUDRATE, "")
        )

        if user_input is None or refresh_requested:
            try:
                await self._async_refresh_collector_uart_status()
            except Exception as exc:
                self._collector_uart_last_error = _exception_detail(exc)
                errors["base"] = "collector_uart_read_failed"
            else:
                self._collector_uart_last_error = ""
                if refresh_requested:
                    self._collector_uart_last_result = self._tr(
                        "common.dynamic.collector_uart_refresh_done",
                        "Collector UART status has been refreshed.",
                    )
                    selected_action = COLLECTOR_UART_ACTION_APPLY

        if apply_requested:
            if self._collector_uart_runtime_change_unavailable():
                errors["base"] = "collector_uart_runtime_unavailable"
            elif submitted_baudrate not in COLLECTOR_UART_BAUDRATES:
                errors[CONF_COLLECTOR_UART_BAUDRATE] = "collector_uart_baudrate_invalid"
            if not bool(defaults.get(CONF_CONFIRM_COLLECTOR_UART_APPLY)):
                errors[CONF_CONFIRM_COLLECTOR_UART_APPLY] = "collector_uart_apply_not_confirmed"

            if not errors:
                try:
                    await self._async_apply_collector_uart_baudrate(submitted_baudrate)
                except Exception as exc:
                    self._collector_uart_last_error = _exception_detail(exc)
                    errors["base"] = "collector_uart_write_failed"
                else:
                    self._collector_uart_last_error = ""
                    self._collector_uart_last_result = self._tr(
                        "common.dynamic.collector_uart_apply_done",
                        "The collector accepted the new UART speed.",
                    )
                    return self.async_create_entry(data=dict(self._config_entry.options))

        default_baudrate = (
            submitted_baudrate
            or self._collector_uart_current_baudrate
            or self._normalize_collector_uart_baudrate(self._runtime_collector_uart_settings())
            or "2400"
        )
        runtime_change_unavailable = self._collector_uart_runtime_change_unavailable()
        if runtime_change_unavailable:
            selected_action = COLLECTOR_UART_ACTION_REFRESH
            schema_fields = {
                vol.Required(CONF_COLLECTOR_UART_ACTION, default=selected_action): _collector_uart_action_selector(
                    refresh_label=self._collector_uart_refresh_action_label(),
                    apply_label=self._collector_uart_apply_action_label(),
                    include_apply=False,
                ),
            }
        else:
            schema_fields = {
                vol.Required(CONF_COLLECTOR_UART_BAUDRATE, default=default_baudrate): _collector_uart_baudrate_selector(),
                vol.Required(CONF_COLLECTOR_UART_ACTION, default=selected_action): _collector_uart_action_selector(
                    refresh_label=self._collector_uart_refresh_action_label(),
                    apply_label=self._collector_uart_apply_action_label(),
                ),
                vol.Required(CONF_CONFIRM_COLLECTOR_UART_APPLY, default=False): BooleanSelector(),
            }
        return self.async_show_form(
            step_id="collector_uart",
            data_schema=vol.Schema(schema_fields),
            errors=errors,
            description_placeholders=self._collector_uart_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_runtime(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._interface_options:
            self._interface_options = await self.hass.async_add_executor_job(_get_ipv4_interfaces)
        # A detected bridge is inherently Home Assistant only and the operation-mode
        # selector is hidden for it, so the submitted form carries no mode value.
        # Fail-safe: factory collectors / unanswered probes keep today's selector.
        capabilities = self._collector_capabilities()
        is_bridge = capabilities.virtual_bridge
        errors: dict[str, str] = {}
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            if is_bridge:
                # Force HA-only regardless of any (absent) selector input.
                flat_input[CONF_COLLECTOR_OPERATION_MODE] = COLLECTOR_OPERATION_HA_ONLY
            flat_input.setdefault(
                CONF_COLLECTOR_OPERATION_MODE,
                self._config_entry.options.get(
                    CONF_COLLECTOR_OPERATION_MODE,
                    self._config_entry.data.get(
                        CONF_COLLECTOR_OPERATION_MODE,
                        DEFAULT_COLLECTOR_OPERATION_MODE,
                    ),
                ),
            )
            flat_input.setdefault(
                CONF_POLL_MODE,
                self._config_entry.options.get(CONF_POLL_MODE, POLL_MODE_MANUAL),
            )
            if flat_input.get(CONF_POLL_MODE) not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
                errors[CONF_POLL_MODE] = "invalid_selection"
            connection_type = self._config_entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
            branch = get_connection_branch(connection_type)
            errors.update(EybondLocalConfigFlow._validate_connection_inputs(
                flat_input,
                fields=branch.form_layout.runtime_fields,
            ))
            if flat_input.get(CONF_COLLECTOR_OPERATION_MODE) not in COLLECTOR_OPERATION_MODES:
                errors[CONF_COLLECTOR_OPERATION_MODE] = "invalid_selection"
            if not errors:
                if (
                    flat_input.get(CONF_POLL_MODE) == POLL_MODE_MANUAL
                    and CONF_POLL_INTERVAL not in flat_input
                ):
                    self._runtime_poll_interval_pending_input = dict(flat_input)
                    return await self.async_step_runtime_poll_interval()
                return self.async_create_entry(
                    data=self._build_runtime_options_from_flat_input(flat_input)
                )

        connection_type = self._config_entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
        branch = get_connection_branch(connection_type)
        connection_values = branch.build_runtime_option_values(
            data=self._config_entry.data,
            options=self._config_entry.options,
            default_server_ip=self._config_entry.data[CONF_SERVER_IP],
            default_broadcast=DEFAULT_DISCOVERY_TARGET,
        )
        poll_interval = self._config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        poll_mode = self._config_entry.options.get(CONF_POLL_MODE, POLL_MODE_MANUAL)
        if poll_mode not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
            poll_mode = POLL_MODE_MANUAL
        control_mode = self._config_entry.options.get(
            CONF_CONTROL_MODE,
            self._config_entry.data.get(CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE),
        )
        collector_operation_mode = self._config_entry.options.get(
            CONF_COLLECTOR_OPERATION_MODE,
            self._config_entry.data.get(
                CONF_COLLECTOR_OPERATION_MODE,
                DEFAULT_COLLECTOR_OPERATION_MODE,
            ),
        )
        if collector_operation_mode not in COLLECTOR_OPERATION_MODES:
            collector_operation_mode = DEFAULT_COLLECTOR_OPERATION_MODE

        schema_fields: dict[Any, Any] = {
            vol.Required(CONF_POLL_MODE, default=poll_mode): _poll_mode_selector(
                self._translation_bundle,
            ),
            vol.Required(CONF_CONTROL_MODE, default=control_mode): _control_mode_selector(
                self._translation_bundle,
            ),
        }
        if poll_mode == POLL_MODE_MANUAL:
            schema_fields[vol.Required(CONF_POLL_INTERVAL, default=poll_interval)] = (
                _POLL_INTERVAL_SELECTOR
            )
        if not is_bridge:
            # Only a factory collector / unanswered probe gets the SmartESS+HA vs
            # HA-only choice. A bridge has no SmartESS cloud side, so the selector
            # is hidden and an informational note is shown instead.
            schema_fields[
                vol.Required(
                    CONF_COLLECTOR_OPERATION_MODE,
                    default=collector_operation_mode,
                )
            ] = _collector_operation_mode_selector(
                self._tr(
                    "common.dynamic.collector_operation_smartess_and_ha",
                    "SmartESS cloud + Home Assistant",
                ),
                self._tr(
                    "common.dynamic.collector_operation_ha_only",
                    "Home Assistant only",
                ),
            )
        schema_fields[vol.Required("connection")] = section(
            vol.Schema(
                self._build_connection_fields_schema(
                    connection_type,
                    fields=branch.form_layout.runtime_fields,
                    values=connection_values,
                )
            ),
            {"collapsed": True},
        )
        data_schema = vol.Schema(schema_fields)

        return self.async_show_form(
            step_id="runtime",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "model_name": self._config_entry.data.get(CONF_DETECTED_MODEL, "Unknown"),
                "serial_number": self._config_entry.data.get(CONF_DETECTED_SERIAL, "Unknown"),
                "confidence": self._confidence_label(
                    self._config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none")
                ),
                "control_summary": self._control_summary(
                    control_mode=control_mode,
                    confidence=self._config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none"),
                ),
                "collector_operation_mode_note": (
                    self._tr(
                        "common.dynamic.collector_operation_mode_bridge_note",
                        "Local bridge — always Home Assistant only; it has no "
                        "SmartESS cloud side.",
                    )
                    if is_bridge
                    else ""
                ),
            },
        )

    @_with_translation_bundle
    async def async_step_diagnostics(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        placeholders = self._diagnostics_placeholders()
        primary_action = placeholders["support_workflow_primary_action"]
        menu_options = self._diagnostics_menu_options(primary_action)

        return self.async_show_menu(
            step_id="diagnostics",
            menu_options=menu_options,
            description_placeholders=placeholders,
        )

    def _build_runtime_options_from_flat_input(self, flat_input: dict[str, Any]) -> dict[str, Any]:
        connection_type = self._config_entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
        persisted_options = build_runtime_option_settings(connection_type, flat_input)
        persisted_options[CONF_POLL_INTERVAL] = flat_input.get(
            CONF_POLL_INTERVAL,
            self._config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL),
        )
        persisted_options[CONF_POLL_MODE] = flat_input.get(
            CONF_POLL_MODE,
            self._config_entry.options.get(CONF_POLL_MODE, POLL_MODE_MANUAL),
        )
        persisted_options[CONF_CONTROL_MODE] = flat_input[CONF_CONTROL_MODE]
        persisted_options[CONF_COLLECTOR_OPERATION_MODE] = flat_input[
            CONF_COLLECTOR_OPERATION_MODE
        ]
        for key in (
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
        ):
            if key in self._config_entry.options:
                persisted_options[key] = self._config_entry.options[key]
        return persisted_options

    @_with_translation_bundle
    async def async_step_runtime_poll_interval(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        pending = dict(self._runtime_poll_interval_pending_input)
        if not pending:
            return await self.async_step_runtime()
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            pending[CONF_POLL_INTERVAL] = flat_input.get(
                CONF_POLL_INTERVAL,
                self._config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL),
            )
            return self.async_create_entry(
                data=self._build_runtime_options_from_flat_input(pending)
            )
        return self.async_show_form(
            step_id="runtime_poll_interval",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_POLL_INTERVAL,
                        default=self._config_entry.options.get(
                            CONF_POLL_INTERVAL,
                            DEFAULT_POLL_INTERVAL,
                        ),
                    ): _POLL_INTERVAL_SELECTOR,
                }
            ),
            errors={},
        )

    @_with_translation_bundle
    async def async_step_diagnostic_commands(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Run a one-off multiline diagnostic scenario from the options UI."""

        errors: dict[str, str] = {}
        submitted = dict(user_input or {})
        commands = str(
            submitted.get("diagnostic_commands", self._diagnostic_commands_text) or ""
        )
        stop_on_error = bool(submitted.get("diagnostic_stop_on_error", True))
        confirm_write = bool(submitted.get("diagnostic_confirm_write", False))
        publish_download_copy = bool(
            submitted.get(
                "diagnostic_publish_download_copy",
                self._diagnostic_publish_download_copy,
            )
        )

        if user_input is not None:
            self._diagnostic_commands_text = commands
            self._diagnostic_publish_download_copy = publish_download_copy
            if not commands.strip():
                errors["diagnostic_commands"] = "diagnostic_commands_required"
            else:
                coordinator = self._coordinator()
                if coordinator is None or not callable(
                    getattr(coordinator, "async_run_diagnostic_commands", None)
                ):
                    errors["base"] = "diagnostic_commands_unavailable"
                else:
                    try:
                        result = await coordinator.async_run_diagnostic_commands(
                            commands=commands,
                            stop_on_error=stop_on_error,
                            confirm_write=confirm_write,
                            publish_download_copy=publish_download_copy,
                        )
                    except Exception:
                        logger.exception("Diagnostic command scenario failed")
                        errors["base"] = "diagnostic_commands_failed"
                    else:
                        self._diagnostic_commands_output = str(
                            result.get("output") or ""
                        )
                        self._diagnostic_commands_download_url = str(
                            result.get("download_url") or ""
                        )
                        self._diagnostic_commands_result_path = str(
                            result.get("result_path") or ""
                        )

        schema: dict[Any, Any] = {
            vol.Required(
                "diagnostic_commands",
                default=commands,
            ): _MULTILINE_TEXT_SELECTOR,
            vol.Required(
                "diagnostic_stop_on_error",
                default=stop_on_error,
            ): _BOOLEAN_SELECTOR,
            vol.Required(
                "diagnostic_confirm_write",
                default=confirm_write,
            ): _BOOLEAN_SELECTOR,
            vol.Required(
                "diagnostic_publish_download_copy",
                default=publish_download_copy,
            ): _BOOLEAN_SELECTOR,
        }
        if self._diagnostic_commands_output:
            schema[
                vol.Optional(
                    "diagnostic_result",
                    default=self._diagnostic_commands_output,
                )
            ] = _MULTILINE_LOG_TEXT_SELECTOR

        download_markdown = (
            self._tr(
                "common.dynamic.download_file",
                "[Download file]({url})",
                {"url": self._diagnostic_commands_download_url},
            )
            if self._diagnostic_commands_download_url
            else self._tr("common.dynamic.not_available", "Not available")
        )
        return self.async_show_form(
            step_id="diagnostic_commands",
            data_schema=vol.Schema(schema),
            errors=errors,
            description_placeholders={
                "diagnostic_result_path": self._diagnostic_commands_result_path
                or self._tr("common.dynamic.not_created_yet", "Not created yet"),
                "diagnostic_download_markdown": download_markdown,
            },
        )

    @_with_translation_bundle
    async def async_step_shadow_learning(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 1: intro and consent.

        Replaces the former technical action dropdown (now
        ``async_step_shadow_learning_advanced``) with one linear user-facing
        workflow: intro/consent -> credentials -> progress -> review -> result.
        No live cloud operation runs until the user gives explicit consent.
        """
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "shadow_learning_title",
                    "Expand support for this device",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        errors: dict[str, str] = {}
        consent = bool((user_input or {}).get("shadow_learning_confirm_cloud_write", False))
        if user_input is not None:
            if consent:
                # Start a fresh wizard pass: drop ALL of the previous run's
                # result state, not just credentials. Otherwise a run that fails
                # early (e.g. a preflight blocker) would still show the prior
                # run's overlay/controls/read counts as if they were its own.
                self._reset_control_discovery_run_state()
                self._shadow_learning_state["wizard_consent"] = True
                return await self.async_step_shadow_learning_credentials()
            errors["shadow_learning_confirm_cloud_write"] = "required"

        return self.async_show_form(
            step_id="shadow_learning",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        "shadow_learning_confirm_cloud_write",
                        default=consent,
                    ): _BOOLEAN_SELECTOR,
                }
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_intro_hint",
                "Home Assistant will briefly sign in to {cloud_provider_label} to "
                "find which settings it can control on this device. Your login is "
                "used only for this check and is not saved.\n\n"
                "⚠️ Before you continue, fully CLOSE the {cloud_app_label} mobile "
                "app. If it stays open it competes with this check for the device "
                "and can disrupt the scan or interfere with the inverter.\n\n"
                "Confirm below to continue.",
            ),
        )

    @_with_translation_bundle
    async def async_step_shadow_learning_credentials(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 2: cloud credentials.

        Asks only for the cloud username/password. Credentials are held in
        transient flow state for the current run only and are never written to
        the config entry or its options.
        """
        coordinator = self._coordinator()
        if coordinator is None or not bool(self._shadow_learning_state.get("wizard_consent")):
            # Credentials are unreachable without a coordinator and prior consent.
            return await self.async_step_shadow_learning()

        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        username = str(defaults.get("username") or "").strip()
        password = str(defaults.get("password") or "").strip()
        if user_input is not None:
            if not username:
                errors["username"] = "required"
            if not password:
                errors["password"] = "required"
            if not errors:
                # Transient only — used by the automatic runner (EYB-REF-041)
                # and dropped at the result step; never persisted to the entry.
                self._shadow_learning_state["wizard_credentials"] = {
                    "username": username,
                    "password": password,
                }
                return await self.async_step_shadow_learning_progress()

        return self.async_show_form(
            step_id="shadow_learning_credentials",
            data_schema=vol.Schema(
                _smartess_credential_schema_fields(
                    required=True,
                    username_default=username,
                    password_default="",
                )
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_credentials_hint",
                "Enter your {cloud_provider_label} username and password. They "
                "are used only for this one check and are not saved.",
            ),
        )

    def _set_control_discovery_progress(
        self, fraction: float, stage: str, *, done: int = 0, total: int = 0
    ) -> None:
        """Advance the guided control-discovery progress bar.

        Records the latest fraction and drives the determinate progress bar via
        ``async_update_progress`` when the running Home Assistant core supports it
        (older cores just show the spinner). The progress step label stays static
        — only the bar animates — because re-rendering the dialog to update text
        visibly flickers. ``stage``/``done``/``total`` are accepted for call-site
        readability and future use.
        """

        clamped = max(0.0, min(1.0, float(fraction)))
        self._shadow_learning_state["progress"] = {
            "fraction": clamped,
            "stage": str(stage),
            "done": int(done),
            "total": int(total),
        }
        update = getattr(self, "async_update_progress", None)
        if callable(update):
            with suppress(Exception):
                update(clamped)

    @_with_translation_bundle
    async def async_step_shadow_learning_progress(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 3: automatic progress.

        Shows ``async_show_progress`` while the automatic discovery runner works,
        rendering a determinate progress bar (on cores that support it) and a
        stage-specific label that updates as the runner advances, then moves on
        to the review screen.
        """
        coordinator = self._coordinator()
        if coordinator is None or not self._shadow_learning_state.get("wizard_credentials"):
            # Progress is unreachable until consent + credentials are gathered.
            return await self.async_step_shadow_learning()

        pipeline = self._shadow_learning_state.get("wizard_progress_task")
        if pipeline is None:
            # Fresh run: start at the review overview page once it completes.
            self._shadow_learning_state.pop("review_phase", None)
            # Prime the determinate bar at an explicit 0% before the first real
            # update. HA's progress bar mis-renders its fill on the very first
            # non-zero value (the scale only draws correctly from the second
            # update on); seeding 0% makes that sacrificial first render an empty
            # bar, so the first visible fill (the next update) renders correctly.
            self._set_control_discovery_progress(0.0, "starting")
            pipeline = self.hass.async_create_task(self._async_run_control_discovery())
            self._shadow_learning_state["wizard_progress_task"] = pipeline

        if pipeline.done():
            self._shadow_learning_state["wizard_progress_task"] = None
            return self.async_show_progress_done(
                next_step_id="shadow_learning_review",
            )

        # The dialog renders once (progress_task is the pipeline itself, so HA
        # only re-runs this step when discovery finishes). The label is therefore
        # static — the live feedback is the determinate bar, which the runner
        # advances via async_update_progress as each stage completes. Re-rendering
        # the dialog on a timer to animate the label visibly flickers, so we don't.
        fraction = max(0.0, min(1.0, float(
            dict(self._shadow_learning_state.get("progress") or {}).get("fraction") or 0.0
        )))
        update = getattr(self, "async_update_progress", None)
        if callable(update):
            with suppress(Exception):
                update(fraction)
        return self.async_show_progress(
            step_id="shadow_learning_progress",
            progress_action="shadow_learning",
            progress_task=pipeline,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_progress_status",
                "Checking which extra device features are available…",
            ),
        )

    async def _async_run_control_discovery(self) -> None:
        """Run the automatic control-discovery pipeline for the guided wizard.

        Executed as the progress-step background task. In one pass — and with no
        preview-plan, manual field-id, numeric-value, or action-sequencing step —
        it performs: preflight -> start the fail-closed shadow session -> fetch
        cloud settings -> build a bounded automatic plan -> run learning ->
        generate the device-scoped overlay draft -> stop the session and restore
        the collector endpoint -> publish support artifacts.

        Fail-closed: any failure attempts to stop the shadow session and restore
        the endpoint, records the error in flow state, and preserves whatever
        trace/support evidence already exists. This coroutine never raises, so the
        progress step always advances to the review screen.
        """

        coordinator = self._coordinator()
        if coordinator is None:
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": "coordinator_not_loaded",
            }
            return None

        credentials = dict(self._shadow_learning_state.get("wizard_credentials") or {})
        username = str(credentials.get("username") or "").strip()
        password = str(credentials.get("password") or "")
        if not username or not password:
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": "credentials_required",
            }
            return None

        try:
            await self._async_execute_control_discovery(
                coordinator,
                username=username,
                password=password,
            )
        except asyncio.CancelledError:
            # HA's flow manager cancels this progress task when the user closes
            # the options dialog mid-scan. CancelledError does NOT subclass
            # Exception, so it would otherwise skip the fail-closed cleanup and
            # leave the collector redirected to the local proxy until the
            # session lease expires. Run the cleanup (which never raises) even
            # while being cancelled, then propagate the cancellation.
            self._shadow_learning_state["discovery"] = {
                "status": "cancelled",
                "reason": "control_discovery_cancelled",
            }
            await _finish_cleanup_on_cancel(
                self._async_control_discovery_failsafe_stop(coordinator)
            )
            raise
        except Exception as exc:
            # Fail-closed cleanup: stop the shadow session and restore the
            # collector endpoint, then surface the failure in flow state.
            await self._async_control_discovery_failsafe_stop(coordinator)
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": str(exc),
            }
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.control_discovery_failed",
                "Control discovery could not finish. The temporary cloud "
                "connection was closed if it had been opened.",
            )
            # Preserve whatever trace/support evidence already exists.
            with suppress(Exception):
                self._publish_shadow_learning_artifacts(coordinator)
        return None

    async def _async_execute_control_discovery(
        self,
        coordinator,
        *,
        username: str,
        password: str,
    ) -> None:
        """Run the automatic discovery happy path; raise on any failure.

        Fail-closed cleanup after a failure is owned by the caller
        (``_async_run_control_discovery``); this method only stops the session
        itself on its own successful exit.
        """

        # The progress step already primed the bar at 0%. Give the frontend a
        # brief moment to actually paint that empty bar before the first non-zero
        # value, so the determinate scale renders correctly from the start instead
        # of mis-drawing the very first fill (an HA progress-dialog quirk).
        await asyncio.sleep(0.5)
        self._set_control_discovery_progress(0.01, "preflight")
        preflight_started = time.monotonic()
        preflight = await self._build_shadow_learning_preflight_snapshot(coordinator)
        preflight = dict(preflight)
        preflight["duration_ms"] = int(round((time.monotonic() - preflight_started) * 1000.0))
        self._shadow_learning_state["preflight"] = preflight
        self._set_control_discovery_progress(0.03, "preflight")
        if not bool(preflight.get("can_start")):
            blockers = preflight.get("blockers") or []
            if not isinstance(blockers, list):
                blockers = []
            raise RuntimeError(
                "shadow_learning_preflight_blocked:" + ",".join(str(item) for item in blockers)
                if blockers
                else "shadow_learning_preflight_blocked"
            )

        provider = self._control_discovery_cloud_provider(coordinator)
        if provider not in {"smartess", "valuecloud"}:
            raise RuntimeError(
                self._tr(
                    "common.dynamic.control_discovery_provider_not_supported",
                    "Automatic control discovery is not available for "
                    "{cloud_provider_label} yet. Local read-only support and "
                    "support packages still work.",
                    {
                        "cloud_provider_label": self._control_discovery_cloud_provider_label(
                            coordinator
                        )
                    },
                )
            )

        self._set_control_discovery_progress(0.10, "connecting")
        session = await coordinator.async_start_shadow_learning(allow_ack_writes=False)
        self._shadow_learning_state["session"] = dict(session or {})
        self._publish_shadow_learning_artifacts(coordinator)

        if not self._shadow_learning_route_accepts_control(coordinator):
            raise RuntimeError("shadow_learning_session_not_ready")

        if provider == "valuecloud":
            identity, result, read_bindings = await self._async_execute_valuecloud_control_discovery(
                coordinator,
                username=username,
                password=password,
            )
        else:
            identity, result, read_bindings = await self._async_execute_smartess_control_discovery(
                coordinator,
                username=username,
                password=password,
            )

        self._shadow_learning_state["orchestration"] = result
        plan = result.get("plan") if isinstance(result, dict) else None
        if isinstance(plan, list):
            self._shadow_learning_state["plan"] = {
                "source": f"{provider}_orchestration_plan",
                "items": plan,
                "count": len(plan),
            }
        self._shadow_learning_state["session"] = {
            **dict(self._shadow_learning_state.get("session") or {}),
            "status": "degraded"
            if (
                int(result.get("degraded_count") or 0) > 0
                or int(result.get("leaked_count") or 0) > 0
            )
            else "ready",
        }
        self._publish_shadow_learning_artifacts(coordinator)

        orchestration = dict(self._shadow_learning_state.get("orchestration") or {})
        planned_count = int(orchestration.get("planned_write_count") or 0)
        executed_count = int(orchestration.get("executed_result_count") or 0)
        leaked_count = int(orchestration.get("leaked_count") or 0)
        degraded_count = int(orchestration.get("degraded_count") or 0)
        if leaked_count > 0:
            # SAFETY: at least one control write was accepted by the cloud (ERR_NONE) and did
            # not have a matching local proxy write observation -- proof the write bypassed our
            # proxy and may have reached the REAL inverter. The run was hard-stopped at the
            # first such write, but a live change may already have been applied to the hardware.
            # Do not build or offer a partial overlay from a safety-aborted run; let the caller
            # perform the fail-closed stop/restore path and surface this as an error.
            raise RuntimeError(
                self._tr(
                    "common.dynamic.control_discovery_leaked",
                    "SAFETY STOP: the inverter dropped off the local proxy during the scan and a "
                    "control command reached the real inverter before the scan was halted. CHECK "
                    "THE INVERTER NOW (especially its output/on-off state). Run the scan only in "
                    "HA-only mode until this is resolved.",
                )
            )
        if degraded_count > 0:
            raise RuntimeError(
                self._tr(
                    "common.dynamic.control_discovery_degraded",
                    "The temporary cloud connection dropped during the scan. The scan was "
                    "stopped before adding controls. Please try again.",
                )
            )
        if planned_count > 0 and executed_count < planned_count:
            raise RuntimeError(
                self._tr(
                    "common.dynamic.control_discovery_run_incomplete",
                    "The device could not be fully checked this time. The temporary cloud "
                    "connection was closed before the scan finished. Please try again.",
                )
            )

        self._set_control_discovery_progress(0.88, "building")
        correlation = result.get("correlation")
        read_map = result.get("read_map")
        if isinstance(correlation, dict):
            await self._async_generate_control_discovery_overlay(
                coordinator,
                identity=identity,
                correlation=correlation,
                read_map=read_map if isinstance(read_map, dict) else None,
                read_bindings=read_bindings,
            )

        # Success path: stop the session and restore the endpoint, then publish
        # the final artifact bundle. (Failure cleanup is owned by the caller.)
        self._set_control_discovery_progress(0.95, "finalizing")
        await self._async_control_discovery_stop(coordinator)
        self._publish_shadow_learning_artifacts(coordinator)
        self._set_control_discovery_progress(1.0, "finalizing")
        self._shadow_learning_state["status"] = self._tr(
            "common.dynamic.control_discovery_done",
            "Control discovery finished. The temporary cloud connection is closed.",
        )
        found_controls = int(
            dict(self._shadow_learning_state.get("overlay") or {}).get(
                "generated_capability_count"
            )
            or 0
        )
        sent_count = int(orchestration.get("sent_count") or 0)
        read_map_for_result = orchestration.get("read_map")
        read_event_count = int(
            read_map_for_result.get("read_event_count") or 0
        ) if isinstance(read_map_for_result, dict) else 0
        # A run that found nothing AND transmitted no probes at all did not actually
        # observe the device -- it stalled on the connection (e.g. the collector never
        # reconnected through the temporary proxy). That is a retryable error, not a
        # genuine "this device has no controls" result, so surface it as a failure with a
        # clear retry hint instead of the misleading "nothing found this time" message.
        if found_controls == 0 and sent_count == 0 and read_event_count == 0:
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": self._tr(
                    "common.dynamic.control_discovery_run_incomplete",
                    "The device could not be probed this time (the temporary cloud "
                    "connection did not come up). Please try the scan again.",
                ),
                "found_controls": 0,
            }
        else:
            self._shadow_learning_state["discovery"] = {
                "status": "ok",
                "found_controls": found_controls,
            }
        return None

    async def _async_execute_smartess_control_discovery(
        self,
        coordinator,
        *,
        username: str,
        password: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        """Execute SmartESS-specific cloud control discovery."""

        self._set_control_discovery_progress(0.18, "fetching")
        cloud_bundle = await self.hass.async_add_executor_job(
            lambda: fetch_device_bundle_for_collector(
                username=username,
                password=password,
                collector_pn=str(coordinator.smartess_collector_pn or ""),
            )
        )
        identity = (
            self._shadow_learning_cloud_identity_from_bundle(cloud_bundle)
            or self._shadow_learning_cloud_identity(coordinator)
        )
        if identity is None:
            raise RuntimeError("shadow_learning_identity_unavailable")
        self._shadow_learning_state["identity"] = identity
        self._publish_shadow_learning_artifacts(coordinator)

        _login_envelope, cloud_session = await self.hass.async_add_executor_job(
            lambda: login_with_password(username=username, password=password)
        )
        settings_dat = self._shadow_learning_settings_dat_from_bundle(cloud_bundle)
        if settings_dat is None:
            settings_envelope = await self.hass.async_add_executor_job(
                lambda: fetch_signed_action(
                    action=build_device_settings_action(
                        pn=identity["pn"],
                        sn=identity["sn"],
                        devcode=identity["devcode"],
                        devaddr=identity["devaddr"],
                    ),
                    session=SessionCredentials(
                        token=cloud_session.token,
                        secret=cloud_session.secret,
                        uid=cloud_session.uid,
                        usr=cloud_session.usr,
                        role=cloud_session.role,
                        expire=cloud_session.expire,
                    ),
                ),
            )
            settings_dat = settings_envelope.dat

        observation_source = self._shadow_learning_observation_source(coordinator)
        self._shadow_learning_state["session"] = {
            **dict(self._shadow_learning_state.get("session") or {}),
            "status": "learning",
        }
        self._set_control_discovery_progress(0.30, "testing")
        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=cloud_session,
            pn=identity["pn"],
            sn=identity["sn"],
            devcode=identity["devcode"],
            devaddr=identity["devaddr"],
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=True,
            all_choice_values=True,
            max_fields=CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS,
            continue_on_error=True,
            delay_seconds=0.0,
            **self._shadow_learning_orchestrator_callbacks(coordinator, observation_source),
        )

        read_map = result.get("read_map")
        read_bindings: dict[str, Any] | None = None
        if isinstance(read_map, dict) and read_map.get("registers"):
            read_bindings = await self._async_bind_session_read_labels(
                cloud_session=cloud_session,
                identity=identity,
                read_map=read_map,
            )
            if read_bindings:
                self._shadow_learning_state["read_bindings"] = read_bindings
        return identity, result, read_bindings

    async def _async_execute_valuecloud_control_discovery(
        self,
        coordinator,
        *,
        username: str,
        password: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        """Execute ValueCloud-specific cloud control discovery."""

        self._set_control_discovery_progress(0.18, "fetching")
        _login_envelope, cloud_session = await self.hass.async_add_executor_job(
            lambda: valuecloud_cloud_module.login_with_password(
                username=username,
                password=password,
            )
        )
        cloud_bundle = await self.hass.async_add_executor_job(
            lambda: valuecloud_cloud_module.fetch_device_bundle_for_collector_with_session(
                session=cloud_session,
                collector_pn=str(coordinator.smartess_collector_pn or ""),
            )
        )
        identity = (
            self._shadow_learning_cloud_identity_from_bundle(cloud_bundle)
            or self._shadow_learning_cloud_identity(coordinator)
        )
        if identity is None:
            raise RuntimeError("shadow_learning_identity_unavailable")
        self._shadow_learning_state["identity"] = identity
        self._publish_shadow_learning_artifacts(coordinator)

        normalized = cloud_bundle.get("normalized") if isinstance(cloud_bundle, dict) else None
        batch_control = (
            normalized.get("batch_control")
            if isinstance(normalized, dict) and isinstance(normalized.get("batch_control"), dict)
            else None
        )
        control_strategy = (
            normalized.get("control_strategy")
            if isinstance(normalized, dict) and isinstance(normalized.get("control_strategy"), dict)
            else None
        )
        device_ctrl = (
            normalized.get("device_ctrl")
            if isinstance(normalized, dict) and isinstance(normalized.get("device_ctrl"), dict)
            else None
        )
        if not isinstance(batch_control, dict) and not isinstance(control_strategy, dict) and not isinstance(device_ctrl, dict):
            raise RuntimeError("valuecloud_batch_control_unavailable")

        observation_source = self._shadow_learning_observation_source(coordinator)
        self._shadow_learning_state["session"] = {
            **dict(self._shadow_learning_state.get("session") or {}),
            "status": "learning",
        }
        self._set_control_discovery_progress(0.30, "testing")
        result = await async_orchestrate_valuecloud_shadow_learning(
            batch_control=batch_control,
            control_strategy=control_strategy,
            device_ctrl=device_ctrl,
            session=cloud_session,
            pn=identity["pn"],
            sn=identity["sn"],
            devcode=identity["devcode"],
            devaddr=identity["devaddr"],
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=True,
            all_choice_values=True,
            max_fields=CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS,
            continue_on_error=True,
            delay_seconds=0.0,
            **self._shadow_learning_orchestrator_callbacks(coordinator, observation_source),
        )
        return identity, result, None

    def _shadow_learning_orchestrator_callbacks(self, coordinator, observation_source) -> dict[str, Any]:
        """Return shared shadow-observation callbacks for provider runners."""

        def _on_test_progress(done: int, total: int) -> None:
            fraction = 0.30 + 0.55 * (done / total) if total > 0 else 0.30
            self._set_control_discovery_progress(
                fraction,
                "testing",
                done=min(done + 1, total) if total else 0,
                total=total,
            )

        return {
            "observation_cursor": (
                getattr(observation_source, "observation_cursor", None)
                if observation_source is not None
                else None
            ),
            "current_observations_since": (
                getattr(observation_source, "observations_since", None)
                if observation_source is not None
                else None
            ),
            "wait_for_observations_since": (
                (lambda cursor, timeout_seconds: observation_source.wait_for_observations_since(
                    cursor,
                    timeout_seconds=timeout_seconds,
                ))
                if observation_source is not None
                and callable(getattr(observation_source, "wait_for_observations_since", None))
                else None
            ),
            "is_session_ready": lambda: self._shadow_learning_route_accepts_control(coordinator),
            "read_map_snapshot": (
                (lambda: observation_source.read_map)
                if observation_source is not None
                and hasattr(observation_source, "read_map")
                else None
            ),
            "on_progress": _on_test_progress,
        }

    async def _async_bind_session_read_labels(
        self,
        *,
        cloud_session: SessionCredentials,
        identity: dict[str, Any],
        read_map: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Correlate the cloud's labeled sensors against the session read map."""

        registers = read_map.get("registers")
        if not isinstance(registers, dict) or not registers:
            return None
        # The whole correlation is best-effort: the caller treats read-label
        # binding as supplemental, so NOTHING here (cloud fetch, parsing, or the
        # value-correlation binder) may fail the discovery run after the probe
        # sweep already succeeded.
        try:
            envelope = await self.hass.async_add_executor_job(
                lambda: fetch_signed_action(
                    action=build_device_detail_action(
                        pn=str(identity.get("pn") or ""),
                        sn=str(identity.get("sn") or ""),
                        devcode=int(identity.get("devcode") or 0),
                        devaddr=int(identity.get("devaddr") or 1),
                    ),
                    session=cloud_session,
                )
            )
            dat = envelope.dat if isinstance(envelope.dat, dict) else {}
            pars = dat.get("pars") if isinstance(dat.get("pars"), dict) else {}
            sensors: list[dict[str, Any]] = []
            for items in pars.values():
                if isinstance(items, list):
                    sensors.extend(item for item in items if isinstance(item, dict))
            if not sensors:
                return None
            report = bind_cloud_labels_to_registers(sensors=sensors, registers=registers)
            return report.to_json_dict()
        except Exception as exc:
            logger.debug("Read-label binding failed during learning session: %s", exc)
            return None

    async def _async_generate_control_discovery_overlay(
        self,
        coordinator,
        *,
        identity: dict[str, Any],
        correlation: dict[str, Any],
        read_map: dict[str, Any] | None = None,
        read_bindings: dict[str, Any] | None = None,
    ) -> None:
        """Generate the inactive device-scoped overlay draft from correlation evidence."""

        session = dict(self._shadow_learning_state.get("session") or {})
        session_manifest = {
            "session_id": str(
                session.get("session_id")
                or session.get("trace_path")
                or datetime.now().strftime("%Y%m%dT%H%M%S")
            ),
            "collector_pn": coordinator.smartess_collector_pn,
            "cloud_pn": str(identity.get("pn") or ""),
            "cloud_sn": str(identity.get("sn") or ""),
            "devcode": identity.get("devcode"),
            "devaddr": identity.get("devaddr"),
        }
        result = await self.hass.async_add_executor_job(
            lambda: generate_shadow_learning_overlay_drafts(
                config_dir=Path(self.hass.config.config_dir),
                source_profile_name=str(coordinator.effective_profile_name or ""),
                source_schema_name=str(coordinator.effective_register_schema_name or ""),
                session_manifest=session_manifest,
                correlation=correlation,
                read_map=read_map,
                read_bindings=read_bindings,
                overwrite=False,
            )
        )
        self._shadow_learning_state["overlay"] = {
            "profile_path": str(result.profile_path),
            "schema_path": str(result.schema_path),
            "generated_capability_count": int(result.generated_capability_count),
            "generated_read_count": int(result.generated_read_count),
            "skipped_duplicate_count": int(result.skipped_duplicate_count),
            "manifest": dict(result.manifest),
            "profile_name": str(result.manifest.get("output", {}).get("profile_name") or ""),
            "schema_name": str(result.manifest.get("output", {}).get("schema_name") or ""),
        }
        self._publish_shadow_learning_artifacts(coordinator)
        return None

    async def _async_control_discovery_stop(self, coordinator) -> dict[str, Any]:
        """Stop the shadow session and restore the endpoint on the success path."""

        stop = getattr(coordinator, "async_stop_shadow_learning", None)
        if not callable(stop):
            return {}
        result = await stop(reason="control_discovery_done")
        merged = {
            **dict(self._shadow_learning_state.get("session") or {}),
            **(dict(result) if isinstance(result, dict) else {}),
            "status": "stopped",
        }
        self._shadow_learning_state["session"] = merged
        return dict(result) if isinstance(result, dict) else {}

    async def _async_control_discovery_failsafe_stop(self, coordinator) -> None:
        """Best-effort fail-closed stop + endpoint restore after a discovery failure.

        Tolerant of an already-stopped or never-started session: it never raises,
        so it is safe to call regardless of how far the pipeline progressed.
        """

        stop = getattr(coordinator, "async_stop_shadow_learning", None)
        if not callable(stop):
            return None
        try:
            result = await stop(
                reason="control_discovery_failed",
                raise_when_not_running=False,
            )
        except Exception as exc:
            logger.warning(
                "Control discovery fail-closed stop failed for entry %s: %s",
                getattr(self._config_entry, "entry_id", ""),
                exc,
            )
            return None
        if isinstance(result, dict):
            self._shadow_learning_state["session"] = {
                **dict(self._shadow_learning_state.get("session") or {}),
                **result,
            }
        return None

    @_with_translation_bundle
    async def async_step_shadow_learning_review(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 4: review what was found.

        Shown as two pages on the same step. Page one is a read-only overview of
        everything discovered for this device: the new controls that can be added
        and the controls already supported by Home Assistant (so the user sees the
        full picture, including ones left off). Page two lets the user rename and
        enable the new controls. Normal-risk controls default to enabled; risky or
        uncertain ones default to disabled.

        Choices are stored in transient flow state (``review_selections``); the
        discovered evidence (``review_model.learned_all``) is never mutated, so
        disabled controls are preserved for the support package and an edited
        label never overwrites the developer-facing field name.
        """
        coordinator = self._coordinator()
        if coordinator is None:
            return await self.async_step_shadow_learning()

        new_controls = self._control_discovery_review_controls()
        already_controls = self._control_discovery_already_supported_controls()
        new_reads = self._control_discovery_review_read_sensors()
        already_reads = self._control_discovery_already_supported_read_sensors()

        # Nothing discovered at all (or discovery failed earlier): skip the
        # redundant "nothing found" page entirely and go straight to the detailed
        # result screen, which explains empty vs failed and offers retry / support
        # / return. Showing an intermediate empty page first was pure friction.
        if not new_controls and not already_controls and not new_reads and not already_reads:
            return await self.async_step_shadow_learning_result()

        # Page one: read-only overview of everything found.
        if str(self._shadow_learning_state.get("review_phase") or "overview") != "edit":
            if user_input is not None:
                if new_controls or new_reads:
                    self._shadow_learning_state["review_phase"] = "edit"
                    return await self.async_step_shadow_learning_review()
                return await self.async_step_shadow_learning_result()
            new_count = len(new_controls)
            existing_count = len(already_controls)
            new_read_count = len(new_reads)
            existing_read_count = len(already_reads)
            on_count = sum(1 for control in new_controls if bool(control.get("enabled_by_default")))
            read_on_count = sum(1 for sensor in new_reads if bool(sensor.get("enabled_by_default")))
            overview_placeholders = {
                "control_discovery_count": str(
                    new_count + existing_count + new_read_count + existing_read_count
                ),
                "control_discovery_new_count": str(new_count),
                "control_discovery_existing_count": str(existing_count),
                "control_discovery_new_read_count": str(new_read_count),
                "control_discovery_existing_read_count": str(existing_read_count),
                "control_discovery_on_count": str(on_count),
                "control_discovery_off_count": str(new_count - on_count),
                "control_discovery_read_on_count": str(read_on_count),
                "control_discovery_read_off_count": str(new_read_count - read_on_count),
                "control_discovery_overview": self._control_discovery_overview_markdown(
                    new_controls, already_controls, new_reads, already_reads
                ),
            }
            return self.async_show_form(
                step_id="shadow_learning_review",
                data_schema=vol.Schema({}),
                errors={},
                description_placeholders=self._control_discovery_placeholders(
                    coordinator,
                    "common.dynamic.control_discovery_overview_intro",
                    "Found {control_discovery_count} extra control(s) for this device — "
                    "{control_discovery_new_count} new, "
                    "{control_discovery_existing_count} already in Home Assistant. "
                    "Continue to choose which new items to add.\n\n"
                    "{control_discovery_overview}",
                    hint_placeholders=overview_placeholders,
                    extra=overview_placeholders,
                ),
            )

        # Page two: pick which new controls to add. Each option is labelled with
        # the control's friendly name (the entity is named that automatically —
        # there is no rename field), and the descriptions live on the overview.
        prior = self._control_discovery_prior_selections()
        prior_reads = self._control_discovery_prior_read_selections()
        if user_input is not None:
            self._store_control_discovery_selections(new_controls, new_reads, user_input)
            self._shadow_learning_state.pop("review_phase", None)
            return await self.async_step_shadow_learning_result()

        control_options = [
            SelectOptionDict(
                value=str(control.get("key") or ""),
                label=self._control_discovery_control_label(control),
            )
            for control in new_controls
            if str(control.get("key") or "")
        ]
        read_options = [
            SelectOptionDict(
                value=str(sensor.get("key") or ""),
                label=self._control_discovery_read_sensor_label(sensor),
            )
            for sensor in new_reads
            if str(sensor.get("key") or "")
        ]
        default_enabled = self._control_discovery_default_enabled_keys(new_controls, prior)
        default_enabled_reads = self._control_discovery_default_enabled_read_keys(
            new_reads, prior_reads
        )
        review_placeholders = {
            "control_discovery_count": str(len(new_controls)),
            "control_discovery_on_count": str(len(default_enabled)),
            "control_discovery_off_count": str(len(new_controls) - len(default_enabled)),
            "control_discovery_read_count": str(len(new_reads)),
            "control_discovery_read_on_count": str(len(default_enabled_reads)),
            "control_discovery_read_off_count": str(len(new_reads) - len(default_enabled_reads)),
        }
        schema_fields: dict[Any, Any] = {}
        if control_options:
            schema_fields[
                vol.Optional("enabled_controls", default=default_enabled)
            ] = SelectSelector(
                SelectSelectorConfig(
                    options=control_options,
                    multiple=True,
                    mode=SelectSelectorMode.LIST,
                )
            )
        if read_options:
            schema_fields[
                vol.Optional("enabled_read_sensors", default=default_enabled_reads)
            ] = SelectSelector(
                SelectSelectorConfig(
                    options=read_options,
                    multiple=True,
                    mode=SelectSelectorMode.LIST,
                )
            )
        return self.async_show_form(
            step_id="shadow_learning_review",
            data_schema=vol.Schema(schema_fields),
            errors={},
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_review_intro",
                "Choose the extra controls and read sensors you want to add to Home "
                "Assistant. Risky controls are left off — enable them only if "
                "you know what they do.",
                hint_placeholders=review_placeholders,
                extra=review_placeholders,
            ),
        )

    def _control_discovery_review_controls(self) -> list[dict[str, Any]]:
        """Return the discovered controls (``review_model.learned_all``) to review.

        Reads the deterministic review model embedded in the generated overlay
        manifest (EYB-REF-042). Returns copies so callers cannot mutate the stored
        evidence, and an empty list when no overlay/review model exists (e.g. a
        failed or empty discovery run).
        """

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        learned_all = review_model.get("learned_all")
        if not isinstance(learned_all, list):
            return []
        return [dict(entry) for entry in learned_all if isinstance(entry, dict)]

    def _control_discovery_review_read_sensors(self) -> list[dict[str, Any]]:
        """Return generated learned read sensors available for review."""

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        learned_all = review_model.get("learned_read_all")
        if not isinstance(learned_all, list):
            return []
        return [dict(entry) for entry in learned_all if isinstance(entry, dict)]

    # Per-run keys in _shadow_learning_state, cleared between wizard passes.
    # The wizard_* keys (consent, credentials, progress_task) are lifecycle
    # state managed separately and are intentionally NOT in this set.
    _CONTROL_DISCOVERY_RUN_STATE_KEYS = (
        "activation",
        "discovery",
        "identity",
        "orchestration",
        "overlay",
        "preflight",
        "progress",
        "read_bindings",
        "review_phase",
        "review_selections",
        "session",
        "status",
        "support_package_path",
    )

    def _reset_control_discovery_run_state(self) -> None:
        """Drop every result key from the previous discovery run plus credentials."""

        for key in self._CONTROL_DISCOVERY_RUN_STATE_KEYS:
            self._shadow_learning_state.pop(key, None)
        self._shadow_learning_state.pop("wizard_credentials", None)
        self._shadow_learning_state.pop("wizard_progress_task", None)

    def _control_discovery_failed(self) -> bool:
        """Return whether the discovery run ended in an error (vs a genuine empty result)."""

        discovery = self._shadow_learning_state.get("discovery")
        return isinstance(discovery, dict) and str(discovery.get("status") or "") == "error"

    def _control_discovery_error_detail(self) -> str:
        """Return the best-available failure detail (discovery reason or last status).

        Surfaced to the user on the result screen so a failed run / failed support
        export is self-diagnosable instead of showing only a generic error.
        """

        discovery = self._shadow_learning_state.get("discovery")
        reason = str(discovery.get("reason") or "") if isinstance(discovery, dict) else ""
        if not reason:
            reason = str(self._shadow_learning_state.get("status") or "")
        return reason.strip()

    def _control_discovery_already_supported_controls(self) -> list[dict[str, Any]]:
        """Return controls discovered but already supported by the base schema.

        These come from the overlay manifest's ``skipped_duplicates`` (registers
        already mapped by Home Assistant). They are shown read-only on the review
        overview so the user sees the full picture of what was found, not only the
        new additions.
        """

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        skipped = manifest.get("skipped_duplicates")
        if not isinstance(skipped, list):
            return []
        return [dict(entry) for entry in skipped if isinstance(entry, dict)]

    def _control_discovery_already_supported_read_sensors(self) -> list[dict[str, Any]]:
        """Return learned read candidates skipped because they are already covered."""

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        skipped = review_model.get("read_excluded_by_policy")
        if not isinstance(skipped, list):
            return []
        return [dict(entry) for entry in skipped if isinstance(entry, dict)]

    def _control_discovery_overview_markdown(
        self,
        new_controls: list[dict[str, Any]],
        already_controls: list[dict[str, Any]],
        new_reads: list[dict[str, Any]] | None = None,
        already_reads: list[dict[str, Any]] | None = None,
    ) -> str:
        """Render a readable, non-technical overview of everything discovered.

        A markdown bullet list (which renders reliably in the flow form) grouped
        into new controls — each with its friendly type and suggested state — and
        controls already in Home Assistant.
        """

        def clean(value: Any) -> str:
            return str(value or "").replace("\n", " ").strip()

        lines: list[str] = []
        new_reads = list(new_reads or [])
        already_reads = list(already_reads or [])
        if new_controls:
            heading = self._tr(
                "common.dynamic.control_discovery_overview_new_heading",
                "New controls found ({count})",
                {"count": str(len(new_controls))},
            )
            lines.append(f"**{heading}**")
            for control in new_controls:
                name = clean(self._control_discovery_control_label(control))
                type_label = clean(
                    self._control_discovery_type_label(str(control.get("value_kind") or ""))
                )
                status = clean(self._control_discovery_status_note(control))
                lines.append(f"- {name} — {type_label} · {status}")
        if already_controls:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_existing_heading",
                "Already in Home Assistant ({count})",
                {"count": str(len(already_controls))},
            )
            lines.append(f"**{heading}**")
            for control in already_controls:
                name = clean(control.get("field_name") or control.get("field_id"))
                lines.append(f"- {name}")
        if new_reads:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_new_reads_heading",
                "New read sensors found ({count})",
                {"count": str(len(new_reads))},
            )
            lines.append(f"**{heading}**")
            for sensor in new_reads:
                name = clean(self._control_discovery_read_sensor_label(sensor))
                lines.append(f"- {name} — Sensor · Suggested on")
        if already_reads:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_existing_reads_heading",
                "Read sensors already in Home Assistant ({count})",
                {"count": str(len(already_reads))},
            )
            lines.append(f"**{heading}**")
            for sensor in already_reads:
                name = clean(
                    sensor.get("field_name")
                    or sensor.get("default_label")
                    or sensor.get("title")
                )
                reason = clean(sensor.get("reason") or "")
                suffix = f" · {reason}" if reason else ""
                lines.append(f"- {name}{suffix}")
        return "\n".join(lines)

    def _control_discovery_prior_selections(self) -> dict[str, dict[str, Any]]:
        """Return any previously stored per-control selections, keyed by control key."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        controls = selections.get("controls")
        return controls if isinstance(controls, dict) else {}

    def _control_discovery_prior_read_selections(self) -> dict[str, dict[str, Any]]:
        """Return previously stored per-read-sensor selections."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        read_sensors = selections.get("read_sensors")
        return read_sensors if isinstance(read_sensors, dict) else {}

    def _control_discovery_default_enabled_keys(
        self,
        controls: list[dict[str, Any]],
        prior: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Return the control keys that should be pre-selected on the edit page.

        Honours a prior selection when the user revisits the screen, otherwise
        falls back to the review model's ``enabled_by_default`` decision
        (normal-risk on, high-risk/uncertain off).
        """

        enabled: list[str] = []
        for control in controls:
            key = str(control.get("key") or "")
            if not key:
                continue
            saved = prior.get(key)
            saved = saved if isinstance(saved, dict) else {}
            if "enabled" in saved:
                is_on = bool(saved.get("enabled"))
            else:
                is_on = bool(control.get("enabled_by_default"))
            if is_on:
                enabled.append(key)
        return enabled

    def _control_discovery_default_enabled_read_keys(
        self,
        read_sensors: list[dict[str, Any]],
        prior: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Return read sensor keys pre-selected on the edit page."""

        enabled: list[str] = []
        for sensor in read_sensors:
            key = str(sensor.get("key") or "")
            if not key:
                continue
            saved = prior.get(key)
            saved = saved if isinstance(saved, dict) else {}
            if "enabled" in saved:
                is_on = bool(saved.get("enabled"))
            else:
                is_on = bool(sensor.get("enabled_by_default"))
            if is_on:
                enabled.append(key)
        return enabled

    def _store_control_discovery_selections(
        self,
        controls: list[dict[str, Any]],
        read_sensors: list[dict[str, Any]],
        user_input: dict[str, Any],
    ) -> None:
        """Persist the user's per-control name + enable choices into flow state.

        Stores choices keyed by the discovered control key under
        ``review_selections`` for the activation / support-package steps. This is
        additive flow state: it never edits the overlay manifest, so the discovered
        evidence (including disabled controls and the original field names) stays
        intact.
        """

        selected_raw = user_input.get("enabled_controls")
        selected = (
            {str(key) for key in selected_raw}
            if isinstance(selected_raw, (list, tuple, set))
            else set()
        )
        selected_reads_raw = user_input.get("enabled_read_sensors")
        selected_reads = (
            {str(key) for key in selected_reads_raw}
            if isinstance(selected_reads_raw, (list, tuple, set))
            else set()
        )
        stored: dict[str, dict[str, Any]] = {}
        enabled_by_user: list[str] = []
        excluded_by_user: list[str] = []
        for control in controls:
            key = str(control.get("key") or "")
            if not key:
                continue
            # The friendly discovered name is used as-is (no rename field); it
            # becomes the entity's label when activated.
            label = self._control_discovery_control_label(control)
            enabled = key in selected
            stored[key] = {
                "key": key,
                "register": _coerce_int(control.get("register")) or 0,
                "field_id": str(control.get("field_id") or ""),
                "value_kind": str(control.get("value_kind") or ""),
                "risk_level": str(control.get("risk_level") or ""),
                "label": label,
                "default_label": label,
                "enabled": enabled,
                "enabled_by_default": bool(control.get("enabled_by_default")),
            }
            if enabled:
                enabled_by_user.append(key)
            else:
                excluded_by_user.append(key)
        stored_reads: dict[str, dict[str, Any]] = {}
        read_enabled_by_user: list[str] = []
        read_excluded_by_user: list[str] = []
        for sensor in read_sensors:
            key = str(sensor.get("key") or "")
            if not key:
                continue
            label = self._control_discovery_read_sensor_label(sensor)
            enabled = key in selected_reads
            stored_reads[key] = {
                "key": key,
                "register": _coerce_int(sensor.get("register")) or 0,
                "kind": str(sensor.get("kind") or ""),
                "spec_set": str(sensor.get("spec_set") or ""),
                "label": label,
                "default_label": label,
                "enabled": enabled,
                "enabled_by_default": bool(sensor.get("enabled_by_default")),
            }
            if enabled:
                read_enabled_by_user.append(key)
            else:
                read_excluded_by_user.append(key)
        self._shadow_learning_state["review_selections"] = {
            "controls": stored,
            "read_sensors": stored_reads,
            "enabled_by_user": enabled_by_user,
            "excluded_by_user": excluded_by_user,
            "read_enabled_by_user": read_enabled_by_user,
            "read_excluded_by_user": read_excluded_by_user,
        }

    def _control_discovery_control_label(self, control: dict[str, Any]) -> str:
        """Return the discovered default label for one control."""

        default_label = str(control.get("default_label") or "").strip()
        if default_label:
            return default_label
        return default_learned_control_label(
            field_name=str(control.get("field_name") or ""),
            field_id=str(control.get("field_id") or ""),
            register=_coerce_int(control.get("register")),
        )

    def _control_discovery_read_sensor_label(self, sensor: dict[str, Any]) -> str:
        """Return the discovered default label for one learned read sensor."""

        default_label = str(sensor.get("default_label") or "").strip()
        if default_label:
            return default_label
        field_name = str(sensor.get("field_name") or "").strip()
        if field_name:
            return field_name
        register = _coerce_int(sensor.get("register"))
        if register is not None and register > 0:
            return f"Discovered sensor {register}"
        return "Discovered sensor"


    def _control_discovery_type_label(self, value_kind: str) -> str:
        """Map an internal value kind to a friendly, non-technical control type."""

        kind = str(value_kind or "").strip().lower()
        if kind == "bool":
            return self._tr("common.dynamic.control_discovery_type_switch", "Switch")
        if kind == "enum":
            return self._tr("common.dynamic.control_discovery_type_select", "Option")
        if kind == "action":
            return self._tr("common.dynamic.control_discovery_type_button", "Button")
        if kind in {"u16", "u32_high_first", "u32_low_first"}:
            return self._tr("common.dynamic.control_discovery_type_number", "Number")
        return self._tr("common.dynamic.control_discovery_type_other", "Setting")

    def _control_discovery_status_note(self, control: dict[str, Any]) -> str:
        """Return a short, non-technical suggested-state note for one control."""

        risk = str(control.get("risk_level") or "").strip().lower()
        if risk == "high":
            return self._tr(
                "common.dynamic.control_discovery_status_high",
                "Risky — off by default",
            )
        if risk == "uncertain" or not bool(control.get("enabled_by_default")):
            return self._tr(
                "common.dynamic.control_discovery_status_uncertain",
                "Needs a check — off by default",
            )
        return self._tr(
            "common.dynamic.control_discovery_status_normal",
            "Suggested on",
        )

    @_with_translation_bundle
    async def async_step_shadow_learning_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 5: final result.

        The discovery session is already stopped by this point and the transient
        credentials gathered for the run are dropped here, so nothing sensitive
        is retained after the wizard ends.

        The action selector adapts to the run's outcome:

        - **Apply the selected parameters** — only when the user actually turned
          on at least one discovered control. It activates the device-scoped
          learned overlay with exactly that selection (and labels) via
          ``build_activation_selection`` + ``async_activate_device_scoped_overlay``.
          On success it CONFIRMS on the same screen (does not bounce to the menu);
        - **Try the scan again** — shown in place of Apply when the run failed;
          restarts the guided wizard;
        - **Download the support package** — always offered; confirms in place;
        - **Return to the menu** — leaves the wizard.

        Activation is never automatic. After any in-place action the default
        selection becomes "Return to the menu" so the next submit leaves cleanly.
        """
        coordinator = self._coordinator()
        # Drop transient credentials once the wizard reaches its end.
        self._shadow_learning_state.pop("wizard_credentials", None)
        if coordinator is None:
            return await self.async_step_shadow_learning()

        controls = self._control_discovery_review_controls()
        # Failure is decided ONLY by the run's status (discovery.status=="error").
        # error_detail falls back to the last status line, which on success holds
        # the success message — OR-ing it in here turned a successful-but-empty
        # run into a failure screen that printed the success text as the error.
        failed = self._control_discovery_failed()
        error_detail = self._control_discovery_error_detail() if failed else ""
        selected_count = self._control_discovery_enabled_selection_count()
        read_count = self._control_discovery_enabled_read_selection_count()
        # Learned read sensors are applied with the schema overlay regardless of
        # control selection, so selected read sensors make activation worthwhile
        # on their own.
        can_activate = (bool(controls) and selected_count > 0) or read_count > 0

        errors: dict[str, str] = {}
        notice = ""
        if user_input is not None:
            action = str(
                user_input.get("result_action") or CONTROL_DISCOVERY_RESULT_ACTION_DONE
            )
            if action == CONTROL_DISCOVERY_RESULT_ACTION_RETRY:
                # Re-run the guided wizard from the consent step (it resets the
                # run's transient state); credentials are re-gathered there.
                for key in (
                    "discovery",
                    "progress",
                    "review_phase",
                    "review_selections",
                    "wizard_progress_task",
                ):
                    self._shadow_learning_state.pop(key, None)
                return await self.async_step_shadow_learning()
            if action == CONTROL_DISCOVERY_RESULT_ACTION_ACTIVATE and can_activate:
                error = await self._async_control_discovery_activate_selection(
                    coordinator
                )
                if error is None:
                    # Confirm and STAY on this screen -- applying must not bounce
                    # the user straight back to the menu.
                    if read_count > 0 and selected_count > 0:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_applied_both",
                            "✓ The selected controls and {read_count} read "
                            "sensor(s) were added to Home Assistant.",
                            {"read_count": str(read_count)},
                        )
                    elif read_count > 0:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_applied_reads",
                            "✓ {read_count} read sensor(s) were added to Home Assistant.",
                            {"read_count": str(read_count)},
                        )
                    else:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_applied",
                            "✓ The selected control(s) were added to Home Assistant.",
                        )
                else:
                    errors["base"] = error
            elif action == CONTROL_DISCOVERY_RESULT_ACTION_SUPPORT:
                error = await self._async_control_discovery_export_support(
                    coordinator
                )
                if error is None:
                    notice = self._tr(
                        "common.dynamic.control_discovery_result_notice_support",
                        "✓ Support package saved.",
                    )
                else:
                    errors["base"] = error
            else:  # CONTROL_DISCOVERY_RESULT_ACTION_DONE
                return await self.async_step_init()

        # "Apply the selected parameters" shows only when the user turned on at
        # least one discovered control; on a failed run it is replaced by "Try the
        # scan again". Support + Return are always offered.
        action_options: list[SelectOptionDict] = []
        if can_activate:
            action_options.append(
                SelectOptionDict(
                    value=CONTROL_DISCOVERY_RESULT_ACTION_ACTIVATE,
                    label=self._tr(
                        "common.dynamic.control_discovery_result_action_activate",
                        "Apply the selected parameters",
                    ),
                )
            )
        elif failed:
            action_options.append(
                SelectOptionDict(
                    value=CONTROL_DISCOVERY_RESULT_ACTION_RETRY,
                    label=self._tr(
                        "common.dynamic.control_discovery_result_action_retry",
                        "Try the scan again",
                    ),
                )
            )
        action_options.append(
            SelectOptionDict(
                value=CONTROL_DISCOVERY_RESULT_ACTION_SUPPORT,
                label=self._tr(
                    "common.dynamic.control_discovery_result_action_support",
                    "Download the support package",
                ),
            )
        )
        action_options.append(
            SelectOptionDict(
                value=CONTROL_DISCOVERY_RESULT_ACTION_DONE,
                label=self._tr(
                    "common.dynamic.control_discovery_result_action_done",
                    "Return to the menu",
                ),
            )
        )
        # After a successful action default to leaving; otherwise to the primary
        # (apply / retry / support) option.
        default_action = (
            CONTROL_DISCOVERY_RESULT_ACTION_DONE if notice else action_options[0]["value"]
        )

        if controls:
            if can_activate:
                body_key = "common.dynamic.control_discovery_result_intro"
                body_default = (
                    "Discovery finished and the temporary cloud connection is "
                    "closed. {control_discovery_selected_count} control(s) are "
                    "turned on. Apply them, download the support package, or return "
                    "to the menu."
                )
            else:
                body_key = "common.dynamic.control_discovery_result_intro_none_selected"
                body_default = (
                    "Discovery finished and the temporary cloud connection is "
                    "closed. You did not turn on any of the discovered controls, so "
                    "there is nothing to apply. Download the support package, or "
                    "return to the menu."
                )
            hint_placeholders = {"control_discovery_selected_count": str(selected_count)}
        elif read_count > 0:
            # No controls to review, but read sensors were learned: activation is
            # still worthwhile, so offer Apply instead of a dead-end.
            body_key = "common.dynamic.control_discovery_result_reads_only"
            body_default = (
                "Discovery finished and the temporary cloud connection is "
                "closed. {control_discovery_read_count} read sensor(s) were "
                "discovered. Apply them, download the support package, or return "
                "to the menu."
            )
            hint_placeholders = {"control_discovery_read_count": str(read_count)}
        elif failed:
            body_key = "common.dynamic.control_discovery_result_failed"
            body_default = (
                "The check couldn't finish ({control_discovery_error}). Try the "
                "scan again, download the support package so the developer can see "
                "what happened, or return to the menu."
            )
            hint_placeholders = {"control_discovery_error": error_detail or "unknown error"}
        else:
            body_key = "common.dynamic.control_discovery_result_empty_with_support"
            body_default = (
                "The check has finished and the temporary cloud connection is "
                "closed. No controls were found to add this time. Download the "
                "support package so the developer can inspect what happened, or "
                "return to the menu."
            )
            hint_placeholders = {}

        placeholders = self._control_discovery_placeholders(
            coordinator,
            body_key,
            body_default,
            hint_placeholders=hint_placeholders,
            extra=hint_placeholders,
        )
        if controls and read_count > 0:
            # Controls path already has its intro; note the learned reads too.
            read_line = self._tr(
                "common.dynamic.control_discovery_result_reads_note",
                "Plus {control_discovery_read_count} read sensor(s) were "
                "discovered and will be added on Apply.",
                {"control_discovery_read_count": str(read_count)},
            )
            placeholders["control_discovery_hint"] = (
                f"{placeholders.get('control_discovery_hint', '')}\n\n{read_line}"
            )
        if notice:
            placeholders["control_discovery_hint"] = (
                f"{notice}\n\n{placeholders.get('control_discovery_hint', '')}"
            )
        return self.async_show_form(
            step_id="shadow_learning_result",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        "result_action",
                        default=default_action,
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=action_options,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    ),
                }
            ),
            errors=errors,
            description_placeholders=placeholders,
        )

    def _control_discovery_enabled_selection_count(self) -> int:
        """Return how many discovered controls the user has turned on."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        enabled = selections.get("enabled_by_user")
        return len(enabled) if isinstance(enabled, list) else 0

    def _control_discovery_enabled_read_selection_count(self) -> int:
        """Return how many discovered read sensors the user has turned on."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        enabled = selections.get("read_enabled_by_user")
        if isinstance(enabled, list):
            return len(enabled)
        # Backward-compatible fallback for packages/runs created before read
        # review selections existed.
        return int(
            dict(self._shadow_learning_state.get("overlay") or {}).get(
                "generated_read_count"
            )
            or 0
        )

    def _control_discovery_review_selection_payload(
        self,
        overlay: dict[str, Any],
    ) -> dict[str, Any]:
        """Return the user's reviewed control selection without activating it."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        selected_controls = selections.get("controls")
        selected_reads = selections.get("read_sensors")
        if (
            (not isinstance(selected_controls, dict) or not selected_controls)
            and (not isinstance(selected_reads, dict) or not selected_reads)
        ):
            return {}

        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        return build_activation_selection(
            review_model=review_model,
            selections=selections,
        )

    async def _async_control_discovery_activate_selection(
        self, coordinator
    ) -> str | None:
        """Activate the user-selected discovered controls for this device.

        Builds the activation selection from the review model
        (``overlay.manifest.review_model``, EYB-REF-042) and the user's review
        choices (``review_selections``, EYB-REF-043) and activates the
        device-scoped learned overlay with exactly those controls (EYB-REF-044),
        so runtime exposes only what the user turned on. Returns ``None`` on
        success or an error code for the result form on failure.

        The discovered evidence is read-only here: ``build_activation_selection``
        never mutates the overlay manifest, so ``learned_all`` (including the
        disabled controls) stays intact for the support package.
        """

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        profile_name = str(overlay.get("profile_name") or "").strip()
        schema_name = str(overlay.get("schema_name") or "").strip()
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        try:
            if not profile_name or not schema_name:
                raise RuntimeError("shadow_learning_overlay_unavailable")
            selection = build_activation_selection(
                review_model=review_model,
                selections=selections,
            )
            activation = await coordinator.async_activate_device_scoped_overlay(
                profile_name=profile_name,
                register_schema_name=schema_name,
                selection=selection,
            )
            self._shadow_learning_state["activation"] = dict(activation)
            self._publish_shadow_learning_artifacts(coordinator)
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.shadow_learning_status_overlay_activated",
                "Discovered controls activated for this device and reload "
                "requested.",
            )
            return None
        except Exception as exc:  # noqa: BLE001 - surfaced to the user as a form error
            self._shadow_learning_state["status"] = str(exc)
            return "shadow_learning_failed"

    async def _async_control_discovery_export_support(
        self, coordinator
    ) -> str | None:
        """Export a support package from the guided result screen.

        Mirrors the advanced path's support-only export: publishes the current
        UX artifacts and writes a sanitized support archive without re-running
        any live SmartESS operation. Returns ``None`` on success or an error code
        on failure.
        """

        try:
            self._publish_shadow_learning_artifacts(coordinator)
            path = await coordinator.async_export_support_package_with_cloud_refresh(
                smartess_username="",
                smartess_password="",
                wants_refresh=False,
            )
            self._shadow_learning_state["support_package_path"] = str(path)
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.shadow_learning_status_support_exported",
                "Support package exported without running control discovery.",
            )
            return None
        except Exception as exc:  # noqa: BLE001 - surfaced to the user as a form error
            self._shadow_learning_state["status"] = str(exc)
            return "shadow_learning_failed"

    def _control_discovery_placeholders(
        self,
        coordinator,
        hint_key: str,
        hint_default: str,
        *,
        hint_placeholders: dict[str, Any] | None = None,
        extra: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Build wizard description placeholders plus one step-specific hint.

        Reuses the existing shadow-learning placeholder set so the status table
        renders, and adds ``control_discovery_hint`` for the guided steps. The
        hint string lives in ``flow_translations``; wiring it into the rendered
        step templates (``translations/*.json``) is a follow-up.

        ``hint_placeholders`` lets a dynamic step (the review screen) format the
        hint with run-specific values (e.g. the discovered control count and a
        summary table). ``extra`` exposes those same values as standalone
        placeholders for templates that prefer to render them separately.
        """

        placeholders = dict(self._shadow_learning_placeholders(coordinator))
        placeholders["control_discovery_hint"] = self._tr(
            hint_key,
            hint_default,
            {**placeholders, **(hint_placeholders or {})},
        )
        if extra:
            placeholders.update(extra)
        return placeholders

    @staticmethod
    def _control_discovery_cloud_provider(coordinator) -> str:
        provider = str(getattr(coordinator, "cloud_evidence_provider", "") or "").strip().lower()
        if provider:
            return provider
        family = str(getattr(coordinator, "collector_cloud_family", "") or "").strip().lower()
        if "value" in family:
            return "valuecloud"
        if "smartess" in family:
            return "smartess"
        # Historical entries only had the SmartESS-specific cloud-learning
        # implementation, so unknown legacy entries keep the previous behavior.
        return "smartess"

    def _control_discovery_cloud_provider_label(self, coordinator) -> str:
        provider = self._control_discovery_cloud_provider(coordinator)
        if provider == "valuecloud":
            return "ValueCloud"
        if provider == "smartess":
            return "SmartESS"
        return provider or self._tr("common.dynamic.cloud_provider", "cloud service")

    def _control_discovery_cloud_app_label(self, coordinator) -> str:
        provider = self._control_discovery_cloud_provider(coordinator)
        if provider == "valuecloud":
            return "SmartValue"
        if provider == "smartess":
            return "SmartESS"
        return self._control_discovery_cloud_provider_label(coordinator)

    @staticmethod
    def _preflight_effective_metadata(coordinator) -> dict[str, Any]:
        """Return the effective metadata the preflight validates.

        Delegates to the coordinator so this preview preflight and the actual
        learning start path (``async_start_shadow_learning``) share ONE fallback
        implementation. They used to each carry their own copy and drifted: the
        preview fell back to live metadata while the start path passed the raw
        (empty, for a partial tier) persisted snapshot, so learning previewed as
        startable and then failed with ``missing_effective_metadata_snapshot``.
        """

        return coordinator.shadow_learning_effective_metadata

    async def _build_shadow_learning_preflight_snapshot(self, coordinator) -> dict[str, Any]:
        connected = bool(getattr(coordinator.data, "connected", False))
        raw_capture = None
        if connected:
            with suppress(Exception):
                raw_capture = await coordinator._runtime.async_capture_support_evidence()
        seed, blockers = build_shadow_learning_seed(
            session_id=f"{self._config_entry.entry_id}_preview",
            entry_id=self._config_entry.entry_id,
            collector_pn=coordinator.smartess_collector_pn,
            collector_cloud_family=coordinator.collector_cloud_family,
            raw_passthrough_frame_format=getattr(
                coordinator,
                "collector_raw_passthrough_frame_format",
                "",
            ),
            collector_cloud_profile_key=coordinator.collector_cloud_profile_key,
            collector_cloud_profile_label=coordinator.collector_cloud_profile_label,
            collector_cloud_profile_source=coordinator.collector_cloud_profile_source,
            collector_cloud_profile_confidence=coordinator.collector_cloud_profile_confidence,
            collector_callback_endpoint=coordinator.collector_callback_target_endpoint,
            effective_metadata_snapshot=self._preflight_effective_metadata(coordinator),
            raw_capture=raw_capture,
        )
        preflight = build_shadow_learning_preflight(seed)
        effective_blockers = list(blockers or preflight.blockers)
        can_start = bool(preflight.can_start)
        if not connected:
            # The register seed can only be captured from a LIVE collector. When it is offline
            # the seed is empty and the only blocker is the cryptic "missing_register_seed";
            # surface the real cause so the user knows to bring the collector back online rather
            # than suspecting a code regression.
            effective_blockers = ["collector_not_connected"] + [
                blocker for blocker in effective_blockers if blocker != "missing_register_seed"
            ]
        # Memory guard: the scan spins up a cloud sign-in + proxy capture +
        # correlation pass. On a memory-tight host that spike can push the box
        # into the OOM killer (and a watchdog reset). Refuse up front rather than
        # risk taking the whole appliance down. Unknown memory (non-Linux) skips
        # the guard.
        available_mib = await self.hass.async_add_executor_job(read_available_memory_mib)
        memory_blocker = shadow_learning_memory_blocker(available_mib)
        if memory_blocker:
            effective_blockers = [memory_blocker] + effective_blockers
            can_start = False
        route_status = self._shadow_learning_route_status(coordinator)
        return {
            "can_start": can_start,
            "blockers": effective_blockers,
            "protocol_adapter_key": str(seed.protocol_adapter_key or ""),
            "protocol_adapter_supported": bool(
                seed.protocol_adapter_key and not any(
                    str(blocker).startswith("unsupported_shadow_learning_protocol:")
                    for blocker in effective_blockers
                )
            ),
            "collector_pn": coordinator.smartess_collector_pn,
            "profile_name": str(coordinator.effective_profile_name or ""),
            "schema_name": str(coordinator.effective_register_schema_name or ""),
            "shadow_session_state": self._shadow_learning_session_state(coordinator),
            "shadow_session_active": bool(route_status.get("running")),
            "shadow_session_ready": bool(route_status.get("ready")),
            "shadow_session_running": bool(route_status.get("running")),
            "shadow_session_collector_connected": bool(route_status.get("collector_connected")),
            "shadow_session_upstream_connected": bool(route_status.get("upstream_connected")),
        }

    def _shadow_learning_route_status(self, coordinator) -> dict[str, Any]:
        route_status_fn = getattr(getattr(coordinator, "_runtime", None), "shadow_learning_route_status", None)
        if callable(route_status_fn):
            status = route_status_fn()
            if isinstance(status, dict):
                return {
                    "running": bool(status.get("running")),
                    "collector_connected": bool(status.get("collector_connected")),
                    "collector_protocol_ingress": bool(status.get("collector_protocol_ingress")),
                    "route_protocol_activity": bool(status.get("route_protocol_activity")),
                    "upstream_connected": bool(status.get("upstream_connected")),
                    "ready": bool(status.get("ready")),
                    "upstream_error": str(status.get("upstream_error") or ""),
                }

        route_running_fn = getattr(getattr(coordinator, "_runtime", None), "shadow_learning_route_running", None)
        running = bool(route_running_fn()) if callable(route_running_fn) else False
        return {
            "running": running,
            "collector_connected": False,
            "collector_protocol_ingress": False,
            "route_protocol_activity": False,
            "upstream_connected": False,
            "ready": False,
            "upstream_error": "",
        }

    def _shadow_learning_settings_dat(self, coordinator) -> dict[str, Any] | None:
        record = self._cached_cloud_evidence_record(coordinator)
        if record is None:
            return None
        payload = dict(record.payload or {})
        bundle = payload.get("payload")
        if not isinstance(bundle, dict):
            return None
        normalized = bundle.get("normalized")
        if not isinstance(normalized, dict):
            return None
        settings = normalized.get("device_settings")
        if not isinstance(settings, dict):
            return None
        fields = settings.get("fields")
        if not isinstance(fields, list):
            return None
        return {"field": [dict(item) for item in fields if isinstance(item, dict)]}

    def _shadow_learning_settings_dat_from_bundle(
        self, bundle: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """Return raw SmartESS settings dat from a live device bundle."""

        if not isinstance(bundle, dict):
            return None
        responses = bundle.get("responses")
        if not isinstance(responses, dict):
            return None
        settings = responses.get("device_settings")
        if not isinstance(settings, dict):
            return None
        dat = settings.get("dat")
        return dict(dat) if isinstance(dat, dict) else None

    def _shadow_learning_cloud_identity(self, coordinator) -> dict[str, Any] | None:
        record = self._cached_cloud_evidence_record(coordinator)
        if record is None:
            return None
        identity = record.payload.get("device_identity")
        if not isinstance(identity, dict):
            return None
        pn = str(identity.get("pn") or "").strip()
        sn = str(identity.get("sn") or "").strip()
        devcode = identity.get("devcode")
        devaddr = identity.get("devaddr")
        if not pn or not sn or devcode is None or devaddr is None:
            return None
        return {
            "pn": pn,
            "sn": sn,
            "devcode": int(devcode),
            "devaddr": int(devaddr),
        }

    def _cached_cloud_evidence_record(self, coordinator):
        latest = getattr(coordinator, "_latest_smartess_cloud_evidence_record", None)
        if callable(latest):
            return latest()
        return None

    def _shadow_learning_cloud_identity_from_bundle(
        self, bundle: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """Return pn/sn/devcode/devaddr from a live SmartESS device bundle."""

        if not isinstance(bundle, dict):
            return None
        request = bundle.get("request")
        if not isinstance(request, dict):
            return None
        params = request.get("params")
        if not isinstance(params, dict):
            return None
        pn = str(params.get("pn") or "").strip()
        sn = str(params.get("sn") or "").strip()
        devcode = _coerce_int(params.get("devcode"))
        devaddr = _coerce_int(params.get("devaddr"))
        if not pn or not sn or devcode is None or devaddr is None:
            return None
        return {
            "pn": pn,
            "sn": sn,
            "devcode": devcode,
            "devaddr": devaddr,
        }

    def _shadow_learning_observed_writes(self, coordinator) -> tuple[Any, ...]:
        handler = self._shadow_learning_observation_source(coordinator)
        observations = getattr(handler, "write_observations", ())
        return tuple(observations or ())

    def _shadow_learning_observation_source(self, coordinator):
        runtime = getattr(coordinator, "_runtime", None)
        # ``coordinator._runtime`` is the EybondHub itself, which owns
        # ``_link_manager`` directly; navigate to it without an intermediate
        # ``_hub`` hop. A wrapped runtime that exposes the hub under ``_hub`` is
        # still supported as a fallback.
        link_manager = getattr(runtime, "_link_manager", None)
        if link_manager is None:
            hub = getattr(runtime, "_hub", None)
            link_manager = getattr(hub, "_link_manager", None)
        return getattr(link_manager, "_shadow_learning_handler", None)

    def _publish_shadow_learning_artifacts(self, coordinator) -> dict[str, Any]:
        """Publish the current UX artifact state into support-package runtime values."""

        publish = getattr(coordinator, "publish_shadow_learning_artifacts", None)
        if not callable(publish):
            return {}
        state = dict(self._shadow_learning_state or {})
        plan = dict(state.get("plan") or {})
        orchestration = dict(state.get("orchestration") or {})
        if not orchestration:
            discovery = state.get("discovery")
            if isinstance(discovery, dict) and discovery:
                orchestration = {
                    "source": "control_discovery_runner",
                    "status": str(discovery.get("status") or ""),
                    "reason": str(discovery.get("reason") or ""),
                    "preflight": dict(state.get("preflight") or {}),
                }
        correlation = orchestration.get("correlation")
        if not isinstance(correlation, dict):
            correlation = {}
        session = dict(state.get("session") or {})
        identity = dict(state.get("identity") or {})
        overlay = dict(state.get("overlay") or {})
        activation = dict(state.get("activation") or {})
        device_scope = {
            "collector_pn": str(
                getattr(coordinator, "smartess_collector_pn", "") or ""
            ),
            "cloud_pn": str(identity.get("pn") or ""),
            "cloud_sn": str(identity.get("sn") or ""),
            "devcode": identity.get("devcode"),
            "devaddr": identity.get("devaddr"),
        }
        overlay_manifest = overlay.get("manifest")
        if isinstance(overlay_manifest, dict):
            manifest_scope = overlay_manifest.get("scope")
            if isinstance(manifest_scope, dict):
                device_scope.update(manifest_scope)
            elif manifest_scope:
                device_scope["scope"] = str(manifest_scope)
        if not activation and overlay:
            activation = {
                "status": "draft_generated",
                "active": False,
                "scope": device_scope.get("scope") or "device",
                "activation_scope": device_scope,
                "profile_name": str(overlay.get("profile_name") or ""),
                "register_schema_name": str(overlay.get("schema_name") or ""),
            }
            review_selection = self._control_discovery_review_selection_payload(
                overlay
            )
            if review_selection:
                activation.update(review_selection)
                activation["status"] = "review_selected"
        return publish(
            plan=plan,
            orchestration=orchestration,
            correlation=correlation,
            trace_path=str(session.get("trace_path") or ""),
            profile_draft_path=str(overlay.get("profile_path") or ""),
            schema_draft_path=str(overlay.get("schema_path") or ""),
            activation=activation,
            session_id=str(
                session.get("session_id")
                or session.get("trace_path")
                or ""
            ),
            device_scope=device_scope,
        )

    def _shadow_learning_session_state(self, coordinator) -> str:
        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        explicit_state = str(values.get("shadow_learning_session_status") or "").strip().lower()
        route_status = self._shadow_learning_route_status(coordinator)

        # Route status is authoritative for live execution readiness.
        if bool(route_status.get("ready")):
            if explicit_state == "learning":
                return "learning"
            return "ready"
        if bool(route_status.get("running")):
            if bool(route_status.get("collector_connected")) and not bool(route_status.get("route_protocol_activity")):
                return "waiting_for_collector"
            if bool(route_status.get("route_protocol_activity")) and not bool(route_status.get("upstream_connected")):
                return "connecting_upstream"
            return "degraded"

        if explicit_state in {
            "preflight",
            "starting",
            "restoring",
            "restore_failed",
            "failed",
            "stopped",
        }:
            return explicit_state
        return "stopped"

    def _shadow_learning_route_accepts_control(self, coordinator) -> bool:
        """Return whether SmartESS control commands may be sent through the route.

        SAFETY-CRITICAL. A ``ctrlDevice`` is delivered cloud -> the collector's
        MAIN (param-21) link. That write reaches the inverter UNPROXIED unless the
        param-21 link currently terminates on our proxy. The only real-time signal
        for that is ``collector_connected`` -- the live collector->proxy socket,
        which is STABLE for the duration of a scan (it is the separate *upstream*
        proxy->cloud socket that is short-lived, not this one). If the collector
        reboots after the endpoint switch and reconnects onto the real server, or
        reverts mid-scan, ``collector_connected`` drops and control must stop
        immediately. A "reached us once" signal is NOT acceptable here: it stays
        true after a revert and let probing continue onto the real server, which
        turned off the user's inverter output.
        """

        status = self._shadow_learning_route_status(coordinator)
        if not bool(status.get("running")):
            return False
        if str(status.get("upstream_error") or "").strip():
            return False
        # SAFETY: start readiness and write readiness are deliberately different.
        # Start only needs a collector->proxy reconnect; an actual ctrlDevice must
        # also have a live proxy->cloud upstream socket, otherwise SmartESS may
        # deliver the command over the real-server route and bypass our shadow.
        return route_status_indicates_control_write_ready(status)

    def _shadow_learning_placeholders(self, coordinator) -> dict[str, str]:
        state = dict(self._shadow_learning_state or {})
        preflight = dict(state.get("preflight") or {})
        plan = dict(state.get("plan") or {})
        orchestration = dict(state.get("orchestration") or {})
        correlation = dict(orchestration.get("correlation") or {})
        overlay = dict(state.get("overlay") or {})
        activation = dict(state.get("activation") or {})
        session = dict(state.get("session") or {})
        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}

        learned_summary = overlay.get("manifest", {}).get("learned_capabilities", [])
        destructive_count = 0
        action_count = 0
        if isinstance(learned_summary, list):
            for item in learned_summary:
                if not isinstance(item, dict):
                    continue
                if str(item.get("value_kind") or "") == "action":
                    action_count += 1
                if str(item.get("safety_class") or "") == "destructive":
                    destructive_count += 1

        blockers = preflight.get("blockers") or []
        if not isinstance(blockers, list):
            blockers = []
        can_start = bool(preflight.get("can_start"))
        session_state = str(self._shadow_learning_session_state(coordinator) or "").strip()
        if not session_state:
            session_state = str(session.get("status") or preflight.get("shadow_session_state") or "").strip()
        return {
            "cloud_provider": self._control_discovery_cloud_provider(coordinator),
            "cloud_provider_label": self._control_discovery_cloud_provider_label(coordinator),
            "cloud_app_label": self._control_discovery_cloud_app_label(coordinator),
            "shadow_learning_status": str(state.get("status") or self._tr("common.dynamic.not_run_yet", "Not run yet")),
            "shadow_learning_preflight": self._tr(
                "common.dynamic.shadow_learning_preflight_summary",
                "Can start: {can_start}; blockers: {blockers}",
                {
                    "can_start": self._tr("common.dynamic.yes", "Yes") if can_start else self._tr("common.dynamic.no", "No"),
                    "blockers": ", ".join(blockers) or self._tr("common.dynamic.none", "None"),
                },
            ),
            "shadow_learning_session_state": session_state or self._tr("common.dynamic.not_run_yet", "Not run yet"),
            "shadow_learning_trace_path": str(session.get("trace_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "shadow_learning_plan_count": str(len(plan.get("items") or [])),
            "shadow_learning_found_controls": str(int(overlay.get("generated_capability_count") or 0)),
            "shadow_learning_found_actions": str(action_count),
            "shadow_learning_found_destructive": str(destructive_count),
            "shadow_learning_unmatched_fields": str(int(correlation.get("unmatched_attempt_count") or 0)),
            "shadow_learning_overlay_profile_path": str(overlay.get("profile_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "shadow_learning_overlay_schema_path": str(overlay.get("schema_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "shadow_learning_support_package_path": str(state.get("support_package_path") or values.get("support_package_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "shadow_learning_activation_scope": str(activation.get("scope") or values.get("effective_device_scoped_overlay_scope") or self._tr("common.dynamic.not_available", "Not available")),
            "shadow_learning_activation_status": (
                self._tr("common.dynamic.yes", "Yes")
                if bool(values.get("effective_device_scoped_overlay_active"))
                else self._tr("common.dynamic.no", "No")
            ),
            "shadow_learning_warning": self._tr(
                "common.dynamic.shadow_learning_warning",
                "Control discovery is advanced and optional. It briefly uses {cloud_provider_label} to test which settings Home Assistant can control. Testing all option values or numeric settings can trigger cloud-side actions and requires explicit preview and confirmation.",
                {
                    "cloud_provider_label": self._control_discovery_cloud_provider_label(
                        coordinator
                    )
                },
            ),
        }

    @_with_translation_bundle
    async def async_step_proxy_capture(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "proxy_capture_title",
                    "Collector Proxy Capture",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        errors: dict[str, str] = {}
        action = ""
        touch_proxy_capture_lease = getattr(coordinator, "async_touch_proxy_capture_lease", None)
        if user_input is not None:
            action = str(user_input.get("proxy_capture_action") or "refresh").strip()
            try:
                if action == "start":
                    overview = coordinator.proxy_capture_overview
                    await coordinator.async_start_proxy_capture(
                        anonymized=True,
                        confirm_redirect=bool(getattr(overview, "redirect_required", False)),
                    )
                    self._proxy_capture_action_result = self._tr(
                        "common.dynamic.proxy_capture_action_started",
                        "Capture started.",
                    )
                elif action == PROXY_CAPTURE_ACTION_RESET_TIMER:
                    expires_at = ""
                    if touch_proxy_capture_lease is not None:
                        expires_at = str(await touch_proxy_capture_lease(extend=True) or "").strip()
                    if expires_at:
                        self._proxy_capture_action_result = self._tr(
                            "common.dynamic.proxy_capture_action_timer_reset",
                            "Proxy timer reset.",
                        )
                    else:
                        self._proxy_capture_action_result = self._tr(
                            "common.dynamic.proxy_capture_action_already_stopped",
                            "Capture was already stopped. Status refreshed.",
                        )
                elif action == "stop":
                    await coordinator.async_stop_proxy_capture()
                    self._proxy_capture_action_result = self._tr(
                        "common.dynamic.proxy_capture_action_stopped",
                        "Capture stopped.",
                    )
                else:
                    refresh = getattr(coordinator, "async_request_refresh", None)
                    if refresh is not None:
                        await refresh()
                    self._proxy_capture_action_result = self._tr(
                        "common.dynamic.proxy_capture_action_refreshed",
                        "Live log refreshed.",
                    )
            except Exception as exc:  # pragma: no cover - HA renders the error key.
                if await self._handle_proxy_capture_action_error(coordinator, action, exc):
                    errors.clear()
                else:
                    errors.setdefault("base", "proxy_capture_action_failed")
                    self._proxy_capture_action_result = self._proxy_capture_action_error_message(exc)

        if touch_proxy_capture_lease is not None and user_input is None:
            await touch_proxy_capture_lease(extend=False)

        return self._show_proxy_capture_form(coordinator, errors=errors)

    async def _handle_proxy_capture_action_error(
        self,
        coordinator,
        action: str,
        exc: Exception,
    ) -> bool:
        if action != "stop":
            return False
        if str(exc or "").strip() != "proxy_capture_not_running":
            return False

        refresh = getattr(coordinator, "async_request_refresh", None)
        if refresh is not None:
            await refresh()
        self._proxy_capture_action_result = self._tr(
            "common.dynamic.proxy_capture_action_already_stopped",
            "Capture was already stopped. Status refreshed.",
        )
        return True

    def _proxy_capture_action_error_message(self, exc: Exception) -> str:
        raw_error = str(exc or "").strip()
        if not raw_error:
            return self._tr(
                "common.dynamic.proxy_capture_action_error_internal",
                "Collector proxy capture could not be started. Check the Home Assistant log and try again.",
            )

        error_code, _separator, detail = raw_error.partition(":")
        if error_code == "proxy_capture_route_stopped":
            return self._tr(
                "common.dynamic.proxy_capture_action_error_route_stopped",
                "Collector proxy route stopped before the collector reconnected. Check the Home Assistant log and try again.",
            )
        if error_code == "proxy_capture_collector_reconnect_timeout":
            return self._tr(
                "common.dynamic.proxy_capture_action_error_reconnect_timeout",
                "Collector did not reconnect through the proxy in time. Check the collector callback settings and try again.",
            )
        if error_code in {
            "proxy_capture_upstream_connect_failed",
            "proxy_capture_upstream_unreachable",
        }:
            return self._tr(
                "common.dynamic.proxy_capture_action_error_upstream_connect_failed",
                "Home Assistant could not connect to the current upstream collector endpoint: {detail}.",
                {
                    "detail": detail or self._tr("common.dynamic.not_available", "Not available"),
                },
            )
        if error_code == "proxy_capture_not_running":
            return self._tr(
                "common.dynamic.proxy_capture_action_already_stopped",
                "Capture was already stopped. Status refreshed.",
            )
        if " " not in raw_error and raw_error.lower() == raw_error:
            return self._tr(
                "common.dynamic.proxy_capture_action_error_internal",
                "Collector proxy capture could not be started. Check the Home Assistant log and try again.",
            )
        return raw_error

    def _proxy_capture_action_options(self, coordinator) -> list[SelectOptionDict]:
        overview = coordinator.proxy_capture_overview
        options: list[SelectOptionDict] = []
        if overview.can_stop:
            options.append(
                SelectOptionDict(
                    value="stop",
                    label=self._tr("common.dynamic.proxy_capture_action_stop", "Stop proxy capture"),
                )
            )
            options.append(
                SelectOptionDict(
                    value=PROXY_CAPTURE_ACTION_RESET_TIMER,
                    label=self._tr(
                        "common.dynamic.proxy_capture_action_reset_timer",
                        "Reset proxy timer",
                    ),
                )
            )
        if overview.can_start:
            options.append(
                SelectOptionDict(
                    value="start",
                    label=self._tr("common.dynamic.proxy_capture_action_start", "Start proxy capture"),
                )
            )
        options.append(
            SelectOptionDict(
                value="refresh",
                label=self._tr(
                    "common.dynamic.proxy_capture_action_refresh",
                    "Refresh live log",
                ),
            )
        )
        return options

    def _default_proxy_capture_action(self, coordinator, options: list[SelectOptionDict]) -> str:
        """Return the default proxy-capture action for the current form state."""

        option_values = {str(option["value"]) for option in options}
        overview = coordinator.proxy_capture_overview
        if overview.can_start and "start" in option_values:
            return "start"
        if overview.can_stop and "refresh" in option_values:
            return "refresh"
        if "refresh" in option_values:
            return "refresh"
        return str(options[0]["value"] if options else "refresh")

    def _show_proxy_capture_form(
        self,
        coordinator,
        *,
        errors: dict[str, str] | None = None,
    ) -> ConfigFlowResult:
        options = self._proxy_capture_action_options(coordinator)
        default_action = self._default_proxy_capture_action(coordinator, options)
        placeholders = self._diagnostics_placeholders()
        return self.async_show_form(
            step_id="proxy_capture",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        "proxy_capture_live_log_view",
                        default=placeholders.get("proxy_capture_live_log") or "",
                    ): _MULTILINE_LOG_TEXT_SELECTOR,
                    vol.Required("proxy_capture_action", default=default_action): SelectSelector(
                        SelectSelectorConfig(options=options, mode=SelectSelectorMode.DROPDOWN)
                    ),
                }
            ),
            errors=errors or {},
            description_placeholders=placeholders,
        )

    @_with_translation_bundle
    async def async_step_create_support_package(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_archive_title",
                    "Support Archive",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        capabilities = self._collector_capabilities()
        is_bridge = capabilities.virtual_bridge
        can_refresh_cloud_evidence = (
            self._cloud_evidence_export_available(coordinator)
            and capabilities.cloud_evidence
        )
        saved_cloud_evidence_path = self._current_cloud_evidence_path(coordinator)
        had_saved_cloud_evidence = bool(saved_cloud_evidence_path) and capabilities.cloud_evidence

        if user_input is None and can_refresh_cloud_evidence:
            return self._show_create_support_package_form(
                coordinator=coordinator,
                saved_cloud_evidence_path=saved_cloud_evidence_path,
            )

        archive_cloud_mode = self._default_support_archive_cloud_mode(
            had_saved_cloud_evidence=had_saved_cloud_evidence,
        )
        smartess_username = ""
        smartess_password = ""
        wants_inline_refresh = False

        if can_refresh_cloud_evidence:
            form_input = user_input or {}
            archive_cloud_mode = str(
                form_input.get(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE)
                or self._default_support_archive_cloud_mode(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                )
            )
            smartess_username = str(form_input.get("username") or "").strip()
            smartess_password = str(form_input.get("password") or "").strip()
            wants_inline_refresh = archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
            errors: dict[str, str] = {}
            if wants_inline_refresh:
                if not smartess_username:
                    errors["username"] = "required"
                if not smartess_password:
                    errors["password"] = "required"
            if errors:
                return self._show_create_support_package_form(
                    coordinator=coordinator,
                    saved_cloud_evidence_path=saved_cloud_evidence_path,
                    user_input=form_input,
                    errors=errors,
                )

        try:
            path = await coordinator.async_export_support_package_with_cloud_refresh(
                smartess_username=smartess_username,
                smartess_password=smartess_password,
                wants_refresh=wants_inline_refresh,
            )
        except Exception as exc:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_archive_title",
                    "Support Archive",
                ),
                status=self._diagnostics_result_tr(
                    "support_archive_failed_status",
                    "Support archive export failed: {error}",
                    {"error": str(exc)},
                ),
                next_step=self._diagnostics_result_tr(
                    (
                        "support_archive_failed_next_refresh"
                        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
                        else "support_archive_failed_next"
                    ),
                    (
                        "Check the cloud account credentials, or rerun Create support archive and choose a different cloud evidence mode."
                        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
                        else "Check whether the entry is loaded and the Home Assistant config directory is writable, then try again."
                    ),
                ),
            )

        download_url = str(
            coordinator.data.values.get("support_package_download_url")
            or coordinator.data.values.get("support_package_download_relative_url")
            or ""
        )
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "support_archive_created_title",
                "Support Archive Created",
            ),
            status=self._diagnostics_result_tr(
                "support_archive_created_status",
                "A combined support archive with runtime data, raw capture evidence, an anonymized replay fixture, and matching cloud evidence when available was written to the Home Assistant config directory.\n\n{support_archive_cloud_detail}",
                {
                    "support_archive_cloud_detail": self._support_archive_cloud_result_detail(
                        archive_cloud_mode=archive_cloud_mode,
                        had_saved_cloud_evidence=had_saved_cloud_evidence,
                    )
                },
            ),
            path=path,
            download_url=download_url,
            next_step=self._diagnostics_result_tr(
                "support_archive_created_next",
                "Send this single ZIP file to the developer. Create local experimental drafts only after the archive has been reviewed.",
            ),
        )

    @_with_translation_bundle
    async def async_step_reload_local_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "reload_local_metadata_title",
                    "Reload Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "wait_for_entry_loaded",
                    "Wait for the entry to finish loading, then try again.",
                ),
            )

        await coordinator.async_reload_local_metadata()
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_title",
                "Local Metadata Reload Triggered",
            ),
            status=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_status",
                "Local metadata caches were cleared and the entry reload was requested.",
            ),
            next_step=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_next",
                "Refresh the device page after the entry reconnects to confirm whether local overrides were applied.",
            ),
        )

    @_with_translation_bundle
    async def async_step_rollback_local_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        rollback_paths = self._local_metadata_rollback_paths()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_title",
                    "Rollback Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "wait_for_entry_loaded",
                    "Wait for the entry to finish loading, then try again.",
                ),
            )

        if not rollback_paths.paths:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_title",
                    "Rollback Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "rollback_local_metadata_unavailable_status",
                    "No active managed local metadata override is available to roll back for this entry.",
                ),
                next_step=self._diagnostics_result_tr(
                    "rollback_local_metadata_unavailable_next",
                    "Create or activate a local override first, or use Reload local metadata if the files were already removed manually.",
                ),
            )

        if user_input is not None:
            try:
                removed_paths = await coordinator.async_rollback_local_metadata()
            except Exception as exc:
                return await self._async_show_diagnostics_result(
                    action_title=self._diagnostics_result_tr(
                        "rollback_local_metadata_title",
                        "Rollback Local Metadata",
                    ),
                    status=self._diagnostics_result_tr(
                        "rollback_local_metadata_failed_status",
                        "Local metadata rollback failed: {error}",
                        {"error": str(exc)},
                    ),
                    next_step=self._diagnostics_result_tr(
                        "rollback_local_metadata_failed_next",
                        "Check whether the active override files still exist under /config/eybond_local/, then try again.",
                    ),
                )

            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_title",
                    "Local Metadata Rolled Back",
                ),
                status=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_status",
                    "The active managed local override files were removed and the entry reload was requested.",
                ),
                path=" ; ".join(removed_paths),
                next_step=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_next",
                    "Refresh the device page after the entry reconnects to confirm that the built-in metadata is active again.",
                ),
            )

        not_available = self._tr("common.dynamic.not_available", "Not available")
        return self.async_show_form(
            step_id="rollback_local_metadata",
            data_schema=vol.Schema({}),
            description_placeholders={
                "rollback_target_count": str(len(rollback_paths.paths)),
                "rollback_profile_path": str(rollback_paths.profile_path or not_available),
                "rollback_schema_path": str(rollback_paths.schema_path or not_available),
            },
        )

    @_with_translation_bundle
    async def async_step_diagnostics_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if user_input is not None:
            return await self.async_step_diagnostics()

        return self.async_show_form(
            step_id="diagnostics_result",
            data_schema=vol.Schema({}),
            description_placeholders=self._diagnostics_result,
        )

    def _control_summary(self, *, control_mode: str, confidence: str) -> str:
        if control_mode == CONTROL_MODE_FULL:
            return self._tr("common.dynamic.control_full", "All controls are enabled.")
        if control_mode == CONTROL_MODE_READ_ONLY:
            return self._tr(
                "common.dynamic.control_read_only",
                "Monitoring only — no control entities are exposed.",
            )
        if confidence == "high":
            return self._tr(
                "common.dynamic.control_auto",
                "Tested controls are enabled automatically.",
            )
        return self._tr(
            "common.dynamic.control_waiting",
            "Monitoring only until a high-confidence detection is confirmed.",
        )

    def _confidence_label(self, confidence: str) -> str:
        return {
            "high": self._tr("common.dynamic.confidence_high", "High confidence"),
            "medium": self._tr("common.dynamic.confidence_medium", "Medium confidence"),
            "low": self._tr("common.dynamic.confidence_low", "Low confidence"),
            "none": self._tr("common.dynamic.confidence_none", "No confidence"),
        }.get(confidence, confidence)

    def _coordinator(self):
        return getattr(self._config_entry, "runtime_data", None)

    async def _async_with_options_collector_session(self):
        spec = build_connection_spec(self._config_entry.data, self._config_entry.options)
        collector_ip = str(
            getattr(spec, "collector_ip", "")
            or self._config_entry.options.get(CONF_COLLECTOR_IP, "")
            or self._config_entry.data.get(CONF_COLLECTOR_IP, "")
            or ""
        ).strip()
        if not collector_ip:
            raise RuntimeError("collector_ip_unavailable")

        transport = SharedEybondTransport(
            host=getattr(spec, "server_ip", self._config_entry.data[CONF_SERVER_IP]),
            port=getattr(spec, "tcp_port", DEFAULT_TCP_PORT),
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            heartbeat_interval=float(getattr(spec, "heartbeat_interval", DEFAULT_HEARTBEAT_INTERVAL)),
            collector_ip=collector_ip,
        )
        await transport.start()
        try:
            with suppress(Exception):
                await async_probe_target(
                    bind_ip=getattr(spec, "server_ip", self._config_entry.data[CONF_SERVER_IP]),
                    advertised_server_ip=getattr(
                        spec,
                        "effective_advertised_server_ip",
                        getattr(spec, "server_ip", self._config_entry.data[CONF_SERVER_IP]),
                    ),
                    advertised_server_port=getattr(
                        spec,
                        "effective_advertised_tcp_port",
                        getattr(spec, "tcp_port", DEFAULT_TCP_PORT),
                    ),
                    target_ip=collector_ip,
                    udp_port=getattr(spec, "udp_port", DEFAULT_UDP_PORT),
                    timeout=1.0,
                )
            connected = await transport.wait_until_connected(timeout=5.0)
            if not connected:
                raise ConnectionError("collector_not_connected")
            await transport.wait_until_heartbeat(timeout=1.5)
            return transport, SmartEssLocalSession(transport)
        except Exception:
            await transport.stop()
            raise

    async def _async_query_options_collector_text(
        self,
        session: SmartEssLocalSession,
        parameter: int,
    ) -> str:
        response = await session.query_collector(parameter)
        if response.code != 0:
            return ""
        return self._collector_query_response_text(response)

    async def _async_refresh_collector_wifi_status(self) -> None:
        transport, session = await self._async_with_options_collector_session()
        try:
            current_ssid = await self._async_query_options_collector_text(session, SET_TARGET_SSID)
            network_diagnostics = await self._async_query_options_collector_text(
                session,
                QUERY_NETWORK_DIAGNOSTICS,
            )
            scan_text = await self._async_query_options_collector_text(session, QUERY_WIFI_SCAN_LIST)
        finally:
            await transport.stop()

        self._collector_wifi_current_ssid = current_ssid
        self._collector_wifi_network_diagnostics = network_diagnostics
        self._collector_wifi_networks = self._parse_collector_wifi_scan_response(scan_text)

    async def _async_apply_collector_wifi_settings(self, *, ssid: str, password: str) -> None:
        transport, session = await self._async_with_options_collector_session()
        try:
            ssid_response = await session.set_collector(SET_TARGET_SSID, ssid)
            if ssid_response.status != 0 or ssid_response.parameter != SET_TARGET_SSID:
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_TARGET_SSID}:status={ssid_response.status}"
                )
            password_response = await session.set_collector(SET_TARGET_PASSWORD, password)
            if password_response.status != 0 or password_response.parameter != SET_TARGET_PASSWORD:
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_TARGET_PASSWORD}:status={password_response.status}"
                )
            readback = await session.query_collector(SET_TARGET_SSID)
            if readback.code == 0:
                self._collector_wifi_current_ssid = self._collector_query_response_text(readback)
            apply_response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
            if apply_response.status != 0 or apply_response.parameter != SET_REBOOT_OR_APPLY:
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_REBOOT_OR_APPLY}:status={apply_response.status}"
                )
        finally:
            await transport.stop()

    async def _async_refresh_collector_uart_status(self) -> None:
        transport, session = await self._async_with_options_collector_session()
        try:
            hardware_version = await self._async_query_options_collector_text(
                session,
                QUERY_HARDWARE_VERSION,
            )
            current_settings = await self._async_query_options_collector_text(
                session,
                QUERY_SERIAL_BAUDRATE,
            )
        finally:
            await transport.stop()

        if not hardware_version:
            hardware_version = self._runtime_collector_hardware_version()
        if not current_settings:
            current_settings = self._runtime_collector_uart_settings()
        self._collector_uart_hardware_version = hardware_version
        self._collector_uart_current_settings = current_settings
        self._collector_uart_current_baudrate = self._normalize_collector_uart_baudrate(
            current_settings
        )

    async def _async_apply_collector_uart_baudrate(self, baudrate: str) -> None:
        if self._collector_uart_runtime_change_unavailable():
            raise RuntimeError("collector_uart_runtime_unavailable")

        baudrate = self._normalize_collector_uart_baudrate(baudrate)
        if baudrate not in COLLECTOR_UART_BAUDRATES:
            raise ValueError(f"unsupported_collector_uart_baudrate:{baudrate}")

        transport, session = await self._async_with_options_collector_session()
        try:
            response = await session.set_collector(SET_SERIAL_BAUDRATE, baudrate)
            if response.status != 0 or response.parameter != SET_SERIAL_BAUDRATE:
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_SERIAL_BAUDRATE}:status={response.status}"
                )
        finally:
            await transport.stop()

        coordinator = self._coordinator()
        if coordinator is None:
            return
        invalidator = getattr(coordinator, "invalidate_collector_runtime_values", None)
        if callable(invalidator):
            invalidator()
        refresh = getattr(coordinator, "async_request_refresh", None)
        if callable(refresh):
            await refresh()

    @staticmethod
    def _collector_query_response_text(response) -> str:
        text = str(response.text or "").strip().strip("\x00")
        if text and all(character.isprintable() or character in "\r\n\t" for character in text):
            return text
        raw = bytes(getattr(response, "data", b"") or b"").rstrip(b"\x00")
        return raw.hex() if raw else text

    @staticmethod
    def _parse_collector_wifi_scan_response(scan_text: str) -> tuple[SmartEssBleWifiNetwork, ...]:
        text = str(scan_text or "").strip()
        if text.startswith("["):
            text = f"49,{text}"
        return parse_wifi_scan_response(text)

    def _collector_wifi_placeholders(self) -> dict[str, str]:
        return {
            "collector_ip": str(
                self._config_entry.options.get(
                    CONF_COLLECTOR_IP,
                    self._config_entry.data.get(CONF_COLLECTOR_IP, ""),
                )
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "current_ssid": self._collector_wifi_current_ssid
            or self._tr("common.dynamic.not_available", "Not available"),
            "status_updates": self._collector_wifi_status_updates(),
        }

    def _collector_wifi_status_updates(self) -> str:
        lines: list[str] = []
        if self._collector_wifi_last_result:
            lines.append(
                self._tr(
                    "common.dynamic.collector_wifi_last_action_line",
                    "**Last action:** {value}",
                    {"value": self._collector_wifi_last_result},
                )
            )
        if self._collector_wifi_last_error:
            lines.append(
                self._tr(
                    "common.dynamic.collector_wifi_last_error_line",
                    "**Last error:** {value}",
                    {"value": self._collector_wifi_last_error},
                )
            )
        if not lines:
            return ""
        return "\n\n" + "\n".join(lines)

    def _collector_wifi_refresh_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_wifi_action_refresh",
            "Refresh Wi-Fi list and status",
        )

    def _collector_wifi_apply_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_wifi_action_apply",
            "Apply Wi-Fi settings to the current collector",
        )

    def _runtime_collector_uart_settings(self) -> str:
        coordinator = self._coordinator()
        data = getattr(coordinator, "data", None)
        values = getattr(data, "values", None)
        if isinstance(values, dict):
            return str(values.get("collector_serial_baudrate") or "")
        return ""

    def _runtime_collector_hardware_version(self) -> str:
        coordinator = self._coordinator()
        data = getattr(coordinator, "data", None)
        values = getattr(data, "values", None)
        if isinstance(values, dict):
            return str(values.get("collector_hardware_version") or "")
        return ""

    def _collector_uart_runtime_change_unavailable(self) -> bool:
        capabilities = self._collector_capabilities()
        if capabilities.uart_management:
            return not capabilities.uart_runtime_speed_change
        hardware = (
            self._collector_uart_hardware_version
            or self._runtime_collector_hardware_version()
        ).lower()
        return any(marker in hardware for marker in ("bk72", "bk723", "rtl87", "libretiny"))

    @staticmethod
    def _normalize_collector_uart_baudrate(value: object) -> str:
        text = str(value or "").strip().strip("\x00")
        if not text:
            return ""
        baudrate = text.split(",", 1)[0].strip()
        return baudrate if baudrate in COLLECTOR_UART_BAUDRATES else ""

    def _collector_uart_placeholders(self) -> dict[str, str]:
        raw_settings = self._collector_uart_current_settings or self._runtime_collector_uart_settings()
        current_uart = raw_settings or self._collector_uart_current_baudrate
        hardware_version = self._collector_uart_hardware_version or self._runtime_collector_hardware_version()
        return {
            "collector_ip": str(
                self._config_entry.options.get(
                    CONF_COLLECTOR_IP,
                    self._config_entry.data.get(CONF_COLLECTOR_IP, ""),
                )
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "current_uart": current_uart or self._tr("common.dynamic.not_available", "Not available"),
            "hardware_version": hardware_version or self._tr("common.dynamic.not_available", "Not available"),
            "runtime_unavailable_note": self._collector_uart_runtime_unavailable_note(),
            "status_updates": self._collector_uart_status_updates(),
        }

    def _collector_uart_status_updates(self) -> str:
        lines: list[str] = []
        if self._collector_uart_last_result:
            lines.append(
                self._tr(
                    "common.dynamic.collector_uart_last_action_line",
                    "**Last action:** {value}",
                    {"value": self._collector_uart_last_result},
                )
            )
        if self._collector_uart_last_error:
            lines.append(
                self._tr(
                    "common.dynamic.collector_uart_last_error_line",
                    "**Last error:** {value}",
                    {"value": self._collector_uart_last_error},
                )
            )
        if not lines:
            return ""
        return "\n\n" + "\n".join(lines)

    def _collector_uart_refresh_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_uart_action_refresh",
            "Refresh UART status",
        )

    def _collector_uart_apply_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_uart_action_apply",
            "Apply UART speed to the current collector",
        )

    def _collector_uart_runtime_unavailable_note(self) -> str:
        if not self._collector_uart_runtime_change_unavailable():
            return ""
        return self._tr(
            "common.dynamic.collector_uart_runtime_unavailable_note",
            "\n\nThis collector reports BK72xx/LibreTiny hardware. Runtime UART speed switching is not available on this platform. Change `baud_rate:` in the ESPHome YAML and reflash the collector.",
        )

    def _metadata_source_summary(self, metadata) -> str:
        if metadata is None:
            return self._tr("common.dynamic.not_available", "Not available")
        source_path = getattr(metadata, "source_path", "") or self._tr(
            "common.dynamic.unknown_path", "Unknown path"
        )
        source_scope = getattr(metadata, "source_scope", "") or "unknown"
        if source_scope == "builtin":
            return self._tr(
                "common.dynamic.built_in_metadata",
                "Built-in metadata ({path})",
                {"path": source_path},
            )
        if source_scope == "external":
            return self._tr(
                "common.dynamic.local_override",
                "Local override ({path})",
                {"path": source_path},
            )
        return self._tr(
            "common.dynamic.external_metadata",
            "External metadata ({path})",
            {"path": source_path},
        )

    def _diagnostics_menu_options(self, primary_action: str) -> list[str]:
        coordinator = self._coordinator()
        rollback_paths = self._local_metadata_rollback_paths()
        menu_options: list[str] = [
            "create_support_package",
        ]

        # The free-form diagnostic command runner issues raw reads/writes/AT
        # commands directly on the device; expose its UI form only in Home
        # Assistant Advanced Mode. The run_diagnostic_commands action stays
        # available and is itself write-gated by confirm_write.
        if getattr(self, "show_advanced_options", False):
            menu_options.append("diagnostic_commands")

        if primary_action == "reload_local_metadata":
            menu_options.append("reload_local_metadata")

        if rollback_paths.paths and "rollback_local_metadata" not in menu_options:
            menu_options.append("rollback_local_metadata")

        # Proxy capture redirects the collector's cloud callback (FC=3 param 21)
        # to record the SmartESS cloud's control reads. A virtual bridge has no
        # SmartESS cloud side, so the action has nothing to capture — omit it
        # for a detected bridge. Fail-safe: factory collectors / unanswered
        # probes keep proxy capture exactly as before.
        if self._collector_capabilities().proxy_capture:
            menu_options.append("proxy_capture")
        return menu_options

    def _cloud_evidence_export_available(self, coordinator) -> bool:
        """Return whether this entry can fetch provider-specific cloud evidence."""

        generic_available = getattr(coordinator, "cloud_evidence_export_available", None)
        if generic_available is not None:
            return bool(generic_available)
        return bool(getattr(coordinator, "smartess_cloud_export_available", False))

    def _current_cloud_evidence_path(self, coordinator=None) -> str:
        """Return the latest cloud evidence path visible to diagnostics."""

        coordinator = coordinator or self._coordinator()
        if coordinator is None:
            return ""

        live_path = str(getattr(coordinator, "smartess_cloud_evidence_path", "") or "").strip()
        if live_path:
            return live_path

        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        return str(values.get("cloud_evidence_path") or "").strip()

    def _default_support_archive_cloud_mode(self, *, had_saved_cloud_evidence: bool) -> str:
        if had_saved_cloud_evidence:
            return SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED
        return SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY

    def _support_archive_cloud_mode_label(self, archive_cloud_mode: str) -> str:
        return {
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED: self._tr(
                "common.dynamic.support_archive_cloud_mode_use_saved",
                "Use saved cloud evidence",
            ),
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH: self._tr(
                "common.dynamic.support_archive_cloud_mode_refresh",
                "Fetch or refresh cloud evidence now",
            ),
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY: self._tr(
                "common.dynamic.support_archive_cloud_mode_archive_only",
                "Create the archive without cloud evidence",
            ),
        }.get(archive_cloud_mode, archive_cloud_mode)

    def _support_archive_cloud_mode_selector(
        self,
        *,
        had_saved_cloud_evidence: bool,
        can_refresh_cloud_evidence: bool = True,
    ) -> SelectSelector:
        options: list[SelectOptionDict] = []
        if had_saved_cloud_evidence:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                    ),
                )
            )
        else:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                    ),
                )
            )
        if can_refresh_cloud_evidence:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                    ),
                )
            )
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    def _support_archive_cloud_plan_summary(
        self,
        *,
        had_saved_cloud_evidence: bool,
        can_refresh_cloud_evidence: bool,
    ) -> str:
        if had_saved_cloud_evidence and can_refresh_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_saved_refreshable",
                "Saved cloud evidence will be included automatically, or you can refresh it in this same step before the archive is built.",
            )
        if had_saved_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_saved_only",
                "Saved cloud evidence will be included automatically when it matches this entry.",
            )
        if can_refresh_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_refreshable",
                "No cloud evidence is saved yet. You can fetch it in this step and include it in the same archive, or continue without it.",
            )
        return self._tr(
            "common.dynamic.support_archive_cloud_plan_unavailable",
            "No cloud evidence is currently available for this entry.",
        )

    def _support_archive_cloud_result_detail(
        self,
        *,
        archive_cloud_mode: str,
        had_saved_cloud_evidence: bool,
    ) -> str:
        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH:
            return self._tr(
                "common.dynamic.support_archive_cloud_result_refreshed",
                "Fresh cloud evidence was fetched in this step and included in the archive.",
            )
        if had_saved_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_result_saved",
                "Saved cloud evidence was included in the archive.",
            )
        return self._tr(
            "common.dynamic.support_archive_cloud_result_none",
            "No cloud evidence was included in the archive.",
        )

    def _show_create_support_package_form(
        self,
        *,
        coordinator,
        saved_cloud_evidence_path: str,
        user_input: dict[str, Any] | None = None,
        errors: dict[str, str] | None = None,
    ) -> ConfigFlowResult:
        capabilities = self._collector_capabilities()
        had_saved_cloud_evidence = bool(saved_cloud_evidence_path) and capabilities.cloud_evidence
        can_refresh_cloud_evidence = (
            self._cloud_evidence_export_available(coordinator)
            and capabilities.cloud_evidence
        )
        if not capabilities.cloud_evidence:
            saved_cloud_evidence_path = ""
            had_saved_cloud_evidence = False
        defaults = {
            CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: str(
                (user_input or {}).get(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE)
                or self._default_support_archive_cloud_mode(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                )
            ),
            "username": str((user_input or {}).get("username") or ""),
            "password": str((user_input or {}).get("password") or ""),
        }
        not_available = self._tr("common.dynamic.not_available", "Not available")
        not_created_yet = self._tr("common.dynamic.not_created_yet", "Not created yet")
        return self.async_show_form(
            step_id="create_support_package",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE,
                        default=defaults[CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE],
                    ): self._support_archive_cloud_mode_selector(
                        had_saved_cloud_evidence=had_saved_cloud_evidence,
                        can_refresh_cloud_evidence=can_refresh_cloud_evidence,
                    ),
                    **_smartess_credential_schema_fields(
                        required=False,
                        username_default=defaults["username"],
                        password_default=defaults["password"],
                    ),
                }
            ),
            errors=errors or {},
            description_placeholders={
                "collector_pn": str(
                    getattr(coordinator, "smartess_collector_pn", "") or not_available
                ),
                "cloud_evidence_path": saved_cloud_evidence_path or not_created_yet,
                "smartess_archive_plan_summary": self._support_archive_cloud_plan_summary(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                    can_refresh_cloud_evidence=can_refresh_cloud_evidence,
                ),
                "refresh_mode_label": self._support_archive_cloud_mode_label(
                    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                ),
            },
        )

    def _smartess_cloud_diagnostics_hint(self) -> str:
        coordinator = self._coordinator()
        if (
            coordinator is None
            or self._collector_is_virtual_bridge()
            or not bool(getattr(coordinator, "smartess_cloud_export_available", False))
        ):
            return ""

        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        cloud_evidence_path = str(values.get("cloud_evidence_path") or "").strip()

        if getattr(coordinator, "smartess_smg_bridge_plan", None) is not None:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_bridge",
                "Current SmartESS cloud evidence is ready to generate a SmartESS SMG bridge for this runtime.",
            )
        elif getattr(coordinator, "smartess_known_family_draft_plan", None) is not None:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_draft",
                "Current SmartESS cloud evidence is ready to generate a SmartESS draft for this runtime.",
            )
        elif cloud_evidence_path:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_refresh",
                "SmartESS cloud evidence is already saved for this entry and can be refreshed after app-side changes.",
            )
        else:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_available",
                "SmartESS cloud evidence is available for this entry even if local detection is already high-confidence.",
            )

        return self._tr(
            "common.dynamic.smartess_cloud_diagnostics_hint",
            "**SmartESS cloud:** {detail} It can still refine local metadata or re-enable bridge-backed entities for an existing device. The visible entity count may stay the same when existing entities are upgraded instead of creating new IDs. **Create support archive** can include saved cloud evidence directly and can refresh it inline before the ZIP is built. Open **Advanced metadata tools** when you need to export the cloud evidence separately or generate drafts from it.",
            {"detail": detail},
        )

    def _localized_local_override_status(
        self,
        details: dict[str, Any],
        *,
        kind: str,
    ) -> str:
        path = str(details.get("path") or "").strip()
        kind_label = self._tr(
            f"common.dynamic.local_override_kind_{kind}",
            kind.replace("_", " "),
        )
        if bool(details.get("exists")) and path:
            return self._tr(
                "common.dynamic.local_override_status_active",
                "Active local override at {path}.",
                {"path": path},
            )
        if path:
            return self._tr(
                "common.dynamic.local_override_status_missing",
                "No active local override. Create {path} to override the built-in {kind}.",
                {"path": path, "kind": kind_label},
            )
        return self._tr(
            "common.dynamic.local_override_status_unavailable",
            "No built-in {kind} is available for this entry.",
            {"kind": kind_label},
        )

    def _localized_local_metadata_status(self, values: dict[str, Any]) -> str:
        raw_status = str(values.get("local_metadata_status") or "").strip()
        if not raw_status:
            return self._tr(
                "common.dynamic.no_diagnostics_action",
                "No diagnostics action has been run yet.",
            )
        translation_key = _LOCAL_METADATA_STATUS_TRANSLATION_KEYS.get(raw_status)
        if translation_key is None:
            return raw_status
        return self._tr(
            f"common.dynamic.local_metadata_status_{translation_key}",
            raw_status,
        )

    def _smartess_cloud_exported_next_step(self) -> str:
        coordinator = self._coordinator()
        if coordinator is not None and getattr(coordinator, "smartess_smg_bridge_plan", None) is not None:
            return self._diagnostics_result_tr(
                "smartess_cloud_evidence_exported_next_bridge",
                "Open Advanced metadata tools to create the SmartESS SMG bridge, then reload local metadata to apply it. If you only need the evidence, create a support archive instead.",
            )
        if coordinator is not None and getattr(coordinator, "smartess_known_family_draft_plan", None) is not None:
            return self._diagnostics_result_tr(
                "smartess_cloud_evidence_exported_next_draft",
                "Open Advanced metadata tools to create the SmartESS draft, then reload local metadata to apply it. If you only need the evidence, create a support archive instead.",
            )
        return self._diagnostics_result_tr(
            "smartess_cloud_evidence_exported_next",
            "Open Advanced metadata tools to review what can be generated from this evidence. If local overrides already exist, reload local metadata there after updating them, or create a support archive to share the evidence with the developer.",
        )

    def _support_action_label(self, action: str) -> str:
        return {
            "create_support_package": self._tr(
                "common.dynamic.action_create_support_package",
                "Create support archive",
            ),
            "reload_local_metadata": self._tr(
                "common.dynamic.action_reload_local_metadata",
                "Reload local metadata",
            ),
            "rollback_local_metadata": self._tr(
                "common.dynamic.action_rollback_local_metadata",
                "Rollback local metadata",
            ),
            "proxy_capture": self._tr(
                "common.dynamic.action_proxy_capture",
                "Collector traffic capture",
            ),
        }.get(action, action)

    def _local_metadata_rollback_paths(self):
        coordinator = self._coordinator()
        return resolve_local_metadata_rollback_paths(
            config_dir=Path(self.hass.config.config_dir),
            profile_name=(getattr(coordinator, "effective_profile_name", "") or None),
            schema_name=(getattr(coordinator, "effective_register_schema_name", "") or None),
            profile_metadata=getattr(coordinator, "effective_profile_metadata", None),
            schema_metadata=getattr(coordinator, "effective_register_schema_metadata", None),
        )

    def _support_workflow_translation_key(self, level: str, field: str) -> str:
        return f"common.dynamic.support_workflow_{level}_{field}"

    def _diagnostics_result_tr(
        self,
        field: str,
        default: str,
        placeholders: dict[str, Any] | None = None,
    ) -> str:
        return self._tr(
            f"common.dynamic.diagnostics_result_{field}",
            default,
            placeholders,
        )

    def _localized_support_workflow(self, values: dict[str, Any]) -> dict[str, str]:
        level = str(values.get("support_workflow_level") or "unknown")
        primary_action = str(values.get("support_workflow_primary_action") or "create_support_package")
        step_1 = self._tr(
            self._support_workflow_translation_key(level, "step_1"),
            str(values.get("support_workflow_step_1") or "Run the primary diagnostics action."),
        )
        step_2 = self._tr(
            self._support_workflow_translation_key(level, "step_2"),
            str(values.get("support_workflow_step_2") or "Send the ZIP file to the developer."),
        )
        step_3 = self._tr(
            self._support_workflow_translation_key(level, "step_3"),
            str(values.get("support_workflow_step_3") or "Use advanced metadata tools only if requested."),
        )
        return {
            "support_workflow_level": level,
            "support_workflow_level_label": self._tr(
                self._support_workflow_translation_key(level, "level_label"),
                str(values.get("support_workflow_level_label") or "Unknown support"),
            ),
            "support_workflow_summary": self._tr(
                self._support_workflow_translation_key(level, "summary"),
                str(values.get("support_workflow_summary") or "Support status is not available yet."),
            ),
            "support_workflow_next_action": self._tr(
                self._support_workflow_translation_key(level, "next_action"),
                str(values.get("support_workflow_next_action") or "Run detection or create a support archive when the inverter is available."),
            ),
            "support_workflow_step_1": step_1,
            "support_workflow_step_2": step_2,
            "support_workflow_step_3": step_3,
            "support_workflow_plan": self._tr(
                "common.dynamic.plan_template",
                "Step 1: {step_1} Step 2: {step_2} Step 3: {step_3}",
                {"step_1": step_1, "step_2": step_2, "step_3": step_3},
            ),
            "support_workflow_advanced_hint": self._tr(
                self._support_workflow_translation_key(level, "advanced_hint"),
                str(values.get("support_workflow_advanced_hint") or "Advanced metadata tools are secondary and should be used only after the primary support path is complete."),
            ),
            "support_workflow_primary_action": primary_action,
            "support_workflow_primary_action_label": self._support_action_label(primary_action),
        }

    def _diagnostics_placeholders(self) -> dict[str, str]:
        coordinator = self._coordinator()
        values = coordinator.data.values if coordinator is not None else {}
        effective_owner_name = coordinator.effective_owner_name if coordinator is not None else ""
        effective_owner_key = coordinator.effective_owner_key if coordinator is not None else ""
        smartess_family_name = coordinator.smartess_family_name if coordinator is not None else ""
        effective_profile_name = coordinator.effective_profile_name if coordinator is not None else ""
        effective_register_schema_name = (
            coordinator.effective_register_schema_name if coordinator is not None else ""
        )
        profile_metadata = coordinator.effective_profile_metadata if coordinator is not None else None
        register_schema_metadata = (
            coordinator.effective_register_schema_metadata if coordinator is not None else None
        )
        config_dir = Path(self.hass.config.config_dir)
        profile_override = local_profile_override_details(
            config_dir,
            effective_profile_name or None,
        )
        schema_override = local_register_schema_override_details(
            config_dir,
            effective_register_schema_name or None,
        )
        placeholders = {
            "model_name": self._config_entry.data.get(
                CONF_DETECTED_MODEL,
                self._tr("common.dynamic.unknown", "Unknown"),
            ),
            "serial_number": self._config_entry.data.get(
                CONF_DETECTED_SERIAL,
                self._tr("common.dynamic.unknown", "Unknown"),
            ),
            "effective_owner_name": effective_owner_name or self._tr("common.dynamic.not_available", "Not available"),
            "effective_owner_key": effective_owner_key or self._tr("common.dynamic.not_available", "Not available"),
            "smartess_family_name": smartess_family_name,
            "smartess_family_line": (
                self._tr(
                    "common.dynamic.smartess_family_line",
                    "\n**SmartESS family:** {family}",
                    {"family": smartess_family_name},
                )
                if smartess_family_name
                else ""
            ),
            "profile_name": effective_profile_name or self._tr("common.dynamic.not_available", "Not available"),
            "register_schema_name": effective_register_schema_name or self._tr("common.dynamic.not_available", "Not available"),
            "support_archive_action_label": self._support_action_label("create_support_package"),
            "effective_profile_source": self._metadata_source_summary(profile_metadata),
            "effective_schema_source": self._metadata_source_summary(register_schema_metadata),
            "profile_override_status": self._localized_local_override_status(
                profile_override,
                kind="profile",
            ),
            "schema_override_status": self._localized_local_override_status(
                schema_override,
                kind="register_schema",
            ),
            "suggested_profile_output": effective_profile_name or self._tr("common.dynamic.not_available", "Not available"),
            "suggested_schema_output": effective_register_schema_name or self._tr("common.dynamic.not_available", "Not available"),
            "support_package_path": str(values.get("support_package_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "support_package_download_url": str(
                values.get("support_package_download_url")
                or values.get("support_package_download_relative_url")
                or ""
            ),
            "support_package_download_markdown": (
                self._download_link_markup(
                    str(
                        values.get("support_package_download_url")
                        or values.get("support_package_download_relative_url")
                        or ""
                    ),
                    label=self._tr(
                        "common.dynamic.download_support_archive_label",
                        "Download support archive",
                    ),
                )
                if values.get("support_package_download_url")
                or values.get("support_package_download_relative_url")
                else self._tr("common.dynamic.not_available_yet", "Not available yet")
            ),
            "cloud_evidence_path": self._current_cloud_evidence_path(coordinator)
            or self._tr("common.dynamic.not_created_yet", "Not created yet"),
            "proxy_capture_status_label": self._localized_proxy_capture_status_label(values),
            "proxy_capture_summary": str(values.get("proxy_capture_summary") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_capture_blocking_reason": self._localized_proxy_capture_blocking_reason(values),
            "proxy_capture_current_endpoint": str(values.get("proxy_capture_current_endpoint") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_capture_target_endpoint": str(values.get("proxy_capture_target_endpoint") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_capture_masked_endpoint": str(values.get("proxy_capture_masked_endpoint") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_capture_redirect_required": (
                self._tr("common.dynamic.yes", "Yes")
                if values.get("proxy_capture_redirect_required")
                else self._tr("common.dynamic.no", "No")
            ),
            "proxy_capture_can_stop": (
                self._tr("common.dynamic.yes", "Yes")
                if values.get("proxy_capture_can_stop")
                else self._tr("common.dynamic.no", "No")
            ),
            "proxy_trace_path": str(values.get("proxy_trace_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "proxy_trace_manifest_path": str(values.get("proxy_trace_saved_result_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "proxy_trace_manifest_download_url": str(values.get("proxy_trace_saved_result_download_url") or ""),
            "proxy_trace_manifest_download_markdown": (
                self._tr(
                    "common.dynamic.download_proxy_capture_result",
                    "[Download saved result]({url})",
                    {"url": values.get("proxy_trace_saved_result_download_url") or ""},
                )
                if values.get("proxy_trace_saved_result_download_url")
                else self._tr("common.dynamic.not_available_yet", "Not available yet")
            ),
            "proxy_capture_saved_result_section": self._proxy_capture_saved_result_section(
                saved_result_download_url=str(
                    values.get("proxy_trace_saved_result_download_url") or ""
                ).strip(),
                status=str(values.get("proxy_capture_status") or ""),
            ),
            "proxy_trace_line_count": str(values.get("proxy_trace_line_count") or 0),
            "proxy_trace_kind_summary": str(values.get("proxy_trace_kind_summary") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_trace_recent_kinds": str(values.get("proxy_trace_recent_kinds") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_trace_recent_events": str(values.get("proxy_trace_recent_events") or ""),
            "proxy_capture_live_log": self._proxy_capture_live_log(values),
            "proxy_capture_user_plan": self._proxy_capture_user_plan(values),
            "proxy_capture_timer_summary": self._proxy_capture_timer_summary(values),
            "proxy_capture_duration_minutes": str(
                _coerce_proxy_capture_duration_minutes(
                    values.get(CONF_PROXY_CAPTURE_DURATION_MINUTES),
                    default=DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
                )
            ),
            "proxy_capture_remaining_minutes": str(
                _coerce_proxy_capture_duration_minutes(
                    values.get("proxy_capture_remaining_minutes"),
                    default=0,
                    minimum=0,
                )
            ),
            "proxy_trace_last_timestamp": str(values.get("proxy_trace_last_timestamp") or self._tr("common.dynamic.not_available", "Not available")),
            "proxy_capture_session_expires_at": self._format_proxy_capture_session_expires_at(
                values.get("proxy_capture_session_expires_at")
            ),
            "proxy_capture_action_result": str(getattr(self, "_proxy_capture_action_result", "") or self._tr("common.dynamic.not_run_yet", "Not run yet")),
            "local_profile_draft_path": str(values.get("local_profile_draft_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "local_schema_draft_path": str(values.get("local_schema_draft_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "local_metadata_status": self._localized_local_metadata_status(values),
            "smartess_cloud_diagnostics_hint": self._smartess_cloud_diagnostics_hint(),
        }
        placeholders.update(self._localized_support_workflow(values))
        return placeholders

    def _localized_proxy_capture_status_label(self, values: dict[str, Any]) -> str:
        status = str(values.get("proxy_capture_status") or "").strip()
        fallback = str(values.get("proxy_capture_status_label") or "").strip()
        if not status and fallback:
            status = fallback.lower()
        return self._tr(
            f"common.dynamic.proxy_capture_status_{status}",
            fallback or self._tr("common.dynamic.not_available", "Not available"),
        )

    def _localized_proxy_capture_blocking_reason(self, values: dict[str, Any]) -> str:
        reason = str(values.get("proxy_capture_blocking_reason") or "").strip()
        if not reason:
            return self._tr("common.dynamic.not_applicable", "Not applicable")
        return self._tr(
            f"common.dynamic.proxy_capture_blocking_{reason}",
            reason,
        )

    def _format_proxy_capture_session_expires_at(self, value: object) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""

        normalized = f"{raw[:-1]}+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return raw

        localized = parsed
        timezone_name = str(
            getattr(getattr(self.hass, "config", None), "time_zone", "") or ""
        ).strip()
        if parsed.tzinfo is not None and timezone_name:
            try:
                localized = parsed.astimezone(ZoneInfo(timezone_name))
            except (ValueError, ZoneInfoNotFoundError):
                localized = parsed

        formatted = localized.strftime("%d.%m.%Y %H:%M")
        if localized.tzinfo is None:
            return formatted

        timezone_label = (localized.tzname() or "").strip()
        if timezone_label in {"+00:00", "UTC+00:00"}:
            timezone_label = "UTC"
        return f"{formatted} {timezone_label}".strip()

    def _proxy_capture_user_plan(self, values: dict[str, Any]) -> str:
        blocking_reason = self._localized_proxy_capture_blocking_reason(values)
        if values.get("proxy_capture_can_stop"):
            expires_at = self._format_proxy_capture_session_expires_at(
                values.get("proxy_capture_session_expires_at")
            )
            remaining = self._format_proxy_capture_remaining_time(
                values.get("proxy_capture_remaining_seconds")
            )
            if expires_at:
                return self._tr(
                    "common.dynamic.proxy_capture_plan_running_with_lease",
                    "Capture is in progress. Refresh live log updates the events shown here. Use Reset proxy timer to extend the current session. Home Assistant will stop the capture and restore the collector connection in {remaining_time}, no later than {expires_at}. When you have enough data, choose Stop capture.",
                    {
                        "expires_at": expires_at,
                        "remaining_time": remaining or expires_at,
                    },
                )
            return self._tr(
                "common.dynamic.proxy_capture_plan_running",
                "Capture is in progress. Leave this page open and use Refresh live log to see new events. Use Reset proxy timer to extend the current session when needed. When you have enough data, choose Stop capture.",
            )
        if str(values.get("proxy_capture_blocking_reason") or "").strip():
            return self._tr(
                "common.dynamic.proxy_capture_plan_blocked",
                "Capture cannot start yet: {reason}",
                {"reason": blocking_reason},
            )
        if str(values.get("proxy_trace_saved_result_download_url") or "").strip() or str(
            values.get("proxy_trace_saved_result_path") or ""
        ).strip():
            return self._tr(
                "common.dynamic.proxy_capture_plan_ready_after_session",
                "The previous capture is complete. Download the saved result below or start a new capture when you need another session.",
            )
        return self._tr(
            "common.dynamic.proxy_capture_plan_start",
            "Start capture will have Home Assistant accept collector traffic on the proxy endpoint and record it here.",
        )

    def _format_proxy_capture_remaining_time(self, value: object) -> str:
        try:
            seconds = max(0, int(float(value)))
        except (TypeError, ValueError):
            return ""
        if seconds <= 0:
            return self._tr("common.dynamic.proxy_capture_remaining_less_than_minute", "less than 1 min")
        minutes = max(1, (seconds + 59) // 60)
        unit = self._tr("common.dynamic.duration_minutes_short", "min")
        return f"{minutes} {unit}"

    def _proxy_capture_timer_summary(self, values: dict[str, Any]) -> str:
        configured_minutes = _coerce_proxy_capture_duration_minutes(
            values.get(CONF_PROXY_CAPTURE_DURATION_MINUTES),
            default=DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
        )
        if values.get("proxy_capture_can_stop"):
            remaining = self._format_proxy_capture_remaining_time(
                values.get("proxy_capture_remaining_seconds")
            )
            expires_at = self._format_proxy_capture_session_expires_at(
                values.get("proxy_capture_session_expires_at")
            )
            if remaining and expires_at:
                return self._tr(
                    "common.dynamic.proxy_capture_timer_running_with_deadline",
                    "Remaining: {remaining_time}. Auto-stop: {expires_at}.",
                    {"remaining_time": remaining, "expires_at": expires_at},
                )
            if remaining:
                return self._tr(
                    "common.dynamic.proxy_capture_timer_running",
                    "Remaining: {remaining_time}.",
                    {"remaining_time": remaining},
                )
        return self._tr(
            "common.dynamic.proxy_capture_timer_configured",
            "Session duration: {duration_minutes} min.",
            {"duration_minutes": configured_minutes},
        )

    def _proxy_capture_saved_result_section(
        self,
        *,
        saved_result_download_url: str,
        status: str,
    ) -> str:
        normalized_status = str(status or "").strip()
        if normalized_status in {"starting", "running", "stopping", "restoring"}:
            return ""
        if not saved_result_download_url:
            return ""
        download_markdown = (
            self._tr(
                "common.dynamic.download_proxy_capture_result",
                "[Download saved result]({url})",
                {"url": saved_result_download_url},
            )
            if saved_result_download_url
            else self._tr("common.dynamic.not_available_yet", "Not available yet")
        )
        return self._tr(
            "common.dynamic.proxy_capture_saved_result_section",
            "**Saved result:** {download}",
            {
                "download": download_markdown,
            },
        )

    def _proxy_capture_live_log(self, values: dict[str, Any]) -> str:
        status = str(values.get("proxy_capture_status") or "").strip()
        if status not in {"starting", "running", "stopping", "restoring"}:
            return self._tr(
                "common.dynamic.proxy_capture_live_log_not_started",
                "The live log is empty. Start capture, then use Refresh live log to show new events here.",
            )
        live_log = str(values.get("proxy_trace_live_log") or "").strip()
        if live_log:
            return live_log
        recent_events = str(values.get("proxy_trace_recent_events") or "").strip()
        if recent_events:
            return recent_events
        return self._tr(
            "common.dynamic.proxy_capture_live_log_waiting",
            "Capture is running. No traffic has reached the log yet. Use Refresh live log after the collector reconnects.",
        )

    async def _async_show_diagnostics_result(
        self,
        *,
        action_title: str,
        status: str,
        path: str = "",
        download_url: str = "",
        next_step: str = "",
    ) -> ConfigFlowResult:
        self._diagnostics_result = {
            "action_title": action_title,
            "status": status,
            "path": path or self._tr("common.dynamic.not_applicable", "Not applicable"),
            "download_url": download_url or "",
            "download_markdown": (
                self._download_link_markup(
                    download_url,
                    label=self._tr(
                        "common.dynamic.download_file_label",
                        "Download file",
                    ),
                )
                if download_url
                else self._tr("common.dynamic.not_available", "Not available")
            ),
            "next_step": next_step
            or self._tr(
                "common.dynamic.return_to_diagnostics",
                "Return to diagnostics to run another action.",
            ),
        }
        return await self.async_step_diagnostics_result()

    def _download_link_markup(self, url: str, *, label: str) -> str:
        """Return a browser download link that HA frontend should not SPA-route."""

        raw_url = str(url or "").strip()
        safe_url = html_escape(raw_url, quote=True)
        safe_label = html_escape(str(label or "").strip() or "Download", quote=False)
        if not raw_url:
            return self._tr("common.dynamic.not_available", "Not available")
        return (
            f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer" '
            f"download>{safe_label}</a>"
        )
