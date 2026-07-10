"""Home Assistant coordinator for the EyeBond Local integration."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import dataclasses
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import logging
import math
from pathlib import Path
import socket
from types import SimpleNamespace
from typing import Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import network
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from ..collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    DEFAULT_COLLECTOR_SERVER_PROTOCOL,
    default_collector_server_port,
    format_collector_server_endpoint_for_cloud_profile,
    format_collector_server_endpoint as format_runtime_collector_server_endpoint,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint as normalize_runtime_collector_server_endpoint,
    parse_collector_server_endpoint as parse_runtime_collector_server_endpoint,
    resolve_collector_server_endpoint as resolve_runtime_collector_server_endpoint,
    home_assistant_callback_endpoint,
)
from ..collector.cloud_family import (
    COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY,
    COLLECTOR_CLOUD_FAMILY_UNKNOWN,
    collector_cloud_family_observation_from_endpoint,
    default_collector_cloud_host,
)
from ..collector.capabilities import (
    CollectorCapabilityProfile,
    collector_capability_profile_from_runtime,
)
from ..collector.transport_profile import (
    collector_cloud_family_from_entry_context,
    resolve_collector_transport_profile,
)
from ..metadata.collector_cloud_profile_catalog_loader import (
    resolve_collector_cloud_provider,
)
from ..const import (
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_TYPE,
    CONF_CONNECTION_MODE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_DISCOVERY_INTERVAL,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_HINT,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    CONF_SERVER_IP,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_COLLECTOR_OPERATION_MODE,
    DEFAULT_CONTROL_MODE,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_MODE,
    DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    CONTROL_MODE_AUTO,
    CONTROL_MODE_FULL,
    CONTROL_MODE_READ_ONLY,
    COLLECTOR_OPERATION_HA_ONLY,
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
    COLLECTOR_OPERATION_MODES,
    DOMAIN,
    DRIVER_HINT_AUTO,
    LOCAL_METADATA_DIR,
    MAX_PROXY_CAPTURE_DURATION_MINUTES,
    MIN_PROXY_CAPTURE_DURATION_MINUTES,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from ..connection.models import build_connection_spec
from ..collector.entity_scope import is_collector_entity_key
from ..control_policy import (
    controls_enabled,
    controls_reason,
    controls_summary,
)
from ..drivers.registry import get_driver
from ..drivers.registry import all_write_capabilities
from ..fixtures.utils import anonymize_fixture_json, build_command_fixture_responses
from ..metadata.effective_metadata import resolve_effective_metadata_selection
from ..metadata.effective_metadata_snapshot import (
    EffectiveMetadataSnapshot,
    build_effective_metadata_snapshot_from_runtime,
    effective_metadata_snapshot_from_dict,
)
from ..metadata.local_metadata import (
    clear_local_metadata_loader_caches,
    create_local_profile_draft,
    create_local_schema_draft,
    rollback_local_metadata_overrides,
)
from ..metadata.profile_loader import builtin_base_profile_name, load_driver_profile
from ..metadata.register_schema_loader import builtin_base_schema_name
from ..naming import collector_display_name
from ..metadata.smartess_draft import (
    SmartEssKnownFamilyDraftPlan,
    create_smartess_known_family_draft,
    resolve_smartess_known_family_draft_plan,
)
from ..metadata.smartess_smg_bridge import (
    SmartEssSmgBridgePlan,
    create_smartess_smg_bridge_draft,
    resolve_smartess_smg_bridge_plan,
)
from ..models import (
    CapabilityPreset,
    DetectedInverter,
    ProbeTarget,
    RuntimeSnapshot,
    WriteCapability,
)
from ..naming import installation_title, legacy_installation_titles
from .factory import create_runtime_manager
from .manager import RuntimeManager
from .poll_policy import poll_policy_for_driver
from .poll_scheduler import PollDecision, PollScheduler, clamp_interval, normalize_poll_mode
from ..schema import (
    build_runtime_ui_schema,
    capability_write_exposure_allowed,
    preset_write_exposure_allowed,
)
from ..support.bundle import build_support_bundle_payload, export_support_bundle
from ..support.cloud_evidence import (
    fetch_and_export_device_bundle_cloud_evidence,
    load_latest_cloud_evidence,
)
from ..support.collector_registry import (
    get_collector_registry_record,
    get_collector_registry_record_by_last_seen_ip,
    remember_collector_original_endpoint,
)
from ..support.proxy_capture import build_proxy_capture_overview
from ..support.proxy_session import (
    build_proxy_capture_command,
    build_proxy_capture_restore_trigger_path,
    build_proxy_capture_trace_path,
    inspect_proxy_capture_start_status,
    inspect_proxy_capture_trace,
    open_proxy_trace_output_file,
    summarize_proxy_capture_trace,
)
from ..support.proxy_trace import (
    build_proxy_capture_lease_deadline,
    build_proxy_capture_session_state,
    build_proxy_trace_manifest,
    clear_proxy_capture_session_state,
    export_proxy_trace_bundle,
    export_proxy_trace_manifest,
    load_latest_proxy_trace_manifest,
    load_proxy_capture_session_state,
    parse_proxy_capture_session_timestamp,
    proxy_capture_restore_guard_reason,
    proxy_capture_session_is_active,
    proxy_capture_session_is_expired,
    refresh_proxy_capture_session_lease,
    publish_proxy_trace_download_copy,
    save_proxy_capture_session_state,
)
from ..support.shadow_learning_backend import (
    build_shadow_learning_preflight,
    build_shadow_learning_seed,
    build_shadow_learning_trace_path,
)
from ..support.shadow_learning_proxy import route_status_indicates_control_ready
from ..support.shadow_learning_session import (
    build_shadow_learning_lease_deadline,
    build_shadow_learning_session_state,
    clear_shadow_learning_session_state,
    load_shadow_learning_session_state,
    save_shadow_learning_session_state,
    shadow_learning_session_is_active,
    shadow_learning_session_is_expired,
    shadow_learning_session_timestamp,
)
from ..support.diagnostic_export import export_diagnostic_run
from ..support.diagnostic_runner import (
    DiagnosticRuntimeContext,
    DiagnosticSingleFlight,
    run_scenario,
)
from ..support.download import sign_support_package_download_url
from ..support.memory_guard import read_available_memory_mib, shadow_learning_memory_blocker
from ..support.package import export_support_package
from ..support.shadow_learning_review_model import normalize_activation_selection
from ..support.workflow import build_support_workflow_state

logger = logging.getLogger(__name__)

_PENDING_COLLECTOR_OPERATION_SYNC_STATUSES: frozenset[str] = frozenset(
    {"applied", "waiting_for_collector", "cooldown"}
)
_HIDDEN_HA_ONLY_COLLECTOR_VALUE_KEYS: frozenset[str] = frozenset(
    {"collector_udp_reply", "collector_udp_reply_from"}
)

_DEFAULT_PROXY_CAPTURE_PORT = 18899
_COLLECTOR_HA_PRIMARY_RECONCILE_COOLDOWN_SECONDS = 300.0
_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY = "effective_metadata_snapshot"
_UNSUPPORTED_COMMANDS_OPTION_KEY = "driver_unsupported_commands"
_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY = "driver_unsupported_commands_version"
_UNSUPPORTED_COMMANDS_OPTION_VERSION = 2
_DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY = "device_scoped_overlay_activation"
_POLL_INTERVAL_MIN_SECONDS = 2
_POLL_INTERVAL_MAX_SECONDS = 3600
_POLL_UTILIZATION_WARNING_RATIO = 0.9
_POLL_OVERRUN_RATIO = 1.0
_POLL_STABLE_STREAK_THRESHOLD = 3
_POLL_RECOMMENDED_TARGET_UTILIZATION = 0.7
_POLL_NOTIFICATION_COOLDOWN_SECONDS = 12 * 60 * 60
_POLL_FIXED_RATE_MIN_DELAY_SECONDS = 1.0
_RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE = "collector_offline"
_RUNTIME_DRIVER_STATE_DRIVER_UNBOUND = "driver_unbound"
_RUNTIME_DRIVER_STATE_DRIVER_BOUND = "driver_bound"
_COLLECTOR_POLL_CONTEXT_COLLECTOR = "collector"
_COLLECTOR_POLL_CONTEXT_DETECTION = "detection"
_COLLECTOR_POLL_CONTEXT_RUNTIME = "runtime"


def _bounded_shadow_learning_artifact_path(
    *,
    config_dir: Path,
    value: object,
    relative_root: Path,
) -> str:
    """Return an existing artifact path only when it stays inside its expected root."""

    normalized = str(value or "").strip()
    if not normalized:
        return ""
    path = Path(normalized)
    if not path.is_absolute():
        return ""
    root = (config_dir / relative_root).resolve()
    candidate = path.resolve()
    if candidate == root or root not in candidate.parents:
        return ""
    if not candidate.exists() or not candidate.is_file():
        return ""
    return str(candidate)
_CONF_COLLECTOR_CLOUD_PROFILE_KEY = "collector_cloud_profile_key"
_CONF_COLLECTOR_CLOUD_PROFILE_LABEL = "collector_cloud_profile_label"
_CONF_COLLECTOR_CLOUD_PROFILE_SOURCE = "collector_cloud_profile_source"
_CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE = "collector_cloud_profile_confidence"

_LOCALIZED_RUNTIME_TEXT: dict[str, dict[str, str]] = {
    "proxy_capture_notification_title": {
        "en": "EyeBond Local Collector Capture",
        "ru": "EyeBond Local: захват трафика коллектора",
        "uk": "EyeBond Local: захоплення трафіку колектора",
    },
    "proxy_capture_notification_body": {
        "en": "Your collector traffic capture is ready.\n\n[Download capture bundle]({download_url})",
        "ru": "Захват трафика коллектора готов.\n\n[Скачать архив захвата]({download_url})",
        "uk": "Захоплення трафіку колектора готове.\n\n[Завантажити архів захоплення]({download_url})",
    },
    "proxy_capture_notification_body_no_link": {
        "en": "Your collector traffic capture is ready.\n\nSaved archive: {saved_path}",
        "ru": "Захват трафика коллектора готов.\n\nСохраненный архив: {saved_path}",
        "uk": "Захоплення трафіку колектора готове.\n\nЗбережений архів: {saved_path}",
    },
    "proxy_capture_restore_unconfirmed_title": {
        "en": "EyeBond Local Collector Restore Needs Attention",
        "ru": "EyeBond Local: проверьте восстановление коллектора",
        "uk": "EyeBond Local: перевірте відновлення колектора",
    },
    "proxy_capture_restore_unconfirmed_body": {
        "en": "The proxy capture stopped, but automatic SmartESS endpoint restore was not confirmed. The collector may still point at Home Assistant. If SmartESS no longer sees the collector, manually set Collector Operation Mode to SmartESS + Home Assistant after the collector reconnects.",
        "ru": "Захват трафика остановлен, но автоматическое восстановление endpoint SmartESS не подтверждено. Коллектор может всё ещё указывать на Home Assistant. Если SmartESS больше не видит коллектор, после его повторного подключения вручную установите Collector Operation Mode в SmartESS + Home Assistant.",
        "uk": "Захоплення трафіку зупинено, але автоматичне відновлення endpoint SmartESS не підтверджено. Колектор може все ще вказувати на Home Assistant. Якщо SmartESS більше не бачить колектор, після повторного підключення колектора вручну встановіть Collector Operation Mode у SmartESS + Home Assistant.",
    },
    "support_archive_notification_title": {
        "en": "EyeBond Local Support Archive",
        "ru": "EyeBond Local: архив поддержки",
        "uk": "EyeBond Local: архів підтримки",
    },
    "support_archive_notification_body": {
        "en": "Your support archive is ready.\n\n[Download support archive]({download_url})",
        "ru": "Архив поддержки готов.\n\n[Скачать архив поддержки]({download_url})",
        "uk": "Архів підтримки готовий.\n\n[Завантажити архів підтримки]({download_url})",
    },
    "poll_interval_high_utilization_title": {
        "en": "EyeBond Local polling interval is tight",
        "ru": "EyeBond Local: интервал опроса близок к пределу",
        "uk": "EyeBond Local: інтервал опитування близький до межі",
    },
    "poll_interval_high_utilization_body": {
        "en": "The device polling cycle is using about {utilization_percent}% of the configured {poll_interval}s interval. If updates are delayed, increase the manual polling interval or switch Sensor refresh mode to Automatic. Recommended minimum for this device is about {recommended_interval}s.",
        "ru": "Цикл опроса устройства использует около {utilization_percent}% настроенного интервала {poll_interval}s. Если обновления задерживаются, увеличьте ручной интервал опроса или переключите режим обновления сенсоров на автоматический. Рекомендуемый минимум для этого устройства — около {recommended_interval}s.",
        "uk": "Цикл опитування пристрою використовує близько {utilization_percent}% налаштованого інтервалу {poll_interval}s. Якщо оновлення затримуються, збільште ручний інтервал опитування або перемкніть режим оновлення сенсорів на автоматичний. Рекомендований мінімум для цього пристрою — близько {recommended_interval}s.",
    },
}


def _runtime_language(hass) -> str:
    language = str(getattr(getattr(hass, "config", None), "language", "en") or "en").lower()
    return language.split("-", 1)[0]


def _localized_runtime_text(hass, key: str, **placeholders: Any) -> str:
    templates = _LOCALIZED_RUNTIME_TEXT.get(key, {})
    template = templates.get(_runtime_language(hass), templates.get("en", ""))
    if not template:
        return ""
    return template.format(**placeholders)


def _proxy_capture_notification_id(entry_id: str, bundle_path: Path | str) -> str:
    stem = Path(str(bundle_path or "capture")).stem or "capture"
    return f"{DOMAIN}_proxy_capture_{entry_id}_{stem}"


def _clamp_poll_interval_seconds(value: object) -> int:
    interval = int(math.ceil(clamp_interval(value)))
    return min(
        _POLL_INTERVAL_MAX_SECONDS,
        max(_POLL_INTERVAL_MIN_SECONDS, interval),
    )


def _poll_recommended_interval_seconds(
    *,
    current_interval: float,
    observed_duration: float,
) -> int:
    """Return a safe minimum poll interval for the observed refresh duration."""

    try:
        duration = max(0.0, float(observed_duration))
    except (TypeError, ValueError):
        duration = 0.0
    try:
        current = max(0.0, float(current_interval))
    except (TypeError, ValueError):
        current = float(DEFAULT_POLL_INTERVAL)
    if duration <= 0.0:
        return _clamp_poll_interval_seconds(current)
    recommended = math.ceil(duration / _POLL_RECOMMENDED_TARGET_UTILIZATION)
    if duration >= current:
        recommended = max(recommended, math.ceil(current) + 1)
    return _clamp_poll_interval_seconds(recommended)


def _runtime_driver_state_from_snapshot(snapshot: RuntimeSnapshot) -> str:
    values = getattr(snapshot, "values", None)
    if isinstance(values, Mapping):
        state = str(values.get("runtime_driver_state") or "").strip()
        if state in {
            _RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE,
            _RUNTIME_DRIVER_STATE_DRIVER_UNBOUND,
            _RUNTIME_DRIVER_STATE_DRIVER_BOUND,
        }:
            return state
    if not bool(getattr(snapshot, "connected", False)):
        return _RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE
    if getattr(snapshot, "inverter", None) is not None:
        return _RUNTIME_DRIVER_STATE_DRIVER_BOUND
    return _RUNTIME_DRIVER_STATE_DRIVER_UNBOUND


def _poll_context_for_runtime_driver_state(runtime_driver_state: str) -> str:
    if runtime_driver_state == _RUNTIME_DRIVER_STATE_DRIVER_BOUND:
        return _COLLECTOR_POLL_CONTEXT_RUNTIME
    if runtime_driver_state == _RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE:
        return _COLLECTOR_POLL_CONTEXT_COLLECTOR
    return _COLLECTOR_POLL_CONTEXT_DETECTION


def _snapshot_reconnect_count(snapshot: object) -> int:
    values = getattr(snapshot, "values", None)
    if not isinstance(values, dict):
        return 0
    try:
        return int(values.get("runtime_reconnect_count", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _is_clean_runtime_poll_cycle(
    *,
    previous_runtime_driver_state: str,
    runtime_driver_state: str,
    previous_reconnect_count: int,
    reconnect_count: int,
) -> bool:
    """Return whether one cycle measured only a steady-state runtime poll.

    A cycle that bound the driver (detection ran inside it) or that recovered
    the collector connection (device came back after being unreachable)
    measures that recovery work, not the device's normal poll cost. Such
    cycles must not feed the scheduler, the high-utilization warning, or the
    poll-duration statistics.
    """

    return (
        runtime_driver_state == _RUNTIME_DRIVER_STATE_DRIVER_BOUND
        and previous_runtime_driver_state == _RUNTIME_DRIVER_STATE_DRIVER_BOUND
        and reconnect_count <= previous_reconnect_count
    )


def _poll_non_runtime_retry_interval_seconds(
    *,
    current_interval: float,
    observed_duration: float,
    decision: PollDecision,
) -> int:
    """Return a temporary auto retry interval for non-runtime poll contexts."""

    current = clamp_interval(
        current_interval,
        minimum=decision.policy_min_interval,
        maximum=decision.policy_max_interval,
    )
    try:
        duration = max(0.0, float(observed_duration))
    except (TypeError, ValueError, OverflowError):
        duration = 0.0
    if not math.isfinite(duration):
        duration = 0.0
    if duration <= 0.0:
        return int(math.ceil(current))
    retry = max(current, math.ceil(duration * 1.3))
    return int(
        math.ceil(
            clamp_interval(
                retry,
                minimum=decision.policy_min_interval,
                maximum=decision.policy_max_interval,
            )
        )
    )


def _format_collector_server_endpoint(
    *,
    server_host: str,
    server_port: int,
    server_protocol: str,
    include_port: bool = True,
    include_protocol: bool = True,
) -> str:
    """Normalize the SmartESS collector parameter 21 endpoint payload."""

    return format_runtime_collector_server_endpoint(
        server_host=server_host,
        server_port=server_port,
        server_protocol=server_protocol,
        include_port=include_port,
        include_protocol=include_protocol,
    )


def _parse_collector_server_endpoint(endpoint: str) -> tuple[str, int, str]:
    """Parse one SmartESS collector endpoint string like host,port,TCP."""

    return parse_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
    )


def _resolve_collector_server_endpoint(
    endpoint: str,
    *,
    cloud_family: str = "",
) -> tuple[str, int, str]:
    """Resolve one collector endpoint into effective host/port/protocol semantics."""

    return resolve_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        cloud_family=cloud_family,
    )


def _collector_server_endpoints_equal(
    left: str,
    right: str,
    *,
    cloud_family: str = "",
) -> bool:
    """Return whether two collector endpoints resolve to the same target."""

    try:
        return _resolve_collector_server_endpoint(
            left,
            cloud_family=cloud_family,
        ) == _resolve_collector_server_endpoint(
            right,
            cloud_family=cloud_family,
        )
    except ValueError:
        return str(left or "").strip() == str(right or "").strip()


def _resolve_shadow_learning_main_redirect(
    *,
    home_assistant_primary: bool,
    current_endpoint: str,
    rollback_target: str,
    upstream_endpoint: str,
    callback_endpoint: str,
) -> tuple[str, bool]:
    """Return (restore_endpoint, redirect_required) for a shadow-learning main-endpoint switch.

    When the collector is already in HA-only mode it is already isolated on the HA endpoint:
    there is nothing to redirect and -- crucially -- nothing to restore. Restoring to the
    real-server rollback target in that case would move an already-isolated collector ONTO the
    real server after the scan (and leave its control entities unavailable). Mirrors proxy
    capture, which also no-ops when already HA-only.

    Otherwise (SmartESS + HA) the persisted main param-21 endpoint is the real server. Drive
    the redirect (and restore target) off the REAL upstream endpoint -- the remembered rollback
    target, else the upstream the proxy forwards to, else the live endpoint -- NOT the
    possibly-already-HA live endpoint (the additive callback can make it look like HA, which
    would skip the switch and leave the collector live on the real server). This moves the main
    endpoint to the proxy for the whole scan and restores it to the real server afterwards.
    """

    if home_assistant_primary:
        return "", False
    restore_endpoint = str(
        (rollback_target or "").strip()
        or (upstream_endpoint or "").strip()
        or (current_endpoint or "").strip()
    ).strip()
    redirect_required = bool(
        restore_endpoint and restore_endpoint != str(callback_endpoint or "").strip()
    )
    return restore_endpoint, redirect_required


def _normalize_preserved_collector_server_endpoint(endpoint: str) -> str:
    """Normalize one callback endpoint while keeping its compact raw shape."""

    return normalize_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        preserve_shape=True,
    )


def _known_collector_cloud_family(value: object) -> str:
    """Return a concrete collector cloud family, ignoring unknown placeholders."""

    family = str(value or "").strip()
    if family in {"", COLLECTOR_CLOUD_FAMILY_UNKNOWN}:
        return ""
    return family


def _known_collector_cloud_profile_value(value: object) -> str:
    """Return one normalized non-empty collector cloud profile metadata value."""

    return str(value or "").strip()


def _package_dir() -> Path:
    """Return the installed integration package directory."""

    return Path(__file__).resolve().parents[1]


def _read_package_json(filename: str) -> dict[str, Any]:
    """Read one package JSON file without letting diagnostics fail startup."""

    try:
        payload = json.loads((_package_dir() / filename).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_build_info_file() -> dict[str, str]:
    """Read BUILD_INFO.txt embedded by the manual archive builder, if present."""

    path = _package_dir() / "BUILD_INFO.txt"
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return {"build_info_present": "false"}

    result: dict[str, str] = {"build_info_present": "true"}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip().lower().replace(" ", "_")
        if normalized_key:
            result[normalized_key] = value.strip()
    return result


def _integration_build_runtime_values() -> dict[str, object]:
    """Return support-facing package/build diagnostics for the loaded Python code."""

    manifest = _read_package_json("manifest.json")
    build_info = _read_build_info_file()
    values: dict[str, object] = {
        "integration_package_dir": str(_package_dir()),
        "integration_manifest_version": str(manifest.get("version") or ""),
        "integration_build_info_present": build_info.get("build_info_present") == "true",
    }
    for key in ("git_describe", "git_commit", "commit_date", "built_at"):
        value = str(build_info.get(key) or "").strip()
        if value:
            values[f"integration_build_{key}"] = value
    return values


def _collector_cloud_family_from_endpoint_shape(endpoint: object) -> str:
    """Infer a callback family from endpoint syntax when stronger evidence is absent."""

    observation = collector_cloud_family_observation_from_endpoint(endpoint)
    family = _known_collector_cloud_family(observation.family)
    if family:
        return family

    try:
        parsed = inspect_collector_server_endpoint(
            str(endpoint or ""),
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return ""

    if not parsed.has_explicit_port:
        return COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY
    return ""


def _collector_original_endpoint_source_options(
    *,
    endpoint: str,
    profile_key: str,
    source: str,
    observed_at: str | None = None,
) -> dict[str, str]:
    """Return option metadata for one preserved original cloud endpoint."""

    normalized_endpoint = str(endpoint or "").strip()
    if not normalized_endpoint:
        return {}

    normalized_profile_key = str(profile_key or "").strip().lower()
    normalized_source = str(source or "").strip() or "runtime_observed"
    timestamp = str(observed_at or "").strip() or datetime.now(timezone.utc).isoformat()
    return {
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: normalized_endpoint,
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: normalized_profile_key,
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: normalized_source,
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT: timestamp,
    }


def _format_home_assistant_collector_endpoint(
    *,
    server_host: str,
    template_endpoint: str = "",
    cloud_family: str = "",
) -> str:
    """Build a proxy-capture endpoint mirroring the template's port shape.

    Deliberately keeps the template port: the proxy-capture listener mirrors
    the cloud port. The HA CALLBACK target must never use this — it goes
    through collector_endpoint.home_assistant_callback_endpoint, which pins
    the entry's listener port.
    """

    server_port = default_collector_server_port(cloud_family=cloud_family)
    server_protocol = DEFAULT_COLLECTOR_SERVER_PROTOCOL
    if template_endpoint:
        try:
            _host, server_port, server_protocol = _resolve_collector_server_endpoint(
                template_endpoint,
                cloud_family=cloud_family,
            )
        except ValueError:
            server_port = DEFAULT_COLLECTOR_SERVER_PORT
            server_protocol = DEFAULT_COLLECTOR_SERVER_PROTOCOL
    return format_collector_server_endpoint_for_cloud_profile(
        server_host=server_host,
        cloud_family=cloud_family,
        server_port=server_port,
        server_protocol=server_protocol,
        template_endpoint=template_endpoint,
        require_tcp=True,
    )


def _default_cloud_upstream_endpoint(
    *,
    cloud_family: str,
    template_endpoint: str = "",
) -> str:
    """Build a family-default upstream callback endpoint when the original one is unknown."""

    normalized_family = str(cloud_family or "").strip().lower()
    default_host = default_collector_cloud_host(normalized_family)
    if not default_host:
        return ""

    return format_collector_server_endpoint_for_cloud_profile(
        server_host=default_host,
        cloud_family=normalized_family,
        server_port=None,
        server_protocol=DEFAULT_COLLECTOR_SERVER_PROTOCOL,
        template_endpoint=template_endpoint,
        require_tcp=True,
    )


def _private_ipv4_host(host: str) -> ipaddress.IPv4Address | None:
    try:
        address = ipaddress.ip_address(str(host or "").strip())
    except ValueError:
        return None
    if address.version != 4 or not address.is_private:
        return None
    return address


def _same_ipv4_24(left: str, right: str) -> bool:
    left_address = _private_ipv4_host(left)
    right_address = _private_ipv4_host(right)
    if left_address is None or right_address is None:
        return False
    return ipaddress.ip_network(f"{left_address}/24", strict=False) == ipaddress.ip_network(
        f"{right_address}/24",
        strict=False,
    )


def _coerce_proxy_capture_duration_minutes(value: object) -> int:
    try:
        minutes = int(round(float(value)))
    except (TypeError, ValueError):
        minutes = DEFAULT_PROXY_CAPTURE_DURATION_MINUTES
    return max(
        MIN_PROXY_CAPTURE_DURATION_MINUTES,
        min(MAX_PROXY_CAPTURE_DURATION_MINUTES, minutes),
    )


def _proxy_capture_remaining_seconds(expires_at: object) -> int:
    deadline = parse_proxy_capture_session_timestamp(str(expires_at or ""))
    if deadline is None:
        return 0
    return max(0, int((deadline - datetime.now(timezone.utc)).total_seconds()))


_PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS = (
    "proxy_capture_session_status",
    "proxy_capture_session_started_at",
    "proxy_capture_session_expires_at",
    "proxy_capture_session_anonymized",
    "proxy_capture_remaining_seconds",
    "proxy_capture_remaining_minutes",
)


class EybondLocalCoordinator(DataUpdateCoordinator[RuntimeSnapshot]):
    """Owns the hub and exposes its snapshots to Home Assistant entities."""

    config_entry: ConfigEntry

    def __init__(self, hass, entry: ConfigEntry) -> None:
        self.config_entry = entry
        connection_spec = build_connection_spec(entry.data, entry.options)
        self._connection_spec = connection_spec
        self._runtime: RuntimeManager = create_runtime_manager(
            connection_spec,
            driver_hint=entry.options.get(CONF_DRIVER_HINT, entry.data.get(CONF_DRIVER_HINT, "auto")),
            connection_mode=entry.data.get(CONF_CONNECTION_MODE, ""),
        )
        # The runtime inverter is built from built-in detection and never carries the
        # learned overlay capabilities on its own. Give the runtime a hook so that, once
        # a device-scoped overlay is active, the activated learned controls are merged
        # into the detected inverter -- otherwise they exist only in effective metadata
        # and never become entities (or writable) because every entity/write path reads
        # the runtime inverter's capabilities.
        self._device_overlay_merge_status = ""
        set_overlay_applier = getattr(self._runtime, "set_inverter_overlay_applier", None)
        if callable(set_overlay_applier):
            set_overlay_applier(self._apply_device_overlay_to_inverter)
        set_snapshot_observer = getattr(
            self._runtime,
            "set_runtime_snapshot_observer",
            None,
        )
        if callable(set_snapshot_observer):
            set_snapshot_observer(self._publish_runtime_intermediate_snapshot)
        set_connection_watcher = getattr(
            self._runtime,
            "set_collector_connection_watcher",
            None,
        )
        if callable(set_connection_watcher):
            set_connection_watcher(self._on_collector_connection_established)
        persisted_unsupported = entry.options.get(_UNSUPPORTED_COMMANDS_OPTION_KEY)
        persisted_unsupported_version = entry.options.get(
            _UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY
        )
        set_unsupported = getattr(
            self._runtime,
            "set_persistent_unsupported_commands",
            None,
        )
        if (
            callable(set_unsupported)
            and persisted_unsupported_version == _UNSUPPORTED_COMMANDS_OPTION_VERSION
            and isinstance(persisted_unsupported, (list, tuple))
        ):
            set_unsupported(tuple(persisted_unsupported))
        super().__init__(
            hass,
            logger,
            name=DOMAIN,
            update_interval=timedelta(
                seconds=entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            ),
        )
        self.data = RuntimeSnapshot()
        self._remembered_collector_server_endpoint = str(
            entry.options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT) or ""
        ).strip()
        self._last_synced_device_meta: tuple[str, str, str, str, str] = (
            "",
            "",
            "",
            "",
            "",
        )
        self._last_synced_collector_device_meta: tuple[str, str, str, str, str] = (
            "",
            "",
            "",
            "",
            "",
        )
        self._tooling_values: dict[str, Any] = {}
        self._cached_smartess_cloud_evidence_record = None
        self._cached_smartess_cloud_evidence_warmed = False
        self._cached_effective_metadata = None
        self._cached_proxy_capture_session_state = None
        self._cached_shadow_learning_session_state = None
        # Once True, _cached_shadow_learning_session_state is authoritative and
        # the per-refresh disk read is skipped. The save/clear paths keep the
        # cache in sync (this coordinator is the only writer of the file), so the
        # steady-state cost is zero when learning is never used.
        self._shadow_learning_session_state_loaded = False
        self._proxy_trace_download_manifest_path = ""
        self._proxy_trace_download_details: tuple[str, str] = ("", "")
        self._proxy_capture_deadline_refresh_handle = None
        self._suppress_entry_reload_count = 0
        if (
            persisted_unsupported is not None
            and persisted_unsupported_version != _UNSUPPORTED_COMMANDS_OPTION_VERSION
        ):
            options = dict(self.config_entry.options)
            options.pop(_UNSUPPORTED_COMMANDS_OPTION_KEY, None)
            options.pop(_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY, None)
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                options=options,
            )
            logger.info(
                "Discarded stale unsupported inverter command cache for this device; "
                "commands will be rechecked on the current transport."
            )
        self._ha_primary_reconcile_last_signature: tuple[str, str] = ("", "")
        self._ha_primary_reconcile_last_attempt_monotonic = 0.0
        self._collector_operation_pending_target_endpoint = ""
        self._entity_platforms_initialized = False
        self._entity_platform_reload_requested = False
        self._entity_platforms_loaded_with_inverter_identity = False
        self._entity_platforms_loaded_with_driver_fallback = False
        self._platform_loaded_effective_metadata_signature: tuple[str, str, str] = (
            "",
            "",
            "",
        )
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False
        # Diagnostic command runner: at most one scenario per config entry, with
        # normal polling quiesced while it holds the shared transport.
        self._diagnostic_active = False
        self._diagnostic_flight = DiagnosticSingleFlight()
        self._support_package_active = False
        self._support_package_flight = DiagnosticSingleFlight(
            busy_error="support_package_export_in_progress"
        )
        self._runtime_operation_lock = asyncio.Lock()
        self._poll_duration_ewma_seconds = 0.0
        self._poll_duration_max_seconds = 0.0
        self._poll_recent_durations_seconds: list[float] = []
        self._poll_last_cycle_started_monotonic = 0.0
        self._collector_poll_overrun_streak = 0
        self._collector_poll_high_utilization_streak = 0
        self._poll_normal_utilization_streak = 0
        self._poll_notification_active = False
        self._poll_last_notification_monotonic = 0.0
        self._poll_non_runtime_retry_interval_seconds = 0
        self._poll_scheduler_driver_key = str(
            entry.options.get(CONF_DRIVER_HINT, entry.data.get(CONF_DRIVER_HINT, "auto"))
            or "auto"
        )
        self._poll_scheduler = PollScheduler(
            policy=poll_policy_for_driver(self._poll_scheduler_driver_key),
            mode=self._configured_poll_mode(),
            manual_interval=self._configured_poll_interval_seconds(),
        )

    def prime_startup_snapshot(self) -> bool:
        """Seed coordinator data from persisted entry metadata without network I/O.

        Home Assistant setup should not wait for a full inverter detection pass
        just to create collector/runtime entities. This lightweight snapshot
        provides stable collector identity and an explicit detection-pending
        state; the ordinary background refresh will replace it with live data.
        """

        existing_snapshot = self.data if isinstance(self.data, RuntimeSnapshot) else None
        inverter = self._prime_startup_inverter_from_persisted_metadata()
        if (
            existing_snapshot is not None
            and existing_snapshot.values
            and (existing_snapshot.inverter is not None or inverter is None)
        ):
            return False

        connection = self._connection_spec
        collector = SimpleNamespace(
            remote_ip=str(getattr(connection, "collector_ip", "") or ""),
            collector_pn=str(getattr(connection, "collector_pn", "") or ""),
            profile_name="",
            smartess_protocol_name="",
            smartess_protocol_asset_name="",
            smartess_collector_version="",
            collector_cloud_family=str(
                getattr(connection, "collector_cloud_family", "") or ""
            ),
            collector_virtual_bridge=False,
            collector_bridge_version="",
        )
        values: dict[str, object] = dict(getattr(existing_snapshot, "values", {}) or {})
        values.update({
            "connection_type": self.config_entry.data.get(
                CONF_CONNECTION_TYPE,
                "eybond",
            ),
            "collector_operation_mode": self.collector_operation_mode,
            "control_mode": self.control_mode,
            "detection_confidence": self.detection_confidence,
            "runtime_driver_state": _RUNTIME_DRIVER_STATE_DRIVER_UNBOUND,
            "runtime_detection_status": "detecting_inverter",
            "collector_poll_context": _COLLECTOR_POLL_CONTEXT_DETECTION,
            "collector_poll_mode": self._configured_poll_mode(),
            "collector_poll_current_interval_seconds": self._configured_poll_interval_seconds(),
            "collector_poll_interval_configured_seconds": self._configured_poll_interval_seconds(),
            "collector_poll_manual_interval_seconds": self._configured_poll_interval_seconds(),
            "collector_poll_duration_ms": 0,
            "collector_poll_utilization_percent": 0,
            "collector_poll_recommended_min_interval_seconds": self._configured_poll_interval_seconds(),
            "last_error": "startup_detection_pending",
        })
        connection_mode = self.config_entry.data.get(CONF_CONNECTION_MODE, "")
        if connection_mode:
            values["connection_mode"] = connection_mode
        if collector.remote_ip:
            values["collector_remote_ip"] = collector.remote_ip
            values["configured_collector_ip"] = collector.remote_ip
        if collector.collector_pn:
            values["collector_pn"] = collector.collector_pn
        if collector.collector_cloud_family:
            values["collector_cloud_family"] = collector.collector_cloud_family

        endpoint = str(
            self.config_entry.options.get(
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
                self.config_entry.data.get("collector_server_endpoint", ""),
            )
            or ""
        ).strip()
        if not endpoint and getattr(connection, "server_ip", ""):
            endpoint = format_runtime_collector_server_endpoint(
                server_host=getattr(connection, "effective_advertised_server_ip", "")
                or getattr(connection, "server_ip", ""),
                server_port=getattr(connection, "effective_advertised_tcp_port", 0)
                or getattr(connection, "tcp_port", DEFAULT_COLLECTOR_SERVER_PORT),
                server_protocol=DEFAULT_COLLECTOR_SERVER_PROTOCOL,
            )
        if endpoint:
            values["collector_server_endpoint"] = endpoint

        snapshot = RuntimeSnapshot(
            connected=True,
            collector=collector,
            inverter=inverter,
            values=values,
        )
        try:
            snapshot.last_error = "startup_detection_pending"
        except Exception:
            pass
        self.data = snapshot
        self._cached_effective_metadata = None
        return True

    def _prime_startup_inverter_from_persisted_metadata(self) -> DetectedInverter | None:
        """Build a lightweight inverter identity from persisted metadata, if available.

        Entity platforms are constructed once during setup. When startup uses a
        collector-only primed snapshot, writable capability entities would be
        skipped until a later entry reload. If the entry already carries a
        confirmed inverter identity/effective metadata from an earlier runtime
        detection, expose that metadata immediately without waiting for network I/O.
        The live refresh replaces this lightweight object with the real probe
        result.
        """

        detected_model = str(
            self.config_entry.data.get(CONF_DETECTED_MODEL) or ""
        ).strip()
        detected_serial = str(
            self.config_entry.data.get(CONF_DETECTED_SERIAL) or ""
        ).strip()
        if not (detected_model or detected_serial):
            return None

        snapshot = self.effective_metadata_snapshot
        profile_name = str(getattr(snapshot, "profile_name", "") or "").strip()
        register_schema_name = str(
            getattr(snapshot, "register_schema_name", "") or ""
        ).strip()
        variant_key = str(getattr(snapshot, "variant_key", "") or "default").strip()

        driver_key = str(
            self.config_entry.options.get(
                CONF_DRIVER_HINT,
                self.config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            or ""
        ).strip()
        driver = None
        if driver_key and driver_key != DRIVER_HINT_AUTO:
            try:
                driver = get_driver(driver_key)
            except KeyError:
                driver = None
        if driver is None and profile_name:
            try:
                profile = load_driver_profile(profile_name)
            except Exception:
                profile = None
            if profile is not None:
                driver_key = str(getattr(profile, "driver_key", "") or "").strip()
                try:
                    driver = get_driver(driver_key) if driver_key else None
                except KeyError:
                    driver = None
        if driver is None:
            return None

        if not profile_name:
            profile_name = str(getattr(driver, "profile_name", "") or "").strip()
        if not register_schema_name:
            register_schema_name = str(
                getattr(driver, "register_schema_name", "") or ""
            ).strip()

        try:
            profile = load_driver_profile(profile_name)
        except Exception:
            return None

        probe_targets = tuple(getattr(driver, "probe_targets", ()) or ())
        probe_target = (
            probe_targets[0]
            if probe_targets
            else ProbeTarget(devcode=0, collector_addr=0, device_addr=0)
        )
        return DetectedInverter(
            driver_key=str(getattr(driver, "key", "") or driver_key),
            protocol_family=str(
                getattr(profile, "protocol_family", "")
                or getattr(driver, "key", "")
                or driver_key
            ),
            model_name=detected_model,
            serial_number=detected_serial,
            probe_target=probe_target,
            variant_key=variant_key or "default",
            details={
                "runtime_detection_status": "startup_persisted_identity",
                "detection_confidence": self.detection_confidence,
            },
            profile_name=profile_name,
            register_schema_name=register_schema_name,
            capability_groups=tuple(getattr(profile, "groups", ()) or ()),
            capabilities=tuple(getattr(profile, "capabilities", ()) or ()),
            capability_presets=tuple(getattr(profile, "presets", ()) or ()),
        )

    @property
    def proxy_capture_configured_duration_minutes(self) -> int:
        """Return the configured proxy capture duration in minutes."""

        return _coerce_proxy_capture_duration_minutes(
            self.config_entry.options.get(
                CONF_PROXY_CAPTURE_DURATION_MINUTES,
                self.config_entry.data.get(CONF_PROXY_CAPTURE_DURATION_MINUTES),
            )
        )

    @property
    def proxy_capture_remaining_seconds(self) -> int:
        """Return the last published active proxy capture remaining time."""

        values = self._proxy_capture_runtime_values()
        try:
            return max(0, int(float(values.get("proxy_capture_remaining_seconds") or 0)))
        except (TypeError, ValueError):
            return 0

    @property
    def proxy_capture_remaining_minutes(self) -> int:
        """Return remaining proxy capture minutes rounded up for UI controls."""

        seconds = self.proxy_capture_remaining_seconds
        if seconds <= 0:
            return 0
        return max(1, (seconds + 59) // 60)

    @property
    def proxy_capture_display_duration_minutes(self) -> int:
        """Return the number shown by runtime/UI controls."""

        if self.proxy_capture_overview.can_stop and self.proxy_capture_remaining_minutes > 0:
            return _coerce_proxy_capture_duration_minutes(self.proxy_capture_remaining_minutes)
        return self.proxy_capture_configured_duration_minutes

    def proxy_capture_duration_availability_reason(self) -> str | None:
        """Return why the proxy timer setting is temporarily unavailable."""

        overview = self.proxy_capture_overview
        if overview.critical_phase:
            return "proxy_capture_critical_phase"
        if overview.can_start or overview.can_stop:
            return None
        return str(overview.blocking_reason or "proxy_capture_not_ready")

    def _raise_if_high_level_collector_actions_disabled(self) -> None:
        """Reject high-level collector actions when the current write policy blocks them."""

        if not self.collector_actions_enabled:
            raise PermissionError(
                f"collector_control_disabled:{self.control_mode}:{self.controls_reason}"
            )

        lock_code = self.collector_configuration_lock_code()
        if lock_code is not None:
            raise RuntimeError(lock_code)

    def collector_operation_mode_apply_lock_code(self) -> str | None:
        """Return one lock code while the collector is still applying a mode change."""

        sync_status = str(
            self.data.values.get("collector_operation_endpoint_sync_status") or ""
        ).strip()
        if sync_status in _PENDING_COLLECTOR_OPERATION_SYNC_STATUSES:
            return "collector_operation_mode_apply_pending"
        return None

    def collector_operation_mode_apply_lock_reason(self) -> str | None:
        """Return a user-facing reason while the collector is still applying a mode change."""

        if self.collector_operation_mode_apply_lock_code() is None:
            return None
        return (
            "Collector is applying the new operation mode. "
            "Wait for the collector to restart and reconnect."
        )

    def collector_configuration_lock_code(self) -> str | None:
        """Return one lock code while collector callback actions must stay blocked."""

        overview = self.proxy_capture_overview
        overview_status = str(getattr(overview, "status", "") or "").strip()
        if overview_status in {"starting", "stopping", "restoring"}:
            return "collector_configuration_proxy_transition_active"
        if overview_status == "running":
            return "collector_configuration_proxy_session_active"
        return self.collector_operation_mode_apply_lock_code()

    def collector_configuration_lock_reason(self) -> str | None:
        """Return a user-facing reason while collector callback actions must stay blocked."""

        lock_code = self.collector_configuration_lock_code()
        if lock_code == "collector_configuration_proxy_transition_active":
            return (
                "Proxy capture is changing the collector callback. "
                "Wait for the transition to finish."
            )
        if lock_code == "collector_configuration_proxy_session_active":
            return "Stop proxy capture before changing collector callback actions."
        if lock_code == "collector_operation_mode_apply_pending":
            return self.collector_operation_mode_apply_lock_reason()
        return None

    async def async_setup(self) -> None:
        """Start the underlying hub."""

        self._configure_reverse_discovery_mode()
        await self._runtime.async_start()
        if self.collector_home_assistant_primary:
            await self._async_prepare_home_assistant_callback_listener(
                self.collector_callback_target_endpoint
            )
        await self._async_recover_proxy_capture_state()
        await self._async_recover_shadow_learning_state()
        await self._async_warm_smartess_cloud_evidence_cache()
        await self._async_warm_effective_metadata_cache()

    async def async_shutdown(self) -> None:
        """Stop the underlying hub."""

        async with self._shutdown_lock:
            if self._shutdown_complete:
                return
            self._shutdown_complete = True
            await self._async_cancel_diagnostic_run()
            await self._support_package_flight.cancel()
            self._cancel_proxy_capture_deadline_refresh()
            set_snapshot_observer = getattr(
                self._runtime,
                "set_runtime_snapshot_observer",
                None,
            )
            if callable(set_snapshot_observer):
                set_snapshot_observer(None)
            set_overlay_applier = getattr(
                self._runtime,
                "set_inverter_overlay_applier",
                None,
            )
            if callable(set_overlay_applier):
                set_overlay_applier(None)
            set_connection_watcher = getattr(
                self._runtime,
                "set_collector_connection_watcher",
                None,
            )
            if callable(set_connection_watcher):
                set_connection_watcher(None)
            try:
                await self.async_stop_shadow_learning(
                    reason="shutdown",
                    request_refresh=False,
                    raise_when_not_running=False,
                )
            except Exception as exc:
                logger.warning(
                    "Shadow learning shutdown cleanup failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
            await self._async_stop_proxy_capture_process(force=True)
            await self._runtime.async_stop()
        # Base-class teardown (debouncer shutdown, unschedule refresh) must
        # run too, or a queued request_refresh can still drive a poll against
        # the stopped link.
        await super().async_shutdown()

    async def _async_cancel_diagnostic_run(self) -> None:
        """Cancel any in-flight diagnostic command run (called on unload)."""

        await self._diagnostic_flight.cancel()

    async def async_run_diagnostic_commands(
        self,
        *,
        commands: str,
        stop_on_error: bool = True,
        operation_timeout: float | None = None,
        integration_version: str = "",
        confirm_write: bool = False,
        publish_download_copy: bool = False,
    ) -> dict:
        """Run one diagnostic command scenario against the shared collector link.

        Only one scenario runs per config entry at a time; normal polling is
        quiesced while the run holds the transport. Permanent config-entry
        settings (driver hint, probe target, detection snapshot) are never
        modified. Scenarios that write to the device require ``confirm_write``.
        """

        async def _factory() -> dict:
            context = self._build_diagnostic_context(
                stop_on_error=stop_on_error,
                operation_timeout=operation_timeout,
                integration_version=integration_version,
                confirm_write=confirm_write,
            )
            return await self._async_execute_diagnostic(
                commands,
                context,
                publish_download_copy=publish_download_copy,
            )

        return await self._diagnostic_flight.run(
            _factory,
            on_start=self._mark_diagnostic_active,
            on_finish=self._mark_diagnostic_idle,
        )

    def _mark_diagnostic_active(self) -> None:
        self._diagnostic_active = True

    def _mark_diagnostic_idle(self) -> None:
        self._diagnostic_active = False

    @property
    def support_package_export_running(self) -> bool:
        """Return whether this entry is currently building a support archive."""

        return self._support_package_active or self._support_package_flight.running

    def _mark_support_package_active(self) -> None:
        self._support_package_active = True
        self._publish_tooling_values(
            support_package_export_running=True,
            support_package_export_status="running",
            local_metadata_status="Support archive export running",
        )

    def _mark_support_package_idle(self) -> None:
        self._support_package_active = False
        self._publish_tooling_values(
            support_package_export_running=False,
            support_package_export_status="idle",
        )

    def _build_diagnostic_context(
        self,
        *,
        stop_on_error: bool,
        operation_timeout: float | None,
        integration_version: str,
        confirm_write: bool = False,
    ) -> DiagnosticRuntimeContext:
        snapshot = self.data
        inverter = snapshot.inverter if snapshot is not None else None
        transport = self._diagnostic_link_transport()
        driver_hint = self.config_entry.options.get(
            CONF_DRIVER_HINT,
            self.config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
        )

        def _is_connected() -> bool:
            return bool(transport is not None and getattr(transport, "connected", False))

        return DiagnosticRuntimeContext(
            transport=transport,
            active_driver_key=inverter.driver_key if inverter is not None else None,
            active_probe_target=inverter.probe_target if inverter is not None else None,
            configured_driver_hint=driver_hint,
            driver_default_probe_target=self._diagnostic_default_probe_target,
            is_connected=_is_connected,
            entry_id=self.config_entry.entry_id,
            integration_version=integration_version,
            catalog_detection=self._diagnostic_catalog_detection(),
            runtime_debug=self._diagnostic_runtime_debug(transport),
            default_stop_on_error=stop_on_error,
            default_operation_timeout=operation_timeout,
            confirm_write=confirm_write,
        )

    def _diagnostic_link_transport(self):
        accessor = getattr(self._runtime, "diagnostic_link_transport", None)
        if callable(accessor):
            return accessor()
        return None

    def _diagnostic_runtime_debug(self, transport: object) -> dict[str, object]:
        """Return transport internals needed to diagnose raw command routing."""

        debug: dict[str, object] = {
            "transport_type": type(transport).__name__ if transport is not None else "",
            "transport_id": id(transport) if transport is not None else 0,
            "transport_connected": bool(getattr(transport, "connected", False)),
        }
        try:
            collector = getattr(transport, "collector_info", None)
            if collector is not None:
                debug.update(
                    {
                        "collector_remote_ip": getattr(collector, "remote_ip", "") or "",
                        "collector_remote_port": getattr(collector, "remote_port", None),
                        "collector_pn_present": bool(
                            str(getattr(collector, "collector_pn", "") or "").strip()
                        ),
                        "raw_request_count": getattr(collector, "raw_request_count", 0),
                        "raw_response_count": getattr(collector, "raw_response_count", 0),
                        "raw_timeout_count": getattr(collector, "raw_timeout_count", 0),
                        "raw_unhandled_line_count": getattr(
                            collector,
                            "raw_unhandled_line_count",
                            0,
                        ),
                        "raw_last_spacing_wait_ms": getattr(
                            collector,
                            "raw_last_spacing_wait_ms",
                            0,
                        ),
                        "raw_last_response_duration_ms": getattr(
                            collector,
                            "raw_last_response_duration_ms",
                            0,
                        ),
                        "raw_last_total_duration_ms": getattr(
                            collector,
                            "raw_last_total_duration_ms",
                            0,
                        ),
                        "raw_last_request_ascii": getattr(
                            collector,
                            "raw_last_request_ascii",
                            "",
                        )
                        or "",
                        "raw_last_response_ascii": getattr(
                            collector,
                            "raw_last_response_ascii",
                            "",
                        )
                        or "",
                        "raw_last_timeout_request_ascii": getattr(
                            collector,
                            "raw_last_timeout_request_ascii",
                            "",
                        )
                        or "",
                        "raw_last_parser": getattr(collector, "raw_last_parser", "")
                        or "",
                        "raw_last_frame_format": getattr(
                            collector,
                            "raw_last_frame_format",
                            "",
                        )
                        or "",
                    }
                )
        except Exception as exc:  # noqa: BLE001 - diagnostics must not block scenario
            debug["collector_info_error"] = str(exc)

        try:
            connection_getter = getattr(transport, "_at_connection", None)
            if callable(connection_getter):
                connection = connection_getter(create_placeholder=False)
                debug["at_connection_id"] = id(connection) if connection is not None else 0
                debug["at_connection_connected"] = bool(
                    getattr(connection, "connected", False)
                )
                if connection is not None:
                    reader_task = getattr(connection, "_reader_task", None)
                    writer = getattr(connection, "_writer", None)
                    pending_raw = getattr(connection, "_pending_raw_response", None)
                    debug.update(
                        {
                            "at_reader_task_done": bool(
                                reader_task is not None and reader_task.done()
                            ),
                            "at_writer_closing": bool(
                                writer is not None and writer.is_closing()
                            ),
                            "at_pending_raw_present": pending_raw is not None,
                            "at_pending_raw_done": bool(
                                pending_raw is not None and pending_raw.done()
                            ),
                            "at_raw_frame_format": getattr(
                                connection,
                                "_raw_passthrough_frame_format",
                                "",
                            )
                            or "",
                        }
                    )
        except Exception as exc:  # noqa: BLE001 - diagnostics must not block scenario
            debug["connection_debug_error"] = str(exc)
        return debug

    @staticmethod
    def _diagnostic_default_probe_target(driver_key: str):
        try:
            from ..drivers.registry import get_driver

            driver = get_driver(driver_key)
        except KeyError:
            return None
        targets = getattr(driver, "probe_targets", ())
        return targets[0] if targets else None

    def _diagnostic_catalog_detection(self) -> dict:
        try:
            snapshot = self.effective_metadata_snapshot
            if snapshot is None:
                return {}
            return {
                "candidate_keys": list(getattr(snapshot, "candidate_keys", ()) or ()),
                "surface_key": getattr(snapshot, "surface_key", "") or "",
                "evidence_fingerprint": getattr(snapshot, "evidence_fingerprint", "")
                or "",
            }
        except Exception:  # noqa: BLE001 - diagnostic context must never block a run
            return {}

    async def _async_execute_diagnostic(
        self,
        commands: str,
        context: DiagnosticRuntimeContext,
        *,
        publish_download_copy: bool = False,
    ) -> dict:
        async with self._runtime_operation_lock:
            result = await run_scenario(commands, context)
        result.context["runtime_debug_after"] = self._diagnostic_runtime_debug(
            getattr(context, "transport", None)
        )
        config_dir = Path(self.hass.config.config_dir)
        entry_id = self.config_entry.entry_id
        export = await self.hass.async_add_executor_job(
            lambda: export_diagnostic_run(
                config_dir=config_dir,
                entry_id=entry_id,
                result=result,
                publish_download_copy=publish_download_copy,
            )
        )
        return {
            "success": result.success,
            "output": result.output,
            "results": result.results,
            "context": result.context,
            "started_at": result.started_at,
            "finished_at": result.finished_at,
            "result_path": str(export.result_path),
            "download_url": export.download_url,
        }

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Reconcile listener bind/discovery state after HA or network readiness changes."""

        changed = await self._async_reconcile_network(reason=reason)
        if changed:
            if self.collector_home_assistant_primary:
                await self._async_prepare_home_assistant_callback_listener(
                    self.collector_callback_target_endpoint
                )
            await self.async_request_refresh()
        return changed

    async def _async_reconcile_network(self, *, reason: str) -> bool:
        reconcile = getattr(self._runtime, "async_reconcile_network", None)
        if reconcile is None:
            return False
        changed = bool(await reconcile(reason=reason))
        if changed:
            self._ha_primary_reconcile_last_signature = ("", "")
            logger.warning(
                "Reconciled EyeBond listener network state for entry %s after %s",
                self.config_entry.entry_id,
                reason or "network_change",
            )
        return changed

    async def _async_reconcile_collector_session_profile(self, *, reason: str) -> bool:
        """Align the runtime link with the best known collector cloud profile."""

        protocol = self.collector_session_protocol
        identity_strategy = self.collector_identity_strategy
        raw_passthrough_bootstrap = self.collector_raw_passthrough_bootstrap
        raw_passthrough_frame_format = self.collector_raw_passthrough_frame_format
        raw_passthrough_min_interval_ms = self.collector_raw_passthrough_min_interval_ms
        if (
            not protocol
            and not identity_strategy
            and not raw_passthrough_bootstrap
            and not raw_passthrough_frame_format
            and raw_passthrough_min_interval_ms <= 0
        ):
            return False

        reconcile = getattr(self._runtime, "async_reconcile_collector_session_profile", None)
        if reconcile is None:
            return False

        changed = bool(
            await reconcile(
                collector_session_protocol=protocol,
                collector_identity_strategy=identity_strategy,
                collector_raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                collector_raw_passthrough_frame_format=raw_passthrough_frame_format,
                collector_raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
                reason=reason,
            )
        )
        if changed:
            logger.warning(
                "Reconciled EyeBond collector session profile for entry %s after %s: protocol=%s identity=%s raw_bootstrap=%s raw_frame=%s raw_min_interval_ms=%s",
                self.config_entry.entry_id,
                reason or "collector_session_profile_change",
                protocol or "unknown",
                identity_strategy or "unknown",
                raw_passthrough_bootstrap or "unknown",
                raw_passthrough_frame_format or "unknown",
                raw_passthrough_min_interval_ms,
            )
        return changed

    def mark_entity_platforms_initialized(
        self,
        *,
        has_inverter_identity: bool | None = None,
        has_driver_fallback: bool | None = None,
    ) -> None:
        """Record that Home Assistant entity platforms finished loading."""

        self._entity_platforms_initialized = True
        loaded_with_inverter_identity = (
            self.has_inverter_identity
            if has_inverter_identity is None
            else bool(has_inverter_identity)
        )
        loaded_with_driver_fallback = bool(has_driver_fallback)
        self._entity_platforms_loaded_with_inverter_identity = loaded_with_inverter_identity
        self._entity_platforms_loaded_with_driver_fallback = loaded_with_driver_fallback
        self._platform_loaded_effective_metadata_signature = (
            self._effective_metadata_reload_signature_from_snapshot(
                self.effective_metadata_snapshot
            )
        )
        if self.has_inverter_identity and not loaded_with_inverter_identity:
            self._request_entry_reload_for_late_identity()

    def _effective_metadata_reload_signature_from_snapshot(
        self,
        snapshot: EffectiveMetadataSnapshot,
    ) -> tuple[str, str, str]:
        """Return one strict drift signature used for controlled reload checks."""

        if not snapshot.is_valid:
            return ("", "", "")
        variant_key = str(getattr(snapshot, "variant_key", "") or "").strip()
        profile_name = str(getattr(snapshot, "profile_name", "") or "").strip()
        register_schema_name = str(
            getattr(snapshot, "register_schema_name", "") or ""
        ).strip()
        if not (variant_key and profile_name and register_schema_name):
            return ("", "", "")
        return (variant_key, profile_name, register_schema_name)

    def _request_entry_reload_for_metadata_drift(
        self,
        *,
        setup_signature: tuple[str, str, str],
        runtime_signature: tuple[str, str, str],
    ) -> None:
        """Reload once when effective metadata drifts after platforms are loaded."""

        if not getattr(self, "_entity_platforms_initialized", False):
            return
        if getattr(self, "_entity_platform_reload_requested", False):
            return
        if not (
            getattr(self, "_entity_platforms_loaded_with_inverter_identity", False)
            or getattr(self, "_entity_platforms_loaded_with_driver_fallback", False)
        ):
            return
        if not all(runtime_signature):
            return

        first_runtime_signature = not any(setup_signature)
        if not first_runtime_signature and not all(setup_signature):
            return
        if not first_runtime_signature and setup_signature == runtime_signature:
            return

        self._entity_platform_reload_requested = True
        if first_runtime_signature:
            logger.info(
                "Reloading EyeBond entry %s after first confirmed effective metadata snapshot (%s)",
                self.config_entry.entry_id,
                "/".join(runtime_signature),
            )
        else:
            logger.info(
                "Reloading EyeBond entry %s after effective metadata drift (%s -> %s)",
                self.config_entry.entry_id,
                "/".join(setup_signature),
                "/".join(runtime_signature),
            )
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self.config_entry.entry_id)
        )

    def _request_entry_reload_for_late_identity(self) -> None:
        """Reload once when runtime confirms an inverter after platform setup."""

        if not getattr(self, "_entity_platforms_initialized", False):
            return
        if getattr(self, "_entity_platform_reload_requested", False):
            return
        self._entity_platform_reload_requested = True
        logger.info(
            "Reloading EyeBond entry %s after late runtime inverter confirmation",
            self.config_entry.entry_id,
        )
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self.config_entry.entry_id)
        )

    def _cancel_proxy_capture_deadline_refresh(self) -> None:
        """Cancel one scheduled deadline-triggered refresh if it exists."""

        handle = getattr(self, "_proxy_capture_deadline_refresh_handle", None)
        if handle is not None:
            handle.cancel()
        self._proxy_capture_deadline_refresh_handle = None

    async def _async_request_proxy_capture_deadline_refresh(self) -> None:
        """Ask the coordinator to reconcile proxy state when the lease expires."""

        self._proxy_capture_deadline_refresh_handle = None
        try:
            await self.async_request_refresh()
        except Exception as exc:
            logger.warning(
                "Proxy capture deadline refresh failed for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )

    def _schedule_proxy_capture_deadline_refresh(self, expires_at: object) -> None:
        """Schedule one coordinator refresh for the active proxy-capture deadline."""

        self._cancel_proxy_capture_deadline_refresh()
        deadline = parse_proxy_capture_session_timestamp(expires_at)
        if deadline is None:
            return

        loop = getattr(self.hass, "loop", None)
        if loop is None or not hasattr(loop, "call_later"):
            return

        delay = max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())

        def _trigger_refresh() -> None:
            create_task = getattr(self.hass, "async_create_task", None)
            coroutine = self._async_request_proxy_capture_deadline_refresh()
            if create_task is not None:
                create_task(coroutine)
            else:
                asyncio.create_task(coroutine)

        self._proxy_capture_deadline_refresh_handle = loop.call_later(delay, _trigger_refresh)

    def _proxy_capture_state_needs_reconcile(self, state: object | None) -> bool:
        """Return whether one interactive proxy action should first reconcile stale state."""

        if state is None or not proxy_capture_session_is_active(state):
            return False
        if proxy_capture_session_is_expired(state):
            return True
        status = str(getattr(state, "status", "") or "").strip()
        return status == "running" and not self._proxy_capture_process_running()

    def _proxy_capture_collector_ip(self) -> str:
        """Return the collector IP used to route proxy capture on shared ingress."""

        configured_ip = str(self.config_entry.data.get(CONF_COLLECTOR_IP) or "").strip()
        if configured_ip and configured_ip != DEFAULT_COLLECTOR_IP:
            return configured_ip
        collector = getattr(self.data, "collector", None)
        return str(getattr(collector, "remote_ip", "") or "").strip()

    @property
    def collector_operation_mode(self) -> str:
        """Return the persisted collector callback ownership mode."""

        if self.collector_capabilities.ha_only_required:
            return COLLECTOR_OPERATION_HA_ONLY

        mode = str(
            self.config_entry.options.get(
                CONF_COLLECTOR_OPERATION_MODE,
                self.config_entry.data.get(
                    CONF_COLLECTOR_OPERATION_MODE,
                    DEFAULT_COLLECTOR_OPERATION_MODE,
                ),
            )
            or DEFAULT_COLLECTOR_OPERATION_MODE
        ).strip()
        if mode not in COLLECTOR_OPERATION_MODES:
            return DEFAULT_COLLECTOR_OPERATION_MODE
        return mode

    @property
    def collector_home_assistant_primary(self) -> bool:
        """Return whether Home Assistant owns the collector callback endpoint."""

        return self.collector_operation_mode == COLLECTOR_OPERATION_HA_ONLY

    def _sync_forced_collector_operation_mode(self) -> None:
        """Persist forced HA-only mode once runtime proves the collector requires it."""

        capabilities = self.collector_capabilities
        if not capabilities.ha_only_required:
            return

        data = dict(self.config_entry.data)
        options = dict(self.config_entry.options)
        changed = False
        if data.get(CONF_COLLECTOR_OPERATION_MODE) != COLLECTOR_OPERATION_HA_ONLY:
            data[CONF_COLLECTOR_OPERATION_MODE] = COLLECTOR_OPERATION_HA_ONLY
            changed = True
        if options.get(CONF_COLLECTOR_OPERATION_MODE) != COLLECTOR_OPERATION_HA_ONLY:
            options[CONF_COLLECTOR_OPERATION_MODE] = COLLECTOR_OPERATION_HA_ONLY
            changed = True
        if capabilities.virtual_bridge and not data.get("collector_virtual_bridge"):
            data["collector_virtual_bridge"] = True
            changed = True
        if capabilities.virtual_bridge and not options.get("collector_virtual_bridge"):
            options["collector_virtual_bridge"] = True
            changed = True
        if capabilities.virtual_bridge and data.get("collector_bridge_kind") != "esp-collector":
            data["collector_bridge_kind"] = "esp-collector"
            changed = True
        if capabilities.virtual_bridge and options.get("collector_bridge_kind") != "esp-collector":
            options["collector_bridge_kind"] = "esp-collector"
            changed = True
        if capabilities.virtual_bridge:
            bridge_version = str(
                getattr(self.data.collector, "collector_bridge_version", "")
                or self.data.values.get("collector_bridge_version")
                or ""
            ).strip()
            if bridge_version and data.get("collector_bridge_version") != bridge_version:
                data["collector_bridge_version"] = bridge_version
                changed = True
            if bridge_version and options.get("collector_bridge_version") != bridge_version:
                options["collector_bridge_version"] = bridge_version
                changed = True
        if changed:
            self._async_update_entry_without_reload(data=data, options=options)

    def _collector_is_virtual_bridge(self) -> bool:
        """Return True when the running collector is a detected virtual bridge.

        Positive-only: reads the runtime snapshot's parsed hardware-version token and
        defaults to False when the snapshot is unavailable, so a factory
        collector behaves exactly as before.
        """

        return self.collector_capabilities.virtual_bridge

    @property
    def collector_capabilities(self) -> CollectorCapabilityProfile:
        """Return collector kind/capability profile for the current runtime."""

        snapshot = getattr(self, "data", RuntimeSnapshot())
        collector = getattr(snapshot, "collector", None)
        values = getattr(snapshot, "values", None)
        config_entry = getattr(self, "config_entry", None)
        return collector_capability_profile_from_runtime(
            collector=collector,
            values=values if isinstance(values, dict) else {},
            data=dict(getattr(config_entry, "data", {}) or {}),
            options=dict(getattr(config_entry, "options", {}) or {}),
        )

    def _configure_reverse_discovery_mode(self) -> None:
        """Control steady reverse discovery according to the collector ownership mode."""

        set_reverse_discovery_enabled = getattr(
            self._runtime,
            "set_reverse_discovery_enabled",
            None,
        )
        if set_reverse_discovery_enabled is None:
            return
        current_endpoint = ""
        snapshot = getattr(self, "data", None)
        values = getattr(snapshot, "values", None)
        if isinstance(values, dict):
            current_endpoint = str(values.get("collector_server_endpoint") or "").strip()
        endpoint_already_targets_ha = bool(
            current_endpoint
            and self._endpoint_host_targets_this_home_assistant(current_endpoint)
        )
        # HA-only normally disables steady reverse discovery, because a factory
        # collector reconnects to the persisted (param-21-written) endpoint on
        # its own. A virtual bridge has no cloud fallback, and older bridge
        # firmware may not persist that endpoint, so keep reverse discovery
        # ENABLED even when forced to HA-only unless its live endpoint already
        # points at this HA host. For any collector mode, once CLDSRVHOST1 is
        # already local, repeated UDP redirects are redundant.
        keep_reverse_discovery = (
            not endpoint_already_targets_ha
            and (
                not self.collector_home_assistant_primary
                or self._collector_is_virtual_bridge()
            )
        )
        set_reverse_discovery_enabled(keep_reverse_discovery)

    def consume_entry_reload_suppression(self) -> bool:
        """Return whether the next config-entry update listener should skip reload."""

        if getattr(self, "_suppress_entry_reload_count", 0) <= 0:
            return False
        self._suppress_entry_reload_count -= 1
        return True

    def _async_update_entry_without_reload(self, **update_kwargs: Any) -> None:
        """Persist runtime metadata without reloading the entry we are actively running."""

        self._suppress_entry_reload_count = getattr(self, "_suppress_entry_reload_count", 0) + 1
        changed = False
        try:
            changed = bool(
                self.hass.config_entries.async_update_entry(
                    self.config_entry,
                    **update_kwargs,
                )
            )
        finally:
            if not changed:
                # A no-op update fires no update listener, so nothing would
                # ever consume the suppression - and the NEXT genuine options
                # change would have its reload silently swallowed.
                self._suppress_entry_reload_count = max(
                    self._suppress_entry_reload_count - 1, 0
                )

    def _normalized_remembered_collector_server_endpoint(self) -> str:
        endpoint = str(
            getattr(self, "_remembered_collector_server_endpoint", "") or ""
        ).strip()
        if not endpoint:
            return ""
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(endpoint)
            host, _port, _protocol = _parse_collector_server_endpoint(normalized_endpoint)
        except ValueError:
            return ""
        if host == self._effective_callback_server_host:
            return ""
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return ""
        return normalized_endpoint

    @property
    def _effective_callback_server_host(self) -> str:
        runtime_host = str(
            getattr(self._runtime, "effective_advertised_server_ip", "") or ""
        ).strip()
        if runtime_host:
            return runtime_host
        return str(
            getattr(self._connection_spec, "effective_advertised_server_ip", "") or ""
        ).strip()

    def _endpoint_host_targets_this_home_assistant(self, endpoint: str) -> bool:
        """Return whether one collector endpoint is explicitly aimed at this HA host."""

        try:
            host, _port, _protocol = _parse_collector_server_endpoint(endpoint)
        except ValueError:
            return False

        normalized_host = str(host or "").strip().lower()
        if not normalized_host:
            return False

        candidate_hosts = {
            str(self._effective_callback_server_host or "").strip().lower(),
            str(getattr(self._runtime, "effective_advertised_server_ip", "") or "").strip().lower(),
            str(getattr(self._connection_spec, "effective_advertised_server_ip", "") or "").strip().lower(),
            str(getattr(self._connection_spec, "server_ip", "") or "").strip().lower(),
        }
        candidate_hosts.discard("")
        return normalized_host in candidate_hosts

    async def _async_prepare_home_assistant_callback_listener(self, endpoint: str) -> None:
        ensure_listener = getattr(self._runtime, "async_ensure_callback_listener", None)
        if ensure_listener is None:
            return

        callback_host, callback_port, _callback_protocol = _resolve_collector_server_endpoint(
            endpoint,
            cloud_family=self.collector_cloud_family,
        )
        if callback_host != self._effective_callback_server_host:
            return

        await ensure_listener(callback_port)

    def _endpoint_looks_like_local_collector_callback(self, endpoint: str) -> bool:
        try:
            host, _port, _protocol = _parse_collector_server_endpoint(endpoint)
        except ValueError:
            return False
        if host == self._effective_callback_server_host:
            return True
        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        collector_ip = str(config_data.get(CONF_COLLECTOR_IP) or "").strip()
        return bool(collector_ip and _same_ipv4_24(host, collector_ip))

    async def _async_remember_collector_server_endpoint(self, snapshot: RuntimeSnapshot) -> None:
        current_endpoint = str(snapshot.values.get("collector_server_endpoint") or "").strip()
        if not current_endpoint:
            return
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(current_endpoint)
            host, _port, _protocol = _parse_collector_server_endpoint(normalized_endpoint)
        except ValueError:
            return
        if host == self._effective_callback_server_host:
            return
        if self.collector_home_assistant_primary:
            return
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return

        remembered_endpoint = self._normalized_remembered_collector_server_endpoint()
        if remembered_endpoint and normalized_endpoint != remembered_endpoint:
            return
        if normalized_endpoint == remembered_endpoint:
            return

        profile_key = (
            _collector_cloud_family_from_endpoint_shape(normalized_endpoint)
            or _known_collector_cloud_family(snapshot.values.get("collector_cloud_family"))
            or self.collector_cloud_family
        )
        self._remembered_collector_server_endpoint = normalized_endpoint
        options = dict(self.config_entry.options)
        options.update(
            _collector_original_endpoint_source_options(
                endpoint=normalized_endpoint,
                profile_key=profile_key,
                source="runtime_observed",
            )
        )
        self._async_update_entry_without_reload(options=options)
        await self._async_remember_collector_original_endpoint_in_registry(
            snapshot=snapshot,
            endpoint=normalized_endpoint,
            profile_key=profile_key,
            source="runtime_observed",
            observed_at=str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT) or ""
            ),
        )

    async def _async_restore_collector_original_endpoint_from_registry(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Restore preserved original endpoint options from the PN registry when absent."""

        if self._normalized_remembered_collector_server_endpoint():
            return
        collector_pn = self._preferred_collector_pn(snapshot)
        collector = getattr(snapshot, "collector", None)
        collector_ip = (
            str(getattr(collector, "remote_ip", "") or "").strip()
            or str(self.config_entry.data.get(CONF_COLLECTOR_IP, "") or "").strip()
        )
        if not collector_pn and not collector_ip:
            return

        config_dir = Path(self.hass.config.config_dir)
        try:
            record = await self.hass.async_add_executor_job(
                lambda: (
                    get_collector_registry_record(
                        config_dir=config_dir,
                        collector_pn=collector_pn,
                    )
                    if collector_pn
                    else None
                )
            )
            if record is None and collector_ip:
                record = await self.hass.async_add_executor_job(
                    lambda: get_collector_registry_record_by_last_seen_ip(
                        config_dir=config_dir,
                        last_seen_ip=collector_ip,
                    )
                )
        except Exception as exc:
            logger.debug("Could not read collector registry: %s", exc)
            return
        if record is None or not record.original_endpoint_raw:
            return
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(
                record.original_endpoint_raw
            )
        except ValueError:
            return
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return

        self._remembered_collector_server_endpoint = normalized_endpoint
        options = dict(self.config_entry.options)
        options.update(
            _collector_original_endpoint_source_options(
                endpoint=normalized_endpoint,
                profile_key=record.cloud_profile_key,
                source=record.source or "collector_registry",
                observed_at=record.observed_at,
            )
        )
        self._async_update_entry_without_reload(options=options)

    async def _async_remember_collector_original_endpoint_in_registry(
        self,
        *,
        snapshot: RuntimeSnapshot,
        endpoint: str,
        profile_key: str,
        source: str,
        observed_at: str = "",
    ) -> None:
        """Persist the original endpoint in the collector PN registry when possible."""

        collector_pn = self._preferred_collector_pn(snapshot)
        if not collector_pn:
            return
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(endpoint)
        except ValueError:
            return
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return

        collector = getattr(snapshot, "collector", None)
        last_seen_ip = str(getattr(collector, "remote_ip", "") or "").strip()
        config_dir = Path(self.hass.config.config_dir)
        try:
            await self.hass.async_add_executor_job(
                lambda: remember_collector_original_endpoint(
                    config_dir=config_dir,
                    collector_pn=collector_pn,
                    original_endpoint_raw=normalized_endpoint,
                    cloud_profile_key=profile_key,
                    source=source,
                    observed_at=observed_at,
                    last_seen_ip=last_seen_ip,
                )
            )
        except Exception as exc:
            logger.debug("Could not update collector registry: %s", exc)

    async def _async_remember_runtime_identity(self, snapshot: RuntimeSnapshot) -> None:
        """Persist stronger collector/inverter identity once runtime detection succeeds."""

        current_data = dict(self.config_entry.data)
        updated_data = dict(current_data)
        current_options = dict(self.config_entry.options)
        updated_options = dict(current_options)
        had_inverter_identity = bool(
            str(current_data.get(CONF_DETECTED_MODEL) or "").strip()
            or str(current_data.get(CONF_DETECTED_SERIAL) or "").strip()
        )

        def _set_data_if_value(key: str, value: object) -> None:
            if value is None:
                return
            normalized = value if isinstance(value, int) else str(value).strip()
            if normalized == "":
                return
            if updated_data.get(key) != normalized:
                updated_data[key] = normalized

        collector_pn = self._preferred_collector_pn(snapshot)
        if collector_pn and updated_data.get(CONF_COLLECTOR_PN) != collector_pn:
            updated_data[CONF_COLLECTOR_PN] = collector_pn

        collector = snapshot.collector
        collector_ip = str(getattr(collector, "remote_ip", "") or "").strip()
        if collector_ip and not str(updated_data.get(CONF_COLLECTOR_IP) or "").strip():
            updated_data[CONF_COLLECTOR_IP] = collector_ip

        collector_cloud_family = _known_collector_cloud_family(
            snapshot.values.get("collector_cloud_family")
        )
        if not collector_cloud_family:
            collector_cloud_family = self.collector_cloud_family
        if collector_cloud_family and updated_data.get(CONF_COLLECTOR_CLOUD_FAMILY) != collector_cloud_family:
            updated_data[CONF_COLLECTOR_CLOUD_FAMILY] = collector_cloud_family

        if collector is not None:
            _set_data_if_value(
                CONF_SMARTESS_COLLECTOR_VERSION,
                getattr(collector, "smartess_collector_version", "")
                or snapshot.values.get("smartess_collector_version"),
            )
            _set_data_if_value(
                CONF_SMARTESS_PROTOCOL_ASSET_ID,
                getattr(collector, "smartess_protocol_asset_id", "")
                or snapshot.values.get("smartess_protocol_asset_id"),
            )
            _set_data_if_value(
                CONF_SMARTESS_PROFILE_KEY,
                getattr(collector, "smartess_protocol_profile_key", "")
                or snapshot.values.get("smartess_protocol_profile_key")
                or snapshot.values.get("smartess_profile_key"),
            )
            _set_data_if_value(
                CONF_SMARTESS_DEVICE_ADDRESS,
                getattr(collector, "smartess_device_address", None)
                if getattr(collector, "smartess_device_address", None) is not None
                else snapshot.values.get("smartess_device_address"),
            )
            collector_cloud_profile_key = (
                getattr(collector, "collector_cloud_profile_key", "")
                or getattr(collector, "smartess_protocol_profile_key", "")
                or snapshot.values.get("collector_cloud_profile_key")
                or snapshot.values.get("smartess_protocol_profile_key")
                or snapshot.values.get("smartess_profile_key")
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_KEY,
                collector_cloud_profile_key,
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_LABEL,
                getattr(collector, "collector_cloud_profile_label", "")
                or getattr(collector, "smartess_protocol_name", "")
                or getattr(collector, "smartess_protocol_asset_name", "")
                or snapshot.values.get("collector_cloud_profile_label")
                or snapshot.values.get("smartess_protocol_name")
                or snapshot.values.get("smartess_protocol_asset_name"),
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_SOURCE,
                getattr(collector, "collector_cloud_profile_source", "")
                or snapshot.values.get("collector_cloud_profile_source")
                or ("runtime_observed" if collector_cloud_profile_key else ""),
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE,
                getattr(collector, "collector_cloud_profile_confidence", "")
                or snapshot.values.get("collector_cloud_profile_confidence")
                or ("high" if collector_cloud_profile_key else ""),
            )

        inverter = snapshot.inverter
        if inverter is not None:
            detected_model = str(inverter.model_name or "").strip()
            detected_serial = str(inverter.serial_number or "").strip()
            driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
            variant_key = str(getattr(inverter, "variant_key", "") or "").strip()
            if detected_model and updated_data.get(CONF_DETECTED_MODEL) != detected_model:
                updated_data[CONF_DETECTED_MODEL] = detected_model
            if detected_serial and updated_data.get(CONF_DETECTED_SERIAL) != detected_serial:
                updated_data[CONF_DETECTED_SERIAL] = detected_serial
            if (
                not detected_serial
                and variant_key == "smartess_0925"
                and str(updated_data.get(CONF_DETECTED_SERIAL) or "").strip()
            ):
                updated_data[CONF_DETECTED_SERIAL] = ""
            if str(updated_data.get(CONF_DETECTION_CONFIDENCE) or "").strip() in {
                "",
                "none",
                "low",
                "medium",
            }:
                updated_data[CONF_DETECTION_CONFIDENCE] = "high"
            if updated_data.get(CONF_CONTROL_MODE) == CONTROL_MODE_READ_ONLY:
                updated_data[CONF_CONTROL_MODE] = DEFAULT_CONTROL_MODE
            if updated_options.get(CONF_CONTROL_MODE) == CONTROL_MODE_READ_ONLY:
                updated_options[CONF_CONTROL_MODE] = DEFAULT_CONTROL_MODE
            if driver_key:
                if str(updated_data.get(CONF_DRIVER_HINT) or "").strip() in {"", DRIVER_HINT_AUTO}:
                    updated_data[CONF_DRIVER_HINT] = driver_key
                if str(updated_options.get(CONF_DRIVER_HINT) or "").strip() in {"", DRIVER_HINT_AUTO}:
                    updated_options[CONF_DRIVER_HINT] = driver_key

        current_effective_snapshot = effective_metadata_snapshot_from_dict(
            current_options.get(_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY)
        )
        updated_effective_snapshot = self._build_runtime_effective_metadata_snapshot(
            snapshot,
            entry_data=updated_data,
            current_snapshot=current_effective_snapshot,
        )
        if updated_effective_snapshot is not None:
            updated_snapshot_data = updated_effective_snapshot.as_dict()
            if updated_snapshot_data != current_effective_snapshot.as_dict():
                updated_options[_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY] = (
                    updated_snapshot_data
                )
            self._request_entry_reload_for_metadata_drift(
                setup_signature=getattr(
                    self,
                    "_platform_loaded_effective_metadata_signature",
                    ("", "", ""),
                ),
                runtime_signature=self._effective_metadata_reload_signature_from_snapshot(
                    updated_effective_snapshot
                ),
            )

        if updated_data == current_data and updated_options == current_options:
            return

        current_title = str(self.config_entry.title or "").strip()
        previous_preferred_title = installation_title(
            collector_pn=current_data.get(CONF_COLLECTOR_PN, ""),
            collector_ip=current_data.get(CONF_COLLECTOR_IP, ""),
            detected_model=current_data.get(CONF_DETECTED_MODEL, ""),
            detected_serial=current_data.get(CONF_DETECTED_SERIAL, ""),
        )
        updated_title = installation_title(
            collector_pn=updated_data.get(CONF_COLLECTOR_PN, ""),
            collector_ip=updated_data.get(CONF_COLLECTOR_IP, ""),
            detected_model=updated_data.get(CONF_DETECTED_MODEL, ""),
            detected_serial=updated_data.get(CONF_DETECTED_SERIAL, ""),
        )
        legacy_titles = legacy_installation_titles(
            detected_model=current_data.get(CONF_DETECTED_MODEL, ""),
            detected_serial=current_data.get(CONF_DETECTED_SERIAL, ""),
            collector_ip=current_data.get(CONF_COLLECTOR_IP, ""),
            server_ip=current_data.get(CONF_SERVER_IP, ""),
        )

        update_kwargs: dict[str, Any] = {}
        if updated_data != current_data:
            update_kwargs["data"] = updated_data
        if updated_options != current_options:
            update_kwargs["options"] = updated_options
        if (
            updated_title
            and updated_title != current_title
            and current_title in {"EyeBond Setup Pending", previous_preferred_title, *legacy_titles}
        ):
            update_kwargs["title"] = updated_title

        self._async_update_entry_without_reload(**update_kwargs)
        gained_inverter_identity = bool(
            str(updated_data.get(CONF_DETECTED_MODEL) or "").strip()
            or str(updated_data.get(CONF_DETECTED_SERIAL) or "").strip()
        )
        platforms_need_identity_reload = bool(
            getattr(self, "_entity_platforms_initialized", False)
            and not getattr(self, "_entity_platforms_loaded_with_inverter_identity", False)
        )
        if gained_inverter_identity and (not had_inverter_identity or platforms_need_identity_reload):
            self._request_entry_reload_for_late_identity()

    def _support_context_title(self) -> str:
        """Return the support artifact title, preferring confirmed inverter identity."""

        inverter = self.data.inverter
        model_name = str(getattr(inverter, "model_name", "") or "").strip()
        serial_number = str(getattr(inverter, "serial_number", "") or "").strip()
        if model_name and serial_number:
            return f"{model_name} ({serial_number})"
        if model_name:
            return model_name
        return str(self.config_entry.title or "").strip() or "EyeBond Local"

    def _build_runtime_effective_metadata_snapshot(
        self,
        snapshot: RuntimeSnapshot,
        *,
        entry_data: dict[str, Any],
        current_snapshot,
    ):
        """Return one persisted snapshot only when live runtime identity is confirmed."""

        inverter = snapshot.inverter
        if inverter is None:
            return None

        model_name = str(getattr(inverter, "model_name", "") or "").strip()
        serial_number = str(getattr(inverter, "serial_number", "") or "").strip()
        if not (model_name or serial_number):
            return None

        # Persist only when runtime supplied concrete metadata, not driver defaults alone.
        profile_name = str(getattr(inverter, "profile_name", "") or "").strip()
        register_schema_name = str(
            getattr(inverter, "register_schema_name", "") or ""
        ).strip()
        if not profile_name or not register_schema_name:
            return None

        effective_selection = resolve_effective_metadata_selection(
            inverter=inverter,
            driver=self.current_driver,
            collector=snapshot.collector,
            entry_data=entry_data,
            entry_options=self.config_entry.options,
        )
        confidence = str(
            entry_data.get(CONF_DETECTION_CONFIDENCE)
            or self.detection_confidence
            or "none"
        ).strip()
        stable_snapshot = build_effective_metadata_snapshot_from_runtime(
            inverter=inverter,
            selection=effective_selection,
            confidence=confidence,
            generation=current_snapshot.generation,
            generated_at=current_snapshot.generated_at,
        )
        if not stable_snapshot.is_valid:
            return None
        if (
            stable_snapshot.effective_owner_key == current_snapshot.effective_owner_key
            and stable_snapshot.effective_owner_name == current_snapshot.effective_owner_name
            and stable_snapshot.variant_key == current_snapshot.variant_key
            and stable_snapshot.profile_name == current_snapshot.profile_name
            and stable_snapshot.register_schema_name == current_snapshot.register_schema_name
            and stable_snapshot.confidence == current_snapshot.confidence
            and stable_snapshot.candidate_keys == current_snapshot.candidate_keys
            and stable_snapshot.resolution_level == current_snapshot.resolution_level
            and stable_snapshot.surface_key == current_snapshot.surface_key
            and stable_snapshot.evidence_fingerprint == current_snapshot.evidence_fingerprint
            and stable_snapshot.catalog_version == current_snapshot.catalog_version
            and stable_snapshot.descriptor_revisions == current_snapshot.descriptor_revisions
        ):
            return None

        new_snapshot = build_effective_metadata_snapshot_from_runtime(
            inverter=inverter,
            selection=effective_selection,
            confidence=confidence,
            generation=max(int(current_snapshot.generation), 0) + 1,
            generated_at=datetime.now(timezone.utc),
        )
        if not new_snapshot.is_valid:
            return None
        return new_snapshot

    def _endpoint_effective_parts(self, endpoint: str) -> tuple[str, int, str]:
        try:
            return _resolve_collector_server_endpoint(
                endpoint,
                cloud_family=self.collector_cloud_family,
            )
        except ValueError:
            return "", 0, ""

    async def _async_reconcile_collector_operation_mode_endpoint(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Keep collector parameter 21 aligned with Home-Assistant-primary mode."""

        snapshot.values.pop("collector_operation_endpoint_sync_error", None)
        current_endpoint = str(snapshot.values.get("collector_server_endpoint") or "").strip()
        current_parts = self._endpoint_effective_parts(current_endpoint)
        pending_target_endpoint = str(
            getattr(self, "_collector_operation_pending_target_endpoint", "") or ""
        ).strip()
        pending_target_parts = self._endpoint_effective_parts(pending_target_endpoint)
        if self.collector_home_assistant_primary:
            target_endpoint = self.collector_callback_target_endpoint
            if not target_endpoint:
                self._collector_operation_pending_target_endpoint = ""
                snapshot.values["collector_operation_endpoint_sync_status"] = "target_unavailable"
                return

            await self._async_prepare_home_assistant_callback_listener(target_endpoint)
        else:
            self._collector_operation_pending_target_endpoint = ""
            snapshot.values["collector_operation_endpoint_sync_status"] = "not_managed"
            if not current_parts[0] or not self._endpoint_looks_like_local_collector_callback(
                current_endpoint
            ):
                return
            if await self._async_shadow_learning_blocks_endpoint_reconcile():
                snapshot.values[
                    "collector_operation_endpoint_sync_status"
                ] = "shadow_learning_active"
                return
            if self.proxy_capture_overview.status in {
                "starting",
                "running",
                "stopping",
                "restoring",
            }:
                return
            target_endpoint = self.proxy_capture_upstream_endpoint
            if not target_endpoint:
                return

        target_parts = self._endpoint_effective_parts(target_endpoint)
        pending_matches_target = bool(
            pending_target_parts[0] and pending_target_parts == target_parts
        )
        if pending_matches_target and not snapshot.connected:
            snapshot.values["collector_operation_endpoint_sync_status"] = "waiting_for_collector"
            return

        if current_parts == target_parts and current_parts[0]:
            self._collector_operation_pending_target_endpoint = ""
            snapshot.values["collector_operation_endpoint_sync_status"] = "aligned"
            return

        if pending_matches_target and snapshot.connected and not current_endpoint:
            self._collector_operation_pending_target_endpoint = ""
            snapshot.values["collector_server_endpoint"] = pending_target_endpoint
            snapshot.values["collector_operation_endpoint_sync_status"] = "aligned"
            return

        if not snapshot.connected:
            snapshot.values["collector_operation_endpoint_sync_status"] = "waiting_for_collector"
            return

        try:
            normalized_current = _normalize_preserved_collector_server_endpoint(current_endpoint)
        except ValueError:
            normalized_current = current_endpoint
        signature = (normalized_current, target_endpoint)
        now = asyncio.get_running_loop().time()
        if (
            signature == self._ha_primary_reconcile_last_signature
            and now - self._ha_primary_reconcile_last_attempt_monotonic
            < _COLLECTOR_HA_PRIMARY_RECONCILE_COOLDOWN_SECONDS
        ):
            snapshot.values["collector_operation_endpoint_sync_status"] = "cooldown"
            return

        self._ha_primary_reconcile_last_signature = signature
        self._ha_primary_reconcile_last_attempt_monotonic = now
        try:
            result = await self._runtime.async_set_collector_server_endpoint(
                target_endpoint,
                apply_changes=True,
            )
        except Exception as exc:
            snapshot.values["collector_operation_endpoint_sync_status"] = "failed"
            snapshot.values["collector_operation_endpoint_sync_error"] = str(exc)
            logger.warning(
                "Failed to align collector callback endpoint for Home Assistant only mode: current=%s target=%s error=%s",
                current_endpoint or "unknown",
                target_endpoint,
                exc,
            )
            return

        snapshot.values["collector_server_endpoint"] = str(
            result.get("readback_endpoint") or result.get("requested_endpoint") or target_endpoint
        )
        self._collector_operation_pending_target_endpoint = snapshot.values[
            "collector_server_endpoint"
        ]
        snapshot.values["collector_operation_endpoint_sync_status"] = str(
            result.get("status") or "applied"
        )

    async def _async_shadow_learning_blocks_endpoint_reconcile(self) -> bool:
        """Return whether shadow learning temporarily owns the collector endpoint.

        In SmartESS+HA mode the steady-state reconcile normally restores a local
        callback endpoint back to SmartESS. During shadow learning that same
        endpoint is the safety boundary. Restoring it mid-run gives SmartESS a
        direct route to the collector and can leak the next ``ctrlDevice`` write
        to the real inverter.
        """

        if self._shadow_learning_process_running():
            return True
        try:
            state = await self._async_active_shadow_learning_state(require_process=False)
        except Exception:
            return False
        return bool(state is not None and shadow_learning_session_is_active(state))

    def _prune_hidden_collector_values_for_mode(self, snapshot: RuntimeSnapshot) -> None:
        """Hide collector diagnostics that do not apply in Home-Assistant-primary mode."""

        if not self.collector_home_assistant_primary:
            return
        for key in _HIDDEN_HA_ONLY_COLLECTOR_VALUE_KEYS:
            snapshot.values.pop(key, None)

    async def _async_update_data(self) -> RuntimeSnapshot:
        if getattr(self, "_shutdown_complete", False) and self.data is not None:
            # A refresh queued before shutdown (debounced request, connection
            # watcher, write follow-up) must not drive the stopped link.
            return self.data
        if self._diagnostic_active and self.data is not None:
            # A diagnostic command run holds the shared transport. Skip the live
            # poll so it does not contend on the bus; return the last snapshot.
            return self.data
        async with self._runtime_operation_lock:
            if self._diagnostic_active and self.data is not None:
                return self.data
            self._ensure_poll_scheduler()
            self._configure_poll_scheduler_from_options()
            poll_interval = self._current_poll_cycle_interval_seconds()
            previous_runtime_driver_state = (
                _runtime_driver_state_from_snapshot(self.data)
                if isinstance(self.data, RuntimeSnapshot)
                else ""
            )
            previous_reconnect_count = (
                _snapshot_reconnect_count(self.data)
                if isinstance(self.data, RuntimeSnapshot)
                else 0
            )
            loop = asyncio.get_running_loop()
            cycle_started = loop.time()
            previous_started = float(
                getattr(self, "_poll_last_cycle_started_monotonic", 0.0) or 0.0
            )
            self._poll_last_cycle_started_monotonic = cycle_started
            snapshot = await self._async_update_data_with_runtime_lock(
                poll_interval_seconds=poll_interval
            )
            cycle_duration = max(0.0, loop.time() - cycle_started)
            self._update_poll_scheduler_policy_from_snapshot(snapshot)
            runtime_driver_state = _runtime_driver_state_from_snapshot(snapshot)
            poll_context = _poll_context_for_runtime_driver_state(runtime_driver_state)
            runtime_poll_success = _is_clean_runtime_poll_cycle(
                previous_runtime_driver_state=previous_runtime_driver_state,
                runtime_driver_state=runtime_driver_state,
                previous_reconnect_count=previous_reconnect_count,
                reconnect_count=_snapshot_reconnect_count(snapshot),
            )
            decision = self._poll_scheduler.observe(
                cycle_duration,
                success=runtime_poll_success,
            )
            next_poll_interval = self._next_poll_cycle_interval_seconds(
                current_interval=poll_interval,
                duration_seconds=cycle_duration,
                poll_context=poll_context,
                decision=decision,
            )
            start_interval = (
                max(0.0, cycle_started - previous_started)
                if previous_started > 0.0
                else None
            )
            self._record_poll_cycle_metrics(
                snapshot,
                poll_interval_seconds=poll_interval,
                duration_seconds=cycle_duration,
                start_interval_seconds=start_interval,
                decision=decision,
                runtime_driver_state=runtime_driver_state,
                poll_context=poll_context,
                next_interval_seconds=next_poll_interval,
                clean_runtime_poll=runtime_poll_success,
            )
            self._sync_fixed_rate_poll_update_interval(
                snapshot,
                poll_interval_seconds=next_poll_interval,
                duration_seconds=cycle_duration,
                scheduler_mode=decision.mode,
            )
            self._maybe_persist_unsupported_commands(snapshot)
            return snapshot

    def _configured_poll_interval_seconds(self) -> int:
        config_entry = getattr(self, "config_entry", None)
        options = getattr(config_entry, "options", {}) or {}
        return _clamp_poll_interval_seconds(
            options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )

    def _configured_poll_mode(self) -> str:
        config_entry = getattr(self, "config_entry", None)
        options = getattr(config_entry, "options", {}) or {}
        if CONF_POLL_MODE not in options:
            return POLL_MODE_MANUAL
        return normalize_poll_mode(options.get(CONF_POLL_MODE, DEFAULT_POLL_MODE))

    def _ensure_poll_scheduler(self) -> None:
        if isinstance(getattr(self, "_poll_scheduler", None), PollScheduler):
            return
        config_entry = getattr(self, "config_entry", None)
        options = getattr(config_entry, "options", {}) or {}
        driver_key = str(
            getattr(self, "_poll_scheduler_driver_key", "")
            or options.get(CONF_DRIVER_HINT, "auto")
            or "auto"
        )
        self._poll_scheduler_driver_key = driver_key
        self._poll_scheduler = PollScheduler(
            policy=poll_policy_for_driver(driver_key),
            mode=self._configured_poll_mode(),
            manual_interval=self._configured_poll_interval_seconds(),
        )

    def _configure_poll_scheduler_from_options(self) -> None:
        self._ensure_poll_scheduler()
        self._poll_scheduler.configure(
            mode=self._configured_poll_mode(),
            manual_interval=self._configured_poll_interval_seconds(),
        )
        if self._configured_poll_mode() != POLL_MODE_AUTO:
            self._poll_non_runtime_retry_interval_seconds = 0

    def _current_poll_cycle_interval_seconds(self) -> float:
        self._ensure_poll_scheduler()
        scheduler_interval = self._poll_scheduler.current_interval()
        if self._configured_poll_mode() != POLL_MODE_AUTO:
            return scheduler_interval
        retry_interval = float(
            getattr(self, "_poll_non_runtime_retry_interval_seconds", 0.0) or 0.0
        )
        if retry_interval <= 0.0:
            return scheduler_interval
        policy = getattr(self._poll_scheduler, "policy", poll_policy_for_driver(""))
        return clamp_interval(
            retry_interval,
            minimum=policy.min_auto_interval,
            maximum=policy.max_auto_interval,
        )

    def _next_poll_cycle_interval_seconds(
        self,
        *,
        current_interval: float,
        duration_seconds: float,
        poll_context: str,
        decision: PollDecision,
    ) -> float:
        if decision.mode != POLL_MODE_AUTO:
            self._poll_non_runtime_retry_interval_seconds = 0
            return decision.effective_interval
        if poll_context == _COLLECTOR_POLL_CONTEXT_RUNTIME:
            self._poll_non_runtime_retry_interval_seconds = 0
            return decision.effective_interval
        retry_interval = _poll_non_runtime_retry_interval_seconds(
            current_interval=current_interval,
            observed_duration=duration_seconds,
            decision=decision,
        )
        self._poll_non_runtime_retry_interval_seconds = retry_interval
        return retry_interval

    def _update_poll_scheduler_policy_from_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return
        driver_key = str(values.get("driver_key") or "").strip()
        if not driver_key:
            return
        if driver_key == getattr(self, "_poll_scheduler_driver_key", ""):
            return
        self._poll_scheduler_driver_key = driver_key
        self._poll_scheduler.configure(policy=poll_policy_for_driver(driver_key))

    def _record_poll_cycle_metrics(
        self,
        snapshot: RuntimeSnapshot,
        *,
        poll_interval_seconds: float,
        duration_seconds: float | None = None,
        start_interval_seconds: float | None = None,
        decision: PollDecision | None = None,
        runtime_driver_state: str | None = None,
        poll_context: str | None = None,
        next_interval_seconds: float | None = None,
        clean_runtime_poll: bool | None = None,
    ) -> None:
        """Publish poll-pipeline utilization and protect against stable overruns."""

        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return
        driver_duration_ms = values.get("collector_poll_duration_ms")
        if duration_seconds is None:
            try:
                duration = max(0.0, float(driver_duration_ms) / 1000.0)
            except (TypeError, ValueError):
                return
        else:
            try:
                duration = max(0.0, float(duration_seconds))
            except (TypeError, ValueError):
                return
        interval = clamp_interval(poll_interval_seconds)

        if not runtime_driver_state:
            runtime_driver_state = _runtime_driver_state_from_snapshot(snapshot)
        if not poll_context:
            poll_context = _poll_context_for_runtime_driver_state(runtime_driver_state)
        runtime_poll = poll_context == _COLLECTOR_POLL_CONTEXT_RUNTIME
        # Cycles that bound the driver or recovered the connection measure that
        # recovery work, not the normal poll cost: keep them out of the duration
        # statistics, the warning streaks, and the scheduler alike.
        clean_poll = runtime_poll if clean_runtime_poll is None else bool(clean_runtime_poll)

        if clean_poll:
            self._poll_duration_max_seconds = max(
                float(getattr(self, "_poll_duration_max_seconds", 0.0) or 0.0),
                duration,
            )
            current_ewma = float(getattr(self, "_poll_duration_ewma_seconds", 0.0) or 0.0)
            if current_ewma <= 0.0:
                self._poll_duration_ewma_seconds = duration
            else:
                self._poll_duration_ewma_seconds = (
                    current_ewma * 0.7 + duration * 0.3
                )
            recent = list(getattr(self, "_poll_recent_durations_seconds", []) or [])
            recent.append(duration)
            self._poll_recent_durations_seconds = recent
            if len(self._poll_recent_durations_seconds) > 20:
                self._poll_recent_durations_seconds = self._poll_recent_durations_seconds[-20:]

        next_interval = (
            clamp_interval(next_interval_seconds)
            if next_interval_seconds is not None
            else (
                clamp_interval(decision.effective_interval)
                if decision is not None
                else interval
            )
        )
        utilization_ratio = duration / float(interval) if interval > 0 else 0.0
        if runtime_poll and clean_poll and utilization_ratio >= _POLL_OVERRUN_RATIO:
            self._collector_poll_overrun_streak = (
                int(getattr(self, "_collector_poll_overrun_streak", 0) or 0) + 1
            )
        else:
            self._collector_poll_overrun_streak = 0

        if runtime_poll and clean_poll and utilization_ratio >= _POLL_UTILIZATION_WARNING_RATIO:
            self._collector_poll_high_utilization_streak = (
                int(getattr(self, "_collector_poll_high_utilization_streak", 0) or 0) + 1
            )
        else:
            self._collector_poll_high_utilization_streak = 0

        recent_peak = max(self._poll_recent_durations_seconds[-5:] or [duration])
        recommended = (
            int(math.ceil(decision.recommended_interval))
            if decision is not None
            else _poll_recommended_interval_seconds(
                current_interval=interval,
                observed_duration=recent_peak,
            )
        )
        if duration_seconds is not None:
            try:
                values["collector_driver_poll_duration_ms"] = int(driver_duration_ms)
            except (TypeError, ValueError):
                values.pop("collector_driver_poll_duration_ms", None)
        if start_interval_seconds is not None:
            values["collector_poll_start_interval_ms"] = int(
                round(max(0.0, start_interval_seconds) * 1000.0)
            )
        values.update(
            {
                "collector_poll_interval_configured_seconds": self._configured_poll_interval_seconds(),
                "collector_poll_manual_interval_seconds": self._configured_poll_interval_seconds(),
                "collector_poll_mode": (
                    decision.mode if decision is not None else self._configured_poll_mode()
                ),
                "collector_poll_policy_driver_key": getattr(
                    self,
                    "_poll_scheduler_driver_key",
                    "",
                ),
                "collector_poll_policy_min_interval_seconds": (
                    decision.policy_min_interval
                    if decision is not None
                    else getattr(
                        getattr(self, "_poll_scheduler", None),
                        "policy",
                        poll_policy_for_driver(""),
                    ).min_auto_interval
                ),
                "collector_poll_policy_max_interval_seconds": (
                    decision.policy_max_interval
                    if decision is not None
                    else getattr(
                        getattr(self, "_poll_scheduler", None),
                        "policy",
                        poll_policy_for_driver(""),
                    ).max_auto_interval
                ),
                "runtime_driver_state": runtime_driver_state,
                "collector_poll_context": poll_context,
                "collector_poll_current_interval_seconds": interval,
                "collector_poll_next_interval_seconds": next_interval,
                "collector_poll_target_start_interval_seconds": next_interval,
                "collector_poll_duration_ms": int(round(duration * 1000.0)),
                "collector_poll_duration_avg_ms": int(
                    round(self._poll_duration_ewma_seconds * 1000.0)
                ),
                "collector_poll_duration_max_ms": int(
                    round(self._poll_duration_max_seconds * 1000.0)
                ),
                "collector_poll_utilization_percent": int(round(utilization_ratio * 100.0)),
                "collector_poll_overrun_streak": self._collector_poll_overrun_streak,
                "collector_poll_high_utilization_streak": self._collector_poll_high_utilization_streak,
                "collector_poll_recommended_min_interval_seconds": recommended,
            }
        )
        detection_retry_interval = float(
            getattr(self, "_poll_non_runtime_retry_interval_seconds", 0.0) or 0.0
        )
        if (
            poll_context != _COLLECTOR_POLL_CONTEXT_RUNTIME
            and detection_retry_interval > 0.0
        ):
            values["collector_poll_detection_retry_interval_seconds"] = int(
                math.ceil(detection_retry_interval)
            )
        else:
            values.pop("collector_poll_detection_retry_interval_seconds", None)

        if (
            self._configured_poll_mode() == POLL_MODE_MANUAL
            and runtime_poll
            and self._collector_poll_high_utilization_streak
            >= _POLL_STABLE_STREAK_THRESHOLD
        ):
            self._notify_poll_high_utilization(
                poll_interval=int(math.ceil(interval)),
                recommended_interval=recommended,
                utilization_ratio=utilization_ratio,
            )

        if (
            runtime_poll
            and clean_poll
            and utilization_ratio < _POLL_UTILIZATION_WARNING_RATIO
        ):
            self._poll_normal_utilization_streak = (
                int(getattr(self, "_poll_normal_utilization_streak", 0) or 0) + 1
            )
            if self._poll_normal_utilization_streak >= _POLL_STABLE_STREAK_THRESHOLD:
                self._dismiss_poll_high_utilization_notification()
        elif runtime_poll and clean_poll:
            self._poll_normal_utilization_streak = 0

    def _sync_fixed_rate_poll_update_interval(
        self,
        snapshot: RuntimeSnapshot,
        *,
        poll_interval_seconds: float,
        duration_seconds: float,
        scheduler_mode: str = POLL_MODE_MANUAL,
    ) -> None:
        """Keep configured poll interval as start-to-start target.

        Home Assistant's DataUpdateCoordinator sleeps ``update_interval`` after
        ``_async_update_data`` completes.  Without compensating for the refresh
        duration, a configured 10s poll with a 5s refresh becomes a ~15s
        start-to-start cadence.  Store the configured poll interval as the user
        target and make HA's internal post-refresh delay the remaining time.
        """

        interval = clamp_interval(poll_interval_seconds)
        try:
            duration = max(0.0, float(duration_seconds))
        except (TypeError, ValueError):
            duration = 0.0
        delay = max(_POLL_FIXED_RATE_MIN_DELAY_SECONDS, float(interval) - duration)
        self.update_interval = timedelta(seconds=delay)
        values = getattr(snapshot, "values", None)
        if isinstance(values, dict):
            values["collector_poll_effective_update_delay_ms"] = int(
                round(delay * 1000.0)
            )
            values["collector_poll_effective_update_delay_seconds"] = round(delay, 3)
            values["collector_poll_scheduler_mode"] = "fixed_rate"

    def _poll_high_utilization_notification_id(self) -> str:
        return f"{DOMAIN}_poll_interval_high_utilization_{self.config_entry.entry_id}"

    def _dismiss_poll_high_utilization_notification(self) -> None:
        """Retract the warning once polling is sustainably back within budget."""

        if not getattr(self, "_poll_notification_active", False):
            return
        self._poll_notification_active = False
        try:
            persistent_notification.async_dismiss(
                self.hass,
                self._poll_high_utilization_notification_id(),
            )
        except Exception:
            logger.debug(
                "Failed to dismiss poll high-utilization notification",
                exc_info=True,
            )

    def _notify_poll_high_utilization(
        self,
        *,
        poll_interval: int,
        recommended_interval: int,
        utilization_ratio: float,
    ) -> None:
        self._poll_normal_utilization_streak = 0
        now = asyncio.get_running_loop().time()
        if (
            float(getattr(self, "_poll_last_notification_monotonic", 0.0) or 0.0)
            > 0.0
            and now - float(getattr(self, "_poll_last_notification_monotonic", 0.0) or 0.0)
            < _POLL_NOTIFICATION_COOLDOWN_SECONDS
        ):
            return
        self._poll_last_notification_monotonic = now
        # Marked active only when a notification is actually created, so a
        # later dismiss never targets a notification that was throttled away.
        self._poll_notification_active = True
        persistent_notification.async_create(
            self.hass,
            _localized_runtime_text(
                self.hass,
                "poll_interval_high_utilization_body",
                poll_interval=poll_interval,
                recommended_interval=recommended_interval,
                utilization_percent=int(round(utilization_ratio * 100.0)),
            ),
            title=_localized_runtime_text(
                self.hass,
                "poll_interval_high_utilization_title",
            ),
            notification_id=self._poll_high_utilization_notification_id(),
        )

    async def _async_update_data_with_runtime_lock(
        self,
        *,
        poll_interval_seconds: float | None = None,
    ) -> RuntimeSnapshot:
        """Refresh runtime data while holding the shared transport operation lock."""

        self._ensure_poll_scheduler()
        poll_interval = float(
            poll_interval_seconds
            if poll_interval_seconds is not None
            else self._poll_scheduler.current_interval()
        )
        # Per-phase wall-clock timing: poll cycles have repeatedly turned out
        # to be dominated by phases nobody suspected; the breakdown makes the
        # next "why is the cycle 60s" question a sensor read, not a tcpdump.
        _phase_timings: dict[str, int] = {}
        _loop = asyncio.get_running_loop()

        async def _timed(phase: str, coro):
            phase_started = _loop.time()
            try:
                return await coro
            finally:
                _phase_timings[phase] = _phase_timings.get(phase, 0) + int(
                    round((_loop.time() - phase_started) * 1000.0)
                )

        await _timed("network_reconcile", self._async_reconcile_network(reason="refresh"))
        await _timed(
            "session_profile",
            self._async_reconcile_collector_session_profile(reason="refresh"),
        )
        snapshot = await _timed(
            "runtime_refresh",
            self._runtime.async_refresh(poll_interval=poll_interval),
        )
        snapshot = await _timed(
            "snapshot_profile",
            self._async_prepare_runtime_snapshot_profile(snapshot),
        )
        if await _timed(
            "session_profile",
            self._async_reconcile_collector_session_profile(
                reason="post_refresh_profile_discovery"
            ),
        ):
            snapshot = await _timed(
                "runtime_refresh",
                self._runtime.async_refresh(poll_interval=poll_interval),
            )
            snapshot = await _timed(
                "snapshot_profile",
                self._async_prepare_runtime_snapshot_profile(snapshot),
            )
        await _timed(
            "endpoint_reconcile",
            self._async_reconcile_collector_operation_mode_endpoint(snapshot),
        )
        snapshot.values["collector_poll_phase_breakdown"] = ", ".join(
            f"{phase}={elapsed_ms}ms"
            for phase, elapsed_ms in sorted(
                _phase_timings.items(), key=lambda item: -item[1]
            )
        )
        snapshot.values["connection_type"] = self.config_entry.data.get(CONF_CONNECTION_TYPE, "eybond")
        snapshot.values["collector_operation_mode"] = self.collector_operation_mode
        snapshot.values["detection_confidence"] = self.detection_confidence
        snapshot.values["control_mode"] = self.control_mode
        snapshot.values["controls_enabled"] = self.controls_enabled
        snapshot.values["control_policy_reason"] = self.controls_reason
        snapshot.values["control_policy_summary"] = self.controls_summary
        write_exposure_context = self._write_exposure_context()
        snapshot.values["effective_variant_key"] = write_exposure_context["variant_key"]
        snapshot.values["effective_profile_name"] = write_exposure_context["profile_name"]
        snapshot.values["effective_profile_source_scope"] = write_exposure_context[
            "profile_source_scope"
        ]
        snapshot.values["effective_schema_source_scope"] = write_exposure_context[
            "schema_source_scope"
        ]
        snapshot.values["effective_device_scoped_overlay_active"] = write_exposure_context[
            "device_scoped_overlay_active"
        ]
        snapshot.values["effective_device_scoped_overlay_scope"] = write_exposure_context[
            "device_scoped_overlay_scope"
        ]
        # Store as a sorted list, not the raw frozenset: snapshot values are serialized
        # to JSON for the support package, and a frozenset is not JSON-serializable
        # (it raised "Object of type frozenset is not JSON serializable" and blocked
        # every export once a selection existed). The reader accepts list/tuple/set.
        _selected_control_keys = write_exposure_context["selected_control_keys"]
        snapshot.values["effective_device_scoped_overlay_selected_control_keys"] = (
            sorted(_selected_control_keys) if _selected_control_keys is not None else None
        )
        snapshot.values["effective_capabilities_experimental"] = write_exposure_context[
            "effective_capabilities_experimental"
        ]
        # Diagnostics: what the device-overlay merge decided this cycle, and the resulting
        # inverter capability picture. Surfaced into the support package so the merge is
        # observable on-device instead of inferred (the support bundle does not otherwise
        # serialize inverter.capabilities).
        _runtime_inverter = getattr(snapshot, "inverter", None)
        _runtime_capabilities = tuple(getattr(_runtime_inverter, "capabilities", ()) or ())
        _learned_capability_keys = sorted(
            str(getattr(capability, "key", ""))
            for capability in _runtime_capabilities
            if getattr(capability, "is_device_scoped_experimental", False)
        )
        if _selected_control_keys is None:
            _exposed_learned_capability_keys = (
                _learned_capability_keys
                if write_exposure_context["device_scoped_overlay_active"]
                else []
            )
        else:
            _exposed_learned_capability_keys = [
                key for key in _learned_capability_keys if key in _selected_control_keys
            ]
        snapshot.values["effective_overlay_merge_status"] = self._device_overlay_merge_status
        snapshot.values["effective_inverter_capability_count"] = len(_runtime_capabilities)
        snapshot.values["effective_inverter_all_learned_capability_keys"] = _learned_capability_keys
        snapshot.values["effective_inverter_exposed_learned_capability_keys"] = (
            _exposed_learned_capability_keys
        )
        snapshot.values["effective_inverter_learned_capability_keys"] = _learned_capability_keys
        snapshot.values.update(self._support_workflow_values(snapshot))
        snapshot.values.update(self._collector_onboarding_values(snapshot))
        snapshot.values.update(self._tooling_values)
        snapshot.values.update(await self._proxy_capture_values(snapshot))
        self._prune_hidden_collector_values_for_mode(snapshot)
        from .. import _async_self_heal_sensor_display_precision

        await _async_self_heal_sensor_display_precision(self.hass, self.config_entry)
        self.async_sync_device_registry(snapshot)
        return snapshot

    async def _async_prepare_runtime_snapshot_profile(
        self,
        snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """Persist runtime-learned profile facts before transport-profile reconcile."""

        snapshot = await self._async_reconcile_proxy_capture_session(snapshot)
        snapshot = await self._async_reconcile_shadow_learning_session(snapshot)
        await self._async_restore_collector_original_endpoint_from_registry(snapshot)
        await self._async_remember_collector_server_endpoint(snapshot)
        await self._async_remember_runtime_identity(snapshot)
        # Keep self.data aligned with the fresh snapshot before helpers that
        # inspect coordinator state instead of the local snapshot argument.
        self.data = snapshot
        self._sync_forced_collector_operation_mode()
        self._configure_reverse_discovery_mode()
        await self._async_warm_effective_metadata_cache()
        collector_cloud_family = self.collector_cloud_family
        if collector_cloud_family:
            snapshot.values["collector_cloud_family"] = collector_cloud_family
        collector_cloud_profile_key = self.collector_cloud_profile_key
        if collector_cloud_profile_key:
            snapshot.values["collector_cloud_profile_key"] = collector_cloud_profile_key
        collector_cloud_profile_label = self.collector_cloud_profile_label
        if collector_cloud_profile_label:
            snapshot.values["collector_cloud_profile_label"] = collector_cloud_profile_label
        collector_cloud_profile_source = self.collector_cloud_profile_source
        if collector_cloud_profile_source:
            snapshot.values["collector_cloud_profile_source"] = collector_cloud_profile_source
        collector_cloud_profile_confidence = self.collector_cloud_profile_confidence
        if collector_cloud_profile_confidence:
            snapshot.values["collector_cloud_profile_confidence"] = (
                collector_cloud_profile_confidence
            )
        return snapshot

    def _maybe_persist_unsupported_commands(self, snapshot: RuntimeSnapshot) -> None:
        """Persist the empirically learned unsupported-command set on change."""

        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return
        raw = values.get("driver_unsupported_commands")
        if not isinstance(raw, str) or not raw.strip():
            return
        commands = sorted(
            command.strip()
            for command in raw.split(",")
            if command.strip()
        )
        stored = self.config_entry.options.get(_UNSUPPORTED_COMMANDS_OPTION_KEY)
        stored_version = self.config_entry.options.get(_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY)
        if (
            stored_version == _UNSUPPORTED_COMMANDS_OPTION_VERSION
            and isinstance(stored, (list, tuple))
            and sorted(stored) == commands
        ):
            return
        set_unsupported = getattr(
            self._runtime,
            "set_persistent_unsupported_commands",
            None,
        )
        if callable(set_unsupported):
            set_unsupported(tuple(commands))
        options = dict(self.config_entry.options)
        options[_UNSUPPORTED_COMMANDS_OPTION_KEY] = commands
        options[_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY] = (
            _UNSUPPORTED_COMMANDS_OPTION_VERSION
        )
        self._async_update_entry_without_reload(options=options)
        logger.info(
            "Persisted unsupported inverter commands for this device: %s",
            ", ".join(commands),
        )

    async def async_recheck_supported_commands(self) -> None:
        """Forget the learned unsupported-command set and re-probe everything."""

        clear_cache = getattr(self._runtime, "clear_unsupported_command_cache", None)
        if callable(clear_cache):
            clear_cache()
        options = dict(self.config_entry.options)
        removed_commands = options.pop(_UNSUPPORTED_COMMANDS_OPTION_KEY, None) is not None
        removed_version = (
            options.pop(_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY, None) is not None
        )
        if removed_commands or removed_version:
            self._async_update_entry_without_reload(options=options)
        await self.async_request_refresh()

    def _on_collector_connection_established(self, remote_ip: str) -> None:
        """Refresh immediately when the collector dials back in.

        Without this the reconnected collector sits idle until the next
        scheduled poll, which after failed detection cycles can be more than
        a minute away (non-runtime retry backoff).
        """

        if getattr(self, "_shutdown_complete", False):
            return
        snapshot = self.data if isinstance(self.data, RuntimeSnapshot) else None
        if snapshot is not None:
            runtime_driver_state = _runtime_driver_state_from_snapshot(snapshot)
            if snapshot.connected and runtime_driver_state == _RUNTIME_DRIVER_STATE_DRIVER_BOUND:
                return
        logger.debug(
            "Collector connection from %s while not bound; requesting immediate refresh",
            remote_ip,
        )
        self.hass.async_create_task(self.async_request_refresh())

    def _publish_runtime_intermediate_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        """Publish collector state while the runtime is still probing the inverter."""

        if getattr(self, "_shutdown_complete", False):
            return
        snapshot.values["connection_type"] = self.config_entry.data.get(
            CONF_CONNECTION_TYPE,
            "eybond",
        )
        snapshot.values["collector_operation_mode"] = self.collector_operation_mode
        snapshot.values["detection_confidence"] = self.detection_confidence
        snapshot.values["control_mode"] = self.control_mode
        snapshot.values["controls_enabled"] = self.controls_enabled
        snapshot.values["control_policy_reason"] = self.controls_reason
        snapshot.values["control_policy_summary"] = self.controls_summary
        self.data = snapshot
        self.async_set_updated_data(snapshot)

    async def async_write_capability(self, capability_key: str, value: Any) -> Any:
        """Write one inverter capability and refresh coordinator state."""

        inverter = self.data.inverter
        if inverter is None:
            raise RuntimeError("inverter_not_detected")
        capability = inverter.get_capability(capability_key)
        if not self.can_expose_capability(capability):
            raise PermissionError(
                f"capability_control_disabled:{capability.key}:{self.controls_reason}"
            )
        try:
            # Serialize the control write against the polling loop: both take
            # _runtime_operation_lock so a write and a refresh never interleave
            # Modbus frames on the shared transport (which could cross-correlate
            # the write read-back with a poll response on a safety-critical
            # write). The follow-up refresh is only scheduled here, so it takes
            # the lock later in its own task — no re-entrancy.
            async with self._runtime_operation_lock:
                written_value = await self._runtime.async_write_capability(capability_key, value)
        except Exception:
            await self.async_request_refresh()
            raise
        await self.async_request_refresh()
        return written_value

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        """Apply one declarative preset and refresh coordinator state."""

        inverter = self.data.inverter
        if inverter is None:
            raise RuntimeError("inverter_not_detected")
        preset = inverter.get_capability_preset(preset_key)
        if not self.can_expose_preset(preset):
            raise PermissionError(
                f"preset_control_disabled:{preset.key}:{self.controls_reason}"
            )
        try:
            # Serialize against the polling loop on the shared transport (see
            # async_write_capability for the rationale).
            async with self._runtime_operation_lock:
                result = await self._runtime.async_apply_preset(preset_key)
        except Exception:
            await self.async_request_refresh()
            raise
        await self.async_request_refresh()
        return result

    async def async_sync_inverter_clock(self) -> dict[str, str]:
        """Write the current Home Assistant local date/time into the inverter clock."""

        now = dt_util.now().replace(microsecond=0)
        date_value = now.strftime("%Y-%m-%d")
        time_value = now.strftime("%H:%M:%S")

        await self.async_write_capability("inverter_date_write", date_value)
        await self.async_write_capability("inverter_time_write", time_value)

        return {
            "inverter_date": date_value,
            "inverter_time": time_value,
        }

    async def async_set_collector_server_endpoint(
        self,
        *,
        server_host: str,
        server_port: int,
        server_protocol: str = "TCP",
        apply_changes: bool = True,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Stage or apply collector parameter 21 behind an explicit full-control gate."""

        if self.control_mode != CONTROL_MODE_FULL:
            raise PermissionError(
                f"collector_control_disabled:{self.control_mode}:{self.controls_reason}"
            )
        if not confirm_redirect:
            raise ValueError("collector_server_reconfig_requires_confirmation")

        endpoint = _format_collector_server_endpoint(
            server_host=server_host,
            server_port=server_port,
            server_protocol=server_protocol,
        )
        return await self.async_set_raw_collector_server_endpoint(
            endpoint=endpoint,
            apply_changes=apply_changes,
            confirm_redirect=True,
        )

    async def async_set_raw_collector_server_endpoint(
        self,
        *,
        endpoint: str,
        apply_changes: bool = True,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Stage or apply collector parameter 21 using the caller's raw endpoint shape."""

        if self.control_mode != CONTROL_MODE_FULL:
            raise PermissionError(
                f"collector_control_disabled:{self.control_mode}:{self.controls_reason}"
            )
        if not confirm_redirect:
            raise ValueError("collector_server_reconfig_requires_confirmation")
        lock_code = self.collector_configuration_lock_code()
        if lock_code is not None:
            raise RuntimeError(lock_code)

        normalized_endpoint = _normalize_preserved_collector_server_endpoint(endpoint)
        await self._async_prepare_home_assistant_callback_listener(normalized_endpoint)
        result = await self._runtime.async_set_collector_server_endpoint(
            normalized_endpoint,
            apply_changes=apply_changes,
        )
        if not apply_changes:
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=normalized_endpoint,
                collector_callback_endpoint_pending_apply_required=True,
            )
            await self.async_request_refresh()
        else:
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=None,
                collector_callback_endpoint_pending_apply_required=None,
            )
        return result

    async def async_bind_collector_to_home_assistant(
        self,
        *,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Move the collector callback endpoint back to this Home Assistant listener."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_redirect:
            raise ValueError("collector_bind_home_assistant_requires_confirmation")

        target_endpoint = self.collector_callback_target_endpoint
        current_endpoint = str(self.data.values.get("collector_server_endpoint") or "").strip()
        if current_endpoint == target_endpoint:
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=None,
                collector_callback_endpoint_pending_apply_required=None,
            )
            return {
                "status": "already_bound",
                "requested_endpoint": target_endpoint,
                "readback_endpoint": target_endpoint,
                "target_role": "home_assistant",
            }

        await self._async_prepare_home_assistant_callback_listener(target_endpoint)
        result = await self._runtime.async_set_collector_server_endpoint(
            target_endpoint,
            apply_changes=True,
        )
        result["target_role"] = "home_assistant"
        self._publish_snapshot_values(
            collector_callback_endpoint_pending=None,
            collector_callback_endpoint_pending_apply_required=None,
        )
        return result

    async def async_apply_collector_changes(
        self,
        *,
        confirm_restart: bool = False,
    ) -> dict[str, object]:
        """Apply staged collector-side config changes behind an explicit full-control gate."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_restart:
            raise ValueError("collector_apply_requires_confirmation")
        result = await self._runtime.async_apply_collector_changes()
        self._publish_snapshot_values(
            collector_callback_endpoint_pending=None,
            collector_callback_endpoint_pending_apply_required=None,
        )
        return result

    async def async_trigger_collector_rediscovery(self) -> dict[str, object]:
        """Send one explicit bootstrap discovery probe to recover collector connectivity."""

        lock_code = self.collector_configuration_lock_code()
        if lock_code in {
            "collector_configuration_proxy_transition_active",
            "collector_configuration_proxy_session_active",
        }:
            raise RuntimeError(lock_code)

        target_endpoint = self.collector_callback_target_endpoint
        if target_endpoint:
            await self._async_prepare_home_assistant_callback_listener(target_endpoint)

        result = await self._runtime.async_trigger_reverse_discovery()
        result.setdefault("target_role", "bootstrap")
        result["collector_callback_target_endpoint"] = target_endpoint
        await self.async_request_refresh()
        return result

    async def async_reboot_collector(
        self,
        *,
        confirm_restart: bool = False,
    ) -> dict[str, object]:
        """Trigger one collector reboot-intent action behind an explicit full-control gate."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_restart:
            raise ValueError("collector_reboot_requires_confirmation")
        return await self._runtime.async_reboot_collector()

    async def async_rollback_collector_server_endpoint(
        self,
        *,
        apply_changes: bool = True,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Rollback collector parameter 21 to the remembered original external endpoint."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_redirect:
            raise ValueError("collector_rollback_requires_confirmation")

        rollback_endpoint = self.collector_server_endpoint_rollback_target
        if not rollback_endpoint:
            raise RuntimeError("collector_rollback_endpoint_unavailable")

        runtime_target = str(
            getattr(self._runtime, "collector_server_endpoint_rollback_target", "") or ""
        ).strip()
        rollback_source = (
            "session_cached_previous_endpoint"
            if runtime_target and runtime_target == rollback_endpoint
            else "remembered_original_endpoint"
        )

        result = await self._runtime.async_set_collector_server_endpoint(
            rollback_endpoint,
            apply_changes=apply_changes,
        )
        result["status"] = "rollback_applied" if apply_changes else "rollback_staged"
        result["rollback_source"] = rollback_source
        result["rollback_endpoint"] = rollback_endpoint
        result.setdefault("target_role", "smartess")
        if not apply_changes:
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=rollback_endpoint,
                collector_callback_endpoint_pending_apply_required=True,
            )
            await self.async_request_refresh()
        else:
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=None,
                collector_callback_endpoint_pending_apply_required=None,
            )
        return result

    async def async_start_proxy_capture(
        self,
        *,
        anonymized: bool = True,
        confirm_redirect: bool = False,
        duration_minutes: int | None = None,
    ) -> dict[str, object]:
        """Start one live collector proxy capture session."""

        mode_apply_lock_code = self.collector_operation_mode_apply_lock_code()
        if mode_apply_lock_code is not None:
            raise RuntimeError(mode_apply_lock_code)

        overview = self.proxy_capture_overview
        if not overview.can_start:
            raise RuntimeError(str(overview.blocking_reason or "proxy_capture_not_ready"))
        if overview.redirect_required and not confirm_redirect:
            raise ValueError("proxy_capture_redirect_requires_confirmation")
        active_shadow_state = await self._async_active_shadow_learning_state(require_process=False)
        if active_shadow_state is not None and shadow_learning_session_is_active(active_shadow_state):
            raise RuntimeError("shadow_learning_route_running")
        if self._shadow_learning_process_running():
            raise RuntimeError("shadow_learning_route_running")
        if self._proxy_capture_process_running():
            raise RuntimeError("proxy_capture_already_running")

        upstream_endpoint = self.proxy_capture_upstream_endpoint
        if not upstream_endpoint:
            raise RuntimeError("proxy_capture_upstream_endpoint_unavailable")

        upstream_host, upstream_port, _upstream_protocol = _resolve_collector_server_endpoint(
            upstream_endpoint,
            cloud_family=self.collector_cloud_family,
        )
        target_host, target_port, target_protocol = _resolve_collector_server_endpoint(
            overview.target_endpoint,
            cloud_family=self.collector_cloud_family,
        )

        configured_duration_minutes = _coerce_proxy_capture_duration_minutes(
            duration_minutes
            if duration_minutes is not None
            else self.proxy_capture_configured_duration_minutes
        )
        if configured_duration_minutes != self.proxy_capture_configured_duration_minutes:
            await self.async_set_proxy_capture_duration_minutes(configured_duration_minutes)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        route_owner_id = f"proxy_capture:{self.config_entry.entry_id}:{timestamp}"
        trace_path = await self.hass.async_add_executor_job(
            lambda: build_proxy_capture_trace_path(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                collector_pn=self.smartess_collector_pn,
                timestamp=timestamp,
            )
        )
        restore_trigger_path = build_proxy_capture_restore_trigger_path(trace_path)
        try:
            await self.hass.async_add_executor_job(restore_trigger_path.unlink)
        except FileNotFoundError:
            pass
        started_at = datetime.now(timezone.utc).isoformat()
        state = build_proxy_capture_session_state(
            entry_id=self.config_entry.entry_id,
            route_owner_id=route_owner_id,
            collector_pn=self.smartess_collector_pn,
            trace_path=str(trace_path),
            original_endpoint=overview.current_endpoint,
            proxy_endpoint=overview.target_endpoint,
            restore_required=overview.redirect_required,
            anonymized=anonymized,
            started_at=started_at,
            expires_at=build_proxy_capture_lease_deadline(
                lease_seconds=configured_duration_minutes * 60,
            ),
            status="starting",
        )
        await self._async_save_proxy_capture_session_state(state)
        self._publish_tooling_values(
            **self._proxy_capture_overview_runtime_values(active_state=state),
            proxy_trace_saved_result_path="",
            proxy_trace_saved_result_download_url="",
            proxy_trace_manifest_download_url="",
            local_metadata_status="Starting collector proxy capture",
        )

        route_started = False
        try:
            await self._async_preflight_proxy_capture_network(
                target_host=target_host,
                target_port=target_port,
                upstream_host=upstream_host,
                upstream_port=upstream_port,
            )

            async def _async_open_proxy_trace_output(path: Path):
                return await self.hass.async_add_executor_job(
                    open_proxy_trace_output_file,
                    path,
                )

            async def _async_close_proxy_trace_output(output):
                await self.hass.async_add_executor_job(output.close)

            await self._runtime.async_start_proxy_capture_route(
                owner_id=route_owner_id,
                entry_id=self.config_entry.entry_id,
                collector_ip=self._proxy_capture_collector_ip(),
                collector_pn=self.smartess_collector_pn,
                collector_session_protocol=self.collector_session_protocol,
                listen_port=target_port,
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                output_path=trace_path,
                masked_endpoint=self.proxy_capture_upstream_endpoint,
                restore_trigger_path=restore_trigger_path,
                async_open_output=_async_open_proxy_trace_output,
                async_close_output=_async_close_proxy_trace_output,
            )
            route_started = True
            if overview.redirect_required:
                await self._runtime.async_set_collector_server_endpoint(
                    overview.target_endpoint,
                    apply_changes=True,
                )
            else:
                disconnect_current = getattr(
                    self._runtime,
                    "async_disconnect_collector_connections",
                    None,
                )
                if disconnect_current is not None:
                    await disconnect_current(reason="proxy_capture_start")
            await self._async_wait_for_proxy_capture_reconnect(trace_path)
            running_state = build_proxy_capture_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                restore_required=state.restore_required,
                anonymized=state.anonymized,
                started_at=state.started_at,
                expires_at=state.expires_at,
                status="running",
            )
            await self._async_save_proxy_capture_session_state(running_state)
        except Exception as exc:
            error_text = str(exc or "").strip()
            error_code = error_text.split(":", 1)[0] if error_text else type(exc).__name__
            if overview.redirect_required and overview.current_endpoint:
                await self._async_best_effort_restore_after_start_failure(overview.current_endpoint)
            if route_started:
                await self._async_stop_proxy_capture_process(owner_id=state.route_owner_id)
            await self._async_clear_proxy_capture_session_state()
            try:
                await self.async_request_refresh()
            except Exception as exc:
                logger.warning(
                    "Proxy capture failure refresh failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
            self._publish_tooling_values(
                **self._proxy_capture_overview_runtime_values(),
                proxy_capture_start_error=error_text,
                proxy_capture_start_error_code=error_code,
                proxy_capture_start_error_type=type(exc).__name__,
                local_metadata_status="Collector proxy capture failed to start",
            )
            raise

        await self.async_request_refresh()
        self._publish_tooling_values(
            **self._proxy_capture_overview_runtime_values(active_state=running_state),
            local_metadata_status="Collector proxy capture running",
        )
        return {
            "status": "running",
            "trace_path": str(trace_path),
            "redirect_required": overview.redirect_required,
            "masked_endpoint": overview.masked_endpoint,
            "duration_minutes": configured_duration_minutes,
        }

    async def async_stop_proxy_capture(
        self,
        *,
        reason: str = "stopped",
        prefer_proxy_restore_trigger: bool = True,
        request_refresh: bool = True,
    ) -> dict[str, object]:
        """Stop one live collector proxy capture session and finalize its manifest."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None:
            raise RuntimeError("proxy_capture_not_running")

        config_dir = Path(self.hass.config.config_dir)
        stopping_state = build_proxy_capture_session_state(
            entry_id=state.entry_id,
            route_owner_id=state.route_owner_id,
            collector_pn=state.collector_pn,
            trace_path=state.trace_path,
            original_endpoint=state.original_endpoint,
            proxy_endpoint=state.proxy_endpoint,
            restore_required=state.restore_required,
            anonymized=state.anonymized,
            started_at=state.started_at,
            expires_at=state.expires_at,
            status="stopping",
        )
        await self._async_save_proxy_capture_session_state(stopping_state)
        self._publish_tooling_values(
            **self._proxy_capture_overview_runtime_values(active_state=stopping_state),
            local_metadata_status=self._proxy_capture_local_status(reason, phase="stopping")
        )

        restore_info = await self._async_guarded_proxy_capture_restore(
            state=state,
            prefer_proxy_restore_trigger=prefer_proxy_restore_trigger,
        )
        restored_endpoint = str(restore_info.get("restored_endpoint") or "")
        restore_confirmed = bool(restore_info.get("restore_confirmed"))
        restore_mode = str(restore_info.get("restore_mode") or "")
        restore_skipped_reason = str(restore_info.get("restore_skipped_reason") or "")
        current_endpoint = str(restore_info.get("current_endpoint") or "")

        if state.restore_required and state.original_endpoint and restore_mode in {"proxy_trigger", "direct"}:
            restoring_state = build_proxy_capture_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                restore_required=state.restore_required,
                anonymized=state.anonymized,
                started_at=state.started_at,
                expires_at=state.expires_at,
                status="restoring",
            )
            await self._async_save_proxy_capture_session_state(restoring_state)
            self._publish_tooling_values(
                **self._proxy_capture_overview_runtime_values(
                    active_state=restoring_state,
                    current_endpoint=current_endpoint,
                ),
                local_metadata_status=self._proxy_capture_local_status(reason, phase="stopping"),
            )

        trace_path = Path(state.trace_path)
        result_status = self._proxy_capture_result_status(reason, restore_confirmed=restore_confirmed)
        manifest_path = await self.hass.async_add_executor_job(
            lambda: export_proxy_trace_manifest(
                config_dir=config_dir,
                manifest=build_proxy_trace_manifest(
                    source="collector_proxy_capture",
                    trace_path=str(trace_path),
                    entry_id=self.config_entry.entry_id,
                    collector_pn=self.smartess_collector_pn,
                    anonymized=state.anonymized,
                    session={
                        "started_at": state.started_at,
                        "stopped_at": datetime.now(timezone.utc).isoformat(),
                        "original_endpoint": state.original_endpoint,
                        "proxy_endpoint": state.proxy_endpoint,
                        "current_endpoint": current_endpoint,
                        "restore_required": state.restore_required,
                        "restored_endpoint": restored_endpoint,
                        "restore_confirmed": restore_confirmed,
                        "restore_mode": restore_mode,
                        "restore_skipped_reason": restore_skipped_reason,
                        "final_status": result_status,
                    },
                    summary=summarize_proxy_capture_trace(trace_path),
                ),
            )
        )
        bundle_path = await self.hass.async_add_executor_job(
            lambda: export_proxy_trace_bundle(
                manifest_path=manifest_path,
                overwrite=True,
            )
        )
        _download_path, relative_download_url = await self.hass.async_add_executor_job(
            lambda: publish_proxy_trace_download_copy(
                config_dir=config_dir,
                source_path=bundle_path,
            )
        )
        download_url = self._absolute_local_download_url(relative_download_url)
        await self._async_clear_proxy_capture_session_state()
        if request_refresh:
            await self.async_request_refresh()
        final_proxy_values = self._proxy_capture_overview_runtime_values(current_endpoint=current_endpoint)
        final_proxy_values["proxy_trace_path"] = str(trace_path)
        final_proxy_values["proxy_trace_manifest_path"] = str(manifest_path)
        self._publish_tooling_values(
            **final_proxy_values,
            proxy_trace_saved_result_path=str(bundle_path),
            proxy_trace_saved_result_download_url=download_url,
            proxy_trace_manifest_download_url=download_url,
            local_metadata_status=self._proxy_capture_local_status(reason, phase="finished"),
        )
        persistent_notification.async_create(
            self.hass,
            _localized_runtime_text(
                self.hass,
                "proxy_capture_notification_body" if download_url else "proxy_capture_notification_body_no_link",
                download_url=download_url,
                saved_path=str(bundle_path),
            ),
            title=_localized_runtime_text(self.hass, "proxy_capture_notification_title"),
            notification_id=_proxy_capture_notification_id(
                self.config_entry.entry_id,
                bundle_path,
            ),
        )
        if not restore_confirmed:
            self._notify_proxy_capture_restore_unconfirmed()
        return {
            "status": result_status,
            "trace_path": str(trace_path),
            "manifest_path": str(manifest_path),
            "saved_result_path": str(bundle_path),
            "saved_result_download_url": download_url,
            "restored_endpoint": restored_endpoint,
            "restore_mode": restore_mode,
            "restore_skipped_reason": restore_skipped_reason,
            "current_endpoint": current_endpoint,
        }

    def _shadow_learning_main_redirect(self, callback_endpoint: str) -> tuple[str, bool]:
        """Return (restore_endpoint, redirect_required) for the scan's main-endpoint switch.

        SAFETY: the collector's MAIN (param 21) endpoint must be moved to the local proxy for
        the whole session. In "SmartESS + HA" the HA callback is *additive* -- it does NOT
        rewrite param 21 -- so the collector keeps a live link to the real cloud, and a
        mid-scan reconnect/reboot lets a real ctrlDevice command reach the inverter. Drive the
        redirect (and the restore target) off the REAL upstream endpoint (the remembered
        rollback target / upstream), not the possibly-already-HA live endpoint: gating on the
        live endpoint would skip the param-21 switch (leaving the collector on the real server)
        and would later restore to HA, stranding the collector off SmartESS.
        """

        current_endpoint = str(self.data.values.get("collector_server_endpoint") or "").strip()
        return _resolve_shadow_learning_main_redirect(
            home_assistant_primary=self.collector_home_assistant_primary,
            current_endpoint=current_endpoint,
            rollback_target=self.collector_server_endpoint_rollback_target,
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            callback_endpoint=callback_endpoint,
        )

    async def async_start_shadow_learning(
        self,
        *,
        output_path: Path | None = None,
        raw_capture: dict[str, Any] | None = None,
        allow_ack_writes: bool = False,
    ) -> dict[str, object]:
        """Start one fail-closed shadow-learning runtime session."""

        active_proxy_state = await self._async_active_proxy_capture_state(require_process=False)
        if active_proxy_state is not None and proxy_capture_session_is_active(active_proxy_state):
            raise RuntimeError("proxy_capture_route_running")
        if self._runtime.proxy_capture_route_running():
            raise RuntimeError("proxy_capture_route_running")
        if self._shadow_learning_process_running():
            raise RuntimeError("shadow_learning_already_running")

        add_executor_job = getattr(
            getattr(self, "hass", None),
            "async_add_executor_job",
            None,
        )
        if callable(add_executor_job):
            available_mib = await add_executor_job(read_available_memory_mib)
        else:
            available_mib = read_available_memory_mib()
        memory_blocker = shadow_learning_memory_blocker(available_mib)
        if memory_blocker:
            raise RuntimeError(f"shadow_learning_preflight_blocked:{memory_blocker}")

        if raw_capture is None and self.data.connected:
            try:
                raw_capture = await self._runtime.async_capture_support_evidence()
            except Exception as exc:
                logger.debug("Shadow-learning support capture unavailable: %s", exc)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        trace_path = (
            Path(output_path)
            if output_path is not None
            else await self.hass.async_add_executor_job(
                lambda: build_shadow_learning_trace_path(
                    config_dir=Path(self.hass.config.config_dir),
                    entry_id=self.config_entry.entry_id,
                    collector_pn=self.smartess_collector_pn,
                    timestamp=timestamp,
                )
            )
        )

        seed, blockers = build_shadow_learning_seed(
            session_id=f"{self.config_entry.entry_id}_{timestamp}",
            entry_id=self.config_entry.entry_id,
            collector_pn=self.smartess_collector_pn,
            collector_cloud_family=self.collector_cloud_family,
            raw_passthrough_frame_format=self.collector_raw_passthrough_frame_format,
            collector_cloud_profile_key=self.collector_cloud_profile_key,
            collector_cloud_profile_label=self.collector_cloud_profile_label,
            collector_cloud_profile_source=self.collector_cloud_profile_source,
            collector_cloud_profile_confidence=self.collector_cloud_profile_confidence,
            collector_callback_endpoint=self.collector_callback_target_endpoint,
            effective_metadata_snapshot=self.shadow_learning_effective_metadata,
            raw_capture=raw_capture,
            write_response_mode="ack" if allow_ack_writes else "exception",
            allow_ack_writes=allow_ack_writes,
        )
        if blockers:
            raise RuntimeError("shadow_learning_preflight_blocked:" + ",".join(blockers))
        route_owner_id = str(
            getattr(seed, "session_id", "")
            or f"shadow_learning:{self.config_entry.entry_id}:{timestamp}"
        )

        preflight = build_shadow_learning_preflight(seed)
        if not preflight.can_start:
            raise RuntimeError("shadow_learning_preflight_blocked:" + ",".join(preflight.blockers))

        callback_endpoint = self.collector_callback_target_endpoint
        upstream_endpoint = self.proxy_capture_upstream_endpoint
        if not upstream_endpoint:
            raise RuntimeError("shadow_learning_upstream_endpoint_unavailable")

        _callback_host, callback_port, _callback_protocol = _resolve_collector_server_endpoint(
            callback_endpoint,
            cloud_family=self.collector_cloud_family,
        )
        upstream_host, upstream_port, _upstream_protocol = _resolve_collector_server_endpoint(
            upstream_endpoint,
            cloud_family=self.collector_cloud_family,
        )
        await self._async_preflight_proxy_capture_network(
            target_host=self._effective_callback_server_host,
            target_port=callback_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
        )

        restore_endpoint, restore_required = self._shadow_learning_main_redirect(callback_endpoint)
        started_at = shadow_learning_session_timestamp()
        expires_at = build_shadow_learning_lease_deadline(
            lease_seconds=self.proxy_capture_configured_duration_minutes * 60,
        )
        state = build_shadow_learning_session_state(
            entry_id=self.config_entry.entry_id,
            route_owner_id=route_owner_id,
            collector_pn=self.smartess_collector_pn,
            trace_path=str(trace_path),
            original_endpoint=restore_endpoint,
            proxy_endpoint=callback_endpoint,
            upstream_endpoint=upstream_endpoint,
            restore_required=restore_required,
            started_at=started_at,
            expires_at=expires_at,
            updated_at=started_at,
            status="preflight",
        )
        await self._async_save_shadow_learning_session_state(state)
        self._publish_tooling_values(
            shadow_learning_session_status="preflight",
            shadow_learning_session_ready=False,
            shadow_learning_trace_path=str(trace_path),
            shadow_learning_proxy_endpoint=callback_endpoint,
            shadow_learning_upstream_endpoint=upstream_endpoint,
            local_metadata_status="Starting shadow-learning route",
        )

        route_started = False
        try:
            starting_state = build_shadow_learning_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                upstream_endpoint=state.upstream_endpoint,
                restore_required=state.restore_required,
                started_at=state.started_at,
                expires_at=state.expires_at,
                updated_at=shadow_learning_session_timestamp(),
                restore_attempt_count=state.restore_attempt_count,
                last_restore_attempt_at=state.last_restore_attempt_at,
                last_restore_error=state.last_restore_error,
                status="starting",
            )
            await self._async_save_shadow_learning_session_state(starting_state)
            await self._runtime.async_start_shadow_learning_route(
                owner_id=state.route_owner_id,
                entry_id=state.entry_id,
                collector_ip=self._proxy_capture_collector_ip(),
                collector_pn=self.smartess_collector_pn,
                collector_session_protocol=self.collector_session_protocol,
                listen_port=callback_port,
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                output_path=trace_path,
                seed=seed,
            )
            route_started = True

            min_ready_sequence = 0
            if restore_required:
                min_ready_sequence = int(
                    self._shadow_learning_route_status().get(
                        "collector_connection_sequence"
                    )
                    or 0
                )
                redirect_result = await self._runtime.async_set_collector_server_endpoint(
                    callback_endpoint,
                    apply_changes=True,
                )
                readback_endpoint = str(
                    redirect_result.get("readback_endpoint") or ""
                ).strip()
                if not readback_endpoint or not _collector_server_endpoints_equal(
                    readback_endpoint,
                    callback_endpoint,
                    cloud_family=self.collector_cloud_family,
                ):
                    raise RuntimeError(
                        "shadow_learning_endpoint_redirect_not_confirmed"
                    )
            else:
                disconnect_current = getattr(
                    self._runtime,
                    "async_disconnect_collector_connections",
                    None,
                )
                if disconnect_current is not None:
                    await disconnect_current(reason="shadow_learning_start")

            await self._async_wait_for_shadow_learning_ready(
                trace_path=trace_path,
                timeout_seconds=75.0,
                min_collector_connection_sequence=min_ready_sequence,
            )
            ready_state = build_shadow_learning_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                upstream_endpoint=state.upstream_endpoint,
                restore_required=state.restore_required,
                started_at=state.started_at,
                expires_at=state.expires_at,
                updated_at=shadow_learning_session_timestamp(),
                restore_attempt_count=state.restore_attempt_count,
                last_restore_attempt_at=state.last_restore_attempt_at,
                last_restore_error=state.last_restore_error,
                status="ready",
            )
            await self._async_save_shadow_learning_session_state(ready_state)
        except Exception as exc:
            restore_confirmed = True
            restore_error = ""
            if restore_required and restore_endpoint:
                restore_confirmed, restore_error = await self._async_best_effort_restore_after_start_failure(
                    restore_endpoint
                )
            if route_started:
                await self._runtime.async_stop_shadow_learning_route(
                    owner_id=state.route_owner_id,
                )
            if restore_confirmed:
                await self._async_clear_shadow_learning_session_state()
            else:
                failed_state = build_shadow_learning_session_state(
                    entry_id=state.entry_id,
                    route_owner_id=state.route_owner_id,
                    collector_pn=state.collector_pn,
                    trace_path=state.trace_path,
                    original_endpoint=state.original_endpoint,
                    proxy_endpoint=state.proxy_endpoint,
                    upstream_endpoint=state.upstream_endpoint,
                    restore_required=state.restore_required,
                    started_at=state.started_at,
                    expires_at=state.expires_at,
                    updated_at=shadow_learning_session_timestamp(),
                    restore_attempt_count=state.restore_attempt_count + 1,
                    last_restore_attempt_at=shadow_learning_session_timestamp(),
                    last_restore_error=restore_error or str(exc),
                    status="restore_failed",
                )
                await self._async_save_shadow_learning_session_state(failed_state)
            try:
                await self.async_request_refresh()
            except Exception as exc:
                logger.warning(
                    "Shadow-learning failure refresh failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
            self._publish_tooling_values(
                shadow_learning_session_status=("failed" if restore_confirmed else "restore_failed"),
                shadow_learning_session_ready=False,
                local_metadata_status="Shadow-learning route failed to start",
            )
            raise

        await self.async_request_refresh()
        self._publish_tooling_values(
            shadow_learning_session_status="ready",
            shadow_learning_session_ready=True,
            shadow_learning_trace_path=str(trace_path),
            shadow_learning_proxy_endpoint=callback_endpoint,
            shadow_learning_upstream_endpoint=upstream_endpoint,
            local_metadata_status="Shadow-learning route ready",
        )
        return {
            "status": "ready",
            "session_id": state.route_owner_id,
            "trace_path": str(trace_path),
            "collector_callback_endpoint": callback_endpoint,
            "upstream_endpoint": upstream_endpoint,
            "write_response_mode": seed.write_response_mode,
            "restore_required": restore_required,
        }

    def publish_shadow_learning_artifacts(
        self,
        *,
        plan: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        correlation: dict[str, Any] | None = None,
        trace_path: str = "",
        profile_draft_path: str = "",
        schema_draft_path: str = "",
        activation: dict[str, Any] | None = None,
        session_id: str = "",
        device_scope: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish one sanitized shadow-learning artifact bundle for support export."""

        from ..support.package import build_shadow_learning_runtime_values

        config_dir = Path(self.hass.config.config_dir).resolve()
        normalized_trace_path = _bounded_shadow_learning_artifact_path(
            config_dir=config_dir,
            value=trace_path,
            relative_root=Path(LOCAL_METADATA_DIR) / "shadow_learning_traces",
        )
        normalized_profile_path = _bounded_shadow_learning_artifact_path(
            config_dir=config_dir,
            value=profile_draft_path,
            relative_root=Path(LOCAL_METADATA_DIR) / "profiles",
        )
        normalized_schema_path = _bounded_shadow_learning_artifact_path(
            config_dir=config_dir,
            value=schema_draft_path,
            relative_root=Path(LOCAL_METADATA_DIR) / "register_schemas",
        )
        published_values = build_shadow_learning_runtime_values(
            plan=plan,
            orchestration=orchestration,
            correlation=correlation,
            trace_path=normalized_trace_path,
            profile_draft_path=normalized_profile_path,
            schema_draft_path=normalized_schema_path,
            activation=activation,
            session_id=session_id,
            device_scope=device_scope,
        )
        self._publish_tooling_values(**published_values)
        return dict(published_values["shadow_learning_artifacts"])

    async def async_stop_shadow_learning(
        self,
        *,
        reason: str = "stopped",
        request_refresh: bool = True,
        raise_when_not_running: bool = True,
        clear_failed_restore: bool = False,
    ) -> dict[str, object]:
        """Stop one in-process shadow-learning session and restore collector endpoint."""

        state = await self._async_active_shadow_learning_state(require_process=False)
        if state is None and not self._shadow_learning_process_running():
            if raise_when_not_running:
                raise RuntimeError("shadow_learning_not_running")
            return {"status": "not_running"}

        route_owner_id = str(getattr(state, "route_owner_id", "") or "")
        if state is not None:
            stopping_state = build_shadow_learning_session_state(
                entry_id=state.entry_id,
                route_owner_id=route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                upstream_endpoint=state.upstream_endpoint,
                restore_required=state.restore_required,
                started_at=state.started_at,
                expires_at=state.expires_at,
                updated_at=shadow_learning_session_timestamp(),
                restore_attempt_count=state.restore_attempt_count,
                last_restore_attempt_at=state.last_restore_attempt_at,
                last_restore_error=state.last_restore_error,
                status="restoring",
            )
            await self._async_save_shadow_learning_session_state(stopping_state)

        if route_owner_id:
            await self._runtime.async_stop_shadow_learning_route(
                owner_id=route_owner_id,
            )
        else:
            await self._runtime.async_stop_shadow_learning_route()

        restored_endpoint = ""
        restore_confirmed = True
        restore_error = ""
        restore_attempt_at = ""
        if state is not None and state.restore_required and state.original_endpoint:
            restore_attempt_at = shadow_learning_session_timestamp()
            try:
                restored_endpoint = await self._async_restore_proxy_capture_endpoint(
                    state.original_endpoint
                )
            except Exception as exc:
                restore_confirmed = False
                restore_error = str(exc)
                logger.warning(
                    "Shadow-learning restore failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
                self._notify_proxy_capture_restore_unconfirmed()

        if restore_confirmed or clear_failed_restore or state is None or not state.restore_required:
            await self._async_clear_shadow_learning_session_state()
        else:
            failed_state = build_shadow_learning_session_state(
                entry_id=state.entry_id,
                route_owner_id=route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                upstream_endpoint=state.upstream_endpoint,
                restore_required=state.restore_required,
                started_at=state.started_at,
                expires_at=state.expires_at,
                updated_at=shadow_learning_session_timestamp(),
                restore_attempt_count=state.restore_attempt_count + 1,
                last_restore_attempt_at=restore_attempt_at,
                last_restore_error=restore_error,
                status="restore_failed",
            )
            await self._async_save_shadow_learning_session_state(failed_state)
        if request_refresh:
            await self.async_request_refresh()
        self._publish_tooling_values(
            shadow_learning_session_status="stopped" if restore_confirmed else "restore_failed",
            shadow_learning_session_ready=False,
            local_metadata_status="Shadow-learning route stopped",
        )
        return {
            "status": "stopped" if restore_confirmed else "restore_unconfirmed",
            "reason": str(reason or "stopped"),
            "restored_endpoint": restored_endpoint,
            "restore_confirmed": restore_confirmed,
        }

    async def async_touch_proxy_capture_lease(self, *, extend: bool = True) -> str:
        """Publish active proxy-session countdown values and optionally refresh the lease."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None or not proxy_capture_session_is_active(state):
            return ""
        if self._proxy_capture_state_needs_reconcile(state):
            self._cancel_proxy_capture_deadline_refresh()
            await self.async_request_refresh()
            return ""

        published_state = state
        if extend:
            published_state = refresh_proxy_capture_session_lease(
                state,
                lease_seconds=self.proxy_capture_configured_duration_minutes * 60,
            )
            await self._async_save_proxy_capture_session_state(published_state)
        self._schedule_proxy_capture_deadline_refresh(published_state.expires_at)
        self._publish_tooling_values(
            **self._proxy_capture_overview_runtime_values(active_state=published_state)
        )
        return published_state.expires_at

    async def async_set_proxy_capture_duration_minutes(self, value: object) -> int:
        """Persist proxy capture duration and update the active session deadline explicitly."""

        duration_minutes = _coerce_proxy_capture_duration_minutes(value)
        options = dict(self.config_entry.options)
        if options.get(CONF_PROXY_CAPTURE_DURATION_MINUTES) != duration_minutes:
            options[CONF_PROXY_CAPTURE_DURATION_MINUTES] = duration_minutes
            self._async_update_entry_without_reload(options=options)

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is not None and proxy_capture_session_is_active(state):
            if self._proxy_capture_state_needs_reconcile(state):
                self._cancel_proxy_capture_deadline_refresh()
                await self.async_request_refresh()
                self._publish_tooling_values(**self._proxy_capture_timer_runtime_values(None))
                return duration_minutes
            updated_state = build_proxy_capture_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                restore_required=state.restore_required,
                anonymized=state.anonymized,
                started_at=state.started_at,
                expires_at=build_proxy_capture_lease_deadline(
                    lease_seconds=duration_minutes * 60,
                ),
                status=state.status,
            )
            await self._async_save_proxy_capture_session_state(updated_state)
            self._schedule_proxy_capture_deadline_refresh(updated_state.expires_at)
            self._publish_tooling_values(
                **self._proxy_capture_overview_runtime_values(active_state=updated_state)
            )
        else:
            self._cancel_proxy_capture_deadline_refresh()
            self._publish_tooling_values(**self._proxy_capture_timer_runtime_values(None))
        return duration_minutes

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        """Return the remembered collector callback endpoint for rollback/proxy restore."""

        runtime = getattr(self, "_runtime", None)
        runtime_target = str(
            getattr(runtime, "collector_server_endpoint_rollback_target", "") or ""
        ).strip()
        if runtime_target:
            try:
                runtime_target = _normalize_preserved_collector_server_endpoint(runtime_target)
            except ValueError:
                runtime_target = ""
            else:
                if self._endpoint_looks_like_local_collector_callback(runtime_target):
                    runtime_target = ""
        if runtime_target:
            return runtime_target
        return self._normalized_remembered_collector_server_endpoint()

    @property
    def collector_callback_target_endpoint(self) -> str:
        """Return the effective callback endpoint configured for this entry."""

        template_endpoint = str(
            self.data.values.get("collector_server_endpoint")
            or self.collector_server_endpoint_rollback_target
            or ""
        ).strip()
        return home_assistant_callback_endpoint(
            server_host=self._effective_callback_server_host,
            listener_port=int(
                getattr(self._connection_spec, "effective_advertised_tcp_port", 0)
                or getattr(self._connection_spec, "tcp_port", 0)
                or 0
            ),
            template_endpoint=template_endpoint,
            cloud_family=self.collector_cloud_family,
        )

    @property
    def proxy_capture_target_endpoint(self) -> str:
        """Return the dedicated callback endpoint reserved for proxy capture sessions."""

        upstream_endpoint = self.proxy_capture_upstream_endpoint
        return _format_home_assistant_collector_endpoint(
            server_host=self._effective_callback_server_host,
            template_endpoint=upstream_endpoint,
            cloud_family=self.collector_cloud_family,
        )

    @property
    def proxy_capture_upstream_endpoint(self) -> str:
        """Return the endpoint that the proxy should forward collector traffic to."""

        rollback_target = self.collector_server_endpoint_rollback_target
        if rollback_target:
            try:
                _parse_collector_server_endpoint(rollback_target)
            except ValueError:
                rollback_target = ""

        current_endpoint = str(self.data.values.get("collector_server_endpoint") or "").strip()
        if current_endpoint:
            try:
                current_endpoint = _normalize_preserved_collector_server_endpoint(current_endpoint)
                current_host, _current_port, _current_protocol = _parse_collector_server_endpoint(current_endpoint)
            except ValueError:
                current_host = ""
            if (
                current_host != self._effective_callback_server_host
                and not self._endpoint_looks_like_local_collector_callback(current_endpoint)
            ):
                return current_endpoint

        if rollback_target:
            return rollback_target

        return _default_cloud_upstream_endpoint(
            cloud_family=self.collector_cloud_family,
            template_endpoint=current_endpoint,
        )

    @property
    def collector_cloud_family(self) -> str:
        """Return the best available collector cloud family known to the coordinator."""

        collector = getattr(self.data, "collector", None)
        family = _known_collector_cloud_family(
            getattr(collector, "collector_cloud_family", "")
        )
        if family:
            return family
        family = _known_collector_cloud_family(
            self.data.values.get("collector_cloud_family")
        )
        if family:
            return family
        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        config_options = getattr(config_entry, "options", {}) if config_entry is not None else {}

        endpoint_candidates = (
            self.data.values.get("collector_server_endpoint"),
            self.collector_server_endpoint_rollback_target,
            getattr(self, "_remembered_collector_server_endpoint", ""),
        )
        family = collector_cloud_family_from_entry_context(
            config_data,
            config_options,
            extra_endpoints=endpoint_candidates,
        )
        if family:
            return family

        for endpoint in (*endpoint_candidates, config_options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT, "")):
            family = _collector_cloud_family_from_endpoint_shape(endpoint)
            if family:
                return family
        return ""

    @property
    def collector_session_protocol(self) -> str:
        """Return the callback session protocol implied by the cloud profile."""

        return self.collector_transport_profile.session_protocol

    @property
    def collector_identity_strategy(self) -> str:
        """Return the collector identity strategy implied by the cloud profile."""

        return self.collector_transport_profile.identity_strategy

    @property
    def collector_raw_passthrough_bootstrap(self) -> str:
        """Return the raw inverter payload bootstrap mode implied by the cloud profile."""

        return self.collector_transport_profile.raw_passthrough_bootstrap

    @property
    def collector_raw_passthrough_frame_format(self) -> str:
        """Return the raw inverter payload frame format implied by the cloud profile."""

        return self.collector_transport_profile.raw_passthrough_frame_format

    @property
    def collector_raw_passthrough_min_interval_ms(self) -> int:
        """Return minimum interval between raw passthrough requests."""

        return self.collector_transport_profile.raw_passthrough_min_interval_ms

    @property
    def collector_transport_profile(self):
        """Return the resolved callback transport profile for this runtime."""

        return resolve_collector_transport_profile(
            cloud_family=self.collector_cloud_family,
            runtime_owner_key=self._collector_runtime_owner_key(),
            virtual_bridge=self._collector_is_virtual_bridge(),
        )

    def _collector_runtime_owner_key(self) -> str:
        """Return the best known local inverter runtime owner for transport choice."""

        for source in (self.config_entry.options, self.config_entry.data):
            driver_hint = str(
                source.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO) or DRIVER_HINT_AUTO
            ).strip().lower()
            if driver_hint and driver_hint != DRIVER_HINT_AUTO:
                return driver_hint

        snapshot = self.effective_metadata_snapshot
        if isinstance(snapshot, Mapping):
            owner_key = str(snapshot.get("effective_owner_key") or "").strip().lower()
        else:
            owner_key = str(getattr(snapshot, "effective_owner_key", "") or "").strip().lower()
        if owner_key:
            return owner_key
        try:
            return str(self.effective_owner_key or "").strip().lower()
        except Exception:
            return ""

    @property
    def collector_cloud_profile_key(self) -> str:
        """Return the best available observed collector cloud profile key."""

        collector = getattr(self.data, "collector", None)
        key = _known_collector_cloud_profile_value(
            getattr(collector, "collector_cloud_profile_key", "")
        )
        if key:
            return key
        key = _known_collector_cloud_profile_value(
            getattr(collector, "smartess_protocol_profile_key", "")
        )
        if key:
            return key

        values = getattr(self.data, "values", {})
        key = _known_collector_cloud_profile_value(values.get("collector_cloud_profile_key"))
        if key:
            return key
        key = _known_collector_cloud_profile_value(values.get("smartess_protocol_profile_key"))
        if key:
            return key
        key = _known_collector_cloud_profile_value(values.get("smartess_profile_key"))
        if key:
            return key

        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        key = _known_collector_cloud_profile_value(config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_KEY))
        if key:
            return key
        return _known_collector_cloud_profile_value(config_data.get(CONF_SMARTESS_PROFILE_KEY))

    @property
    def collector_cloud_profile_label(self) -> str:
        """Return the best available observed collector cloud profile label."""

        collector = getattr(self.data, "collector", None)
        for candidate in (
            getattr(collector, "collector_cloud_profile_label", ""),
            getattr(collector, "smartess_protocol_name", ""),
            getattr(collector, "smartess_protocol_asset_name", ""),
        ):
            label = _known_collector_cloud_profile_value(candidate)
            if label:
                return label

        values = getattr(self.data, "values", {})
        for candidate in (
            values.get("collector_cloud_profile_label"),
            values.get("smartess_protocol_name"),
            values.get("smartess_protocol_asset_name"),
        ):
            label = _known_collector_cloud_profile_value(candidate)
            if label:
                return label

        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        return _known_collector_cloud_profile_value(config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_LABEL))

    @property
    def collector_cloud_profile_source(self) -> str:
        """Return the source of the observed collector cloud profile identity."""

        collector = getattr(self.data, "collector", None)
        source = _known_collector_cloud_profile_value(
            getattr(collector, "collector_cloud_profile_source", "")
        )
        if source:
            return source

        values = getattr(self.data, "values", {})
        source = _known_collector_cloud_profile_value(values.get("collector_cloud_profile_source"))
        if source:
            return source

        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        source = _known_collector_cloud_profile_value(config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_SOURCE))
        if source:
            return source

        if self.collector_cloud_profile_key:
            if getattr(self.data, "collector", None) is not None:
                return "runtime_observed"
            if _known_collector_cloud_profile_value(config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_KEY)):
                return "entry_persisted"
        return ""

    @property
    def collector_cloud_profile_confidence(self) -> str:
        """Return confidence for the observed collector cloud profile identity."""

        collector = getattr(self.data, "collector", None)
        confidence = _known_collector_cloud_profile_value(
            getattr(collector, "collector_cloud_profile_confidence", "")
        )
        if confidence:
            return confidence

        values = getattr(self.data, "values", {})
        confidence = _known_collector_cloud_profile_value(values.get("collector_cloud_profile_confidence"))
        if confidence:
            return confidence

        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        confidence = _known_collector_cloud_profile_value(
            config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE)
        )
        if confidence:
            return confidence

        if self.collector_cloud_profile_key:
            if getattr(self.data, "collector", None) is not None:
                return "high"
            if _known_collector_cloud_profile_value(config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_KEY)):
                return "low"
        return ""

    @property
    def detection_confidence(self) -> str:
        """Return the saved detection confidence for this entry."""

        return self.config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none")

    @property
    def control_mode(self) -> str:
        """Return the configured control mode override."""

        return self.config_entry.options.get(
            CONF_CONTROL_MODE,
            self.config_entry.data.get(CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE),
        )

    def collector_operation_mode_change_reason(self, *, target_mode: str = "") -> str | None:
        """Return why the collector operation mode cannot be changed right now."""

        overview = self.proxy_capture_overview
        overview_status = str(getattr(overview, "status", "") or "").strip()
        if overview_status in {"starting", "stopping", "restoring"}:
            return "collector_operation_mode_proxy_transition_active"
        if overview_status == "running":
            return "collector_operation_mode_proxy_session_active"
        mode_apply_lock_code = self.collector_operation_mode_apply_lock_code()
        if mode_apply_lock_code is not None:
            return mode_apply_lock_code
        if (
            self.collector_capabilities.ha_only_required
            and str(target_mode or "").strip() == COLLECTOR_OPERATION_SMARTESS_AND_HA
        ):
            return "collector_operation_mode_target_unavailable"
        if not self.data.connected:
            return "collector_operation_mode_collector_not_connected"

        normalized_target_mode = str(target_mode or "").strip()
        if normalized_target_mode == COLLECTOR_OPERATION_SMARTESS_AND_HA:
            current_endpoint = str(self.data.values.get("collector_server_endpoint") or "").strip()
            if (
                (
                    not current_endpoint
                    or self._endpoint_looks_like_local_collector_callback(current_endpoint)
                )
                and not self.proxy_capture_upstream_endpoint
            ):
                return "collector_operation_mode_rollback_endpoint_unavailable"

        return None

    async def async_set_collector_operation_mode(self, mode: str) -> str:
        """Persist one collector ownership mode and apply its runtime side effects."""

        normalized_mode = str(mode or "").strip()
        if normalized_mode not in COLLECTOR_OPERATION_MODES:
            raise ValueError("collector_operation_mode_invalid")
        if normalized_mode == self.collector_operation_mode:
            return normalized_mode

        change_reason = self.collector_operation_mode_change_reason(
            target_mode=normalized_mode
        )
        if change_reason is not None:
            raise RuntimeError(change_reason)

        current_endpoint = str(self.data.values.get("collector_server_endpoint") or "").strip()
        current_parts = self._endpoint_effective_parts(current_endpoint)
        applied_endpoint = ""
        applied_status = ""

        if normalized_mode == COLLECTOR_OPERATION_HA_ONLY:
            await self._async_remember_collector_server_endpoint(self.data)
            target_endpoint = self.collector_callback_target_endpoint
            if not target_endpoint:
                raise RuntimeError("collector_operation_mode_target_unavailable")
            await self._async_prepare_home_assistant_callback_listener(target_endpoint)
            target_parts = self._endpoint_effective_parts(target_endpoint)
            if current_parts != target_parts:
                self._collector_operation_pending_target_endpoint = target_endpoint
                self._publish_snapshot_values(
                    collector_operation_endpoint_sync_status="waiting_for_collector",
                    collector_operation_endpoint_sync_error=None,
                )
                try:
                    result = await self._runtime.async_set_collector_server_endpoint(
                        target_endpoint,
                        apply_changes=True,
                    )
                except Exception as exc:
                    self._collector_operation_pending_target_endpoint = ""
                    self._publish_snapshot_values(
                        collector_operation_endpoint_sync_status="failed",
                        collector_operation_endpoint_sync_error=str(exc),
                    )
                    raise
                applied_endpoint = str(
                    result.get("readback_endpoint")
                    or result.get("requested_endpoint")
                    or target_endpoint
                )
                applied_status = str(result.get("status") or "applied")
                self._collector_operation_pending_target_endpoint = applied_endpoint
        else:
            rollback_endpoint = self.proxy_capture_upstream_endpoint
            if current_endpoint and not self._endpoint_looks_like_local_collector_callback(
                current_endpoint
            ):
                rollback_endpoint = ""
            elif not rollback_endpoint:
                raise RuntimeError(
                    "collector_operation_mode_rollback_endpoint_unavailable"
                )
            target_parts = self._endpoint_effective_parts(rollback_endpoint)
            if rollback_endpoint and current_parts != target_parts:
                self._collector_operation_pending_target_endpoint = rollback_endpoint
                self._publish_snapshot_values(
                    collector_operation_endpoint_sync_status="waiting_for_collector",
                    collector_operation_endpoint_sync_error=None,
                )
                try:
                    result = await self._runtime.async_set_collector_server_endpoint(
                        rollback_endpoint,
                        apply_changes=True,
                    )
                except Exception as exc:
                    self._collector_operation_pending_target_endpoint = ""
                    self._publish_snapshot_values(
                        collector_operation_endpoint_sync_status="failed",
                        collector_operation_endpoint_sync_error=str(exc),
                    )
                    raise
                applied_endpoint = str(
                    result.get("readback_endpoint")
                    or result.get("requested_endpoint")
                    or rollback_endpoint
                )
                applied_status = str(result.get("status") or "applied")
                self._collector_operation_pending_target_endpoint = applied_endpoint

        data = dict(self.config_entry.data)
        options = dict(self.config_entry.options)
        data[CONF_COLLECTOR_OPERATION_MODE] = normalized_mode
        options[CONF_COLLECTOR_OPERATION_MODE] = normalized_mode
        self._async_update_entry_without_reload(data=data, options=options)

        self._configure_reverse_discovery_mode()
        if applied_endpoint:
            self._publish_snapshot_values(
                collector_server_endpoint=applied_endpoint,
                collector_operation_endpoint_sync_status=applied_status,
                collector_operation_endpoint_sync_error=None,
            )
        await self.async_request_refresh()
        return normalized_mode

    async def async_set_control_mode(self, mode: str) -> str:
        """Persist one integration control policy mode and reload the entry."""

        normalized_mode = str(mode or "").strip()
        if normalized_mode not in {CONTROL_MODE_AUTO, CONTROL_MODE_READ_ONLY, CONTROL_MODE_FULL}:
            raise ValueError("control_mode_invalid")
        if normalized_mode == self.control_mode:
            return normalized_mode

        data = dict(self.config_entry.data)
        options = dict(self.config_entry.options)
        data[CONF_CONTROL_MODE] = normalized_mode
        options[CONF_CONTROL_MODE] = normalized_mode
        self._async_update_entry_without_reload(
            data=data,
            options=options,
        )
        # The exposed capability-entity surface depends on the control mode
        # (untested capabilities exist only in full-control mode), and the
        # platforms materialize entities exactly once at setup — without a
        # reload, switching the mode changes nothing the user can see
        # (0.3.0-beta.1 field report: MUST PV1800 got no controls after
        # enabling full control). The suppressed update above plus one
        # explicit scheduled reload keeps this a single deterministic reload.
        self.hass.config_entries.async_schedule_reload(self.config_entry.entry_id)
        return normalized_mode

    @property
    def controls_enabled(self) -> bool:
        """Whether writes are globally enabled for this entry."""

        return controls_enabled(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            write_capability_count=self._current_write_capability_count(),
        )

    @property
    def collector_actions_enabled(self) -> bool:
        """Whether collector-scoped actions are allowed for this entry."""

        return self.control_mode in {"auto", CONTROL_MODE_FULL}

    @property
    def controls_reason(self) -> str:
        """Why writes are enabled or disabled for this entry."""

        return controls_reason(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            write_capability_count=self._current_write_capability_count(),
        )

    @property
    def controls_summary(self) -> str:
        """Human-readable summary of the current control policy."""

        return controls_summary(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            write_capability_count=self._current_write_capability_count(),
        )

    def _current_write_capability_count(self) -> int | None:
        """Return the number of writable controls known for the current runtime."""

        inverter = self.identified_inverter
        if inverter is not None:
            return len(tuple(getattr(inverter, "capabilities", ()) or ()))

        driver = self.current_driver
        if driver is not None:
            return len(tuple(getattr(driver, "write_capabilities", ()) or ()))

        return None

    def can_expose_capability(self, capability: WriteCapability) -> bool:
        """Whether one capability should exist as a writable HA entity."""

        context = self._write_exposure_context()
        return capability_write_exposure_allowed(
            capability,
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            variant_key=context["variant_key"],
            profile_source_scope=context["profile_source_scope"],
            schema_source_scope=context["schema_source_scope"],
            profile_name=context["profile_name"],
            device_scoped_overlay_active=context["device_scoped_overlay_active"],
            selected_control_keys=context["selected_control_keys"],
        )

    def capability_enabled_by_default(self, capability: WriteCapability) -> bool:
        """Entity-registry default-enabled state for one capability.

        Learned overlay capabilities are generated with ``enabled_default=False`` so they
        stay inactive until activation. Once the user has selected and activated a device-
        scoped learned control (so it is exposable), enable it by default -- otherwise the
        entity would be created but registered disabled and stay hidden on the device page.
        Every other capability keeps its declared default.
        """

        if capability.is_device_scoped_experimental and self.can_expose_capability(capability):
            return True
        return capability.enabled_default

    def can_expose_preset(self, preset: CapabilityPreset) -> bool:
        """Whether one preset should exist as a writable HA entity."""

        inverter = self.identified_inverter
        if inverter is None:
            capabilities_by_key = {
                capability.key: capability
                for capability in all_write_capabilities()
            }
        else:
            capabilities_by_key = {capability.key: capability for capability in inverter.capabilities}
        context = self._write_exposure_context()
        return preset_write_exposure_allowed(
            preset,
            capabilities_by_key=capabilities_by_key,
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            variant_key=context["variant_key"],
            profile_source_scope=context["profile_source_scope"],
            schema_source_scope=context["schema_source_scope"],
            profile_name=context["profile_name"],
            device_scoped_overlay_active=context["device_scoped_overlay_active"],
            selected_control_keys=context["selected_control_keys"],
        )

    def _write_exposure_context(self) -> dict[str, Any]:
        """Return normalized metadata context shared by write exposure checks."""

        metadata = self.effective_metadata
        inverter = self.identified_inverter
        snapshot = self.effective_metadata_snapshot
        variant_key = str(
            getattr(inverter, "variant_key", "") or getattr(snapshot, "variant_key", "") or ""
        ).strip()
        return {
            "variant_key": variant_key,
            "profile_name": str(getattr(metadata, "profile_name", "") or "").strip(),
            "profile_source_scope": str(
                getattr(getattr(metadata, "profile_metadata", None), "source_scope", "") or ""
            ).strip(),
            "schema_source_scope": str(
                getattr(getattr(metadata, "register_schema_metadata", None), "source_scope", "")
                or ""
            ).strip(),
            "device_scoped_overlay_active": bool(
                getattr(metadata, "device_scoped_overlay_active", False)
            ),
            "device_scoped_overlay_scope": str(
                getattr(metadata, "device_scoped_overlay_scope", "") or ""
            ).strip(),
            "selected_control_keys": getattr(
                metadata, "device_scoped_overlay_selected_control_keys", None
            ),
            "effective_capabilities_experimental": bool(
                getattr(metadata, "device_scoped_overlay_active", False)
            ),
        }

    @property
    def current_driver(self):
        """Return the registered driver for the detected inverter, if any."""

        inverter = self.identified_inverter
        try:
            if inverter is not None:
                driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
                if driver_key:
                    return get_driver(driver_key)
            if not self.has_inverter_identity:
                return None
            driver_hint = self.config_entry.options.get(
                CONF_DRIVER_HINT,
                self.config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            if driver_hint and driver_hint != DRIVER_HINT_AUTO:
                return get_driver(driver_hint)
        except KeyError:
            pass
        return None

    @property
    def identified_inverter(self):
        """Return the runtime inverter only when it has a usable identity."""

        inverter = self.data.inverter
        if inverter is None:
            return None

        model_name = str(getattr(inverter, "model_name", "") or "").strip()
        serial_number = str(getattr(inverter, "serial_number", "") or "").strip()
        if model_name or serial_number:
            return inverter

        detected_model = str(self.config_entry.data.get(CONF_DETECTED_MODEL) or "").strip()
        detected_serial = str(self.config_entry.data.get(CONF_DETECTED_SERIAL) or "").strip()
        if detected_model or detected_serial:
            return inverter
        return None

    def _apply_device_overlay_to_inverter(self, inverter, collector):
        """Merge activated device-scoped learned controls into the detected inverter.

        The runtime detects the inverter against built-in bindings, so its capabilities
        never include the learned overlay controls. This hook (invoked by the runtime
        right after detection) resolves the effective metadata for the detected device
        and, when a device-scoped overlay is active, appends the activated learned
        capabilities (plus any capability group they require) so they materialize as
        entities and are writable. Detected capabilities are preserved; none are removed.
        """

        if inverter is None:
            self._device_overlay_merge_status = "inverter_none"
            return inverter
        try:
            metadata = resolve_effective_metadata_selection(
                inverter=inverter,
                driver=None,
                collector=collector,
                entry_data=self.config_entry.data,
                entry_options=self.config_entry.options,
                persisted_snapshot=self.effective_metadata_snapshot,
            )
            if not metadata.device_scoped_overlay_active:
                self._device_overlay_merge_status = "inactive"
                return inverter
            profile = metadata.profile_metadata
            if profile is None:
                self._device_overlay_merge_status = "no_profile_metadata"
                return inverter
            existing_keys = {capability.key for capability in inverter.capabilities}
            learned = tuple(
                capability
                for capability in profile.capabilities
                if capability.is_device_scoped_experimental
                and capability.key not in existing_keys
            )
            if not learned:
                already = sum(
                    1
                    for capability in inverter.capabilities
                    if getattr(capability, "is_device_scoped_experimental", False)
                )
                self._device_overlay_merge_status = f"no_new_learned(already={already})"
                return inverter
            needed_group_keys = {capability.group for capability in learned}
            existing_group_keys = {group.key for group in inverter.capability_groups}
            extra_groups = tuple(
                group
                for group in profile.groups
                if group.key in needed_group_keys and group.key not in existing_group_keys
            )
            self._device_overlay_merge_status = (
                f"merged({'+'.join(capability.key for capability in learned)})"
            )
            return dataclasses.replace(
                inverter,
                capabilities=inverter.capabilities + learned,
                capability_groups=inverter.capability_groups + extra_groups,
            )
        except Exception as exc:
            self._device_overlay_merge_status = f"error:{type(exc).__name__}:{exc}"
            logger.warning(
                "Failed to merge device-scoped learned controls into the detected "
                "inverter; activated controls will not appear this cycle",
                exc_info=True,
            )
            return inverter

    @property
    def has_inverter_identity(self) -> bool:
        """Return whether this entry has a confirmed or persisted inverter identity."""

        if self.identified_inverter is not None:
            return True
        detected_model = str(self.config_entry.data.get(CONF_DETECTED_MODEL) or "").strip()
        detected_serial = str(self.config_entry.data.get(CONF_DETECTED_SERIAL) or "").strip()
        return bool(detected_model or detected_serial)

    @property
    def effective_metadata(self):
        """Return the effective metadata selection for the current entry state."""

        cached = getattr(self, "_cached_effective_metadata", None)
        if cached is not None:
            return cached
        return resolve_effective_metadata_selection(
            inverter=self.identified_inverter,
            driver=self.current_driver,
            collector=self.data.collector,
            entry_data=self.config_entry.data,
            entry_options=self.config_entry.options,
            persisted_snapshot=self.effective_metadata_snapshot,
        )

    @property
    def effective_metadata_snapshot(self) -> EffectiveMetadataSnapshot:
        """Return the persisted effective metadata snapshot when one is stored."""

        options = getattr(self.config_entry, "options", {}) or {}
        return effective_metadata_snapshot_from_dict(
            options.get(_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY)
        )

    @property
    def effective_owner_key(self) -> str:
        """Return the actual runtime owner key for the selected effective metadata."""

        return self.effective_metadata.effective_owner_key

    @property
    def effective_owner_name(self) -> str:
        """Return the internal runtime-path label for the selected effective metadata."""

        return self.effective_metadata.effective_owner_name

    @property
    def smartess_family_name(self) -> str:
        """Return the SmartESS family label when collector hints resolved one."""

        return self.effective_metadata.smartess_family_name

    @property
    def smartess_raw_profile_name(self) -> str:
        """Return the raw SmartESS asset profile name when available."""

        return self.effective_metadata.raw_profile_name

    @property
    def smartess_raw_register_schema_name(self) -> str:
        """Return the raw SmartESS asset schema name when available."""

        return self.effective_metadata.raw_register_schema_name

    @property
    def effective_profile_metadata(self):
        """Return the loaded effective profile metadata when available."""

        return self.effective_metadata.profile_metadata

    @property
    def effective_register_schema_metadata(self):
        """Return the loaded effective register schema metadata when available."""

        return self.effective_metadata.register_schema_metadata

    @property
    def effective_profile_name(self) -> str:
        """Return the effective detected profile name when available."""

        return self.effective_metadata.profile_name

    @property
    def effective_register_schema_name(self) -> str:
        """Return the effective detected register schema name when available."""

        return self.effective_metadata.register_schema_name

    @property
    def shadow_learning_effective_metadata(self) -> Any:
        """Return the effective metadata a shadow-learning seed should carry.

        Prefer the persisted snapshot, but the partial / unidentified tier never
        persists one (it has no controls profile by design), so fall back to the
        LIVE effective metadata (the family base schema). Without this fallback
        the start path blocks with ``missing_effective_metadata_snapshot`` on
        exactly the devices learning exists for. This is the single source of
        truth shared with the config-flow preflight so the preview and the
        actual start can never drift.
        """

        snapshot = self.effective_metadata_snapshot
        if str(getattr(snapshot, "register_schema_name", "") or "").strip():
            return snapshot
        return {
            "effective_owner_key": self.effective_owner_key,
            "profile_name": self.effective_profile_name,
            "register_schema_name": self.effective_register_schema_name,
        }

    @property
    def smartess_collector_pn(self) -> str:
        """Return the collector PN used for SmartESS cloud evidence matching."""

        return self._preferred_collector_pn(self.data)

    def _preferred_collector_pn(self, snapshot: RuntimeSnapshot | None = None) -> str:
        """Return the most complete collector PN available from config and runtime."""

        snapshot = snapshot or self.data
        configured_pn = str(self.config_entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
        live_pn = str(getattr(snapshot.collector, "collector_pn", "") or "").strip()
        if not live_pn:
            return configured_pn
        if not configured_pn:
            return live_pn
        if configured_pn == live_pn:
            return live_pn
        if configured_pn.startswith(live_pn):
            return configured_pn
        if live_pn.startswith(configured_pn):
            return live_pn
        return live_pn

    @property
    def smartess_cloud_export_available(self) -> bool:
        """Return whether SmartESS cloud export can be attempted for this entry."""

        return (
            bool(self.smartess_collector_pn)
            and resolve_collector_cloud_provider(self.collector_cloud_family) == "smartess"
        )

    @property
    def cloud_evidence_provider(self) -> str:
        """Return the account/cloud provider used for support cloud evidence."""

        return resolve_collector_cloud_provider(self.collector_cloud_family)

    @property
    def cloud_evidence_export_available(self) -> bool:
        """Return whether provider-specific cloud evidence can be attempted."""

        return bool(self.smartess_collector_pn) and self.cloud_evidence_provider in {
            "smartess",
            "valuecloud",
        }

    @property
    def smartess_cloud_evidence_path(self) -> str:
        """Return the latest saved SmartESS cloud evidence path for this entry."""

        record = self._latest_smartess_cloud_evidence_record()
        return str(record.path) if record is not None else ""

    @property
    def latest_proxy_trace_path(self) -> str:
        """Return the latest saved proxy trace data path for this entry."""

        values = self._proxy_capture_runtime_values()
        return str(values.get("proxy_trace_path") or "").strip()

    @property
    def latest_proxy_trace_manifest_path(self) -> str:
        """Return the latest saved proxy trace manifest path for this entry."""

        values = self._proxy_capture_runtime_values()
        return str(values.get("proxy_trace_manifest_path") or "").strip()

    @property
    def proxy_capture_overview(self):
        """Return one normalized proxy capture runtime overview."""

        snapshot = self.data
        state = self._active_proxy_capture_state()
        values = self._proxy_capture_runtime_values()
        return build_proxy_capture_overview(
            control_mode=self.control_mode,
            collector_control_allowed=self.collector_actions_enabled,
            collector_proxy_capture_allowed=self.collector_capabilities.proxy_capture,
            collector_connected=bool(snapshot.connected),
            collector_cloud_family=self.collector_cloud_family,
            current_endpoint=str(
                values.get("collector_server_endpoint")
                or snapshot.values.get("collector_server_endpoint")
                or ""
            ),
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            target_endpoint=self.proxy_capture_target_endpoint,
            active_state=state,
            latest_trace_path=self.latest_proxy_trace_path,
            latest_manifest_path=self.latest_proxy_trace_manifest_path,
        )

    async def _async_recover_proxy_capture_state(self) -> None:
        """Best-effort restore collector callback state after an interrupted session."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None or not proxy_capture_session_is_active(state):
            return
        logger.warning(
            "Recovering interrupted proxy capture for entry %s with state %s",
            self.config_entry.entry_id,
            state.status,
        )
        try:
            await self.async_stop_proxy_capture(
                reason="recovered_after_restart",
                prefer_proxy_restore_trigger=False,
                request_refresh=False,
            )
        except Exception as exc:
            logger.warning("Proxy capture recovery failed for entry %s: %s", self.config_entry.entry_id, exc)
            self._notify_proxy_capture_restore_unconfirmed()
            await self._async_clear_proxy_capture_session_state()

    async def _async_recover_shadow_learning_state(self) -> None:
        """Best-effort restore collector callback state after interrupted shadow learning."""

        state = await self._async_active_shadow_learning_state(require_process=False)
        recoverable_status = str(getattr(state, "status", "") or "").strip()
        if state is None or (
            not shadow_learning_session_is_active(state)
            and recoverable_status != "restore_failed"
        ):
            return
        logger.warning(
            "Recovering interrupted shadow-learning session for entry %s with state %s",
            self.config_entry.entry_id,
            state.status,
        )
        try:
            stop_reason = (
                "expired_lease"
                if shadow_learning_session_is_expired(state)
                else "recovered_after_restart"
            )
            await self.async_stop_shadow_learning(
                reason=stop_reason,
                request_refresh=False,
                raise_when_not_running=False,
            )
        except Exception as exc:
            logger.warning(
                "Shadow-learning recovery failed for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            self._notify_proxy_capture_restore_unconfirmed()

    async def _async_stop_proxy_capture_process(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active shared-ingress proxy capture route when it exists."""

        stop_route = getattr(self._runtime, "async_stop_proxy_capture_route", None)
        if stop_route is not None:
            if owner_id or force:
                await stop_route(owner_id=owner_id, force=force)
            else:
                await stop_route()

    async def _async_restore_proxy_capture_endpoint(self, endpoint: str) -> str:
        """Restore one collector callback endpoint captured before proxy redirect."""

        _parse_collector_server_endpoint(endpoint)
        result = await self._runtime.async_set_collector_server_endpoint(
            endpoint,
            apply_changes=True,
        )
        return str(result.get("readback_endpoint") or endpoint)

    async def _async_read_live_collector_server_endpoint(self) -> str:
        """Return the latest collector endpoint, preferring a direct live management read."""

        fallback = str(self.data.values.get("collector_server_endpoint") or "").strip()
        try:
            result = await self._runtime.async_get_collector_server_endpoint_state()
        except Exception as exc:
            logger.warning(
                "Unable to read live collector endpoint for proxy capture safeguard on entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            return fallback
        return str(result.get("current_endpoint") or fallback or "").strip()

    async def _async_preflight_proxy_capture_network(
        self,
        *,
        target_host: str,
        target_port: int,
        upstream_host: str,
        upstream_port: int,
    ) -> None:
        """Fail early when the proxy route is clearly unsafe."""

        await self._async_validate_proxy_capture_target(target_host=target_host, target_port=target_port)
        await self._async_validate_proxy_capture_upstream(upstream_host=upstream_host, upstream_port=upstream_port)

    async def _async_validate_proxy_capture_upstream(self, *, upstream_host: str, upstream_port: int) -> None:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(upstream_host, upstream_port),
                timeout=5.0,
            )
        except Exception as exc:
            raise RuntimeError(f"proxy_capture_upstream_unreachable:{type(exc).__name__}:{exc}") from exc
        writer.close()
        await writer.wait_closed()
        del reader

    async def _async_validate_proxy_capture_target(self, *, target_host: str, target_port: int) -> None:
        try:
            target_ip = ipaddress.ip_address(target_host)
        except ValueError:
            return
        if target_ip.is_loopback or target_ip.is_unspecified:
            raise RuntimeError("proxy_capture_target_not_reachable_from_collector_lan:loopback_or_unspecified")

        collector_ip = str(self.config_entry.data.get(CONF_COLLECTOR_IP) or "").strip()
        if not collector_ip or collector_ip == DEFAULT_COLLECTOR_IP:
            return
        try:
            collector_addr = ipaddress.ip_address(collector_ip)
        except ValueError:
            return
        if not (target_ip.is_private and collector_addr.is_private):
            return

        source_ip = await self.hass.async_add_executor_job(_local_source_ip_for_target, collector_ip)
        if not source_ip:
            return
        if source_ip != target_host:
            raise RuntimeError(
                "proxy_capture_target_not_reachable_from_collector_lan:"
                f"target={target_host}:{target_port}:source={source_ip}:"
                "use_collector_callback_endpoint_override_or_external_transport"
            )

    async def _async_wait_for_proxy_capture_reconnect(self, trace_path: Path) -> None:
        deadline = asyncio.get_running_loop().time() + 75.0
        while asyncio.get_running_loop().time() < deadline:
            if not self._proxy_capture_process_running():
                raise RuntimeError("proxy_capture_route_stopped")
            status = await self.hass.async_add_executor_job(
                lambda: inspect_proxy_capture_start_status(trace_path)
            )
            upstream_error = str(status.get("upstream_error") or "")
            if upstream_error:
                raise RuntimeError(f"proxy_capture_upstream_connect_failed:{upstream_error}")
            if status.get("connected"):
                return
            await asyncio.sleep(1.0)
        raise TimeoutError("proxy_capture_collector_reconnect_timeout")

    async def _async_trigger_proxy_capture_restore(
        self,
        *,
        trace_path: Path,
        owner_id: str,
    ) -> bool:
        trigger_path = build_proxy_capture_restore_trigger_path(trace_path)
        await self.hass.async_add_executor_job(
            lambda: trigger_path.write_text(
                datetime.now(timezone.utc).isoformat() + "\n",
                encoding="utf-8",
            )
        )
        deadline = asyncio.get_running_loop().time() + 20.0
        while asyncio.get_running_loop().time() < deadline:
            status = await self.hass.async_add_executor_job(
                lambda: inspect_proxy_capture_start_status(trace_path)
            )
            if status.get("restore_confirmed"):
                try:
                    await self.hass.async_add_executor_job(trigger_path.unlink)
                except FileNotFoundError:
                    pass
                await self._async_stop_proxy_capture_process(owner_id=owner_id)
                return True
            if status.get("restore_missing"):
                break
            await asyncio.sleep(0.5)
        await self._async_stop_proxy_capture_process(owner_id=owner_id)
        return False

    async def _async_best_effort_restore_after_start_failure(self, endpoint: str) -> tuple[bool, str]:
        try:
            await self._async_restore_proxy_capture_endpoint(endpoint)
            return True, ""
        except Exception as exc:
            logger.warning("Proxy capture start rollback failed for entry %s: %s", self.config_entry.entry_id, exc)
            self._notify_proxy_capture_restore_unconfirmed()
            return False, str(exc)

    async def _async_reconcile_proxy_capture_session(
        self,
        snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """Auto-stop abandoned proxy sessions on lease expiry or after proxy loss."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None or not proxy_capture_session_is_active(state):
            return snapshot

        stop_reason = ""
        if proxy_capture_session_is_expired(state):
            stop_reason = "expired_lease"
        elif not self._proxy_capture_process_running():
            stop_reason = "interrupted_process_exit"

        if not stop_reason:
            return snapshot

        logger.warning(
            "Stopping proxy capture for entry %s due to %s",
            self.config_entry.entry_id,
            stop_reason,
        )
        await self.async_stop_proxy_capture(
            reason=stop_reason,
            prefer_proxy_restore_trigger=stop_reason == "expired_lease",
            request_refresh=False,
        )
        self._ensure_poll_scheduler()
        return await self._runtime.async_refresh(
            poll_interval=self._poll_scheduler.current_interval()
        )

    async def _async_reconcile_shadow_learning_session(
        self,
        snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """Auto-stop abandoned shadow-learning sessions on lease expiry or route interruption."""

        state = await self._async_active_shadow_learning_state(require_process=False)
        if state is None or not shadow_learning_session_is_active(state):
            return snapshot

        stop_reason = ""
        if shadow_learning_session_is_expired(state):
            stop_reason = "expired_lease"
        elif not self._shadow_learning_process_running():
            stop_reason = "interrupted_process_exit"

        if not stop_reason:
            return snapshot

        logger.warning(
            "Stopping shadow learning for entry %s due to %s",
            self.config_entry.entry_id,
            stop_reason,
        )
        await self.async_stop_shadow_learning(
            reason=stop_reason,
            request_refresh=False,
            raise_when_not_running=False,
        )
        self._ensure_poll_scheduler()
        return await self._runtime.async_refresh(
            poll_interval=self._poll_scheduler.current_interval()
        )

    def _proxy_capture_process_running(self) -> bool:
        route_running = getattr(self._runtime, "proxy_capture_route_running", None)
        return bool(route_running is not None and route_running())

    def _shadow_learning_process_running(self) -> bool:
        route_running = getattr(self._runtime, "shadow_learning_route_running", None)
        return bool(route_running is not None and route_running())

    def _shadow_learning_route_status(self) -> dict[str, object]:
        route_status = getattr(self._runtime, "shadow_learning_route_status", None)
        if route_status is None:
            return {
                "running": self._shadow_learning_process_running(),
                "collector_connected": False,
                "collector_connection_sequence": 0,
                "collector_protocol_ingress": False,
                "route_protocol_activity": False,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }
        status = route_status()
        if not isinstance(status, dict):
            return {
                "running": self._shadow_learning_process_running(),
                "collector_connected": False,
                "collector_connection_sequence": 0,
                "collector_protocol_ingress": False,
                "route_protocol_activity": False,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }
        return {
            "running": bool(status.get("running")),
            "collector_connected": bool(status.get("collector_connected")),
            "collector_connection_sequence": int(status.get("collector_connection_sequence") or 0),
            "collector_protocol_ingress": bool(status.get("collector_protocol_ingress")),
            "route_protocol_activity": bool(status.get("route_protocol_activity")),
            "upstream_connected": bool(status.get("upstream_connected")),
            "ready": bool(status.get("ready")),
            "upstream_error": str(status.get("upstream_error") or ""),
        }

    async def _async_wait_for_shadow_learning_ready(
        self,
        *,
        trace_path: Path,
        timeout_seconds: float,
        min_collector_connection_sequence: int = 0,
    ) -> None:
        del trace_path
        deadline = asyncio.get_running_loop().time() + max(float(timeout_seconds), 1.0)
        phase = "waiting_for_collector"
        while asyncio.get_running_loop().time() < deadline:
            if not self._shadow_learning_process_running():
                raise RuntimeError("shadow_learning_route_stopped")
            status = self._shadow_learning_route_status()
            upstream_error = str(status.get("upstream_error") or "")
            if upstream_error:
                raise RuntimeError(f"shadow_learning_upstream_connect_failed:{upstream_error}")
            # Return the instant the collector has reconnected to our proxy and is speaking our
            # protocol -- the same moment proxy capture keys off. Do NOT wait for the full
            # ``ready`` flag: it additionally requires the short-lived upstream proxy->cloud
            # socket, which connects on demand, so waiting for it false-timed-out here and
            # triggered a premature restore of the collector back to the real server. This is the
            # SAME predicate the per-write control gate uses, so start and gate never disagree.
            if route_status_indicates_control_ready(status):
                if (
                    min_collector_connection_sequence > 0
                    and int(status.get("collector_connection_sequence") or 0)
                    <= min_collector_connection_sequence
                ):
                    await asyncio.sleep(1.0)
                    continue
                return
            collector_connected = bool(status.get("collector_connected"))
            next_phase = "connecting_upstream" if collector_connected else "waiting_for_collector"
            if next_phase != phase:
                phase = next_phase
                state = await self._async_active_shadow_learning_state(require_process=False)
                if state is not None:
                    await self._async_save_shadow_learning_session_state(
                        build_shadow_learning_session_state(
                            entry_id=state.entry_id,
                            route_owner_id=state.route_owner_id,
                            collector_pn=state.collector_pn,
                            trace_path=state.trace_path,
                            original_endpoint=state.original_endpoint,
                            proxy_endpoint=state.proxy_endpoint,
                            upstream_endpoint=state.upstream_endpoint,
                            restore_required=state.restore_required,
                            started_at=state.started_at,
                            expires_at=state.expires_at,
                            updated_at=shadow_learning_session_timestamp(),
                            restore_attempt_count=state.restore_attempt_count,
                            last_restore_attempt_at=state.last_restore_attempt_at,
                            last_restore_error=state.last_restore_error,
                            status=phase,
                        )
                    )
                self._publish_tooling_values(
                    shadow_learning_session_status=phase,
                    shadow_learning_session_ready=False,
                    local_metadata_status=(
                        "Shadow-learning waiting for collector"
                        if phase == "waiting_for_collector"
                        else "Shadow-learning connecting upstream"
                    ),
                )
            await asyncio.sleep(1.0)
        raise TimeoutError("shadow_learning_collector_reconnect_timeout")

    async def _async_guarded_proxy_capture_restore(
        self,
        *,
        state,
        prefer_proxy_restore_trigger: bool,
    ) -> dict[str, object]:
        """Restore the collector callback only while the proxy still owns the endpoint."""

        current_endpoint = await self._async_read_live_collector_server_endpoint()
        restore_skipped_reason = proxy_capture_restore_guard_reason(
            state,
            current_endpoint=current_endpoint,
        )
        if not state.restore_required or not state.original_endpoint:
            await self._async_stop_proxy_capture_process(owner_id=state.route_owner_id)
            return {
                "current_endpoint": current_endpoint,
                "restored_endpoint": current_endpoint,
                "restore_confirmed": True,
                "restore_mode": "not_required",
                "restore_skipped_reason": "",
            }

        if restore_skipped_reason:
            await self._async_stop_proxy_capture_process(owner_id=state.route_owner_id)
            return {
                "current_endpoint": current_endpoint,
                "restored_endpoint": current_endpoint,
                "restore_confirmed": restore_skipped_reason == "current_endpoint_changed",
                "restore_mode": "skipped",
                "restore_skipped_reason": restore_skipped_reason,
            }

        if prefer_proxy_restore_trigger and self._proxy_capture_process_running():
            restored_by_trigger = await self._async_trigger_proxy_capture_restore(
                trace_path=Path(state.trace_path),
                owner_id=state.route_owner_id,
            )
            if restored_by_trigger:
                return {
                    "current_endpoint": current_endpoint,
                    "restored_endpoint": state.original_endpoint,
                    "restore_confirmed": True,
                    "restore_mode": "proxy_trigger",
                    "restore_skipped_reason": "",
                }

            current_endpoint = await self._async_read_live_collector_server_endpoint()
            restore_skipped_reason = proxy_capture_restore_guard_reason(
                state,
                current_endpoint=current_endpoint,
            )
            if restore_skipped_reason:
                return {
                    "current_endpoint": current_endpoint,
                    "restored_endpoint": current_endpoint,
                    "restore_confirmed": restore_skipped_reason == "current_endpoint_changed",
                    "restore_mode": "skipped",
                    "restore_skipped_reason": restore_skipped_reason,
                }

        try:
            restored_endpoint = await self._async_restore_proxy_capture_endpoint(state.original_endpoint)
        except Exception as exc:
            logger.warning("Proxy capture direct restore failed for entry %s: %s", self.config_entry.entry_id, exc)
            await self._async_stop_proxy_capture_process(owner_id=state.route_owner_id)
            return {
                "current_endpoint": current_endpoint,
                "restored_endpoint": current_endpoint,
                "restore_confirmed": False,
                "restore_mode": "direct_failed",
                "restore_skipped_reason": "",
            }

        await self._async_stop_proxy_capture_process(owner_id=state.route_owner_id)
        return {
            "current_endpoint": current_endpoint,
            "restored_endpoint": restored_endpoint,
            "restore_confirmed": True,
            "restore_mode": "direct",
            "restore_skipped_reason": "",
        }

    def _proxy_capture_result_status(self, reason: str, *, restore_confirmed: bool) -> str:
        normalized_reason = str(reason or "stopped").strip() or "stopped"
        if restore_confirmed:
            return {
                "expired_lease": "expired_stopped",
                "recovered_after_restart": "recovered_after_restart",
                "interrupted_process_exit": "recovered_after_process_exit",
            }.get(normalized_reason, "stopped")
        return {
            "expired_lease": "expired_restore_unconfirmed",
            "recovered_after_restart": "recovered_after_restart_restore_unconfirmed",
            "interrupted_process_exit": "recovered_after_process_exit_restore_unconfirmed",
        }.get(normalized_reason, "stopped_restore_unconfirmed")

    def _proxy_capture_local_status(self, reason: str, *, phase: str) -> str:
        normalized_reason = str(reason or "stopped").strip() or "stopped"
        if phase == "stopping":
            return "Stopping collector proxy capture"
        return {
            "recovered_after_restart": "Recovered interrupted collector proxy capture",
            "interrupted_process_exit": "Recovered interrupted collector proxy capture",
        }.get(normalized_reason, "Collector proxy capture stopped")

    def _notify_proxy_capture_restore_unconfirmed(self) -> None:
        persistent_notification.async_create(
            self.hass,
            _localized_runtime_text(self.hass, "proxy_capture_restore_unconfirmed_body"),
            title=_localized_runtime_text(self.hass, "proxy_capture_restore_unconfirmed_title"),
            notification_id=f"{DOMAIN}_proxy_capture_restore_unconfirmed_{self.config_entry.entry_id}",
        )

    @property
    def smartess_known_family_draft_plan(self) -> SmartEssKnownFamilyDraftPlan | None:
        """Return one safe SmartESS known-family draft plan when available."""

        collector = self.data.collector
        record = self._latest_smartess_cloud_evidence_record()
        return resolve_smartess_known_family_draft_plan(
            smartess_protocol_asset_id=(
                getattr(collector, "smartess_protocol_asset_id", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "") or "")
            ),
            smartess_profile_key=(
                getattr(collector, "smartess_protocol_profile_key", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_PROFILE_KEY, "") or "")
            ),
            cloud_evidence=record.payload if record is not None else None,
        )

    @property
    def smartess_smg_bridge_plan(self) -> SmartEssSmgBridgePlan | None:
        """Return one safe SmartESS-backed SMG bridge plan when available."""

        record = self._latest_smartess_cloud_evidence_record()
        return resolve_smartess_smg_bridge_plan(
            effective_owner_key=self.effective_owner_key,
            source_profile_name=self.effective_profile_name,
            source_schema_name=self.effective_register_schema_name,
            source_profile_path=str(getattr(self.effective_profile_metadata, "source_path", "") or ""),
            source_schema_path=str(getattr(self.effective_register_schema_metadata, "source_path", "") or ""),
            cloud_evidence=record.payload if record is not None else None,
        )

    async def async_export_smartess_cloud_evidence(
        self,
        *,
        username: str,
        password: str,
    ) -> str:
        """Fetch and persist one SmartESS cloud-evidence bundle for this entry."""

        if self.cloud_evidence_provider != "smartess":
            raise RuntimeError(
                f"cloud_evidence_provider_not_supported:{self.cloud_evidence_provider or 'unknown'}"
            )
        return await self.async_export_cloud_evidence(
            username=username,
            password=password,
        )

    async def async_export_cloud_evidence(
        self,
        *,
        username: str,
        password: str,
    ) -> str:
        """Fetch and persist one provider-specific cloud-evidence bundle for this entry."""

        collector_pn = self.smartess_collector_pn
        if not collector_pn:
            raise RuntimeError("cloud_evidence_collector_pn_not_available")
        provider = self.cloud_evidence_provider
        if provider not in {"smartess", "valuecloud"}:
            raise RuntimeError(f"cloud_evidence_provider_not_supported:{provider or 'unknown'}")

        record = await self.hass.async_add_executor_job(
            lambda: fetch_and_export_device_bundle_cloud_evidence(
                provider=provider,
                config_dir=Path(self.hass.config.config_dir),
                username=username,
                password=password,
                collector_pn=collector_pn,
                source=f"{provider}_cloud_diagnostics",
                entry_id=self.config_entry.entry_id,
            )
        )
        self._cached_smartess_cloud_evidence_record = record
        self._cached_smartess_cloud_evidence_warmed = True
        self._publish_tooling_values(
            cloud_evidence_path=str(record.path),
            local_metadata_status=(
                "SmartESS cloud evidence exported"
                if provider == "smartess"
                else "Cloud evidence exported"
            ),
        )
        return str(record.path)

    async def async_export_support_bundle(self) -> str:
        """Export one JSON support bundle for the current entry."""

        await self._async_refresh_before_support_export()
        integration_build_values = await self.hass.async_add_executor_job(
            _integration_build_runtime_values
        )
        collector_registry_lookup = await self._async_collector_registry_lookup()
        support_bundle_payload = self._build_support_bundle_payload(
            integration_build_values=integration_build_values,
            collector_registry_lookup=collector_registry_lookup,
        )
        path = await self.hass.async_add_executor_job(
            lambda: export_support_bundle(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self._support_context_title(),
                connected=support_bundle_payload["runtime"]["connected"],
                collector=support_bundle_payload["runtime"]["collector"],
                inverter=support_bundle_payload["runtime"]["inverter"],
                values=support_bundle_payload["runtime"]["values"],
                data=support_bundle_payload["entry"]["data"],
                options=support_bundle_payload["entry"]["options"],
                profile_name=support_bundle_payload["source_metadata"]["profile_name"],
                register_schema_name=support_bundle_payload["source_metadata"]["register_schema_name"],
                cloud_evidence=support_bundle_payload["evidence"]["cloud"],
            )
        )
        self._publish_tooling_values(
            cloud_evidence_path=str(
                support_bundle_payload["runtime"]["values"].get("cloud_evidence_path") or ""
            ),
            support_bundle_path=str(path),
            local_metadata_status="Support bundle exported",
        )
        return str(path)

    async def async_export_support_package(self) -> str:
        """Export one combined support archive with raw capture and replay fixture."""

        return await self.async_export_support_package_with_cloud_refresh()

    async def async_export_support_package_with_cloud_refresh(
        self,
        *,
        smartess_username: str = "",
        smartess_password: str = "",
        wants_refresh: bool | None = None,
    ) -> str:
        """Export one support archive, optionally refreshing SmartESS cloud evidence first.

        ``wants_refresh`` lets the caller override the legacy "refresh when any
        credential field is non-empty" inference so that ``USE_SAVED`` mode can
        be honored even when credentials are pre-filled in the form. The legacy
        behavior is preserved when the parameter is left unset.
        """

        async def _factory() -> str:
            try:
                return await self._async_export_support_package_with_cloud_refresh_unlocked(
                    smartess_username=smartess_username,
                    smartess_password=smartess_password,
                    wants_refresh=wants_refresh,
                )
            except Exception:
                self._publish_tooling_values(
                    local_metadata_status="Support archive export failed"
                )
                raise

        return await self._support_package_flight.run(
            _factory,
            on_start=self._mark_support_package_active,
            on_finish=self._mark_support_package_idle,
        )

    async def _async_export_support_package_with_cloud_refresh_unlocked(
        self,
        *,
        smartess_username: str = "",
        smartess_password: str = "",
        wants_refresh: bool | None = None,
    ) -> str:
        """Export one support archive after the single-flight guard is acquired."""

        if wants_refresh is None:
            wants_refresh = bool(smartess_username or smartess_password)
        if wants_refresh:
            if not smartess_username or not smartess_password:
                raise RuntimeError("cloud_credentials_required")
            try:
                await self.async_export_cloud_evidence(
                    username=smartess_username,
                    password=smartess_password,
                )
            except Exception as exc:
                if self._cached_smartess_cloud_evidence_record is None:
                    raise
                logger.warning(
                    "Cloud evidence refresh failed; building archive with last saved evidence: %s",
                    exc,
                )
                self._publish_tooling_values(
                    local_metadata_status=(
                        "Cloud evidence refresh failed; using last saved evidence"
                    ),
                )

        integration_build_values = await self.hass.async_add_executor_job(
            _integration_build_runtime_values
        )
        support_bundle_payload, raw_capture = await self._async_build_support_package_payloads(
            integration_build_values=integration_build_values,
        )
        fixture = self._build_support_fixture(raw_capture)
        anonymized_fixture = anonymize_fixture_json(fixture) if fixture is not None else None
        profile_metadata = self.effective_profile_metadata
        register_schema_metadata = self.effective_register_schema_metadata

        export_result = await self.hass.async_add_executor_job(
            lambda: export_support_package(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self._support_context_title(),
                support_bundle=support_bundle_payload,
                raw_capture=raw_capture,
                fixture=fixture,
                anonymized_fixture=anonymized_fixture,
                profile_source=self._metadata_source_payload(profile_metadata),
                register_schema_source=self._metadata_source_payload(register_schema_metadata),
            )
        )
        path = export_result.path
        if export_result.download_url:
            relative_download_url = str(export_result.download_url)
            download_url = self._absolute_local_download_url(relative_download_url)
        else:
            # Use a short-lived signed HA API path for browser navigation. A
            # plain authenticated API URL returns 401 when opened from markdown,
            # because the browser does not attach the HA bearer token to a
            # normal link click. Store the HA-relative path for diagnostics, but
            # expose an absolute URL in the UI: HA's config-flow frontend may
            # otherwise SPA-route a relative /api link as the current Lovelace
            # route with only the authSig query preserved.
            relative_download_url = sign_support_package_download_url(
                self.hass,
                self.config_entry.entry_id,
            )
            download_url = self._absolute_local_download_url(relative_download_url)
        self._publish_tooling_values(
            cloud_evidence_path=str(
                support_bundle_payload["runtime"]["values"].get("cloud_evidence_path") or ""
            ),
            support_package_path=str(path),
            support_package_download_path=str(export_result.download_path or ""),
            support_package_download_url=download_url,
            support_package_download_relative_url=relative_download_url,
            local_metadata_status="Support archive exported",
        )
        if download_url:
            persistent_notification.async_create(
                self.hass,
                _localized_runtime_text(
                    self.hass,
                    "support_archive_notification_body",
                    download_url=download_url,
                ),
                title=_localized_runtime_text(self.hass, "support_archive_notification_title"),
                notification_id=f"{DOMAIN}_support_package_{self.config_entry.entry_id}",
            )
        return str(path)

    async def _async_refresh_before_support_export(self) -> None:
        """Best-effort refresh so support archives reflect self-healed runtime state."""

        try:
            snapshot = await self._async_update_data()
        except Exception as exc:  # noqa: BLE001 - support export must remain available
            logger.warning(
                "Support archive pre-refresh failed for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            return
        if snapshot is not None:
            self.data = snapshot

    async def _async_build_support_package_payloads(
        self,
        *,
        integration_build_values: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Build support bundle payload and raw capture under one runtime operation lock."""

        async with self._runtime_operation_lock:
            try:
                snapshot = await self._async_update_data_with_runtime_lock()
            except Exception as exc:  # noqa: BLE001 - support export must remain available
                logger.warning(
                    "Support archive pre-refresh failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
            else:
                if snapshot is not None:
                    self.data = snapshot

            collector_registry_lookup = await self._async_collector_registry_lookup()
            support_bundle_payload = self._build_support_bundle_payload(
                integration_build_values=integration_build_values,
                collector_registry_lookup=collector_registry_lookup,
            )
            try:
                raw_capture = await self._runtime.async_capture_support_evidence()
            except Exception as exc:
                raw_capture = {
                    "capture_kind": "unsupported_or_failed",
                    "error": str(exc),
                    "captured_ranges": [],
                    "range_failures": [],
                }

        return support_bundle_payload, raw_capture

    async def _async_collector_registry_lookup(self) -> tuple[str, Any | None]:
        """Read the collector registry record without blocking the Home Assistant loop."""

        collector_pn = self._preferred_collector_pn(self.data)
        if not collector_pn:
            return "unavailable", None

        try:
            record = await self.hass.async_add_executor_job(
                lambda: get_collector_registry_record(
                    config_dir=Path(self.hass.config.path()),
                    collector_pn=collector_pn,
                )
            )
        except Exception as exc:  # pragma: no cover - defensive diagnostics only
            return f"error:{type(exc).__name__}", None

        return ("found" if record is not None else "missing"), record

    async def async_create_local_profile_draft(self) -> str:
        """Create or refresh one local experimental profile draft."""

        return await self.async_create_local_profile_draft_named()

    async def async_create_local_profile_draft_named(
        self,
        output_profile_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> str:
        """Create or refresh one local experimental profile draft."""

        source_profile_name = self.effective_profile_name
        if not source_profile_name:
            raise RuntimeError("driver_profile_not_available")
        path = await self.hass.async_add_executor_job(
            lambda: create_local_profile_draft(
                config_dir=Path(self.hass.config.config_dir),
                source_profile_name=source_profile_name,
                output_profile_name=output_profile_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            local_profile_draft_path=str(path),
            local_metadata_status="Local profile draft created",
        )
        return str(path)

    async def async_create_local_schema_draft(self) -> str:
        """Create or refresh one local experimental register schema draft."""

        return await self.async_create_local_schema_draft_named()

    async def async_create_local_schema_draft_named(
        self,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> str:
        """Create or refresh one local experimental register schema draft."""

        source_schema_name = self.effective_register_schema_name
        if not source_schema_name:
            raise RuntimeError("driver_register_schema_not_available")
        path = await self.hass.async_add_executor_job(
            lambda: create_local_schema_draft(
                config_dir=Path(self.hass.config.config_dir),
                source_schema_name=source_schema_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            local_schema_draft_path=str(path),
            local_metadata_status="Local register schema draft created",
        )
        return str(path)

    async def async_reload_local_metadata(self) -> None:
        """Reload the current config entry after local metadata changes."""

        self._cached_effective_metadata = None
        clear_local_metadata_loader_caches()
        self._publish_tooling_values(local_metadata_status="Reloading local metadata")
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)

    async def async_activate_device_scoped_overlay(
        self,
        *,
        profile_name: str,
        register_schema_name: str,
        selection: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist one explicit device-scoped learned overlay activation and reload.

        ``selection`` carries the user's control choices for this device (built from the
        review screen via ``build_activation_selection``). When provided, the activation
        records ``selected_controls`` (with user labels), ``excluded_controls`` (with
        retained reasons), and ``selected_control_keys`` so runtime exposes only the
        selected learned controls. When omitted, the activation declares no selection and
        runtime keeps exposing every learned control (legacy behavior).
        """

        normalized_profile_name = str(profile_name or "").strip()
        normalized_schema_name = str(register_schema_name or "").strip()
        if not normalized_profile_name or not normalized_schema_name:
            raise ValueError("device_scoped_overlay_activation_requires_profile_and_schema")

        collector = self.data.collector
        inverter = self.identified_inverter
        write_context = self._write_exposure_context()
        activation_scope: dict[str, Any] = {
            "effective_owner_key": str(self.effective_owner_key or "").strip(),
            # Rebase to the built-in base: when activating an overlay while another is
            # already active, ``effective_*_name`` is the *previous* overlay's learned
            # name. Storing that raw would poison the device-scope match on reload (the
            # runtime base resolves to the built-in name), silently suppressing the
            # activation. The scope matcher also rebases defensively, so old activations
            # self-heal; this keeps newly written activations clean at the source.
            "base_profile_name": str(
                builtin_base_profile_name(self.effective_profile_name or "")
            ).strip(),
            "base_register_schema_name": str(
                builtin_base_schema_name(self.effective_register_schema_name or "")
            ).strip(),
            "variant_key": str(write_context.get("variant_key") or "").strip(),
            "collector_pn": str(
                getattr(collector, "collector_pn", "")
                or self.config_entry.data.get(CONF_COLLECTOR_PN, "")
                or ""
            ).strip(),
            "smartess_protocol_asset_id": str(
                getattr(collector, "smartess_protocol_asset_id", "")
                or self.config_entry.data.get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "")
                or ""
            ).strip(),
            "smartess_protocol_profile_key": str(
                getattr(collector, "smartess_protocol_profile_key", "")
                or self.config_entry.data.get(CONF_SMARTESS_PROFILE_KEY, "")
                or ""
            ).strip(),
            "smartess_device_address": (
                getattr(collector, "smartess_device_address", None)
                if getattr(collector, "smartess_device_address", None) is not None
                else self.config_entry.data.get(CONF_SMARTESS_DEVICE_ADDRESS)
            ),
            "inverter_model": str(getattr(inverter, "model_name", "") or "").strip(),
            "inverter_serial": str(getattr(inverter, "serial_number", "") or "").strip(),
        }
        activation = {
            "profile_name": normalized_profile_name,
            "register_schema_name": normalized_schema_name,
            "scope": "device",
            "activated_at": datetime.now(timezone.utc).isoformat(),
            "activation_scope": activation_scope,
        }
        if selection is not None:
            activation.update(normalize_activation_selection(selection))

        options = dict(self.config_entry.options)
        options[_DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY] = activation
        self._async_update_entry_without_reload(options=options)

        self._cached_effective_metadata = None
        clear_local_metadata_loader_caches()
        self._publish_tooling_values(
            local_metadata_status="Device-scoped learned overlay activated; reloading local metadata",
        )
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)
        return activation

    async def async_rollback_local_metadata(self) -> tuple[str, ...]:
        """Remove active managed local overrides and reload the entry."""

        removed_paths = await self.hass.async_add_executor_job(
            lambda: rollback_local_metadata_overrides(
                config_dir=Path(self.hass.config.config_dir),
                profile_name=self.effective_profile_name or None,
                schema_name=self.effective_register_schema_name or None,
                profile_metadata=self.effective_profile_metadata,
                schema_metadata=self.effective_register_schema_metadata,
            )
        )
        clear_local_metadata_loader_caches()
        self._cached_effective_metadata = None
        self._publish_tooling_values(local_metadata_status="Rolling back local metadata")
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)
        return tuple(str(path) for path in removed_paths)

    async def async_create_smartess_known_family_draft_named(
        self,
        output_profile_name: str | None = None,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> tuple[str, str]:
        """Create local profile/schema drafts from latest SmartESS known-family evidence."""

        record = self._latest_smartess_cloud_evidence_record()
        if record is None:
            raise RuntimeError("smartess_cloud_evidence_not_available")

        plan = self.smartess_known_family_draft_plan
        if plan is None:
            raise RuntimeError("smartess_known_family_not_resolved")

        profile_path, schema_path = await self.hass.async_add_executor_job(
            lambda: create_smartess_known_family_draft(
                config_dir=Path(self.hass.config.config_dir),
                plan=plan,
                cloud_evidence=record.payload,
                output_profile_name=output_profile_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            cloud_evidence_path=str(record.path),
            local_profile_draft_path=str(profile_path),
            local_schema_draft_path=str(schema_path),
            local_metadata_status="SmartESS local draft created",
        )
        return str(profile_path), str(schema_path)

    async def async_create_smartess_smg_bridge_named(
        self,
        output_profile_name: str | None = None,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> tuple[str, str]:
        """Create one SmartESS-backed SMG bridge draft pair."""

        record = self._latest_smartess_cloud_evidence_record()
        if record is None:
            raise RuntimeError("smartess_cloud_evidence_not_available")

        plan = self.smartess_smg_bridge_plan
        if plan is None:
            raise RuntimeError("smartess_smg_bridge_not_resolved")

        profile_path, schema_path = await self.hass.async_add_executor_job(
            lambda: create_smartess_smg_bridge_draft(
                config_dir=Path(self.hass.config.config_dir),
                plan=plan,
                cloud_evidence=record.payload,
                output_profile_name=output_profile_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            cloud_evidence_path=str(record.path),
            local_profile_draft_path=str(profile_path),
            local_schema_draft_path=str(schema_path),
            local_metadata_status="SmartESS SMG bridge created",
        )
        return str(profile_path), str(schema_path)

    def _latest_smartess_cloud_evidence_record(self):
        """Return the latest SmartESS cloud-evidence record for this entry.

        Reads from the in-memory cache populated by ``_async_warm_smartess_cloud_evidence_cache``
        and the export helpers. Sync callers (config-flow form rendering, sync
        properties) get a cached value without doing blocking disk IO on the
        event loop.
        """

        return self._cached_smartess_cloud_evidence_record

    def _load_latest_smartess_cloud_evidence_record_blocking(self):
        """Return the latest SmartESS cloud-evidence record by reading disk.

        Must only be called from an executor thread (or a sync test path).
        """

        return load_latest_cloud_evidence(
            Path(self.hass.config.config_dir),
            entry_id=self.config_entry.entry_id,
            collector_pn=self.smartess_collector_pn,
        )

    async def _async_warm_smartess_cloud_evidence_cache(self) -> None:
        """Refresh the cached SmartESS cloud-evidence record from disk."""

        record = await self.hass.async_add_executor_job(
            self._load_latest_smartess_cloud_evidence_record_blocking
        )
        self._cached_smartess_cloud_evidence_record = record
        self._cached_smartess_cloud_evidence_warmed = True

    def _warm_effective_metadata_cache_blocking(self):
        """Resolve effective metadata and force profile/schema cache population."""

        metadata = resolve_effective_metadata_selection(
            inverter=self.identified_inverter,
            driver=self.current_driver,
            collector=self.data.collector,
            entry_data=self.config_entry.data,
            entry_options=self.config_entry.options,
            persisted_snapshot=self.effective_metadata_snapshot,
        )
        # Access the lazy fields in the executor thread. The sync properties are used
        # later by HA runtime/UI code, so their JSON files must already be cached there.
        _ = metadata.profile_metadata
        _ = metadata.register_schema_metadata
        return metadata

    async def _async_warm_effective_metadata_cache(self) -> None:
        """Warm profile/schema loaders outside the event loop."""

        try:
            self._cached_effective_metadata = await self.hass.async_add_executor_job(
                self._warm_effective_metadata_cache_blocking
            )
        except Exception as exc:
            self._cached_effective_metadata = None
            logger.debug("Effective metadata cache warm-up failed: %s", exc)

    def _latest_proxy_trace_record(self):
        """Return the latest proxy-trace manifest record for this entry."""

        return load_latest_proxy_trace_manifest(
            Path(self.hass.config.config_dir),
            entry_id=self.config_entry.entry_id,
            collector_pn=self.smartess_collector_pn,
        )

    async def _async_latest_proxy_trace_record(self):
        """Return the latest proxy-trace manifest record for this entry without blocking."""

        return await self.hass.async_add_executor_job(
            lambda: load_latest_proxy_trace_manifest(
                Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                collector_pn=self.smartess_collector_pn,
            )
        )

    def _active_proxy_capture_state(self, *, require_process: bool = True):
        """Return the last persisted proxy capture session state cached by async paths."""

        del require_process
        cached_state = getattr(self, "_cached_proxy_capture_session_state", None)
        if cached_state is not None:
            return cached_state
        return None

    async def _async_active_proxy_capture_state(self, *, require_process: bool = True):
        """Return the persisted active proxy capture state when it belongs to this entry."""

        del require_process
        state = await self.hass.async_add_executor_job(
            lambda: load_proxy_capture_session_state(Path(self.hass.config.config_dir))
        )
        if state is None:
            self._cached_proxy_capture_session_state = None
            return None
        if state.entry_id and state.entry_id != self.config_entry.entry_id:
            self._cached_proxy_capture_session_state = None
            return None
        collector_pn = self.smartess_collector_pn
        if collector_pn and state.collector_pn and state.collector_pn != collector_pn:
            self._cached_proxy_capture_session_state = None
            return None
        self._cached_proxy_capture_session_state = state
        return state

    async def _async_save_proxy_capture_session_state(self, state) -> None:
        """Persist one proxy capture session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: save_proxy_capture_session_state(
                config_dir=Path(self.hass.config.config_dir),
                state=state,
            )
        )
        self._cached_proxy_capture_session_state = state
        if proxy_capture_session_is_active(state):
            self._schedule_proxy_capture_deadline_refresh(state.expires_at)
        else:
            self._cancel_proxy_capture_deadline_refresh()

    async def _async_clear_proxy_capture_session_state(self) -> None:
        """Delete persisted proxy capture session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: clear_proxy_capture_session_state(Path(self.hass.config.config_dir))
        )

        self._cached_proxy_capture_session_state = None
        self._cancel_proxy_capture_deadline_refresh()
        self._clear_proxy_capture_session_runtime_values()

    async def _async_active_shadow_learning_state(self, *, require_process: bool = True):
        """Return the persisted active shadow-learning state when it belongs to this entry."""

        del require_process
        if self._shadow_learning_session_state_loaded:
            # Authoritative in-memory cache: save/clear keep it fresh and this
            # coordinator is the only writer, so skip the per-refresh disk read.
            return self._cached_shadow_learning_session_state
        state = await self.hass.async_add_executor_job(
            lambda: load_shadow_learning_session_state(Path(self.hass.config.config_dir))
        )
        self._shadow_learning_session_state_loaded = True
        if state is None:
            self._cached_shadow_learning_session_state = None
            return None
        if state.entry_id and state.entry_id != self.config_entry.entry_id:
            self._cached_shadow_learning_session_state = None
            return None
        collector_pn = self.smartess_collector_pn
        if collector_pn and state.collector_pn and state.collector_pn != collector_pn:
            self._cached_shadow_learning_session_state = None
            return None
        self._cached_shadow_learning_session_state = state
        return state

    async def _async_save_shadow_learning_session_state(self, state) -> None:
        """Persist one shadow-learning session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: save_shadow_learning_session_state(
                config_dir=Path(self.hass.config.config_dir),
                state=state,
            )
        )
        self._cached_shadow_learning_session_state = state
        self._shadow_learning_session_state_loaded = True

    async def _async_clear_shadow_learning_session_state(self) -> None:
        """Delete persisted shadow-learning session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: clear_shadow_learning_session_state(Path(self.hass.config.config_dir))
        )
        self._cached_shadow_learning_session_state = None
        self._shadow_learning_session_state_loaded = True

    def _clear_proxy_capture_session_runtime_values(self) -> None:
        """Drop stale transient proxy-session values from both cache and current snapshot."""

        snapshot_values = getattr(self.data, "values", None)
        for key in _PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS:
            self._tooling_values.pop(key, None)
            if isinstance(snapshot_values, dict):
                snapshot_values.pop(key, None)

    def _proxy_capture_runtime_values(self) -> dict[str, Any]:
        """Return current proxy-capture UI values with snapshot data preferred over tooling cache."""

        values = dict(getattr(self, "_tooling_values", {}))
        values.update(getattr(self.data, "values", {}) or {})
        return values

    def _proxy_capture_timer_runtime_values(self, state=None) -> dict[str, Any]:
        """Return proxy capture duration and countdown runtime values."""

        remaining_seconds = 0
        if state is not None:
            remaining_seconds = _proxy_capture_remaining_seconds(getattr(state, "expires_at", ""))
        remaining_minutes = max(1, (remaining_seconds + 59) // 60) if remaining_seconds > 0 else 0
        return {
            CONF_PROXY_CAPTURE_DURATION_MINUTES: self.proxy_capture_configured_duration_minutes,
            "proxy_capture_remaining_seconds": remaining_seconds,
            "proxy_capture_remaining_minutes": remaining_minutes,
        }

    def _proxy_capture_overview_runtime_values(
        self,
        *,
        active_state=None,
        current_endpoint: str = "",
    ) -> dict[str, Any]:
        """Build immediate proxy-capture runtime values for transition-aware entity UX."""

        snapshot = self.data
        runtime_values = self._proxy_capture_runtime_values()
        overview = build_proxy_capture_overview(
            control_mode=self.control_mode,
            collector_control_allowed=self.collector_actions_enabled,
            collector_proxy_capture_allowed=self.collector_capabilities.proxy_capture,
            collector_connected=bool(snapshot.connected),
            collector_cloud_family=self.collector_cloud_family,
            current_endpoint=str(
                current_endpoint
                or runtime_values.get("collector_server_endpoint")
                or snapshot.values.get("collector_server_endpoint")
                or ""
            ),
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            target_endpoint=self.proxy_capture_target_endpoint,
            active_state=active_state,
            latest_trace_path=self.latest_proxy_trace_path,
            latest_manifest_path=self.latest_proxy_trace_manifest_path,
        )
        values: dict[str, Any] = {
            "proxy_capture_status": overview.status,
            "proxy_capture_status_label": overview.status_label,
            "proxy_capture_summary": overview.summary,
            "proxy_capture_blocking_reason": overview.blocking_reason,
            "proxy_capture_can_start": overview.can_start,
            "proxy_capture_can_stop": overview.can_stop,
            "proxy_capture_critical_phase": overview.critical_phase,
            "proxy_capture_redirect_required": overview.redirect_required,
            "proxy_capture_collector_cloud_family": self.collector_cloud_family,
            "proxy_capture_current_endpoint": overview.current_endpoint,
            "proxy_capture_target_endpoint": overview.target_endpoint,
            "proxy_capture_masked_endpoint": overview.masked_endpoint,
            "proxy_trace_path": overview.latest_trace_path,
            "proxy_trace_manifest_path": overview.latest_manifest_path,
        }
        values.update(self._proxy_capture_timer_runtime_values(active_state))
        if active_state is not None:
            values.update(
                {
                    "proxy_capture_session_status": str(active_state.status or "").strip(),
                    "proxy_capture_session_started_at": str(active_state.started_at or "").strip(),
                    "proxy_capture_session_expires_at": str(active_state.expires_at or "").strip(),
                    "proxy_capture_session_anonymized": bool(active_state.anonymized),
                }
            )
        return values

    async def _async_proxy_trace_manifest_download_details(self, manifest_path: str) -> tuple[str, str]:
        """Return the saved ZIP bundle path and published URL for one proxy capture."""

        normalized_manifest_path = str(manifest_path or "").strip()
        if not normalized_manifest_path:
            return "", ""
        if normalized_manifest_path == self._proxy_trace_download_manifest_path:
            return self._proxy_trace_download_details

        def _build_download_details() -> tuple[str, str]:
            path = Path(normalized_manifest_path)
            if not path.exists():
                return "", ""
            bundle_path = export_proxy_trace_bundle(
                manifest_path=path,
                overwrite=True,
            )
            _download_path, relative_url = publish_proxy_trace_download_copy(
                config_dir=Path(self.hass.config.config_dir),
                source_path=bundle_path,
            )
            return str(bundle_path), relative_url

        try:
            bundle_path, relative_url = await self.hass.async_add_executor_job(
                _build_download_details
            )
        except OSError:
            return "", ""

        absolute_url = self._absolute_local_download_url(relative_url)
        self._proxy_trace_download_manifest_path = normalized_manifest_path
        self._proxy_trace_download_details = (bundle_path, absolute_url)
        return self._proxy_trace_download_details

    def _publish_tooling_values(self, **values: Any) -> None:
        """Publish in-memory tooling results into coordinator snapshot values."""

        if getattr(self, "_shutdown_complete", False):
            return
        self._tooling_values.update(values)
        snapshot = self.data
        snapshot.values.update(self._tooling_values)
        publish = getattr(self, "async_set_updated_data", None)
        if publish is not None:
            publish(snapshot)

    def _publish_snapshot_values(self, **values: Any) -> None:
        """Publish transient runtime values into the live coordinator snapshot only."""

        if getattr(self, "_shutdown_complete", False):
            return
        snapshot = self.data
        for key, value in values.items():
            if value is None:
                snapshot.values.pop(key, None)
            else:
                snapshot.values[key] = value
        publish = getattr(self, "async_set_updated_data", None)
        if publish is not None:
            publish(snapshot)

    def invalidate_collector_runtime_values(self) -> None:
        """Invalidate cached collector-side runtime values before a forced refresh."""

        invalidator = getattr(self._runtime, "invalidate_collector_runtime_values", None)
        if callable(invalidator):
            invalidator()

    def _absolute_local_download_url(self, relative_url: str) -> str:
        """Return an absolute HA URL for one HA-relative download path when possible."""

        if not relative_url:
            return ""
        try:
            base_url = network.get_url(
                self.hass,
                allow_internal=True,
                allow_external=True,
                allow_cloud=False,
                prefer_external=True,
            ).rstrip("/")
        except network.NoURLAvailableError:
            return relative_url
        return f"{base_url}{relative_url}"

    def _support_workflow_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return user-facing support workflow guidance for the current entry."""

        snapshot = snapshot or self.data
        metadata = self.effective_metadata
        collector = snapshot.collector
        workflow = build_support_workflow_state(
            has_inverter=snapshot.inverter is not None,
            variant_key=getattr(snapshot.inverter, "variant_key", ""),
            profile_name=metadata.profile_name,
            effective_owner_key=metadata.effective_owner_key,
            effective_owner_name=metadata.effective_owner_name,
            smartess_family_name=metadata.smartess_family_name,
            detection_confidence=self.detection_confidence,
            profile_source_scope=getattr(metadata.profile_metadata, "source_scope", ""),
            schema_source_scope=getattr(metadata.register_schema_metadata, "source_scope", ""),
            smartess_protocol_asset_id=(
                getattr(collector, "smartess_protocol_asset_id", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "") or "")
            ),
            smartess_profile_key=(
                getattr(collector, "smartess_protocol_profile_key", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_PROFILE_KEY, "") or "")
            ),
            smartess_collector_version=(
                getattr(collector, "smartess_collector_version", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_COLLECTOR_VERSION, "") or "")
            ),
        )
        return {
            "support_workflow_level": workflow["level"],
            "support_workflow_level_label": workflow["level_label"],
            "support_workflow_summary": workflow["summary"],
            "support_workflow_next_action": workflow["next_action"],
            "support_workflow_primary_action": workflow["primary_action"],
            "support_workflow_step_1": workflow["step_1"],
            "support_workflow_step_2": workflow["step_2"],
            "support_workflow_step_3": workflow["step_3"],
            "support_workflow_plan": workflow["plan"],
            "support_workflow_advanced_hint": workflow["advanced_hint"],
        }

    def _collector_original_endpoint_runtime_values(
        self,
        *,
        include_registry: bool = False,
        registry_lookup: tuple[str, Any | None] | None = None,
    ) -> dict[str, Any]:
        """Return non-sensitive diagnostics for preserved original endpoint state."""

        options = getattr(self.config_entry, "options", {}) or {}
        remembered_endpoint = self._normalized_remembered_collector_server_endpoint()
        values: dict[str, Any] = {
            "collector_original_endpoint_known": bool(remembered_endpoint),
            "collector_original_endpoint_profile_key": str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY, "") or ""
            ).strip(),
            "collector_original_endpoint_source": str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE, "") or ""
            ).strip(),
            "collector_original_endpoint_observed_at": str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT, "") or ""
            ).strip(),
        }
        if not include_registry:
            return values

        collector_pn = self._preferred_collector_pn(self.data)
        if registry_lookup is not None:
            registry_status, record = registry_lookup
        else:
            registry_status = "unavailable"
            record = None
        if collector_pn and registry_lookup is None:
            try:
                record = get_collector_registry_record(
                    config_dir=Path(self.hass.config.path()),
                    collector_pn=collector_pn,
                )
                registry_status = "found" if record is not None else "missing"
            except Exception as exc:  # pragma: no cover - defensive diagnostics only
                registry_status = f"error:{type(exc).__name__}"
        values["collector_registry_record_status"] = registry_status
        values["collector_registry_record_pn_known"] = bool(collector_pn)
        if record is not None:
            values.update(
                {
                    "collector_registry_original_endpoint": record.original_endpoint_raw,
                    "collector_registry_cloud_profile_key": record.cloud_profile_key,
                    "collector_registry_source": record.source,
                    "collector_registry_observed_at": record.observed_at,
                    "collector_registry_last_seen_ip": record.last_seen_ip,
                }
            )
        return values

    def _collector_transport_profile_runtime_values(self) -> dict[str, Any]:
        """Return diagnostics that compare resolved profile and live link state."""

        profile = self.collector_transport_profile
        values: dict[str, Any] = {
            "collector_resolved_cloud_family": profile.cloud_family,
            "collector_resolved_runtime_owner_key": profile.runtime_owner_key,
            "collector_resolved_session_protocol": profile.session_protocol,
            "collector_resolved_identity_strategy": profile.identity_strategy,
        }
        connection = getattr(self, "_connection_spec", None)
        if connection is not None:
            values.update(
                {
                    "collector_connection_cloud_family": str(
                        getattr(connection, "collector_cloud_family", "") or ""
                    ),
                    "collector_connection_session_protocol": str(
                        getattr(connection, "collector_session_protocol", "") or ""
                    ),
                    "collector_connection_identity_strategy": str(
                        getattr(connection, "collector_identity_strategy", "") or ""
                    ),
                }
            )
        runtime = getattr(self, "_runtime", None)
        link_diagnostics = getattr(runtime, "listener_diagnostics", None)
        if callable(link_diagnostics):
            try:
                diagnostics = link_diagnostics()
            except Exception as exc:  # pragma: no cover - defensive diagnostics only
                values["collector_runtime_link_diagnostics_error"] = type(exc).__name__
            else:
                values["collector_runtime_link_session_protocol"] = str(
                    diagnostics.get("collector_callback_session_protocol") or ""
                )
                values["collector_runtime_link_identity_strategy"] = str(
                    diagnostics.get("collector_callback_identity_strategy") or ""
                )
        return values

    def _collector_onboarding_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return compact collector-side onboarding status helpers for entity UX."""

        snapshot = snapshot or self.data
        support_label = str(snapshot.values.get("support_workflow_level_label") or "").strip()
        return {
            "collector_onboarding_status": support_label or "Unknown",
            **self._collector_original_endpoint_runtime_values(),
            **self._collector_transport_profile_runtime_values(),
        }

    async def _proxy_capture_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return user-facing proxy capture status helpers for diagnostics UX."""

        snapshot = snapshot or self.data
        state = await self._async_active_proxy_capture_state(require_process=False)
        record = await self._async_latest_proxy_trace_record()
        trace_path = str(getattr(state, "trace_path", "") or "").strip()
        if not trace_path and record is not None:
            trace = record.payload.get("trace") if isinstance(record.payload, dict) else None
            if isinstance(trace, dict):
                trace_path = str(trace.get("path") or "").strip()
        manifest_path = "" if state is not None or record is None else str(record.path)
        trace_details = await self.hass.async_add_executor_job(
            lambda: inspect_proxy_capture_trace(Path(trace_path))
        ) if trace_path else {
            "exists": False,
            "line_count": 0,
            "kind_summary": "",
            "recent_kinds": "",
            "recent_events": "",
            "live_log": "",
            "last_timestamp": "",
        }
        overview = build_proxy_capture_overview(
            control_mode=self.control_mode,
            collector_control_allowed=self.collector_actions_enabled,
            collector_proxy_capture_allowed=self.collector_capabilities.proxy_capture,
            collector_connected=bool(snapshot.connected),
            collector_cloud_family=self.collector_cloud_family,
            current_endpoint=str(snapshot.values.get("collector_server_endpoint") or ""),
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            target_endpoint=self.proxy_capture_target_endpoint,
            active_state=state,
            latest_trace_path=trace_path,
            latest_manifest_path=manifest_path,
        )
        manifest_download_path, manifest_download_url = await self._async_proxy_trace_manifest_download_details(
            overview.latest_manifest_path
        )
        values: dict[str, Any] = {
            "proxy_capture_status": overview.status,
            "proxy_capture_status_label": overview.status_label,
            "proxy_capture_summary": overview.summary,
            "proxy_capture_blocking_reason": overview.blocking_reason,
            "proxy_capture_can_start": overview.can_start,
            "proxy_capture_can_stop": overview.can_stop,
            "proxy_capture_critical_phase": overview.critical_phase,
            "proxy_capture_redirect_required": overview.redirect_required,
            "proxy_capture_collector_cloud_family": self.collector_cloud_family,
            "proxy_capture_current_endpoint": overview.current_endpoint,
            "proxy_capture_target_endpoint": overview.target_endpoint,
            "proxy_capture_masked_endpoint": overview.masked_endpoint,
            "proxy_trace_path": overview.latest_trace_path,
            "proxy_trace_manifest_path": overview.latest_manifest_path,
            "proxy_trace_saved_result_path": manifest_download_path,
            "proxy_trace_saved_result_download_url": manifest_download_url,
            "proxy_trace_manifest_download_url": manifest_download_url,
            "proxy_trace_line_count": trace_details.get("line_count", 0),
            "proxy_trace_kind_summary": str(trace_details.get("kind_summary") or ""),
            "proxy_trace_recent_kinds": str(trace_details.get("recent_kinds") or ""),
            "proxy_trace_recent_events": str(trace_details.get("recent_events") or ""),
            "proxy_trace_live_log": str(trace_details.get("live_log") or ""),
            "proxy_trace_last_timestamp": str(trace_details.get("last_timestamp") or ""),
        }
        values.update(self._proxy_capture_timer_runtime_values(state))
        if state is not None:
            values["proxy_capture_session_status"] = state.status
            values["proxy_capture_session_started_at"] = state.started_at
            values["proxy_capture_session_expires_at"] = state.expires_at
            values["proxy_capture_session_anonymized"] = state.anonymized
        return values

    def _build_support_bundle_payload(
        self,
        *,
        integration_build_values: Mapping[str, object] | None = None,
        collector_registry_lookup: tuple[str, Any | None] | None = None,
    ) -> dict[str, Any]:
        inverter = self.data.inverter
        metadata = self.effective_metadata
        smartess_protocol = metadata.smartess_protocol
        values = dict(self.data.values)
        values.update(integration_build_values or _integration_build_runtime_values())
        values.update(self._collector_transport_profile_runtime_values())
        values.update(
            self._collector_original_endpoint_runtime_values(
                include_registry=True,
                registry_lookup=collector_registry_lookup,
            )
        )
        cloud_evidence_record = self._latest_smartess_cloud_evidence_record()
        cloud_evidence = None
        if cloud_evidence_record is not None:
            cloud_evidence = cloud_evidence_record.payload
            values["cloud_evidence_path"] = str(cloud_evidence_record.path)
        inverter_payload = None
        if inverter is not None:
            values["ui_schema"] = build_runtime_ui_schema(inverter, self.data.values)
            inverter_payload = self._inverter_payload(inverter)
        return build_support_bundle_payload(
            entry_id=self.config_entry.entry_id,
            entry_title=self._support_context_title(),
            connected=self.data.connected,
            collector=self._collector_payload(),
            inverter=inverter_payload,
            values=values,
            data=dict(self.config_entry.data),
            options=dict(self.config_entry.options),
            profile_name=metadata.profile_name,
            register_schema_name=metadata.register_schema_name,
            variant_key=getattr(inverter, "variant_key", ""),
            effective_owner_key=metadata.effective_owner_key,
            effective_owner_name=metadata.effective_owner_name,
            smartess_family_name=metadata.smartess_family_name,
            raw_profile_name=metadata.raw_profile_name,
            raw_register_schema_name=metadata.raw_register_schema_name,
            smartess_protocol_asset_id=getattr(smartess_protocol, "asset_id", ""),
            smartess_profile_key=getattr(smartess_protocol, "profile_key", ""),
            cloud_evidence=cloud_evidence,
        )

    def _collector_payload(self) -> dict[str, Any] | None:
        if self.data.collector is None:
            return None
        return {
            "remote_ip": self.data.collector.remote_ip,
            "remote_port": self.data.collector.remote_port,
            "connection_count": self.data.collector.connection_count,
            "connection_replace_count": self.data.collector.connection_replace_count,
            "disconnect_count": self.data.collector.disconnect_count,
            "pending_request_drop_count": self.data.collector.pending_request_drop_count,
            "last_disconnect_reason": self.data.collector.last_disconnect_reason,
            "discovery_restart_count": self.data.collector.discovery_restart_count,
            "last_discovery_reason": self.data.collector.last_discovery_reason,
            "collector_pn": self.data.collector.collector_pn,
            "profile_key": self.data.collector.profile_key,
            "profile_name": self.data.collector.profile_name,
            "last_udp_reply": self.data.collector.last_udp_reply,
            "last_udp_reply_from": self.data.collector.last_udp_reply_from,
            "last_devcode": self.data.collector.last_devcode,
            "smartess_collector_version": self.data.collector.smartess_collector_version,
            "smartess_protocol_raw_id": self.data.collector.smartess_protocol_raw_id,
            "smartess_protocol_asset_id": self.data.collector.smartess_protocol_asset_id,
            "smartess_protocol_asset_name": self.data.collector.smartess_protocol_asset_name,
            "smartess_protocol_suffix": self.data.collector.smartess_protocol_suffix,
            "smartess_protocol_profile_key": self.data.collector.smartess_protocol_profile_key,
            "smartess_protocol_name": self.data.collector.smartess_protocol_name,
            "smartess_device_address": self.data.collector.smartess_device_address,
            "collector_cloud_profile_key": (
                self.data.collector.collector_cloud_profile_key
                or self.collector_cloud_profile_key
            ),
            "collector_cloud_profile_label": (
                self.data.collector.collector_cloud_profile_label
                or self.collector_cloud_profile_label
            ),
            "collector_cloud_profile_source": (
                self.data.collector.collector_cloud_profile_source
                or self.collector_cloud_profile_source
            ),
            "collector_cloud_profile_confidence": (
                self.data.collector.collector_cloud_profile_confidence
                or self.collector_cloud_profile_confidence
            ),
        }

    @staticmethod
    def _inverter_payload(inverter) -> dict[str, Any]:
        return {
            "driver_key": inverter.driver_key,
            "protocol_family": inverter.protocol_family,
            "model_name": inverter.model_name,
            "variant_key": inverter.variant_key,
            "serial_number": inverter.serial_number,
            "profile_name": inverter.profile_name,
            "register_schema_name": inverter.register_schema_name,
            "probe_target": {
                "devcode": inverter.probe_target.devcode,
                "collector_addr": inverter.probe_target.collector_addr,
                "device_addr": inverter.probe_target.device_addr,
            },
            "details": dict(inverter.details),
        }

    def _build_support_fixture(
        self,
        raw_capture: dict[str, Any],
    ) -> dict[str, Any] | None:
        inverter = self.data.inverter
        ranges = list(raw_capture.get("fixture_ranges") or [])
        command_responses = build_command_fixture_responses(raw_capture)
        probe_target = None
        fixture_name = ""
        if inverter is not None:
            probe_target = {
                "devcode": inverter.probe_target.devcode,
                "collector_addr": inverter.probe_target.collector_addr,
                "device_addr": inverter.probe_target.device_addr,
            }
            fixture_name = f"{inverter.driver_key}_support_capture"
        elif raw_capture.get("capture_kind") == "generic_register_dump":
            best_capture = self._best_generic_capture(raw_capture)
            if best_capture is not None:
                ranges = list(best_capture.get("fixture_ranges") or ranges)
                probe_target = dict(best_capture.get("probe_target") or {})
                fixture_name = f"{best_capture.get('driver_key', 'unknown')}_support_capture"
        if not ranges and not command_responses:
            return None

        collector_payload = self._collector_payload() or {}
        fixture: dict[str, Any] = {
            "fixture_version": 1,
            "name": fixture_name or "unknown_driver_support_capture",
            "collector": {
                "remote_ip": collector_payload.get("remote_ip"),
                "collector_pn": collector_payload.get("collector_pn"),
                "last_devcode": collector_payload.get("last_devcode"),
                "profile_key": collector_payload.get("profile_key"),
                "profile_name": collector_payload.get("profile_name"),
            },
            "probe_target": probe_target,
        }
        if ranges:
            fixture["ranges"] = ranges
        if command_responses:
            fixture["command_responses"] = command_responses
        return fixture

    @staticmethod
    def _best_generic_capture(raw_capture: dict[str, Any]) -> dict[str, Any] | None:
        captures = list(raw_capture.get("captures") or [])
        if not captures:
            return None
        return max(
            captures,
            key=lambda capture: (
                len(capture.get("fixture_ranges") or []),
                -len(capture.get("range_failures") or []),
            ),
        )


    @staticmethod
    def _metadata_source_payload(metadata) -> dict[str, Any] | None:
        if metadata is None:
            return None
        return {
            "name": getattr(metadata, "source_name", ""),
            "scope": getattr(metadata, "source_scope", ""),
            "path": getattr(metadata, "source_path", ""),
        }

    def _build_inverter_device_info(self, snapshot: RuntimeSnapshot | None = None) -> DeviceInfo:
        """Build stable metadata for the main inverter device."""

        snapshot = snapshot or self.data
        collector_identifier = (DOMAIN, f"{self.config_entry.entry_id}:collector")
        name = "EyeBond Inverter"
        model = None
        serial_number = None
        detected_model = str(self.config_entry.data.get(CONF_DETECTED_MODEL) or "").strip()
        detected_serial = str(self.config_entry.data.get(CONF_DETECTED_SERIAL) or "").strip()
        runtime_model = str(getattr(snapshot.inverter, "model_name", "") or "").strip()
        runtime_serial = str(getattr(snapshot.inverter, "serial_number", "") or "").strip()

        if runtime_model or runtime_serial:
            name = runtime_model or detected_model or name
            model = runtime_model or detected_model or None
            serial_number = runtime_serial or detected_serial or None
        else:
            if detected_model:
                name = detected_model
                model = detected_model
            elif self.config_entry.title:
                name = self.config_entry.title
            if detected_serial:
                serial_number = detected_serial

        info: dict[str, object] = {
            "identifiers": {(DOMAIN, self.config_entry.entry_id)},
            "name": name,
            "manufacturer": "OEM / EyeBond",
            "via_device": collector_identifier,
        }
        if model:
            info["model"] = model
        if serial_number:
            info["serial_number"] = serial_number
        return DeviceInfo(**info)

    def _build_collector_device_info(self, snapshot: RuntimeSnapshot | None = None) -> DeviceInfo:
        """Build stable metadata for the collector-side device."""

        snapshot = snapshot or self.data
        collector = snapshot.collector
        values = snapshot.values or {}
        model = "EyeBond Collector"
        serial_number = self._preferred_collector_pn(snapshot)
        collector_ip = str(self.config_entry.data.get(CONF_COLLECTOR_IP, "") or "").strip()
        sw_version = ""
        hw_version = str(values.get("collector_hardware_version") or "").strip()
        collector_type = str(values.get("collector_type") or "").strip()

        manufacturer = "OEM / EyeBond"
        configuration_url = ""
        is_virtual_bridge = bool(getattr(collector, "collector_virtual_bridge", False))

        if collector is not None:
            if collector_type:
                model = collector_type
            elif collector.profile_name:
                model = collector.profile_name
            elif collector.smartess_protocol_name:
                model = collector.smartess_protocol_name
            elif collector.smartess_protocol_asset_name:
                model = collector.smartess_protocol_asset_name
            if collector.smartess_collector_version:
                sw_version = collector.smartess_collector_version
        elif collector_type:
            model = collector_type

        if is_virtual_bridge:
            # A detected community bridge gets an honest identity instead of the
            # generic factory "EyeBond Collector" / "Wi-Fi.DTU" model. It never
            # talks to the SmartESS cloud, so its parsed semver is authoritative.
            manufacturer = "ESP EyeBond Collector (community)"
            model = "ESP EyeBond Collector"
            bridge_version = str(getattr(collector, "collector_bridge_version", "") or "").strip()
            if bridge_version:
                sw_version = bridge_version
            configuration_url = "https://github.com/groove-max/esp-eybond-collector"

        name = collector_display_name(
            collector_pn=serial_number,
            collector_ip=collector_ip,
        )

        info: dict[str, object] = {
            "identifiers": {(DOMAIN, f"{self.config_entry.entry_id}:collector")},
            "name": name,
            "manufacturer": manufacturer,
            "model": model,
        }
        if serial_number:
            info["serial_number"] = serial_number
        if sw_version:
            info["sw_version"] = sw_version
        if hw_version:
            info["hw_version"] = hw_version
        if configuration_url:
            info["configuration_url"] = configuration_url
        return DeviceInfo(**info)

    def inverter_device_info(self) -> DeviceInfo:
        """Build stable device metadata for inverter-owned entities."""

        if not self.has_inverter_identity:
            return self.collector_device_info()
        return self._build_inverter_device_info(self.data)

    def collector_device_info(self) -> DeviceInfo:
        """Build stable device metadata for collector-owned entities."""

        return self._build_collector_device_info(self.data)

    def device_info_for_key(self, key: str) -> DeviceInfo:
        """Return the owning device metadata for one entity key."""

        if is_collector_entity_key(key):
            return self.collector_device_info()
        return self.inverter_device_info()

    def device_info(self) -> DeviceInfo:
        """Backward-compatible alias for the main inverter device metadata."""

        return self.inverter_device_info()

    def async_sync_device_registry(self, snapshot: RuntimeSnapshot | None = None) -> None:
        """Update existing HA device entries with the latest metadata."""

        self._async_sync_collector_device_registry(snapshot)
        self._async_sync_inverter_device_registry(snapshot)

    def _async_sync_inverter_device_registry(self, snapshot: RuntimeSnapshot | None = None) -> None:
        """Update the inverter HA device entry with the latest model metadata."""

        if not self.has_inverter_identity:
            registry = dr.async_get(self.hass)
            device = registry.async_get_device(identifiers={(DOMAIN, self.config_entry.entry_id)})
            remove_device = getattr(registry, "async_remove_device", None)
            if device is not None and callable(remove_device):
                try:
                    remove_device(device.id)
                except Exception:
                    logger.debug(
                        "Failed to remove stale inverter device for entry %s",
                        self.config_entry.entry_id,
                        exc_info=True,
                    )
            self._last_synced_device_meta = ("", "", "", "", "")
            return

        info = self._build_inverter_device_info(snapshot)
        identifiers = info.get("identifiers")
        if not identifiers:
            return

        registry = dr.async_get(self.hass)
        desired_name = info.get("name") or ""
        desired_model = info.get("model") or ""
        desired_serial = info.get("serial_number") or ""
        desired_manufacturer = info.get("manufacturer") or ""
        desired_via_device = info.get("via_device")
        desired_via_device_id = None
        if desired_via_device:
            collector_device = registry.async_get_device(identifiers={desired_via_device})
            if collector_device is not None:
                desired_via_device_id = collector_device.id
        meta = (
            desired_name,
            desired_model,
            desired_serial,
            desired_manufacturer,
            desired_via_device_id or "",
        )
        if meta == self._last_synced_device_meta:
            return

        registry.async_get_or_create(config_entry_id=self.config_entry.entry_id, **info)
        self._last_synced_device_meta = meta

    def _async_sync_collector_device_registry(self, snapshot: RuntimeSnapshot | None = None) -> None:
        """Update the collector HA device entry with the latest metadata."""

        info = self._build_collector_device_info(snapshot)
        identifiers = info.get("identifiers")
        if not identifiers:
            return

        registry = dr.async_get(self.hass)
        desired_name = info.get("name") or ""
        desired_model = info.get("model") or ""
        desired_serial = info.get("serial_number") or ""
        desired_manufacturer = info.get("manufacturer") or ""
        desired_sw_version = info.get("sw_version") or ""
        desired_hw_version = info.get("hw_version") or ""
        meta = (
            desired_name,
            desired_model,
            desired_serial,
            desired_manufacturer,
            desired_sw_version,
            desired_hw_version,
        )
        if meta == self._last_synced_collector_device_meta:
            return

        registry.async_get_or_create(config_entry_id=self.config_entry.entry_id, **info)
        self._last_synced_collector_device_meta = meta


def _local_source_ip_for_target(target_ip: str) -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((target_ip, 9))
            return str(sock.getsockname()[0] or "")
    except OSError:
        return ""
