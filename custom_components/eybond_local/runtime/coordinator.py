"""Home Assistant coordinator for the EyeBond Local integration."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import dataclasses
from datetime import datetime, timedelta, timezone
import ipaddress
import logging
from pathlib import Path
import socket
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
    format_collector_server_endpoint as format_runtime_collector_server_endpoint,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint as normalize_runtime_collector_server_endpoint,
    parse_collector_server_endpoint as parse_runtime_collector_server_endpoint,
    resolve_collector_server_endpoint as resolve_runtime_collector_server_endpoint,
)
from ..collector.cloud_family import (
    COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY,
    COLLECTOR_CLOUD_FAMILY_UNKNOWN,
    collector_cloud_family_observation_from_endpoint,
    default_collector_cloud_host,
)
from ..const import (
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
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
from ..metadata.profile_loader import builtin_base_profile_name
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
from ..models import CapabilityPreset, RuntimeSnapshot, WriteCapability
from ..naming import installation_title, legacy_installation_titles
from .factory import create_runtime_manager
from .manager import RuntimeManager
from ..schema import (
    build_runtime_ui_schema,
    capability_write_exposure_allowed,
    preset_write_exposure_allowed,
)
from ..support.bundle import build_support_bundle_payload, export_support_bundle
from ..support.cloud_evidence import (
    fetch_and_export_smartess_device_bundle_cloud_evidence,
    load_latest_cloud_evidence,
)
from ..support.proxy_capture import build_proxy_capture_overview
from ..support.proxy_session import (
    build_proxy_capture_command,
    build_proxy_capture_restore_trigger_path,
    build_proxy_capture_trace_path,
    inspect_proxy_capture_start_status,
    inspect_proxy_capture_trace,
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
_DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY = "device_scoped_overlay_activation"
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


def _collector_endpoint_format_flags(endpoint: str) -> tuple[bool, bool]:
    """Return whether port/protocol were explicit in the original endpoint."""

    try:
        parsed = inspect_collector_server_endpoint(
            endpoint,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return True, True
    return parsed.has_explicit_port, parsed.has_explicit_protocol


def _known_collector_cloud_family(value: object) -> str:
    """Return a concrete collector cloud family, ignoring unknown placeholders."""

    family = str(value or "").strip()
    if family in {"", COLLECTOR_CLOUD_FAMILY_UNKNOWN}:
        return ""
    return family


def _known_collector_cloud_profile_value(value: object) -> str:
    """Return one normalized non-empty collector cloud profile metadata value."""

    return str(value or "").strip()


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


def _format_home_assistant_collector_endpoint(
    *,
    server_host: str,
    template_endpoint: str = "",
    cloud_family: str = "",
) -> str:
    """Build the Home Assistant callback endpoint using the legacy default cloud port."""

    include_port, include_protocol = _collector_endpoint_format_flags(template_endpoint)
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
    return _format_collector_server_endpoint(
        server_host=server_host,
        server_port=server_port,
        server_protocol=server_protocol,
        include_port=include_port,
        include_protocol=include_protocol,
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

    include_port, include_protocol = _collector_endpoint_format_flags(template_endpoint)
    if not template_endpoint and normalized_family == COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY:
        include_port = False
        include_protocol = False

    return _format_collector_server_endpoint(
        server_host=default_host,
        server_port=default_collector_server_port(cloud_family=normalized_family),
        server_protocol=DEFAULT_COLLECTOR_SERVER_PROTOCOL,
        include_port=include_port,
        include_protocol=include_protocol,
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

    async def async_shutdown(self) -> None:
        """Stop the underlying hub."""

        async with self._shutdown_lock:
            if self._shutdown_complete:
                return
            self._cancel_proxy_capture_deadline_refresh()
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
            self._shutdown_complete = True

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

    def _configure_reverse_discovery_mode(self) -> None:
        """Control steady reverse discovery according to the collector ownership mode."""

        set_reverse_discovery_enabled = getattr(
            self._runtime,
            "set_reverse_discovery_enabled",
            None,
        )
        if set_reverse_discovery_enabled is not None:
            set_reverse_discovery_enabled(not self.collector_home_assistant_primary)

    def consume_entry_reload_suppression(self) -> bool:
        """Return whether the next config-entry update listener should skip reload."""

        if getattr(self, "_suppress_entry_reload_count", 0) <= 0:
            return False
        self._suppress_entry_reload_count -= 1
        return True

    def _async_update_entry_without_reload(self, **update_kwargs: Any) -> None:
        """Persist runtime metadata without reloading the entry we are actively running."""

        self._suppress_entry_reload_count = getattr(self, "_suppress_entry_reload_count", 0) + 1
        try:
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                **update_kwargs,
            )
        except Exception:
            self._suppress_entry_reload_count = max(self._suppress_entry_reload_count - 1, 0)
            raise

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
        if normalized_endpoint == remembered_endpoint:
            return

        self._remembered_collector_server_endpoint = normalized_endpoint
        options = dict(self.config_entry.options)
        options[CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT] = normalized_endpoint
        self._async_update_entry_without_reload(options=options)

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
            if detected_model and updated_data.get(CONF_DETECTED_MODEL) != detected_model:
                updated_data[CONF_DETECTED_MODEL] = detected_model
            if detected_serial and updated_data.get(CONF_DETECTED_SERIAL) != detected_serial:
                updated_data[CONF_DETECTED_SERIAL] = detected_serial
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
        await self._async_reconcile_network(reason="refresh")
        snapshot = await self._runtime.async_refresh(
            poll_interval=float(
                self.config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            )
        )
        snapshot = await self._async_reconcile_proxy_capture_session(snapshot)
        snapshot = await self._async_reconcile_shadow_learning_session(snapshot)
        await self._async_remember_collector_server_endpoint(snapshot)
        await self._async_remember_runtime_identity(snapshot)
        # Keep self.data aligned with the fresh snapshot before helpers that
        # inspect coordinator state instead of the local snapshot argument.
        self.data = snapshot
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
        await self._async_reconcile_collector_operation_mode_endpoint(snapshot)
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
        snapshot.values["effective_overlay_merge_status"] = self._device_overlay_merge_status
        snapshot.values["effective_inverter_capability_count"] = len(_runtime_capabilities)
        snapshot.values["effective_inverter_learned_capability_keys"] = sorted(
            str(getattr(capability, "key", ""))
            for capability in _runtime_capabilities
            if getattr(capability, "is_device_scoped_experimental", False)
        )
        snapshot.values.update(self._support_workflow_values(snapshot))
        snapshot.values.update(self._collector_onboarding_values(snapshot))
        snapshot.values.update(self._tooling_values)
        snapshot.values.update(await self._proxy_capture_values(snapshot))
        self._prune_hidden_collector_values_for_mode(snapshot)
        from .. import _async_self_heal_sensor_display_precision

        await _async_self_heal_sensor_display_precision(self.hass, self.config_entry)
        self.async_sync_device_registry(snapshot)
        return snapshot

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
            await self.async_request_refresh()
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
        return await self._runtime.async_apply_collector_changes()

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
            await self.async_request_refresh()
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
            await self._runtime.async_start_proxy_capture_route(
                owner_id=route_owner_id,
                entry_id=self.config_entry.entry_id,
                collector_ip=self._proxy_capture_collector_ip(),
                listen_port=target_port,
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                output_path=trace_path,
                masked_endpoint=self.proxy_capture_upstream_endpoint,
                restore_trigger_path=restore_trigger_path,
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
        except Exception:
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
            collector_cloud_profile_key=self.collector_cloud_profile_key,
            collector_cloud_profile_label=self.collector_cloud_profile_label,
            collector_cloud_profile_source=self.collector_cloud_profile_source,
            collector_cloud_profile_confidence=self.collector_cloud_profile_confidence,
            collector_callback_endpoint=self.collector_callback_target_endpoint,
            effective_metadata_snapshot=self.effective_metadata_snapshot,
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
        return _format_home_assistant_collector_endpoint(
            server_host=self._effective_callback_server_host,
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
        family = _known_collector_cloud_family(
            config_data.get(CONF_COLLECTOR_CLOUD_FAMILY, "")
        )
        if family:
            return family

        config_options = getattr(config_entry, "options", {}) if config_entry is not None else {}
        for endpoint in (
            self.data.values.get("collector_server_endpoint"),
            self.collector_server_endpoint_rollback_target,
            getattr(self, "_remembered_collector_server_endpoint", ""),
            config_options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT, ""),
        ):
            family = _collector_cloud_family_from_endpoint_shape(endpoint)
            if family:
                return family
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
        """Persist one integration control policy mode without reloading the entry."""

        normalized_mode = str(mode or "").strip()
        if normalized_mode not in {CONTROL_MODE_AUTO, CONTROL_MODE_READ_ONLY, CONTROL_MODE_FULL}:
            raise ValueError("control_mode_invalid")
        if normalized_mode == self.control_mode:
            return normalized_mode

        data = dict(self.config_entry.data)
        options = dict(self.config_entry.options)
        data[CONF_CONTROL_MODE] = normalized_mode
        options[CONF_CONTROL_MODE] = normalized_mode
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            data=data,
            options=options,
        )
        return normalized_mode

    @property
    def controls_enabled(self) -> bool:
        """Whether writes are globally enabled for this entry."""

        return controls_enabled(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
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
        )

    @property
    def controls_summary(self) -> str:
        """Human-readable summary of the current control policy."""

        return controls_summary(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
        )

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

        return bool(self.smartess_collector_pn)

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
        return await self._runtime.async_refresh(
            poll_interval=float(
                self.config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            )
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
        return await self._runtime.async_refresh(
            poll_interval=float(
                self.config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            )
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

        collector_pn = self.smartess_collector_pn
        if not collector_pn:
            raise RuntimeError("smartess_collector_pn_not_available")

        record = await self.hass.async_add_executor_job(
            lambda: fetch_and_export_smartess_device_bundle_cloud_evidence(
                config_dir=Path(self.hass.config.config_dir),
                username=username,
                password=password,
                collector_pn=collector_pn,
                source="smartess_cloud_diagnostics",
                entry_id=self.config_entry.entry_id,
            )
        )
        self._cached_smartess_cloud_evidence_record = record
        self._cached_smartess_cloud_evidence_warmed = True
        self._publish_tooling_values(
            cloud_evidence_path=str(record.path),
            local_metadata_status="SmartESS cloud evidence exported",
        )
        return str(record.path)

    async def async_export_support_bundle(self) -> str:
        """Export one JSON support bundle for the current entry."""

        support_bundle_payload = self._build_support_bundle_payload()
        path = await self.hass.async_add_executor_job(
            lambda: export_support_bundle(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self.config_entry.title,
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

        if wants_refresh is None:
            wants_refresh = bool(smartess_username or smartess_password)
        if wants_refresh:
            if not smartess_username or not smartess_password:
                raise RuntimeError("smartess_credentials_required")
            try:
                await self.async_export_smartess_cloud_evidence(
                    username=smartess_username,
                    password=smartess_password,
                )
            except Exception as exc:
                if self._cached_smartess_cloud_evidence_record is None:
                    raise
                logger.warning(
                    "SmartESS cloud refresh failed; building archive with last saved evidence: %s",
                    exc,
                )
                self._publish_tooling_values(
                    local_metadata_status=(
                        "SmartESS cloud refresh failed; using last saved evidence"
                    ),
                )

        support_bundle_payload = self._build_support_bundle_payload()
        driver = self.current_driver
        try:
            raw_capture = await self._runtime.async_capture_support_evidence()
        except Exception as exc:
            raw_capture = {
                "capture_kind": "unsupported_or_failed",
                "error": str(exc),
                "captured_ranges": [],
                "range_failures": [],
            }
        fixture = self._build_support_fixture(raw_capture)
        anonymized_fixture = anonymize_fixture_json(fixture) if fixture is not None else None
        profile_metadata = self.effective_profile_metadata
        register_schema_metadata = self.effective_register_schema_metadata

        export_result = await self.hass.async_add_executor_job(
            lambda: export_support_package(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self.config_entry.title,
                support_bundle=support_bundle_payload,
                raw_capture=raw_capture,
                fixture=fixture,
                anonymized_fixture=anonymized_fixture,
                profile_source=self._metadata_source_payload(profile_metadata),
                register_schema_source=self._metadata_source_payload(register_schema_metadata),
            )
        )
        path = export_result.path
        relative_download_url = str(export_result.download_url or "")
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

        self._tooling_values.update(values)
        snapshot = self.data
        snapshot.values.update(self._tooling_values)
        publish = getattr(self, "async_set_updated_data", None)
        if publish is not None:
            publish(snapshot)

    def _publish_snapshot_values(self, **values: Any) -> None:
        """Publish transient runtime values into the live coordinator snapshot only."""

        snapshot = self.data
        for key, value in values.items():
            if value is None:
                snapshot.values.pop(key, None)
            else:
                snapshot.values[key] = value
        publish = getattr(self, "async_set_updated_data", None)
        if publish is not None:
            publish(snapshot)

    def _absolute_local_download_url(self, relative_url: str) -> str:
        """Return an absolute HA URL for one `/local/...` path when possible."""

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

    def _collector_onboarding_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return compact collector-side onboarding status helpers for entity UX."""

        snapshot = snapshot or self.data
        support_label = str(snapshot.values.get("support_workflow_level_label") or "").strip()
        return {
            "collector_onboarding_status": support_label or "Unknown",
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

    def _build_support_bundle_payload(self) -> dict[str, Any]:
        inverter = self.data.inverter
        metadata = self.effective_metadata
        smartess_protocol = metadata.smartess_protocol
        values = dict(self.data.values)
        cloud_evidence_record = load_latest_cloud_evidence(
            Path(self.hass.config.config_dir),
            entry_id=self.config_entry.entry_id,
            collector_pn=(
                getattr(self.data.collector, "collector_pn", "")
                or str(self.config_entry.data.get(CONF_COLLECTOR_PN, "") or "")
            ),
        )
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
            entry_title=self.config_entry.title,
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
        sw_version = str(self.config_entry.data.get(CONF_SMARTESS_COLLECTOR_VERSION, "") or "").strip()
        hw_version = str(values.get("collector_hardware_version") or "").strip()
        collector_type = str(values.get("collector_type") or "").strip()

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

        name = collector_display_name(
            collector_pn=serial_number,
            collector_ip=collector_ip,
        )

        info: dict[str, object] = {
            "identifiers": {(DOMAIN, f"{self.config_entry.entry_id}:collector")},
            "name": name,
            "manufacturer": "OEM / EyeBond",
            "model": model,
        }
        if serial_number:
            info["serial_number"] = serial_number
        if sw_version:
            info["sw_version"] = sw_version
        if hw_version:
            info["hw_version"] = hw_version
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
