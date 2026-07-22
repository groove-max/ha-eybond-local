"""Hub that orchestrates runtime links, payload drivers, and polling."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from time import monotonic, time as _wall_time
from typing import Any, Callable

from ..canonical_telemetry import (
    apply_canonical_measurements,
    canonical_measurements_for_driver,
)
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DRIVER_HINT_AUTO,
)
from ..connection.models import EybondConnectionSpec
from ..connection.session_handle import ADAPTER_COLLECTOR_AT_COMMANDS
from ..collector.capabilities import (
    collector_capability_profile_from_runtime,
    parse_esp_collector_hardware_token,
)
from ..collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint as normalize_runtime_collector_server_endpoint,
)
from ..collector.management import (
    CollectorEndpointWriteResult,
    CollectorManagementCapabilities,
    CollectorManagementError,
    CollectorManagementUnsupportedError,
    CollectorSystemActionResult,
    select_collector_management_adapter,
)
from ..drivers.base import InverterDriver
from ..drivers.read_result import (
    DriverReadMode,
    DriverReadResult,
    coerce_driver_read_result,
)
from ..drivers.command_support import (
    apply_unsupported_diagnostics,
    clear_unsupported_commands,
    commit_cycle_failures,
    seed_unsupported_commands,
)
from ..drivers.registry import iter_drivers
from ..onboarding.driver_detection import async_detect_inverter
from ..link_models import EybondLinkRoute
from ..link_transport import async_send_payload, select_payload_route
from ..models import CapabilityBlocker, DetectedInverter, RuntimeSnapshot, WriteCapability
from ..payload.modbus import ModbusSession, to_signed_16
from ..runtime_labels import runtime_path_label
from .collector_metadata import (
    CollectorMetadataRefreshResult,
    CollectorMetadataService,
)
from .link import EybondRuntimeLinkManager, resolve_server_ip

logger = logging.getLogger(__name__)

# A same-identity TCP replacement is allowed one bounded recovery attempt using
# the existing runtime reconnect budget. This is not a polling/NAT timer: the
# longer wait is enabled only by an observed owned-session generation change.
_SESSION_HANDOVER_CONNECT_TIMEOUT = 5.0
_SESSION_HANDOVER_MAX_GENERATIONS = 3


def _prefer_more_complete_collector_pn(current: object, candidate: object) -> str:
    normalized_current = str(current or "").strip()
    normalized_candidate = str(candidate or "").strip()
    if not normalized_candidate:
        return normalized_current
    if not normalized_current:
        return normalized_candidate
    if normalized_candidate == normalized_current:
        return normalized_candidate
    if normalized_candidate.startswith(normalized_current):
        return normalized_candidate
    if normalized_current.startswith(normalized_candidate):
        return normalized_current
    return normalized_candidate


def _reconcile_durable_collector_pn(
    durable: object,
    observed: object,
) -> tuple[str, bool]:
    """Reconcile the durable (entry/registry) PN against a live observed PN.

    Phase 2 PN stability: the durable full PN is authoritative. A shorter live
    heartbeat PN (a prefix of the durable one) must never downgrade it; a longer
    same-identity PN enriches it; a genuinely different full PN is an identity
    conflict -- keep the durable PN and report the conflict rather than silently
    switching identity. Returns ``(pn, conflict)``.
    """

    durable_pn = str(durable or "").strip()
    observed_pn = str(observed or "").strip()
    if not durable_pn:
        return observed_pn, False
    if not observed_pn or observed_pn == durable_pn:
        return durable_pn, False
    if observed_pn.startswith(durable_pn):
        # Live session revealed the fuller same-identity PN -> enrich.
        return observed_pn, False
    if durable_pn.startswith(observed_pn):
        # Live heartbeat carried only a prefix -> keep the durable full PN.
        return durable_pn, False
    # Different identities entirely: keep the durable PN, flag the conflict.
    return durable_pn, True


def _split_collector_endpoint(endpoint: object) -> tuple[str, int | None, str]:
    raw = str(endpoint or "").strip()
    if not raw:
        return "", None, ""
    try:
        parsed = inspect_collector_server_endpoint(
            raw,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return raw, None, ""
    return parsed.host, parsed.port, parsed.protocol


_DEFAULT_PROXY_CAPTURE_PORT = DEFAULT_COLLECTOR_SERVER_PORT
RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE = "collector_offline"
RUNTIME_DRIVER_STATE_DRIVER_UNBOUND = "driver_unbound"

# Bounded per-command timeout for the at_text support-archive ASCII probe;
# generous enough for a slow 2400-baud line response, small enough that the
# driver-provided probe commands stay under half a minute even in total silence.
_AT_TEXT_ASCII_PROBE_TIMEOUT = 3.0
RUNTIME_DRIVER_STATE_DRIVER_BOUND = "driver_bound"

# --- Explicit runtime state-machine tracks -----------------------------------
# These are diagnostic/auditable projections of the hub's otherwise-implicit
# runtime state. They are derived in _build_snapshot and never drive transport
# or ownership decisions. The five tracks are kept INDEPENDENT so a collector-
# only observation can never erase a confirmed inverter identity: the inverter
# track has its own lifecycle (absent -> detecting -> provisional/live_confirmed,
# or conflict) that is not coupled to the session/collector tracks.
RUNTIME_SESSION_STATE_OFFLINE = "offline"
RUNTIME_SESSION_STATE_ONLINE = "online"

RUNTIME_COLLECTOR_STATE_UNKNOWN = "unknown"
RUNTIME_COLLECTOR_STATE_IDENTIFIED = "identified"

RUNTIME_INVERTER_STATE_ABSENT = "absent"
RUNTIME_INVERTER_STATE_DETECTING = "detecting"
RUNTIME_INVERTER_STATE_PROVISIONAL = "provisional"
RUNTIME_INVERTER_STATE_LIVE_CONFIRMED = "live_confirmed"
RUNTIME_INVERTER_STATE_CONFLICT = "conflict"

RUNTIME_POLL_STATE_OFFLINE = "offline"
RUNTIME_POLL_STATE_DETECTING = "detecting"
RUNTIME_POLL_STATE_POLLING = "polling"
RUNTIME_POLL_STATE_DEGRADED = "degraded"

# A provisional (startup-persisted) binding refreshes itself against live
# detection. Bound the number of refresh attempts so a permanently-silent
# inverter cannot re-run detection on every single poll.
_INVERTER_BINDING_REFRESH_MAX_ATTEMPTS = 3
_PROVISIONAL_INVERTER_DETECTION_STATUSES = frozenset(
    {"startup_persisted_identity", "persisted_model_probe_degraded"}
)

# Keep a small ring of recent composite state transitions for the support
# package. Bounded on purpose: no unbounded growth, no per-poll logging.
_RUNTIME_STATE_TRANSITION_HISTORY_MAX = 20


def _inverter_identity_signature(inverter: object | None) -> str:
    """Return a stable identity signature (driver|model|serial) for comparison."""

    if inverter is None:
        return ""
    driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
    model = str(getattr(inverter, "model_name", "") or "").strip()
    serial = str(getattr(inverter, "serial_number", "") or "").strip()
    return "|".join((driver_key, model, serial))


def _inverter_identity_is_present(inverter: object | None) -> bool:
    """Return whether an inverter object carries a usable model/serial identity."""

    if inverter is None:
        return False
    return bool(
        str(getattr(inverter, "model_name", "") or "").strip()
        or str(getattr(inverter, "serial_number", "") or "").strip()
    )


def _inverter_identities_conflict(current: object, candidate: object) -> bool:
    """Return whether two present identities denote different physical inverters.

    Serial number is the strongest signal: two non-empty different serials are a
    conflict. When a serial is unavailable, a different driver_key or model is a
    conflict. Same identity, or a refinement of a missing field, is NOT a conflict.
    """

    cur_serial = str(getattr(current, "serial_number", "") or "").strip()
    cand_serial = str(getattr(candidate, "serial_number", "") or "").strip()
    if cur_serial and cand_serial:
        return cur_serial != cand_serial
    cur_driver = str(getattr(current, "driver_key", "") or "").strip()
    cand_driver = str(getattr(candidate, "driver_key", "") or "").strip()
    if cur_driver and cand_driver and cur_driver != cand_driver:
        return True
    cur_model = str(getattr(current, "model_name", "") or "").strip()
    cand_model = str(getattr(candidate, "model_name", "") or "").strip()
    if cur_model and cand_model and cur_model != cand_model:
        return True
    return False


_VOLATILE_COLLECTOR_VALUE_KEYS: frozenset[str] = frozenset(
    {
        "smartess_collector_version",
        "smartess_protocol_raw_id",
        "smartess_protocol_asset_id",
        "smartess_protocol_asset_name",
        "smartess_protocol_suffix",
        "smartess_protocol_profile_key",
        "smartess_protocol_name",
        "smartess_device_address",
        "collector_protocol_version",
        "collector_type",
        "collector_hardware_version",
        "collector_local_ip_address",
        "collector_server_endpoint",
        "collector_callback_owner",
        "collector_reboot_required",
        "collector_transmission_mode",
        "collector_serial_baudrate",
        "collector_network_diagnostics",
        "collector_signal_strength",
        "collector_signal_strength_raw",
        "collector_signal_strength_source",
        "collector_signal_quality",
        "collector_upload_mode",
        "collector_system_time",
        "collector_link_status",
        "collector_cloud_heartbeat_value",
        "collector_ssid",
        "collector_wifi_scan_list",
        "collector_virtual_bridge",
        "collector_bridge_kind",
        "collector_bridge_version",
        "collector_udp_reply",
        "collector_udp_reply_from",
    }
)


def _is_home_assistant_callback_endpoint(
    endpoint: object,
    *,
    server_ip: str,
    advertised_server_ip: str,
    advertised_tcp_port: int,
) -> bool:
    host, port, protocol = _split_collector_endpoint(endpoint)
    normalized_host = host.lower()
    allowed_hosts = {
        str(server_ip or "").strip().lower(),
        str(advertised_server_ip or "").strip().lower(),
    }
    allowed_hosts.discard("")
    return (
        bool(normalized_host)
        and normalized_host in allowed_hosts
        and port in {int(advertised_tcp_port or 0), _DEFAULT_PROXY_CAPTURE_PORT}
        and protocol.upper() == "TCP"
    )


def _callback_owner_label(
    endpoint: object,
    *,
    server_ip: str,
    advertised_server_ip: str,
    advertised_tcp_port: int,
) -> str:
    host, _port, _protocol = _split_collector_endpoint(endpoint)
    normalized_host = host.lower()
    if _is_home_assistant_callback_endpoint(
        endpoint,
        server_ip=server_ip,
        advertised_server_ip=advertised_server_ip,
        advertised_tcp_port=advertised_tcp_port,
    ):
        return "Home Assistant"
    if "eybond" in normalized_host or "smartess" in normalized_host:
        return "SmartESS cloud"
    if normalized_host:
        return "Custom endpoint"
    return "Unknown"


def _collector_signal_quality(signal_strength: object) -> str:
    try:
        value = int(signal_strength)
    except (TypeError, ValueError):
        return "unknown"
    if value >= -70:
        return "excellent"
    if value >= -85:
        return "good"
    if value >= -100:
        return "fair"
    return "weak"


def _collector_signal_source_label(source: object) -> str:
    normalized = str(source or "").strip().lower()
    if normalized == "wifi_rssi":
        return "Wi-Fi RSSI"
    if normalized == "gprs_csq":
        return "GPRS CSQ"
    return "Unknown"


def _error_code(exc: BaseException) -> str:
    return str(exc)


def _is_retryable_collector_error(exc: BaseException) -> bool:
    """Return whether one transport error is worth retrying after reconnect."""

    return isinstance(exc, ConnectionError) and _error_code(exc) in {
        "collector_disconnected",
        "collector_not_connected",
        "collector_heartbeat_timeout",
        "collector_write_timeout",
    }


def _should_mark_snapshot_disconnected(exc: BaseException) -> bool:
    """Return whether one refresh error should make live sensors unavailable."""

    return _error_code(exc) in {
        "collector_disconnected",
        "collector_not_connected",
        "collector_heartbeat_timeout",
        "collector_write_timeout",
    }


def _should_force_reconnect(exc: BaseException) -> bool:
    """Return whether one refresh error warrants a forced collector reconnect."""

    return _error_code(exc) in {
        "collector_write_timeout",
    }


def _normalize_collector_server_endpoint(endpoint: str) -> str:
    return normalize_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        preserve_shape=True,
    )




def _should_confirm_write(capability: WriteCapability) -> bool:
    """Return whether a write should be verified by immediate readback."""

    return capability.value_kind != "action"


def _write_readback_matches(
    capability: WriteCapability,
    *,
    requested_value: object,
    written_value: object,
    readback_value: object,
) -> bool:
    """Return whether one refreshed value confirms the requested write."""

    if readback_value == written_value or readback_value == requested_value:
        return True

    if capability.enum_value_map and isinstance(requested_value, int):
        expected_label = capability.enum_value_map.get(requested_value)
        if expected_label is not None and readback_value == expected_label:
            return True

    return False


def _write_not_confirmed_error(
    capability: WriteCapability,
    *,
    written_value: object,
    readback_value: object,
    refresh_error: str,
) -> RuntimeError:
    """Return one explicit error for a write that did not confirm by readback."""

    readback_text = "unavailable" if readback_value is None else repr(readback_value)
    message = (
        f"Command accepted, but {capability.display_name!r} did not confirm by readback. "
        f"Expected {written_value!r}, got {readback_text}."
    )
    if refresh_error:
        message = f"{message} Refresh reported {refresh_error}."
    return RuntimeError(f"write_not_confirmed:{capability.key}:{message}")


class EybondHub:
    """Coordinates runtime link connectivity, driver probing and polling."""

    @property
    def detected_inverter(self) -> DetectedInverter | None:
        """Return the currently bound detected inverter, if any.

        Exposed so the coordinator can hand the detected model to a driver's
        ``poll_policy_for`` once identity is known (a catalog driver may pick a
        model-specific policy). ``None`` before detection.
        """

        return self._inverter

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        """Return the rollback endpoint remembered during the active runtime session."""

        if self._collector_last_server_endpoint_before_change:
            return self._collector_last_server_endpoint_before_change
        return ""

    @property
    def effective_server_ip(self) -> str:
        """Return the collector-facing local host selected by the link manager."""

        return self._link_manager.effective_server_ip

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the callback host advertised to the collector."""

        return self._link_manager.effective_advertised_server_ip

    @property
    def listener_bind_host(self) -> str:
        """Return the ACTUAL local TCP bind host of the callback listener.

        A narrow read-only pass-through of the link's own public
        ``listener_bind_host`` -- so a cold repair can borrow the shared TCP
        listener on the exact host the runtime binds, never a guessed one.
        """

        return self._link_manager.listener_bind_host

    def diagnostic_link_transport(self):
        """Return the shared payload transport for read-only diagnostic command runs.

        Exposes the active collector link so the diagnostic command runner can
        reuse the existing connection instead of opening its own socket. Returns
        ``None`` when no link manager/transport is available.
        """

        link_manager = getattr(self, "_link_manager", None)
        if link_manager is None:
            return None
        return getattr(link_manager, "transport", None)

    def __init__(
        self,
        *,
        connection: EybondConnectionSpec,
        driver_hint: str = DRIVER_HINT_AUTO,
        connection_mode: str = "",
    ) -> None:
        self._driver_hint = driver_hint
        self._connection = connection
        self._connection_mode = connection_mode
        self._link_manager = EybondRuntimeLinkManager(
            server_ip=connection.server_ip,
            advertised_server_ip=connection.advertised_server_ip,
            collector_ip=connection.collector_ip,
            collector_pn=connection.collector_pn,
            # EXPECTED (inferred cloud-family) hint only -- read the explicit
            # expected field, never the legacy read-only alias, and never as a
            # confirmed owner.
            collector_expected_session_protocol=(
                connection.collector_expected_session_protocol
            ),
            collector_identity_strategy=connection.collector_identity_strategy,
            collector_raw_passthrough_bootstrap=connection.collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=(
                connection.collector_raw_passthrough_frame_format
            ),
            collector_raw_passthrough_min_interval_ms=(
                connection.collector_raw_passthrough_min_interval_ms
            ),
            confirmed_session_protocol_evidence=getattr(
                connection, "confirmed_session_protocol_evidence", None
            ),
            tcp_port=connection.tcp_port,
            advertised_tcp_port=connection.advertised_tcp_port,
            udp_port=connection.udp_port,
            discovery_target=connection.discovery_target,
            discovery_interval=connection.discovery_interval,
            heartbeat_interval=connection.heartbeat_interval,
        )
        self._driver: InverterDriver | None = None
        self._inverter: DetectedInverter | None = None
        self._inverter_binding_needs_live_detection_refresh = False
        self._inverter_binding_refresh_attempts = 0
        self._inverter_identity_conflict = ""
        self._last_driver_bound_identity = ""
        self._state_transition_history: deque[str] = deque(
            maxlen=_RUNTIME_STATE_TRANSITION_HISTORY_MAX
        )
        self._last_composite_state: tuple[str, ...] = ()
        self._inverter_overlay_applier: (
            Callable[[DetectedInverter, Any], DetectedInverter] | None
        ) = None
        self._inverter_detection_observer: (
            Callable[[InverterDriver, DetectedInverter], None] | None
        ) = None
        self._snapshot_observer: Callable[[RuntimeSnapshot], None] | None = None
        self._last_snapshot = RuntimeSnapshot()
        self._runtime_read_state: dict[str, Any] = {}
        # Last-good runtime MEASUREMENT values, kept strictly separate from
        # ``DetectedInverter.details`` (detection/bootstrap), collector metadata,
        # identity/config fields, and diagnostics. A driver DELTA overlays here;
        # a FULL replaces it. ``_runtime_measurement_owned_keys`` is every key the
        # runtime has ever measured for the current inverter identity, so detection
        # ``details`` can never re-seed a key the runtime already owns. The cache is
        # bound to ``_runtime_measurement_identity`` (driver|model|serial) and is
        # invalidated only when that durable identity actually changes -- a plain
        # reconnect of the same PN/driver keeps the last-good values.
        self._runtime_measurement_values: dict[str, Any] = {}
        self._runtime_measurement_owned_keys: set[str] = set()
        # Keys the PREVIOUS identity owned. When the binding changes they are
        # remembered here so the next snapshot also purges them from the carried
        # ``_last_snapshot`` (not just from the cache), then they are re-provided
        # by the new identity's detection details / cache if still relevant.
        self._stale_runtime_owned_keys: set[str] = set()
        self._runtime_measurement_identity: str = ""
        self._runtime_measurement_last_mode: str = ""
        self._runtime_measurement_fresh_count: int = 0
        self._runtime_measurement_reused_count: int = 0
        self._persistent_unsupported_commands: tuple[str, ...] = ()
        # Collector-metadata TELEMETRY ownership lives entirely in the service:
        # the generic hub owns neither the wire, the cadence, the caches, nor the
        # dead-channel verdict. It reads the negotiated metadata routes from the
        # link (route authority) and consumes one normalized result. The service
        # keeps its OWN channel health -- separate from the driver's
        # unsupported-command negative cache.
        self._collector_metadata_service = CollectorMetadataService(
            generation_provider=self._owned_session_generation,
        )
        self._collector_runtime_read_fresh = False
        self._collector_outage_caches_cleared = False
        self._last_collector_metadata_result: CollectorMetadataRefreshResult | None = None
        self._collector_last_server_endpoint_before_change = ""
        # Last collector-management operation record (non-sensitive) for support
        # diagnostics; populated by ``_run_management_operation``.
        self._last_management_operation: dict[str, object] | None = None
        self._write_blockers: dict[str, CapabilityBlocker] = {}
        self._last_operating_mode: object | None = None
        self._last_success_monotonic: float | None = None
        self._recovery_backoff_until_monotonic = 0.0
        self._recovery_streak = 0
        self._reconnect_count = 0
        self._last_recovery_reason = ""
        self._stable_owned_session_generation = 0

    async def async_start(self) -> None:
        """Start the underlying runtime link and discovery loop."""

        await self._link_manager.async_start()

    async def async_stop(self) -> None:
        """Stop discovery and the active runtime link."""

        self._snapshot_observer = None
        self._inverter_overlay_applier = None
        self._inverter_detection_observer = None
        self.set_collector_connection_watcher(None)
        await self._link_manager.async_stop()

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Re-resolve listener network state after HA/network readiness changes."""

        return await self._link_manager.async_reconcile_network(reason=reason)

    async def async_reconcile_collector_session_profile(
        self,
        *,
        collector_session_protocol: str,
        collector_identity_strategy: str,
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
        reason: str = "collector_session_profile_change",
    ) -> bool:
        """Rebuild link transports after a runtime-learned collector profile change."""

        return await self._link_manager.async_reconcile_collector_session_profile(
            collector_session_protocol=collector_session_protocol,
            collector_identity_strategy=collector_identity_strategy,
            collector_raw_passthrough_bootstrap=collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=collector_raw_passthrough_frame_format,
            collector_raw_passthrough_min_interval_ms=(
                collector_raw_passthrough_min_interval_ms
            ),
            reason=reason,
        )

    def listener_diagnostics(self) -> dict[str, object]:
        """Return active collector listener/session diagnostics."""

        diagnostics = getattr(self._link_manager, "listener_diagnostics", None)
        if callable(diagnostics):
            return dict(diagnostics())
        return {}

    def has_confirmed_wire_binding(self) -> bool:
        """Return whether the link has ever confirmed a live wire binding.

        Delegated so the coordinator's per-poll session-profile reconcile can be
        bootstrap-only: once a live wire is confirmed, the live session is the
        transport authority and the cloud-family/persisted profile must not drive
        a steady-state destructive rebuild.
        """

        probe = getattr(self._link_manager, "has_confirmed_wire_binding", None)
        if callable(probe):
            try:
                return bool(probe())
            except Exception:  # pragma: no cover - defensive
                return False
        return False

    def confirmed_session_protocol_evidence(self) -> tuple[str, str]:
        """Return ``(protocol, durable_pn)`` of the confirmed live wire, else ("","").

        Sourced ONLY from a trusted live SessionHandle (the link's confirmed wire
        binding). The coordinator persists this as durable ``live_session``
        provenance so a same-PN restart can bootstrap it. Never an inferred hint.
        """

        binding = getattr(self._link_manager, "confirmed_wire_binding", None)
        if binding is None:
            return "", ""
        protocol = str(getattr(binding, "session_protocol", "") or "").strip().lower()
        pn = str(getattr(binding, "collector_pn", "") or "").strip()
        if not protocol or not pn:
            return "", ""
        return protocol, pn

    def _owned_session_generation(self) -> int:
        """Return the registry-owned socket generation, if the link exposes it."""

        return int(getattr(self._link_manager, "owned_session_generation", 0) or 0)

    def _session_handover_active(self) -> bool:
        """Return whether registry lifecycle evidence shows a replacement."""

        return bool(
            self.has_confirmed_wire_binding()
            and self._owned_session_generation()
            != self._stable_owned_session_generation
        )

    async def _async_try_connect_for_session_lifecycle(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        """Connect once normally, or follow a bounded same-PN handover chain.

        Some collectors replace one long-lived socket with a short-lived first
        replacement and immediately dial again. Each registry-observed session
        generation gets the normal reconnect budget, capped to a small number
        of generations. A confirmed binding without a generation change is a
        normal offline collector and receives no repeated grace windows.
        """

        handover = self._session_handover_active()
        attempts = _SESSION_HANDOVER_MAX_GENERATIONS if handover else 1
        for attempt in range(attempts):
            generation = self._owned_session_generation()
            attempt_timeout = (
                max(float(timeout), _SESSION_HANDOVER_CONNECT_TIMEOUT)
                if handover
                else float(timeout)
            )
            if await self._link_manager.async_try_connect(
                timeout=attempt_timeout,
                require_heartbeat=require_heartbeat,
            ):
                return True

            # A handover may have started while the ordinary connect attempt
            # was already in flight. Promote into lifecycle recovery only on
            # positive generation evidence, never merely because a binding
            # exists.
            handover = self._session_handover_active()
            if not handover or attempt + 1 >= _SESSION_HANDOVER_MAX_GENERATIONS:
                return False

            if self._owned_session_generation() == generation:
                wait_for_change = getattr(
                    self._link_manager,
                    "async_wait_for_owned_session_change",
                    None,
                )
                if not callable(wait_for_change):
                    return False
                try:
                    await asyncio.wait_for(
                        wait_for_change(generation),
                        timeout=_SESSION_HANDOVER_CONNECT_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    return False
        return False

    def _mark_owned_session_stable(self) -> None:
        """Record the owned session generation that completed a driver poll."""

        self._stable_owned_session_generation = self._owned_session_generation()

    def set_initial_inverter_binding(
        self,
        driver: InverterDriver,
        inverter: DetectedInverter,
    ) -> None:
        """Seed runtime polling from persisted high-confidence inverter metadata."""

        if self._driver is not None or self._inverter is not None:
            return
        self._driver = driver
        self._inverter = inverter
        self._accept_inverter_binding_identity()
        details = getattr(inverter, "details", {}) or {}
        self._inverter_binding_needs_live_detection_refresh = (
            str(details.get("runtime_detection_status") or "").strip()
            in _PROVISIONAL_INVERTER_DETECTION_STATUSES
        )
        self._reset_runtime_read_state()
        self._write_blockers.clear()

    def set_inverter_overlay_applier(
        self, applier: Callable[[DetectedInverter, Any], DetectedInverter] | None
    ) -> None:
        """Install a hook that post-processes the detected inverter.

        The coordinator uses this to merge activated device-scoped learned controls into
        the detected inverter (whose capabilities otherwise reflect only built-in
        detection), so the learned controls become entities and are writable.
        """

        self._inverter_overlay_applier = applier

    def set_inverter_detection_observer(
        self,
        observer: Callable[[InverterDriver, DetectedInverter], None] | None,
    ) -> None:
        """Install a best-effort observer for newly detected inverter identity.

        Runtime detection may succeed before the first runtime value read succeeds.
        The coordinator uses this hook to persist the confirmed identity before a
        later read timeout/reload can collapse the entry back to collector-only.
        """

        self._inverter_detection_observer = observer

    def set_runtime_snapshot_observer(
        self,
        observer: Callable[[RuntimeSnapshot], None] | None,
    ) -> None:
        """Install a best-effort observer for intermediate runtime snapshots."""

        self._snapshot_observer = observer

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        """Pass reverse-discovery policy changes through to the runtime link layer."""

        self._link_manager.set_reverse_discovery_enabled(enabled)

    def set_callback_ownership(self, registry: object, entry_id: str) -> None:
        """Pass the domain callback-session registry + entry id to the link layer.

        The link uses this as its production ownership authority for live
        session location, exact socket claim, negotiated wire, and
        claimed-by-other callback diagnostics.
        """

        set_ownership = getattr(self._link_manager, "set_callback_ownership", None)
        if callable(set_ownership):
            set_ownership(registry, entry_id)

    def set_collector_connection_watcher(self, callback: Callable[[str], None] | None) -> None:
        """Notify ``callback(remote_ip)`` when this entry's collector dials in."""

        set_watcher = getattr(self._link_manager, "set_collector_connection_watcher", None)
        if callable(set_watcher):
            set_watcher(callback)

    async def async_ensure_callback_listener(self, port: int) -> None:
        """Ensure one auxiliary callback listener is available for collector redirects."""

        await self._link_manager.async_ensure_callback_listener(port)

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        """Send one explicit UDP bootstrap redirect through the runtime link layer."""

        return await self._link_manager.async_trigger_reverse_discovery(
            port=port,
            timeout=timeout,
        )

    async def async_start_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        masked_endpoint: str = "",
        restore_trigger_path=None,
        async_open_output=None,
        async_close_output=None,
    ) -> None:
        """Start one in-process proxy capture route on the active runtime link."""

        route_kwargs = {
            "collector_ip": collector_ip,
            "collector_pn": collector_pn,
            "collector_session_protocol": collector_session_protocol,
            "listen_port": listen_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "output_path": output_path,
            "masked_endpoint": masked_endpoint,
            "restore_trigger_path": restore_trigger_path,
        }
        if async_open_output is not None:
            route_kwargs["async_open_output"] = async_open_output
        if async_close_output is not None:
            route_kwargs["async_close_output"] = async_close_output
        if owner_id:
            route_kwargs["owner_id"] = owner_id
        if entry_id:
            route_kwargs["entry_id"] = entry_id
        await self._link_manager.async_start_proxy_capture_route(
            **route_kwargs,
        )

    async def async_stop_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process proxy capture route."""

        if owner_id or force:
            await self._link_manager.async_stop_proxy_capture_route(
                owner_id=owner_id,
                force=force,
            )
        else:
            await self._link_manager.async_stop_proxy_capture_route()

    def proxy_capture_route_running(self) -> bool:
        """Return whether the runtime link currently owns one proxy route."""

        return self._link_manager.proxy_capture_route_running()

    async def async_start_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        seed,
    ) -> None:
        """Start one in-process shadow-learning route on the active runtime link."""

        route_kwargs = {
            "collector_ip": collector_ip,
            "collector_pn": collector_pn,
            "collector_session_protocol": collector_session_protocol,
            "listen_port": listen_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "output_path": output_path,
            "seed": seed,
        }
        if owner_id:
            route_kwargs["owner_id"] = owner_id
        if entry_id:
            route_kwargs["entry_id"] = entry_id
        await self._link_manager.async_start_shadow_learning_route(**route_kwargs)

    async def async_stop_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process shadow-learning route."""

        if owner_id or force:
            await self._link_manager.async_stop_shadow_learning_route(
                owner_id=owner_id,
                force=force,
            )
        else:
            await self._link_manager.async_stop_shadow_learning_route()

    def shadow_learning_route_running(self) -> bool:
        """Return whether the runtime link currently owns one shadow route."""

        return self._link_manager.shadow_learning_route_running()

    def shadow_learning_route_ready(self) -> bool:
        """Return whether the active shadow route is ready for cloud control learning."""

        return self._link_manager.shadow_learning_route_ready()

    def shadow_learning_route_status(self) -> dict[str, object]:
        """Return detailed status for the active shadow route."""

        return self._link_manager.shadow_learning_route_status()

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        """Drop active collector sockets without changing collector settings."""

        await self._link_manager.async_disconnect_collector_connections(reason=reason)

    async def async_refresh(self, *, poll_interval: float | None = None) -> RuntimeSnapshot:
        """Refresh the current runtime snapshot."""

        if not self._link_manager.connected:
            self._reset_runtime_read_state()
            ok = await self._async_try_connect_for_session_lifecycle(
                timeout=0.75,
            )
            if not ok:
                collector_values = await self._async_read_collector_runtime_values(
                    poll_interval=poll_interval,
                    force_liveness=True,
                )
                if (
                    self._driver is None
                    and self._inverter is None
                    and self._collector_runtime_read_fresh
                ):
                    self._collector_outage_caches_cleared = False
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error="inverter_heartbeat_missing",
                        connected=True,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
                self._clear_collector_value_caches_for_outage()
                self._reset_volatile_collector_link_fields()
                snapshot = self._build_snapshot(
                    extra_values=self._combined_collector_runtime_values(),
                    last_error="waiting_for_collector",
                    connected=False,
                )
                self._last_snapshot = snapshot
                return snapshot

        ok = await self._async_try_connect_for_session_lifecycle(
            timeout=1.5,
            require_heartbeat=True,
        )
        if not ok:
            self._reset_runtime_read_state()
            if self._link_manager.connected:
                if self._driver is None and self._inverter is None:
                    collector_values = await self._async_read_collector_runtime_values(
                        poll_interval=poll_interval,
                        force_liveness=True,
                    )
                    if self._collector_runtime_read_fresh:
                        self._collector_outage_caches_cleared = False
                        snapshot = self._build_snapshot(
                            extra_values=collector_values,
                            last_error="inverter_heartbeat_missing",
                            connected=True,
                        )
                        self._last_snapshot = snapshot
                        return snapshot

                logger.warning(
                    "Collector heartbeat timed out; resetting stale runtime connection"
                )
                try:
                    await self._async_recover_heartbeat_timeout(timeout=5.0)
                    ok = True
                except Exception as exc:
                    logger.warning("Collector heartbeat recovery failed: %s", exc)
                    self._record_recovery_failure(reason="collector_heartbeat_timeout")
                    self._clear_collector_value_caches_for_outage()
                    collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error="collector_heartbeat_timeout",
                        connected=False,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                self._clear_collector_value_caches_for_outage()
                collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                self._reset_volatile_collector_link_fields()
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error="waiting_for_collector",
                    connected=False,
                )
                self._last_snapshot = snapshot
                return snapshot

        if not ok:
            self._clear_collector_value_caches_for_outage()
            collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
            snapshot = self._build_snapshot(
                extra_values=collector_values,
                last_error="collector_heartbeat_timeout",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        # Sub-phase timing for the bound path: the coordinator-level breakdown
        # repeatedly pointed at "runtime_refresh" as one opaque number.
        refresh_phase_started = asyncio.get_running_loop().time()
        refresh_phases: dict[str, int] = {}

        def _mark_refresh_phase(phase: str) -> None:
            nonlocal refresh_phase_started
            now_monotonic = asyncio.get_running_loop().time()
            refresh_phases[phase] = refresh_phases.get(phase, 0) + int(
                round((now_monotonic - refresh_phase_started) * 1000.0)
            )
            refresh_phase_started = now_monotonic

        collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
        _mark_refresh_phase("collector_metadata")
        detect_error = ""
        if self._driver is None or self._inverter is None:
            self._publish_intermediate_snapshot(
                collector_values,
                status="detecting_inverter",
            )
            _mark_refresh_phase("intermediate_snapshot")

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            _mark_refresh_phase("driver_detection")
            if detect_error == "collector_session_changed":
                # The collector replaced its socket (possibly on another shared
                # listener) while the driver sweep was running.  Bind the new
                # registry-owned session immediately and restart detection once;
                # never publish the old sweep's offline/result state.
                self._reset_runtime_read_state()
                reconnected = await self._link_manager.async_try_connect(
                    timeout=1.5,
                    require_heartbeat=True,
                )
                _mark_refresh_phase("session_handover")
                if reconnected:
                    collector_values = await self._async_read_collector_runtime_values(
                        poll_interval=poll_interval,
                        force_liveness=True,
                    )
                    self._publish_intermediate_snapshot(
                        collector_values,
                        status="detecting_inverter",
                    )
                    detect_error = await self._async_detect_driver()
                    _mark_refresh_phase("driver_detection_after_handover")
                else:
                    detect_error = "waiting_for_collector"
            if self._driver is None or self._inverter is None:
                logger.warning("Driver detection failed: %s", detect_error)
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error=detect_error,
                    connected=self._link_manager.connected,
                )
                self._last_snapshot = snapshot
                return snapshot
        elif self._inverter_binding_needs_live_detection_refresh:
            # A startup-persisted (provisional) binding refreshes itself against
            # live detection. Bound the attempts so a permanently-silent inverter
            # cannot re-run detection on every poll: on success/conflict
            # _async_detect_driver clears the flag; on transient failure we stop
            # after a few tries and keep the provisional binding.
            self._inverter_binding_refresh_attempts += 1
            detect_error = await self._async_detect_driver()
            _mark_refresh_phase("driver_identity_refresh")
            if detect_error:
                if (
                    self._inverter_binding_refresh_attempts
                    >= _INVERTER_BINDING_REFRESH_MAX_ATTEMPTS
                ):
                    self._inverter_binding_needs_live_detection_refresh = False
                logger.debug(
                    "Deferred inverter identity refresh attempt %d failed: %s; keeping persisted binding",
                    self._inverter_binding_refresh_attempts,
                    detect_error,
                )

        remaining_backoff = self._recovery_backoff_remaining()
        if remaining_backoff > 0:
            logger.warning(
                "Runtime refresh backoff active after %s; skipping refresh for %.1fs",
                self._last_recovery_reason or "runtime_error",
                remaining_backoff,
            )
            snapshot = self._build_snapshot(
                extra_values=collector_values,
                last_error=self._last_recovery_reason or self._last_snapshot.last_error or "request_timeout",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        async def _async_read_driver_values() -> dict[str, object]:
            loop = asyncio.get_running_loop()
            started = loop.time()
            raw = await self._driver.async_read_values(
                self._link_manager.transport,
                self._inverter,
                runtime_state=self._runtime_read_state,
                poll_interval=poll_interval,
                now_monotonic=loop.time() if poll_interval is not None else None,
            )
            duration = max(0.0, loop.time() - started)
            # Typed contract: a bare dict means FULL; a DriverReadResult carries
            # its own FULL/DELTA mode. The measurement VALUES are folded into the
            # hub's last-good cache and applied in _build_snapshot; only driver
            # diagnostics + the poll duration flow through here.
            result = coerce_driver_read_result(
                raw, driver_key=getattr(self._inverter, "driver_key", "")
            )
            self._resolve_runtime_measurements(result)
            runtime_values: dict[str, object] = dict(result.diagnostics)
            runtime_values.update(self._runtime_measurement_diagnostics())
            runtime_values["collector_poll_duration_ms"] = int(round(duration * 1000.0))
            return runtime_values

        try:
            runtime_values = await _async_read_driver_values()
            _mark_refresh_phase("driver_read")
        except Exception as exc:
            if _error_code(exc) == "request_timeout":
                # This timeout belongs to one inverter/UART payload request.
                # The collector TCP link and heartbeat may still be perfectly
                # healthy; tearing that link down turns one missed inverter
                # reply into a full callback/re-detection outage. Retry once on
                # the SAME transport, then keep last-known-good values if the
                # inverter remains silent for this cycle.
                logger.warning(
                    "Inverter payload request timed out; retrying without collector reconnect"
                )
                try:
                    runtime_values = await _async_read_driver_values()
                    _mark_refresh_phase("driver_read_retry_same_session")
                except Exception as retry_exc:
                    if _is_retryable_collector_error(retry_exc):
                        # The retry produced positive transport-failure
                        # evidence. Only now may connection recovery run.
                        logger.warning(
                            "Collector transport failed during payload retry: %s; reconnecting",
                            retry_exc,
                        )
                        try:
                            self._record_recovery_attempt(reason=_error_code(retry_exc))
                            await self._async_ensure_connected(
                                timeout=5.0,
                                require_heartbeat=True,
                            )
                            self._reset_runtime_read_state()
                            runtime_values = await _async_read_driver_values()
                        except Exception as reconnect_exc:
                            logger.warning(
                                "Runtime refresh failed after collector reconnect: %s",
                                reconnect_exc,
                            )
                            self._reset_runtime_read_state()
                            self._record_recovery_failure(
                                reason=_error_code(reconnect_exc)
                            )
                            snapshot = self._build_snapshot(
                                extra_values=collector_values,
                                last_error=str(reconnect_exc),
                                connected=(
                                    False
                                    if _should_mark_snapshot_disconnected(
                                        reconnect_exc
                                    )
                                    else None
                                ),
                            )
                            self._last_snapshot = snapshot
                            return snapshot
                        # Recovery succeeded; continue the normal snapshot path.
                    else:
                        logger.warning(
                            "Inverter payload retry failed without collector link failure: %s",
                            retry_exc,
                        )
                        retained_values = dict(
                            getattr(self._last_snapshot, "values", {}) or {}
                        )
                        retained_values.update(collector_values)
                        retained_values["runtime_payload_error"] = _error_code(
                            retry_exc
                        )
                        snapshot = self._build_snapshot(
                            extra_values=retained_values,
                            last_error=_error_code(retry_exc),
                            connected=self._link_manager.connected,
                        )
                        self._last_snapshot = snapshot
                        return snapshot
            elif _is_retryable_collector_error(exc):
                logger.warning("Runtime refresh failed: %s; retrying after collector reconnect", exc)
                try:
                    self._record_recovery_attempt(reason=_error_code(exc))
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    self._reset_runtime_read_state()
                    runtime_values = await _async_read_driver_values()
                except Exception as retry_exc:
                    logger.warning("Runtime refresh failed after retry: %s", retry_exc)
                    self._reset_runtime_read_state()
                    self._record_recovery_failure(reason=_error_code(retry_exc))
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            elif _should_force_reconnect(exc):
                logger.warning(
                    "Runtime refresh failed: %s; forcing collector reconnect and retry",
                    exc,
                )
                try:
                    self._record_recovery_attempt(reason=_error_code(exc))
                    await self._link_manager.async_reset_connection(reason=str(exc))
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    self._reset_runtime_read_state()
                    runtime_values = await _async_read_driver_values()
                except Exception as retry_exc:
                    logger.warning("Runtime refresh failed after forced reconnect: %s", retry_exc)
                    self._reset_runtime_read_state()
                    self._record_recovery_failure(reason=_error_code(retry_exc))
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                logger.warning("Runtime refresh failed: %s", exc)
                self._reset_runtime_read_state()
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error=str(exc),
                    connected=False if _should_mark_snapshot_disconnected(exc) else None,
                )
                self._last_snapshot = snapshot
                return snapshot

        self._record_refresh_success()
        commit_cycle_failures(self._runtime_read_state)
        merged_values = {**collector_values, **runtime_values}
        apply_unsupported_diagnostics(merged_values, self._runtime_read_state)
        snapshot = self._build_snapshot(
            extra_values=merged_values,
            last_error=detect_error or None,
        )
        self._mark_owned_session_stable()
        _mark_refresh_phase("snapshot_build")
        metadata_result = self._last_collector_metadata_result
        if metadata_result is not None:
            refresh_phases["collector_metadata_fc"] = metadata_result.framed_duration_ms
            refresh_phases["collector_metadata_at"] = metadata_result.at_duration_ms
        snapshot.values["runtime_refresh_phase_breakdown"] = ", ".join(
            f"{phase}={elapsed_ms}ms"
            for phase, elapsed_ms in sorted(
                refresh_phases.items(), key=lambda item: -item[1]
            )
        )
        self._last_snapshot = snapshot
        return snapshot

    async def async_activate_claimed_session(
        self,
        *,
        expected_session_id: str,
        timeout: float,
    ) -> bool:
        """Activate an already-certified callback socket without sending UDP.

        This is the post-setup half of a recovery handoff.  The recovery
        transaction has already proved and pinned the exact physical session;
        activation may only consume that claim, never start a fresh callback
        attempt or substitute another same-PN socket.
        """

        return await self._link_manager.async_activate_claimed_session(
            expected_session_id=expected_session_id,
            timeout=timeout,
        )

    async def _async_read_collector_runtime_values(
        self,
        *,
        poll_interval: float | None,
        force_liveness: bool = False,
    ) -> dict[str, object]:
        """Refresh collector-side metadata via the metadata service (thin delegate).

        The hub knows nothing about FC parameter numbers, AT command names,
        transport methods, channel selection, bootstrap encoding, or channel
        cadence/cache/dead-channel internals: it reads the negotiated metadata
        routes from the link (route authority) and hands them to the service,
        then consumes one normalized result. Sets ``_collector_runtime_read_fresh``
        when at least one channel returned live data this call.
        """

        routes = self._link_manager.collector_metadata_routes()
        result = await self._collector_metadata_service.async_refresh(
            routes,
            poll_interval=poll_interval,
            force_liveness=force_liveness,
        )
        self._last_collector_metadata_result = result
        self._collector_runtime_read_fresh = result.fresh
        return result.merged_values

    def _clear_collector_runtime_value_caches(self) -> None:
        self._collector_metadata_service.invalidate()

    def _reset_runtime_read_state(self) -> None:
        """Clear per-session read state, re-seeding the persisted facts.

        The unsupported-command set is an empirical device fact persisted in
        the config entry; a reconnect must not forget it and start burning
        timeouts on known-dead commands again.
        """

        self._runtime_read_state.clear()
        if self._persistent_unsupported_commands:
            seed_unsupported_commands(
                self._runtime_read_state,
                self._persistent_unsupported_commands,
            )

    def _reset_runtime_measurement_cache(self) -> None:
        """Drop last-good runtime measurements (a different device/driver)."""

        self._runtime_measurement_values = {}
        self._runtime_measurement_owned_keys = set()
        self._runtime_measurement_last_mode = ""
        self._runtime_measurement_fresh_count = 0
        self._runtime_measurement_reused_count = 0

    def _accept_inverter_binding_identity(self) -> None:
        """The single binding/cache lifecycle boundary.

        Invoked the MOMENT a new driver/inverter binding is accepted -- never
        lazily on the first successful read -- so a bind whose first read fails
        (or a snapshot built before any read) can never surface the previous
        device's measurements. If the durable identity (``driver|model|serial``)
        actually changed, the previous identity's owned keys are marked stale so
        the next snapshot also purges them from the carried ``_last_snapshot``,
        and the measurement cache is cleared. A reconnect or learned-overlay
        refresh of the SAME identity is a no-op, so last-good values survive.
        """

        token = _inverter_identity_signature(self._inverter)
        if token == self._runtime_measurement_identity:
            return
        self._stale_runtime_owned_keys |= self._runtime_measurement_owned_keys
        self._reset_runtime_measurement_cache()
        self._runtime_measurement_identity = token

    def _resolve_runtime_measurements(
        self, result: "DriverReadResult"
    ) -> dict[str, Any]:
        """Fold one typed driver read result into the last-good measurement cache.

        FULL replaces the cache (missing keys are dropped); DELTA overlays the
        given values and removes ``removed_keys`` while retaining every other
        last-good value. ``details`` is never consulted here. Identity lifecycle
        is owned by :meth:`_accept_inverter_binding_identity` (the bind boundary),
        never re-derived here.
        """

        fresh_keys = set(result.values)
        if result.mode is DriverReadMode.FULL:
            self._runtime_measurement_values = dict(result.values)
            reused = 0
        else:
            self._runtime_measurement_values.update(result.values)
            for key in result.removed_keys:
                self._runtime_measurement_values.pop(key, None)
            reused = len(set(self._runtime_measurement_values) - fresh_keys)
        self._runtime_measurement_owned_keys.update(fresh_keys)
        self._runtime_measurement_owned_keys.update(result.removed_keys)
        self._runtime_measurement_last_mode = result.mode.value
        self._runtime_measurement_fresh_count = len(fresh_keys)
        self._runtime_measurement_reused_count = reused
        return dict(self._runtime_measurement_values)

    def _runtime_measurement_diagnostics(self) -> dict[str, object]:
        """Return neutral fresh/reused/mode diagnostics for the runtime snapshot."""

        return {
            "runtime_read_mode": self._runtime_measurement_last_mode,
            "runtime_measurement_fresh_count": self._runtime_measurement_fresh_count,
            "runtime_measurement_reused_count": self._runtime_measurement_reused_count,
            "runtime_measurement_value_count": len(self._runtime_measurement_values),
            "runtime_measurement_owned_key_count": len(
                self._runtime_measurement_owned_keys
            ),
        }

    def set_persistent_unsupported_commands(self, commands: tuple[str, ...]) -> None:
        """Install the persisted unsupported-command set for this device.

        Any ``collector:``-namespaced metadata channel key is filtered out and
        never seeded into the DRIVER negative cache: metadata channel health is
        the metadata service's own state, persisted separately. This is
        belt-and-suspenders for a config entry not yet migrated -- the coordinator
        splits and migrates the persisted set, but a stray metadata key here is
        still kept out of the driver table.
        """

        self._persistent_unsupported_commands = tuple(
            command
            for command in (str(command or "").strip() for command in commands)
            if command and not command.startswith("collector:")
        )
        seed_unsupported_commands(
            self._runtime_read_state,
            self._persistent_unsupported_commands,
        )

    def set_persistent_metadata_dead_channels(self, channels: tuple[str, ...]) -> None:
        """Install the persisted metadata dead-channel set for this device."""

        self._collector_metadata_service.seed_dead_channels(
            tuple(
                channel
                for channel in (str(channel or "").strip() for channel in channels)
                if channel
            )
        )

    def collector_metadata_dead_channels(self) -> tuple[str, ...]:
        """Return the metadata dead-channel set for config-entry persistence."""

        return self._collector_metadata_service.dead_channels()

    def clear_unsupported_command_cache(self) -> None:
        """Forget both negative caches so the next cycles re-probe everything.

        The "Re-check supported commands" action revives inverter commands AND
        metadata channels; they are cleared through their SEPARATE stores.
        """

        self._persistent_unsupported_commands = ()
        clear_unsupported_commands(self._runtime_read_state)
        self._collector_metadata_service.clear_channel_health()

    def invalidate_collector_runtime_values(self) -> None:
        """Drop cached collector-side values so the next refresh reads them live."""

        self._clear_collector_runtime_value_caches()

    def _reset_volatile_collector_link_fields(self) -> None:
        """Drop link-scoped collector fields that must not survive an offline gap."""

        clear_reply = getattr(self._link_manager, "clear_discovery_reply", None)
        if callable(clear_reply):
            # The real link manager rebuilds collector_info from the announcer
            # on every access: the source must be cleared, not a snapshot.
            clear_reply()
        collector = self._link_manager.collector_info
        collector.last_udp_reply = ""
        collector.last_udp_reply_from = ""

    def _clear_collector_value_caches_for_outage(self) -> None:
        """Force one fresh collector read at the start of an outage.

        Consecutive failed cycles must not re-run the full (slow) AT metadata
        sweep every time: that inflates the failed-cycle duration, which the
        poll scheduler then mirrors into an equally long retry backoff.
        """

        if self._collector_outage_caches_cleared:
            return
        self._collector_outage_caches_cleared = True
        self._clear_collector_runtime_value_caches()

    def _combined_collector_runtime_values(self) -> dict[str, object]:
        return self._collector_metadata_service.merged_values()

    # --- Compatibility delegates -------------------------------------------------
    # Thin views over the metadata service so tests/diagnostics can read (and
    # legacy call sites can seed) the caches WITHOUT the hub holding a second copy
    # of the state. The service remains the single source of truth.

    @property
    def _collector_runtime_values(self) -> dict[str, object]:
        return self._collector_metadata_service.framed_values

    @_collector_runtime_values.setter
    def _collector_runtime_values(self, value: dict[str, object]) -> None:
        self._collector_metadata_service.framed_values = value

    @property
    def _collector_at_runtime_values(self) -> dict[str, object]:
        return self._collector_metadata_service.at_values

    @_collector_at_runtime_values.setter
    def _collector_at_runtime_values(self, value: dict[str, object]) -> None:
        self._collector_metadata_service.at_values = value

    @property
    def _collector_runtime_values_dirty(self) -> bool:
        return self._collector_metadata_service.dirty

    @_collector_runtime_values_dirty.setter
    def _collector_runtime_values_dirty(self, value: bool) -> None:
        self._collector_metadata_service.dirty = value

    @property
    def _collector_runtime_last_refresh_monotonic(self) -> float:
        return self._collector_metadata_service.framed_last_refresh_monotonic

    @_collector_runtime_last_refresh_monotonic.setter
    def _collector_runtime_last_refresh_monotonic(self, value: float) -> None:
        self._collector_metadata_service.framed_last_refresh_monotonic = value

    @property
    def _collector_at_runtime_last_attempt_monotonic(self) -> float:
        return self._collector_metadata_service.at_last_attempt_monotonic

    @_collector_at_runtime_last_attempt_monotonic.setter
    def _collector_at_runtime_last_attempt_monotonic(self, value: float) -> None:
        self._collector_metadata_service.at_last_attempt_monotonic = value

    def _publish_intermediate_snapshot(
        self,
        collector_values: dict[str, object],
        *,
        status: str,
    ) -> None:
        """Publish known collector state before a potentially slow inverter probe."""

        if not str(status or "").strip():
            return
        if self._snapshot_observer is None:
            return
        snapshot = self._build_snapshot(
            extra_values={**collector_values, "runtime_detection_status": status},
        )
        self._last_snapshot = snapshot
        try:
            self._snapshot_observer(snapshot)
        except Exception:
            logger.debug("Runtime intermediate snapshot observer failed", exc_info=True)

    async def async_write_capability(
        self,
        capability_key: str,
        value: object,
    ) -> object:
        """Write one validated capability through the active driver."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                raise RuntimeError(detect_error or "no_supported_driver_matched")

        snapshot = await self.async_refresh()
        capability = self._inverter.get_capability(capability_key)
        runtime_state = capability.runtime_state(snapshot.values)
        if not runtime_state.editable:
            reasons = "; ".join(runtime_state.reasons) or "capability_not_editable"
            raise ValueError(f"capability_not_editable:{capability_key}:{reasons}")

        written_value: object | None = None
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                written_value = await self._driver.async_write_capability(
                    self._link_manager.transport,
                    self._inverter,
                    capability_key,
                    value,
                )
                self._write_blockers.pop(capability_key, None)
                break
            except Exception as exc:
                last_error = exc
                if attempt == 0 and _is_retryable_collector_error(exc):
                    logger.warning(
                        "Write %s failed: %s; retrying once after collector reconnect",
                        capability_key,
                        exc,
                    )
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    continue
                classification = self._driver.classify_write_error(
                    capability,
                    exc,
                    operating_mode=snapshot.values.get("operating_mode"),
                )
                if classification.user_error is not None:
                    raise classification.user_error from exc
                if classification.blocker is not None:
                    logger.warning(
                        "Blocking capability %s after write failure: %s (%s)",
                        capability_key,
                        classification.blocker.reason,
                        classification.blocker.code,
                    )
                    self._write_blockers[capability_key] = classification.blocker
                raise

        if written_value is None:
            raise last_error or RuntimeError(f"write_failed:{capability_key}")

        snapshot = await self.async_refresh()
        if snapshot.last_error in {"collector_disconnected", "collector_not_connected", "waiting_for_collector"}:
            logger.warning(
                "Refresh after write reported: %s; retrying once after collector reconnect",
                snapshot.last_error,
            )
            await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
            snapshot = await self.async_refresh()
        if snapshot.last_error:
            logger.warning("Refresh after write reported: %s", snapshot.last_error)

        if _should_confirm_write(capability):
            readback_value = snapshot.values.get(capability.value_key)
            if not _write_readback_matches(
                capability,
                requested_value=value,
                written_value=written_value,
                readback_value=readback_value,
            ):
                logger.warning(
                    "Write %s was accepted but did not confirm by readback; expected=%r readback=%r refresh_error=%s",
                    capability_key,
                    written_value,
                    readback_value,
                    snapshot.last_error or "",
                )
                raise _write_not_confirmed_error(
                    capability,
                    written_value=written_value,
                    readback_value=readback_value,
                    refresh_error=snapshot.last_error,
                )
        return written_value

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        """Apply one declarative preset through sequential capability writes."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                raise RuntimeError(detect_error or "no_supported_driver_matched")

        snapshot = await self.async_refresh()
        preset = self._inverter.get_capability_preset(preset_key)
        runtime_state = preset.runtime_state(self._inverter, snapshot.values)
        if not runtime_state.visible:
            reasons = "; ".join(runtime_state.reasons) or "preset_not_visible"
            raise ValueError(f"preset_not_visible:{preset_key}:{reasons}")
        if not runtime_state.applicable:
            reasons = "; ".join(runtime_state.reasons or runtime_state.warnings) or "preset_not_applicable"
            raise ValueError(f"preset_not_applicable:{preset_key}:{reasons}")

        results: list[dict[str, object]] = []
        for item in sorted(preset.items, key=lambda item: (item.order, item.capability_key)):
            capability = self._inverter.get_capability(item.capability_key)
            current_value = snapshot.values.get(capability.value_key)
            target_label = capability.enum_value_map.get(item.value, item.value)
            if current_value == item.value or current_value == target_label:
                results.append(
                    {
                        "key": capability.key,
                        "status": "unchanged",
                        "current_value": current_value,
                        "target_value": target_label,
                    }
                )
                continue

            written_value = await self.async_write_capability(capability.key, item.value)
            snapshot = self._last_snapshot
            results.append(
                {
                    "key": capability.key,
                    "status": "written",
                    "current_value": current_value,
                    "target_value": target_label,
                    "written_value": written_value,
                }
            )

        return {
            "preset_key": preset.key,
            "title": preset.title,
            "results": results,
            "warnings": list(runtime_state.warnings),
        }

    def _collector_management_adapter(self):
        """Build the negotiated collector-management adapter (single switch: link).

        The wire is chosen ONCE, in ``link.collector_management_adapter_id``
        (live trusted SessionHandle > confirmed binding > conflict/unknown ->
        none). This hub never guesses framed/AT: it just hands both transport
        providers to the factory, which resolves the live transport lazily so a
        reconnect/handover never leaves the adapter holding a stale socket.
        """

        return select_collector_management_adapter(
            self._link_manager.collector_management_adapter_id(),
            framed_transport_provider=lambda: (
                getattr(self._link_manager, "active_transport", None)
                or self._link_manager.transport
            ),
            at_transport_provider=lambda: (
                getattr(self._link_manager, "active_collector_at_transport", None)
                or getattr(self._link_manager, "collector_at_transport", None)
            ),
        )

    def collector_management_capabilities(self) -> CollectorManagementCapabilities:
        """Return the CURRENT management capabilities (recomputed each call).

        Because the adapter is re-selected from the negotiated live wire on every
        call, capabilities reflect a live handover/adoption immediately without a
        config-entry reload.
        """

        return self._collector_management_adapter().capabilities

    def collector_management_diagnostics(self) -> dict[str, object]:
        """Return non-sensitive collector-management diagnostics.

        Never includes endpoint values, Wi-Fi credentials, or other secrets --
        only the selected adapter, its capabilities, and the last operation's
        status/error-class/duration/timestamp.
        """

        caps = self.collector_management_capabilities()
        provenance_getter = getattr(
            self._link_manager, "collector_management_adapter_provenance", None
        )
        diagnostics: dict[str, object] = {
            "collector_management_adapter_id": (
                self._link_manager.collector_management_adapter_id()
            ),
            "collector_management_adapter_provenance": (
                provenance_getter() if callable(provenance_getter) else ""
            ),
            "collector_management_capabilities": {
                "read_endpoint_state": caps.read_endpoint_state,
                "write_endpoint": caps.write_endpoint,
                "apply_changes": caps.apply_changes,
                "reboot": caps.reboot,
            },
        }
        if self._last_management_operation is not None:
            diagnostics["collector_management_last_operation"] = dict(
                self._last_management_operation
            )
        return diagnostics

    def collector_metadata_diagnostics(self) -> dict[str, object]:
        """Return non-sensitive collector-metadata TELEMETRY diagnostics.

        Delegates to the metadata service (routes / provenance / generation /
        per-channel outcome+duration / cache age+dirty / dead channels). Never
        includes endpoint values, Wi-Fi credentials, or raw AT payloads.
        """

        routes = None
        routes_getter = getattr(self._link_manager, "collector_metadata_routes", None)
        if callable(routes_getter):
            try:
                routes = routes_getter()
            except Exception:  # pragma: no cover - defensive during diagnostics
                routes = None
        return self._collector_metadata_service.diagnostics(routes)

    def _apply_collector_metadata_diagnostics(self, values: dict[str, object]) -> None:
        """Flatten metadata diagnostics into snapshot values for the support bundle.

        Safe, structured flat fields only -- counts / ages / typed error codes /
        per-channel failure counts / partial flags -- never endpoint values,
        credentials, raw AT payloads, or peer IP.
        """

        try:
            diagnostics = self.collector_metadata_diagnostics()
        except Exception:  # pragma: no cover - defensive during snapshot build
            return
        routes = [r for r in (diagnostics.get("routes") or []) if isinstance(r, dict)]
        channel_ids = [str(r.get("channel_id", "")) for r in routes if r.get("channel_id")]
        values["collector_metadata_route_channels"] = ", ".join(channel_ids)
        values["collector_metadata_route_provenance"] = str(
            diagnostics.get("route_provenance", "")
        )
        values["collector_metadata_session_generation"] = diagnostics.get(
            "session_generation", 0
        )
        values["collector_metadata_identity_known"] = bool(
            diagnostics.get("identity_known", False)
        )
        values["collector_metadata_identity_transitions"] = diagnostics.get(
            "identity_transitions", 0
        )

        def _join(pairs: list[str]) -> str:
            return ", ".join(pairs)

        statuses = [f"{r['channel_id']}={r.get('status', '')}" for r in routes if r.get("channel_id")]
        if statuses:
            values["collector_metadata_channel_status"] = _join(statuses)
        durations = [
            f"{r['channel_id']}={r.get('duration_ms', 0)}ms"
            for r in routes
            if r.get("channel_id") and r.get("duration_ms")
        ]
        if durations:
            values["collector_metadata_channel_duration_ms"] = _join(durations)
        errors = [
            f"{r['channel_id']}={r.get('error_code', '')}"
            for r in routes
            if r.get("channel_id") and r.get("error_code")
        ]
        if errors:
            values["collector_metadata_channel_errors"] = _join(errors)
        commands = [
            f"{r['channel_id']}={r.get('successful_commands', 0)}/{r.get('attempted_commands', 0)}"
            for r in routes
            if r.get("channel_id") and r.get("attempted_commands")
        ]
        if commands:
            values["collector_metadata_channel_commands"] = _join(commands)
        failures = [
            f"{r['channel_id']}={r.get('consecutive_failures', 0)}"
            for r in routes
            if r.get("channel_id") and r.get("consecutive_failures")
        ]
        if failures:
            values["collector_metadata_channel_failures"] = _join(failures)
        partial = [
            str(r["channel_id"]) for r in routes if r.get("channel_id") and r.get("partial")
        ]
        if partial:
            values["collector_metadata_partial_channels"] = _join(partial)

        refresh = diagnostics.get("refresh") or {}
        if isinstance(refresh, dict):
            values["collector_metadata_last_read_fresh"] = bool(
                refresh.get("last_read_fresh", False)
            )
        cache = diagnostics.get("cache") or {}
        if isinstance(cache, dict):
            values["collector_metadata_cache_dirty"] = bool(cache.get("dirty", False))
            values["collector_metadata_framed_cache_keys"] = cache.get("framed_cached_keys", 0)
            values["collector_metadata_at_cache_keys"] = cache.get("at_cached_keys", 0)
            framed_age = cache.get("framed_age_seconds")
            if framed_age is not None:
                values["collector_metadata_framed_age_seconds"] = framed_age
            at_age = cache.get("at_age_seconds")
            if at_age is not None:
                values["collector_metadata_at_age_seconds"] = at_age
        dead = [d for d in (diagnostics.get("dead_channels") or []) if isinstance(d, dict)]
        dead_ids = [str(d.get("channel_id", "")) for d in dead if d.get("channel_id")]
        if dead_ids:
            values["collector_metadata_dead_channels"] = ", ".join(dead_ids)
            values["collector_metadata_dead_channel_detail"] = ", ".join(
                f"{d['channel_id']}={d.get('consecutive_failures', 0)}/{d.get('threshold', 0)}"
                for d in dead
                if d.get("channel_id")
            )

    async def _run_management_operation(self, name: str, operation):
        """Execute one management operation, recording non-sensitive diagnostics.

        Records operation name, ok/error status, typed error class + short code,
        duration, and timestamp -- NEVER endpoint values or credentials -- so the
        per-action methods stay free of diagnostics bookkeeping.
        """

        started = asyncio.get_running_loop().time()
        record: dict[str, object] = {
            "operation": name,
            "status": "ok",
            "error_class": "",
            "error_code": "",
            "timestamp": _wall_time(),
        }
        try:
            return await operation()
        except CollectorManagementError as exc:
            record["status"] = "error"
            record["error_class"] = type(exc).__name__
            record["error_code"] = str(exc).split(":", 1)[0]
            raise
        except Exception as exc:  # noqa: BLE001 - recorded, then re-raised
            record["status"] = "error"
            record["error_class"] = type(exc).__name__
            raise
        finally:
            record["duration_ms"] = int(
                round((asyncio.get_running_loop().time() - started) * 1000.0)
            )
            self._last_management_operation = record

    def _collector_endpoint_write_result_to_dict(
        self, result: CollectorEndpointWriteResult
    ) -> dict[str, object]:
        """Map the normalized write result to the runtime/coordinator dict shape.

        ``status`` is HONEST: ``applied`` only when a requested apply was
        confirmed (``apply_performed``), otherwise ``staged`` (write done, no
        apply requested). A requested-but-unconfirmed apply never reaches here --
        the adapter raises. ``readback_endpoint`` is the real read (may be "").
        """

        out: dict[str, object] = {
            "status": "applied" if result.apply_performed else "staged",
            "requested_endpoint": result.requested_endpoint,
            "readback_endpoint": result.readback_endpoint,
            "apply_changes": result.apply_requested,
            "write_confirmed": result.write_confirmed,
            "apply_performed": result.apply_performed,
            "confirmation_source": result.confirmation_source,
        }
        if result.previous_endpoint:
            out["previous_endpoint"] = result.previous_endpoint
        if result.reboot_or_apply_required:
            out["reboot_required"] = result.reboot_or_apply_required
        if result.adapter_id == ADAPTER_COLLECTOR_AT_COMMANDS:
            out["management_protocol"] = "at_text"
        out.update(dict(result.extra or {}))
        if result.warnings:
            out["warning"] = result.warnings[0]
        return out

    async def async_set_collector_server_endpoint(
        self,
        endpoint: str,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        """Stage or apply the collector's upstream endpoint via the management adapter."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        normalized_endpoint = _normalize_collector_server_endpoint(endpoint)
        adapter = self._collector_management_adapter()
        result = await self._run_management_operation(
            "write_endpoint",
            lambda: adapter.async_write_endpoint(
                normalized_endpoint, apply_changes=apply_changes
            ),
        )

        if result.previous_endpoint and result.previous_endpoint != normalized_endpoint:
            self._collector_last_server_endpoint_before_change = result.previous_endpoint

        # Overlay the EFFECTIVE endpoint (real readback if the collector echoed
        # it, else the requested value it just wrote) as authoritative action
        # state: the hub knows what it requested, so this is honest -- it never
        # fabricates the result's ``readback_endpoint``. The service marks the
        # framed cache fresh so the next cadence-gated sweep does not clobber it.
        overlay: dict[str, object] = {
            "collector_server_endpoint": result.readback_endpoint or result.requested_endpoint
        }
        if result.reboot_or_apply_required:
            overlay["collector_reboot_required"] = result.reboot_or_apply_required
        self._collector_metadata_service.apply_authoritative_values(overlay)
        return self._collector_endpoint_write_result_to_dict(result)

    async def async_apply_collector_changes(self) -> dict[str, object]:
        """Trigger collector apply on parameter 29 without changing parameter 21."""

        return await self._async_execute_collector_system_action(action="apply")

    async def async_reboot_collector(self) -> dict[str, object]:
        """Trigger collector reboot-intent on parameter 29."""

        return await self._async_execute_collector_system_action(action="reboot")

    async def async_rollback_collector_server_endpoint(
        self,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        """Rollback parameter 21 to the cached endpoint remembered in this runtime session."""

        rollback_endpoint = self.collector_server_endpoint_rollback_target
        if not rollback_endpoint:
            raise RuntimeError("collector_rollback_endpoint_unavailable")

        result = await self.async_set_collector_server_endpoint(
            rollback_endpoint,
            apply_changes=apply_changes,
        )
        result["status"] = "rollback_applied" if apply_changes else "rollback_staged"
        result["rollback_source"] = "session_cached_previous_endpoint"
        result["rollback_endpoint"] = rollback_endpoint
        return result

    async def async_get_collector_server_endpoint_state(self) -> dict[str, object]:
        """Return the live collector endpoint and reboot-required flag from local management."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        adapter = self._collector_management_adapter()
        state = await self._run_management_operation(
            "read_endpoint_state", adapter.async_read_endpoint_state
        )

        overlay: dict[str, object] = {}
        if state.current_endpoint:
            overlay["collector_server_endpoint"] = state.current_endpoint
        if state.reboot_required:
            overlay["collector_reboot_required"] = state.reboot_required
        self._collector_metadata_service.apply_authoritative_values(overlay)
        return {
            "current_endpoint": state.current_endpoint,
            "reboot_required": state.reboot_required,
        }

    async def async_capture_support_evidence(self) -> dict[str, object]:
        """Capture matched-driver or generic raw evidence for one support archive."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        detect_error = ""
        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                if (
                    self._collector_capabilities().virtual_bridge
                    and "probe_timeout" in str(detect_error or "")
                ):
                    return self._collector_only_support_evidence(detect_error)
                return await self._async_capture_generic_support_evidence(detect_error)

        try:
            evidence = await self._driver.async_capture_support_evidence(
                self._link_manager.transport,
                self._inverter,
            )
        except Exception as exc:
            if _is_retryable_collector_error(exc):
                logger.warning(
                    "Support evidence capture failed: %s; retrying after collector reconnect",
                    exc,
                )
                await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                evidence = await self._driver.async_capture_support_evidence(
                    self._link_manager.transport,
                    self._inverter,
                )
            else:
                raise

        return evidence

    def _collector_capabilities(self):
        """Return collector capabilities from current hub runtime evidence."""

        return collector_capability_profile_from_runtime(
            collector=getattr(self._link_manager, "collector_info", None),
            values=self._combined_collector_runtime_values(),
        )

    def _collector_only_support_evidence(self, detect_error: str) -> dict[str, object]:
        """Return bounded support evidence when a local bridge has no inverter link."""

        return {
            "capture_kind": "collector_only",
            "driver_hint": self._driver_hint,
            "connection_mode": self._connection_mode,
            "detection_error": detect_error or "collector_link_probe_timeout",
            "captures": [],
            "range_failures": [],
            "note": (
                "Local bridge was detected, but no downstream inverter replied during "
                "driver detection; generic register scans were skipped."
            ),
        }

    async def _async_capture_at_text_ascii_probe(self) -> dict[str, object] | None:
        """Capture a raw ASCII probe trace over an at_text collector callback.

        Generic register dumps only cover Modbus drivers, but at_text
        collectors bridge raw-serial ASCII inverters (PI30 family, G-ASCII).
        When detection fails there, the register scans abort on the route
        guard and the archive carries no wire evidence at all — this bounded
        read-only sweep records what each query actually got back.
        """

        # EXPECTED (inferred) hint only -- gates whether to attempt the ASCII
        # probe at all; it is never treated as confirmed wire evidence here.
        expected_session_protocol = str(
            self._connection.collector_expected_session_protocol or ""
        ).strip().lower()
        if expected_session_protocol != "at_text":
            return None

        transport = self._link_manager.transport
        attempts: list[dict[str, object]] = []
        # Detection may have failed, so ``self._driver`` may be unbound: gather
        # read-only probe plans from every driver via the registry. The commands
        # are protocol policy owned by each driver; the hub only executes them.
        for driver in iter_drivers(DRIVER_HINT_AUTO):
            for probe in driver.support_probe_plan():
                route = select_payload_route(
                    transport,
                    EybondLinkRoute(devcode=1, collector_addr=255),
                    payload_family=probe.payload_family,
                )
                attempt: dict[str, object] = {
                    # The owning driver is authoritative here (registry
                    # iteration), so record its key as probe provenance rather
                    # than trusting a duplicated field on the descriptor.
                    "driver_key": driver.key,
                    "payload_family": probe.payload_family,
                    "command": probe.command,
                    "request_hex": probe.request.hex(),
                }
                try:
                    response = await async_send_payload(
                        transport,
                        probe.request,
                        route=route,
                        request_timeout=_AT_TEXT_ASCII_PROBE_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    attempt["error"] = "request_timeout"
                except Exception as exc:
                    attempt["error"] = str(exc)
                else:
                    attempt["response_hex"] = response.hex()
                    attempt["response_ascii"] = response.decode(
                        "ascii", errors="replace"
                    )
                attempts.append(attempt)

        collector = self._link_manager.collector_info
        return {
            "session_protocol": expected_session_protocol,
            "raw_passthrough_frame_format": collector.raw_last_frame_format,
            "raw_request_count": collector.raw_request_count,
            "raw_response_count": collector.raw_response_count,
            "raw_timeout_count": collector.raw_timeout_count,
            "raw_unhandled_line_count": collector.raw_unhandled_line_count,
            "attempts": attempts,
        }

    async def _async_capture_generic_support_evidence(
        self,
        detect_error: str,
    ) -> dict[str, object]:
        """Capture generic register evidence when no built-in driver matches."""

        captures: list[dict[str, Any]] = []

        for driver in iter_drivers(self._driver_hint):
            schema = getattr(driver, "register_schema_metadata", None)
            probe_targets = getattr(driver, "probe_targets", ())
            if schema is None or not probe_targets:
                continue

            target = probe_targets[0]
            ranges = _capture_ranges_from_schema(schema)
            if not ranges:
                continue

            session = ModbusSession(
                self._link_manager.transport,
                route=target.link_route,
                slave_id=target.payload_address,
            )

            captured_ranges: list[dict[str, Any]] = []
            fixture_ranges: list[dict[str, Any]] = []
            failures: list[dict[str, Any]] = []

            for start, count in ranges:
                try:
                    values = await session.read_holding(start, count)
                except Exception as exc:
                    failures.append(
                        {
                            "start": start,
                            "count": count,
                            "error": str(exc),
                        }
                    )
                    continue

                captured_ranges.append(_format_support_range(start, values))
                fixture_ranges.append(
                    {
                        "start": start,
                        "count": count,
                        "values": list(values),
                    }
                )

            captures.append(
                {
                    "driver_key": driver.key,
                    "driver_name": runtime_path_label(driver.key),
                    "driver_implementation_name": driver.name,
                    "runtime_path_name": runtime_path_label(driver.key),
                    "profile_name": getattr(driver, "profile_name", ""),
                    "register_schema_name": getattr(driver, "register_schema_name", ""),
                    "probe_target": {
                        "devcode": target.devcode,
                        "collector_addr": target.collector_addr,
                        "device_addr": target.device_addr,
                    },
                    "planned_ranges": [
                        {"start": start, "count": count}
                        for start, count in ranges
                    ],
                    "captured_ranges": captured_ranges,
                    "range_failures": failures,
                    "fixture_ranges": fixture_ranges,
                }
            )

        evidence: dict[str, object] = {
            "capture_kind": "generic_register_dump",
            "driver_hint": self._driver_hint,
            "connection_mode": self._connection_mode,
            "detection_error": detect_error or "no_supported_driver_matched",
            "captures": captures,
        }
        ascii_probe = await self._async_capture_at_text_ascii_probe()
        if ascii_probe is not None:
            evidence["at_text_ascii_probe"] = ascii_probe
        return evidence

    async def _async_execute_collector_system_action(self, *, action: str) -> dict[str, object]:
        """Run a standalone collector apply/reboot via the management adapter."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        adapter = self._collector_management_adapter()
        result: CollectorSystemActionResult = await self._run_management_operation(
            action,
            (
                adapter.async_apply_changes
                if action == "apply"
                else adapter.async_reboot
            ),
        )

        overlay: dict[str, object] = {"collector_reboot_required": "0"}
        if result.current_endpoint:
            overlay["collector_server_endpoint"] = result.current_endpoint
        self._collector_metadata_service.apply_authoritative_values(overlay)
        return {
            "status": "applied" if action == "apply" else "reboot_triggered",
            "action": action,
            "current_endpoint": result.current_endpoint,
            "reboot_required_before": result.reboot_required_before,
            "warning": (
                result.warnings[0]
                if result.warnings
                else "collector system action accepted; the current session may disconnect before the next refresh"
            ),
        }

    async def _async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        """Ensure there is an active collector connection, retrying discovery if needed."""

        ok = await self._async_try_connect_for_session_lifecycle(
            timeout=timeout,
            require_heartbeat=require_heartbeat,
        )
        if ok:
            return
        if require_heartbeat and self._link_manager.connected:
            await self._async_recover_heartbeat_timeout(timeout=timeout)
            return
        raise ConnectionError("collector_not_connected")

    async def _async_recover_heartbeat_timeout(self, *, timeout: float) -> None:
        """Drop a stale connected socket and wait for a fresh heartbeat."""

        self._record_recovery_attempt(reason="collector_heartbeat_timeout")
        await self._link_manager.async_reset_connection(reason="collector_heartbeat_timeout")
        await self._link_manager.async_ensure_connected(
            timeout=timeout,
            require_heartbeat=True,
        )

    async def _async_detect_driver(self) -> str:
        detection_task = asyncio.create_task(
            async_detect_inverter(
                self._link_manager.transport,
                driver_hint=self._driver_hint,
            ),
            name="eybond_inverter_detection",
        )
        wait_for_session_change = getattr(
            self._link_manager,
            "async_wait_for_owned_session_change",
            None,
        )
        session_change_task: asyncio.Task[None] | None = None
        if callable(wait_for_session_change):
            generation = int(
                getattr(self._link_manager, "owned_session_generation", 0) or 0
            )
            session_change_task = asyncio.create_task(
                wait_for_session_change(generation),
                name="eybond_detection_session_guard",
            )
        try:
            if session_change_task is None:
                context = await detection_task
            else:
                done, _pending = await asyncio.wait(
                    (detection_task, session_change_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if session_change_task in done and detection_task not in done:
                    detection_task.cancel()
                    try:
                        await detection_task
                    except asyncio.CancelledError:
                        pass
                    logger.info(
                        "Discarding inverter detection after owned collector session changed"
                    )
                    return "collector_session_changed"
                context = await detection_task
        except RuntimeError as exc:
            return str(exc)
        finally:
            if session_change_task is not None:
                session_change_task.cancel()
                try:
                    await session_change_task
                except asyncio.CancelledError:
                    pass

        # Identity-conflict guard: a durable/provisional binding is sticky. When
        # live detection reports a DIFFERENT full identity, report the conflict
        # and keep the durable identity -- never silently swap it. This runs on
        # the deferred provisional refresh and on any post-reset re-detection.
        previous = self._inverter
        if (
            _inverter_identity_is_present(previous)
            and _inverter_identity_is_present(context.inverter)
            and _inverter_identities_conflict(previous, context.inverter)
        ):
            self._inverter_identity_conflict = (
                f"{_inverter_identity_signature(previous)}"
                f" != {_inverter_identity_signature(context.inverter)}"
            )
            # Terminal for the provisional refresh: stop retrying and keep durable.
            self._inverter_binding_needs_live_detection_refresh = False
            self._inverter_binding_refresh_attempts = 0
            logger.warning(
                "Runtime inverter identity conflict: durable=%s live=%s; keeping durable identity",
                _inverter_identity_signature(previous),
                _inverter_identity_signature(context.inverter),
            )
            return "inverter_identity_conflict"

        self._inverter_identity_conflict = ""
        self._driver = context.driver
        self._inverter = context.inverter
        self._accept_inverter_binding_identity()
        self._inverter_binding_needs_live_detection_refresh = False
        self._inverter_binding_refresh_attempts = 0
        # The overlay merge is applied in _build_snapshot (every refresh, once the
        # collector identity is populated), not here -- at detection the collector is
        # not yet identified, so the device-scope match would fail and never retry.
        self._reset_runtime_read_state()
        self._write_blockers.clear()
        logger.info(
            "Detected inverter driver=%s protocol=%s serial=%s confidence=%s",
            context.inverter.driver_key,
            context.inverter.protocol_family,
            context.inverter.serial_number,
            context.match.confidence,
        )
        observer = self._inverter_detection_observer
        if observer is not None:
            try:
                observer(context.driver, context.inverter)
            except Exception:
                logger.debug("Runtime inverter detection observer failed", exc_info=True)
        return ""

    def _recovery_backoff_delay(self) -> float:
        base = max(2.0, float(self._connection.request_timeout))
        return min(60.0, base * (2 ** max(self._recovery_streak - 1, 0)))

    def _recovery_backoff_remaining(self) -> float:
        if self._recovery_backoff_until_monotonic <= 0.0:
            return 0.0
        return max(0.0, self._recovery_backoff_until_monotonic - monotonic())

    def _record_recovery_attempt(self, *, reason: str) -> None:
        self._reconnect_count += 1
        self._last_recovery_reason = reason

    def _record_recovery_failure(self, *, reason: str) -> None:
        self._recovery_streak += 1
        self._last_recovery_reason = reason
        self._recovery_backoff_until_monotonic = monotonic() + self._recovery_backoff_delay()

    def _record_refresh_success(self) -> None:
        self._last_success_monotonic = monotonic()
        self._collector_outage_caches_cleared = False
        self._recovery_streak = 0
        self._recovery_backoff_until_monotonic = 0.0
        self._last_recovery_reason = ""

    def _record_state_transition(self, tracks: tuple[str, ...]) -> None:
        """Append a composite state transition to the bounded diagnostic ring."""

        if tracks == self._last_composite_state:
            return
        self._last_composite_state = tracks
        self._state_transition_history.append("/".join(tracks))

    def _apply_runtime_state_values(
        self,
        values: dict[str, object],
        *,
        snapshot_connected: bool,
        last_error: str | None,
    ) -> None:
        """Derive the explicit runtime state-machine tracks onto the snapshot.

        These are diagnostic/auditable projections of the hub's implicit state;
        they never drive transport, ownership, or wire decisions. The tracks are
        independent on purpose -- the inverter track keeps its own lifecycle so a
        collector-only observation never erases a confirmed inverter identity.
        """

        collector = self._link_manager.collector_info
        collector_pn = str(getattr(collector, "collector_pn", "") or "").strip()
        durable_pn = str(getattr(self._connection, "collector_pn", "") or "").strip()
        inverter = self._inverter
        identity_present = _inverter_identity_is_present(inverter)
        detection_status = str(values.get("runtime_detection_status") or "").strip()
        recovering = (
            self._recovery_streak > 0
            or self._recovery_backoff_remaining() > 0.0
            or bool(last_error)
        )

        session_state = (
            RUNTIME_SESSION_STATE_ONLINE
            if snapshot_connected
            else RUNTIME_SESSION_STATE_OFFLINE
        )
        collector_state = (
            RUNTIME_COLLECTOR_STATE_IDENTIFIED
            if (collector_pn or durable_pn)
            else RUNTIME_COLLECTOR_STATE_UNKNOWN
        )

        if self._inverter_identity_conflict:
            inverter_state = RUNTIME_INVERTER_STATE_CONFLICT
        elif not identity_present:
            inverter_state = (
                RUNTIME_INVERTER_STATE_DETECTING
                if snapshot_connected
                else RUNTIME_INVERTER_STATE_ABSENT
            )
        elif (
            detection_status in _PROVISIONAL_INVERTER_DETECTION_STATUSES
            or self._inverter_binding_needs_live_detection_refresh
        ):
            inverter_state = RUNTIME_INVERTER_STATE_PROVISIONAL
        else:
            inverter_state = RUNTIME_INVERTER_STATE_LIVE_CONFIRMED

        # runtime_driver_state keeps its historical three values (backward compat).
        if not snapshot_connected:
            driver_state = RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE
        elif inverter is not None:
            driver_state = RUNTIME_DRIVER_STATE_DRIVER_BOUND
        else:
            driver_state = RUNTIME_DRIVER_STATE_DRIVER_UNBOUND

        if not snapshot_connected:
            poll_state = RUNTIME_POLL_STATE_OFFLINE
        elif inverter is None:
            poll_state = RUNTIME_POLL_STATE_DETECTING
        elif recovering:
            poll_state = RUNTIME_POLL_STATE_DEGRADED
        else:
            poll_state = RUNTIME_POLL_STATE_POLLING

        values["runtime_session_state"] = session_state
        values["runtime_collector_state"] = collector_state
        values["runtime_inverter_state"] = inverter_state
        values["runtime_driver_state"] = driver_state
        values["runtime_poll_state"] = poll_state

        if self._inverter_identity_conflict:
            values["runtime_identity_conflict"] = self._inverter_identity_conflict
        elif values.get("collector_pn_identity_conflict"):
            values["runtime_identity_conflict"] = "collector_pn"
        else:
            values.pop("runtime_identity_conflict", None)

        # The last identity that reached driver_bound stays available across
        # offline/degraded snapshots so support can see the confirmed inverter
        # even while it is temporarily unreachable.
        if driver_state == RUNTIME_DRIVER_STATE_DRIVER_BOUND and identity_present:
            signature = _inverter_identity_signature(inverter)
            if signature:
                self._last_driver_bound_identity = signature
        if self._last_driver_bound_identity:
            values["runtime_last_driver_bound_identity"] = self._last_driver_bound_identity

        self._record_state_transition(
            (session_state, collector_state, inverter_state, driver_state, poll_state)
        )
        if self._state_transition_history:
            values["runtime_state_transitions"] = " | ".join(
                self._state_transition_history
            )

    def _build_snapshot(
        self,
        *,
        extra_values: dict[str, object] | None = None,
        last_error: str | None = None,
        connected: bool | None = None,
        preserve_inverter_values: bool = False,
    ) -> RuntimeSnapshot:
        generated_canonical_keys: set[str] = set()
        runtime_owned_keys: set[str] = set()
        if self._inverter is not None and not preserve_inverter_values:
            generated_canonical_keys = {
                description.key
                for description in canonical_measurements_for_driver(self._inverter.driver_key)
            }
        if not preserve_inverter_values:
            # ALL runtime-owned keys (current identity's + the previous identity's
            # stale keys) are dropped from the carried snapshot, then the current
            # runtime cache is re-applied below. This removes canonical AND
            # non-canonical / raw / optional driver values that a FULL snapshot,
            # a DELTA ``removed_keys``, or an identity change no longer includes --
            # generically, never via a driver-specific key list.
            runtime_owned_keys = (
                self._runtime_measurement_owned_keys | self._stale_runtime_owned_keys
            )

        stripped_keys = generated_canonical_keys | runtime_owned_keys
        values = {
            key: value
            for key, value in self._last_snapshot.values.items()
            if (
                not key.startswith("capability_block_")
                and key not in stripped_keys
                and key not in _VOLATILE_COLLECTOR_VALUE_KEYS
            )
        }
        for key in _VOLATILE_COLLECTOR_VALUE_KEYS:
            values.pop(key, None)
        collector = self._link_manager.collector_info

        # PN stability: the durable entry PN is authoritative. A short live
        # heartbeat PN must not downgrade it, and a different full PN is an
        # identity conflict rather than a normal update.
        durable_collector_pn = str(
            getattr(self._connection, "collector_pn", "") or ""
        ).strip()
        collector_pn_identity_conflict = False
        if durable_collector_pn:
            reconciled_pn, collector_pn_identity_conflict = _reconcile_durable_collector_pn(
                durable_collector_pn,
                collector.collector_pn,
            )
            if collector_pn_identity_conflict:
                logger.warning(
                    "Collector PN identity conflict: entry expects %s but live session reports %s; keeping the durable identity",
                    durable_collector_pn,
                    collector.collector_pn,
                )
            if reconciled_pn and reconciled_pn != collector.collector_pn:
                collector.collector_pn = reconciled_pn
                collector.collector_pn_prefix = reconciled_pn[:1]
                collector.collector_pn_digits = reconciled_pn[1:]

        collector_field_overrides = extra_values or {}
        if collector_field_overrides:
            merged_collector_pn = _prefer_more_complete_collector_pn(
                collector.collector_pn,
                collector_field_overrides.get("collector_pn"),
            )
            if merged_collector_pn and merged_collector_pn != collector.collector_pn:
                collector.collector_pn = merged_collector_pn
                collector.collector_pn_prefix = merged_collector_pn[:1]
                collector.collector_pn_digits = merged_collector_pn[1:]
            collector.smartess_collector_version = str(
                collector_field_overrides.get("smartess_collector_version", collector.smartess_collector_version) or ""
            )
            collector.smartess_protocol_raw_id = str(
                collector_field_overrides.get("smartess_protocol_raw_id", collector.smartess_protocol_raw_id) or ""
            )
            collector.smartess_protocol_asset_id = str(
                collector_field_overrides.get("smartess_protocol_asset_id", collector.smartess_protocol_asset_id) or ""
            )
            collector.smartess_protocol_asset_name = str(
                collector_field_overrides.get("smartess_protocol_asset_name", collector.smartess_protocol_asset_name) or ""
            )
            collector.smartess_protocol_suffix = str(
                collector_field_overrides.get("smartess_protocol_suffix", collector.smartess_protocol_suffix) or ""
            )
            collector.smartess_protocol_profile_key = str(
                collector_field_overrides.get("smartess_protocol_profile_key", collector.smartess_protocol_profile_key) or ""
            )
            collector.smartess_protocol_name = str(
                collector_field_overrides.get("smartess_protocol_name", collector.smartess_protocol_name) or ""
            )
            if collector_field_overrides.get("smartess_device_address") is not None:
                collector.smartess_device_address = int(collector_field_overrides["smartess_device_address"])
            hardware_token = parse_esp_collector_hardware_token(
                collector_field_overrides.get("collector_hardware_version")
            )
            if hardware_token.is_bridge:
                collector.collector_virtual_bridge = True
                collector.collector_bridge_kind = "esp-collector"
                collector.collector_bridge_version = hardware_token.version

        if collector.remote_ip:
            values["collector_remote_ip"] = collector.remote_ip
        values["collector_connection_count"] = collector.connection_count
        values["collector_connection_replace_count"] = collector.connection_replace_count
        values["collector_disconnect_count"] = collector.disconnect_count
        values["collector_pending_request_drop_count"] = collector.pending_request_drop_count
        values["collector_raw_request_count"] = collector.raw_request_count
        values["collector_raw_response_count"] = collector.raw_response_count
        values["collector_raw_timeout_count"] = collector.raw_timeout_count
        values["collector_raw_unhandled_line_count"] = collector.raw_unhandled_line_count
        values["collector_raw_last_spacing_wait_ms"] = (
            collector.raw_last_spacing_wait_ms
        )
        values["collector_raw_last_response_duration_ms"] = (
            collector.raw_last_response_duration_ms
        )
        values["collector_raw_last_total_duration_ms"] = (
            collector.raw_last_total_duration_ms
        )
        for key, value in (
            ("collector_raw_last_request_ascii", collector.raw_last_request_ascii),
            ("collector_raw_last_request_hex", collector.raw_last_request_hex),
            ("collector_raw_last_response_ascii", collector.raw_last_response_ascii),
            ("collector_raw_last_response_hex", collector.raw_last_response_hex),
            (
                "collector_raw_last_timeout_request_ascii",
                collector.raw_last_timeout_request_ascii,
            ),
            ("collector_raw_last_parser", collector.raw_last_parser),
            ("collector_raw_last_frame_format", collector.raw_last_frame_format),
        ):
            if value:
                values[key] = value
            else:
                values.pop(key, None)
        values["collector_discovery_restart_count"] = collector.discovery_restart_count
        if collector.collector_pn:
            values["collector_pn"] = collector.collector_pn
        if collector_pn_identity_conflict:
            values["collector_pn_identity_conflict"] = True
        if collector.profile_name:
            values["collector_profile"] = collector.profile_name
        if collector.profile_key:
            values["collector_profile_key"] = collector.profile_key
        if collector.last_disconnect_reason:
            values["collector_last_disconnect_reason"] = collector.last_disconnect_reason
        else:
            values.pop("collector_last_disconnect_reason", None)
        if collector.last_discovery_reason:
            values["collector_last_discovery_reason"] = collector.last_discovery_reason
        else:
            values.pop("collector_last_discovery_reason", None)
        if collector.heartbeat_devcode is not None:
            values["collector_heartbeat_devcode"] = f"0x{collector.heartbeat_devcode:04X}"
        if collector.heartbeat_payload_hex:
            values["collector_heartbeat_payload"] = collector.heartbeat_payload_hex
        if collector.heartbeat_age_seconds is not None:
            values["collector_heartbeat_age_seconds"] = round(collector.heartbeat_age_seconds, 1)
        else:
            values.pop("collector_heartbeat_age_seconds", None)
        if collector.heartbeat_ascii:
            values["collector_heartbeat_ascii"] = collector.heartbeat_ascii
        if collector.heartbeat_payload_len is not None:
            values["collector_heartbeat_payload_len"] = collector.heartbeat_payload_len
        if collector.heartbeat_format_key:
            values["collector_heartbeat_format"] = collector.heartbeat_format_key
        if collector.heartbeat_suffix_ascii:
            values["collector_heartbeat_suffix"] = collector.heartbeat_suffix_ascii
        if collector.heartbeat_suffix_kind:
            values["collector_heartbeat_suffix_kind"] = collector.heartbeat_suffix_kind
        if collector.heartbeat_suffix_uint is not None:
            values["collector_heartbeat_suffix_uint"] = collector.heartbeat_suffix_uint
        if collector.devcode_major is not None:
            values["collector_devcode_major"] = collector.devcode_major
        if collector.devcode_minor is not None:
            values["collector_devcode_minor"] = collector.devcode_minor
        if collector.collector_pn_prefix:
            values["collector_pn_prefix"] = collector.collector_pn_prefix
        if collector.collector_pn_digits:
            values["collector_pn_digits"] = collector.collector_pn_digits
        values["connection_type"] = CONNECTION_TYPE_EYBOND
        if self._connection_mode:
            values["connection_mode"] = self._connection_mode
        if self._connection.collector_ip:
            values["configured_collector_ip"] = self._connection.collector_ip
        listener_diagnostics = getattr(self._link_manager, "listener_diagnostics", None)
        if listener_diagnostics is not None:
            values.update(listener_diagnostics())
            if not values.get("collector_listener_last_error"):
                values.pop("collector_listener_last_error", None)
        # Non-sensitive collector-management diagnostics: selected adapter
        # capabilities + the last operation's status/error-class/duration/time
        # (NEVER endpoint values or credentials).
        try:
            caps = self.collector_management_capabilities()
        except Exception:  # pragma: no cover - defensive during snapshot build
            caps = None
        if caps is not None:
            values["collector_management_can_read_endpoint_state"] = caps.read_endpoint_state
            values["collector_management_can_write_endpoint"] = caps.write_endpoint
            values["collector_management_can_apply_changes"] = caps.apply_changes
            values["collector_management_can_reboot"] = caps.reboot
        if self._last_management_operation is not None:
            op = self._last_management_operation
            values["collector_management_last_operation"] = op.get("operation", "")
            values["collector_management_last_status"] = op.get("status", "")
            values["collector_management_last_error_class"] = op.get("error_class", "")
            values["collector_management_last_error_code"] = op.get("error_code", "")
            values["collector_management_last_duration_ms"] = op.get("duration_ms", 0)
            values["collector_management_last_timestamp"] = op.get("timestamp", 0.0)
        # Non-sensitive collector-metadata TELEMETRY diagnostics: channel routes /
        # provenance / generation / per-channel outcome+duration / cache dirty /
        # dead channels (NEVER endpoint values, credentials, or raw payloads).
        self._apply_collector_metadata_diagnostics(values)
        # Collector Devcode is STABLE identity: the heartbeat frame's devcode.
        # 0x0000 is a valid devcode, so gate on ``is not None`` and NEVER fall
        # back to the volatile last-frame devcode -- that alternates with every
        # data forward and made the sensor flip between 0x0994 / 0x0001 / 0x0000.
        # The last frame is exposed separately as an honestly-labelled frame
        # diagnostic so it can never be mistaken for the collector identity.
        if collector.heartbeat_devcode is not None:
            values["collector_devcode"] = f"0x{collector.heartbeat_devcode:04X}"
        else:
            values.pop("collector_devcode", None)
        if collector.last_devcode is not None:
            values["collector_last_frame_devcode"] = f"0x{collector.last_devcode:04X}"
        else:
            values.pop("collector_last_frame_devcode", None)
        if collector.last_udp_reply:
            values["collector_udp_reply"] = collector.last_udp_reply
        if collector.last_udp_reply_from:
            values["collector_udp_reply_from"] = collector.last_udp_reply_from
        if collector.smartess_collector_version:
            values["smartess_collector_version"] = collector.smartess_collector_version
        if collector.smartess_protocol_raw_id:
            values["smartess_protocol_raw_id"] = collector.smartess_protocol_raw_id
        if collector.smartess_protocol_asset_id:
            values["smartess_protocol_asset_id"] = collector.smartess_protocol_asset_id
        if collector.smartess_protocol_asset_name:
            values["smartess_protocol_asset_name"] = collector.smartess_protocol_asset_name
        if collector.smartess_protocol_suffix:
            values["smartess_protocol_suffix"] = collector.smartess_protocol_suffix
        if collector.smartess_protocol_profile_key:
            values["smartess_protocol_profile_key"] = collector.smartess_protocol_profile_key
        if collector.smartess_protocol_name:
            values["smartess_protocol_name"] = collector.smartess_protocol_name
        if collector.smartess_device_address is not None:
            values["smartess_device_address"] = collector.smartess_device_address
        if collector.collector_virtual_bridge:
            values["collector_virtual_bridge"] = True
            if collector.collector_bridge_kind:
                values["collector_bridge_kind"] = collector.collector_bridge_kind
            if collector.collector_bridge_version:
                values["collector_bridge_version"] = collector.collector_bridge_version

        if self._inverter is not None:
            values["driver_key"] = self._inverter.driver_key
            values["protocol_family"] = self._inverter.protocol_family
            if not (extra_values and "runtime_detection_status" in extra_values):
                values.pop("runtime_detection_status", None)
            if self._inverter.variant_key:
                values["variant_key"] = self._inverter.variant_key
            if self._inverter.profile_name:
                values["profile_name"] = self._inverter.profile_name
            if self._inverter.register_schema_name:
                values["register_schema_name"] = self._inverter.register_schema_name
            values["model_name"] = self._inverter.model_name
            values["serial_number"] = self._inverter.serial_number
            # Inverter payload route (probe target). This is the route the driver
            # reaches the inverter over -- distinct from the collector-management
            # route and from any single last frame. Every field gates on
            # ``is not None`` so devcode/addr 0x0000 stay visible.
            probe_target = getattr(self._inverter, "probe_target", None)
            if probe_target is not None:
                if getattr(probe_target, "devcode", None) is not None:
                    values["inverter_route_devcode"] = f"0x{probe_target.devcode:04X}"
                if getattr(probe_target, "collector_addr", None) is not None:
                    values["inverter_route_collector_addr"] = probe_target.collector_addr
                if getattr(probe_target, "device_addr", None) is not None:
                    values["inverter_route_device_addr"] = probe_target.device_addr
            if self._inverter.capabilities:
                values["write_capabilities"] = ", ".join(
                    capability.key for capability in self._inverter.capabilities
                )
            # Detection ``details`` are bootstrap/config ONLY. They seed values
            # before the first runtime poll, but they must never re-seed a key
            # the runtime measurement cache already owns -- that is exactly the
            # revert-to-detection bug (e.g. battery_voltage 27.3 -> 27.2). Once a
            # key has been measured for this identity it is owned by the cache.
            owned = self._runtime_measurement_owned_keys
            if owned:
                for key, value in self._inverter.details.items():
                    if key not in owned:
                        values[key] = value
            else:
                values.update(self._inverter.details)
            # Last-good runtime measurements are authoritative for their keys on
            # EVERY snapshot build (including error / last-known-good paths), so a
            # cycle that omitted a measurement keeps the previous live value.
            values.update(self._runtime_measurement_values)

        if extra_values:
            values.update(extra_values)

        callback_endpoint = values.get("collector_server_endpoint")
        if callback_endpoint:
            values["collector_callback_owner"] = _callback_owner_label(
                callback_endpoint,
                server_ip=self._connection.server_ip,
                advertised_server_ip=self._connection.effective_advertised_server_ip,
                advertised_tcp_port=self._connection.effective_advertised_tcp_port,
            )
        else:
            values.pop("collector_callback_owner", None)

        signal_strength = values.get("collector_signal_strength")
        if signal_strength is not None:
            values["collector_signal_quality"] = _collector_signal_quality(signal_strength)
        else:
            values.pop("collector_signal_quality", None)

        signal_source = values.get("collector_signal_strength_source")
        if signal_source:
            values["collector_signal_strength_source"] = _collector_signal_source_label(
                signal_source
            )
        else:
            values.pop("collector_signal_strength_source", None)

        if self._inverter is not None:
            apply_canonical_measurements(
                self._inverter.driver_key,
                values,
                variant_key=self._inverter.variant_key,
            )

        values["runtime_recovery_streak"] = self._recovery_streak
        values["runtime_reconnect_count"] = self._reconnect_count
        values["runtime_backoff_seconds"] = round(self._recovery_backoff_remaining(), 1)
        if self._last_success_monotonic is not None:
            values["runtime_last_success_age_seconds"] = round(
                max(0.0, monotonic() - self._last_success_monotonic),
                1,
            )
        else:
            values.pop("runtime_last_success_age_seconds", None)
        if self._last_recovery_reason:
            values["runtime_last_recovery_reason"] = self._last_recovery_reason
        else:
            values.pop("runtime_last_recovery_reason", None)

        operating_mode = values.get("operating_mode")
        if operating_mode != self._last_operating_mode:
            clearable = [
                capability_key
                for capability_key, blocker in self._write_blockers.items()
                if blocker.clear_on == "mode_change"
            ]
            if self._last_operating_mode is not None and clearable:
                logger.info(
                    "Operating mode changed from %r to %r; clearing %d capability write blockers",
                    self._last_operating_mode,
                    operating_mode,
                    len(clearable),
                )
            for capability_key in clearable:
                self._write_blockers.pop(capability_key, None)
            self._last_operating_mode = operating_mode

        for capability_key, blocker in sorted(self._write_blockers.items()):
            values[f"capability_block_reason_{capability_key}"] = blocker.reason
            values[f"capability_block_code_{capability_key}"] = blocker.code
            if blocker.suggested_action:
                values[f"capability_block_action_{capability_key}"] = blocker.suggested_action
            if blocker.exception_code is not None:
                values[f"capability_block_exception_{capability_key}"] = blocker.exception_code
        if self._write_blockers:
            values["blocked_write_capabilities"] = ", ".join(sorted(self._write_blockers))
            values["blocked_write_count"] = len(self._write_blockers)
            values["blocked_write_summary"] = "; ".join(
                f"{capability_key}: {blocker.code}"
                for capability_key, blocker in sorted(self._write_blockers.items())
            )
        else:
            values.pop("blocked_write_capabilities", None)
            values.pop("blocked_write_count", None)
            values.pop("blocked_write_summary", None)

        snapshot_connected = self._link_manager.connected if connected is None else connected
        self._apply_runtime_state_values(
            values,
            snapshot_connected=snapshot_connected,
            last_error=last_error,
        )

        if last_error:
            values["last_error"] = last_error
        else:
            values.pop("last_error", None)

        if self._inverter_overlay_applier is not None and self._inverter is not None:
            # Merge activated device-scoped learned controls into the inverter on every
            # snapshot. This is idempotent (it only appends not-yet-present learned
            # capabilities). It must run here, not only at detection: detection completes
            # before the collector identity is fully populated, so a detection-time scope
            # match can fail and never retry; here the collector is ready and the merge
            # converges, so the learned controls reliably become entities and are writable.
            self._inverter = self._inverter_overlay_applier(self._inverter, collector)

        return RuntimeSnapshot(
            connected=snapshot_connected,
            collector=collector,
            inverter=self._inverter,
            values=values,
            last_error=last_error,
        )


def _capture_ranges_from_schema(schema: Any) -> tuple[tuple[int, int], ...]:
    """Build one generic support-capture plan from register schema metadata."""

    planned: list[tuple[int, int]] = []
    for block_key in ("status", "serial", "live", "config"):
        try:
            block = schema.block(block_key)
        except KeyError:
            continue
        planned.append((block.start, block.count))

    try:
        planned.extend(
            (spec.register, spec.word_count)
            for spec in schema.spec_set("aux_config")
        )
    except KeyError:
        pass

    scalar_registers = getattr(schema, "scalar_registers", {})
    planned.extend(
        (register, 1)
        for register in sorted(set(scalar_registers.values()))
    )
    return _merge_capture_ranges(planned)


def _merge_capture_ranges(
    ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...],
) -> tuple[tuple[int, int], ...]:
    normalized = sorted(
        (
            (int(start), int(count))
            for start, count in ranges
            if count > 0
        ),
        key=lambda item: item[0],
    )
    if not normalized:
        return ()

    merged: list[tuple[int, int]] = []
    current_start, current_count = normalized[0]
    current_end = current_start + current_count

    for start, count in normalized[1:]:
        end = start + count
        if start <= current_end:
            current_end = max(current_end, end)
            current_count = current_end - current_start
            continue
        merged.append((current_start, current_count))
        current_start = start
        current_count = count
        current_end = end

    merged.append((current_start, current_count))
    return tuple(merged)


def _decode_ascii_words(registers: list[int]) -> str:
    chars: list[str] = []
    for value in registers:
        for byte in ((value >> 8) & 0xFF, value & 0xFF):
            if byte in (0x00, 0xFF):
                continue
            char = chr(byte)
            if char.isalnum() or char in " -_/.":
                chars.append(char)
    return "".join(chars)


def _format_support_range(start: int, values: list[int]) -> dict[str, Any]:
    entries = []
    for offset, value in enumerate(values):
        entries.append(
            {
                "register": start + offset,
                "u16": value,
                "s16": to_signed_16(value),
                "hex": f"0x{value:04X}",
            }
        )
    return {
        "start": start,
        "count": len(values),
        "ascii": _decode_ascii_words(values),
        "words": list(values),
        "values": entries,
    }
