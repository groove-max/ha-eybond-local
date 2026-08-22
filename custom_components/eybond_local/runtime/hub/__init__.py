"""Composition root for the EyeBond runtime hub."""

from __future__ import annotations

from .common import (
    Any,
    Callable,
    CapabilityBlocker,
    CollectorMetadataRefreshResult,
    CollectorMetadataService,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DRIVER_DETECTION_FULL_SCAN,
    DRIVER_HINT_AUTO,
    DetectedInverter,
    EybondConnectionSpec,
    EybondRuntimeLinkManager,
    InverterDriver,
    RuntimeInverterCandidate,
    RuntimeSnapshot,
    TypedTelemetryFrame,
    _RUNTIME_STATE_TRANSITION_HISTORY_MAX,
    deque,
)
from .lifecycle import HubLifecycleMixin
from .refresh import HubRefreshMixin
from .management import HubManagementMixin
from .support import HubSupportMixin
from .detection import HubDetectionMixin
from .snapshot import HubSnapshotMixin


class EybondHub(
    HubLifecycleMixin,
    HubRefreshMixin,
    HubManagementMixin,
    HubSupportMixin,
    HubDetectionMixin,
    HubSnapshotMixin,
):
    """Coordinates runtime link connectivity, driver probing and polling."""

    def __init__(
        self,
        *,
        connection: EybondConnectionSpec,
        driver_hint: str = DRIVER_HINT_AUTO,
        driver_detection_strategy: str = DEFAULT_DRIVER_DETECTION_STRATEGY,
        connection_mode: str = "",
    ) -> None:
        self._driver_hint = driver_hint
        self._driver_detection_strategy = (
            driver_detection_strategy
            if driver_detection_strategy == DRIVER_DETECTION_FULL_SCAN
            else DEFAULT_DRIVER_DETECTION_STRATEGY
        )
        self._connection = connection
        self._connection_mode = connection_mode
        self._link_manager = EybondRuntimeLinkManager(
            server_ip=connection.server_ip,
            advertised_server_ip=connection.advertised_server_ip,
            collector_ip=connection.collector_ip,
            collector_pn=connection.collector_pn,
            collector_configured_session_protocol=(
                connection.collector_configured_session_protocol
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
        self._inverter_protocol_candidates: tuple[RuntimeInverterCandidate, ...] = ()
        self._inverter_protocol_candidate_generation = -1
        self._link_baud_sweep_generation = -1
        # Configured with the domain callback ownership context before runtime
        # start.  Collector mutations fail closed without a real config-entry id;
        # a standalone/bare hub may still poll, but it may not rewrite UART.
        self._collector_operation_entry_id = ""
        self._inverter_binding_needs_live_detection_refresh = False
        self._inverter_binding_refresh_attempts = 0
        self._inverter_identity_conflict = ""
        self._last_driver_bound_identity = ""
        # Sanitized evidence from the most recent runtime inverter-driver
        # sweep.  This is diagnostic state only: it never influences ordering,
        # budgets or binding.  The generation lets support distinguish a log
        # produced on the current session from one retained across reconnect.
        self._inverter_detection_probe_log: tuple[dict[str, object], ...] = ()
        self._inverter_detection_probe_budget_exhausted = False
        self._inverter_detection_probe_generation = -1
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
        self._runtime_measurement_telemetry = TypedTelemetryFrame.empty()
        self._runtime_measurement_owned_keys: set[str] = set()
        # Driver diagnostics have their own exact-snapshot lifecycle. They are
        # not measurements and never enter TypedTelemetryFrame, but the hub must
        # still own replacement/removal so a diagnostic omitted by the next
        # successful read cannot linger forever in RuntimeSnapshot.values.
        self._runtime_driver_diagnostics: dict[str, Any] = {}
        self._runtime_driver_diagnostic_owned_keys: set[str] = set()
        # Keys the PREVIOUS identity owned. When the binding changes they are
        # remembered here so the next snapshot also purges them from the carried
        # ``_last_snapshot`` (not just from the cache), then they are re-provided
        # by the new identity's detection details / cache if still relevant.
        self._stale_runtime_owned_keys: set[str] = set()
        self._stale_runtime_driver_diagnostic_keys: set[str] = set()
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
