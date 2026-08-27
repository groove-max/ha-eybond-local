"""Composition root for the EyeBond runtime link manager."""

from __future__ import annotations

from .common import (
    CALLBACK_STATE_IDLE,
    Callable,
    CallbackSessionRegistry,
    CollectorAtTransport,
    CollectorTransport,
    ConfirmedWireBinding,
    DiscoveryAnnouncer,
    InProcessFailClosedShadowProxyHandler,
    InProcessProxyCaptureHandler,
    RouteLease,
    RuntimeLinkManager,
    SharedCollectorAtTransport,
    SharedEybondTransport,
    SharedProxyCaptureRoute,
    _DEFAULT_LISTENER_BIND_HOST,
    _UnavailablePayloadTransport,
    asyncio,
    logger,
    resolve_server_ip,
)
from .session_projection import LinkSessionProjectionMixin
from .lifecycle import LinkLifecycleMixin
from .callback import LinkCallbackMixin
from .cloud_routes import LinkCloudRoutesMixin
from .connection import LinkConnectionMixin
from .wire_authority import LinkWireAuthorityMixin
from .transport_lifecycle import LinkTransportLifecycleMixin
from ...collector.session_identity_negotiator import (
    ExactSessionIdentityNegotiator,
)


class EybondRuntimeLinkManager(
    LinkSessionProjectionMixin,
    LinkLifecycleMixin,
    LinkCallbackMixin,
    LinkCloudRoutesMixin,
    LinkConnectionMixin,
    LinkWireAuthorityMixin,
    LinkTransportLifecycleMixin,
):
    """EyeBond-specific runtime lifecycle behind a neutral manager API."""

    def __init__(
        self,
        *,
        server_ip: str,
        collector_ip: str,
        tcp_port: int,
        udp_port: int,
        discovery_target: str,
        discovery_interval: int,
        heartbeat_interval: int,
        advertised_server_ip: str = "",
        advertised_tcp_port: int = 0,
        collector_pn: str = "",
        collector_identity_challenge_protocol: str = "",
        collector_configured_session_protocol: str = "",
        collector_identity_strategy: str = "",
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
        confirmed_session_protocol_evidence: "ConfirmedSessionProtocolEvidence | None" = None,
    ) -> None:
        self._configured_server_ip = server_ip
        self._configured_advertised_server_ip = advertised_server_ip.strip()
        self._collector_ip = collector_ip
        self._collector_pn = str(collector_pn or "").strip()
        challenge_protocol = str(
            collector_identity_challenge_protocol or ""
        ).strip().lower()
        self._configured_identity_challenge_protocol = (
            challenge_protocol
            if challenge_protocol in ("at_text", "eybond_framed")
            else ""
        )
        self._active_identity_challenge_protocol = ""
        self._session_identity_negotiator = ExactSessionIdentityNegotiator()
        # Configuration derived only from PN-bound confirmed live evidence.
        # The evidence object below remains the authority handed to transports.
        self._configured_collector_session_protocol = str(
            collector_configured_session_protocol or ""
        ).strip().lower()
        self._collector_identity_strategy = str(collector_identity_strategy or "").strip().lower()
        self._collector_raw_passthrough_bootstrap = (
            str(collector_raw_passthrough_bootstrap or "").strip().lower()
        )
        self._collector_raw_passthrough_frame_format = (
            str(collector_raw_passthrough_frame_format or "").strip().lower()
        )
        self._collector_raw_passthrough_min_interval_ms = max(
            0,
            int(collector_raw_passthrough_min_interval_ms or 0),
        )
        self._tcp_port = int(tcp_port)
        self._configured_advertised_tcp_port = int(advertised_tcp_port or 0)
        self._udp_port = int(udp_port)
        self._discovery_target = discovery_target
        self._discovery_interval = int(discovery_interval)
        self._heartbeat_interval = int(heartbeat_interval)
        self._effective_server_ip = resolve_server_ip(server_ip, collector_ip=collector_ip)
        self._listener_bind_host = _DEFAULT_LISTENER_BIND_HOST
        self._listener_status = "stopped"
        self._listener_last_error = ""
        self._listener_rebind_count = 0
        self._started = False
        self._discovery_restart_count = 0
        self._last_discovery_reason = ""
        self._reverse_discovery_enabled = True
        self._auxiliary_listener_ports: set[int] = set()
        # Session ownership + live wire negotiation goes through the registry
        # API, not by scanning listener internals. This runtime-scoped registry
        # observes only this entry's own listeners and owns this entry's durable
        # collector identity, so the negotiated SessionHandle represents the
        # entry-claimed session only.
        self._session_registry = CallbackSessionRegistry(
            sessions_source=self._iter_observed_sessions,
        )
        self._runtime_claim_pn: str | None = None
        # Phase 3 callback_on_demand one-shot trigger state.
        self._callback_trigger_count = 0
        self._last_callback_state: str = CALLBACK_STATE_IDLE
        self._last_callback_detail: str = ""
        # Cross-entry ownership authority (the domain registry) + this entry id,
        # injected read-only by the coordinator. Used only to classify the
        # "session claimed by another entry" callback outcome. Never used to read
        # listener internals or to pick a wire.
        self._callback_ownership_registry: CallbackSessionRegistry | None = None
        self._callback_entry_id: str = ""
        # A collector can move between already-open shared listeners while a
        # long inverter detection is running.  Track the owned socket identity
        # independently of polling so callers can invalidate work tied to the
        # replaced session.  The token contains only registry-owned session
        # identity/location -- never peer IP, endpoint, or collector type.
        self._owned_session_generation = 0
        self._owned_session_fingerprint: tuple[str, int] = ("", 0)
        # Socket ownership and wire confirmation are related but distinct
        # lifecycle transitions.  A newly accepted socket first appears as
        # pending and later becomes routed without changing session_id/port.
        # Track that trusted-wire transition separately so the confirmed
        # binding is adopted when the SAME socket becomes routable, without
        # falsely bumping the socket generation or cancelling work tied to it.
        self._owned_binding_observation_fingerprint: tuple[str, str, str] = (
            "",
            "",
            "",
        )
        self._owned_session_changed = asyncio.Event()
        self._owned_session_monitor_task: asyncio.Task[None] | None = None
        # Confirmed wire binding. A collector reconnect briefly leaves NO live
        # session observed (the new socket is parked/identified but not yet
        # routed). Absence of a live session is NOT evidence that the wire,
        # adapters, driver, or identity changed -- it is a session handover. Once
        # a trusted live SessionHandle has been observed, its DURABLE wire facts
        # are adopted into this immutable binding (never the transient socket
        # metadata), so cloud-family/persisted bootstrap cannot momentarily win
        # during the gap and trigger a destructive framed->at_text->framed
        # transport rebuild. It is written ONLY by the explicit lifecycle path
        # ``_adopt_trusted_live_binding`` (never by a diagnostics/accessor read),
        # from positive non-contradictory live evidence.
        self._confirmed_wire_binding: ConfirmedWireBinding | None = None
        self._seed_confirmed_wire_binding_from_evidence(
            confirmed_session_protocol_evidence
        )
        if server_ip and self._effective_server_ip and self._effective_server_ip != server_ip:
            logger.warning(
                "Configured EyeBond server_ip %s is not active on this host; falling back to %s",
                server_ip,
                self._effective_server_ip,
            )
        self._transport: CollectorTransport
        self._at_transport: CollectorAtTransport
        self._unavailable_payload_transport = _UnavailablePayloadTransport()
        self._auxiliary_transports: dict[int, SharedEybondTransport]
        self._auxiliary_at_transports: dict[int, SharedCollectorAtTransport]
        self._proxy_capture_route: SharedProxyCaptureRoute | None = None
        self._proxy_capture_handler: InProcessProxyCaptureHandler | None = None
        self._shadow_learning_route: SharedProxyCaptureRoute | None = None
        self._shadow_learning_handler: InProcessFailClosedShadowProxyHandler | None = None
        self._route_lease_lock = asyncio.Lock()
        self._route_lease: RouteLease | None = None
        self._announcer: DiscoveryAnnouncer
        self._collector_connection_watcher: Callable[[str], None] | None = None
        self._rebuild_link(self._effective_server_ip)
