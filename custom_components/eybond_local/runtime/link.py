"""Runtime link-manager layer between generic hub logic and concrete transports."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import ipaddress
import json
import logging
import socket
import subprocess
from typing import Callable, Protocol

from ..collector.cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_collector,
    select_preferred_collector_cloud_family,
)
from ..collector.discovery import DiscoveryAnnouncer, async_send_callback_trigger
from ..collector.metadata import (
    CollectorMetadataRouteSet,
    build_collector_metadata_routes,
)
from ..connection.confirmed_session_protocol import ConfirmedSessionProtocolEvidence
from ..connection.session_handle import (
    ADAPTER_NONE,
    ADAPTER_INVERTER_FRAMED_FC4,
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    ConfirmedWireBinding,
    SessionHandle,
)
from ..connection.session_registry import (
    CallbackSessionRegistry,
    pn_is_same_identity,
    reconcile_pn,
)
from ..collector.transport import (
    CollectorAtTransport,
    CollectorListenerBindError,
    CollectorTransport,
    SharedCollectorAtTransport,
    SharedEybondTransport,
    SharedProxyCaptureRoute,
)
from ..const import DEFAULT_REQUEST_TIMEOUT
from ..link_models import LinkRoute
from ..link_transport import PayloadLinkTransport
from ..models import CollectorInfo
from ..support.proxy_session import InProcessProxyCaptureHandler
from ..support.shadow_learning import ShadowWriteObservation
from ..support.shadow_learning_backend import ShadowLearningSeed
from ..support.shadow_learning_proxy import InProcessFailClosedShadowProxyHandler

logger = logging.getLogger(__name__)

# How long a runtime callback attempt waits for the shared causality lease before
# giving up quietly. Home Assistant already owns retry/backoff for the runtime, so
# queueing briefly is right and blocking is not.
_RUNTIME_CAUSALITY_LEASE_WAIT = 5.0

_DEFAULT_LISTENER_BIND_HOST = "0.0.0.0"

# Stable entry key for this link's own claim in its runtime-scoped session
# registry. One link manages exactly one collector identity, so a fixed key is
# sufficient; ownership is by durable PN, never peer IP.
_RUNTIME_SESSION_ENTRY_KEY = "runtime"

# Phase 3: typed outcomes of a callback_on_demand connect attempt. Surfaced in
# listener diagnostics / support packages so a failed one-shot callback is
# explainable instead of a generic "collector_offline".
CALLBACK_STATE_IDLE = ""
CALLBACK_STATE_CONNECTED = "callback_connected"
CALLBACK_STATE_TIMEOUT = "callback_timeout"
CALLBACK_STATE_IDENTITY_MISMATCH = "callback_identity_mismatch"
CALLBACK_STATE_CLAIMED_BY_OTHER = "callback_session_claimed_by_other_entry"
CALLBACK_STATE_LISTENER_UNAVAILABLE = "callback_listener_unavailable"
CALLBACK_STATE_LISTENER_ERROR = "callback_listener_error"

# One-shot UDP callback trigger send/reply window (seconds). This is not a
# polling interval -- exactly one datagram is sent per connect attempt.
_CALLBACK_TRIGGER_TIMEOUT = 0.75

# Actionable, user-facing explanations for each typed callback outcome. Surfaced
# in listener diagnostics / support packages so a failed callback is explainable
# rather than a generic "collector offline". Kept provider/hostname-neutral.
_CALLBACK_STATE_MESSAGES: dict[str, str] = {
    CALLBACK_STATE_CONNECTED: "The collector connected to Home Assistant.",
    CALLBACK_STATE_TIMEOUT: (
        "Home Assistant asked the collector to connect but it did not call back "
        "in time. Check the network path, the endpoint the collector points at, "
        "and any firewall between the collector and Home Assistant."
    ),
    CALLBACK_STATE_IDENTITY_MISMATCH: (
        "A collector connected, but it is a different collector than this entry "
        "expects. Check that the correct collector is being targeted."
    ),
    CALLBACK_STATE_CLAIMED_BY_OTHER: (
        "This collector is already bound to another Home Assistant entry. Remove "
        "the duplicate entry so only one owns this collector."
    ),
    CALLBACK_STATE_LISTENER_UNAVAILABLE: (
        "The Home Assistant listener that receives collector connections is not "
        "ready yet. It usually recovers on its own shortly."
    ),
    CALLBACK_STATE_LISTENER_ERROR: (
        "The Home Assistant listener that receives collector connections failed "
        "to start. Check the diagnostics for the listener error detail."
    ),
}


def _callback_state_message(state: str) -> str:
    """Return an actionable user-facing message for one typed callback state."""

    return _CALLBACK_STATE_MESSAGES.get(str(state or "").strip(), "")


@dataclass(frozen=True, slots=True)
class RouteLease:
    """Exclusive ownership record for the shared collector callback route."""

    mode: str
    owner_id: str
    entry_id: str
    collector_ip: str
    listen_port: int
    upstream_host: str
    upstream_port: int
    state: str


class _UnavailablePayloadTransport:
    """Fail-closed payload transport used when adapter negotiation conflicts."""

    @property
    def connected(self) -> bool:
        return False

    async def wait_until_connected(self, timeout: float) -> bool:
        return False

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
        request_timeout: float | None = None,
    ) -> bytes:
        raise TypeError("inverter_forward_adapter_not_available")

    def select_payload_route(
        self,
        route: LinkRoute,
        *,
        payload_family: str = "",
    ) -> LinkRoute:
        raise TypeError("inverter_forward_adapter_not_available")


def _prefer_more_complete_collector_pn(current: str, candidate: str) -> str:
    # Short/full PN reconciliation lives in the registry; this defers to it.
    return reconcile_pn(current, candidate)


def _default_local_ip() -> str:
    """Return the primary local IPv4 used for outbound traffic."""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except Exception:
        return ""


def _active_ipv4_addresses() -> tuple[str, ...]:
    """Return active global IPv4 addresses on this host."""

    return tuple(ip for ip, _prefixlen in _active_ipv4_interfaces())


def _active_ipv4_interfaces() -> tuple[tuple[str, int], ...]:
    """Return active global IPv4 addresses with prefix lengths on this host."""

    try:
        output = subprocess.check_output(
            ["ip", "-j", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        raw = json.loads(output)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        raw = []

    addresses: list[tuple[str, int]] = []
    for item in raw:
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
            try:
                prefixlen = int(addr.get("prefixlen", 32) or 32)
            except (TypeError, ValueError):
                prefixlen = 32
            addresses.append((ip, prefixlen))
    if not addresses:
        try:
            output = subprocess.check_output(
                ["ip", "-o", "-4", "addr", "show", "up"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.SubprocessError):
            output = ""
        for line in output.splitlines():
            parts = line.split()
            if "inet" not in parts:
                continue
            try:
                cidr = parts[parts.index("inet") + 1]
                interface = ipaddress.ip_interface(cidr)
            except (ValueError, IndexError):
                continue
            ip = str(interface.ip)
            if ip.startswith("127."):
                continue
            addresses.append((ip, interface.network.prefixlen))
    if not addresses:
        fallback = _default_local_ip()
        return ((fallback, 32),) if fallback else ()
    return tuple(dict.fromkeys(addresses))


def _same_ipv4_24_subnet(left: str, right: str) -> bool:
    """Return whether two IPv4 addresses share the same /24 subnet."""

    try:
        left_address = ipaddress.ip_address(left)
        right_address = ipaddress.ip_address(right)
    except ValueError:
        return False
    if left_address.version != 4 or right_address.version != 4:
        return False
    return ipaddress.ip_network(f"{left}/24", strict=False) == ipaddress.ip_network(
        f"{right}/24",
        strict=False,
    )


def resolve_server_ip(configured_ip: str, *, collector_ip: str = "") -> str:
    """Return a bindable server IP, preferring the collector-facing subnet when possible."""

    active_interfaces = _active_ipv4_interfaces()
    active_ips = tuple(ip for ip, _prefixlen in active_interfaces)
    if configured_ip and configured_ip in active_ips:
        return configured_ip

    try:
        collector_address = ipaddress.ip_address(collector_ip) if collector_ip else None
    except ValueError:
        collector_address = None

    if collector_address is not None and collector_address.version == 4:
        for ip, prefixlen in active_interfaces:
            try:
                network = ipaddress.ip_interface(f"{ip}/{prefixlen}").network
            except ValueError:
                continue
            if collector_address in network:
                return ip

        # For direct AP-mode collectors, keeping the same-subnet callback IP is safer than
        # auto-healing to an unrelated default-route interface that the collector cannot reach.
        if configured_ip and _same_ipv4_24_subnet(configured_ip, collector_ip):
            return configured_ip

    fallback = _default_local_ip()
    if fallback and fallback in active_ips:
        return fallback
    if active_ips:
        return active_ips[0]
    return configured_ip


def _callback_identity_status_values(
    *,
    pending_count: int,
    recent_count: int,
    duplicate_peer_ip_count: int,
    sessions: list[dict[str, object]],
    expects_collector_identity: bool = False,
    owned_session_observed: bool = False,
    handover_in_progress: bool = False,
) -> dict[str, object]:
    """Return compact, user-facing callback identity diagnostics.

    ``owned_session_observed`` means we hold a confirmed binding or a current
    live session; ``handover_in_progress`` means we hold a confirmed binding but
    its live socket is momentarily absent. A ``conflict`` is reported ONLY on
    positive evidence -- a ``route_identity_mismatch`` state (the listener proved
    a different collector answered on our route). A merely-identified foreign
    session on a shared listener is unresolved/unowned, never a conflict for this
    entry (two collectors behind one peer IP each keep their own identity). A
    normal same-collector socket replacement is reported as ``reconnecting``.
    """

    identified_count = 0
    unresolved_count = 0
    mismatch_count = 0
    timeout_count = 0
    waiting_count = 0
    foreign_identified_count = 0
    pending_states = {
        "pending",
        "waiting_for_identity",
        "waiting_for_route_identity",
    }
    for session in sessions:
        state = str(session.get("state") or "").strip()
        if session.get("collector_identity_masked"):
            identified_count += 1
            # An identified session that our entry does not own is a foreign
            # collector sharing the listener, not a conflict. Ownership is
            # decided by the registry (durable PN), never by presence here.
            if expects_collector_identity and not owned_session_observed:
                foreign_identified_count += 1
                unresolved_count += 1
            continue
        if state == "route_identity_mismatch":
            mismatch_count += 1
            unresolved_count += 1
            continue
        if state.endswith("_timeout"):
            timeout_count += 1
            unresolved_count += 1
            continue
        if state in pending_states:
            waiting_count += 1
            unresolved_count += 1

    if mismatch_count:
        status = "conflict"
        summary = (
            "A collector callback was identified, but it does not match the expected collector PN."
        )
    elif handover_in_progress:
        status = "reconnecting"
        summary = (
            "The collector is replacing its connection; the previously confirmed session is being handed over."
        )
    elif pending_count <= 0:
        status = "idle"
        summary = "No unresolved collector callback sessions are pending."
    elif duplicate_peer_ip_count and unresolved_count:
        status = "unresolved"
        summary = (
            "Multiple collector callbacks share the same peer IP and at least one session is still not safely identified."
        )
    elif timeout_count:
        status = "unresolved"
        summary = "A collector callback is pending, but the identity probe timed out."
    elif waiting_count:
        status = "unresolved"
        summary = "A collector callback is pending, but the collector identity is not known yet."
    elif foreign_identified_count:
        status = "unresolved"
        summary = (
            "An identified collector callback is not owned by this entry (another collector on the shared listener)."
        )
    else:
        status = "ok"
        summary = "Pending collector callbacks have a known collector identity."

    return {
        "collector_callback_identity_status": status,
        "collector_callback_identity_summary": summary,
        "collector_callback_identified_session_count": identified_count,
        "collector_callback_foreign_identified_session_count": foreign_identified_count,
        "collector_callback_unresolved_session_count": unresolved_count,
        "collector_callback_identity_mismatch_count": mismatch_count,
        "collector_callback_identity_timeout_count": timeout_count,
        "collector_callback_identity_waiting_count": waiting_count,
        "collector_callback_recent_session_count": recent_count,
    }


class RuntimeLinkManager(Protocol):
    """Minimal runtime lifecycle contract for one active physical link."""

    @property
    def transport(self) -> PayloadLinkTransport:
        ...

    @property
    def connected(self) -> bool:
        ...

    @property
    def collector_info(self) -> CollectorInfo:
        ...

    async def async_start(self) -> None:
        ...

    async def async_stop(self) -> None:
        ...

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        ...

    async def async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        ...

    async def async_reset_connection(self, *, reason: str = "") -> None:
        ...


class EybondRuntimeLinkManager:
    """EyeBond-specific runtime lifecycle wrapped behind a neutral manager API."""

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

    def set_collector_connection_watcher(self, callback: Callable[[str], None] | None) -> None:
        """Notify ``callback(remote_ip)`` when this entry's collector dials in.

        Survives link rebuilds; used to trigger an immediate refresh instead
        of waiting out the poll backoff after the collector reconnects.
        """

        self._collector_connection_watcher = callback
        self._apply_collector_connection_watcher()

    def _apply_collector_connection_watcher(self) -> None:
        for transport in (
            self._transport,
            *self._auxiliary_transports.values(),
        ):
            set_watcher = getattr(transport, "set_connection_watcher", None)
            if callable(set_watcher):
                set_watcher(self._collector_connection_watcher)

    def _current_owned_session_fingerprint(self) -> tuple[str, int]:
        """Return the owned socket identity used to invalidate stale work."""

        session = self._owned_domain_session()
        if session is None:
            return ("", 0)
        return (
            str(getattr(session, "session_id", "") or ""),
            int(getattr(session, "listener_port", 0) or 0),
        )

    def _current_trusted_binding_observation_fingerprint(
        self,
    ) -> tuple[str, str, str]:
        """Return positive live-wire evidence for explicit binding adoption.

        The socket can move from ``waiting_for_route_identity`` to
        ``routed_framed``/``routed_at_text`` without changing session id or
        listener port.  Only the latter state is trusted wire evidence, so this
        fingerprint deliberately stays empty until the live SessionHandle is
        observed and non-conflicting.
        """

        handle = self._live_session_handle()
        if not handle.observed or handle.conflict:
            return ("", "", "")
        return (
            str(handle.session_id or "").strip(),
            str(handle.wire_framing or "").strip(),
            str(handle.collector_pn or "").strip(),
        )

    def _reconcile_owned_session_binding_observation(self) -> None:
        """Adopt a binding when the owned socket gains trusted wire evidence."""

        fingerprint = self._current_trusted_binding_observation_fingerprint()
        if fingerprint == getattr(
            self,
            "_owned_binding_observation_fingerprint",
            ("", "", ""),
        ):
            return
        self._owned_binding_observation_fingerprint = fingerprint
        self._adopt_trusted_live_binding()

    @property
    def owned_session_generation(self) -> int:
        """Return the generation of the currently owned inbound socket."""

        return self._owned_session_generation

    async def async_wait_for_owned_session_change(self, generation: int) -> None:
        """Wait until registry ownership moves to another live socket.

        This is used to cancel inverter detection that was started against a
        socket which has since disconnected or been replaced on another shared
        listener.  The background monitor is the event source; this method does
        not inspect listener internals.
        """

        while self._owned_session_generation == int(generation):
            self._owned_session_changed.clear()
            if self._owned_session_generation != int(generation):
                return
            await self._owned_session_changed.wait()

    def _start_owned_session_monitor(self) -> None:
        if self._owned_session_monitor_task is not None:
            return
        if not self._domain_ownership_active():
            return
        self._owned_session_fingerprint = self._current_owned_session_fingerprint()
        self._owned_session_monitor_task = asyncio.create_task(
            self._async_owned_session_monitor(),
            name=f"eybond_owned_session_{self._callback_entry_id}",
        )

    async def _stop_owned_session_monitor(self) -> None:
        task = self._owned_session_monitor_task
        self._owned_session_monitor_task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _async_owned_session_monitor(self) -> None:
        """Observe registry-owned socket replacement across shared listeners."""

        while True:
            await asyncio.sleep(0.2)
            # Routing/negotiation can complete on the same socket, so it is not
            # sufficient to react only to session_id/port replacement.
            self._reconcile_owned_session_binding_observation()
            fingerprint = self._current_owned_session_fingerprint()
            if fingerprint == self._owned_session_fingerprint:
                continue
            self._owned_session_fingerprint = fingerprint
            self._owned_session_generation += 1
            # A newly-observed owned socket is an explicit session event: adopt
            # its trusted wire as the confirmed binding here (not on any read).
            self._adopt_trusted_live_binding()
            self._owned_session_changed.set()
            if fingerprint[0] and self._collector_connection_watcher is not None:
                session = self._owned_domain_session()
                try:
                    self._collector_connection_watcher(
                        str(getattr(session, "peer_ip", "") or "")
                    )
                except Exception:
                    logger.debug(
                        "Owned-session connection watcher failed",
                        exc_info=True,
                    )

    def clear_discovery_reply(self) -> None:
        """Drop the remembered UDP discovery reply.

        ``collector_info`` rebuilds its snapshot from the announcer on every
        call, so stale-reply cleanup must clear the announcer source — not a
        returned copy.
        """

        self._announcer.last_reply = ""
        self._announcer.last_reply_from = ""

    @property
    def active_transport(self) -> CollectorTransport | None:
        """Return the connected payload transport selected for the active collector."""

        if self._inverter_forward_adapter() == ADAPTER_INVERTER_RAW_PASSTHROUGH:
            return None
        return self._connected_payload_transport()

    @property
    def active_collector_at_transport(self) -> CollectorAtTransport | None:
        """Return the connected AT transport selected for the active collector."""

        return self._connected_at_transport()

    @property
    def transport(self) -> CollectorTransport:
        """Return the active payload-capable transport."""

        adapter = self._inverter_forward_adapter()
        if adapter == ADAPTER_NONE:
            return self._unavailable_payload_transport
        if adapter == ADAPTER_INVERTER_RAW_PASSTHROUGH:
            return self.active_collector_at_transport or self._at_transport
        return self.active_transport or self._transport

    @property
    def collector_at_transport(self) -> CollectorAtTransport:
        """Return the collector AT transport sharing the same listener port."""

        return self.active_collector_at_transport or self._at_transport

    @property
    def connected(self) -> bool:
        """Return whether the physical link is currently connected (socket-level).

        Connectivity is independent of whether the payload WIRE is known yet:
        payload forwarding is separately fail-closed via the inverter adapter
        (``self.transport`` is the unavailable transport until the wire is
        observed/confirmed). So a connected-but-unobserved socket reports
        connected here but does NOT forward payloads -- reads go through the
        fail-closed ``transport`` and wait for observed/confirmed evidence.

        A contradictory live wire observation (``conflict``) is the one hard
        fail-closed state: it reports NOT connected so the runtime never treats a
        self-contradicting session as usable.
        """

        if self._live_session_handle().conflict:
            return False
        return (
            self.active_transport is not None
            or self.active_collector_at_transport is not None
        )

    @property
    def collector_info(self) -> CollectorInfo:
        """Return collector metadata merged with the latest UDP discovery reply."""

        _, ambiguous = self._selected_connected_remote_ip()
        if ambiguous:
            collector = CollectorInfo()
            at_collector = CollectorInfo()
        else:
            collector_transport = self.active_transport
            at_transport = self.active_collector_at_transport
            collector = collector_transport.collector_info if collector_transport is not None else self._transport.collector_info
            at_collector = at_transport.collector_info if at_transport is not None else self._at_transport.collector_info
        if not collector.remote_ip and at_collector.remote_ip:
            collector.remote_ip = at_collector.remote_ip
            collector.remote_port = at_collector.remote_port
        if at_collector.connection_count > collector.connection_count:
            collector.remote_port = at_collector.remote_port
            collector.connection_count = at_collector.connection_count
            collector.connection_replace_count = at_collector.connection_replace_count
            collector.disconnect_count = at_collector.disconnect_count
            collector.last_disconnect_reason = at_collector.last_disconnect_reason
            collector.pending_request_drop_count = at_collector.pending_request_drop_count
        # For at_text collectors all raw inverter traffic lives on the AT
        # connection; without this merge support bundles report zero raw
        # requests even while probes are actively timing out on the wire.
        if (
            at_collector.raw_request_count > collector.raw_request_count
            or at_collector.raw_unhandled_line_count > collector.raw_unhandled_line_count
        ):
            collector.raw_request_count = at_collector.raw_request_count
            collector.raw_response_count = at_collector.raw_response_count
            collector.raw_timeout_count = at_collector.raw_timeout_count
            collector.raw_unhandled_line_count = at_collector.raw_unhandled_line_count
            collector.raw_last_request_ascii = at_collector.raw_last_request_ascii
            collector.raw_last_request_hex = at_collector.raw_last_request_hex
            collector.raw_last_response_ascii = at_collector.raw_last_response_ascii
            collector.raw_last_response_hex = at_collector.raw_last_response_hex
            collector.raw_last_timeout_request_ascii = (
                at_collector.raw_last_timeout_request_ascii
            )
            collector.raw_last_parser = at_collector.raw_last_parser
            collector.raw_last_frame_format = at_collector.raw_last_frame_format
            collector.raw_last_spacing_wait_ms = at_collector.raw_last_spacing_wait_ms
            collector.raw_last_response_duration_ms = (
                at_collector.raw_last_response_duration_ms
            )
            collector.raw_last_total_duration_ms = (
                at_collector.raw_last_total_duration_ms
            )
        merged_pn = _prefer_more_complete_collector_pn(
            collector.collector_pn,
            at_collector.collector_pn,
        )
        if merged_pn and merged_pn != collector.collector_pn:
            collector.collector_pn = merged_pn
            collector.collector_pn_prefix = merged_pn[:1]
            collector.collector_pn_digits = merged_pn[1:]
        apply_collector_cloud_family_observation(
            collector,
            select_preferred_collector_cloud_family(
                collector_cloud_family_observation_from_collector(collector),
                collector_cloud_family_observation_from_collector(at_collector),
            ),
        )
        if not collector.smartess_collector_version and at_collector.smartess_collector_version:
            collector.smartess_collector_version = at_collector.smartess_collector_version
        collector.last_udp_reply = self._announcer.last_reply
        collector.last_udp_reply_from = self._announcer.last_reply_from
        collector.discovery_restart_count = self._discovery_restart_count
        collector.last_discovery_reason = self._last_discovery_reason
        return collector

    @property
    def effective_server_ip(self) -> str:
        """Return the current collector-facing IP used for discovery and advertising."""

        return self._effective_server_ip

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the advertised callback IP used by UDP bootstrap probes."""

        return self._configured_advertised_server_ip or self._effective_server_ip

    @property
    def effective_advertised_tcp_port(self) -> int:
        """Return the advertised callback TCP port used by UDP bootstrap probes."""

        return self._configured_advertised_tcp_port or self._tcp_port

    @property
    def listener_bind_host(self) -> str:
        """Return the local TCP bind host used by collector callback listeners."""

        return self._listener_bind_host

    @property
    def listener_status(self) -> str:
        """Return the listener lifecycle status for diagnostics."""

        return self._listener_status

    @property
    def listener_last_error(self) -> str:
        """Return the latest listener start error for diagnostics."""

        return self._listener_last_error

    def _current_live_session_state(self) -> str:
        """Return the current real session state for diagnostics (pure read).

        ``SessionHandle`` always describes the CURRENT socket; this collapses it
        to a coarse, honest label separate from the confirmed wire binding.
        """

        handle = self._live_session_handle()
        if handle.conflict:
            return "conflict"
        if handle.observed:
            return "active"
        if str(getattr(handle, "session_id", "") or "").strip():
            return "pending"
        return "absent"

    def listener_diagnostics(self) -> dict[str, object]:
        """Return listener bind and advertised endpoint diagnostics."""

        # Report the CURRENT live session and the CONFIRMED wire binding as two
        # separate facts. The effective wire/adapters describe how the runtime
        # routes RIGHT NOW: the live session when observed, otherwise the
        # confirmed binding (so a mid-handover support bundle shows framed_fc4,
        # not a momentary "unknown"). ``adapter_conflict`` reflects the CURRENT
        # live conflict only (fail-closed signal), never the binding.
        live_handle = self._live_session_handle()
        binding = self._effective_wire_binding()
        live_effective = live_handle.observed and not live_handle.conflict
        if live_effective:
            eff_wire = live_handle.wire_framing
            eff_sources = live_handle.identity_sources
            eff_forward = live_handle.inverter_forward_adapter
            eff_proxy = live_handle.proxy_adapter
        elif binding is not None:
            eff_wire = binding.wire_framing
            eff_sources = binding.identity_sources
            eff_forward = binding.inverter_forward_adapter
            eff_proxy = binding.proxy_adapter
        else:
            eff_wire = live_handle.wire_framing
            eff_sources = live_handle.identity_sources
            eff_forward = live_handle.inverter_forward_adapter
            eff_proxy = live_handle.proxy_adapter
        # Collector management is resolved by its OWN single resolver (conflict ->
        # none/"conflict"), NOT the shared wire/forward selection above.
        _mgmt_adapter_id, _mgmt_provenance = self._collector_management_selection()
        current_live_session = self._current_live_session_state()
        diagnostics: dict[str, object] = {
            "collector_listener_status": self._listener_status,
            "collector_listener_bind_host": self._listener_bind_host,
            "collector_listener_bind_endpoint": f"{self._listener_bind_host}:{self._tcp_port}",
            "collector_listener_effective_host": self._effective_server_ip,
            "collector_listener_advertised_endpoint": (
                f"{self.effective_advertised_server_ip}:{self.effective_advertised_tcp_port}"
            ),
            "collector_listener_rebind_count": self._listener_rebind_count,
            "collector_listener_last_error": self._listener_last_error,
            "collector_callback_observed_session_protocol": (
                self._owned_observed_session_protocol()
            ),
            # Configured, confirmed and live remain separate. There is no
            # cloud-derived preliminary/expected protocol tier.
            "collector_configured_session_protocol": (
                self._configured_collector_session_protocol
            ),
            "collector_confirmed_session_protocol": (
                binding.session_protocol if binding is not None else ""
            ),
            "collector_live_session_protocol": (
                ""
                if live_handle.conflict
                else (
                    "eybond_framed"
                    if live_handle.uses_framed_wire
                    else ("at_text" if live_handle.uses_at_text_wire else "")
                )
            ),
            # Current real session vs confirmed binding, reported separately.
            "collector_current_live_session": current_live_session,
            "collector_confirmed_wire_binding": (
                binding.wire_framing if binding is not None else "none"
            ),
            "collector_callback_wire_framing": eff_wire,
            "collector_callback_identity_sources": ", ".join(sorted(eff_sources)),
            # Collector-management adapter + provenance come from the ONE resolver,
            # so a live conflict reports (none, "conflict") -- the stale confirmed
            # binding is never shown as the effective management adapter, and the
            # id/provenance can never disagree.
            "collector_callback_collector_management_adapter": _mgmt_adapter_id,
            "collector_management_adapter_id": _mgmt_adapter_id,
            "collector_management_adapter_provenance": _mgmt_provenance,
            "collector_callback_inverter_forward_adapter": eff_forward,
            "collector_callback_proxy_adapter": eff_proxy,
            "collector_callback_adapter_conflict": live_handle.conflict,
            "collector_callback_identity_strategy": self._collector_identity_strategy,
            "collector_callback_raw_passthrough_bootstrap": (
                self._collector_raw_passthrough_bootstrap
            ),
            "collector_callback_raw_passthrough_frame_format": (
                self._collector_raw_passthrough_frame_format
            ),
            "collector_callback_raw_passthrough_min_interval_ms": (
                self._collector_raw_passthrough_min_interval_ms
            ),
        }
        diagnostics.update(self._session_ownership_diagnostics())
        diagnostics.update(self._session_inventory_diagnostics())
        diagnostics.update(self.callback_trigger_diagnostics())
        return diagnostics

    def _session_ownership_diagnostics(self) -> dict[str, object]:
        """Return domain transport-ownership diagnostics for the support bundle.

        Makes the end-to-end ownership chain auditable: which entry claim the
        domain registry resolved, the exact claimed session id, the listener
        port the collector actually dialed, the primary configured port, and the
        listener port of the transport currently carrying the connection.
        """

        domain_active = self._domain_ownership_active()
        session = self._owned_domain_session() if domain_active else None
        active_port = 0
        if getattr(self, "_transport", None) is not None and self._transport.connected:
            active_port = self._tcp_port
        else:
            for port in sorted(getattr(self, "_auxiliary_listener_ports", ()) or ()):
                transport = self._auxiliary_transports.get(port)
                if transport is not None and transport.connected:
                    active_port = port
                    break
            else:
                for port, transport in sorted(
                    (getattr(self, "_auxiliary_at_transports", {}) or {}).items()
                ):
                    if transport is not None and transport.connected:
                        active_port = port
                        break
        ownership_state = "no_domain_registry"
        if domain_active:
            if session is not None:
                ownership_state = str(getattr(session, "state", "") or "observed")
            else:
                ownership_state = "no_owned_session"
        return {
            "collector_session_ownership_authority": (
                "domain_registry" if domain_active else "runtime_registry"
            ),
            "collector_session_claim_entry_id": (
                self._callback_entry_id if domain_active else ""
            ),
            "collector_claimed_session_id": (
                str(getattr(session, "session_id", "") or "") if session else ""
            ),
            "collector_claimed_listener_port": (
                int(getattr(session, "listener_port", 0) or 0) if session else 0
            ),
            "collector_primary_tcp_port": self._tcp_port,
            "collector_active_listener_port": active_port,
            "collector_session_ownership_state": ownership_state,
        }

    def _owned_observed_session_protocol(self) -> str:
        """Return the effective observed session protocol for this entry (pure read).

        A trusted current live session reports its own protocol. During a
        transient gap (or a live conflict) the CONFIRMED wire binding is
        reported, so the coordinator never sees "" and never lets cloud-family
        bootstrap flip the profile to at_text mid-handover. Empty only before any
        live wire has ever been confirmed.
        """

        handle = self._live_session_handle()
        if not handle.conflict:
            if handle.uses_framed_wire:
                return "eybond_framed"
            if handle.uses_at_text_wire:
                return "at_text"
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.session_protocol
        return ""

    def _session_inventory_diagnostics(self) -> dict[str, object]:
        """Return passive callback-session inventory diagnostics."""

        summaries: list[dict[str, object]] = []
        seen_listeners: set[str] = set()
        for transport in self._payload_transports():
            listener_key = str(getattr(transport, "listener_key", "") or "")
            dedup_key = listener_key or f"transport:{id(transport)}"
            if dedup_key in seen_listeners:
                continue
            seen_listeners.add(dedup_key)
            diagnostics = transport.session_inventory_diagnostics()
            summaries.append(diagnostics)

        pending_count = sum(int(item.get("pending_session_count", 0) or 0) for item in summaries)
        recent_count = sum(int(item.get("recent_session_count", 0) or 0) for item in summaries)
        duplicate_peer_ips: set[str] = set()
        sessions: list[dict[str, object]] = []
        for item in summaries:
            for peer_ip in item.get("duplicate_peer_ips", []) or []:
                if isinstance(peer_ip, str) and peer_ip:
                    duplicate_peer_ips.add(peer_ip)
            for session in item.get("sessions", []) or []:
                if isinstance(session, dict):
                    sessions.append(dict(session))

        duplicate_peer_ip_count = len(duplicate_peer_ips)
        result: dict[str, object] = {
            "collector_callback_pending_session_count": pending_count,
            "collector_callback_recent_session_count": recent_count,
            "collector_callback_duplicate_peer_ip_count": duplicate_peer_ip_count,
            "collector_callback_duplicate_peer_ips": ", ".join(sorted(duplicate_peer_ips)),
            "collector_callback_session_inventory": sessions,
        }
        # A conflict is reported only on POSITIVE evidence (a
        # ``route_identity_mismatch`` state in the inventory). ``reconnecting`` is
        # reported only during a GENUINE handover -- a confirmed binding plus an
        # owned pending/new socket the registry can see (a fully offline
        # collector is idle, not endlessly reconnecting). A foreign identified
        # session on a shared listener is unresolved/unowned, never a conflict.
        binding = self._effective_wire_binding()
        live = self._live_session_handle()
        result.update(
            _callback_identity_status_values(
                pending_count=pending_count,
                recent_count=recent_count,
                duplicate_peer_ip_count=duplicate_peer_ip_count,
                sessions=sessions,
                expects_collector_identity=bool(str(self._collector_pn or "").strip()),
                owned_session_observed=bool(binding is not None or live.observed),
                handover_in_progress=self._handover_in_progress(),
            )
        )
        return result

    async def async_start(self) -> None:
        """Start the active link transport and its discovery loop."""

        await self._rebuild_if_server_ip_changed(reason="runtime_start")
        self._listener_status = "starting"
        try:
            await self._start_all_transports()
        except Exception as exc:
            self._started = False
            self._record_listener_error(exc)
            await self._stop_all_transports()
            raise

        self._started = True
        self._listener_status = "listening"
        self._listener_last_error = ""
        self._start_owned_session_monitor()
        # Phase 3: no continuous announcer. callback_on_demand sends a one-shot
        # trigger per connect attempt (async_try_connect); nothing runs here.
        await self._announcer.stop()

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Re-resolve the collector-facing host and rebuild listeners if it changed."""

        was_started = self._started
        changed = await self._rebuild_if_server_ip_changed(reason=reason)
        if not changed or not was_started:
            return changed

        self._listener_status = "starting"
        try:
            await self._start_all_transports()
        except Exception as exc:
            self._started = False
            self._record_listener_error(exc)
            await self._stop_all_transports()
            raise

        self._listener_status = "listening"
        self._listener_last_error = ""
        self._started = True
        await self._announcer.stop()
        return True

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
        """Rebuild transports when the resolved callback session profile changes."""

        normalized_protocol = str(collector_session_protocol or "").strip().lower()
        normalized_strategy = str(collector_identity_strategy or "").strip().lower()
        normalized_raw_bootstrap = str(collector_raw_passthrough_bootstrap or "").strip().lower()
        normalized_raw_frame = str(collector_raw_passthrough_frame_format or "").strip().lower()
        normalized_raw_min_interval_ms = max(
            0,
            int(collector_raw_passthrough_min_interval_ms or 0),
        )
        if (
            normalized_protocol == self._configured_collector_session_protocol
            and normalized_strategy == self._collector_identity_strategy
            and normalized_raw_bootstrap == self._collector_raw_passthrough_bootstrap
            and normalized_raw_frame == self._collector_raw_passthrough_frame_format
            and normalized_raw_min_interval_ms == self._collector_raw_passthrough_min_interval_ms
        ):
            return False

        # A live conflict (contradictory wire observation) blocks any profile
        # rebuild until new NON-contradictory positive live evidence appears.
        # Tearing transports down on top of a conflict would destroy a working
        # listener and act on evidence we have explicitly rejected. Preserve
        # the conflict until a positive wire observation resolves it.
        live = self._live_session_handle()
        if live.conflict:
            logger.debug(
                "Ignoring session-profile reconcile after %s: live session is in "
                "an unresolved wire conflict (%s); preserving the confirmed wire",
                reason or "collector_session_profile_change",
                live.conflict,
            )
            return False

        # Live session handover is NOT a profile change. The confirmed wire
        # binding is the authority: a reconcile request whose protocol
        # contradicts it is untrusted configuration, not wire evidence. Tearing
        # transports down for it caused framed->at_text->framed flapping and
        # needless re-onboarding. Rebuild only when no wire has been confirmed
        # yet or when the requested protocol is positively observed live.
        # Steady-state live wire changes go through set_negotiated_wire.
        binding = self._effective_wire_binding()
        confirmed_protocol = binding.session_protocol if binding is not None else ""
        if (
            confirmed_protocol
            and normalized_protocol
            and normalized_protocol != confirmed_protocol
            and self._raw_live_observed_protocol() != normalized_protocol
        ):
            logger.debug(
                "Ignoring session-profile reconcile after %s: requested protocol %s "
                "contradicts the confirmed live wire %s with no live evidence "
                "(transient reconnect handover, not a profile change)",
                reason or "collector_session_profile_change",
                normalized_protocol or "unknown",
                confirmed_protocol,
            )
            return False

        logger.warning(
            "EyeBond callback session profile changed after %s: protocol %s -> %s, identity %s -> %s, raw_bootstrap %s -> %s, raw_frame %s -> %s, raw_min_interval_ms %s -> %s; rebuilding transport",
            reason or "collector_session_profile_change",
            self._configured_collector_session_protocol or "unknown",
            normalized_protocol or "unknown",
            self._collector_identity_strategy or "unknown",
            normalized_strategy or "unknown",
            self._collector_raw_passthrough_bootstrap or "unknown",
            normalized_raw_bootstrap or "unknown",
            self._collector_raw_passthrough_frame_format or "unknown",
            normalized_raw_frame or "unknown",
            self._collector_raw_passthrough_min_interval_ms,
            normalized_raw_min_interval_ms,
        )
        was_started = self._started
        if was_started:
            await self._announcer.stop()
            await self._stop_all_transports()

        self._configured_collector_session_protocol = normalized_protocol
        self._collector_identity_strategy = normalized_strategy
        self._collector_raw_passthrough_bootstrap = normalized_raw_bootstrap
        self._collector_raw_passthrough_frame_format = normalized_raw_frame
        self._collector_raw_passthrough_min_interval_ms = normalized_raw_min_interval_ms
        self._rebuild_link(self._effective_server_ip)
        self._listener_rebind_count += 1

        if not was_started:
            return True

        self._listener_status = "starting"
        try:
            await self._start_all_transports()
        except Exception as exc:
            self._started = False
            self._record_listener_error(exc)
            await self._stop_all_transports()
            raise

        self._listener_status = "listening"
        self._listener_last_error = ""
        self._started = True
        await self._announcer.stop()
        return True

    async def async_stop(self) -> None:
        """Stop discovery and the active link transport."""

        await self._stop_owned_session_monitor()
        await self.async_stop_proxy_capture_route(force=True)
        await self.async_stop_shadow_learning_route(force=True)
        await self._announcer.stop()
        await self._stop_all_transports()
        self._started = False
        self._listener_status = "stopped"

    async def async_ensure_callback_listener(self, port: int) -> None:
        """Ensure one auxiliary callback listener is available for collector redirects."""

        requested_port = int(port or 0)
        if requested_port <= 0 or requested_port == self._tcp_port:
            return

        if requested_port not in self._auxiliary_listener_ports:
            self._auxiliary_listener_ports.add(requested_port)
            payload_transport, at_transport = self._build_transport_pair(
                self._listener_bind_host,
                requested_port,
            )
            self._auxiliary_transports[requested_port] = payload_transport
            self._auxiliary_at_transports[requested_port] = at_transport
            self._apply_collector_connection_watcher()

        try:
            await self._auxiliary_transports[requested_port].start()
            await self._auxiliary_at_transports[requested_port].start()
        except Exception as exc:
            self._record_listener_error(exc)
            raise

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        """Send one explicit UDP bootstrap probe without enabling background discovery."""

        target_ip = str(self._collector_ip or self._discovery_target or "").strip()
        if not target_ip:
            raise RuntimeError("collector_discovery_target_unavailable")

        advertised_port = int(port or self._configured_advertised_tcp_port or self._tcp_port)
        probe = await async_send_callback_trigger(
            bind_ip=self._effective_server_ip,
            advertised_server_ip=self.effective_advertised_server_ip,
            advertised_server_port=advertised_port,
            target_ip=target_ip,
            udp_port=self._udp_port,
            timeout=float(timeout),
            source="runtime_manual_trigger",
        )
        self._announcer.last_reply = probe.reply
        self._announcer.last_reply_from = probe.reply_from
        return {
            "status": "reply_received" if probe.reply else "probe_sent",
            "target_ip": probe.target_ip,
            "advertised_endpoint": (
                f"{self.effective_advertised_server_ip}:{advertised_port}"
            ),
            "message": probe.message,
            "reply": probe.reply,
            "reply_from": probe.reply_from,
            "local_port": probe.local_port,
        }

    def set_callback_ownership(
        self,
        registry: CallbackSessionRegistry | None,
        entry_id: str,
    ) -> None:
        """Inject the domain callback-session registry + this entry id.

        The domain registry (the one passive discovery feeds from EVERY shared
        listener in the process) is the single transport-ownership authority:
        when it is installed, the runtime resolves its owned live SessionHandle,
        the exact claimed session id, and the listener port the collector
        actually dialed from it -- under the REAL config entry id claimed at
        setup. It never reads listener internals; ownership stays PN/session
        based (peer IP is never a key). Without a domain registry (standalone
        hubs, unit tests) the runtime falls back to its own listener-scoped
        registry, so the two ownership paths are never active at the same time.
        """

        self._callback_ownership_registry = registry
        self._callback_entry_id = str(entry_id or "").strip()

    async def _send_callback_trigger(self) -> None:
        """Send exactly ONE UDP callback trigger for a callback_on_demand attempt.

        This is the one-shot replacement for the old continuous
        ``DiscoveryAnnouncer`` loop: one datagram per connect attempt, never a
        repeating N-second broadcast. The bounded wait for the inbound session
        happens after this returns. ``collector_ip``/``discovery_target`` are only
        the UDP target here, not identity.
        """

        target_ip = str(self._collector_ip or self._discovery_target or "").strip()
        if not target_ip:
            # No UDP target to poke; the collector may still dial in on its own,
            # and the bounded wait handles that.
            return
        advertised_port = int(self._configured_advertised_tcp_port or self._tcp_port)
        self._callback_trigger_count += 1
        try:
            probe = await async_send_callback_trigger(
                bind_ip=self._effective_server_ip,
                advertised_server_ip=self.effective_advertised_server_ip,
                advertised_server_port=advertised_port,
                target_ip=target_ip,
                udp_port=self._udp_port,
                source="runtime_callback_on_demand",
                timeout=_CALLBACK_TRIGGER_TIMEOUT,
            )
        except Exception as exc:  # pragma: no cover - defensive UDP send guard
            logger.debug("EyeBond one-shot callback trigger send failed: %s", exc)
            return
        self._announcer.last_reply = probe.reply
        self._announcer.last_reply_from = probe.reply_from

    def _callback_ownership_owner_for_pn(self, collector_pn: str) -> str:
        registry = self._callback_ownership_registry
        if registry is None:
            return ""
        try:
            return str(registry.owner_for_pn(collector_pn) or "")
        except Exception:
            return ""

    def _callback_listener_ready(self) -> bool:
        """Return whether a one-shot callback trigger has a ready listener."""

        return bool(self._started and self._listener_status == "listening")

    def _observed_foreign_session_exists(self, collector_pn: str) -> bool:
        """Return whether an inbound session with a NON-matching PN is observed."""

        from ..connection.session_registry import pn_is_same_identity

        for session in self._session_registry.observed_sessions():
            observed_pn = str(session.collector_pn or "").strip()
            if observed_pn and not pn_is_same_identity(collector_pn, observed_pn):
                return True
        return False

    def _classify_callback_failure(self) -> tuple[str, str]:
        """Classify why a callback_on_demand attempt did not connect (typed)."""

        if not self._started or self._listener_status == "error":
            detail = str(self._listener_last_error or "").strip()
            if detail:
                return CALLBACK_STATE_LISTENER_ERROR, detail
            return CALLBACK_STATE_LISTENER_UNAVAILABLE, self._listener_status
        collector_pn = str(self._collector_pn or "").strip()
        if collector_pn:
            if self._matching_live_session_exists(collector_pn):
                # Our collector's session is here but we did not connect: when a
                # DIFFERENT entry owns this identity in the domain registry, the
                # claim (not the network) is what blocked us -> typed conflict.
                owner = self._callback_ownership_owner_for_pn(collector_pn)
                if owner and self._callback_entry_id and owner != self._callback_entry_id:
                    return CALLBACK_STATE_CLAIMED_BY_OTHER, owner
                return CALLBACK_STATE_TIMEOUT, "session_not_yet_connected"
            if self._observed_foreign_session_exists(collector_pn):
                return CALLBACK_STATE_IDENTITY_MISMATCH, ""
        return CALLBACK_STATE_TIMEOUT, ""

    def _matching_live_session_exists(self, collector_pn: str) -> bool:
        """Return whether ANY live session of this durable identity is observed.

        Ownership-independent on purpose: classification must see the session
        even when a different entry owns it (that is exactly the
        claimed-by-other-entry outcome). Domain registry first (all shared
        listeners), else this runtime's own listener-scoped view.
        """

        from ..connection.session_registry import pn_is_same_identity

        registry = getattr(self, "_callback_ownership_registry", None)
        if registry is not None:
            try:
                sessions = registry.observed_sessions_per_socket()
            except Exception:
                sessions = ()
            for session in sessions:
                state = str(getattr(session, "state", "") or "").strip().lower()
                if state.startswith("closed"):
                    continue
                if pn_is_same_identity(
                    collector_pn, str(getattr(session, "collector_pn", "") or "")
                ):
                    return True
        # This runtime's own listeners are real observations too (the domain
        # registry may not cover every listener in test/standalone setups).
        for session in self._session_registry.observed_sessions():
            if pn_is_same_identity(
                collector_pn, str(session.collector_pn or "")
            ):
                return True
        return False

    def _record_callback_state(self, state: str, detail: str = "") -> None:
        self._last_callback_state = state
        self._last_callback_detail = str(detail or "")

    def _note_callback_failure(self) -> None:
        # Only meaningful for callback_on_demand; and only a real failure when we
        # are not connected (a heartbeat-only timeout keeps the CONNECTED state).
        if not self._reverse_discovery_enabled or self.connected:
            return
        state, detail = self._classify_callback_failure()
        self._record_callback_state(state, detail)

    def _note_callback_connected(self) -> None:
        if self._reverse_discovery_enabled and self.connected:
            self._record_callback_state(CALLBACK_STATE_CONNECTED)

    def callback_trigger_diagnostics(self) -> dict[str, object]:
        """Return typed callback_on_demand trigger/outcome diagnostics."""

        return {
            "collector_callback_on_demand": bool(self._reverse_discovery_enabled),
            "collector_callback_trigger_count": self._callback_trigger_count,
            "collector_callback_state": self._last_callback_state,
            "collector_callback_state_detail": self._last_callback_detail,
            "collector_callback_state_message": _callback_state_message(
                self._last_callback_state
            ),
        }

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        """Control whether UDP reverse discovery may redirect the collector."""

        was_enabled = self._reverse_discovery_enabled
        self._reverse_discovery_enabled = bool(enabled)
        if was_enabled and not self._reverse_discovery_enabled:
            announcer = self._announcer
            announcer.last_reply = ""
            announcer.last_reply_from = ""
            if getattr(announcer, "running", False):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
                loop.create_task(
                    announcer.stop(),
                    name="eybond_stop_reverse_discovery_announcer",
                )

    async def async_start_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        proxy_wire_mode: str = "transparent",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        masked_endpoint: str = "",
        restore_trigger_path=None,
        async_open_output=None,
        async_close_output=None,
    ) -> None:
        """Route one collector's callback connection through the in-process proxy."""

        if proxy_wire_mode != "transparent":
            raise ValueError("proxy_wire_mode_unsupported")
        if (
            type(expected_session_protocol) is not str
            or expected_session_protocol != expected_session_protocol.strip()
            or expected_session_protocol.lower() not in {"at_text", "eybond_framed"}
        ):
            raise ValueError("proxy_expected_session_protocol_invalid")

        normalized_owner_id = self._normalize_route_owner_id(
            mode="proxy_capture",
            owner_id=owner_id,
            entry_id=entry_id,
            output_path=output_path,
        )
        await self._acquire_route_lease(
            mode="proxy_capture",
            owner_id=normalized_owner_id,
            entry_id=entry_id,
            collector_ip=collector_ip,
            listen_port=listen_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
        )
        handler: InProcessProxyCaptureHandler | None = None
        route: SharedProxyCaptureRoute | None = None
        try:
            handler = InProcessProxyCaptureHandler(
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                output_path=output_path,
                expected_collector_pn=collector_pn,
                masked_endpoint=masked_endpoint,
                restore_trigger_path=restore_trigger_path,
                async_open_output=async_open_output,
                async_close_output=async_close_output,
            )
            await handler.start()
            route = SharedProxyCaptureRoute(
                host=self._listener_bind_host,
                port=int(listen_port),
                collector_ip=collector_ip,
                collector_pn=collector_pn,
                expected_session_protocol=expected_session_protocol,
                handler=handler.handle_client,
            )
            await route.start()
            self._proxy_capture_handler = handler
            self._proxy_capture_route = route
            await self._set_route_lease_state(normalized_owner_id, "running")
        except BaseException as exc:
            self._record_listener_error(exc)
            try:
                if route is not None:
                    await route.stop()
            finally:
                try:
                    if handler is not None:
                        await handler.stop()
                finally:
                    await self._release_route_lease(
                        mode="proxy_capture",
                        owner_id=normalized_owner_id,
                    )
            raise

    async def async_start_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        seed: ShadowLearningSeed,
    ) -> None:
        """Route one collector callback connection through the fail-closed shadow proxy."""

        if (
            type(expected_session_protocol) is not str
            or expected_session_protocol != expected_session_protocol.strip()
            or expected_session_protocol.lower() not in {"at_text", "eybond_framed"}
        ):
            raise ValueError("shadow_expected_session_protocol_invalid")

        normalized_owner_id = self._normalize_route_owner_id(
            mode="shadow_learning",
            owner_id=owner_id,
            entry_id=entry_id,
            output_path=output_path,
        )
        await self._acquire_route_lease(
            mode="shadow_learning",
            owner_id=normalized_owner_id,
            entry_id=entry_id,
            collector_ip=collector_ip,
            listen_port=listen_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
        )
        handler: InProcessFailClosedShadowProxyHandler | None = None
        route: SharedProxyCaptureRoute | None = None
        try:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                seed=seed,
                output_path=output_path,
            )
            await handler.start()
            route = SharedProxyCaptureRoute(
                host=self._listener_bind_host,
                port=int(listen_port),
                collector_ip=collector_ip,
                collector_pn=collector_pn,
                expected_session_protocol=expected_session_protocol,
                handler=handler.handle_client,
            )
            await route.start()
        except Exception as exc:
            self._record_listener_error(exc)
            try:
                if route is not None:
                    await route.stop()
            finally:
                try:
                    if handler is not None:
                        await handler.stop()
                finally:
                    await self._release_route_lease(
                        mode="shadow_learning",
                        owner_id=normalized_owner_id,
                    )
            raise
        self._shadow_learning_handler = handler
        self._shadow_learning_route = route
        await self._set_route_lease_state(normalized_owner_id, "running")

    async def async_stop_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process proxy route, if any."""

        await self._begin_route_stop(
            mode="proxy_capture",
            owner_id=owner_id,
            force=force,
        )
        route = self._proxy_capture_route
        handler = self._proxy_capture_handler
        self._proxy_capture_route = None
        self._proxy_capture_handler = None
        try:
            if route is not None:
                await route.stop()
            if handler is not None:
                await handler.stop()
        finally:
            await self._release_route_lease(
                mode="proxy_capture",
                owner_id=owner_id,
                force=force,
            )

    async def async_stop_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process shadow-learning route, if any."""

        await self._begin_route_stop(
            mode="shadow_learning",
            owner_id=owner_id,
            force=force,
        )
        route = self._shadow_learning_route
        handler = self._shadow_learning_handler
        self._shadow_learning_route = None
        self._shadow_learning_handler = None
        try:
            if route is not None:
                await route.stop()
            if handler is not None:
                await handler.stop()
        finally:
            await self._release_route_lease(
                mode="shadow_learning",
                owner_id=owner_id,
                force=force,
            )

    @property
    def route_lease(self) -> RouteLease | None:
        """Return the current exclusive callback-route lease."""

        return self._route_lease

    @staticmethod
    def _normalize_route_owner_id(
        *,
        mode: str,
        owner_id: str,
        entry_id: str,
        output_path: object,
    ) -> str:
        normalized = str(owner_id or "").strip()
        if normalized:
            return normalized
        return f"{mode}:{str(entry_id or '').strip()}:{str(output_path)}"

    async def _acquire_route_lease(
        self,
        *,
        mode: str,
        owner_id: str,
        entry_id: str,
        collector_ip: str,
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
    ) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is not None:
                raise RuntimeError(f"{current.mode}_route_running")
            if mode != "proxy_capture" and self.proxy_capture_route_running():
                raise RuntimeError("proxy_capture_route_running")
            if mode != "shadow_learning" and self.shadow_learning_route_running():
                raise RuntimeError("shadow_learning_route_running")
            self._route_lease = RouteLease(
                mode=mode,
                owner_id=owner_id,
                entry_id=str(entry_id or "").strip(),
                collector_ip=str(collector_ip or "").strip(),
                listen_port=int(listen_port),
                upstream_host=str(upstream_host or "").strip(),
                upstream_port=int(upstream_port),
                state="starting",
            )

    async def _set_route_lease_state(self, owner_id: str, state: str) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is None or current.owner_id != owner_id:
                raise RuntimeError("route_lease_owner_mismatch")
            self._route_lease = replace(current, state=str(state or "").strip())

    async def _begin_route_stop(
        self,
        *,
        mode: str,
        owner_id: str,
        force: bool,
    ) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is None:
                return
            if current.mode != mode:
                if force:
                    return
                raise RuntimeError(f"{current.mode}_route_running")
            normalized_owner_id = str(owner_id or "").strip()
            if normalized_owner_id and normalized_owner_id != current.owner_id and not force:
                raise RuntimeError("route_lease_owner_mismatch")
            self._route_lease = replace(current, state="stopping")

    async def _release_route_lease(
        self,
        *,
        mode: str,
        owner_id: str,
        force: bool = False,
    ) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is None or current.mode != mode:
                return
            normalized_owner_id = str(owner_id or "").strip()
            if normalized_owner_id and normalized_owner_id != current.owner_id and not force:
                raise RuntimeError("route_lease_owner_mismatch")
            self._route_lease = None

    def proxy_capture_route_running(self) -> bool:
        """Return whether an in-process proxy route is currently active."""

        handler = self._proxy_capture_handler
        return bool(handler is not None and handler.running)

    def shadow_learning_route_running(self) -> bool:
        """Return whether an in-process shadow-learning route is currently active."""

        handler = self._shadow_learning_handler
        return bool(handler is not None and handler.running)

    def shadow_learning_route_ready(self) -> bool:
        """Return whether the active shadow route has collector and upstream connectivity."""

        handler = self._shadow_learning_handler
        return bool(handler is not None and handler.ready)

    def shadow_learning_route_status(self) -> dict[str, object]:
        """Return status details for the active shadow route."""

        handler = self._shadow_learning_handler
        if handler is None:
            return {
                "running": False,
                "collector_connected": False,
                "collector_protocol_ingress": False,
                "route_protocol_activity": False,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }
        return dict(handler.status())

    def shadow_learning_write_observations(
        self,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return observations from the active route without exposing its handler."""

        handler = self._shadow_learning_handler
        if handler is None:
            return ()
        return tuple(handler.write_observations)

    def shadow_learning_observation_cursor(self) -> int:
        """Return the active route's observation tail without exposing its handler."""

        handler = self._shadow_learning_handler
        if handler is None:
            return 0
        return handler.observation_cursor()

    def shadow_learning_observations_since(
        self,
        cursor: int,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return observations from one validated active-route cursor."""

        if type(cursor) is not int or cursor < 0:
            raise ValueError("shadow_learning_observation_cursor_invalid")
        handler = self._shadow_learning_handler
        if handler is None:
            return ()
        return tuple(handler.observations_since(cursor))

    async def async_wait_for_shadow_learning_observations_since(
        self,
        cursor: int,
        *,
        timeout_seconds: float,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Wait for active-route observations without exposing its handler."""

        if type(cursor) is not int or cursor < 0:
            raise ValueError("shadow_learning_observation_cursor_invalid")
        if (
            type(timeout_seconds) not in (int, float)
            or type(timeout_seconds) is bool
            or timeout_seconds < 0
        ):
            raise ValueError("shadow_learning_observation_timeout_invalid")
        handler = self._shadow_learning_handler
        if handler is None:
            return ()
        return tuple(
            await handler.wait_for_observations_since(
                cursor,
                timeout_seconds=float(timeout_seconds),
            )
        )

    def shadow_learning_read_map_snapshot(self) -> dict[str, object]:
        """Return a detached read-map snapshot from the active route."""

        handler = self._shadow_learning_handler
        if handler is None:
            return {}
        read_map = handler.read_map
        return dict(read_map) if isinstance(read_map, dict) else {}

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        """Drop current collector sockets without restarting discovery."""

        logger.warning(
            "Disconnecting collector runtime connections after %s remote=%s configured_collector_ip=%s",
            reason or "runtime_disconnect",
            self.collector_info.remote_ip or "unknown",
            self._collector_ip or "unknown",
        )
        await self._disconnect_all_transports()

    async def _async_follow_owned_session_listener(self) -> None:
        """Attach a transport facade to the listener the owned session lives on.

        The primary configured tcp_port stays the callback target/fallback, but
        it must not limit ownership of an already-accepted PN session: when the
        domain registry shows this entry's collector on a DIFFERENT shared
        listener port, bring up the auxiliary facade for that port so the claim
        can activate exactly that socket. The port comes ONLY from a live
        observed owned session (never from the endpoint hostname, peer IP, or
        collector type), and the facade attaches to the already-running shared
        listener -- it does not open arbitrary ports.
        """

        session = self._owned_domain_session()
        if session is None:
            return
        port = int(getattr(session, "listener_port", 0) or 0)
        if port <= 0 or port == self._tcp_port:
            return
        already_prepared = port in self._auxiliary_listener_ports
        try:
            await self.async_ensure_callback_listener(port)
        except Exception as exc:
            logger.debug(
                "Could not attach facade to owned-session listener %s: %s",
                port,
                exc,
            )
            return
        if not already_prepared:
            logger.info(
                "Following owned collector session %s to listener port %s (primary %s)",
                str(getattr(session, "session_id", "") or "unknown"),
                port,
                self._tcp_port,
            )
            # New facades must receive the negotiated wire + exact claim target.
            self._apply_live_wire_to_transports()

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        """Try to ensure a live collector connection without raising on timeout."""

        if self._route_lease is not None and not self.connected:
            # Proxy/shadow owns the post-redirect reconnect. Runtime must not
            # send a callback trigger concurrently and create a second framed
            # HA session that races the new cloud session for the shared
            # listener.
            return False

        # Transport ownership end-to-end: if the domain registry shows this
        # entry's owned session on another already-running shared listener,
        # attach the facade there BEFORE waiting, so inbound entries connect to
        # the socket the collector actually opened (no UDP involved).
        await self._async_follow_owned_session_listener()
        self._apply_live_wire_to_transports()

        # A registry-owned exact session is already causally certified for this
        # entry. It may still be parked while the freshly-created transport
        # facade starts, so ``self.connected`` can be false even though the
        # physical socket is present and claimable. Activate/wait for that exact
        # socket BEFORE considering a new callback trigger. If it cannot be used,
        # fail this attempt closed; a later attempt may trigger only after the
        # registry no longer exposes the stale owned session. Never overwrite a
        # proven handoff with another set>server sequence merely because a
        # co-located foreign socket delayed facade activation.
        if (
            self._reverse_discovery_enabled
            and not self.connected
            and self._claimed_session_id()
        ):
            return await self._async_await_callback_session(
                timeout=timeout,
                require_heartbeat=require_heartbeat,
            )

        # callback_on_demand: send exactly ONE UDP callback trigger for this
        # attempt, then bounded-wait for the inbound session. inbound entries
        # have _reverse_discovery_enabled=False and never reach this, so they
        # never send a UDP trigger -- they only claim/wait for an already-inbound
        # session.
        if self._reverse_discovery_enabled and not self.connected:
            if not self._callback_listener_ready():
                self._note_callback_failure()
                return False
            # THE causal window of a callback_on_demand connect is trigger ->
            # session, not the datagram alone: the collector dials back seconds
            # later. Take the shared lease before the trigger and hold it across
            # the wait below, so no other attempt can snapshot a baseline while
            # OUR late session is still in flight and adopt it as its own answer.
            # Refusing the send while somebody else owns causality is not enough:
            # a datagram sent just before their lease still produces a session
            # inside their window.
            return await self._async_callback_connect_within_causality(
                timeout=timeout, require_heartbeat=require_heartbeat
            )

        return await self._async_await_callback_session(
            timeout=timeout, require_heartbeat=require_heartbeat
        )

    async def async_activate_claimed_session(
        self,
        *,
        expected_session_id: str,
        timeout: float,
    ) -> bool:
        """Activate exactly one registry-certified callback socket, without UDP.

        Strategy-recovery has already established causality, strong identity and
        permanent ownership.  This boundary performs only the missing transport
        handoff: attach the runtime facade to the listener, route the exact
        claimed session into it and verify that the same claim is still active.
        It deliberately does not call :meth:`async_try_connect`, because losing
        the claim must fail closed instead of silently sending a new set>server.
        """

        expected = str(expected_session_id or "").strip()
        if not expected:
            return False
        if self._domain_ownership_active():
            registry = self._callback_ownership_registry
            if (
                registry.claimed_session_id(self._callback_entry_id) != expected
                or registry.session_handle_for_owned_session(
                    self._callback_entry_id,
                    expected,
                )
                is None
            ):
                return False
        elif self._claimed_session_id() != expected:
            return False

        self._activation_session_id = expected
        try:
            await self._async_follow_owned_session_listener()
            self._apply_live_wire_to_transports()
            if self._claimed_session_id() != expected:
                return False
            activated = await self._async_await_callback_session(
                timeout=max(0.0, float(timeout)),
                require_heartbeat=False,
            )
            return bool(
                activated
                and self.connected
                and self._claimed_session_id() == expected
            )
        finally:
            self._activation_session_id = ""
            # Restore the ordinary dynamic provider after the one exact
            # handoff has either completed or failed.
            self._apply_live_wire_to_transports()

    def _callback_attempt_seq(self) -> str:
        """A unique id for ONE runtime callback attempt.

        Deliberately opaque: never a peer IP, hostname, endpoint or PN. It only
        has to be unique per attempt so the coordinator can tell attempts apart.
        """

        self._callback_attempt_counter = getattr(self, "_callback_attempt_counter", 0) + 1
        return f"{id(self):x}:{self._callback_attempt_counter}"

    async def _async_callback_connect_within_causality(
        self, *, timeout: float, require_heartbeat: bool
    ) -> bool:
        """Own causality for ONE runtime callback_on_demand connect attempt.

        The lease is released only once this attempt reaches its terminal point
        (connected, or the bounded wait gave up), so a late session is always
        attributable to it and never to whoever ran next.
        """

        from ..connection.callback_ledger import (
            CallbackCausalityBusyError,
            get_callback_trigger_ledger,
        )

        attempt_id = f"runtime_callback:{self._callback_attempt_seq()}"
        try:
            async with get_callback_trigger_ledger().causality_lease(
                attempt_id, timeout=_RUNTIME_CAUSALITY_LEASE_WAIT
            ):
                await self._send_callback_trigger()
                return await self._async_await_callback_session(
                    timeout=timeout, require_heartbeat=require_heartbeat
                )
        except CallbackCausalityBusyError:
            # Someone else owns causality (an onboarding attempt, an inbound
            # verification). We did NOT trigger, so this is not a collector
            # failure: stay silent and let Home Assistant retry.
            logger.debug("Runtime callback deferred: causality is owned elsewhere")
            return False

    async def _async_await_callback_session(
        self, *, timeout: float, require_heartbeat: bool
    ) -> bool:
        """Bounded wait for the session, then adopt it. No trigger is sent here."""

        if self._inverter_forward_adapter() == ADAPTER_INVERTER_RAW_PASSTHROUGH:
            if self.active_collector_at_transport is None:
                ok = await self._async_wait_for_at_connection(timeout=timeout)
                if not ok:
                    self._note_callback_failure()
                    return False

            await self._announcer.stop()
            self._note_callback_connected()
            # A freshly-connected session is positive live evidence: adopt its
            # trusted wire now so the confirmed binding survives the next gap.
            self._adopt_trusted_live_binding()
            return self.connected

        if not self.connected:
            ok = await self._async_wait_for_payload_connection(timeout=timeout)
            if not ok:
                self._note_callback_failure()
                return False

        # The callback session itself connected; heartbeat is a separate concern.
        self._note_callback_connected()
        # A freshly-connected session is positive live evidence: adopt its
        # trusted wire now so the confirmed binding survives the next gap.
        self._adopt_trusted_live_binding()

        if require_heartbeat:
            heartbeat_ok = await self._async_wait_for_payload_heartbeat(timeout=min(timeout, 1.5))
            if not heartbeat_ok:
                return False

        await self._announcer.stop()
        return self.connected

    async def async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        """Ensure a live collector connection or raise a standard transport error."""

        ok = await self.async_try_connect(
            timeout=timeout,
            require_heartbeat=require_heartbeat,
        )
        if not ok:
            if require_heartbeat and self.connected:
                raise ConnectionError("collector_heartbeat_timeout")
            raise ConnectionError("collector_not_connected")

    async def async_reset_connection(self, *, reason: str = "") -> None:
        collector = self.collector_info
        logger.warning(
            "Resetting collector runtime connection after %s remote=%s configured_collector_ip=%s collector_pn=%s heartbeat_devcode=%s last_devcode=%s",
            reason or "runtime_error",
            collector.remote_ip or "unknown",
            self._collector_ip or "unknown",
            collector.collector_pn or "unknown",
            f"0x{collector.heartbeat_devcode:04X}" if collector.heartbeat_devcode is not None else "unknown",
            f"0x{collector.last_devcode:04X}" if collector.last_devcode is not None else "unknown",
        )
        await self._disconnect_all_transports()
        # Phase 3: no continuous announcer restart here. The next connect attempt
        # (async_try_connect) sends a single one-shot callback trigger.

    def _payload_transports(self) -> tuple[CollectorTransport, ...]:
        transports: list[CollectorTransport] = [self._transport]
        transports.extend(
            self._auxiliary_transports[port]
            for port in sorted(self._auxiliary_listener_ports)
            if port in self._auxiliary_transports
        )
        return tuple(transports)

    def _at_transports(self) -> tuple[CollectorAtTransport, ...]:
        transports: list[CollectorAtTransport] = [self._at_transport]
        transports.extend(
            self._auxiliary_at_transports[port]
            for port in sorted(self._auxiliary_listener_ports)
            if port in self._auxiliary_at_transports
        )
        return tuple(transports)

    def _selected_connected_remote_ip(self) -> tuple[str, bool]:
        if self._collector_pn:
            return "", False
        if self._collector_ip:
            return self._collector_ip, False

        payload_ips = {
            str(transport.collector_info.remote_ip or "").strip()
            for transport in self._payload_transports()
            if transport.connected and str(transport.collector_info.remote_ip or "").strip()
        }
        at_ips = {
            str(transport.collector_info.remote_ip or "").strip()
            for transport in self._at_transports()
            if transport.connected and str(transport.collector_info.remote_ip or "").strip()
        }

        if len(payload_ips) > 1 or len(at_ips) > 1:
            return "", True
        if payload_ips and at_ips:
            if payload_ips == at_ips:
                return next(iter(payload_ips)), False
            return "", True
        if payload_ips:
            return next(iter(payload_ips)), False
        if at_ips:
            return next(iter(at_ips)), False
        return "", False

    def _connected_payload_transport(self) -> CollectorTransport | None:
        selected_remote_ip, ambiguous = self._selected_connected_remote_ip()
        if ambiguous:
            return None

        connected: list[CollectorTransport] = []
        for transport in self._payload_transports():
            if not transport.connected:
                continue
            remote_ip = str(transport.collector_info.remote_ip or "").strip()
            if selected_remote_ip and remote_ip and remote_ip != selected_remote_ip:
                continue
            connected.append(transport)
            if transport.collector_info.heartbeat_fresh:
                return transport
        return connected[0] if connected else None

    def _connected_at_transport(self) -> CollectorAtTransport | None:
        selected_remote_ip, ambiguous = self._selected_connected_remote_ip()
        if ambiguous:
            return None

        for transport in self._at_transports():
            if not transport.connected:
                continue
            remote_ip = str(transport.collector_info.remote_ip or "").strip()
            if selected_remote_ip and remote_ip and remote_ip != selected_remote_ip:
                continue
            return transport
        return None

    def _apply_confirmed_session_protocol_to_transports(self) -> None:
        """Push the durable confirmed session protocol to every transport owner.

        This is the DURABLE probe-permission channel, DISTINCT from
        ``set_negotiated_wire`` (the live-wire activation). It dynamically
        (un)registers the confirmed listener protocol owner on the running
        primary AND auxiliary transports so a same-process SILENT reconnect can
        be identity-probed WITHOUT an HA restart and WITHOUT a listener rebuild.
        The value is the CONFIRMED protocol only (live handle or PN-validated
        confirmed binding); "" clears the owner (binding dropped on a durable-PN
        change). The inferred/expected cloud-family protocol is NEVER passed
        here. Called by the single binding writer ``_adopt_trusted_live_binding``
        on every adopt/clear so the transport owner always mirrors the binding.
        """

        confirmed_protocol = self._confirmed_session_protocol()
        for transport in (*self._payload_transports(), *self._at_transports()):
            if callable(getattr(transport, "set_confirmed_session_protocol", None)):
                transport.set_confirmed_session_protocol(confirmed_protocol)

    def _adopt_trusted_live_binding(self) -> None:
        """Adopt the current trusted live wire as the confirmed binding.

        This is the ONLY writer of ``_confirmed_wire_binding``. It is an explicit
        lifecycle step (called from the connect path and the owned-session
        monitor), never a side effect of a diagnostics/accessor read. A trusted
        observed handle of the same durable identity is adopted as an immutable
        ``ConfirmedWireBinding`` (durable wire facts only, no socket metadata).
        A conflict or an unobserved handle changes nothing. A stale binding for a
        now-different durable identity is dropped.
        """

        durable_pn = str(self._collector_pn or "").strip()
        handle = self._live_session_handle()
        live_pn = str(getattr(handle, "collector_pn", "") or "").strip()
        # Invariant: a confirmed binding requires a durable ENTRY PN AND a live
        # session PN of the SAME short/full identity. An unidentified live
        # session, an entry without a durable PN, or a foreign live identity can
        # never create or overwrite the binding. The stored PN is the preferred
        # (fuller) of the two, so a later short/full enrichment stays one
        # identity.
        if durable_pn and live_pn and pn_is_same_identity(durable_pn, live_pn):
            preferred_pn = reconcile_pn(durable_pn, live_pn)
            binding = ConfirmedWireBinding.from_handle(handle, collector_pn=preferred_pn)
            if binding is not None:
                self._confirmed_wire_binding = binding
                # A newly confirmed live wire is durable probe permission: push it
                # to the running transports so a later silent same-PN reconnect can
                # be identity-probed without a rebuild.
                self._apply_confirmed_session_protocol_to_transports()
                return
        # No positive evidence to adopt. Never overwrite an existing binding with
        # a foreign/absent identity; only drop one left over from a rebind to a
        # genuinely different durable identity.
        existing = getattr(self, "_confirmed_wire_binding", None)
        if existing is not None and durable_pn:
            if not existing.collector_pn or not pn_is_same_identity(
                durable_pn, existing.collector_pn
            ):
                self._confirmed_wire_binding = None
        # Whether the binding was just dropped (durable-PN change) or simply
        # unchanged, re-assert the confirmed owner on the transports so a cleared
        # binding also unregisters the listener protocol owner.
        self._apply_confirmed_session_protocol_to_transports()

    def _seed_confirmed_wire_binding_from_evidence(
        self,
        evidence: "ConfirmedSessionProtocolEvidence | None",
    ) -> None:
        """Seed the confirmed binding from confirmed-live evidence -- fail-closed.

        This is a TRUST BOUNDARY, not a "validated by construction" shortcut. The
        object is re-validated by ``ConfirmedSessionProtocolEvidence.coerce``,
        which rejects anything that is not a genuine evidence instance (a
        duck-typed ``SimpleNamespace`` never passes) AND re-checks every
        invariant against the entry PN -- ``source == live_session``, a known
        confirmed wire protocol, a non-empty durable PN, and the same short/full
        identity. A forged instance built via the raw dataclass constructor
        (bad source / unknown protocol / empty PN) therefore seeds nothing. Any
        untrusted input yields no binding (never an exception). A live
        SessionHandle still overrides whatever is seeded here.
        """

        validated = ConfirmedSessionProtocolEvidence.coerce(
            evidence, entry_pn=self._collector_pn
        )
        if validated is None:
            return
        # ``validated.collector_pn`` is already reconciled to the fuller identity.
        seeded = ConfirmedWireBinding.from_confirmed_protocol(
            collector_pn=validated.collector_pn,
            session_protocol=validated.protocol,
        )
        if seeded is not None:
            self._confirmed_wire_binding = seeded

    def _effective_wire_binding(self) -> ConfirmedWireBinding | None:
        """Return the confirmed wire binding for this collector (pure read).

        Never mutates runtime state. Returns ``None`` when nothing has been
        confirmed yet, or when the stored binding belongs to a now-different
        durable identity (defensively ignored without rewriting the field).
        """

        binding = getattr(self, "_confirmed_wire_binding", None)
        if binding is None:
            return None
        # A binding must carry a durable PN (the adoption invariant guarantees
        # this); a PN-less binding is never trusted.
        if not str(getattr(binding, "collector_pn", "") or "").strip():
            return None
        collector_pn = str(self._collector_pn or "").strip()
        if (
            collector_pn
            and binding.collector_pn
            and not pn_is_same_identity(collector_pn, binding.collector_pn)
        ):
            return None
        return binding

    def _confirmed_session_protocol(self) -> str:
        """Return the confirmed session protocol, else an empty string.

        Confirmed evidence only: a trusted live-observed wire (strongest) or the
        confirmed wire binding (live-derived this session, or persisted
        confirmed-live seeded for the same durable PN). This is what may register
        a listener protocol owner and seed a bootstrap adapter. No preliminary
        cloud-family protocol exists at this boundary.
        Construction-safe: falls back to the pure binding read if the live
        handle cannot be resolved yet (transports not built).
        """

        try:
            handle = self._live_session_handle()
        except Exception:
            handle = None
        if handle is not None and not handle.conflict and handle.observed:
            if handle.uses_framed_wire:
                return "eybond_framed"
            if handle.uses_at_text_wire:
                return "at_text"
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.session_protocol
        return ""

    def has_confirmed_wire_binding(self) -> bool:
        """Return whether a live wire has ever been confirmed for this collector.

        Once true, the live session is the transport authority: cloud-family /
        configuration metadata must not drive a
        steady-state destructive transport rebuild.
        """

        return self._effective_wire_binding() is not None

    @property
    def confirmed_wire_binding(self) -> ConfirmedWireBinding | None:
        """Return the confirmed wire binding (pure read), or None."""

        return self._effective_wire_binding()

    def _has_owned_pending_session(self) -> bool:
        """Return whether the registry currently sees an owned (pending/new) socket.

        Lifecycle evidence for a handover: a socket for THIS entry's identity is
        present but has not yet become a trusted live handle. A fully absent
        socket is offline, not a handover -- there is no timeout involved. The
        domain path uses the registry's owned-session location (present even for
        a parked/identified socket); the fallback path uses the claimed handle's
        session id.
        """

        if self._domain_ownership_active():
            return self._owned_domain_session() is not None
        handle = self._live_session_handle()
        return bool(str(getattr(handle, "session_id", "") or "").strip())

    def _handover_in_progress(self) -> bool:
        """Return whether an owned session handover is genuinely in progress.

        True only when ALL hold: a confirmed binding exists; the current live
        handle is not yet a trusted (observed) session and is not in conflict;
        and the registry actually sees an owned pending/new socket for this
        entry. A confirmed binding with NO owned socket is offline/idle, never an
        endless ``reconnecting``.
        """

        if self._effective_wire_binding() is None:
            return False
        live = self._live_session_handle()
        if live.observed or live.conflict:
            return False
        return self._has_owned_pending_session()

    def _raw_live_observed_protocol(self) -> str:
        """Return the CURRENTLY observed live protocol (no binding), else ""."""

        handle = self._live_session_handle()
        if handle.uses_framed_wire:
            return "eybond_framed"
        if handle.uses_at_text_wire:
            return "at_text"
        return ""

    def _inverter_forward_adapter(self) -> str:
        """Return the adapter that must carry inverter payloads.

        Authority order, all pure reads:
        - a live ``conflict`` fails closed to no adapter (contradictory wire);
        - a trusted ``observed`` live session uses its own adapter;
        - a transient gap uses the CONFIRMED wire binding (a same-collector
          handover never downgrades the wire);
        - with NO live and NO confirmed evidence the result is ADAPTER_NONE
          (fail-closed). The inferred/persisted EXPECTED protocol is NOT an
          adapter fallback -- there is no "unknown -> framed_fc4" default, so a
          connected socket whose wire has never been observed or confirmed is
          never forwarded.
        """

        handle = self._live_session_handle()
        if handle.conflict:
            return ADAPTER_NONE
        if handle.observed:
            return handle.inverter_forward_adapter
        binding = self._effective_wire_binding()
        if binding is not None:
            # Confirmed evidence: a live-derived confirmed wire (reconnect gap) or
            # a PN-validated persisted confirmed-live protocol.
            return binding.inverter_forward_adapter
        # No confirmed evidence. FAIL CLOSED. The inverter adapter is never
        # chosen from cloud-family, endpoint, driver, or unproven persisted data,
        # and there is no legacy "unknown -> framed_fc4" fallback: a connected
        # socket without an observed/confirmed wire is not safe to forward.
        return ADAPTER_NONE

    def _uses_at_text_payload(self) -> bool:
        """Compatibility helper for tests/diagnostics."""

        return self._inverter_forward_adapter() == ADAPTER_INVERTER_RAW_PASSTHROUGH

    def _collector_management_selection(self) -> tuple[str, str]:
        """Return ``(adapter_id, provenance)`` -- the SINGLE management resolver.

        The one place adapter id and provenance are decided together, so they can
        never disagree in diagnostics. Authority order (collector-management role):

        * live ``conflict`` -> ``(none, "conflict")`` -- a contradictory wire fails
          closed and the stale confirmed binding is NOT reported as effective;
        * trusted ``observed`` live session -> ``(its adapter, "live")``;
        * transient gap with a CONFIRMED binding -> ``(binding adapter, "confirmed_binding")``;
        * no live and no confirmed evidence -> ``(none, "unavailable")``.

        The inferred/expected protocol never participates.
        """

        handle = self._live_session_handle()
        if handle.conflict:
            return ADAPTER_NONE, "conflict"
        if handle.observed:
            return handle.collector_management_adapter, "live"
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.collector_management_adapter, "confirmed_binding"
        return ADAPTER_NONE, "unavailable"

    def collector_management_adapter_id(self) -> str:
        """Return the negotiated collector-management adapter id (the single switch)."""

        return self._collector_management_selection()[0]

    def collector_management_adapter_provenance(self) -> str:
        """Return the management-adapter selection provenance (see the resolver)."""

        return self._collector_management_selection()[1]

    def _collector_bootstrap_claimable(self) -> bool:
        """Return whether a pre-heartbeat collector-only bootstrap read is allowed.

        A collector-only ESP produces no inverter heartbeat until it has an
        inverter, so its identity (FC=2 param 6) must be readable before a live
        payload wire is observed. That read is allowed ONLY on a socket the entry
        already OWNS through the registry -- i.e. a registry-claimed session id.
        The configured collector target is a connection address, NOT ownership
        evidence: it does not prove any pending socket belongs to this entry, and
        two PN-less entries behind one NAT/public target would resolve to the same
        socket, so it must never by itself yield a metadata route. The claimed
        session id is PN/identity-scoped by the registry, so a foreign strong PN
        is never claimed and an ambiguous PN-less collector yields no claim (and
        therefore no route).
        """

        if self.connected:
            return False
        return bool(str(self._claimed_session_id() or "").strip())

    def collector_metadata_routes(self) -> CollectorMetadataRouteSet:
        """Return the metadata channel routes for this entry's owned collector.

        Public route-authority facade for collector-metadata TELEMETRY. It is the
        ONE place framed/AT metadata channels are selected, built from trusted,
        owned session evidence -- the live observed ``SessionHandle`` (or the
        ``ConfirmedWireBinding`` during a handover gap), plus registry ownership
        for the collector-only bootstrap. It never routes by collector kind,
        cloud family, hostname, peer IP, driver key, or an inferred/persisted
        protocol without confirmed evidence.

        Dual-channel: a framed base metadata channel and an AT supplemental
        metadata channel can be routed simultaneously. The bootstrap channel is
        offered only when no framed metadata channel is available (a framed wire
        reads param 6 in its normal sweep).
        """

        generation = self._owned_session_generation
        session_id = self._claimed_session_id()
        handle = self._live_session_handle()
        if handle.conflict:
            # A contradictory live wire fails closed: no metadata channels, and
            # the stale confirmed binding is NOT reported as effective.
            return CollectorMetadataRouteSet(
                generation=generation,
                session_id=session_id,
                provenance="conflict",
            )

        if handle.observed:
            provenance = "live"
        elif self._effective_wire_binding() is not None:
            provenance = "confirmed_binding"
        else:
            provenance = "unavailable"

        # ``active_transport`` is the connected framed payload transport (None on
        # an at_text/raw wire, since the payload rides the AT session there);
        # ``active_collector_at_transport`` is the connected AT transport. Their
        # presence already encodes the negotiated wire via the fail-closed
        # inverter-forward adapter, so metadata channel selection follows the wire
        # without re-deriving it from any discriminator.
        framed_transport = self.active_transport
        at_transport = self.active_collector_at_transport

        if (
            framed_transport is None
            and at_transport is None
            and self._collector_bootstrap_claimable()
        ):
            # Pre-heartbeat collector-only bootstrap: route the AT/bootstrap read
            # to the claimable raw AT transport (it carries the registry-mediated
            # pending-socket claim internally).
            at_transport = self._at_transport
            if provenance == "unavailable":
                provenance = "bootstrap_claimable"

        return build_collector_metadata_routes(
            framed_transport=framed_transport,
            at_transport=at_transport,
            bootstrap_transport=at_transport,
            generation=generation,
            session_id=session_id,
            provenance=provenance,
            # Durable collector identity (PN) keys the service cache/health; a
            # short PN later enriched to the full PN is the SAME identity. Never
            # the peer IP.
            identity=str(self._collector_pn or "").strip(),
        )

    @property
    def session_handle(self) -> SessionHandle:
        """Return the negotiated live session handle for this entry's collector."""

        return self._live_session_handle()

    def _domain_ownership_active(self) -> bool:
        """Return whether the domain registry is the ownership authority here."""

        return (
            getattr(self, "_callback_ownership_registry", None) is not None
            and bool(getattr(self, "_callback_entry_id", ""))
        )

    def _owned_domain_session(self):
        """Return this entry's best owned live session from the DOMAIN registry.

        The domain registry observes every shared listener in the process, so
        this is what lets an entry whose primary tcp_port is e.g. 8899 find its
        own collector dialing the 18899 listener. Location (session_id +
        listener_port) is meaningful even for a parked/identified socket that is
        still waiting to be claimed; closed / route-identity-mismatch / foreign-
        owned sockets never qualify (registry-side filtering).
        """

        if not self._domain_ownership_active():
            return None
        try:
            exact_session_id = str(
                getattr(self, "_activation_session_id", "") or ""
            ).strip()
            if exact_session_id:
                registry = self._callback_ownership_registry
                if (
                    registry.claimed_session_id(self._callback_entry_id)
                    != exact_session_id
                    or registry.session_handle_for_owned_session(
                        self._callback_entry_id,
                        exact_session_id,
                    )
                    is None
                ):
                    return None
                return next(
                    (
                        session
                        for session in registry.observed_sessions_per_socket()
                        if session.session_id == exact_session_id
                    ),
                    None,
                )
            return self._callback_ownership_registry.owned_session_location(
                self._callback_entry_id
            )
        except Exception:
            logger.debug("Domain session location lookup failed", exc_info=True)
            return None

    def _claimed_session_id(self) -> str:
        """Return the registry-claimed session id for this entry's owned session.

        Domain-registry path: the exact session id of the entry-owned observed
        session -- including a parked/identified socket that has not been
        activated yet (activation is exactly what the claim is for). Fallback
        (no domain registry): only a trusted observed-wire session is returned;
        a route-identity mismatch / not-yet-routed session negotiates to an
        unknown wire and is never handed to the transport as the claim target.
        """

        if self._domain_ownership_active():
            session = self._owned_domain_session()
            return str(getattr(session, "session_id", "") or "") if session else ""
        handle = self._live_session_handle()
        return handle.session_id if handle.observed else ""

    def _effective_transport_wire(self) -> str:
        """Return the wire selector to push down: live if observed, else confirmed.

        A genuine live wire is applied in-place (this is how a real framed<->
        at_text change is adopted -- no destructive rebuild). During a transient
        gap the confirmed binding's wire is kept so the AT/framed activation
        stays ready for the reconnecting socket instead of being cleared.
        """

        handle = self._live_session_handle()
        if not handle.conflict and handle.observed:
            return handle.transport_wire
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.transport_wire
        return handle.transport_wire

    def _apply_live_wire_to_transports(self) -> None:
        """Push the effective wire + claim target down to the transports.

        This is an explicit lifecycle path (called every connect attempt), so it
        also ADOPTS a freshly-observed trusted session as the confirmed wire
        binding. It makes the runtime the single source of truth for (a) AT-vs-
        framed activation inside the transport (live if observed, else the
        confirmed binding) and (b) which inbound socket the transport claims (the
        registry-chosen session id; empty during a gap so no stale socket is
        claimed).
        """

        # ``_adopt_trusted_live_binding`` is the single writer of the confirmed
        # binding and now re-asserts the confirmed session-protocol owner on the
        # transports itself (operation (b) below), so it is applied here too.
        self._adopt_trusted_live_binding()
        wire = self._effective_transport_wire()
        for transport in self._at_transports():
            if callable(getattr(transport, "set_negotiated_wire", None)):
                transport.set_negotiated_wire(wire)
        # Two DISTINCT operations, applied to primary AND auxiliary transports:
        # (a) set_negotiated_wire(live wire) above -> AT/framed activation now;
        # (b) set_confirmed_session_protocol(durable probe permission) via
        #     ``_apply_confirmed_session_protocol_to_transports`` (invoked by the
        #     adopt step above) -> dynamically (un)register the confirmed listener
        #     protocol owner so a same-process silent reconnect can be safely
        #     identity-probed WITHOUT an HA restart and WITHOUT a listener rebuild.
        for transport in (*self._payload_transports(), *self._at_transports()):
            if callable(getattr(transport, "set_claimed_session_provider", None)):
                transport.set_claimed_session_provider(self._claimed_session_id)

    def _iter_observed_sessions(self) -> tuple[dict[str, object], ...]:
        """Return raw observed inbound sessions across this entry's own listeners.

        Uses the public ``observed_collector_sessions`` transport facade -- never
        the listener's private ``_session_inventory``. This is the only source
        the runtime session registry reads; ownership and short/full PN identity
        matching and untrusted-state exclusion all live in the registry.
        """

        sessions: list[dict[str, object]] = []
        seen_listeners: set[str] = set()
        for transport in self._payload_transports():
            provider = getattr(transport, "observed_collector_sessions", None)
            if not callable(provider):
                continue
            listener_key = str(getattr(transport, "listener_key", "") or "")
            dedup_key = listener_key or f"transport:{id(transport)}"
            if dedup_key in seen_listeners:
                continue
            seen_listeners.add(dedup_key)
            try:
                sessions.extend(provider())
            except Exception:
                continue
        return tuple(sessions)

    def _live_session_handle(self) -> SessionHandle:
        """Return the negotiated live SessionHandle for this entry's claimed session.

        Domain-registry path (production): the handle comes from the DOMAIN
        CallbackSessionRegistry under the REAL config entry id whose claim was
        registered at setup. That registry observes every shared listener in
        the process, so the handle follows the collector to whichever listener
        port it actually dialed. Fallback path (no domain registry injected --
        standalone hubs/unit tests): the runtime's own listener-scoped registry
        under a private key. The two paths are never active simultaneously, so
        there is exactly one ownership authority at any time. Neither path scans
        listener internals; ownership is durable-PN based (peer IP never a key);
        untrusted states negotiate to an unknown wire.
        """

        if self._domain_ownership_active():
            try:
                handle = self._callback_ownership_registry.session_handle_for_entry(
                    self._callback_entry_id
                )
            except Exception:
                logger.debug("Domain session handle lookup failed", exc_info=True)
                handle = None
            return handle or SessionHandle()

        registry = self._session_registry
        collector_pn = str(self._collector_pn or "").strip()
        # Keep the registry claim aligned with this entry's durable identity so
        # the handle represents the entry-owned session only. Re-claim only when
        # the durable PN changes (e.g. after a session-profile reconcile).
        if self._runtime_claim_pn != collector_pn:
            registry.release(_RUNTIME_SESSION_ENTRY_KEY)
            if collector_pn:
                try:
                    registry.claim(
                        _RUNTIME_SESSION_ENTRY_KEY,
                        collector_pn=collector_pn,
                    )
                except ValueError as exc:
                    # This should be unreachable for the normal runtime-scoped
                    # registry, but do not cache a failed claim as successful:
                    # keep the handle unknown and retry on the next observation
                    # cycle instead of freezing wire negotiation until PN changes.
                    logger.debug(
                        "Runtime callback-session claim rejected; will retry: %s",
                        exc,
                    )
                    self._runtime_claim_pn = ""
                    return SessionHandle()
            self._runtime_claim_pn = collector_pn
        if not collector_pn:
            return SessionHandle()
        return registry.session_handle_for_entry(_RUNTIME_SESSION_ENTRY_KEY) or SessionHandle()

    async def _async_wait_for_at_connection(self, *, timeout: float) -> bool:
        transports = self._at_transports()
        if len(transports) == 1:
            return await transports[0].wait_until_connected(timeout=timeout) and transports[0].connected

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if self.active_collector_at_transport is not None:
                return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            wait_timeout = min(0.1, remaining)
            for transport in transports:
                ok = await transport.wait_until_connected(timeout=wait_timeout)
                if ok and self._connected_at_transport() is not None:
                    return True

    async def _async_wait_for_payload_connection(self, *, timeout: float) -> bool:
        transports = self._payload_transports()
        if len(transports) == 1:
            return await transports[0].wait_until_connected(timeout=timeout) and transports[0].connected

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if self.active_transport is not None:
                return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            wait_timeout = min(0.1, remaining)
            for transport in transports:
                ok = await transport.wait_until_connected(timeout=wait_timeout)
                if ok and self._connected_payload_transport() is not None:
                    return True

    async def _async_wait_for_payload_heartbeat(self, *, timeout: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        first_pass = True
        while True:
            # A collector may replace its TCP socket while this wait is in
            # progress. Re-resolve the registry-owned session and connected
            # transport on every pass instead of pinning the socket that was
            # live at method entry. A transient no-socket window is therefore
            # part of handover, not an immediate heartbeat failure.
            await self._async_follow_owned_session_listener()
            self._apply_live_wire_to_transports()
            selected_remote_ip, ambiguous = self._selected_connected_remote_ip()
            if ambiguous:
                return False
            transports = tuple(
                transport
                for transport in self._payload_transports()
                if transport.connected
                and (
                    not selected_remote_ip
                    or not str(transport.collector_info.remote_ip or "").strip()
                    or str(transport.collector_info.remote_ip or "").strip()
                    == selected_remote_ip
                )
            )
            connected_wait_completed = False
            for transport in transports:
                if not transport.connected:
                    continue

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False

                wait_timeout = timeout if first_pass else remaining
                ok = await transport.wait_until_heartbeat(timeout=wait_timeout)
                if ok:
                    return True
                # A connected transport returning False exhausted its heartbeat
                # wait normally. Only a socket that vanished during the wait is
                # handover evidence and warrants re-resolving a replacement.
                if transport.connected:
                    connected_wait_completed = True

            if connected_wait_completed:
                return False

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            first_pass = False
            await asyncio.sleep(min(0.05, remaining))

    async def _start_all_transports(self) -> None:
        for transport in self._payload_transports():
            await transport.start()
        for transport in self._at_transports():
            await transport.start()

    async def _stop_all_transports(self) -> None:
        for transport in reversed(self._at_transports()):
            await transport.stop()
        for transport in reversed(self._payload_transports()):
            await transport.stop()

    async def _disconnect_all_transports(self) -> None:
        for transport in reversed(self._at_transports()):
            await transport.disconnect()
        for transport in reversed(self._payload_transports()):
            await transport.disconnect()

    def _rebuild_link(self, server_ip: str) -> None:
        """Create the transport/discovery pair for one collector-facing IP."""

        effective_target = self._collector_ip or self._discovery_target
        effective_advertised_server_ip = self._configured_advertised_server_ip or server_ip
        effective_advertised_tcp_port = self._configured_advertised_tcp_port or self._tcp_port
        self._effective_server_ip = server_ip
        self._listener_bind_host = _DEFAULT_LISTENER_BIND_HOST
        self._transport, self._at_transport = self._build_transport_pair(
            self._listener_bind_host,
            self._tcp_port,
        )
        self._auxiliary_transports = {}
        self._auxiliary_at_transports = {}
        for port in sorted(self._auxiliary_listener_ports):
            payload_transport, at_transport = self._build_transport_pair(
                self._listener_bind_host,
                port,
            )
            self._auxiliary_transports[port] = payload_transport
            self._auxiliary_at_transports[port] = at_transport
        self._announcer = DiscoveryAnnouncer(
            bind_ip=server_ip,
            advertised_server_ip=effective_advertised_server_ip,
            advertised_server_port=effective_advertised_tcp_port,
            target_ip=effective_target,
            udp_port=self._udp_port,
            interval=float(self._discovery_interval),
        )
        self._apply_collector_connection_watcher()

    def _build_transport_pair(
        self,
        bind_host: str,
        port: int,
    ) -> tuple[SharedEybondTransport, SharedCollectorAtTransport]:
        payload_transport = SharedEybondTransport(
            host=bind_host,
            port=port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            heartbeat_interval=float(self._heartbeat_interval),
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            # Only a CONFIRMED protocol is handed to the shared listener.
            # "" means passive observation only.
            collector_session_protocol=self._confirmed_session_protocol(),
            collector_identity_strategy=self._collector_identity_strategy,
            collector_raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
            collector_raw_passthrough_min_interval_ms=(
                self._collector_raw_passthrough_min_interval_ms
            ),
        )
        at_transport = SharedCollectorAtTransport(
            host=bind_host,
            port=port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            # Only a CONFIRMED protocol is handed to the shared listener.
            # "" means passive observation only.
            collector_session_protocol=self._confirmed_session_protocol(),
            collector_identity_strategy=self._collector_identity_strategy,
            collector_raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
            collector_raw_passthrough_min_interval_ms=(
                self._collector_raw_passthrough_min_interval_ms
            ),
        )
        return payload_transport, at_transport

    async def _rebuild_if_server_ip_changed(self, *, reason: str) -> bool:
        resolved_server_ip = resolve_server_ip(
            self._configured_server_ip,
            collector_ip=self._collector_ip,
        )
        if resolved_server_ip == self._effective_server_ip:
            return False

        logger.warning(
            "EyeBond advertised listener IP changed from %s to %s after %s; rebuilding transport",
            self._effective_server_ip or "unknown",
            resolved_server_ip or "unknown",
            reason or "network_change",
        )
        await self._announcer.stop()
        await self._stop_all_transports()
        self._rebuild_link(resolved_server_ip)
        self._listener_rebind_count += 1
        return True

    def _record_listener_error(self, exc: Exception) -> None:
        self._listener_status = "error"
        if isinstance(exc, CollectorListenerBindError):
            self._listener_last_error = str(exc.error)
            return
        self._listener_last_error = str(exc)
