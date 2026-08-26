"""Shared types, constants and pure helpers for the runtime-link family."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import ipaddress
import json
import logging
import socket
import subprocess
from typing import Callable, Protocol

from ...collector.cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_collector,
    select_preferred_collector_cloud_family,
)
from ...collector.discovery import DiscoveryAnnouncer, async_send_callback_trigger
from ...collector.metadata import (
    CollectorMetadataRouteSet,
    build_collector_metadata_routes,
)
from ...connection.confirmed_session_protocol import ConfirmedSessionProtocolEvidence
from ...connection.session_handle import (
    ADAPTER_COLLECTOR_AT_COMMANDS,
    ADAPTER_NONE,
    ADAPTER_INVERTER_AT_MIXED,
    ADAPTER_INVERTER_FRAMED_FC4,
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    ConfirmedWireBinding,
    SessionHandle,
)
from ...collector_identity import (
    pn_is_same_identity,
    reconcile_pn,
)
from ...connection.session_registry import CallbackSessionRegistry
from ...collector.transport import (
    CollectorAtTransport,
    CollectorListenerBindError,
    CollectorTransport,
    SharedCollectorAtTransport,
    SharedEybondTransport,
    SharedProxyCaptureRoute,
)
from ...const import DEFAULT_REQUEST_TIMEOUT
from ...link_models import LinkRoute
from ...link_transport import PayloadLinkTransport
from ...models import CollectorInfo
from ...support.proxy_capture.session import InProcessProxyCaptureHandler
from ...support.shadow_learning import ShadowWriteObservation
from ...support.shadow_learning.backend import ShadowLearningSeed
from ...support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler

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
