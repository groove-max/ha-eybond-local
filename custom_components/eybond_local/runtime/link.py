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
from ..collector.discovery import DiscoveryAnnouncer, async_probe_target
from ..collector.transport import (
    CollectorAtTransport,
    CollectorListenerBindError,
    CollectorTransport,
    SharedCollectorAtTransport,
    SharedEybondTransport,
    SharedProxyCaptureRoute,
)
from ..const import DEFAULT_REQUEST_TIMEOUT
from ..link_transport import PayloadLinkTransport
from ..models import CollectorInfo
from ..support.proxy_session import InProcessProxyCaptureHandler
from ..support.shadow_learning_backend import ShadowLearningSeed
from ..support.shadow_learning_proxy import InProcessFailClosedShadowProxyHandler

logger = logging.getLogger(__name__)

_DEFAULT_LISTENER_BIND_HOST = "0.0.0.0"


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


def _prefer_more_complete_collector_pn(current: str, candidate: str) -> str:
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
    return normalized_current


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
) -> dict[str, object]:
    """Return compact, user-facing callback identity diagnostics."""

    identified_count = 0
    unresolved_count = 0
    mismatch_count = 0
    timeout_count = 0
    waiting_count = 0
    pending_states = {
        "pending",
        "waiting_for_identity",
        "waiting_for_route_identity",
    }
    for session in sessions:
        state = str(session.get("state") or "").strip()
        if session.get("collector_identity_masked"):
            identified_count += 1
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

    if pending_count <= 0:
        status = "idle"
        summary = "No unresolved collector callback sessions are pending."
    elif mismatch_count:
        status = "conflict"
        summary = (
            "A collector callback was identified, but it does not match the expected collector PN."
        )
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
    else:
        status = "ok"
        summary = "Pending collector callbacks have a known collector identity."

    return {
        "collector_callback_identity_status": status,
        "collector_callback_identity_summary": summary,
        "collector_callback_identified_session_count": identified_count,
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
        collector_session_protocol: str = "",
        collector_identity_strategy: str = "",
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
    ) -> None:
        self._configured_server_ip = server_ip
        self._configured_advertised_server_ip = advertised_server_ip.strip()
        self._collector_ip = collector_ip
        self._collector_pn = str(collector_pn or "").strip()
        self._collector_session_protocol = str(collector_session_protocol or "").strip().lower()
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
        if server_ip and self._effective_server_ip and self._effective_server_ip != server_ip:
            logger.warning(
                "Configured EyeBond server_ip %s is not active on this host; falling back to %s",
                server_ip,
                self._effective_server_ip,
            )
        self._transport: CollectorTransport
        self._at_transport: CollectorAtTransport
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
        set_watcher = getattr(self._transport, "set_connection_watcher", None)
        if callable(set_watcher):
            set_watcher(self._collector_connection_watcher)

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

        if self._uses_at_text_payload():
            return None
        return self._connected_payload_transport()

    @property
    def active_collector_at_transport(self) -> CollectorAtTransport | None:
        """Return the connected AT transport selected for the active collector."""

        return self._connected_at_transport()

    @property
    def transport(self) -> CollectorTransport:
        """Return the active payload-capable transport."""

        if self._uses_at_text_payload():
            return self.active_collector_at_transport or self._at_transport
        return self.active_transport or self._transport

    @property
    def collector_at_transport(self) -> CollectorAtTransport:
        """Return the collector AT transport sharing the same listener port."""

        return self.active_collector_at_transport or self._at_transport

    @property
    def connected(self) -> bool:
        """Return whether the physical link is currently connected."""

        if self._uses_at_text_payload():
            return self.active_collector_at_transport is not None
        return self.active_transport is not None

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

    def listener_diagnostics(self) -> dict[str, object]:
        """Return listener bind and advertised endpoint diagnostics."""

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
            "collector_callback_session_protocol": self._collector_session_protocol,
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
        diagnostics.update(self._session_inventory_diagnostics())
        return diagnostics

    def _session_inventory_diagnostics(self) -> dict[str, object]:
        """Return passive callback-session inventory diagnostics."""

        summaries: list[dict[str, object]] = []
        seen_listeners: set[int] = set()
        for transport in self._payload_transports():
            listener = getattr(transport, "_listener", None)
            if listener is None:
                continue
            listener_id = id(listener)
            if listener_id in seen_listeners:
                continue
            seen_listeners.add(listener_id)
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
        result.update(
            _callback_identity_status_values(
                pending_count=pending_count,
                recent_count=recent_count,
                duplicate_peer_ip_count=duplicate_peer_ip_count,
                sessions=sessions,
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
        if self._reverse_discovery_enabled:
            await self._ensure_discovery(reason="runtime_start")
        else:
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
        if self._reverse_discovery_enabled:
            await self._ensure_discovery(reason=reason or "network_change")
        else:
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
            normalized_protocol == self._collector_session_protocol
            and normalized_strategy == self._collector_identity_strategy
            and normalized_raw_bootstrap == self._collector_raw_passthrough_bootstrap
            and normalized_raw_frame == self._collector_raw_passthrough_frame_format
            and normalized_raw_min_interval_ms == self._collector_raw_passthrough_min_interval_ms
        ):
            return False

        logger.warning(
            "EyeBond callback session profile changed after %s: protocol %s -> %s, identity %s -> %s, raw_bootstrap %s -> %s, raw_frame %s -> %s, raw_min_interval_ms %s -> %s; rebuilding transport",
            reason or "collector_session_profile_change",
            self._collector_session_protocol or "unknown",
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

        self._collector_session_protocol = normalized_protocol
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
        if self._reverse_discovery_enabled:
            await self._ensure_discovery(reason=reason or "collector_session_profile_change")
        else:
            await self._announcer.stop()
        return True

    async def async_stop(self) -> None:
        """Stop discovery and the active link transport."""

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
        probe = await async_probe_target(
            bind_ip=self._effective_server_ip,
            advertised_server_ip=self.effective_advertised_server_ip,
            advertised_server_port=advertised_port,
            target_ip=target_ip,
            udp_port=self._udp_port,
            timeout=float(timeout),
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
        """Route one collector's callback connection through the in-process proxy."""

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
                collector_session_protocol=collector_session_protocol,
                handler=handler.handle_client,
            )
            await route.start()
            self._proxy_capture_handler = handler
            self._proxy_capture_route = route
            await self._set_route_lease_state(normalized_owner_id, "running")
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
        collector_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        seed: ShadowLearningSeed,
    ) -> None:
        """Route one collector callback connection through the fail-closed shadow proxy."""

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
                collector_session_protocol=collector_session_protocol,
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

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        """Drop current collector sockets without restarting discovery."""

        logger.warning(
            "Disconnecting collector runtime connections after %s remote=%s configured_collector_ip=%s",
            reason or "runtime_disconnect",
            self.collector_info.remote_ip or "unknown",
            self._collector_ip or "unknown",
        )
        await self._disconnect_all_transports()

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        """Try to ensure a live collector connection without raising on timeout."""

        if self._uses_at_text_payload():
            if self.active_collector_at_transport is None:
                if self._reverse_discovery_enabled:
                    await self._ensure_discovery(reason="waiting_for_callback")
                ok = await self._async_wait_for_at_connection(timeout=timeout)
                if not ok:
                    return False

            await self._announcer.stop()
            return self.connected

        if not self.connected:
            if self._reverse_discovery_enabled:
                await self._ensure_discovery(reason="waiting_for_callback")
            ok = await self._async_wait_for_payload_connection(timeout=timeout)
            if not ok:
                return False

        if require_heartbeat:
            heartbeat_ok = await self._async_wait_for_payload_heartbeat(timeout=min(timeout, 1.5))
            if not heartbeat_ok:
                if self._reverse_discovery_enabled:
                    await self._ensure_discovery(reason="heartbeat_timeout")
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
        if self._reverse_discovery_enabled:
            await self._ensure_discovery(reason=reason or "runtime_reset")

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

    def _uses_at_text_payload(self) -> bool:
        return str(self._collector_session_protocol or "").strip().lower() == "at_text"

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
                or str(transport.collector_info.remote_ip or "").strip() == selected_remote_ip
            )
        )
        if not transports:
            return False
        if len(transports) == 1:
            return await transports[0].wait_until_heartbeat(timeout=timeout)

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            for transport in transports:
                if not transport.connected:
                    continue

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False

                ok = await transport.wait_until_heartbeat(timeout=min(0.1, remaining))
                if ok:
                    return True

            if deadline - asyncio.get_running_loop().time() <= 0:
                return False

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

    async def _ensure_discovery(self, *, reason: str) -> None:
        """Start discovery if needed and track why it restarted."""

        was_running = bool(getattr(self._announcer, "running", False))
        await self._announcer.start()
        is_running = bool(getattr(self._announcer, "running", True))
        if not was_running and is_running:
            self._discovery_restart_count += 1
            self._last_discovery_reason = reason

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
            collector_session_protocol=self._collector_session_protocol,
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
            collector_session_protocol=self._collector_session_protocol,
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
