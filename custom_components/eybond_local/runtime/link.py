"""Runtime link-manager layer between generic hub logic and concrete transports."""

from __future__ import annotations

import json
import logging
import socket
import subprocess
from typing import Protocol

from ..collector.discovery import DiscoveryAnnouncer
from ..collector.transport import CollectorTransport, SharedEybondTransport
from ..const import DEFAULT_REQUEST_TIMEOUT
from ..link_transport import PayloadLinkTransport
from ..models import CollectorInfo

logger = logging.getLogger(__name__)


def _default_local_ip() -> str:
    """Return the primary local IPv4 used for outbound traffic."""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except OSError:
        return ""


def _active_ipv4_addresses() -> tuple[str, ...]:
    """Return active global IPv4 addresses on this host."""

    try:
        output = subprocess.check_output(
            ["ip", "-j", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        raw = json.loads(output)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        fallback = _default_local_ip()
        return (fallback,) if fallback else ()

    addresses: list[str] = []
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
            addresses.append(ip)
    if not addresses:
        fallback = _default_local_ip()
        return (fallback,) if fallback else ()
    return tuple(dict.fromkeys(addresses))


def resolve_server_ip(configured_ip: str) -> str:
    """Return a bindable server IP, falling back if the configured one is stale."""

    active_ips = _active_ipv4_addresses()
    if configured_ip and configured_ip in active_ips:
        return configured_ip

    fallback = _default_local_ip()
    if fallback and fallback in active_ips:
        return fallback
    if active_ips:
        return active_ips[0]
    return configured_ip


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
    ) -> None:
        self._configured_server_ip = server_ip
        self._configured_advertised_server_ip = advertised_server_ip.strip()
        self._collector_ip = collector_ip
        self._tcp_port = int(tcp_port)
        self._configured_advertised_tcp_port = int(advertised_tcp_port or 0)
        self._udp_port = int(udp_port)
        self._discovery_target = discovery_target
        self._discovery_interval = int(discovery_interval)
        self._heartbeat_interval = int(heartbeat_interval)
        self._effective_server_ip = resolve_server_ip(server_ip)
        self._discovery_restart_count = 0
        self._last_discovery_reason = ""
        if server_ip and self._effective_server_ip and self._effective_server_ip != server_ip:
            logger.warning(
                "Configured EyeBond server_ip %s is not active on this host; falling back to %s",
                server_ip,
                self._effective_server_ip,
            )
        self._transport: CollectorTransport
        self._announcer: DiscoveryAnnouncer
        self._rebuild_link(self._effective_server_ip)

    @property
    def transport(self) -> CollectorTransport:
        """Return the active payload-capable transport."""

        return self._transport

    @property
    def connected(self) -> bool:
        """Return whether the physical link is currently connected."""

        return self._transport.connected

    @property
    def collector_info(self) -> CollectorInfo:
        """Return collector metadata merged with the latest UDP discovery reply."""

        collector = self._transport.collector_info
        collector.last_udp_reply = self._announcer.last_reply
        collector.last_udp_reply_from = self._announcer.last_reply_from
        collector.discovery_restart_count = self._discovery_restart_count
        collector.last_discovery_reason = self._last_discovery_reason
        return collector

    @property
    def effective_server_ip(self) -> str:
        """Return the current bind IP used by the EyeBond listener."""

        return self._effective_server_ip

    async def async_start(self) -> None:
        """Start the active link transport and its discovery loop."""

        resolved_server_ip = resolve_server_ip(self._configured_server_ip)
        if resolved_server_ip != self._effective_server_ip:
            logger.warning(
                "EyeBond listener IP changed from %s to %s; rebuilding transport",
                self._effective_server_ip,
                resolved_server_ip,
            )
            await self._announcer.stop()
            await self._transport.stop()
            self._rebuild_link(resolved_server_ip)

        await self._transport.start()
        await self._ensure_discovery(reason="runtime_start")

    async def async_stop(self) -> None:
        """Stop discovery and the active link transport."""

        await self._announcer.stop()
        await self._transport.stop()

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        """Try to ensure a live collector connection without raising on timeout."""

        if not self._transport.connected:
            await self._ensure_discovery(reason="waiting_for_callback")
            ok = await self._transport.wait_until_connected(timeout=timeout)
            if not ok:
                return False

        if require_heartbeat:
            heartbeat_ok = await self._transport.wait_until_heartbeat(timeout=min(timeout, 1.5))
            if not heartbeat_ok:
                await self._ensure_discovery(reason="heartbeat_timeout")
                return False

        await self._announcer.stop()
        return self._transport.connected

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
            if require_heartbeat and self._transport.connected:
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
        await self._transport.disconnect()
        await self._ensure_discovery(reason=reason or "runtime_reset")

    async def _ensure_discovery(self, *, reason: str) -> None:
        """Start discovery if needed and track why it restarted."""

        was_running = bool(getattr(self._announcer, "running", False))
        await self._announcer.start()
        is_running = bool(getattr(self._announcer, "running", True))
        if not was_running and is_running:
            self._discovery_restart_count += 1
            self._last_discovery_reason = reason

    def _rebuild_link(self, server_ip: str) -> None:
        """Create the transport/discovery pair for one effective EyeBond bind IP."""

        effective_target = self._collector_ip or self._discovery_target
        effective_advertised_server_ip = self._configured_advertised_server_ip or server_ip
        effective_advertised_tcp_port = self._configured_advertised_tcp_port or self._tcp_port
        self._effective_server_ip = server_ip
        self._transport = SharedEybondTransport(
            host=server_ip,
            port=self._tcp_port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            heartbeat_interval=float(self._heartbeat_interval),
            collector_ip=self._collector_ip,
        )
        self._announcer = DiscoveryAnnouncer(
            bind_ip=server_ip,
            advertised_server_ip=effective_advertised_server_ip,
            advertised_server_port=effective_advertised_tcp_port,
            target_ip=effective_target,
            udp_port=self._udp_port,
            interval=float(self._discovery_interval),
        )
