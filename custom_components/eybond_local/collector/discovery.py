"""UDP discovery sender for SmartESS/EyeBond collectors."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import socket

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DiscoveryProbeResult:
    """Result of one synchronous EyeBond UDP discovery probe."""

    target_ip: str
    message: str
    local_port: int
    reply: str = ""
    reply_from: str = ""


def build_discovery_messages(advertised_server_ip: str, advertised_server_port: int) -> tuple[bytes, ...]:
    """Return a small set of compatible `set>server=` payload variants."""

    base = f"set>server={advertised_server_ip}:{advertised_server_port};"
    return (
        base.encode("ascii"),
        f"{base}\r\n".encode("ascii"),
        f"{base}\n".encode("ascii"),
    )


def _probe_target_sync(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    target_ip: str,
    udp_port: int,
    timeout: float,
) -> DiscoveryProbeResult:
    messages = build_discovery_messages(advertised_server_ip, advertised_server_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout)
    try:
        try:
            sock.bind((bind_ip, 0))
        except OSError:
            sock.bind(("", 0))
        local_port = sock.getsockname()[1]
        for message in messages:
            sock.sendto(message, (target_ip, udp_port))
            try:
                data, addr = sock.recvfrom(2048)
                return DiscoveryProbeResult(
                    target_ip=target_ip,
                    message=message.decode("ascii", errors="replace"),
                    local_port=local_port,
                    reply=data.decode("ascii", errors="replace").strip(),
                    reply_from=f"{addr[0]}:{addr[1]}",
                )
            except OSError:
                continue
        return DiscoveryProbeResult(
            target_ip=target_ip,
            message=messages[0].decode("ascii", errors="replace"),
            local_port=local_port,
        )
    finally:
        sock.close()


async def async_probe_target(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    target_ip: str,
    udp_port: int,
    timeout: float,
) -> DiscoveryProbeResult:
    """Send one-shot discovery probes and capture the first UDP response."""

    return await asyncio.to_thread(
        _probe_target_sync,
        bind_ip=bind_ip,
        advertised_server_ip=advertised_server_ip,
        advertised_server_port=advertised_server_port,
        target_ip=target_ip,
        udp_port=udp_port,
        timeout=timeout,
    )


class DiscoveryAnnouncer:
    """Periodically broadcasts set>server=... until the collector connects."""

    def __init__(
        self,
        *,
        bind_ip: str,
        advertised_server_ip: str,
        advertised_server_port: int,
        target_ip: str,
        udp_port: int,
        interval: float,
    ) -> None:
        self._bind_ip = bind_ip
        self._advertised_server_ip = advertised_server_ip
        self._advertised_server_port = int(advertised_server_port)
        self._target_ip = target_ip
        self._udp_port = int(udp_port)
        self._interval = float(interval)
        self._task: asyncio.Task[None] | None = None
        self.last_reply: str = ""
        self.last_reply_from: str = ""

    @property
    def running(self) -> bool:
        """Return whether the background discovery loop is active."""

        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start the background broadcast loop if it is not running yet."""

        if self.running:
            return
        self._task = asyncio.create_task(self._run(), name="eybond_discovery")

    async def stop(self) -> None:
        """Stop the background broadcast loop."""

        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        message = build_discovery_messages(
            self._advertised_server_ip,
            self._advertised_server_port,
        )[0]

        try:
            while True:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    sock.settimeout(0.75)
                    try:
                        try:
                            sock.bind((self._bind_ip, 0))
                        except OSError:
                            sock.bind(("", 0))
                        sock.sendto(message, (self._target_ip, self._udp_port))
                        logger.debug(
                            "Discovery TX target=%s:%d payload=%s",
                            self._target_ip,
                            self._udp_port,
                            message.decode("ascii"),
                        )
                        try:
                            data, addr = sock.recvfrom(2048)
                            self.last_reply = data.decode("ascii", errors="replace").strip()
                            self.last_reply_from = f"{addr[0]}:{addr[1]}"
                            logger.debug(
                                "Discovery RX from=%s reply=%s",
                                self.last_reply_from,
                                self.last_reply,
                            )
                        except OSError:
                            pass
                    finally:
                        sock.close()
                except OSError as exc:
                    logger.debug("Discovery TX failed: %s", exc)
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise
