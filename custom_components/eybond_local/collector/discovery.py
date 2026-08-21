"""UDP discovery sender for SmartESS/EyeBond collectors."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import socket
from time import monotonic
from typing import Callable

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


def _probe_target_replies_sync(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    target_ip: str,
    udp_port: int,
    timeout: float,
) -> tuple[DiscoveryProbeResult, ...]:
    messages = build_discovery_messages(advertised_server_ip, advertised_server_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    replies: list[DiscoveryProbeResult] = []
    seen: set[tuple[str, int]] = set()
    try:
        try:
            sock.bind((bind_ip, 0))
        except OSError:
            sock.bind(("", 0))
        local_port = sock.getsockname()[1]
        for message in messages:
            sock.sendto(message, (target_ip, udp_port))
            deadline = monotonic() + max(0.0, timeout)
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                sock.settimeout(remaining)
                try:
                    data, addr = sock.recvfrom(2048)
                except OSError:
                    break
                key = (addr[0], addr[1])
                if key in seen:
                    continue
                seen.add(key)
                replies.append(
                    DiscoveryProbeResult(
                        target_ip=target_ip,
                        message=message.decode("ascii", errors="replace"),
                        local_port=local_port,
                        reply=data.decode("ascii", errors="replace").strip(),
                        reply_from=f"{addr[0]}:{addr[1]}",
                    )
                )
            if replies:
                break
        return tuple(replies)
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


async def async_probe_target_replies(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    target_ip: str,
    udp_port: int,
    timeout: float,
) -> tuple[DiscoveryProbeResult, ...]:
    """Send one-shot discovery probes and capture all UDP responses in the window."""

    return await asyncio.to_thread(
        _probe_target_replies_sync,
        bind_ip=bind_ip,
        advertised_server_ip=advertised_server_ip,
        advertised_server_port=advertised_server_port,
        target_ip=target_ip,
        udp_port=udp_port,
        timeout=timeout,
    )


async def async_send_callback_trigger(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    target_ip: str,
    udp_port: int,
    timeout: float,
    source: str = "",
    retry_window: float = 0.0,
    stop_requested: Callable[[], bool] | None = None,
) -> DiscoveryProbeResult:
    """Send one logical callback trigger and record it once in the ledger.

    This is THE production facade for asking a collector to dial back. All
    integration paths (runtime one-shot callback, manual onboarding probes,
    auto-scan, config-flow management probes) must go through it so the
    behavioral inbound verification can prove "no trigger was sent anywhere"
    from one monotonic generation. ``async_probe_target`` stays a raw wire
    utility for tests/tools.

    ``set>server`` is UDP and a collector may drop its old TCP socket before
    its UDP receiver is ready after a reboot.  ``retry_window`` therefore lets
    the *same* idempotent route be retransmitted inside this one logical send.
    The ledger is advanced exactly once and the caller's causality lease stays
    unchanged.  A UDP reply or ``stop_requested`` (normally a fresh TCP socket)
    ends retransmission immediately.  The default keeps the historical
    one-pass behaviour for all callers that do not explicitly opt in.
    """

    from ..connection.callback_ledger import get_callback_trigger_ledger

    ledger = get_callback_trigger_ledger()
    with ledger.callback_send_scope():
        ledger.record(target=target_ip, source=source)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(retry_window))
        result: DiscoveryProbeResult | None = None
        while True:
            remaining = max(0.0, deadline - loop.time())
            probe_timeout = float(timeout)
            if retry_window > 0.0:
                # One probe sends three compatible payload variants.  Divide
                # the remaining logical window between them so this helper
                # never stretches the recovery deadline by another full probe.
                probe_timeout = min(probe_timeout, remaining / 3.0)
            result = await async_probe_target(
                bind_ip=bind_ip,
                advertised_server_ip=advertised_server_ip,
                advertised_server_port=advertised_server_port,
                target_ip=target_ip,
                udp_port=udp_port,
                timeout=max(0.01, probe_timeout),
            )
            if result.reply:
                return result
            if stop_requested is not None:
                try:
                    if stop_requested():
                        return result
                except Exception:
                    # Observation is only an optimization; failure to inspect
                    # it must not turn into a false successful send.
                    pass
            if retry_window <= 0.0 or loop.time() >= deadline:
                return result


async def async_send_callback_trigger_replies(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    target_ip: str,
    udp_port: int,
    timeout: float,
    source: str = "",
) -> tuple[DiscoveryProbeResult, ...]:
    """Ledger-recorded variant of ``async_probe_target_replies`` (fan-out scans)."""

    from ..connection.callback_ledger import get_callback_trigger_ledger

    ledger = get_callback_trigger_ledger()
    with ledger.callback_send_scope():
        ledger.record(target=target_ip, source=source)
        return await async_probe_target_replies(
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
        from ..connection.callback_ledger import (
            CallbackTriggerInhibitedError,
            get_callback_trigger_ledger,
        )

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
                        # The announcer is a production callback-trigger sender
                        # (proxy capture uses it): every datagram must move the
                        # integration-wide ledger generation. Recorded HERE at
                        # the single send site (the announcer does not go
                        # through the probe facade), so one datagram increments
                        # the generation exactly once.
                        ledger = get_callback_trigger_ledger()
                        with ledger.callback_send_scope():
                            ledger.record(
                                target=self._target_ip,
                                source="discovery_announcer",
                            )
                            sock.sendto(message, (self._target_ip, self._udp_port))
                            logger.debug(
                                "Discovery TX target=%s:%d payload=%s",
                                self._target_ip,
                                self._udp_port,
                                message.decode("ascii"),
                            )
                            try:
                                data, addr = sock.recvfrom(2048)
                                self.last_reply = data.decode(
                                    "ascii", errors="replace"
                                ).strip()
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
                except CallbackTriggerInhibitedError as exc:
                    logger.debug("Discovery TX deferred: %s", exc)
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise
