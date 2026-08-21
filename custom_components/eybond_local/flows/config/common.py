"""Small shared primitives for config-flow lifecycles."""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager

from ...const import (
    DEFAULT_DISCOVERY_TARGET,
)
from ...onboarding.timeouts import (
    auto_scan_timeout_seconds as _onboarding_auto_scan_timeout_seconds,
)
from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY

_ONBOARDING_TIMEOUT_POLICY = DEFAULT_ONBOARDING_TIMEOUT_POLICY

_AUTO_SCAN_TIMEOUT = _onboarding_auto_scan_timeout_seconds(_ONBOARDING_TIMEOUT_POLICY)

_PASSIVE_LISTENER_HOST = "0.0.0.0"


@asynccontextmanager
async def _async_timeout(timeout_seconds: float):
    """Use asyncio.timeout when available, with a Python 3.10-compatible fallback."""

    native_timeout = getattr(asyncio, "timeout", None)
    if native_timeout is not None:
        async with native_timeout(timeout_seconds):
            yield
        return

    task = asyncio.current_task()
    if task is None:
        yield
        return

    loop = asyncio.get_running_loop()
    timed_out = False

    def _cancel_current_task() -> None:
        nonlocal timed_out
        timed_out = True
        task.cancel()

    handle = loop.call_later(timeout_seconds, _cancel_current_task)
    try:
        yield
    except asyncio.CancelledError as exc:
        if timed_out:
            raise TimeoutError from exc
        raise
    finally:
        handle.cancel()


def _compute_broadcast_24(ip: str) -> str:
    parts = ip.split(".")
    if len(parts) != 4:
        return DEFAULT_DISCOVERY_TARGET
    return f"{parts[0]}.{parts[1]}.{parts[2]}.255"


def _sanitize_collector_route_hint(
    collector_ip: str,
    *,
    server_ip: str = "",
    discovery_target: str = "",
) -> str:
    candidate = str(collector_ip).strip()
    if not candidate:
        return ""
    if candidate == DEFAULT_DISCOVERY_TARGET:
        return ""
    default_broadcast = _compute_broadcast_24(server_ip) if server_ip else ""
    if (
        discovery_target
        and candidate == discovery_target
        and default_broadcast
        and candidate == default_broadcast
    ):
        return ""
    return candidate


def _is_ipv4(ip: str) -> bool:
    try:
        socket.inet_aton(ip)
        return True
    except OSError:
        return False
