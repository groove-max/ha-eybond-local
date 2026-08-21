"""Temporary proxy route over the single shared collector listener."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from .common import _close_writer_bounded
from .listener import (
    _SharedEybondListener,
    _acquire_shared_listener,
    _release_shared_listener,
)

logger = logging.getLogger(__name__)

class SharedProxyCaptureRoute:
    """Route one collector callback accepted by the shared listener into a proxy handler."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
    ) -> None:
        self._host = str(host)
        self._port = int(port)
        self._collector_ip = str(collector_ip or "").strip()
        self._collector_pn = str(collector_pn or "").strip()
        if (
            type(expected_session_protocol) is not str
            or expected_session_protocol != expected_session_protocol.strip()
            or expected_session_protocol.lower() not in {"at_text", "eybond_framed"}
        ):
            raise ValueError("proxy_expected_session_protocol_invalid")
        self._expected_session_protocol = str(
            expected_session_protocol or ""
        ).strip().lower()
        self._handler = handler
        self._listener: _SharedEybondListener | None = None
        self._reservation_token: int | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_listener(self._host, self._port)
        self._reservation_token = self._listener.register_exclusive_collector_route(
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            transparent=True,
            expected_session_protocol=self._expected_session_protocol,
        )
        self._running = True
        self._task = asyncio.create_task(
            self._route_loop(),
            name=f"shared_proxy_capture_route_{self._collector_ip or self._port}",
        )

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        listener = self._listener
        self._listener = None
        reservation_token = self._reservation_token
        self._reservation_token = None
        if listener is not None:
            try:
                if reservation_token is not None:
                    await listener.unregister_exclusive_collector_route(
                        reservation_token
                    )
            finally:
                await _release_shared_listener(listener)

    async def _route_loop(self) -> None:
        try:
            while self._running:
                listener = self._listener
                if listener is None:
                    return
                transparent_pop = getattr(
                    listener,
                    "pop_pending_socket_for_transparent_route",
                    None,
                )
                if callable(transparent_pop) and self._reservation_token is not None:
                    pending = await transparent_pop(self._reservation_token)
                else:
                    # Compatibility for narrow listener test doubles.
                    pending = await listener.pop_pending_socket_for_route(
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        session_protocol=self._expected_session_protocol,
                    )
                if pending is None:
                    await asyncio.sleep(0.1)
                    continue
                reader, pump_task = _reader_with_initial_bytes(
                    pending.initial_bytes,
                    pending.reader,
                )
                pending.initial_bytes = b""
                try:
                    await self._handler(reader, pending.writer)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A handler crash must not kill the route loop and leave
                    # the claimed socket dangling open.
                    logger.exception(
                        "Proxy capture handler failed for %s; closing the claimed socket",
                        pending.remote_ip,
                    )
                    await _close_writer_bounded(pending.writer)
                finally:
                    if pump_task is not None:
                        pump_task.cancel()
                        try:
                            await pump_task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
        except asyncio.CancelledError:
            raise


def _reader_with_initial_bytes(
    initial_bytes: bytes,
    source: asyncio.StreamReader,
) -> tuple[asyncio.StreamReader, asyncio.Task[None] | None]:
    prefix = bytes(initial_bytes or b"")
    if not prefix:
        return source, None

    replay = asyncio.StreamReader()
    replay.feed_data(prefix)

    async def _pump() -> None:
        try:
            while True:
                chunk = await source.read(4096)
                if not chunk:
                    replay.feed_eof()
                    return
                replay.feed_data(chunk)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            replay.set_exception(exc)

    return replay, asyncio.create_task(_pump(), name="collector_proxy_replay_reader")
