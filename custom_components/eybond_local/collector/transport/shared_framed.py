"""Public framed transport facade over the shared listener."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from ...link_models import EybondLinkRoute, LinkRoute
from ...models import CollectorInfo
from ..protocol import EybondHeader
from .common import _bounded_write_timeout, _copy_collector_info
from .connections import _CollectorConnection
from .listener import (
    _LISTENERS,
    _LISTENERS_LOCK,
    _SharedEybondListener,
    _acquire_shared_payload_listener,
    _release_shared_listener,
)

class SharedEybondTransport:
    """One per-entry transport facade backed by a shared TCP listener."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        request_timeout: float,
        heartbeat_interval: float,
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        collector_identity_strategy: str = "",
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._request_timeout = request_timeout
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._collector_ip = collector_ip
        self._collector_pn = str(collector_pn or "").strip()
        # DURABLE confirmed protocol owner value. Only the runtime passes the
        # validated confirmed protocol here; onboarding never does, so an
        # inferred/expected hint can never register a durable confirmed owner.
        self._collector_session_protocol = str(collector_session_protocol or "").strip().lower()
        # Registry-mediated claim: the runtime injects a resolver that returns
        # the registry-chosen session id for this collector, so the claim targets
        # exactly the owned session instead of re-deriving ownership by IP/PN.
        self._claimed_session_provider: Any = None
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
        self._listener: _SharedEybondListener | None = None
        self._connection_watcher_callback: Callable[[str], None] | None = None
        self._connection_watcher_token: int | None = None

    def set_connection_watcher(self, callback: Callable[[str], None] | None) -> None:
        """Fire ``callback(remote_ip)`` whenever this collector dials back in.

        Registered on the shared listener once the transport starts; safe to
        call before ``start()``.
        """

        if self._listener is not None and self._connection_watcher_token is not None:
            self._listener.remove_connection_watcher(self._connection_watcher_token)
            self._connection_watcher_token = None
        self._connection_watcher_callback = callback
        if callback is not None and self._listener is not None:
            self._connection_watcher_token = self._listener.add_connection_watcher(
                self._collector_ip,
                callback,
            )

    @property
    def connected(self) -> bool:
        connection = self._connection(create_placeholder=False)
        return connection.connected if connection is not None else False

    @property
    def collector_info(self) -> CollectorInfo:
        connection = self._connection(create_placeholder=False)
        if connection is not None:
            return connection.collector_info
        return _copy_collector_info(
            CollectorInfo(remote_ip=self._collector_ip, collector_pn=self._collector_pn)
        )

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_payload_listener(
            self._host,
            self._port,
            self._collector_ip,
            self._collector_pn,
            self._collector_session_protocol,
        )
        if self._connection_watcher_callback is not None and self._connection_watcher_token is None:
            self._connection_watcher_token = self._listener.add_connection_watcher(
                self._collector_ip,
                self._connection_watcher_callback,
            )
        self._connection(create_placeholder=bool(self._collector_ip))

    async def stop(self, *, preserve_session_id: str = "") -> None:
        """Release this facade, optionally leaving one exact socket observable.

        The preservation hook is for exact-session lifecycle boundaries: a
        strongly identified session may have to survive scan-to-admission,
        runtime reload/rebuild, or unload-to-removal finalization. It never
        preserves by route or peer address.
        """

        if self._listener is None:
            return
        preserve_session_id = str(preserve_session_id or "").strip()
        listener = self._listener
        self._listener = None
        if self._connection_watcher_token is not None:
            listener.remove_connection_watcher(self._connection_watcher_token)
            self._connection_watcher_token = None
        await _release_shared_listener(
            listener,
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            collector_session_protocol=self._collector_session_protocol,
            close_payload=True,
            close_pending=True,
            unregister_payload_owner=True,
            unregister_payload_pn_owner=True,
            unregister_session_protocol_owner=True,
            preserve_session_id=preserve_session_id,
        )

    async def async_snapshot_shared_connection(self) -> _CollectorConnection | None:
        if not self._collector_ip and not self._collector_pn:
            return None
        async with _LISTENERS_LOCK:
            listener = _LISTENERS.get((self._host, self._port))
            if listener is None:
                return None
            if self._collector_pn:
                connection = listener._connection_by_collector_pn(
                    self._collector_pn,
                    listener._connections_by_pn,
                )
            else:
                connection = listener._connections.get(self._collector_ip)
            if connection is None or not connection.connected:
                return None
            return connection

    def session_inventory_diagnostics(self) -> dict[str, object]:
        if self._listener is None:
            return {
                "pending_session_count": 0,
                "recent_session_count": 0,
                "duplicate_peer_ip_count": 0,
                "duplicate_peer_ips": [],
                "sessions": [],
            }
        return self._listener.session_inventory_diagnostics()

    @property
    def listener_key(self) -> str:
        """Return a stable, public identity for the shared listener this uses.

        Runtime code dedups transports that share one listener by this key
        instead of ``id(transport._listener)``, so it never touches listener
        internals.
        """

        return f"{self._host}:{self._port}"

    def observed_collector_sessions(self) -> tuple[dict[str, object], ...]:
        """Return raw observed inbound sessions on this listener (public facade).

        Runtime session ownership/negotiation reads through this instead of
        reaching into the listener's private ``_session_inventory``. Each dict is
        stamped with the listener port so the callback session registry can build
        a SessionHandle without knowing listener internals.
        """

        if self._listener is None:
            return ()
        sessions: list[dict[str, object]] = []
        for session in self._listener.discovered_collector_sessions():
            if not isinstance(session, dict):
                continue
            enriched = dict(session)
            enriched.setdefault("listener_port", int(self._port))
            sessions.append(enriched)
        return tuple(sessions)

    async def async_disconnect_if_new_shared_connection(
        self,
        snapshot: _CollectorConnection | None,
    ) -> None:
        if not self._collector_ip and not self._collector_pn:
            return
        async with _LISTENERS_LOCK:
            listener = _LISTENERS.get((self._host, self._port))
            if listener is None:
                return
            if self._collector_pn:
                connection = listener._connection_by_collector_pn(
                    self._collector_pn,
                    listener._connections_by_pn,
                )
            else:
                connection = listener._connections.get(self._collector_ip)
        if connection is None or connection is snapshot:
            return
        await connection.disconnect()

    async def disconnect(self) -> None:
        connection = self._connection(create_placeholder=False)
        if connection is None:
            return
        await connection.disconnect()

    def set_collector_ip(self, collector_ip: str) -> None:
        self._collector_ip = collector_ip
        self._connection(create_placeholder=bool(self._collector_ip))

    def set_claimed_session_provider(self, provider: Any) -> None:
        """Set the runtime's registry-mediated claimed-session-id resolver."""

        self._claimed_session_provider = provider

    def set_confirmed_session_protocol(self, protocol: str) -> None:
        """Set the CONFIRMED session-protocol owner on the shared listener.

        Idempotent. This is the durable-probe-permission counterpart of
        ``set_negotiated_wire`` (live activation): a confirmed protocol owner lets
        the listener send a safe identity probe to a SILENT session of this
        collector. It is distinct from the live wire. Only a confirmed wire
        (``eybond_framed`` / ``at_text``) may own; anything else clears the owner.
        The inferred/expected cloud-family protocol must never reach this method.

        Before the listener is acquired this only stores the value (applied at
        ``start()``). Once acquired it dynamically unregisters the old owner and
        registers the new one WITHOUT rebuilding the TCP listener; re-setting the
        same value is a no-op; ``stop()`` unregisters exactly the current value
        once (no double unregister / leak).
        """

        normalized = str(protocol or "").strip().lower()
        if normalized not in ("at_text", "eybond_framed"):
            normalized = ""
        if normalized == self._collector_session_protocol:
            return
        listener = self._listener
        if listener is not None:
            if self._collector_session_protocol:
                listener.unregister_session_protocol_owner(
                    self._collector_session_protocol
                )
            if normalized:
                listener.register_session_protocol_owner(normalized)
        self._collector_session_protocol = normalized

    def _resolve_claimed_session_id(self) -> str:
        provider = self._claimed_session_provider
        if not callable(provider):
            return ""
        try:
            return str(provider() or "").strip()
        except Exception:
            return ""

    async def wait_until_connected(self, timeout: float) -> bool:
        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            listener = self._listener
            if listener is None:
                return False
            connection = self._connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                return True

            # Re-resolved every iteration: the runtime's registry-mediated
            # provider may learn (or lose) the claimed session while we wait.
            claimed_session_id = self._resolve_claimed_session_id()
            if self._collector_ip or self._collector_pn or claimed_session_id:
                pending = await listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                    session_id=claimed_session_id,
                )
                if pending is not None:
                    connection = await listener.activate_pending_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        heartbeat_interval=self._heartbeat_interval,
                        write_timeout=self._write_timeout,
                    )
                    if connection.connected:
                        return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            if connection is not None and (
                self._collector_ip or self._collector_pn or claimed_session_id
            ):
                ok = await connection.wait_until_connected(timeout=min(0.1, remaining))
                if ok:
                    return True
                continue

            await asyncio.sleep(min(0.1, remaining))

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            connection = self._connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False
                return await connection.wait_until_heartbeat(timeout=remaining)

            claimed_session_id = self._resolve_claimed_session_id()
            if self._collector_ip or self._collector_pn or claimed_session_id:
                pending = await self._listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                    session_id=claimed_session_id,
                )
                if pending is not None:
                    connection = await self._listener.activate_pending_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        heartbeat_interval=self._heartbeat_interval,
                        write_timeout=self._write_timeout,
                    )
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        return False
                    return await connection.wait_until_heartbeat(timeout=remaining)

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def wait_until_liveness(self, timeout: float) -> bool:
        """Wait for recent correlated traffic on the exact selected session."""

        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            connection = self._connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False
                return await connection.wait_until_liveness(timeout=remaining)

            claimed_session_id = self._resolve_claimed_session_id()
            if self._collector_ip or self._collector_pn or claimed_session_id:
                pending = await self._listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                    session_id=claimed_session_id,
                )
                if pending is not None:
                    connection = await self._listener.activate_pending_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        heartbeat_interval=self._heartbeat_interval,
                        write_timeout=self._write_timeout,
                    )
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        return False
                    return await connection.wait_until_liveness(timeout=remaining)

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        connection = await self._active_connection_for_send()
        return await connection.async_send_forward(
            payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=self._request_timeout,
        )

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
    ) -> bytes:
        if not isinstance(route, EybondLinkRoute):
            raise TypeError(f"unsupported_link_route:{route.family}")
        return await self.async_send_forward(
            payload,
            devcode=route.devcode,
            collector_addr=route.collector_addr,
        )

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
        connection = await self._active_connection_for_send()
        return await connection.async_send_collector(
            fcode=fcode,
            payload=payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=self._request_timeout,
        )

    async def _active_connection_for_send(self) -> _CollectorConnection:
        connection = self._connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return connection

        if self._listener is None:
            raise ConnectionError("collector_not_connected")

        claimed_session_id = self._resolve_claimed_session_id()
        if self._collector_ip or self._collector_pn or claimed_session_id:
            pending = await self._listener.pop_pending_socket_for_route(
                collector_ip=self._collector_ip,
                collector_pn=self._collector_pn,
                session_protocol=self._collector_session_protocol,
                session_id=claimed_session_id,
            )
            if pending is not None:
                return await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    heartbeat_interval=self._heartbeat_interval,
                    write_timeout=self._write_timeout,
                )

        if connection is None or not connection.connected:
            raise ConnectionError("collector_not_connected")

        return connection

    def _connection(self, *, create_placeholder: bool) -> _CollectorConnection | None:
        if self._listener is None:
            return None
        claimed_session_id = self._resolve_claimed_session_id()
        if claimed_session_id:
            connection = self._listener.payload_connection_for_session(
                claimed_session_id
            )
            if connection is not None:
                return connection
            if not self._collector_ip and not self._collector_pn:
                # A session-pinned-ONLY transport (the callback identity read):
                # the claimed socket either resolves or there is nothing this
                # transport may talk to. The "current connection" fallback below
                # hands back an arbitrary live socket -- exactly the
                # substitution a session claim exists to prevent. A claimed but
                # still-parked socket is activated by the pop path, not here.
                return None
        if self._collector_pn:
            connection = self._listener.ensure_connection(
                "",
                self._heartbeat_interval,
                self._write_timeout,
                self._collector_pn,
            )
            if connection is not None:
                return connection
        if create_placeholder:
            return self._listener.ensure_connection(
                self._collector_ip,
                self._heartbeat_interval,
                self._write_timeout,
                self._collector_pn,
            )
        if self._collector_ip:
            return self._listener.ensure_connection(
                self._collector_ip,
                self._heartbeat_interval,
                self._write_timeout,
                self._collector_pn,
            )
        return self._listener.current_connection(
            heartbeat_interval=self._heartbeat_interval,
            write_timeout=self._write_timeout,
        )
