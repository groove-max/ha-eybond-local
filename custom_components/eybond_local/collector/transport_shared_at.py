"""Public AT/raw transport facade over the shared listener."""

from __future__ import annotations

import asyncio
from typing import Any

from .at import CollectorAtResponse
from ..link_models import LinkRoute, RawSerialLinkRoute
from ..models import CollectorInfo
from .protocol import EybondHeader
from .transport_common import _bounded_write_timeout, _copy_collector_info
from .transport_connections import _CollectorAtConnection, _CollectorConnection
from .transport_listener import (
    _SharedEybondListener,
    _acquire_shared_at_listener,
    _release_shared_listener,
)

class SharedCollectorAtTransport:
    """One per-entry plain-AT transport facade backed by the shared TCP listener."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        request_timeout: float,
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
        self._request_timeout = float(request_timeout)
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._collector_ip = collector_ip
        self._collector_pn = str(collector_pn or "").strip()
        self._collector_session_protocol = str(collector_session_protocol or "").strip().lower()
        # Live negotiated wire (from the SessionHandle) set by the runtime. When
        # present it is authoritative over the persisted protocol for deciding
        # AT-vs-framed activation, so a stale persisted hint cannot mis-route a
        # claimed live session. Empty until a live session is negotiated.
        self._negotiated_wire = ""
        # Registry-mediated claim resolver (see SharedEybondTransport).
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

    @property
    def connected(self) -> bool:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return True

        connection = self._at_connection(create_placeholder=False)
        return connection.connected if connection is not None else False

    @property
    def collector_info(self) -> CollectorInfo:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return framed.collector_info

        connection = self._at_connection(create_placeholder=False)
        if connection is not None:
            return connection.collector_info
        if self._listener is not None:
            pending = self._listener._select_pending_socket(self._collector_ip)
            if pending is not None:
                return _copy_collector_info(CollectorInfo(remote_ip=pending.remote_ip))
        return _copy_collector_info(
            CollectorInfo(remote_ip=self._collector_ip, collector_pn=self._collector_pn)
        )

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_at_listener(
            self._host,
            self._port,
            self._collector_ip,
            self._collector_pn,
            self._collector_session_protocol,
        )
        self._at_connection(create_placeholder=bool(self._collector_ip))

    async def stop(self) -> None:
        if self._listener is None:
            return
        listener = self._listener
        self._listener = None
        await _release_shared_listener(
            listener,
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            collector_session_protocol=self._collector_session_protocol,
            close_at=True,
            unregister_at_owner=True,
            unregister_at_pn_owner=True,
            unregister_session_protocol_owner=True,
        )

    async def disconnect(self) -> None:
        connection = self._at_connection(create_placeholder=False)
        if connection is not None:
            await connection.disconnect()

    async def wait_until_connected(self, timeout: float) -> bool:
        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            listener = self._listener
            if listener is None:
                return False
            if not self._uses_at_text_session():
                framed = self._framed_connection(create_placeholder=False)
                if framed is not None and framed.connected:
                    return True

            connection = self._at_connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                return True

            claimed_session_id = self._resolve_claimed_session_id()
            if self._collector_ip or self._collector_pn or claimed_session_id:
                pending = await listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                    session_id=claimed_session_id,
                )
                if pending is not None:
                    if self._uses_at_text_session():
                        connection = await listener.activate_pending_at_connection(
                            pending,
                            collector_ip=self._collector_ip,
                            collector_pn=self._collector_pn,
                            write_timeout=self._write_timeout,
                            raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                            raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                            raw_passthrough_min_interval_ms=(
                                self._collector_raw_passthrough_min_interval_ms
                            ),
                        )
                        if connection.connected:
                            return True
                    else:
                        framed = await listener.activate_pending_connection(
                            pending,
                            collector_ip=self._collector_ip,
                            collector_pn=self._collector_pn,
                            heartbeat_interval=60.0,
                            write_timeout=self._write_timeout,
                        )
                        if framed.connected:
                            return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def async_query(self, command: str) -> CollectorAtResponse:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return await framed.async_query(command, request_timeout=self._request_timeout)

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_query(command, request_timeout=self._request_timeout)

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
                if self._uses_at_text_session():
                    connection = await self._listener.activate_pending_at_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        write_timeout=self._write_timeout,
                        raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                        raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                        raw_passthrough_min_interval_ms=(
                            self._collector_raw_passthrough_min_interval_ms
                        ),
                    )
                    return await connection.async_query(command, request_timeout=self._request_timeout)
                framed = await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    heartbeat_interval=60.0,
                    write_timeout=self._write_timeout,
                )
                return await framed.async_query(command, request_timeout=self._request_timeout)

        if connection is None:
            raise ConnectionError("collector_not_connected")

        if not connection.connected:
            raise ConnectionError("collector_not_connected")

        return await connection.async_query(command, request_timeout=self._request_timeout)

    async def async_query_bridge_hardware_version(
        self,
    ) -> tuple[EybondHeader, bytes]:
        """Read FC=2 parameter 6 through an AT-shaped bridge bootstrap session.

        This method is intentionally narrower than ``CollectorTransport``.  A
        plain AT cloud session is not a general FC transport; the only framed
        request allowed here is the positive ESP bridge identity token.
        """

        fcode = 2
        payload = b"\x06"
        devcode = 0
        collector_addr = 1

        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return await framed.async_send_collector(
                    fcode=fcode,
                    payload=payload,
                    devcode=devcode,
                    collector_addr=collector_addr,
                    request_timeout=self._request_timeout,
                )

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_send_bridge_identity_probe(
                fcode=fcode,
                payload=payload,
                devcode=devcode,
                collector_addr=collector_addr,
                request_timeout=self._request_timeout,
            )

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
                if self._uses_at_text_session():
                    connection = await self._listener.activate_pending_at_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        write_timeout=self._write_timeout,
                        raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                        raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                        raw_passthrough_min_interval_ms=(
                            self._collector_raw_passthrough_min_interval_ms
                        ),
                    )
                    return await connection.async_send_bridge_identity_probe(
                        fcode=fcode,
                        payload=payload,
                        devcode=devcode,
                        collector_addr=collector_addr,
                        request_timeout=self._request_timeout,
                    )
                framed = await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    heartbeat_interval=60.0,
                    write_timeout=self._write_timeout,
                )
                return await framed.async_send_collector(
                    fcode=fcode,
                    payload=payload,
                    devcode=devcode,
                    collector_addr=collector_addr,
                    request_timeout=self._request_timeout,
                )

        if connection is None:
            raise ConnectionError("collector_not_connected")

        if not connection.connected:
            raise ConnectionError("collector_not_connected")

        return await connection.async_send_bridge_identity_probe(
            fcode=fcode,
            payload=payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=self._request_timeout,
        )

    async def async_write(self, command: str, value: str) -> CollectorAtResponse:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return await framed.async_write(
                    command,
                    value,
                    request_timeout=self._request_timeout,
                )

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_write(
                command,
                value,
                request_timeout=self._request_timeout,
            )

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
                if self._uses_at_text_session():
                    connection = await self._listener.activate_pending_at_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        collector_pn=self._collector_pn,
                        write_timeout=self._write_timeout,
                        raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                        raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                        raw_passthrough_min_interval_ms=(
                            self._collector_raw_passthrough_min_interval_ms
                        ),
                    )
                    return await connection.async_write(
                        command,
                        value,
                        request_timeout=self._request_timeout,
                    )
                framed = await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    heartbeat_interval=60.0,
                    write_timeout=self._write_timeout,
                )
                return await framed.async_write(
                    command,
                    value,
                    request_timeout=self._request_timeout,
                )

        if connection is None:
            raise ConnectionError("collector_not_connected")

        if not connection.connected:
            raise ConnectionError("collector_not_connected")

        return await connection.async_write(
            command,
            value,
            request_timeout=self._request_timeout,
        )

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
        request_timeout: float | None = None,
    ) -> bytes:
        """Send one raw inverter payload over the active AT stream."""

        if not isinstance(route, RawSerialLinkRoute):
            raise TypeError(f"unsupported_link_route:{route.family}")
        if not self._uses_at_text_session():
            raise TypeError("raw_serial_route_requires_at_text_session")
        effective_request_timeout = (
            float(request_timeout)
            if request_timeout is not None
            else self._request_timeout
        )

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_send_raw_payload(
                payload,
                request_timeout=effective_request_timeout,
            )

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
                connection = await self._listener.activate_pending_at_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    write_timeout=self._write_timeout,
                    raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                    raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                    raw_passthrough_min_interval_ms=(
                        self._collector_raw_passthrough_min_interval_ms
                    ),
                )
                return await connection.async_send_raw_payload(
                    payload,
                    request_timeout=effective_request_timeout,
                )

        raise ConnectionError("collector_not_connected")

    def select_payload_route(
        self,
        route: LinkRoute,
        *,
        payload_family: str = "",
    ) -> LinkRoute:
        if self._uses_at_text_session():
            return RawSerialLinkRoute(protocol=payload_family)
        return route

    @property
    def listener_key(self) -> str:
        """Return a stable, public identity for the shared listener this uses."""

        return f"{self._host}:{self._port}"

    def set_negotiated_wire(self, wire: str) -> None:
        """Set the live negotiated wire (from the runtime's SessionHandle).

        ``"at_text"`` / ``"raw_tcp"`` / ``"eybond_framed"`` make the live session
        authoritative for activation; ``""`` clears it and restores the
        persisted fallback.
        """

        normalized = str(wire or "").strip().lower()
        if normalized == "framed":
            normalized = "eybond_framed"
        self._negotiated_wire = (
            normalized
            if normalized in ("at_text", "raw_tcp", "eybond_framed")
            else ""
        )

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

    def _uses_at_text_session(self) -> bool:
        # The live negotiated wire is authoritative: once the runtime has
        # negotiated the claimed session's wire (via the SessionHandle), the
        # persisted collector_session_protocol must NOT be a second source of
        # truth. The persisted value is consulted only as a fallback before any
        # live session has been observed.
        if self._negotiated_wire == "at_text":
            return True
        if self._negotiated_wire == "raw_tcp":
            return True
        if self._negotiated_wire == "eybond_framed":
            return False
        return self._collector_session_protocol == "at_text"

    def _at_connection(self, *, create_placeholder: bool) -> _CollectorAtConnection | None:
        if self._listener is None:
            return None
        claimed_session_id = self._resolve_claimed_session_id()
        if claimed_session_id:
            connection = self._listener.at_connection_for_session(claimed_session_id)
            if connection is not None:
                connection.set_raw_passthrough_bootstrap(
                    self._collector_raw_passthrough_bootstrap
                )
                connection.set_raw_passthrough_frame_format(
                    self._collector_raw_passthrough_frame_format
                )
                connection.set_raw_passthrough_min_interval_ms(
                    self._collector_raw_passthrough_min_interval_ms
                )
                return connection
            if not self._collector_ip and not self._collector_pn:
                # Session-pinned-ONLY transport (the callback identity read):
                # never substitute the "current" arbitrary AT socket for the
                # claimed one. A claimed-but-parked socket is activated by the
                # pop path, not resolved here.
                return None
        if self._collector_pn:
            connection = self._listener.ensure_at_connection(
                "",
                self._write_timeout,
                self._collector_pn,
                self._collector_raw_passthrough_bootstrap,
                self._collector_raw_passthrough_frame_format,
                self._collector_raw_passthrough_min_interval_ms,
            )
            if connection is not None:
                return connection
        if create_placeholder:
            return self._listener.ensure_at_connection(
                self._collector_ip,
                self._write_timeout,
                self._collector_pn,
                self._collector_raw_passthrough_bootstrap,
                self._collector_raw_passthrough_frame_format,
                self._collector_raw_passthrough_min_interval_ms,
            )
        if self._collector_ip:
            return self._listener.ensure_at_connection(
                self._collector_ip,
                self._write_timeout,
                self._collector_pn,
                self._collector_raw_passthrough_bootstrap,
                self._collector_raw_passthrough_frame_format,
                self._collector_raw_passthrough_min_interval_ms,
            )
        connection = self._listener.current_at_connection(write_timeout=self._write_timeout)
        if connection is not None:
            connection.set_raw_passthrough_bootstrap(self._collector_raw_passthrough_bootstrap)
            connection.set_raw_passthrough_frame_format(self._collector_raw_passthrough_frame_format)
            connection.set_raw_passthrough_min_interval_ms(
                self._collector_raw_passthrough_min_interval_ms
            )
        return connection

    def _framed_connection(self, *, create_placeholder: bool) -> _CollectorConnection | None:
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
                # Session-pinned-ONLY transport: same exclusivity as
                # _at_connection -- the claimed socket or nothing.
                return None
        if self._collector_pn:
            connection = self._listener.ensure_connection(
                "",
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
                collector_pn=self._collector_pn,
            )
            if connection is not None:
                return connection
        if create_placeholder:
            return self._listener.ensure_connection(
                self._collector_ip,
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
                collector_pn=self._collector_pn,
            )
        if self._collector_ip:
            return self._listener.ensure_connection(
                self._collector_ip,
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
                collector_pn=self._collector_pn,
            )
        return self._listener.current_connection(
            heartbeat_interval=60.0,
            write_timeout=self._write_timeout,
        )
