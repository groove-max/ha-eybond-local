"""Explicit diagnostic wire overrides, pinned to the caller's current socket."""

from __future__ import annotations

from ...link_models import EybondLinkRoute, RawSerialLinkRoute
from .request_trace import SocketRequestTrace
from .send_ownership import SocketSendOwner


class BoundDiagnosticTransport:
    """No reconnect, fallback, wire learning or permanent metadata changes."""

    def __init__(self, connection, current_connection, *, wire: str, timeout: float) -> None:
        if wire not in ("fc4", "raw_modbus"):
            raise ValueError("diagnostic_wire_unsupported")
        if connection is None:
            raise ConnectionError("collector_not_connected")
        self._connection = connection
        self._current_connection = current_connection
        self._owner = SocketSendOwner.capture(connection)
        self._raw_session = connection._diagnostic_modbus
        self.wire = wire
        self._timeout = timeout
        self._trace: SocketRequestTrace | None = None

    def _check(self):
        if self._current_connection() is not self._connection:
            raise ConnectionError("collector_session_changed")
        self._owner.check(self._connection)

    @property
    def connected(self) -> bool:
        try:
            self._check()
            return True
        except ConnectionError:
            return False

    @property
    def diagnostic_received_hex(self) -> str:
        if self.wire == "raw_modbus" and self._trace is not None and self._trace.write_started:
            return self._raw_session.received.hex()
        return ""

    @property
    def diagnostic_failure_reason(self) -> str:
        if self.wire == "raw_modbus" and self._trace is not None and self._trace.write_started:
            return self._raw_session.failure_reason
        return ""

    @property
    def diagnostic_transport_progress(self) -> dict[str, object]:
        return self._trace.snapshot() if self._trace is not None else {}

    def select_payload_route(self, route, *, payload_family=""):
        self._check()
        if self.wire == "raw_modbus":
            if payload_family != "modbus_rtu":
                raise TypeError("diagnostic_raw_modbus_requires_modbus")
            return RawSerialLinkRoute(protocol="modbus_rtu")
        if not isinstance(route, EybondLinkRoute):
            raise TypeError("diagnostic_fc4_requires_eybond_route")
        return route

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        self._trace = trace = SocketRequestTrace()
        self._check()
        timeout = self._timeout if request_timeout is None else float(request_timeout)
        connection = self._connection
        if self.wire == "raw_modbus":
            if not isinstance(route, RawSerialLinkRoute) or route.protocol != "modbus_rtu":
                raise TypeError("diagnostic_raw_modbus_requires_modbus")
            return await self._raw_session.send(connection, self._owner, payload, timeout, trace=trace)
        if not isinstance(route, EybondLinkRoute):
            raise TypeError("diagnostic_fc4_requires_eybond_route")
        # Both implementations capture the socket owner before their first
        # await. An AT diagnostic must not adopt its observed framing mode.
        sender = getattr(connection, "async_send_framed_inverter_payload", None)
        if sender is not None:
            response = await sender(payload, devcode=route.devcode,
                                    collector_addr=route.collector_addr,
                                    request_timeout=timeout, learn_wire=False, trace=trace)
        else:
            _, response = await connection.async_send_collector(
                fcode=4, payload=payload, devcode=route.devcode,
                collector_addr=route.collector_addr, request_timeout=timeout, trace=trace,
            )
        self._owner.check_reply(connection)
        return response
