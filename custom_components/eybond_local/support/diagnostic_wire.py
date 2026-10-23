"""Run-local route selection and bounded, exportable payload observations."""

from __future__ import annotations

import asyncio
import time

from ..link_transport import async_send_payload, select_payload_route


class DiagnosticTraceTransport:
    """Observe requests without selecting a profile or retrying any operation."""

    def __init__(self, transport, *, wire: str, timeout: float) -> None:
        self.wire = wire
        self.events: list[dict] = []
        self.dropped_events = 0
        self.line = 0
        if wire == "auto":
            self._transport = transport
        else:
            factory = getattr(transport, "create_diagnostic_transport", None)
            if not callable(factory):
                raise ValueError("diagnostic_wire_override_not_supported")
            self._transport = factory(wire, timeout=timeout)

    @property
    def connected(self):
        return self._transport.connected

    def select_payload_route(self, route, *, payload_family=""):
        return select_payload_route(self._transport, route, payload_family=payload_family)

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        started = time.monotonic()
        event = {
            "line": self.line,
            "wire": self.wire, "route_family": route.family,
            "request_hex": payload[:512].hex(), "request_size": len(payload),
            "request_truncated": len(payload) > 512,
        }
        if hasattr(route, "devcode"):
            event.update(devcode=route.devcode, collector_addr=route.collector_addr)
        try:
            result = await async_send_payload(self._transport, payload, route=route,
                                              request_timeout=request_timeout)
            event.update(response_hex=result[:512].hex(), response_size=len(result),
                         response_truncated=len(result) > 512, status="reply")
            return result
        except BaseException as exc:
            event.update(status="cancelled" if isinstance(exc, asyncio.CancelledError) else "error",
                         error_type=type(exc).__name__)
            received = getattr(self._transport, "diagnostic_received_hex", "")
            if received:
                event["received_fragment_hex"] = received[:1024]
            reason = getattr(self._transport, "diagnostic_failure_reason", "")
            if reason:
                event["reason"] = reason
            # Explicit transport diagnostics make one attempt, not the normal
            # driver's retry policy. Raw timeout already fenced its socket.
            if isinstance(exc, asyncio.TimeoutError) and self.wire != "auto":
                raise ConnectionError("diagnostic_request_timeout") from exc
            raise
        finally:
            event["duration_ms"] = int((time.monotonic() - started) * 1000)
            if len(self.events) < 128:
                self.events.append(event)
            else:
                self.dropped_events += 1
