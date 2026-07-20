"""Session-pinned authoritative collector-identity reader (neutral, reusable).

THE one production implementation of "read this collector's full PN over
exactly ONE already-claimed session". Extracted from the callback identity
transaction so the inbound recovery verifier reuses the same reader instead of
growing a second wire switch or a second identity matcher. Users:

* ``connection.callback_identity`` -- the identity transaction's default reader;
* ``connection.recovery.verification`` -- the inbound recovery verifier's
  strong-identity probes (old socket before reboot, new socket after).

Both wires reuse the transports' claimed-session mechanism EXCLUSIVELY: the
transports are constructed with NO collector_ip and NO collector_pn, so the
only route they can resolve is the claimed session id -- never a socket picked
by peer IP, a PN index, or "the current connection". The two shapes reuse the
integration's existing reads:

* framed  -> the neutral ``CollectorWireManagementSession.query_collector_pn``
  (FC=2 parameter 2);
* at_text -> the DTUPN query (``AT+DTUPN``).

Both replies are also stamped into the listener inventory by the transport
itself (``fc2_parameter_2`` / ``at_dtupn``), which is how the session becomes
strongly identified for the session registry immediately afterwards.

The wire argument must be the LIVE NEGOTIATED wire (from
``negotiate_session_adapters`` over the observed session); an unknown/untrusted
wire is refused -- guessing a frame for a stranger's socket is exactly what
this module exists to prevent.
"""

from __future__ import annotations

from contextlib import suppress
import logging
from typing import Any

from ..connection.session_handle import WIRE_AT_TEXT, WIRE_FRAMED

logger = logging.getLogger(__name__)


class SessionPinnedIdentityReader:
    """Authoritative full-PN read, pinned to exactly one claimed session id."""

    def __init__(self, *, host: str, request_timeout: float = 5.0) -> None:
        self._host = host
        self._request_timeout = float(request_timeout)

    async def async_read_full_pn(
        self,
        *,
        session_id: str,
        session_protocol: str,
        listener_port: int,
        expected_pn: str = "",
    ) -> tuple[str, str]:
        """Return ``(full_pn, identity_source)``; ``("", "")`` when unreadable.

        ``expected_pn`` is advisory context only -- it never routes the read
        (routing by an expected PN could land the read on a DIFFERENT live
        socket of the same collector).
        """

        del expected_pn
        wire = str(session_protocol or "").strip().lower()
        if wire == WIRE_FRAMED:
            return (
                await self._async_read_framed(session_id, listener_port),
                "fc2_parameter_2",
            )
        if wire == WIRE_AT_TEXT:
            return (await self._async_read_at(session_id, listener_port), "at_dtupn")
        # Fail closed: an unknown/raw/untrusted wire is not something we may
        # guess at.
        logger.debug("Identity read skipped: untrusted wire %r", session_protocol)
        return ("", "")

    async def _async_read_framed(self, session_id: str, listener_port: int) -> str:
        from .collector_wire import CollectorWireManagementSession
        from .transport import SharedEybondTransport

        # collector_ip/collector_pn stay EMPTY on purpose: the claimed session
        # id must be the transport's only route (see the module docstring).
        transport = SharedEybondTransport(
            host=self._host,
            port=int(listener_port),
            request_timeout=self._request_timeout,
            heartbeat_interval=60.0,
            collector_ip="",
            collector_pn="",
        )
        transport.set_claimed_session_provider(lambda: session_id)
        # The NEUTRAL management session: FC=2 parameter 2 and nothing else.
        return await self._async_with_transport(
            transport,
            lambda: CollectorWireManagementSession(transport).query_collector_pn(),
        )

    async def _async_read_at(self, session_id: str, listener_port: int) -> str:
        from .transport import SharedCollectorAtTransport

        transport = SharedCollectorAtTransport(
            host=self._host,
            port=int(listener_port),
            request_timeout=self._request_timeout,
            collector_ip="",
            collector_pn="",
            collector_session_protocol=WIRE_AT_TEXT,
        )
        transport.set_claimed_session_provider(lambda: session_id)

        async def _query() -> str:
            response = await transport.async_query("DTUPN")
            return str(getattr(response, "value", "") or "").strip()

        return await self._async_with_transport(transport, _query)

    async def _async_with_transport(self, transport: Any, read) -> str:
        """Start, read, and ALWAYS stop -- on success, error and cancellation."""

        await transport.start()
        try:
            connected = await transport.wait_until_connected(
                timeout=self._request_timeout
            )
            if not connected:
                return ""
            return str(await read() or "").strip()
        finally:
            with suppress(Exception):
                await transport.stop()


__all__ = ["SessionPinnedIdentityReader"]
