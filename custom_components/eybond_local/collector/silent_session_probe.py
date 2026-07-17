"""The ONE narrow public boundary for identifying fully-silent sessions.

A first-contact socket that volunteers no bytes is invisible to the public
session inventory (PN-less sockets are deliberately not discovery candidates).
Two callers legitimately need to identify such a socket anyway, each holding
its own explicit wire authority:

* the callback identity transaction -- the USER's explicit bootstrap protocol
  selection for the exact silent session a previous attempt causally
  attributed;
* the controlled-reset recovery engine -- the wire observed on the TRUSTED
  live SessionHandle of the very session it rebooted.

Both go through this channel and nothing else: the shared-listener
acquire/release stays inside the collector layer, no listener internals leak
out, and the channel itself never infers a wire -- the protocol is always the
caller's typed authority. One call performs exactly ONE read-only identity
query (framed FC=2 parameter 2 / ``AT+DTUPN``) on exactly one session id; a
valid strong-PN reply is recorded in the listener inventory (making the
session visible to every normal path), anything else changes nothing.
"""

from __future__ import annotations

from contextlib import suppress
import logging
from typing import Any

logger = logging.getLogger(__name__)


class SilentSessionIdentityProbeChannel:
    """Session-id-pinned identity probing of silent sockets on ONE listener."""

    def __init__(self, *, host: str, port: int) -> None:
        self._host = str(host or "").strip() or "0.0.0.0"
        self._port = int(port or 0)
        self._listener: Any = None

    async def async_open(self) -> None:
        """Borrow the shared listener (refcounted; idempotent per channel)."""

        if self._listener is not None or self._port <= 0:
            return
        from .transport import _acquire_shared_listener

        try:
            self._listener = await _acquire_shared_listener(self._host, self._port)
        except Exception:
            logger.debug(
                "Silent-session probe channel could not borrow %s:%s",
                self._host,
                self._port,
                exc_info=True,
            )
            self._listener = None

    @property
    def available(self) -> bool:
        """Whether the channel actually holds an open listener.

        Distinct on purpose from a probe that RAN and produced no PN: a caller
        must be able to tell "the identity query could not even be attempted"
        (an environment/open failure) from "the query ran and the socket did
        not answer" so it can surface an honest, differentiated failure.
        """

        return self._listener is not None

    async def async_close(self) -> None:
        """Return the borrowed listener (idempotent)."""

        listener, self._listener = self._listener, None
        if listener is None:
            return
        from .transport import _release_shared_listener

        with suppress(Exception):
            await _release_shared_listener(listener)

    def snapshot_silent_session_ids(self) -> frozenset[str]:
        """Ids of live sessions that have volunteered NO identity yet."""

        listener = self._listener
        if listener is None:
            return frozenset()
        view = getattr(listener, "silent_pending_collector_sessions", None)
        if not callable(view):
            return frozenset()
        try:
            return frozenset(
                str(session.get("session_id") or "").strip()
                for session in view()
                if str(session.get("session_id") or "").strip()
            )
        except Exception:  # pragma: no cover - a diagnostics read must not break
            logger.debug("Silent session snapshot failed", exc_info=True)
            return frozenset()

    async def async_identify_exact_session(
        self,
        session_id: str,
        *,
        session_protocol: str,
    ) -> str:
        """ONE read-only identity query of one exact session on one wire.

        Returns the strong PN recorded by the listener, or ``""`` when the
        query RAN and the socket produced no strong PN. This method must be
        called only when :attr:`available` is True (a closed channel is a
        separate, honestly-distinguishable condition -- see that property).
        Never retries, never guesses, never falls back to another protocol.
        """

        listener = self._listener
        sid = str(session_id or "").strip()
        protocol = str(session_protocol or "").strip().lower()
        if listener is None or not sid or not protocol:
            return ""
        identify = getattr(listener, "async_identify_pending_session", None)
        if not callable(identify):
            return ""
        try:
            return str(
                await identify(sid, session_protocol=protocol) or ""
            ).strip()
        except Exception:
            logger.info(
                "Silent-session identity probe failed on %s", sid, exc_info=True
            )
            return ""


__all__ = ["SilentSessionIdentityProbeChannel"]
