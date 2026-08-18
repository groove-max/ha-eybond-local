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

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import logging
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionObservation:
    """One live session the shared listener has accepted, keyed by session id.

    Carries ONLY the minimal identity fields the onboarding exact-session selector
    needs -- ``session_id``, ``collector_pn`` (``""`` while silent), the
    ``identity_source`` (``""`` / ``framed_heartbeat`` / ``fc2_parameter_2`` /
    ``at_dtupn``), the observed ``protocol_shape`` and lifecycle ``state``. It
    deliberately exposes NO peer IP, no bytes and no listener internals, so a
    caller can select the ONE fresh session of an attempt without ever making a
    peer-IP / order / prefix decision.
    """

    session_id: str
    collector_pn: str = ""
    identity_source: str = ""
    protocol_shape: str = ""
    state: str = ""


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

    def snapshot_session_observations(self) -> tuple[SessionObservation, ...]:
        """Every live session on this listener, by session id, as typed identity
        observations -- the UNION of the two public views: PN-less pending
        (silent) sockets AND already-identified sessions. This is what lets one
        selector treat a fresh silent, weak-heartbeat or strong session uniformly
        (by session id) instead of three parallel branches. No peer IP, no bytes.
        """

        listener = self._listener
        if listener is None:
            return ()
        observations: dict[str, SessionObservation] = {}
        silent = getattr(listener, "silent_pending_collector_sessions", None)
        if callable(silent):
            try:
                for session in silent():
                    session_id = str(session.get("session_id") or "").strip()
                    if session_id:
                        observations[session_id] = SessionObservation(
                            session_id=session_id,
                            state=str(session.get("state") or ""),
                        )
            except Exception:  # pragma: no cover - a diagnostics read must not break
                logger.debug("Silent observation snapshot failed", exc_info=True)
        identified = getattr(listener, "discovered_collector_sessions", None)
        if callable(identified):
            try:
                for session in identified():
                    session_id = str(session.get("session_id") or "").strip()
                    if not session_id:
                        continue
                    # An identified session supersedes the silent view for the same
                    # id (it has a PN/source); peer_ip in the raw dict is ignored.
                    observations[session_id] = SessionObservation(
                        session_id=session_id,
                        collector_pn=str(session.get("collector_pn") or ""),
                        identity_source=str(
                            session.get("collector_identity_source") or ""
                        ),
                        protocol_shape=str(session.get("protocol_shape") or ""),
                        state=str(session.get("state") or ""),
                    )
            except Exception:  # pragma: no cover - a diagnostics read must not break
                logger.debug("Identified observation snapshot failed", exc_info=True)
        return tuple(observations.values())

    async def async_identify_exact_session(
        self,
        session_id: str,
        *,
        session_protocol: str,
    ) -> str:
        """ONE read-only identity query of one exact session, gated to a STRONG PN.

        Returns a full PN ONLY when, AFTER the query, THIS exact session id carries
        a strong (``fc2_parameter_2`` / ``at_dtupn``) inventory identity; returns
        ``""`` in every other case. In particular the low-level route probe may
        fall back to a WEAK short ``framed_heartbeat`` PN -- a heartbeat can land
        during its initial read and the subsequent FC=2 upgrade can then time out
        or error -- and that weak observation must NOT cross this strong-probe
        boundary and masquerade as an exact-session strong identity. The weak PN is
        deliberately LEFT INTACT in the listener inventory (it is an honest weak
        fact for other route callers); it is simply not returned here.

        Must be called only when :attr:`available` is True (a closed channel is a
        separate, honestly-distinguishable condition -- see that property). Never
        retries, never guesses, never falls back to another protocol; cancellation
        is never swallowed, and a diagnostics read failure fails closed to ``""``.
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
            low_level_pn = str(
                await identify(sid, session_protocol=protocol) or ""
            ).strip()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.info(
                "Silent-session identity probe failed on %s", sid, exc_info=True
            )
            return ""
        return self._strong_identity_for_session(sid, low_level_pn)

    def _strong_identity_for_session(self, session_id: str, low_level_pn: str) -> str:
        """Gate a low-level probe result to a STRONG, same-identity observation.

        Re-reads THIS exact session id from the channel's OWN public observation
        view (never peer IP, order or prefix) and returns its PN only when the
        post-probe inventory identity is strong AND -- if the probe returned any PN
        -- the same identity (``pn_is_same_identity``). A framed-heartbeat / empty /
        unknown / foreign source, a missing session, or a diagnostics read failure
        all fail closed to ``""``.
        """

        from ..collector_identity import (
            identity_source_is_strong,
            pn_is_same_identity,
        )

        try:
            observations = self.snapshot_session_observations()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - a diagnostics read must not break
            logger.debug(
                "Post-probe observation snapshot failed for %s",
                session_id,
                exc_info=True,
            )
            return ""
        for obs in observations:
            if obs.session_id != session_id:
                continue
            pn = str(obs.collector_pn or "").strip()
            if not pn or not identity_source_is_strong(obs.identity_source):
                return ""
            low = str(low_level_pn or "").strip()
            if low and not pn_is_same_identity(low, pn):
                return ""
            return pn
        return ""


__all__ = ["SessionObservation", "SilentSessionIdentityProbeChannel"]
