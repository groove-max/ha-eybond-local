"""The ONE public cold-bootstrap boundary (Batch 8B.1).

The degraded-repair transaction and the config-flow repair step must NOT own
listener/transport internals, must NOT pick a wire themselves, and must NOT
re-implement the registry session projection or the production trigger sender.
This class is the single collector-layer boundary that owns all of it, so the
higher layers hold none of it:

* shared-listener lifecycle -- borrow/return the refcounted shared listener;
* exact-session identity query -- ONE read-only FC=2 / ``AT+DTUPN`` probe on
  exactly one session id, through the narrow
  :class:`SilentSessionIdentityProbeChannel`;
* the NEGOTIATED-WIRE authority -- the wire for a candidate socket may come
  ONLY from (A) the live negotiated ``SessionHandle`` of that exact observed
  session, or (B) an already-validated PN-bound
  :class:`ConfirmedSessionProtocolEvidence` from entry data. Cloud family,
  hostname, collector kind, peer IP, an expected identity or any persisted
  hint can never select a wire here;
* the production trigger sender -- one ledger-recorded ``set>server`` send;
* the registry session projection -- the per-socket observation the caller
  matches on, derived from the registry's own strong/weak verdict.

Nothing here decides ownership, causality or matching: those stay in the
transaction. This boundary is pure I/O + the wire authority.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

logger = logging.getLogger(__name__)

# The only two wires an authoritative identity read can ride.
_READABLE_WIRES = frozenset({"eybond_framed", "at_text"})


@dataclass(frozen=True, slots=True)
class ExactSessionRead:
    """The typed result of one exact-session identity read.

    ``wire_available`` distinguishes "no trusted wire authority existed, so NO
    identity IO was attempted" from "the read ran and produced no strong PN".
    The transaction needs that distinction to fail closed with the honest
    ``wire_unavailable`` reason instead of a generic timeout.
    """

    wire_available: bool
    session_protocol: str = ""
    collector_pn: str = ""


class CallbackBootstrapChannel:
    """Public cold-bootstrap I/O boundary bound to ONE listener + one entry."""

    def __init__(
        self,
        *,
        registry: Any,
        host: str,
        port: int,
        entry_data: Mapping[str, Any] | None = None,
        entry_options: Mapping[str, Any] | None = None,
        entry_pn: str = "",
        trigger_timeout: float = 5.0,
    ) -> None:
        from ..connection.callback_ledger import get_callback_trigger_ledger
        from .silent_session_probe import SilentSessionIdentityProbeChannel

        self._registry = registry
        self._entry_data = dict(entry_data or {})
        self._entry_options = dict(entry_options or {})
        self._entry_pn = str(entry_pn or "").strip()
        self._port = int(port or 0)
        self._probe = SilentSessionIdentityProbeChannel(host=host, port=port)
        self._sender = _BootstrapTriggerSender(timeout=trigger_timeout)
        # THE one causality authority: the exact process-global ledger the sender
        # physically records through (``async_send_callback_trigger`` funnels
        # every send into this ledger's ``callback_send_scope``). The transaction
        # opens its causality lease on THIS object, so the window that admits the
        # send and the window that counts it can never diverge.
        self._ledger = get_callback_trigger_ledger()

    # -- trigger sender (also reused by the Phase-B recovery transaction) ----
    @property
    def trigger_sender(self) -> Any:
        return self._sender

    @property
    def ledger(self) -> Any:
        """The callback ledger the sender records through (one authority)."""

        return self._ledger

    # -- listener lifecycle --------------------------------------------------
    async def async_open(self) -> None:
        await self._probe.async_open()

    async def async_close(self) -> None:
        await self._probe.async_close()

    @property
    def listener_available(self) -> bool:
        return self._probe.available

    # -- the UNIFIED per-socket projection -----------------------------------
    def sessions(self) -> tuple[dict[str, Any], ...]:
        """Every live socket the transaction must consider, deduped by id.

        Two disjoint sources are merged so a FULLY-SILENT (PN-less) callback
        socket is not invisible to the transaction:

        * PN-bearing observations from the registry -- the derived
          ``has_strong_identity`` verdict and the ``raw`` observation the wire
          authority negotiates from;
        * live PN-less pending sockets from the shared listener (via the public
          silent-probe boundary), emitted as SYNTHETIC records that carry ONLY a
          session id and this channel's listener port -- no wire guess, no
          expected protocol, no fabricated identity, no peer IP.

        Dedup is strict by ``session_id`` and the registry observation ALWAYS
        wins: once a probe enriches a silent socket it appears in the registry
        with an identity, so the synthetic record is dropped and one id never
        appears twice.
        """

        observed = tuple(
            {
                "session_id": session.session_id,
                "collector_pn": session.collector_pn,
                "state": session.state,
                "has_strong_identity": session.has_strong_identity,
                "collector_identity_source": session.identity_source,
                "listener_port": session.listener_port,
                "raw": dict(session.raw),
            }
            for session in self._registry.observed_sessions_per_socket()
        )
        seen = {row["session_id"] for row in observed}
        silent = tuple(
            {
                "session_id": sid,
                "collector_pn": "",
                "state": "",
                "has_strong_identity": False,
                "collector_identity_source": "",
                "listener_port": self._port,
                # No wire/protocol/identity hint: a silent socket's wire can come
                # only from confirmed PN-bound evidence (authority B).
                "raw": {"session_id": sid, "listener_port": self._port},
            }
            for sid in self._probe.snapshot_silent_session_ids()
            if sid and sid not in seen
        )
        return observed + silent

    # -- one ledger-recorded set>server --------------------------------------
    async def async_send_trigger(self, route: Any) -> None:
        await self._sender.async_send(route)

    # -- exact-session identity read under the wire authority ----------------
    async def async_read_exact_session_identity(
        self, session: Mapping[str, Any]
    ) -> ExactSessionRead:
        """Read ONE exact session's strong PN, or report no trusted wire.

        The wire is resolved by the two-source authority below. If neither
        source yields a readable wire, NO identity IO is attempted and
        ``wire_available`` is False.
        """

        session_id = str(session.get("session_id") or "").strip()
        wire = self._resolve_wire(session)
        if not session_id or not wire:
            return ExactSessionRead(wire_available=False)
        pn = await self._probe.async_identify_exact_session(
            session_id, session_protocol=wire
        )
        return ExactSessionRead(
            wire_available=True, session_protocol=wire, collector_pn=str(pn or "").strip()
        )

    def resolve_wire(self, session: Mapping[str, Any]) -> str:
        """Public view of the wire authority verdict (for guards/diagnostics)."""

        return self._resolve_wire(session)

    def _resolve_wire(self, session: Mapping[str, Any]) -> str:
        """Return the readable transport wire, or "" (fail-closed).

        A. the LIVE negotiated SessionHandle of this exact observed session --
           observed live wire, no wire conflict, a framed/AT readable wire; or
        B. an already-validated PN-bound ConfirmedSessionProtocolEvidence.

        Nothing else (expected identity, cloud family, hostname, collector kind,
        peer IP, a raw ``protocol_shape``/``session_protocol`` field taken on its
        own) may select a wire.
        """

        # A -- live negotiated handle of the exact observed session.
        from ..connection.session_handle import negotiate_session_adapters

        raw = session.get("raw")
        observed = raw if isinstance(raw, Mapping) else session
        handle = negotiate_session_adapters(observed)
        if (
            handle.observed
            and not handle.conflict
            and handle.transport_wire in _READABLE_WIRES
        ):
            return handle.transport_wire

        # B -- validated PN-bound confirmed evidence from THIS entry.
        from ..connection.confirmed_session_protocol import (
            ConfirmedSessionProtocolEvidence,
        )

        evidence = ConfirmedSessionProtocolEvidence.from_entry(
            self._entry_data, self._entry_options, entry_pn=self._entry_pn
        )
        if evidence is not None and evidence.protocol in _READABLE_WIRES:
            return evidence.protocol
        return ""


class _BootstrapTriggerSender:
    """One ledger-recorded ``set>server`` send, in the collector layer.

    Funnels through ``collector.discovery.async_send_callback_trigger`` -- the
    ONE choke point that wraps every send in ``ledger.callback_send_scope``, so
    a send inside a held causality lease is attributed to that attempt and a
    send outside every window is refused. ``async_send(route)`` matches the
    ``CallbackRecoveryTriggerSender`` shape so the Phase-B recovery transaction
    can reuse the exact same sender.
    """

    def __init__(self, *, timeout: float) -> None:
        self._timeout = float(timeout)

    async def async_send(self, route: Any) -> None:
        from .discovery import async_send_callback_trigger

        await async_send_callback_trigger(
            bind_ip=route.bind_ip,
            advertised_server_ip=route.advertised_ha_host,
            advertised_server_port=int(route.advertised_ha_port),
            target_ip=route.trigger_target_ip,
            udp_port=int(route.trigger_udp_port),
            timeout=self._timeout,
            source="degraded_repair_bootstrap",
        )


__all__ = [
    "CallbackBootstrapChannel",
    "ExactSessionRead",
]
