"""One reusable callback transaction that establishes LINK + IDENTITY only.

This is the single place where "make the collector dial in and prove which
collector it is" happens. Every active callback path -- manual onboarding, the
manual retry, reconfigure repair, and the pending entry's bounded attempt --
runs exactly this transaction, so none of them re-assembles the proof.

Why it exists
-------------
The callback paths used to establish the TCP session and then run the full
``detector.async_auto_detect`` driver sweep BEFORE any durable identity was
confirmed. Identity was inferred afterwards from whatever PN that sweep happened
to surface. The sweep costs tens of seconds, fans out extra UDP probes, and
routinely outlives the very session it is meant to identify -- so the attempt
ended in ``callback_timeout``/``callback_trigger_interference`` and the caller
looped. Identity is cheap and authoritative: one read on the socket the
collector just opened. It must not be held hostage to driver detection.

Scope -- deliberately narrow
----------------------------
This transaction NEVER detects an inverter, never selects a driver, never runs
a link sweep and never touches PollPolicy/endpoint ownership/provider evidence.
It answers exactly one question: *which collector is on the other end of the
session our trigger caused?* Whatever a caller wants to detect afterwards is the
caller's business, and by then the identity is certified and the session claimed.

The flow, in order:

1. open ONE attributed attempt (per-attempt trigger accounting);
2. snapshot the observed-session baseline;
3. send exactly ONE UDP callback trigger (``callback_on_demand``) or ZERO
   (``inbound`` -- Home Assistant never dials out);
4. wait, bounded, for a NEW session that is not in the baseline;
5. transient-claim exactly that ``session_id``;
6. decide the live wire from the OBSERVED session alone;
7. read the full PN authoritatively -- framed: FC=2 parameter 2; AT: DTUPN;
8. reconcile the identity through the shared matcher's rules;
9. promote the claim to the full PN;
10. prepare the certified handoff;
11. return a typed outcome;
12. on ANY failure or cancellation, release the transient ownership.

Identity is the durable full PN and nothing else. Peer IP, hostname, endpoint,
collector kind, cloud family and a persisted "expected protocol" are never
consulted -- the wire comes from what the socket actually did.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import logging
from typing import Any, Protocol
import uuid

from ..connection.callback_ledger import (
    CallbackCausalityBusyError,
    get_callback_trigger_ledger,
)
from ..connection.session_handle import (
    WIRE_AT_TEXT,
    WIRE_FRAMED,
    negotiate_session_adapters,
)
from ..connection.session_registry import SESSION_STATE_CLOSED, prefer_full_pn
from ..const import (
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
)
from .callback_matching import (
    MATCH_TIMEOUT,
    match_callback_answer,
)
from .timeouts import DEFAULT_ONBOARDING_TIMEOUT_POLICY

logger = logging.getLogger(__name__)

# Typed outcomes. These are translation keys / status values, never raw text.
IDENTITY_OK = ""
IDENTITY_TIMEOUT = "callback_timeout"
IDENTITY_MISMATCH = "callback_identity_mismatch"
IDENTITY_AMBIGUOUS = "callback_identity_ambiguous"
IDENTITY_TRIGGER_INTERFERENCE = "callback_trigger_interference"
# Distinct from interference ON PURPOSE: "we never got a datagram out" is our own
# failure (an inhibited window, a socket error), not evidence that somebody else
# disturbed us. Calling it interference sent users looking for a phantom
# competing flow.
IDENTITY_TRIGGER_NOT_SENT = "callback_trigger_not_sent"
IDENTITY_CONFLICT = "callback_identity_conflict"
# The session opened, but the authoritative read could not produce a full PN.
IDENTITY_UNVERIFIED = "callback_identity_unverified"
IDENTITY_TARGET_REQUIRED = "callback_target_required"

# Budgets live in the ONE onboarding policy, not as magic numbers here. A request
# may override them (tests), but production always resolves from the policy.
_SESSION_POLL_INTERVAL = 0.25


@dataclass(frozen=True, slots=True)
class CallbackIdentityRequest:
    """Everything ONE identity transaction needs. No driver/detection inputs."""

    server_ip: str
    tcp_port: int
    udp_port: int
    # Where the single trigger goes. Required for callback_on_demand; unused (and
    # never dialed) for inbound. It is a TARGET, never identity evidence.
    target_ip: str = ""
    strategy: str = CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
    # A previously-known identity this attempt must match (passive discovery, or
    # the PN an entry already stores). Empty = whatever answers our trigger.
    expected_pn: str = ""
    # A socket known to pre-date the attempt (the passively observed one).
    old_session_id: str = ""
    owner_prefix: str = "callback_identity"
    # 0 = resolve from DEFAULT_ONBOARDING_TIMEOUT_POLICY at call time. An explicit
    # value is a test override; production never passes one.
    session_wait_timeout: float = 0.0
    lease_wait_timeout: float = 0.0


@dataclass(frozen=True, slots=True)
class CallbackIdentityOutcome:
    """Typed result of ONE identity transaction.

    IDENTITY PROOF ONLY -- never a recovery proof. A certified outcome states
    exactly: *the live session ``session_id`` on wire ``session_protocol``
    belongs to the collector with full PN ``collector_pn``, and a registry
    handoff is prepared under ``handoff_owner``*. It deliberately proves
    nothing about the future:

    * NOT that the collector can be reached again after this session is lost;
    * NOT that a callback trigger is a working recovery route;
    * NOT that the collector is "inbound".

    Accordingly this outcome must never, by itself, be turned into
    ``connection_strategy`` / ``connection_strategy_evidence`` writes, endpoint
    writes, or any endpoint-ownership change. Recovery is a SEPARATE proof
    (the future RecoveryContract); nothing in this module produces one.
    """

    result: str
    collector_pn: str = ""
    session_id: str = ""
    session_protocol: str = ""
    identity_source: str = ""
    # The registry owner holding the PREPARED handoff. The caller passes this to
    # promotion/entry creation; it is the only thing that can certify the PN.
    handoff_owner: str = ""

    @property
    def identity_certified(self) -> bool:
        """The session<->full-PN binding is certified and a handoff is prepared.

        Named deliberately: this certifies IDENTITY (see the class docstring),
        never a recovery route. The old name ``confirmed`` read as "the callback
        way of (re)connecting is confirmed", which is a claim this transaction
        cannot make.
        """

        return (
            self.result == IDENTITY_OK
            and bool(self.collector_pn)
            and bool(self.handoff_owner)
        )


class CallbackIdentityReader(Protocol):
    """Reads a collector's full PN over ONE already-claimed session."""

    async def async_read_full_pn(
        self,
        *,
        session_id: str,
        session_protocol: str,
        listener_port: int,
        expected_pn: str = "",
    ) -> tuple[str, str]:
        """Return ``(full_pn, identity_source)``; ``("", "")`` when unreadable."""


class CallbackTriggerSender(Protocol):
    """Sends exactly one UDP callback trigger."""

    async def async_send(self, request: CallbackIdentityRequest) -> None: ...


class _ProductionTriggerSender:
    """The one production trigger facade (records itself in the shared ledger)."""

    async def async_send(self, request: CallbackIdentityRequest) -> None:
        from ..collector.discovery import async_send_callback_trigger

        await async_send_callback_trigger(
            bind_ip=request.server_ip,
            advertised_server_ip=request.server_ip,
            advertised_server_port=int(request.tcp_port),
            target_ip=request.target_ip,
            udp_port=int(request.udp_port),
            timeout=DEFAULT_ONBOARDING_TIMEOUT_POLICY.discovery_timeout,
            source="callback_identity_transaction",
        )


class _SessionPinnedIdentityReader:
    """Authoritative full-PN read, pinned to exactly one claimed session id.

    Both wires reuse the transports' claimed-session mechanism EXCLUSIVELY: the
    transports are constructed with NO collector_ip and NO collector_pn, so the
    only route they can resolve is the claimed session id -- never a socket
    picked by peer IP, a PN index, or "the current connection". (Passing the
    caller's ``expected_pn`` as the transport's ``collector_pn`` would re-open
    PN routing: a second live socket of the same collector could then serve the
    read, and the certified session<->PN binding would be about the wrong
    socket.) The two shapes reuse the integration's existing reads:

    * framed  -> the neutral ``CollectorWireManagementSession.query_collector_pn``
      (FC=2 parameter 2);
    * at_text -> the DTUPN query (``AT+DTUPN``).

    Both replies are also stamped into the listener inventory by the transport
    itself (``fc2_parameter_2`` / ``at_dtupn``), which is how the session becomes
    strongly identified for the shared matcher immediately afterwards.
    """

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
        wire = str(session_protocol or "").strip().lower()
        if wire == WIRE_FRAMED:
            return (await self._async_read_framed(session_id, listener_port, expected_pn), "fc2_parameter_2")
        if wire == WIRE_AT_TEXT:
            return (await self._async_read_at(session_id, listener_port, expected_pn), "at_dtupn")
        # Fail closed: an unknown/raw/untrusted wire is not something we may guess
        # at. Guessing here is exactly how a wrong frame gets written to a
        # stranger's socket.
        logger.debug("Identity read skipped: untrusted wire %r", session_protocol)
        return ("", "")

    async def _async_read_framed(
        self, session_id: str, listener_port: int, expected_pn: str
    ) -> str:
        from ..collector.collector_wire import CollectorWireManagementSession
        from ..collector.transport import SharedEybondTransport

        # collector_ip/collector_pn stay EMPTY on purpose: the claimed session
        # id must be the transport's only route (see the class docstring).
        transport = SharedEybondTransport(
            host=self._host,
            port=int(listener_port),
            request_timeout=self._request_timeout,
            heartbeat_interval=60.0,
            collector_ip="",
            collector_pn="",
        )
        transport.set_claimed_session_provider(lambda: session_id)
        # The NEUTRAL management session: FC=2 parameter 2 and nothing else. The
        # SmartESS subclass would drag provider/cloud/catalog concerns into a
        # module that must know only "collector" and "wire".
        return await self._async_with_transport(
            transport,
            lambda: CollectorWireManagementSession(transport).query_collector_pn(),
        )

    async def _async_read_at(
        self, session_id: str, listener_port: int, expected_pn: str
    ) -> str:
        from ..collector.transport import SharedCollectorAtTransport

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


def _session_views(hass: Any) -> tuple[dict[str, Any], ...]:
    """The registry's PUBLIC per-socket view. No listener internals."""

    registry = _registry(hass)
    if registry is None:
        return ()
    try:
        sessions = registry.observed_sessions_per_socket()
    except Exception:  # pragma: no cover - a diagnostics read must not break us
        logger.debug("Session scan failed", exc_info=True)
        return ()
    return tuple(
        {
            "session_id": str(getattr(session, "session_id", "") or ""),
            "collector_pn": str(getattr(session, "collector_pn", "") or ""),
            # The registry's NORMALIZED state, which is what the shared matcher
            # takes (same shape every other caller hands it).
            "state": str(getattr(session, "state", "") or ""),
            # The listener inventory mapping, verbatim. This is the documented
            # input of negotiate_session_adapters; the registry normalizes
            # routed_framed/routed_at_text down to "active", which would erase the
            # very evidence the negotiator runs on.
            "raw": dict(getattr(session, "raw", None) or {}),
            "listener_port": int(getattr(session, "listener_port", 0) or 0),
            "has_strong_identity": bool(
                getattr(session, "has_strong_identity", False)
            ),
        }
        for session in sessions
    )


def _registry(hass: Any):
    from ..passive_discovery import get_callback_session_registry

    return get_callback_session_registry(hass)


def _live_wire(session: dict[str, Any]) -> str:
    """Decide the live wire through the SINGLE negotiation authority.

    ``negotiate_session_adapters`` owns every wire rule there is -- untrusted
    lifecycle states (waiting/parked/route_identity_mismatch/closed_no_payload)
    never yield a wire, a state-vs-shape contradiction is reported as a conflict
    rather than silently resolved, a sniffed byte shape never overrides an
    untrusted lifecycle state, and a persisted/expected protocol is not live
    evidence. Re-deriving any of that here would be a second resolver that could
    drift from the first; this module must have no opinion of its own.

    Returns "" for anything we may not write to.
    """

    handle = negotiate_session_adapters(session.get("raw"))
    if not handle.observed or handle.conflict:
        # Fail closed: an unobserved or self-contradicting socket is not
        # something to guess a frame for.
        return ""
    if handle.uses_framed_wire:
        return WIRE_FRAMED
    if handle.uses_at_text_wire:
        return WIRE_AT_TEXT
    return ""


def _is_live(session: dict[str, Any]) -> bool:
    """Is this socket still open, per the REGISTRY's lifecycle verdict?

    Only "is it closed" is asked here, using the registry's own normalized state
    constant. Whether an OPEN socket may be written to at all -- untrusted,
    parked, waiting, route-mismatched, conflicting -- is a wire-trust question,
    and that belongs entirely to negotiate_session_adapters (see _live_wire).
    Restating any of those states here would be a second rule set.
    """

    return not str(session.get("state") or "").strip().lower().startswith(
        SESSION_STATE_CLOSED
    )


async def _async_wait_for_new_session(
    hass: Any,
    *,
    baseline: frozenset[str],
    old_session_id: str,
    timeout: float,
) -> dict[str, Any] | None:
    """Wait, bounded, for ONE new live session that is not in the baseline.

    No detection, no identity requirement: a socket that just opened has not
    volunteered its PN yet, and waiting for it to do so is what used to burn the
    session. We only need something to read from.
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout))
    while True:
        fresh = [
            session
            for session in _session_views(hass)
            if session["session_id"]
            and session["session_id"] not in baseline
            and session["session_id"] != old_session_id
            and _is_live(session)
        ]
        if fresh:
            return fresh[0] if len(fresh) == 1 else _prefer_readable(fresh)
        if loop.time() >= deadline:
            return None
        await asyncio.sleep(_SESSION_POLL_INTERVAL)


def _prefer_readable(sessions: list[dict[str, Any]]) -> dict[str, Any]:
    """Pick the session we can actually read from, among several new ones.

    Ambiguity is NOT resolved here -- the shared matcher does that after the read,
    on identities. This only avoids picking a socket whose wire is untrusted when
    a readable one is available.
    """

    for session in sessions:
        if _live_wire(session):
            return session
    return sessions[0]


async def async_run_callback_identity_transaction(
    hass: Any,
    request: CallbackIdentityRequest,
    *,
    reader: CallbackIdentityReader | None = None,
    sender: CallbackTriggerSender | None = None,
) -> CallbackIdentityOutcome:
    """Establish link + identity for ONE collector. Never detects a driver.

    Returns a typed outcome. On success the caller holds a registry-certified
    prepared handoff under ``handoff_owner``; on ANY other path this transaction
    owns nothing -- the transient claim is released before returning or
    propagating.
    """

    strategy = str(request.strategy or "").strip() or CONNECTION_STRATEGY_INBOUND
    if strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND and not str(
        request.target_ip or ""
    ).strip():
        return CallbackIdentityOutcome(result=IDENTITY_TARGET_REQUIRED)

    owner = f"{request.owner_prefix}:{uuid.uuid4().hex}"
    ledger = get_callback_trigger_ledger()
    from ..passive_discovery import active_callback_probe_scope

    with active_callback_probe_scope(hass, owner) as retained_sessions:
        try:
            # EXCLUSIVE causality, taken BEFORE the baseline and held through the
            # prepared handoff. Concurrent correct attempts queue here and each
            # then succeeds on its own clean window -- they no longer overlap and
            # spoil each other.
            async with ledger.causality_lease(
                owner,
                timeout=(
                    request.lease_wait_timeout
                    or DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_causality_lease_wait
                ),
            ) as attempt:
                outcome = await _async_run_attempt(
                    hass,
                    request,
                    owner=owner,
                    attempt=attempt,
                    strategy=strategy,
                    reader=reader,
                    sender=sender,
                )
        except CallbackCausalityBusyError:
            # Honest: we never got a window, so we never sent our sequence.
            logger.info("Callback attempt %s could not acquire causality", owner)
            return CallbackIdentityOutcome(result=IDENTITY_TRIGGER_NOT_SENT)
        if outcome.identity_certified and outcome.session_id:
            retained_sessions.add(outcome.session_id)
        return outcome


async def _async_run_attempt(
    hass: Any,
    request: CallbackIdentityRequest,
    *,
    owner: str,
    attempt: Any,
    strategy: str,
    reader: CallbackIdentityReader | None,
    sender: CallbackTriggerSender | None,
) -> CallbackIdentityOutcome:
    """The attempt body. Owns claim cleanup on every exit path."""

    registry = _registry(hass)
    if registry is None:
        return CallbackIdentityOutcome(result=IDENTITY_TIMEOUT)

    baseline = frozenset(
        session["session_id"] for session in _session_views(hass) if session["session_id"]
    )

    # --- 3. exactly one trigger, or none -------------------------------------
    expected_sends = 1 if strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND else 0
    if expected_sends:
        try:
            await (sender or _ProductionTriggerSender()).async_send(request)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("Callback trigger could not be sent: %s", exc)
    if attempt.own_sends != expected_sends:
        # Honest and specific. inbound declares zero and must have sent zero; a
        # callback attempt that never got its datagram out is OUR failure, not
        # somebody else's interference.
        logger.info(
            "Callback attempt %s sent %d of %d own triggers",
            owner,
            attempt.own_sends,
            expected_sends,
        )
        return CallbackIdentityOutcome(result=IDENTITY_TRIGGER_NOT_SENT)

    claimed = False
    try:
        # --- 4. wait for a socket -------------------------------------------
        session = await _async_wait_for_new_session(
            hass,
            baseline=baseline,
            old_session_id=str(request.old_session_id or "").strip(),
            timeout=(
                request.session_wait_timeout
                or DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_identity_session_wait
            ),
        )
        if session is None:
            return CallbackIdentityOutcome(result=IDENTITY_TIMEOUT)

        # Provenance is settled BEFORE we take ownership. A trigger somebody else
        # fired during our window means a session that appeared is not
        # attributable to us -- so we must not claim it, read it, or race another
        # attempt for it. (The shared matcher re-checks this below; this is the
        # gate that keeps us from touching a socket we cannot account for.)
        if attempt.foreign_sends:
            logger.info(
                "Callback attempt %s saw %d foreign trigger(s); not attributable",
                owner,
                attempt.foreign_sends,
            )
            return CallbackIdentityOutcome(result=IDENTITY_TRIGGER_INTERFERENCE)

        session_id = session["session_id"]

        # --- 5. own exactly that socket -------------------------------------
        try:
            registry.claim_session(owner, session_id=session_id)
            claimed = True
        except ValueError as exc:
            logger.info("Callback session %s already claimed: %s", session_id, exc)
            return CallbackIdentityOutcome(result=IDENTITY_CONFLICT)

        # --- 6. live wire, from the observation alone ------------------------
        wire = _live_wire(session)
        if not wire:
            return CallbackIdentityOutcome(result=IDENTITY_UNVERIFIED)

        # --- 7. authoritative read ------------------------------------------
        read = reader or _SessionPinnedIdentityReader(host=request.server_ip or "0.0.0.0")
        try:
            full_pn, identity_source = await read.async_read_full_pn(
                session_id=session_id,
                session_protocol=wire,
                listener_port=int(session.get("listener_port") or request.tcp_port),
                expected_pn=request.expected_pn,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("Authoritative identity read failed on %s: %s", session_id, exc)
            return CallbackIdentityOutcome(result=IDENTITY_UNVERIFIED)
        full_pn = str(full_pn or "").strip()
        if not full_pn:
            return CallbackIdentityOutcome(result=IDENTITY_UNVERIFIED)

        # --- 8. reconcile through the SHARED matcher -------------------------
        # One matcher, one rule set. It is fed honest per-attempt numbers rather
        # than a global generation delta: `fired` is what happened during OUR
        # window, so a concurrent flow's trigger shows up as interference instead
        # of silently confirming us.
        match = match_callback_answer(
            _session_views(hass),
            baseline_session_ids=baseline,
            result_pn=full_pn,
            expected_pn=request.expected_pn,
            old_session_id=str(request.old_session_id or "").strip(),
            trigger_generation_before=0,
            trigger_generation_after=attempt.observed_sends,
            expected_own_triggers=expected_sends,
        )
        if not match.confirmed:
            return CallbackIdentityOutcome(result=match.result or IDENTITY_TIMEOUT)
        if match.session_id != session_id:
            # The matcher bound a different socket than the one we read: our
            # evidence and its verdict disagree, so we have proven nothing.
            logger.info(
                "Callback attempt %s read %s but matched %s", owner, session_id, match.session_id
            )
            return CallbackIdentityOutcome(result=IDENTITY_MISMATCH)

        # --- 9 + 10. promote, then certify ----------------------------------
        # The matcher proved the socket and the read are the same identity; the
        # AUTHORITATIVE read is the more complete spelling of it. A socket that
        # only ever advertised a short heartbeat PN must not cost us the full one
        # we just read, so reconcile through the registry's single rule.
        certified = prefer_full_pn(match.collector_pn, full_pn)
        try:
            registry.promote_claim_to_full_pn(owner, certified)
            registry.prepare_handoff(owner, certified)
        except ValueError as exc:
            logger.info("Callback identity %s not certifiable: %s", certified, exc)
            return CallbackIdentityOutcome(result=IDENTITY_CONFLICT)

        claimed = False  # ownership now belongs to the prepared handoff
        return CallbackIdentityOutcome(
            result=IDENTITY_OK,
            collector_pn=certified,
            session_id=session_id,
            session_protocol=wire,
            identity_source=identity_source,
            handoff_owner=owner,
        )
    finally:
        # --- 12. release on every failure AND on cancellation ---------------
        if claimed:
            with suppress(Exception):
                registry.release(owner)


__all__ = [
    "CallbackIdentityOutcome",
    "CallbackIdentityReader",
    "CallbackIdentityRequest",
    "CallbackTriggerSender",
    "IDENTITY_AMBIGUOUS",
    "IDENTITY_CONFLICT",
    "IDENTITY_MISMATCH",
    "IDENTITY_OK",
    "IDENTITY_TARGET_REQUIRED",
    "IDENTITY_TIMEOUT",
    "IDENTITY_TRIGGER_INTERFERENCE",
    "IDENTITY_TRIGGER_NOT_SENT",
    "IDENTITY_UNVERIFIED",
    "async_run_callback_identity_transaction",
]
