"""One reusable callback transaction that establishes LINK + IDENTITY only.

This is the single place where "make the collector dial in and prove which
collector it is" happens. Every active callback path -- manual onboarding, the
manual retry, reconfigure repair, and the pending entry's bounded attempt --
runs exactly this transaction, so none of them re-assembles the proof.

Why it exists
-------------
The callback paths used to establish the TCP session and then run a full driver
sweep BEFORE any durable identity was confirmed. Identity was inferred
afterwards from whatever PN that sweep happened to surface. The sweep costs
tens of seconds, fans out extra UDP probes, and routinely outlives the very
session it is meant to identify -- so the attempt ended in
``callback_timeout``/``callback_trigger_interference`` and the caller looped.
Identity is cheap and authoritative: one read on the socket the collector just
opened. It must not be held hostage to driver detection.

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

from .callback_ledger import (
    CallbackCausalityBusyError,
    get_callback_trigger_ledger,
)
from .session_handle import (
    WIRE_AT_TEXT,
    WIRE_FRAMED,
    negotiate_session_adapters,
)
from ..collector_identity import prefer_full_pn
from .session_registry import SESSION_STATE_CLOSED
from ..const import (
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
)
from .callback_matching import (
    MATCH_TIMEOUT,
    match_callback_answer,
)
from ..timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY

logger = logging.getLogger(__name__)

# Typed outcomes. These are translation keys / status values, never raw text.
IDENTITY_OK = ""
IDENTITY_TIMEOUT = "callback_timeout"
# A TCP session ARRIVED inside our causal window but volunteered neither wire
# nor identity before the deadline. Deliberately distinct from
# IDENTITY_TIMEOUT ("nothing arrived"): calling a live-but-silent socket "the
# collector did not call back" sent users debugging the wrong layer. The
# recovery for THIS result is an explicit user-selected bootstrap protocol.
IDENTITY_SESSION_SILENT = "callback_session_silent"
# The explicit bootstrap probe ran its single read-only identity query and the
# socket gave no valid strong-PN answer on the chosen wire. Never triggers an
# automatic second-protocol attempt.
IDENTITY_WIRE_PROBE_FAILED = "onboarding_wire_probe_failed"
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


# The only provenance an onboarding wire-probe intent may carry.
BOOTSTRAP_SOURCE_EXPLICIT_USER = "explicit_user_selection"
BOOTSTRAP_SOURCE_OBSERVED_SCAN = "observed_active_scan"

# The bound silent socket disappeared before the user's bootstrap continuation
# could run its single read-only query. A NEW attempt happens only on an
# explicit user action -- the intent is never rebound automatically.
IDENTITY_SILENT_SESSION_STALE = "callback_silent_session_unavailable"


@dataclass(frozen=True, slots=True)
class SilentSessionBootstrapOffer:
    """The typed continuation target a silent attempt hands to the user.

    Ephemeral, identity-free: it carries ONLY the exact session id the failed
    attempt causally attributed (its window, its interference gate). It is not
    evidence, not an owner, and never persisted; its sole use is constructing
    an :class:`OnboardingWireProbeIntent` once the user explicitly picks a
    protocol.
    """

    session_id: str

    def __post_init__(self) -> None:
        if (
            type(self.session_id) is not str
            or not self.session_id
            or self.session_id != self.session_id.strip()
        ):
            raise ValueError("silent_bootstrap_offer_session_invalid")


@dataclass(frozen=True, slots=True)
class OnboardingWireProbeIntent:
    """Ephemeral typed capability: probe ONE silent onboarding socket's wire.

    This is deliberately NOT evidence and NOT a protocol owner:

    * it is not a ``ConfirmedSessionProtocolEvidence`` and never becomes one
      by itself -- only a valid strong-PN reply on the chosen wire does;
    * it is not a ``RecoveryContract`` input;
    * it is never persisted and never registered as a listener-wide confirmed
      protocol owner;
    * it lives inside exactly ONE callback identity attempt, binds to the one
      causally-new session of that attempt, and permits exactly ONE read-only
      identity query (framed FC=2 parameter 2 / ``AT+DTUPN``);
    * a wrong choice yields a typed failure -- there is NO automatic fallback
      to the other protocol;
    * failure/cancel destroys the capability with the attempt.

    The only accepted provenance is the user's explicit selection on the
    silent-session recovery step: nothing may mint this from collector kind,
    PN prefix, cloud family, hostname, endpoint, peer IP or a persisted
    expected protocol.
    """

    protocol: str
    # The exact silent session the PREVIOUS attempt causally attributed (its
    # window, its interference gate) and surfaced as its typed
    # ``silent_bootstrap_offer``. The continuation binds to this socket and to
    # NOTHING else -- never to another newly-arrived session, however
    # tempting (a foreign same-IP socket is indistinguishable without it).
    session_id: str
    source: str = BOOTSTRAP_SOURCE_EXPLICIT_USER

    @classmethod
    def for_offer(
        cls, offer: "SilentSessionBootstrapOffer", *, protocol: str
    ) -> "OnboardingWireProbeIntent":
        """The one constructor production uses: offer + explicit protocol."""

        if type(offer) is not SilentSessionBootstrapOffer:
            raise TypeError("silent_bootstrap_offer_required")
        return cls(protocol=protocol, session_id=offer.session_id)

    def __post_init__(self) -> None:
        if type(self.protocol) is not str or self.protocol not in (
            WIRE_FRAMED,
            WIRE_AT_TEXT,
        ):
            raise ValueError("onboarding_wire_probe_protocol_invalid")
        if (
            type(self.session_id) is not str
            or not self.session_id
            or self.session_id != self.session_id.strip()
        ):
            raise ValueError("onboarding_wire_probe_session_invalid")
        if type(self.source) is not str or self.source != BOOTSTRAP_SOURCE_EXPLICIT_USER:
            raise ValueError("onboarding_wire_probe_source_invalid")


@dataclass(frozen=True, slots=True)
class ObservedSessionWireProbeIntent:
    """Zero-send identity capability backed by one exact observed scan wire.

    This is not identity evidence and not a protocol guess. It is bound to the
    active scan's exact source session and observed wire. Its target is either
    that same socket or the exact silent-session offer causally produced by the
    following addressed attempt; it never follows a peer IP or another socket.
    ``collector_pn`` and ``identity_source`` are only the observation the
    authoritative read must reconcile with; a weak heartbeat source is allowed
    precisely so FC=2/DTUPN can upgrade it on that same socket. Only the read's
    strong result may certify identity.
    """

    protocol: str
    session_id: str
    collector_pn: str
    identity_source: str
    wire_source_session_id: str
    source: str = BOOTSTRAP_SOURCE_OBSERVED_SCAN

    @classmethod
    def for_silent_offer(
        cls,
        offer: "SilentSessionBootstrapOffer",
        *,
        observed: "ObservedSessionWireProbeIntent",
    ) -> "ObservedSessionWireProbeIntent":
        """Retarget observed wire authority to one causally-bound silent socket."""

        if type(offer) is not SilentSessionBootstrapOffer:
            raise TypeError("silent_bootstrap_offer_required")
        if type(observed) is not cls:
            raise TypeError("observed_wire_probe_intent_required")
        return cls(
            protocol=observed.protocol,
            session_id=offer.session_id,
            collector_pn=observed.collector_pn,
            identity_source=observed.identity_source,
            wire_source_session_id=observed.wire_source_session_id,
        )

    def __post_init__(self) -> None:
        if type(self.protocol) is not str or self.protocol not in (
            WIRE_FRAMED,
            WIRE_AT_TEXT,
        ):
            raise ValueError("observed_wire_probe_protocol_invalid")
        for label, value in (
            ("session", self.session_id),
            ("collector_pn", self.collector_pn),
            ("wire_source_session", self.wire_source_session_id),
        ):
            if type(value) is not str:
                raise TypeError(f"observed_wire_probe_{label}_must_be_str")
            if not value or value != value.strip():
                raise ValueError(f"observed_wire_probe_{label}_invalid")
        if type(self.identity_source) is not str:
            raise TypeError("observed_wire_probe_identity_source_must_be_str")
        if self.identity_source != self.identity_source.strip():
            raise ValueError("observed_wire_probe_identity_source_invalid")
        if type(self.source) is not str or self.source != BOOTSTRAP_SOURCE_OBSERVED_SCAN:
            raise ValueError("observed_wire_probe_source_invalid")


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
    # The ONLY wire authority for a first-ever fully-silent socket: the user's
    # explicit bootstrap protocol selection (see OnboardingWireProbeIntent).
    # ``None`` keeps today's passive-evidence-only behavior.
    bootstrap_probe: OnboardingWireProbeIntent | ObservedSessionWireProbeIntent | None = None


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
    # Set ONLY with result == IDENTITY_SESSION_SILENT and ONLY when exactly one
    # causally-new silent socket existed in this attempt's window: the typed
    # continuation target a user-selected bootstrap probe binds to. Never
    # identity, never an owner.
    silent_bootstrap_offer: "SilentSessionBootstrapOffer | None" = None

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


# THE production reader, extracted to a neutral reusable module so the inbound
# recovery verifier shares the exact same session-pinned implementation (one
# wire switch, one identity matcher). Kept under the old private name because
# it IS this module's default-reader seam.
from ..collector.session_identity_reader import (  # noqa: E402  (seam placement)
    SessionPinnedIdentityReader as _SessionPinnedIdentityReader,
)


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
    probe_channel: Any = None,
    pending_baseline: frozenset[str] = frozenset(),
    bootstrap: OnboardingWireProbeIntent | ObservedSessionWireProbeIntent | None = None,
) -> tuple[dict[str, Any] | None, bool, str, str]:
    """Wait, bounded, for the ONE session this attempt may own.

    Returns ``(session, silent_seen, offer_session_id, failure)``.

    WITHOUT ``bootstrap`` (a triggering attempt): any causally-new live
    session outside the baseline qualifies -- no identity requirement, we only
    need something to read from. A causally-new PENDING socket that never
    becomes readable is reported via ``silent_seen`` (+ ``offer_session_id``
    when it was exactly one, the typed continuation target).

    WITH ``bootstrap`` (the user's silent-session continuation): NO trigger
    was sent, so no newly-arrived session is attributable to this attempt --
    the ONLY session that may match, be probed or be adopted is the exact
    bound ``bootstrap.session_id``. If that socket is gone the typed
    ``IDENTITY_SILENT_SESSION_STALE`` failure is returned immediately; the
    intent is never rebound to another session.
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout))
    bound_session_id = str(getattr(bootstrap, "session_id", "") or "")
    offer_session_id = ""
    silent_seen = False
    probed = False
    while True:
        views = _session_views(hass)
        if bootstrap is not None:
            fresh = [
                session
                for session in views
                if session["session_id"] == bound_session_id and _is_live(session)
            ]
        else:
            fresh = [
                session
                for session in views
                if session["session_id"]
                and session["session_id"] not in baseline
                and session["session_id"] != old_session_id
                and _is_live(session)
            ]
        if fresh:
            picked = fresh[0] if len(fresh) == 1 else _prefer_readable(fresh)
            return picked, silent_seen, offer_session_id, ""

        live_silent = (
            probe_channel.snapshot_silent_session_ids()
            if probe_channel is not None
            else frozenset()
        )

        if bootstrap is not None:
            if bound_session_id not in live_silent:
                # Not silent-pending and (above) not readable either: the
                # bound socket is GONE. Typed, immediate, no rebinding.
                return None, silent_seen, "", IDENTITY_SILENT_SESSION_STALE
            if not probed:
                probed = True
                pn = await probe_channel.async_identify_exact_session(
                    bound_session_id,
                    session_protocol=bootstrap.protocol,
                )
                if not pn:
                    # Wrong wire / no answer: typed failure, and deliberately
                    # no automatic attempt with the other protocol.
                    return None, silent_seen, "", IDENTITY_WIRE_PROBE_FAILED
                # The strong-PN reply was recorded by the listener; the next
                # poll sees the session as readable and the normal
                # claim/read/match path continues unchanged.
        else:
            new_silent = frozenset(
                session_id
                for session_id in live_silent
                if session_id not in pending_baseline
                and session_id not in baseline
                and session_id != old_session_id
            )
            if new_silent:
                silent_seen = True
                # Exactly ONE causally-new silent socket is an unambiguous
                # continuation target for a LATER explicit user selection;
                # two or more stay ambiguous and are never offered or probed.
                offer_session_id = (
                    next(iter(new_silent)) if len(new_silent) == 1 else ""
                )

        if loop.time() >= deadline:
            return None, silent_seen, offer_session_id, ""
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

    bootstrap = request.bootstrap_probe
    if bootstrap is not None and type(bootstrap) not in (
        OnboardingWireProbeIntent,
        ObservedSessionWireProbeIntent,
    ):
        # Fail closed on ducks: only the strict typed capability (whose
        # constructor pinned protocol + explicit-user provenance) may permit
        # a bootstrap probe. Nothing is triggered, claimed or probed.
        logger.info("Rejected non-typed onboarding wire probe intent")
        return CallbackIdentityOutcome(result=IDENTITY_WIRE_PROBE_FAILED)

    # The silent-socket taxonomy (and the optional bootstrap probe) go through
    # the ONE narrow public transport boundary; the shared listener stays a
    # collector-layer concern.
    from ..collector.silent_session_probe import SilentSessionIdentityProbeChannel

    probe_channel = SilentSessionIdentityProbeChannel(
        host=str(request.server_ip or "").strip() or "0.0.0.0",
        port=int(request.tcp_port or 0),
    )
    await probe_channel.async_open()

    baseline = frozenset(
        session["session_id"] for session in _session_views(hass) if session["session_id"]
    )
    pending_baseline = probe_channel.snapshot_silent_session_ids()

    # --- 3. exactly one trigger, or none -------------------------------------
    # The bootstrap CONTINUATION owns no new causal window: the silent socket
    # was already attributed by the previous attempt's trigger, so this
    # attempt sends ZERO datagrams and only runs its one read-only query.
    expected_sends = (
        1
        if strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND and bootstrap is None
        else 0
    )
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
        (
            session,
            silent_seen,
            offer_session_id,
            wait_failure,
        ) = await _async_wait_for_new_session(
            hass,
            baseline=baseline,
            old_session_id=str(request.old_session_id or "").strip(),
            timeout=(
                request.session_wait_timeout
                or DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_identity_session_wait
            ),
            probe_channel=probe_channel,
            pending_baseline=pending_baseline,
            bootstrap=bootstrap,
        )
        offer = (
            SilentSessionBootstrapOffer(session_id=offer_session_id)
            if offer_session_id
            else None
        )
        if session is None:
            if wait_failure:
                return CallbackIdentityOutcome(
                    result=wait_failure, silent_bootstrap_offer=offer
                )
            if silent_seen:
                # A session ARRIVED and stayed silent: honest, distinct
                # taxonomy -- the recovery is the explicit bootstrap protocol
                # bound to exactly this socket, not another blind trigger.
                return CallbackIdentityOutcome(
                    result=IDENTITY_SESSION_SILENT,
                    silent_bootstrap_offer=offer,
                )
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
        match_baseline = baseline
        if bootstrap is not None and bootstrap.session_id in match_baseline:
            # The user-bound silent socket legitimately pre-dates this retry:
            # the PREVIOUS attempt causally attributed it. Only that exact id
            # is exempted; every other baseline socket stays excluded.
            match_baseline = frozenset(
                sid for sid in match_baseline if sid != bootstrap.session_id
            )
        match = match_callback_answer(
            _session_views(hass),
            baseline_session_ids=match_baseline,
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
        with suppress(Exception):
            await probe_channel.async_close()


__all__ = [
    "BOOTSTRAP_SOURCE_EXPLICIT_USER",
    "BOOTSTRAP_SOURCE_OBSERVED_SCAN",
    "IDENTITY_SESSION_SILENT",
    "IDENTITY_SILENT_SESSION_STALE",
    "IDENTITY_WIRE_PROBE_FAILED",
    "OnboardingWireProbeIntent",
    "ObservedSessionWireProbeIntent",
    "SilentSessionBootstrapOffer",
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
