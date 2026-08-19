"""Callback session ownership registry.

Today the shared listener spreads "who owns which live collector session" across
several reference-counter dicts and connection-index maps keyed inconsistently by
peer IP, collector PN, or session id. There is no single object that answers
"which config entry owns which inbound session". This module introduces that
object as an explicit facade on top of the existing shared listener inventory.

Identity rules (the whole point of the facade):

- **Full collector PN is durable identity.** Two sessions with different full PNs
  are always distinct collectors, even behind one NAT peer IP.
- **Short PN is temporary discovery identity only.** A short PN observed from a
  weak source may be a prefix of the full PN read later; the registry enriches a
  short-PN claim into the full PN rather than creating a second owner.
- **Session id is transient socket identity only.**
- **Peer IP is diagnostic/display information only** and is never used as an
  ownership or dedup key.
- A session is either unclaimed or owned by exactly one config entry.

Short/full PN reconciliation lives in the adjacent pure
``collector_identity`` module; this registry consumes that one rule
while owning all mutable session claims and handoffs.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Callable

from ..collector_identity import (
    identity_source_is_strong as _identity_source_is_strong,
    normalize_pn as _normalize_pn,
    pn_is_same_identity as _pn_is_same_identity,
    prefer_full_pn as _prefer_full_pn,
)
from .session_handle import SessionHandle, negotiate_session_adapters

# Normalized session lifecycle states.
SESSION_STATE_ACCEPTED = "accepted"
SESSION_STATE_IDENTIFIED_WEAK = "identified_weak"
SESSION_STATE_IDENTIFIED_STRONG = "identified_strong"
SESSION_STATE_CLAIMED = "claimed"
SESSION_STATE_ACTIVE = "active"
SESSION_STATE_CLOSED = "closed"


# Listener inventory states that mean the socket is routed/live.
_ACTIVE_INVENTORY_STATES = frozenset({"routed_at_text", "routed_framed"})
_CLAIMED_INVENTORY_STATES = frozenset({"claimed"})
_CLOSED_INVENTORY_STATES = frozenset(
    {"closed_disconnected", "closed_no_payload", "parked_peer_closed"}
)
_UNDISCOVERABLE_INVENTORY_STATES = frozenset(
    {
        "route_identity_mismatch",
        "waiting_for_route_identity",
        "parked_waiting_for_identity",
    }
)


def _identity_is_strong(source: object) -> bool:
    return _identity_source_is_strong(source)


def _state_from_inventory(inventory_state: str, identity_source: str) -> str:
    """Map a listener inventory state + identity source to a registry state."""

    if inventory_state in _ACTIVE_INVENTORY_STATES:
        return SESSION_STATE_ACTIVE
    if inventory_state in _CLAIMED_INVENTORY_STATES:
        return SESSION_STATE_CLAIMED
    if inventory_state in _CLOSED_INVENTORY_STATES:
        return SESSION_STATE_CLOSED
    if _identity_is_strong(identity_source):
        return SESSION_STATE_IDENTIFIED_STRONG
    return SESSION_STATE_IDENTIFIED_WEAK


@dataclass(frozen=True, slots=True)
class PermanentOwnedSessionCertification:
    """A registry-issued permanent-owner recovery capability (Batch 8).

    The typed, exact-type capability a recovery run UNDER an existing permanent
    owner produces — deliberately DISTINCT from the onboarding prepared-handoff
    slot. It certifies exactly one ``(owner_id, session_id, collector_pn)``
    triple and asserts nothing about an ownership transfer. Only the registry
    constructs it (``certify_permanent_owned_session``) and only the registry
    re-verifies it (``reverify_permanent_owned_session``); a forged look-alike
    fails the strict ``type() is`` re-check at commit time.
    """

    owner_id: str
    session_id: str
    collector_pn: str


@dataclass(frozen=True, slots=True)
class CallbackSession:
    """One normalized inbound collector session with ownership state."""

    session_id: str
    peer_ip: str = ""
    peer_port: int = 0
    listener_port: int = 0
    protocol_shape: str = ""
    session_protocol: str = ""
    collector_pn: str = ""
    identity_source: str = ""
    state: str = SESSION_STATE_ACCEPTED
    owner_entry_id: str = ""
    # The original observed session mapping (listener inventory shape), kept so
    # consumers can work with the raw dict without re-deriving fields. Coalescing
    # keeps the winning session's raw mapping.
    raw: Mapping[str, object] = field(default_factory=dict)

    @property
    def has_strong_identity(self) -> bool:
        return _identity_is_strong(self.identity_source)

    @property
    def claimed(self) -> bool:
        return bool(self.owner_entry_id)

    @property
    def discoverable(self) -> bool:
        """Return whether this session is safe to publish as a device candidate."""

        inventory_state = str(self.raw.get("state") or "").strip().lower()
        if inventory_state == "route_identity_mismatch":
            # A weak mismatch is only an ambiguous heartbeat prefix. A strong
            # mismatch, however, is positive evidence that this is a different
            # fully identified collector and therefore a valid new candidate.
            return self.state != SESSION_STATE_CLOSED and self.has_strong_identity
        return (
            self.state != SESSION_STATE_CLOSED
            and inventory_state not in _UNDISCOVERABLE_INVENTORY_STATES
        )


@dataclass(slots=True)
class _Claim:
    """One owner's claim over a session and/or durable collector identity.

    Ownership moves through three lifecycle stages, all keyed on the SAME owner
    id (a per-attempt ``callback_verification:<uuid>`` for a flow, or the
    ``entry_id`` for a permanent entry) -- never on a PN-derived owner id:

    * active verification claim: ``handoff_pending=False`` (a flow is still
      proving the identity; setup must never transfer this);
    * prepared handoff: ``handoff_pending=True`` on a verification owner (the flow
      is about to create/update the entry; setup completes exactly this one,
      found by PN);
    * permanent entry claim: ``handoff_pending=False`` on the ``entry_id`` owner
      (a settled claim -- setup's completion clears the flag, so it can never be
      re-completed out from under the entry).
    """

    entry_id: str
    collector_pn: str = ""
    session_id: str = ""
    session_protocol: str = ""
    handoff_pending: bool = False


@dataclass(slots=True)
class CallbackSessionRegistry:
    """Ownership + short/full PN reconciliation over observed inbound sessions.

    Phase 1 scope: this is an ownership + reconciliation *facade*. Transport
    ownership is NOT fully solved yet -- the registry does not own sockets. The
    shared listener still accepts and claims the actual TCP sockets (via
    ``pop_pending_socket_for_route`` / ``activate_pending_connection``); the
    registry owns the ownership bookkeeping and the identity reconciliation that
    used to be duplicated across passive discovery, config flow, and the
    listener. A later phase can route the listener's socket-claim through this
    registry so there is a single claim path end to end.

    ``sessions_source`` is a callable returning the raw session dicts observed by
    the listener(s) (the shape of ``_SharedEybondListener.discovered_collector_sessions``
    plus an optional ``listener_port`` / ``session_protocol`` key). It is injected
    so the registry can be unit-tested without a live listener.
    """

    sessions_source: Callable[[], Iterable[Mapping[str, object]]] | None = None
    _claims: dict[str, _Claim] = field(default_factory=dict)

    # --- observation ----------------------------------------------------------

    def _raw_sessions(self) -> tuple[Mapping[str, object], ...]:
        if self.sessions_source is None:
            return ()
        try:
            return tuple(self.sessions_source() or ())
        except Exception:
            return ()

    @staticmethod
    def _normalize(raw: Mapping[str, object]) -> CallbackSession:
        identity_source = str(raw.get("collector_identity_source") or "").strip()
        inventory_state = str(raw.get("state") or "").strip()
        return CallbackSession(
            session_id=str(raw.get("session_id") or "").strip(),
            peer_ip=str(raw.get("peer_ip") or "").strip(),
            peer_port=int(raw.get("peer_port") or 0),
            listener_port=int(raw.get("listener_port") or 0),
            protocol_shape=str(raw.get("protocol_shape") or "").strip(),
            session_protocol=str(raw.get("session_protocol") or "").strip(),
            collector_pn=_normalize_pn(raw.get("collector_pn")),
            identity_source=identity_source,
            state=_state_from_inventory(inventory_state, identity_source),
            raw=MappingProxyType(dict(raw)),
        )

    def _coalesce(
        self,
        sessions: Iterable[CallbackSession],
    ) -> list[CallbackSession]:
        """Collapse short/full PN duplicates of one collector into one session.

        Distinct full PNs are always kept distinct -- this is what keeps two
        collectors behind one NAT peer IP separate. Peer IP is never used to
        merge or split.
        """

        coalesced: list[CallbackSession] = []
        for session in sessions:
            if not session.collector_pn:
                coalesced.append(session)
                continue
            for index, existing in enumerate(coalesced):
                if not existing.collector_pn:
                    continue
                if not _pn_is_same_identity(existing.collector_pn, session.collector_pn):
                    continue
                # Same collector observed twice (short + full / weak + strong):
                # keep the strongest, most complete identity.
                keep_new = False
                if session.has_strong_identity and not existing.has_strong_identity:
                    keep_new = True
                elif (
                    session.has_strong_identity == existing.has_strong_identity
                    and len(session.collector_pn) > len(existing.collector_pn)
                ):
                    keep_new = True
                if keep_new:
                    merged_pn = _prefer_full_pn(existing.collector_pn, session.collector_pn)
                    coalesced[index] = replace(session, collector_pn=merged_pn)
                break
            else:
                coalesced.append(session)
        return coalesced

    def _normalized_sessions(self) -> list[CallbackSession]:
        """Return per-socket normalized sessions (pre-coalesce) with owner attached."""

        normalized = [self._normalize(raw) for raw in self._raw_sessions()]
        return [self._attach_owner(session) for session in normalized if session.session_id]

    def observed_sessions(self) -> tuple[CallbackSession, ...]:
        """Return coalesced observed sessions with ownership state attached."""

        return tuple(self._coalesce(self._normalized_sessions()))

    def observed_sessions_per_socket(self) -> tuple[CallbackSession, ...]:
        """Return per-socket sessions (no short/full coalescing), owner attached.

        The coalesced view collapses several live sockets of one collector into
        one candidate -- right for discovery, wrong for behavioral verification,
        which must distinguish "the same old socket" from "a NEW socket of the
        same collector" (baseline vs post-restart/post-trigger session).
        """

        return tuple(self._normalized_sessions())

    def _attach_owner(self, session: CallbackSession) -> CallbackSession:
        owner = self._owner_for_session(session)
        if not owner:
            return session
        state = session.state
        # Ownership must not resurrect a closed socket as merely "claimed".
        # Closed remains terminal; only non-active live observations are
        # promoted to the claimed lifecycle state.
        if state not in (SESSION_STATE_ACTIVE, SESSION_STATE_CLOSED):
            state = SESSION_STATE_CLAIMED
        return replace(session, owner_entry_id=owner, state=state)

    def list_unclaimed_sessions(self) -> tuple[CallbackSession, ...]:
        """Return observed sessions that no config entry owns yet."""

        return tuple(
            session
            for session in self.observed_sessions()
            if not session.owner_entry_id and session.discoverable
        )

    def current_session_for_pn(
        self,
        collector_pn: object,
        *,
        require_exact: bool = False,
    ) -> CallbackSession | None:
        """Return the best currently observed socket for one collector PN.

        Config flows can stay open while a collector reconnects (or is updated
        over OTA).  The session id captured when discovery created the flow is
        therefore only an observation, never a durable handle.  Resolve the
        current socket here, by PN, immediately before taking a transient
        session claim.

        ``require_exact`` is used when the flow only has weak identity evidence:
        such a flow may follow the same exact short PN onto a replacement socket,
        but it must not choose one of several longer prefix matches.  Peer IP is
        deliberately absent from both matching and ranking.
        """

        pn = _normalize_pn(collector_pn)
        if not pn:
            return None
        best: CallbackSession | None = None
        best_rank: tuple[int, int, int] | None = None
        for session in self._normalized_sessions():
            if session.state == SESSION_STATE_CLOSED:
                continue
            if require_exact:
                if session.collector_pn != pn:
                    continue
            elif not _pn_is_same_identity(session.collector_pn, pn):
                continue
            raw_state = str(session.raw.get("state") or "").strip().lower()
            rank = (
                1 if raw_state in _ACTIVE_INVENTORY_STATES else 0,
                1 if session.has_strong_identity else 0,
                len(session.collector_pn),
            )
            # The listener inventory is acceptance-ordered.  On an exact tie,
            # prefer its later item so a replacement socket wins over a stale
            # overlap that has not reached its EOF callback yet.
            if best_rank is None or rank >= best_rank:
                best = session
                best_rank = rank
        return best

    # --- ownership ------------------------------------------------------------

    def _owner_for_session(self, session: CallbackSession) -> str:
        for entry_id, claim in self._claims.items():
            if claim.session_id and claim.session_id == session.session_id:
                return entry_id
            if claim.collector_pn and _pn_is_same_identity(
                claim.collector_pn, session.collector_pn
            ):
                return entry_id
        return ""

    def owner_for_pn(self, collector_pn: object) -> str:
        """Return the entry id that owns a durable PN identity, if any."""

        pn = _normalize_pn(collector_pn)
        if not pn:
            return ""
        for entry_id, claim in self._claims.items():
            if claim.collector_pn and _pn_is_same_identity(claim.collector_pn, pn):
                return entry_id
        return ""

    def claim(
        self,
        entry_id: str,
        *,
        collector_pn: object = "",
        session_id: object = "",
        session_protocol: object = "",
    ) -> CallbackSession | None:
        """Claim a session for one entry by durable PN and/or transient session id.

        Enforces single ownership: a PN already owned by another entry raises
        ``ValueError``. A short PN is enriched to the full PN if a matching
        session is observed. Returns the matched observed session, or ``None`` if
        nothing matches yet (the claim is still recorded so a later-arriving
        session binds to it).

        For an owner that ALREADY holds a claim this is fail-closed: it may only
        ENRICH that claim (same identity, or filling a previously-empty field),
        never re-point it. Re-binding an owner to another collector or another
        physical socket is not this method's job and each legal transition has its
        own explicit API -- :meth:`promote_claim_to_full_pn` / :meth:`reconcile_identity`
        for identity enrichment, :meth:`retarget_claim_to_reconnected_session` for
        the same collector on a new socket, :meth:`prepare_handoff` /
        :meth:`complete_handoff` for ownership transfer, and :meth:`release` +
        a fresh claim for a new attempt or a different identity. Re-claiming
        wholesale used to REPLACE the record, which silently switched identity,
        dropped the session/protocol and reset ``handoff_pending``.
        """

        entry_id = str(entry_id or "").strip()
        if not entry_id:
            raise ValueError("entry_id_required")
        pn = _normalize_pn(collector_pn)
        sid = str(session_id or "").strip()

        if pn:
            other = self.owner_for_pn(pn)
            if other and other != entry_id:
                raise ValueError(f"session_already_claimed:{pn}:{other}")

        # Refuse EXPLICIT caller intent that would re-bind rather than enrich,
        # before deriving anything and before touching any state. Silently
        # ignoring it would be just as wrong as applying it: the caller believes
        # the claim now points where it asked.
        existing = self._claims.get(entry_id)
        if existing is not None:
            if pn and existing.collector_pn and not _pn_is_same_identity(
                existing.collector_pn, pn
            ):
                raise ValueError(f"claim_identity_mismatch:{existing.collector_pn}:{pn}")
            if sid and existing.session_id and sid != existing.session_id:
                raise ValueError(f"claim_session_mismatch:{existing.session_id}:{sid}")

        # Enrich the durable PN from the strongest matching observed session.
        matched: CallbackSession | None = None
        for session in self.observed_sessions():
            if sid and session.session_id == sid:
                matched = session
                break
            if pn and _pn_is_same_identity(pn, session.collector_pn):
                if matched is None or (
                    session.has_strong_identity and not matched.has_strong_identity
                ):
                    matched = session
        # TRUST BOUNDARY: what the CALLER declares and what the matched SOCKET
        # reports are two independent pieces of evidence, and a session_id lookup
        # can land on a socket belonging to a different collector entirely. If
        # they disagree the claim would assert a contradiction -- identity A bound
        # to collector B's physical session -- and this method would hand back B's
        # session as though it were A's. prefer_full_pn cannot arbitrate that: its
        # contract is "the more complete spelling of ONE identity", so given two
        # different identities it merely returns the longer string and silently
        # picks a winner. Fail closed BEFORE any of it, for a new owner and an
        # existing one alike. Both known identities are checked against the socket
        # because prefix-identity is not transitive: A_short may match both
        # A_full and a divergent A_other.
        if matched is not None and matched.collector_pn:
            for known in (pn, existing.collector_pn if existing is not None else ""):
                if known and not _pn_is_same_identity(known, matched.collector_pn):
                    raise ValueError(
                        f"claim_session_identity_mismatch:{known}:{matched.collector_pn}"
                    )
        if matched is not None:
            if matched.collector_pn:
                # Same identity is PROVEN immediately above, so this is a pure
                # short->full enrichment of ONE collector -- prefer_full_pn's
                # actual contract -- never a choice between two candidates.
                pn = _prefer_full_pn(pn, matched.collector_pn)
            if not sid:
                sid = matched.session_id
        protocol = str(session_protocol or "").strip() or (
            matched.session_protocol if matched else ""
        )

        if existing is None:
            self._claims[entry_id] = _Claim(
                entry_id=entry_id,
                collector_pn=pn,
                session_id=sid,
                session_protocol=protocol,
            )
        else:
            # Defense in depth. The explicit guard above proved the caller's PN is
            # this claim's identity, and the trust-boundary guard proved any
            # socket-DERIVED enrichment is too, so this cannot fire today -- it
            # stays as a last gate on the write itself.
            if pn and existing.collector_pn and not _pn_is_same_identity(
                existing.collector_pn, pn
            ):
                raise ValueError(f"claim_identity_mismatch:{existing.collector_pn}:{pn}")
            # Enrichment only: never downgrade the identity, never overwrite a
            # session/protocol already bound (a reconnect is retarget's job), and
            # never touch handoff_pending -- re-claiming must not un-prepare a
            # handoff that setup is about to complete. Same identity is proven by
            # both guards above, so prefer_full_pn only ever picks the more
            # complete spelling of the one collector.
            existing.collector_pn = _prefer_full_pn(existing.collector_pn, pn)
            if not existing.session_id:
                existing.session_id = sid
            if not existing.session_protocol:
                existing.session_protocol = protocol
        if matched is None:
            return None
        return self._attach_owner(matched)

    def claim_identity(self, entry_id: str, collector_pn: object) -> None:
        """Record a PN-ONLY durable ownership claim WITHOUT scanning sessions.

        The identity-only intent a cold repair needs BEFORE any socket exists: it
        asserts durable ownership of a collector PN and nothing about a socket.
        Unlike :meth:`claim` it NEVER inspects the observed session inventory, so
        it can never auto-bind a ``session_id`` / ``session_protocol`` -- binding a
        socket is a separate, post-proof step
        (:meth:`retarget_claim_to_reconnected_session`). Peer IP is never consulted.

        This asserts identity ownership ONLY. It is exactly three operations and
        nothing else: (1) create a fresh PN-only claim; (2) fill a COMPLETELY
        empty, unbound claim; (3) short->full enrich an already PN-bearing claim
        of the same identity. It must NEVER turn a transient session/protocol/
        handoff claim into a durable identity claim -- promoting a bound socket to
        a durable identity is :meth:`promote_claim_to_full_pn` after strong
        exact-session evidence, and this identity-only API must not become a
        second promotion path (not even when a bound socket happens to report the
        same PN -- it does not look).

        Contract:

        * the single-owner guard runs FIRST -- a PN owned by another entry raises
          ``ValueError('session_already_claimed:<pn>:<other>')`` before any mutation;
        * an existing PN-BEARING claim: same identity -> short->full enrichment;
          a foreign identity -> ``ValueError('claim_identity_mismatch:<current>:<pn>')``;
          its ``session_id`` / ``session_protocol`` / ``handoff_pending`` are preserved;
        * an existing PN-LESS claim: if it carries ANY ``session_id`` /
          ``session_protocol`` / ``handoff_pending`` it is a transient session
          claim -> ``ValueError('claim_identity_transient_claim_conflict')`` with NO
          mutation; only a COMPLETELY empty claim may take the PN;
        * no existing claim -> a fresh PN-only claim.

        Never scans the inventory, never looks up a PN in observations, never
        matches by peer IP, never re-binds a socket. Every refusal is BEFORE any
        mutation. Returns ``None``: it asserts ownership intent, never a socket.
        """

        entry_id = str(entry_id or "").strip()
        if not entry_id:
            raise ValueError("entry_id_required")
        pn = _normalize_pn(collector_pn)
        if not pn:
            raise ValueError("collector_pn_required")
        # Single-owner guard FIRST, before any mutation.
        other = self.owner_for_pn(pn)
        if other and other != entry_id:
            raise ValueError(f"session_already_claimed:{pn}:{other}")
        existing = self._claims.get(entry_id)
        if existing is None:
            self._claims[entry_id] = _Claim(entry_id=entry_id, collector_pn=pn)
            return None
        if existing.collector_pn:
            # PN-bearing claim: same-identity short->full enrichment, or foreign
            # refusal. session_id / session_protocol / handoff_pending untouched.
            if not _pn_is_same_identity(existing.collector_pn, pn):
                raise ValueError(
                    f"claim_identity_mismatch:{existing.collector_pn}:{pn}"
                )
            existing.collector_pn = _prefer_full_pn(existing.collector_pn, pn)
            return None
        # PN-less claim: only a COMPLETELY empty, unbound claim may take a PN. A
        # transient session/protocol/handoff claim is NOT silently promoted to a
        # durable identity claim -- that would let identity-only intent assert
        # identity A over a physically-bound socket B, a contradiction the
        # certification would only catch later. Refuse BEFORE any mutation.
        if existing.session_id or existing.session_protocol or existing.handoff_pending:
            raise ValueError("claim_identity_transient_claim_conflict")
        existing.collector_pn = pn
        return None

    def claim_session(
        self,
        entry_id: str,
        *,
        session_id: object,
    ) -> None:
        """Record a TRANSIENT claim that owns exactly one session id.

        Used by the connection-strategy verification before the session's
        durable identity is strong: the claim never copies a weak/short PN into
        durable ownership and never matches other same-prefix sessions -- it
        owns only the given socket. Promote it with
        :meth:`promote_claim_to_full_pn` once the strong full PN is observed.

        Raises ``ValueError`` when the session (or the durable identity it is
        currently reporting) is already owned by another entry: single-owner is
        preserved even for transient claims.

        Fail-closed for an owner that already holds a claim: re-claiming the SAME
        session id is idempotent, and any OTHER session id raises
        ``claim_session_mismatch`` without mutating anything. A live claim is
        never silently moved to another socket -- the same collector reappearing
        on a new socket is :meth:`retarget_claim_to_reconnected_session` (which
        proves the old socket is closed and the identity matches), and a new
        attempt must :meth:`release` first. Re-claiming used to REPLACE the
        record, so it also demoted a durable-PN claim back to a PN-less transient
        one and reset ``handoff_pending``; the claim is now updated in place and
        keeps its identity and handoff state.
        """

        entry_id = str(entry_id or "").strip()
        if not entry_id:
            raise ValueError("entry_id_required")
        sid = str(session_id or "").strip()
        if not sid:
            raise ValueError("session_id_required")

        for other_id, other in self._claims.items():
            if other_id != entry_id and other.session_id == sid:
                raise ValueError(f"session_already_claimed:{sid}:{other_id}")
        # The session's currently-reported durable identity may already be
        # owned (e.g. by a configured entry); a transient claim must not carve
        # that socket out from under its owner.
        observed_pn = ""
        for session in self._normalized_sessions():
            if session.session_id != sid:
                continue
            observed_pn = session.collector_pn
            if observed_pn:
                owner = self.owner_for_pn(observed_pn)
                if owner and owner != entry_id:
                    raise ValueError(f"session_already_claimed:{observed_pn}:{owner}")
            break

        existing = self._claims.get(entry_id)
        if existing is not None:
            if existing.session_id and existing.session_id != sid:
                raise ValueError(f"claim_session_mismatch:{existing.session_id}:{sid}")
            if (
                existing.collector_pn
                and observed_pn
                and not _pn_is_same_identity(existing.collector_pn, observed_pn)
            ):
                # Same session id, but it is no longer the same collector: the
                # socket this owner proved for A now reports B. Re-claiming it is
                # NOT idempotent -- it would silently re-attest an identity the
                # evidence now contradicts, on a claim that may already be a
                # prepared handoff. Fail closed; a genuinely new attempt releases
                # first, and a same-collector reconnect is retarget's job.
                raise ValueError(
                    f"claim_session_identity_mismatch:{existing.collector_pn}:{observed_pn}"
                )
            # In place: a durable PN already proven for this owner survives, and so
            # does a prepared handoff. Only the (empty) socket binding is filled.
            existing.session_id = sid
            return
        self._claims[entry_id] = _Claim(entry_id=entry_id, session_id=sid)

    def promote_claim_to_full_pn(self, entry_id: str, full_pn: object) -> bool:
        """Promote a claim to the confirmed full durable PN of the SAME collector.

        "Promote" is strictly an ENRICHMENT of one identity, never a switch to
        another. A claim is derived from a real session, so the collector it
        stands for is a fact; the only thing an observation may add is a more
        complete spelling of that same PN. Two guards, in this order:

        * single owner -- another owner already holds this identity ->
          ``session_already_claimed`` (unchanged, and checked first so the error
          still names the real conflict);
        * identity -- this claim already stands for a DIFFERENT collector ->
          ``promote_identity_mismatch``. Without this, promoting A's claim to an
          as-yet-unowned B silently re-pointed it, and the identity check in
          :meth:`prepare_handoff` was then satisfied by the very value that had
          just been smuggled in. That check stays as defense in depth; THIS is
          where the invariant is actually enforced.

        Both refusals raise BEFORE any mutation: claim PN, session and
        ``handoff_pending`` are left byte-for-byte as they were.

        Returns whether the claim now stands on the given identity (see below);
        an identity is never DOWNGRADED, so promoting a claim that already holds
        the full PN to a short prefix of it keeps the full one.

        Return value
        ------------
        ``True``  -- the claim durably stands on this identity. Idempotent: a
                     re-promotion to the same (or a weaker spelling of the same)
                     identity is a safe no-op and still ``True``, because the
                     postcondition the caller asked for holds.
        ``False`` -- there was nothing to promote: no claim under this owner, or
                     no usable PN. Never means "refused" -- a refusal raises.
        """

        entry_id = str(entry_id or "").strip()
        claim = self._claims.get(entry_id)
        if claim is None:
            return False
        pn = _normalize_pn(full_pn)
        if not pn:
            return False
        other = self.owner_for_pn(pn)
        if other and other != entry_id:
            raise ValueError(f"session_already_claimed:{pn}:{other}")
        current = claim.collector_pn
        if current and not _pn_is_same_identity(current, pn):
            raise ValueError(f"promote_identity_mismatch:{current}:{pn}")
        # Empty -> adopt; short -> full enriches; full -> short keeps the full PN.
        claim.collector_pn = _prefer_full_pn(current, pn)
        return True

    def claimed_session_id(self, entry_id: str) -> str:
        """Return the transient session id recorded on one entry's claim."""

        claim = self._claims.get(str(entry_id or "").strip())
        return claim.session_id if claim is not None else ""

    def _certify_owner_pn(self, owner: str, sid: str) -> str:
        """Shared certification core: durable full PN, or "" fail-closed."""

        if not owner or not sid:
            return ""
        claim = self._claims.get(owner)
        if claim is None or claim.session_id != sid:
            return ""
        claim_pn = _normalize_pn(claim.collector_pn)
        if not claim_pn:
            return ""
        for session in self._normalized_sessions():
            if session.session_id != sid:
                continue
            if session.state == SESSION_STATE_CLOSED:
                return ""
            if not session.has_strong_identity:
                return ""
            if not _pn_is_same_identity(session.collector_pn, claim_pn):
                return ""
            session_owner = self._owner_for_session(session)
            if session_owner and session_owner != owner:
                return ""
            return _prefer_full_pn(claim_pn, session.collector_pn)
        return ""

    def certify_owner_reconnected_session(
        self,
        entry_id: str,
        session_id: object,
    ) -> str:
        """Certify a PERMANENT owner's recovered session — the honest capability.

        Certifies that EXACTLY this owner's claim currently holds EXACTLY this
        session id, that the session is live with a STRONG observed identity,
        that the identity is the claim's own durable PN, and that no other
        owner holds the socket. Returns the durable full PN, or ``""``
        fail-closed. Deliberately NOT a handoff and NO PN lookup.
        """

        return self._certify_owner_pn(
            str(entry_id or "").strip(), str(session_id or "").strip()
        )

    def certify_permanent_owned_session(
        self,
        entry_id: str,
        session_id: object,
    ) -> "PermanentOwnedSessionCertification | None":
        """Issue a TYPED permanent-owner capability, or ``None`` fail-closed.

        The exact-type object the recovery transaction consumes INSTEAD of the
        onboarding ``handoff_owner`` slot: it says nothing about a prepared
        onboarding handoff, only that this exact owner/session/durable-PN triple
        is registry-certified right now. Re-check it just before commit with
        :meth:`reverify_permanent_owned_session`.
        """

        owner = str(entry_id or "").strip()
        sid = str(session_id or "").strip()
        certified_pn = self._certify_owner_pn(owner, sid)
        if not certified_pn:
            return None
        return PermanentOwnedSessionCertification(
            owner_id=owner, session_id=sid, collector_pn=certified_pn
        )

    def reverify_permanent_owned_session(
        self,
        certification: object,
    ) -> bool:
        """Re-certify a previously-issued capability against LIVE registry state.

        Fail-closed: the object must literally be a
        :class:`PermanentOwnedSessionCertification` (no ducks), and the same
        owner/session must STILL certify to the same durable identity. A stale
        capability (the claim was retargeted away, the socket closed, the PN
        changed) is rejected.
        """

        if type(certification) is not PermanentOwnedSessionCertification:
            return False
        certified_pn = self._certify_owner_pn(
            certification.owner_id, certification.session_id
        )
        if not certified_pn:
            return False
        return _pn_is_same_identity(certified_pn, certification.collector_pn)

    def pin_owner_claim_to_session(
        self,
        entry_id: str,
        session_id: object,
    ) -> bool:
        """Explicitly pin a durable by-PN claim to a live session it owns.

        A runtime entry owns its identity durably by PN; the claim record may
        carry no session id until an explicit operation records one. This is
        that operation (idempotent): it pins ``claim.session_id`` to a session
        that is LIVE, of the claim's OWN durable PN, and not owned by another
        entry. It never changes identity, never binds a foreign socket, and is
        the explicit prerequisite the exact wire resolver demands instead of a
        PN search. Returns whether the claim now names the session.
        """

        owner = str(entry_id or "").strip()
        sid = str(session_id or "").strip()
        if not owner or not sid:
            return False
        claim = self._claims.get(owner)
        if claim is None:
            return False
        if claim.session_id == sid:
            return True
        claim_pn = _normalize_pn(claim.collector_pn)
        if not claim_pn:
            return False
        for session in self._normalized_sessions():
            if session.session_id != sid:
                continue
            if session.state == SESSION_STATE_CLOSED:
                return False
            if not _pn_is_same_identity(session.collector_pn, claim_pn):
                return False
            session_owner = self._owner_for_session(session)
            if session_owner and session_owner != owner:
                return False
            claim.session_id = sid
            return True
        return False

    def session_handle_for_owned_session(
        self,
        entry_id: str,
        session_id: object,
    ) -> SessionHandle | None:
        """Negotiated handle for a session THIS owner's claim EXACTLY pins.

        Stricter than :meth:`session_handle_for_claimed_session`: all three of
        the registry claim owner, ``claim.session_id`` and the passed
        ``session_id`` must agree, and the socket must be live and not owned by
        anyone else. There is NO PN search and NO fallback to another same-PN
        session -- if the claim is not pinned to this exact socket, the caller
        must run an explicit registry operation first. Returns ``None``
        fail-closed for a closed/mismatched/foreign-owned session.
        """

        owner = str(entry_id or "").strip()
        sid = str(session_id or "").strip()
        if not owner or not sid:
            return None
        claim = self._claims.get(owner)
        if claim is None or claim.session_id != sid:
            return None
        for session in self._normalized_sessions():
            if session.session_id != sid:
                continue
            if session.state == SESSION_STATE_CLOSED:
                return None
            session_owner = self._owner_for_session(session)
            if session_owner and session_owner != owner:
                return None
            return negotiate_session_adapters(session.raw)
        return None

    def retarget_claim_to_reconnected_session(
        self,
        entry_id: str,
        session_id: object,
    ) -> bool:
        """Move a verification claim from its closed socket to a new candidate.

        This is only valid after the previously claimed physical session is
        terminal and only for a live session whose observed PN is the same
        identity (possibly the weak heartbeat prefix) as the claim's already
        strong durable PN. The caller may then perform a read-only identity
        query on exactly this socket. Peer IP is deliberately irrelevant.
        """

        owner = str(entry_id or "").strip()
        target_sid = str(session_id or "").strip()
        claim = self._claims.get(owner)
        if claim is None or not claim.collector_pn or not target_sid:
            return False
        sessions = {session.session_id: session for session in self._normalized_sessions()}
        previous = sessions.get(claim.session_id)
        if previous is not None and previous.state != SESSION_STATE_CLOSED:
            raise ValueError(f"previous_session_still_live:{claim.session_id}")
        target = sessions.get(target_sid)
        if (
            target is None
            or target.state == SESSION_STATE_CLOSED
            or not _pn_is_same_identity(claim.collector_pn, target.collector_pn)
        ):
            return False
        target_owner = self._owner_for_session(target)
        if target_owner and target_owner != owner:
            raise ValueError(f"session_already_claimed:{target_sid}:{target_owner}")
        claim.session_id = target_sid
        return True

    def reconcile_identity(self, *, session_id: object, full_pn: object) -> bool:
        """Promote a claim's short PN to a full PN observed on the same session.

        Reconciliation happens in exactly one place: here. Returns whether a
        claim was updated.
        """

        sid = str(session_id or "").strip()
        full = _normalize_pn(full_pn)
        if not sid or not full:
            return False
        for claim in self._claims.values():
            if claim.session_id == sid and _pn_is_same_identity(claim.collector_pn, full):
                if full != claim.collector_pn:
                    claim.collector_pn = _prefer_full_pn(claim.collector_pn, full)
                    return True
        return False

    def prepare_handoff(self, attempt_owner: str, full_pn: object) -> bool:
        """Mark a verification attempt's claim as a committed handoff (atomic).

        Called by the config flow the moment it is about to create/update the
        entry: it pins the attempt's claim to the strong full ``full_pn`` and
        flips it ``committed=True`` so entry setup may later complete exactly this
        handoff (looked up by PN). It does NOT change the owner id -- the attempt
        owner stays a unique ``callback_verification:<uuid>``; the PN is only an
        INDEX, never encoded into the owner. Single ownership is enforced: a PN
        already owned by another owner raises ``ValueError``. Only the attempt's
        own claim is touched; another flow's claim is never affected.
        """

        owner = str(attempt_owner or "").strip()
        claim = self._claims.get(owner)
        if claim is None:
            return False
        pn = _normalize_pn(full_pn)
        if not pn:
            raise ValueError("full_pn_required")
        other = self.owner_for_pn(pn)
        if other and other != owner:
            raise ValueError(f"session_already_claimed:{pn}:{other}")
        if claim.collector_pn and not _pn_is_same_identity(claim.collector_pn, pn):
            # The claim already stands for a DIFFERENT collector. Preparing it for
            # another identity would silently re-point a live claim (A -> B) and
            # hand the wrong collector to the entry. prefer_full_pn must never be
            # applied across identities -- it only ever enriches short -> full of
            # the SAME one. Refuse without mutating the claim.
            raise ValueError(f"handoff_identity_mismatch:{claim.collector_pn}:{pn}")
        claim.collector_pn = _prefer_full_pn(claim.collector_pn, pn)
        claim.handoff_pending = True
        return True

    def prepared_handoff_identity(self, attempt_owner: str, candidate_pn: object) -> str:
        """Return the CERTIFIED canonical PN for a prepared handoff, else ``""``.

        This is the registry's single public proof for a promotion trust
        boundary. A caller may not promote an entry on a PN it merely believes
        in: it must present an owner, and the registry answers with the identity
        it can actually vouch for. Non-empty only when ALL hold:

        * the owner still has a live claim here;
        * that claim is a committed handoff (``prepare_handoff`` ran);
        * the claim was taken on a CONCRETE observed session -- a PN-only claim
          (``claim(owner, collector_pn=...)``) never proves that this collector
          was seen on the wire for this attempt, so it can never promote;
        * the claim's durable PN is the SAME identity as ``candidate_pn``, judged
          by the registry's own short/full reconciliation -- never a string
          compare.

        The value returned is the CLAIM's PN, not the caller's: the claim holds
        the session-derived identity, so a caller passing a short prefix gets the
        full PN back and the entry is stamped with the fuller value. Callers must
        not reach into the claim map to answer this themselves.
        """

        owner = str(attempt_owner or "").strip()
        pn = _normalize_pn(candidate_pn)
        if not owner or not pn:
            return ""
        claim = self._claims.get(owner)
        if claim is None or not claim.handoff_pending:
            return ""
        if not claim.session_id:
            # Never certified: nothing was observed on the wire under this owner.
            return ""
        if not claim.collector_pn or not _pn_is_same_identity(claim.collector_pn, pn):
            return ""
        return _prefer_full_pn(claim.collector_pn, pn)

    def complete_handoff(self, full_pn: object, entry_id: str) -> bool:
        """Transfer a COMMITTED handoff for one PN to its permanent entry.

        Called by entry setup. It moves ONLY a committed handoff (never an active,
        uncommitted verification claim -- setup cannot steal a claim a flow is
        still proving). The move is atomic: the identity is never momentarily
        unowned. Returns ``False`` when no committed handoff exists for this PN
        (setup then claims the durable PN directly). Single ownership is enforced
        against a different permanent identity on the destination.

        Peer IP is never consulted here.
        """

        pn = _normalize_pn(full_pn)
        to_id = str(entry_id or "").strip()
        if not pn or not to_id:
            raise ValueError("handoff_args_required")
        source_owner = ""
        for owner, claim in self._claims.items():
            if (
                claim.handoff_pending
                and owner != to_id
                and claim.collector_pn
                and _pn_is_same_identity(claim.collector_pn, pn)
            ):
                source_owner = owner
                break
        if not source_owner:
            return False
        source = self._claims[source_owner]
        existing = self._claims.get(to_id)
        if (
            existing is not None
            and existing.collector_pn
            and not _pn_is_same_identity(existing.collector_pn, source.collector_pn)
        ):
            raise ValueError(f"session_already_claimed:{source.collector_pn}:{to_id}")
        # The destination is a SETTLED permanent claim (handoff_pending=False), so
        # it can never be re-completed out from under the entry.
        self._claims[to_id] = _Claim(
            entry_id=to_id,
            collector_pn=_prefer_full_pn(
                source.collector_pn, existing.collector_pn if existing else ""
            ),
            session_id=source.session_id,
            session_protocol=source.session_protocol,
            handoff_pending=False,
        )
        del self._claims[source_owner]
        return True

    def release(self, entry_id: str) -> bool:
        """Release the claim held by exactly one owner. Returns whether one existed.

        Only the named owner's claim is removed -- a concurrent flow/entry owning
        a different (even same-PN) claim is never touched.
        """

        return self._claims.pop(str(entry_id or "").strip(), None) is not None

    def claimed_identity(self, entry_id: str) -> str:
        """Return the durable PN currently claimed by one entry."""

        claim = self._claims.get(str(entry_id or "").strip())
        return claim.collector_pn if claim else ""

    # --- session handle / adapter negotiation ---------------------------------

    def session_handle_for_pn(self, collector_pn: object) -> SessionHandle | None:
        """Return the negotiated live SessionHandle for one durable PN identity.

        The handle's adapters/wire come from safe observation of the live session
        (byte shape + routed state), never from a persisted protocol hint. Returns
        ``None`` when no live session is observed for this identity yet.
        """

        pn = _normalize_pn(collector_pn)
        if not pn:
            return None
        # Rank over per-socket sessions (not the coalesced view) so a routed,
        # identity-established session always wins over a same-PN-prefix session
        # that is a route-identity mismatch or still awaiting identity. Untrusted
        # states negotiate to an unknown wire (``observed`` False), so they can
        # never become the runtime wire truth for this claimed identity.
        best_handle: SessionHandle | None = None
        best_rank: tuple[int, int, int, int] | None = None
        for session in self._normalized_sessions():
            if not _pn_is_same_identity(session.collector_pn, pn):
                continue
            handle = negotiate_session_adapters(session.raw)
            raw_state = str(session.raw.get("state") or "").strip().lower()
            rank = (
                1 if handle.observed else 0,
                1 if raw_state in ("routed_framed", "routed_at_text") else 0,
                1 if session.has_strong_identity else 0,
                len(session.collector_pn),
            )
            # Inventory is acceptance-ordered.  A same-quality replacement
            # socket must win over an older socket whose EOF/closed callback is
            # still in flight; otherwise runtime keeps targeting the stale
            # session id while every fresh callback remains visibly unbound.
            if best_rank is None or rank >= best_rank:
                best_rank = rank
                best_handle = handle
        return best_handle

    def session_handle_for_entry(self, entry_id: str) -> SessionHandle | None:
        """Return the negotiated live SessionHandle for one entry's claimed identity."""

        pn = self.claimed_identity(entry_id)
        if not pn:
            return None
        return self.session_handle_for_pn(pn)

    def session_handle_for_claimed_session(
        self,
        entry_id: str,
        *,
        expected_pn: object = "",
    ) -> SessionHandle | None:
        """Negotiated handle for EXACTLY the socket this entry's claim holds.

        Unlike :meth:`session_handle_for_entry` (PN-ranked across live sessions
        of a promoted durable identity), this is SESSION-ID-pinned: right for a
        transient verification claim that has not been promoted to a durable PN
        yet, and for management operations that must land on the one socket the
        claim owns -- never a same-PN sibling. Returns ``None`` when the claim
        holds no socket or the socket is no longer observed. ``expected_pn`` is
        used only to re-evaluate a route mismatch produced by another claimant;
        it never selects a session and only a strong same-PN observation can
        authorize that owner-scoped handle.
        """

        owner = str(entry_id or "").strip()
        claim = self._claims.get(owner)
        sid = claim.session_id if claim is not None else ""
        if not sid:
            return None
        for session in self._normalized_sessions():
            if session.session_id != sid:
                continue
            if session.state == SESSION_STATE_CLOSED:
                # A closed socket has no live wire: its remembered
                # protocol_shape must never resurrect a management adapter for
                # a connection that no longer exists.
                return None
            handle = negotiate_session_adapters(session.raw)
            raw_state = str(session.raw.get("state") or "").strip().lower()
            if raw_state != "route_identity_mismatch" or handle.observed:
                return handle

            # ``route_identity_mismatch`` is relative to a PREVIOUS route
            # claimant, not an intrinsic statement that this strongly
            # identified socket belongs to nobody.  On a shared listener an
            # offline entry may inspect collector B's fresh socket while
            # waiting for collector A and reject it for A.  If B's own
            # transaction then claims that EXACT session, the stale raw state
            # must not make B unreadable forever.
            #
            # The override is deliberately owner-scoped and read-only.  It is
            # available only when the exact registry claim, a strong identity
            # observed on this same socket, and either the claim's durable PN
            # or the caller's strict expected PN all agree.  A PN-less claim
            # without an expected identity, a weak observation, or a foreign
            # PN keeps the ordinary fail-closed handle.  The listener inventory
            # is not mutated; the session-pinned transport will claim the
            # physical socket before doing I/O.
            supplied = (
                expected_pn
                if type(expected_pn) is str and expected_pn == expected_pn.strip()
                else ""
            )
            claimed_pn = _normalize_pn(claim.collector_pn)
            anchor = claimed_pn or _normalize_pn(supplied)
            if (
                not anchor
                or not session.has_strong_identity
                or not session.collector_pn
                or not _pn_is_same_identity(anchor, session.collector_pn)
                or (
                    claimed_pn
                    and supplied
                    and not _pn_is_same_identity(claimed_pn, supplied)
                )
            ):
                return handle
            owned_observation = dict(session.raw)
            owned_observation["state"] = "claimed"
            return negotiate_session_adapters(owned_observation)
        return None

    def owned_session_location(self, entry_id: str) -> CallbackSession | None:
        """Return the best live observed session for one entry's claimed identity.

        This is the transport-ownership complement of ``session_handle_for_entry``:
        the handle carries the negotiated wire (which only becomes trusted once
        the socket is routed), while the LOCATION -- session_id + listener_port --
        is already meaningful for a parked/identified inbound socket that is
        waiting to be claimed. The runtime uses it to attach a transport facade
        to the listener the collector actually dialed (never derived from the
        endpoint hostname, peer IP, or collector type) and to claim exactly that
        session id. Closed and route-identity-mismatch sockets never qualify.
        """

        pn = self.claimed_identity(entry_id)
        if not pn:
            return None
        best: CallbackSession | None = None
        best_rank: tuple[int, int, int] | None = None
        for session in self._normalized_sessions():
            if not _pn_is_same_identity(session.collector_pn, pn):
                continue
            raw_state = str(session.raw.get("state") or "").strip().lower()
            if (
                session.state == SESSION_STATE_CLOSED
                or raw_state == "route_identity_mismatch"
            ):
                continue
            owner = self._owner_for_session(session)
            if owner and owner != str(entry_id or "").strip():
                # Single-owner invariant: never point a runtime at a socket a
                # different entry owns (e.g. a transient verification claim).
                continue
            rank = (
                1 if raw_state in ("routed_framed", "routed_at_text", "claimed") else 0,
                1 if session.has_strong_identity else 0,
                len(session.collector_pn),
            )
            # Same tie rule as current_session_for_pn/session_handle_for_pn:
            # later acceptance wins when identity, state and strength are
            # otherwise equal.  Peer IP is deliberately not part of ranking.
            if best_rank is None or rank >= best_rank:
                best_rank = rank
                best = session
        return best

    # --- diagnostics ----------------------------------------------------------

    def diagnostics(self) -> dict[str, object]:
        """Return an opaque, masking-free ownership view for support bundles."""

        sessions = self.observed_sessions()
        return {
            "claim_count": len(self._claims),
            "observed_session_count": len(sessions),
            "unclaimed_session_count": sum(
                1 for session in sessions if not session.owner_entry_id
            ),
            "claims": [
                {
                    "entry_id": claim.entry_id,
                    "collector_pn": claim.collector_pn,
                    "session_id": claim.session_id,
                    "session_protocol": claim.session_protocol,
                }
                for claim in self._claims.values()
            ],
            "sessions": [
                {
                    "session_id": session.session_id,
                    "peer_ip": session.peer_ip,
                    "listener_port": session.listener_port,
                    "collector_pn": session.collector_pn,
                    "identity_source": session.identity_source,
                    "state": session.state,
                    "owner_entry_id": session.owner_entry_id,
                }
                for session in sessions
            ],
        }
