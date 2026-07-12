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

Short/full PN reconciliation lives here and only here.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Callable

from .session_handle import SessionHandle, negotiate_session_adapters

# One canonical prefix-match length for short/full PN reconciliation. A weak
# short PN (e.g. the heartbeat prefix) can be a prefix of the full AT+DTUPN PN;
# below this length a prefix is too ambiguous to treat as the same collector.
CALLBACK_PN_PREFIX_MATCH_MIN_LEN = 10

# Normalized session lifecycle states.
SESSION_STATE_ACCEPTED = "accepted"
SESSION_STATE_IDENTIFIED_WEAK = "identified_weak"
SESSION_STATE_IDENTIFIED_STRONG = "identified_strong"
SESSION_STATE_CLAIMED = "claimed"
SESSION_STATE_ACTIVE = "active"
SESSION_STATE_CLOSED = "closed"

# Identity sources considered strong (a full, authoritative PN).
_STRONG_IDENTITY_SOURCES = frozenset({"at_dtupn", "fc2_parameter_2"})


def identity_source_is_strong(source: object) -> bool:
    """Return whether one observation is authoritative collector identity."""

    return str(source or "").strip() in _STRONG_IDENTITY_SOURCES


def prefer_identity_source(current: object, candidate: object) -> str:
    """Keep the strongest identity evidence observed for one live session.

    Heartbeats may arrive after an authoritative AT/FC2 identity query. They
    must not downgrade that already-established evidence merely because they
    are newer.
    """

    current_value = str(current or "").strip()
    candidate_value = str(candidate or "").strip()
    if identity_source_is_strong(current_value):
        return current_value
    if identity_source_is_strong(candidate_value):
        return candidate_value
    return candidate_value or current_value

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


def normalize_pn(value: object) -> str:
    """Return a trimmed collector PN string."""

    return str(value or "").strip()


def pn_is_same_identity(
    left: object,
    right: object,
    *,
    min_prefix_len: int = CALLBACK_PN_PREFIX_MATCH_MIN_LEN,
) -> bool:
    """Return whether two PNs denote the same durable collector identity.

    Exact match, or one is a prefix of the other and both are at least
    ``min_prefix_len`` characters. This is the *only* place short/full PN
    reconciliation is defined for ownership purposes.
    """

    a = normalize_pn(left)
    b = normalize_pn(right)
    if not a or not b:
        return False
    if a == b:
        return True
    if min(len(a), len(b)) < min_prefix_len:
        return False
    return a.startswith(b) or b.startswith(a)


def prefer_full_pn(left: object, right: object) -> str:
    """Return the more complete PN of two same-identity PNs (the longer one)."""

    a = normalize_pn(left)
    b = normalize_pn(right)
    if not a:
        return b
    if not b:
        return a
    return a if len(a) >= len(b) else b


def reconcile_pn(current: object, candidate: object) -> str:
    """Merge two PN observations into the more complete durable identity.

    Short PN enriches to the full PN when one is a prefix of the other; a genuine
    identity conflict (neither is a prefix of the other) keeps ``current`` rather
    than silently switching identity. This is the single home for connection- and
    runtime-level short/full PN reconciliation -- the transport, hub, and link
    all defer here instead of re-implementing the same prefix logic.
    """

    a = normalize_pn(current)
    b = normalize_pn(candidate)
    if not b:
        return a
    if not a:
        return b
    if a == b:
        return a
    if b.startswith(a):
        return b
    if a.startswith(b):
        return a
    return a


def _identity_is_strong(source: object) -> bool:
    return identity_source_is_strong(source)


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
    """One entry's ownership claim over a durable collector identity."""

    entry_id: str
    collector_pn: str = ""
    session_id: str = ""
    session_protocol: str = ""


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
            collector_pn=normalize_pn(raw.get("collector_pn")),
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
                if not pn_is_same_identity(existing.collector_pn, session.collector_pn):
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
                    merged_pn = prefer_full_pn(existing.collector_pn, session.collector_pn)
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

        pn = normalize_pn(collector_pn)
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
            elif not pn_is_same_identity(session.collector_pn, pn):
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
            if claim.collector_pn and pn_is_same_identity(
                claim.collector_pn, session.collector_pn
            ):
                return entry_id
        return ""

    def owner_for_pn(self, collector_pn: object) -> str:
        """Return the entry id that owns a durable PN identity, if any."""

        pn = normalize_pn(collector_pn)
        if not pn:
            return ""
        for entry_id, claim in self._claims.items():
            if claim.collector_pn and pn_is_same_identity(claim.collector_pn, pn):
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
        """

        entry_id = str(entry_id or "").strip()
        if not entry_id:
            raise ValueError("entry_id_required")
        pn = normalize_pn(collector_pn)
        sid = str(session_id or "").strip()

        if pn:
            other = self.owner_for_pn(pn)
            if other and other != entry_id:
                raise ValueError(f"session_already_claimed:{pn}:{other}")

        # Enrich the durable PN from the strongest matching observed session.
        matched: CallbackSession | None = None
        for session in self.observed_sessions():
            if sid and session.session_id == sid:
                matched = session
                break
            if pn and pn_is_same_identity(pn, session.collector_pn):
                if matched is None or (
                    session.has_strong_identity and not matched.has_strong_identity
                ):
                    matched = session
        if matched is not None:
            if matched.collector_pn:
                pn = prefer_full_pn(pn, matched.collector_pn)
            if not sid:
                sid = matched.session_id

        self._claims[entry_id] = _Claim(
            entry_id=entry_id,
            collector_pn=pn,
            session_id=sid,
            session_protocol=str(session_protocol or "").strip()
            or (matched.session_protocol if matched else ""),
        )
        if matched is None:
            return None
        return self._attach_owner(matched)

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
        for session in self._normalized_sessions():
            if session.session_id != sid:
                continue
            if session.collector_pn:
                owner = self.owner_for_pn(session.collector_pn)
                if owner and owner != entry_id:
                    raise ValueError(
                        f"session_already_claimed:{session.collector_pn}:{owner}"
                    )
            break

        self._claims[entry_id] = _Claim(entry_id=entry_id, session_id=sid)

    def promote_claim_to_full_pn(self, entry_id: str, full_pn: object) -> bool:
        """Promote a transient session claim to an exact full durable PN.

        Called once the claimed session reports strong identity. Enforces the
        single-owner invariant against durable PN claims; raises ``ValueError``
        when another entry already owns the identity. Returns whether the claim
        was updated.
        """

        entry_id = str(entry_id or "").strip()
        claim = self._claims.get(entry_id)
        if claim is None:
            return False
        pn = normalize_pn(full_pn)
        if not pn:
            return False
        other = self.owner_for_pn(pn)
        if other and other != entry_id:
            raise ValueError(f"session_already_claimed:{pn}:{other}")
        claim.collector_pn = pn
        return True

    def claimed_session_id(self, entry_id: str) -> str:
        """Return the transient session id recorded on one entry's claim."""

        claim = self._claims.get(str(entry_id or "").strip())
        return claim.session_id if claim is not None else ""

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
            or not pn_is_same_identity(claim.collector_pn, target.collector_pn)
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
        full = normalize_pn(full_pn)
        if not sid or not full:
            return False
        for claim in self._claims.values():
            if claim.session_id == sid and pn_is_same_identity(claim.collector_pn, full):
                if full != claim.collector_pn:
                    claim.collector_pn = prefer_full_pn(claim.collector_pn, full)
                    return True
        return False

    def release(self, entry_id: str) -> bool:
        """Release any claim held by one entry. Returns whether one existed."""

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

        pn = normalize_pn(collector_pn)
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
            if not pn_is_same_identity(session.collector_pn, pn):
                continue
            handle = negotiate_session_adapters(session.raw)
            raw_state = str(session.raw.get("state") or "").strip().lower()
            rank = (
                1 if handle.observed else 0,
                1 if raw_state in ("routed_framed", "routed_at_text") else 0,
                1 if session.has_strong_identity else 0,
                len(session.collector_pn),
            )
            if best_rank is None or rank > best_rank:
                best_rank = rank
                best_handle = handle
        return best_handle

    def session_handle_for_entry(self, entry_id: str) -> SessionHandle | None:
        """Return the negotiated live SessionHandle for one entry's claimed identity."""

        pn = self.claimed_identity(entry_id)
        if not pn:
            return None
        return self.session_handle_for_pn(pn)

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
            if not pn_is_same_identity(session.collector_pn, pn):
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
            if best_rank is None or rank > best_rank:
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
