"""The single rule for "did a collector answer THIS callback attempt?".

This is now the ONLY implementation. Every caller gets its verdict here and
nothing re-derives it:

* the config flow's manual/known-IP verification (an EXPECTED identity is known
  from passive discovery);
* the config flow's PN-less reconfigure repair (``bind_any``: no prior identity,
  so the probe's own result PN is the evidence);
* the pending entry's one bounded runtime attempt.

Callers keep their own side-effects (claiming/releasing the session, flow error
keys, typed pending statuses) but never their own matching. A second, laxer
matcher is exactly how a foreign collector gets bound. The rules, in order:

1. **Trigger provenance.** EXACTLY the attempt's own triggers may have been
   recorded while it ran -- no more and no fewer. More means someone else
   triggered concurrently and a new session is not attributable to us; fewer
   means our own trigger never went out, so an appearing session is a
   coincidence we must not claim credit for. Either way:
   ``callback_trigger_interference``. (A manual probe that confirms passively
   sends zero triggers and declares ``expected_own_triggers=0``.)
2. **Baseline.** A session that already existed before our trigger can never be
   the answer to it.
3. **Liveness / strength.** Closed, route-identity-mismatched, weak or PN-less
   observations never confirm.
4. **Identity evidence.** The probe's OWN result PN is the evidence of what this
   attempt reached (or the EXPECTED identity, when passive discovery already
   named the collector this flow is for). Only a same-identity session may bind.
   Without it, fail closed -- never "the first new strong session".
5. **Ambiguity.** If more than one DISTINCT strong identity appeared after the
   trigger, the attempt cannot prove which one answered it: bind nothing.

Peer IP, hostname, endpoint, collector kind and cloud family are never consulted:
identity is the durable full PN, reconciled only by the session registry.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .session_registry import pn_is_same_identity

# Typed outcomes (translation keys; never raw exception text).
MATCH_OK = ""
MATCH_TIMEOUT = "callback_timeout"
MATCH_IDENTITY_MISMATCH = "callback_identity_mismatch"
MATCH_IDENTITY_AMBIGUOUS = "callback_identity_ambiguous"
MATCH_TRIGGER_INTERFERENCE = "callback_trigger_interference"


@dataclass(frozen=True, slots=True)
class CallbackMatch:
    """The verdict for one callback attempt."""

    result: str
    session_id: str = ""
    collector_pn: str = ""

    @property
    def confirmed(self) -> bool:
        return self.result == MATCH_OK and bool(self.collector_pn)


def _is_same_identity(left: str, right: str) -> bool:
    """Exact match, or a strict weak->strong enrichment of the SAME identity."""

    if not left or not right:
        return False
    if left == right:
        return True
    return pn_is_same_identity(left, right)


def _distinct_identities(pns: Iterable[str]) -> list[str]:
    distinct: list[str] = []
    for pn in pns:
        if not pn:
            continue
        if not any(_is_same_identity(known, pn) for known in distinct):
            distinct.append(pn)
    return distinct


def match_callback_answer(
    sessions: Iterable[Mapping[str, Any]],
    *,
    baseline_session_ids: frozenset[str] | set[str],
    result_pn: str = "",
    expected_pn: str = "",
    old_session_id: str = "",
    trigger_generation_before: int | None = None,
    trigger_generation_after: int | None = None,
    expected_own_triggers: int = 1,
) -> CallbackMatch:
    """Return which session (if any) answered THIS attempt.

    ``result_pn`` is the identity the attempt's OWN probe reached; it is the
    evidence that binds a session to this attempt. ``expected_pn`` is a
    previously-known identity (passive discovery) that the answer must match.
    """

    baseline = frozenset(baseline_session_ids or ())

    # 1. Trigger provenance: EXACTLY our own trigger(s) must have fired, no more
    #    and no fewer. Too many means someone else triggered concurrently and a
    #    new session is no longer attributable to us; too few means our own
    #    trigger never went out, so nothing that appeared can be our answer
    #    either (it would be a coincidental dial-in we must not claim credit for).
    if trigger_generation_before is not None and trigger_generation_after is not None:
        fired = int(trigger_generation_after) - int(trigger_generation_before)
        if fired != max(0, int(expected_own_triggers)):
            return CallbackMatch(result=MATCH_TRIGGER_INTERFERENCE)

    # 4a. An expected identity gates the probe result itself.
    if expected_pn:
        if not result_pn:
            return CallbackMatch(result=MATCH_TIMEOUT)
        if not _is_same_identity(expected_pn, result_pn):
            return CallbackMatch(result=MATCH_IDENTITY_MISMATCH)

    # 2/3. Collect every NEW, live, strong, full-PN observation.
    fresh: list[tuple[str, str]] = []
    for session in sessions:
        session_id = str(session.get("session_id") or "").strip()
        if not session_id or session_id in baseline:
            continue
        if old_session_id and session_id == old_session_id:
            continue
        state = str(session.get("state") or "").strip().lower()
        if state.startswith("closed") or state == "route_identity_mismatch":
            continue
        if not session.get("has_strong_identity"):
            continue
        collector_pn = str(session.get("collector_pn") or "").strip()
        if not collector_pn:
            continue
        fresh.append((session_id, collector_pn))

    if not fresh:
        return CallbackMatch(result=MATCH_TIMEOUT)

    # 5. Ambiguity is judged on everything that appeared, BEFORE narrowing to our
    #    identity: two different collectors answering in our window means we
    #    cannot prove ours answered because of OUR trigger.
    distinct = _distinct_identities(pn for _sid, pn in fresh)
    if len(distinct) > 1:
        return CallbackMatch(result=MATCH_IDENTITY_AMBIGUOUS)

    # 4b. Identity evidence: bind only a session matching what THIS attempt
    #     actually reached (or the expected identity when one is known).
    anchor = expected_pn or result_pn
    if not anchor:
        # The probe confirmed no durable identity -> fail closed. Never adopt an
        # arbitrary new session just because it is strong.
        return CallbackMatch(result=MATCH_IDENTITY_MISMATCH)

    for session_id, collector_pn in fresh:
        if _is_same_identity(anchor, collector_pn):
            return CallbackMatch(
                result=MATCH_OK, session_id=session_id, collector_pn=collector_pn
            )

    # Something answered, but it is not the identity this attempt reached.
    return CallbackMatch(result=MATCH_IDENTITY_MISMATCH)


__all__ = [
    "CallbackMatch",
    "MATCH_IDENTITY_AMBIGUOUS",
    "MATCH_IDENTITY_MISMATCH",
    "MATCH_OK",
    "MATCH_TIMEOUT",
    "MATCH_TRIGGER_INTERFERENCE",
    "match_callback_answer",
]
