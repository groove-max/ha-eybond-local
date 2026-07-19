"""Attempt-scoped, exact-session identity resolution for the AUTOMATIC scan.

The automatic normal/deep scan must identify a callback collector on its EXACT
session id whenever the peer-IP claim cannot -- e.g. a stale same-peer-IP socket
of another route is parked -- regardless of whether the fresh session is fully
silent, has volunteered a WEAK identity (a short framed heartbeat PN), or has
already volunteered a STRONG identity. All three are handled by ONE selector over
the union of the listener's public session observations, keyed by session id.
It does so WITHOUT any of the things that would make it unsafe:

* it never selects a session by peer IP, socket order or PN prefix -- only ONE
  post-baseline session (observed inside the attempt's trigger window) is
  eligible, and two are typed ambiguity (no pick at all);
* it never reads collector kind / cloud family / hostname / a persisted expected
  protocol; the wire is the attempt's OWN typed capability
  (:class:`AutomaticFramedIdentityIntent`), framed-only by construction, and a
  raw/duck argument cannot authorize a probe;
* it never registers a listener-wide protocol owner -- every read goes through
  the ONE public boundary
  (``SilentSessionIdentityProbeChannel.async_identify_exact_session``);
* it performs at most ONE read-only framed FC=2 parameter-2 query, and never on
  a session that already carries a strong identity.

HONEST causality: the caller holds the EXCLUSIVE callback causality lease across
the baseline, the single trigger and this resolution, but that lease serialises
only THIS process's competing callback sends. It does NOT exclude a delayed
earlier callback, an external trigger, or a self-initiated inbound connect. So a
post-baseline session is merely one *observed inside an integration-serialised
trigger window* -- the strong identity is proven by the exact-session FC=2 read
(or an already-recorded strong source), NOT by the lease. This resolution is an
identity fact only: it is NOT a RecoveryProof and NOT proof of any
connection_strategy, and it creates no RecoveryContract and no inbound/callback
strategy.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ..connection.session_handle import WIRE_FRAMED

# The only accepted provenance for an automatic framed identity attempt. It is
# deliberately NOT the user-explicit ``BOOTSTRAP_SOURCE_EXPLICIT_USER`` -- these
# two authority kinds never substitute for one another.
AUTOMATIC_FRAMED_IDENTITY_SOURCE = "automatic_onboarding_attempt"


@dataclass(frozen=True, slots=True)
class AutomaticFramedIdentityIntent:
    """Typed capability authorizing ONE framed FC=2 parameter-2 identity read.

    Framed-only BY CONSTRUCTION: the automatic scan speaks only ``eybond_framed``;
    fully-silent AT auto-support stays honestly unsupported (a repeat set>server
    makes no fresh socket and framed/AT must never share one). This is a strict
    typed CAPABILITY, not a wire string -- the resolver accepts ONLY this exact
    type, so a duck / raw argument cannot authorize a probe. Minted only by the
    detector itself, never from collector metadata. The underlying
    ``SilentSessionIdentityProbeChannel`` stays wire-neutral for other callers.
    """

    source: str = AUTOMATIC_FRAMED_IDENTITY_SOURCE

    def __post_init__(self) -> None:
        if (
            type(self.source) is not str
            or self.source != AUTOMATIC_FRAMED_IDENTITY_SOURCE
        ):
            raise ValueError("automatic_framed_identity_source_invalid")


@dataclass(frozen=True, slots=True)
class SilentIdentityResolution:
    """The typed outcome of one automatic exact-session identity attempt."""

    session_id: str = ""
    collector_pn: str = ""
    # Two or more post-baseline sessions appeared in the union view: attribution is
    # impossible without a peer-IP / order tiebreak, so nothing is probed/adopted.
    ambiguous: bool = False

    @property
    def identified(self) -> bool:
        return bool(self.session_id and self.collector_pn)


# The bounded window (seconds) the resolution waits, under the lease, for the
# collector triggered by THIS attempt to dial in. Kept tight so concurrent
# attempts serialize only briefly on the exclusive lease; the caller further
# bounds it by the connect budget.
DEFAULT_SILENT_IDENTITY_WAIT_SECONDS = 3.0


async def async_resolve_silent_session_identity(
    probe_channel,
    *,
    wire_intent: AutomaticFramedIdentityIntent,
    baseline,
    deadline: float,
    old_session_id: str = "",
    poll_interval: float = 0.05,
) -> SilentIdentityResolution:
    """Resolve the identity of the ONE post-baseline session, or fail closed.

    ``probe_channel`` is a :class:`SilentSessionIdentityProbeChannel` the caller
    already opened; ``baseline`` is the frozenset of session ids observed in the
    union view (PN-less pending AND already-identified) BEFORE this attempt's
    trigger, captured under the same lease. A post-baseline session is one
    observed inside the integration-serialised trigger window -- the lease does
    not *prove* the trigger caused it; only the exact-session read (or an
    already-recorded strong source) proves identity.

    Exactly ONE fresh (post-baseline) session id in the union view is eligible:

    * it already carries a STRONG identity (``fc2_parameter_2`` / ``at_dtupn``):
      accept its full PN and pin the exact session -- NO probe (this is how a
      passive AT+DTUPN banner is adopted untouched);
    * else (PN-less silent, or a WEAK short framed-heartbeat PN): UPGRADE to a
      strong/full PN with ONE framed FC=2 read on that exact session id.

    Two or more fresh session ids -> typed ambiguity; a baseline session is never
    eligible; nothing is ever selected by peer IP, order or prefix.
    """

    from ..connection.session_registry import identity_source_is_strong

    if probe_channel is None or not getattr(probe_channel, "available", False):
        return SilentIdentityResolution()
    if type(wire_intent) is not AutomaticFramedIdentityIntent:
        # Fail closed on ducks: only the strict typed capability authorizes a
        # probe. Nothing is observed or probed.
        return SilentIdentityResolution()

    baseline_ids = frozenset(baseline or ())
    loop = asyncio.get_running_loop()
    while True:
        observations = probe_channel.snapshot_session_observations()
        fresh = [
            obs
            for obs in observations
            if obs.session_id
            and obs.session_id not in baseline_ids
            and obs.session_id != old_session_id
        ]
        if len(fresh) == 1:
            obs = fresh[0]
            pn = str(obs.collector_pn or "").strip()
            if pn and identity_source_is_strong(obs.identity_source):
                # Already strong -> accept + pin the EXACT session, NO re-probe.
                return SilentIdentityResolution(
                    session_id=obs.session_id, collector_pn=pn
                )
            # PN-less silent OR weak framed heartbeat -> upgrade to a strong/full
            # PN with ONE framed FC=2 read on the SAME exact session id.
            probed = await probe_channel.async_identify_exact_session(
                obs.session_id, session_protocol=WIRE_FRAMED
            )
            return SilentIdentityResolution(
                session_id=obs.session_id, collector_pn=str(probed or "").strip()
            )
        if len(fresh) > 1:
            # Never resolve ambiguity by peer IP or arrival order.
            return SilentIdentityResolution(ambiguous=True)
        if loop.time() >= deadline:
            return SilentIdentityResolution()
        await asyncio.sleep(poll_interval)


__all__ = [
    "AUTOMATIC_FRAMED_IDENTITY_SOURCE",
    "AutomaticFramedIdentityIntent",
    "DEFAULT_SILENT_IDENTITY_WAIT_SECONDS",
    "SilentIdentityResolution",
    "async_resolve_silent_session_identity",
]
