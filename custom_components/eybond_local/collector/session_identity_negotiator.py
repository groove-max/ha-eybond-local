"""Bounded exact-session negotiation for a fully silent collector socket.

The callback trigger and causality lease remain the caller's responsibility.
This object answers only: which single bounded identity dialect may be sent
to which exact observed session during this attempt?

One attempt probes one physical socket with one dialect.  If a completely
silent socket does not answer, that exact socket is retired before the next
normal callback attempt may try the alternate dialect.  This prevents an AT
query from following a framed query (or vice versa) on one stream while keeping
the established one-``set>server``-per-attempt contract intact.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from ..collector_identity import (
    identity_source_is_strong,
    pn_is_same_identity,
    validated_collector_pn,
)
from .identity_probe import (
    PROBE_AT_DTUPN,
    PROBE_FRAMED_FC1,
    PROBE_FRAMED_FC2,
    silent_probe_kind_for_protocol,
)
from .silent_session_probe import SessionObservation

NEGOTIATION_IDENTIFIED = "identified"
NEGOTIATION_NO_SESSION = "no_session"
NEGOTIATION_AMBIGUOUS = "ambiguous"
NEGOTIATION_FOREIGN_IDENTITY = "foreign_identity"
NEGOTIATION_PROBE_FAILED = "probe_failed"

_TERMINAL_STATES = frozenset(
    {
        "closed_disconnected",
        "closed_identity_negotiation_retry",
        "closed_no_payload",
        "parked_evicted",
        "parked_expired",
        "parked_peer_closed",
        "parked_read_failed",
    }
)


class ExactSessionProbeChannel(Protocol):
    """Minimal exact-session channel consumed by the negotiator."""

    def snapshot_session_observations(self) -> tuple[SessionObservation, ...]: ...

    async def async_identify_exact_session(
        self,
        session_id: str,
        *,
        session_protocol: str,
        identity_probe_kind: str = "",
    ) -> str: ...

    async def async_retire_exact_session(self, session_id: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class ExactSessionIdentityResult:
    """Terminal result of one bounded session-identity attempt."""

    status: str
    session_id: str = ""
    collector_pn: str = ""
    session_protocol: str = ""
    identity_source: str = ""
    probe_kind: str = ""

    @property
    def identified(self) -> bool:
        return self.status == NEGOTIATION_IDENTIFIED


def _protocol_from_observation(observation: SessionObservation) -> str:
    source = observation.identity_source
    shape = observation.protocol_shape
    if source in {
        "fc1_identity_challenge",
        "fc2_parameter_2",
        "framed_heartbeat",
    } or shape in {"eybond_framed", "eybond_framed_or_binary"}:
        return "eybond_framed"
    if source == "at_dtupn" or shape == "at_text":
        return "at_text"
    return ""


def _known_probe_for_observation(
    observation: SessionObservation,
) -> tuple[str, str]:
    protocol = _protocol_from_observation(observation)
    if protocol == "eybond_framed":
        return protocol, PROBE_FRAMED_FC2
    if protocol == "at_text":
        return protocol, PROBE_AT_DTUPN
    return "", ""


async def _retire_exact_session_critical(
    channel: ExactSessionProbeChannel,
    session_id: str,
) -> bool:
    """Retire a dialect-touched socket despite repeated caller cancellation.

    The caller's cancellation is re-raised only after the one retirement task
    reaches a terminal state; no cleanup task is left detached.
    """

    task = asyncio.create_task(channel.async_retire_exact_session(session_id))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
    try:
        result = bool(task.result())
    except Exception:
        result = False
    if cancelled:
        raise asyncio.CancelledError
    return result


class ExactSessionIdentityNegotiator:
    """Choose one safe identity challenge per ordinary callback attempt."""

    def __init__(self) -> None:
        self._next_unknown_candidate = 0

    def _unknown_candidate(self, preferred_protocol: str) -> tuple[str, str]:
        if preferred_protocol == "at_text":
            ordered = ("at_text", "eybond_framed")
        else:
            # Framed FC=1 is the safest default for the historical EyeBond
            # server protocol and matches the issue-37 wire capture. Metadata
            # may only reorder this tuple; it never becomes evidence.
            ordered = ("eybond_framed", "at_text")
        protocol = ordered[self._next_unknown_candidate % len(ordered)]
        return protocol, silent_probe_kind_for_protocol(protocol)

    def _advance_unknown_candidate(self) -> None:
        self._next_unknown_candidate = (self._next_unknown_candidate + 1) % 2

    def _reset_unknown_candidate(self) -> None:
        self._next_unknown_candidate = 0

    async def async_negotiate(
        self,
        *,
        channel: ExactSessionProbeChannel,
        expected_pn: str,
        baseline_session_ids: frozenset[str],
        deadline: float,
        preferred_protocol: str = "",
    ) -> ExactSessionIdentityResult:
        """Identify one exact live socket, failing closed on ambiguity/foreign PN."""

        durable_pn = validated_collector_pn(expected_pn)
        preferred = (
            preferred_protocol
            if preferred_protocol in {"eybond_framed", "at_text"}
            else ""
        )
        baseline = frozenset(
            session_id
            for session_id in baseline_session_ids
            if type(session_id) is str
            and session_id
            and session_id == session_id.strip()
        )
        if not durable_pn:
            return ExactSessionIdentityResult(NEGOTIATION_FOREIGN_IDENTITY)

        loop = asyncio.get_running_loop()
        selected: SessionObservation | None = None
        while loop.time() < deadline:
            observations = tuple(
                observation
                for observation in channel.snapshot_session_observations()
                if type(observation) is SessionObservation
                and observation.session_id
                and observation.state not in _TERMINAL_STATES
                and not observation.state.startswith("closed")
            )
            fresh = tuple(
                observation
                for observation in observations
                if observation.session_id not in baseline
            )
            if len(fresh) > 1:
                return ExactSessionIdentityResult(NEGOTIATION_AMBIGUOUS)
            if len(fresh) == 1:
                selected = fresh[0]
            else:
                matching_strong = tuple(
                    observation
                    for observation in observations
                    if observation.collector_pn
                    and identity_source_is_strong(observation.identity_source)
                    and pn_is_same_identity(durable_pn, observation.collector_pn)
                )
                if len(matching_strong) > 1:
                    return ExactSessionIdentityResult(NEGOTIATION_AMBIGUOUS)
                if len(matching_strong) == 1:
                    selected = matching_strong[0]
                else:
                    unresolved = tuple(
                        observation
                        for observation in observations
                        if not (
                            observation.collector_pn
                            and identity_source_is_strong(
                                observation.identity_source
                            )
                        )
                    )
                    if len(unresolved) > 1:
                        return ExactSessionIdentityResult(NEGOTIATION_AMBIGUOUS)
                    if len(unresolved) == 1:
                        selected = unresolved[0]

            if selected is not None:
                break
            await asyncio.sleep(min(0.05, max(0.0, deadline - loop.time())))

        if selected is None:
            return ExactSessionIdentityResult(NEGOTIATION_NO_SESSION)

        observed_pn = validated_collector_pn(selected.collector_pn)
        if observed_pn and identity_source_is_strong(selected.identity_source):
            if not pn_is_same_identity(durable_pn, observed_pn):
                return ExactSessionIdentityResult(
                    NEGOTIATION_FOREIGN_IDENTITY,
                    session_id=selected.session_id,
                    collector_pn=observed_pn,
                    identity_source=selected.identity_source,
                )
            protocol = _protocol_from_observation(selected)
            if not protocol:
                return ExactSessionIdentityResult(NEGOTIATION_PROBE_FAILED)
            self._reset_unknown_candidate()
            return ExactSessionIdentityResult(
                NEGOTIATION_IDENTIFIED,
                session_id=selected.session_id,
                collector_pn=observed_pn,
                session_protocol=protocol,
                identity_source=selected.identity_source,
            )

        if observed_pn and not pn_is_same_identity(durable_pn, observed_pn):
            # A weak non-matching PN is not positive identity, but it is enough
            # to refuse writing a speculative dialect to that socket.
            return ExactSessionIdentityResult(
                NEGOTIATION_FOREIGN_IDENTITY,
                session_id=selected.session_id,
                collector_pn=observed_pn,
                identity_source=selected.identity_source,
            )

        protocol, probe_kind = _known_probe_for_observation(selected)
        unknown_wire = not protocol
        if unknown_wire:
            protocol, probe_kind = self._unknown_candidate(preferred)

        try:
            identified_pn = await channel.async_identify_exact_session(
                selected.session_id,
                session_protocol=protocol,
                identity_probe_kind=probe_kind,
            )
        except asyncio.CancelledError as original_cancel:
            if unknown_wire:
                try:
                    await _retire_exact_session_critical(
                        channel,
                        selected.session_id,
                    )
                except asyncio.CancelledError:
                    pass
            raise original_cancel

        strong_pn = validated_collector_pn(identified_pn)
        if strong_pn:
            if not pn_is_same_identity(durable_pn, strong_pn):
                return ExactSessionIdentityResult(
                    NEGOTIATION_FOREIGN_IDENTITY,
                    session_id=selected.session_id,
                    collector_pn=strong_pn,
                    session_protocol=protocol,
                    probe_kind=probe_kind,
                )
            post = next(
                (
                    observation
                    for observation in channel.snapshot_session_observations()
                    if observation.session_id == selected.session_id
                ),
                selected,
            )
            self._reset_unknown_candidate()
            return ExactSessionIdentityResult(
                NEGOTIATION_IDENTIFIED,
                session_id=selected.session_id,
                collector_pn=strong_pn,
                session_protocol=protocol,
                identity_source=post.identity_source,
                probe_kind=probe_kind,
            )

        if unknown_wire:
            retired = await _retire_exact_session_critical(
                channel,
                selected.session_id,
            )
            still_present = any(
                observation.session_id == selected.session_id
                for observation in channel.snapshot_session_observations()
            )
            # Do not select another dialect until the old physical stream is
            # certainly gone. A raced peer-close is equivalent to retirement.
            if retired or not still_present:
                self._advance_unknown_candidate()
        return ExactSessionIdentityResult(
            NEGOTIATION_PROBE_FAILED,
            session_id=selected.session_id,
            session_protocol=protocol,
            identity_source=selected.identity_source,
            probe_kind=probe_kind,
        )


__all__ = [
    "ExactSessionIdentityNegotiator",
    "ExactSessionIdentityResult",
    "NEGOTIATION_AMBIGUOUS",
    "NEGOTIATION_FOREIGN_IDENTITY",
    "NEGOTIATION_IDENTIFIED",
    "NEGOTIATION_NO_SESSION",
    "NEGOTIATION_PROBE_FAILED",
]
