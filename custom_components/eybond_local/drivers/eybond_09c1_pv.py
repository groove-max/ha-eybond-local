"""Bounded, per-runtime PV1/PV2 qualification; no change to legacy 09C1 reads."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable

from ..payload.urtu09c1 import (
    PV_CHANNEL_COMMANDS, Urtu09C1Error, Urtu09C1Session, parse_pv_channel,
)
from .command_support import (
    command_skipped_as_unsupported, commit_cycle_failures, record_command_failure,
    record_command_success, unsupported_commands,
)

STATE_KEY = "urtu09c1_pv_reads"
_PREFIX = "09c1:"
_INTERVAL = 30.0


@dataclass
class PvSample:
    command: str
    next_due: float = 0
    sampled_at: float | None = None
    ttl: float = 60.0
    values: dict[str, object] = field(default_factory=dict)
    outcome: str = "not_checked"

    def clear(self) -> None:
        self.sampled_at = None
        self.values.clear()

    def fresh_values(self, now: float) -> dict[str, object]:
        if self.sampled_at is not None and not 0 <= now - self.sampled_at < self.ttl:
            self.clear()
            self.outcome = "expired"
        return dict(self.values)


@dataclass
class PvReads:
    # Runtime-scoped references only; never persist samples or infer identity.
    # This is a bounded sample cache, not a socket-identity certificate.
    # PayloadLinkTransport intentionally has no session-generation identity.
    transport: object
    inverter: object
    last_clock: float
    qualified: bool = False
    samples: tuple[PvSample, ...] = field(default_factory=lambda: tuple(
        PvSample(command) for command in PV_CHANNEL_COMMANDS
    ))

    def clear(self) -> None:
        self.qualified = False
        for sample in self.samples:
            sample.clear()
            sample.next_due = 0
            sample.outcome = "not_checked"

    async def refresh_one(
        self, session: Urtu09C1Session, runtime_state: dict,
        clock: Callable[[], float], *, ttl: float, link_available: bool = True,
    ) -> tuple[dict[str, object], dict[str, object]]:
        """One extra request at most, after Q1; use its existing 4-second bound."""
        now = clock()
        for sample in self.samples:
            if command_skipped_as_unsupported(runtime_state, _PREFIX + sample.command):
                sample.clear()
                sample.outcome = "unsupported"
            elif sample.outcome == "unsupported":
                # The existing explicit re-check action cleared negative facts.
                sample.next_due = now
                sample.outcome = "not_checked"
        due = [sample for sample in self.samples
               if sample.outcome != "unsupported" and now >= sample.next_due]
        sample = min(due, key=lambda item: item.next_due) if due else None
        updated_command = ""
        # A link already lost during a legacy optional read is not PV evidence.
        if not link_available or not session.transport.connected:
            self.clear()
            for item in self.samples:
                item.outcome = "connection_lost"
        elif sample is not None:
            key = _PREFIX + sample.command
            try:
                parsed = parse_pv_channel(await session.request(sample.command), command=sample.command)
            except (Urtu09C1Error, ConnectionError, asyncio.TimeoutError) as exc:
                sample.clear()
                sample.outcome = (
                    "timeout" if isinstance(exc, asyncio.TimeoutError) else "invalid_response"
                )
                if isinstance(exc, ConnectionError) or not session.transport.connected:
                    # Do not turn transport loss into an unsupported command.
                    self.clear()
                    for item in self.samples:
                        item.outcome = "connection_lost"
                else:
                    record_command_failure(runtime_state, key)
            else:
                sample.values = parsed
                sample.sampled_at = clock()
                # Set expiry at acquisition, never extend an old sample just
                # because a later poll supplies a longer adaptive interval.
                sample.ttl = ttl
                sample.outcome = "ok"
                updated_command = sample.command
                record_command_success(runtime_state, key)
            sample.next_due = clock() + _INTERVAL
        # Q1 succeeded. No await between staging and committing strikes; a
        # cancelled cycle cannot leave a strike to be committed by another poll.
        record_command_success(runtime_state, _PREFIX + "Q1")
        commit_cycle_failures(runtime_state)
        now = clock()
        self.last_clock = now
        values, diagnostics = {}, {}
        for sample in self.samples:
            if command_skipped_as_unsupported(runtime_state, _PREFIX + sample.command):
                sample.clear()
                sample.outcome = "unsupported"
            values.update(sample.fresh_values(now))
            if sample.sampled_at is not None:
                diagnostics[f"urtu09c1_{sample.command.lower()}_age_seconds"] = round(now - sample.sampled_at, 3)
        # Both commands must supply valid, still-current samples before this
        # runtime exposes any extension channel. PV? is never channel evidence.
        # Once qualified, a missing channel withdraws only itself, never a sum.
        if all(sample.values for sample in self.samples):
            self.qualified = True
        if not self.qualified:
            values.clear()
        diagnostics["urtu09c1_pv_qualified"] = self.qualified
        diagnostics["urtu09c1_pv_status"] = "; ".join(
            f"{sample.command}=" + (
                "cached" if sample.outcome == "ok" and sample.command != updated_command else sample.outcome
            ) for sample in self.samples
        )
        diagnostics["driver_unsupported_commands"] = ", ".join(unsupported_commands(runtime_state))
        return values, diagnostics


def pv_reads_for(runtime_state: dict, transport: object, inverter: object, now: float) -> PvReads:
    reads = runtime_state.get(STATE_KEY)
    if (
        type(reads) is not PvReads
        or reads.transport is not transport
        or reads.inverter is not inverter
        or now < reads.last_clock
    ):
        reads = PvReads(transport, inverter, now)
        runtime_state[STATE_KEY] = reads
    return reads
