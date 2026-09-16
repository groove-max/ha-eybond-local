"""Bounded optional FC4 reads, with per-runtime freshness and negative caching."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable

from ..payload.short_ascii import (
    RB_CURRENT_POWER_KEYS, ShortAsciiError, ShortAsciiSession,
    parse_f, parse_rb, parse_rh,
)
from .command_support import (
    command_skipped_as_unsupported, commit_cycle_failures, record_command_failure,
    record_command_success, unsupported_commands,
)
from .short_ascii_battery_dc import battery_dc_power_values
from .short_ascii_rb_filter import RbPublishFilter

STATE_KEY = "short_ascii_optional_reads"
_PREFIX = "short_ascii:"


@dataclass
class OptionalSample:
    command: str
    interval: float
    ttl: float
    parser: Callable[[bytes], dict[str, object]]
    next_due: float = 0
    sampled_at: float | None = None
    values: dict[str, object] = field(default_factory=dict)
    outcome: str = "not_checked"

    def clear(self) -> None:
        self.values.clear()
        self.sampled_at = None

    def fresh_values(self, now: float, *, hold_until: float | None = None) -> dict[str, object]:
        if self.sampled_at is not None:
            age = now - self.sampled_at
            within_ttl = 0 <= age < self.ttl
            within_hold = hold_until is not None and now < hold_until and age >= 0
            if not within_ttl and not within_hold:
                self.clear()
                self.outcome = "expired"
        return dict(self.values)


@dataclass
class OptionalReads:
    # These references are runtime scope, not a second source of device identity.
    # The hub clears runtime_state on recovery/rebinding. Never persist samples.
    transport: object
    inverter: object
    last_clock: float
    samples: tuple[OptionalSample, ...] = field(default_factory=lambda: (
        OptionalSample("RB", interval=30, ttl=60, parser=parse_rb),
        OptionalSample("F", interval=900, ttl=900, parser=parse_f),
        # Settings block; same cadence as F. Gate for RB current ÷10 publish.
        OptionalSample("RH", interval=900, ttl=900, parser=parse_rh),
    ))
    rb_filter: RbPublishFilter = field(default_factory=RbPublishFilter)

    def clear(self) -> None:
        for sample in self.samples:
            sample.clear()
            sample.next_due = 0
            sample.outcome = "not_checked"
        self.rb_filter.clear()

    def _sample(self, command: str) -> OptionalSample | None:
        for sample in self.samples:
            if sample.command == command:
                return sample
        return None

    def _fresh_sample(self, command: str, now: float) -> OptionalSample | None:
        sample = self._sample(command)
        if sample is None or sample.sampled_at is None:
            return None
        if not 0 <= now - sample.sampled_at < sample.ttl:
            return None
        return sample

    def _f_ratings(self, now: float) -> tuple[float | None, float | None, float | None]:
        sample = self._fresh_sample("F", now)
        if sample is None:
            return None, None, None
        rated_v = sample.values.get("short_ascii_rated_voltage")
        rated_a = sample.values.get("short_ascii_rated_current")
        rated_bat = sample.values.get("short_ascii_rated_battery_voltage")
        return (
            float(rated_v) if isinstance(rated_v, (int, float)) else None,
            float(rated_a) if isinstance(rated_a, (int, float)) else None,
            float(rated_bat) if isinstance(rated_bat, (int, float)) else None,
        )

    def _f_ratings_complete(self, now: float) -> bool:
        rated_v, rated_a, rated_bat = self._f_ratings(now)
        return (
            rated_v is not None and rated_v > 0
            and rated_a is not None and rated_a > 0
            and rated_bat is not None and rated_bat > 0
        )

    def _rh_decimals_enabled(self, now: float) -> bool:
        """True only while a fresh RH sample reports accuracy == 1 (with decimals)."""
        sample = self._fresh_sample("RH", now)
        if sample is None:
            return False
        return sample.values.get("short_ascii_bms_current_display_accuracy") == 1

    def _currents_publishable(self, now: float) -> bool:
        # T5.S3.W: RH=1 is the scale discriminator. F2: I/P need F for hard-reject.
        return self._rh_decimals_enabled(now) and self._f_ratings_complete(now)

    def _strip_ungated_currents(
        self, values: dict[str, object], now: float,
    ) -> dict[str, object]:
        if self._currents_publishable(now):
            return values
        out = dict(values)
        for key in RB_CURRENT_POWER_KEYS:
            out.pop(key, None)
        return out

    def _apply_rb_parse(self, sample: OptionalSample, parsed: dict[str, object], now: float) -> None:
        rated_v, rated_a, rated_bat = self._f_ratings(now)
        candidate = self._strip_ungated_currents(dict(parsed), now)
        # Derive measured DC only when I keys remain (RH=1 + F present).
        candidate.update(battery_dc_power_values(candidate))
        decision = self.rb_filter.decide(
            candidate, now=now,
            rated_voltage=rated_v, rated_current=rated_a, rated_battery_voltage=rated_bat,
        )
        sample.outcome = decision.outcome
        sample.next_due = now + sample.interval
        if decision.keep_previous:
            # Never restore held values without a real sampled_at (zombie guard).
            if (
                decision.outcome == "link_loss_hold"
                and decision.values is not None
                and sample.sampled_at is not None
                and not sample.values
            ):
                sample.values = dict(decision.values)
            return
        sample.values = dict(decision.values or ())
        if decision.refresh_sampled_at:
            sample.sampled_at = now

    async def refresh_one(
        self, session: ShortAsciiSession, runtime_state: dict,
        clock: Callable[[], float],
    ) -> tuple[dict[str, object], dict[str, object]]:
        """At most one extra query per successful Q1 cycle; no discovery budget."""
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
        # Oldest scheduled group first; an unsupported RB cannot starve F/RH.
        sample = min(due, key=lambda item: item.next_due) if due else None
        if sample is not None:
            key = _PREFIX + sample.command
            try:
                parsed = sample.parser(await session.request(sample.command))
            except (ShortAsciiError, asyncio.TimeoutError) as exc:
                # Envelope/transport failure: drop; do not start 180 s hold.
                sample.clear()
                if sample.command == "RB":
                    self.rb_filter.clear()
                sample.outcome = "timeout" if isinstance(exc, asyncio.TimeoutError) else "invalid_response"
                sample.next_due = clock() + 30
                if not session.transport.connected:
                    raise ConnectionError("short_ascii_optional_connection_lost") from None
                record_command_failure(runtime_state, key)
            else:
                if sample.command == "RB":
                    self._apply_rb_parse(sample, parsed, clock())
                else:
                    sample.values = dict(parsed)
                    sample.sampled_at = clock()
                    sample.next_due = sample.sampled_at + sample.interval
                    sample.outcome = "ok"
                record_command_success(runtime_state, key)
        # No staged strike survives a failed/cancelled cycle. Q1 was confirmed
        # by our caller, and from here to commit there are no suspension points.
        record_command_success(runtime_state, _PREFIX + "Q1")
        commit_cycle_failures(runtime_state)
        now = clock()
        self.last_clock = now
        values, diagnostics = {}, {}
        for sample in self.samples:
            hold_until = self.rb_filter.hold_until if sample.command == "RB" else None
            fresh = sample.fresh_values(now, hold_until=hold_until)
            # TTL/expiry cleared the sample: drop last_good so a later link-loss
            # cannot resurrect values with sampled_at is None.
            if sample.command == "RB" and sample.sampled_at is None:
                self.rb_filter.clear()
            if sample.command == "RB":
                # Re-gate every cycle: RH/F expiry must strip I/P from held RB.
                fresh = self._strip_ungated_currents(fresh, now)
            elif sample.command == "RH":
                # Settings gate only — do not publish RH fields as sensors.
                fresh = {}
            if sample.sampled_at is not None:
                diagnostics[f"short_ascii_{sample.command.lower()}_age_seconds"] = round(
                    now - sample.sampled_at, 3,
                )
            values.update(fresh)
        diagnostics["short_ascii_optional_status"] = "; ".join(
            f"{sample.command}={sample.outcome}" for sample in self.samples
        )
        diagnostics["driver_unsupported_commands"] = ", ".join(
            key for key in unsupported_commands(runtime_state) if key.startswith(_PREFIX)
        )
        # Q1.C1: publish every cycle (hub replaces diagnostics wholesale).
        diagnostics.update(self.rb_filter.diagnostic_counters())
        return values, diagnostics


def optional_reads_for(runtime_state: dict, transport: object, inverter: object, now: float) -> OptionalReads:
    reads = runtime_state.get(STATE_KEY)
    if (
        type(reads) is not OptionalReads
        or reads.transport is not transport
        or reads.inverter is not inverter
        or now < reads.last_clock
    ):
        reads = OptionalReads(transport, inverter, now)
        runtime_state[STATE_KEY] = reads
    return reads
