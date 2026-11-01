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
from .short_ascii_mppt_optional import (
    ADMIT_OPTION_KEY, COMMAND as MPPT_COMMAND, INTERVAL as MPPT_INTERVAL,
    STRUCTURAL_BACKOFF, TTL as MPPT_TTL, MpptPollDiag, is_admitted,
    is_structural_aux_error, request_runtime_sample,
)
from .short_ascii_rb_filter import RbPublishFilter

STATE_KEY = "short_ascii_optional_reads"
_PREFIX = "short_ascii:"
# Envelope/transport soft errors. MPPT soft-handles ConnectionError (fence);
# FC4 re-raises disconnect so Q1 wipe stays correct.
_SOFT_ERRORS = (ShortAsciiError, asyncio.TimeoutError, ConnectionError, ValueError, TypeError)


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

    def fresh_values(self, now: float) -> dict[str, object]:
        if self.sampled_at is not None and not 0 <= now - self.sampled_at < self.ttl:
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
        # Aux 0200 only when admitted; parser unused — request path special-cased.
        OptionalSample(MPPT_COMMAND, interval=MPPT_INTERVAL, ttl=MPPT_TTL, parser=dict),
    ))
    rb_filter: RbPublishFilter = field(default_factory=RbPublishFilter)
    # Quiet stuck-aux counters. Survive optional.clear() on a Q1 miss.
    mppt_diag: MpptPollDiag = field(default_factory=MpptPollDiag)

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
        # RH=1 is the scale discriminator. F must be present for I/P bound evidence.
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

    async def _request_mppt(self) -> dict[str, object]:
        """Solicit documented 0200 only via the shared facade; never 0202."""

        return await request_runtime_sample(self.transport)

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
            # Hard-reject: leave prior values + sampled_at for ~60 s TTL.
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
        mppt_admitted = is_admitted(runtime_state)
        diag = self.mppt_diag
        for sample in self.samples:
            if sample.command == MPPT_COMMAND and not mppt_admitted:
                # Stock installs must not poll aux 0200.
                sample.clear()
                sample.outcome = "not_admitted"
                sample.next_due = now + sample.interval
                diag.note_not_admitted(self.transport)
                continue
            if command_skipped_as_unsupported(runtime_state, _PREFIX + sample.command):
                sample.clear()
                sample.outcome = "unsupported"
            elif sample.outcome == "unsupported":
                # The existing explicit re-check action cleared negative facts.
                sample.next_due = now
                sample.outcome = "not_checked"
            elif sample.outcome == "not_admitted" and mppt_admitted:
                sample.next_due = now
                sample.outcome = "not_checked"
        if mppt_admitted:
            diag.note_due(any(
                sample.command == MPPT_COMMAND and sample.outcome != "unsupported"
                and now >= sample.next_due
                for sample in self.samples
            ))
        due = [sample for sample in self.samples
               if sample.outcome not in ("unsupported", "not_admitted")
               and now >= sample.next_due]
        # Oldest due first within the chosen set. Prefer FC4 (RB/F/RH) over aux
        # MPPT when both are due so a virgin MPPT next_due=0 cannot starve an
        # intentional RB refresh; MPPT still runs when it is the only due sample.
        # While MPPT is stale, the first RB+MPPT collision takes MPPT so prefer-
        # FC4 cannot starve 0200 across reconnect wipes. F/RH still prefer FC4;
        # fresh MPPT keeps prefer-FC4.
        fc4_due = [sample for sample in due if sample.command != MPPT_COMMAND]
        prefer_fc4 = bool(fc4_due) and any(s.command == MPPT_COMMAND for s in due)
        rb_contending = any(s.command == "RB" for s in fc4_due)
        mppt_sample = next((s for s in due if s.command == MPPT_COMMAND), None)
        mppt_stale = (
            mppt_sample is None
            or mppt_sample.sampled_at is None
            or (now - mppt_sample.sampled_at) > MPPT_TTL
        )
        force_mppt = bool(
            prefer_fc4 and rb_contending and mppt_stale and mppt_sample is not None
        )
        if force_mppt:
            candidates = [mppt_sample]
        else:
            candidates = fc4_due or due
        sample = min(candidates, key=lambda item: item.next_due) if candidates else None
        if force_mppt:
            diag.note_forced_anti_starve()
        elif prefer_fc4 and sample is not None and sample.command != MPPT_COMMAND:
            diag.note_prefer_fc4_skip(contending_rb=rb_contending)
        if sample is not None:
            key = _PREFIX + sample.command
            mppt_retried = False
            try:
                if sample.command == MPPT_COMMAND:
                    diag.note_attempt(self.transport)
                    try:
                        parsed = await self._request_mppt()
                    except _SOFT_ERRORS as first_exc:
                        # Structural TypeError cannot recover mid-cycle; flaky
                        # aux may. Disconnect skips the second send.
                        if is_structural_aux_error(first_exc):
                            raise
                        if not session.transport.connected:
                            raise
                        mppt_retried = True
                        parsed = await self._request_mppt()
                else:
                    frame = await session.request(sample.command)
                    parsed = sample.parser(frame)
            except _SOFT_ERRORS as exc:
                # Envelope/transport failure: drop; do not invent a hold.
                sample.clear()
                if sample.command == "RB":
                    self.rb_filter.clear()
                sample.outcome = (
                    "timeout"
                    if isinstance(exc, (asyncio.TimeoutError, ConnectionError))
                    else "invalid_response"
                )
                if sample.command == MPPT_COMMAND:
                    diag.note_fail(self.transport, exc)
                    if is_structural_aux_error(exc):
                        # Missing facade: long backoff, never next_due=now storm.
                        sample.next_due = clock() + STRUCTURAL_BACKOFF
                    else:
                        # No 30 s penalty: due again next poll. Aux 0200 is flaky
                        # under contention and may fence the socket; never raise
                        # that into Q1 wipe, and never feed the unsupported cache.
                        sample.next_due = clock()
                else:
                    sample.next_due = clock() + 30
                    if isinstance(exc, ConnectionError) or not session.transport.connected:
                        raise ConnectionError("short_ascii_optional_connection_lost") from None
                    record_command_failure(runtime_state, key)
            else:
                if sample.command == "RB":
                    self._apply_rb_parse(sample, parsed, clock())
                else:
                    sample.values = dict(parsed)
                    sample.sampled_at = clock()
                    sample.next_due = sample.sampled_at + sample.interval
                    sample.outcome = (
                        "no_data" if parsed.get("short_ascii_bms_data_available") is False else "ok"
                    )
                    if sample.command == MPPT_COMMAND:
                        diag.note_ok(sample.sampled_at, retried=mppt_retried)
                record_command_success(runtime_state, key)
        # No staged strike survives a failed/cancelled cycle. Q1 was confirmed
        # by our caller, and from here to commit there are no suspension points.
        record_command_success(runtime_state, _PREFIX + "Q1")
        commit_cycle_failures(runtime_state)
        now = clock()
        self.last_clock = now
        values, diagnostics = {}, {}
        for sample in self.samples:
            fresh = sample.fresh_values(now)
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
        diagnostics.update(self.rb_filter.diagnostic_counters())
        diagnostics.update(diag.as_diagnostics(now))
        if not mppt_admitted:
            diagnostics["short_ascii_mppt_admission"] = ADMIT_OPTION_KEY
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
