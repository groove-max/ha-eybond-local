"""Optional live MPPT via the documented auxiliary 0200 read only.

Solicits through ``link_transport.async_auxiliary_read`` (framed/AT facade).
Does not harvest tip AABB, wait on EyeBond TID ambiguity, or merge settings
0202. Stock installs must set entry option ``admit_short_ascii_mppt`` before
any 0200 poll; ``enabled_default: false`` alone only hides entities.
Live PV/MPPT measurement keys stay opt-in (``enabled_default: false``);
quiet ``mppt_error_code`` / ``mppt_error`` diagnostics default on in
``eybond_short_ascii/base.json``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from ..link_transport import async_auxiliary_read
from ..payload.short_ascii_mppt import mppt_error_label, parse_mppt_runtime_wire

# Exact 21-byte read allow-listed by AuxiliaryReadSession._READ_QUERIES.
RUNTIME_QUERY_0200 = b"\x5a\xa5\x02\x00" + bytes(16) + b"\x02"
COMMAND = "MPPT"
# Config-entry options / runtime_state key. Default absent/false = no poll.
ADMIT_OPTION_KEY = "admit_short_ascii_mppt"
# Explicit channel health; survives OptionalReads rebuild on transport/clock.
HEALTH_KEY = "short_ascii_mppt_health"
REQUEST_TIMEOUT = 4.0
# Live PV cadence matches RB: refresh often, expire before the next RB window.
INTERVAL = 30.0
TTL = 60.0
# Missing facade / wrong transport class — not a flaky decode. Back off far past
# the soft next_due=now path so a TypeError cannot storm the poll loop.
_STRUCTURAL_MARKER = "unsupported_auxiliary_transport"
STRUCTURAL_BACKOFF = 120.0

# Keys admitted as opt-in schema sensors; distinct owners stay distinct.
_VALUE_KEYS = (
    ("pv_voltage", "pv_voltage_v"),
    ("pv_power", "pv_power_w"),
    ("mppt_battery_voltage", "mppt_battery_voltage_v"),
    ("mppt_temperature", "mppt_temperature_c"),
    ("dc_load_current", "dc_load_current_a"),
    ("mppt_work_mode_code", "work_mode_code"),
    ("mppt_daily_energy", "daily_energy_kwh"),
    ("mppt_total_energy", "total_energy_kwh"),
    ("mppt_error_code", "fault_code"),
)

_FRAMING_KILL_REASONS = frozenset({
    "binary_frame_ambiguous",
    "aabb_checksum_invalid",
})

HEALTH_HEALTHY = "healthy"
HEALTH_RETRY_ONCE = "retry_once"
HEALTH_POISONED = "poisoned"


def is_admitted(runtime_state: dict) -> bool:
    """True only when the site explicitly admits aux 0200 (not entity enable)."""

    return runtime_state.get(ADMIT_OPTION_KEY) is True


def is_structural_aux_error(exc: BaseException) -> bool:
    """True when the facade lacks aux — retrying next_due=now cannot recover."""

    return isinstance(exc, TypeError) and _STRUCTURAL_MARKER in str(exc)


def assert_runtime_query_only(payload: bytes) -> None:
    """Refuse settings 0202 (and any other subtype) as live telemetry."""

    if payload != RUNTIME_QUERY_0200:
        raise ValueError("mppt_optional_query_not_runtime_0200")


def values_from_reply(wire: bytes) -> dict[str, object]:
    """Decode one AABB/0200 reply into optional sample values."""

    sample = parse_mppt_runtime_wire(wire)
    values = {key: getattr(sample, attr) for key, attr in _VALUE_KEYS}
    values["mppt_error"] = mppt_error_label(sample.fault_code)
    return values


async def request_runtime_sample(transport: object) -> dict[str, object]:
    """Solicit documented 0200 only; never settings 0202."""

    assert_runtime_query_only(RUNTIME_QUERY_0200)
    wire = await async_auxiliary_read(
        transport, RUNTIME_QUERY_0200, request_timeout=REQUEST_TIMEOUT,
    )
    return values_from_reply(wire)


async def capture_runtime_exchange(transport: object) -> dict[str, str]:
    """One correlated 0200 request/reply hex for Support Archive evidence."""

    assert_runtime_query_only(RUNTIME_QUERY_0200)
    wire = await async_auxiliary_read(
        transport, RUNTIME_QUERY_0200, request_timeout=REQUEST_TIMEOUT,
    )
    return {
        "0200_request": RUNTIME_QUERY_0200.hex(),
        "0200": wire.hex() if type(wire) is bytes else "",
    }


_FAIL_TIMEOUT = "timeout"
_FAIL_CONNECTION = "connection"
_FAIL_DECODE = "decode"
_FAIL_STRUCTURAL = "structural"
_FAIL_NOT_ADMITTED = "not_admitted"


def _collector_disconnect_snapshot(transport: object) -> tuple[int | None, str]:
    info = getattr(transport, "collector_info", None)
    if info is None:
        return None, ""
    count = getattr(info, "disconnect_count", None)
    reason = getattr(info, "last_disconnect_reason", "") or ""
    return (int(count) if isinstance(count, int) else None), str(reason)


def peek_mppt_framing_failure_latch(transport: object) -> str:
    """Read the MIXED framing-kill latch without clearing it.

    Prefer a facade ``peek_mppt_framing_failure_latch`` (live collector, works
    after disconnect). Test doubles fall back to the mutable
    ``collector_info`` attribute.
    """

    peeker = getattr(transport, "peek_mppt_framing_failure_latch", None)
    if callable(peeker):
        return str(peeker() or "")
    info = getattr(transport, "collector_info", None)
    if info is None:
        return ""
    return str(getattr(info, "mppt_framing_failure_latch", "") or "")


def take_mppt_framing_failure_latch(transport: object) -> str:
    """Read and clear the MIXED framing-kill latch (live object, not a snapshot).

    Production facades expose ``take_mppt_framing_failure_latch`` so a copied
    ``collector_info`` cannot leave the live latch stuck. Test doubles that
    hang a mutable ``CollectorInfo`` on the transport fall back to clearing
    that attribute directly.
    """

    taker = getattr(transport, "take_mppt_framing_failure_latch", None)
    if callable(taker):
        return str(taker() or "")
    info = getattr(transport, "collector_info", None)
    if info is None:
        return ""
    reason = getattr(info, "mppt_framing_failure_latch", "") or ""
    if hasattr(info, "mppt_framing_failure_latch"):
        info.mppt_framing_failure_latch = ""
    return str(reason)


def is_framing_kill_latch(reason: str) -> bool:
    return reason in _FRAMING_KILL_REASONS


@dataclass
class MpptHealth:
    """Explicit aux-channel health on runtime_state (not inside OptionalReads).

    States: healthy | retry_once (in-cycle only) | poisoned (monotonic deadline).
    """

    state: str = HEALTH_HEALTHY
    poisoned_until: float = 0.0  # time.monotonic() deadline while poisoned

    def is_poisoned(self, mono: float | None = None) -> bool:
        if self.state != HEALTH_POISONED:
            return False
        now = time.monotonic() if mono is None else mono
        return now < self.poisoned_until

    def mark_healthy(self) -> None:
        self.state = HEALTH_HEALTHY
        self.poisoned_until = 0.0

    def mark_retry_once(self) -> None:
        self.state = HEALTH_RETRY_ONCE

    def mark_poisoned(self, *, mono: float | None = None) -> None:
        """Set poisoned_until before any further await (structural / framing kill)."""

        now = time.monotonic() if mono is None else mono
        self.state = HEALTH_POISONED
        self.poisoned_until = now + STRUCTURAL_BACKOFF


def mppt_health_for(runtime_state: dict) -> MpptHealth:
    """Return channel health; rebuild of OptionalReads must not drop this."""

    health = runtime_state.get(HEALTH_KEY)
    if type(health) is not MpptHealth:
        health = MpptHealth()
        runtime_state[HEALTH_KEY] = health
    return health


def should_poison_mppt_aux(exc: BaseException, framing_latch: str) -> bool:
    """True when another immediate 0200 would only reopen a structural wound.

    A framing latch is poison regardless of the soft exception shape (TimeoutError
    included). Do not require ConnectionError — take-clearing the latch and then
    treating the miss as a flaky timeout would discard the kill.
    """

    return is_structural_aux_error(exc) or is_framing_kill_latch(framing_latch)


def classify_mppt_fail(exc: BaseException) -> str:
    """Map a soft MPPT exception to a quiet fail-class tag (not sample.outcome)."""

    if isinstance(exc, asyncio.TimeoutError):
        return _FAIL_TIMEOUT
    if is_structural_aux_error(exc):
        return _FAIL_STRUCTURAL
    if isinstance(exc, ConnectionError):
        return _FAIL_CONNECTION
    return _FAIL_DECODE


@dataclass
class MpptPollDiag:
    """Quiet counters for stuck-aux discrimination. Entities stay opt-in."""

    poll_attempts: int = 0
    poll_ok: int = 0
    poll_fail: int = 0
    consecutive_failures: int = 0
    fail_reason: str = ""
    fail_timeout: int = 0
    fail_connection: int = 0
    fail_decode: int = 0
    retry_recovered: int = 0
    not_admitted_cycles: int = 0
    skipped_prefer_fc4: int = 0
    prefer_fc4_skip_streak: int = 0
    forced_anti_starve: int = 0
    due_this_cycle: bool = False
    last_success_at: float | None = None
    aux_connected: bool | None = None
    aux_fence_reason: str = ""
    aux_last_error: str = ""
    last_fail_disconnect_count: int | None = None
    last_fail_disconnect_reason: str = ""

    def note_not_admitted(self, transport: object) -> None:
        self.not_admitted_cycles += 1
        self.fail_reason = _FAIL_NOT_ADMITTED
        self.due_this_cycle = False
        self.aux_connected = bool(getattr(transport, "connected", True))

    def note_due(self, due: bool) -> None:
        self.due_this_cycle = due

    def note_prefer_fc4_skip(self, *, contending_rb: bool = True) -> None:
        self.skipped_prefer_fc4 += 1
        if contending_rb:
            self.prefer_fc4_skip_streak += 1

    def reset_prefer_fc4_streak(self) -> None:
        self.prefer_fc4_skip_streak = 0

    def note_forced_anti_starve(self) -> None:
        self.forced_anti_starve += 1
        self.prefer_fc4_skip_streak = 0

    def note_attempt(self, transport: object) -> None:
        self.poll_attempts += 1
        self.prefer_fc4_skip_streak = 0
        self.aux_connected = bool(getattr(transport, "connected", True))

    def note_ok(self, now: float, *, retried: bool) -> None:
        self.poll_ok += 1
        self.consecutive_failures = 0
        self.fail_reason = ""
        self.last_success_at = now
        self.prefer_fc4_skip_streak = 0
        self.aux_fence_reason = ""
        self.aux_last_error = ""
        if retried:
            self.retry_recovered += 1

    def note_fail(self, transport: object, exc: BaseException) -> None:
        reason = classify_mppt_fail(exc)
        self.poll_fail += 1
        self.consecutive_failures += 1
        self.fail_reason = reason
        if reason == _FAIL_TIMEOUT:
            self.fail_timeout += 1
        elif reason == _FAIL_CONNECTION:
            self.fail_connection += 1
        elif reason != _FAIL_STRUCTURAL:
            self.fail_decode += 1
        connected = bool(getattr(transport, "connected", True))
        self.aux_connected = connected
        self.aux_last_error = f"{type(exc).__name__}:{exc}"[:200]
        self.aux_fence_reason = "disconnected" if not connected else type(exc).__name__
        count, disc_reason = _collector_disconnect_snapshot(transport)
        self.last_fail_disconnect_count = count
        self.last_fail_disconnect_reason = disc_reason

    def as_diagnostics(self, now: float) -> dict[str, object]:
        age: float | None = None
        if self.last_success_at is not None:
            age = round(max(0.0, now - self.last_success_at), 3)
        out: dict[str, object] = {
            "mppt_poll_attempts": self.poll_attempts,
            "mppt_poll_ok": self.poll_ok,
            "mppt_poll_fail": self.poll_fail,
            "mppt_fail_reason": self.fail_reason,
            "mppt_fail_timeout": self.fail_timeout,
            "mppt_fail_connection": self.fail_connection,
            "mppt_fail_decode": self.fail_decode,
            "mppt_retry_recovered": self.retry_recovered,
            "mppt_not_admitted": self.not_admitted_cycles,
            "mppt_consecutive_failures": self.consecutive_failures,
            "mppt_due_this_cycle": 1 if self.due_this_cycle else 0,
            "mppt_skipped_prefer_fc4": self.skipped_prefer_fc4,
            "mppt_forced_anti_starve": self.forced_anti_starve,
            "mppt_last_success_age_s": age,
            "aux_connected": self.aux_connected,
            "aux_fence_reason": self.aux_fence_reason,
            "aux_last_error": self.aux_last_error,
        }
        if self.last_fail_disconnect_count is not None:
            out["mppt_fail_disconnect_count"] = self.last_fail_disconnect_count
        if self.last_fail_disconnect_reason:
            out["mppt_fail_disconnect_reason"] = self.last_fail_disconnect_reason
        return out
