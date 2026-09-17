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

from ..link_transport import async_auxiliary_read
from ..payload.short_ascii_mppt import mppt_error_label, parse_mppt_runtime_wire

# Exact 21-byte read allow-listed by AuxiliaryReadSession._READ_QUERIES.
RUNTIME_QUERY_0200 = b"\x5a\xa5\x02\x00" + bytes(16) + b"\x02"
COMMAND = "MPPT"
# Config-entry options / runtime_state key. Default absent/false = no poll.
ADMIT_OPTION_KEY = "admit_short_ascii_mppt"
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
