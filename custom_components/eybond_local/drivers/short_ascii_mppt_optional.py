"""Optional live MPPT via the documented auxiliary 0200 read only.

Solicits through ``link_transport.async_auxiliary_read`` (framed/AT facade).
Does not harvest tip AABB, wait on EyeBond TID ambiguity, merge settings
0202, or register schema entities — those remain separate admission work.
"""

from __future__ import annotations

from ..link_transport import async_auxiliary_read
from ..payload.short_ascii_mppt import parse_mppt_runtime_wire

# Exact 21-byte read allow-listed by AuxiliaryReadSession._READ_QUERIES.
RUNTIME_QUERY_0200 = b"\x5a\xa5\x02\x00" + bytes(16) + b"\x02"
COMMAND = "MPPT"
REQUEST_TIMEOUT = 4.0
# Live PV cadence matches RB: refresh often, expire before the next RB window.
INTERVAL = 30.0
TTL = 60.0

# Keys suitable for a later schema slice; distinct owners stay distinct.
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


def assert_runtime_query_only(payload: bytes) -> None:
    """Refuse settings 0202 (and any other subtype) as live telemetry."""

    if payload != RUNTIME_QUERY_0200:
        raise ValueError("mppt_optional_query_not_runtime_0200")


def values_from_reply(wire: bytes) -> dict[str, object]:
    """Decode one AABB/0200 reply into optional sample values."""

    sample = parse_mppt_runtime_wire(wire)
    return {key: getattr(sample, attr) for key, attr in _VALUE_KEYS}


async def request_runtime_sample(transport: object) -> dict[str, object]:
    """Solicit documented 0200 only; never settings 0202."""

    assert_runtime_query_only(RUNTIME_QUERY_0200)
    wire = await async_auxiliary_read(
        transport, RUNTIME_QUERY_0200, request_timeout=REQUEST_TIMEOUT,
    )
    return values_from_reply(wire)
