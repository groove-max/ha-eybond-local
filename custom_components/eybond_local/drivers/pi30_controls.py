"""Device-advertised PI30 charge-current controls, not a retail model guess.

PI30 Communication Protocol 2015-09-24, sections 2.16/2.17 and 3.3/3.5,
defines selectable-value queries and the corresponding three-digit writes.
Only missing controls are enriched after positive protocol identification.
Existing model-specific controls (including their tested status) are preserved.
"""

from __future__ import annotations

import asyncio
import re

from ..models import CapabilityChoice, WriteCapability
from ..payload.pi30 import Pi30Error


_CONTROLS = (
    ("max_charging_current", "QMCHGCR", "MCHGC", "Max Total Charge Current"),
    ("max_ac_charging_current", "QMUCHGCR", "MUCHGC", "Max Utility Charge Current"),
)


def parse_charge_current_choices(payload: str) -> tuple[int, ...]:
    """Accept a bounded list of distinct three-digit positive ampere values."""

    if not isinstance(payload, str) or not re.fullmatch(r"[0-9]{3}(?: [0-9]{3}){0,63}", payload):
        raise Pi30Error("invalid_charge_current_choices")
    values = tuple(int(field) for field in payload.split(" "))
    if 0 in values or len(set(values)) != len(values):
        raise Pi30Error("invalid_charge_current_choices")
    return values


async def async_enrich_charge_current_controls(
    session, values: dict, capabilities: tuple[WriteCapability, ...], *, timeout: float,
) -> tuple[WriteCapability, ...]:
    """Read at most two missing lists within a shared optional time budget.

    A timeout/NAK/malformed list never invalidates an already detected inverter.
    This is not part of the signature scan or regular polling. Writes remain
    untested/Full-Control-only and accept only the device's advertised values.
    """

    result = list(capabilities)
    existing = {cap.key for cap in capabilities}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, timeout)
    for index, (key, query, command, title) in enumerate(_CONTROLS):
        current = values.get(key)
        if key in existing or type(current) is not int or current <= 0:
            continue
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            payload = await asyncio.wait_for(session.request(query), timeout=min(1.5, remaining))
            choices = parse_charge_current_choices(payload)
        except Exception:  # Optional query/parser failure; cancellation is BaseException.
            continue
        if current not in choices:
            continue
        result.append(WriteCapability(
            key=key, register=0, value_kind="enum", command=command, command_width=3,
            note=f"PI30 {query} advertised these values on this device; local writes are untested.",
            tested=False, provenance="doc_backed", support_tier="conditional",
            title=title, group="battery", order=260 + 10 * index, unit="A",
            enabled_default=True, requires_confirm=True,
            change_summary="Limits battery charging current to a value advertised by this inverter.",
            choices=tuple(CapabilityChoice(value=value, label=f"{value} A", order=order)
                          for order, value in enumerate(choices)),
        ))
    return tuple(result)
