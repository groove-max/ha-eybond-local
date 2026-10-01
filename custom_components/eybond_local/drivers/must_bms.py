"""Optional PV3300 BMS reads: bounded, runtime-scoped and never carried forward."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable

from ..payload.modbus import ModbusError
from ..payload.register_decode import decode_block

READ_TIMEOUT = 3.0
RETRY_DELAY = 60.0
UNSUPPORTED_RETRY_DELAY = 300.0
STATE_KEY = "must_bms_read"


@dataclass
class BmsReadState:
    transport: object
    inverter: object
    last_clock: float
    retry_at: float = 0.0
    status: str = "not_checked"


def decode_bms(words: list[int], schema) -> dict[str, object]:
    """Decode the vendor's 109..113 block without inventing a battery state."""
    if len(words) != 5 or any(type(word) is not int or not 0 <= word <= 65535 for word in words):
        raise ModbusError("invalid_bms_block")
    # The vendor decoder ignores the reserved word at 112 in its no-data test.
    if all(words[index] == 0 for index in (0, 1, 2, 4)):
        return {}
    values = decode_block(109, words, schema.spec_set("bms"), all_ones_unavailable=True)
    values = {key: value for key, value in values.items() if value is not None}
    # SOC is signed in protocol 6422/1916. Never clamp invalid/FFFF to 0/100.
    if not 0 <= values["battery_soc"] <= 100:
        values.pop("battery_soc")
    # An entirely unpopulated all-ones block is not a valid -0.1 A sample.
    if all(words[index] == 65535 for index in (0, 1, 2, 4)):
        return {}
    return values


async def async_read_bms(session, transport, inverter, schema, runtime_state: dict, clock: Callable[[], float]):
    """Read once after core telemetry; failure removes only optional values.

    Successful samples are not cached: MUST returns FULL snapshots, so a failed
    read or negative-cache hit cannot label an old SOC as fresh. Negative facts
    are temporary and scoped to this runtime binding, never the shared driver.
    """
    now = clock()
    state = runtime_state.get(STATE_KEY)
    if (
        type(state) is not BmsReadState
        or state.transport is not transport
        or state.inverter is not inverter
        or now < state.last_clock
    ):
        state = BmsReadState(transport, inverter, now)
        runtime_state[STATE_KEY] = state
    values = {}
    if now >= state.retry_at:
        delay = RETRY_DELAY
        try:
            async with asyncio.timeout(READ_TIMEOUT):
                words = await session.read_holding(109, 5)
        except ConnectionError:
            # A positive link failure still belongs to normal runtime recovery.
            raise
        except Exception as exc:
            if getattr(transport, "connected", True) is False:
                raise ConnectionError("must_bms_connection_lost") from exc
            if isinstance(exc, ModbusError) and str(exc) in {"exception_code:1", "exception_code:2"}:
                state.status = "unsupported"
                delay = UNSUPPORTED_RETRY_DELAY
            else:
                state.status = "timeout" if isinstance(exc, TimeoutError) else "invalid_response"
        else:
            try:
                values = decode_bms(words, schema)
            except ModbusError:
                state.status = "invalid_response"
            else:
                state.status = "ok" if "battery_soc" in values else "invalid_soc" if values else "no_data"
                if values:
                    delay = 0.0
        state.retry_at = clock() + delay
    state.last_clock = clock()
    return values, {
        "must_bms_status": state.status,
        "must_bms_retry_after_seconds": round(max(0, state.retry_at - state.last_clock), 3),
    }
