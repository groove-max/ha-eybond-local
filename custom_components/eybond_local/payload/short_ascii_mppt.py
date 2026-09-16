"""Vendor 19B4 segment-4 semantics for an already framed MPPT runtime reply.

This module cannot find a TCP boundary, admit a session or send a query. AABB
and EyeBond can overlap on the wire; the caller must establish the grammar
independently. Offline tooling may explicitly assume it and label that choice.
An optional driver module may merge decoded keys into runtime FULL results;
this payload module itself still does not solicit, admit, or send.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from ..collector.transport.binary_framing import (
    BinaryFrame, BinaryGrammar, validate_aabb_frame,
)


class MpptWorkMode(IntEnum):
    TRACKING = 1
    NIGHT = 2
    CONSTANT_VOLTAGE = 3


class MpptFault(IntEnum):
    INTERNAL_OVERTEMPERATURE = 1
    BATTERY_VOLTAGE_DETECTION_FAILED = 2
    PV_OVERVOLTAGE = 3
    BATTERY_OVERVOLTAGE = 5
    BATTERY_UNDERVOLTAGE = 6
    DC_LOAD_OVERCURRENT = 7


@dataclass(frozen=True, slots=True)
class MpptRuntimeSample:
    """One decoded sample, not an inverter identity or a live/freshness claim.

    MPPT battery voltage and temperature have their own owners; they are not
    BMS/reference voltage or main-inverter temperature. LOAD is the controller's
    DC current, not AC output current/power. Unknown enum codes remain raw.
    """

    pv_voltage_v: float
    pv_power_w: int
    mppt_battery_voltage_v: float
    mppt_temperature_c: float
    dc_load_current_a: float
    work_mode_code: int
    daily_energy_kwh: float
    total_energy_kwh: float
    fault_code: int

    @property
    def work_mode(self) -> MpptWorkMode | None:
        try:
            return MpptWorkMode(self.work_mode_code)
        except ValueError:
            return None

    @property
    def fault(self) -> MpptFault | None:
        # The XML lists faults 1/2/3/5/6/7, not a general bitfield. Preserve 0
        # and other unlisted codes without inventing a named state or flags.
        try:
            return MpptFault(self.fault_code)
        except ValueError:
            return None


def parse_mppt_runtime(frame: BinaryFrame) -> MpptRuntimeSample:
    """Decode only an exact, checksum-valid AABB/0200 runtime frame.

    Settings (0202) share the envelope but NOT these fields. Offsets below are
    whole-wire offsets: bytes2..3 are the two reserved/subtype bytes in XML
    segment4. XML optional keys mislabel PV power as current and DC load current
    as active power; the field units and descriptions establish their meaning.
    """

    if (
        not isinstance(frame, BinaryFrame)
        or frame.grammar is not BinaryGrammar.AABB
        or frame.header is not None
        or type(frame.wire) is not bytes
    ):
        raise ValueError("mppt_frame_contract_invalid")
    wire = frame.wire
    validate_aabb_frame(wire)
    if wire[2:4] != b"\x02\x00":
        raise ValueError("mppt_not_runtime")

    def word(offset: int) -> int:
        return int.from_bytes(wire[offset:offset + 2], "big")

    return MpptRuntimeSample(
        pv_voltage_v=word(4) / 10,
        pv_power_w=word(6) * 10,  # XML: raw * 0.01 kW, not 0.01 W or amperes.
        mppt_battery_voltage_v=word(8) / 10,
        mppt_temperature_c=word(10) / 10,  # ureg, no undocumented signed conversion.
        dc_load_current_a=word(12) / 10,
        work_mode_code=wire[14],
        daily_energy_kwh=word(15) / 10,
        total_energy_kwh=word(17) / 10,
        fault_code=wire[19],
    )


def parse_mppt_runtime_wire(wire: bytes) -> MpptRuntimeSample:
    """Decode raw AABB/0200 reply bytes without drivers naming the grammar."""

    if type(wire) is not bytes:
        raise ValueError("mppt_frame_contract_invalid")
    return parse_mppt_runtime(BinaryFrame(BinaryGrammar.AABB, wire))
