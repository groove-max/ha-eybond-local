"""Qualified 09C1 read-only ASCII payloads inside EyeBond FC4.

Vendor 09C1 XML and DevUrtu09C1_01 define fixed columns, with no address
byte or checksum. This is NOT the addressed URTU1920 short-ASCII dialect.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import re

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload

PV_CHANNEL_COMMANDS = ("PV1", "PV2")
READ_COMMANDS = ("Q1", "QF", "PV?", "F", "G?", *PV_CHANNEL_COMMANDS)
PROTOCOL_ID = "EYBOND_09C1"


class Urtu09C1Error(ValueError):
    """A reply does not meet the qualified 09C1 wire contract."""


def build_request(command: str) -> bytes:
    if type(command) is not str or command not in READ_COMMANDS:
        raise Urtu09C1Error("09c1_read_command_unsupported")
    return command.encode("ascii") + b"\r"


def _body(frame: bytes, length: int) -> bytes:
    if type(frame) is not bytes or len(frame) != length:
        raise Urtu09C1Error("09c1_response_length")
    if frame[:1] != b"(" or frame[-1:] != b"\r":
        raise Urtu09C1Error("09c1_response_envelope")
    return frame[1:-1]


def _number(field: bytes, pattern: bytes) -> float:
    if re.fullmatch(pattern, field) is None:
        raise Urtu09C1Error("09c1_field_format")
    return float(field.decode("ascii"))


def _spaces(body: bytes, positions: tuple[int, ...]) -> None:
    if any(body[index] != 32 for index in positions):
        raise Urtu09C1Error("09c1_separator")


def parse_q1(frame: bytes) -> dict[str, object]:
    body = _body(frame, 47)
    _spaces(body, (5, 11, 17, 21, 26, 31, 36))
    flags = body[37:45]
    if re.fullmatch(rb"[01]{8}", flags) is None:
        raise Urtu09C1Error("09c1_status_flags")
    return {
        "urtu09c1_q1_length": len(frame),
        "grid_voltage": _number(body[:5], rb"[0-9]{3}\.[0-9]"),
        "urtu09c1_fault_voltage": _number(body[6:11], rb"[0-9]{3}\.[0-9]"),
        "output_voltage": _number(body[12:17], rb"[0-9]{3}\.[0-9]"),
        "load_percent": _number(body[18:21], rb"[0-9]{3}"),
        # Unlike URTU1920, this column is INPUT frequency. QF owns output Hz.
        "grid_frequency": _number(body[22:26], rb"[0-9]{2}\.[0-9]"),
        "battery_voltage": _number(body[27:31], rb"[0-9]{2}\.[0-9]"),
        "temperature": _number(body[32:36], rb"(?:[0-9]{2}|-[0-9])\.[0-9]"),
        "urtu09c1_status_flags": flags.decode("ascii"),
        "grid_available": flags[0] == 48,
        "battery_low": flags[1] == 49,
        "urtu09c1_ac_charger_enabled": flags[2] == 49,
        "inverter_fault": flags[3] == 49,
        "operating_mode": {
            b"00": "Bypass", b"01": "AVR", b"10": "Battery (energy saving)",
            b"11": "Inverter failure",
        }[flags[4:6]],
        "urtu09c1_buzzer_enabled": flags[7] == 49,
    }


def parse_qf(frame: bytes) -> dict[str, object]:
    body = _body(frame, 6)
    return {
        "urtu09c1_qf_length": len(frame),
        "output_frequency": _number(body, rb"[0-9]{2}\.[0-9]"),
    }


def parse_pv(frame: bytes) -> dict[str, object]:
    body = _body(frame, 19)
    _spaces(body, (4, 8, 10))
    if re.fullmatch(rb"[0-9] [0-9]{6}", body[9:]) is None:
        raise Urtu09C1Error("09c1_pv_tail")
    # The captured fault glyph differs from the XML's numeric enum encoding.
    # Energy's custom decimal/concatenation encoding needs separate validation.
    # Preserve both in support captures; do not invent alarms or HA energy.
    return {
        "urtu09c1_pv_length": len(frame),
        "pv_voltage": _number(body[:4], rb"[0-9]{4}") / 10,
        "pv_current": _number(body[5:8], rb"[0-9]{3}") / 10,
    }


def parse_pv_channel(frame: bytes, *, command: str) -> dict[str, object]:
    """Decode a qualified reply using protocol, not physical-port, numbering.

    Owner captures qualify the same fixed shape as PV?, not a capability flag,
    channel echo, combined reading, power measurement or energy counter.
    """
    if type(command) is not str or command not in PV_CHANNEL_COMMANDS:
        raise Urtu09C1Error("09c1_pv_channel_unsupported")
    values = parse_pv(frame)
    return {
        f"{command.lower()}_voltage": values["pv_voltage"],
        f"{command.lower()}_current": values["pv_current"],
    }


def parse_f(frame: bytes) -> dict[str, object]:
    body = _body(frame, 22)
    _spaces(body, (5, 9, 15))
    rated = body[6:9]
    if re.fullmatch(rb"[0-9]{2}K", rated):
        watts = float(rated[:2]) * 1000
    elif re.fullmatch(rb"[0-9]\.[0-9]", rated):
        watts = float(rated) * 1000
    else:
        watts = _number(rated, rb"[0-9]{3}")
    return {
        "protocol_id": PROTOCOL_ID,
        "urtu09c1_f_length": len(frame),
        "urtu09c1_rated_voltage": _number(body[:5], rb"[0-9]{3}\.[0-9]"),
        "urtu09c1_rated_power": watts,
        "urtu09c1_rated_battery_voltage": _number(body[10:15], rb"[0-9]{2}\.[0-9]{2}"),
        "urtu09c1_rated_frequency": _number(body[16:20], rb"[0-9]{2}\.[0-9]"),
    }


@dataclass(frozen=True, slots=True)
class Urtu09C1Session:
    transport: PayloadLinkTransport
    route: EybondLinkRoute
    timeout: float = 4.0

    async def request(self, command: str) -> bytes:
        if type(self.route) is not EybondLinkRoute:
            raise Urtu09C1Error("09c1_requires_fc4")
        return await asyncio.wait_for(async_send_payload(
            self.transport, build_request(command), route=self.route,
            request_timeout=self.timeout,
        ), timeout=self.timeout)
