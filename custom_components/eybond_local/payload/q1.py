"""Strict fixed-column Q1 envelope, without model-specific flag/scaling guesses.

Shared by the qualified 09C1 map and an explicitly selected Q1-only reader.
A valid Q1 packet alone does not identify the rest of the inverter protocol.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import re

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload

STATUS_REQUEST = b"Q1\r"


class Q1Error(ValueError):
    """The response does not match the captured 47-byte Q1 layout."""


@dataclass(frozen=True, slots=True)
class Q1Sample:
    input_voltage: float
    fault_voltage: float
    output_voltage: float
    load_percent: float
    frequency: float
    battery_value: float
    battery_text: str
    temperature: float
    flags: str


def decode_q1(frame: bytes) -> Q1Sample:
    """Decode syntax only; the owning profile decides the battery/flag meaning."""
    if type(frame) is not bytes or len(frame) != 47:
        raise Q1Error("q1_response_length")
    if frame[:1] != b"(" or frame[-1:] != b"\r":
        raise Q1Error("q1_response_envelope")
    body = frame[1:-1]
    if any(body[index] != 32 for index in (5, 11, 17, 21, 26, 31, 36)):
        raise Q1Error("q1_separator")
    flags = body[37:45]
    if re.fullmatch(rb"[01]{8}", flags) is None:
        raise Q1Error("q1_status_flags")

    def number(field: bytes, pattern: bytes) -> float:
        if re.fullmatch(pattern, field) is None:
            raise Q1Error("q1_field_format")
        return float(field.decode("ascii"))

    return Q1Sample(
        input_voltage=number(body[:5], rb"[0-9]{3}\.[0-9]"),
        fault_voltage=number(body[6:11], rb"[0-9]{3}\.[0-9]"),
        output_voltage=number(body[12:17], rb"[0-9]{3}\.[0-9]"),
        load_percent=number(body[18:21], rb"[0-9]{3}"),
        frequency=number(body[22:26], rb"[0-9]{2}\.[0-9]"),
        battery_value=number(body[27:31], rb"(?:[0-9]{2}\.[0-9]|[0-9]\.[0-9]{2})"),
        battery_text=body[27:31].decode("ascii"),
        temperature=number(body[32:36], rb"(?:[0-9]{2}|-[0-9])\.[0-9]"),
        flags=flags.decode("ascii"),
    )


@dataclass(frozen=True, slots=True)
class Q1Session:
    transport: PayloadLinkTransport
    route: EybondLinkRoute
    timeout: float = 4.0

    async def read_status(self) -> bytes:
        if type(self.route) is not EybondLinkRoute:
            raise Q1Error("q1_requires_fc4")
        # Owner evidence qualifies FC4 0001/FF only. Do not switch UART, try raw
        # serial, or send other dialects' commands to discover more features.
        return await asyncio.wait_for(async_send_payload(
            self.transport, STATUS_REQUEST, route=self.route,
            request_timeout=self.timeout,
        ), timeout=self.timeout)
