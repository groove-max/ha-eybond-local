"""Experimental read-only VPHB/099E status payload inside EyeBond FC4.

The SDK supplies the query and additive checksum; the field offsets come from
the public FiveStar/V-Series experiment, replayed against issue #82's Manyi
capture. See ADDING_DRIVERS.md for provenance and the remaining validation gap.
No model, status flags, settings or charge-current extension is inferred here.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload

PROTOCOL_ID = "EYBOND_099E_VPHB"
STATUS_REQUEST = bytes.fromhex("434d534706494e562d48420d")
OBSERVED_FRAME_LENGTHS = (78, 84)
_FIELDS = (
    ("grid_voltage", 17, 2, 10),
    ("grid_frequency", 19, 2, 10),
    ("output_voltage", 21, 2, 10),
    ("output_frequency", 23, 2, 10),
    ("battery_voltage", 25, 2, 10),
    ("battery_soc", 27, 2, 1),
    ("load_percent", 29, 1, 1),
    ("temperature", 30, 1, 1),
    ("pv_voltage", 39, 2, 1),
    ("solar_charger_temperature", 42, 1, 1),
)


class VphbError(ValueError):
    """A frame does not satisfy the experimentally supported wire contract."""


@dataclass(frozen=True, slots=True)
class VphbStatus:
    """A validated envelope; retain the entire packet, including unknown bytes."""

    raw: bytes

    def __post_init__(self) -> None:
        if type(self.raw) is not bytes or len(self.raw) not in OBSERVED_FRAME_LENGTHS:
            raise VphbError("vphb_response_length")
        if self.raw[:4] != b"VPHB":
            raise VphbError("vphb_response_prefix")
        # True for both independent samples; not a claim about other variants.
        if self.raw[4] != len(self.raw) - 5:
            raise VphbError("vphb_length_byte")
        if sum(self.raw[5:-1]) & 0xFF != self.raw[-1]:
            raise VphbError("vphb_checksum")

    def values(self) -> dict[str, object]:
        return {
            "protocol_id": PROTOCOL_ID,
            **{
                key: int.from_bytes(self.raw[offset:offset + width], "little") / divisor
                for key, offset, width, divisor in _FIELDS
            },
        }

    def evidence(self) -> dict[str, object]:
        return {
            "response_hex": self.raw.hex(),
            "response_length": len(self.raw),
            "checksum_valid": True,
            # Use the existing wire-evidence contract: the archive redactor
            # preserves binary bytes but still masks embedded identifiers.
            "vendor_window": {"offset": 5, "payload_hex": self.raw[5:71].hex()},
            "unparsed_extension": {"offset": 71, "payload_hex": self.raw[71:-1].hex()},
            "field_map_verification": "experimental_replay_only",
        }


@dataclass(frozen=True, slots=True)
class VphbSession:
    transport: PayloadLinkTransport
    route: EybondLinkRoute
    timeout: float = 4.0

    async def read_status(self) -> bytes:
        if type(self.route) is not EybondLinkRoute:
            raise VphbError("vphb_requires_fc4")
        # No implicit UART/AT switch, route sweep or retry. The transport owns
        # FC4 transaction correlation and reconnect/cancellation handling.
        return await asyncio.wait_for(async_send_payload(
            self.transport, STATUS_REQUEST, route=self.route,
            request_timeout=self.timeout,
        ), timeout=self.timeout)
