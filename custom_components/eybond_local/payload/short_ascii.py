"""Read-only FC4 short commands for the captured URTU1920 checksum dialect.

Commands are ASCII plus a binary address and CR, not PI30 or a raw UART route.
MP/Q1/MD shapes are corroborated by two saved device exchanges. Q1 field widths
come from the vendor's 19B4 segment 1; decoding never shifts columns by magnitude.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import re

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload


BASE_READ_COMMANDS = ("MP", "Q1", "MD")
READ_COMMANDS = (*BASE_READ_COMMANDS, "F", "RH", "RB")
PROTOCOL_ID = "EYBOND_SHORT_ASCII"
WIRE_DIALECT = "urtu1920_checksum"
# RB charge/discharge keys published only when optional RH reports decimals (RH=1).
RB_CURRENT_KEYS = ("bms_charging_current", "bms_discharging_current")
RB_CURRENT_POWER_KEYS = (*RB_CURRENT_KEYS, "battery_power")


class ShortAsciiError(ValueError):
    """A response does not establish the supported wire/field contract."""


def build_short_ascii_request(command: str, device_addr: int) -> bytes:
    if type(command) is not str or command not in READ_COMMANDS:
        raise ShortAsciiError("short_ascii_read_command_unsupported")
    if type(device_addr) is not int or not 0 <= device_addr <= 255:
        raise ShortAsciiError("short_ascii_address_invalid")
    return command.encode("ascii") + bytes([device_addr]) + b"\r"


def _envelope(frame: bytes, *, length: int, status: bool) -> None:
    if type(frame) is not bytes or len(frame) != length:
        raise ShortAsciiError("short_ascii_response_length")
    if frame[-1:] != b"\r" or (status and frame[0] != 1):
        raise ShortAsciiError("short_ascii_response_envelope")


def parse_md(frame: bytes) -> dict[str, object]:
    _envelope(frame, length=24, status=False)
    # The vendor exposes a 20-byte SOFTWARE VERSION and no inverter serial.
    # Do not convert the firmware prefix into a retail model or collector PN.
    if frame[20:] != b"  \x00\r" or not re.fullmatch(
        rb"S[0-9]-[0-9]{4}-[0-9]{6}-V[0-9]\.[A-Z0-9]{2}", frame[:20],
    ):
        raise ShortAsciiError("short_ascii_firmware_shape")
    return {
        "short_ascii_firmware": frame[:20].decode("ascii"),
        "protocol_id": PROTOCOL_ID,
        "short_ascii_wire_dialect": WIRE_DIALECT,
        "short_ascii_md_length": len(frame),
    }


def parse_mp(frame: bytes) -> dict[str, object]:
    _envelope(frame, length=38, status=True)
    # This binary settings block participates in shape qualification only.
    # Its body may contain CR and arbitrary high bytes; never strip/truncate it.
    return {"short_ascii_mp_length": len(frame)}


def parse_f(frame: bytes) -> dict[str, object]:
    """Rated values, not measured power or a pack-voltage multiplier."""
    _envelope(frame, length=22, status=False)
    if frame[:1] != b"#" or any(frame[index] != 32 for index in (6, 10, 16)):
        raise ShortAsciiError("short_ascii_f_format")
    return {
        "short_ascii_rated_voltage": _decimal(frame[1:6], rb"[0-9]{3}\.[0-9]"),
        "short_ascii_rated_current": _decimal(frame[7:10], rb"[0-9]{3}"),
        "short_ascii_rated_battery_voltage": _decimal(frame[11:16], rb"[0-9]{2}\.[0-9]{2}"),
        "short_ascii_rated_frequency": _decimal(frame[17:21], rb"[0-9]{2}\.[0-9]"),
    }


def parse_rh(frame: bytes) -> dict[str, object]:
    """Qualified RH settings block: 27-byte body, sum8, trailing CR.

    Live proxy captures match vendor 19B4 segment-5 length/checksum. Only the
    BMS current-display-accuracy enum (``rh_crtu_39``) is decoded: ``0`` =
    without decimals, ``1`` = with decimals. Other RH fields stay unpublished.
    """
    _envelope(frame, length=30, status=True)
    body = frame[1:-2]
    if sum(body) & 0xFF != frame[-2]:
        raise ShortAsciiError("short_ascii_rh_checksum")
    accuracy = body[8]
    if accuracy not in (0, 1):
        raise ShortAsciiError("short_ascii_rh_field")
    return {"short_ascii_bms_current_display_accuracy": accuracy}


def parse_rb(frame: bytes) -> dict[str, object]:
    """Qualified RB dialect: 25 documented bytes, 12 zero padding bytes, sum8.

    This checksum differs from Q1. Only this exact captured layout is accepted;
    the XML describes the first 25 bytes, not arbitrary future extensions.

    Charge/discharge raw words use 19B4 segment-7 ``multiply=0.1`` (``/10``).
    Optional runtime code publishes those keys only when RH reports decimals
    (accuracy ``1``) and F ratings are available for I/P hard-reject bounds.
    Battery DC watts are derived outside this parser (MX2).
    """
    _envelope(frame, length=40, status=True)
    body = frame[1:-2]
    if sum(body) & 0xFF != frame[-2]:
        raise ShortAsciiError("short_ascii_rb_checksum")
    if body[25:] != bytes(12):
        raise ShortAsciiError("short_ascii_rb_padding")
    voltage = int.from_bytes(body[0:2], "big")
    soc = body[2]
    if soc > 100 or body[23] not in (0, 1) or body[24] not in (0, 1):
        raise ShortAsciiError("short_ascii_rb_field")
    if voltage == 0:
        if soc:
            raise ShortAsciiError("short_ascii_rb_voltage_unavailable")
        # The capture loses V/SOC while trailing fields still retain old data.
        # Report data availability, NOT a proved physical BMS connection state.
        return {"short_ascii_bms_data_available": False}
    values: dict[str, object] = {
        "short_ascii_bms_data_available": True,
        "bms_total_voltage": voltage / 10,
        "battery_soc": soc,
        "short_ascii_bms_charge_path_enabled": body[23] == 1,
        "short_ascii_bms_discharge_path_enabled": body[24] == 1,
    }
    for key, offset, divisor in (
        ("bms_charging_current", 3, 10),
        ("bms_discharging_current", 5, 10),
        ("bms_cell_temperature", 7, 10),
        ("bms_cycle_count", 9, 1),
        ("bms_internal_temperature", 11, 10),
        ("bms_mos_temperature", 13, 10),
        ("bms_max_cell_voltage", 15, 100),
        ("bms_min_cell_voltage", 17, 100),
        ("bms_low_protection_voltage", 19, 10),
        ("bms_charge_cutoff_voltage", 21, 10),
    ):
        values[key] = int.from_bytes(body[offset:offset + 2], "big") / divisor
    return values


def _decimal(field: bytes, pattern: bytes) -> float:
    if re.fullmatch(pattern, field) is None:
        raise ShortAsciiError("short_ascii_q1_field_format")
    return float(field.decode("ascii"))


def parse_q1(frame: bytes) -> dict[str, object]:
    _envelope(frame, length=51, status=True)
    # Sum of unsigned body bytes, excluding status, checksum and final CR.
    # Check FIRST: binary checksum bytes may themselves equal CR/ASCII/NUL.
    if sum(frame[1:-3]) & 0xFFFF != int.from_bytes(frame[-3:-1], "big"):
        raise ShortAsciiError("short_ascii_q1_checksum")
    body = frame[1:-3]
    if any(body[index] != 32 for index in (5, 11, 17, 21, 26, 31, 36)):
        raise ShortAsciiError("short_ascii_q1_separator")
    # The fault-voltage column also carries "04 03"/"04 04" on both captures.
    # Its meaning is not established: preserve it in evidence, not as a sensor.
    if any(byte < 32 or byte > 126 for byte in body[6:11]):
        raise ShortAsciiError("short_ascii_q1_fault_field")
    flags = body[37:44]
    if re.fullmatch(rb"[01]{7}", flags) is None:
        raise ShortAsciiError("short_ascii_q1_flags")
    return {
        "short_ascii_q1_length": len(frame),
        "grid_voltage": _decimal(body[0:5], rb"[0-9]{3}\.[0-9]"),
        "output_voltage": _decimal(body[12:17], rb"[0-9]{3}\.[0-9]"),
        "load_percent": _decimal(body[18:21], rb"[0-9]{3}"),
        "output_frequency": _decimal(body[22:26], rb"[0-9]{2}\.[0-9]"),
        "battery_reference_voltage": _decimal(body[27:31], rb"[0-9]{2}\.[0-9]"),
        "temperature": _decimal(body[32:36], rb"(?:[0-9]{2}|-[0-9])\.[0-9]"),
        "short_ascii_status_flags": flags.decode("ascii"),
        "short_ascii_fault_code": body[44],
        "inverter_fault": flags[1] == 49,
        "grid_available": flags[2] == 48,
        "short_ascii_mains_input_connected": flags[3] == 48,
        "battery_low": flags[4] == 49,
        "pv_controller_present": flags[5] == 49,
    }


@dataclass(frozen=True, slots=True)
class ShortAsciiSession:
    transport: PayloadLinkTransport
    route: EybondLinkRoute
    device_addr: int
    timeout: float = 4.0

    async def request(self, command: str) -> bytes:
        if type(self.route) is not EybondLinkRoute:
            raise ShortAsciiError("short_ascii_requires_fc4")
        payload = build_short_ascii_request(command, self.device_addr)
        # The qualified dialect uses FC4 even on AT-primary collectors. Never
        # choose AtMixed/raw UART or bootstrap a different mode as a fallback.
        return await asyncio.wait_for(async_send_payload(
            self.transport, payload, route=self.route, request_timeout=self.timeout,
        ), timeout=self.timeout)
