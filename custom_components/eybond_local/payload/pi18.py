"""Experimental PI18 helpers routed over a generic payload link."""

from __future__ import annotations

import asyncio
from typing import Any

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload
from .pi30 import crc16_xmodem


class Pi18Error(Exception):
    """Raised when a PI18 frame cannot be encoded or decoded."""


def build_request(command: str) -> bytes:
    """Build one PI18 command frame."""

    if not command or not command.isascii():
        raise Pi18Error("invalid_command")
    payload = command.encode("ascii")
    crc = crc16_xmodem(payload)
    return payload + bytes(((crc >> 8) & 0xFF, crc & 0xFF)) + b"\r"


def parse_response(frame: bytes) -> str:
    """Decode one PI18 response frame into its ASCII payload body."""

    if len(frame) < 6:
        raise Pi18Error("response_too_short")
    if frame[-1] != 0x0D:
        raise Pi18Error("missing_terminator")

    body = frame[:-3]
    crc_received = frame[-3:-1]
    crc = crc16_xmodem(body)
    if crc_received != bytes(((crc >> 8) & 0xFF, crc & 0xFF)):
        raise Pi18Error("crc_mismatch")

    try:
        text = body.decode("ascii")
    except UnicodeDecodeError as exc:
        raise Pi18Error("invalid_ascii") from exc

    if not text.startswith("^D") or len(text) < 5 or not text[2:5].isdigit():
        raise Pi18Error("unexpected_prefix")
    return text[5:]


class Pi18Session:
    """PI18 session routed through one generic payload link."""

    def __init__(
        self,
        transport: PayloadLinkTransport,
        *,
        route: EybondLinkRoute | None = None,
        devcode: int | None = None,
        collector_addr: int | None = None,
    ) -> None:
        self._transport = transport
        if route is None:
            if devcode is None or collector_addr is None:
                raise TypeError("pi18_route_required")
            route = EybondLinkRoute(
                devcode=devcode,
                collector_addr=collector_addr,
            )
        self._route = route

    async def request(self, command: str) -> str:
        try:
            response = await async_send_payload(
                self._transport,
                build_request(command),
                route=self._route,
            )
        except asyncio.TimeoutError as exc:
            raise Pi18Error("request_timeout") from exc

        payload = parse_response(response)
        if payload in {"NAK", "NOA", "ERCRC"}:
            raise Pi18Error(payload.lower())
        return payload


def parse_protocol_id(payload: str) -> dict[str, Any]:
    text = payload.strip()
    if not text or not text.isdigit():
        raise Pi18Error("protocol_id_invalid")
    return {"protocol_id": f"PI{text}", "protocol_id_code": int(text)}


def parse_serial_number(payload: str) -> dict[str, str]:
    text = payload.strip()
    if len(text) < 2 or not text[:2].isdigit():
        raise Pi18Error("serial_number_invalid")
    available = int(text[:2])
    serial_number = text[2 : 2 + available].strip()
    if len(serial_number) < 6:
        raise Pi18Error("serial_number_empty")
    return {"serial_number": serial_number}


def parse_firmware_versions(payload: str) -> dict[str, str]:
    fields = _parse_csv_fields(payload)
    if len(fields) < 3:
        raise Pi18Error("firmware_version_count")
    return {
        "main_cpu_firmware_version": fields[0],
        "secondary_cpu_firmware_version": fields[1],
        "tertiary_cpu_firmware_version": fields[2],
    }


def parse_qpiri(payload: str) -> dict[str, Any]:
    fields = _parse_csv_fields(payload)
    if len(fields) < 25:
        raise Pi18Error("qpiri_field_count")
    values = _decode_fields(fields, _QPIRI_LAYOUT)
    values["qpiri_field_count"] = len(fields)
    return values


def parse_qpigs(payload: str) -> dict[str, Any]:
    fields = _parse_csv_fields(payload)
    if len(fields) < 28:
        raise Pi18Error("qpigs_field_count")
    values = _decode_fields(fields, _QPIGS_LAYOUT)
    values["qpigs_field_count"] = len(fields)

    pv1_power = values.get("pv1_input_power")
    pv2_power = values.get("pv2_input_power")
    if isinstance(pv1_power, (int, float)) and isinstance(pv2_power, (int, float)):
        values["pv_input_power"] = int(pv1_power + pv2_power)

    pv1_voltage = values.get("pv1_input_voltage")
    pv2_voltage = values.get("pv2_input_voltage")
    if isinstance(pv1_voltage, (int, float)) and isinstance(pv2_voltage, (int, float)):
        values["pv_input_voltage"] = round(pv1_voltage + pv2_voltage, 1)

    battery_charge = values.get("battery_charge_current")
    battery_discharge = values.get("battery_discharge_current")
    if isinstance(battery_charge, (int, float)) and isinstance(battery_discharge, (int, float)):
        values["battery_power_balance_current"] = round(battery_charge - battery_discharge, 1)

    return values


def parse_qmod(payload: str) -> dict[str, int]:
    text = payload.strip()
    if len(text) != 2 or not text.isdigit():
        raise Pi18Error("mode_code_invalid")
    return {"operating_mode_code": int(text)}


def parse_qflag(payload: str) -> dict[str, Any]:
    fields = _parse_csv_fields(payload)
    if len(fields) < 8:
        raise Pi18Error("qflag_field_count")

    values: dict[str, Any] = {}
    for key, raw in zip(_QFLAG_KEYS, fields, strict=False):
        if raw not in {"0", "1"}:
            raise Pi18Error("qflag_value_invalid")
        values[key] = raw == "1"
    values["capability_flags_raw"] = ",".join(fields)
    return values


def parse_qfws(payload: str) -> dict[str, Any]:
    fields = _parse_csv_fields(payload)
    if len(fields) < 17:
        raise Pi18Error("qfws_field_count")

    fault_code = int(fields[0])
    values: dict[str, Any] = {
        "fault_code": fault_code,
        "warning_active": fault_code != 0,
    }
    active_flags: list[str] = []
    for key, raw in zip(_QFWS_KEYS, fields[1:], strict=False):
        if raw not in {"0", "1"}:
            raise Pi18Error("qfws_value_invalid")
        state = raw == "1"
        values[key] = state
        if state:
            active_flags.append(key)
    values["warning_flags_raw"] = ",".join(fields[1:])
    values["warning_status"] = ", ".join(active_flags) if active_flags else "Ok"
    return values


def parse_energy_counter(payload: str, *, key: str) -> dict[str, int]:
    text = payload.strip()
    if not text or not text.isdigit():
        raise Pi18Error("energy_counter_invalid")
    return {key: int(text)}


def parse_current_time(payload: str) -> dict[str, str]:
    text = payload.strip()
    if len(text) < 8 or not text[:8].isdigit():
        raise Pi18Error("current_time_invalid")
    return {"clock_token": text[:8]}


def _parse_csv_fields(payload: str) -> list[str]:
    text = payload.strip()
    if not text:
        return []
    return [field.strip() for field in text.split(",")]


def _decode_fields(fields: list[str], layout: tuple[tuple[str, str], ...]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for index, (key, kind) in enumerate(layout):
        if index >= len(fields):
            break
        values[key] = _decode_value(fields[index], kind)
    return values


def _decode_value(raw: str, kind: str) -> Any:
    if kind == "int":
        return int(raw)
    if kind == "div10":
        return round(int(raw) / 10.0, 1)
    raise Pi18Error(f"unsupported_kind:{kind}")


_QPIRI_LAYOUT: tuple[tuple[str, str], ...] = (
    ("input_rating_voltage", "div10"),
    ("input_rating_current", "div10"),
    ("output_rating_voltage", "div10"),
    ("output_rating_frequency", "div10"),
    ("output_rating_current", "div10"),
    ("output_rating_apparent_power", "int"),
    ("output_rating_active_power", "int"),
    ("battery_rating_voltage", "div10"),
    ("battery_recharge_voltage", "div10"),
    ("battery_redischarge_voltage", "div10"),
    ("battery_under_voltage", "div10"),
    ("battery_bulk_voltage", "div10"),
    ("battery_float_voltage", "div10"),
    ("battery_type_code", "int"),
    ("max_ac_charging_current", "int"),
    ("max_charging_current", "int"),
    ("input_voltage_range_code", "int"),
    ("output_source_priority_code", "int"),
    ("charger_source_priority_code", "int"),
    ("parallel_max_num", "int"),
    ("machine_type_code", "int"),
    ("topology_code", "int"),
    ("output_mode_code", "int"),
    ("solar_power_priority_code", "int"),
    ("mppt_string_count", "int"),
)

_QPIGS_LAYOUT: tuple[tuple[str, str], ...] = (
    ("grid_voltage", "div10"),
    ("grid_frequency", "div10"),
    ("output_voltage", "div10"),
    ("output_frequency", "div10"),
    ("output_apparent_power", "int"),
    ("output_active_power", "int"),
    ("output_load_percent", "int"),
    ("battery_voltage", "div10"),
    ("battery_voltage_scc", "div10"),
    ("battery_voltage_scc2", "div10"),
    ("battery_discharge_current", "int"),
    ("battery_charge_current", "int"),
    ("battery_capacity", "int"),
    ("inverter_heat_sink_temperature", "int"),
    ("mppt1_charger_temperature", "int"),
    ("mppt2_charger_temperature", "int"),
    ("pv1_input_power", "int"),
    ("pv2_input_power", "int"),
    ("pv1_input_voltage", "div10"),
    ("pv2_input_voltage", "div10"),
    ("configuration_state_code", "int"),
    ("mppt1_charger_status_code", "int"),
    ("mppt2_charger_status_code", "int"),
    ("load_connection_code", "int"),
    ("battery_power_direction_code", "int"),
    ("dc_ac_power_direction_code", "int"),
    ("line_power_direction_code", "int"),
    ("local_parallel_id", "int"),
)

_QFLAG_KEYS: tuple[str, ...] = (
    "buzzer_enabled",
    "overload_bypass_enabled",
    "lcd_reset_to_default_enabled",
    "overload_restart_enabled",
    "over_temperature_restart_enabled",
    "lcd_backlight_enabled",
    "primary_source_interrupt_alarm_enabled",
    "record_fault_code_enabled",
    "reserved_flag_enabled",
)

_QFWS_KEYS: tuple[str, ...] = (
    "line_fail_warning",
    "output_circuit_short_warning",
    "inverter_over_temperature_warning",
    "fan_lock_warning",
    "battery_voltage_high_warning",
    "battery_low_warning",
    "battery_under_warning",
    "overload_warning",
    "eeprom_fail_warning",
    "power_limit_warning",
    "pv1_voltage_high_warning",
    "pv2_voltage_high_warning",
    "mppt1_overload_warning",
    "mppt2_overload_warning",
    "battery_too_low_scc1_warning",
    "battery_too_low_scc2_warning",
)
