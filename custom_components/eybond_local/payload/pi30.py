"""Minimal PI30 ASCII helpers routed over a generic payload link."""

from __future__ import annotations

import asyncio
from typing import Any

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload


class Pi30Error(Exception):
    """Raised when a PI30 frame cannot be encoded or decoded."""


def crc16_xmodem(data: bytes) -> int:
    """Compute the PI30 CRC16/XMODEM checksum."""

    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def build_request(command: str) -> bytes:
    """Build one PI30 ASCII command frame."""

    if not command or not command.isascii():
        raise Pi30Error("invalid_command")

    payload = command.encode("ascii")
    return payload + _encode_crc(payload) + b"\r"


def parse_response(frame: bytes) -> str:
    """Decode one PI30 response frame into its ASCII payload without the leading '('."""

    if len(frame) < 4:
        raise Pi30Error("response_too_short")
    if frame[-1] != 0x0D:
        raise Pi30Error("missing_terminator")

    body = frame[:-3]
    crc_received = frame[-3:-1]
    if crc_received != _encode_crc(body):
        raise Pi30Error("crc_mismatch")
    if not body.startswith(b"("):
        raise Pi30Error("unexpected_prefix")

    try:
        return body[1:].decode("ascii")
    except UnicodeDecodeError as exc:
        raise Pi30Error("invalid_ascii") from exc


def parse_space_fields(payload: str) -> list[str]:
    """Split one PI30 ASCII payload into space-delimited fields."""

    text = payload.strip()
    if not text:
        return []
    return [field for field in text.split(" ") if field != ""]


class Pi30Session:
    """PI30 ASCII session routed through one generic payload link."""

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
                raise TypeError("pi30_route_required")
            route = EybondLinkRoute(
                devcode=devcode,
                collector_addr=collector_addr,
            )
        self._route = route

    async def request(self, command: str) -> str:
        """Send one PI30 command and return the decoded ASCII payload."""

        try:
            response = await async_send_payload(
                self._transport,
                build_request(command),
                route=self._route,
            )
        except asyncio.TimeoutError as exc:
            raise Pi30Error("request_timeout") from exc

        payload = parse_response(response)
        if payload in {"NAK", "NOA", "ERCRC"}:
            raise Pi30Error(payload.lower())
        return payload


def parse_qpiri(payload: str) -> dict[str, Any]:
    """Decode static ratings/configuration fields from QPIRI."""

    fields = parse_space_fields(payload)
    if len(fields) < 21:
        raise Pi30Error("qpiri_field_count")

    layout = _QPIRI_LAYOUT_28 if len(fields) >= 28 else _QPIRI_LAYOUT_26 if len(fields) >= 26 else _QPIRI_LAYOUT_25 if len(fields) >= 25 else _QPIRI_LAYOUT_21
    values = _decode_fields(fields, layout)
    values["qpiri_field_count"] = len(fields)
    return values


def parse_qpigs(payload: str) -> dict[str, Any]:
    """Decode live values from QPIGS."""

    fields = parse_space_fields(payload)
    if len(fields) < 17:
        raise Pi30Error("qpigs_field_count")

    layout = _QPIGS_LAYOUT_24 if len(fields) >= 24 else _QPIGS_LAYOUT_21 if len(fields) >= 21 else _QPIGS_LAYOUT_17
    values = _decode_fields(fields, layout)
    values["qpigs_field_count"] = len(fields)

    pv_input_voltage = values.get("pv_input_voltage")
    pv_input_current = values.get("pv_input_current")
    if isinstance(pv_input_voltage, (int, float)) and isinstance(pv_input_current, (int, float)):
        values["pv_input_power"] = round(pv_input_voltage * pv_input_current, 1)

    battery_charge_current = values.get("battery_charge_current")
    battery_discharge_current = values.get("battery_discharge_current")
    if isinstance(battery_charge_current, (int, float)) and isinstance(battery_discharge_current, (int, float)):
        values["battery_power_balance_current"] = round(
            battery_charge_current - battery_discharge_current,
            1,
        )

    status_bits = values.get("status_bits")
    if isinstance(status_bits, str) and status_bits:
        values["status_bits_raw"] = status_bits

    return values


def parse_protocol_id(payload: str) -> dict[str, str]:
    """Decode the protocol identifier from QPI."""

    protocol_id = payload.strip()
    if not protocol_id:
        raise Pi30Error("protocol_id_empty")
    return {"protocol_id": protocol_id}


def parse_serial_number(payload: str) -> dict[str, str]:
    """Decode the inverter serial number from QID."""

    serial_number = payload.strip()
    if not serial_number:
        raise Pi30Error("serial_number_empty")
    return {"serial_number": serial_number}


def parse_model_number(payload: str) -> dict[str, str]:
    """Decode the model number from QMN."""

    model_number = payload.strip()
    if not model_number:
        raise Pi30Error("model_number_empty")
    return {"model_number": model_number}


def parse_firmware_version(payload: str, *, key: str) -> dict[str, str]:
    """Decode one firmware version response."""

    firmware_version = payload.strip()
    if not firmware_version:
        raise Pi30Error("firmware_version_empty")
    return {key: firmware_version}


def parse_qflag(payload: str) -> dict[str, Any]:
    """Decode static capability flags from QFLAG."""

    text = payload.strip()
    if not text or not text.startswith("E"):
        raise Pi30Error("qflag_format")

    disabled_index = text.find("D", 1)
    enabled_letters = text[1:disabled_index] if disabled_index >= 0 else text[1:]
    disabled_letters = text[disabled_index + 1 :] if disabled_index >= 0 else ""
    enabled_set = set(enabled_letters)
    disabled_set = set(disabled_letters)

    values: dict[str, Any] = {
        "capability_flags_enabled": enabled_letters,
        "capability_flags_disabled": disabled_letters,
    }
    for letter, key in _QFLAG_KEY_MAP.items():
        if letter in enabled_set:
            values[key] = True
        elif letter in disabled_set:
            values[key] = False

    return values


def parse_qpiws(payload: str) -> dict[str, Any]:
    """Decode the PI30 alarm bitfield returned by QPIWS."""

    bits = payload.strip()
    if not bits or any(bit not in {"0", "1"} for bit in bits):
        raise Pi30Error("qpiws_format")

    alarm_active = any(bit == "1" for bit in bits)
    return {
        "alarm_bits_raw": bits,
        "alarm_active": alarm_active,
        "qpiws_bit_count": len(bits),
    }


def parse_q1(payload: str) -> dict[str, Any]:
    """Decode auxiliary PI30 telemetry returned by Q1."""

    fields = _parse_q1_fields(payload)
    if len(fields) < 13:
        raise Pi30Error("q1_field_count")

    layout = _Q1_LAYOUT_27 if len(fields) >= 27 else _Q1_LAYOUT_22 if len(fields) >= 22 else _Q1_LAYOUT_17 if len(fields) >= 17 else _Q1_LAYOUT_13
    values = _decode_optional_fields(fields, layout)

    return values


def parse_energy_counter(payload: str, *, key: str) -> dict[str, int]:
    """Decode one integer energy counter payload."""

    text = payload.strip()
    if not text:
        raise Pi30Error("energy_counter_empty")
    return {key: int(text)}


def parse_qt_clock(payload: str) -> dict[str, str]:
    """Decode the QT clock payload used to construct dated energy queries."""

    clock = payload.strip()
    if len(clock) < 8 or not clock[:8].isdigit():
        raise Pi30Error("qt_clock_format")
    return {"clock_token": clock[:8]}


def parse_qmod(payload: str) -> dict[str, str]:
    """Decode the current operating mode returned by QMOD."""

    mode_code = payload.strip()
    if not mode_code:
        raise Pi30Error("mode_code_empty")
    return {"operating_mode_code": mode_code}


def _encode_crc(payload: bytes) -> bytes:
    crc = crc16_xmodem(payload)
    high = (crc >> 8) & 0xFF
    low = crc & 0xFF
    return bytes((_escape_crc_byte(high), _escape_crc_byte(low)))


def _escape_crc_byte(value: int) -> int:
    return value + 1 if value in {0x28, 0x0D, 0x0A} else value


def _decode_fields(fields: list[str], layout: tuple[tuple[str, str], ...]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for index, (key, kind) in enumerate(layout):
        if index >= len(fields):
            break
        raw = fields[index]
        values[key] = _decode_value(raw, kind)
    return values


def _decode_optional_fields(
    fields: list[str],
    layout: tuple[tuple[str | None, str], ...],
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for index, (key, kind) in enumerate(layout):
        if index >= len(fields):
            break
        if key is None:
            continue
        raw = fields[index]
        if raw == "":
            continue
        values[key] = _decode_value(raw, kind)
    return values


def _parse_q1_fields(payload: str) -> list[str]:
    text = payload.strip()
    if not text:
        return []
    primary = [field for field in text.split(" ") if field != ""]
    fallback = [field for field in text.split(",") if field != ""]
    return fallback if len(fallback) > len(primary) else primary


def _decode_value(raw: str, kind: str) -> Any:
    if kind == "str":
        return raw
    if kind == "bool":
        return raw not in {"0", "00", "000", "0000", "0.0", "0.00"}
    if kind == "int":
        try:
            return int(raw)
        except ValueError:
            return int(float(raw))
    if kind == "float":
        return round(float(raw), 2)
    raise Pi30Error(f"unsupported_kind:{kind}")


_QPIRI_LAYOUT_21: tuple[tuple[str, str], ...] = (
    ("input_rating_voltage", "float"),
    ("input_rating_current", "float"),
    ("output_rating_voltage", "float"),
    ("output_rating_frequency", "float"),
    ("output_rating_current", "float"),
    ("output_rating_apparent_power", "int"),
    ("output_rating_active_power", "int"),
    ("battery_rating_voltage", "float"),
    ("battery_recharge_voltage", "float"),
    ("battery_under_voltage", "float"),
    ("battery_bulk_voltage", "float"),
    ("battery_float_voltage", "float"),
    ("battery_type_code", "int"),
    ("max_ac_charging_current", "int"),
    ("max_charging_current", "int"),
    ("input_voltage_range_code", "int"),
    ("output_source_priority_code", "int"),
    ("charger_source_priority_code", "int"),
    ("parallel_max_num", "int"),
    ("machine_type_code", "int"),
    ("topology_code", "int"),
)

_QPIRI_LAYOUT_25: tuple[tuple[str, str], ...] = (
    *_QPIRI_LAYOUT_21,
    ("output_mode_code", "int"),
    ("battery_redischarge_voltage", "float"),
    ("pv_ok_condition_for_parallel_code", "int"),
    ("pv_power_balance_code", "int"),
)

_QPIRI_LAYOUT_26: tuple[tuple[str, str], ...] = (
    *_QPIRI_LAYOUT_25,
    ("max_charging_time_cv_stage", "int"),
)

_QPIRI_LAYOUT_28: tuple[tuple[str, str], ...] = (
    *_QPIRI_LAYOUT_26,
    ("operation_logic_code", "int"),
    ("max_discharging_current", "int"),
)

_QPIGS_LAYOUT_17: tuple[tuple[str, str], ...] = (
    ("input_voltage", "float"),
    ("input_frequency", "float"),
    ("output_voltage", "float"),
    ("output_frequency", "float"),
    ("output_apparent_power", "int"),
    ("output_active_power", "int"),
    ("load_percent", "int"),
    ("inverter_bus_voltage", "int"),
    ("battery_voltage", "float"),
    ("battery_charge_current", "int"),
    ("battery_percent", "int"),
    ("inverter_bus_temperature", "int"),
    ("pv_input_current", "float"),
    ("pv_input_voltage", "float"),
    ("battery_scc_voltage", "float"),
    ("battery_discharge_current", "int"),
    ("status_bits", "str"),
)

_QPIGS_LAYOUT_21: tuple[tuple[str, str], ...] = (
    *_QPIGS_LAYOUT_17,
    ("battery_voltage_offset_fans_on", "int"),
    ("eeprom_version", "int"),
    ("pv_charging_power", "int"),
    ("device_status_bits", "str"),
)

_QPIGS_LAYOUT_24: tuple[tuple[str, str], ...] = (
    *_QPIGS_LAYOUT_21,
    ("solar_feed_to_grid_status", "int"),
    ("country_code", "int"),
    ("solar_feed_to_grid_power", "int"),
)

_Q1_LAYOUT_13: tuple[tuple[str | None, str], ...] = (
    ("time_until_absorb_charge", "int"),
    ("time_until_float_charge", "int"),
    (None, "int"),
    ("tracker_temperature", "int"),
    ("inverter_temperature", "int"),
    ("battery_temperature", "int"),
    ("transformer_temperature", "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    ("fan_speed", "int"),
    (None, "int"),
    ("inverter_charge_state_code", "int"),
)

_Q1_LAYOUT_17: tuple[tuple[str | None, str], ...] = (
    ("time_until_absorb_charge", "int"),
    ("time_until_float_charge", "int"),
    (None, "bool"),
    (None, "bool"),
    (None, "int"),
    ("tracker_temperature", "int"),
    ("inverter_temperature", "int"),
    ("battery_temperature", "int"),
    ("transformer_temperature", "int"),
    (None, "int"),
    ("fan_lock_status", "bool"),
    (None, "int"),
    ("fan_speed", "int"),
    (None, "int"),
    (None, "bool"),
    (None, "float"),
    ("inverter_charge_state_code", "int"),
)

_Q1_LAYOUT_22: tuple[tuple[str | None, str], ...] = (
    *_Q1_LAYOUT_17,
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
)

_Q1_LAYOUT_27: tuple[tuple[str | None, str], ...] = (
    ("time_until_absorb_charge", "int"),
    ("time_until_float_charge", "int"),
    ("scc_flag", "bool"),
    ("allow_scc_on_flag", "bool"),
    ("charge_average_current", "int"),
    ("tracker_temperature", "int"),
    ("inverter_temperature", "int"),
    ("battery_temperature", "int"),
    ("transformer_temperature", "int"),
    (None, "int"),
    ("fan_lock_status", "bool"),
    (None, "int"),
    ("fan_speed", "int"),
    ("scc_charge_power", "int"),
    ("parallel_warning", "bool"),
    ("sync_frequency", "float"),
    ("inverter_charge_state_code", "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
    (None, "int"),
)

_QFLAG_KEY_MAP = {
    "a": "buzzer_enabled",
    "b": "overload_bypass_enabled",
    "d": "solar_feed_to_grid_enabled",
    "j": "power_saving_enabled",
    "k": "lcd_reset_to_default_enabled",
    "l": "data_log_pop_up_enabled",
    "u": "overload_restart_enabled",
    "v": "over_temperature_restart_enabled",
    "x": "lcd_backlight_enabled",
    "y": "primary_source_interrupt_alarm_enabled",
    "z": "record_fault_code_enabled",
}
