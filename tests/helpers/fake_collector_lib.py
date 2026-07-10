"""Shared protocol helpers and preset definitions for the fake collector tool."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re

from custom_components.eybond_local.collector.at import build_at_response, normalize_at_command
from custom_components.eybond_local.collector.protocol import FC_HEARTBEAT, build_collector_request
from custom_components.eybond_local.const import DEFAULT_COLLECTOR_ADDR, DEFAULT_MODBUS_DEVICE_ADDR
from custom_components.eybond_local.payload.modbus import crc16_modbus
from custom_components.eybond_local.payload.pi30 import crc16_xmodem

_DISCOVERY_REQUEST_RE = re.compile(r"^set>server=([^:;\s]+):(\d+);$")

QUERY_MODE_SUCCESS = "success"
QUERY_MODE_FAIL = "fail"
QUERY_MODE_TIMEOUT = "timeout"
FC4_MODE_SUCCESS = "success"
FC4_MODE_MODBUS_EXCEPTION = "modbus_exception"
FC4_MODE_TIMEOUT = "timeout"

QUERY_MODE_NAMES = (
    QUERY_MODE_SUCCESS,
    QUERY_MODE_FAIL,
    QUERY_MODE_TIMEOUT,
)
FC4_MODE_NAMES = (
    FC4_MODE_SUCCESS,
    FC4_MODE_MODBUS_EXCEPTION,
    FC4_MODE_TIMEOUT,
)

PRESET_COLLECTOR_ONLY = "collector_only"
PRESET_SMARTESS_HINT = "smartess_hint"
PRESET_MODBUS_SMG_READONLY = "modbus_smg_readonly"
SCENARIO_PRESET_NAMES = (
    PRESET_COLLECTOR_ONLY,
    PRESET_SMARTESS_HINT,
    PRESET_MODBUS_SMG_READONLY,
)


@dataclass(frozen=True, slots=True)
class DiscoveryRedirect:
    """Parsed UDP `set>server=` redirect."""

    server_ip: str
    server_port: int
    raw: str


@dataclass(frozen=True, slots=True)
class CollectorProfile:
    """Fake collector identity and user-visible metadata."""

    pn: str = "E5000099990001"
    mode: str = PRESET_COLLECTOR_ONLY
    serial_number: str = "SMG11K240001"
    model_name: str = "SMG II 6200"
    firmware_version: str = "8.50.12.3"
    protocol_descriptor: str = "0942#smg-6200"
    at_version: str = "1.11"
    collector_type: str = "Wi-Fi.DTU"
    upload_mode: str = "OFF"
    wifi_rssi: str = "-55"
    uart: str = "9600,8,1,NONE"
    link_status: str = "connected"
    wifi_scan_list: str = ""
    # Optional legacy/manual ``AT+VDTU?`` reply used by low-level transport tests.
    # Runtime bridge detection no longer sends this probe; it keys off the
    # FC=2 parameter-6 hardware-version token instead.
    vdtu: str = ""
    rated_power: int = 6200
    protocol_number: int = 1
    device_type: int = 0x1E00
    rated_cell_count: int = 16
    max_discharge_current_protection: int = 80


@dataclass(frozen=True, slots=True)
class CollectorScenario:
    """Resolved fake collector behavior preset plus runtime knobs."""

    preset: str
    profile: CollectorProfile
    heartbeat_devcode: int
    collector_addr: int
    device_addr: int = DEFAULT_MODBUS_DEVICE_ADDR
    reverse_connect_delay: float = 0.0
    first_heartbeat_delay: float = 0.0
    fc2_query_modes: Mapping[int, str] = field(default_factory=dict)
    fc4_mode: str = FC4_MODE_MODBUS_EXCEPTION
    modbus_registers: Mapping[int, int] = field(default_factory=dict)


def parse_discovery_redirect(payload: bytes | str) -> DiscoveryRedirect:
    """Parse one collector discovery redirect packet."""

    if isinstance(payload, bytes):
        text = payload.decode("ascii", errors="ignore")
    else:
        text = str(payload)

    normalized = text.strip()
    match = _DISCOVERY_REQUEST_RE.fullmatch(normalized)
    if match is None:
        raise ValueError("discovery_redirect_invalid")
    return DiscoveryRedirect(
        server_ip=match.group(1),
        server_port=int(match.group(2)),
        raw=normalized,
    )


def build_udp_reply(reply_text: str) -> bytes:
    """Build one UDP discovery reply payload."""

    if not isinstance(reply_text, str) or not reply_text.isascii():
        raise ValueError("udp_reply_not_ascii")
    return reply_text.encode("ascii")


def build_unsolicited_heartbeat(
    *,
    tid: int,
    pn: str,
    devcode: int,
    collector_addr: int,
) -> bytes:
    """Build one collector-originated FC=1 heartbeat carrying the PN prefix."""

    heartbeat_text = (pn[:14]).ljust(14, "\x00")
    return build_collector_request(
        tid,
        heartbeat_text.encode("ascii", errors="ignore"),
        devcode=devcode,
        collector_addr=collector_addr,
        fcode=FC_HEARTBEAT,
    )


def resolve_scenario(
    *,
    preset: str,
    profile: CollectorProfile,
    heartbeat_devcode: int | None = None,
    collector_addr: int | None = None,
    device_addr: int = DEFAULT_MODBUS_DEVICE_ADDR,
    reverse_connect_delay: float = 0.0,
    first_heartbeat_delay: float = 0.0,
    query_5_mode: str | None = None,
    query_14_mode: str | None = None,
    fc4_mode: str | None = None,
) -> CollectorScenario:
    """Resolve one declarative fake collector preset into concrete behavior."""

    normalized_preset = str(preset or PRESET_COLLECTOR_ONLY).strip().lower()
    if normalized_preset not in SCENARIO_PRESET_NAMES:
        raise ValueError(f"unsupported_preset:{normalized_preset}")

    default_devcode = 0x0001 if normalized_preset == PRESET_MODBUS_SMG_READONLY else 0x0994
    default_collector_addr = (
        DEFAULT_COLLECTOR_ADDR
        if normalized_preset == PRESET_MODBUS_SMG_READONLY
        else 0x01
    )
    default_fc2_modes = {
        5: QUERY_MODE_SUCCESS if normalized_preset in {PRESET_SMARTESS_HINT, PRESET_MODBUS_SMG_READONLY} else QUERY_MODE_FAIL,
        14: QUERY_MODE_SUCCESS if normalized_preset in {PRESET_SMARTESS_HINT, PRESET_MODBUS_SMG_READONLY} else QUERY_MODE_FAIL,
    }
    resolved_fc4_mode = (
        FC4_MODE_SUCCESS
        if normalized_preset == PRESET_MODBUS_SMG_READONLY
        else FC4_MODE_MODBUS_EXCEPTION
    )

    resolved_query_5_mode = _normalize_query_mode(query_5_mode or default_fc2_modes[5])
    resolved_query_14_mode = _normalize_query_mode(query_14_mode or default_fc2_modes[14])
    resolved_fc4_mode = _normalize_fc4_mode(fc4_mode or resolved_fc4_mode)

    registers: dict[int, int] = {}
    if normalized_preset == PRESET_MODBUS_SMG_READONLY:
        registers = _build_modbus_smg_registers(profile)

    return CollectorScenario(
        preset=normalized_preset,
        profile=profile,
        heartbeat_devcode=default_devcode if heartbeat_devcode is None else int(heartbeat_devcode),
        collector_addr=default_collector_addr if collector_addr is None else int(collector_addr),
        device_addr=int(device_addr),
        reverse_connect_delay=max(0.0, float(reverse_connect_delay)),
        first_heartbeat_delay=max(0.0, float(first_heartbeat_delay)),
        fc2_query_modes={5: resolved_query_5_mode, 14: resolved_query_14_mode},
        fc4_mode=resolved_fc4_mode,
        modbus_registers=registers,
    )


def build_query_collector_response(parameter: int, scenario: CollectorScenario) -> bytes | None:
    """Build one FC=2 SmartESS local collector reply payload or drop it."""

    parameter_u8 = int(parameter) & 0xFF
    behavior = scenario.fc2_query_modes.get(parameter_u8, QUERY_MODE_FAIL)
    if behavior == QUERY_MODE_TIMEOUT:
        return None
    if behavior == QUERY_MODE_FAIL:
        return bytes((1, parameter_u8))

    profile = scenario.profile
    if parameter_u8 == 5:
        return bytes((0, parameter_u8)) + profile.firmware_version.encode("ascii")
    if parameter_u8 == 14:
        return bytes((0, parameter_u8)) + profile.protocol_descriptor.encode("ascii")
    return bytes((1, parameter_u8))


def build_set_collector_response(parameter: int, *, success: bool = False) -> bytes:
    """Build one FC=3 collector-set response payload."""

    return bytes((0 if success else 1, int(parameter) & 0xFF))


def build_forward_response(
    payload: bytes,
    scenario: CollectorScenario,
    *,
    modbus_exception: int = 1,
) -> bytes | None:
    """Build one FC=4 response according to the active scenario."""

    if scenario.fc4_mode == FC4_MODE_TIMEOUT:
        return None

    if scenario.fc4_mode == FC4_MODE_SUCCESS:
        modbus_payload = _build_modbus_response(payload, scenario)
        if modbus_payload is not None:
            return modbus_payload

    modbus_payload = _build_modbus_exception_payload(payload, exception_code=modbus_exception)
    if modbus_payload is not None:
        return modbus_payload

    if _looks_like_pi30_payload(payload):
        return _build_pi30_text_response("NAK")

    return b""


def build_at_reply(
    command: str,
    *,
    profile: CollectorProfile,
    cloud_endpoint: str,
    write_ack: bool = False,
) -> bytes:
    """Build one AT response line for a supported command."""

    normalized = normalize_at_command(command)
    if write_ack:
        return build_at_response(normalized, "W000")

    if normalized == "DTUPN":
        return build_at_response(normalized, profile.pn)
    if normalized == "ATVER":
        return build_at_response(normalized, profile.at_version)
    if normalized == "ENUPMODE":
        return build_at_response(normalized, profile.upload_mode)
    if normalized == "SYST":
        return build_at_response(
            normalized,
            datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
        )
    if normalized == "WFSS":
        return build_at_response(normalized, profile.wifi_rssi)
    if normalized == "UART":
        return build_at_response(normalized, profile.uart)
    if normalized == "DTUTYPE":
        return build_at_response(normalized, profile.collector_type)
    if normalized == "FWVER":
        return build_at_response(normalized, profile.firmware_version)
    if normalized == "CLDSRVHOST1":
        return build_at_response(normalized, cloud_endpoint)
    if normalized == "HTBT":
        return build_at_response(normalized, "")
    if normalized == "LINK":
        return build_at_response(normalized, profile.link_status)
    if normalized == "INTPARA49":
        return build_at_response(normalized, profile.wifi_scan_list)
    if normalized == "VDTU":
        # Factory collectors leave ``vdtu`` empty -> no prefix -> not a bridge.
        return build_at_response(normalized, profile.vdtu)
    return build_at_response(normalized, "")


def _normalize_query_mode(value: str) -> str:
    normalized = str(value or QUERY_MODE_FAIL).strip().lower()
    if normalized not in QUERY_MODE_NAMES:
        raise ValueError(f"unsupported_query_mode:{normalized}")
    return normalized


def _normalize_fc4_mode(value: str) -> str:
    normalized = str(value or FC4_MODE_MODBUS_EXCEPTION).strip().lower()
    if normalized not in FC4_MODE_NAMES:
        raise ValueError(f"unsupported_fc4_mode:{normalized}")
    return normalized


def _build_modbus_smg_registers(profile: CollectorProfile) -> dict[int, int]:
    registers: dict[int, int] = {
        register: 0
        for start, stop in (
            (100, 110),
            (171, 185),
            (186, 198),
            (201, 235),
            (300, 344),
            (351, 352),
            (406, 407),
            (420, 421),
            (626, 645),
        )
        for register in range(start, stop)
    }

    for offset, value in _ascii_words(profile.model_name, word_count=12).items():
        registers[172 + offset] = value
    for offset, value in _ascii_words(profile.serial_number, word_count=12).items():
        registers[186 + offset] = value
    for offset, value in _ascii_words("U1.00", word_count=8).items():
        registers[626 + offset] = value

    registers.update(
        {
            171: int(profile.device_type) & 0xFFFF,
            184: int(profile.protocol_number) & 0xFFFF,
            201: 3,
            202: 2300,
            203: 5000,
            204: 120,
            210: 2295,
            211: 12,
            212: 5000,
            213: 2500,
            215: 512,
            219: 650,
            220: 10,
            223: 800,
            225: 40,
            231: 97,
            300: 0,
            301: 1,
            302: 0,
            303: 3,
            305: 1,
            306: 1,
            307: 0,
            308: 1,
            309: 1,
            310: 0,
            313: 1,
            314: 0x1234,
            315: 0x5678,
            316: 1,
            320: 2300,
            321: 5000,
            322: 2,
            323: 620,
            324: 560,
            325: 540,
            326: 520,
            327: 480,
            329: 470,
            331: 1,
            332: 600,
            333: 200,
            334: 580,
            335: 60,
            336: 120,
            337: 30,
            338: 1,
            341: 25,
            342: 45,
            343: 15,
            351: int(profile.max_discharge_current_protection) & 0xFFFF,
            406: 0,
            420: 1,
            643: int(profile.rated_power) & 0xFFFF,
            644: int(profile.rated_cell_count) & 0xFFFF,
        }
    )
    return registers


def _ascii_words(text: str, *, word_count: int) -> dict[int, int]:
    payload = str(text or "").encode("ascii", errors="ignore")[: word_count * 2].ljust(word_count * 2, b"\x00")
    return {
        offset: int.from_bytes(payload[offset * 2 : offset * 2 + 2], "big")
        for offset in range(word_count)
    }


def _looks_like_pi30_payload(payload: bytes) -> bool:
    return bool(payload.endswith(b"\r") and payload[:1].isalpha())


def _build_modbus_response(payload: bytes, scenario: CollectorScenario) -> bytes | None:
    if len(payload) < 4:
        return None

    body = payload[:-2]
    crc_received = int.from_bytes(payload[-2:], "little")
    if crc16_modbus(body) != crc_received:
        return None

    slave_id = body[0]
    function_code = body[1]
    if slave_id != scenario.device_addr:
        return _build_modbus_exception_payload(payload, exception_code=2)

    if function_code == 0x03:
        address = int.from_bytes(body[2:4], "big")
        count = int.from_bytes(body[4:6], "big")
        words: list[int] = []
        for register in range(address, address + count):
            if register not in scenario.modbus_registers:
                return _build_modbus_exception_payload(payload, exception_code=2)
            words.append(int(scenario.modbus_registers[register]) & 0xFFFF)

        response = bytearray([slave_id, 0x03, count * 2])
        for value in words:
            response.extend(value.to_bytes(2, "big", signed=False))
        response_crc = crc16_modbus(response)
        response.extend(response_crc.to_bytes(2, "little"))
        return bytes(response)

    if function_code == 0x10:
        return _build_modbus_exception_payload(payload, exception_code=1)

    return _build_modbus_exception_payload(payload, exception_code=1)


def _build_modbus_exception_payload(payload: bytes, *, exception_code: int) -> bytes | None:
    if len(payload) < 4:
        return None

    body = payload[:-2]
    crc_received = int.from_bytes(payload[-2:], "little")
    if crc16_modbus(body) != crc_received:
        return None

    slave_id = body[0]
    function_code = body[1]
    if function_code >= 0x80:
        return None

    response = bytes((slave_id, function_code | 0x80, int(exception_code) & 0xFF))
    crc = crc16_modbus(response).to_bytes(2, "little")
    return response + crc


def _build_pi30_text_response(value: str) -> bytes:
    body = f"({value}".encode("ascii")
    return body + _encode_pi30_crc(body) + b"\r"


def _encode_pi30_crc(payload: bytes) -> bytes:
    crc = crc16_xmodem(payload)
    high = (crc >> 8) & 0xFF
    low = crc & 0xFF
    return bytes((_escape_pi30_crc(high), _escape_pi30_crc(low)))


def _escape_pi30_crc(value: int) -> int:
    return value + 1 if value in {0x28, 0x0D, 0x0A} else value


__all__ = [
    "CollectorProfile",
    "CollectorScenario",
    "DiscoveryRedirect",
    "FC4_MODE_MODBUS_EXCEPTION",
    "FC4_MODE_NAMES",
    "FC4_MODE_SUCCESS",
    "FC4_MODE_TIMEOUT",
    "PRESET_COLLECTOR_ONLY",
    "PRESET_MODBUS_SMG_READONLY",
    "PRESET_SMARTESS_HINT",
    "QUERY_MODE_FAIL",
    "QUERY_MODE_NAMES",
    "QUERY_MODE_SUCCESS",
    "QUERY_MODE_TIMEOUT",
    "SCENARIO_PRESET_NAMES",
    "build_at_reply",
    "build_forward_response",
    "build_query_collector_response",
    "build_set_collector_response",
    "build_udp_reply",
    "build_unsolicited_heartbeat",
    "parse_discovery_redirect",
    "resolve_scenario",
]
