"""Protocol adapter layer for shadow-learning local observations."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from ..payload.modbus import (
    crc16_modbus,
    decode_read_request,
    decode_write_request,
)
from .shadow_learning import (
    ShadowWriteObservation,
    utc_now_iso,
    write_observation_from_modbus_request,
)


_MODBUS_RTU_FAMILIES: frozenset[str] = frozenset(
    {
        "modbus_smg",
        "srne_modbus",
        "must_pv_ph18",
    }
)
_MODBUS_RTU_CLOUD_FAMILIES: frozenset[str] = frozenset(
    {
        "legacy_binary",
    }
)
_ASCII_FAMILIES: frozenset[str] = frozenset({"pi18", "pi30"})
_EYBOND_G_ASCII_FAMILIES: frozenset[str] = frozenset(
    {
        "eybond_g_ascii",
    }
)
_EYBOND_G_ASCII_FALLBACK_READ_COMMANDS: frozenset[str] = frozenset(
    {
        "F",
        "GMOD",
        "SVFW",
        "GTMP",
        "GLINE",
        "GBAT",
        "GBUS",
        "GCHG",
        "GOP",
        "GINV",
        "GWS",
        "BL",
        "GPV",
    }
)
_EYBOND_G_ASCII_COMMAND_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "protocol_catalogs"
    / "command_schemas"
    / "eybond_g_ascii"
    / "base.json"
)


@dataclass(frozen=True, slots=True)
class ShadowReadRequest:
    """Protocol-neutral read request consumed by the shadow backend."""

    unit: int
    function_code: int
    address: int
    count: int
    command: str = ""


@dataclass(frozen=True, slots=True)
class ShadowWriteRequest:
    """Protocol-neutral write request consumed by the shadow backend."""

    unit: int
    function_code: int
    address: int
    values: tuple[int, ...]
    command: str = ""
    value: str = ""

    @property
    def count(self) -> int:
        return len(self.values)


@dataclass(frozen=True, slots=True)
class ShadowLearningProtocolAdapter:
    """Base adapter contract for protocol-specific shadow-learning decoding."""

    key: str
    supported: bool
    blocker: str = ""

    def decode_read_request(self, frame: bytes) -> ShadowReadRequest | None:
        return None

    def decode_write_request(self, frame: bytes) -> ShadowWriteRequest | None:
        return None

    def write_observation(
        self,
        *,
        frame: bytes,
        devcode: int | None,
        devaddr: int | None,
        timestamp: str = "",
        source: str = "shadow_learning",
    ) -> ShadowWriteObservation | None:
        return None

    def build_read_response(self, request: ShadowReadRequest, values: list[int]) -> bytes:
        raise NotImplementedError

    def build_seeded_read_response(
        self,
        request: ShadowReadRequest,
        values: list[int],
        command_responses: dict[str, str],
    ) -> bytes:
        return self.build_read_response(request, values)

    def build_write_ack_response(self, request: ShadowWriteRequest) -> bytes:
        raise NotImplementedError

    def build_write_exception_response(
        self, request: ShadowWriteRequest, *, exception_code: int
    ) -> bytes:
        raise NotImplementedError

    def build_raw_exception(self, frame: bytes, *, exception_code: int) -> bytes:
        raise NotImplementedError

    def apply_write_to_register_bank(
        self, request: ShadowWriteRequest, register_bank: dict[int, int]
    ) -> None:
        return None


class ModbusRtuShadowLearningAdapter(ShadowLearningProtocolAdapter):
    """Shadow-learning adapter for Modbus RTU payloads."""

    def __init__(self) -> None:
        super().__init__(key="modbus_rtu", supported=True)

    def decode_read_request(self, frame: bytes) -> ShadowReadRequest | None:
        request = decode_read_request(frame)
        if request is None:
            return None
        return ShadowReadRequest(
            unit=request.slave_id,
            function_code=request.function_code,
            address=request.address,
            count=request.count,
        )

    def decode_write_request(self, frame: bytes) -> ShadowWriteRequest | None:
        request = decode_write_request(frame)
        if request is None:
            return None
        return ShadowWriteRequest(
            unit=request.slave_id,
            function_code=request.function_code,
            address=request.address,
            values=tuple(request.values),
        )

    def write_observation(
        self,
        *,
        frame: bytes,
        devcode: int | None,
        devaddr: int | None,
        timestamp: str = "",
        source: str = "shadow_learning",
    ) -> ShadowWriteObservation | None:
        return write_observation_from_modbus_request(
            frame=frame,
            devcode=devcode,
            devaddr=devaddr,
            timestamp=timestamp or utc_now_iso(),
            source=source,
        )

    def build_read_response(self, request: ShadowReadRequest, values: list[int]) -> bytes:
        payload = bytearray([request.unit, request.function_code, len(values) * 2])
        for value in values:
            payload.extend(int(value).to_bytes(2, "big", signed=False))
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def build_write_ack_response(self, request: ShadowWriteRequest) -> bytes:
        payload = bytearray([request.unit, request.function_code])
        payload.extend(int(request.address).to_bytes(2, "big", signed=False))
        if request.function_code == 0x06 and request.values:
            payload.extend(int(request.values[0]).to_bytes(2, "big", signed=False))
        else:
            payload.extend(int(len(request.values)).to_bytes(2, "big", signed=False))
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def build_write_exception_response(
        self, request: ShadowWriteRequest, *, exception_code: int
    ) -> bytes:
        payload = bytearray([request.unit, request.function_code | 0x80, exception_code])
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def build_raw_exception(self, frame: bytes, *, exception_code: int) -> bytes:
        unit = frame[0] if len(frame) > 0 else 0
        function = frame[1] if len(frame) > 1 else 0
        payload = bytearray([unit, function | 0x80, exception_code])
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def apply_write_to_register_bank(
        self, request: ShadowWriteRequest, register_bank: dict[int, int]
    ) -> None:
        if request.function_code == 0x06 and request.values:
            register_bank[request.address] = int(request.values[0])
            return
        for offset, value in enumerate(request.values):
            register_bank[request.address + offset] = int(value)


class EybondGAsciiShadowLearningAdapter(ShadowLearningProtocolAdapter):
    """Shadow-learning adapter for ValueCloud/EyeBond G-ASCII payloads."""

    def __init__(self) -> None:
        super().__init__(key="eybond_g_ascii", supported=True)

    def decode_read_request(self, frame: bytes) -> ShadowReadRequest | None:
        command = _decode_g_ascii_command(frame)
        if not command:
            return None
        if _is_g_ascii_read_command(command):
            return ShadowReadRequest(
                unit=0,
                function_code=0,
                address=0,
                count=0,
                command=command,
            )
        return None

    def decode_write_request(self, frame: bytes) -> ShadowWriteRequest | None:
        command = _decode_g_ascii_command(frame)
        if not command or _is_g_ascii_read_command(command):
            return None
        key, value = _split_g_ascii_write_command(command)
        return ShadowWriteRequest(
            unit=0,
            function_code=0,
            address=-1,
            values=(),
            command=key,
            value=value,
        )

    def write_observation(
        self,
        *,
        frame: bytes,
        devcode: int | None,
        devaddr: int | None,
        timestamp: str = "",
        source: str = "shadow_learning",
    ) -> ShadowWriteObservation | None:
        request = self.decode_write_request(frame)
        if request is None:
            return None
        return ShadowWriteObservation(
            timestamp=str(timestamp or utc_now_iso()),
            source=str(source or "shadow_learning"),
            unit=0,
            function_code=0,
            register=-1,
            values=(),
            devcode=devcode,
            devaddr=devaddr,
            raw_payload_hex=frame.hex(),
            protocol=self.key,
            command=request.command,
            value=request.value,
        )

    def build_read_response(self, request: ShadowReadRequest, values: list[int]) -> bytes:
        return self.build_raw_exception(b"", exception_code=0x01)

    def build_seeded_read_response(
        self,
        request: ShadowReadRequest,
        values: list[int],
        command_responses: dict[str, str],
    ) -> bytes:
        command = str(request.command or "").strip().upper()
        if not command:
            return self.build_raw_exception(b"", exception_code=0x01)
        value = _lookup_g_ascii_response(command, command_responses)
        if value is None:
            return self.build_raw_exception(command.encode("ascii", errors="ignore") + b"\r", exception_code=0x01)
        return _normalize_g_ascii_response(command, value)

    def build_write_ack_response(self, request: ShadowWriteRequest) -> bytes:
        return b"ACK\r"

    def build_write_exception_response(
        self, request: ShadowWriteRequest, *, exception_code: int
    ) -> bytes:
        return self.build_raw_exception(b"", exception_code=exception_code)

    def build_raw_exception(self, frame: bytes, *, exception_code: int) -> bytes:
        return b"NAK\r"


class UnsupportedShadowLearningAdapter(ShadowLearningProtocolAdapter):
    """Fail-closed adapter for explicit protocols without local learning support."""

    def __init__(self, protocol_family: str) -> None:
        normalized = str(protocol_family or "unknown").strip() or "unknown"
        super().__init__(
            key="unsupported",
            supported=False,
            blocker=f"unsupported_shadow_learning_protocol:{normalized}",
        )


def resolve_shadow_learning_protocol_adapter(
    snapshot: dict[str, Any] | None,
    *,
    collector_cloud_family: str = "",
    raw_passthrough_frame_format: str = "",
) -> ShadowLearningProtocolAdapter:
    """Resolve the cloud-side protocol adapter for one shadow-learning session.

    ``snapshot`` describes the local HA-to-inverter protocol. The collector
    cloud family describes the cloud/session provider, not necessarily the
    inverter payload protocol, so it must not by itself select a G-ASCII
    adapter. Unknown legacy snapshots default to Modbus for backward
    compatibility. Explicit non-Modbus local protocols fail closed unless
    runtime protocol evidence identifies a supported dialect.
    """

    snapshot = snapshot if isinstance(snapshot, dict) else {}
    cloud_family = str(collector_cloud_family or "").strip().lower()
    raw_frame_format = str(raw_passthrough_frame_format or "").strip().lower()
    if cloud_family in _MODBUS_RTU_CLOUD_FAMILIES:
        return ModbusRtuShadowLearningAdapter()

    protocol_family = str(snapshot.get("protocol_family") or "").strip().lower()
    driver_key = str(snapshot.get("driver_key") or "").strip().lower()
    effective_owner_key = str(snapshot.get("effective_owner_key") or "").strip().lower()
    profile_name = str(snapshot.get("profile_name") or "").strip().lower()
    schema_name = str(snapshot.get("register_schema_name") or "").strip().lower()
    explicit_keys = {
        value
        for value in (protocol_family, driver_key, effective_owner_key)
        if value
    }
    if explicit_keys & _MODBUS_RTU_FAMILIES:
        return ModbusRtuShadowLearningAdapter()
    if explicit_keys & _EYBOND_G_ASCII_FAMILIES:
        return EybondGAsciiShadowLearningAdapter()
    if raw_frame_format == "plain_line" and (
        "eybond_g_ascii" in profile_name
        or "eybond_g_ascii" in schema_name
    ):
        return EybondGAsciiShadowLearningAdapter()
    if explicit_keys & _ASCII_FAMILIES:
        return UnsupportedShadowLearningAdapter(protocol_family or driver_key)

    evidence = " ".join(
        value
        for value in (
            protocol_family,
            driver_key,
            effective_owner_key,
            profile_name,
            schema_name,
        )
        if value
    )
    if not evidence:
        return ModbusRtuShadowLearningAdapter()
    if "modbus" in evidence or "smg" in evidence:
        return ModbusRtuShadowLearningAdapter()
    if "eybond_g_ascii" in evidence:
        return EybondGAsciiShadowLearningAdapter()
    return UnsupportedShadowLearningAdapter(protocol_family or driver_key or profile_name)


def _decode_g_ascii_command(frame: bytes) -> str:
    if not frame or len(frame) > 128 or not frame.endswith(b"\r"):
        return ""
    payload = frame[:-1]
    if not payload:
        return ""
    try:
        text = payload.decode("ascii").strip()
    except UnicodeDecodeError:
        return ""
    if not text or text.startswith("AT+"):
        return ""
    if any(ord(char) < 0x20 or ord(char) > 0x7E for char in text):
        return ""
    return text.upper()


def _is_g_ascii_read_command(command: str) -> bool:
    normalized = str(command or "").strip().upper()
    return (
        normalized in _eybond_g_ascii_read_commands()
        or (normalized.startswith("GPDAT") and normalized[5:].isdigit())
        or normalized.endswith("?")
    )


@lru_cache(maxsize=1)
def _eybond_g_ascii_read_commands() -> frozenset[str]:
    """Return read/query commands from the shared G-ASCII command schema.

    Shadow learning, runtime support captures, and offline protocol evidence
    must classify the same commands the same way.  Keep a conservative fallback
    so learning remains fail-closed-but-usable if a local package is incomplete.
    """

    commands: set[str] = set(_EYBOND_G_ASCII_FALLBACK_READ_COMMANDS)
    try:
        schema = json.loads(_EYBOND_G_ASCII_COMMAND_SCHEMA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return frozenset(commands)

    for raw_item in schema.get("commands") or []:
        if not isinstance(raw_item, dict):
            continue
        access = str(raw_item.get("access") or "").strip().lower()
        if access not in {"read", "query"}:
            continue
        command = str(raw_item.get("command") or "").strip().upper()
        if command:
            commands.add(command)
    return frozenset(commands)


def _split_g_ascii_write_command(command: str) -> tuple[str, str]:
    normalized = str(command or "").strip().upper()
    for index, char in enumerate(normalized):
        if char.isdigit() or char in {"+", "-", "."}:
            return normalized[:index] or normalized, normalized[index:]
    return normalized, ""


def _lookup_g_ascii_response(command: str, responses: dict[str, str]) -> str | None:
    normalized = str(command or "").strip().upper()
    for key in (normalized, normalized.rstrip("?")):
        if key in responses:
            return responses[key]
    return None


def _normalize_g_ascii_response(command: str, value: str) -> bytes:
    text = str(value or "").strip()
    if not text:
        return b"NAK\r"
    if text.endswith("\r"):
        return text.encode("ascii", errors="replace")
    upper = text.upper()
    if upper in {"ACK", "NAK", "NOA", "ERCRC"}:
        return f"{upper}\r".encode("ascii")
    if text.startswith(("(", "#")):
        return f"{text}\r".encode("ascii", errors="replace")
    if str(command or "").strip().upper() == "BL":
        return f"BL{text}\r".encode("ascii", errors="replace")
    if str(command or "").strip().upper() == "F":
        return f"#{text}\r".encode("ascii", errors="replace")
    return f"({text}\r".encode("ascii", errors="replace")


__all__ = [
    "ShadowLearningProtocolAdapter",
    "ShadowReadRequest",
    "ShadowWriteRequest",
    "ModbusRtuShadowLearningAdapter",
    "EybondGAsciiShadowLearningAdapter",
    "UnsupportedShadowLearningAdapter",
    "resolve_shadow_learning_protocol_adapter",
]
