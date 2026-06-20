"""Protocol adapter layer for shadow-learning local observations."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class ShadowReadRequest:
    """Protocol-neutral read request consumed by the shadow backend."""

    unit: int
    function_code: int
    address: int
    count: int


@dataclass(frozen=True, slots=True)
class ShadowWriteRequest:
    """Protocol-neutral write request consumed by the shadow backend."""

    unit: int
    function_code: int
    address: int
    values: tuple[int, ...]

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
) -> ShadowLearningProtocolAdapter:
    """Resolve the cloud-side protocol adapter for one shadow-learning session.

    ``snapshot`` describes the local HA-to-inverter protocol. Shadow learning
    observes the cloud-to-collector data plane instead, so a known collector
    cloud family wins over the local driver. Unknown legacy snapshots default
    to Modbus for backward compatibility. An explicit non-Modbus local protocol
    fails closed only when no supported cloud-side dialect is known.
    """

    snapshot = snapshot if isinstance(snapshot, dict) else {}
    cloud_family = str(collector_cloud_family or "").strip().lower()
    if cloud_family in _MODBUS_RTU_CLOUD_FAMILIES:
        return ModbusRtuShadowLearningAdapter()

    protocol_family = str(snapshot.get("protocol_family") or "").strip().lower()
    driver_key = str(snapshot.get("driver_key") or "").strip().lower()
    profile_name = str(snapshot.get("profile_name") or "").strip().lower()
    schema_name = str(snapshot.get("register_schema_name") or "").strip().lower()
    explicit_keys = {
        value
        for value in (protocol_family, driver_key)
        if value
    }
    if explicit_keys & _MODBUS_RTU_FAMILIES:
        return ModbusRtuShadowLearningAdapter()
    if explicit_keys & _ASCII_FAMILIES:
        return UnsupportedShadowLearningAdapter(protocol_family or driver_key)

    evidence = " ".join(
        value for value in (protocol_family, driver_key, profile_name, schema_name) if value
    )
    if not evidence:
        return ModbusRtuShadowLearningAdapter()
    if "modbus" in evidence or "smg" in evidence:
        return ModbusRtuShadowLearningAdapter()
    return UnsupportedShadowLearningAdapter(protocol_family or driver_key or profile_name)


__all__ = [
    "ShadowLearningProtocolAdapter",
    "ShadowReadRequest",
    "ShadowWriteRequest",
    "ModbusRtuShadowLearningAdapter",
    "UnsupportedShadowLearningAdapter",
    "resolve_shadow_learning_protocol_adapter",
]
