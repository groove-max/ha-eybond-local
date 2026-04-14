"""Minimal Modbus RTU helpers routed over a generic payload link."""

from __future__ import annotations

import asyncio

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload


class ModbusError(Exception):
    """Raised when a Modbus RTU response cannot be decoded."""


def crc16_modbus(data: bytes) -> int:
    """Compute Modbus CRC16."""

    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def to_signed_16(value: int) -> int:
    """Convert an unsigned 16-bit register into a signed integer."""

    return value - 0x10000 if value >= 0x8000 else value


def build_read_holding_request(slave_id: int, address: int, count: int) -> bytes:
    """Build a Modbus RTU read holding registers request."""

    payload = bytearray(
        [
            slave_id,
            0x03,
            (address >> 8) & 0xFF,
            address & 0xFF,
            (count >> 8) & 0xFF,
            count & 0xFF,
        ]
    )
    crc = crc16_modbus(payload)
    payload.extend(crc.to_bytes(2, "little"))
    return bytes(payload)


def build_write_multiple_request(slave_id: int, address: int, values: list[int]) -> bytes:
    """Build a Modbus RTU write multiple registers request."""

    register_count = len(values)
    byte_count = register_count * 2
    payload = bytearray(
        [
            slave_id,
            0x10,
            (address >> 8) & 0xFF,
            address & 0xFF,
            (register_count >> 8) & 0xFF,
            register_count & 0xFF,
            byte_count,
        ]
    )
    for value in values:
        payload.extend(value.to_bytes(2, "big", signed=False))
    crc = crc16_modbus(payload)
    payload.extend(crc.to_bytes(2, "little"))
    return bytes(payload)


def parse_read_holding_response(frame: bytes, *, slave_id: int, count: int) -> list[int]:
    """Decode a Modbus RTU read holding registers response."""

    if len(frame) < 5:
        raise ModbusError("response_too_short")

    if frame[0] != slave_id:
        raise ModbusError(f"unexpected_slave_id:{frame[0]}")

    function_code = frame[1]
    if function_code == 0x83 and len(frame) >= 5:
        raise ModbusError(f"exception_code:{frame[2]}")
    if function_code != 0x03:
        raise ModbusError(f"unexpected_function:{function_code}")

    expected_byte_count = count * 2
    byte_count = frame[2]
    if byte_count != expected_byte_count:
        raise ModbusError(f"unexpected_byte_count:{byte_count}")

    expected_length = 3 + byte_count + 2
    if len(frame) != expected_length:
        raise ModbusError(f"unexpected_length:{len(frame)}")

    crc_received = int.from_bytes(frame[-2:], "little")
    crc_expected = crc16_modbus(frame[:-2])
    if crc_received != crc_expected:
        raise ModbusError("crc_mismatch")

    registers: list[int] = []
    payload = frame[3:-2]
    for offset in range(0, len(payload), 2):
        registers.append(int.from_bytes(payload[offset : offset + 2], "big"))
    return registers


def parse_write_multiple_response(
    frame: bytes,
    *,
    slave_id: int,
    address: int,
    register_count: int,
) -> None:
    """Validate a Modbus RTU write multiple registers response."""

    if len(frame) < 5:
        raise ModbusError("response_too_short")
    if frame[0] != slave_id:
        raise ModbusError(f"unexpected_slave_id:{frame[0]}")
    if frame[1] == 0x90:
        crc_received = int.from_bytes(frame[-2:], "little")
        crc_expected = crc16_modbus(frame[:-2])
        if crc_received != crc_expected:
            raise ModbusError("crc_mismatch")
        raise ModbusError(f"exception_code:{frame[2]}")
    if len(frame) != 8:
        raise ModbusError(f"unexpected_length:{len(frame)}")
    if frame[1] != 0x10:
        raise ModbusError(f"unexpected_function:{frame[1]}")

    crc_received = int.from_bytes(frame[-2:], "little")
    crc_expected = crc16_modbus(frame[:-2])
    if crc_received != crc_expected:
        raise ModbusError("crc_mismatch")

    address_received = int.from_bytes(frame[2:4], "big")
    count_received = int.from_bytes(frame[4:6], "big")
    if address_received != address:
        raise ModbusError(f"unexpected_address:{address_received}")
    if count_received != register_count:
        raise ModbusError(f"unexpected_register_count:{count_received}")


class ModbusSession:
    """Modbus RTU session routed through one generic payload link."""

    def __init__(
        self,
        transport: PayloadLinkTransport,
        *,
        route: EybondLinkRoute | None = None,
        devcode: int | None = None,
        collector_addr: int | None = None,
        slave_id: int,
    ) -> None:
        self._transport = transport
        if route is None:
            if devcode is None or collector_addr is None:
                raise TypeError("modbus_route_required")
            route = EybondLinkRoute(
                devcode=devcode,
                collector_addr=collector_addr,
            )
        self._route = route
        self._slave_id = slave_id

    async def read_holding(self, address: int, count: int) -> list[int]:
        """Read holding registers from the inverter."""

        request = build_read_holding_request(self._slave_id, address, count)
        last_error: ModbusError | None = None
        for attempt in range(2):
            try:
                response = await async_send_payload(
                    self._transport,
                    request,
                    route=self._route,
                )
            except asyncio.TimeoutError as exc:
                last_error = ModbusError("request_timeout")
                if attempt == 0:
                    await asyncio.sleep(0.15)
                    continue
                raise last_error from exc
            try:
                return parse_read_holding_response(response, slave_id=self._slave_id, count=count)
            except ModbusError as exc:
                last_error = exc
                if attempt == 0 and _is_retryable_read_error(exc):
                    await asyncio.sleep(0.15)
                    continue
                raise
        raise last_error or ModbusError("read_failed")

    async def write_holding(self, address: int, values: list[int]) -> None:
        """Write one or more holding registers to the inverter."""

        request = build_write_multiple_request(self._slave_id, address, values)
        try:
            response = await async_send_payload(
                self._transport,
                request,
                route=self._route,
            )
        except asyncio.TimeoutError as exc:
            raise ModbusError("request_timeout") from exc
        parse_write_multiple_response(
            response,
            slave_id=self._slave_id,
            address=address,
            register_count=len(values),
        )


def _is_retryable_read_error(error: ModbusError) -> bool:
    """Return whether one Modbus read error looks transient enough to retry once."""

    text = str(error)
    return text in {
        "response_too_short",
        "crc_mismatch",
    } or text.startswith("unexpected_length:")
