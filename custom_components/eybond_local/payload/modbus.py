"""Minimal Modbus RTU helpers routed over a generic payload link."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ..link_models import EybondLinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload


class ModbusError(Exception):
    """Raised when a Modbus RTU response cannot be decoded."""


@dataclass(frozen=True, slots=True)
class ModbusReadRequestFrame:
    """Decoded Modbus RTU read request frame (function 0x03 or 0x04)."""

    slave_id: int
    function_code: int
    address: int
    count: int


@dataclass(frozen=True, slots=True)
class ModbusWriteRequestFrame:
    """Decoded Modbus RTU write request frame (function 0x06 or 0x10)."""

    slave_id: int
    function_code: int
    address: int
    values: tuple[int, ...]

    @property
    def count(self) -> int:
        """Return number of written registers."""

        return len(self.values)


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


def merge_register_field(current: int, mask: int, field: int) -> int:
    """Return one 16-bit register with only the masked bits replaced by ``field``.

    Read-modify-write primitive shared by SMG bitmask capabilities and the raw
    diagnostic ``write_bit`` command, so there is one merge implementation. Bits
    of ``field`` outside ``mask`` are ignored; the result is clamped to 16 bits.
    """

    masked = mask & 0xFFFF
    kept = int(current) & 0xFFFF & ~masked
    return (kept | (int(field) & masked)) & 0xFFFF


def merge_register_bit(current: int, bit_index: int, bit_value: int) -> int:
    """Return one 16-bit register with a single bit set or cleared.

    Single-bit case of :func:`merge_register_field`. ``bit_index`` is ``0..15``
    and ``bit_value`` is ``0`` or ``1``; the other 15 bits are preserved.
    """

    mask = 1 << bit_index
    field = (bit_value & 1) << bit_index
    return merge_register_field(current, mask, field)


def decode_read_request(frame: bytes) -> ModbusReadRequestFrame | None:
    """Decode one Modbus RTU read request frame when valid."""

    if len(frame) != 8:
        return None
    function_code = frame[1]
    if function_code not in {0x03, 0x04}:
        return None
    crc_received = int.from_bytes(frame[-2:], "little")
    crc_expected = crc16_modbus(frame[:-2])
    if crc_received != crc_expected:
        return None
    return ModbusReadRequestFrame(
        slave_id=frame[0],
        function_code=function_code,
        address=int.from_bytes(frame[2:4], "big"),
        count=int.from_bytes(frame[4:6], "big"),
    )


def decode_write_request(frame: bytes) -> ModbusWriteRequestFrame | None:
    """Decode one Modbus RTU write request frame when valid."""

    if len(frame) < 8:
        return None
    function_code = frame[1]
    crc_received = int.from_bytes(frame[-2:], "little")
    crc_expected = crc16_modbus(frame[:-2])
    if crc_received != crc_expected:
        return None

    if function_code == 0x06 and len(frame) == 8:
        return ModbusWriteRequestFrame(
            slave_id=frame[0],
            function_code=function_code,
            address=int.from_bytes(frame[2:4], "big"),
            values=(int.from_bytes(frame[4:6], "big"),),
        )

    if function_code != 0x10 or len(frame) < 9:
        return None

    address = int.from_bytes(frame[2:4], "big")
    register_count = int.from_bytes(frame[4:6], "big")
    byte_count = frame[6]
    if byte_count != register_count * 2:
        return None
    if len(frame) != 9 + byte_count:
        return None

    values: list[int] = []
    register_bytes = frame[7:-2]
    for offset in range(0, len(register_bytes), 2):
        values.append(int.from_bytes(register_bytes[offset : offset + 2], "big"))
    return ModbusWriteRequestFrame(
        slave_id=frame[0],
        function_code=function_code,
        address=address,
        values=tuple(values),
    )


def build_read_request(
    slave_id: int,
    address: int,
    count: int,
    *,
    function: int = 0x03,
) -> bytes:
    """Build a Modbus RTU read registers request (function 0x03 or 0x04)."""

    if function not in (0x03, 0x04):
        raise ValueError(f"unsupported_read_function:{function}")
    payload = bytearray(
        [
            slave_id,
            function,
            (address >> 8) & 0xFF,
            address & 0xFF,
            (count >> 8) & 0xFF,
            count & 0xFF,
        ]
    )
    crc = crc16_modbus(payload)
    payload.extend(crc.to_bytes(2, "little"))
    return bytes(payload)


def build_read_holding_request(slave_id: int, address: int, count: int) -> bytes:
    """Build a Modbus RTU read holding registers request."""

    return build_read_request(slave_id, address, count, function=0x03)


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


def build_write_single_request(slave_id: int, address: int, value: int) -> bytes:
    """Build a Modbus RTU write single holding register request."""

    payload = bytearray(
        [
            slave_id,
            0x06,
            (address >> 8) & 0xFF,
            address & 0xFF,
            (int(value) >> 8) & 0xFF,
            int(value) & 0xFF,
        ]
    )
    crc = crc16_modbus(payload)
    payload.extend(crc.to_bytes(2, "little"))
    return bytes(payload)


def parse_read_registers_response(
    frame: bytes,
    *,
    slave_id: int,
    count: int,
    function: int = 0x03,
) -> list[int]:
    """Decode a Modbus RTU read registers response (function 0x03 or 0x04)."""

    if function not in (0x03, 0x04):
        raise ValueError(f"unsupported_read_function:{function}")
    if len(frame) < 5:
        raise ModbusError("response_too_short")

    if frame[0] != slave_id:
        raise ModbusError(f"unexpected_slave_id:{frame[0]}")

    function_code = frame[1]
    if function_code == (function | 0x80) and len(frame) >= 5:
        raise ModbusError(f"exception_code:{frame[2]}")
    if function_code != function:
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


def parse_read_holding_response(frame: bytes, *, slave_id: int, count: int) -> list[int]:
    """Decode a Modbus RTU read holding registers response."""

    return parse_read_registers_response(
        frame, slave_id=slave_id, count=count, function=0x03
    )


def parse_write_single_response(
    frame: bytes,
    *,
    slave_id: int,
    address: int,
) -> int:
    """Validate a Modbus RTU write-single response and return the echoed/status value.

    Standard Modbus FC06 responses echo the requested value. Some legacy
    SmartESS/EyeBond collectors have been observed returning a different
    value while still acknowledging the written register with a valid CRC.
    Callers that need strict value confirmation should read the register back
    after this acknowledgement.
    """

    if len(frame) < 5:
        raise ModbusError("response_too_short")
    if frame[0] != slave_id:
        raise ModbusError(f"unexpected_slave_id:{frame[0]}")
    if frame[1] == 0x86:
        crc_received = int.from_bytes(frame[-2:], "little")
        crc_expected = crc16_modbus(frame[:-2])
        if crc_received != crc_expected:
            raise ModbusError("crc_mismatch")
        raise ModbusError(f"exception_code:{frame[2]}")
    if len(frame) != 8:
        raise ModbusError(f"unexpected_length:{len(frame)}")
    if frame[1] != 0x06:
        raise ModbusError(f"unexpected_function:{frame[1]}")

    crc_received = int.from_bytes(frame[-2:], "little")
    crc_expected = crc16_modbus(frame[:-2])
    if crc_received != crc_expected:
        raise ModbusError("crc_mismatch")

    address_received = int.from_bytes(frame[2:4], "big")
    if address_received != address:
        raise ModbusError(f"unexpected_address:{address_received}")
    return int.from_bytes(frame[4:6], "big")


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

    async def read_registers(
        self,
        address: int,
        count: int,
        *,
        function: int = 0x03,
    ) -> list[int]:
        """Read holding (0x03) or input (0x04) registers from the inverter."""

        request = build_read_request(self._slave_id, address, count, function=function)
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
                return parse_read_registers_response(
                    response,
                    slave_id=self._slave_id,
                    count=count,
                    function=function,
                )
            except ModbusError as exc:
                last_error = exc
                if attempt == 0 and _is_retryable_read_error(exc):
                    await asyncio.sleep(0.15)
                    continue
                raise
        raise last_error or ModbusError("read_failed")

    async def read_holding(self, address: int, count: int) -> list[int]:
        """Read holding registers from the inverter."""

        return await self.read_registers(address, count, function=0x03)

    async def read_input(self, address: int, count: int) -> list[int]:
        """Read input registers from the inverter."""

        return await self.read_registers(address, count, function=0x04)

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

    async def write_single_holding(self, address: int, value: int) -> int:
        """Write one holding register using Modbus function code 0x06."""

        request = build_write_single_request(self._slave_id, address, value)
        try:
            response = await async_send_payload(
                self._transport,
                request,
                route=self._route,
            )
        except asyncio.TimeoutError as exc:
            raise ModbusError("request_timeout") from exc
        return parse_write_single_response(
            response,
            slave_id=self._slave_id,
            address=address,
        )


def _is_retryable_read_error(error: ModbusError) -> bool:
    """Return whether one Modbus read error looks transient enough to retry once."""

    text = str(error)
    return text in {
        "response_too_short",
        "crc_mismatch",
    } or text.startswith("unexpected_length:")
