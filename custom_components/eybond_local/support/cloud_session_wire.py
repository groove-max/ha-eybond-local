"""Streaming parser for the mixed collector-cloud TCP data plane."""

from __future__ import annotations

from ..collector.protocol import (
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    FC_SET_COLLECTOR,
    FC_SET_DEVICE_REG,
    FC_TRIGGER_QUERY_HISTORY,
    FC_TRIGGER_QUERY_REAL_TIME,
    HEADER_SIZE,
    decode_header,
)
from ..payload.modbus import crc16_modbus

_MODBUS_FIXED_LEN_FCODES = frozenset({3, 4, 6})
_MODBUS_WRITE_MULTIPLE_FCODE = 16
_KNOWN_EYBOND_FCODES = frozenset(
    {
        FC_HEARTBEAT,
        FC_QUERY_COLLECTOR,
        FC_SET_COLLECTOR,
        FC_FORWARD_TO_DEVICE,
        FC_TRIGGER_QUERY_REAL_TIME,
        FC_SET_DEVICE_REG,
        FC_TRIGGER_QUERY_HISTORY,
    }
)
_MODBUS_INCOMPLETE = object()
_ASCII_INCOMPLETE = object()


def consume_cloud_message(buffer: bytearray) -> tuple[str, bytes] | None:
    """Consume one complete AT, inverter, or EyeBond message from ``buffer``."""

    if not buffer:
        return None
    if buffer.startswith(b"AT+"):
        newline = buffer.find(b"\n")
        if newline < 0:
            return None
        line = bytes(buffer[: newline + 1])
        del buffer[: newline + 1]
        return "at", line

    modbus = _consume_modbus_rtu_frame(buffer)
    if modbus is _MODBUS_INCOMPLETE:
        frame = _consume_eybond_frame_if_complete(buffer)
        if frame is not None:
            return "frame", frame
        return None
    if modbus is not None:
        return "modbus", modbus

    ascii_frame = _consume_g_ascii_frame(buffer)
    if ascii_frame is _ASCII_INCOMPLETE:
        frame = _consume_eybond_frame_if_complete(buffer)
        if frame is not None:
            return "frame", frame
        return None
    if ascii_frame is not None:
        return "ascii", ascii_frame

    if len(buffer) < HEADER_SIZE:
        return None
    header = decode_header(bytes(buffer[:HEADER_SIZE]))
    total_len = header.total_len
    if total_len < HEADER_SIZE:
        raise RuntimeError("cloud_session_invalid_header_length")
    if len(buffer) < total_len:
        return None
    frame = bytes(buffer[:total_len])
    del buffer[:total_len]
    return "frame", frame


def _consume_eybond_frame_if_complete(buffer: bytearray) -> bytes | None:
    if len(buffer) < HEADER_SIZE:
        return None
    try:
        header = decode_header(bytes(buffer[:HEADER_SIZE]))
    except Exception:
        return None
    total_len = header.total_len
    if total_len < HEADER_SIZE or len(buffer) < total_len:
        return None
    if header.fcode not in _KNOWN_EYBOND_FCODES:
        return None
    frame = bytes(buffer[:total_len])
    del buffer[:total_len]
    return frame


def _consume_modbus_rtu_frame(buffer: bytearray) -> bytes | object | None:
    if len(buffer) < 2:
        return None
    function = buffer[1]
    if function in _MODBUS_FIXED_LEN_FCODES:
        frame_length = 8
    elif function == _MODBUS_WRITE_MULTIPLE_FCODE:
        if len(buffer) < 7:
            return _MODBUS_INCOMPLETE
        frame_length = 9 + buffer[6]
    else:
        return None
    if len(buffer) < frame_length:
        return _MODBUS_INCOMPLETE
    frame = bytes(buffer[:frame_length])
    if not _modbus_crc_is_valid(frame):
        return None
    del buffer[:frame_length]
    return frame


def _modbus_crc_is_valid(frame: bytes) -> bool:
    if len(frame) < 4:
        return False
    return crc16_modbus(frame[:-2]) == int.from_bytes(frame[-2:], "little")


def _consume_g_ascii_frame(buffer: bytearray) -> bytes | object | None:
    if not buffer:
        return None
    first = buffer[0]
    if not (first == 0x23 or first == 0x28 or 0x41 <= first <= 0x5A):
        return None
    carriage = buffer.find(b"\r")
    if carriage < 0:
        if len(buffer) < 64:
            return _ASCII_INCOMPLETE
        return None
    if carriage > 128:
        return None
    frame = bytes(buffer[: carriage + 1])
    body = frame[:-1]
    if not body or body.startswith(b"AT+"):
        return None
    if any(byte < 0x20 or byte > 0x7E for byte in body):
        return None
    del buffer[: carriage + 1]
    return frame


__all__ = ["consume_cloud_message"]
