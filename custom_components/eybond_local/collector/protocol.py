"""EyeBond transport framing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import struct

FC_HEARTBEAT = 1
FC_QUERY_COLLECTOR = 2
FC_SET_COLLECTOR = 3
FC_FORWARD_TO_DEVICE = 4
FC_TRIGGER_QUERY_REAL_TIME = 17
FC_SET_DEVICE_REG = 18
FC_TRIGGER_QUERY_HISTORY = 19

HEADER_SIZE = 8
WIRE_LEN_OFFSET = 6


@dataclass(frozen=True, slots=True)
class EybondHeader:
    """Decoded EyeBond frame header."""

    tid: int
    devcode: int
    wire_len: int
    devaddr: int
    fcode: int

    @property
    def total_len(self) -> int:
        return self.wire_len + WIRE_LEN_OFFSET

    @property
    def payload_len(self) -> int:
        return self.total_len - HEADER_SIZE


class TIDCounter:
    """Transaction id counter that wraps at 0xFFFF."""

    def __init__(self) -> None:
        self._value = 0

    def next(self) -> int:
        self._value = (self._value + 1) & 0xFFFF
        return self._value


def encode_header(
    tid: int,
    devcode: int,
    total_len: int,
    devaddr: int,
    fcode: int,
) -> bytes:
    """Encode the fixed 8-byte EyeBond header."""

    return struct.pack(">HHHBB", tid, devcode, total_len - WIRE_LEN_OFFSET, devaddr, fcode)


def decode_header(frame: bytes) -> EybondHeader:
    """Decode the fixed 8-byte EyeBond header."""

    tid, devcode, wire_len, devaddr, fcode = struct.unpack(">HHHBB", frame[:HEADER_SIZE])
    return EybondHeader(
        tid=tid,
        devcode=devcode,
        wire_len=wire_len,
        devaddr=devaddr,
        fcode=fcode,
    )


def build_forward_to_device(
    tid: int,
    payload: bytes,
    *,
    devcode: int,
    collector_addr: int,
) -> bytes:
    """Wrap a device payload in a collector FC=4 frame."""

    return build_collector_request(
        tid,
        payload,
        devcode=devcode,
        collector_addr=collector_addr,
        fcode=FC_FORWARD_TO_DEVICE,
    )


def build_collector_request(
    tid: int,
    payload: bytes,
    *,
    devcode: int,
    collector_addr: int,
    fcode: int,
) -> bytes:
    """Build a raw collector request frame for an arbitrary function code."""

    total_len = HEADER_SIZE + len(payload)
    return encode_header(
        tid=tid,
        devcode=devcode,
        total_len=total_len,
        devaddr=collector_addr,
        fcode=fcode,
    ) + payload


def build_heartbeat_request(tid: int, interval: int) -> bytes:
    """Build an FC=1 collector heartbeat request."""

    payload = build_heartbeat_payload(interval)
    total_len = HEADER_SIZE + len(payload)
    return encode_header(
        tid=tid,
        devcode=0,
        total_len=total_len,
        devaddr=1,
        fcode=FC_HEARTBEAT,
    ) + payload


def build_heartbeat_payload(interval: int) -> bytes:
    """Build the FC=1 heartbeat payload without the EyeBond header."""

    now = datetime.now(timezone.utc)
    return bytes(
        [
            (now.year - 2000) & 0xFF,
            now.month,
            now.day,
            now.hour,
            now.minute,
            now.second,
        ]
    ) + struct.pack(">H", interval)


def parse_heartbeat_pn(payload: bytes) -> str:
    """Heartbeat payloads carry a collector part number in ASCII."""

    return payload[:14].decode("ascii", errors="ignore").strip("\x00")
