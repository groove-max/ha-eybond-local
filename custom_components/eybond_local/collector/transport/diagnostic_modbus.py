"""Explicit, socket-scoped Modbus diagnostics on an existing mixed stream.

Never inferred by discovery, never a permanent wire/profile selection. The
normal reader remains the ONLY reader. An uncertain transaction fences its
physical socket because RTU has no transaction id with which to reject a late
reply. No endpoint, UART/bootstrap or reconnect command is sent here.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math

from ...payload.modbus import crc16_modbus, decode_read_request, decode_write_request
from ..protocol import decode_header
from .binary_framing import BinaryFramingError, runtime_eybond_header_error
from .send_ownership import SocketSendOwner, finish_request_future


@dataclass(frozen=True)
class ModbusDiagnosticClaim:
    request: bytes
    future: asyncio.Future[bytes]
    deadline: float


def _could_be_eybond(prefix: bytes) -> bool:
    """Reject an actual overlapping header, without reading past an RTU reply.

    Short RTU replies (5/7 bytes) are admitted by the explicitly requested
    raw grammar, exact slave/function/length and CRC. Reading an eighth byte
    would consume the NEXT message or deadlock a legitimate short reply.
    This is diagnostic opt-in, never an automatic wire-detection heuristic.
    """
    if len(prefix) >= 8:
        return not runtime_eybond_header_error(decode_header(prefix[:8]))
    return False


class DiagnosticModbusSession:
    """Lifetime belongs to one run() generation, not to a reusable facade."""

    def __init__(self) -> None:
        self.claim: ModbusDiagnosticClaim | None = None
        self.closed = False
        self.received = b""
        self.failure_reason = ""

    def close(self, reason: str = "") -> None:
        self.closed = True
        if reason:
            self.failure_reason = reason
        claim = self.claim
        if claim is not None and not claim.future.done():
            claim.future.set_exception(ConnectionError("collector_disconnected"))

    async def consume(self, reader, first: bytes, guard) -> bool:
        """Consume a matching RTU boundary; return all lookahead otherwise."""
        claim = self.claim
        if claim is None or claim.future.done() or self.closed:
            return False
        request = claim.request
        if first != request[:1]:
            return False

        async def take(size):
            remaining = claim.deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise BinaryFramingError("diagnostic_raw_frame_timeout")
            try:
                data = await guard.read(asyncio.wait_for(reader.readexactly(size), remaining))
            except asyncio.TimeoutError as exc:
                raise BinaryFramingError("diagnostic_raw_frame_timeout") from exc
            if self.closed or self.claim is not claim or claim.future.done():
                raise BinaryFramingError("diagnostic_raw_response_unowned")
            return data

        function = await take(1)
        if function[0] not in (request[1], request[1] | 0x80):
            reader.unread(function)
            return False
        prefix = first + function + await take(1)
        self.received = prefix
        if function[0] & 0x80:
            size = 5
        elif function[0] in (3, 4):
            if prefix[2] != 2 * int.from_bytes(request[4:6], "big"):
                # A framed management message can share the slave/function
                # prefix. Give all lookahead back to the ordinary reader.
                reader.unread(prefix[1:])
                return False
            size = 5 + prefix[2]
        else:
            size = 8
        prefix += await take(min(size, 8) - len(prefix))
        self.received = prefix
        if _could_be_eybond(prefix):
            raise BinaryFramingError("diagnostic_raw_eybond_ambiguous")
        if len(prefix) < size:
            prefix += await take(size - len(prefix))
        self.received = prefix
        if crc16_modbus(prefix[:-2]) != int.from_bytes(prefix[-2:], "little"):
            raise BinaryFramingError("diagnostic_raw_crc_mismatch")
        if not function[0] & 0x80 and function[0] in (6, 16):
            if prefix[:6] != request[:6]:
                raise BinaryFramingError("diagnostic_raw_write_echo_mismatch")
        claim.future.set_result(prefix)
        return True

    async def send(self, connection, owner: SocketSendOwner, payload: bytes, timeout: float) -> bytes:
        """Read OR write once, under the existing socket request/write locks."""
        read = decode_read_request(payload)
        write = decode_write_request(payload)
        if (read is None and write is None) or not 1 <= payload[0] <= 247:
            raise ValueError("diagnostic_raw_modbus_request_invalid")
        if not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError("diagnostic_request_timeout_invalid")
        reader_task = connection._reader_task
        guard = connection._auxiliary_session
        async with connection._request_lock:
            owner.check(connection)
            if self.closed:
                raise ConnectionError("collector_session_changed")
            if guard.enabled or not guard.at_boundary:
                raise ConnectionError("diagnostic_reader_busy")
            future = asyncio.get_running_loop().create_future()
            claim = ModbusDiagnosticClaim(bytes(payload), future,
                                          asyncio.get_running_loop().time() + timeout)
            self.received = b""
            self.failure_reason = ""
            sent = False
            try:
                async with connection._write_lock:
                    owner.check(connection)
                    if guard.enabled or not guard.at_boundary:
                        raise ConnectionError("diagnostic_reader_busy")
                    self.claim = claim
                    sent = True
                    owner.writer.write(payload)
                    await asyncio.wait_for(owner.writer.drain(), connection._write_timeout)
                response = await asyncio.wait_for(future, max(
                    0.001, claim.deadline - asyncio.get_running_loop().time(),
                ))
                owner.check_reply(connection)
                return response
            except BaseException:
                if sent:
                    # Synchronous fencing, even on cancellation. Never close a
                    # successor through mutable connection.disconnect().
                    self.close()
                    guard.close()
                    owner.writer.close()
                    if reader_task is not None:
                        reader_task.cancel()
                raise
            finally:
                if self.claim is claim:
                    self.claim = None
                finish_request_future(future)
