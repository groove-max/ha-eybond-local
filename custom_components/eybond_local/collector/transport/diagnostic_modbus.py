"""Request-bound RTU replies and explicit diagnostics on a mixed stream.

The decoder is shared with typed runtime raw Modbus requests; diagnostic
dispatch is never inferred by discovery or a permanent wire/profile choice. The
normal reader remains the ONLY reader. An uncertain transaction fences its
physical socket because RTU has no transaction id with which to reject a late
reply. No endpoint, UART/bootstrap or reconnect command is sent here.
"""

from __future__ import annotations

import asyncio

from ...payload.modbus import crc16_modbus, decode_read_request, decode_write_request
from ..protocol import decode_header
from .binary_framing import BinaryFramingError, runtime_eybond_header_error
from .diagnostic_raw import RawDiagnosticClaim as ModbusDiagnosticClaim, RawDiagnosticSession


def _could_be_eybond(prefix: bytes) -> bool:
    """Reject an actual overlapping header, without reading past an RTU reply.

    Short RTU replies (5/7 bytes) are admitted by the explicitly requested
    raw grammar, exact slave/function/length and CRC. Reading an eighth byte
    would consume the NEXT message or deadlock a legitimate short reply.
    This requires a dispatched, typed request, never a wire-detection heuristic.
    """
    if len(prefix) >= 8:
        return not runtime_eybond_header_error(decode_header(prefix[:8]))
    return False


class DiagnosticModbusSession(RawDiagnosticSession):
    """Lifetime belongs to one run() generation, not to a reusable facade."""

    def __init__(
        self, *, strict_write_echo: bool = True,
        reason_prefix: str = "diagnostic_raw",
    ) -> None:
        super().__init__()
        # Runtime raw RTU shares this request-bound decoder, but legacy FC06
        # acknowledgements may return a status value instead of the written one.
        # Explicit diagnostics retain their stricter echo requirement.
        self.strict_write_echo = strict_write_echo
        self.reason_prefix = reason_prefix

    def _error(self, reason: str) -> BinaryFramingError:
        return BinaryFramingError(f"{self.reason_prefix}_{reason}")

    async def consume(self, reader, first: bytes, guard, on_delivery=None) -> bool:
        """Consume a matching RTU boundary; return all lookahead otherwise.

        ``on_delivery`` runs only for an owned reply that passed CRC and echo
        checks, immediately before the waiter is resolved.
        """
        claim = self.claim
        if claim is None or claim.future.done() or self.closed:
            return False
        request = claim.request
        if first != request[:1]:
            return False

        async def take(size):
            remaining = claim.deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise self._error("frame_timeout")
            try:
                data = await guard.read(asyncio.wait_for(reader.readexactly(size), remaining))
            except asyncio.TimeoutError as exc:
                raise self._error("frame_timeout") from exc
            if self.closed or self.claim is not claim or claim.future.done():
                raise self._error("response_unowned")
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
            raise self._error("eybond_ambiguous")
        if len(prefix) < size:
            prefix += await take(size - len(prefix))
        self.received = prefix
        if crc16_modbus(prefix[:-2]) != int.from_bytes(prefix[-2:], "little"):
            raise self._error("crc_mismatch")
        if not function[0] & 0x80 and function[0] in (6, 16):
            echo_size = 4 if function[0] == 6 and not self.strict_write_echo else 6
            if prefix[:echo_size] != request[:echo_size]:
                raise self._error("write_echo_mismatch")
        if on_delivery is not None:
            on_delivery()
        claim.future.set_result(prefix)
        return True

    def validate_request(self, payload: bytes) -> None:
        """Accept one addressed RTU read or write before acquiring the socket."""
        read = decode_read_request(payload)
        write = decode_write_request(payload)
        if (read is None and write is None) or not 1 <= payload[0] <= 247:
            raise ValueError("diagnostic_raw_modbus_request_invalid")
