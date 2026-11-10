"""Explicit CRC-protected PI30 on a socket, independent of its normal wire."""

from __future__ import annotations

import asyncio

from ...payload.pi30 import Pi30Error, build_request, parse_response
from ..protocol import decode_header
from .binary_framing import BinaryFramingError, runtime_eybond_header_error
from .diagnostic_raw import RawDiagnosticSession


MAX_PI30_DIAGNOSTIC_FRAME = 1024


class DiagnosticPi30Session(RawDiagnosticSession):
    """A single outstanding PI30 frame, consumed only by the normal reader.

    CRC and request ownership do not prove a model or cloud compatibility.
    PI30 has no transaction ID; an uncertain exchange fences this socket.
    """

    def __init__(self) -> None:
        super().__init__()
        self.framed_header_pending = False

    @staticmethod
    def validate_request(payload: bytes) -> None:
        body = payload[:-3]
        if (not body or len(payload) > MAX_PI30_DIAGNOSTIC_FRAME
                or any(byte < 0x20 or byte > 0x7e for byte in body)
                or build_request(body.decode("ascii")) != payload):
            raise ValueError("diagnostic_raw_pi30_request_invalid")

    def _error(self, reason: str) -> BinaryFramingError:
        self.failure_reason = f"diagnostic_raw_pi30_{reason}"
        return BinaryFramingError(self.failure_reason)

    async def consume(self, reader, first: bytes, guard, on_delivery=None) -> bool:
        self.framed_header_pending = False
        claim = self.claim
        if claim is None or claim.future.done() or self.closed or first != b"(":
            return False
        frame = bytearray(first)
        self.received = bytes(frame)
        short_error = ""
        while len(frame) < MAX_PI30_DIAGNOSTIC_FRAME:
            remaining = claim.deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise self._error(short_error or "frame_timeout")
            try:
                byte = await guard.read(asyncio.wait_for(reader.readexactly(1), remaining))
            except asyncio.TimeoutError as exc:
                raise self._error(short_error or "frame_timeout") from exc
            except asyncio.IncompleteReadError as exc:
                raise self._error("frame_incomplete") from exc
            if self.closed or self.claim is not claim or claim.future.done():
                raise self._error("response_unowned")
            frame.extend(byte)
            self.received = bytes(frame)
            if len(frame) == 8 and not runtime_eybond_header_error(decode_header(frame)):
                # A real framed management message can share the '(' TID byte.
                # Hand the complete header back, without treating it as PI30.
                reader.unread(bytes(frame[1:]))
                self.framed_header_pending = True
                self.received = b""
                return False
            if len(frame) >= 8 and short_error:
                raise self._error(short_error)
            if byte == b"\r":
                try:
                    parse_response(bytes(frame))
                except Pi30Error as exc:
                    if len(frame) < 8:
                        # CR may be a binary TID/header byte. Only a valid
                        # short PI30 reply may win before the full header is
                        # available; otherwise retain bounded lookahead.
                        short_error = str(exc)
                        continue
                    raise self._error(str(exc)) from exc
                if any(byte < 0x20 or byte > 0x7e for byte in frame[1:-3]):
                    raise self._error("invalid_ascii")
                if on_delivery is not None:
                    on_delivery()
                claim.future.set_result(bytes(frame))
                return True
        raise self._error("frame_too_large")
