"""Socket-local correlation for the observed zero-length FC4 no-result envelope.

This is NOT a relaxed binary grammar. Only an aligned eight-byte header matching
a recently transmitted FC4's full route is consumable, once. It can only fail a
request, never return telemetry or acknowledge a write. Timed-out requests retain
a short tombstone so a late no-result reply cannot poison the next transaction.
The #89 PCAPs prove suffixes of 29 or 45 zeros, and seven zeros for a
single-register Modbus input read. Only adjacent boundaries
may consume those exact suffixes, under the original evidence deadline.
There is no zero-byte scanning, guessed length, or TCP-packet-boundary framing.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
import logging
from dataclasses import dataclass, replace
from time import monotonic

from ...payload.modbus import crc16_modbus
from ..protocol import EybondHeader, FC_FORWARD_TO_DEVICE, HEADER_SIZE
from .binary_framing import BinaryFramingError

logger = logging.getLogger(__name__)
_MAX_REQUESTS = 16
_RETENTION_SECONDS = 10.0
_OBSERVED_PADDING_LENGTHS = (29, 45)
_DIAGNOSTIC_PEEK_LIMIT = 256


@dataclass(frozen=True, slots=True)
class EmptyForwardPaddingBoundary:
    """One adjacent boundary, not permission to search for another frame."""

    deadline: float
    tid: int
    devcode: int
    devaddr: int
    consumed_bytes: int = 0
    allow_short_suffix: bool = False


class EmptyForwardReplyWindow:
    """One physical socket's bounded request evidence, never shared on reconnect."""

    def __init__(self) -> None:
        self._requests: OrderedDict[int, EmptyForwardPaddingBoundary] = OrderedDict()
        self._padding_boundary: EmptyForwardPaddingBoundary | None = None

    def take_padding_boundary(self) -> EmptyForwardPaddingBoundary | None:
        """Capture once at the next boundary, including AT/raw/heartbeat reads."""
        boundary, self._padding_boundary = self._padding_boundary, None
        return boundary

    async def read_padding_prefix(
        self, prefix: bytes, boundary: EmptyForwardPaddingBoundary | None,
        readexactly: Callable[[int], Awaitable[bytes]], *, timeout: float,
    ) -> bytes:
        """Return the intact header prefix, or empty after consuming the exact short suffix.

        Seven zeros cannot start a valid (positive-length) EyeBond header. Deal
        with them before waiting for an eighth byte: it belongs to the next
        frame and may not arrive until the next poll.
        """
        if not (boundary and boundary.allow_short_suffix and not boundary.consumed_bytes
                and prefix == bytes(3)):
            return prefix
        # TID wrap + an all-zero route can give another correlated negative
        # FC4 the same first seven bytes. Let the normal 8-byte parser decide
        # rather than stripping part of that reply.
        other = self._requests.get(0)
        if other and other.devcode == other.devaddr == 0 and monotonic() < other.deadline:
            return prefix
        remaining = min(timeout, boundary.deadline - monotonic())
        if remaining <= 0:
            return prefix
        try:
            prefix += await asyncio.wait_for(readexactly(7 - len(prefix)), remaining)
        except asyncio.TimeoutError as exc:
            raise BinaryFramingError("collector_empty_forward_padding_timeout") from exc
        if prefix != bytes(7):
            return prefix
        self._padding_boundary = replace(boundary, consumed_bytes=7)
        return b""

    async def async_consume_padding(
        self, header: bytes, boundary: EmptyForwardPaddingBoundary | None,
        readexactly: Callable[[int], Awaitable[bytes]], *, timeout: float,
        peek_buffered: Callable[[int], bytes] | None = None,
    ) -> bool:
        """Recognize the fixed suffix only; bad/truncated tails close the owner."""
        if boundary is None or header != bytes(HEADER_SIZE):
            return False
        remaining = min(timeout, boundary.deadline - monotonic())
        if remaining <= 0:
            raise BinaryFramingError("collector_empty_forward_padding_timeout")
        if boundary.consumed_bytes == _OBSERVED_PADDING_LENGTHS[-1]:
            # No third segment is authorized. Do not let a generic one-window
            # resync accept more zeros depending on TCP coalescing.
            # Count only buffered bytes; never wait, drain, or log their content.
            follow = peek_buffered(_DIAGNOSTIC_PEEK_LIMIT) if peek_buffered else b""
            zeros = len(follow) - len(follow.lstrip(b"\x00"))
            logger.warning(
                "Excess zero data after correlated empty FC4 tid=%d "
                "devcode=0x%04X devaddr=0x%02X consumed_padding_bytes=%d "
                "additional_zero_prefix_bytes=%d buffered_peek_bytes=%d "
                "peek_limit=%d exact_padding_length=unknown; closing session",
                boundary.tid, boundary.devcode, boundary.devaddr,
                boundary.consumed_bytes, HEADER_SIZE + zeros, len(follow),
                _DIAGNOSTIC_PEEK_LIMIT,
            )
            raise BinaryFramingError("collector_empty_forward_padding_excess")
        target = next(n for n in _OBSERVED_PADDING_LENGTHS if n > boundary.consumed_bytes)
        segment_length = target - boundary.consumed_bytes
        try:
            tail = await asyncio.wait_for(
                readexactly(segment_length - HEADER_SIZE), remaining,
            )
        except asyncio.TimeoutError as exc:
            raise BinaryFramingError("collector_empty_forward_padding_timeout") from exc
        segment = header + tail
        if segment != bytes(segment_length):
            # The first nonzero byte is an observation, not proof that the
            # preceding zeros were padding. Do not read ahead or log payload.
            logger.debug(
                "Invalid suffix after empty FC4 tid=%d consumed_padding_bytes=%d "
                "expected_zero_boundary=%d first_nonzero_offset=%d",
                boundary.tid, boundary.consumed_bytes, target,
                boundary.consumed_bytes + len(segment) - len(segment.lstrip(b"\x00")),
            )
            raise BinaryFramingError("collector_empty_forward_padding_invalid")
        logger.debug(
            "Consumed fixed%d-zero suffix of correlated empty FC4; no inverter result",
            target,
        )
        self._padding_boundary = replace(boundary, consumed_bytes=target)
        return True

    def forget(self, tid: int) -> None:
        self._requests.pop(tid, None)

    def record(self, header: EybondHeader, payload: bytes = b"") -> None:
        # Every newly allocated TID invalidates its previous generation,
        # including reuse by an FC1/FC2/FC3 request or heartbeat.
        self.forget(header.tid)
        if header.fcode != FC_FORWARD_TO_DEVICE:
            return
        single_input_read = (
            len(payload) == 8 and 1 <= payload[0] <= 247 and payload[1] == 4
            and payload[4:6] == b"\x00\x01"
            and crc16_modbus(payload[:6]) == int.from_bytes(payload[6:], "little")
        )
        self._requests[header.tid] = EmptyForwardPaddingBoundary(
            monotonic() + _RETENTION_SECONDS, header.tid, header.devcode, header.devaddr,
            allow_short_suffix=single_input_read,
        )
        while len(self._requests) > _MAX_REQUESTS:
            self._requests.popitem(last=False)

    def consume(
        self, header: EybondHeader,
        pending: dict[int, asyncio.Future[tuple[EybondHeader, bytes]]],
    ) -> bool:
        if header.fcode != FC_FORWARD_TO_DEVICE or header.payload_len != -2:
            return False
        request = self._requests.get(header.tid)
        if request is None:
            return False
        if monotonic() >= request.deadline:
            self.forget(header.tid)
            return False
        # Ordinary valid replies may rewrite route fields. This malformed-wire
        # compatibility exception deliberately requires an exact route match.
        if (header.devcode, header.devaddr) != (request.devcode, request.devaddr):
            return False
        self.forget(header.tid)
        future = pending.get(header.tid)
        active = future is not None and not future.done()
        if active:
            future.set_exception(TimeoutError("collector_empty_forward_response"))
        self._padding_boundary = request
        logger.debug(
            "Consumed correlated empty FC4 envelope tid=%d devcode=0x%04X "
            "devaddr=0x%02X active_request=%s; no inverter result",
            header.tid, header.devcode, header.devaddr, active,
        )
        return True
