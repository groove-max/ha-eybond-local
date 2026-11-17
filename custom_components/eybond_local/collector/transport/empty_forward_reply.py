"""Socket-local correlation for the observed zero-length FC4 no-result envelope.

This is NOT a relaxed binary grammar. Only an aligned eight-byte header matching
a recently transmitted FC4's full route is consumable, once. It can only fail a
request, never return telemetry or acknowledge a write. Timed-out requests retain
a short tombstone so a late no-result reply cannot poison the next transaction.
The #89 PCAP also proves a fixed29-zero suffix. Only the immediately following
boundary may consume this exact suffix, under the original evidence deadline.
There is no zero-byte scanning, guessed length, or TCP-packet-boundary framing.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
import logging
from dataclasses import dataclass, replace
from time import monotonic

from ..protocol import EybondHeader, FC_FORWARD_TO_DEVICE, HEADER_SIZE
from .binary_framing import BinaryFramingError

logger = logging.getLogger(__name__)
_MAX_REQUESTS = 16
_RETENTION_SECONDS = 10.0
_OBSERVED_PADDING = bytes(29)
_DIAGNOSTIC_PEEK_LIMIT = 256


@dataclass(frozen=True, slots=True)
class EmptyForwardPaddingBoundary:
    """One adjacent boundary, not permission to search for another frame."""

    deadline: float
    tid: int
    devcode: int
    devaddr: int
    consumed: bool = False


class EmptyForwardReplyWindow:
    """One physical socket's bounded request evidence, never shared on reconnect."""

    def __init__(self) -> None:
        self._requests: OrderedDict[int, tuple[int, int, float]] = OrderedDict()
        self._padding_boundary: EmptyForwardPaddingBoundary | None = None

    def take_padding_boundary(self) -> EmptyForwardPaddingBoundary | None:
        """Capture once at the next boundary, including AT/raw/heartbeat reads."""
        boundary, self._padding_boundary = self._padding_boundary, None
        return boundary

    async def async_consume_padding(
        self, header: bytes, boundary: EmptyForwardPaddingBoundary | None,
        readexactly: Callable[[int], Awaitable[bytes]], *, timeout: float,
        peek_buffered: Callable[[int], bytes] | None = None,
    ) -> bool:
        """Recognize the fixed suffix only; bad/truncated tails close the owner."""
        if boundary is None or header != _OBSERVED_PADDING[:HEADER_SIZE]:
            return False
        remaining = min(timeout, boundary.deadline - monotonic())
        if remaining <= 0:
            raise BinaryFramingError("collector_empty_forward_padding_timeout")
        if boundary.consumed:
            # #89's later log proves MORE than 29 zeros, not the exact length.
            # Do not let the framed reader's one-window resync turn 29+8 zeros
            # into a valid response merely because TCP coalesced the next frame.
            # Count only buffered bytes; never wait, drain, or log their content.
            follow = peek_buffered(_DIAGNOSTIC_PEEK_LIMIT) if peek_buffered else b""
            zeros = len(follow) - len(follow.lstrip(b"\x00"))
            logger.warning(
                "Excess zero data after correlated empty FC4 tid=%d "
                "devcode=0x%04X devaddr=0x%02X consumed_padding_bytes=%d "
                "additional_zero_prefix_bytes=%d buffered_peek_bytes=%d "
                "peek_limit=%d exact_padding_length=unknown; closing session",
                boundary.tid, boundary.devcode, boundary.devaddr,
                len(_OBSERVED_PADDING), HEADER_SIZE + zeros, len(follow),
                _DIAGNOSTIC_PEEK_LIMIT,
            )
            raise BinaryFramingError("collector_empty_forward_padding_excess")
        try:
            tail = await asyncio.wait_for(
                readexactly(len(_OBSERVED_PADDING) - HEADER_SIZE), remaining,
            )
        except asyncio.TimeoutError as exc:
            raise BinaryFramingError("collector_empty_forward_padding_timeout") from exc
        if header + tail != _OBSERVED_PADDING:
            raise BinaryFramingError("collector_empty_forward_padding_invalid")
        logger.debug("Consumed fixed29-zero suffix of correlated empty FC4; no inverter result")
        self._padding_boundary = replace(boundary, consumed=True)
        return True

    def forget(self, tid: int) -> None:
        self._requests.pop(tid, None)

    def record(self, header: EybondHeader) -> None:
        # Every newly allocated TID invalidates its previous generation,
        # including reuse by an FC1/FC2/FC3 request or heartbeat.
        self.forget(header.tid)
        if header.fcode != FC_FORWARD_TO_DEVICE:
            return
        self._requests[header.tid] = (
            header.devcode, header.devaddr, monotonic() + _RETENTION_SECONDS,
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
        devcode, address, expires = request
        if monotonic() >= expires:
            self.forget(header.tid)
            return False
        # Ordinary valid replies may rewrite route fields. This malformed-wire
        # compatibility exception deliberately requires an exact route match.
        if (header.devcode, header.devaddr) != (devcode, address):
            return False
        self.forget(header.tid)
        future = pending.get(header.tid)
        active = future is not None and not future.done()
        if active:
            future.set_exception(TimeoutError("collector_empty_forward_response"))
        self._padding_boundary = EmptyForwardPaddingBoundary(
            expires, header.tid, header.devcode, header.devaddr,
        )
        logger.debug(
            "Consumed correlated empty FC4 envelope tid=%d devcode=0x%04X "
            "devaddr=0x%02X active_request=%s; no inverter result",
            header.tid, header.devcode, header.devaddr, active,
        )
        return True
