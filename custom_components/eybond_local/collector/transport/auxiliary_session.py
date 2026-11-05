"""Socket-scoped ownership for the documented read-only auxiliary exchange.

This internal channel is not a new transport or a model detector. Admission is
explicit; normal framed/AT sessions never infer it from incoming magic bytes.
The two read queries have no transaction identifier. Therefore an interrupted
exchange fences its physical socket before another request can use that stream.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math
from typing import Awaitable, TypeVar

from .binary_framing import BinaryFrame, BinaryFramingError, BinaryGrammar
from .common import _cancel_and_join_task, _close_writer_bounded

_T = TypeVar("_T")

_READ_QUERIES = {
    b"\x5a\xa5\x02\x00" + bytes(16) + b"\x02": b"\x02\x00",
    b"\x5a\xa5\x02\x02" + bytes(16) + b"\x04": b"\x02\x02",
}


@dataclass(frozen=True, slots=True)
class AuxiliaryReadClaim:
    """Delivery ownership captured before assembling the first response byte."""

    subtype: bytes
    future: asyncio.Future[bytes]


class AuxiliaryReadSession:
    """One physical session, independent of caller future lifetime.

    MIXED overlap selection uses the outstanding AuxiliaryReadClaim subtype,
    never future.done() or checksum preference alone. Matching claim plus AABB
    validation admits the 21-byte reply; no claim or subtype mismatch on an
    EyeBond/AABB overlap stays ``binary_frame_ambiguous`` (session-fatal).
    """

    def __init__(self) -> None:
        self.enabled = False
        self.at_boundary = True
        self.closed = False
        self.claim: AuxiliaryReadClaim | None = None

    def close(self) -> None:
        self.closed = True
        if self.claim is not None and not self.claim.future.done():
            self.claim.future.set_exception(ConnectionError("collector_disconnected"))

    async def read(self, operation: Awaitable[_T]) -> _T:
        """Fence parser awaits against this physical session's retirement.

        Cancellation alone is insufficient: a completed read/wait_for may win
        that race. Check after the ENTIRE parser wait (including its timeout),
        before bytes or errors can update connection-wide state. This lifetime
        guard also applies when the optional auxiliary grammar is disabled.
        """

        try:
            return await operation
        finally:
            if self.closed:
                raise asyncio.CancelledError

    def accept(self, frame: BinaryFrame, claim: AuxiliaryReadClaim | None) -> None:
        """Deliver only to the owner present when this frame started arriving."""

        if frame.grammar is not BinaryGrammar.AABB:
            raise ValueError("auxiliary_frame_grammar_invalid")
        if (
            self.closed or claim is None or claim is not self.claim
            or claim.future.done()
        ):
            raise BinaryFramingError("aabb_response_unowned")
        if frame.wire[2:4] != claim.subtype:
            raise BinaryFramingError("aabb_response_subtype_mismatch")
        claim.future.set_result(frame.wire)

    async def send(
        self,
        payload: bytes,
        *,
        writer: asyncio.StreamWriter,
        reader_task: asyncio.Task[None] | None,
        request_lock: asyncio.Lock,
        write_lock: asyncio.Lock,
        write_timeout: float,
        request_timeout: float,
    ) -> bytes:
        """Send an exact documented read, never an arbitrary auxiliary write.

        The writer and session are pinned BEFORE waiting for either lock. A
        replaced session is closed before the successor becomes visible, so a
        waiting operation cannot migrate to that successor accidentally.
        """

        subtype = _READ_QUERIES.get(payload)
        if subtype is None:
            raise ValueError("auxiliary_read_query_unsupported")
        if not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError("auxiliary_request_timeout_invalid")
        async with request_lock:
            if self.closed or writer.is_closing():
                raise ConnectionError("collector_not_connected")
            if not self.at_boundary:
                raise ConnectionError("auxiliary_frame_in_progress")
            if self.claim is not None:
                raise RuntimeError("auxiliary_request_already_pending")
            self.enabled = True
            future = asyncio.get_running_loop().create_future()
            claim = AuxiliaryReadClaim(subtype, future)
            sent = False
            try:
                async with write_lock:
                    if self.closed or writer.is_closing():
                        raise ConnectionError("collector_not_connected")
                    # Receipt could have begun while waiting for the write lock.
                    if not self.at_boundary:
                        raise ConnectionError("auxiliary_frame_in_progress")
                    self.claim = claim
                    sent = True  # write() itself can fail after a partial write.
                    writer.write(payload)
                    await asyncio.wait_for(writer.drain(), timeout=write_timeout)
                return await asyncio.wait_for(future, timeout=request_timeout)
            except BaseException:
                if sent:
                    # Synchronous fencing survives repeated cancellation. Never
                    # disconnect through mutable connection state: it may now
                    # point to a newer socket owned by another run() generation.
                    self.close()
                    writer.close()
                    if reader_task is not None:
                        reader_task.cancel()
                    await _close_writer_bounded(writer)
                    if reader_task is not None:
                        await _cancel_and_join_task(reader_task)
                raise
            finally:
                if self.claim is claim:
                    self.claim = None
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    # A disconnect during drain may fail a future we have not
                    # awaited yet. Retrieve it without changing the raised error.
                    future.exception()
