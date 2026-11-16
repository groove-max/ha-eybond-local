"""Bounded binary framing, independent of inverter metadata and future liveness.

This codec is an internal building block for the short-ASCII auxiliary channel.
It does not negotiate a collector protocol or admit that channel itself.
The session owner supplies the admitted grammar. A future merely being
alive is never evidence. Delivery ownership stays with the auxiliary session.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
import logging
import math
from typing import Protocol

from ..protocol import (
    HEADER_SIZE, EybondHeader, decode_header,
    FC_HEARTBEAT, FC_QUERY_COLLECTOR, FC_SET_COLLECTOR, FC_FORWARD_TO_DEVICE,
    FC_TRIGGER_QUERY_REAL_TIME, FC_SET_DEVICE_REG, FC_TRIGGER_QUERY_HISTORY,
)


logger = logging.getLogger(__name__)

AABB_MAGIC = b"\xaa\xbb"
AABB_FRAME_SIZE = 21
AABB_SUBTYPES = frozenset({b"\x02\x00", b"\x02\x02"})
AABB_PREFIXES = frozenset(AABB_MAGIC + subtype for subtype in AABB_SUBTYPES)
MAX_EYBOND_PAYLOAD_SIZE = 4096
MAX_BINARY_FRAME_SIZE = HEADER_SIZE + MAX_EYBOND_PAYLOAD_SIZE
RUNTIME_EYBOND_FCODES = frozenset({
    FC_HEARTBEAT, FC_QUERY_COLLECTOR, FC_SET_COLLECTOR, FC_FORWARD_TO_DEVICE,
    FC_TRIGGER_QUERY_REAL_TIME, FC_SET_DEVICE_REG, FC_TRIGGER_QUERY_HISTORY,
})
# function_invalid is fatal. A 7-byte raw RTU prefix plus the next frame's
# first byte is often payload-too-large, not an illegal function. Discarding
# that window on length alone adopts a misaligned header and stalls on its
# payload (#39). Length and payload-size errors may drop one window only when
# the whole next frame is already inside the same feed() call. The production
# reader never passes bytes it has not read into that decision, so an illegal
# header that does not start with ``aa bb`` closes even when the next frame
# is already buffered.
# Desynchronized length junk (e.g. 000f02ff0000ff04): drop the entire illegal
# 8-byte window once, then read a fresh header. Do not one-byte slide (that
# forms false-legal headers such as ff0000ff04aabb02). Ambiguous AABB overlap
# and AABB checksum stay fatal.
RESYNCABLE_HEADER_REASONS = frozenset({
    "collector_frame_length_invalid",
    "collector_frame_payload_too_large",
})


def runtime_eybond_header_error(header: EybondHeader) -> str:
    """The existing runtime header contract, shared by all binary readers.

    Mechanical decode_header stays permissive for transparent cloud tooling.
    The supported function set, size limits and failure reasons are unchanged.
    """

    if header.payload_len < 0:
        return "collector_frame_length_invalid"
    if header.payload_len > MAX_EYBOND_PAYLOAD_SIZE:
        return "collector_frame_payload_too_large"
    if header.fcode not in RUNTIME_EYBOND_FCODES:
        return "collector_frame_function_invalid"
    return ""


def is_resyncable_header_error(reason: str) -> bool:
    """True when an illegal EyeBond header may be skipped with one whole-window discard."""

    return reason in RESYNCABLE_HEADER_REASONS

class BinaryGrammar(Enum):
    """A session's allowed binary grammars, NOT a request-waiter preference."""

    EYBOND = "eybond"
    AABB = "aabb"
    MIXED = "mixed"


class BinaryFramingError(ValueError):
    """An invalid, incomplete or ambiguous boundary requires session recovery."""


@dataclass(frozen=True, slots=True)
class BinaryFrame:
    """One structural frame; interpreting payload values is a separate concern."""

    grammar: BinaryGrammar
    wire: bytes
    header: EybondHeader | None = None


def validate_aabb_frame(wire: bytes) -> None:
    """Validate one exact 21-byte MPPT reply, including its additive checksum.

    The checksum covers bytes2..19; the final byte is the sum modulo256.
    Both observed reply subtypes share framing, not necessarily field semantics.
    """

    if len(wire) != AABB_FRAME_SIZE:
        raise BinaryFramingError("aabb_length_invalid")
    if not wire.startswith(AABB_MAGIC):
        raise BinaryFramingError("aabb_magic_invalid")
    if wire[2:4] not in AABB_SUBTYPES:
        raise BinaryFramingError("aabb_subtype_unsupported")
    if sum(wire[2:-1]) & 0xFF != wire[-1]:
        raise BinaryFramingError("aabb_checksum_invalid")


class BinaryFrameDecoder:
    """Incrementally decode ONE frame with a fixed deadline and bounded buffer.

    ``feed`` returns bytes consumed from this chunk. Any suffix belongs to the
    caller and must be processed independently (possibly as an AT line).
    The buffer, grammar and deadline never reset on fragmentation or retry.

    A single ``feed()`` call that already contains the next complete frame
    keeps the same-chunk resync: an illegal EyeBond length or
    payload-too-large header that does not start with ``aa bb`` drops that
    8-byte window once. On ``MIXED``, the next frame in that same call is
    decoded unless its eight bytes are a legal EyeBond header and an AABB
    prefix, in which case the read closes with ``binary_frame_ambiguous``.
    On ``EYBOND``, that next frame is decoded.
    ``collector_frame_function_invalid`` is fatal and is never discarded.
    Skipped bytes are never published. A second illegal window that does not
    start with ``aa bb`` closes.
    Ambiguous AABB/EyeBond overlap and a bad AABB checksum stay terminal. A
    later ``feed()`` is not part of that decision, so a split read closes on
    the illegal header that does not start with ``aa bb``.
    ``async_read_binary_frame`` feeds only bytes it has already read. It
    never relies on bytes that have not been read yet, so an illegal header
    that does not start with ``aa bb`` closes there whether or not later
    bytes are already buffered.

    On ``MIXED``, eight bytes that are a legal EyeBond header and an AABB
    prefix are ``binary_frame_ambiguous``. The declared tail is not consulted,
    whether that tail is already buffered, arrives during the read, or arrives
    after the reader returns. Bytes that start with ``aa bb`` and are not a
    legal EyeBond header still select the 21-byte reply; the checksum is
    checked once those bytes arrive. A future merely being alive is never a
    grammar claim.
    """

    def __init__(
        self,
        grammar: BinaryGrammar,
        *,
        started_at: float,
        timeout: float,
    ) -> None:
        if not isinstance(grammar, BinaryGrammar):
            raise ValueError("binary_grammar_invalid")
        if not math.isfinite(started_at) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("binary_deadline_invalid")
        self._grammar = grammar
        self._last_now = started_at
        self._deadline = started_at + timeout
        if not math.isfinite(self._deadline):
            raise ValueError("binary_deadline_invalid")
        self._buffer = bytearray()
        self._header: EybondHeader | None = None
        self._size = 0
        self._kind: BinaryGrammar | None = None
        self._error = ""
        self._result: BinaryFrame | None = None
        self._illegal_header_discarded = False
        self._discarded_header_hex = ""

    @property
    def buffered_size(self) -> int:
        return len(self._buffer)

    @property
    def frame(self) -> BinaryFrame | None:
        return self._result

    @property
    def deadline(self) -> float:
        return self._deadline

    @property
    def bytes_needed(self) -> int:
        """Exact next read size: never speculate beyond this frame boundary."""

        if self._error or self._result is not None:
            return 0
        return (self._size or HEADER_SIZE) - len(self._buffer)

    def _fail(self, reason: str) -> None:
        self._error = reason
        raise BinaryFramingError(reason)

    def _check(self, now: float) -> None:
        if self._error:
            raise BinaryFramingError(self._error)
        if not math.isfinite(now):
            self._fail("binary_clock_invalid")
        if now < self._last_now:
            self._fail("binary_clock_regressed")
        self._last_now = now
        if self._result is None and now >= self._deadline:
            self._fail("binary_frame_timeout")

    def _discard_illegal_header(self, reason: str) -> None:
        """Drop the entire illegal 8-byte window; never adopt its payload size.

        At most one discard per frame attempt. The next candidate must be a
        fresh 8-byte header — never a mix of discarded bytes and following bytes.
        """

        if self._illegal_header_discarded:
            self._fail(reason)
        discarded = bytes(self._buffer[:HEADER_SIZE])
        self._discarded_header_hex = discarded.hex()
        logger.warning(
            "Discarded illegal EyeBond header hex=%s reason=%s",
            self._discarded_header_hex,
            reason,
        )
        del self._buffer[:HEADER_SIZE]
        self._illegal_header_discarded = True
        self._header = None
        self._kind = None
        self._size = 0

    def _maybe_discard_illegal_header(self, header_error: str, follow: bytes) -> bool:
        """Discard one illegal window only when this feed() call holds the next frame."""

        if not is_resyncable_header_error(header_error):
            return False
        if len(follow) < HEADER_SIZE:
            return False
        next_header = decode_header(follow)
        if runtime_eybond_header_error(next_header):
            return False
        # Header plus that header's payload. A raw RTU prefix can decode as a
        # legal function with a multi-kilobyte length; do not adopt it.
        if len(follow) < next_header.total_len:
            return False
        self._discard_illegal_header(header_error)
        return True

    def _note_discard_recovered(self, header: EybondHeader) -> None:
        """One WARNING so a live log can prove the discard landed on a real frame."""

        if not self._illegal_header_discarded:
            return
        logger.warning(
            "Illegal EyeBond header discard recovered discarded=%s "
            "next_tid=%d next_devcode=0x%04X next_fc=%d next_payload=%d",
            self._discarded_header_hex,
            header.tid,
            header.devcode,
            header.fcode,
            header.payload_len,
        )

    def _select_boundary(self, *, follow: bytes) -> None:
        wire = bytes(self._buffer)
        header = decode_header(wire)
        header_error = runtime_eybond_header_error(header)
        auxiliary_prefix = wire[:4] in AABB_PREFIXES

        if self._grammar is BinaryGrammar.EYBOND:
            if header_error:
                if self._maybe_discard_illegal_header(header_error, follow):
                    return
                self._fail(header_error)
            self._kind = BinaryGrammar.EYBOND
            self._header = header
            self._size = header.total_len
            self._note_discard_recovered(header)
        elif self._grammar is BinaryGrammar.AABB:
            # Integrity is checked once the entire bounded21-byte reply arrives.
            self._kind = BinaryGrammar.AABB
            self._size = AABB_FRAME_SIZE
        elif auxiliary_prefix and not header_error:
            # Both grammars accept these eight bytes. Choosing 21 bytes would
            # depend on whether the declared tail has arrived. Close instead.
            self._fail("binary_frame_ambiguous")
        elif not header_error:
            self._kind = BinaryGrammar.EYBOND
            self._header = header
            self._size = header.total_len
            self._note_discard_recovered(header)
        elif wire.startswith(AABB_MAGIC):
            self._kind = BinaryGrammar.AABB
            self._size = AABB_FRAME_SIZE
        elif self._maybe_discard_illegal_header(header_error, follow):
            return
        else:
            self._fail(header_error)

    def feed(self, data: bytes, *, now: float) -> int:
        """Consume only this frame's bytes, or fail without publishing a frame."""

        self._check(now)
        if self._result is not None:
            return 0
        consumed = 0
        while consumed < len(data):
            target = self._size or HEADER_SIZE
            take = min(target - len(self._buffer), len(data) - consumed)
            self._buffer.extend(data[consumed:consumed + take])
            consumed += take
            if not self._size and len(self._buffer) == HEADER_SIZE:
                self._select_boundary(follow=data[consumed:])
            if self._size and len(self._buffer) == self._size:
                wire = bytes(self._buffer)
                if self._kind is BinaryGrammar.AABB:
                    try:
                        validate_aabb_frame(wire)
                    except BinaryFramingError as exc:
                        self._fail(str(exc))
                assert self._kind is not None
                self._result = BinaryFrame(self._kind, wire, self._header)
                break
        return consumed

    def finish(self, *, now: float) -> BinaryFrame:
        """Finish at EOF; partial data never becomes a shorter successful frame."""

        self._check(now)
        if self._result is None:
            self._fail("binary_frame_truncated")
        assert self._result is not None
        return self._result


class BinaryFrameReader(Protocol):
    """The socket owner remains the only reader; no background tasks are added."""

    async def readexactly(self, size: int) -> bytes:
        ...


async def async_read_binary_frame(
    reader: BinaryFrameReader,
    *,
    prefix: bytes,
    grammar: BinaryGrammar,
    started_at: float,
    timeout: float,
) -> BinaryFrame:
    """Read the rest of a frame under its ORIGINAL first-byte deadline.

    Supply at most the fixed8-byte header as prefix. Cancellation is propagated
    to the owner: this function never retries or reuses a partly consumed stream.
    It must not be called by competing readers or after a guessed grammar switch.
    On ``MIXED``, eight bytes that are a legal EyeBond header and an AABB
    prefix are ``binary_frame_ambiguous``. This function does not wait for the
    declared EyeBond length. Bytes that start with ``aa bb`` and are not a
    legal EyeBond header still select the 21-byte reply. An illegal EyeBond
    length or payload-too-large header that does not start with ``aa bb``
    closes. Each ``feed()`` here contains
    only bytes already read, so this reader does not use the decoder's
    same-chunk resync: an illegal header that does not start with ``aa bb``
    closes even when the next frame is already buffered.
    Do not infer a grammar from ``future.done()``.
    """

    if not 0 < len(prefix) <= HEADER_SIZE:
        raise ValueError("binary_prefix_length_invalid")
    loop = asyncio.get_running_loop()
    decoder = BinaryFrameDecoder(grammar, started_at=started_at, timeout=timeout)
    decoder.feed(prefix, now=loop.time())
    while decoder.frame is None:
        now = loop.time()
        decoder.feed(b"", now=now)
        try:
            tail = await asyncio.wait_for(
                reader.readexactly(decoder.bytes_needed),
                timeout=decoder.deadline - now,
            )
        except asyncio.TimeoutError as exc:
            raise BinaryFramingError("binary_frame_timeout") from exc
        except asyncio.IncompleteReadError as exc:
            decoder.feed(exc.partial, now=loop.time())
            return decoder.finish(now=loop.time())
        decoder.feed(tail, now=loop.time())
    return decoder.finish(now=loop.time())
