"""Bounded binary framing, independent of inverter metadata and future liveness.

This codec is an internal building block for the short-ASCII auxiliary channel.
It does not negotiate a collector protocol or admit that channel itself.
The session owner supplies the admitted grammar and any outstanding
AuxiliaryReadClaim snapshot. A future merely being alive is never evidence.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
import math
from typing import Protocol

from ..protocol import (
    HEADER_SIZE, EybondHeader, decode_header,
    FC_HEARTBEAT, FC_QUERY_COLLECTOR, FC_SET_COLLECTOR, FC_FORWARD_TO_DEVICE,
    FC_TRIGGER_QUERY_REAL_TIME, FC_SET_DEVICE_REG, FC_TRIGGER_QUERY_HISTORY,
)


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


class BinaryGrammar(Enum):
    """A session's allowed binary grammars, NOT a request-waiter preference."""

    EYBOND = "eybond"
    AABB = "aabb"
    MIXED = "mixed"


class BinaryFramingError(ValueError):
    """An invalid, incomplete or ambiguous boundary requires session recovery."""


class AuxiliaryBoundaryClaim(Protocol):
    """Ownership snapshot for MIXED overlap selection: subtype only.

    Callers pass the AuxiliaryReadClaim captured when the frame started.
    Do not infer intent from future.done() or waiter lifetime.
    """

    subtype: bytes


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
    An error is terminal: the caller must not guess a boundary and continue.

    MIXED refuses EyeBond/AABB overlaps unless an outstanding claim's subtype
    matches ``wire[2:4]``. A valid checksum alone is never enough, and a
    future merely being alive is never a grammar claim. With a matching claim,
    choose AABB then ``validate_aabb_frame``; checksum failure stays a reject.
    """

    def __init__(
        self,
        grammar: BinaryGrammar,
        *,
        started_at: float,
        timeout: float,
        auxiliary_claim: AuxiliaryBoundaryClaim | None = None,
    ) -> None:
        if not isinstance(grammar, BinaryGrammar):
            raise ValueError("binary_grammar_invalid")
        if not math.isfinite(started_at) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("binary_deadline_invalid")
        self._grammar = grammar
        self._auxiliary_claim = auxiliary_claim
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

    def _select_boundary(self) -> None:
        wire = bytes(self._buffer)
        header = decode_header(wire)
        header_error = runtime_eybond_header_error(header)
        auxiliary_prefix = wire[:4] in AABB_PREFIXES

        if self._grammar is BinaryGrammar.EYBOND:
            if header_error:
                self._fail(header_error)
            self._kind = BinaryGrammar.EYBOND
            self._header = header
            self._size = header.total_len
        elif self._grammar is BinaryGrammar.AABB:
            # Integrity is checked once the entire bounded21-byte reply arrives.
            self._kind = BinaryGrammar.AABB
            self._size = AABB_FRAME_SIZE
        elif auxiliary_prefix and not header_error:
            claim = self._auxiliary_claim
            if claim is not None and wire[2:4] == claim.subtype:
                # Outstanding claim subtype matches: choose AABB, then validate.
                self._kind = BinaryGrammar.AABB
                self._size = AABB_FRAME_SIZE
            else:
                # No claim or subtype mismatch: fail-close. Do not read a
                # speculative longer EyeBond tail.
                self._fail("binary_frame_ambiguous")
        elif not header_error:
            self._kind = BinaryGrammar.EYBOND
            self._header = header
            self._size = header.total_len
        elif wire.startswith(AABB_MAGIC):
            self._kind = BinaryGrammar.AABB
            self._size = AABB_FRAME_SIZE
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
                self._select_boundary()
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
    auxiliary_claim: AuxiliaryBoundaryClaim | None = None,
) -> BinaryFrame:
    """Read the rest of a frame under its ORIGINAL first-byte deadline.

    Supply at most the fixed8-byte header as prefix. Cancellation is propagated
    to the owner: this function never retries or reuses a partly consumed stream.
    It must not be called by competing readers or after a guessed grammar switch.
    Pass the claim snapshot from when the frame started; do not infer from
    future.done().
    """

    if not 0 < len(prefix) <= HEADER_SIZE:
        raise ValueError("binary_prefix_length_invalid")
    loop = asyncio.get_running_loop()
    decoder = BinaryFrameDecoder(
        grammar,
        started_at=started_at,
        timeout=timeout,
        auxiliary_claim=auxiliary_claim,
    )
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
