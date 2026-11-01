"""Structural framing regressions for the auxiliary-channel implementation.

All wire values are synthetic. These tests do not enable the new grammar on a
collector or validate an inverter model, sensor mapping, or real cloud session.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.protocol import EybondHeader, build_collector_request
from custom_components.eybond_local.collector.transport.binary_framing import (
    AABB_FRAME_SIZE,
    MAX_BINARY_FRAME_SIZE,
    BinaryFrameDecoder,
    BinaryFramingError,
    BinaryGrammar,
    async_read_binary_frame,
    runtime_eybond_header_error,
    validate_aabb_frame,
)


def _checksum(wire: bytes) -> bytes:
    return wire[:-1] + bytes([sum(wire[2:-1]) & 0xFF])


def _claim(subtype: bytes = b"\x02\x00"):
    """Claim snapshot for decoder tests; only subtype is consulted."""

    return SimpleNamespace(subtype=subtype)


def _auxiliary(
    *,
    subtype: bytes = b"\x02\x00",
    collision: bool = False,
    voltage: int = 1085,
    power: int | None = None,
) -> bytes:
    data = bytearray(21)
    data[:4] = b"\xaa\xbb" + subtype
    data[4:6] = voltage.to_bytes(2, "big")
    if power is not None:
        data[6:8] = power.to_bytes(2, "big")
    else:
        data[7] = 4 if collision else 70
    return _checksum(bytes(data))


def _collision_0200() -> bytes:
    """Checksum-valid AABB/0200 that is also a legal EyeBond header (PV 113.1 V, 20 W)."""

    return _auxiliary(voltage=1131, power=2)


def _framed(*, tid: int = 0xAABB, devcode: int = 0x0200, size: int = 20) -> bytes:
    return build_collector_request(
        tid, bytes(size), devcode=devcode, collector_addr=0xFF, fcode=4,
    )


class BinaryFrameDecoderTests(unittest.TestCase):
    def _decoder(self, grammar=BinaryGrammar.MIXED, *, timeout=1.0, claim=None):
        return BinaryFrameDecoder(
            grammar, started_at=10.0, timeout=timeout, auxiliary_claim=claim,
        )

    def test_exact_checksum_and_both_subtypes(self) -> None:
        for subtype in (b"\x02\x00", b"\x02\x02"):
            with self.subTest(subtype=subtype):
                wire = _auxiliary(subtype=subtype)
                validate_aabb_frame(wire)
                decoder = self._decoder()
                self.assertEqual(decoder.feed(wire, now=10.1), 21)
                frame = decoder.finish(now=10.1)
                self.assertEqual(frame.grammar, BinaryGrammar.AABB)
                self.assertEqual(frame.wire, wire)
                self.assertIsNone(frame.header)

    def test_existing_header_contract_remains_identical_at_every_function(self) -> None:
        from custom_components.eybond_local.collector.transport.common import (
            _runtime_eybond_header_error,
        )
        for wire_len in (0, 1, 2, 3, 4098, 4099, 65535):
            for function in range(256):
                header = EybondHeader(0xAABB, 0x0200, wire_len, 0xFF, function)
                # Expected behavior recorded from the pre-extraction implementation.
                if wire_len < 2:
                    reason = "collector_frame_length_invalid"
                elif wire_len > 4098:
                    reason = "collector_frame_payload_too_large"
                elif function not in {1, 2, 3, 4, 17, 18, 19}:
                    reason = "collector_frame_function_invalid"
                else:
                    reason = ""
                self.assertEqual(runtime_eybond_header_error(header), reason)
                self.assertEqual(_runtime_eybond_header_error(header), reason)

    def test_every_split_point_and_one_byte_fragmentation(self) -> None:
        for grammar, wire in (
            (BinaryGrammar.MIXED, _auxiliary()),
            (BinaryGrammar.MIXED, _framed(devcode=0x02FF)),
            (BinaryGrammar.EYBOND, _framed()),
            (BinaryGrammar.AABB, _auxiliary(collision=True)),
        ):
            splits = [[wire[:i], wire[i:]] for i in range(len(wire) + 1)]
            splits.append([bytes([byte]) for byte in wire])
            for pieces in splits:
                with self.subTest(grammar=grammar, split_lengths=list(map(len, pieces))):
                    decoder = self._decoder(grammar)
                    for piece in pieces:
                        self.assertEqual(decoder.feed(piece, now=10.1), len(piece))
                    self.assertEqual(decoder.finish(now=10.1).wire, wire)

    def test_coalesced_at_or_binary_suffix_not_consumed(self) -> None:
        wire = _auxiliary()
        for suffix in (b"AT+SYST:123\r\n", _framed(), _auxiliary(subtype=b"\x02\x02")):
            with self.subTest(suffix_length=len(suffix)):
                decoder = self._decoder()
                chunk = wire + suffix
                consumed = decoder.feed(chunk, now=10.1)
                self.assertEqual(consumed, len(wire))
                self.assertEqual(chunk[consumed:], suffix)
                self.assertEqual(decoder.feed(suffix, now=10.2), 0)
                self.assertEqual(decoder.finish(now=10.2).wire, wire)

    def test_checksum_failure_is_terminal_and_never_publishes(self) -> None:
        wire = _auxiliary()
        bad = wire[:-1] + bytes([wire[-1] ^ 1])
        decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "aabb_checksum_invalid"):
            decoder.feed(bad + _framed(), now=10.1)
        self.assertIsNone(decoder.frame)
        self.assertEqual(decoder.buffered_size, AABB_FRAME_SIZE)
        with self.assertRaisesRegex(BinaryFramingError, "aabb_checksum_invalid"):
            decoder.feed(wire, now=10.2)

    def test_no_extra_bytes_accepted_by_exact_frame_validator(self) -> None:
        for length in range(23):
            if length == 21:
                continue
            with self.subTest(length=length):
                wire = (_auxiliary() + b"xx")[:length]
                with self.assertRaisesRegex(BinaryFramingError, "aabb_length_invalid"):
                    validate_aabb_frame(wire)

    def test_bad_magic_and_unknown_subtype(self) -> None:
        for wire, reason in (
            (_checksum(b"xx" + _auxiliary()[2:]), "aabb_magic_invalid"),
            (_auxiliary(subtype=b"\x02\x03"), "aabb_subtype_unsupported"),
        ):
            with self.subTest(reason=reason):
                decoder = self._decoder(BinaryGrammar.AABB)
                with self.assertRaisesRegex(BinaryFramingError, reason):
                    decoder.feed(wire, now=10.1)
                self.assertIsNone(decoder.frame)

    def test_mixed_unsupported_subtype_is_not_silently_harvested(self) -> None:
        decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "aabb_subtype_unsupported"):
            decoder.feed(_auxiliary(subtype=b"\x02\x03"), now=10.1)

    def test_eybond_aabb_tid_does_not_depend_on_waiter_lifetime(self) -> None:
        # No future/callback can influence this decision. A negotiated framed
        # stream drains exactly the same frame before/after consumer cancellation.
        for size in (0, 4, 13, 20, 4096):
            with self.subTest(size=size):
                wire = _framed(size=size)
                decoder = self._decoder(BinaryGrammar.EYBOND)
                self.assertEqual(decoder.feed(wire + b"TAIL", now=10.1), len(wire))
                frame = decoder.finish(now=10.1)
                self.assertEqual(frame.wire, wire)
                self.assertEqual(frame.header.tid, 0xAABB)
                self.assertEqual(frame.grammar, BinaryGrammar.EYBOND)

    def test_dual_valid_21_bytes_requires_owner_not_checksum_preference(self) -> None:
        wire = _collision_0200()
        validate_aabb_frame(wire)
        # Matching outstanding claim: AABB, not a long EyeBond frame.
        owned = self._decoder(claim=_claim())
        self.assertEqual(owned.feed(wire + b"TAIL", now=10.1), 21)
        frame = owned.finish(now=10.1)
        self.assertEqual(frame.grammar, BinaryGrammar.AABB)
        self.assertEqual(frame.wire, wire)
        # Bad checksum with the same claim: reject, never publish.
        bad = wire[:-1] + bytes([wire[-1] ^ 1])
        bad_decoder = self._decoder(claim=_claim())
        with self.assertRaisesRegex(BinaryFramingError, "aabb_checksum_invalid"):
            bad_decoder.feed(bad, now=10.1)
        self.assertIsNone(bad_decoder.frame)
        # No claim: still ambiguous; checksum alone is not ownership.
        decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(wire, now=10.1)
        self.assertIsNone(decoder.frame)
        self.assertEqual(decoder.buffered_size, 8)
        # Claim subtype mismatch: do not accept as that claim's reply.
        mismatched = self._decoder(claim=_claim(b"\x02\x02"))
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            mismatched.feed(wire, now=10.1)
        self.assertIsNone(mismatched.frame)
        self.assertEqual(mismatched.buffered_size, 8)

    def test_late_framed_checksum_collision_does_not_consume_21_bytes(self) -> None:
        wire = _framed()
        wire = _checksum(wire[:21]) + wire[21:]
        validate_aabb_frame(wire[:21])
        # No claim: ambiguous and must not consume a speculative 21-byte AABB.
        decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(wire + b"AT+SYST:123\r\n", now=10.1)
        self.assertEqual(decoder.buffered_size, 8)
        self.assertIsNone(decoder.frame)
        # Matching claim: take AABB only; leftover EyeBond tail stays unconsumed.
        owned = self._decoder(claim=_claim())
        chunk = wire + b"AT+SYST:123\r\n"
        self.assertEqual(owned.feed(chunk, now=10.1), 21)
        self.assertEqual(owned.finish(now=10.1).wire, wire[:21])
        self.assertEqual(chunk[21:], wire[21:] + b"AT+SYST:123\r\n")

    def test_plausible_1091_byte_header_does_not_swallow_following_traffic(self) -> None:
        wire = _collision_0200()
        validate_aabb_frame(wire)
        # No claim: fail-close at the 8-byte overlap; do not swallow following traffic.
        decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(wire + _framed(), now=10.1)
        self.assertEqual(decoder.buffered_size, 8)
        self.assertIsNone(decoder.frame)
        # Matching 0200 claim: AABB of 21 bytes, not a ~1137-byte EyeBond frame.
        owned = self._decoder(claim=_claim())
        chunk = wire + _framed()
        self.assertEqual(owned.feed(chunk, now=10.1), 21)
        self.assertEqual(owned.finish(now=10.1).grammar, BinaryGrammar.AABB)
        self.assertEqual(chunk[21:], _framed())

    def test_partial_eof_at_every_offset_is_terminal(self) -> None:
        wire = _auxiliary()
        for offset in range(len(wire)):
            with self.subTest(offset=offset):
                decoder = self._decoder()
                decoder.feed(wire[:offset], now=10.1)
                with self.assertRaisesRegex(BinaryFramingError, "binary_frame_truncated"):
                    decoder.finish(now=10.2)
                with self.assertRaisesRegex(BinaryFramingError, "binary_frame_truncated"):
                    decoder.feed(wire[offset:], now=10.3)
                self.assertIsNone(decoder.frame)

    def test_fragmentation_does_not_renew_overall_deadline(self) -> None:
        decoder = self._decoder(timeout=0.05)
        self.assertEqual(decoder.deadline, 10.05)
        wire = _auxiliary()
        for i in range(4):
            decoder.feed(wire[i:i + 1], now=10.01 + i * 0.01)
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_timeout"):
            decoder.feed(wire[4:], now=10.05)
        self.assertEqual(decoder.deadline, 10.05)
        self.assertEqual(decoder.buffered_size, 4)
        self.assertIsNone(decoder.frame)

    def test_idle_tick_can_expire_without_new_bytes(self) -> None:
        decoder = self._decoder(timeout=0.05)
        decoder.feed(b"\xaa", now=10.01)
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_timeout"):
            decoder.feed(b"", now=11.0)

    def test_large_payload_is_bounded_before_buffer_growth(self) -> None:
        decoder = self._decoder(BinaryGrammar.EYBOND)
        with self.assertRaisesRegex(BinaryFramingError, "collector_frame_payload_too_large"):
            decoder.feed(_framed(size=4097), now=10.1)
        self.assertLessEqual(decoder.buffered_size, MAX_BINARY_FRAME_SIZE)
        self.assertEqual(decoder.buffered_size, 8)

    def test_invalid_header_length_and_function(self) -> None:
        for wire, reason in (
            (bytes.fromhex("aabb02000000ff04"), "collector_frame_length_invalid"),
            (bytes.fromhex("001002ff0002ffff"), "collector_frame_function_invalid"),
        ):
            with self.subTest(reason=reason):
                decoder = self._decoder(BinaryGrammar.EYBOND)
                with self.assertRaisesRegex(BinaryFramingError, reason):
                    decoder.feed(wire, now=10.1)

    def test_invalid_constructor_deadline_and_grammar(self) -> None:
        for start, timeout in ((10, 0), (10, -1), (10, float("inf")), (float("nan"), 1)):
            with self.subTest(start=start, timeout=timeout):
                with self.assertRaisesRegex(ValueError, "binary_deadline_invalid"):
                    BinaryFrameDecoder(BinaryGrammar.MIXED, started_at=start, timeout=timeout)
        with self.assertRaisesRegex(ValueError, "binary_grammar_invalid"):
            self._decoder("mixed")

    def test_backwards_or_nonfinite_clock_cannot_extend_deadline(self) -> None:
        for value, reason in ((9.0, "binary_clock_regressed"), (float("nan"), "binary_clock_invalid")):
            with self.subTest(reason=reason):
                decoder = self._decoder()
                with self.assertRaisesRegex(BinaryFramingError, reason):
                    decoder.feed(_auxiliary(), now=value)
                self.assertIsNone(decoder.frame)

    def test_codec_imports_no_sensor_schema_driver_or_future(self) -> None:
        path = ROOT / "custom_components/eybond_local/collector/transport/binary_framing.py"
        tree = ast.parse(path.read_text())
        imports = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        self.assertFalse(any(part in name for name in imports for part in ("metadata", "payload", "driver")))
        names = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        self.assertNotIn("done", names)
        self.assertNotIn("set_result", names)


class BinaryFrameReaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_coalesced_stream_keeps_tail_outside_decoder(self) -> None:
        for grammar, wire in (
            (BinaryGrammar.MIXED, _auxiliary()),
            (BinaryGrammar.EYBOND, _framed()),
            (BinaryGrammar.AABB, _auxiliary(collision=True)),
        ):
            for offset in (1, 3, 8):
                with self.subTest(grammar=grammar, offset=offset):
                    reader = asyncio.StreamReader()
                    suffix = b"AT+SYST:123\r\n" + bytes(8192)
                    reader.feed_data(wire[offset:] + suffix)
                    reader.feed_eof()
                    result = await async_read_binary_frame(
                        reader, prefix=wire[:offset], grammar=grammar,
                        started_at=asyncio.get_running_loop().time(), timeout=1.0,
                    )
                    self.assertEqual(result.wire, wire)
                    self.assertEqual(await reader.read(), suffix)

    async def test_stream_one_byte_delivery(self) -> None:
        wire = _auxiliary()
        reader = asyncio.StreamReader()

        async def feed() -> None:
            for value in wire[1:]:
                await asyncio.sleep(0)
                reader.feed_data(bytes([value]))
            reader.feed_eof()

        feeder = asyncio.create_task(feed())
        try:
            result = await async_read_binary_frame(
                reader, prefix=wire[:1], grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time(), timeout=1.0,
            )
            self.assertEqual(result.wire, wire)
        finally:
            await feeder

    async def test_eof_does_not_create_a_short_response(self) -> None:
        wire = _auxiliary()
        for size in range(1, len(wire)):
            with self.subTest(size=size):
                reader = asyncio.StreamReader()
                reader.feed_data(wire[1:size])
                reader.feed_eof()
                with self.assertRaisesRegex(BinaryFramingError, "binary_frame_truncated"):
                    await async_read_binary_frame(
                        reader, prefix=wire[:1], grammar=BinaryGrammar.MIXED,
                        started_at=asyncio.get_running_loop().time(), timeout=1.0,
                    )

    async def test_timeout_cancels_outstanding_read(self) -> None:
        cancelled = asyncio.Event()

        class Reader:
            async def readexactly(self, size):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_timeout"):
            await async_read_binary_frame(
                Reader(), prefix=b"\xaa", grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time(), timeout=0.02,
            )
        self.assertTrue(cancelled.is_set())

    async def test_cancellation_stops_child_read_without_retry(self) -> None:
        entered = asyncio.Event()
        cancelled = asyncio.Event()
        calls = []

        class Reader:
            async def readexactly(self, size):
                calls.append(size)
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        task = asyncio.create_task(async_read_binary_frame(
            Reader(), prefix=b"\xaa", grammar=BinaryGrammar.MIXED,
            started_at=asyncio.get_running_loop().time(), timeout=2.0,
        ))
        try:
            await asyncio.wait_for(entered.wait(), timeout=1.0)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1.0)
        self.assertTrue(cancelled.is_set())
        self.assertEqual(calls, [7])

    async def test_expired_prefix_deadline_does_not_start_a_new_read(self) -> None:
        class Reader:
            async def readexactly(self, size):
                raise AssertionError("must not read after the original deadline")

        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_timeout"):
            await async_read_binary_frame(
                Reader(), prefix=b"\xaa", grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time() - 1.0, timeout=0.05,
            )

    async def test_ambiguous_prefix_never_reads_speculative_tail(self) -> None:
        wire = _collision_0200()
        # No claim: ambiguous at the 8-byte prefix; do not read the longer tail.
        reader = asyncio.StreamReader()
        reader.feed_data(wire[8:] + b"AT+SYST:123\r\n")
        reader.feed_eof()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            await async_read_binary_frame(
                reader, prefix=wire[:8], grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time(), timeout=1.0,
            )
        self.assertEqual(await reader.read(), wire[8:] + b"AT+SYST:123\r\n")
        # Matching claim: read exactly the remaining AABB bytes, leave the AT suffix.
        owned_reader = asyncio.StreamReader()
        owned_reader.feed_data(wire[8:] + b"AT+SYST:123\r\n")
        owned_reader.feed_eof()
        result = await async_read_binary_frame(
            owned_reader, prefix=wire[:8], grammar=BinaryGrammar.MIXED,
            started_at=asyncio.get_running_loop().time(), timeout=1.0,
            auxiliary_claim=_claim(),
        )
        self.assertEqual(result.grammar, BinaryGrammar.AABB)
        self.assertEqual(result.wire, wire)
        self.assertEqual(await owned_reader.read(), b"AT+SYST:123\r\n")
        # Subtype mismatch with an outstanding claim stays ambiguous.
        mismatch_reader = asyncio.StreamReader()
        mismatch_reader.feed_data(wire[8:])
        mismatch_reader.feed_eof()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            await async_read_binary_frame(
                mismatch_reader, prefix=wire[:8], grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time(), timeout=1.0,
                auxiliary_claim=_claim(b"\x02\x02"),
            )
        self.assertEqual(await mismatch_reader.read(), wire[8:])

    async def test_wrong_prefix_length_is_rejected_before_reading(self) -> None:
        for prefix in (b"", bytes(9)):
            with self.subTest(size=len(prefix)):
                with self.assertRaisesRegex(ValueError, "binary_prefix_length_invalid"):
                    await async_read_binary_frame(
                        asyncio.StreamReader(), prefix=prefix, grammar=BinaryGrammar.MIXED,
                        started_at=asyncio.get_running_loop().time(), timeout=1.0,
                    )


if __name__ == "__main__":
    unittest.main()
