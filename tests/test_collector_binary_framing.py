"""Structural framing regressions for the auxiliary-channel implementation.

All wire values are synthetic. These tests do not enable the new grammar on a
collector or validate an inverter model, sensor mapping, or real cloud session.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE,
    EybondHeader,
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.transport.auxiliary_session import (
    AuxiliaryReadClaim,
    AuxiliaryReadSession,
)
from custom_components.eybond_local.collector.transport.binary_framing import (
    AABB_FRAME_SIZE,
    BinaryFrameDecoder,
    BinaryFramingError,
    BinaryGrammar,
    async_read_binary_frame,
    runtime_eybond_header_error,
    validate_aabb_frame,
)
from custom_components.eybond_local.payload.short_ascii_mppt import (
    parse_mppt_runtime_wire,
)


def _checksum(wire: bytes) -> bytes:
    return wire[:-1] + bytes([sum(wire[2:-1]) & 0xFF])


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
    def _decoder(self, grammar=BinaryGrammar.MIXED, *, timeout=1.0):
        return BinaryFrameDecoder(grammar, started_at=10.0, timeout=timeout)

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

    def test_legal_header_aabb_prefix_is_always_ambiguous_on_mixed(self) -> None:
        """A legal EyeBond header that is also an AABB prefix closes on MIXED.

        117 V / 2600 W and 120 V / 2600 W are in that set: the voltage word is
        a legal length and the low byte of power/10 is function code 4.
        """

        cases = (
            ("113.1 V / 20 W", _collision_0200()),
            ("117 V / 2600 W", _auxiliary(voltage=1170, power=260)),
            (
                "120 V / 2600 W",
                bytes.fromhex("aabb020004b00104020800fa000a010000000000ca"),
            ),
        )
        for label, wire in cases:
            with self.subTest(label=label):
                validate_aabb_frame(wire)
                header = decode_header(wire)
                self.assertEqual(runtime_eybond_header_error(header), "")
                self.assertGreater(header.total_len, len(wire))
                decoder = self._decoder()
                with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
                    decoder.feed(wire + b"TAIL", now=10.1)
                self.assertIsNone(decoder.frame)
                self.assertEqual(decoder.buffered_size, 8)
                bad = wire[:-1] + bytes([wire[-1] ^ 1])
                bad_decoder = self._decoder()
                with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
                    bad_decoder.feed(bad, now=10.1)
                self.assertIsNone(bad_decoder.frame)
                self.assertEqual(bad_decoder.buffered_size, 8)
        explicit = self._decoder(BinaryGrammar.AABB)
        self.assertEqual(explicit.feed(cases[0][1], now=10.1), 21)
        self.assertEqual(explicit.finish(now=10.1).grammar, BinaryGrammar.AABB)

    def test_noncolliding_wattage_still_returns_aabb(self) -> None:
        """370 W: the low byte of power/10 is not a runtime EyeBond function code."""

        wire = _auxiliary(voltage=1200, power=37)
        header = decode_header(wire)
        self.assertEqual(header.fcode, 37)
        self.assertEqual(
            runtime_eybond_header_error(header),
            "collector_frame_function_invalid",
        )
        owned = self._decoder()
        self.assertEqual(owned.feed(wire, now=10.1), 21)
        frame = owned.finish(now=10.1)
        self.assertEqual(frame.grammar, BinaryGrammar.AABB)
        self.assertEqual(frame.wire, wire)
        bad = wire[:-1] + bytes([wire[-1] ^ 1])
        bad_decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "aabb_checksum_invalid"):
            bad_decoder.feed(bad, now=10.1)
        self.assertIsNone(bad_decoder.frame)
        self.assertEqual(bad_decoder.buffered_size, AABB_FRAME_SIZE)

    def test_late_framed_checksum_collision_does_not_consume_21_bytes(self) -> None:
        wire = _framed()
        wire = _checksum(wire[:21]) + wire[21:]
        validate_aabb_frame(wire[:21])
        decoder = self._decoder()
        self.assertGreaterEqual(len(wire), decode_header(wire).total_len)
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(wire + b"AT+SYST:123\r\n", now=10.1)
        self.assertEqual(decoder.buffered_size, 8)
        self.assertIsNone(decoder.frame)

    def test_complete_eybond_frame_is_not_truncated_to_aabb(self) -> None:
        """Fixture: a complete FC4 frame whose first 21 bytes are also a valid AABB reply."""

        wire = bytes.fromhex("aabb020004b00104020800fa000a010000000000ca") + bytes(1185)
        header = decode_header(wire)
        self.assertEqual(len(wire), header.total_len)
        self.assertEqual(header.total_len, 1206)
        self.assertEqual(runtime_eybond_header_error(header), "")
        validate_aabb_frame(wire[:21])
        decoder = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(wire, now=10.1)
        self.assertEqual(decoder.buffered_size, 8)
        self.assertIsNone(decoder.frame)
        # The header alone is the overlap. The declared tail is not required.
        split = self._decoder()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            split.feed(wire[:8], now=10.1)
        self.assertEqual(split.buffered_size, 8)
        self.assertIsNone(split.frame)

    def test_plausible_1091_byte_header_does_not_swallow_following_traffic(self) -> None:
        wire = _collision_0200()
        validate_aabb_frame(wire)
        decoder = self._decoder()
        chunk = wire + _framed()
        self.assertLess(len(chunk), decode_header(wire).total_len)
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(chunk, now=10.1)
        self.assertEqual(decoder.buffered_size, 8)
        self.assertIsNone(decoder.frame)

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
        oversized_header = _framed(size=4097)[:HEADER_SIZE]
        # Header-only: next header is not buffered, so close. Never adopt 4097.
        with self.assertRaisesRegex(BinaryFramingError, "collector_frame_payload_too_large"):
            decoder.feed(oversized_header, now=10.1)
        self.assertIsNone(decoder.frame)
        self.assertEqual(decoder.bytes_needed, 0)

    def test_illegal_junk_header_resyncs_to_aabb_follow_on(self) -> None:
        """Whole-window discard must not form ff0000ff04aabb02 (payload_len=1192)."""

        junk = bytes.fromhex("000f02ff0000ff04")
        # EyeBond follow-on whose first bytes are aa bb (tid 0xAABB), small payload.
        # Use non-0200/0202 devcode so MIXED does not treat the header as AABB overlap.
        eybond_follow = _framed(tid=0xAABB, size=4, devcode=0x02FF)
        self.assertEqual(eybond_follow[:2], b"\xaa\xbb")
        self.assertNotIn(eybond_follow[:4], (b"\xaa\xbb\x02\x00", b"\xaa\xbb\x02\x02"))
        for grammar in (BinaryGrammar.EYBOND, BinaryGrammar.MIXED):
            with self.subTest(grammar=grammar, follow="eybond_tid_aabb"):
                decoder = self._decoder(grammar)
                chunk = junk + eybond_follow
                self.assertEqual(decoder.feed(chunk, now=10.1), len(junk) + len(eybond_follow))
                frame = decoder.finish(now=10.1)
                self.assertEqual(frame.grammar, BinaryGrammar.EYBOND)
                self.assertEqual(frame.wire, eybond_follow)
                self.assertNotEqual(frame.wire[:HEADER_SIZE], junk)
                self.assertLess(frame.header.payload_len, 100)
                self.assertNotEqual(frame.header.payload_len, 1192)

    def test_two_illegal_headers_close_without_publish(self) -> None:
        junk = bytes.fromhex("000f02ff0000ff04")
        second = bytes([0xFF]) * HEADER_SIZE
        decoder = self._decoder(BinaryGrammar.EYBOND)
        with self.assertRaisesRegex(
            BinaryFramingError,
            "collector_frame_(length_invalid|payload_too_large|function_invalid)",
        ):
            decoder.feed(junk + second, now=10.1)
        self.assertIsNone(decoder.frame)
        self.assertEqual(decoder.buffered_size, HEADER_SIZE)

    def test_invalid_header_length_and_function(self) -> None:
        legal = _framed(tid=0x1234, devcode=0x0456, size=5)
        legal_header = decode_header(legal)
        self.assertEqual(runtime_eybond_header_error(legal_header), "")
        self.assertGreater(len(legal), HEADER_SIZE)
        self.assertGreaterEqual(len(legal), legal_header.total_len)
        for wire, reason in (
            (bytes.fromhex("aabb02000000ff04"), "collector_frame_length_invalid"),
            (bytes.fromhex("001002ff0002ffff"), "collector_frame_function_invalid"),
        ):
            with self.subTest(reason=reason):
                # Header only: nothing follows, so close. Do not wait for 8 more bytes.
                decoder = self._decoder(BinaryGrammar.EYBOND)
                with self.assertRaisesRegex(BinaryFramingError, reason):
                    decoder.feed(wire, now=10.1)
                self.assertIsNone(decoder.frame)
                self.assertEqual(decoder.bytes_needed, 0)
                if reason == "collector_frame_function_invalid":
                    # Stays fatal beside a complete legal follow-on frame.
                    follow = legal
                    follow_reason = reason
                else:
                    # Length-invalid may discard once when the next header is
                    # already buffered. A second illegal window still closes.
                    follow = bytes([0xFF]) * HEADER_SIZE
                    follow_reason = (
                        "collector_frame_(length_invalid|payload_too_large|function_invalid)"
                    )
                exhausted = self._decoder(BinaryGrammar.EYBOND)
                with self.assertRaisesRegex(BinaryFramingError, follow_reason):
                    exhausted.feed(wire + follow, now=10.1)
                self.assertIsNone(exhausted.frame)
                if reason == "collector_frame_function_invalid":
                    self.assertEqual(exhausted.buffered_size, HEADER_SIZE)

    def test_raw_rtu_prefix_closes_payload_too_large_without_false_payload(self) -> None:
        """7-byte RTU plus a real FC4 frame must not become a 2303-byte read."""

        raw_rtu = bytes.fromhex("0103021234b533")
        valid = build_collector_request(
            0x6AEE,
            raw_rtu,
            devcode=0x0200,
            collector_addr=1,
            fcode=4,
        )
        chunk = raw_rtu + valid
        misaligned = chunk[HEADER_SIZE:HEADER_SIZE * 2]
        self.assertEqual(misaligned.hex(), "ee02000009010401")
        false_header = decode_header(misaligned)
        self.assertEqual(false_header.fcode, 1)
        self.assertEqual(false_header.payload_len, 2303)
        self.assertEqual(runtime_eybond_header_error(false_header), "")
        self.assertEqual(
            runtime_eybond_header_error(decode_header(chunk[:HEADER_SIZE])),
            "collector_frame_payload_too_large",
        )
        for grammar in (BinaryGrammar.EYBOND, BinaryGrammar.MIXED):
            with self.subTest(grammar=grammar):
                decoder = self._decoder(grammar)
                with self.assertRaisesRegex(
                    BinaryFramingError,
                    "collector_frame_payload_too_large",
                ):
                    decoder.feed(chunk, now=10.1)
                self.assertIsNone(decoder.frame)
                self.assertEqual(decoder.bytes_needed, 0)
                self.assertEqual(decoder.buffered_size, HEADER_SIZE)

    def test_raw_rtu_prefix_close_does_not_log_discard_recovered(self) -> None:
        """0103021234b533 plus a real FC4 reply closes and does not log recovery."""

        raw_rtu = bytes.fromhex("0103021234b533")
        valid = build_collector_request(
            0x6AEE,
            raw_rtu,
            devcode=0x0200,
            collector_addr=1,
            fcode=4,
        )
        chunk = raw_rtu + valid
        log_name = "custom_components.eybond_local.collector.transport.binary_framing"
        for grammar in (BinaryGrammar.EYBOND, BinaryGrammar.MIXED):
            with self.subTest(grammar=grammar):
                decoder = self._decoder(grammar)
                with self.assertNoLogs(log_name, level="WARNING"):
                    with self.assertRaisesRegex(
                        BinaryFramingError,
                        "collector_frame_payload_too_large",
                    ):
                        decoder.feed(chunk, now=10.1)
                self.assertIsNone(decoder.frame)
                self.assertEqual(decoder.bytes_needed, 0)
                self.assertEqual(decoder.buffered_size, HEADER_SIZE)

    def test_discard_recovered_log_names_the_buffered_legal_frame(self) -> None:
        """A one-window discard logs the legal frame that was kept, not the junk."""

        junk = bytes.fromhex("000f02ff0000ff04")
        discarded = decode_header(junk)
        legal = build_collector_request(
            0x1234,
            b"abcde",
            devcode=0x0456,
            collector_addr=1,
            fcode=2,
        )
        legal_header = decode_header(legal)
        self.assertEqual(
            runtime_eybond_header_error(discarded),
            "collector_frame_length_invalid",
        )
        self.assertEqual(runtime_eybond_header_error(legal_header), "")
        self.assertGreaterEqual(len(legal), legal_header.total_len)
        self.assertNotEqual(
            (legal_header.fcode, legal_header.payload_len),
            (discarded.fcode, discarded.payload_len),
        )
        log_name = "custom_components.eybond_local.collector.transport.binary_framing"
        expected = (
            f"discarded={junk.hex()} "
            f"next_tid={legal_header.tid} next_devcode=0x{legal_header.devcode:04X} "
            f"next_fc={legal_header.fcode} next_payload={legal_header.payload_len}"
        )
        wrong = f"next_fc={discarded.fcode} next_payload={discarded.payload_len}"
        for grammar in (BinaryGrammar.EYBOND, BinaryGrammar.MIXED):
            with self.subTest(grammar=grammar):
                decoder = self._decoder(grammar)
                with self.assertLogs(log_name, level="WARNING") as caught:
                    self.assertEqual(
                        decoder.feed(junk + legal, now=10.1),
                        len(junk) + len(legal),
                    )
                frame = decoder.finish(now=10.1)
                self.assertEqual(frame.wire, legal)
                self.assertIsNotNone(frame.header)
                self.assertEqual(frame.header.fcode, legal_header.fcode)
                self.assertEqual(frame.header.payload_len, legal_header.payload_len)
                recovered = [line for line in caught.output if "discard recovered" in line]
                self.assertEqual(len(recovered), 1)
                self.assertIn(expected, recovered[0])
                self.assertNotIn(wrong, recovered[0])

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
    async def _read_for_claim(self, session, claim, reader, prefix):
        """The claim future is the waiter this read resolves.

        A framing error closes the session. ``close()`` sets ``closed`` and
        fails the claim future with ``ConnectionError``; the framing error
        then propagates. This path does not assign ``closed`` itself.
        """

        try:
            frame = await session.read(async_read_binary_frame(
                reader,
                prefix=prefix,
                grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time(),
                timeout=1.0,
            ))
        except BinaryFramingError:
            session.close()
            raise
        session.accept(frame, claim)
        return frame

    async def test_payload_too_large_header_never_reads_claimed_payload(self) -> None:
        """payload_too_large is decided at the header, including after a discard."""

        oversized_header = _framed(size=4097)[:HEADER_SIZE]
        # Discard once, then a second illegal header closes — never readexactly(4097).
        stream = oversized_header + bytes([0xFF]) * HEADER_SIZE
        reads: list[int] = []

        class Reader:
            def __init__(self) -> None:
                self._data = stream[HEADER_SIZE:]

            async def readexactly(self, size: int) -> bytes:
                reads.append(size)
                if size > len(self._data):
                    raise asyncio.IncompleteReadError(self._data, size)
                chunk, self._data = self._data[:size], self._data[size:]
                return chunk

        with self.assertRaisesRegex(
            BinaryFramingError,
            "collector_frame_payload_too_large",
        ):
            await async_read_binary_frame(
                Reader(),
                prefix=oversized_header,
                grammar=BinaryGrammar.EYBOND,
                started_at=asyncio.get_running_loop().time(),
                timeout=1.0,
            )
        # Next header was not in the prefix, so do not readexactly another window.
        self.assertEqual(reads, [])
        self.assertNotIn(4097, reads)

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
        reader = asyncio.StreamReader()
        reader.feed_data(wire[8:] + b"AT+SYST:123\r\n")
        reader.feed_eof()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            await async_read_binary_frame(
                reader, prefix=wire[:8], grammar=BinaryGrammar.MIXED,
                started_at=asyncio.get_running_loop().time(), timeout=1.0,
            )
        self.assertEqual(await reader.read(), wire[8:] + b"AT+SYST:123\r\n")

    async def test_complete_frame_arriving_during_blocked_read_is_ambiguous(self) -> None:
        """A legal overlap that arrives while the header read waits stays unpublished."""

        wire = bytes.fromhex("aabb020004b00104020800fa000a010000000000ca") + bytes(1185)
        self.assertEqual(len(wire), decode_header(wire).total_len)
        loop = asyncio.get_running_loop()
        session = AuxiliaryReadSession()
        session.enabled = True
        future = loop.create_future()
        claim = AuxiliaryReadClaim(b"\x02\x00", future)
        session.claim = claim
        reader = asyncio.StreamReader()
        task = asyncio.create_task(self._read_for_claim(
            session, claim, reader, wire[:7],
        ))
        await self._wait_until_read_blocks(reader, task)
        reader.feed_data(wire[7:])
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            await asyncio.wait_for(task, timeout=1.0)
        self.assertEqual(bytes(reader._buffer), wire[8:])
        error = future.exception()
        self.assertIsInstance(error, ConnectionError)
        self.assertEqual(str(error), "collector_disconnected")
        self.assertTrue(session.closed)

    async def test_noncolliding_wattage_claim_mismatch_does_not_resolve_waiter(self) -> None:
        """370 W still selects AABB; a 0202 claim must not receive that sample."""

        wire = _auxiliary(voltage=1200, power=37)
        loop = asyncio.get_running_loop()
        session = AuxiliaryReadSession()
        session.enabled = True
        mismatch = loop.create_future()
        mismatch_claim = AuxiliaryReadClaim(b"\x02\x02", mismatch)
        session.claim = mismatch_claim
        reader = asyncio.StreamReader()
        reader.feed_data(wire[1:])
        reader.feed_eof()
        frame = await async_read_binary_frame(
            reader, prefix=wire[:1], grammar=BinaryGrammar.MIXED,
            started_at=loop.time(), timeout=1.0,
        )
        self.assertEqual(frame.grammar, BinaryGrammar.AABB)
        self.assertEqual(frame.wire, wire)
        with self.assertRaisesRegex(BinaryFramingError, "aabb_response_subtype_mismatch"):
            session.accept(frame, mismatch_claim)
        self.assertFalse(mismatch.done())
        owned = loop.create_future()
        owned_claim = AuxiliaryReadClaim(b"\x02\x00", owned)
        session.claim = owned_claim
        session.accept(frame, owned_claim)
        self.assertEqual(owned.result(), wire)
        self.assertFalse(mismatch.done())

    async def test_delayed_tail_after_21_bytes_is_ambiguous(self) -> None:
        """The 21-byte prefix is ambiguous before the other 1185 bytes exist."""

        wire = bytes.fromhex("aabb020004b00104020800fa000a010000000000ca") + bytes(1185)
        self.assertEqual(len(wire), 1206)
        self.assertEqual(decode_header(wire).total_len, 1206)
        false_sample = parse_mppt_runtime_wire(wire[:21])
        self.assertEqual(
            (
                false_sample.pv_voltage_v,
                false_sample.pv_power_w,
                false_sample.mppt_battery_voltage_v,
            ),
            (120.0, 2600, 52.0),
        )
        loop = asyncio.get_running_loop()
        session = AuxiliaryReadSession()
        session.enabled = True
        future = loop.create_future()
        claim = AuxiliaryReadClaim(b"\x02\x00", future)
        session.claim = claim
        reader = asyncio.StreamReader()
        task = asyncio.create_task(self._read_for_claim(
            session, claim, reader, wire[:1],
        ))
        await self._wait_until_read_blocks(reader, task)
        reader.feed_data(wire[1:21])
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            await asyncio.wait_for(task, timeout=1.0)
        reader.feed_data(wire[21:])
        self.assertEqual(bytes(reader._buffer), wire[8:])
        error = future.exception()
        self.assertIsInstance(error, ConnectionError)
        self.assertEqual(str(error), "collector_disconnected")
        self.assertTrue(session.closed)

    async def _wait_until_read_blocks(self, reader, task) -> None:
        for _ in range(50):
            await asyncio.sleep(0)
            if task.done():
                self.fail(f"read finished before blocking: {task.exception()!r}")
            if getattr(reader, "_waiter", None) is not None:
                return
        self.fail("read did not block")

    async def test_complete_frame_already_buffered_is_not_read_as_aabb(self) -> None:
        wire = bytes.fromhex("aabb020004b00104020800fa000a010000000000ca") + bytes(1185)
        self.assertEqual(len(wire), decode_header(wire).total_len)
        loop = asyncio.get_running_loop()
        session = AuxiliaryReadSession()
        session.enabled = True
        future = loop.create_future()
        claim = AuxiliaryReadClaim(b"\x02\x00", future)
        session.claim = claim
        reader = asyncio.StreamReader()
        reader.feed_data(wire[8:])
        reader.feed_eof()
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            await self._read_for_claim(session, claim, reader, wire[:8])
        self.assertEqual(await reader.read(), wire[8:])
        error = future.exception()
        self.assertIsInstance(error, ConnectionError)
        self.assertEqual(str(error), "collector_disconnected")
        self.assertTrue(session.closed)

    async def test_illegal_length_header_closes_without_publishing_following_fc4(self) -> None:
        """An outstanding 0200 read still closes on junk before a buffered FC4 frame."""

        junk = bytes.fromhex("0001000100010104")
        legal = _framed(tid=0x1234, devcode=0x0456, size=20)
        self.assertEqual(len(junk), HEADER_SIZE)
        self.assertEqual(len(legal), 28)
        self.assertEqual(decode_header(legal).fcode, 4)
        self.assertEqual(
            runtime_eybond_header_error(decode_header(junk)),
            "collector_frame_length_invalid",
        )
        self.assertEqual(runtime_eybond_header_error(decode_header(legal)), "")
        loop = asyncio.get_running_loop()

        def outstanding():
            session = AuxiliaryReadSession()
            session.enabled = True
            future: asyncio.Future[bytes] = loop.create_future()
            claim = AuxiliaryReadClaim(b"\x02\x00", future)
            session.claim = claim
            return session, claim, future

        session, claim, future = outstanding()
        reader = asyncio.StreamReader()
        reader.feed_data(junk[1:] + legal)
        with self.assertRaisesRegex(BinaryFramingError, "collector_frame_length_invalid"):
            await self._read_for_claim(session, claim, reader, junk[:1])
        error = future.exception()
        self.assertIsInstance(error, ConnectionError)
        self.assertEqual(str(error), "collector_disconnected")
        self.assertTrue(session.closed)
        self.assertEqual(bytes(reader._buffer), legal)

        session, claim, future = outstanding()
        reader = asyncio.StreamReader()
        task = asyncio.create_task(self._read_for_claim(
            session, claim, reader, junk[:1],
        ))
        await self._wait_until_read_blocks(reader, task)
        reader.feed_data(junk[1:])
        with self.assertRaisesRegex(BinaryFramingError, "collector_frame_length_invalid"):
            await asyncio.wait_for(task, timeout=1.0)
        reader.feed_data(legal)
        error = future.exception()
        self.assertIsInstance(error, ConnectionError)
        self.assertEqual(str(error), "collector_disconnected")
        self.assertTrue(session.closed)

    async def test_noncolliding_0200_reply_resolves_its_waiter(self) -> None:
        """A 0200 reply that is not a legal EyeBond header resolves that claim."""

        wire = _auxiliary()
        loop = asyncio.get_running_loop()
        session = AuxiliaryReadSession()
        session.enabled = True
        future = loop.create_future()
        claim = AuxiliaryReadClaim(b"\x02\x00", future)
        session.claim = claim
        reader = asyncio.StreamReader()
        reader.feed_data(wire[1:])
        reader.feed_eof()
        frame = await self._read_for_claim(session, claim, reader, wire[:1])
        self.assertEqual(frame.grammar, BinaryGrammar.AABB)
        self.assertEqual(frame.wire, wire)
        self.assertTrue(future.done())
        self.assertEqual(future.result(), wire)
        self.assertFalse(session.closed)

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
