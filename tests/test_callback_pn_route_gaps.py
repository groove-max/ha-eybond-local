"""PN route gaps: peer-IP alias, and a frame that follows an AT line."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.protocol import (
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    HEADER_SIZE,
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.transport import (
    _PendingCollectorSocket,
    _SharedEybondListener,
)
from custom_components.eybond_local.collector.transport.common import (
    _collector_pn_from_initial_chunk,
    _leading_at_lines_end,
)

PN = "V001107SYN82291016"
PEER = "192.0.2.8"
NESTED_PN = "E50000200000000001"
WEAK_PN = "E5000020000000"
OTHER_PN = "E50000200000009777"
AT_PREFIX = b"AT+INFO\r\n"


def _heartbeat(pn14: str) -> bytes:
    return build_collector_request(
        1,
        pn14.encode("ascii"),
        devcode=0,
        collector_addr=1,
        fcode=FC_HEARTBEAT,
    )


def _fc2(pn: str) -> bytes:
    return build_collector_request(
        1,
        bytes([0, 2]) + pn.encode("ascii"),
        devcode=0x0994,
        collector_addr=1,
        fcode=FC_QUERY_COLLECTOR,
    )


def _fc4(payload: bytes) -> bytes:
    return build_collector_request(
        1, payload, devcode=0x0994, collector_addr=1, fcode=4,
    )


class _Writer:
    def is_closing(self) -> bool:
        return False

    def write(self, data: bytes) -> None:
        del data

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class PnRouteGapTests(unittest.IsolatedAsyncioTestCase):
    def listener(self) -> _SharedEybondListener:
        return _SharedEybondListener(host="127.0.0.1", port=0)

    def pending(self, listener: _SharedEybondListener, frame: bytes):
        listener._remember_session(
            session_id="sock-1",
            remote_ip=PEER,
            remote_port=40000,
        )
        pending = _PendingCollectorSocket(
            session_id="sock-1",
            remote_ip=PEER,
            remote_port=40000,
            reader=asyncio.StreamReader(),
            writer=_Writer(),  # type: ignore[arg-type]
            initial_bytes=frame,
        )
        listener._pending_sockets[pending.session_id] = pending
        return pending

    async def test_strong_pn_route_does_not_replace_the_peer_ip_connection(self):
        listener = self.listener()
        listener.register_payload_pn_owner(PN)
        owned = listener.ensure_connection("", 60.0, 0.5, PN)
        ip_only = listener.ensure_connection(PEER, 60.0, 0.5)
        self.pending(listener, _fc2(PN))

        with patch.object(owned, "run", new=AsyncMock()):
            await listener._sniff_pending_socket(
                listener._pending_sockets["sock-1"]
            )

        self.assertIs(listener._connections[PEER], ip_only)
        self.assertIsNot(listener._connections[PEER], owned)
        self.assertIs(listener._connections_by_pn[PN], owned)

    async def test_strong_at_pn_route_does_not_replace_the_peer_ip_connection(self):
        listener = self.listener()
        listener.register_at_pn_owner(PN)
        owned = listener.ensure_at_connection("", 0.5, PN)
        ip_only = listener.ensure_at_connection(PEER, 0.5)
        self.pending(listener, f"AT+DTUPN:{PN}\r\n".encode("ascii"))

        with patch.object(owned, "run", new=AsyncMock()):
            await listener._sniff_pending_socket(
                listener._pending_sockets["sock-1"]
            )

        self.assertIs(listener._at_connections[PEER], ip_only)
        self.assertIsNot(listener._at_connections[PEER], owned)
        self.assertIs(listener._at_connections_by_pn[PN], owned)

    async def test_ip_only_framed_socket_stays_on_its_peer_connection(self):
        listener = self.listener()
        listener.register_payload_owner(PEER)
        ip_only = listener.ensure_connection(PEER, 60.0, 0.5)
        frame = build_collector_request(
            1, b"", devcode=0x0994, collector_addr=1, fcode=4,
        )
        self.pending(listener, frame)

        with patch.object(ip_only, "run", new=AsyncMock()) as run:
            await listener._sniff_pending_socket(
                listener._pending_sockets["sock-1"]
            )

        run.assert_awaited()
        self.assertIs(listener._connections[PEER], ip_only)

    async def test_pn_inside_a_following_frame_does_not_select_its_owner(self):
        listener = self.listener()
        listener.register_at_pn_owner(NESTED_PN)
        owned = listener.ensure_at_connection("", 0.5, NESTED_PN)
        pending = self.pending(
            listener, AT_PREFIX + _fc4(b"\n" + _fc2(NESTED_PN)),
        )
        pending.reader.feed_eof()

        with patch.object(owned, "run", new=AsyncMock()) as run:
            await listener._sniff_pending_socket(pending)

        run.assert_not_awaited()
        self.assertIs(listener._at_connections_by_pn[NESTED_PN], owned)
        entry = listener._session_inventory["sock-1"]
        self.assertNotEqual(entry.collector_pn, NESTED_PN)

    async def test_frame_newline_after_unterminated_at_text_selects_no_owner(self):
        for at_text in (b"AT+INFO", b"AT+INFO\r"):
            with self.subTest(at_text=at_text):
                listener = self.listener()
                listener.register_at_pn_owner(NESTED_PN)
                owned = listener.ensure_at_connection("", 0.5, NESTED_PN)
                pending = self.pending(
                    listener, at_text + _fc4(b"\n" + _fc2(NESTED_PN)),
                )
                pending.reader.feed_eof()

                with (
                    patch.object(owned, "run", new=AsyncMock()) as run,
                    patch.object(
                        listener,
                        "_mark_session_identity",
                        wraps=listener._mark_session_identity,
                    ) as mark,
                ):
                    await listener._sniff_pending_socket(pending)

                run.assert_not_awaited()
                self.assertIs(listener._at_connections_by_pn[NESTED_PN], owned)
                recorded = [call.args[1] for call in mark.call_args_list]
                self.assertNotIn(NESTED_PN, recorded)
                entry = listener._session_inventory["sock-1"]
                self.assertNotEqual(entry.collector_pn, NESTED_PN)

    async def test_split_frame_after_an_at_line_reads_only_the_full_pn(self):
        listener = self.listener()
        listener.register_at_pn_owner(NESTED_PN)
        owned = listener.ensure_at_connection("", 0.5, NESTED_PN)
        identity = _fc2(NESTED_PN)
        pending = self.pending(listener, AT_PREFIX + identity[:-2])
        pending.reader.feed_data(identity[-2:])

        with (
            patch.object(owned, "run", new=AsyncMock()) as run,
            patch.object(
                listener,
                "_mark_session_identity",
                wraps=listener._mark_session_identity,
            ) as mark,
            patch.object(
                listener,
                "_park_unclaimed_pending_socket",
                wraps=listener._park_unclaimed_pending_socket,
            ) as park,
        ):
            await listener._sniff_pending_socket(pending)

        park.assert_not_called()
        recorded = [call.args[1] for call in mark.call_args_list]
        self.assertIn(NESTED_PN, recorded)
        self.assertNotIn(NESTED_PN[:16], recorded)
        run.assert_awaited()
        entry = listener._session_inventory["sock-1"]
        self.assertEqual(entry.collector_pn, NESTED_PN)
        self.assertEqual(entry.collector_identity_source, "fc2_parameter_2")

    async def test_unfinished_at_line_is_not_read_as_a_frame_header(self):
        listener = self.listener()
        listener.register_at_pn_owner(NESTED_PN)
        owned = listener.ensure_at_connection("", 0.5, NESTED_PN)
        first = b"AT+X\r A" + b"A" + b"B" * 56
        self.assertEqual(len(first), 64)
        self.assertIsNone(_leading_at_lines_end(first))
        header = decode_header(first[:HEADER_SIZE])
        self.assertLess(header.payload_len, 4096)
        rest = b"C" * 100 + b"\n" + _fc2(NESTED_PN)
        rest += b"D" * (HEADER_SIZE + header.payload_len - len(first) - len(rest))
        pending = self.pending(listener, first)
        pending.reader.feed_data(rest)
        pending.reader.feed_eof()

        with (
            patch.object(owned, "run", new=AsyncMock()) as run,
            patch.object(
                pending.reader,
                "readexactly",
                wraps=pending.reader.readexactly,
            ) as readexactly,
            patch.object(
                listener,
                "_mark_session_identity",
                wraps=listener._mark_session_identity,
            ) as mark,
            patch.object(
                listener, "_watch_parked_pending_socket", new=AsyncMock(),
            ),
        ):
            await listener._sniff_pending_socket(pending)

        readexactly.assert_not_called()
        run.assert_not_awaited()
        recorded = [call.args[1] for call in mark.call_args_list]
        self.assertNotIn(NESTED_PN, recorded)
        entry = listener._session_inventory["sock-1"]
        self.assertNotEqual(entry.collector_pn, NESTED_PN)
        self.assertEqual(entry.state, "parked_no_at_owner")
        self.assertEqual(pending.initial_bytes, first)

    async def test_weak_pn_after_an_at_line_uses_the_ip_at_owner(self):
        listener = self.listener()
        listener.register_at_owner(PEER)
        ip_only = listener.ensure_at_connection(PEER, 0.5)
        pending = self.pending(listener, AT_PREFIX + _heartbeat(WEAK_PN))
        pending.reader.feed_eof()

        with (
            patch.object(ip_only, "run", new=AsyncMock()) as run,
            patch.object(
                listener,
                "_park_unclaimed_pending_socket",
                wraps=listener._park_unclaimed_pending_socket,
            ) as park,
        ):
            await listener._sniff_pending_socket(pending)

        park.assert_not_called()
        run.assert_awaited_once()
        self.assertIs(listener._at_connections[PEER], ip_only)
        self.assertIs(listener._session_at_connections["sock-1"], ip_only)
        entry = listener._session_inventory["sock-1"]
        self.assertEqual(entry.state, "routed_at_text")
        self.assertNotIn(
            entry.collector_identity_source, ("fc2_parameter_2", "at_dtupn"),
        )

    async def test_strong_fc2_after_an_at_line_keeps_its_pn_owner(self):
        listener = self.listener()
        listener.register_at_pn_owner(PN)
        listener.register_at_owner(PEER)
        owned = listener.ensure_at_connection("", 0.5, PN)
        ip_only = listener.ensure_at_connection(PEER, 0.5)
        pending = self.pending(listener, AT_PREFIX + _fc2(PN))
        pending.reader.feed_eof()

        with (
            patch.object(owned, "run", new=AsyncMock()) as owned_run,
            patch.object(ip_only, "run", new=AsyncMock()) as ip_run,
        ):
            await listener._sniff_pending_socket(pending)

        owned_run.assert_awaited_once()
        ip_run.assert_not_awaited()
        self.assertIs(listener._at_connections[PEER], ip_only)
        self.assertIs(listener._at_connections_by_pn[PN], owned)
        entry = listener._session_inventory["sock-1"]
        self.assertEqual(entry.collector_pn, PN)
        self.assertEqual(entry.collector_identity_source, "fc2_parameter_2")

    async def test_weak_pn_after_an_at_line_skips_another_pn_owner(self):
        for with_ip_owner in (True, False):
            with self.subTest(with_ip_owner=with_ip_owner):
                listener = self.listener()
                listener.register_at_pn_owner(OTHER_PN)
                owned = listener.ensure_at_connection("", 0.5, OTHER_PN)
                ip_only = None
                if with_ip_owner:
                    listener.register_at_owner(PEER)
                    ip_only = listener.ensure_at_connection(PEER, 0.5)
                pending = self.pending(listener, AT_PREFIX + _heartbeat(WEAK_PN))
                pending.reader.feed_eof()

                with (
                    patch.object(owned, "run", new=AsyncMock()) as owned_run,
                    patch.object(
                        listener,
                        "_park_unclaimed_pending_socket",
                        wraps=listener._park_unclaimed_pending_socket,
                    ) as park,
                ):
                    if ip_only is None:
                        await listener._sniff_pending_socket(pending)
                    else:
                        with patch.object(
                            ip_only, "run", new=AsyncMock(),
                        ) as ip_run:
                            await listener._sniff_pending_socket(pending)
                        ip_run.assert_awaited_once()

                owned_run.assert_not_awaited()
                self.assertIs(listener._at_connections_by_pn[OTHER_PN], owned)
                self.assertIsNot(
                    listener._session_at_connections.get("sock-1"), owned,
                )
                if with_ip_owner:
                    park.assert_not_called()
                    entry = listener._session_inventory["sock-1"]
                    self.assertEqual(entry.state, "routed_at_text")
                else:
                    park.assert_called_once()
                    self.assertEqual(
                        park.call_args.kwargs["session_state"],
                        "parked_no_at_owner",
                    )


class InitialChunkPnTests(unittest.TestCase):
    def test_frame_after_a_non_dtupn_at_line_keeps_its_pn(self):
        pn14 = "V001107SYN8229"
        self.assertEqual(
            _collector_pn_from_initial_chunk(b"AT+INFO\r\n" + _heartbeat(pn14)),
            (pn14, "framed_heartbeat"),
        )
        self.assertEqual(
            _collector_pn_from_initial_chunk(b"AT+INFO\r\n" + _fc2(PN)),
            (PN, "fc2_parameter_2"),
        )

    def test_dtupn_still_wins_when_a_frame_follows(self):
        chunk = f"AT+DTUPN:{PN}\r\n".encode("ascii") + _fc2("V001107SYN998877")
        self.assertEqual(_collector_pn_from_initial_chunk(chunk), (PN, "at_dtupn"))

    def test_frame_at_offset_zero_is_unchanged(self):
        self.assertEqual(
            _collector_pn_from_initial_chunk(_fc2(PN)),
            (PN, "fc2_parameter_2"),
        )

    def test_at_line_without_a_following_frame_has_no_pn(self):
        self.assertEqual(_collector_pn_from_initial_chunk(b"AT+INFO\r\n"), ("", ""))

    def test_frame_after_several_complete_at_lines_keeps_its_pn(self):
        lines = b"AT+INFO\r\nAT+VER:1.0\r\nAT+X\n"
        pn14 = NESTED_PN[:14]
        self.assertEqual(
            _collector_pn_from_initial_chunk(lines + _heartbeat(pn14)),
            (pn14, "framed_heartbeat"),
        )
        self.assertEqual(
            _collector_pn_from_initial_chunk(lines + _fc2(NESTED_PN)),
            (NESTED_PN, "fc2_parameter_2"),
        )

    def test_complete_prefixed_identity_keeps_its_pn(self):
        self.assertEqual(
            _collector_pn_from_initial_chunk(AT_PREFIX + _fc2(NESTED_PN)),
            (NESTED_PN, "fc2_parameter_2"),
        )

    def test_newline_inside_a_following_frame_is_not_a_boundary(self):
        chunk = AT_PREFIX + _fc4(b"\n" + _fc2(NESTED_PN))
        self.assertEqual(_collector_pn_from_initial_chunk(chunk), ("", ""))

    def test_non_at_bytes_before_the_frame_hide_its_pn(self):
        identity = _fc2(NESTED_PN)
        for prefix in (
            AT_PREFIX + b"NOTAT\n",
            AT_PREFIX + b"\r\n",
            AT_PREFIX + b"AT+info\n",
            b" " + AT_PREFIX,
            b"NOTAT\n",
        ):
            with self.subTest(prefix=prefix):
                self.assertEqual(
                    _collector_pn_from_initial_chunk(prefix + identity),
                    ("", ""),
                )

    def test_short_frame_is_not_a_pn(self):
        identity = _fc2(NESTED_PN)
        self.assertEqual(
            _collector_pn_from_initial_chunk(AT_PREFIX + identity[:-2]),
            ("", ""),
        )
        self.assertEqual(_collector_pn_from_initial_chunk(identity[:-2]), ("", ""))
        self.assertEqual(
            _collector_pn_from_initial_chunk(AT_PREFIX + identity),
            (NESTED_PN, "fc2_parameter_2"),
        )

    def test_incomplete_at_line_is_not_walked_past(self):
        self.assertEqual(
            _collector_pn_from_initial_chunk(b"AT+INFO" + _fc2(NESTED_PN)),
            ("", ""),
        )

    def test_frame_newline_does_not_end_an_unterminated_at_line(self):
        for at_text in (b"AT+INFO", b"AT+INFO\r"):
            for nested in (_fc2(NESTED_PN), _heartbeat(NESTED_PN[:14])):
                with self.subTest(at_text=at_text, nested=nested):
                    self.assertEqual(
                        _collector_pn_from_initial_chunk(
                            at_text + _fc4(b"\n" + nested)
                        ),
                        ("", ""),
                    )

    def test_at_lines_with_printable_text_are_still_boundaries(self):
        for line in (
            b"AT+CSQ=20\r\n",
            b"AT+VER:1.0\n",
            b"AT+INFO " + b"X" * (512 - 9) + b"\n",
        ):
            with self.subTest(line=line):
                self.assertEqual(
                    _leading_at_lines_end(line + _fc2(NESTED_PN)), len(line),
                )
                self.assertEqual(
                    _collector_pn_from_initial_chunk(line + _fc2(NESTED_PN)),
                    (NESTED_PN, "fc2_parameter_2"),
                )

    def test_at_line_end_rejects_non_text_and_overlong_lines(self):
        self.assertIsNone(_leading_at_lines_end(b"AT+INFO\r"))
        self.assertIsNone(_leading_at_lines_end(b"AT+INFO " + b"X" * 400))
        self.assertEqual(_leading_at_lines_end(b"AT+INFO\x00\n"), 0)
        self.assertEqual(_leading_at_lines_end(b"AT+INFO" + b"X" * 600 + b"\n"), 0)
        self.assertEqual(
            _leading_at_lines_end(AT_PREFIX + b"AT+INFO" + _fc4(b"\n")),
            len(AT_PREFIX),
        )
