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
    build_collector_request,
)
from custom_components.eybond_local.collector.transport import (
    _PendingCollectorSocket,
    _SharedEybondListener,
)
from custom_components.eybond_local.collector.transport.common import (
    _collector_pn_from_initial_chunk,
)

PN = "V001107SYN82291016"
PEER = "192.0.2.8"
NESTED_PN = "E50000200000000001"
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
