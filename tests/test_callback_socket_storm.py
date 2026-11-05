"""Callback socket-storm policy: one identified collector, overlapping dials."""

from __future__ import annotations

import asyncio
import socket
import struct
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.transport.listener import (
    _SharedEybondListener,
)
from custom_components.eybond_local.collector.transport.socket_storm import (
    GRACE_SECONDS,
    HEALTHY_SECONDS,
    STALE_SECONDS,
    CallbackSocketStorm,
    StormSocket,
    reset_tcp_writer,
)

_PN = "E5000020000000"
_OTHER_PN = "E5000099990001"
_LOG = "custom_components.eybond_local.collector.transport.socket_storm"


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _socket(
    session_id: str,
    *,
    peer_ip: str = "203.0.113.10",
    strong_pn: str = "",
    last_framed_exchange_at: float | None = None,
    routed_payload: bool = False,
    accepted_at: float = 0.0,
) -> StormSocket:
    return StormSocket(
        session_id=session_id,
        peer_ip=peer_ip,
        strong_pn=strong_pn,
        last_framed_exchange_at=last_framed_exchange_at,
        writer=object(),
        routed_payload=routed_payload,
        accepted_at=accepted_at,
    )


def _reasons(lines: list[str]) -> list[str]:
    reasons: list[str] = []
    for line in lines:
        marker = "reason="
        if marker in line:
            reasons.append(line.rsplit(marker, 1)[1].strip())
    return reasons


class CallbackSocketStormTests(unittest.TestCase):
    def test_constants_match_the_contract(self) -> None:
        self.assertEqual(HEALTHY_SECONDS, 15.0)
        self.assertEqual(STALE_SECONDS, 45.0)
        self.assertEqual(GRACE_SECONDS, 15.0)

    def test_healthy_socket_kept_and_siblings_reset(self) -> None:
        storm = CallbackSocketStorm()
        now = 100.0
        healthy = _socket("keep", strong_pn=_PN, last_framed_exchange_at=now - 15.0)
        sibling = _socket("drop", strong_pn=_PN)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            action = storm.consider([healthy, sibling], "drop", now)
        self.assertEqual(action.reason, "keep_healthy")
        self.assertEqual(action.session_ids, ("drop",))
        self.assertEqual(_reasons(caught.output), ["keep_healthy"])
        self.assertIn("closed_sessions=drop", caught.output[0])
        self.assertIn(f"pn={_PN}", caught.output[0])

    def test_framed_exchange_just_outside_15s_is_still_kept(self) -> None:
        storm = CallbackSocketStorm()
        now = 100.0
        recent = _socket("keep", strong_pn=_PN, last_framed_exchange_at=now - 15.001)
        sibling = _socket("drop", strong_pn=_PN)
        action = storm.consider([recent, sibling], "drop", now)
        self.assertEqual(action.reason, "keep_healthy")
        self.assertEqual(action.session_ids, ("drop",))
        stale = _socket("keep", strong_pn=_PN, last_framed_exchange_at=now - 45.001)
        action = CallbackSocketStorm().consider([stale, sibling], "drop", now)
        self.assertEqual(action.reason, "keep_candidate")
        self.assertEqual(action.session_ids, ("drop",))

    def test_none_exchanged_keeps_one_candidate_and_admits_a_later_lone_socket(self) -> None:
        storm = CallbackSocketStorm()
        first = _socket("a", strong_pn=_PN)
        second = _socket("b", strong_pn=_PN)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            action = storm.consider([first, second], "b", 0.0)
        self.assertEqual(action.reason, "keep_candidate")
        self.assertEqual(action.session_ids, ("b",))
        self.assertEqual(_reasons(caught.output), ["keep_candidate"])
        later = storm.consider([_socket("e", strong_pn=_PN)], "e", 1.0)
        self.assertEqual(later.session_ids, ())
        self.assertEqual(later.reason, "")

    def test_grace_replaces_stale_admitted_socket_with_newer_overlap(self) -> None:
        storm = CallbackSocketStorm()
        now = 100.0
        stale = _socket(
            "old",
            strong_pn=_PN,
            last_framed_exchange_at=now - HEALTHY_SECONDS - 1,
        )
        sibling = _socket("sib", strong_pn=_PN)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            first = storm.consider([stale, sibling], "sib", now)
        self.assertEqual(first.reason, "keep_healthy")
        self.assertEqual(first.session_ids, ("sib",))
        fresh = _socket("new", strong_pn=_PN)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            replaced = storm.consider([stale, fresh], "new", now + 1)
        self.assertEqual(replaced.reason, "replace_stale")
        self.assertEqual(replaced.session_ids, ("old",))
        self.assertEqual(_reasons(caught.output), ["replace_stale"])
        newer = _socket(
            "newer",
            strong_pn=_PN,
            last_framed_exchange_at=now + 2,
        )
        with self.assertLogs(_LOG, level="WARNING") as caught:
            healthy = storm.consider([stale, newer], "newer", now + 2)
        self.assertEqual(healthy.reason, "keep_healthy")
        self.assertIn("old", healthy.session_ids)

    def test_second_dial_during_grace_resets_only_the_extra(self) -> None:
        storm = CallbackSocketStorm()
        first = _socket("a", strong_pn=_PN)
        second = _socket("b", strong_pn=_PN)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            storm.consider([first, second], "b", 0.0)
            extra = _socket("f", strong_pn=_PN)
            during = storm.consider([first, extra], "f", GRACE_SECONDS - 0.1)
        self.assertEqual(during.reason, "keep_candidate")
        self.assertEqual(during.session_ids, ("f",))
        self.assertEqual(_reasons(caught.output), ["keep_candidate"])

    def test_after_grace_with_no_exchange_extra_replaces_candidate(self) -> None:
        storm = CallbackSocketStorm()
        first = _socket("a", strong_pn=_PN)
        second = _socket("b", strong_pn=_PN)
        storm.consider([first, second], "b", 0.0)
        extra = _socket("g", strong_pn=_PN)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            replaced = storm.consider([first, extra], "g", GRACE_SECONDS)
        self.assertEqual(replaced.reason, "replace_stale")
        self.assertEqual(replaced.session_ids, ("a",))
        self.assertEqual(_reasons(caught.output), ["replace_stale"])

    def test_pn_less_sockets_sharing_an_ip_are_not_a_storm(self) -> None:
        storm = CallbackSocketStorm()
        first = _socket("a")
        second = _socket("b")
        action = storm.consider([first, second], "b", 0.0)
        self.assertEqual(action.session_ids, ())
        self.assertEqual(action.reason, "")
        foreign = _socket("c", strong_pn=_OTHER_PN)
        identified = _socket("d", strong_pn=_PN)
        separate = storm.consider([foreign, identified], "d", 1.0)
        self.assertEqual(separate.session_ids, ())

    def test_probing_pn_less_sibling_of_live_routed_payload_is_reset(self) -> None:
        storm = CallbackSocketStorm()
        identified = _socket("known", strong_pn=_PN, routed_payload=True)
        probing = _socket("probe")
        action = storm.consider([identified, probing], "known", 0.0)
        self.assertEqual(action.reason, "keep_candidate")
        self.assertEqual(action.session_ids, ("probe",))
        parked = _socket("parked", strong_pn=_PN)
        left = CallbackSocketStorm().consider([parked, _socket("fresh")], "fresh", 0.0)
        self.assertEqual(left.session_ids, ())

    def test_older_pn_less_socket_is_not_a_sibling_of_a_later_routed_payload(self) -> None:
        storm = CallbackSocketStorm()
        older = _socket("stale", accepted_at=1.0)
        routed = _socket(
            "known", strong_pn=_PN, routed_payload=True, accepted_at=5.0
        )
        action = storm.consider([older, routed], "known", 5.0)
        self.assertEqual(action.session_ids, ())
        newer = _socket("probe", accepted_at=6.0)
        action = storm.consider([older, routed, newer], "known", 6.0)
        self.assertEqual(action.session_ids, ("probe",))

    def test_pn_less_sibling_warning_uses_pn_unknown_and_peer_ip(self) -> None:
        storm = CallbackSocketStorm()
        identified = _socket(
            "known",
            strong_pn=_PN,
            last_framed_exchange_at=0.0,
            routed_payload=True,
        )
        sibling = _socket("sib")
        with self.assertLogs(_LOG, level="WARNING") as caught:
            action = storm.consider([identified, sibling], "sib", 1.0)
        self.assertEqual(action.reason, "keep_healthy")
        self.assertEqual(action.session_ids, ("sib",))
        self.assertIn("pn=pn-unknown", caught.output[0])
        self.assertIn("peer=203.0.113.10", caught.output[0])
        self.assertNotIn(f"pn={_PN}", caught.output[0])

    def test_different_strong_pn_is_left_alone(self) -> None:
        storm = CallbackSocketStorm()
        storm.consider(
            [_socket("a", strong_pn=_PN), _socket("b", strong_pn=_PN)],
            "b",
            0.0,
        )
        other = storm.consider(
            [_socket("c", strong_pn=_OTHER_PN)],
            "c",
            1.0,
        )
        self.assertEqual(other.session_ids, ())
        self.assertEqual(other.reason, "")
        mixed = storm.consider(
            [
                _socket("keep", strong_pn=_PN, routed_payload=True),
                _socket("other", strong_pn=_OTHER_PN),
            ],
            "other",
            1.0,
        )
        self.assertEqual(mixed.session_ids, ())
        self.assertEqual(mixed.reason, "")

    def test_abort_is_rst_not_a_graceful_close(self) -> None:
        class _Transport:
            def __init__(self) -> None:
                self.aborted = False

            def abort(self) -> None:
                self.aborted = True

        class _LingerSocket:
            def __init__(self) -> None:
                self.option: tuple[int, int, bytes] | None = None

            def setsockopt(self, level: int, option: int, value: bytes) -> None:
                self.option = (level, option, value)

        class _Writer:
            def __init__(self, sock: _LingerSocket) -> None:
                self.transport = _Transport()
                self._sock = sock
                self.closed = False

            def get_extra_info(self, name: str) -> object:
                return self._sock if name == "socket" else None

            def close(self) -> None:
                self.closed = True

        linger_sock = _LingerSocket()
        writer = _Writer(linger_sock)
        reset_tcp_writer(writer)
        self.assertTrue(writer.transport.aborted)
        self.assertFalse(writer.closed)
        self.assertEqual(
            linger_sock.option,
            (socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)),
        )

        class _LingerWriter:
            def __init__(self, sock: _LingerSocket) -> None:
                self.transport = None
                self._sock = sock
                self.closed = False

            def get_extra_info(self, name: str) -> object:
                return self._sock if name == "socket" else None

            def close(self) -> None:
                self.closed = True

        linger_sock = _LingerSocket()
        linger_writer = _LingerWriter(linger_sock)
        reset_tcp_writer(linger_writer)
        self.assertEqual(
            linger_sock.option,
            (socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)),
        )
        self.assertTrue(linger_writer.closed)


class CallbackSocketStormListenerTests(unittest.IsolatedAsyncioTestCase):
    async def test_listen_socket_is_not_closed(self) -> None:
        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        await listener.acquire()
        try:
            listen_sock = listener._server.sockets[0]
            self.assertGreaterEqual(listen_sock.fileno(), 0)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            session_id = await _wait_session_id(listener)
            listener._mark_session_identity(session_id, _PN, "at_dtupn")
            listener._mark_session_state(session_id, "routed_framed")
            payload = SimpleNamespace(
                _writer=writer,
                _reader=reader,
                connected=True,
                collector_info=SimpleNamespace(remote_ip="127.0.0.1"),
                last_framed_exchange_monotonic=None,
            )

            async def _disconnect() -> None:
                payload.connected = False

            payload.disconnect = _disconnect
            listener._session_payload_connections[session_id] = payload
            self.assertFalse(listener._socket_storm_reset(session_id))

            _second_reader, second_writer = await asyncio.open_connection(
                "127.0.0.1",
                port,
            )
            extra_id = await _wait_until(
                lambda: _other_session_id(listener, session_id),
            )
            await _wait_until(lambda: listener._socket_storm_reset(extra_id))
            self.assertFalse(listener._socket_storm_reset(session_id))
            await _expect_rst(_second_reader)
            self.assertGreaterEqual(listen_sock.fileno(), 0)
            self.assertFalse(getattr(listen_sock, "_closed", False))

            _third_reader, third_writer = await asyncio.open_connection(
                "127.0.0.1",
                port,
            )
            self.assertGreaterEqual(listen_sock.fileno(), 0)
            third_writer.close()
            second_writer.close()
            writer.close()
        finally:
            await listener.release()


async def _expect_rst(reader: asyncio.StreamReader) -> None:
    """Read until the peer observes a TCP reset, not a graceful FIN."""

    try:
        while True:
            data = await asyncio.wait_for(reader.read(64), timeout=1.0)
            if data == b"":
                raise AssertionError("peer saw a graceful FIN")
    except ConnectionResetError:
        return


def _other_session_id(listener: _SharedEybondListener, exclude: str) -> str:
    for entry in listener._session_inventory.values():
        if entry.session_id != exclude:
            return entry.session_id
    return ""


async def _wait_session_id(listener: _SharedEybondListener) -> str:
    def ready() -> str:
        for entry in listener._session_inventory.values():
            if entry.state != "closed_socket_storm":
                return entry.session_id
        return ""

    await _wait_until(lambda: bool(ready()))
    return ready()


async def _wait_until(predicate):
    for _ in range(50):
        value = predicate()
        if value:
            return value
        await asyncio.sleep(0.02)
    raise AssertionError("timed out waiting for callback socket state")


if __name__ == "__main__":
    unittest.main()
