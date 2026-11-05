"""Callback socket-storm policy: one identified collector, overlapping dials."""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import sys
import unittest
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.protocol import (
    FC_FORWARD_TO_DEVICE,
    HEADER_SIZE,
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.transport import listener as listener_module
from custom_components.eybond_local.collector.transport import socket_storm as socket_storm_module
from custom_components.eybond_local.collector.transport.listener import (
    _SharedEybondListener,
)
from custom_components.eybond_local.collector.transport.socket_storm import (
    GRACE_SECONDS,
    HEALTHY_SECONDS,
    STALE_SECONDS,
    CallbackSocketStorm,
    StormSocket,
    _Cycle,
    _pick_survivor,
    reset_tcp_writer,
)

_PN = "E5000020000000"
_OTHER_PN = "E5000099990001"
_PN_A = "E50000200000000001"
_PN_B = "E50000200000009777"
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
            kept = storm.consider([stale, fresh], "new", now + 1)
        # The exchange is 17s old, after this socket's accept, and inside 45s.
        self.assertEqual(kept.reason, "keep_healthy")
        self.assertEqual(kept.session_ids, ("new",))
        self.assertEqual(_reasons(caught.output), ["keep_healthy"])
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

    def test_quiet_redial_every_ten_seconds_does_not_grow_the_storm_set(self) -> None:
        """A redial inside each grace must not restart grace for every socket.

        The incumbent framed once before the first redial, then nothing
        frames again. The collector redials the same PN every 10 seconds.
        """

        storm = CallbackSocketStorm()
        live = {
            "a": _socket(
                "a",
                strong_pn=_PN,
                last_framed_exchange_at=0.0,
                routed_payload=True,
                accepted_at=-5.0,
            )
        }
        now = 1.0
        for index in range(1, 6):
            session_id = f"n{index}"
            live[session_id] = _socket(
                session_id,
                strong_pn=_PN,
                routed_payload=True,
                accepted_at=now,
            )
            action = storm.consider(list(live.values()), session_id, now)
            for reset_id in action.session_ids:
                live.pop(reset_id)
            if action.grace_deadline is not None and action.grace_deadline < now + 10.0:
                timer = storm.consider(
                    list(live.values()),
                    session_id,
                    action.grace_deadline,
                )
                for reset_id in timer.session_ids:
                    live.pop(reset_id)
            now += 10.0
        self.assertLessEqual(len(live), 2, sorted(live))

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

    def test_pick_survivor_at_newcomer_accept_plus_one_quiet_incumbent_exchange_before_accept_does_not_beat_newcomer(
        self,
    ) -> None:
        """A framed exchange older than the new socket must not win at accept+1.

        The incumbent stays the admitted socket for grace, and both stay up.
        After grace, replace_stale admits the newcomer. An exchange at or after
        the newcomer accept still resets the extra.
        """

        accept = 60.0
        now = accept + 1.0
        incumbent = _socket(
            "old",
            strong_pn=_PN,
            accepted_at=10.0,
            last_framed_exchange_at=accept - 0.5,
        )
        newcomer = _socket("new", strong_pn=_PN, accepted_at=accept)
        keep, reason = _pick_survivor(
            [incumbent, newcomer],
            _Cycle(pn=_PN),
            newcomer,
            now,
        )
        self.assertEqual(reason, "hold_quiet")
        self.assertEqual(keep.session_id, "old")

        storm = CallbackSocketStorm()
        with self.assertNoLogs(_LOG, level="WARNING"):
            held = storm.consider([incumbent, newcomer], "new", now)
        self.assertEqual(held.session_ids, ())
        self.assertEqual(held.reason, "hold_quiet")
        during = storm.consider(
            [incumbent, newcomer],
            "new",
            now + GRACE_SECONDS - 0.1,
        )
        self.assertEqual(during.session_ids, ())
        with self.assertLogs(_LOG, level="WARNING") as caught:
            replaced = storm.consider(
                [incumbent, newcomer],
                "new",
                now + GRACE_SECONDS,
            )
        self.assertEqual(replaced.reason, "replace_stale")
        self.assertEqual(replaced.session_ids, ("old",))
        self.assertEqual(_reasons(caught.output), ["replace_stale"])

        lived = _socket(
            "old",
            strong_pn=_PN,
            accepted_at=10.0,
            last_framed_exchange_at=accept,
        )
        action = CallbackSocketStorm().consider(
            [lived, newcomer],
            "new",
            now,
        )
        self.assertEqual(action.reason, "keep_healthy")
        self.assertEqual(action.session_ids, ("new",))

    def test_no_exchange_after_grace_keeps_routed_session_listed_second(self) -> None:
        """A pending socket listed first must not replace a live routed session.

        Neither socket has an exchange time. Quiet grace holds both, and
        after grace keep_candidate still keeps the routed session. An
        exchange that predates the newcomer still replace_stale after grace.
        """

        storm = CallbackSocketStorm()
        pending = _socket("pending", strong_pn=_PN, accepted_at=2.0)
        routed = _socket(
            "routed",
            strong_pn=_PN,
            routed_payload=True,
            accepted_at=1.0,
        )
        held = storm.consider([pending, routed], "pending", 10.0)
        self.assertEqual(held.session_ids, ())
        self.assertEqual(held.reason, "hold_quiet")
        with self.assertLogs(_LOG, level="WARNING") as caught:
            kept = storm.consider(
                [pending, routed],
                "pending",
                10.0 + GRACE_SECONDS,
            )
        self.assertEqual(kept.reason, "keep_candidate")
        self.assertEqual(kept.session_ids, ("pending",))
        self.assertNotIn("routed", kept.session_ids)
        self.assertEqual(_reasons(caught.output), ["keep_candidate"])

        quiet = _socket(
            "old",
            strong_pn=_PN,
            routed_payload=True,
            accepted_at=1.0,
            last_framed_exchange_at=2.0,
        )
        newcomer = _socket("new", strong_pn=_PN, accepted_at=3.0)
        quiet_storm = CallbackSocketStorm()
        self.assertEqual(
            quiet_storm.consider([quiet, newcomer], "new", 4.0).session_ids,
            (),
        )
        replaced = quiet_storm.consider(
            [quiet, newcomer],
            "new",
            4.0 + GRACE_SECONDS,
        )
        self.assertEqual(replaced.reason, "replace_stale")
        self.assertEqual(replaced.session_ids, ("old",))

    def test_open_grace_keeps_incumbent_when_exchange_follows_newcomer_accept(self) -> None:
        """A warm cycle must not replace_stale a post-accept exchange inside 45s.

        At t=3, A accepted at 1.0 with an exchange at 2.0 beats E accepted at
        0.5, so A is admitted until t=18. At t=17.5, C accepted at 1.5 before
        that exchange, and identified only now, must lose. The exchange is
        15.5s old and still inside the stale window.
        """

        storm = CallbackSocketStorm()
        incumbent = _socket(
            "A",
            strong_pn=_PN,
            accepted_at=1.0,
            last_framed_exchange_at=2.0,
        )
        earlier = _socket("E", strong_pn=_PN, accepted_at=0.5)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            admitted = storm.consider([incumbent, earlier], "E", 3.0)
        self.assertEqual(admitted.reason, "keep_healthy")
        self.assertEqual(admitted.session_ids, ("E",))
        self.assertEqual(_reasons(caught.output), ["keep_healthy"])

        newcomer = _socket("C", strong_pn=_PN, accepted_at=1.5)
        with self.assertLogs(_LOG, level="WARNING") as caught:
            during = storm.consider([incumbent, newcomer], "C", 17.5)
        self.assertEqual(during.reason, "keep_healthy")
        self.assertEqual(during.session_ids, ("C",))
        self.assertNotIn("A", during.session_ids)
        self.assertEqual(_reasons(caught.output), ["keep_healthy"])

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

    def test_pn_less_socket_on_routed_peer_is_not_a_sibling(self) -> None:
        """A shared IP is not ownership, even after a routed payload exists."""

        storm = CallbackSocketStorm()
        identified = _socket(
            "known", strong_pn=_PN, routed_payload=True, accepted_at=1.0
        )
        probing = _socket("probe", accepted_at=2.0)
        action = storm.consider([identified, probing], "known", 2.0)
        self.assertEqual(action.session_ids, ())
        self.assertEqual(action.reason, "")
        later = storm.consider([identified, probing], "probe", 2.0)
        self.assertEqual(later.session_ids, ())
        self.assertEqual(later.reason, "")
        parked = _socket("parked", strong_pn=_PN)
        left = CallbackSocketStorm().consider([parked, _socket("fresh")], "fresh", 0.0)
        self.assertEqual(left.session_ids, ())

    def test_pn_less_arrival_order_is_not_ownership(self) -> None:
        storm = CallbackSocketStorm()
        older = _socket("stale", accepted_at=1.0)
        routed = _socket(
            "known", strong_pn=_PN, routed_payload=True, accepted_at=5.0
        )
        action = storm.consider([older, routed], "known", 5.0)
        self.assertEqual(action.session_ids, ())
        newer = _socket("probe", accepted_at=6.0)
        action = storm.consider([older, routed, newer], "known", 6.0)
        self.assertEqual(action.session_ids, ())
        action = storm.consider([older, routed, newer], "probe", 6.0)
        self.assertEqual(action.session_ids, ())

    def test_pn_less_socket_on_routed_peer_does_not_log_a_storm(self) -> None:
        storm = CallbackSocketStorm()
        identified = _socket(
            "known",
            strong_pn=_PN,
            last_framed_exchange_at=0.0,
            routed_payload=True,
        )
        sibling = _socket("sib")
        with self.assertNoLogs(_LOG, level="WARNING"):
            action = storm.consider([identified, sibling], "sib", 1.0)
        self.assertEqual(action.session_ids, ())
        self.assertEqual(action.reason, "")

    def test_later_different_strong_pn_on_routed_peer_is_not_a_sibling(self) -> None:
        storm = CallbackSocketStorm()
        routed = _socket(
            "known", strong_pn=_PN, routed_payload=True, accepted_at=1.0
        )
        other = _socket(
            "other", strong_pn=_OTHER_PN, accepted_at=2.0
        )
        action = storm.consider([routed, other], "other", 2.0)
        self.assertEqual(action.session_ids, ())
        self.assertEqual(action.reason, "")
        reverse = storm.consider([routed, other], "known", 2.0)
        self.assertEqual(reverse.session_ids, ())
        self.assertEqual(reverse.reason, "")

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
            # Same strong PN, presented on the wire. Accept alone must not
            # reset this socket. The first socket has not framed since this
            # one was accepted, so identification keeps both through grace.
            second_writer.write(_fc2_parameter_2(2, _PN))
            await second_writer.drain()
            extra_id = await _wait_until(
                lambda: _other_session_id(listener, session_id),
            )
            await _wait_until(
                lambda: listener._session_inventory[extra_id].collector_pn == _PN
            )
            self.assertFalse(listener._socket_storm_reset(extra_id))
            self.assertFalse(listener._socket_storm_reset(session_id))
            await _wait_until(
                lambda: any(
                    pending.session_id == extra_id
                    for pending in listener._pending_sockets.values()
                )
                or listener.payload_connection_for_session(extra_id) is not None
            )

            # A framed exchange after the second socket arrived still keeps
            # one survivor and RSTs the extra.
            payload.last_framed_exchange_monotonic = monotonic()
            listener._apply_socket_storm(extra_id)
            self.assertTrue(listener._socket_storm_reset(extra_id))
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

    async def test_later_different_fc2_pn_on_same_ip_stays_routed(self) -> None:
        """A later collector behind the same NAT must still be identified.

        Both clients use 127.0.0.1. A is already routed_framed from FC2
        parameter 2. B then sends a different full FC2 PN. Sharing the peer
        IP is not ownership, so B must not be reset before that PN is read.
        One framed exchange on A after that identification must leave B
        routed. Quiet grace would hide a same-IP grouping until it ended.
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_payload_pn_owner(_PN_A)
        listener.register_payload_pn_owner(_PN_B)
        await listener.acquire()
        writer_a = writer_b = None
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(_fc2_parameter_2(1, _PN_A))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN_A and entry.state == "routed_framed"
                ),
            )
            self.assertEqual(entry_a.collector_identity_source, "fc2_parameter_2")
            connection_a = await _wait_connected(listener, entry_a.session_id)

            _reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(_fc2_parameter_2(2, _PN_B))
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.state in {"routed_framed", "closed_socket_storm"},
            )
            self.assertEqual(
                (entry_b.collector_pn, entry_b.collector_identity_source, entry_b.state),
                (_PN_B, "fc2_parameter_2", "routed_framed"),
                _session_snapshot(listener),
            )
            self.assertEqual(entry_a.state, "routed_framed")
            self.assertEqual(entry_a.collector_pn, _PN_A)
            self.assertNotEqual(entry_a.session_id, entry_b.session_id)
            await _complete_framed_exchange(reader_a, writer_a, connection_a)
            self.assertEqual(
                entry_b.state,
                "routed_framed",
                _session_snapshot(listener),
            )
            self.assertEqual(entry_a.state, "routed_framed", _session_snapshot(listener))
        finally:
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_quiet_same_pn_redial_is_not_reset_when_incumbent_exchange_predates_accept(
        self,
    ) -> None:
        """Half-open incumbent: last framed exchange is before the redial accept.

        A real listener must keep both at identification. Grace is then driven
        through the monotonic clock passed to storm.consider, without a 45s sleep.
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_payload_pn_owner(_PN)
        await listener.acquire()
        writer_a = writer_b = None
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(_fc2_parameter_2(1, _PN))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN and entry.state == "routed_framed"
                ),
            )
            connection_a = await _wait_connected(listener, entry_a.session_id)
            exchange_at = await _complete_framed_exchange(
                reader_a,
                writer_a,
                connection_a,
            )
            self.assertIsNotNone(exchange_at)

            reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(_fc2_parameter_2(2, _PN))
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.collector_pn == _PN
                and entry.state in {"routed_framed", "closed_socket_storm"},
            )
            self.assertLess(exchange_at, entry_b.accepted_monotonic)
            self.assertNotEqual(
                entry_b.state,
                "closed_socket_storm",
                _session_snapshot(listener),
            )
            self.assertEqual(entry_a.state, "routed_framed")
            self.assertFalse(listener._socket_storm_reset(entry_a.session_id))
            self.assertFalse(listener._socket_storm_reset(entry_b.session_id))
            await _wait_connected(listener, entry_b.session_id)

            stamp = listener_module.monotonic()
            listener_module.monotonic = lambda: stamp + GRACE_SECONDS
            try:
                listener._apply_socket_storm(entry_b.session_id)
            finally:
                listener_module.monotonic = monotonic
            self.assertEqual(
                listener._session_inventory[entry_a.session_id].state,
                "closed_socket_storm",
            )
            self.assertNotEqual(
                listener._session_inventory[entry_b.session_id].state,
                "closed_socket_storm",
            )
            self.assertGreaterEqual(listener._server.sockets[0].fileno(), 0)
            await _expect_rst(reader_a)
        finally:
            listener_module.monotonic = monotonic
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_live_incumbent_stays_in_storm_set_after_inventory_churn(
        self,
    ) -> None:
        """A live routed socket must stay visible to the storm after churn.

        A is routed with a framed exchange. The collector then opens and
        drops more short PN-less dials than the session inventory keeps.
        B redials the same FC2 PN. After grace, the half-open A must be
        reset and B kept, exactly as without the churn.
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_payload_pn_owner(_PN_A)
        await listener.acquire()
        writer_a = writer_b = None
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(_fc2_parameter_2(1, _PN_A))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN_A and entry.state == "routed_framed"
                ),
            )
            connection_a = await _wait_connected(listener, entry_a.session_id)
            await _complete_framed_exchange(reader_a, writer_a, connection_a)

            for _ in range(listener._MAX_SESSION_INVENTORY):
                _churn_reader, churn_writer = await asyncio.open_connection(
                    "127.0.0.1",
                    port,
                )
                churn_writer.close()
            await _wait_until(
                lambda: listener._session_seq > listener._MAX_SESSION_INVENTORY
            )
            self.assertTrue(connection_a.connected)

            reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(_fc2_parameter_2(2, _PN_A))
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.collector_pn == _PN_A
                and entry.state in {"routed_framed", "closed_socket_storm"},
            )
            await _wait_connected(listener, entry_b.session_id)
            self.assertIn(
                entry_a.session_id,
                {item.session_id for item in listener._storm_sockets()},
                _session_snapshot(listener),
            )

            stamp = listener_module.monotonic()
            listener_module.monotonic = lambda: stamp + GRACE_SECONDS
            try:
                listener._apply_socket_storm(entry_b.session_id)
            finally:
                listener_module.monotonic = monotonic
            await _expect_rst(reader_a)
        finally:
            listener_module.monotonic = monotonic
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_quiet_same_pn_grace_end_rechecks_without_another_pn(self) -> None:
        """Grace end must re-check on its own. No second PN, no direct storm call.

        A completes one framed exchange, then B redials the same FC2 PN. Both
        stay up through the quiet grace. When that grace elapses the old
        socket is reset and the newcomer stays routed.
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_payload_pn_owner(_PN_A)
        await listener.acquire()
        writer_a = writer_b = None
        previous_grace = socket_storm_module.GRACE_SECONDS
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(_fc2_parameter_2(1, _PN_A))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN_A and entry.state == "routed_framed"
                ),
            )
            connection_a = await _wait_connected(listener, entry_a.session_id)
            exchange_at = await _complete_framed_exchange(
                reader_a,
                writer_a,
                connection_a,
            )
            self.assertIsNotNone(exchange_at)

            socket_storm_module.GRACE_SECONDS = 0.4
            reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(_fc2_parameter_2(2, _PN_A))
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.collector_pn == _PN_A
                and entry.state in {"routed_framed", "closed_socket_storm"},
            )
            self.assertLess(exchange_at, entry_b.accepted_monotonic)
            self.assertEqual(entry_a.state, "routed_framed", _session_snapshot(listener))
            self.assertEqual(entry_b.state, "routed_framed", _session_snapshot(listener))
            await asyncio.sleep(socket_storm_module.GRACE_SECONDS + 0.3)
            self.assertEqual(
                entry_a.state,
                "closed_socket_storm",
                _session_snapshot(listener),
            )
            self.assertEqual(entry_b.state, "routed_framed", _session_snapshot(listener))
            self.assertGreaterEqual(listener._server.sockets[0].fileno(), 0)
            await _expect_rst(reader_a)
        finally:
            socket_storm_module.GRACE_SECONDS = previous_grace
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_same_pn_at_redial_identity_line_keeps_quiet_grace(self) -> None:
        """A same-PN AT redial whose only bytes are DTUPN must honor quiet grace.

        Socket A sends AT+DTUPN and is routed. About 0.3s later B sends the
        same line. That prefix is identification, so A stays routed through
        the patched grace. Pre-fix failure:

        AssertionError: 'closed_socket_storm' != 'routed_at_text'
        pn='E50000200000000001' source='at_dtupn' state=closed_socket_storm
        Callback socket storm pn=E50000200000000001 peer=127.0.0.1 closed_sessions=listener-52232-1 reason=keep_healthy
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_at_pn_owner(_PN_A)
        await listener.acquire()
        writer_a = writer_b = None
        previous_grace = socket_storm_module.GRACE_SECONDS
        captured: list[str] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record.getMessage())

        storm_log = logging.getLogger(_LOG)
        previous_level = storm_log.level
        handler = _Capture()
        handler.setLevel(logging.WARNING)
        storm_log.setLevel(logging.WARNING)
        storm_log.addHandler(handler)
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(f"AT+DTUPN:{_PN_A}\r\n".encode("ascii"))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN_A and entry.state == "routed_at_text"
                ),
            )
            self.assertEqual(entry_a.collector_identity_source, "at_dtupn")
            connection_a = await _wait_at_connected(listener, entry_a.session_id)
            await _wait_until(
                lambda: connection_a.collector_info.collector_pn == _PN_A
            )

            await asyncio.sleep(0.3)
            socket_storm_module.GRACE_SECONDS = 1.0
            _reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(f"AT+DTUPN:{_PN_A}\r\n".encode("ascii"))
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.collector_pn == _PN_A
                and entry.state in {"routed_at_text", "closed_socket_storm"},
            )
            connection_b = await _wait_at_connected(listener, entry_b.session_id)
            await _wait_until(
                lambda: connection_b.collector_info.collector_pn == _PN_A
            )
            # Inside the patched grace, and past the pre-fix reset at ~0.3s.
            await asyncio.sleep(0.35)
            detail = f"{_session_snapshot(listener)} logs={captured}"
            self.assertEqual(entry_a.state, "routed_at_text", detail)
            self.assertEqual(entry_b.state, "routed_at_text", detail)
            self.assertNotIn("keep_healthy", " ".join(captured), detail)
            self.assertIsNone(connection_a.last_framed_exchange_monotonic)
            self.assertIsNone(connection_b.last_framed_exchange_monotonic)
            self.assertTrue(listener._socket_storm_rechecks, detail)
            self.assertGreaterEqual(listener._server.sockets[0].fileno(), 0)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader_a.read(1), timeout=0.05)
        finally:
            storm_log.removeHandler(handler)
            storm_log.setLevel(previous_level)
            socket_storm_module.GRACE_SECONDS = previous_grace
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_split_same_pn_at_redial_identity_line_keeps_quiet_grace(self) -> None:
        """A same-PN AT redial whose DTUPN line is split across reads keeps grace.

        Socket A sends a complete AT+DTUPN and is routed. Socket B writes
        AT+DTUPN:<pn>, waits about 50ms, then writes the CRLF. That line still
        first records B's PN, so A stays routed inside the patched grace.
        Pre-fix failure:

        AssertionError: 'closed_socket_storm' != 'routed_at_text'
        pn='E50000200000000001' source='at_dtupn' state=closed_socket_storm
        Callback socket storm pn=E50000200000000001 peer=127.0.0.1 closed_sessions=listener-57631-1 reason=keep_healthy
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_at_pn_owner(_PN_A)
        await listener.acquire()
        writer_a = writer_b = None
        previous_grace = socket_storm_module.GRACE_SECONDS
        captured: list[str] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record.getMessage())

        storm_log = logging.getLogger(_LOG)
        previous_level = storm_log.level
        handler = _Capture()
        handler.setLevel(logging.WARNING)
        storm_log.setLevel(logging.WARNING)
        storm_log.addHandler(handler)
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(f"AT+DTUPN:{_PN_A}\r\n".encode("ascii"))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN_A and entry.state == "routed_at_text"
                ),
            )
            self.assertEqual(entry_a.collector_identity_source, "at_dtupn")
            connection_a = await _wait_at_connected(listener, entry_a.session_id)
            await _wait_until(
                lambda: connection_a.collector_info.collector_pn == _PN_A
            )

            await asyncio.sleep(0.3)
            socket_storm_module.GRACE_SECONDS = 1.0
            _reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(f"AT+DTUPN:{_PN_A}".encode("ascii"))
            await writer_b.drain()
            await asyncio.sleep(0.05)
            writer_b.write(b"\r\n")
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.collector_pn == _PN_A
                and entry.state in {"routed_at_text", "closed_socket_storm"},
            )
            connection_b = await _wait_at_connected(listener, entry_b.session_id)
            await _wait_until(
                lambda: connection_b.collector_info.collector_pn == _PN_A
            )
            await asyncio.sleep(0.35)
            detail = f"{_session_snapshot(listener)} logs={captured}"
            self.assertEqual(entry_a.state, "routed_at_text", detail)
            self.assertEqual(entry_b.state, "routed_at_text", detail)
            self.assertNotIn("keep_healthy", " ".join(captured), detail)
            self.assertIsNone(connection_a.last_framed_exchange_monotonic)
            self.assertIsNone(connection_b.last_framed_exchange_monotonic)
            self.assertTrue(listener._socket_storm_rechecks, detail)
            self.assertGreaterEqual(listener._server.sockets[0].fileno(), 0)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader_a.read(1), timeout=0.05)
        finally:
            storm_log.removeHandler(handler)
            storm_log.setLevel(previous_level)
            socket_storm_module.GRACE_SECONDS = previous_grace
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_post_accept_exchange_rechecks_without_another_pn(self) -> None:
        """An exchange after the redial resets the extra without another PN."""

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        listener.register_payload_pn_owner(_PN_A)
        await listener.acquire()
        writer_a = writer_b = None
        try:
            reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port)
            writer_a.write(_fc2_parameter_2(1, _PN_A))
            await writer_a.drain()
            entry_a = await _wait_session(
                listener,
                lambda entry: (
                    entry.collector_pn == _PN_A and entry.state == "routed_framed"
                ),
            )
            connection_a = await _wait_connected(listener, entry_a.session_id)

            reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port)
            writer_b.write(_fc2_parameter_2(2, _PN_A))
            await writer_b.drain()
            entry_b = await _wait_session(
                listener,
                lambda entry: entry.session_id != entry_a.session_id
                and entry.collector_pn == _PN_A
                and entry.state in {"routed_framed", "closed_socket_storm"},
            )
            self.assertEqual(entry_a.state, "routed_framed", _session_snapshot(listener))
            self.assertEqual(entry_b.state, "routed_framed", _session_snapshot(listener))
            self.assertGreater(entry_b.accepted_monotonic, entry_a.accepted_monotonic)
            await _complete_framed_exchange(reader_a, writer_a, connection_a)
            await _wait_until(
                lambda: listener._session_inventory[entry_b.session_id].state
                == "closed_socket_storm"
            )
            self.assertEqual(entry_a.state, "routed_framed", _session_snapshot(listener))
            self.assertEqual(
                entry_b.state,
                "closed_socket_storm",
                _session_snapshot(listener),
            )
            await _expect_rst(reader_b)
        finally:
            for writer in (writer_a, writer_b):
                if writer is not None:
                    writer.close()
            await listener.release()

    async def test_no_exchange_pending_socket_does_not_reset_live_routed_at_session(
        self,
    ) -> None:
        """A pending same-PN newcomer must not replace a live routed AT session.

        Neither socket has an exchange time. The pending socket is listed
        before the AT session. When the first decision is quiet grace, time
        moves past that grace and the AT session must still stay up.
        """

        port = _free_tcp_port()
        listener = _SharedEybondListener(host="127.0.0.1", port=port)
        await listener.acquire()
        writer_at = writer_pending = None
        at_server_writer = pending_server_writer = None
        try:
            _reader_at, writer_at = await asyncio.open_connection("127.0.0.1", port)
            at_id = await _wait_session_id(listener)
            at_pending = _pending_for(listener, at_id)
            self.assertIsNotNone(at_pending)
            at_server_writer = at_pending.writer
            await listener._pause_pending_sniff(at_pending)
            listener._remove_pending_socket(at_pending)
            listener._mark_session_identity(at_id, _PN, "at_dtupn")
            listener._mark_session_state(at_id, "routed_at_text")
            at_connection = SimpleNamespace(
                _writer=writer_at,
                _reader=_reader_at,
                connected=True,
                collector_info=SimpleNamespace(remote_ip="127.0.0.1"),
                last_framed_exchange_monotonic=None,
            )

            async def _disconnect() -> None:
                at_connection.connected = False

            at_connection.disconnect = _disconnect
            listener._session_at_connections[at_id] = at_connection

            _reader_pending, writer_pending = await asyncio.open_connection(
                "127.0.0.1",
                port,
            )
            pending_id = await _wait_until(
                lambda: _other_session_id(listener, at_id),
            )
            pending = _pending_for(listener, pending_id)
            self.assertIsNotNone(pending)
            pending_server_writer = pending.writer
            await listener._pause_pending_sniff(pending)
            self.assertIn(pending_id, {
                item.session_id for item in listener._pending_sockets.values()
            })
            listener._mark_session_identity(pending_id, _PN, "at_dtupn")
            if listener._session_inventory[at_id].state != "closed_socket_storm":
                stamp = listener_module.monotonic()
                listener_module.monotonic = lambda: stamp + GRACE_SECONDS
                try:
                    listener._apply_socket_storm(pending_id)
                finally:
                    listener_module.monotonic = monotonic
            self.assertNotEqual(
                listener._session_inventory[at_id].state,
                "closed_socket_storm",
                _session_snapshot(listener),
            )
            at_socket = next(
                item
                for item in listener._storm_sockets()
                if item.session_id == at_id
            )
            self.assertTrue(at_socket.routed_payload)
            self.assertIsNone(at_socket.last_framed_exchange_at)
            at_connection = listener._session_at_connections[at_id]
            self.assertTrue(at_connection.connected)
            at_connection.connected = False
            disconnected = next(
                item
                for item in listener._storm_sockets()
                if item.session_id == at_id
            )
            self.assertFalse(disconnected.routed_payload)
            at_connection.connected = True
            listener._mark_session_state(at_id, "identified")
            identified = next(
                item
                for item in listener._storm_sockets()
                if item.session_id == at_id
            )
            self.assertFalse(identified.routed_payload)
        finally:
            listener_module.monotonic = monotonic
            for writer in (
                writer_at,
                writer_pending,
                at_server_writer,
                pending_server_writer,
            ):
                if writer is None:
                    continue
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
                except (ConnectionError, OSError, asyncio.TimeoutError):
                    pass
            await listener.release()


def _pending_for(listener: _SharedEybondListener, session_id: str):
    for pending in listener._pending_sockets.values():
        if pending.session_id == session_id:
            return pending
    return None


async def _wait_connected(listener: _SharedEybondListener, session_id: str):
    def ready():
        connection = listener.payload_connection_for_session(session_id)
        return connection if connection is not None and connection.connected else None

    await _wait_until(ready)
    return ready()


async def _wait_at_connected(listener: _SharedEybondListener, session_id: str):
    def ready():
        connection = listener.at_connection_for_session(session_id)
        return connection if connection is not None and connection.connected else None

    await _wait_until(ready)
    return ready()


async def _complete_framed_exchange(reader, writer, connection) -> float:
    """Answer one correlated FC4 so the live socket records a framed exchange."""

    send = asyncio.create_task(
        connection.async_send_collector(
            fcode=FC_FORWARD_TO_DEVICE,
            payload=b"\x10\x20",
            devcode=1,
            collector_addr=1,
            request_timeout=2.0,
        )
    )

    async def read_frame():
        header_bytes = await reader.readexactly(HEADER_SIZE)
        header = decode_header(header_bytes)
        payload = b""
        if header.payload_len:
            payload = await reader.readexactly(header.payload_len)
        return header, payload

    read_task: asyncio.Task | None = None
    try:
        while not send.done():
            read_task = asyncio.create_task(read_frame())
            done, _pending = await asyncio.wait(
                {send, read_task},
                timeout=2.0,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise AssertionError("timed out waiting for a framed exchange")
            if send in done and read_task not in done:
                read_task.cancel()
                break
            header, payload = read_task.result()
            read_task = None
            if header.fcode != FC_FORWARD_TO_DEVICE:
                continue
            writer.write(
                build_collector_request(
                    header.tid,
                    payload,
                    devcode=header.devcode,
                    collector_addr=header.devaddr,
                    fcode=header.fcode,
                )
            )
            await writer.drain()
        await send
    finally:
        if read_task is not None and not read_task.done():
            read_task.cancel()
        if not send.done():
            send.cancel()
            try:
                await send
            except (asyncio.CancelledError, Exception):
                pass
    exchange_at = connection.last_framed_exchange_monotonic
    if not isinstance(exchange_at, float):
        raise AssertionError("framed exchange was not recorded")
    return exchange_at


async def _expect_rst(reader: asyncio.StreamReader) -> None:
    """Read until the peer observes a TCP reset, not a graceful FIN."""

    try:
        while True:
            data = await asyncio.wait_for(reader.read(64), timeout=1.0)
            if data == b"":
                raise AssertionError("peer saw a graceful FIN")
    except ConnectionResetError:
        return


def _fc2_parameter_2(tid: int, pn: str) -> bytes:
    return build_collector_request(
        tid,
        b"\x00\x02" + pn.encode("ascii"),
        devcode=2376,
        collector_addr=1,
        fcode=2,
    )


def _session_snapshot(listener: _SharedEybondListener) -> str:
    rows = [
        (
            f"pn={entry.collector_pn!r} source={entry.collector_identity_source!r} "
            f"state={entry.state}"
        )
        for entry in listener._session_inventory.values()
    ]
    return " ".join(rows)


async def _wait_session(listener: _SharedEybondListener, predicate):
    matched = None

    def ready() -> bool:
        nonlocal matched
        for entry in listener._session_inventory.values():
            if predicate(entry):
                matched = entry
                return True
        return False

    try:
        await _wait_until(ready)
    except AssertionError as exc:
        raise AssertionError(_session_snapshot(listener)) from exc
    return matched


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
