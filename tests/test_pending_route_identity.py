"""An application send must not claim an unidentified socket from peer IP alone."""
from __future__ import annotations

import asyncio
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE, build_collector_request, decode_header,
)
from custom_components.eybond_local.collector.transport import (
    SharedEybondTransport, _PendingCollectorSocket, _SharedEybondListener,
)


class _Writer:
    def is_closing(self):
        return False


class PendingRouteIdentityTests(unittest.IsolatedAsyncioTestCase):
    def pending(self):
        listener = _SharedEybondListener(host="127.0.0.1", port=0)
        listener._remember_session(session_id="new-socket", remote_ip="192.0.2.8", remote_port=40000)
        pending = _PendingCollectorSocket(
            session_id="new-socket", remote_ip="192.0.2.8", remote_port=40000,
            reader=asyncio.StreamReader(), writer=_Writer(),
        )
        listener._pending_sockets[pending.session_id] = pending
        return listener, pending

    async def test_silent_ip_and_protocol_hint_do_not_claim_or_pause_watcher(self):
        for protocol in ("", "eybond_framed", "at_text"):
            with self.subTest(protocol=protocol):
                listener, pending = self.pending()
                with patch.object(listener, "_pause_pending_sniff", new_callable=AsyncMock) as pause:
                    claimed = await listener.pop_pending_socket_for_route(
                        collector_ip="192.0.2.8", session_protocol=protocol)
                self.assertIsNone(claimed)
                pause.assert_not_awaited()
                self.assertTrue(listener._pending_socket_still_registered(pending))

    async def test_exact_session_authority_still_claims_silent_socket(self):
        listener, pending = self.pending()
        self.assertIs(await listener.pop_pending_socket_for_route(session_id=pending.session_id), pending)

    async def test_current_framed_bytes_allow_legacy_ip_route(self):
        listener, pending = self.pending()
        pending.initial_bytes = build_collector_request(1, b"", devcode=0x0994, collector_addr=1, fcode=4)
        self.assertIs(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"), pending)

    async def test_strong_identity_without_named_owner_allows_legacy_ip_route(self):
        listener, pending = self.pending()
        listener._mark_session_identity(pending.session_id, "V001107SYN82291016", "at_dtupn")
        self.assertIs(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"), pending)

    async def test_named_owner_cannot_be_stolen_by_ip_only_application(self):
        for register in ("register_payload_pn_owner", "register_at_pn_owner"):
            with self.subTest(register=register):
                listener, pending = self.pending()
                pn = "V001107SYN82291016"
                getattr(listener, register)(pn)
                listener._mark_session_identity(pending.session_id, pn, "fc2_parameter_2")
                with patch.object(listener, "_pause_pending_sniff", new_callable=AsyncMock) as pause:
                    self.assertIsNone(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"))
                pause.assert_not_awaited()
                self.assertTrue(listener._pending_socket_still_registered(pending))
                self.assertIs(await listener.pop_pending_socket_for_route(collector_pn=pn), pending)

    async def test_unrelated_owner_does_not_block_ip_only_claim_of_unowned_pn(self):
        for register in ("register_payload_pn_owner", "register_at_pn_owner"):
            with self.subTest(register=register):
                listener, pending = self.pending()
                getattr(listener, register)("V001107SYN99887766")
                listener._mark_session_identity(pending.session_id, "V001107SYN82291016", "fc2_parameter_2")
                self.assertIs(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"), pending)

    async def test_weak_pn_matching_owner_is_not_taken_by_ip_only_application(self):
        owner_pn = "V001107SYN82291016"
        # Ten characters is CALLBACK_PN_PREFIX_MATCH_MIN_LEN: the shortest prefix that matches.
        weak_pn = owner_pn[:10]
        for register in ("register_payload_pn_owner", "register_at_pn_owner"):
            with self.subTest(register=register):
                listener, pending = self.pending()
                getattr(listener, register)(owner_pn)
                pending.initial_bytes = build_collector_request(1, b"", devcode=0x0994, collector_addr=1, fcode=4)
                listener._mark_session_first_bytes(pending.session_id, pending.initial_bytes)
                listener._mark_session_identity(pending.session_id, weak_pn, "framed_heartbeat")
                self.assertTrue(listener._collector_pn_matches(owner_pn, weak_pn))
                with patch.object(listener, "_pause_pending_sniff", new_callable=AsyncMock) as pause:
                    self.assertIsNone(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"))
                pause.assert_not_awaited()
                self.assertTrue(listener._pending_socket_still_registered(pending))
                self.assertIs(await listener.pop_pending_socket_for_route(collector_pn=owner_pn), pending)

    async def test_unowned_pn_with_framed_bytes_is_claimed_by_ip_only(self):
        owner_pn = "V001107SYN82291016"
        cases = (
            ("weak", "V009999SYN", "framed_heartbeat"),
            ("strong", "V009999SYN82291016", "fc2_parameter_2"),
        )
        for label, pn, source in cases:
            with self.subTest(source=label):
                listener, pending = self.pending()
                listener.register_payload_pn_owner(owner_pn)
                pending.initial_bytes = build_collector_request(1, b"", devcode=0x0994, collector_addr=1, fcode=4)
                listener._mark_session_identity(pending.session_id, pn, source)
                self.assertFalse(listener._collector_pn_matches(owner_pn, pn))
                self.assertIs(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"), pending)

    async def _ip_claim_across_sniff_drain(self, register):
        listener, pending = self.pending()
        pn = "V001107SYN82291016"
        listener.register_payload_pn_owner("V001107SYN99887766")
        listener._mark_session_identity(pending.session_id, pn, "fc2_parameter_2")
        started, draining, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def sniff():
            started.set()
            try:
                await asyncio.get_running_loop().create_future()
            finally:
                draining.set()
                await release.wait()

        pending.sniff_task = sniff_task = asyncio.create_task(sniff())
        claim = resumed = None
        try:
            await started.wait()
            claim = asyncio.create_task(listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"))
            await draining.wait()
            self.assertFalse(claim.done())
            if register is not None:
                getattr(listener, register)(pn)
            release.set()
            claimed = await claim
            resumed = pending.sniff_task
            return listener, pending, pn, claimed, resumed
        finally:
            release.set()
            for task in (claim, sniff_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(t for t in (claim, sniff_task) if t is not None), return_exceptions=True)

    async def test_owner_registered_during_sniff_drain_keeps_socket_from_ip_only_claim(self):
        for register in ("register_payload_pn_owner", "register_at_pn_owner"):
            with self.subTest(register=register):
                listener, pending, pn, claimed, resumed = await self._ip_claim_across_sniff_drain(register)
                try:
                    self.assertIsNone(claimed)
                    self.assertTrue(listener._pending_socket_still_registered(pending))
                    self.assertIsNotNone(resumed)
                    self.assertFalse(resumed.done())
                    self.assertIs(await listener.pop_pending_socket_for_route(collector_pn=pn), pending)
                finally:
                    if resumed is not None:
                        resumed.cancel()
                        await asyncio.gather(resumed, return_exceptions=True)

    async def test_ip_only_claim_across_sniff_drain_without_matching_owner(self):
        listener, pending, _pn, claimed, resumed = await self._ip_claim_across_sniff_drain(None)
        self.assertIs(claimed, pending)
        self.assertIsNone(resumed)
        self.assertFalse(listener._pending_socket_still_registered(pending))

    async def _ip_claim_during_split_frame_sniff(self, split):
        listener, pending = self.pending()
        listener._mark_session_identity(pending.session_id, "V001107SYN82291016", "fc2_parameter_2")
        frame = build_collector_request(1, b"SYNTHETIC", devcode=0x0994, collector_addr=1, fcode=4)
        if split is None:
            split = len(frame)
        pending.reader.feed_data(frame[:split])
        pending.sniff_task = sniff_task = asyncio.create_task(listener._sniff_pending_socket(pending))
        try:
            deadline = asyncio.get_running_loop().time() + 1
            while pending.reader._buffer:
                self.assertLess(asyncio.get_running_loop().time(), deadline)
                await asyncio.sleep(.01)
            await asyncio.sleep(.01)
            claimed = await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8")
            self.assertIs(claimed, pending)
            replay = pending.initial_bytes
            if split < len(frame):
                pending.reader.feed_data(frame[split:])
                replay += await asyncio.wait_for(pending.reader.read(64), 1)
            return frame, replay
        finally:
            if not sniff_task.done():
                sniff_task.cancel()
            await asyncio.gather(sniff_task, return_exceptions=True)

    async def test_ip_claim_inside_split_frame_sniff_replays_from_header(self):
        frame, replay = await self._ip_claim_during_split_frame_sniff(HEADER_SIZE + 3)
        self.assertEqual(replay[:HEADER_SIZE], frame[:HEADER_SIZE])
        self.assertEqual(replay, frame)

    async def test_ip_claim_after_buffered_full_frame_replays_from_header(self):
        frame, replay = await self._ip_claim_during_split_frame_sniff(None)
        self.assertEqual(replay, frame)

    async def test_own_at_text_bytes_allow_legacy_ip_route(self):
        for via in ("initial_bytes", "inventory_shape"):
            with self.subTest(via=via):
                listener, pending = self.pending()
                chunk = b"AT+INFO\r\n"
                if via == "initial_bytes":
                    pending.initial_bytes = chunk
                else:
                    listener._mark_session_first_bytes(pending.session_id, chunk)
                    self.assertEqual(listener._session_inventory[pending.session_id].protocol_shape, "at_text")
                self.assertIs(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"), pending)

    async def test_at_text_hint_on_silent_socket_does_not_claim(self):
        listener, pending = self.pending()
        with patch.object(listener, "_pause_pending_sniff", new_callable=AsyncMock) as pause:
            self.assertIsNone(await listener.pop_pending_socket_for_route(
                collector_ip="192.0.2.8", session_protocol="at_text"))
            self.assertIsNone(await listener.pop_pending_socket_for_route(collector_ip="192.0.2.8"))
        pause.assert_not_awaited()
        self.assertTrue(listener._pending_socket_still_registered(pending))

    async def test_loopback_application_payload_never_reaches_silent_peer(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        transport = SharedEybondTransport(host="127.0.0.1", port=port,
            collector_ip="127.0.0.1", heartbeat_interval=60, request_timeout=.25)
        await transport.start()
        writer = receiver = None
        received = []
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            deadline = asyncio.get_running_loop().time() + 2
            while not transport._listener._pending_sockets:
                self.assertLess(asyncio.get_running_loop().time(), deadline)
                await asyncio.sleep(.01)

            async def receive():
                while True:
                    header = decode_header(await reader.readexactly(HEADER_SIZE))
                    payload = await reader.readexactly(header.payload_len)
                    received.append((header.fcode, payload))
                    if header.fcode == 4:
                        writer.write(build_collector_request(header.tid, b"SYNTHETIC_REPLY",
                            devcode=header.devcode, collector_addr=header.devaddr, fcode=4))
                        await writer.drain()

            receiver = asyncio.create_task(receive())
            try:
                await transport.async_send_forward(b"LOCAL_TEST_ONLY", devcode=0x0994, collector_addr=1)
            except (ConnectionError, asyncio.TimeoutError):
                pass
            self.assertFalse(any(fc == 4 for fc, _payload in received))
            self.assertFalse(transport.connected)
            self.assertTrue(transport._listener._pending_sockets)
        finally:
            if receiver:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
            if writer:
                writer.close()
                await writer.wait_closed()
            await transport.stop()
