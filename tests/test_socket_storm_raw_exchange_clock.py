"""Raw serial completions and the callback socket-storm liveness clock.

Maksym, PR #78 (issuecomment-6641731294): a successful raw PI30 or typed raw
Modbus exchange on an AT callback socket never reaches
``last_framed_exchange_monotonic`` or the session exchange callback, so after
a same-PN redial's quiet grace the storm policy replaces a working incumbent.

These tests drive the real ``_CollectorAtConnection`` and framed
``_CollectorConnection`` read loops and send paths over synthetic streams,
with the real ``_SharedEybondListener`` storm policy. Only the monotonic clock
is replaced, in both modules, so t=10/20/30/36 are exact. Nothing assigns the
exchange time; only the reader delivery path records it.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.protocol import (
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    HEADER_SIZE,
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.transport import (
    connections as connections_module,
)
from custom_components.eybond_local.collector.transport import listener as listener_module
from custom_components.eybond_local.collector.transport.connections import (
    _CollectorAtConnection,
    _CollectorConnection,
)
from custom_components.eybond_local.collector.transport.send_ownership import (
    SocketSendOwner,
)
from custom_components.eybond_local.collector.transport.listener import (
    _SharedEybondListener,
)
from custom_components.eybond_local.collector.transport.socket_storm import (
    GRACE_SECONDS,
    HEALTHY_SECONDS,
    STALE_SECONDS,
)
from custom_components.eybond_local.payload.modbus import (
    build_read_request,
    crc16_modbus,
)

_PN = "E50000200000000001"
_LOG = "custom_components.eybond_local.collector.transport.socket_storm"

_PI30_REQUEST = b"QPIGS\xb7\xa9\r"
_PI30_REPLY = b"(230.0 50.0 230.0 50.0 0115 0067 002\x12\x34\r"
_MODBUS_REQUEST = build_read_request(1, 0x0100, 1)


def _crc(body: bytes) -> bytes:
    return body + crc16_modbus(body).to_bytes(2, "little")


_MODBUS_REPLY = _crc(b"\x01\x03\x02\x00\x07")

_AUX_QUERY = b"\x5a\xa5\x02\x00" + bytes(16) + b"\x02"


def _aux_reply(subtype: bytes = b"\x02\x00") -> bytes:
    body = b"\xaa\xbb" + subtype + b"\x04\x3d\x00\x46" + bytes(12)
    return body + bytes([sum(body[2:]) & 255])


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _Writer:
    """Synthetic peer: answers on write; drain can be held open."""

    def __init__(self, reader: asyncio.StreamReader, peer: tuple[str, int]) -> None:
        self.reader = reader
        self.peer = peer
        self.closed = False
        self.writes: list[bytes] = []
        self.drain_gate: asyncio.Event | None = None
        self.drain_entered = asyncio.Event()
        self.responder = None
        self.eof_on_close = True

    def is_closing(self) -> bool:
        return self.closed

    def get_extra_info(self, key, default=None):
        return self.peer if key == "peername" else default

    def write(self, payload: bytes) -> None:
        self.writes.append(bytes(payload))
        responder = self.responder
        if responder is not None:
            reply = responder(bytes(payload))
            if reply:
                self.reader.feed_data(reply)

    async def drain(self) -> None:
        self.drain_entered.set()
        if self.drain_gate is not None:
            await self.drain_gate.wait()

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            if self.eof_on_close:
                self.reader.feed_eof()

    async def wait_closed(self) -> None:
        return None


async def _wait(predicate, timeout: float = 1.0) -> None:
    async def check() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(check(), timeout)


class _ClockedCase(unittest.IsolatedAsyncioTestCase):
    maxDiff = None

    async def asyncSetUp(self) -> None:
        self.clock = _Clock()
        for module in (connections_module, listener_module):
            patcher = patch.object(module, "monotonic", self.clock)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.exchanges: list[tuple[str, float]] = []

    async def _stop(self, connection, task) -> None:
        try:
            await connection.disconnect()
        except Exception:
            pass
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _run(
        self,
        connection: _CollectorAtConnection,
        session_id: str,
        *,
        peer: tuple[str, int],
        on_exchange=None,
        identity=None,
        closed=None,
        disconnect=None,
    ):
        reader = asyncio.StreamReader()
        writer = _Writer(reader, peer)

        def record(sid: str) -> None:
            self.exchanges.append((sid, self.clock.now))
            if on_exchange is not None:
                on_exchange(sid)

        task = asyncio.create_task(
            connection.run(
                reader,
                writer,
                session_id=session_id,
                session_identity_callback=identity,
                session_exchange_callback=record,
                session_closed_callback=closed,
                disconnect_callback=disconnect,
            )
        )
        self.addAsyncCleanup(self._stop, connection, task)
        await _wait(lambda: connection._writer is writer and connection.connected)
        return reader, writer, task

    def _exchanges_since(self, session_id: str, since: float) -> list[tuple[str, float]]:
        return [item for item in self.exchanges if item[0] == session_id and item[1] >= since]

    async def _drain_case(self, connection, writer, start_send, pending_future, reply_for):
        """Deliver at t=30 while the sender is inside drain(); resume at t=31.5."""

        captured: list[asyncio.Future] = []

        def responder(wire: bytes) -> bytes | None:
            reply = reply_for(wire)
            if reply is not None:
                captured.append(pending_future())
            return reply

        writer.responder = responder
        writer.drain_gate = asyncio.Event()
        self.clock.now = 30.0
        send = asyncio.create_task(start_send())
        await _wait(lambda: bool(captured) and captured[0].done())
        # Parsed and delivered while the sender is still blocked in drain().
        self.assertFalse(send.done())
        self.assertTrue(writer.drain_entered.is_set())
        receipt_at = self.clock.now
        self.clock.now = 31.5
        writer.drain_gate.set()
        response = await send
        resumed_at = self.clock.now
        self.assertEqual((receipt_at, resumed_at), (30.0, 31.5))
        observed = {
            "last_framed_exchange_monotonic": connection.last_framed_exchange_monotonic,
            "exchange_callbacks": list(self.exchanges),
        }
        expected = {
            "last_framed_exchange_monotonic": receipt_at,
            "exchange_callbacks": [("s-a", receipt_at)],
        }
        self.assertEqual(observed, expected)
        return response


class _StormRig(_ClockedCase):
    """Real listener policy with two same-PN AT sessions, no TCP server."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.listener = _SharedEybondListener(host="127.0.0.1", port=0)
        self.addCleanup(self._cancel_rechecks)
        self.storm_lines: list[str] = []
        lines = self.storm_lines

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                lines.append(record.getMessage())

        storm_log = logging.getLogger(_LOG)
        handler = _Capture(level=logging.WARNING)
        previous = storm_log.level
        storm_log.setLevel(logging.WARNING)
        storm_log.addHandler(handler)
        self.addCleanup(storm_log.setLevel, previous)
        self.addCleanup(storm_log.removeHandler, handler)

    def _cancel_rechecks(self) -> None:
        for _deadline, handle in tuple(self.listener._socket_storm_rechecks.values()):
            handle.cancel()
        self.listener._socket_storm_rechecks.clear()

    async def _open_identified(self, session_id: str, *, at: float, peer: tuple[str, int]):
        listener = self.listener
        self.clock.now = at
        listener._remember_session(
            session_id=session_id, remote_ip=peer[0], remote_port=peer[1],
        )
        connection = _CollectorAtConnection(
            remote_ip_hint=peer[0],
            write_timeout=5.0,
            raw_passthrough_bootstrap="none",
        )
        listener._session_at_connections[session_id] = connection
        listener._mark_session_state(session_id, "routed_at_text")
        reader, writer, _task = await self._run(
            connection,
            session_id,
            peer=peer,
            on_exchange=listener._apply_socket_storm,
            identity=listener._mark_session_identity,
            closed=listener._mark_socket_session_closed,
            disconnect=listener._drop_connection_indexes_for_connection,
        )
        reader.feed_data(f"AT+DTUPN:{_PN}\r\n".encode("ascii"))
        await _wait(lambda: listener._session_inventory[session_id].collector_pn == _PN)
        return connection, reader, writer

    async def _at_exchange(self, connection, writer, *, at: float) -> None:
        self.clock.now = at
        writer.responder = (
            lambda wire: b"AT+FWVER:1.2.3\r\n" if wire.startswith(b"AT+FWVER") else None
        )
        response = await connection.async_query("FWVER", request_timeout=1.0)
        writer.responder = None
        self.assertEqual(response.value, "1.2.3")

    def _fire_grace_recheck(self) -> bool:
        """Run the armed grace-end recheck, the same callable call_later holds."""

        current = self.listener._socket_storm_rechecks.get(_PN)
        if current is None:
            return False
        _deadline, handle = current
        callback, args = handle._callback, handle._args
        handle.cancel()
        callback(*args)
        return True

    def _storm_decisions(self) -> list[tuple[str, str]]:
        decisions = []
        for line in self.storm_lines:
            closed = line.split("closed_sessions=", 1)[1].split(" ", 1)[0]
            reason = line.rsplit("reason=", 1)[1].strip()
            decisions.append((reason, closed))
        return decisions

    async def _setup_overlap(self):
        incumbent, _reader_a, writer_a = await self._open_identified(
            "s-a", at=0.0, peer=("192.0.2.10", 40001),
        )
        await self._at_exchange(incumbent, writer_a, at=10.0)
        self.assertEqual(incumbent.last_framed_exchange_monotonic, 10.0)
        self.assertEqual(self.exchanges, [("s-a", 10.0)])

        _newcomer, _reader_b, _writer_b = await self._open_identified(
            "s-b", at=20.0, peer=("192.0.2.10", 40002),
        )
        deadline, _handle = self.listener._socket_storm_rechecks[_PN]
        self.assertEqual(deadline, 20.0 + GRACE_SECONDS)
        self.assertEqual(self.storm_lines, [])
        return incumbent, writer_a

    async def _raw_completion_at_30(self, incumbent, writer_a, *, request, reply, protocol):
        before = incumbent.collector_info.raw_response_count
        self.clock.now = 30.0
        writer_a.responder = lambda wire: reply if wire == request else None
        response = await incumbent.async_send_raw_payload(
            request, request_timeout=1.0, payload_protocol=protocol,
        )
        writer_a.responder = None
        return response, incumbent.collector_info.raw_response_count - before

    async def _reproduce(self, *, request, reply, protocol) -> None:
        incumbent, writer_a = await self._setup_overlap()
        response, raw_delta = await self._raw_completion_at_30(
            incumbent, writer_a, request=request, reply=reply, protocol=protocol,
        )
        clock_after_raw = incumbent.last_framed_exchange_monotonic
        callbacks_after_raw = self._exchanges_since("s-a", 30.0)

        self.clock.now = 36.0
        self._fire_grace_recheck()
        inventory = self.listener._session_inventory
        observed = {
            "response": response,
            "raw_response_count_delta": raw_delta,
            "incumbent_last_framed_exchange_monotonic": clock_after_raw,
            "incumbent_exchange_callbacks_at_or_after_t30": callbacks_after_raw,
            "storm_decisions": self._storm_decisions(),
            "incumbent_state": inventory["s-a"].state,
            "newcomer_state": inventory["s-b"].state,
        }
        expected = {
            "response": reply,
            "raw_response_count_delta": 1,
            "incumbent_last_framed_exchange_monotonic": 30.0,
            "incumbent_exchange_callbacks_at_or_after_t30": [("s-a", 30.0)],
            "storm_decisions": [("keep_healthy", "s-b")],
            "incumbent_state": "routed_at_text",
            "newcomer_state": "closed_socket_storm",
        }
        self.assertEqual(observed, expected)


class RawCompletionStormReproductionTests(_StormRig):
    def test_constants(self) -> None:
        self.assertEqual(
            (GRACE_SECONDS, STALE_SECONDS, HEALTHY_SECONDS), (15.0, 45.0, 15.0),
        )

    async def test_raw_pi30_completion_keeps_incumbent_after_grace(self) -> None:
        """Maksym's reproduction, raw PI30. Fails on PR #78 head."""

        await self._reproduce(
            request=_PI30_REQUEST, reply=_PI30_REPLY, protocol="pi30",
        )

    async def test_raw_modbus_completion_keeps_incumbent_after_grace(self) -> None:
        """Maksym's reproduction, typed raw Modbus via _runtime_modbus. Fails."""

        await self._reproduce(
            request=_MODBUS_REQUEST, reply=_MODBUS_REPLY, protocol="modbus_rtu",
        )

    async def test_positive_control_policy_keeps_incumbent_given_t30(self) -> None:
        """Same real raw completion, then the t=30 receipt time handed to the policy.

        Isolates the grace-end recheck: the exchange callback is detached so
        the t=30 delivery cannot settle the storm before the recheck runs.
        """

        incumbent, writer_a = await self._setup_overlap()
        incumbent._session_exchange_callback = None
        response, raw_delta = await self._raw_completion_at_30(
            incumbent, writer_a,
            request=_PI30_REQUEST, reply=_PI30_REPLY, protocol="pi30",
        )
        self.assertEqual((response, raw_delta), (_PI30_REPLY, 1))
        self.assertEqual(incumbent.last_framed_exchange_monotonic, 30.0)
        self.assertEqual(self._exchanges_since("s-a", 30.0), [])
        self.clock.now = 36.0
        self.assertTrue(self._fire_grace_recheck())
        self.assertEqual(self._storm_decisions(), [("keep_healthy", "s-b")])
        self.assertEqual(self.listener._session_inventory["s-a"].state, "routed_at_text")
        self.assertEqual(
            self.listener._session_inventory["s-b"].state, "closed_socket_storm",
        )


class RawCompletionNegativeTests(_StormRig):
    """None of these may refresh the clock. All pass on PR #78 head."""

    async def _pending_then_timeout(self, *, request, protocol) -> None:
        incumbent, writer_a = await self._setup_overlap()
        before_timeouts = incumbent.collector_info.raw_timeout_count
        self.clock.now = 30.0
        send = asyncio.create_task(
            incumbent.async_send_raw_payload(
                request, request_timeout=0.05, payload_protocol=protocol,
            )
        )
        await _wait(lambda: request in writer_a.writes)
        # Sent, not completed.
        self.assertEqual(incumbent.last_framed_exchange_monotonic, 10.0)
        self.assertEqual(self._exchanges_since("s-a", 30.0), [])
        with self.assertRaises(asyncio.TimeoutError):
            await send
        self.assertEqual(
            incumbent.collector_info.raw_timeout_count - before_timeouts, 1,
        )
        self.assertNotEqual(incumbent.last_framed_exchange_monotonic, 30.0)
        self.assertEqual(self._exchanges_since("s-a", 30.0), [])
        self.assertEqual(self._exchanges_since("s-b", 0.0), [])

    async def test_pending_then_timed_out_raw_pi30_does_not_refresh(self) -> None:
        await self._pending_then_timeout(request=_PI30_REQUEST, protocol="pi30")

    async def test_pending_then_timed_out_raw_modbus_does_not_refresh(self) -> None:
        await self._pending_then_timeout(request=_MODBUS_REQUEST, protocol="modbus_rtu")

    async def test_first_dtupn_identity_including_split_line_does_not_refresh(self) -> None:
        connection = _CollectorAtConnection(
            remote_ip_hint="192.0.2.20", write_timeout=1.0,
            raw_passthrough_bootstrap="none",
        )
        identities: list[tuple[str, str, str]] = []
        reader, _writer, _task = await self._run(
            connection, "s-split", peer=("192.0.2.20", 40003),
            identity=lambda *args: identities.append(args),
        )
        self.clock.now = 5.0
        reader.feed_data(f"AT+DTUPN:{_PN}".encode("ascii"))
        await asyncio.sleep(0.02)
        reader.feed_data(b"\r\n")
        await _wait(lambda: bool(identities))
        self.assertEqual(identities, [("s-split", _PN, "at_dtupn")])
        self.assertIsNone(connection.last_framed_exchange_monotonic)
        self.assertEqual(self.exchanges, [])


class ReceiptTimeDuringDrainTests(_ClockedCase):
    """The reply is delivered while the sender is still inside writer.drain().

    Delivery happens at t=30. The sender resumes at t=31.5. A fix must stamp
    the guarded reader/delivery time (30.0), never the sender wake-up.
    """

    async def _open(self) -> tuple[_CollectorAtConnection, _Writer]:
        connection = _CollectorAtConnection(
            remote_ip_hint="192.0.2.30", write_timeout=5.0,
            raw_passthrough_bootstrap="none",
        )
        _reader, writer, _task = await self._run(
            connection, "s-a", peer=("192.0.2.30", 40004),
        )
        return connection, writer

    async def test_raw_pi30_reply_during_drain_stamps_receipt_time(self) -> None:
        """Fails: raw ASCII delivery (_handle_raw_ascii_line) never stamps."""

        connection, writer = await self._open()
        await self._drain_case(
            connection, writer,
            lambda: connection.async_send_raw_payload(
                _PI30_REQUEST, request_timeout=1.0, payload_protocol="pi30",
            ),
            lambda: connection._pending_raw_response,
            lambda wire: _PI30_REPLY if wire == _PI30_REQUEST else None,
        )

    async def test_raw_modbus_reply_during_drain_stamps_receipt_time(self) -> None:
        """Fails: DiagnosticModbusSession.consume set_result never stamps."""

        connection, writer = await self._open()
        await self._drain_case(
            connection, writer,
            lambda: connection.async_send_raw_payload(
                _MODBUS_REQUEST, request_timeout=1.0, payload_protocol="modbus_rtu",
            ),
            lambda: connection._runtime_modbus.claim.future,
            lambda wire: _MODBUS_REPLY if wire == _MODBUS_REQUEST else None,
        )

    async def test_auxiliary_reply_during_drain_stamps_receipt_time(self) -> None:
        """Fails: AuxiliaryReadSession.accept set_result never stamps.

        Maksym: the receipt-time rule applies to auxiliary replies too.
        """

        connection, writer = await self._open()
        await self._drain_case(
            connection, writer,
            lambda: connection.async_send_auxiliary_read(_AUX_QUERY, request_timeout=1.0),
            lambda: connection._auxiliary_session.claim.future,
            lambda wire: _aux_reply() if wire == _AUX_QUERY else None,
        )

    async def test_mixed_fc4_reply_during_drain_stamps_receipt_time(self) -> None:
        """Passes: the mixed framed reader stamps before set_result."""

        connection, writer = await self._open()

        def reply_for(wire: bytes) -> bytes | None:
            if len(wire) < HEADER_SIZE:
                return None
            header = decode_header(wire[:HEADER_SIZE])
            if header.fcode != FC_FORWARD_TO_DEVICE:
                return None
            return build_collector_request(
                header.tid, b"\x01\x03\x02\x00\x07",
                devcode=header.devcode, collector_addr=header.devaddr,
                fcode=header.fcode,
            )

        def pending():
            (future,) = connection._pending_framed_response.values()
            return future

        await self._drain_case(
            connection, writer,
            lambda: connection.async_send_framed_inverter_payload(
                _MODBUS_REQUEST, devcode=1, collector_addr=1, request_timeout=1.0,
            ),
            pending,
            reply_for,
        )

    async def test_at_text_reply_during_drain_stamps_receipt_time(self) -> None:
        """Passes: AT text stamps in _handle_at_response_line before set_result."""

        connection, writer = await self._open()
        await self._drain_case(
            connection, writer,
            lambda: connection.async_query("FWVER", request_timeout=1.0),
            lambda: connection._pending_response,
            lambda wire: b"AT+FWVER:1.2.3\r\n" if wire.startswith(b"AT+FWVER") else None,
        )


class FramedSessionDeliveryTests(_ClockedCase):
    """Framed ``_CollectorConnection`` sessions are storm candidates as well.

    An explicit diagnostic raw Modbus read holds ``_request_lock``, so FC4
    polling cannot stamp while it runs. Its owned reply, and an owned auxiliary
    reply, must stamp at delivery like every other completed exchange.
    """

    async def _open(self) -> tuple[_CollectorConnection, asyncio.StreamReader, _Writer]:
        connection = _CollectorConnection(
            remote_ip_hint="192.0.2.50", heartbeat_interval=300.0, write_timeout=5.0,
        )
        reader, writer, _task = await self._run(
            connection, "s-a", peer=("192.0.2.50", 40009),
        )
        # The outbound FC1 heartbeat goes first, so a held drain() below
        # blocks the request under test and not the heartbeat.
        await _wait(lambda: bool(writer.writes) and not connection._write_lock.locked())
        self.assertEqual(decode_header(writer.writes[0][:HEADER_SIZE]).fcode, FC_HEARTBEAT)
        return connection, reader, writer

    def _diagnostic_read(self, connection, *, timeout: float = 1.0):
        owner = SocketSendOwner.capture(connection)
        return connection._diagnostic_modbus.send(
            connection, owner, _MODBUS_REQUEST, timeout,
        )

    async def _diagnostic_exchange_at_10(self, connection, writer) -> None:
        self.clock.now = 10.0
        writer.responder = lambda wire: _MODBUS_REPLY if wire == _MODBUS_REQUEST else None
        self.assertEqual(await self._diagnostic_read(connection), _MODBUS_REPLY)
        writer.responder = None
        self.assertEqual(self.exchanges, [("s-a", 10.0)])

    async def _auxiliary_exchange_at_10(self, connection, writer) -> None:
        self.clock.now = 10.0
        writer.responder = lambda wire: _aux_reply() if wire == _AUX_QUERY else None
        response = await connection.async_send_auxiliary_read(
            _AUX_QUERY, request_timeout=1.0,
        )
        writer.responder = None
        self.assertEqual(response, _aux_reply())
        self.assertEqual(self.exchanges, [("s-a", 10.0)])

    async def test_framed_diagnostic_raw_modbus_reply_during_drain_stamps_receipt_time(self) -> None:
        """Fails on c0af77c: the framed reader calls consume without on_delivery."""

        connection, _reader, writer = await self._open()
        response = await self._drain_case(
            connection, writer,
            lambda: self._diagnostic_read(connection),
            lambda: connection._diagnostic_modbus.claim.future,
            lambda wire: _MODBUS_REPLY if wire == _MODBUS_REQUEST else None,
        )
        self.assertEqual(response, _MODBUS_REPLY)

    async def test_framed_auxiliary_reply_during_drain_stamps_receipt_time(self) -> None:
        """Fails on c0af77c: the framed reader calls accept without on_delivery."""

        connection, _reader, writer = await self._open()
        response = await self._drain_case(
            connection, writer,
            lambda: connection.async_send_auxiliary_read(_AUX_QUERY, request_timeout=1.0),
            lambda: connection._auxiliary_session.claim.future,
            lambda wire: _aux_reply() if wire == _AUX_QUERY else None,
        )
        self.assertEqual(response, _aux_reply())

    async def test_framed_diagnostic_crc_mismatch_does_not_stamp(self) -> None:
        connection, _reader, writer = await self._open()
        await self._diagnostic_exchange_at_10(connection, writer)
        bad = _MODBUS_REPLY[:-1] + bytes([_MODBUS_REPLY[-1] ^ 0xFF])
        self.clock.now = 30.0
        writer.responder = lambda wire: bad if wire == _MODBUS_REQUEST else None
        with self.assertRaises(ConnectionError):
            await self._diagnostic_read(connection)
        self.assertEqual(
            connection._diagnostic_modbus.failure_reason, "diagnostic_raw_crc_mismatch",
        )
        self.assertEqual(self.exchanges, [("s-a", 10.0)])
        self.assertNotEqual(connection.last_framed_exchange_monotonic, 30.0)

    async def test_framed_unowned_auxiliary_reply_does_not_stamp(self) -> None:
        connection, reader, writer = await self._open()
        await self._auxiliary_exchange_at_10(connection, writer)
        self.clock.now = 30.0
        reader.feed_data(_aux_reply())
        await _wait(lambda: not connection.connected)
        self.assertEqual(
            connection.collector_info.last_disconnect_reason, "aabb_response_unowned",
        )
        self.assertEqual(self.exchanges, [("s-a", 10.0)])
        self.assertNotEqual(connection.last_framed_exchange_monotonic, 30.0)

    async def test_framed_auxiliary_subtype_mismatch_does_not_stamp(self) -> None:
        connection, _reader, writer = await self._open()
        await self._auxiliary_exchange_at_10(connection, writer)
        self.clock.now = 30.0
        writer.responder = (
            lambda wire: _aux_reply(b"\x02\x02") if wire == _AUX_QUERY else None
        )
        with self.assertRaises(ConnectionError):
            await connection.async_send_auxiliary_read(_AUX_QUERY, request_timeout=1.0)
        self.assertEqual(
            connection.collector_info.last_disconnect_reason,
            "aabb_response_subtype_mismatch",
        )
        self.assertEqual(self.exchanges, [("s-a", 10.0)])
        self.assertNotEqual(connection.last_framed_exchange_monotonic, 30.0)

    async def test_framed_inbound_heartbeat_does_not_stamp(self) -> None:
        connection, reader, writer = await self._open()
        await self._diagnostic_exchange_at_10(connection, writer)
        self.clock.now = 30.0
        reader.feed_data(build_collector_request(
            0x8001, _PN[:14].encode("ascii"), devcode=0x0102, collector_addr=255,
            fcode=FC_HEARTBEAT,
        ))
        await _wait(lambda: connection._last_heartbeat_monotonic == 30.0)
        self.assertTrue(connection.connected)
        self.assertEqual(connection.last_framed_exchange_monotonic, 10.0)
        self.assertEqual(self.exchanges, [("s-a", 10.0)])

    async def test_framed_diagnostic_reply_before_replacement_does_not_stamp_successor(
        self,
    ) -> None:
        """A framed diagnostic reply delivered on s-a must not refresh s-b."""

        connection, _reader, writer = await self._open()
        captured: list[asyncio.Future] = []

        def responder(wire: bytes) -> bytes | None:
            if wire != _MODBUS_REQUEST:
                return None
            claim = connection._diagnostic_modbus.claim
            if claim is not None:
                captured.append(claim.future)
            return _MODBUS_REPLY

        writer.responder = responder
        writer.drain_gate = asyncio.Event()
        self.clock.now = 30.0
        send = asyncio.create_task(self._diagnostic_read(connection))
        await _wait(lambda: bool(captured) and captured[0].done())
        self.assertFalse(send.done())
        self.assertEqual(self.exchanges, [("s-a", 30.0)])

        self.clock.now = 32.0
        _reader_2, writer_2, _task_2 = await self._run(
            connection, "s-b", peer=("192.0.2.50", 40010),
        )
        # The old diagnostic still holds the write lock inside drain(), so the
        # new session's heartbeat cannot land yet. Replacement already cleared
        # the exchange clock.
        self.assertIs(connection._writer, writer_2)
        self.assertIsNone(connection.last_framed_exchange_monotonic)

        self.clock.now = 33.0
        writer.drain_gate.set()
        with self.assertRaises(ConnectionError):
            await send
        self.assertIsNone(connection.last_framed_exchange_monotonic)
        self.assertEqual(self._exchanges_since("s-b", 0.0), [])


class ReplacedSessionTests(_ClockedCase):
    """An old AT raw reply must never stamp the session that replaced it."""

    async def _replace_during_drain(self, *, request, reply, protocol, pending) -> None:
        connection = _CollectorAtConnection(
            remote_ip_hint="192.0.2.40", write_timeout=5.0,
            raw_passthrough_bootstrap="none",
        )
        _reader_1, writer_1, _task_1 = await self._run(
            connection, "s-1", peer=("192.0.2.40", 40005),
        )
        captured: list[asyncio.Future] = []

        def responder(wire: bytes) -> bytes | None:
            if wire != request:
                return None
            captured.append(pending(connection))
            return reply

        writer_1.responder = responder
        writer_1.drain_gate = asyncio.Event()
        self.clock.now = 30.0
        send = asyncio.create_task(
            connection.async_send_raw_payload(
                request, request_timeout=1.0, payload_protocol=protocol,
            )
        )
        await _wait(lambda: bool(captured) and captured[0].done())
        self.assertFalse(send.done())
        self.assertEqual(self._exchanges_since("s-1", 30.0), [("s-1", 30.0)])

        self.clock.now = 32.0
        _reader_2, writer_2, _task_2 = await self._run(
            connection, "s-2", peer=("192.0.2.40", 40006),
        )
        self.assertIs(connection._writer, writer_2)
        self.assertIsNone(connection.last_framed_exchange_monotonic)

        self.clock.now = 33.0
        writer_1.drain_gate.set()
        with self.assertRaises(ConnectionError):
            await send
        self.assertIsNone(connection.last_framed_exchange_monotonic)
        self.assertEqual(self._exchanges_since("s-2", 0.0), [])
        self.assertEqual(connection.collector_info.raw_response_count, 0)

    async def test_raw_pi30_delivered_before_replacement_does_not_stamp_successor(self) -> None:
        """The old session is stamped at delivery. The successor is not."""

        await self._replace_during_drain(
            request=_PI30_REQUEST, reply=_PI30_REPLY, protocol="pi30",
            pending=lambda connection: connection._pending_raw_response,
        )

    async def test_raw_modbus_delivered_before_replacement_does_not_stamp_successor(self) -> None:
        await self._replace_during_drain(
            request=_MODBUS_REQUEST, reply=_MODBUS_REPLY, protocol="modbus_rtu",
            pending=lambda connection: connection._runtime_modbus.claim.future,
        )

    async def test_late_raw_reply_on_old_stream_does_not_stamp_successor(self) -> None:
        connection = _CollectorAtConnection(
            remote_ip_hint="192.0.2.41", write_timeout=5.0,
            raw_passthrough_bootstrap="none",
        )
        reader_1, writer_1, _task_1 = await self._run(
            connection, "s-1", peer=("192.0.2.41", 40007),
        )
        # Keep the old stream feedable so the late reply really reaches it.
        writer_1.eof_on_close = False
        self.clock.now = 30.0
        send = asyncio.create_task(
            connection.async_send_raw_payload(
                _PI30_REQUEST, request_timeout=1.0, payload_protocol="pi30",
            )
        )
        await _wait(lambda: _PI30_REQUEST in writer_1.writes)
        self.clock.now = 32.0
        await self._run(connection, "s-2", peer=("192.0.2.41", 40008))
        with self.assertRaises(ConnectionError):
            await send
        self.clock.now = 33.0
        self.assertFalse(reader_1.at_eof())
        reader_1.feed_data(_PI30_REPLY)
        await asyncio.sleep(0.02)
        self.assertIsNone(connection.last_framed_exchange_monotonic)
        self.assertEqual(self.exchanges, [])


if __name__ == "__main__":
    unittest.main()
