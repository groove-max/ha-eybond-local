"""Runtime RTU ownership and mixed-stream regressions; no sockets or devices."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.protocol import (
    build_collector_request, decode_header,
)
from custom_components.eybond_local.collector.transport import (
    SharedCollectorAtTransport, _CollectorAtConnection,
)
from custom_components.eybond_local.collector.transport.binary_framing import (
    runtime_eybond_header_error,
)
from custom_components.eybond_local.collector.transport.common import _PrefixedAsyncReader
from custom_components.eybond_local.link_models import AtMixedLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.payload.modbus import (
    build_read_request, build_write_multiple_request, build_write_single_request,
    crc16_modbus,
)


def _crc(body):
    return body + crc16_modbus(body).to_bytes(2, "little")


def _reply(*, slave=4, function=3, values=(7,)):
    return _crc(bytes((slave, function, len(values) * 2)) + b"".join(
        value.to_bytes(2, "big") for value in values
    ))


class _Reader(asyncio.StreamReader):
    """Expose parser progress without substituting a parser or a socket."""

    def __init__(self):
        super().__init__()
        self.consumed = 0
        self.waiting_size = None

    async def readexactly(self, size):
        self.waiting_size = size
        try:
            data = await super().readexactly(size)
            self.consumed += len(data)
            return data
        finally:
            self.waiting_size = None


class _Writer:
    def __init__(self):
        self.writes = []
        self.closed = False
        self.drain_gate = None
        self.close_gate = None
        self.on_write = None

    def write(self, wire):
        if self.closed:
            raise AssertionError("write to retired stream")
        self.writes.append(bytes(wire))
        if self.on_write is not None:
            self.on_write(bytes(wire))

    async def drain(self):
        if self.drain_gate is not None:
            await self.drain_gate.wait()

    def close(self):
        # Keep the fake reader feedable: buffered/late data must be fenced by
        # production lifetime guards, not hidden by our test writer's EOF.
        self.closed = True

    async def wait_closed(self):
        if self.close_gate is not None:
            await self.close_gate.wait()

    def is_closing(self):
        return self.closed

    def get_extra_info(self, name, default=None):
        return ("192.0.2.46", 1234) if name == "peername" else default


async def _wait(predicate):
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0)


class RawModbusLifetimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tasks = []
        self.connections = []
        self.gates = []

    async def asyncTearDown(self):
        for gate in self.gates:
            gate.set()
        for connection in self.connections:
            await connection.disconnect()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), 2)

    def _task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    def _gate(self):
        gate = asyncio.Event()
        self.gates.append(gate)
        return gate

    async def _open(self, connection=None):
        if connection is None:
            connection = _CollectorAtConnection(
                write_timeout=.2, raw_passthrough_bootstrap="none",
                raw_passthrough_frame_format="plain_line",
            )
            self.connections.append(connection)
        reader, writer = _Reader(), _Writer()
        run = self._task(connection.run(reader, writer))
        await _wait(lambda: connection._writer is writer and connection.connected)
        return connection, reader, writer, run

    def _send(self, connection, payload=None, *, timeout=.5):
        return connection.async_send_raw_payload(
            build_read_request(4, 100, 1) if payload is None else payload,
            request_timeout=timeout, payload_protocol="modbus_rtu",
        )

    async def _start(self, connection, writer, payload=None, *, timeout=.5):
        count = len(writer.writes)
        task = self._task(self._send(connection, payload, timeout=timeout))
        await _wait(lambda: len(writer.writes) > count)
        self.assertIsNone(connection._pending_raw_response)
        self.assertIsNotNone(connection._runtime_modbus.claim)
        return task

    async def _assert_run_stopped(self, run):
        result, = await asyncio.wait_for(asyncio.gather(run, return_exceptions=True), 1)
        self.assertTrue(result is None or isinstance(result, asyncio.CancelledError), repr(result))

    async def _assert_retired(self, connection, writer, run):
        self.assertTrue(writer.closed)
        self.assertFalse(connection.connected)
        self.assertTrue(connection._runtime_modbus.closed)
        self.assertTrue(connection._auxiliary_session.closed)
        self.assertIsNone(connection._runtime_modbus.claim)
        count = len(writer.writes)
        with self.assertRaises(ConnectionError):
            await self._send(connection, build_read_request(4, 200, 1))
        self.assertEqual(len(writer.writes), count)
        await self._assert_run_stopped(run)

    async def test_timeout_and_cancel_at_every_incomplete_reply_split_retire_stream(self):
        wire = _reply()
        for cancel in (False, True):
            for split in range(len(wire)):
                with self.subTest(cancel=cancel, split=split):
                    connection, reader, writer, run = await self._open()
                    pending = await self._start(connection, writer, timeout=.08)
                    reader.feed_data(wire[:split])
                    await _wait(lambda: reader.waiting_size is not None
                                and (split == 0 or reader.consumed > 0))
                    if cancel:
                        pending.cancel()
                    with self.assertRaises(asyncio.CancelledError if cancel else TimeoutError):
                        await pending
                    # Arrival after the waiter ended must neither crash a stale
                    # future nor be eligible for a second request on this socket.
                    reader.feed_data(wire[split:])
                    await self._assert_retired(connection, writer, run)
                    self.assertEqual(connection.collector_info.raw_response_count, 0)
                    self.assertEqual(connection.collector_info.raw_timeout_count, 0 if cancel else 1)
                    self.assertEqual(connection.collector_info.inverter_forward_mode, "")

    async def test_cancel_before_dispatch_has_no_claim_and_preserves_socket(self):
        for stage in ("request_lock", "write_lock", "pacing"):
            with self.subTest(stage=stage):
                connection, reader, writer, _ = await self._open()
                entered, release = self._gate(), self._gate()

                async def spacing():
                    entered.set()
                    await release.wait()
                    return 0

                lock = None if stage == "pacing" else getattr(connection, "_" + stage)
                if lock is not None:
                    await lock.acquire()
                with patch.object(connection, "_async_wait_raw_passthrough_spacing_locked", spacing
                                  if stage == "pacing" else connection._async_wait_raw_passthrough_spacing_locked):
                    try:
                        pending = self._task(self._send(connection))
                        if lock is None:
                            await asyncio.wait_for(entered.wait(), 1)
                        else:
                            await _wait(lambda: bool(lock._waiters))
                        self.assertIsNone(connection._runtime_modbus.claim)
                        self.assertIsNone(connection._pending_raw_response)
                        self.assertEqual(writer.writes, [])
                        pending.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await pending
                    finally:
                        release.set()
                        if lock is not None:
                            lock.release()
                self.assertTrue(connection.connected)
                self.assertFalse(connection._runtime_modbus.closed)
                self.assertFalse(connection._auxiliary_session.closed)
                pending = await self._start(connection, writer)
                reader.feed_data(_reply())
                self.assertEqual(await pending, _reply())

    async def test_claim_exists_at_write_and_deadline_includes_drain_budget(self):
        connection, reader, writer, _ = await self._open()
        observed = []

        def on_write(wire):
            claim = connection._runtime_modbus.claim
            self.assertIsNotNone(claim)
            self.assertEqual(claim.request, wire)
            observed.append(claim.deadline - asyncio.get_running_loop().time())
            reader.feed_data(_reply())

        writer.on_write = on_write
        self.assertEqual(await self._send(connection, timeout=.3), _reply())
        self.assertGreater(observed[0], .45)
        self.assertLessEqual(observed[0], .5)
        self.assertIsNone(connection._runtime_modbus.claim)
        self.assertIsNone(connection._diagnostic_modbus.claim)
        self.assertEqual(connection._diagnostic_modbus.received, b"")

    async def test_cancellation_during_drain_retires_dispatched_request(self):
        connection, reader, writer, run = await self._open()
        writer.drain_gate = self._gate()
        pending = await self._start(connection, writer)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        reader.feed_data(_reply())
        await self._assert_retired(connection, writer, run)

    async def test_queued_second_request_cannot_write_after_timeout_or_cancel(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                connection, reader, writer, run = await self._open()
                first = await self._start(connection, writer, timeout=.08)
                reader.feed_data(_reply()[:3])
                second = self._task(self._send(connection, build_read_request(4, 200, 1)))
                await _wait(lambda: bool(connection._request_lock._waiters))
                if cancel:
                    first.cancel()
                with self.assertRaises(asyncio.CancelledError if cancel else TimeoutError):
                    await first
                with self.assertRaises(ConnectionError):
                    await second
                self.assertEqual(writer.writes, [build_read_request(4, 100, 1)])
                reader.feed_data(_reply()[3:])
                await self._assert_retired(connection, writer, run)

    async def test_fresh_run_after_timeout_has_new_claim_and_ignores_old_stream(self):
        connection, old_reader, old_writer, old_run = await self._open()
        old_session = connection._runtime_modbus
        pending = await self._start(connection, old_writer, timeout=.04)
        with self.assertRaises(TimeoutError):
            await pending
        await self._assert_retired(connection, old_writer, old_run)
        _, reader, writer, _ = await self._open(connection)
        self.assertIsNot(connection._runtime_modbus, old_session)
        self.assertFalse(connection._runtime_modbus.closed)
        self.assertEqual(connection.collector_info.last_disconnect_reason, "")
        pending = await self._start(connection, writer, build_read_request(4, 200, 1))
        old_reader.feed_data(_reply(values=(99,)))
        await asyncio.sleep(0)
        self.assertFalse(connection._runtime_modbus.claim.future.done())
        reader.feed_data(_reply(values=(8,)))
        self.assertEqual(await pending, _reply(values=(8,)))
        self.assertEqual(connection.collector_info.raw_response_count, 1)

    async def test_short_exception_and_long_replies_at_every_split(self):
        cases = [
            (build_read_request(4, 100, 1), _reply()),
            (build_read_request(0x41, 100, 1), _reply(slave=0x41)),
            (build_read_request(4, 100, 1), _crc(b"\x04\x83\x02")),
            (build_read_request(4, 100, 3, function=4),
             _reply(function=4, values=(0x1234, 0x5678, 0x9abc))),
        ]
        for request, wire in cases:
            connection, reader, writer, _ = await self._open()
            for split in range(len(wire) + 1):
                with self.subTest(request=request.hex(), split=split):
                    pending = await self._start(connection, writer, request)
                    reader.feed_data(wire[:split])
                    await asyncio.sleep(0)
                    reader.feed_data(wire[split:])
                    # In particular, a 5/7-byte reply cannot require byte eight.
                    self.assertEqual(await asyncio.wait_for(pending, 1), wire)
                    self.assertTrue(connection.connected)

    async def test_coalesced_management_frames_preserve_raw_boundary_in_both_orders(self):
        for tid in (0x7701, 0x0401, 0x0403):
            for raw_first in (False, True):
                with self.subTest(tid=tid, raw_first=raw_first):
                    connection, reader, writer, _ = await self._open()
                    pending = await self._start(connection, writer)
                    framed = build_collector_request(
                        tid, b"", devcode=0x1200, collector_addr=1, fcode=1,
                    )
                    management = b"AT+FWVER:synthetic\r\n" + framed
                    reader.feed_data(_reply() + management if raw_first else management + _reply())
                    self.assertEqual(await pending, _reply())
                    await _wait(lambda: connection.collector_info.heartbeat_devcode == 0x1200)
                    self.assertEqual(connection.collector_info.smartess_collector_version, "synthetic")
                    self.assertEqual(connection.collector_info.raw_response_count, 1)
                    self.assertTrue(connection.connected)

    async def test_ascii_line_cannot_complete_runtime_rtu_claim(self):
        connection, reader, writer, _ = await self._open()
        pending = await self._start(connection, writer)
        claim = connection._runtime_modbus.claim
        reader.feed_data(b"(PI30\r")
        await _wait(lambda: connection.collector_info.raw_unhandled_line_count == 1)
        self.assertFalse(claim.future.done())
        self.assertIsNone(connection._pending_raw_response)
        reader.feed_data(_reply())
        self.assertEqual(await pending, _reply())

    async def test_runtime_send_can_wait_behind_an_in_progress_management_frame(self):
        for management in (
            b"AT+FWVER:synthetic\r\n",
            build_collector_request(0x0403, b"", devcode=0x1200, collector_addr=1, fcode=1),
        ):
            with self.subTest(management=management.hex()):
                connection, reader, writer, _ = await self._open()
                reader.feed_data(management[:3])
                await _wait(lambda: reader.consumed >= 3 and not connection._auxiliary_session.at_boundary)
                pending = await self._start(connection, writer)
                reader.feed_data(management[3:] + _reply())
                self.assertEqual(await pending, _reply())
                self.assertTrue(connection.connected)

    async def test_bad_crc_in_normal_and_exception_replies_is_connection_error(self):
        for wire in (_reply(), _crc(b"\x04\x83\x02")):
            with self.subTest(wire=wire.hex()):
                connection, reader, writer, run = await self._open()
                pending = await self._start(connection, writer)
                reader.feed_data(wire[:-1] + bytes((wire[-1] ^ 1,)))
                with self.assertRaises(ConnectionError):
                    await pending
                await self._assert_retired(connection, writer, run)
                self.assertEqual(connection.collector_info.last_disconnect_reason, "raw_modbus_crc_mismatch")
                self.assertEqual(connection.collector_info.raw_response_count, 0)
                self.assertEqual(connection.collector_info.inverter_forward_mode, "")

    async def test_wrong_slave_function_and_length_never_deliver_a_raw_reply(self):
        for wire in (_reply(slave=5), _reply(function=4), _reply(values=(7, 8))):
            with self.subTest(wire=wire.hex()):
                connection, reader, writer, run = await self._open()
                pending = await self._start(connection, writer, timeout=.08)
                reader.feed_data(wire)
                with self.assertRaises((ConnectionError, TimeoutError)):
                    await pending
                await self._assert_retired(connection, writer, run)
                self.assertEqual(connection.collector_info.raw_response_count, 0)

    async def test_fc06_status_value_allowed_but_address_and_fc16_count_must_match(self):
        cases = [
            (build_write_single_request(4, 354, 1), build_write_single_request(4, 354, 9), True),
            (build_write_single_request(4, 354, 1), build_write_single_request(4, 355, 9), False),
            (build_write_multiple_request(4, 354, [1, 2]), _crc(b"\x04\x10\x01\x62\x00\x02"), True),
            (build_write_multiple_request(4, 354, [1, 2]), _crc(b"\x04\x10\x01\x62\x00\x01"), False),
        ]
        for request, wire, valid in cases:
            with self.subTest(request=request.hex(), wire=wire.hex()):
                connection, reader, writer, run = await self._open()
                pending = await self._start(connection, writer, request)
                reader.feed_data(wire)
                if valid:
                    self.assertEqual(await pending, wire)
                    self.assertTrue(connection.connected)
                else:
                    with self.assertRaises(ConnectionError):
                        await pending
                    await self._assert_retired(connection, writer, run)
                    self.assertEqual(connection.collector_info.last_disconnect_reason, "raw_modbus_write_echo_mismatch")

    async def test_crc_valid_eybond_overlap_is_not_a_successful_raw_ack(self):
        wire = next(build_write_single_request(4, 354, value) for value in range(2, 4099)
                    if not runtime_eybond_header_error(decode_header(
                        build_write_single_request(4, 354, value))))
        connection, reader, writer, run = await self._open()
        pending = await self._start(connection, writer, wire)
        reader.feed_data(wire)
        with self.assertRaises(ConnectionError):
            await pending
        await self._assert_retired(connection, writer, run)
        self.assertEqual(connection.collector_info.last_disconnect_reason, "raw_modbus_eybond_ambiguous")

    async def test_old_sender_cancellation_during_replacement_cannot_close_successor(self):
        connection, old_reader, old_writer, old_run = await self._open()
        old_session, old_guard = connection._runtime_modbus, connection._auxiliary_session
        old_writer.drain_gate = self._gate()
        old_writer.close_gate = self._gate()
        old_request = await self._start(connection, old_writer)
        cleanup = self._task(connection.disconnect())
        await _wait(lambda: old_writer.closed)
        _, reader, writer, _ = await self._open(connection)
        old_request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await old_request
        self.assertTrue(old_session.closed)
        self.assertTrue(old_guard.closed)
        self.assertIsNot(connection._runtime_modbus, old_session)
        self.assertFalse(connection._runtime_modbus.closed)
        self.assertFalse(writer.closed)
        pending = await self._start(connection, writer, build_read_request(4, 200, 1))
        old_reader.feed_data(_reply(values=(99,)))
        old_writer.close_gate.set()
        await cleanup
        await self._assert_run_stopped(old_run)
        self.assertFalse(connection._runtime_modbus.claim.future.done())
        self.assertEqual(connection.collector_info.last_disconnect_reason, "")
        reader.feed_data(_reply(values=(8,)))
        self.assertEqual(await pending, _reply(values=(8,)))
        self.assertTrue(connection.connected)

    async def test_retired_parser_completed_read_cannot_mutate_fresh_session(self):
        connection, old_reader, old_writer, old_run = await self._open()
        entered, release = self._gate(), self._gate()
        original = _PrefixedAsyncReader.readexactly

        async def delayed(reader, size):
            data = await original(reader, size)
            if reader._reader is old_reader and size == 4:
                entered.set()
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        pass
            return data

        with patch.object(_PrefixedAsyncReader, "readexactly", delayed):
            old_request = await self._start(connection, old_writer)
            old_reader.feed_data(_reply(values=(99,)))
            await asyncio.wait_for(entered.wait(), 1)
            old_writer.close_gate = self._gate()
            cleanup = self._task(connection.disconnect())
            await _wait(lambda: old_writer.closed)
            with self.assertRaises(ConnectionError):
                await old_request
            _, reader, writer, _ = await self._open(connection)
            pending = await self._start(connection, writer)
            release.set()
            old_writer.close_gate.set()
            await cleanup
            await self._assert_run_stopped(old_run)
            self.assertFalse(connection._runtime_modbus.claim.future.done())
            self.assertEqual(connection.collector_info.raw_response_count, 0)
            self.assertEqual(connection.collector_info.raw_last_parser, "")
            self.assertEqual(connection.collector_info.last_disconnect_reason, "")
            reader.feed_data(_reply())
            self.assertEqual(await pending, _reply())

    def _transport(self):
        # Only the private dispatch method is used; no listener is started.
        return SharedCollectorAtTransport(
            host="127.0.0.1", port=18899, collector_ip="192.0.2.46", request_timeout=.08,
        )

    async def test_fc4_timeout_then_raw_success_ignores_late_fc4_reply(self):
        connection, reader, writer, _ = await self._open()
        request = build_read_request(4, 100, 1)
        pending = self._task(self._transport()._async_send_payload_on_connection(
            connection, request, route=AtMixedLinkRoute(devcode=1, collector_addr=1, protocol="modbus_rtu"),
            request_timeout=.08,
        ))
        await _wait(lambda: len(writer.writes) == 2)
        header = decode_header(writer.writes[0][:8])
        self.assertEqual(header.fcode, 4)
        self.assertEqual(writer.writes[0][8:], request)
        self.assertEqual(writer.writes[1], request)
        reader.feed_data(build_collector_request(
            header.tid, _reply(values=(99,)), devcode=header.devcode,
            collector_addr=header.devaddr, fcode=header.fcode,
        ) + _reply())
        self.assertEqual(await pending, _reply())
        self.assertEqual(len(writer.writes), 2)
        self.assertEqual(connection.collector_info.inverter_forward_mode, "raw_serial")

    async def test_both_mixed_attempts_timeout_without_third_attempt(self):
        connection, reader, writer, run = await self._open()
        request = build_read_request(4, 100, 1)
        with self.assertRaises(TimeoutError):
            await self._transport()._async_send_payload_on_connection(
                connection, request,
                route=AtMixedLinkRoute(devcode=1, collector_addr=1, protocol="modbus_rtu"),
                request_timeout=.04,
            )
        self.assertEqual(len(writer.writes), 2)
        self.assertEqual(writer.writes[0][8:], request)
        self.assertEqual(writer.writes[1], request)
        reader.feed_data(_reply())
        await self._assert_retired(connection, writer, run)

    async def test_fc4_timeout_fallback_cannot_migrate_to_replacement(self):
        connection, _, old_writer, old_run = await self._open()
        expired, release = self._gate(), self._gate()
        original = connection.async_send_framed_inverter_payload

        async def delayed_timeout(*args, **kwargs):
            try:
                return await original(*args, **kwargs)
            except TimeoutError:
                expired.set()
                await release.wait()
                raise

        with patch.object(connection, "async_send_framed_inverter_payload", delayed_timeout):
            pending = self._task(self._transport()._async_send_payload_on_connection(
                connection, build_read_request(4, 100, 1),
                route=AtMixedLinkRoute(devcode=1, collector_addr=1, protocol="modbus_rtu"),
                request_timeout=.04,
            ))
            await asyncio.wait_for(expired.wait(), 1)
            await connection.disconnect()
            await self._assert_run_stopped(old_run)
            _, reader, writer, _ = await self._open(connection)
            release.set()
            with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                await pending
        self.assertEqual(len(old_writer.writes), 1)
        self.assertEqual(writer.writes, [])
        self.assertFalse(writer.closed)
        self.assertIsNone(connection._runtime_modbus.claim)
        pending = await self._start(connection, writer)
        reader.feed_data(_reply())
        self.assertEqual(await pending, _reply())

    async def test_explicit_raw_route_and_unknown_mixed_write_never_try_fc4(self):
        for request, route in (
            (build_read_request(4, 100, 1), RawSerialLinkRoute(protocol="modbus_rtu")),
            (build_write_single_request(4, 354, 1),
             AtMixedLinkRoute(devcode=1, collector_addr=1, protocol="modbus_rtu")),
        ):
            with self.subTest(route=route):
                connection, _, writer, run = await self._open()
                with self.assertRaises(TimeoutError):
                    await self._transport()._async_send_payload_on_connection(
                        connection, request, route=route, request_timeout=.04,
                    )
                self.assertEqual(writer.writes, [request])
                await self._assert_retired(connection, writer, run)

    async def test_ascii_still_tries_raw_then_fc4_without_retiring_socket(self):
        connection, reader, writer, _ = await self._open()
        request = b"QPI\r"
        pending = self._task(self._transport()._async_send_payload_on_connection(
            connection, request, route=AtMixedLinkRoute(devcode=1, collector_addr=1, protocol="pi30"),
            request_timeout=.08,
        ))
        await _wait(lambda: len(writer.writes) == 2)
        self.assertEqual(writer.writes[0], request)
        header = decode_header(writer.writes[1][:8])
        self.assertEqual(header.fcode, 4)
        self.assertEqual(writer.writes[1][8:], request)
        reader.feed_data(build_collector_request(
            header.tid, b"(PI30\r", devcode=1, collector_addr=1, fcode=4,
        ))
        self.assertEqual(await pending, b"(PI30\r")
        self.assertTrue(connection.connected)
        self.assertFalse(connection._runtime_modbus.closed)


if __name__ == "__main__":
    unittest.main()
