"""Completion contract across providers, codecs and framed/raw carriers.

Synthetic provider responses exercise the contract, not vendor qualification.
The separate recorded-learning fixture covers actual SmartESS Modbus evidence.
"""
import asyncio
from copy import deepcopy
from dataclasses import replace
import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.dessmonitor_cloud import DessMonitorApiEnvelope, DessMonitorDeviceIdentity
from custom_components.eybond_local.payload.modbus import build_write_single_request, build_read_request
from custom_components.eybond_local.payload.pi30 import build_request
from custom_components.eybond_local.smartess_cloud import ApiEnvelope, SmartEssActionRejectedError, build_device_control_action
from custom_components.eybond_local.support.shadow_learning.completion import validate_completed_commands
from custom_components.eybond_local.support.shadow_learning.protocol import ModbusRtuShadowLearningAdapter, EybondGAsciiShadowLearningAdapter
from custom_components.eybond_local.support.shadow_learning.pi30_protocol import Pi30ShadowLearningAdapter
from tests.test_shadow_learning_completion import PN, ledger, close

CODECS = {
    "modbus_rtu": (ModbusRtuShadowLearningAdapter(), build_write_single_request(5, 300, 1), "modbus"),
    "pi30_ascii": (Pi30ShadowLearningAdapter(), build_request("PEa"), "pi30"),
    "eybond_g_ascii": (EybondGAsciiShadowLearningAdapter(), b"PCVV27.9\r", "ascii"),
}


def exchange(source="dessmonitor", codec="pi30_ascii", framed=True, tid=11, pn=PN):
    adapter, request, raw_carrier = CODECS[codec]
    response = adapter.build_write_exception_response(adapter.decode_write_request(request), exception_code=1)
    if framed:
        request, response = (build_collector_request(tid, p, devcode=0x994, collector_addr=255, fcode=4)
                             for p in (request, response))
    carrier = "frame" if framed else raw_carrier
    if source == "smartess":
        action = {"action": build_device_control_action(pn=pn, sn="offline", devcode=0x994,
                    devaddr=255, field_id="test", value="1")}
        envelope = ApiEnvelope(err=0, desc="ERR_NONE", raw={}, dat={"status": 0, "dat": response.hex()})
    else:
        action = {"identity": DessMonitorDeviceIdentity(pn, "offline", 0x994, 255),
                  "field_id": "test", "value": "1"}
        envelope = DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat={"status": 0, "dat": response.hex()})
    return action, request, response, envelope, carrier


def deliver(book, source="dessmonitor", codec="pi30_ascii", framed=True, tid=11):
    action, request, response, envelope, carrier = exchange(source, codec, framed, tid)
    book.begin(source, action)
    book.request(request, carrier=carrier, protocol=codec)
    book.replied(response)
    book.finish(envelope)


class CompletionAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_delivery_observation_is_bounded_and_never_a_receipt(self):
        from custom_components.eybond_local.support.shadow_learning import completion
        for arrivals, elapsed in (([], 30), ([125], 55), ([129, 158], 60)):
            with self.subTest(arrivals=arrivals), patch.object(completion, "monotonic", return_value=100) as clock:
                book = ledger()
                book.invalidate()
                book.start_closing()
                _, write, _, _, carrier = exchange("smartess", "modbus_rtu", True)
                remaining = iter(arrivals)
                async def sleep(seconds):
                    deadline = clock.return_value + seconds
                    arrival = next(remaining, None)
                    if arrival is not None:
                        self.assertLessEqual(arrival, deadline)
                        clock.return_value = arrival
                        book.request(write, carrier=carrier, protocol="modbus_rtu")
                    clock.return_value = deadline
                with patch.object(completion.asyncio, "sleep", sleep):
                    await book.observe_before_detach(uncertain_stop=True)
                self.assertEqual(clock.return_value, 100 + elapsed)
                self.assertEqual(book.unexpected_write, bool(arrivals))
                self.assertIsNone(close(book))

    async def test_vendor_failed_delivery_quiet_window_and_late_frames(self):
        from custom_components.eybond_local.support.shadow_learning import completion
        failure = SmartEssActionRejectedError(err=1, desc="ERR_FAIL(send failed)")
        for wire in ("none", "intercepted", "refused"):
            for outcome in ("quiet", "late", "forced_stop"):
                with self.subTest(wire=wire, outcome=outcome), patch.object(completion, "monotonic", return_value=100) as clock:
                    book = ledger()
                    action, req, reply, _, carrier = exchange("smartess", "modbus_rtu", True)
                    book.begin("smartess", action)
                    if wire != "none":
                        book.request(req, carrier=carrier, protocol="modbus_rtu")
                    if wire == "refused":
                        book.replied(reply)
                    book.connection_closed()
                    book.finish(failure)
                    book.start_closing()
                    self.assertEqual(book.remaining_quiet_time(), 30)
                    if outcome == "late":
                        book.connection_opened(2)
                        book.request(req, carrier=carrier, protocol="modbus_rtu")
                    elif outcome == "forced_stop":
                        book.route_detaching()
                    clock.return_value = 131
                    await book.observe_before_detach()
                    book.route_detaching()
                    records = close(book)
                    if outcome != "quiet":
                        self.assertIsNone(records)
                        continue
                    self.assertEqual(records[0]["request_hex"], "" if wire == "none" else req.hex())
                    self.assertEqual(records[0]["response_hex"], reply.hex() if wire == "refused" else "")
                    for mutation in ({"source":"dessmonitor"}, {"terminal":{"kind":"dispatch_failed","result":"ERR_TIMEOUT"}},
                                     {"response_hex":"00"}):
                        row = {**records[0], **mutation}
                        with self.assertRaises(ValueError):
                            validate_completed_commands([row])

    def test_no_local_frame_requires_exact_vendor_delivery_failure(self):
        for e in (SmartEssActionRejectedError(err=2, desc="ERR_TIMEOUT"),
                  SmartEssActionRejectedError(err=2, desc="ERR_FAIL(send failed)"),
                  SmartEssActionRejectedError(err=1, desc="ERR_FAIL"), TimeoutError(),
                  RuntimeError("ERR_FAIL(send failed)")):
            book = ledger()
            book.begin("smartess", exchange("smartess")[0])
            book.finish(e)
            self.assertIsNone(close(book))

    async def test_smg_bench_lcd_requests_record_no_fictitious_reply(self):
        from custom_components.eybond_local.payload.modbus import build_write_multiple_request
        from custom_components.eybond_local.support.shadow_learning import completion
        # Bare FC16 / register 305. Two separate submissions on the bench;
        # neither reached the inverter, and neither had a Modbus response.
        for value in (0, 1):
            with patch.object(completion, "monotonic", return_value=100) as clock:
                book = ledger()
                action = build_device_control_action(pn=PN, sn="TEST", devcode=2376, devaddr=1,
                    field_id="sys_eybond_ctrl_53", value=str(value))
                book.begin("smartess", action)
                request = build_write_multiple_request(1, 0x0131, [value])
                self.assertTrue(book.request(request, carrier="modbus", protocol="modbus_rtu"))
                book.finish(SmartEssActionRejectedError(err=1, desc="ERR_FAIL(send failed)"))
                book.start_closing()
                clock.return_value = 130
                await book.observe_before_detach()
                book.route_detaching()
                record, = close(book)
                self.assertEqual((record["request_hex"], record["response_hex"]), (request.hex(), ""))

    async def test_late_tcp_write_during_observation_is_filtered_and_blocks_restore(self):
        from custom_components.eybond_local.support.shadow_learning import completion
        from custom_components.eybond_local.support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler
        from tests.test_shadow_learning_proxy_e2e import _seed
        upstream = asyncio.Queue()
        async with await asyncio.start_server(lambda r,w: upstream.put_nowait((r,w)), "127.0.0.1", 0) as server:
            with tempfile.TemporaryDirectory() as tmp, patch.object(completion, "DISPATCH_FAILURE_QUIET_SECONDS", .1):
                h = InProcessFailClosedShadowProxyHandler(upstream_host="127.0.0.1",
                    upstream_port=server.sockets[0].getsockname()[1], seed=replace(_seed(), collector_pn=PN),
                    output_path=Path(tmp)/"test.jsonl")
                await h.start()
                try:
                    async with await asyncio.start_server(h.handle_client, "127.0.0.1", 0) as proxy, asyncio.timeout(5):
                        dev, peer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
                        ping = build_collector_request(1, b"ping", devcode=1, collector_addr=5, fcode=1)
                        peer.write(ping); await peer.drain()
                        cloud, writer = await upstream.get()
                        try:
                            self.assertEqual(await cloud.readexactly(len(ping)), ping)
                            action, request, refusal, _, _ = exchange("smartess", "modbus_rtu", True)
                            h.completion.begin("smartess", action)
                            h.completion.finish(SmartEssActionRejectedError(err=1, desc="ERR_FAIL(send failed)"))
                            h.completion.start_closing()
                            observing = asyncio.create_task(h.completion.observe_before_detach())
                            writer.write(request); await writer.drain()
                            self.assertEqual(await cloud.readexactly(len(refusal)), refusal)
                            await observing
                            await h.stop()
                            self.assertEqual(await dev.read(), b"")
                            self.assertIsNone(h.completion.completed_commands())
                        finally:
                            peer.close(); writer.close()
                            await asyncio.gather(peer.wait_closed(), writer.wait_closed())
                finally:
                    await h.stop()

    def test_all_89_recorded_smg_refusals_complete_without_api_byte_echo(self):
        data = json.loads((Path(__file__).parent / "fixtures/smartess_smg_recorded_refusals.json").read_text())
        book = ledger()
        for values in data["attempts"]:
            row = dict(zip(data["columns"], values, strict=True))
            request = bytes.fromhex(row["request_hex"])
            adapter = CODECS["modbus_rtu"][0]
            reply = adapter.build_write_exception_response(adapter.decode_write_request(request), exception_code=1)
            action = build_device_control_action(pn=PN, sn="offline", devcode=0x948, devaddr=1,
                                                field_id=row["field_id"], value=row["value"])
            book.begin("smartess", action)
            book.request(request, carrier="modbus", protocol="modbus_rtu")
            book.replied(reply)
            book.finish(SmartEssActionRejectedError(err=row["err"], desc=row["desc"]))
        records = close(book)
        self.assertEqual(len(records or []), 89, book.reason)
        validate_completed_commands(records, source="smartess")

    def test_refusal_requires_exact_typed_result_and_matching_fc16_wire(self):
        from custom_components.eybond_local.payload.modbus import build_write_multiple_request
        adapter = CODECS["modbus_rtu"][0]
        request = build_write_multiple_request(1, 300, [0])
        reply = adapter.build_write_exception_response(adapter.decode_write_request(request), exception_code=1)
        action = exchange(source="smartess")[0]
        for error in (SmartEssActionRejectedError(err=1, desc="ERR_FAIL"),
                      SmartEssActionRejectedError(err=2, desc="ERR_FAIL(Read-Only Register)"),
                      RuntimeError("ERR_FAIL(Read-Only Register)"), TimeoutError()):
            book = ledger(); book.begin("smartess", action)
            book.request(request, carrier="modbus", protocol="modbus_rtu")
            book.replied(reply); book.finish(error)
            self.assertIsNone(close(book))
        for fault in ("no_request", "no_drain", "wrong_crc", "wrong_codec"):
            book = ledger(); book.begin("smartess", action)
            if fault != "no_request":
                book.request(request, carrier="modbus", protocol="modbus_rtu")
            if fault != "no_drain":
                book.replied(reply[:-1] + bytes([reply[-1] ^ 1]) if fault == "wrong_crc"
                             else build_request("NAK") if fault == "wrong_codec" else reply)
            book.finish(SmartEssActionRejectedError(err=1, desc="ERR_FAIL(Read-Only Register)"))
            self.assertIsNone(close(book))

    def test_provider_codec_carrier_matrix(self):
        for source, codec, framed in itertools.product(("smartess", "dessmonitor"), CODECS, (True, False)):
            with self.subTest(source=source, codec=codec, framed=framed):
                book = ledger()
                deliver(book, source, codec, framed)
                deliver(book, source, codec, framed, tid=12)
                result = close(book)
                self.assertIsNotNone(result, book.reason)
                self.assertEqual(len(result), 2)
                validate_completed_commands(result, source=source)

    def test_every_matrix_path_rejects_missing_echo_and_missing_drain(self):
        for source, codec, framed, fault in itertools.product(
                ("smartess", "dessmonitor"), CODECS, (True, False), ("echo", "drain", "ack")):
            with self.subTest(source=source, codec=codec, framed=framed, fault=fault):
                book = ledger()
                action, request, response, envelope, carrier = exchange(source, codec, framed)
                book.begin(source, action)
                book.request(request, carrier=carrier, protocol=codec)
                if fault == "ack":
                    adapter, raw, _ = CODECS[codec]
                    response = adapter.build_write_ack_response(adapter.decode_write_request(raw))
                    if framed:
                        response = build_collector_request(11, response, devcode=0x994, collector_addr=255, fcode=4)
                    envelope = replace(envelope, dat={"status": 0, "dat": response.hex()})
                if fault != "drain":
                    book.replied(response)
                book.finish(replace(envelope, dat={"status": 0}) if fault == "echo" else envelope)
                self.assertIsNone(close(book))

    def test_raw_sequence_rejects_duplicate_unsolicited_overlap_or_connection_change(self):
        for fault in ("duplicate", "late", "overlap", "connection", "double_response", "wrong_provider"):
            with self.subTest(fault=fault):
                book = ledger()
                action, request, response, envelope, carrier = exchange(framed=False)
                book.begin("dessmonitor", action)
                if fault == "overlap":
                    book.begin("dessmonitor", action)
                book.request(request, carrier=carrier, protocol="pi30_ascii")
                if fault == "duplicate":
                    book.request(request, carrier=carrier, protocol="pi30_ascii")
                if fault == "connection":
                    book.connection_opened(2)
                book.replied(response)
                if fault == "double_response":
                    book.replied(response)
                book.finish(exchange("smartess", framed=False)[3] if fault == "wrong_provider" else envelope)
                if fault == "late":
                    book.request(request, carrier=carrier, protocol="pi30_ascii")
                self.assertIsNone(close(book))

    def test_idle_reconnect_does_not_revoke_terminal_commands(self):
        for framed in (True, False):
            book = ledger()
            deliver(book, framed=framed)
            book.connection_opened(2)
            deliver(book, framed=framed)  # same TID on a distinct socket
            book.connection_opened(3)     # idle close after the last command
            records = close(book)
            self.assertEqual([row["epoch"] for row in records], [1, 2])
            for bad in ((0, 1, True) if framed else (0, True)):
                changed = deepcopy(records)
                changed[1]["epoch"] = bad
                with self.assertRaises(ValueError):
                    validate_completed_commands(changed)
            reversed_epochs = deepcopy(records)
            reversed_epochs[0]["epoch"] = 3
            with self.assertRaises(ValueError):
                validate_completed_commands(reversed_epochs)

    def test_disconnected_dispatch_and_reused_epoch_cannot_complete(self):
        for reconnect in (False, True):
            book = ledger()
            book.connection_closed()
            if reconnect:
                book.connection_opened(1)
            deliver(book)
            self.assertIsNone(close(book))

    def test_closed_after_drain_still_requires_the_provider_result(self):
        for outcome in ("terminal", "timeout", "replay"):
            book = ledger()
            action, request, response, envelope, carrier = exchange()
            book.begin("dessmonitor", action)
            book.request(request, carrier=carrier, protocol="pi30_ascii")
            book.replied(response)
            book.connection_closed()
            book.connection_opened(2)
            if outcome == "replay":
                book.request(request, carrier=carrier, protocol="pi30_ascii")
            book.finish(TimeoutError() if outcome == "timeout" else envelope)
            if outcome == "terminal":
                self.assertEqual([r["epoch"] for r in close(book)], [1])
            else:
                self.assertIsNone(close(book))

    def test_preparation_reads_and_at_queries_do_not_poison_completion(self):
        for codec, read in (("pi30_ascii", build_request("QFLAG")),
                            ("modbus_rtu", build_read_request(5, 300, 1))):
            book = ledger()
            frame = build_collector_request(1, read, devcode=0x994, collector_addr=255, fcode=4)
            self.assertFalse(book.request(frame, protocol="unsupported"))
            self.assertFalse(book.request(b"AT+DTUPN?\r\n", carrier="at"))
            deliver(book, codec=codec)
            self.assertIsNotNone(close(book), book.reason)
        book = ledger()
        book.request(b"AT+CLDSRVHOST1=other\r\n", carrier="at")
        deliver(book)
        self.assertIsNone(close(book))

    def test_unknown_codec_provider_and_acceptance_only_are_not_completion(self):
        for source in ("valuecloud", "unknown"):
            book = ledger(); book.begin(source, {"pn": PN})
            self.assertTrue(book.failed)
        book = ledger()
        action, request, response, envelope, carrier = exchange()
        book.begin("dessmonitor", action)
        book.request(request, protocol="unknown")
        book.replied(response); book.finish(envelope)
        self.assertIsNone(close(book))

    def test_shadow_only_collector_bootstrap_is_not_an_inverter_command(self):
        setup = (b"AT+SYST=20261021094414\r\n", b"AT+UART=9600,8,1,NONE\r\n")
        book = ledger()
        for payload in setup:
            self.assertFalse(book.request(payload, carrier="at"))
        self.assertFalse(book.failed)
        self.assertIsNone(close(book))  # bootstrap alone never proves completion
        book = ledger()
        for payload in setup:
            book.request(payload, carrier="at")
        deliver(book, source="smartess", codec="modbus_rtu", framed=False)
        self.assertEqual(len(close(book)), 1)
        for payload in (b"AT+UART=bad\r\n", b"AT+SYST=bad\r\n",
                        b"AT+REBOOT=1\r\n", b"AT+CLDSRVHOST1=other\r\n"):
            book = ledger()
            book.request(payload, carrier="at")
            deliver(book)
            self.assertIsNone(close(book))

    def test_receipt_revalidation_binds_source_codec_epoch_sequence_and_echo(self):
        book = ledger(); deliver(book); deliver(book, tid=12)
        records = close(book)
        for key, value in (("source", "valuecloud"), ("protocol", "modbus_rtu"),
                           ("carrier", "pi30"), ("epoch", 0), ("sequence", 1),
                           ("terminal", {"kind": "accepted"})):
            with self.subTest(key=key):
                mutated = deepcopy(records); mutated[1][key] = value
                with self.assertRaises(ValueError):
                    validate_completed_commands(mutated)
        with self.assertRaises(ValueError):
            validate_completed_commands(records, source="smartess")

    async def test_two_real_sockets_preserve_drained_refusals_across_reconnect(self):
        from tests.test_shadow_learning_proxy_e2e import _seed
        from custom_components.eybond_local.support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler

        action, request, refusal, envelope, _ = exchange(codec="modbus_rtu")
        # Control the two upstream sockets directly; the same TID is valid on both.
        upstream = asyncio.Queue()
        async with await asyncio.start_server(
                lambda r, w: upstream.put_nowait((r, w)), "127.0.0.1", 0) as server:
            with tempfile.TemporaryDirectory() as tmp:
                handler = InProcessFailClosedShadowProxyHandler(
                    upstream_host="127.0.0.1", upstream_port=server.sockets[0].getsockname()[1],
                    seed=replace(_seed(), collector_pn=PN), output_path=Path(tmp) / "trace.jsonl")
                await handler.start()
                try:
                    async with await asyncio.start_server(handler.handle_client, "127.0.0.1", 0) as proxy:
                        async with asyncio.timeout(5):
                            for _ in range(2):
                                device, peer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
                                heartbeat = build_collector_request(1, b"ping", devcode=1, collector_addr=5, fcode=1)
                                peer.write(heartbeat)
                                await peer.drain()
                                cloud, writer = await upstream.get()
                                try:
                                    self.assertEqual(await cloud.readexactly(len(heartbeat)), heartbeat)
                                    handler.completion.begin("dessmonitor", action)
                                    writer.write(request)
                                    await writer.drain()
                                    self.assertEqual(await cloud.readexactly(len(refusal)), refusal)
                                    writer.close()
                                    await writer.wait_closed()
                                    self.assertEqual(await device.read(), b"")
                                finally:
                                    peer.close()
                                    await peer.wait_closed()
                                    writer.close()
                                while handler._tasks:
                                    await asyncio.sleep(.005)
                                handler.completion.finish(envelope)  # arrives AFTER disconnect
                            await handler.stop()
                            self.assertEqual([c["epoch"] for c in handler.completion.completed_commands()], [1, 2])
                finally:
                    await handler.stop()

    async def test_loopback_proxy_drains_all_carriers_without_inverter_write(self):
        from tests.test_shadow_learning_proxy_e2e import _seed
        from tests.test_pi30_shadow_learning import seed as pi30_seed
        from custom_components.eybond_local.support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler

        for codec, framed, tail in itertools.product(CODECS, (True, False), (b"", b"\x00")):
            with self.subTest(codec=codec, framed=framed, tail=tail):
                action, request, expected, envelope, carrier = exchange(codec=codec, framed=framed)
                complete = asyncio.Event()
                cloud_done = asyncio.Event()
                errors = []

                async def cloud(reader, writer):
                    try:
                        await reader.readexactly(12)  # collector heartbeat, no inverter data
                        for setting in (b"SYST=20261021094414", b"UART=9600,8,1,NONE"):
                            writer.write(b"AT+" + setting + b"\r\n")
                            await writer.drain()
                            self.assertEqual(await reader.readuntil(b"\n"),
                                             b"AT+" + setting.replace(b"=", b":", 1) + b"\r\n")
                        handler.completion.begin("dessmonitor", action)
                        writer.write(request + tail); await writer.drain()
                        actual = await reader.readexactly(len(expected))
                        self.assertEqual(actual, expected)
                        handler.completion.finish(envelope)
                        complete.set()
                        await reader.read()
                    except Exception as exc:
                        errors.append(exc); complete.set()
                    finally:
                        writer.close(); await writer.wait_closed(); cloud_done.set()

                server = await asyncio.start_server(cloud, "127.0.0.1", 0)
                try:
                    with tempfile.TemporaryDirectory() as tmp:
                        selected_seed = replace(_seed(), collector_pn=PN)
                        if codec == "pi30_ascii":
                            selected_seed = replace(pi30_seed()[0], collector_pn=PN)
                        elif codec == "eybond_g_ascii":
                            selected_seed = replace(selected_seed, effective_metadata_snapshot={"protocol_family": codec})
                        handler = InProcessFailClosedShadowProxyHandler(
                            upstream_host="127.0.0.1", upstream_port=server.sockets[0].getsockname()[1],
                            seed=selected_seed, output_path=Path(tmp) / "trace.jsonl")
                        await handler.start()
                        proxy = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
                        try:
                            reader, writer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
                            try:
                                writer.write(build_collector_request(1, b"ping", devcode=1, collector_addr=5, fcode=1))
                                await writer.drain()
                                await asyncio.wait_for(complete.wait(), 3)
                                if errors:
                                    raise errors[0]
                                await handler.stop()
                                events = [json.loads(line) for line in (Path(tmp) / "trace.jsonl").read_text().splitlines()]
                                summary = next(row["payload"] for row in events if row["kind"] == "shadow_completion_summary")
                                self.assertEqual(summary["confirmed_commands"], 1)
                                self.assertEqual(summary["complete"], not bool(tail))
                                self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
                                if tail:
                                    self.assertIsNone(handler.completion.completed_commands())
                                    self.assertEqual(handler.completion.reason, "completion_unparsed_cloud_tail")
                                else:
                                    self.assertEqual(len(handler.completion.completed_commands() or []), 1, handler.completion.reason)
                                await asyncio.wait_for(cloud_done.wait(), 3)
                            finally:
                                writer.close(); await writer.wait_closed()
                        finally:
                            proxy.close(); await proxy.wait_closed(); await handler.stop()
                finally:
                    server.close(); await server.wait_closed()
