"""Exact receipts, not matched counters, permit completed-study retirement."""
from dataclasses import replace
import asyncio
import json
from pathlib import Path
import tempfile
import unittest

from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold, CloudLearningProtectionRequired
from custom_components.eybond_local.smartess_cloud import ApiEnvelope, build_device_control_action
from custom_components.eybond_local.support.shadow_learning.completion import CloudCommandCompletion
from custom_components.eybond_local.support.shadow_learning.completion_store import CompletedLearningStore
from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningMutationFence

PN = "Q0000000000001"


def ledger():
    return CloudCommandCompletion(entry_id="entry", study_id="study", collector_pn=PN)


def sample(index=0):
    data = json.loads((Path(__file__).parent / "fixtures/smartess_0925_recorded_learning.json").read_text())
    item = dict(zip(data["attempt_columns"], data["attempts"][index], strict=True))
    reply = bytes.fromhex(item["api_response_frame_hex"])
    h = decode_header(reply)
    request = build_collector_request(h.tid, bytes.fromhex(item["write_payload_hex"]),
                                      devcode=h.devcode, collector_addr=h.devaddr, fcode=4)
    action = build_device_control_action(pn=PN, sn="offline", devcode=1, devaddr=5,
                                        field_id=item["field_id"], value=item["requested_value"])
    envelope = ApiEnvelope(err=0, desc="ERR_NONE", dat={"dat": reply.hex(" "), "status": 0}, raw={})
    return action, request, reply, envelope


def close(book):
    book.start_closing()
    book.closed = True
    return book.completed_commands()


def deliver(book, index=0):
    action, request, reply, envelope = sample(index)
    book.begin("smartess", action)
    book.request(request)
    book.replied(reply)
    book.finish(envelope)


class CompletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_proxy_records_reply_after_drain_and_closes_before_receipt(self):
        from tests.test_shadow_learning_proxy_e2e import _seed
        from custom_components.eybond_local.support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler
        completed = asyncio.Event()
        cloud_done = asyncio.Event()
        action, request, expected, _ = sample()
        observed = []

        async def cloud(reader, writer):
            try:
                observed.append(await reader.readexactly(12))
                handler.completion.begin("smartess", action)
                writer.write(request)
                await writer.drain()
                received = await reader.readexactly(len(expected))
                self.assertEqual(received, expected)
                handler.completion.finish(ApiEnvelope(err=0, desc="ERR_NONE", raw={},
                    dat={"dat": received.hex(" "), "status": 0}))
                completed.set()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
                cloud_done.set()

        server = await asyncio.start_server(cloud, "127.0.0.1", 0)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                handler = InProcessFailClosedShadowProxyHandler(
                    upstream_host="127.0.0.1", upstream_port=server.sockets[0].getsockname()[1],
                    seed=replace(_seed(), collector_pn=PN), output_path=Path(tmp) / "trace.jsonl")
                await handler.start()
                proxy = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
                try:
                    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
                    try:
                        heartbeat = build_collector_request(1, b"ping", devcode=1, collector_addr=5, fcode=1)
                        writer.write(heartbeat); await writer.drain()
                        await asyncio.wait_for(completed.wait(), 3)
                        self.assertIsNone(handler.completion.completed_commands())
                        await handler.stop()
                        self.assertEqual(len(handler.completion.completed_commands()), 1)
                        self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
                        self.assertEqual(observed, [heartbeat])
                        await asyncio.wait_for(cloud_done.wait(), 3)
                    finally:
                        writer.close(); await writer.wait_closed()
                finally:
                    proxy.close(); await proxy.wait_closed(); await handler.stop()
        finally:
            server.close(); await server.wait_closed()

    def test_all_146_recorded_api_refusals_have_exact_receipts(self):
        book = ledger()
        for i in range(146):
            deliver(book, i)
        self.assertIsNone(book.completed_commands())  # route still running
        self.assertEqual(len(close(book)), 146)

    def test_uncertain_and_unsupported_cases_remain_held(self):
        for fault in ("no_echo", "wrong_echo", "wrong_tid", "wrong_route", "wrong_crc", "ack", "no_drain",
                      "timeout", "duplicate", "unsolicited", "reconnect", "foreign_pn", "provider",
                      "api_error", "api_status", "second_send_missing", "close_in_flight"):
            with self.subTest(fault=fault):
                book = ledger()
                action, request, reply, envelope = sample()
                if fault == "unsolicited":
                    book.request(request)
                book.begin("dessmonitor" if fault == "provider" else "smartess",
                           action.replace(PN, "E5000099990002") if fault == "foreign_pn" else action)
                book.request(request)
                if fault == "duplicate":
                    book.request(request)
                if fault in {"wrong_tid", "wrong_route", "wrong_crc", "ack"}:
                    pos = {"wrong_tid": 1, "wrong_route": 3, "wrong_crc": -1, "ack": 9}[fault]
                    broken = bytearray(reply); broken[pos] ^= 1
                    reply = bytes(broken)
                    envelope = replace(envelope, dat={"dat": reply.hex(), "status": 0})
                if fault != "no_drain":
                    book.replied(reply)
                if fault == "no_echo":
                    envelope = replace(envelope, dat={})
                if fault == "wrong_echo":
                    envelope = replace(envelope, dat={"dat": sample(1)[2].hex(), "status": 0})
                if fault == "api_error":
                    envelope = replace(envelope, err=2)
                if fault == "api_status":
                    envelope = replace(envelope, dat={"dat": reply.hex(), "status": False})
                if fault in {"timeout", "reconnect"}:
                    book.invalidate()
                if fault != "close_in_flight":
                    book.finish(envelope)
                if fault == "second_send_missing":
                    book.begin("smartess", sample(1)[0]); book.finish(sample(1)[3])
                self.assertIsNone(close(book))

    def test_reused_tid_and_empty_study_do_not_complete(self):
        self.assertIsNone(close(ledger()))
        book = ledger(); deliver(book); deliver(book)
        self.assertIsNone(close(book))

    async def test_receipt_archival_fence_and_tamper_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CompletedLearningStore(Path(tmp))
            hold = CloudLearningHold(entry_id="entry", study_id="study", collector_pn=PN,
                                     source="smartess", armed_at="2026-10-21T01:00:00Z")
            store.create(hold=hold)
            fence = CloudLearningMutationFence(Path(tmp), PN)
            self.assertTrue(await fence.hold_reason())
            book = ledger(); deliver(book); commands = close(book)
            with self.assertRaises(CloudLearningProtectionRequired):
                store.retire_completed(hold)
            store.record_completed(hold, commands)
            store.record_completed(hold, commands)
            with self.assertRaises(CloudLearningProtectionRequired):
                await fence.accept_completed_retirement(hold)
            store.retire_completed(hold)
            await fence.accept_completed_retirement(hold)
            self.assertFalse(await fence.hold_reason())
            self.assertEqual(store.require_completed_receipt(hold)["commands"], commands)
            self.assertTrue((store._root / "retired_holds" / store._receipt_path(hold).name).exists())
            with self.assertRaises(CloudLearningProtectionRequired):
                store.require_completed_receipt(replace(hold, study_id="foreign"))
            p = store._completed_path(hold)
            p.write_text(p.read_text().replace('commands_completed', 'no_request_sent'))
            with self.assertRaises(CloudLearningProtectionRequired):
                store.require_completed_receipt(hold)
