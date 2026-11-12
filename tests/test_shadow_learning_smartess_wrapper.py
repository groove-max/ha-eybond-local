"""Recorded #94 API wrapping, separate from the actual raw collector carrier.

Offline replay tests never authorize recovery of an existing device's hold.
"""

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
from custom_components.eybond_local.dessmonitor_cloud import DessMonitorApiEnvelope
from custom_components.eybond_local.payload.modbus import build_write_single_request
from custom_components.eybond_local.smartess_cloud import ApiEnvelope, build_device_control_action
from custom_components.eybond_local.support.shadow_learning.completion import validate_completed_commands
from custom_components.eybond_local.support.shadow_learning.completion_store import CompletedLearningStore
from custom_components.eybond_local.support.shadow_learning.protocol import ModbusRtuShadowLearningAdapter
from tests.test_shadow_learning_completion import PN, ledger, close

FIXTURE = Path(__file__).parent / "fixtures/smartess_4200_recorded_wrapped_refusals.json"


def recorded_exchange(index=0):
    data = json.loads(FIXTURE.read_text())
    row = dict(zip(data["attempt_columns"], data["attempts"][index], strict=True))
    request = bytes.fromhex(row["write_payload_hex"])
    adapter = ModbusRtuShadowLearningAdapter()
    response = adapter.build_write_exception_response(adapter.decode_write_request(request), exception_code=1)
    action = build_device_control_action(pn=PN, sn="offline", devcode=6514, devaddr=1,
                                        field_id=row["field_id"], value=row["requested_value"])
    envelope = ApiEnvelope(err=0, desc="ERR_NONE", raw={},
                          dat={"status": 0, "dat": row["api_response_frame_hex"]})
    return action, request, response, envelope


def deliver(book, index=0):
    action, request, response, envelope = recorded_exchange(index)
    book.begin("smartess", action)
    book.request(request, carrier="modbus", protocol="modbus_rtu")
    book.replied(response)
    book.finish(envelope)


class SmartEssWrappedRefusalTests(unittest.TestCase):
    def test_all_90_recorded_exchanges_retain_both_raw_and_api_evidence(self):
        book = ledger()
        for index in range(90):
            deliver(book, index)
        self.assertIsNone(book.completed_commands())  # must stop the live route first
        records = close(book)
        self.assertEqual(len(records or []), 90, book.reason)
        validate_completed_commands(records, source="smartess")
        for item in records:
            self.assertEqual(item["carrier"], "modbus")
            self.assertEqual(item["response_hex"], "0190018dc0")
            self.assertEqual(item["terminal"]["kind"], "smartess_fc4_refusal_echo")
            self.assertEqual(item["terminal"]["api_response_hex"], "000000010007ff040190018dc0")

    def test_wrong_wrapper_or_payload_never_becomes_completion(self):
        faults = ("tid", "devcode", "length", "address", "function", "crc", "unit", "exception",
                  "prefix", "tail", "partial", "nested", "no_echo", "non_hex", "not_string",
                  "status", "api_error", "no_request", "no_drain", "wrong_provider", "framed")
        for fault in faults:
            with self.subTest(fault=fault):
                action, request, response, envelope = recorded_exchange()
                echo = bytearray.fromhex(envelope.dat["dat"])
                offsets = {"tid": 1, "devcode": 3, "length": 5, "address": 6, "function": 7,
                           "crc": 12, "unit": 8, "exception": 10}
                if fault in offsets:
                    echo[offsets[fault]] ^= 1
                if fault == "prefix":
                    echo = b"junk" + echo
                if fault == "tail":
                    echo += b"\x00"
                if fault == "partial":
                    echo = echo[:-1]
                if fault == "nested":
                    echo = build_collector_request(0, echo, devcode=1, collector_addr=255, fcode=4)
                envelope = replace(envelope, dat={"status": 0, "dat": echo.hex()})
                if fault in ("no_echo", "non_hex", "not_string"):
                    envelope = replace(envelope, dat={"status": 0, "dat": {
                        "no_echo": "", "non_hex": "GG", "not_string": 13}[fault]})
                if fault == "status":
                    envelope = replace(envelope, dat={"status": False, "dat": echo.hex()})
                if fault == "api_error":
                    envelope = replace(envelope, err=1)
                if fault == "wrong_provider":
                    envelope = DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat=envelope.dat)
                if fault == "framed":
                    # API's synthetic wrapper may not erase a real collector route.
                    request, response = (build_collector_request(9, data, devcode=6514, collector_addr=1, fcode=4)
                                         for data in (request, response))
                book = ledger()
                book.begin("smartess", action)
                if fault != "no_request":
                    book.request(request, carrier="frame" if fault == "framed" else "modbus", protocol="modbus_rtu")
                if fault != "no_drain":
                    book.replied(response)
                book.finish(envelope)
                self.assertIsNone(close(book))

    def test_fc06_codec_contract_is_supported_without_device_specific_binding(self):
        # Synthetic extension of the same RTU refusal grammar, not field evidence.
        action, _, _, envelope = recorded_exchange()
        request = build_write_single_request(5, 300, 1)
        adapter = ModbusRtuShadowLearningAdapter()
        response = adapter.build_write_exception_response(adapter.decode_write_request(request), exception_code=1)
        echo = build_collector_request(0, response, devcode=1, collector_addr=255, fcode=4)
        book = ledger(); book.begin("smartess", action)
        book.request(request, carrier="modbus", protocol="modbus_rtu"); book.replied(response)
        book.finish(replace(envelope, dat={"status": 0, "dat": echo.hex(" ")}))
        self.assertEqual(len(close(book) or []), 1, book.reason)

    def test_receipt_revalidation_and_disk_roundtrip_do_not_trust_wrapper_kind(self):
        book = ledger(); deliver(book)
        records = close(book)
        for key, value in (("kind", "wire_echo"), ("response_hex", ""), ("api_response_hex", "00" * 13),
                           ("api_response_hex", False), ("api_response_hex", "Z" * 26), ("extra", 1)):
            with self.subTest(key=key, value=value):
                mutated = deepcopy(records); mutated[0]["terminal"][key] = value
                with self.assertRaises(ValueError):
                    validate_completed_commands(mutated)
        mutated = deepcopy(records); mutated[0]["source"] = "dessmonitor"
        with self.assertRaises(ValueError):
            validate_completed_commands(mutated)
        with tempfile.TemporaryDirectory() as tmp:
            store = CompletedLearningStore(Path(tmp))
            hold = CloudLearningHold(entry_id="entry", study_id="study", collector_pn=PN,
                                     source="smartess", armed_at="2026-11-11T21:00:00Z")
            store.create(hold=hold)
            store.record_completed(hold, records)
            self.assertEqual(store.require_completed_receipt(hold)["commands"], records)
