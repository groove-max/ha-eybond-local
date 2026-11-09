"""Discarded BLE bytes are classified, never exported or accepted as identity."""

import json
import unittest

from custom_components.eybond_local.collector.ble_diagnostics import BleExchangeDiagnostics
from custom_components.eybond_local.payload.modbus import crc16_modbus


def rtu_reply(function=3):
    # 91 bytes with the vendor prefix, deliberately including private content.
    frame = bytes((1, function, 78)) + b"private-password".ljust(78, b"\0")
    return frame + crc16_modbus(frame).to_bytes(2, "little")


class BinaryShapeTests(unittest.TestCase):
    def test_complete_reply_requires_grammar_and_crc(self):
        for function in (3, 4):
            frame = rtu_reply(function)
            samples = (
                (frame, "rtu_read"),
                (b"11111111" + frame, "vendor_prefixed_rtu_read"),
                (b"11111111" + frame[:-1], "vendor_prefix_other"),
                (b"11111111" + frame + frame, "vendor_prefix_other"),
                (b"11111111" + frame[:-1] + bytes((frame[-1] ^ 1,)), "vendor_prefix_other"),
                (b"11111111" + frame[:10], "vendor_prefix_other"),
                (bytes(91), "other_binary"),
                (b"\0AT+DTUPN:private-identity\r\n", "at_marker_with_control"),
                (b"AT+PASS:private-password\0", "at_marker_with_control"),
                (b"\0" * 513, "oversize"),
            )
            for source in ("notify", "read"):
                for payload, shape in samples:
                    with self.subTest(source=source, shape=shape, function=function):
                        diag = BleExchangeDiagnostics()
                        diag.observe_binary(payload, source=source)
                        snapshot = diag.snapshot()
                        self.assertEqual(snapshot[f"binary_{source}_shapes"], {shape: 1})
                        self.assertEqual(snapshot["reply_source"], "none")
                        self.assertEqual(snapshot["matched_notify_bytes"], 0)
                        exported = json.dumps(snapshot)
                        for private in ("private-", "11111111", "AT+", payload.hex()):
                            self.assertNotIn(private, exported)

    def test_counts_have_fixed_keys_and_snapshots_are_independent(self):
        diag = BleExchangeDiagnostics()
        for index in range(300):
            diag.observe_binary(b"\0" + str(index).encode(), source="read")
        snapshot = diag.snapshot()
        self.assertEqual(snapshot["binary_read_shapes"], {"other_binary": 300})
        snapshot["binary_read_shapes"]["other_binary"] = 0
        self.assertEqual(diag.binary_read_shapes, {"other_binary": 300})
