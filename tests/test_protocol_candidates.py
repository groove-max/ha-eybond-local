"""Nominal synthetic layouts plus one public wire fixture; no telemetry claims."""

from copy import deepcopy
import json
from pathlib import Path
import unittest

from custom_components.eybond_local.support.protocol_candidates import (
    CATALOG_PATH, MAX_EVENTS, analyze_protocol_candidates,
)


def synthetic_exchange(code="099E"):
    # Deliberately arbitrary headers: the vendor code does not validate them.
    if code == "099E":
        request = "434d534706494e562d48420d"
        body = bytes([0xAB] * 66)
        response = b"\xa1\xb2\xc3\xd4\xe5" + body + bytes([sum(body) & 255])
    else:
        request = "eb90000100017c7f"
        prefix_body = b"\xa1\xb2\xc3\xd4\xe5" + bytes([0xAB] * 48)
        response = prefix_body + (sum(prefix_body) & 65535).to_bytes(2, "big") + b"\xfe"
    return {"wire": "fc4", "route_family": "eybond", "devcode": 1,
            "collector_addr": 255, "request_hex": request,
            "request_size": len(bytes.fromhex(request)), "request_truncated": False,
            "response_hex": response.hex(), "response_size": len(response),
            "response_truncated": False, "status": "reply"}


def analyze(*events, catalog=None):
    return analyze_protocol_candidates({"context": {"wire_events": list(events)}}, catalog=catalog)


class ProtocolCandidateTests(unittest.TestCase):
    def observed_exchange(self):
        source = json.loads((Path(__file__).parent / "fixtures/protocol_candidates/issue82_099e_status.json").read_text())
        return synthetic_exchange() | {"request_hex": source["request_hex"],
                                       "response_hex": source["response_hex"], "response_size": 84}

    def test_extended_device_response_retains_vendor_window_and_unknown_extension(self):
        event = self.observed_exchange()
        original = deepcopy(event)
        report = analyze(event)
        self.assertEqual(report["candidate_keys"], ["eybond_099e_06"])
        observation = report["observations"][0]
        self.assertEqual(observation["expected_response_size"], 84)
        self.assertEqual(observation["body_size"], 78)
        self.assertEqual(observation["vendor_parse_body_size"], 66)
        self.assertEqual(observation["extension_size"], 12)
        self.assertEqual(observation["length_basis"], "observed_extended_body_issue82")
        self.assertEqual(observation["layout_evidence_url"],
                         "https://github.com/groove-max/ha-eybond-local/issues/82")
        self.assertFalse(report["runtime_support_confirmed"] or report["telemetry_decoded"])
        self.assertEqual(report["additional_requests_sent"], 0)
        self.assertEqual(event, original)
        self.assertNotIn(event["response_hex"], json.dumps(report))

    def test_extended_checksum_covers_extension_and_header_is_explicit(self):
        for index, reason in ((0, "unexpected_header"), (4, "unexpected_header"),
                              (5, "checksum_mismatch"), (71, "checksum_mismatch"),
                              (82, "checksum_mismatch"), (83, "checksum_mismatch")):
            event = self.observed_exchange()
            raw = bytearray.fromhex(event["response_hex"])
            raw[index] ^= 1
            event["response_hex"] = raw.hex()
            with self.subTest(index=index):
                report = analyze(event)
                self.assertEqual(report["candidate_keys"], [])
                self.assertEqual(report["observations"][0]["reason"], reason)
        # A valid checksum with some other plausible extension is not admitted.
        for length in (73, 83, 85, 512):
            event = self.observed_exchange()
            body = bytes([1] * (length - 6))
            raw = b"VPHBO" + body + bytes([sum(body) & 255])
            event.update(response_hex=raw.hex(), response_size=len(raw))
            self.assertEqual(analyze(event)["observations"][0]["reason"], "unexpected_length")

    def test_observed_variant_does_not_skip_correlation_or_truncation_checks(self):
        for change in ({"status": "error"}, {"response_truncated": True},
                       {"request_truncated": True}, {"wire": "auto"},
                       {"transport_progress": {"response_received": False}}):
            self.assertEqual(analyze(self.observed_exchange() | change)["candidate_keys"], [])

    def test_observed_variant_schema_rejects_ambiguous_or_unbounded_layouts(self):
        original = json.loads(CATALOG_PATH.read_text())
        for change in ({"response_size": 72}, {"response_size": True}, {"response_size": 513},
                       {"body_size": 66}, {"body_size": True}, {"response_prefix_hex": ""},
                       {"response_prefix_hex": "zz"}, {"response_prefix_hex": "ab" * 6},
                       {"evidence_url": ""}, {"length_basis": None}):
            catalog = deepcopy(original)
            catalog["candidates"][0]["response_variants"][0].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                analyze(self.observed_exchange(), catalog=catalog)
        catalog = deepcopy(original)
        catalog["candidates"][0]["response_variants"] *= 2
        with self.assertRaises(ValueError):
            analyze(catalog=catalog)

    def test_both_static_layouts_are_only_structural_leads(self):
        for code, key in (("099E", "eybond_099e_06"), ("09D5", "eybond_09d5_01")):
            with self.subTest(code=code):
                event = synthetic_exchange(code)
                original = deepcopy(event)
                report = analyze(event)
                self.assertEqual(report["candidate_keys"], [key])
                self.assertEqual(report["source"]["verification"], "static_dex_analysis_only")
                self.assertIn("physical_device_compatibility", report["observations"][0]["unverified"])
                self.assertFalse(report["runtime_support_confirmed"])
                self.assertFalse(report["telemetry_decoded"])
                self.assertEqual(report["additional_requests_sent"], 0)
                self.assertNotIn(event["response_hex"], json.dumps(report))
                self.assertEqual(original, event)

    def test_checksums_are_not_interchangeable(self):
        for code, position in (("099E", -1), ("09D5", -2)):
            event = synthetic_exchange(code)
            raw = bytearray.fromhex(event["response_hex"])
            raw[position] ^= 1
            event["response_hex"] = raw.hex()
            report = analyze(event)
            self.assertEqual(report["candidate_keys"], [])
            self.assertEqual(report["observations"][0]["reason"], "checksum_mismatch")
        event = synthetic_exchange("09D5")
        raw = bytearray.fromhex(event["response_hex"])
        raw[-3:-1] = reversed(raw[-3:-1])
        event["response_hex"] = raw.hex()
        self.assertEqual(analyze(event)["candidate_keys"], [])

    def test_incomplete_or_unqualified_exchanges_never_match(self):
        cases = [
            ({"wire": "auto"}, "wire_not_qualified"),
            ({"route_family": "raw_serial"}, "wire_not_qualified"),
            ({"request_truncated": True}, "truncated_exchange"),
            ({"response_truncated": True}, "truncated_exchange"),
            ({"status": "error"}, "no_completed_exchange"),
            ({"transport_progress": {"response_received": False}}, "response_not_correlated"),
            ({"response_hex": "REDACTED_IDENTITY_RANGE"}, "response_unavailable_or_redacted"),
            ({"response_hex": "ab" * 513}, "response_unavailable_or_redacted"),
            ({"response_size": 71}, "response_size_mismatch"),
            ({"response_hex": "", "response_size": 0}, "empty_reply"),
            ({"response_hex": "ab", "response_size": 1}, "unexpected_length"),
            ({"response_hex": synthetic_exchange()["request_hex"], "response_size": 12}, "request_echo"),
        ]
        for update, reason in cases:
            with self.subTest(reason=reason):
                event = synthetic_exchange() | update
                report = analyze(event)
                self.assertEqual(report["candidate_keys"], [])
                self.assertEqual(report["observations"][0]["reason"], reason)

    def test_request_correlation_not_cloud_id_or_envelope_guess(self):
        for update in ({"request_hex": "5131", "request_size": 2},
                       {"request_size": 999}, {"request_hex": None}):
            report = analyze(synthetic_exchange() | update | {"devcode": 0x099E})
            self.assertEqual(report["observations"], [])
        # A different FC4 envelope is not proof against the inner payload either.
        self.assertTrue(analyze(synthetic_exchange() | {"devcode": 0x0001})["candidate_keys"])

    def test_bounds_and_ambiguity_no_first_match_winner(self):
        events = [None] * MAX_EVENTS + [synthetic_exchange()]
        report = analyze(*events)
        self.assertEqual(report["events_omitted"], 1)
        self.assertEqual(report["candidate_keys"], [])
        report = analyze(synthetic_exchange(), synthetic_exchange("09D5"))
        self.assertTrue(report["ambiguous"])
        self.assertEqual(len(report["candidate_keys"]), 2)
        self.assertEqual(analyze_protocol_candidates({"context": None})["events_examined"], 0)

    def test_catalog_schema_fails_closed(self):
        original = json.loads(CATALOG_PATH.read_text())
        for update in ({"response_size": 0}, {"body_size": 67}, {"request_hex": "zz"},
                       {"checksum": "guess"}, {"unverified": "none"}):
            catalog = deepcopy(original)
            catalog["candidates"][0].update(update)
            with self.subTest(update=update), self.assertRaises(ValueError):
                analyze(synthetic_exchange(), catalog=catalog)
        with self.assertRaises(ValueError):
            analyze(catalog={"schema_version": 99})


if __name__ == "__main__":
    unittest.main()
