from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.utils import (
    anonymize_fixture_json,
    build_command_fixture_responses,
)


class FixtureUtilsTests(unittest.TestCase):
    def test_build_command_fixture_responses_keeps_responses_and_nak_failures(self) -> None:
        command_responses = build_command_fixture_responses(
            {
                "responses": {
                    "QPI": "PI30",
                    "QID": "55355535553555",
                },
                "failures": {
                    "QET": "nak",
                    "QT": "request_timeout",
                },
            }
        )

        self.assertEqual(command_responses["QPI"], "PI30")
        self.assertEqual(command_responses["QID"], "55355535553555")
        self.assertEqual(command_responses["QET"], "NAK")
        self.assertNotIn("QT", command_responses)

    def test_anonymize_fixture_pseudonymizes_command_serial_responses(self) -> None:
        fixture = {
            "fixture_version": 1,
            "name": "pi30_support_capture",
            "collector": {
                "remote_ip": "192.168.1.14",
                "collector_pn": "Q0033482254531",
            },
            "probe_target": {
                "devcode": 2452,
                "collector_addr": 1,
                "device_addr": 0,
            },
            "command_responses": {
                "QID": "55355535553555",
                "^P005ID": "1401234567890123456789",
            },
        }

        anonymized = anonymize_fixture_json(fixture)

        self.assertNotEqual(anonymized["command_responses"]["QID"], "55355535553555")
        self.assertTrue(anonymized["command_responses"]["QID"].isdigit())
        self.assertNotEqual(
            anonymized["command_responses"]["^P005ID"],
            "1401234567890123456789",
        )
        self.assertEqual(anonymized["command_responses"]["^P005ID"][:2], "14")
        self.assertTrue(any(item["field"] == "command_responses.QID" for item in anonymized["redactions"]))


if __name__ == "__main__":
    unittest.main()