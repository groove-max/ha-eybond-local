from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.contribution_record import (  # noqa: E402
    RECORD_VERSION,
    build_contribution_record,
    record_contains_identifier,
)


def _fingerprint() -> dict:
    return {
        "kind": "device",
        "layout_code": 11,
        "model_code": 30721,
        "rated_power": 4200,
        "layout_key": "smg",
        "tier": "partial",
        "entry_key": "",
        "devcodes": [6514],
    }


def _manifest() -> dict:
    return {
        "session": {
            "session_id": "01ABC_20260612",
            "collector_pn": "E5000025000005",
            "cloud_pn": "E50000PRIVATE",
            "cloud_sn": "E50000PRIVATE000001",
        },
        "read_map": {
            "read_blocks": [[201, 34, 79], [186, 12, 79], [404, 4, 79]],
            "registers": {
                "201": [3],
                "215": [531],
                "186": [14642],  # serial word — must be dropped
                "190": [12592],  # serial word — must be dropped
                "404": [5],
            },
            "value_source": "seed_bank",
        },
        "read_bindings": {
            "bindings": [
                {
                    "cloud_id": "bt_eybond_read_28",
                    "title": "Battery Voltage",
                    "unit": "V",
                    "cloud_value": "53.1",
                    "status": "unique",
                    "candidates": [{"register": 215, "divisor": 10}],
                }
            ],
            "unique_count": 1,
        },
        "read_enum_bindings": {"bindings": [], "unique_count": 0},
        "learned_read_sensors": [
            {"key": "learned_read_404", "register": 404, "title": "Aux", "kind": "numeric"}
        ],
    }


class ContributionRecordTests(unittest.TestCase):
    def test_record_has_versioned_fingerprint_and_coverage(self) -> None:
        record = build_contribution_record(
            fingerprint=_fingerprint(),
            manifest=_manifest(),
            collector_version="8.50.12.3",
            integration_version="0.2.0",
        )
        self.assertEqual(record["record_version"], RECORD_VERSION)
        self.assertEqual(record["fingerprint"]["layout_code"], 11)
        self.assertEqual(record["fingerprint"]["model_code"], 30721)
        self.assertEqual(record["fingerprint"]["rated_power"], 4200)
        self.assertEqual(record["register_coverage"], [[201, 34], [186, 12], [404, 4]])
        self.assertEqual(record["capture_meta"]["collector_fw"], "8.50.12.3")
        self.assertEqual(record["cloud_hints"]["devcodes"], [6514])
        self.assertEqual(record["cloud_hints"]["label_count"], 1)

    def test_serial_registers_are_dropped_from_read_map(self) -> None:
        record = build_contribution_record(fingerprint=_fingerprint(), manifest=_manifest())
        registers = record["read_map_registers"]
        self.assertIn("215", registers)
        self.assertIn("404", registers)
        self.assertNotIn("186", registers)  # serial word
        self.assertNotIn("190", registers)  # serial word

    def test_label_evidence_carries_bindings(self) -> None:
        record = build_contribution_record(fingerprint=_fingerprint(), manifest=_manifest())
        numeric = record["label_evidence"]["numeric"]
        self.assertEqual(numeric["bindings"][0]["title"], "Battery Voltage")
        self.assertEqual(numeric["bindings"][0]["candidates"][0]["register"], 215)

    def test_proposed_payloads_are_scrubbed_of_session_identifiers(self) -> None:
        manifest = _manifest()
        proposed_schema = {
            "schema_key": "local_x",
            "shadow_learning_overlay": manifest,  # embeds session pn/sn
            "measurement_descriptions": [{"key": "learned_read_404", "name": "Aux"}],
        }
        record = build_contribution_record(
            fingerprint=_fingerprint(),
            manifest=manifest,
            proposed_schema=proposed_schema,
        )
        self.assertIn("proposed_schema", record)
        self.assertFalse(record_contains_identifier(record))

    def test_no_identifier_survives_anywhere(self) -> None:
        record = build_contribution_record(
            fingerprint=_fingerprint(),
            manifest=_manifest(),
            proposed_profile={"profile_key": "p", "shadow_learning_overlay": _manifest()},
            proposed_schema={"schema_key": "s", "shadow_learning_overlay": _manifest()},
        )
        self.assertFalse(record_contains_identifier(record))
        # devcode is allowed (model hint, not personal data)
        self.assertEqual(record["cloud_hints"]["devcodes"], [6514])

    def test_identifier_audit_detects_a_planted_pn(self) -> None:
        record = build_contribution_record(fingerprint=_fingerprint(), manifest=_manifest())
        record["proposed_schema"] = {"collector_pn": "E5000025000005"}
        self.assertTrue(record_contains_identifier(record))

    def test_empty_inputs_produce_a_valid_skeleton(self) -> None:
        record = build_contribution_record(fingerprint=None, manifest=None)
        self.assertEqual(record["record_version"], RECORD_VERSION)
        self.assertEqual(record["register_coverage"], [])
        self.assertEqual(record["read_map_registers"], {})
        self.assertFalse(record_contains_identifier(record))


if __name__ == "__main__":
    unittest.main()
