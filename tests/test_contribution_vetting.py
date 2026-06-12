from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.contribution_record import (  # noqa: E402
    RECORD_VERSION,
)
from custom_components.eybond_local.support.contribution_vetting import (  # noqa: E402
    RESULT_FAIL,
    RESULT_PASS,
    RESULT_WARN,
    vet_contribution_record,
)


def _good_record(**overrides) -> dict:
    record = {
        "record_version": RECORD_VERSION,
        # A new (uncataloged) device on the known SMG layout family (code 11).
        "fingerprint": {"layout_code": 11, "model_code": 40000, "rated_power": 5500},
        "register_coverage": [[201, 34], [404, 4]],
        "read_map_registers": {"215": [531], "404": [5]},
        "label_evidence": {
            "numeric": {
                "bindings": [
                    {"title": "Battery Voltage", "status": "unique", "candidates": [{"register": 215}]}
                ]
            },
            "enum": {"bindings": []},
        },
        "cloud_hints": {"devcodes": [9999], "label_count": 1},
        "proposed_schema": {
            "spec_sets": {"aux_config": [{"key": "learned_read_404", "register": 404}]}
        },
    }
    record.update(overrides)
    return record


def _checks(report) -> dict[str, str]:
    return {check.name: check.result for check in report.checks}


class ContributionVettingTests(unittest.TestCase):
    def test_clean_new_device_passes(self) -> None:
        report = vet_contribution_record(_good_record())
        self.assertEqual(report.verdict, RESULT_PASS)
        self.assertEqual(report.failed, ())

    def test_unknown_record_version_fails(self) -> None:
        report = vet_contribution_record(_good_record(record_version=99))
        self.assertEqual(report.verdict, RESULT_FAIL)
        self.assertEqual(_checks(report)["record_version"], RESULT_FAIL)

    def test_planted_identifier_fails(self) -> None:
        record = _good_record()
        record["proposed_schema"]["collector_pn"] = "E5000025000005"
        report = vet_contribution_record(record)
        self.assertEqual(_checks(report)["no_identifiers"], RESULT_FAIL)
        self.assertEqual(report.verdict, RESULT_FAIL)

    def test_all_zero_fingerprint_fails(self) -> None:
        report = vet_contribution_record(
            _good_record(fingerprint={"layout_code": 0, "model_code": 0, "rated_power": 0})
        )
        self.assertEqual(_checks(report)["fingerprint"], RESULT_FAIL)

    def test_unknown_layout_warns_not_fails(self) -> None:
        report = vet_contribution_record(
            _good_record(fingerprint={"layout_code": 77, "model_code": 1234, "rated_power": 0})
        )
        self.assertEqual(_checks(report)["layout_known"], RESULT_WARN)
        self.assertNotEqual(report.verdict, RESULT_FAIL)

    def test_existing_catalog_device_warns_as_update(self) -> None:
        # The seeded SMG 6200 fingerprint (layout 1, model 7680).
        report = vet_contribution_record(
            _good_record(fingerprint={"layout_code": 1, "model_code": 7680, "rated_power": 6200})
        )
        self.assertEqual(_checks(report)["catalog_collision"], RESULT_WARN)

    def test_register_outside_coverage_warns(self) -> None:
        record = _good_record()
        record["proposed_schema"]["spec_sets"]["aux_config"].append(
            {"key": "learned_read_999", "register": 999}
        )
        report = vet_contribution_record(record)
        self.assertEqual(_checks(report)["coverage_consistency"], RESULT_WARN)

    def test_no_unique_label_evidence_warns(self) -> None:
        record = _good_record()
        record["label_evidence"]["numeric"]["bindings"][0]["status"] = "ambiguous"
        report = vet_contribution_record(record)
        self.assertEqual(_checks(report)["label_evidence"], RESULT_WARN)

    def test_report_serializes(self) -> None:
        payload = vet_contribution_record(_good_record()).to_json_dict()
        self.assertEqual(payload["verdict"], RESULT_PASS)
        self.assertTrue(all("name" in check for check in payload["checks"]))


if __name__ == "__main__":
    unittest.main()
