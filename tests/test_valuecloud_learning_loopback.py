"""Issue #46 catalogue with real local HTTP/TCP and synthetic device traffic."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "tests" / "helpers"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from valuecloud_learning_lab import ValueCloudLearningLab
from custom_components.eybond_local.support.shadow_learning.overlay_generator import generate_shadow_learning_overlay_drafts
from custom_components.eybond_local.valuecloud_cloud import ValueCloudError


def catalogue():
    """Only public parameter names/choices; no account data or wire-map claims.

    The 24 legacy fields reported in #46, 2026-10-01. Batch controls were empty;
    device_ctrl repeated 12 fields already present in control_strategy.
    """
    rows = [
        ("shutdown_work", "Ongrid Switch", {"0": "Deactivate", "1": "Enable"}),
        ("battery_stop_discharging_voltage", "Battery stop discharging voltage", "V"),
        ("pv_max_charger_current", "PV Max charger current", "A"),
        ("grid_max_charger_current_set", "Grid max charger current set", "A"),
        ("battery_type", "Battery type", dict(enumerate(("no choose", "User default battery", "lithium battery", "SEALED_LEAD battery", "AGM battery", "GEL battery", "FLOODED battery", "No battery")))),
        ("absorb_voltage", "Equalize the charging voltage", "V"),
        ("float_voltage", "Floating charging voltage", "V"),
        ("battery_stop_charging_voltage", "Battery stop charging voltage", "V"),
        ("inverter_offline_working_enable", "inverter offline working enable", {"0": "OFF", "1": "ON"}),
        ("inverter_output_voltage_set", "inverter output voltage Set", "V"),
        ("grid_protect_standard", "grid protect standard", dict(enumerate(("VDE4105", "UPS", "home", "GEN")))),
        ("inverter_max_discharger_current", "inverter max discharger current", "A"),
        ("battery_low_voltage", "battery low voltage", "V"),
        ("eybond_read_20221", "Reverse Power Flow Protection", {"0": "Disable", "1": "Enable CT"}),
        ("eybond_read_20232", "Date", ""),
        ("eybond_read_20233", "Hour", ""),
        ("battery_high_voltage", "battery high voltage", "V"),
        ("max_combine_charger_current", "max combine charger current", "A"),
        ("solaruse_aim", "solaruse_aim", {"0": "LBU", "1": "BLU"}),
        ("energy_use_mode", "energy use mode", {"1": "SBU", "2": "SUB", "3": "UTI", "4": "SOL"}),
        ("inverter_output_frequency_set", "inverter output frequency Set", "Hz"),
        ("inverter_discharges_to_the_grid_to_enable", "inverter discharges to the grid to enable", {"0": "close", "1": "open"}),
        ("inverter_search_mode_enable", "inverter search mode enable", {"0": "OFF", "1": "ON"}),
        ("charger_source_priority", "charger source priority", {"0": "Solar first", "2": "Solar and Utility", "3": "Only Solar"}),
    ]
    return [{"id": key, "name": name, "readwrite": "RW",
             "datatype": 3 if isinstance(detail, dict) else 1,
             "item": [{"key": str(k), "val": v} for k, v in detail.items()] if isinstance(detail, dict) else None,
             "unit": detail if isinstance(detail, str) else None}
            for key, name, detail in rows]


class ValueCloudLoopbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_catalogue_to_captured_inactive_controls(self):
        fields = catalogue()
        with tempfile.TemporaryDirectory() as tmp:
            async with ValueCloudLearningLab(Path(tmp), fields) as lab:
                result = await lab.learn()
            self.assertEqual(len(fields), 24)
            self.assertEqual(result["planned_write_count"], 57)
            self.assertEqual(len(lab.requests), 57)
            self.assertEqual(len(lab.nacks), 57)
            self.assertEqual(result["captured_not_applied_count"], 57)
            self.assertEqual(result["correlation"]["matched_count"], 57)
            self.assertEqual(result["correlation"]["unmatched_attempt_count"], 0)
            self.assertEqual(result["correlation"]["unmatched_write_count"], 0)
            self.assertEqual(lab.collector_traffic, b"")
            self.assertEqual(result["leaked_count"], 0)
            self.assertEqual(result["degraded_count"], 0)
            attempted = {row["field_id"] for row in result["plan"]}
            self.assertEqual(attempted, {f["id"] for f in fields} - {"eybond_read_20232"})
            draft = generate_shadow_learning_overlay_drafts(
                config_dir=Path(tmp), source_profile_name="must_pv_ph18/pv3300.json",
                source_schema_name="must_pv_ph18/pv3300.json",
                session_manifest={"session_id": "synthetic-valuecloud-loopback", "collector_pn": lab.pn,
                                  "devcode": 6422, "devaddr": 1, "write_response_mode": "exception"},
                correlation=result["correlation"],
            )
            profile = json.loads(draft.profile_path.read_text())
            self.assertEqual(draft.generated_capability_count, 17)
            self.assertEqual(draft.skipped_duplicate_count, 6)
            self.assertEqual(draft.generated_capability_count + draft.skipped_duplicate_count, len(attempted))
            self.assertTrue(all(not c["enabled_default"] for c in profile["capabilities"]))
            ongrid = next(c for c in profile["capabilities"] if c["register"] == lab.mapping["shutdown_work"][0])
            self.assertEqual(ongrid["write_function"], 16)
            self.assertEqual(ongrid["title"], "Ongrid Switch")

    async def test_http_timeouts_keep_partial_evidence_without_retry(self):
        for fault, index in (("timeout_before_wire", 0), ("timeout_after_wire", 0),
                             ("timeout_before_wire", 1), ("timeout_after_wire", 1)):
            with self.subTest(fault=fault, index=index), tempfile.TemporaryDirectory() as tmp:
                async with ValueCloudLearningLab(Path(tmp), catalogue()[:1], fault=fault, fault_index=index) as lab:
                    with self.assertRaises(ValueCloudError) as raised:
                        await lab.learn()
                    report = raised.exception.learning_report()
                    self.assertTrue(lab.proxy.ready)
                    self.assertEqual(report["status"], "interrupted")
                    self.assertEqual(report["executed_result_count"], index + 1)
                    self.assertEqual(report["planned_write_count"], 2)
                    self.assertEqual(report["captured_not_applied_count"], index)
                    self.assertEqual(report["correlation"]["matched_count"], index + (fault == "timeout_after_wire"))
                    self.assertEqual(report["results"][-1]["status"], "indeterminate")
                    self.assertEqual(raised.exception.safe_diagnostics()["provider_reason"], "timeout")
                self.assertEqual(len(lab.requests), index + 1)
                self.assertEqual(len(lab.nacks), index + (fault == "timeout_after_wire"))
                self.assertEqual(lab.collector_traffic, b"")
                self.assertNotIn("synthetic-token", str(report))
                self.assertNotIn("synthetic-secret", str(report))

    async def test_unobserved_success_or_lost_route_stops_before_next_control(self):
        for fault, status in (("success_without_wire", "leaked"), ("disconnect_before_wire", "degraded")):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                async with ValueCloudLearningLab(Path(tmp), catalogue()[:1], fault=fault) as lab:
                    result = await lab.learn()
                self.assertEqual(result["planned_write_count"], 2)
                self.assertEqual(result["executed_result_count"], 1)
                self.assertEqual(result["results"][0]["status"], status)
                self.assertEqual(result["correlation"]["matched_count"], 0)
                self.assertEqual(len(lab.requests), 1)
                self.assertFalse(lab.nacks)
                self.assertEqual(lab.collector_traffic, b"")


if __name__ == "__main__":
    unittest.main()
