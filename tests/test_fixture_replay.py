from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.replay import (
    apply_fixture_preset,
    build_fixture_snapshot,
    detect_fixture_path,
    read_fixture_values,
)
from custom_components.eybond_local.fixtures.catalog import catalog_has_entries


LOCAL_FIXTURE_TESTS_ENABLED = (
    os.environ.get("EYBOND_ENABLE_LOCAL_FIXTURE_TESTS") == "1" and catalog_has_entries()
)


FIXTURE_PATH = (
    REPO_ROOT
    / ".local"
    / "fixtures"
    / "catalog"
    / "smg-6200-live-capture"
    / "fixture.json"
)

PI18_FIXTURE_PATH = (
    REPO_ROOT
    / ".local"
    / "fixtures"
    / "catalog"
    / "pi18-5000-synthetic-capture"
    / "fixture.json"
)

PI30_FIXTURE_PATH = (
    REPO_ROOT
    / ".local"
    / "fixtures"
    / "catalog"
    / "pi30-vmii-nxpw5kw-live-capture"
    / "fixture.json"
)


@unittest.skipUnless(
    LOCAL_FIXTURE_TESTS_ENABLED,
    "Local fixture replay tests are disabled. Set EYBOND_ENABLE_LOCAL_FIXTURE_TESTS=1 and populate .local/fixtures/catalog/.",
)
class FixtureReplayTests(unittest.TestCase):
    def test_catalog_fixture_detects_and_reads(self) -> None:
        async def scenario() -> tuple[object, dict[str, object]]:
            context = await detect_fixture_path(FIXTURE_PATH)
            values = await read_fixture_values(context)
            return context, values

        context, values = asyncio.run(scenario())

        self.assertEqual(context.inverter.driver_key, "modbus_smg")
        self.assertEqual(context.inverter.model_name, "SMG 6200")
        self.assertEqual(values["operating_mode"], "Off-Grid")
        self.assertEqual(values["charge_source_priority"], "PV Only")
        self.assertEqual(values["output_source_priority"], "PV-Battery-Utility")
        self.assertEqual(values["warning_code"], 65)
        self.assertEqual(values["warning_count"], 2)
        self.assertIn("PV Low Voltage", values["warning_descriptions"])
        self.assertIn(
            "Reserved / Vendor-Specific Warning Bit 0",
            values["warning_descriptions"],
        )

    def test_noop_preset_apply_keeps_values_and_builds_ui_schema(self) -> None:
        async def scenario() -> dict[str, object]:
            context = await detect_fixture_path(FIXTURE_PATH)
            return await apply_fixture_preset(context, "off_grid_self_consumption")

        result = asyncio.run(scenario())

        self.assertEqual(result["preset"]["preset_key"], "off_grid_self_consumption")
        self.assertEqual(
            [item["status"] for item in result["preset"]["results"]],
            ["unchanged", "unchanged"],
        )
        self.assertEqual(result["ui_schema"]["version"], 4)

    def test_full_snapshot_contains_runtime_ui_schema(self) -> None:
        async def scenario() -> dict[str, object]:
            context = await detect_fixture_path(FIXTURE_PATH)
            values = await read_fixture_values(context)
            return build_fixture_snapshot(context, values=values, full_snapshot=True)

        snapshot = asyncio.run(scenario())

        self.assertEqual(snapshot["inverter"]["driver_key"], "modbus_smg")
        self.assertEqual(snapshot["ui_schema"]["version"], 4)
        self.assertIn("power_flow_summary", snapshot["values"])

    def test_pi18_catalog_fixture_detects_and_reads_without_runtime_registration(self) -> None:
        async def scenario() -> tuple[object, dict[str, object]]:
            context = await detect_fixture_path(PI18_FIXTURE_PATH)
            values = await read_fixture_values(context)
            return context, values

        context, values = asyncio.run(scenario())

        self.assertEqual(context.inverter.driver_key, "pi18")
        self.assertEqual(context.inverter.protocol_family, "pi18")
        self.assertEqual(context.inverter.model_name, "PI18 5000")
        self.assertEqual(values["operating_mode"], "Hybrid")
        self.assertEqual(values["pv_generation_sum"], 1234)
        self.assertTrue(values["buzzer_enabled"])

    def test_pi30_catalog_fixture_detects_vmii_overlay_and_reads_live_values(self) -> None:
        async def scenario() -> tuple[object, dict[str, object]]:
            context = await detect_fixture_path(PI30_FIXTURE_PATH)
            values = await read_fixture_values(context)
            return context, values

        context, values = asyncio.run(scenario())

        self.assertEqual(context.inverter.driver_key, "pi30")
        self.assertEqual(context.inverter.protocol_family, "pi30")
        self.assertEqual(context.inverter.model_name, "PowMr 4.2kW")
        self.assertEqual(context.inverter.variant_key, "pi30_max")
        self.assertEqual(context.inverter.profile_name, "pi30_ascii/models/pi30_max.json")
        self.assertEqual(context.inverter.register_schema_name, "pi30_ascii/models/pi30_max.json")
        self.assertEqual(values["operating_mode"], "Line")
        self.assertEqual(values["alarm_status"], "PV loss warning")
        self.assertEqual(values["tracker_temperature"], 0)
        self.assertNotIn("pv_generation_sum", values)


if __name__ == "__main__":
    unittest.main()
