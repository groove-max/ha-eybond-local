from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.replay import detect_fixture_path, read_fixture_values
from custom_components.eybond_local.fixtures.catalog import catalog_has_entries
from custom_components.eybond_local.schema import build_runtime_ui_schema


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


def _find_capability(schema: dict[str, object], capability_key: str) -> dict[str, object]:
    for group in schema["groups"]:
        for capability in group["capabilities"]:
            if capability["key"] == capability_key:
                return capability
    raise KeyError(capability_key)


@unittest.skipUnless(
    LOCAL_FIXTURE_TESTS_ENABLED,
    "Local runtime-schema fixture tests are disabled. Set EYBOND_ENABLE_LOCAL_FIXTURE_TESTS=1 and populate .local/fixtures/catalog/.",
)
class RuntimeSchemaTests(unittest.TestCase):
    def test_build_runtime_ui_schema_from_fixture(self) -> None:
        async def scenario() -> tuple[dict[str, object], dict[str, object]]:
            context = await detect_fixture_path(FIXTURE_PATH)
            values = await read_fixture_values(context)
            schema = build_runtime_ui_schema(context.inverter, values)
            return values, schema

        values, schema = asyncio.run(scenario())

        self.assertEqual(schema["version"], 4)
        self.assertEqual(schema["driver_key"], "modbus_smg")
        self.assertEqual(schema["model_name"], "SMG 6200")
        self.assertEqual(schema["overview"]["site_mode"], values["site_mode_state"])
        self.assertEqual(schema["overview"]["power_flow"], values["power_flow_summary"])
        self.assertEqual(schema["overview"]["roles"]["battery"], values["battery_role_state"])

        group_keys = {group["key"] for group in schema["groups"]}
        self.assertTrue({"output", "charging", "battery", "system"}.issubset(group_keys))

        charge_priority = _find_capability(schema, "charge_source_priority")
        self.assertEqual(charge_priority["entity_kind"], "select")
        self.assertTrue(charge_priority["visible"])
        self.assertTrue(charge_priority["editable"])
        self.assertEqual(charge_priority["validation_state"], "tested")
        self.assertEqual(charge_priority["support_tier"], "conditional")

        equalization_voltage = _find_capability(schema, "battery_equalization_voltage")
        self.assertTrue(equalization_voltage["visible"])
        self.assertTrue(equalization_voltage["editable"])
        self.assertEqual(equalization_voltage["status"], "editable")
        self.assertEqual(equalization_voltage["validation_state"], "tested")
        self.assertEqual(equalization_voltage["support_tier"], "conditional")

        power_saving = _find_capability(schema, "power_saving_mode")
        self.assertEqual(power_saving["validation_state"], "untested")
        self.assertEqual(power_saving["support_tier"], "blocked")
        self.assertIn("exception_code:7", power_saving["support_notes"])


if __name__ == "__main__":
    unittest.main()
