from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.local_metadata import (
    create_local_profile_draft,
    create_local_schema_draft,
    draft_activates_automatically,
    ensure_local_metadata_dirs,
    local_profile_override_details,
    local_register_schema_override_details,
)


class LocalMetadataTests(unittest.TestCase):
    def test_detects_when_one_draft_name_overrides_builtin_metadata(self) -> None:
        self.assertTrue(draft_activates_automatically("smg_modbus.json", None))
        self.assertTrue(draft_activates_automatically("smg_modbus.json", "smg_modbus.json"))
        self.assertTrue(
            draft_activates_automatically(
                "modbus_smg/models/smg_6200.json",
                "./modbus_smg/models/smg_6200.json",
            )
        )
        self.assertFalse(draft_activates_automatically("smg_modbus.json", "drafts/smg_modbus.json"))
        self.assertFalse(
            draft_activates_automatically(
                "modbus_smg/models/smg_6200.json",
                "modbus_smg/models/local_smg_6200.json",
            )
        )

    def test_creates_local_profile_draft_from_builtin_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            path = create_local_profile_draft(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
            )

            self.assertEqual(path.name, "smg_modbus.json")
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(raw["draft_of"], "smg_modbus.json")
            self.assertTrue(raw["experimental"])
            self.assertIn("(Local Draft)", raw["title"])
            override = local_profile_override_details(config_dir, "smg_modbus.json")
            self.assertTrue(override["exists"])
            self.assertEqual(override["path"], str(path))

    def test_creates_local_schema_draft_that_extends_builtin_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            path = create_local_schema_draft(
                config_dir=config_dir,
                source_schema_name="modbus_smg/models/smg_6200.json",
            )

            self.assertEqual(path.name, "smg_6200.json")
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                raw["extends"],
                "builtin:modbus_smg/models/smg_6200.json",
            )
            self.assertEqual(raw["driver_key"], "modbus_smg")
            self.assertEqual(raw["protocol_family"], "modbus_smg")
            self.assertIn("(Local Draft)", raw["title"])
            override = local_register_schema_override_details(
                config_dir,
                "modbus_smg/models/smg_6200.json",
            )
            self.assertTrue(override["exists"])
            self.assertEqual(override["path"], str(path))

    def test_reports_missing_local_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            profile_override = local_profile_override_details(config_dir, "smg_modbus.json")
            schema_override = local_register_schema_override_details(
                config_dir,
                "modbus_smg/models/smg_6200.json",
            )

            self.assertFalse(profile_override["exists"])
            self.assertIn("No active local override", str(profile_override["status"]))
            self.assertFalse(schema_override["exists"])
            self.assertIn("No active local override", str(schema_override["status"]))


if __name__ == "__main__":
    unittest.main()
