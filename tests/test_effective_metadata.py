from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.const import (  # noqa: E402
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
)
from custom_components.eybond_local.metadata.effective_metadata import (  # noqa: E402
    resolve_effective_metadata_selection,
)
from custom_components.eybond_local.models import CollectorInfo  # noqa: E402


class EffectiveMetadataSelectionTests(unittest.TestCase):
    def test_resolves_smartess_catalog_metadata_from_saved_entry_data(self) -> None:
        selection = resolve_effective_metadata_selection(
            entry_data={
                CONF_SMARTESS_PROTOCOL_ASSET_ID: "0925",
                CONF_SMARTESS_PROFILE_KEY: "smartess_0925",
            }
        )

        self.assertEqual(selection.effective_owner_key, "pi30")
        self.assertEqual(selection.effective_owner_name, "PI30-family runtime")
        self.assertEqual(selection.smartess_family_name, "SmartESS 0925")
        self.assertEqual(selection.raw_profile_name, "smartess_local/models/0925.json")
        self.assertEqual(
            selection.raw_register_schema_name,
            "smartess_local/models/0925.json",
        )
        self.assertEqual(selection.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(
            selection.register_schema_name,
            "pi30_ascii/models/smartess_0925_compat.json",
        )
        self.assertIsNotNone(selection.smartess_protocol)
        self.assertIsNotNone(selection.profile_metadata)
        self.assertIsNotNone(selection.register_schema_metadata)
        self.assertEqual(getattr(selection.profile_metadata, "source_scope", ""), "builtin")
        self.assertEqual(
            getattr(selection.register_schema_metadata, "source_scope", ""),
            "builtin",
        )

    def test_prefers_live_collector_smartess_hint_over_saved_entry_data(self) -> None:
        selection = resolve_effective_metadata_selection(
            collector=CollectorInfo(
                smartess_protocol_asset_id="0925",
                smartess_protocol_profile_key="smartess_0925",
            ),
            entry_data={
                CONF_SMARTESS_PROTOCOL_ASSET_ID: "0200",
                CONF_SMARTESS_PROFILE_KEY: "smartess_0200",
            },
        )

        self.assertEqual(selection.effective_owner_key, "pi30")
        self.assertEqual(selection.effective_owner_name, "PI30-family runtime")
        self.assertEqual(selection.smartess_family_name, "SmartESS 0925")
        self.assertEqual(selection.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(
            selection.register_schema_name,
            "pi30_ascii/models/smartess_0925_compat.json",
        )

    def test_preserves_runtime_owner_alongside_smartess_family_label(self) -> None:
        selection = resolve_effective_metadata_selection(
            driver=types.SimpleNamespace(
                key="pi30",
                name="PI30 / ASCII",
                profile_name="pi30_ascii/models/smartess_0925_compat.json",
                register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
            ),
            entry_data={
                CONF_SMARTESS_PROTOCOL_ASSET_ID: "0925",
                CONF_SMARTESS_PROFILE_KEY: "smartess_0925",
            },
        )

        self.assertEqual(selection.effective_owner_key, "pi30")
        self.assertEqual(selection.effective_owner_name, "PI30-family runtime")
        self.assertEqual(selection.smartess_family_name, "SmartESS 0925")
        self.assertEqual(selection.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(
            selection.register_schema_name,
            "pi30_ascii/models/smartess_0925_compat.json",
        )

    def test_preserves_runtime_owner_when_smartess_hint_conflicts_with_effective_metadata(self) -> None:
        selection = resolve_effective_metadata_selection(
            driver=types.SimpleNamespace(
                key="modbus_smg",
                name="SMG / Modbus",
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            ),
            entry_data={
                CONF_SMARTESS_PROTOCOL_ASSET_ID: "0925",
                CONF_SMARTESS_PROFILE_KEY: "smartess_0925",
            },
        )

        self.assertEqual(selection.effective_owner_key, "modbus_smg")
        self.assertEqual(selection.effective_owner_name, "SMG-family runtime")
        self.assertEqual(selection.smartess_family_name, "SmartESS 0925")
        self.assertEqual(selection.raw_profile_name, "smartess_local/models/0925.json")
        self.assertEqual(selection.profile_name, "smg_modbus.json")
        self.assertEqual(
            selection.register_schema_name,
            "modbus_smg/models/smg_6200.json",
        )


if __name__ == "__main__":
    unittest.main()