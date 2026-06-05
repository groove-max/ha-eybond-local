from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
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
from custom_components.eybond_local.metadata.profile_loader import (  # noqa: E402
    clear_profile_loader_cache,
    set_external_profile_roots,
)
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    clear_register_schema_loader_cache,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.metadata.effective_metadata_snapshot import (  # noqa: E402
    build_effective_metadata_snapshot,
)
from custom_components.eybond_local.models import CollectorInfo  # noqa: E402


class EffectiveMetadataSelectionTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_external_profile_roots(())
        set_external_register_schema_roots(())
        clear_profile_loader_cache()
        clear_register_schema_loader_cache()
        super().tearDown()

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

    def test_uses_persisted_snapshot_metadata_when_live_inverter_is_absent(self) -> None:
        selection = resolve_effective_metadata_selection(
            driver=types.SimpleNamespace(
                key="modbus_smg",
                name="SMG / Modbus",
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            ),
            persisted_snapshot=build_effective_metadata_snapshot(
                effective_owner_key="modbus_smg",
                effective_owner_name="SMG-family runtime",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                confidence="high",
                generation=3,
            ),
        )

        self.assertEqual(selection.effective_owner_key, "modbus_smg")
        self.assertEqual(selection.effective_owner_name, "SMG-family runtime")
        self.assertEqual(selection.profile_name, "modbus_smg/models/anenji_4200_protocol_1.json")
        self.assertEqual(
            selection.register_schema_name,
            "modbus_smg/models/anenji_4200_protocol_1.json",
        )
        self.assertEqual(
            getattr(selection.profile_metadata, "key", ""),
            "modbus_smg_anenji_4200_protocol_1",
        )
        self.assertEqual(
            getattr(selection.register_schema_metadata, "key", ""),
            "modbus_smg_anenji_4200_protocol_1",
        )

    def test_prefers_live_inverter_metadata_over_persisted_snapshot(self) -> None:
        selection = resolve_effective_metadata_selection(
            inverter=types.SimpleNamespace(
                driver_key="modbus_smg",
                profile_name="modbus_smg/models/smg_6200.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            ),
            driver=types.SimpleNamespace(
                key="modbus_smg",
                name="SMG / Modbus",
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            ),
            persisted_snapshot=build_effective_metadata_snapshot(
                effective_owner_key="modbus_smg",
                effective_owner_name="SMG-family runtime",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                confidence="high",
                generation=3,
            ),
        )

        self.assertEqual(selection.profile_name, "modbus_smg/models/smg_6200.json")
        self.assertEqual(selection.register_schema_name, "modbus_smg/models/smg_6200.json")

    def test_invalid_persisted_snapshot_keeps_legacy_fallback(self) -> None:
        selection = resolve_effective_metadata_selection(
            driver=types.SimpleNamespace(
                key="modbus_smg",
                name="SMG / Modbus",
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            ),
            persisted_snapshot=build_effective_metadata_snapshot(
                effective_owner_key="modbus_smg",
                effective_owner_name="SMG-family runtime",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                confidence="none",
                generation=3,
            ),
        )

        self.assertEqual(selection.profile_name, "smg_modbus.json")
        self.assertEqual(selection.register_schema_name, "modbus_smg/models/smg_6200.json")

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

    def test_collector_cloud_profile_identity_does_not_imply_inverter_profile_identity(self) -> None:
        selection = resolve_effective_metadata_selection(
            driver=types.SimpleNamespace(
                key="modbus_smg",
                name="SMG / Modbus",
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            ),
        )

        # Collector cloud profile evidence (for example SmartValue m2m.eybond.com)
        # is endpoint metadata only and must not rewrite inverter identity.
        self.assertEqual(selection.effective_owner_key, "modbus_smg")
        self.assertEqual(selection.profile_name, "smg_modbus.json")
        self.assertEqual(
            selection.register_schema_name,
            "modbus_smg/models/smg_6200.json",
        )

    def test_applies_activated_device_scoped_overlay_when_runtime_identity_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiles_root = root / "profiles"
            schemas_root = root / "register_schemas"
            learned_profile_name = "learned/shadow_learning/device/learned_profile.json"
            learned_schema_name = "learned/shadow_learning/device/learned_schema.json"
            profile_path = profiles_root / learned_profile_name
            schema_path = schemas_root / learned_schema_name
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.parent.mkdir(parents=True, exist_ok=True)

            profile_path.write_text(
                json.dumps(
                    {
                        "extends": "smg_modbus.json",
                        "profile_key": "local_shadow_test",
                        "title": "Local Shadow Test",
                        "driver_key": "modbus_smg",
                        "protocol_family": "modbus_smg",
                        "groups": [{"key": "config", "title": "Config"}],
                        "shadow_learning_overlay": {
                            "scope": "device",
                            "source_profile_name": "smg_modbus.json",
                            "source_schema_name": "modbus_smg/models/smg_6200.json",
                            "session": {
                                "collector_pn": "E5000025388419",
                                "cloud_sn": "SN-001",
                                "devaddr": 1,
                            },
                        },
                        "capabilities": [
                            {
                                "key": "learned_shadow_705",
                                "register": 705,
                                "value_kind": "u16",
                                "note": "learned",
                                "provenance": "cloud_hint",
                                "learned_provenance": {"scope": "device"},
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(
                    {
                        "extends": "builtin:modbus_smg/models/smg_6200.json",
                        "schema_key": "local_shadow_test",
                        "title": "Local Shadow Test",
                        "driver_key": "modbus_smg",
                        "protocol_family": "modbus_smg",
                        "shadow_learning_overlay": {"scope": "device"},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            set_external_profile_roots((profiles_root,))
            set_external_register_schema_roots((schemas_root,))
            clear_profile_loader_cache()
            clear_register_schema_loader_cache()

            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000025388419",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": learned_profile_name,
                        "register_schema_name": learned_schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, learned_profile_name)
            self.assertEqual(selection.register_schema_name, learned_schema_name)
            self.assertEqual(getattr(selection.profile_metadata, "source_scope", ""), "external")

    def test_ignores_activated_device_scoped_overlay_when_runtime_identity_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiles_root = root / "profiles"
            schemas_root = root / "register_schemas"
            learned_profile_name = "learned/shadow_learning/device/learned_profile.json"
            learned_schema_name = "learned/shadow_learning/device/learned_schema.json"
            profile_path = profiles_root / learned_profile_name
            schema_path = schemas_root / learned_schema_name
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.parent.mkdir(parents=True, exist_ok=True)

            profile_path.write_text(
                json.dumps(
                    {
                        "extends": "smg_modbus.json",
                        "profile_key": "local_shadow_test",
                        "title": "Local Shadow Test",
                        "driver_key": "modbus_smg",
                        "protocol_family": "modbus_smg",
                        "groups": [{"key": "config", "title": "Config"}],
                        "shadow_learning_overlay": {
                            "scope": "device",
                            "source_profile_name": "smg_modbus.json",
                            "source_schema_name": "modbus_smg/models/smg_6200.json",
                            "session": {
                                "collector_pn": "E5000025388419",
                                "cloud_sn": "SN-EXPECTED",
                                "devaddr": 1,
                            },
                        },
                        "capabilities": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(
                    {
                        "extends": "builtin:modbus_smg/models/smg_6200.json",
                        "schema_key": "local_shadow_test",
                        "title": "Local Shadow Test",
                        "driver_key": "modbus_smg",
                        "protocol_family": "modbus_smg",
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            set_external_profile_roots((profiles_root,))
            set_external_register_schema_roots((schemas_root,))
            clear_profile_loader_cache()
            clear_register_schema_loader_cache()

            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-OTHER",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000025388419",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": learned_profile_name,
                        "register_schema_name": learned_schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, "smg_modbus.json")
            self.assertEqual(selection.register_schema_name, "modbus_smg/models/smg_6200.json")


if __name__ == "__main__":
    unittest.main()