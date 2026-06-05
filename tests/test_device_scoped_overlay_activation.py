from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


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
from custom_components.eybond_local.models import CollectorInfo  # noqa: E402


class DeviceScopedOverlayActivationTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_external_profile_roots(())
        set_external_register_schema_roots(())
        clear_profile_loader_cache()
        clear_register_schema_loader_cache()
        super().tearDown()

    def test_activation_scope_base_profile_mismatch_prevents_overlay_application(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
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
                    smartess_protocol_profile_key="smartess_0925",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "modbus_smg/family_fallback.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                        },
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, "smg_modbus.json")

    def test_activation_scope_with_matching_profile_and_smartess_key_applies_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
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
                    smartess_protocol_profile_key="smartess_0925",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "smg_modbus.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                            "smartess_protocol_profile_key": "smartess_0925",
                        },
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)
            self.assertEqual(selection.register_schema_name, schema_name)

    def test_missing_runtime_serial_fails_closed_for_expected_session_serial(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000025388419",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, "smg_modbus.json")

    def test_activation_scope_variant_and_model_must_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-001",
                    variant_key="family_fallback",
                    model_name="SMG-6200",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000025388419",
                    smartess_device_address=1,
                    smartess_protocol_profile_key="smartess_0925",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "smg_modbus.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                            "variant_key": "verified_model",
                            "inverter_model": "SMG-5200",
                            "smartess_protocol_profile_key": "smartess_0925",
                        },
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, "smg_modbus.json")


def _write_local_overlay_files(root: Path) -> tuple[str, str]:
    profiles_root = root / "profiles"
    schemas_root = root / "register_schemas"
    profile_name = "learned/shadow_learning/device/overlay_profile.json"
    schema_name = "learned/shadow_learning/device/overlay_schema.json"
    profile_path = profiles_root / profile_name
    schema_path = schemas_root / schema_name
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
    return profile_name, schema_name


if __name__ == "__main__":
    unittest.main()
