from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.smg_identity_anchor_catalog_loader import (  # noqa: E402
    clear_smg_identity_anchor_catalog_cache,
    load_smg_identity_anchor_catalog,
    resolve_smg_identity_anchor,
)


class SmgIdentityAnchorCatalogLoaderTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_smg_identity_anchor_catalog_cache()

    def test_loads_known_identity_anchors(self) -> None:
        catalog = load_smg_identity_anchor_catalog()

        self.assertEqual(catalog.protocol_family, "modbus_smg")
        self.assertEqual(
            tuple(catalog.layout_groups),
            (
                "protocol_1_common",
                "anenji_protocol_3_10",
            ),
        )
        self.assertEqual(catalog.base_layout_groups, ("protocol_1_common",))
        self.assertEqual(
            catalog.variant_layout_groups,
            {
                "anenji_anj_11kw_48v_wifi_p": ("anenji_protocol_3_10",),
            },
        )
        self.assertEqual(
            set(catalog.read_groups),
            {
                "serial_identity",
                "live_identity",
                "config_identity",
                "aux_identity",
                "scalar_identity",
            },
        )
        self.assertEqual(
            set(catalog.anchors),
            {
                "serial",
                "operating_mode",
                "output_rating_voltage",
                "output_rating_frequency",
                "protocol_number",
                "device_type",
                "rated_power",
                "turn_on_mode",
                "remote_switch",
                "pv_grid_connected_max_power",
            },
        )

    def test_groups_are_deterministic_and_declared_ordered(self) -> None:
        catalog = load_smg_identity_anchor_catalog()

        aux_anchor_keys = tuple(anchor.key for anchor in catalog.anchors_for_group("aux_identity"))
        self.assertEqual(
            aux_anchor_keys,
            (
                "protocol_number",
                "device_type",
                "turn_on_mode",
                "remote_switch",
                "pv_grid_connected_max_power",
            ),
        )

    def test_anchor_sources_match_expected_register_concepts(self) -> None:
        catalog = load_smg_identity_anchor_catalog()

        self.assertEqual(catalog.anchors["serial"].source_type, "block")
        self.assertEqual(catalog.anchors["serial"].block_key, "serial")

        self.assertEqual(catalog.anchors["operating_mode"].source_type, "spec")
        self.assertEqual(catalog.anchors["operating_mode"].spec_set_key, "live")
        self.assertEqual(catalog.anchors["operating_mode"].register_key, "operating_mode")

        self.assertEqual(catalog.anchors["rated_power"].source_type, "scalar")
        self.assertEqual(catalog.anchors["rated_power"].scalar_key, "rated_power_register")

    def test_resolve_anchor_by_key(self) -> None:
        anchor = resolve_smg_identity_anchor("protocol_number")

        assert anchor is not None
        self.assertEqual(anchor.key, "protocol_number")
        self.assertEqual(anchor.read_group, "aux_identity")
        self.assertEqual(
            anchor.layout_groups,
            (
                "protocol_1_common",
                "anenji_protocol_3_10",
            ),
        )

    def test_validation_rejects_unknown_group(self) -> None:
        clear_smg_identity_anchor_catalog_cache()
        invalid_catalog = {
            "protocol_family": "modbus_smg",
            "layout_groups": [
                {
                    "key": "protocol_1_common",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                }
            ],
            "base_layout_groups": ["protocol_1_common"],
            "variant_layout_groups": {},
            "read_groups": [{"key": "serial_identity"}],
            "anchors": [
                {
                    "key": "serial",
                    "read_group": "missing_group",
                    "source_type": "block",
                    "block_key": "serial",
                    "layout_groups": ["protocol_1_common"],
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smg_identity_anchor_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "unknown_read_group"):
                load_smg_identity_anchor_catalog()

    def test_validation_rejects_missing_anchor_layout_groups(self) -> None:
        clear_smg_identity_anchor_catalog_cache()
        invalid_catalog = {
            "protocol_family": "modbus_smg",
            "layout_groups": [
                {
                    "key": "protocol_1_common",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                }
            ],
            "base_layout_groups": ["protocol_1_common"],
            "variant_layout_groups": {},
            "read_groups": [{"key": "serial_identity"}],
            "anchors": [
                {
                    "key": "serial",
                    "read_group": "serial_identity",
                    "source_type": "block",
                    "block_key": "serial",
                    "layout_groups": [],
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smg_identity_anchor_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "missing_anchor_layout_groups"):
                load_smg_identity_anchor_catalog()

    def test_validation_rejects_duplicate_anchor_layout_groups(self) -> None:
        clear_smg_identity_anchor_catalog_cache()
        invalid_catalog = {
            "protocol_family": "modbus_smg",
            "layout_groups": [
                {
                    "key": "protocol_1_common",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                }
            ],
            "base_layout_groups": ["protocol_1_common"],
            "variant_layout_groups": {},
            "read_groups": [{"key": "serial_identity"}],
            "anchors": [
                {
                    "key": "serial",
                    "read_group": "serial_identity",
                    "source_type": "block",
                    "block_key": "serial",
                    "layout_groups": ["protocol_1_common", "protocol_1_common"],
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smg_identity_anchor_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "duplicate_anchor_layout_group"):
                load_smg_identity_anchor_catalog()

    def test_validation_rejects_variant_layout_group_overlap_with_base(self) -> None:
        clear_smg_identity_anchor_catalog_cache()
        invalid_catalog = {
            "protocol_family": "modbus_smg",
            "layout_groups": [
                {
                    "key": "protocol_1_common",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                }
            ],
            "base_layout_groups": ["protocol_1_common"],
            "variant_layout_groups": {
                "anenji_anj_11kw_48v_wifi_p": ["protocol_1_common"],
            },
            "read_groups": [{"key": "serial_identity"}],
            "anchors": [
                {
                    "key": "serial",
                    "read_group": "serial_identity",
                    "source_type": "block",
                    "block_key": "serial",
                    "layout_groups": ["protocol_1_common"],
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smg_identity_anchor_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "ambiguous_variant_layout_group_with_base"):
                load_smg_identity_anchor_catalog()

    def test_validation_rejects_shared_variant_layout_group_owner(self) -> None:
        clear_smg_identity_anchor_catalog_cache()
        invalid_catalog = {
            "protocol_family": "modbus_smg",
            "layout_groups": [
                {
                    "key": "protocol_1_common",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                },
                {
                    "key": "anenji_protocol_3_10",
                    "register_schema_name": "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                },
            ],
            "base_layout_groups": ["protocol_1_common"],
            "variant_layout_groups": {
                "anenji_anj_11kw_48v_wifi_p": ["anenji_protocol_3_10"],
                "synthetic_variant": ["anenji_protocol_3_10"],
            },
            "read_groups": [{"key": "serial_identity"}],
            "anchors": [
                {
                    "key": "serial",
                    "read_group": "serial_identity",
                    "source_type": "block",
                    "block_key": "serial",
                    "layout_groups": ["protocol_1_common", "anenji_protocol_3_10"],
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smg_identity_anchor_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "ambiguous_variant_layout_group_owner"):
                load_smg_identity_anchor_catalog()


if __name__ == "__main__":
    unittest.main()
