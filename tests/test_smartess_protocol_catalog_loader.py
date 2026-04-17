from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.smartess_protocol_catalog_loader import (  # noqa: E402
    load_smartess_protocol_catalog,
    resolve_smartess_protocol_catalog_entry,
)


class SmartEssProtocolCatalogLoaderTests(unittest.TestCase):
    def test_loads_known_asset_ids(self) -> None:
        catalog = load_smartess_protocol_catalog()

        self.assertEqual(set(catalog.protocols), {"0200", "0911", "0912", "0921", "0925"})

    def test_loads_0200_entry(self) -> None:
        catalog = load_smartess_protocol_catalog()
        entry = catalog.protocols["0200"]

        self.assertEqual(entry.profile_key, "smartess_0200")
        self.assertEqual(entry.proto_name, "宁波德业")
        self.assertEqual(entry.proto_version, "1.0.0")
        self.assertEqual(entry.device_addresses, (1,))
        self.assertEqual(entry.write_one_function_code, 16)
        self.assertEqual(entry.write_more_function_code, 16)
        self.assertEqual(entry.system_info_function_codes, (3,))
        self.assertEqual(entry.system_setting_function_codes, (3,))

    def test_loads_mixed_function_code_profiles(self) -> None:
        catalog = load_smartess_protocol_catalog()

        entry_0911 = catalog.protocols["0911"]
        self.assertEqual(entry_0911.device_addresses, (3,))
        self.assertEqual(entry_0911.system_info_function_codes, (4,))
        self.assertEqual(entry_0911.system_setting_function_codes, (3,))
        self.assertIn("PVInverter", entry_0911.system_info_groups)

        entry_0912 = catalog.protocols["0912"]
        self.assertEqual(entry_0912.device_addresses, (4,))
        self.assertEqual(entry_0912.root_count, 953)
        self.assertEqual(entry_0912.system_info_segment_count, 12)
        self.assertEqual(entry_0912.system_setting_segment_count, 9)
        self.assertEqual(entry_0912.raw_profile_name, "smartess_local/models/0912.json")
        self.assertEqual(entry_0912.raw_register_schema_name, "smartess_local/models/0912.json")
        self.assertEqual(entry_0912.profile_name, "smartess_local/models/0912.json")
        self.assertEqual(entry_0912.register_schema_name, "smartess_local/models/0912.json")

        entry_0921 = catalog.protocols["0921"]
        self.assertEqual(entry_0921.device_addresses, (1,))
        self.assertEqual(entry_0921.root_count, 97)
        self.assertEqual(entry_0921.system_info_segment_count, 13)
        self.assertEqual(entry_0921.system_setting_segment_count, 13)
        self.assertEqual(entry_0921.raw_profile_name, "smartess_local/models/0921.json")
        self.assertEqual(entry_0921.raw_register_schema_name, "smartess_local/models/0921.json")
        self.assertEqual(entry_0921.profile_name, "smartess_local/models/0921.json")
        self.assertEqual(entry_0921.register_schema_name, "smartess_local/models/0921.json")

        entry_0925 = catalog.protocols["0925"]
        self.assertEqual(entry_0925.device_addresses, (5,))
        self.assertEqual(entry_0925.root_count, 91)
        self.assertEqual(entry_0925.system_info_segment_count, 6)
        self.assertEqual(entry_0925.system_setting_segment_count, 3)
        self.assertEqual(entry_0925.asset_name, "0925.json")
        self.assertEqual(entry_0925.raw_profile_name, "smartess_local/models/0925.json")
        self.assertEqual(entry_0925.raw_register_schema_name, "smartess_local/models/0925.json")
        self.assertEqual(entry_0925.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(entry_0925.register_schema_name, "pi30_ascii/models/smartess_0925_compat.json")

    def test_resolves_entry_by_profile_key(self) -> None:
        entry = resolve_smartess_protocol_catalog_entry(profile_key="smartess_0925")

        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry.asset_id, "0925")
        self.assertEqual(entry.raw_profile_name, "smartess_local/models/0925.json")
        self.assertEqual(entry.profile_name, "pi30_ascii/models/smartess_0925_compat.json")

        entry_0921 = resolve_smartess_protocol_catalog_entry(profile_key="smartess_0921")

        self.assertIsNotNone(entry_0921)
        assert entry_0921 is not None
        self.assertEqual(entry_0921.asset_id, "0921")
        self.assertEqual(entry_0921.profile_name, "smartess_local/models/0921.json")

        entry_0912 = resolve_smartess_protocol_catalog_entry(profile_key="smartess_0912")

        self.assertIsNotNone(entry_0912)
        assert entry_0912 is not None
        self.assertEqual(entry_0912.asset_id, "0912")
        self.assertEqual(entry_0912.profile_name, "smartess_local/models/0912.json")


if __name__ == "__main__":
    unittest.main()