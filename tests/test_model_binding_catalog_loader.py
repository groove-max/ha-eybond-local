from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.pi18 import Pi18Driver  # noqa: E402
from custom_components.eybond_local.drivers.pi30 import Pi30Driver  # noqa: E402
from custom_components.eybond_local.drivers.registry import prime_metadata_caches  # noqa: E402
from custom_components.eybond_local.drivers.smg import SmgModbusDriver  # noqa: E402
from custom_components.eybond_local.metadata.model_binding_catalog_loader import (  # noqa: E402
    DriverModelBinding,
    DriverModelBindingCatalog,
    load_driver_model_binding_catalog,
    resolve_driver_model_binding,
)
from custom_components.eybond_local.metadata.smartess_protocol_catalog_loader import (  # noqa: E402
    SmartEssProtocolCatalog,
    SmartEssProtocolCatalogEntry,
)


class DriverModelBindingCatalogLoaderTests(unittest.TestCase):
    def test_loads_builtin_driver_bindings(self) -> None:
        catalog = load_driver_model_binding_catalog()

        self.assertEqual(
            set(catalog.bindings),
            {
                ("pi30", "default"),
                ("pi18", "default"),
                ("modbus_smg", "anenji_anj_11kw_48v_wifi_p"),
                ("modbus_smg", "default"),
            },
        )

    def test_resolves_pi30_default_binding(self) -> None:
        binding = resolve_driver_model_binding("pi30")

        assert binding is not None
        self.assertEqual(binding.protocol_family, "pi30")
        self.assertEqual(binding.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(binding.register_schema_name, "pi30_ascii/models/smartess_0925_compat.json")

    def test_drivers_use_catalog_backed_metadata_names(self) -> None:
        self.assertEqual(Pi30Driver().profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(Pi30Driver().register_schema_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(Pi18Driver().profile_name, "")
        self.assertEqual(Pi18Driver().register_schema_name, "pi18_ascii/base.json")
        self.assertEqual(SmgModbusDriver().profile_name, "smg_modbus.json")
        self.assertEqual(SmgModbusDriver().register_schema_name, "modbus_smg/models/smg_6200.json")

    def test_resolves_smg_anenji_variant_binding(self) -> None:
        binding = resolve_driver_model_binding(
            "modbus_smg",
            variant_key="anenji_anj_11kw_48v_wifi_p",
        )

        assert binding is not None
        self.assertEqual(
            binding.profile_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(
            binding.register_schema_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )

    def test_prime_metadata_caches_warms_catalog_driven_metadata(self) -> None:
        binding_catalog = DriverModelBindingCatalog(
            bindings={
                ("modbus_smg", "anenji_anj_11kw_48v_wifi_p"): DriverModelBinding(
                    driver_key="modbus_smg",
                    protocol_family="modbus_smg",
                    variant_key="anenji_anj_11kw_48v_wifi_p",
                    profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                    register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                )
            }
        )
        protocol_catalog = SmartEssProtocolCatalog(
            protocols={
                "0925": SmartEssProtocolCatalogEntry(
                    asset_id="0925",
                    profile_key="smartess_0925",
                    raw_profile_name="smartess_local/models/0925.json",
                    raw_register_schema_name="smartess_local/models/0925.json",
                    profile_name="pi30_ascii/models/smartess_0925_compat.json",
                    register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
                )
            }
        )

        with (
            patch("custom_components.eybond_local.drivers.registry.all_measurements"),
            patch("custom_components.eybond_local.drivers.registry.all_binary_sensors"),
            patch("custom_components.eybond_local.drivers.registry.all_write_capabilities"),
            patch("custom_components.eybond_local.drivers.registry.all_capability_groups"),
            patch("custom_components.eybond_local.drivers.registry.all_capability_presets"),
            patch(
                "custom_components.eybond_local.drivers.registry.load_driver_model_binding_catalog",
                return_value=binding_catalog,
            ),
            patch(
                "custom_components.eybond_local.drivers.registry.load_smartess_protocol_catalog",
                return_value=protocol_catalog,
            ),
            patch("custom_components.eybond_local.drivers.registry.load_driver_profile") as load_profile,
            patch("custom_components.eybond_local.drivers.registry.load_register_schema") as load_schema,
        ):
            prime_metadata_caches()

        self.assertEqual(
            [call.args[0] for call in load_profile.call_args_list],
            [
                "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                "smartess_local/models/0925.json",
                "pi30_ascii/models/smartess_0925_compat.json",
            ],
        )
        self.assertEqual(
            [call.args[0] for call in load_schema.call_args_list],
            [
                "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                "smartess_local/models/0925.json",
                "pi30_ascii/models/smartess_0925_compat.json",
            ],
        )


if __name__ == "__main__":
    unittest.main()