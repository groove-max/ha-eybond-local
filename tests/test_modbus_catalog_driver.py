from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402


def _aohai_input_registers() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in (
        (0, 14), (32, 2), (42, 15), (64, 2), (96, 13),
        (127, 2), (141, 12), (284, 6), (320, 12), (379, 12),
    ):
        for offset in range(count):
            registers[start + offset] = 0

    registers.update(
        {
            0: 2,        # Off-Grid
            2: 2301,     # 230.1 V output
            5: 43,       # 4.3 A
            10: 412,     # 41.2 C
            32: 0,       # no fault
            33: 302,     # Low battery warning
            42: 2298,    # grid 229.8 V
            51: 5001,    # 50.01 Hz
            64: 3550,    # PV 355.0 V
            65: 61,      # PV 6.1 A
            127: 264,    # battery 26.4 V (24 V-class unit)
            128: 78,     # SOC 78 %
            141: 0xFF9C, # -1.00 A discharge
            142: 251,    # 25.1 C
            152: 99,     # SOH
            284: 0, 285: 12000,
            286: 0xFFFF, 287: 0xFFFF - 11999,  # signed -12000 raw -> +1200.0 W
            379: 0, 380: 123,  # 12.3 kWh today
        }
    )
    return registers


def _aohai_holding_registers() -> dict[int, int]:
    return {63: 0, 64: 42000}  # rated power raw 42000 -> 4200.0 W


def _target() -> ProbeTarget:
    return ProbeTarget(devcode=1, collector_addr=255, device_addr=1)


def _transport(
    *,
    input_registers: dict[int, int] | None = None,
    holding_registers: dict[int, int] | None = None,
) -> FixtureTransport:
    return FixtureTransport(
        registers=_aohai_holding_registers() if holding_registers is None else holding_registers,
        input_registers=_aohai_input_registers() if input_registers is None else input_registers,
        command_responses=None,
        probe_target=_target(),
    )


def _growatt_input_registers() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in ((0, 45), (48, 35), (93, 1)):
        for offset in range(count):
            registers[start + offset] = 0
    registers.update(
        {
            0: 5,        # PV Charge
            1: 1200,     # PV1 120.0 V
            3: 0, 4: 15000,   # PV1 1500.0 W
            17: 5210,    # battery 52.10 V
            18: 88,      # SOC 88 %
            20: 2299,    # grid 229.9 V
            21: 5001,    # 50.01 Hz
            22: 2301,    # output 230.1 V
            23: 5000,    # 50.00 Hz
            25: 412,     # 41.2 C
            27: 305,     # load 30.5 %
            36: 0, 37: 0,
            48: 0, 49: 123,   # PV1 today 12.3 kWh
            64: 0, 65: 87,    # load today 8.7 kWh
            77: 0xFFFF, 78: 0xEC78,  # battery power raw -5000 -> +500.0 W charging
            93: 89,      # BMS SOC
        }
    )
    return registers


def _growatt_holding_registers() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in ((0, 9), (18, 26), (73, 7)):
        for offset in range(count):
            registers[start + offset] = 0
    registers.update(
        {
            1: 1,        # PV First
            2: 2,        # PV Only
            8: 0,        # APL
            22: 1,       # buzzer on
            34: 70,      # max charge current
            43: 3450,    # DTC: OffGrid SPF 3-5K
            73: 207,     # modbus v2.07
            76: 0, 77: 50000,  # rated 5000.0 W
            78: 0, 79: 50000,
        }
    )
    return registers


def _solis_input_registers() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in (
        (35000, 1), (33000, 49), (33049, 47), (33115, 7), (33132, 50),
    ):
        for offset in range(count):
            registers[start + offset] = 0
    registers.update(
        {
            35000: 8240,   # 0x2030: 1-phase LV hybrid, protocol 0x20
            33095: 15,     # Normal Running
            33049: 3210,   # PV1 321.0 V
            33057: 0, 33058: 2500,  # PV 2500 W
            33073: 2302,   # grid 230.2 V
            33094: 5002,   # 50.02 Hz
            33133: 512,    # battery 51.2 V
            33135: 0,      # charging
            33139: 77,     # SOC 77 %
            33147: 900,    # household load 900 W
            33148: 150,    # backup load 150 W
            33149: 0, 33150: 1200,  # battery power magnitude 1200 W
            33151: 0xFFFF, 33152: 0xFE0C,  # grid port -500 W (importing)
            33035: 123,    # 12.3 kWh today
        }
    )
    return registers


def _deye_holding_registers() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in ((0, 20), (59, 50), (109, 8), (150, 47), (312, 12)):
        for offset in range(count):
            registers[start + offset] = 0
    registers.update(
        {
            0: 768,      # 0x0300 single-phase LV storage
            16: 50000,   # rated 5000.0 W (low word)
            59: 2,       # Normal
            79: 5000,    # 50.00 Hz
            90: 1385,    # DC transformer 38.5 C
            108: 156,    # PV today 15.6 kWh
            150: 2305,   # grid 230.5 V
            169: 0xFF38, # grid power -200 W (selling)
            178: 850,    # load 850 W
            182: 1215,   # battery 21.5 C
            183: 5230,   # battery 52.30 V
            184: 91,     # SOC 91 %
            186: 1200,   # PV1 1200 W
            187: 800,    # PV2 800 W
            190: 0xFC18, # battery power raw -1000 (charging) -> +1000 W
            192: 5001,   # 50.01 Hz
        }
    )
    return registers


class ModbusCatalogDriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_matches_aohai_plausibility_anchors(self) -> None:
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(_transport(), _target())

        assert inverter is not None
        self.assertEqual(inverter.driver_key, "modbus_catalog")
        self.assertEqual(inverter.model_name, "Sandi Aohai FSA (Modbus)")
        self.assertEqual(inverter.register_schema_name, "aohai_fsa/base.json")
        detection = inverter.details["catalog_detection"]
        self.assertEqual(detection["surface_key"], "aohai_fsa_read_only")
        self.assertIn("identity.aohai_battery_percent_raw", detection["evidence"])

    async def test_probe_matches_48v_variant_with_same_map(self) -> None:
        # Family envelope anchors must accept every electrical variant that
        # shares the register map (24 V vs 48 V units).
        registers = _aohai_input_registers()
        registers[127] = 512  # 51.2 V battery
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(
            _transport(input_registers=registers), _target()
        )
        self.assertIsNotNone(inverter)

    async def test_probe_rejects_out_of_envelope_registers(self) -> None:
        registers = _aohai_input_registers()
        registers[0] = 9999      # not a known status code
        registers[128] = 250     # SOC out of range
        registers[127] = 12000   # 1200 V battery — outside any variant
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(
            _transport(input_registers=registers, holding_registers={63: 0, 64: 0}),
            _target(),
        )
        self.assertIsNone(inverter)

    async def test_probe_rejects_silent_device(self) -> None:
        driver = ModbusCatalogDriver()
        transport = FixtureTransport(
            registers={},
            input_registers={},
            command_responses=None,
            probe_target=_target(),
        )
        inverter = await driver.async_probe(transport, _target())
        self.assertIsNone(inverter)

    async def test_read_values_decodes_input_and_holding_spaces(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport()
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["inverter_operation_mode"], "Off-Grid")
        self.assertEqual(values["output_voltage"], 230.1)
        self.assertEqual(values["battery_voltage"], 26.4)
        self.assertEqual(values["battery_percent"], 78)
        self.assertEqual(values["battery_current"], -1.0)
        self.assertEqual(values["warning_code"], "Low battery")
        self.assertEqual(values["grid_frequency"], 50.01)
        # Holding-space rated power must not leak into the input-space
        # pv_input_voltage (register 64 exists in both address spaces).
        self.assertEqual(values["rated_power"], 4200.0)
        self.assertEqual(values["pv_input_voltage"], 355.0)
        # Signed 32-bit with negative multiplier (raw -12000 -> +1200.0 W).
        self.assertEqual(values["output_power"], 1200.0)
        self.assertEqual(values["pv_generation_day"], 12.3)

    async def test_probe_matches_growatt_spf_by_device_type_code(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers=_growatt_input_registers(),
            holding_registers=_growatt_holding_registers(),
        )

        inverter = await driver.async_probe(transport, _target())

        assert inverter is not None
        self.assertEqual(inverter.model_name, "Growatt SPF Off-Grid (Modbus)")
        self.assertEqual(inverter.variant_key, "growatt_spf")
        self.assertEqual(inverter.register_schema_name, "growatt_spf/base.json")
        self.assertEqual(inverter.profile_name, "modbus_catalog/growatt_spf.json")

    async def test_growatt_read_values_decode_including_battery_sign_flip(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers=_growatt_input_registers(),
            holding_registers=_growatt_holding_registers(),
        )
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["inverter_operation_mode"], "PV Charge")
        self.assertEqual(values["pv1_input_power"], 1500.0)
        self.assertEqual(values["battery_voltage"], 52.1)
        self.assertEqual(values["battery_percent"], 88)
        self.assertEqual(values["grid_voltage"], 229.9)
        self.assertEqual(values["load_percent"], 30.5)
        # Growatt reports discharge-positive; canonical convention is
        # charge-positive, so raw -5000 decodes to +500.0 W.
        self.assertEqual(values["battery_power"], 500.0)
        self.assertEqual(values["pv1_generation_day"], 12.3)
        self.assertEqual(values["load_consumption_day"], 8.7)
        # Holding-space config values decode via their enum tables.
        self.assertEqual(values["output_source_priority"], "PV First")
        self.assertEqual(values["charge_source_priority"], "PV Only")
        self.assertEqual(values["max_charge_current"], 70)
        self.assertEqual(values["rated_power"], 5000.0)

    async def test_probe_matches_solis_hybrid_by_model_definition(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers=_solis_input_registers(),
            holding_registers={},
        )

        inverter = await driver.async_probe(transport, _target())

        assert inverter is not None
        self.assertEqual(inverter.model_name, "Solis Hybrid (ESINV Modbus)")
        self.assertEqual(inverter.variant_key, "solis_esinv")
        self.assertEqual(inverter.register_schema_name, "solis_esinv/base.json")
        self.assertEqual(inverter.profile_name, "")

    async def test_solis_read_values_and_canonical_sign_conventions(self) -> None:
        from custom_components.eybond_local.canonical_telemetry import (
            apply_canonical_measurements,
        )

        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers=_solis_input_registers(),
            holding_registers={},
        )
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["inverter_current_status"], "Normal Running")
        self.assertEqual(values["pv_power"], 2500)
        self.assertEqual(values["battery_percent"], 77)
        self.assertEqual(values["battery_current_direction"], "Charging")
        self.assertEqual(values["pv_generation_day"], 12.3)

        apply_canonical_measurements(
            "modbus_catalog", values, variant_key="solis_esinv"
        )
        # Household 900 W + backup 150 W = total load.
        self.assertEqual(values["output_power"], 1050)
        # Magnitude 1200 W with direction "Charging" -> +1200 W.
        self.assertEqual(values["battery_power"], 1200.0)
        # Wire value -500 W (export-positive) -> canonical +500 W import.
        self.assertEqual(values["grid_power"], 500.0)

    async def test_probe_matches_deye_lv_hybrid_by_device_type(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers={},
            holding_registers=_deye_holding_registers(),
        )

        inverter = await driver.async_probe(transport, _target())

        assert inverter is not None
        self.assertEqual(inverter.model_name, "Deye Single-Phase LV Hybrid (Modbus)")
        self.assertEqual(inverter.variant_key, "deye_lv")
        self.assertEqual(inverter.profile_name, "modbus_catalog/deye_lv.json")

    async def test_deye_read_values_decode_offsets_and_signs(self) -> None:
        from custom_components.eybond_local.canonical_telemetry import (
            apply_canonical_measurements,
        )

        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers={},
            holding_registers=_deye_holding_registers(),
        )
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["run_state"], "Normal")
        self.assertEqual(values["rated_power"], 5000.0)
        # Offset-1000 temperature encoding: raw 1385 -> 38.5 C.
        self.assertEqual(values["dc_transformer_temperature"], 38.5)
        self.assertEqual(values["battery_temperature"], 21.5)
        self.assertEqual(values["battery_voltage"], 52.3)
        self.assertEqual(values["battery_percent"], 91)
        # Wire is discharge-positive; raw -1000 flips to +1000 W charging.
        self.assertEqual(values["battery_power"], 1000.0)
        # Grid power is already import-positive on the wire (buy > 0).
        self.assertEqual(values["grid_power"], -200)
        self.assertEqual(values["output_power"], 850)
        self.assertEqual(values["pv_generation_day"], 15.6)

        apply_canonical_measurements("modbus_catalog", values, variant_key="deye_lv")
        self.assertEqual(values["pv_power"], 2000)

    async def test_probe_attaches_pack_profile_capabilities(self) -> None:
        from custom_components.eybond_local.control_policy import can_expose_capability
        from custom_components.eybond_local.const import (
            CONTROL_MODE_AUTO,
            CONTROL_MODE_FULL,
        )

        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers=_growatt_input_registers(),
            holding_registers=_growatt_holding_registers(),
        )

        inverter = await driver.async_probe(transport, _target())

        assert inverter is not None
        # Live entity setup reads inverter.capabilities — they must ride
        # along with the detection result, not sit only in the profile file.
        self.assertTrue(inverter.capabilities)
        self.assertTrue(inverter.capability_groups)
        keys = {capability.key for capability in inverter.capabilities}
        self.assertIn("max_charge_current", keys)
        for capability in inverter.capabilities:
            self.assertFalse(capability.tested, capability.key)
            self.assertFalse(
                can_expose_capability(capability, control_mode=CONTROL_MODE_AUTO),
                capability.key,
            )
            self.assertTrue(
                can_expose_capability(capability, control_mode=CONTROL_MODE_FULL),
                capability.key,
            )

        # The write path works straight off the probe result too.
        result = await driver.async_write_capability(
            transport, inverter, "max_charge_current", 80
        )
        self.assertEqual(result, 80)
        self.assertEqual(transport._registers[34], 80)

    async def test_write_capability_reloads_profile_for_restored_entries(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers=_growatt_input_registers(),
            holding_registers=_growatt_holding_registers(),
        )
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None
        # A restored entry can carry the profile name without materialized
        # capabilities; the driver must fall back to the pack profile.
        inverter.capabilities = ()

        result = await driver.async_write_capability(
            transport, inverter, "buzzer_enabled", False
        )
        self.assertIs(result, False)
        self.assertEqual(transport._registers[22], 0)

    async def test_write_capability_uses_fc16_by_default_and_fc06_on_override(self) -> None:
        from custom_components.eybond_local.models import WriteCapability

        driver = ModbusCatalogDriver()
        transport = _transport()
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None
        inverter.capabilities = (
            WriteCapability(
                key="max_charge_current",
                register=34,
                value_kind="u16",
                note="",
                minimum=10,
                maximum=130,
            ),
            WriteCapability(
                key="output_source_priority",
                register=1,
                value_kind="enum",
                note="",
                enum_map={0: "Battery first", 1: "PV first", 2: "Utility first"},
                write_function=6,
            ),
        )

        result = await driver.async_write_capability(
            transport, inverter, "max_charge_current", 70
        )
        self.assertEqual(result, 70)
        self.assertEqual(transport._registers[34], 70)

        result = await driver.async_write_capability(
            transport, inverter, "output_source_priority", "PV first"
        )
        self.assertEqual(result, "PV first")
        self.assertEqual(transport._registers[1], 1)

    async def test_registry_exposes_driver_and_measurements(self) -> None:
        from custom_components.eybond_local.drivers.registry import (
            driver_options,
            iter_drivers,
        )

        self.assertIn("modbus_catalog", driver_options())
        drivers = iter_drivers("modbus_catalog")
        self.assertEqual(len(drivers), 1)
        keys = {m.key for m in drivers[0].measurements}
        self.assertIn("battery_percent", keys)
        self.assertIn("output_power", keys)


if __name__ == "__main__":
    unittest.main()
