from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver  # noqa: E402
from custom_components.eybond_local.drivers.read_result import (  # noqa: E402
    DriverReadMode,
    DriverReadResult,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402


def _full_values(result: DriverReadResult) -> dict[str, object]:
    if type(result) is not DriverReadResult or result.mode is not DriverReadMode.FULL:
        raise AssertionError("catalog runtime read must be an exact FULL result")
    return result.values


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


def _deye_3ph_high_holding_registers() -> dict[int, int]:
    """Return a sanitized replay of the issue #15 FC03 capture."""

    registers: dict[int, int] = {}
    for start, count in (
        (0, 3),
        (11, 11),
        (60, 1),
        (98, 14),
        (113, 15),
        (128, 15),
        (143, 15),
        (158, 15),
        (173, 12),
        (189, 5),
        (209, 1),
        (235, 4),
        (340, 8),
        (500, 1),
        (514, 15),
        (529, 7),
        (541, 1),
        (553, 6),
        (586, 6),
        (598, 15),
        (616, 10),
        (644, 12),
        (661, 7),
        (671, 13),
    ):
        for offset in range(count):
            registers[start + offset] = 0
    registers.update(
        {
            0: 5,          # observed compact three-phase device type
            2: 260,        # protocol version 0x0104
            20: 14464,     # low-first u32: 0x0001_3880 = 80000 W
            21: 1,
            115: 10,
            116: 30,
            117: 20,
            129: 0,
            130: 1,
            142: 2,
            145: 0,
            500: 2,        # Normal
            514: 189,      # battery charge today 18.9 kWh
            515: 97,       # battery discharge today 9.7 kWh
            516: 24745,
            518: 23156,
            520: 9,
            521: 74,
            522: 21529,
            524: 4544,
            525: 1,
            526: 173,
            527: 62709,
            529: 284,      # PV today 28.4 kWh
            534: 19750,
            535: 1,
            541: 1495,     # offset-1000 temperature: 49.5 C
            586: 307,      # battery temperature 30.7 C (no offset)
            587: 5307,     # 53.07 V
            588: 99,
            590: 97,
            591: 184,
            598: 2348,
            599: 2369,
            600: 2384,
            609: 5000,
            610: 131,
            611: 116,
            612: 124,
            616: 64887,    # signed -649 W
            617: 64872,    # signed -664 W
            618: 64955,    # signed -581 W
            619: 63787,    # signed -1749 W
            622: 64833,    # signed -703 W
            623: 64865,    # signed -671 W
            624: 64861,    # signed -675 W
            625: 63487,    # signed -2049 W
            644: 2350,
            645: 2379,
            646: 2384,
            650: 134,
            651: 410,
            652: 0,
            653: 83,
            655: 5000,
            661: 2343,
            662: 2373,
            663: 2369,
            664: 0,
            665: 0,
            666: 1801,
            667: 1801,
            672: 0,
            673: 0,
            674: 0,
            675: 0,
            676: 3259,
            677: 0,
            678: 3230,
            679: 0,
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

        values = _full_values(await driver.async_read_values(transport, inverter))

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

        values = _full_values(await driver.async_read_values(transport, inverter))

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

        values = _full_values(await driver.async_read_values(transport, inverter))

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

        values = _full_values(await driver.async_read_values(transport, inverter))

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

    async def test_probe_matches_kevolt_deye_3ph_high_exact_fingerprint(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers={},
            holding_registers=_deye_3ph_high_holding_registers(),
        )

        inverter = await driver.async_probe(transport, _target())

        assert inverter is not None
        self.assertEqual(
            inverter.model_name,
            "Deye-Compatible Three-Phase Hybrid 80 kW (Modbus)",
        )
        self.assertEqual(inverter.variant_key, "deye_3ph_high_80kw")
        self.assertEqual(
            inverter.register_schema_name,
            "deye_3ph_high_80kw/base.json",
        )
        self.assertEqual(
            inverter.profile_name,
            "modbus_catalog/deye_3ph_high_80kw.json",
        )
        self.assertEqual(len(inverter.capabilities), 118)
        evidence = inverter.details["identity_evidence"]
        self.assertEqual(evidence["deye_device_type_raw"], 5)
        self.assertEqual(evidence["deye_3ph_protocol_version_raw"], 260)
        self.assertEqual(evidence["deye_3ph_rated_power_low_raw"], 14464)
        self.assertEqual(evidence["deye_3ph_rated_power_high_raw"], 1)

    async def test_kevolt_deye_3ph_read_values_match_captured_scaling(self) -> None:
        from custom_components.eybond_local.canonical_telemetry import (
            apply_canonical_measurements,
        )

        driver = ModbusCatalogDriver()
        transport = _transport(
            input_registers={},
            holding_registers=_deye_3ph_high_holding_registers(),
        )
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["rated_power"], 80000)
        self.assertEqual(values["run_state"], "Normal")
        self.assertEqual(values["battery_shutdown_soc"], 10)
        self.assertEqual(values["battery_restart_soc"], 30)
        self.assertEqual(values["battery_low_soc"], 20)
        self.assertEqual(values["generator_charge_enable"], 0)
        self.assertEqual(values["grid_charge_enable"], 1)
        self.assertEqual(values["grid_export_mode"], "Zero Export to CT")
        self.assertEqual(values["solar_sell_enable"], 0)
        self.assertEqual(values["battery_charge_day"], 18.9)
        self.assertEqual(values["battery_discharge_day"], 9.7)
        self.assertEqual(values["grid_export_sum"], 7008.0)
        self.assertEqual(values["pv_generation_day"], 28.4)
        self.assertEqual(values["pv_generation_sum"], 8528.6)
        self.assertEqual(values["heat_sink_temperature"], 49.5)
        self.assertEqual(values["battery_temperature"], 30.7)
        self.assertEqual(values["battery_voltage"], 53.07)
        self.assertEqual(values["battery_percent"], 99)
        # Sign meaning is not promoted to canonical battery telemetry yet.
        self.assertEqual(values["battery_output_power"], 97)
        self.assertEqual(values["battery_output_current"], 1.84)
        self.assertEqual(values["grid_voltage_l1"], 234.8)
        self.assertEqual(values["grid_voltage_l2"], 236.9)
        self.assertEqual(values["grid_voltage_l3"], 238.4)
        self.assertEqual(values["grid_frequency"], 50.0)
        self.assertEqual(values["grid_power"], -2049)

        apply_canonical_measurements(
            "modbus_catalog", values, variant_key="deye_3ph_high_80kw"
        )
        self.assertEqual(values["output_power"], 544.0)
        self.assertEqual(values["pv_power"], 0.0)
        self.assertNotIn("battery_power", values)

    async def test_kevolt_controls_are_full_mode_only_and_use_fc16_with_readback_keys(self) -> None:
        from custom_components.eybond_local.control_policy import can_expose_capability
        from custom_components.eybond_local.const import (
            CONTROL_MODE_AUTO,
            CONTROL_MODE_FULL,
        )

        registers = _deye_3ph_high_holding_registers()
        transport = _transport(input_registers={}, holding_registers=registers)
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        by_key = {capability.key: capability for capability in inverter.capabilities}
        self.assertEqual(len(by_key), 118)
        self.assertTrue(
            {
                "battery_shutdown_soc",
                "battery_restart_soc",
                "battery_low_soc",
                "generator_charge_enable",
                "grid_charge_enable",
                "grid_export_mode",
                "solar_sell_enable",
                "time_of_use_period_1_time",
                "time_of_use_period_6_generator_charge",
                "maximum_solar_sell_power",
                "external_ct_ratio",
            }.issubset(by_key),
        )
        for capability in by_key.values():
            self.assertFalse(capability.tested, capability.key)
            self.assertFalse(capability.enabled_default, capability.key)
            self.assertEqual(capability.provenance, "doc_backed", capability.key)
            self.assertEqual(capability.write_function, 16, capability.key)
            self.assertFalse(
                can_expose_capability(
                    capability,
                    control_mode=CONTROL_MODE_AUTO,
                    detection_confidence="high",
                ),
                capability.key,
            )
            self.assertTrue(
                can_expose_capability(
                    capability,
                    control_mode=CONTROL_MODE_FULL,
                    detection_confidence="high",
                ),
                capability.key,
            )

        result = await driver.async_write_capability(
            transport,
            inverter,
            "grid_charge_enable",
            False,
        )
        self.assertFalse(result)
        self.assertEqual(transport._registers[130], 0)

        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertEqual(values["grid_charge_enable"], 0)

    async def test_kevolt_shared_fields_scaled_power_and_tou_time_use_one_typed_write_path(self) -> None:
        registers = _deye_3ph_high_holding_registers()
        registers[146] = 0xAA00
        transport = _transport(input_registers={}, holding_registers=registers)
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        enabled = await driver.async_write_capability(
            transport,
            inverter,
            "time_of_use_enable",
            True,
        )
        self.assertIs(enabled, True)
        self.assertEqual(transport._registers[146], 0xAA01)

        power = await driver.async_write_capability(
            transport,
            inverter,
            "maximum_sell_power",
            50000,
        )
        self.assertEqual(power, 50000)
        self.assertEqual(transport._registers[143], 5000)

        period_time = await driver.async_write_capability(
            transport,
            inverter,
            "time_of_use_period_1_time",
            "08:30",
        )
        self.assertEqual(period_time, "08:30")
        self.assertEqual(transport._registers[148], 830)

    async def test_kevolt_runtime_rotates_one_control_block_per_poll(self) -> None:
        class RecordingTransport(FixtureTransport):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.control_reads: list[tuple[int, int]] = []

            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                if address < 500 and address not in {0, 11}:
                    self.control_reads.append((address, count))
                return super()._handle_read_holding(payload)

        transport = RecordingTransport(
            registers=_deye_3ph_high_holding_registers(),
            input_registers={},
            command_responses=None,
            probe_target=_target(),
        )
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None
        transport.control_reads.clear()
        runtime_state: dict[str, object] = {}

        first = _full_values(
            await driver.async_read_values(
                transport,
                inverter,
                runtime_state=runtime_state,
            )
        )
        self.assertEqual(transport.control_reads, [(60, 1)])
        self.assertEqual(first["inverter_power"], "On")

        transport.control_reads.clear()
        second = _full_values(
            await driver.async_read_values(
                transport,
                inverter,
                runtime_state=runtime_state,
            )
        )
        self.assertEqual(transport.control_reads, [(98, 14)])
        self.assertIn("battery_control_mode", second)
        self.assertEqual(second["inverter_power"], "On")
        self.assertNotIn("_modbus_catalog_control_cache", inverter.details)
        self.assertNotIn("modbus_catalog_controls", inverter.details)

        written = await driver.async_write_capability(
            transport,
            inverter,
            "maximum_sell_power",
            50000,
            runtime_state=runtime_state,
        )
        self.assertEqual(written, 50000)
        self.assertEqual(
            runtime_state["modbus_catalog_controls"]["values"][
                "maximum_sell_power"
            ],
            50000,
        )
        self.assertNotIn("maximum_sell_power", inverter.details)

    async def test_kevolt_profile_exposes_only_documented_user_operational_registers(self) -> None:
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(
            _transport(
                input_registers={},
                holding_registers=_deye_3ph_high_holding_registers(),
            ),
            _target(),
        )
        assert inverter is not None
        capability_registers = {item.register for item in inverter.capabilities}

        # Explicitly excluded service/factory surfaces: reset and EEPROM
        # initialization, factory test/calibration, BMS-owned live words,
        # parallel addressing, and grid-code protection curves.
        forbidden_registers = {
            81,
            91,
            92,
            93,
            94,
            195,
            210,
            214,
            223,
            239,
            240,
            269,
            336,
            341,
            350,
            365,
            395,
        }
        self.assertTrue(capability_registers.isdisjoint(forbidden_registers))
        self.assertEqual(
            {group.key for group in inverter.capability_groups},
            {
                "system",
                "battery",
                "charging",
                "generator",
                "smart_load",
                "energy",
                "time_of_use",
                "grid",
                "meter",
                "advanced",
            },
        )

    async def test_kevolt_deye_3ph_fingerprint_rejects_near_collisions(self) -> None:
        mutations = (
            (0, 1280),       # documented generic 0x0500 is not this capture
            (2, 261),        # different protocol revision
            (20, 14465),     # different low rated-power word
            (21, 0),         # different high rated-power word
        )
        driver = ModbusCatalogDriver()
        for register, value in mutations:
            with self.subTest(register=register, value=value):
                registers = _deye_3ph_high_holding_registers()
                registers[register] = value
                inverter = await driver.async_probe(
                    _transport(input_registers={}, holding_registers=registers),
                    _target(),
                )
                self.assertIsNone(inverter)

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
