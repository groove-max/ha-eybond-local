from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.eybond_g_ascii import (  # noqa: E402
    EybondGAsciiDriver,
    _async_collect_eybond_g_ascii_values,
    _looks_like_eybond_g_ascii,
)
from custom_components.eybond_local.fixtures.replay import (  # noqa: E402
    detect_fixture_payload,
    read_fixture_values,
)
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile  # noqa: E402
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    load_register_schema,
)
from custom_components.eybond_local.models import (  # noqa: E402
    CapabilityCondition,
    DetectedInverter,
    ProbeTarget,
    WriteCapability,
)
from custom_components.eybond_local.payload.ascii_line import AsciiLineError  # noqa: E402


class _FakeEybondGAsciiSession:
    def __init__(self, responses: dict[str, str]) -> None:
        self.responses = responses
        self.commands: list[str] = []

    async def request(self, command: str) -> str:
        self.commands.append(command)
        return self.responses[command]

    async def request_raw(self, command: str) -> bytes:
        self.commands.append(command)
        return self.responses[command].encode("ascii")


class _DefaultingFakeEybondGAsciiSession(_FakeEybondGAsciiSession):
    async def request(self, command: str) -> str:
        self.commands.append(command)
        return self.responses.get(command, "")

    async def request_raw(self, command: str) -> bytes:
        self.commands.append(command)
        return self.responses.get(command, "").encode("ascii")


class EybondGAsciiDriverTests(unittest.TestCase):
    def test_capability_visible_if_controls_runtime_visibility(self) -> None:
        capability = WriteCapability(
            key="bms_communication",
            register=-1,
            value_kind="bool",
            note="",
            visible_if=(
                CapabilityCondition(
                    key="g_ascii_bms_available",
                    operator="truthy",
                    effect="hide",
                    reason="BMS telemetry is not available on this device.",
                ),
            ),
        )

        unavailable = capability.runtime_state({"g_ascii_bms_available": False})
        self.assertFalse(unavailable.visible)
        self.assertFalse(unavailable.editable)
        self.assertIn("BMS telemetry is not available on this device.", unavailable.warnings)

        available = capability.runtime_state({"g_ascii_bms_available": True})
        self.assertTrue(available.visible)
        self.assertTrue(available.editable)

    def test_capability_editable_if_does_not_hide_capability(self) -> None:
        capability = WriteCapability(
            key="standby_only",
            register=-1,
            value_kind="bool",
            note="",
            editable_if=(
                CapabilityCondition(
                    key="operating_mode",
                    operator="eq",
                    value="Standby",
                    effect="disable",
                    reason="This setting can only be changed in standby.",
                ),
            ),
        )

        state = capability.runtime_state({"operating_mode": "Battery"})
        self.assertTrue(state.visible)
        self.assertFalse(state.editable)
        self.assertIn("This setting can only be changed in standby.", state.warnings)

    def test_entity_descriptions_are_loaded_from_command_schema_via_register_schema(self) -> None:
        command_schema = json.loads(
            (
                REPO_ROOT
                / "custom_components/eybond_local/protocol_catalogs/command_schemas/eybond_g_ascii/base.json"
            ).read_text(encoding="utf-8")
        )
        register_schema_raw = json.loads(
            (
                REPO_ROOT
                / "custom_components/eybond_local/protocol_catalogs/register_schemas/eybond_g_ascii/base.json"
            ).read_text(encoding="utf-8")
        )

        self.assertNotIn("measurement_descriptions", register_schema_raw)
        self.assertEqual(
            register_schema_raw["measurement_descriptions_from_command_schema"],
            "eybond_g_ascii/base.json",
        )
        schema = load_register_schema("eybond_g_ascii/base.json")
        self.assertEqual(
            [description.key for description in EybondGAsciiDriver.measurements],
            [item["key"] for item in command_schema["measurement_descriptions"]],
        )
        self.assertEqual(EybondGAsciiDriver.measurements, schema.measurement_descriptions)
        self.assertEqual(EybondGAsciiDriver.binary_sensors, schema.binary_sensor_descriptions)

    def test_sparse_temperature_descriptions_are_disabled_by_default(self) -> None:
        descriptions = {description.key: description for description in EybondGAsciiDriver.measurements}

        self.assertTrue(descriptions["eybond_g_ascii_operating_mode_code"].diagnostic)
        self.assertFalse(descriptions["eybond_g_ascii_operating_mode_code"].enabled_default)
        self.assertFalse(descriptions["operating_mode"].diagnostic)
        self.assertTrue(descriptions["operating_mode"].enabled_default)
        self.assertTrue(descriptions["dcdc_control_status"].diagnostic)
        self.assertFalse(descriptions["dcdc_control_status"].enabled_default)
        self.assertTrue(descriptions["inverter_temperature"].diagnostic)
        self.assertFalse(descriptions["inverter_temperature"].enabled_default)
        self.assertTrue(descriptions["low_voltage_mppt_temperature_2"].diagnostic)
        self.assertFalse(descriptions["low_voltage_mppt_temperature_2"].enabled_default)

    def test_family_match_requires_structural_g_command_payload(self) -> None:
        self.assertFalse(
            _looks_like_eybond_g_ascii(
                {"eybond_g_ascii_operating_mode_code": "B"}
            )
        )
        self.assertTrue(
            _looks_like_eybond_g_ascii(
                {"eybond_g_ascii_gdat0_fields": "0 5 4003 0 00 219.7 50.01"}
            )
        )

    def test_probe_signature_ignores_gmod_only_responses(self) -> None:
        class _Transport:
            async def async_send_payload(self, payload, *, route):
                if payload == b"GMOD\r":
                    return b"(B\r"
                raise TimeoutError

            def select_payload_route(self, route, *, payload_family=""):
                return route

        driver = EybondGAsciiDriver()

        self.assertFalse(
            asyncio.run(
                driver.async_probe_signature(
                    _Transport(),
                    ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
                )
            )
        )

    def test_probe_publishes_structural_field_counts_as_catalog_evidence(self) -> None:
        class _CollectorInfo:
            collector_pn = "E5000020000000"
            collector_cloud_family = "valuecloud_at"

        class _Transport:
            collector_info = _CollectorInfo()

            async def async_send_payload(self, payload, *, route):
                responses = {
                    b"GMOD\r": b"(B\r",
                    b"GPDAT0\r": (
                        b"(0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 "
                        b"50.00 04.11 26.4 015.8 025 0904 0904 100 172.8 "
                        b"015.8 0844 048.0\r"
                    ),
                    b"GPV\r": (
                        b"(183.1 027.6 00.43 05.31 00972 03 1 0 448.0 "
                        b"449.0 424.0 426.0 176.0 01 01039 01039 1193 "
                        b"3422 1823 0050 00589 00000 36363 0000000000000000\r"
                    ),
                }
                return responses[payload]

            def select_payload_route(self, route, *, payload_family=""):
                return route

        detected = asyncio.run(
            EybondGAsciiDriver().async_probe(
                _Transport(),
                ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
            )
        )

        self.assertIsNotNone(detected)
        self.assertEqual(detected.variant_key, "g_ascii_family")
        self.assertEqual(detected.profile_name, "eybond_g_ascii/base.json")
        self.assertEqual(detected.register_schema_name, "eybond_g_ascii/base.json")
        evidence = detected.details["catalog_detection"]["evidence"]
        self.assertEqual(evidence["protocol.protocol_id"], "EYBOND_G_ASCII")
        self.assertEqual(evidence["collector.cloud_family"], "valuecloud_at")
        self.assertEqual(evidence["shape.gdat0_field_count"], 22)
        self.assertEqual(evidence["shape.gpv_field_count"], 24)

    def test_probe_resolves_lvyuan_profile_from_offline_fingerprint(self) -> None:
        class _CollectorInfo:
            collector_pn = "E5000020000000"
            collector_cloud_family = "valuecloud_at"

        class _Transport:
            collector_info = _CollectorInfo()

            async def async_send_payload(self, payload, *, route):
                responses = {
                    b"GMOD\r": b"(B\r",
                    b"GPDAT0\r": (
                        b"(0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 "
                        b"50.00 04.11 26.4 015.8 025 0904 0904 100 172.8 "
                        b"015.8 0844 048.0\r"
                    ),
                    b"GPV\r": (
                        b"(183.1 027.6 00.43 05.31 00972 03 1 0 448.0 "
                        b"449.0 424.0 426.0 176.0 01 01039 01039 1193 "
                        b"3422 1823 0050 00589 00000 36363 0000000000000000\r"
                    ),
                    b"F\r": b"#220.0 016 024.0 50.0\r",
                    b"SVFW\r": b"(4.003 (20250217\r",
                }
                return responses[payload]

            def select_payload_route(self, route, *, payload_family=""):
                return route

        detected = asyncio.run(
            EybondGAsciiDriver().async_probe(
                _Transport(),
                ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
            )
        )

        self.assertIsNotNone(detected)
        self.assertEqual(detected.model_name, "LVYUAN TY-SIC-3.6KBE-W1")
        self.assertEqual(detected.variant_key, "lvyuan_ty_sic_3_6kbe_w1")
        self.assertEqual(
            detected.profile_name,
            "eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json",
        )
        self.assertEqual(detected.register_schema_name, "eybond_g_ascii/base.json")
        self.assertEqual(len(detected.capabilities), 34)
        self.assertEqual(
            detected.details["catalog_detection"]["resolution"],
            "exact",
        )
        self.assertNotIn("battery_current", detected.details)
        self.assertNotIn("output_apparent_power", detected.details)
        self.assertNotIn("output_active_power", detected.details)
        evidence = detected.details["catalog_detection"]["evidence"]
        self.assertEqual(evidence["rating.output_voltage"], 220.0)
        self.assertEqual(evidence["rating.output_current"], 16.0)
        self.assertEqual(evidence["rating.battery_voltage"], 24.0)
        self.assertEqual(evidence["rating.frequency"], 50.0)
        self.assertEqual(evidence["firmware.software_version"], "4.003")

    def test_decodes_eybond_g_ascii_runtime_mapping_without_short_gmod_leak(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00000 36363 0000000000000000"
                ),
                "GBAT": "027.5 000.00 02 21.6 23.2",
            }
        )

        values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=False))

        self.assertEqual(values["eybond_g_ascii_operating_mode_code"], "B")
        self.assertEqual(values["operating_mode"], "Battery")
        self.assertEqual(values["gdat0_communication_status_code"], "0")
        self.assertEqual(values["gdat0_operating_mode_code"], "5")
        self.assertNotIn("dcdc_control_status", values)
        self.assertEqual(values["inverter_voltage"], 219.7)
        self.assertEqual(values["inverter_frequency"], 50.01)
        self.assertEqual(values["grid_voltage"], 0.0)
        self.assertEqual(values["grid_frequency"], 0.0)
        self.assertEqual(values["mains_input_voltage"], 0.0)
        self.assertEqual(values["mains_frequency"], 0.0)
        self.assertEqual(values["output_voltage"], 216.4)
        self.assertEqual(values["output_frequency"], 50.0)
        self.assertEqual(values["output_current"], 4.11)
        self.assertEqual(values["output_load_percentage"], 25.0)
        self.assertEqual(values["output_active_power"], 904.0)
        self.assertEqual(values["output_apparent_power"], 904.0)
        self.assertEqual(values["battery_capacity"], 100.0)
        self.assertEqual(values["mainboard_temperature"], 48.0)
        self.assertEqual(values["pv_input_voltage"], 183.1)
        self.assertEqual(values["pv_charging_current"], 0.43)
        self.assertEqual(values["pv_current"], 5.31)
        self.assertEqual(values["pv_power"], 972.0)
        self.assertEqual(values["pv_energy_today"], 5.89)
        self.assertEqual(values["pv_energy_total"], 363.63)
        self.assertEqual(values["battery_voltage"], 27.5)
        self.assertEqual(values["battery_current"], 15.8)

    def test_gbat_zero_does_not_replace_live_gdat0_battery_current(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 014.7 025 0904 0904 100 172.8 014.7 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 14.71 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00000 36363 0000000000000000"
                ),
                "GBAT": "027.5 000.00 02 21.6 23.2",
            }
        )

        values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=False))

        self.assertEqual(values["battery_current"], 14.7)

    def test_runtime_poll_reads_full_g_ascii_runtime_set_every_cycle(self) -> None:
        responses = {
            "GMOD": "B",
            "GPDAT0": (
                "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
            ),
            "GPV": (
                "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                "00589 00000 36363 0000000000000000"
            ),
            "F": "#220.0 016 024.0 50.0",
            "SVFW": "4.003 (20250217.",
            "GTMP": "044.0 015.0 031.0 000.0 000.0",
            "GLINE": "219.7 50.01 264.0 185.0 255.0 194.0 60.00 40.00 00000 008 00006 00000 00046",
            "GBAT": "027.5 000.00 02 21.6 23.2",
            "GBUS": "408.5 380.0 380.0",
            "GCHG": "408.3 026.0 02 000.0 020.46 000.00 28.4 27.6 28.0 060.00 0480 060 120 720 0 4 3 0 000.00 000.00 000.00 000.0 2",
            "GOP": "220.5 50.01 001.38 01.23 00244 00244 00304 00202 00000 008 09896 00008 00046 00000 25483",
            "GINV": "219.9 49.94 000.9",
            "GWS": "00 0000000000000000 0000000000000000",
            "BL": "BL050",
            "FAN???": "050 00020 00020 0 1",
            "TCQN????": "0048",
            "DATE??????": "26 06 27",
            "TIME??????": "12 34 56",
            "GBMS": "00000 65535 65535 65535 65535 65535 65535 65535 00000 00000 65535 65535 65535 65535 00000 65535 65535",
        }
        runtime_state: dict[str, object] = {}
        session = _FakeEybondGAsciiSession(responses)

        first = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                runtime_state=runtime_state,
                poll_interval=10,
                now_monotonic=1000.0,
            )
        )

        expected_commands = [
            "GMOD",
            "GPDAT0",
            "GPV",
            "F",
            "SVFW",
            "GTMP",
            "GLINE",
            "GBAT",
            "GBUS",
            "GCHG",
            "GOP",
            "GINV",
            "GWS",
            "BL",
            "FAN???",
            "TCQN????",
            "DATE??????",
            "TIME??????",
            "GBMS",
        ]

        self.assertEqual(session.commands, expected_commands)
        self.assertEqual(
            first["eybond_g_ascii_runtime_polled_groups"],
            "core, fingerprint, secondary",
        )
        self.assertIn("F", session.commands)
        self.assertIn("GOP", session.commands)
        self.assertEqual(first["rated_output_voltage"], 220.0)
        self.assertEqual(first["output_energy_today"], 0.46)

        session.commands.clear()
        second = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                runtime_state=runtime_state,
                poll_interval=10,
                now_monotonic=1010.0,
            )
        )

        self.assertEqual(session.commands, expected_commands[:-1])
        self.assertEqual(
            second["eybond_g_ascii_runtime_polled_groups"],
            "core, fingerprint, secondary",
        )
        self.assertEqual(second["eybond_g_ascii_gbms_skipped_reason"], "recent_timeout")
        self.assertEqual(second["rated_output_voltage"], 220.0)
        self.assertEqual(second["output_energy_today"], 0.46)

    def test_gmod_operating_mode_codes_follow_protocol_document(self) -> None:
        expected = {
            "P": "Power On",
            "S": "Standby",
            "L": "Line",
            "B": "Battery",
            "F": "Fault",
            "D": "Shutdown",
            "X": "Test",
        }

        for code, mode in expected.items():
            with self.subTest(code=code):
                session = _FakeEybondGAsciiSession(
                    {
                        "GMOD": code,
                        "GPDAT0": (
                            "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                            "04.11 26.4 000.0 025 0904 0904 100 172.8 000.0 0844 048.0"
                        ),
                        "GPV": (
                            "183.1 027.6 00.00 05.31 00972 03 1 0 448.0 449.0 "
                            "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                            "00589 00000 36363 0000000000000000"
                        ),
                    }
                )

                values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=False))

                self.assertEqual(values["eybond_g_ascii_operating_mode_code"], code)
                self.assertEqual(values["operating_mode"], mode)

    def test_decodes_documented_read_only_commands(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00001 36363 0000000000000000."
                ),
                "GBAT": "027.5 000.00 02 21.6 23.2",
                "F": "#220.0 016 024.0 50.0",
                "SVFW": "4.003 (20250217.",
                "GTMP": "044.0 015.0 031.0 000.0 000.0",
                "GLINE": "219.7 50.01 264.0 185.0 255.0 194.0 60.00 40.00 00000 008 00006 00000 00046",
                "GBUS": "408.5 380.0 380.0",
                "GCHG": "408.3 026.0 02 000.0 020.46 000.00 28.4 27.6 28.0 060.00 0480 060 120 720 0 4 3 0 000.00 000.00 000.00 000.0 2",
                "GOP": "220.5 50.01 001.38 01.23 00244 00244 00304 00202 00000 008 09896 00008 00046 00000 25483",
                "GINV": "219.9 49.94 000.9",
                "GWS": "00 0000000000000000 0000000000000000",
                "BL": "BL050",
                "FAN???": "050 00020 00020 0 1",
                "TCQN????": "0048",
                "DATE??????": "26 06 27",
                "TIME??????": "12 34 56",
                "GBMS": "00000 65535 65535 65535 65535 65535 65535 65535 00000 00000 65535 65535 65535 65535 00000 65535 65535",
            }
        )

        values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=False))

        self.assertEqual(values["rated_output_voltage"], 220.0)
        self.assertEqual(values["rated_output_current"], 16.0)
        self.assertEqual(values["rated_battery_voltage"], 24.0)
        self.assertEqual(values["rated_frequency"], 50.0)
        self.assertEqual(values["eybond_g_ascii_software_version"], "4.003")
        self.assertEqual(values["eybond_g_ascii_software_date"], "2025-02-17")
        self.assertEqual(values["pv_side_temperature"], 44.0)
        self.assertEqual(values["charger_temperature"], 15.0)
        self.assertEqual(values["ambient_temperature"], 31.0)
        self.assertEqual(values["grid_voltage"], 219.7)
        self.assertEqual(values["grid_frequency"], 50.01)
        self.assertEqual(values["grid_loss_high_voltage"], 264.0)
        self.assertEqual(values["grid_loss_low_voltage"], 185.0)
        self.assertEqual(values["grid_restore_high_voltage"], 255.0)
        self.assertEqual(values["grid_restore_low_voltage"], 194.0)
        self.assertEqual(values["grid_loss_high_frequency"], 60.0)
        self.assertEqual(values["grid_loss_low_frequency"], 40.0)
        self.assertEqual(values["grid_energy_today"], 0.06)
        self.assertEqual(values["grid_energy_total"], 0.46)
        self.assertEqual(values["bus_voltage"], 408.3)
        self.assertEqual(values["charging_voltage"], 26.0)
        self.assertEqual(values["charging_current"], 0.0)
        self.assertEqual(values["output_voltage"], 220.5)
        self.assertEqual(values["output_frequency"], 50.01)
        self.assertEqual(values["output_current"], 1.38)
        self.assertEqual(values["output_low_current"], 1.23)
        self.assertEqual(values["output_active_power"], 244.0)
        self.assertEqual(values["output_apparent_power"], 304.0)
        self.assertEqual(values["output_low_current_power"], 202.0)
        self.assertEqual(values["output_half_wave_apparent_power"], 0.0)
        self.assertEqual(values["output_load_percentage"], 8.0)
        self.assertEqual(values["output_energy_today"], 0.46)
        self.assertEqual(values["output_energy_total"], 254.83)
        self.assertEqual(values["inverter_voltage"], 219.9)
        self.assertEqual(values["inverter_frequency"], 49.94)
        self.assertEqual(values["fault_code"], "00")
        self.assertEqual(values["warning_status_1"], "0000000000000000")
        self.assertEqual(values["warning_status_2"], "0000000000000000")
        self.assertEqual(values["battery_capacity"], 50.0)
        self.assertEqual(values["pv_energy_total"], 1018.99)
        self.assertEqual(values["inverter_current"], 0.9)
        self.assertEqual(values["low_voltage_mppt_temperature_1"], 0.0)
        self.assertEqual(values["low_voltage_mppt_temperature_2"], 0.0)
        self.assertEqual(values["bus_reference_start_voltage"], 380.0)
        self.assertEqual(values["bus_reference_voltage"], 380.0)
        self.assertEqual(values["constant_voltage_charging_voltage"], 28.4)
        self.assertEqual(values["float_charging_voltage"], 27.6)
        self.assertEqual(values["equalization_charging_voltage"], 28.0)
        self.assertEqual(values["max_charging_current"], 60.0)
        self.assertEqual(values["constant_voltage_charging_time"], 480.0)
        self.assertEqual(values["equalization_charging_time"], 60.0)
        self.assertEqual(values["equalization_timeout"], 120.0)
        self.assertEqual(values["equalization_interval"], 720.0)
        self.assertFalse(values["equalization_enabled"])
        self.assertEqual(values["battery_type_code"], "4")
        self.assertEqual(values["low_power_discharge_time"], 3.0)
        self.assertEqual(values["charging_mode_code"], "0")
        self.assertEqual(values["fan_speed_percentage"], 50.0)
        self.assertEqual(values["fan1_speed_detected"], 20.0)
        self.assertEqual(values["fan2_speed_detected"], 20.0)
        self.assertFalse(values["fan1_stopped"])
        self.assertTrue(values["fan2_stopped"])
        self.assertEqual(values["equalization_elapsed_hours"], 48.0)
        self.assertEqual(values["inverter_date"], "2026-06-27")
        self.assertEqual(values["inverter_time"], "12:34:56")
        self.assertNotIn("bms_voltage", values)
        self.assertNotIn("eybond_g_ascii_gbms_fields", values)

    def test_short_gbms_response_is_ignored_as_non_bms_payload(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00001 36363 0000000000000000."
                ),
                "GBMS": "17 03 13",
            }
        )

        values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=False))

        self.assertNotIn("bms_voltage", values)
        self.assertNotIn("bms_communication_status_code", values)
        self.assertNotIn("eybond_g_ascii_gbms_fields", values)
        self.assertFalse(values["g_ascii_bms_available"])

    def test_decodes_gbms_only_when_real_bms_values_are_present(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00001 36363 0000000000000000."
                ),
                "GBMS": (
                    "00001 00002 0521 01234 0265 00080 01050 02000 "
                    "00000 00001 00500 0532 00000 65535 00000 65535 65535"
                ),
            }
        )

        values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=False))

        self.assertEqual(values["bms_communication_status_code"], "00001")
        self.assertEqual(values["bms_status_code"], "00002")
        self.assertEqual(values["bms_voltage"], 52.1)
        self.assertEqual(values["bms_current"], 12.34)
        self.assertEqual(values["bms_temperature"], 26.5)
        self.assertEqual(values["bms_soc_raw"], 80.0)
        self.assertEqual(values["bms_remaining_capacity"], 105.0)
        self.assertEqual(values["bms_rated_capacity"], 200.0)
        self.assertEqual(values["bms_fault_code"], "00000")
        self.assertEqual(values["bms_warning_code"], "00001")
        self.assertEqual(values["bms_max_charging_current"], 5.0)
        self.assertEqual(values["bms_constant_voltage_point"], 53.2)
        self.assertTrue(values["g_ascii_bms_available"])
        self.assertIn("eybond_g_ascii_gbms_fields", values)

    def test_skips_gbms_when_bms_communication_readback_is_disabled(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 000.0 025 0904 0904 100 172.8 000.0 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 00.00 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00001 36363 0000000000000000."
                ),
                "TE?": "",
                "TD?": "S",
            }
        )
        bms_capability = WriteCapability(
            key="bms_communication",
            register=-1,
            value_kind="bool",
            note="",
            enum_map={0: "Disabled", 1: "Enabled"},
            read_key="g_ascii_setting_bms_communication",
        )

        values = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                capabilities=(bms_capability,),
            )
        )

        self.assertEqual(values["g_ascii_setting_bms_communication"], "Disabled")
        self.assertEqual(
            values["eybond_g_ascii_gbms_skipped_reason"],
            "bms_communication_disabled",
        )
        self.assertFalse(values["g_ascii_bms_available"])
        self.assertNotIn("GBMS", session.commands)

    def test_backs_off_gbms_after_timeout_to_avoid_repeated_cycle_stalls(self) -> None:
        class _TimeoutGbmsSession(_FakeEybondGAsciiSession):
            async def request(self, command: str) -> str:
                self.commands.append(command)
                if command == "GBMS":
                    raise AsciiLineError("request_timeout")
                return self.responses[command]

        session = _TimeoutGbmsSession(
            {
                "GMOD": "B",
                "GPDAT0": (
                    "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                    "04.11 26.4 000.0 025 0904 0904 100 172.8 000.0 0844 048.0"
                ),
                "GPV": (
                    "183.1 027.6 00.00 05.31 00972 03 1 0 448.0 449.0 "
                    "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                    "00589 00001 36363 0000000000000000."
                ),
            }
        )
        runtime_state: dict[str, object] = {}

        asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                runtime_state=runtime_state,
                now_monotonic=100.0,
            )
        )
        self.assertIn("GBMS", session.commands)

        session.commands.clear()
        values = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                runtime_state=runtime_state,
                now_monotonic=110.0,
            )
        )

        self.assertNotIn("GBMS", session.commands)
        self.assertEqual(values["eybond_g_ascii_gbms_skipped_reason"], "recent_timeout")
        self.assertFalse(values["g_ascii_bms_available"])
        self.assertGreater(values["eybond_g_ascii_gbms_retry_after_s"], 0)

    def test_replays_support_fixture_raw_ascii_frames(self) -> None:
        fixture = {
            "fixture_version": 1,
            "name": "eybond_g_ascii_support_capture",
            "collector": {
                "collector_pn": "A0000000000001",
                "collector_cloud_family": "valuecloud_at",
            },
            "probe_target": {
                "devcode": 0x0994,
                "collector_addr": 0xFF,
                "device_addr": 0,
            },
            "command_responses": {
                "GMOD": "(B\r",
                "GPDAT0": (
                    "(0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 "
                    "50.00 04.11 26.4 015.8 025 0904 0904 100 172.8 "
                    "015.8 0844 048.0\r"
                ),
                "GPV": (
                    "(183.1 027.6 00.43 05.31 00972 03 1 0 448.0 "
                    "449.0 424.0 426.0 176.0 01 01039 01039 1193 "
                    "3422 1823 0050 00589 00000 36363 0000000000000000\r"
                ),
                "GBAT": "(027.5 000.00 02 21.6 23.2\r",
                "F": "#220.0 016 024.0 50.0\r",
                "SVFW": "(4.003 (20250217\r",
                "GTMP": "(044.0 015.0 031.0 000.0 000.0\r",
                "GLINE": "(219.7 50.01 264.0 185.0 255.0 194.0 60.00 40.00 00000 008 00006 00000 00046\r",
                "GBUS": "(408.5 380.0 380.0\r",
                "GCHG": "(408.3 026.0 02 000.0 020.46 000.00 28.4 27.6 28.0 060.00 0480 060 120 720 0 4 3 0 000.00 000.00 000.00 000.0 2\r",
                "GOP": "(220.5 50.01 001.38 01.23 00244 00244 00304 00202 00000 008 09896 00008 00046 00000 25483\r",
                "GINV": "(219.9 49.94 000.9\r",
                "GWS": "(00 0000000000000000 0000000000000000\r",
                "BL": "BL050\r",
                "FAN???": "(050 00020 00020 0 1\r",
                "TCQN????": "0048\r",
                "DATE??????": "26 06 27\r",
                "TIME??????": "12 34 56\r",
                "GBMS": "(00000 65535 65535 65535 65535 65535 65535 65535 00000 00000 65535 65535 65535 65535 00000 65535 65535\r",
                "OPR??": "(00\r",
                "TBAT?": "(2\r",
                "V???": "(220\r",
                "HV?": "(0\r",
                "OPM??": "(01\r",
                "CPR??": "(01\r",
                "TE?": "(CDEHKOX\r",
                "TD?": "(ABFGIJLMNPQRTUVYZ\r",
                "CHGC???": "(020\r",
                "GCC???": "(030\r",
                "CST??": "(00\r",
                "TCCV????": "(28.4\r",
                "TCFV????": "(27.6\r",
                "TCQV????": "(28.0\r",
                "TCVT????": "(0480\r",
                "TCQT????": "(0060\r",
                "TCQO????": "(0120\r",
                "TCQI????": "(0720\r",
                "EOD????": "(21.6\r",
                "TBLV????": "(23.2\r",
                "LWDT????": "(0480\r",
                "BTG????": "(23.0\r",
                "BTB????": "(26.0\r",
                "BTO????": "(30.0\r",
                "OVP???": "(280\r",
                "LVP???": "(154\r",
                "CI1??": "(12\r",
                "BSOCU???": "(030\r",
                "BSOCG???": "(075\r",
                "BSOCB???": "(090\r",
            },
        }

        context = asyncio.run(detect_fixture_payload(fixture, driver_hint="eybond_g_ascii"))
        values = asyncio.run(read_fixture_values(context))

        self.assertEqual(context.inverter.serial_number, "A0000000000001")
        self.assertEqual(values["operating_mode"], "Battery")
        self.assertNotIn("dcdc_control_status", values)
        self.assertEqual(values["rated_output_voltage"], 220.0)
        self.assertEqual(values["output_active_power"], 244.0)
        self.assertEqual(values["battery_capacity"], 50.0)
        self.assertEqual(values["g_ascii_setting_max_charging_current"], 20)
        self.assertEqual(values["g_ascii_setting_constant_voltage_charging_voltage"], 28.4)
        self.assertEqual(values["g_ascii_setting_charging_method"], "Auto")
        self.assertEqual(values["fan_speed_percentage"], 50.0)
        self.assertTrue(values["fan2_stopped"])

    def test_support_capture_probes_extended_read_only_command_set(self) -> None:
        class _Transport:
            def __init__(self) -> None:
                self.commands: list[str] = []

            async def async_send_payload(self, payload, *, route):
                command = payload.decode("ascii").rstrip("\r")
                self.commands.append(command)
                responses = {
                    "GPDAT0": (
                        "(0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 "
                        "50.00 04.11 26.4 015.8 025 0904 0904 100 172.8 "
                        "015.8 0844 048.0\r"
                    ),
                    "GPV": "(183.1 027.6 00.43 05.31 00972\r",
                    "FAN???": "(050 00020 00020 0 1\r",
                    "TE?": "(0101\r",
                    "Q1": "NAK\r",
                }
                if command in responses:
                    return responses[command].encode("ascii")
                raise TimeoutError("synthetic_timeout")

            def select_payload_route(self, route, *, payload_family=""):
                return route

        target = ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0)
        inverter = DetectedInverter(
            driver_key="eybond_g_ascii",
            protocol_family="eybond_g_ascii",
            model_name="EyeBond G-ASCII inverter",
            serial_number="A0000000000001",
            probe_target=target,
            register_schema_name="eybond_g_ascii/base.json",
        )
        transport = _Transport()

        evidence = asyncio.run(
            EybondGAsciiDriver().async_capture_support_evidence(transport, inverter)
        )

        self.assertEqual(evidence["capture_kind"], "eybond_g_ascii_protocol_probe")
        self.assertEqual(evidence["protocol_id"], "EYBOND_G_ASCII")
        planned = [item["command"] for item in evidence["planned_commands"]]
        self.assertIn("GPDAT0", planned)
        self.assertIn("GPDAT9", planned)
        self.assertIn("GPID9", planned)
        self.assertIn("FAN???", planned)
        self.assertIn("TE?", planned)
        self.assertIn("DATE??????", planned)
        self.assertNotIn("TD1", planned)
        self.assertNotIn("DATE051203", planned)

        self.assertEqual(evidence["responses"]["GPDAT0"].split()[0], "(0")
        self.assertIn("GPDAT1", evidence["failures"])
        probe = evidence["protocol_probe"]
        self.assertEqual(probe["command_schema_key"], "eybond_g_ascii_base")
        self.assertEqual(probe["response_count"], 5)
        self.assertGreater(probe["failure_count"], 0)
        by_command = {item["command"]: item for item in probe["commands"]}
        self.assertEqual(by_command["GPDAT0"]["field_count"], 22)
        self.assertIsInstance(by_command["GPDAT0"]["duration_ms"], int)
        self.assertGreater(by_command["GPDAT0"]["known_field_count"], 0)
        self.assertIn("known_fields", by_command["GPDAT0"])
        self.assertEqual(by_command["GPDAT0"]["unknown_field_count"], 5)
        self.assertEqual(by_command["GPDAT0"]["status"], "ok")
        self.assertEqual(by_command["Q1"]["status"], "negative_response")
        self.assertEqual(by_command["Q1"]["response_kind"], "NAK")

    def test_learned_command_capability_writes_plain_ascii_command(self) -> None:
        session = _FakeEybondGAsciiSession({"PBL1": "ACK"})
        target = ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0)
        inverter = DetectedInverter(
            driver_key="eybond_g_ascii",
            protocol_family="eybond_g_ascii",
            model_name="EyeBond G-ASCII inverter",
            serial_number="A0000000000001",
            probe_target=target,
            register_schema_name="eybond_g_ascii/base.json",
            capabilities=(
                WriteCapability(
                    key="learned_lcd_backlight",
                    register=-1,
                    command="PBL",
                    value_kind="bool",
                    note="learned",
                    enum_map={0: "Off", 1: "On"},
                ),
            ),
        )
        driver = EybondGAsciiDriver()

        with patch.object(driver, "_session", return_value=session):
            written = asyncio.run(
                driver.async_write_capability(
                    object(),
                    inverter,
                    "learned_lcd_backlight",
                    "On",
                )
            )

        self.assertEqual(session.commands, ["PBL1"])
        self.assertIs(written, True)
        self.assertIs(inverter.details["learned_lcd_backlight"], True)

    def test_learned_command_capability_can_use_exact_command_map(self) -> None:
        session = _FakeEybondGAsciiSession({"OPR00": "ACK"})
        target = ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0)
        inverter = DetectedInverter(
            driver_key="eybond_g_ascii",
            protocol_family="eybond_g_ascii",
            model_name="EyeBond G-ASCII inverter",
            serial_number="A0000000000001",
            probe_target=target,
            register_schema_name="eybond_g_ascii/base.json",
            capabilities=(
                WriteCapability(
                    key="learned_output_priority",
                    register=-1,
                    command="OPR",
                    command_map={12336: "OPR00", 12337: "OPR01"},
                    value_kind="enum",
                    note="learned",
                    enum_map={
                        12336: "Mains output is preferred",
                        12337: "Photovoltaic output is preferred",
                    },
                ),
            ),
        )
        driver = EybondGAsciiDriver()

        with patch.object(driver, "_session", return_value=session):
            written = asyncio.run(
                driver.async_write_capability(
                    object(),
                    inverter,
                    "learned_output_priority",
                    "Mains output is preferred",
                )
            )

        self.assertEqual(session.commands, ["OPR00"])
        self.assertEqual(written, "Mains output is preferred")
        self.assertEqual(
            inverter.details["learned_output_priority"],
            "Mains output is preferred",
        )

    def test_document_backed_g_ascii_capabilities_write_fixed_width_commands(self) -> None:
        profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")
        capabilities = (
            profile.get_capability("max_charging_current_setting"),
            profile.get_capability("constant_voltage_charging_voltage_setting"),
        )
        session = _FakeEybondGAsciiSession(
            {
                "CHGC020": "ACK",
                "TCCV28.3": "ACK",
            }
        )
        inverter = DetectedInverter(
            driver_key="eybond_g_ascii",
            protocol_family="eybond_g_ascii",
            model_name="LVYUAN TY-SIC-3.6KBE-W1",
            serial_number="A0000000000001",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
            register_schema_name="eybond_g_ascii/base.json",
            capabilities=capabilities,
        )
        driver = EybondGAsciiDriver()

        with patch.object(driver, "_session", return_value=session):
            charging_current = asyncio.run(
                driver.async_write_capability(
                    object(),
                    inverter,
                    "max_charging_current_setting",
                    20,
                )
            )
            bulk_voltage = asyncio.run(
                driver.async_write_capability(
                    object(),
                    inverter,
                    "constant_voltage_charging_voltage_setting",
                    28.3,
                )
            )

        self.assertEqual(session.commands, ["CHGC020", "TCCV28.3"])
        self.assertEqual(charging_current, 20)
        self.assertEqual(bulk_voltage, 28.3)

    def test_document_backed_g_ascii_capabilities_are_full_control_only(self) -> None:
        # The base (family) profile keeps document-backed JSDSOLAR controls untested
        # and doc_backed -- Full-control-only until verified on a specific model.
        base_profile = load_driver_profile("eybond_g_ascii/base.json")
        base_capability = base_profile.get_capability("max_charging_current_setting")

        self.assertFalse(base_capability.tested)
        self.assertEqual(base_capability.provenance, "doc_backed")
        self.assertTrue(base_capability.enabled_default)
        self.assertTrue(base_capability.advanced)

    def test_lvyuan_model_promotes_document_backed_capabilities_to_verified(self) -> None:
        # July 2026 user-hardware verification promoted the LVYUAN model's 22
        # document-backed controls to tested/verified (default-visible), while the
        # inherited command definitions and advanced flag stay intact.
        profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")

        for key in ("max_charging_current_setting", "bms_low_soc_shutdown_setting"):
            capability = profile.get_capability(key)
            self.assertTrue(capability.tested, key)
            self.assertEqual(capability.provenance, "verified", key)
            self.assertTrue(capability.enabled_default, key)
            self.assertTrue(capability.advanced, key)
        self.assertFalse(
            [capability.key for capability in profile.capabilities if not capability.tested],
            "every LVYUAN model control is expected to be verified/tested",
        )

    def test_document_backed_g_ascii_capabilities_live_in_base_profile(self) -> None:
        base_raw = json.loads(
            (
                REPO_ROOT
                / "custom_components/eybond_local/protocol_catalogs/profiles/eybond_g_ascii/base.json"
            ).read_text(encoding="utf-8")
        )
        model_raw = json.loads(
            (
                REPO_ROOT
                / "custom_components/eybond_local/protocol_catalogs/profiles/eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json"
            ).read_text(encoding="utf-8")
        )

        base_keys = {item["key"] for item in base_raw["capabilities"]}
        model_entries = {item["key"]: item for item in model_raw["capabilities"]}

        self.assertIn("max_charging_current_setting", base_keys)
        self.assertIn("bms_low_soc_shutdown_setting", base_keys)
        # The LVYUAN model promotes the document-backed controls to verified, but
        # only as metadata-only overrides: the command/value definitions must keep
        # living in the base profile and never be duplicated in the model file.
        for key in ("max_charging_current_setting", "bms_low_soc_shutdown_setting"):
            self.assertIn(key, model_entries)
            override = model_entries[key]
            self.assertLessEqual(
                set(override),
                {"key", "tested", "provenance", "support_notes"},
                key,
            )
            self.assertTrue(override["tested"], key)
            self.assertEqual(override["provenance"], "verified", key)
        self.assertIn("output_priority", model_entries)
        self.assertIn("remote_inverter_switch", model_entries)

    def test_bms_soc_controls_are_hidden_without_live_bms(self) -> None:
        profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")
        capability = profile.get_capability("bms_low_soc_shutdown_setting")

        self.assertFalse(
            capability.runtime_state({"g_ascii_bms_available": False}).visible
        )
        self.assertTrue(
            capability.runtime_state({"g_ascii_bms_available": True}).visible
        )

    def test_bms_soc_readbacks_are_deferred_until_gbms_has_live_values(self) -> None:
        profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")
        common_responses = {
            "GMOD": "B",
            "GPDAT0": (
                "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
            ),
            "GPV": (
                "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                "00589 00001 36363 0000000000000000."
            ),
        }

        no_bms_session = _DefaultingFakeEybondGAsciiSession(
            {
                **common_responses,
                "GBMS": (
                    "00000 65535 65535 65535 65535 65535 65535 65535 "
                    "00000 00000 65535 65535 65535 65535 00000 65535 65535"
                ),
            }
        )
        no_bms_values = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                no_bms_session,
                probe=False,
                capabilities=profile.capabilities,
            )
        )
        self.assertFalse(no_bms_values["g_ascii_bms_available"])
        self.assertNotIn("BSOCU???", no_bms_session.commands)
        self.assertNotIn("BSOCG???", no_bms_session.commands)
        self.assertNotIn("BSOCB???", no_bms_session.commands)

        live_bms_session = _DefaultingFakeEybondGAsciiSession(
            {
                **common_responses,
                "GBMS": (
                    "00001 00002 0521 01234 0265 00080 01050 02000 "
                    "00000 00001 00500 0532 00000 65535 00000 65535 65535"
                ),
                "BSOCU???": "030",
                "BSOCG???": "075",
                "BSOCB???": "090",
            }
        )
        live_bms_values = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                live_bms_session,
                probe=False,
                capabilities=profile.capabilities,
            )
        )
        self.assertTrue(live_bms_values["g_ascii_bms_available"])
        self.assertIn("BSOCU???", live_bms_session.commands)
        self.assertEqual(live_bms_values["g_ascii_setting_bms_low_soc_shutdown"], 30)
        self.assertEqual(live_bms_values["g_ascii_setting_bms_low_soc_switch_to_grid"], 75)
        self.assertEqual(live_bms_values["g_ascii_setting_bms_high_soc_switch_to_battery"], 90)

    def test_capability_readback_is_suppressed_after_three_live_timeouts(self) -> None:
        class _TimeoutChargeCurrentReadbackSession(_DefaultingFakeEybondGAsciiSession):
            async def request(self, command: str) -> str:
                self.commands.append(command)
                if command == "CHGC???":
                    raise AsciiLineError("request_timeout")
                return self.responses.get(command, "")

        profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")
        common_responses = {
            "GMOD": "B",
            "GPDAT0": (
                "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
                "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
            ),
            "GPV": (
                "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 "
                "424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 "
                "00589 00001 36363 0000000000000000."
            ),
        }
        runtime_state: dict[str, object] = {}
        values: dict[str, object] = {}

        for index in range(3):
            session = _TimeoutChargeCurrentReadbackSession(common_responses)
            values = asyncio.run(
                _async_collect_eybond_g_ascii_values(
                    session,
                    probe=False,
                    capabilities=profile.capabilities,
                    runtime_state=runtime_state,
                    now_monotonic=1000.0 + index,
                )
            )
            self.assertIn("CHGC???", session.commands)

        capability = profile.get_capability("max_charging_current_setting")
        state = capability.runtime_state(values)
        self.assertFalse(state.visible)
        self.assertIn("CHGC???", values["eybond_g_ascii_suppressed_readback_commands"])

        session = _TimeoutChargeCurrentReadbackSession(common_responses)
        values = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                capabilities=profile.capabilities,
                runtime_state=runtime_state,
                now_monotonic=1004.0,
            )
        )
        self.assertNotIn("CHGC???", session.commands)
        self.assertFalse(capability.runtime_state(values).visible)

    def test_reads_learned_g_ascii_setting_readbacks(self) -> None:
        session = _FakeEybondGAsciiSession(
            {
                "GMOD": "B",
                "GPDAT0": "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0",
                "GPV": "183.1 027.6 00.43 05.31 00972 03 1 0 448.0 449.0 424.0 426.0 176.0 01 01039 01039 1193 3422 1823 0050 00589 00000 36363 0",
                "F": "#220.0 016 024.0 50.0",
                "SVFW": "4.003 (20250217",
                "GTMP": "",
                "GLINE": "",
                "GBAT": "",
                "GBUS": "",
                "GCHG": "",
                "GOP": "",
                "GINV": "",
                "GWS": "",
                "BL": "",
                "FAN???": "",
                "TCQN????": "",
                "DATE??????": "",
                "TIME??????": "",
                "GBMS": "",
                "OPR??": "01",
                "TBAT?": "2",
                "V???": "220 120",
                "HV?": "1",
                "TE?": "IO",
                "TD?": "SY",
            }
        )
        capabilities = (
            WriteCapability(
                key="learned_output_priority",
                register=-1,
                command="OPR",
                command_map={12336: "OPR00", 12337: "OPR01"},
                value_kind="enum",
                note="learned",
                enum_map={12336: "Utility first", 12337: "PV first"},
                read_key="g_ascii_setting_output_priority",
            ),
            WriteCapability(
                key="learned_battery_type",
                register=-1,
                command="TBAT",
                command_map={48: "TBAT0", 50: "TBAT2"},
                value_kind="enum",
                note="learned",
                enum_map={48: "Lead-acid", 50: "Lithium"},
                read_key="g_ascii_setting_battery_type",
            ),
            WriteCapability(
                key="learned_output_voltage",
                register=-1,
                command="V",
                value_kind="enum",
                note="learned",
                enum_map={120: "120 V display", 220: "220 V"},
                read_key="g_ascii_setting_output_voltage",
            ),
            WriteCapability(
                key="learned_energy_saving",
                register=-1,
                command_map={68: "TDI", 69: "TEI"},
                value_kind="enum",
                note="learned",
                enum_map={68: "Disabled", 69: "Enabled"},
                read_key="g_ascii_setting_energy_saving_mode",
            ),
            WriteCapability(
                key="learned_bms_communication",
                register=-1,
                command_map={68: "TDS", 69: "TES"},
                value_kind="enum",
                note="learned",
                enum_map={68: "Disabled", 69: "Enabled"},
                read_key="g_ascii_setting_bms_communication",
            ),
        )

        values = asyncio.run(
            _async_collect_eybond_g_ascii_values(
                session,
                probe=False,
                capabilities=capabilities,
            )
        )

        self.assertEqual(values["g_ascii_setting_output_priority"], "PV first")
        self.assertEqual(values["g_ascii_setting_battery_type"], "Lithium")
        self.assertEqual(values["g_ascii_setting_output_voltage"], "120 V display")
        self.assertEqual(values["g_ascii_setting_energy_saving_mode"], "Enabled")
        self.assertEqual(values["g_ascii_setting_bms_communication"], "Disabled")

    def test_command_schema_support_probe_plan_is_read_only(self) -> None:
        schema_path = (
            REPO_ROOT
            / "custom_components/eybond_local/protocol_catalogs/command_schemas/eybond_g_ascii/base.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        commands = schema["commands"]
        self.assertTrue(commands)
        self.assertEqual({item["access"] for item in commands}, {"read"})
        self.assertNotIn("TD1", {item["command"] for item in commands})
        self.assertNotIn("DATE051203", {item["command"] for item in commands})

    def test_raw_energy_counters_do_not_claim_strictly_increasing_statistics(self) -> None:
        schema = load_register_schema("eybond_g_ascii/base.json")

        for key in (
            "pv_energy_today",
            "pv_energy_total",
            "grid_energy_today",
            "grid_energy_total",
            "output_energy_today",
            "output_energy_total",
        ):
            with self.subTest(key=key):
                self.assertEqual(schema.measurement_description(key).state_class, "total")


if __name__ == "__main__":
    unittest.main()
