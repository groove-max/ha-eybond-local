from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.drivers.registry import driver_options, get_driver
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget
from custom_components.eybond_local.payload.pi30 import crc16_xmodem


def _frame(payload: str) -> bytes:
    body = f"({payload}".encode("ascii")
    crc = crc16_xmodem(body)
    high = (crc >> 8) & 0xFF
    low = crc & 0xFF
    if high in {0x28, 0x0D, 0x0A}:
        high += 1
    if low in {0x28, 0x0D, 0x0A}:
        low += 1
    return body + bytes((high, low)) + b"\r"


class _FakeTransport:
    def __init__(self, responses: dict[tuple[int, int, str], str]) -> None:
        self._responses = responses
        self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
        self.connected = True
        self.commands: list[str] = []

    async def wait_until_connected(self, timeout: float) -> bool:
        return True

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        return True

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        command = payload[:-3].decode("ascii")
        self.commands.append(command)
        key = (devcode, collector_addr, command)
        if key not in self._responses:
            raise asyncio.TimeoutError()
        return _frame(self._responses[key])

    async def async_send_collector(self, *, fcode: int, payload: bytes = b"", devcode: int = 0, collector_addr: int = 1):
        raise NotImplementedError


class Pi30DriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_detects_pi30_inverter(self) -> None:
        driver = Pi30Driver()
        self.assertEqual(driver.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(driver.register_schema_name, "pi30_ascii/models/smartess_0925_compat.json")
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                (0x0994, 0x01, "QFLAG"): "EazDbjkuvxy",
                (0x0994, 0x01, "QVFW"): "00012.09",
            }
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.driver_key, "pi30")
        self.assertEqual(inverter.protocol_family, "pi30")
        self.assertEqual(inverter.serial_number, "553555355535552")
        self.assertEqual(inverter.model_name, "PI30 4200")
        self.assertEqual(inverter.variant_key, "default")
        self.assertEqual(inverter.profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(inverter.register_schema_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(inverter.details["battery_type"], "User")
        self.assertEqual(inverter.details["output_source_priority"], "SBU first")
        self.assertEqual(inverter.details["machine_type"], "Hybrid")
        self.assertTrue(inverter.details["buzzer_enabled"])
        self.assertEqual(inverter.details["main_cpu_firmware_version"], "00012.09")
        self.assertEqual(driver.profile_metadata.source_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(driver.register_schema_metadata.source_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(driver.measurements, driver.register_schema_metadata.measurement_descriptions)
        self.assertEqual(driver.binary_sensors, driver.register_schema_metadata.binary_sensor_descriptions)
        self.assertTrue(any(cap.key == "output_source_priority" for cap in inverter.capabilities))
        self.assertTrue(any(cap.key == "battery_bulk_voltage" for cap in inverter.capabilities))

    async def test_probe_scales_numeric_capabilities_for_24v_units(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        inverter = await driver.async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                }
            ),
            target,
        )

        assert inverter is not None
        capabilities = {cap.key: cap for cap in inverter.capabilities}
        self.assertEqual(capabilities["battery_bulk_voltage"].native_minimum, 24.0)
        self.assertEqual(capabilities["battery_bulk_voltage"].native_maximum, 29.2)
        self.assertEqual(capabilities["battery_under_voltage"].native_minimum, 20.0)
        self.assertEqual(capabilities["battery_under_voltage"].native_maximum, 24.0)

    async def test_probe_maps_vmii_model_number_to_display_name(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                (0x0994, 0x01, "QMN"): "VMII-NXPW5KW",
            }
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.model_name, "PowMr 4.2kW")
        self.assertEqual(inverter.details["model_number"], "VMII-NXPW5KW")
        self.assertEqual(inverter.profile_name, "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(inverter.register_schema_name, "pi30_ascii/models/vmii_nxpw5kw.json")

    async def test_probe_selects_vmii_model_overlay_when_model_number_matches(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 5000 5000 48.0 54.0 42.0 56.4 54.0 2 30 80 0 2 2 1 10 0 0 54.0 0 1",
                (0x0994, 0x01, "QMN"): "VMII-NXPW5KW",
            }
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.model_name, "PowMr 4.2kW")
        self.assertEqual(inverter.variant_key, "vmii_nxpw5kw")
        self.assertEqual(inverter.profile_name, "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(inverter.register_schema_name, "pi30_ascii/models/vmii_nxpw5kw.json")

    async def test_probe_collects_variant_detection_facts(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 5000 5000 48.0 54.0 42.0 56.4 54.0 2 30 80 0 2 2 1 10 0 0 54.0 0 1 120 1 80",
                (0x0994, 0x01, "QFLAG"): "EadzDbjkuvxy",
                (0x0994, 0x01, "QMOD"): "E",
                (0x0994, 0x01, "QPIWS"): "000000000000000000000000000000000000",
            }
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.details["qpiri_field_count"], 28)
        self.assertEqual(inverter.details["operating_mode_code"], "E")
        self.assertEqual(inverter.details["qpiws_bit_count"], 36)

    async def test_read_values_decodes_live_metrics(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPIGS"): "239.5 49.9 239.5 49.9 0927 0924 015 396 26.60 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 000",
                (0x0994, 0x01, "QMOD"): "L",
                (0x0994, 0x01, "QPIWS"): "00000100000000000000000000000000",
                (0x0994, 0x01, "Q1"): "00001 16971 01 00 00 026 033 022 029 02 00 000 0036 0000 0000 49.95 10 0 060 030 100 030 58.40 000 120 0 0000",
                (0x0994, 0x01, "QET"): "12345",
                (0x0994, 0x01, "QLT"): "2345",
                (0x0994, 0x01, "QT"): "20260407113059",
                (0x0994, 0x01, "QEY2026"): "456",
                (0x0994, 0x01, "QEM202604"): "78",
                (0x0994, 0x01, "QED20260407"): "9",
                (0x0994, 0x01, "QLY2026"): "54",
                (0x0994, 0x01, "QLM202604"): "7",
                (0x0994, 0x01, "QLD20260407"): "1",
            }
        )
        inverter = await Pi30Driver().async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                }
            ),
            target,
        )

        assert inverter is not None
        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["operating_mode"], "Line")
        self.assertEqual(values["output_active_power"], 924)
        self.assertEqual(values["battery_voltage"], 26.6)
        self.assertEqual(values["pv_input_power"], 695.0)
        self.assertTrue(values["alarm_active"])
        self.assertEqual(values["alarm_status"], "Line fail warning")
        self.assertEqual(values["tracker_temperature"], 26)
        self.assertEqual(values["inverter_charge_state"], "No charging")
        self.assertEqual(values["pv_generation_sum"], 12345)
        self.assertEqual(values["ac_in_generation_day"], 1)

    async def test_read_values_ignores_missing_optional_runtime_commands(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPIGS"): "239.5 49.9 239.5 49.9 0927 0924 015 396 26.60 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 000",
                (0x0994, 0x01, "QMOD"): "L",
                (0x0994, 0x01, "QET"): "12345",
            }
        )
        inverter = await Pi30Driver().async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                }
            ),
            target,
        )

        assert inverter is not None
        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["operating_mode"], "Line")
        self.assertNotIn("alarm_status", values)
        self.assertEqual(values["pv_generation_sum"], 12345)
        self.assertNotIn("tracker_temperature", values)

    async def test_read_values_groups_runtime_commands_by_poll_class(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        inverter = await Pi30Driver().async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                }
            ),
            target,
        )

        assert inverter is not None
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPIGS"): "239.5 49.9 239.5 49.9 0927 0924 015 396 26.60 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 000",
                (0x0994, 0x01, "QMOD"): "L",
                (0x0994, 0x01, "QPIWS"): "00000100000000000000000000000000",
                (0x0994, 0x01, "Q1"): "00001 16971 01 00 00 026 033 022 029 02 00 000 0036 0000 0000 49.95 10 0 060 030 100 030 58.40 000 120 0 0000",
                (0x0994, 0x01, "QET"): "12345",
                (0x0994, 0x01, "QLT"): "2345",
                (0x0994, 0x01, "QT"): "20260407113059",
                (0x0994, 0x01, "QEY2026"): "456",
                (0x0994, 0x01, "QEM202604"): "78",
                (0x0994, 0x01, "QED20260407"): "9",
                (0x0994, 0x01, "QLY2026"): "54",
                (0x0994, 0x01, "QLM202604"): "7",
                (0x0994, 0x01, "QLD20260407"): "1",
            }
        )
        runtime_state: dict[str, object] = {}

        first_values = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=100.0,
        )

        self.assertIn("alarm_status", first_values)
        self.assertIn("pv_generation_sum", first_values)
        self.assertEqual(
            transport.commands,
            [
                "QPIGS",
                "QMOD",
                "QPIWS",
                "Q1",
                "QET",
                "QLT",
                "QT",
                "QEY2026",
                "QEM202604",
                "QED20260407",
                "QLY2026",
                "QLM202604",
                "QLD20260407",
            ],
        )

        transport.commands.clear()
        second_values = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=110.0,
        )

        self.assertEqual(second_values["operating_mode"], "Line")
        self.assertNotIn("alarm_status", second_values)
        self.assertNotIn("pv_generation_sum", second_values)
        self.assertEqual(transport.commands, ["QPIGS", "QMOD"])

        transport.commands.clear()
        third_values = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=130.0,
        )

        self.assertIn("alarm_status", third_values)
        self.assertNotIn("pv_generation_sum", third_values)
        self.assertEqual(transport.commands, ["QPIGS", "QMOD", "QPIWS", "Q1"])

        transport.commands.clear()
        fourth_values = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=160.0,
        )

        self.assertIn("pv_generation_sum", fourth_values)
        self.assertEqual(
            transport.commands,
            [
                "QPIGS",
                "QMOD",
                "QPIWS",
                "Q1",
                "QET",
                "QLT",
                "QT",
                "QEY2026",
                "QEM202604",
                "QED20260407",
                "QLY2026",
                "QLM202604",
                "QLD20260407",
            ],
        )

    async def test_write_enum_capability_sends_pi30_command(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                (0x0994, 0x01, "POP00"): "ACK",
            }
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        written = await driver.async_write_capability(
            transport,
            inverter,
            "output_source_priority",
            "Utility first",
        )

        self.assertEqual(written, "Utility first")
        self.assertEqual(inverter.details["output_source_priority"], "Utility first")
        self.assertIn("POP00", transport.commands)

    async def test_write_bool_capability_sends_enable_disable_command(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                (0x0994, 0x01, "PEA"): "ACK",
                (0x0994, 0x01, "PDA"): "ACK",
            }
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        enabled = await driver.async_write_capability(transport, inverter, "buzzer_enabled", True)
        disabled = await driver.async_write_capability(transport, inverter, "buzzer_enabled", False)

        self.assertTrue(enabled)
        self.assertFalse(disabled)
        self.assertIn("PEA", transport.commands)
        self.assertIn("PDA", transport.commands)

    async def test_write_numeric_capability_formats_scaled_voltage_command(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                (0x0994, 0x01, "PBFT27.2"): "ACK",
            }
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        written = await driver.async_write_capability(
            transport,
            inverter,
            "battery_float_voltage",
            27.2,
        )

        self.assertEqual(written, 27.2)
        self.assertEqual(inverter.details["battery_float_voltage"], 27.2)
        self.assertIn("PBFT27.2", transport.commands)

    async def test_support_capture_includes_dynamic_energy_commands(self) -> None:
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        inverter = await driver.async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                }
            ),
            target,
        )

        assert inverter is not None
        evidence = await driver.async_capture_support_evidence(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
                    (0x0994, 0x01, "QPIGS"): "239.5 49.9 239.5 49.9 0927 0924 015 396 26.60 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 000",
                    (0x0994, 0x01, "QMOD"): "L",
                    (0x0994, 0x01, "Q1"): "00001 16971 01 00 00 026 033 022 029 02 00 000 0036 0000 0000 49.95 10 0 060 030 100 030 58.40 000 120 0 0000",
                    (0x0994, 0x01, "QET"): "12345",
                    (0x0994, 0x01, "QLT"): "2345",
                    (0x0994, 0x01, "QT"): "20260407113059",
                    (0x0994, 0x01, "QEY2026"): "456",
                    (0x0994, 0x01, "QEM202604"): "78",
                    (0x0994, 0x01, "QED20260407"): "9",
                    (0x0994, 0x01, "QLY2026"): "54",
                    (0x0994, 0x01, "QLM202604"): "7",
                    (0x0994, 0x01, "QLD20260407"): "1",
                }
            ),
            inverter,
        )

        self.assertEqual(evidence["responses"]["QET"], "12345")
        self.assertEqual(evidence["responses"]["QEY2026"], "456")
        self.assertEqual(evidence["responses"]["QLD20260407"], "1")

    def test_registry_exposes_pi30_driver(self) -> None:
        self.assertIn("pi30", driver_options())
        self.assertEqual(get_driver("pi30").name, "PI30 / ASCII")


if __name__ == "__main__":
    unittest.main()