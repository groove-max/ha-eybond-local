from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.pi18 import Pi18Driver
from custom_components.eybond_local.canonical_telemetry import canonical_measurements_for_driver
from custom_components.eybond_local.entity_descriptions import BASE_SENSOR_DESCRIPTIONS
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget
from custom_components.eybond_local.payload.pi30 import crc16_xmodem


def _frame(payload: str) -> bytes:
    body = f"^D{len(payload) + 3:03d}{payload}".encode("ascii")
    crc = crc16_xmodem(body)
    return body + bytes(((crc >> 8) & 0xFF, crc & 0xFF)) + b"\r"


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

    async def async_send_forward(self, payload: bytes, *, devcode: int, collector_addr: int) -> bytes:
        command = payload[:-3].decode("ascii")
        self.commands.append(command)
        key = (devcode, collector_addr, command)
        if key not in self._responses:
            raise asyncio.TimeoutError()
        return _frame(self._responses[key])


class Pi18DriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_detects_pi18_inverter(self) -> None:
        driver = Pi18Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        transport = _FakeTransport(
            {
                (0x0994, 0x01, "^P005PI"): "18",
                (0x0994, 0x01, "^P005ID"): "1401234567890123456789",
                (0x0994, 0x01, "^P007PIRI"): "2200,190,2200,500,190,5000,5000,480,540,500,420,564,540,2,30,80,0,1,2,1,0,1,0,1,2",
                (0x0994, 0x01, "^P006VFW"): "00001,00002,00003",
            }
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.driver_key, "pi18")
        self.assertEqual(inverter.protocol_family, "pi18")
        self.assertEqual(inverter.model_name, "PI18 5000")
        self.assertEqual(inverter.serial_number, "01234567890123")
        self.assertEqual(inverter.register_schema_name, "pi18_ascii/base.json")
        self.assertEqual(inverter.details["battery_type"], "User")

    async def test_read_values_decodes_runtime_fields(self) -> None:
        driver = Pi18Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        inverter = await driver.async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "^P005PI"): "18",
                    (0x0994, 0x01, "^P005ID"): "1401234567890123456789",
                    (0x0994, 0x01, "^P007PIRI"): "2200,190,2200,500,190,5000,5000,480,540,500,420,564,540,2,30,80,0,1,2,1,0,1,0,1,2",
                }
            ),
            target,
        )

        assert inverter is not None
        values = await driver.async_read_values(
            _FakeTransport(
                {
                    (0x0994, 0x01, "^P005GS"): "2301,500,2300,500,4500,4200,80,520,515,514,20,30,90,35,33,34,1200,800,3200,3100,1,2,1,1,2,2,1,0",
                    (0x0994, 0x01, "^P006MOD"): "05",
                    (0x0994, 0x01, "^P007FLAG"): "1,1,0,0,0,1,1,1,0",
                    (0x0994, 0x01, "^P005FWS"): "00,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                    (0x0994, 0x01, "^P005ET"): "00001234",
                    (0x0994, 0x01, "^P004T"): "20160214201314",
                    (0x0994, 0x01, "^P009EY2016"): "00000123",
                    (0x0994, 0x01, "^P011EM201602"): "00000045",
                    (0x0994, 0x01, "^P013ED20160214"): "00000067",
                }
            ),
            inverter,
        )

        self.assertEqual(values["operating_mode"], "Hybrid")
        self.assertEqual(values["output_active_power"], 4200)
        self.assertEqual(values["pv_input_power"], 2000)
        self.assertTrue(values["buzzer_enabled"])
        self.assertTrue(values["line_fail_warning"])
        self.assertEqual(values["pv_generation_sum"], 1234)

    async def test_capture_support_evidence_collects_raw_responses(self) -> None:
        driver = Pi18Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        inverter = await driver.async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "^P005PI"): "18",
                    (0x0994, 0x01, "^P005ID"): "1401234567890123456789",
                    (0x0994, 0x01, "^P007PIRI"): "2200,190,2200,500,190,5000,5000,480,540,500,420,564,540,2,30,80,0,1,2,1,0,1,0,1,2",
                }
            ),
            target,
        )

        assert inverter is not None
        evidence = await driver.async_capture_support_evidence(
            _FakeTransport(
                {
                    (0x0994, 0x01, "^P005PI"): "18",
                    (0x0994, 0x01, "^P005ID"): "1401234567890123456789",
                    (0x0994, 0x01, "^P007PIRI"): "2200,190,2200,500,190,5000,5000,480,540,500,420,564,540,2,30,80,0,1,2,1,0,1,0,1,2",
                    (0x0994, 0x01, "^P005GS"): "2301,500,2300,500,4500,4200,80,520,515,514,20,30,90,35,33,34,1200,800,3200,3100,1,2,1,1,2,2,1,0",
                    (0x0994, 0x01, "^P006MOD"): "05",
                }
            ),
            inverter,
        )

        self.assertEqual(evidence["capture_kind"], "pi18_experimental_dump")
        self.assertEqual(evidence["responses"]["^P005PI"], "18")
        self.assertIn("^P005GS", evidence["responses"])

    async def test_probe_and_runtime_values_have_entity_coverage_for_noninternal_keys(self) -> None:
        driver = Pi18Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        probe_transport = _FakeTransport(
            {
                (0x0994, 0x01, "^P005PI"): "18",
                (0x0994, 0x01, "^P005ID"): "1401234567890123456789",
                (0x0994, 0x01, "^P007PIRI"): "2200,190,2200,500,190,5000,5000,480,540,500,420,564,540,2,30,80,0,1,2,1,0,1,0,1,2",
                (0x0994, 0x01, "^P006VFW"): "00001,00002,00003",
            }
        )

        inverter = await driver.async_probe(probe_transport, target)

        assert inverter is not None
        runtime_values = await driver.async_read_values(
            _FakeTransport(
                {
                    (0x0994, 0x01, "^P005GS"): "2301,500,2300,500,4500,4200,80,520,515,514,20,30,90,35,33,34,1200,800,3200,3100,1,2,1,1,2,2,1,0",
                    (0x0994, 0x01, "^P006MOD"): "05",
                    (0x0994, 0x01, "^P007FLAG"): "1,1,0,0,0,1,1,1,0",
                    (0x0994, 0x01, "^P005FWS"): "00,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                    (0x0994, 0x01, "^P005ET"): "00001234",
                    (0x0994, 0x01, "^P004T"): "20160214201314",
                    (0x0994, 0x01, "^P009EY2016"): "00000123",
                    (0x0994, 0x01, "^P011EM201602"): "00000045",
                    (0x0994, 0x01, "^P013ED20160214"): "00000067",
                }
            ),
            inverter,
        )

        entity_keys = {
            *(description.key for description in BASE_SENSOR_DESCRIPTIONS),
            *(description.key for description in driver.measurements),
            *(description.key for description in driver.binary_sensors),
            *(description.key for description in canonical_measurements_for_driver("pi18")),
        }
        internal_only = {
            key
            for key in {*inverter.details, *runtime_values}
            if key.endswith("_code") or key.endswith("_raw") or key in {
                "clock_token",
                "qpiri_field_count",
                "qpigs_field_count",
                "battery_power_balance_current",
            }
        }
        missing = sorted(
            key
            for key in {*inverter.details, *runtime_values}
            if key not in internal_only
            if key not in entity_keys
        )

        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()