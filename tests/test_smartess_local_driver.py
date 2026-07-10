from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.registry import driver_options  # noqa: E402
from custom_components.eybond_local.drivers.smartess_local import SmartEssLocalDriver  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport, FixtureTransportError  # noqa: E402
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget  # noqa: E402


def _registers_for_0925() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in (
        (4501, 14),
        (4517, 12),
        (4529, 2),
        (4531, 4),
        (4535, 18),
        (4554, 8),
        (5001, 33),
        (6030, 6),
    ):
        for register in range(start, start + count):
            registers[register] = 0

    registers.update(
        {
            4501: 1,
            4502: 2301,
            4503: 500,
            4504: 667,
            4505: 2400,
            4506: 512,
            4507: 78,
            4508: 12,
            4509: 3,
            4510: 2298,
            4511: 500,
            4512: 4300,
            4513: 4200,
            4514: 65,
            4517: 1,
            4518: 123,
            4519: 45,
            4521: 4200,
            4522: 4200,
            4525: 480,
            4535: 0,
            4536: 2,
            4537: 2,
            4538: 1,
            4539: 2,
            4540: 0,
            4541: 60,
            4542: 230,
            4543: 30,
            4544: 460,
            4545: 520,
            4546: 560,
            4547: 540,
            4548: 420,
            4554: 0,
            4555: 1234,
            4556: 0,
            4557: 4567,
            4558: 0,
            4559: 8901,
            4560: 1,
            4561: 2345,
            5004: 1,
        }
    )
    for register, value in tuple(registers.items()):
        if 4501 <= register <= 4561:
            registers[register] = _wire_word(value)
    return registers


def _wire_word(value: int) -> int:
    value = int(value) & 0xFFFF
    return ((value & 0x00FF) << 8) | ((value & 0xFF00) >> 8)


class SmartEssLocalDriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.driver = SmartEssLocalDriver()
        self.target = ProbeTarget(devcode=0x0001, collector_addr=0x05, device_addr=0x05)

    def _transport(self, *, fail_config_bulk: bool = False) -> FixtureTransport:
        class SplitRouteFixtureTransport(FixtureTransport):
            def _validate_target(self, *, devcode: int, collector_addr: int) -> None:
                if devcode != self._probe_target.devcode:
                    super()._validate_target(devcode=devcode, collector_addr=collector_addr)
                if collector_addr not in {0x05, 0xFF}:
                    super()._validate_target(devcode=devcode, collector_addr=collector_addr)

            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                if fail_config_bulk and address in {5001, 6030} and count > 1:
                    raise FixtureTransportError("config_bulk_rejected")
                return super()._handle_read_holding(payload)

            def _handle_write_single(self, payload: bytes) -> bytes:
                response = super()._handle_write_single(payload)
                address = int.from_bytes(payload[2:4], "big")
                if 4535 <= address <= 4552:
                    self._registers[address] = _wire_word(self._registers[address])
                return response

        return SplitRouteFixtureTransport(
            registers=_registers_for_0925(),
            command_responses=None,
            probe_target=self.target,
        )

    def _inverter(self) -> DetectedInverter:
        return DetectedInverter(
            driver_key="smartess_local",
            protocol_family="smartess_local",
            model_name="PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)",
            serial_number="",
            probe_target=self.target,
            variant_key="smartess_0925",
            profile_name="smartess_local/models/0925.json",
            register_schema_name="smartess_local/models/0925.json",
            capabilities=self.driver.write_capabilities,
        )

    def test_driver_is_selectable(self) -> None:
        self.assertIn("smartess_local", driver_options())
        capability_keys = [capability.key for capability in self.driver.write_capabilities]
        self.assertEqual(len(capability_keys), 30)
        self.assertIn("lcd_backlight_enabled", capability_keys)
        self.assertIn("output_source_priority", capability_keys)

    def test_all_0925_controls_are_verified_for_auto_exposure(self) -> None:
        for capability in self.driver.write_capabilities:
            self.assertTrue(capability.tested, capability.key)
            self.assertEqual(capability.provenance, "verified", capability.key)
            self.assertEqual(capability.resolved_support_tier, "standard", capability.key)

    def test_0925_controls_are_enabled_by_default_except_destructive_actions(self) -> None:
        disabled_by_default = {
            capability.key
            for capability in self.driver.write_capabilities
            if not capability.enabled_default
        }

        self.assertEqual(
            disabled_by_default,
            {
                "clear_power_history",
                "force_battery_equalization",
                "restore_defaults",
            },
        )

    async def test_probe_detects_0925_without_cloud_metadata(self) -> None:
        detected = await self.driver.async_probe(self._transport(), self.target)

        self.assertIsNotNone(detected)
        assert detected is not None
        self.assertEqual(detected.driver_key, "smartess_local")
        self.assertEqual(detected.protocol_family, "smartess_local")
        self.assertEqual(detected.variant_key, "smartess_0925")
        self.assertEqual(detected.model_name, "PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)")
        self.assertEqual(detected.details["smartess_protocol_asset_id"], "0925")
        self.assertEqual(detected.details["smartess_device_address"], 5)
        self.assertEqual(len(detected.capabilities), 30)

    async def test_read_values_decodes_schema_blocks(self) -> None:
        values = await self.driver.async_read_values(
            self._transport(),
            self._inverter(),
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        )

        self.assertEqual(values["protocol_id"], "SMARTESS_0925_MODBUS")
        self.assertEqual(values["ac_input_voltage"], 230.1)
        self.assertEqual(values["pv_input_power"], 2400)
        self.assertEqual(values["battery_voltage"], 51.2)
        self.assertEqual(values["output_voltage"], 229.8)
        self.assertEqual(values["output_active_power"], 4200)
        self.assertEqual(values["output_source_priority"], "SBU")
        self.assertEqual(values["battery_type"], "USER")
        self.assertEqual(values["bulk_charging_voltage"], 56.0)
        self.assertEqual(values["today_energy"], 12.34)
        self.assertEqual(values["all_energy"], 678.81)
        self.assertIs(values["lcd_backlight_enabled"], True)

    async def test_write_lcd_backlight_uses_single_register_function(self) -> None:
        transport = self._transport()
        inverter = self._inverter()

        result = await self.driver.async_write_capability(
            transport,
            inverter,
            "lcd_backlight_enabled",
            False,
        )

        self.assertIs(result, False)
        self.assertIs(inverter.details["lcd_backlight_enabled"], False)
        values = await self.driver.async_read_values(
            transport,
            inverter,
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        )
        self.assertIs(values["lcd_backlight_enabled"], False)

    async def test_write_enum_control_uses_config_state_route(self) -> None:
        transport = self._transport()
        inverter = self._inverter()

        result = await self.driver.async_write_capability(
            transport,
            inverter,
            "output_source_priority",
            "SOL",
        )

        self.assertEqual(result, "SOL")
        values = await self.driver.async_read_values(
            transport,
            inverter,
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        )
        self.assertEqual(values["output_source_priority"], "SOL")

    async def test_read_values_falls_back_to_single_config_control_reads(self) -> None:
        values = await self.driver.async_read_values(
            self._transport(fail_config_bulk=True),
            self._inverter(),
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        )

        self.assertIs(values["buzzer_enabled"], False)
        self.assertIs(values["power_saving_enabled"], False)
        self.assertIs(values["lcd_backlight_enabled"], True)

    async def test_rejected_bulk_blocks_stop_being_retried_every_cycle(self) -> None:
        transport = self._transport(fail_config_bulk=True)
        bulk_attempts: list[int] = []
        original = transport._handle_read_holding

        def _counting(payload: bytes) -> bytes:
            address = int.from_bytes(payload[2:4], "big")
            count = int.from_bytes(payload[4:6], "big")
            if count > 1 and address in {5001, 6030}:
                bulk_attempts.append(address)
            return original(payload)

        transport._handle_read_holding = _counting
        runtime_state: dict[str, object] = {}

        from custom_components.eybond_local.drivers.command_support import (
            UNSUPPORTED_COMMAND_STRIKES,
        )

        # Failure-strike cycles collect the strikes for the rejected bulk
        # reads; the bulk blocks stay on the wire through the last one.
        now = 0.0
        for _ in range(UNSUPPORTED_COMMAND_STRIKES):
            now += 100.0
            attempts_before = len(bulk_attempts)
            await self.driver.async_read_values(
                transport,
                self._inverter(),
                runtime_state=runtime_state,
                poll_interval=10.0,
                now_monotonic=now,
            )
            self.assertGreater(len(bulk_attempts), attempts_before)
        attempts_after_learning = len(bulk_attempts)

        now += 100.0
        values = await self.driver.async_read_values(
            transport,
            self._inverter(),
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=now,
        )

        # No further bulk attempts for the rejected blocks.
        self.assertEqual(len(bulk_attempts), attempts_after_learning)
        self.assertIn("block:config", values["driver_unsupported_commands"])
        # Capability values still come from the single-register fallbacks.
        self.assertIs(values["lcd_backlight_enabled"], True)
        self.assertIs(values["buzzer_enabled"], False)

    async def test_support_capture_reads_0925_config_controls_individually(self) -> None:
        evidence = await self.driver.async_capture_support_evidence(
            self._transport(fail_config_bulk=True),
            self._inverter(),
        )

        self.assertTrue(
            any(
                item["start"] == 5004
                and item["count"] == 1
                and item.get("capability") == "lcd_backlight_enabled"
                for item in evidence["captured_ranges"]
            )
        )
        self.assertFalse(
            any(
                failure["start"] == 5001 and failure["count"] == 33
                for failure in evidence["range_failures"]
            )
        )
        config_plan = [
            item
            for item in evidence["planned_ranges"]
            if item["block"] == "config"
        ][0]
        self.assertEqual(
            config_plan["read_strategy"],
            "individual_non_action_control_registers",
        )

    async def test_write_scaled_control_decodes_readback(self) -> None:
        transport = self._transport()
        inverter = self._inverter()

        result = await self.driver.async_write_capability(
            transport,
            inverter,
            "bulk_charging_voltage",
            57.2,
        )

        self.assertEqual(result, 57.2)
