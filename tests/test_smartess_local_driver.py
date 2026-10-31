from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.registry import driver_options  # noqa: E402
from custom_components.eybond_local.drivers.read_result import (  # noqa: E402
    DriverReadMode,
    DriverReadResult,
)
from custom_components.eybond_local.drivers.smartess_local import SmartEssLocalDriver  # noqa: E402
from custom_components.eybond_local.drivers.command_support import seed_unsupported_commands, unsupported_commands  # noqa: E402
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport, FixtureTransportError  # noqa: E402
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RegisterValueSpec  # noqa: E402


def _delta_projection(result: DriverReadResult) -> dict[str, object]:
    if type(result) is not DriverReadResult or result.mode is not DriverReadMode.DELTA:
        raise AssertionError("SmartESS runtime read must be an exact DELTA result")
    return {**result.values, **result.diagnostics}


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
        values = _delta_projection(await self.driver.async_read_values(
            self._transport(),
            self._inverter(),
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        ))

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

    async def test_cached_slow_blocks_are_not_reported_fresh_on_fast_cycle(self) -> None:
        runtime_state: dict[str, object] = {}
        transport = self._transport()
        inverter = self._inverter()

        first = await self.driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=100.0,
        )
        second = await self.driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=110.0,
        )

        self.assertIs(first.mode, DriverReadMode.DELTA)
        self.assertIs(second.mode, DriverReadMode.DELTA)
        self.assertIn("battery_type", first.values)
        self.assertNotIn("battery_type", second.values)
        self.assertIn("output_active_power", second.values)
        self.assertNotIn("driver_slow_requests", second.values)
        self.assertIn("driver_slow_requests", second.diagnostics)

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
        values = _delta_projection(await self.driver.async_read_values(
            transport,
            inverter,
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        ))
        self.assertIs(values["lcd_backlight_enabled"], False)

    async def test_activated_control_reads_live_config_without_repeating_cached_values(self):
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                transport = self._transport(fail_config_bulk=fallback)
                transport._registers[5018] = 1
                base = next(c for c in self.driver.write_capabilities if c.key == "output_source_priority")
                learned = replace(base, key="learned_priority", read_key="", register=5018,
                                  enum_map={0: "Utility", 1: "Solar", 2: "SBU"},
                                  experimental=True, metadata_scope="device", write_function=6)
                inverter = replace(self._inverter(), capabilities=self._inverter().capabilities + (learned,))
                state = {}
                first = await self.driver.async_read_values(transport, inverter, runtime_state=state,
                    poll_interval=10, now_monotonic=100)
                self.assertEqual(first.values[learned.key], "Solar")
                second = await self.driver.async_read_values(transport, inverter, runtime_state=state,
                    poll_interval=10, now_monotonic=101)
                self.assertNotIn(learned.key, second.values)

    async def test_activated_control_preserves_observed_write_function_and_readback(self):
        for function in (6, 16):
            with self.subTest(function=function):
                transport = self._transport()
                base = next(c for c in self.driver.write_capabilities if c.key == "output_source_priority")
                learned = replace(base, key="learned_priority", read_key="", register=5018,
                                  enum_map={0: "Utility", 1: "Solar", 2: "SBU"},
                                  experimental=True, metadata_scope="device", write_function=function)
                inverter = replace(self._inverter(), capabilities=self._inverter().capabilities + (learned,))
                with patch.object(transport, "async_send_payload", wraps=transport.async_send_payload) as send:
                    result = await self.driver.async_write_capability(transport, inverter, learned.key, "Solar")
                self.assertEqual(result, "Solar")
                frames = [call.args[0] for call in send.call_args_list]
                self.assertEqual([frame[1] for frame in frames], [function, 3])
                self.assertTrue(all(call.kwargs["route"].collector_addr == 5 for call in send.call_args_list))
                values = await self.driver.async_read_values(transport, inverter, runtime_state={},
                    poll_interval=10, now_monotonic=100)
                self.assertEqual(values.values[learned.key], "Solar")

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
        values = _delta_projection(await self.driver.async_read_values(
            transport,
            inverter,
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        ))
        self.assertEqual(values["output_source_priority"], "SOL")

    async def test_read_values_falls_back_to_single_config_control_reads(self) -> None:
        values = _delta_projection(await self.driver.async_read_values(
            self._transport(fail_config_bulk=True),
            self._inverter(),
            runtime_state={},
            poll_interval=10.0,
            now_monotonic=100.0,
        ))

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
        known_values: dict[str, object] = {}

        # Four cycles collect the failure strikes for the rejected bulk reads.
        for now in (100.0, 200.0, 300.0, 400.0):
            cycle = await self.driver.async_read_values(
                transport,
                self._inverter(),
                runtime_state=runtime_state,
                poll_interval=10.0,
                now_monotonic=now,
            )
            known_values.update(cycle.values)
        attempts_after_learning = len(bulk_attempts)
        self.assertGreater(attempts_after_learning, 0)

        values = _delta_projection(await self.driver.async_read_values(
            transport,
            self._inverter(),
            runtime_state=runtime_state,
            poll_interval=10.0,
            now_monotonic=500.0,
        ))
        known_values.update(values)

        # No further bulk attempts for the rejected blocks.
        self.assertEqual(len(bulk_attempts), attempts_after_learning)
        self.assertIn("block:config", values["driver_unsupported_commands"])
        # Capability values still come from the single-register fallbacks.
        self.assertIs(known_values["lcd_backlight_enabled"], True)
        self.assertIs(known_values["buzzer_enabled"], False)

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

    async def test_successful_fallback_refreshes_only_on_slow_cadence(self) -> None:
        for interval, due in ((10.0, 160.0), (30.0, 280.0)):
            with self.subTest(interval=interval):
                transport = self._transport(fail_config_bulk=True)
                reads = _record_reads(transport)
                state = {}
                inverter = self._inverter()

                async def poll(now):
                    return await self.driver.async_read_values(
                        transport, inverter, runtime_state=state,
                        poll_interval=interval, now_monotonic=now,
                    )

                first = await poll(100.0)
                self.assertIs(first.values["lcd_backlight_enabled"], True)
                transport._registers[5004] = 0  # external panel/cloud change
                reads.clear()
                early = await poll(due - 1)
                self.assertNotIn((5004, 1), reads)
                self.assertNotIn("lcd_backlight_enabled", early.values)
                reads.clear()
                refreshed = await poll(due)
                self.assertEqual(reads.count((5004, 1)), 1)
                self.assertIs(refreshed.values["lcd_backlight_enabled"], False)

    async def test_persisted_bulk_skip_preserves_fresh_individual_reads(self) -> None:
        transport = self._transport(fail_config_bulk=True)
        reads = _record_reads(transport)
        state = {}
        seed_unsupported_commands(state, ["block:config", "block:time_settings"])
        for now, backlight in ((100.0, 1), (160.0, 0), (220.0, 1)):
            transport._registers[5004] = backlight
            reads.clear()
            result = await self.driver.async_read_values(
                transport, self._inverter(), runtime_state=state,
                poll_interval=10.0, now_monotonic=now,
            )
            self.assertNotIn((5001, 33), reads)
            self.assertEqual(reads.count((5004, 1)), 1)
            self.assertIs(result.values["lcd_backlight_enabled"], bool(backlight))

    async def test_persisted_live_skip_cannot_disable_required_telemetry(self) -> None:
        """Issue #59: a saved negative cache must not freeze the only live block."""
        transport = self._transport()
        reads = _record_reads(transport)
        state = {}
        seed_unsupported_commands(state, ["block:live", "register:5004"])
        for now, soc, pv, charging in ((100, 75, 3442, 44), (110, 80, 3493, 39)):
            transport._registers.update({
                4507: _wire_word(soc), 4505: _wire_word(pv), 4508: _wire_word(charging),
            })
            reads.clear()
            result = await self.driver.async_read_values(
                transport, self._inverter(), runtime_state=state,
                poll_interval=10, now_monotonic=now,
            )
            self.assertEqual(reads.count((4501, 14)), 1)
            self.assertEqual(result.values["battery_capacity"], soc)
            self.assertEqual(result.values["pv_input_power"], pv)
            self.assertEqual(result.values["charging_current"], charging)
            self.assertEqual(unsupported_commands(state), ("register:5004",))

    async def test_live_timeouts_do_not_become_persistent_unsupported_evidence(self) -> None:
        transport = self._transport()
        original = transport._handle_read_holding
        attempts = []
        failing = True

        def flaky(payload):
            address = int.from_bytes(payload[2:4], "big")
            if address == 4501:
                attempts.append(address)
                if failing:
                    raise TimeoutError("temporary live-read timeout")
            return original(payload)

        transport._handle_read_holding = flaky
        state = {}
        # Other blocks answer each time, which used to commit four strikes.
        for now in (100, 200, 300, 400, 500):
            result = await self.driver.async_read_values(
                transport, self._inverter(), runtime_state=state,
                poll_interval=10, now_monotonic=now,
            )
            self.assertNotIn("battery_capacity", result.values)
            self.assertNotIn("block:live", unsupported_commands(state))
        # Each failed read retains the existing single Modbus retry.
        self.assertEqual(len(attempts), 10)
        failing = False
        recovered = await self.driver.async_read_values(
            transport, self._inverter(), runtime_state=state,
            poll_interval=10, now_monotonic=600,
        )
        self.assertEqual(recovered.values["battery_capacity"], 78)
        self.assertEqual(len(attempts), 11)

    async def test_invalid_live_skip_is_cleared_even_when_live_read_still_fails(self) -> None:
        transport = self._transport()
        original = transport._handle_read_holding

        def flaky(payload):
            if int.from_bytes(payload[2:4], "big") == 4501:
                raise TimeoutError("still offline")
            return original(payload)

        transport._handle_read_holding = flaky
        state = {}
        seed_unsupported_commands(state, ["block:live"])
        result = await self.driver.async_read_values(
            transport, self._inverter(), runtime_state=state,
            poll_interval=10, now_monotonic=100,
        )
        self.assertEqual(unsupported_commands(state), ())
        self.assertEqual(result.diagnostics["driver_unsupported_commands"], "")
        self.assertNotIn("battery_capacity", result.values)

    async def test_failed_fallback_is_not_fresh_and_retry_is_paced(self) -> None:
        transport = self._transport(fail_config_bulk=True)
        reads = _record_reads(transport)
        original = transport._handle_read_holding
        fail = False

        def flaky(payload):
            if fail and int.from_bytes(payload[2:4], "big") == 5004:
                reads.append((5004, 1))
                raise FixtureTransportError("single_register_timeout")
            return original(payload)

        transport._handle_read_holding = flaky
        state = {}
        seed_unsupported_commands(state, ["block:config"])

        async def poll(now):
            return await self.driver.async_read_values(
                transport, self._inverter(), runtime_state=state,
                poll_interval=10.0, now_monotonic=now,
            )

        self.assertIs((await poll(100.0)).values["lcd_backlight_enabled"], True)
        fail = True
        reads.clear()
        failed = await poll(160.0)
        self.assertEqual(reads.count((5004, 1)), 1)
        self.assertNotIn("lcd_backlight_enabled", failed.values)
        self.assertIn("buzzer_enabled", failed.values)
        reads.clear()
        self.assertNotIn("lcd_backlight_enabled", (await poll(170.0)).values)
        self.assertNotIn((5004, 1), reads)
        fail = False
        transport._registers[5004] = 0
        self.assertIs((await poll(220.0)).values["lcd_backlight_enabled"], False)

    async def test_individual_unsupported_register_stays_skipped(self) -> None:
        transport = self._transport(fail_config_bulk=True)
        reads = _record_reads(transport)
        state = {}
        seed_unsupported_commands(state, ["block:config", "register:5004"])
        for now in (100.0, 160.0):
            result = await self.driver.async_read_values(
                transport, self._inverter(), runtime_state=state,
                poll_interval=10.0, now_monotonic=now,
            )
            self.assertNotIn("lcd_backlight_enabled", result.values)
            self.assertIn("buzzer_enabled", result.values)
        self.assertNotIn((5004, 1), reads)
        self.assertEqual(reads.count((5002, 1)), 2)

    async def test_unused_time_block_is_not_polled(self) -> None:
        transport = self._transport()
        reads = _record_reads(transport)
        await self.driver.async_read_values(
            transport, self._inverter(), runtime_state={},
            poll_interval=10.0, now_monotonic=100.0,
        )
        self.assertNotIn((6030, 6), reads)
        self.assertIn((5001, 33), reads)

    async def test_fallback_replaces_previous_bulk_value_without_writes(self) -> None:
        transport = self._transport()
        reads = _record_reads(transport)
        original = transport._handle_read_holding
        fail_bulk = False

        def switching(payload):
            if fail_bulk and int.from_bytes(payload[2:4], "big") == 5001:
                raise FixtureTransportError("bulk_read_timeout")
            return original(payload)

        def reject_write(payload):
            self.fail("polling must not write inverter settings")

        transport._handle_read_holding = switching
        transport._handle_write_single = reject_write
        transport._handle_write_multiple = reject_write
        state = {}
        for now, expected in ((100.0, True), (160.0, False)):
            result = await self.driver.async_read_values(
                transport, self._inverter(), runtime_state=state,
                poll_interval=10.0, now_monotonic=now,
            )
            self.assertIs(result.values["lcd_backlight_enabled"], expected)
            fail_bulk = True
            transport._registers[5004] = 0
        self.assertEqual(reads.count((5004, 1)), 1)

    async def test_schema_range_with_a_decoded_consumer_remains_polled(self) -> None:
        schema = load_register_schema(self.driver.register_schema_name)
        schema = replace(schema, spec_sets={
            **schema.spec_sets,
            "time_settings": (RegisterValueSpec(key="test_word", register=6030),),
        })
        transport = self._transport()
        reads = _record_reads(transport)
        with patch("custom_components.eybond_local.drivers.smartess_local.load_register_schema",
                   return_value=schema):
            result = await self.driver.async_read_values(
                transport, self._inverter(), runtime_state={},
                poll_interval=10.0, now_monotonic=100.0,
            )
        self.assertIn((6030, 6), reads)
        self.assertEqual(result.values["test_word"], 0)

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


def _record_reads(transport):
    reads = []
    original = transport._handle_read_holding

    def recording(payload):
        reads.append((int.from_bytes(payload[2:4], "big"),
                      int.from_bytes(payload[4:6], "big")))
        return original(payload)

    transport._handle_read_holding = recording
    return reads


if __name__ == "__main__":
    unittest.main()
