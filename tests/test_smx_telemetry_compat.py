"""Offline checks of the owner-unit SMX-II read-only compatibility overlay."""

from __future__ import annotations

import unittest

from custom_components.eybond_local.drivers.srne import SrneModbusDriver
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.registry import measurements_for_runtime
from custom_components.eybond_local.fixtures.transport import (
    FixtureTransport,
    FixtureTransportError,
)
from custom_components.eybond_local.metadata.compiled_detection_catalog import (
    load_compiled_detection_catalog,
    resolve_unique_persisted_model_surface,
)
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import crc16_modbus


TARGET = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
OWNER = "SR-2206260036-300917"
SCHEMA = "srne_modbus/smx_ii_24v.json"


class _CountingTransport(FixtureTransport):
    """Record real Modbus requests without changing the driver's read path."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.read_starts: list[int] = []
        self.read_shapes: list[tuple[int, int]] = []
        self.fail_starts: set[int] = set()

    def _handle_read_holding(self, payload: bytes) -> bytes:
        start = int.from_bytes(payload[2:4], "big")
        count = int.from_bytes(payload[4:6], "big")
        self.read_starts.append(start)
        self.read_shapes.append((start, count))
        # On the real owner unit, register 0x010E is readable by itself,
        # while requests touching 0x010F+ return Modbus exception 0x02.
        if start <= 271 < start + count:
            frame = bytes((TARGET.device_addr, 0x83, 0x02))
            return frame + crc16_modbus(frame).to_bytes(2, "little")
        # All 14 registers from the generic three-phase area 554-567 were
        # rejected individually by the owner's single-phase SMX-II (0x02).
        if start < 568 and start + count > 554:
            frame = bytes((TARGET.device_addr, 0x83, 0x02))
            return frame + crc16_modbus(frame).to_bytes(2, "little")
        if start in self.fail_starts:
            raise FixtureTransportError(f"injected_read_failure:{start}")
        return super()._handle_read_holding(payload)


def _fixture(identity: str) -> _CountingTransport:
    schema = load_register_schema(SCHEMA)
    registers = {
        block.start + offset: 0
        for block in schema.blocks
        for offset in range(block.count)
    }
    registers.update({53 + offset: ord(char) for offset, char in enumerate(identity[:20].ljust(20))})
    registers.update({
        257: 277, 258: 0xFE2F, 61485: 30, 61486: 4,
        61492: 22453, 61493: 0, 61494: 10945, 61495: 0,
        524: 0x380A, 525: 0x0312, 526: 0x0C22,
    })
    return _CountingTransport(registers=registers, command_responses=None, probe_target=TARGET)


class SmxTelemetryCompatTests(unittest.IsolatedAsyncioTestCase):
    async def test_scheduled_reads_keep_live_groups_and_rotate_slow_groups(self) -> None:
        driver = SrneModbusDriver()
        transport = _fixture(OWNER)
        inverter = await driver.async_probe(transport, TARGET)
        assert inverter is not None
        transport.read_starts.clear()  # Exclude the identity probe.
        runtime_state: dict = {}

        initial = await driver.async_read_values(
            transport, inverter, runtime_state=runtime_state, poll_interval=5.0
        )
        self.assertIs(initial.mode, DriverReadMode.FULL)
        self.assertEqual(len(transport.read_starts), 29)
        self.assertFalse(any(554 <= start < 568 for start in transport.read_starts))
        self.assertEqual(initial.values["battery_capacity"], 0)

        transport._registers[57346] = 188  # Changed static setting after cold start.
        fast_starts = {256, 260, 263, 267, 270, 516, 528, 61485, 61500}
        seen_slow: set[int] = set()
        refreshed_capacity = False
        for _ in range(20):
            transport.read_starts.clear()
            result = await driver.async_read_values(
                transport, inverter, runtime_state=runtime_state, poll_interval=5.0
            )
            self.assertIs(result.mode, DriverReadMode.DELTA)
            self.assertEqual(len(transport.read_starts), 10)
            self.assertTrue(fast_starts <= set(transport.read_starts))
            slow = set(transport.read_starts) - fast_starts
            self.assertEqual(len(slow), 1)
            seen_slow.update(slow)
            self.assertEqual(result.removed_keys, frozenset())
            self.assertIn("battery_voltage", result.values)
            self.assertIn("battery_charge_today", result.values)
            refreshed_capacity |= result.values.get("battery_capacity") == 188
        self.assertEqual(len(seen_slow), 20)
        self.assertNotIn(554, seen_slow)
        self.assertTrue(refreshed_capacity)

        # Explicit reads without owned runtime state retain full-sweep behavior.
        transport.read_starts.clear()
        direct = await driver.async_read_values(transport, inverter)
        self.assertIs(direct.mode, DriverReadMode.FULL)
        self.assertEqual(len(transport.read_starts), 29)

        # Hub recovery/rebinding clears runtime state and requires a new FULL.
        runtime_state.clear()
        transport.read_starts.clear()
        restarted = await driver.async_read_values(
            transport, inverter, runtime_state=runtime_state
        )
        self.assertIs(restarted.mode, DriverReadMode.FULL)
        self.assertEqual(len(transport.read_starts), 29)

    async def test_slow_rotation_scales_with_configured_poll_interval(self) -> None:
        driver = SrneModbusDriver()
        transport = _fixture(OWNER)
        inverter = await driver.async_probe(transport, TARGET)
        assert inverter is not None
        state: dict = {}
        await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=90.0
        )
        transport.read_starts.clear()
        result = await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=90.0
        )
        self.assertIs(result.mode, DriverReadMode.DELTA)
        self.assertEqual(len(transport.read_starts), 12)  # 9 fast + 3 slow.

    async def test_owner_charge_power_survives_unsupported_pv2_registers(self) -> None:
        driver = SrneModbusDriver()
        transport = _fixture(OWNER)
        transport._registers[270] = 235
        inverter = await driver.async_probe(transport, TARGET)
        assert inverter is not None

        state: dict = {}
        initial = await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=15.0
        )
        self.assertEqual(initial.values["charge_power"], 235)
        self.assertIn((270, 1), transport.read_shapes)
        self.assertNotIn((270, 4), transport.read_shapes)
        self.assertFalse(any(start <= 271 < start + count for start, count in transport.read_shapes))

        transport.read_shapes.clear()
        refreshed = await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=15.0
        )
        self.assertIs(refreshed.mode, DriverReadMode.DELTA)
        self.assertEqual(refreshed.values["charge_power"], 235)
        self.assertEqual(len(transport.read_shapes), 10)
        self.assertIn((270, 1), transport.read_shapes)
        self.assertFalse(any(start <= 271 < start + count for start, count in transport.read_shapes))
        for key in ("pv2_input_voltage", "pv2_input_current", "pv2_input_power"):
            self.assertNotIn(key, refreshed.values)
            self.assertNotIn(key, refreshed.removed_keys)

    async def test_owner_never_reads_unsupported_three_phase_range(self) -> None:
        driver = SrneModbusDriver()
        transport = _fixture(OWNER)
        inverter = await driver.async_probe(transport, TARGET)
        assert inverter is not None

        state: dict = {}
        await driver.async_read_values(transport, inverter, runtime_state=state)
        for _ in range(20):
            await driver.async_read_values(
                transport, inverter, runtime_state=state, poll_interval=15.0
            )
        await driver.async_read_values(transport, inverter)
        self.assertFalse(any(
            start < 568 and start + count > 554
            for start, count in transport.read_shapes
        ))
        schema = load_register_schema(SCHEMA)
        self.assertNotIn("phase", {block.key for block in schema.blocks})
        phase_keys = {
            f"{key}_{phase}"
            for key in (
                "grid_voltage", "output_voltage", "output_current",
                "output_power", "output_va", "load_percent",
            )
            for phase in ("l2", "l3")
        }
        self.assertFalse(phase_keys & {spec.key for spec in schema.spec_set("runtime")})
        self.assertFalse(phase_keys & {spec.key for spec in schema.measurement_descriptions})

    async def test_failed_selected_reads_invalidate_stale_values(self) -> None:
        driver = SrneModbusDriver()
        transport = _fixture(OWNER)
        inverter = await driver.async_probe(transport, TARGET)
        assert inverter is not None
        state: dict = {}
        await driver.async_read_values(transport, inverter, runtime_state=state)

        # Version is the first rotating slow block, live is a fast block.
        transport.fail_starts = {20, 528}
        failed = await driver.async_read_values(
            transport, inverter, runtime_state=state
        )
        self.assertIs(failed.mode, DriverReadMode.DELTA)
        self.assertIn("grid_voltage", failed.removed_keys)
        self.assertIn("app_version", failed.removed_keys)
        self.assertNotIn("battery_capacity", failed.removed_keys)
        self.assertNotIn("grid_voltage", failed.values)

        transport.fail_starts.clear()
        recovered = await driver.async_read_values(
            transport, inverter, runtime_state=state
        )
        self.assertIn("grid_voltage", recovered.values)
        self.assertNotIn("grid_voltage", recovered.removed_keys)

    async def test_exact_owner_uses_full_schema_without_controls(self) -> None:
        driver = SrneModbusDriver()
        transport = _fixture(OWNER)
        inverter = await driver.async_probe(transport, TARGET)
        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.variant_key, "smx_ii_24v")
        self.assertEqual(inverter.register_schema_name, SCHEMA)
        self.assertEqual(inverter.profile_name, "")
        self.assertEqual(inverter.capabilities, ())
        self.assertEqual(inverter.details["catalog_detection"]["resolution"], "exact")
        self.assertEqual(inverter.details["catalog_detection"]["confidence"], "high")

        result = await driver.async_read_values(transport, inverter)
        values = result.values
        self.assertEqual(values["battery_voltage"], 27.7)
        self.assertEqual(values["battery_charge_today"], 30)
        self.assertEqual(values["battery_discharge_today"], 4)
        self.assertEqual(values["battery_charge_total"], 22453)
        self.assertEqual(values["battery_discharge_total"], 10945)
        self.assertIsNone(values["system_datetime"])

        with self.assertRaisesRegex(ValueError, "unsupported_capability"):
            await driver.async_write_capability(transport, inverter, "max_ac_charge_current", 60)

    async def test_unlisted_srne_stays_generic_read_only(self) -> None:
        inverter = await SrneModbusDriver().async_probe(_fixture("SR-OTHER-MODEL"), TARGET)
        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.variant_key, "srne_family")
        self.assertEqual(inverter.register_schema_name, "srne_modbus/base.json")
        self.assertEqual(inverter.details["catalog_detection"]["resolution"], "family")
        self.assertEqual(inverter.capabilities, ())

    async def test_nearby_product_info_does_not_match_owner(self) -> None:
        inverter = await SrneModbusDriver().async_probe(_fixture("SR-2206260036-300918"), TARGET)
        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.register_schema_name, "srne_modbus/base.json")

    async def test_profile_compatibility_and_schema_invariant(self) -> None:
        catalog = load_compiled_detection_catalog()
        surface = catalog.surfaces["srne_modbus_smx_ii"]
        schema = load_register_schema(SCHEMA)
        legacy_profile = load_driver_profile("srne_modbus/smx_ii.json")
        self.assertTrue(surface.read_only)
        self.assertEqual(surface.profile_name, "")
        self.assertEqual(len(schema.measurement_descriptions), 97)
        self.assertEqual(legacy_profile.capabilities, ())
        self.assertEqual(legacy_profile.presets, ())
        keys = {item.key for item in schema.measurement_descriptions}
        self.assertTrue({
            "battery_charge_today", "battery_discharge_today",
            "battery_charge_total", "battery_discharge_total",
        }.issubset(keys))
        resolved = resolve_unique_persisted_model_surface("SRNE " + OWNER)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved[1].key, "srne_modbus_smx_ii")
        self.assertEqual(resolved[1].profile_name, "")
        runtime_keys = {
            item.key for item in measurements_for_runtime(
                driver_key="srne_modbus",
                register_schema_name=SCHEMA,
                variant_key="smx_ii_24v",
                write_capabilities=(),
            )
        }
        self.assertTrue(keys.issubset(runtime_keys))


if __name__ == "__main__":
    unittest.main()
