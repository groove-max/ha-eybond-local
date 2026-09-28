from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.srne import SrneModbusDriver  # noqa: E402
from custom_components.eybond_local.drivers.read_result import (  # noqa: E402
    DriverReadMode,
    DriverReadResult,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402
from custom_components.eybond_local.payload.modbus import (  # noqa: E402
    ModbusError,
    crc16_modbus,
    decode_read_request,
)
from custom_components.eybond_local.support.package import export_support_package  # noqa: E402
from custom_components.eybond_local.support.runtime_projection import build_support_fixture  # noqa: E402


def _full_values(result: DriverReadResult) -> dict[str, object]:
    if type(result) is not DriverReadResult or result.mode is not DriverReadMode.FULL:
        raise AssertionError("SRNE runtime read must be an exact FULL result")
    return result.values


def _low_byte_ascii_words(text: str, count: int) -> list[int]:
    padded = text[:count].ljust(count)
    return [ord(char) for char in padded]


def _srne_registers(*, include_phase: bool = True) -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in ((53, 20), (256, 18), (516, 4), (528, 18), (554, 14)):
        if start == 554 and not include_phase:
            continue
        for offset in range(count):
            registers[start + offset] = 0

    for offset, word in enumerate(_low_byte_ascii_words("SR-EOV24", 20)):
        registers[53 + offset] = word

    registers.update(
        {
            256: 85,
            257: 512,
            258: 123,
            263: 3561,
            264: 42,
            265: 680,
            267: 2,
            270: 900,
            271: 3482,
            272: 38,
            273: 620,
            516: 7,
            528: 5,
            531: 2301,
            533: 5002,
            534: 2298,
            536: 4998,
            537: 56,
            539: 1200,
            540: 1300,
            542: 101,
            543: 64,
            544: 425,
            545: 392,
        }
    )

    if include_phase:
        registers.update(
            {
                554: 2310,
                555: 2320,
                556: 2280,
                557: 2270,
                560: 25,
                561: 26,
                562: 510,
                563: 520,
                564: 610,
                565: 620,
                566: 31,
                567: 32,
            }
        )
    return registers


class SrneModbusDriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_detected_read_only_family_has_restorable_catalog_proof(self):
        from dataclasses import replace
        from custom_components.eybond_local.metadata.effective_metadata_snapshot import (
            build_effective_metadata_snapshot_from_runtime,
            effective_metadata_snapshot_from_dict,
        )

        driver = SrneModbusDriver()
        target = ProbeTarget(1, 255, 1)
        link = FixtureTransport(registers=_srne_registers(), command_responses=None, probe_target=target)
        inverter = await driver.async_probe(link, target)
        proof = inverter.details["catalog_detection"]
        self.assertEqual(proof["resolution"], "family")
        self.assertEqual(proof["confidence"], "medium")
        snapshot = build_effective_metadata_snapshot_from_runtime(
            inverter=inverter, confidence=proof["confidence"],
        )
        self.assertTrue(snapshot.is_valid)
        self.assertTrue(effective_metadata_snapshot_from_dict(snapshot.as_dict()).is_valid)
        self.assertFalse(snapshot.profile_name)
        self.assertFalse(inverter.capabilities)
        for delta in (
            {"catalog_version": ""}, {"catalog_version": "stale"},
            {"candidate_keys": ()}, {"evidence_fingerprint": ""},
            {"descriptor_revisions": ("srne_modbus_family:stale",)},
        ):
            self.assertFalse(replace(snapshot, **delta).is_valid, delta)

    async def test_probe_detects_srne_product_info_on_slave_one(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.driver_key, "srne_modbus")
        self.assertEqual(inverter.protocol_family, "srne_modbus")
        self.assertEqual(inverter.model_name, "SRNE SR-EOV24")
        self.assertEqual(inverter.variant_key, "srne_family")
        self.assertEqual(inverter.profile_name, "")
        self.assertEqual(inverter.register_schema_name, "srne_modbus/base.json")
        self.assertEqual(inverter.probe_target.device_addr, 1)
        self.assertEqual(inverter.details["product_info"], "SR-EOV24")

    async def test_probe_rejects_non_srne_product_info(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        registers = _srne_registers()
        for offset, word in enumerate(_low_byte_ascii_words("INV-0001", 20)):
            registers[53 + offset] = word
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNone(inverter)

    async def test_read_values_decodes_srne_register_map(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["product_info"], "SR-EOV24")
        self.assertEqual(values["battery_percent"], 85)
        self.assertEqual(values["battery_voltage"], 51.2)
        self.assertEqual(values["battery_current"], 12.3)
        self.assertEqual(values["pv1_input_voltage"], 356.1)
        self.assertEqual(values["pv1_input_current"], 4.2)
        self.assertEqual(values["pv1_input_power"], 680)
        self.assertEqual(values["inverter_charge_state"], 2)
        self.assertEqual(values["charge_power"], 900)
        self.assertEqual(values["pv2_input_voltage"], 348.2)
        self.assertEqual(values["pv2_input_current"], 3.8)
        self.assertEqual(values["pv2_input_power"], 620)
        self.assertEqual(values["fault_code"], 7)
        self.assertEqual(values["inverter_operation_mode"], "Inverter")
        self.assertEqual(values["grid_voltage"], 230.1)
        self.assertEqual(values["grid_frequency"], 50.02)
        self.assertEqual(values["output_voltage"], 229.8)
        self.assertEqual(values["output_frequency"], 49.98)
        self.assertEqual(values["output_current"], 5.6)
        self.assertEqual(values["output_power"], 1200)
        self.assertEqual(values["output_va"], 1300)
        self.assertEqual(values["battery_charge_current"], 10.1)
        self.assertEqual(values["load_percent"], 64)
        self.assertEqual(values["dcdc_temperature"], 42.5)
        self.assertEqual(values["inverter_temperature"], 39.2)
        self.assertEqual(values["grid_voltage_l2"], 231.0)
        self.assertEqual(values["output_power_l3"], 520)
        self.assertEqual(values["load_percent_l3"], 32)

    async def test_read_values_tolerates_missing_optional_phase_block(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(include_phase=False),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["product_info"], "SR-EOV24")
        self.assertEqual(values["output_power"], 1200)
        self.assertNotIn("grid_voltage_l2", values)

    async def test_support_evidence_captures_planned_ranges(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        evidence = await driver.async_capture_support_evidence(transport, inverter)

        self.assertEqual(evidence["capture_kind"], "srne_modbus_register_dump")
        self.assertEqual(evidence["range_failures"], [])
        planned = [(item["start"], item["count"]) for item in evidence["planned_ranges"]]
        self.assertIn((53, 20), planned)
        self.assertIn((528, 18), planned)
        self.assertIn((554, 14), planned)
        self.assertEqual(len(evidence["fixture_ranges"]), len(planned))


class _ReadOnlySrneTransport(FixtureTransport):
    """Replay an inverter rejecting reads that include undocumented holes."""

    def __init__(self, registers: dict[int, int], target: ProbeTarget) -> None:
        super().__init__(registers=registers, command_responses=None, probe_target=target)
        self.requests: list[tuple[int, int]] = []

    async def async_send_payload(self, payload, *, route):
        request = decode_read_request(payload)
        if request is None or request.function_code != 3:
            raise AssertionError("Support capture must only issue FC3 reads")
        self.requests.append((request.address, request.count))
        if any(
            address not in self._registers
            for address in range(request.address, request.address + request.count)
        ):
            response = bytes((request.slave_id, 0x83, 2))
            return response + crc16_modbus(response).to_bytes(2, "little")
        return await super().async_send_payload(payload, route=route)


class SrneSupportDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        registers = _srne_registers(include_phase=False)
        del registers[269]  # 0x010D: reserved; never read it separately.
        self.transport = _ReadOnlySrneTransport(registers, target)
        self.inverter = await self.driver.async_probe(self.transport, target)
        assert self.inverter is not None
        self.transport.requests.clear()

    async def test_rejected_dc_block_gets_only_fixed_support_subranges(self) -> None:
        evidence = await self.driver.async_capture_support_evidence(
            self.transport, self.inverter
        )

        normal = [(53, 20), (256, 18), (516, 4), (528, 18), (554, 14)]
        extra = [(256, 3), (263, 3), (267, 1), (270, 1), (271, 3)]
        self.assertEqual(self.transport.requests, normal + extra)
        self.assertEqual(
            [(item["start"], item["count"]) for item in evidence["planned_ranges"]],
            normal,
        )
        self.assertEqual(
            evidence["range_failures"],
            [
                {"start": 256, "count": 18, "error": "exception_code:2"},
                {"start": 554, "count": 14, "error": "exception_code:2"},
            ],
        )
        diagnostics = evidence["dc_subrange_diagnostics"]
        self.assertEqual(diagnostics["status"], "completed")
        self.assertEqual(diagnostics["range_failures"], [])
        self.assertEqual(diagnostics["trigger_range"], {"start": 256, "count": 18})
        self.assertEqual(diagnostics["time_budget_seconds"], 15.0)
        captured = diagnostics["captured_ranges"]
        self.assertEqual([(item["start"], item["count"]) for item in captured], extra)
        self.assertEqual(captured[0]["words"], [85, 512, 123])
        self.assertEqual(len(evidence["captured_ranges"]), 3)
        self.assertEqual(len(evidence["fixture_ranges"]), 8)

        # Runtime fallback is based on its own live rejection, not archive state.
        self.transport.requests.clear()
        values = _full_values(
            await self.driver.async_read_values(self.transport, self.inverter)
        )
        self.assertEqual(self.transport.requests, normal[:2] + extra + normal[2:])
        self.assertEqual(values["output_power"], 1200)
        self.assertEqual(values["battery_voltage"], 51.2)
        self.assertEqual(
            [(plan.start, plan.count) for plan in self.driver.local_register_read_plans(self.inverter)],
            normal[:1] + extra + normal[2:],
        )

    async def test_runtime_fallback_exposes_battery_without_inventing_missing_pv2(self) -> None:
        for register in (271, 272, 273):
            del self.transport._registers[register]
        self.transport._registers.update({256: 93, 257: 287, 258: 65430})
        values = _full_values(await self.driver.async_read_values(self.transport, self.inverter))
        self.assertEqual(values["battery_percent"], 93)
        self.assertEqual(values["battery_voltage"], 28.7)
        self.assertEqual(values["battery_current"], -10.6)
        self.assertEqual(values["pv1_input_power"], 680)
        self.assertNotIn("pv2_input_power", values)
        self.assertNotIn("grid_voltage_l2", values)
        snapshot = await self.driver.async_capture_local_register_snapshot(
            self.transport, self.inverter, collector_pn="E50000200000000001"
        )
        battery = next(block for block in snapshot.blocks if block.plan.start == 256)
        self.assertEqual(battery.values, (93, 287, 65430))
        self.assertEqual(snapshot.failed_block_count, 2)  # PV2 and optional phases

    async def test_runtime_does_not_split_timeouts_or_malformed_parent(self) -> None:
        for error in (TimeoutError(), ModbusError("crc_error"),
                      ModbusError("unexpected_slave_id:0"), ModbusError("exception_code:3")):
            with self.subTest(error=str(error)):
                session = self.driver._session(self.transport, self.inverter.probe_target)
                original = session.read_registers
                calls = []

                async def read(start, count, *, function=3):
                    calls.append((start, count))
                    if (start, count) == (256, 18):
                        raise error
                    return await original(start, count, function=function)

                session.read_registers = read
                with patch.object(self.driver, "_session", return_value=session):
                    values = _full_values(await self.driver.async_read_values(self.transport, self.inverter))
                self.assertNotIn((256, 3), calls)
                self.assertNotIn("battery_voltage", values)
                self.assertEqual(values["output_power"], 1200)

    async def test_runtime_subread_timeout_stops_fallback_without_stale_battery(self) -> None:
        session = self.driver._session(self.transport, self.inverter.probe_target)
        original = session.read_registers
        calls = []

        async def read(start, count, *, function=3):
            calls.append((start, count))
            if (start, count) == (256, 3):
                raise TimeoutError()
            return await original(start, count, function=function)

        session.read_registers = read
        with patch.object(self.driver, "_session", return_value=session):
            values = _full_values(await self.driver.async_read_values(self.transport, self.inverter))
        self.assertIn((256, 3), calls)
        self.assertNotIn((263, 3), calls)
        self.assertNotIn("battery_voltage", values)
        self.assertEqual(values["output_power"], 1200)

    async def test_supported_parent_stays_one_read_and_has_no_cross_device_state(self) -> None:
        await self.driver.async_read_values(self.transport, self.inverter)
        other = _ReadOnlySrneTransport(_srne_registers(), self.inverter.probe_target)
        values = _full_values(await self.driver.async_read_values(other, self.inverter))
        self.assertNotIn((256, 3), other.requests)
        self.assertEqual(other.requests.count((256, 18)), 1)
        self.assertEqual(values["pv2_input_power"], 620)

    async def test_runtime_fallback_propagates_disconnect_and_cancellation(self) -> None:
        for error in (ConnectionError("gone"), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__):
                session = self.driver._session(self.transport, self.inverter.probe_target)
                original = session.read_registers
                calls = []

                async def read(start, count, *, function=3):
                    calls.append((start, count))
                    if (start, count) == (256, 3):
                        raise error
                    return await original(start, count, function=function)

                session.read_registers = read
                with patch.object(self.driver, "_session", return_value=session):
                    with self.assertRaises(type(error)):
                        await self.driver.async_read_values(self.transport, self.inverter)
                self.assertNotIn((263, 3), calls)
                self.assertNotIn((528, 18), calls)

    async def test_rejected_subrange_does_not_expand_the_probe(self) -> None:
        del self.transport._registers[257]
        evidence = await self.driver.async_capture_support_evidence(
            self.transport, self.inverter
        )
        diagnostics = evidence["dc_subrange_diagnostics"]
        self.assertEqual(diagnostics["status"], "completed")
        self.assertEqual(len(self.transport.requests), 10)
        self.assertEqual(
            diagnostics["range_failures"],
            [{"start": 256, "count": 3, "error": "exception_code:2"}],
        )
        self.assertEqual(len(diagnostics["captured_ranges"]), 4)

    async def test_export_preserves_parent_failure_and_subrange_evidence(self) -> None:
        evidence = await self.driver.async_capture_support_evidence(
            self.transport, self.inverter
        )
        fixture = build_support_fixture(
            evidence, inverter=self.inverter, collector_payload=None
        )
        with tempfile.TemporaryDirectory() as config_dir:
            exported = export_support_package(
                config_dir=Path(config_dir),
                entry_id="srne_test",
                entry_title="SRNE diagnostics",
                support_bundle={},
                raw_capture=evidence,
                fixture=fixture,
                anonymized_fixture=None,
            )
            with zipfile.ZipFile(exported.path) as archive:
                self.assertIsNone(archive.testzip())
                raw = json.loads(archive.read("raw_capture.json"))
                saved_fixture = json.loads(archive.read("fixture/raw_fixture.json"))
        self.assertEqual(raw["dc_subrange_diagnostics"], evidence["dc_subrange_diagnostics"])
        self.assertEqual(raw["range_failures"], evidence["range_failures"])
        self.assertEqual(saved_fixture["ranges"], evidence["fixture_ranges"])
        self.assertEqual(saved_fixture["probe_target"]["device_addr"], 1)

    async def test_no_subranges_after_success_or_non_address_failure(self) -> None:
        for failure in (
            None,
            ModbusError("request_timeout"),
            ModbusError("exception_code:3"),
            ModbusError("crc_mismatch"),
            OSError("offline"),
        ):
            with self.subTest(failure=failure):
                async def read(start, count):
                    if (start, count) == (256, 18) and failure is not None:
                        raise failure
                    return [0] * count

                session = type("Session", (), {"read_holding": AsyncMock(side_effect=read)})()
                with patch.object(self.driver, "_session", return_value=session):
                    evidence = await self.driver.async_capture_support_evidence(
                        self.transport, self.inverter
                    )
                self.assertNotIn("dc_subrange_diagnostics", evidence)
                self.assertEqual(session.read_holding.await_count, 5)

    async def test_extra_reads_stop_at_first_non_address_failure(self) -> None:
        for failure in (
            ModbusError("request_timeout"),
            ModbusError("exception_code:3"),
            ModbusError("crc_mismatch"),
            OSError("offline"),
        ):
            with self.subTest(failure=failure):
                async def read(start, count):
                    if (start, count) == (256, 18):
                        raise ModbusError("exception_code:2")
                    if (start, count) == (263, 3):
                        raise failure
                    return [0] * count

                session = type("Session", (), {"read_holding": AsyncMock(side_effect=read)})()
                with patch.object(self.driver, "_session", return_value=session):
                    evidence = await self.driver.async_capture_support_evidence(
                        self.transport, self.inverter
                    )
                diagnostics = evidence["dc_subrange_diagnostics"]
                self.assertEqual(diagnostics["status"], "stopped_on_error")
                self.assertEqual(session.read_holding.await_count, 7)
                self.assertEqual(len(evidence["captured_ranges"]), 4)
                self.assertEqual(len(diagnostics["captured_ranges"]), 1)
                self.assertEqual(diagnostics["range_failures"][0]["error"], str(failure))

    async def test_extra_read_budget_keeps_prior_successes(self) -> None:
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def read(start, count):
            if (start, count) == (256, 18):
                raise ModbusError("exception_code:2")
            if (start, count) == (263, 3):
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    cancelled.set()
            return [0] * count

        session = type("Session", (), {"read_holding": AsyncMock(side_effect=read)})()
        with patch.object(self.driver, "_session", return_value=session), patch(
            "custom_components.eybond_local.drivers.srne._DC_DIAGNOSTIC_TIMEOUT", 0.01
        ):
            evidence = await self.driver.async_capture_support_evidence(
                self.transport, self.inverter
            )
        self.assertTrue(entered.is_set())
        self.assertTrue(cancelled.is_set())
        diagnostics = evidence["dc_subrange_diagnostics"]
        self.assertEqual(diagnostics["status"], "budget_exhausted")
        self.assertEqual(session.read_holding.await_count, 7)
        self.assertEqual(len(evidence["fixture_ranges"]), 5)
        self.assertEqual(diagnostics["range_failures"][0]["error"], "diagnostic_budget_exhausted")

    async def test_external_cancellation_is_not_a_partial_success(self) -> None:
        entered = asyncio.Event()

        async def read(start, count):
            if (start, count) == (256, 18):
                raise ModbusError("exception_code:2")
            if (start, count) == (256, 3):
                entered.set()
                await asyncio.Future()
            return [0] * count

        session = type("Session", (), {"read_holding": AsyncMock(side_effect=read)})()
        with patch.object(self.driver, "_session", return_value=session):
            task = asyncio.create_task(self.driver.async_capture_support_evidence(
                self.transport, self.inverter
            ))
            try:
                await asyncio.wait_for(entered.wait(), 1)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertEqual(session.read_holding.await_count, 6)


if __name__ == "__main__":
    unittest.main()
