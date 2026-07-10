from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.support.diagnostic_targets import (
    AsciiDiagnosticTarget,
    DiagnosticTargetError,
    ModbusDiagnosticTarget,
    UnsupportedDriverError,
    build_diagnostic_target,
    has_diagnostic_target,
    supported_kinds_for_driver,
)


TARGET = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)


def _modbus_transport(registers: dict[int, int]) -> FixtureTransport:
    return FixtureTransport(
        registers=registers,
        command_responses=None,
        probe_target=TARGET,
    )


def _pi_transport(responses: dict[str, str]) -> FixtureTransport:
    command_responses = {(TARGET.devcode, TARGET.collector_addr, k): v for k, v in responses.items()}
    return FixtureTransport(
        registers=None,
        command_responses=command_responses,
        probe_target=TARGET,
    )


class MatrixTests(unittest.TestCase):
    def test_supported_kinds(self) -> None:
        self.assertEqual(supported_kinds_for_driver("modbus_smg"), frozenset({"read", "write", "write_bit"}))
        self.assertEqual(supported_kinds_for_driver("pi30"), frozenset({"ascii"}))
        self.assertEqual(supported_kinds_for_driver("pi18"), frozenset({"ascii"}))
        self.assertEqual(supported_kinds_for_driver("nope"), frozenset())

    def test_has_diagnostic_target(self) -> None:
        self.assertTrue(has_diagnostic_target("modbus_smg"))
        self.assertFalse(has_diagnostic_target("nope"))

    def test_build_unknown_driver_raises(self) -> None:
        with self.assertRaises(UnsupportedDriverError):
            build_diagnostic_target("nope", _modbus_transport({}), TARGET)

    def test_build_smg_returns_modbus_target(self) -> None:
        target = build_diagnostic_target("modbus_smg", _modbus_transport({}), TARGET)
        self.assertIsInstance(target, ModbusDiagnosticTarget)

    def test_build_pi_returns_ascii_target(self) -> None:
        target = build_diagnostic_target("pi30", _pi_transport({}), TARGET)
        self.assertIsInstance(target, AsciiDiagnosticTarget)


class ModbusTargetTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_holding(self) -> None:
        transport = _modbus_transport({171: 8960, 172: 12336})
        target = build_diagnostic_target("modbus_smg", transport, TARGET)
        self.assertEqual(await target.read_holding(171, 2), [8960, 12336])

    async def test_write_single_register(self) -> None:
        transport = _modbus_transport({354: 0})
        target = build_diagnostic_target("modbus_smg", transport, TARGET)
        await target.write_holding(354, [1])
        self.assertEqual(transport._registers[354], 1)

    async def test_write_multiple_registers(self) -> None:
        transport = _modbus_transport({100: 0, 101: 0, 102: 0})
        target = build_diagnostic_target("modbus_smg", transport, TARGET)
        await target.write_holding(100, [1, 2, 0xFFFF])
        self.assertEqual(
            [transport._registers[r] for r in (100, 101, 102)], [1, 2, 0xFFFF]
        )

    async def test_write_bit_set_preserves_other_bits(self) -> None:
        transport = _modbus_transport({354: 0xABCE})
        target = build_diagnostic_target("modbus_smg", transport, TARGET)
        outcome = await target.write_bit(354, 0, 1)
        self.assertEqual(outcome.before, 0xABCE)
        self.assertEqual(outcome.written, 0xABCF)
        self.assertEqual(outcome.mask, 0x0001)
        self.assertEqual(transport._registers[354], 0xABCF)

    async def test_write_bit_clear_preserves_other_bits(self) -> None:
        transport = _modbus_transport({354: 0xFFFF})
        target = build_diagnostic_target("modbus_smg", transport, TARGET)
        outcome = await target.write_bit(354, 7, 0)
        self.assertEqual(outcome.written, 0xFF7F)
        self.assertEqual(transport._registers[354], 0xFF7F)
        # Every other bit stays set.
        self.assertEqual(transport._registers[354] | 0x0080, 0xFFFF)

    async def test_write_bit_each_boundary_index(self) -> None:
        for bit_index in (0, 15):
            transport = _modbus_transport({10: 0x0000})
            target = build_diagnostic_target("modbus_smg", transport, TARGET)
            await target.write_bit(10, bit_index, 1)
            self.assertEqual(transport._registers[10], 1 << bit_index)

    async def test_write_bit_no_write_when_pre_read_fails(self) -> None:
        # Register 354 is absent: the fixture raises on the pre-read, so no write
        # may happen and the register must stay absent.
        transport = _modbus_transport({100: 0})
        target = build_diagnostic_target("modbus_smg", transport, TARGET)
        with self.assertRaises(Exception):
            await target.write_bit(354, 0, 1)
        self.assertNotIn(354, transport._registers)


class AsciiTargetTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_ascii_returns_payload_and_raw(self) -> None:
        transport = _pi_transport({"QPI": "PI30"})
        target = build_diagnostic_target("pi30", transport, TARGET)
        outcome = await target.send_ascii("QPI")
        self.assertEqual(outcome.payload, "PI30")
        self.assertIsNone(outcome.decode_error)
        self.assertTrue(outcome.raw)

    async def test_send_ascii_surfaces_nak_as_data(self) -> None:
        transport = _pi_transport({"QPIGS": "NAK"})
        target = build_diagnostic_target("pi30", transport, TARGET)
        outcome = await target.send_ascii("QPIGS")
        self.assertEqual(outcome.payload, "NAK")
        self.assertIsNone(outcome.decode_error)


if __name__ == "__main__":
    unittest.main()
