from __future__ import annotations

from pathlib import Path
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.payload.modbus import (
    ModbusError,
    ModbusSession,
    merge_register_bit,
    merge_register_field,
)


class _TimeoutTransport:
    def __init__(self) -> None:
        self.calls = 0

    async def async_send_forward(self, payload: bytes, *, devcode: int, collector_addr: int) -> bytes:
        self.calls += 1
        raise asyncio.TimeoutError()


class ModbusPayloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_holding_timeout_reports_request_timeout(self) -> None:
        transport = _TimeoutTransport()
        session = ModbusSession(
            transport,
            devcode=1,
            collector_addr=255,
            slave_id=1,
        )

        with self.assertRaises(ModbusError) as ctx:
            await session.read_holding(100, 2)

        self.assertEqual(str(ctx.exception), "request_timeout")
        self.assertEqual(transport.calls, 2)


class MergeRegisterBitTests(unittest.TestCase):
    def test_set_each_boundary_bit_preserves_other_bits(self) -> None:
        for bit_index in (0, 15):
            mask = 1 << bit_index
            merged = merge_register_bit(0x0000, bit_index, 1)
            self.assertEqual(merged, mask)
            # All other 15 bits stay zero.
            self.assertEqual(merged & ~mask, 0)

    def test_clear_each_boundary_bit_preserves_other_bits(self) -> None:
        for bit_index in (0, 15):
            mask = 1 << bit_index
            merged = merge_register_bit(0xFFFF, bit_index, 0)
            self.assertEqual(merged, 0xFFFF & ~mask)
            # All other 15 bits stay set.
            self.assertEqual(merged | mask, 0xFFFF)

    def test_set_bit_keeps_surrounding_bits(self) -> None:
        # 0xABCE has bit 0 == 0; setting it yields 0xABCF and touches nothing else.
        self.assertEqual(merge_register_bit(0xABCE, 0, 1), 0xABCF)
        # Setting an already-set bit is a no-op.
        self.assertEqual(merge_register_bit(0xABCF, 0, 1), 0xABCF)

    def test_clear_bit_keeps_surrounding_bits(self) -> None:
        self.assertEqual(merge_register_bit(0xABCF, 0, 1), 0xABCF)
        self.assertEqual(merge_register_bit(0xABCF, 0, 0), 0xABCE)

    def test_result_is_clamped_to_16_bits(self) -> None:
        self.assertLessEqual(merge_register_bit(0xFFFF, 15, 1), 0xFFFF)


class MergeRegisterFieldTests(unittest.TestCase):
    def test_replaces_only_masked_field(self) -> None:
        # Mask bits 4..7; write 0xA0 into them, keep the rest of 0x1234.
        merged = merge_register_field(0x1234, 0x00F0, 0x00A0)
        self.assertEqual(merged, 0x12A4)

    def test_field_bits_outside_mask_are_ignored(self) -> None:
        # field carries stray high bits; only masked bits land.
        merged = merge_register_field(0x0000, 0x000F, 0xFFF5)
        self.assertEqual(merged, 0x0005)

    def test_single_bit_field_matches_merge_register_bit(self) -> None:
        for bit_index in range(16):
            mask = 1 << bit_index
            self.assertEqual(
                merge_register_field(0x5555, mask, mask),
                merge_register_bit(0x5555, bit_index, 1),
            )
            self.assertEqual(
                merge_register_field(0x5555, mask, 0),
                merge_register_bit(0x5555, bit_index, 0),
            )


if __name__ == "__main__":
    unittest.main()