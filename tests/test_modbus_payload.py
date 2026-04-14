from __future__ import annotations

from pathlib import Path
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.payload.modbus import ModbusError, ModbusSession


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


if __name__ == "__main__":
    unittest.main()