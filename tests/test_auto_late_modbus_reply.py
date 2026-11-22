"""#89 Auto: distinguish a late framed Modbus read from an unowned RTU prefix."""

import asyncio
from unittest import IsolatedAsyncioTestCase

import test_empty_forward_reply as helpers
from test_collector_auxiliary_session import _wait
from custom_components.eybond_local.collector.protocol import decode_header
from custom_components.eybond_local.payload.modbus import crc16_modbus


class LateAutoReadTests(IsolatedAsyncioTestCase):
    task = helpers.EmptyForwardReaderTests.task
    open = helpers.EmptyForwardReaderTests.open

    async def test_cancelled_catalog_read_does_not_deliver_to_pi30_probe(self):
        # The field log shows the request and eight bytes of the later header.
        # Both the envelope and the ninth RTU byte below are counterfactuals,
        # not additional captured bytes.
        prefix = bytes.fromhex("01030400000000fa")
        rtu = prefix[:7] + crc16_modbus(prefix[:7]).to_bytes(2, "little")
        self.assertEqual(rtu[:8], prefix)
        for framed in (True, False):
            with self.subTest(framed=framed):
                c, r, w, run = await self.open("framed")
                await _wait(lambda: any(decode_header(msg).fcode == 1 for msg in w.writes))
                c._tid._value = 541

                async def send(payload, code, tid):
                    pending = self.task(c.async_send_forward(
                        payload, devcode=code, collector_addr=255, request_timeout=2,
                    ))
                    await _wait(lambda: any(decode_header(msg).tid == tid for msg in w.writes))
                    return pending

                old = await send(bytes.fromhex("0103004c000205dc"), 1, 542)
                old.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await old
                current = await send(bytes.fromhex("515049beac0d"), 0x994, 543)
                if framed:
                    r.feed_data(helpers.frame(542, devcode=1, payload=rtu)
                                + helpers.frame(543, payload=b"current reply"))
                    self.assertEqual(await current, b"current reply")
                    self.assertTrue(c.connected)
                else:
                    r.feed_data(prefix)  # only the bytes actually logged
                    await asyncio.wait_for(run, 1)
                    with self.assertRaises(ConnectionError):
                        await current
                    self.assertEqual(c.collector_info.last_disconnect_reason, "collector_frame_length_invalid")
                await c.disconnect()
