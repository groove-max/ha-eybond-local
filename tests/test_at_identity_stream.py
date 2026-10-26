"""AT identity is a complete bounded line, not an arbitrary TCP read."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
import unittest

from custom_components.eybond_local.collector.identity_probe import build_identity_probe_request
from custom_components.eybond_local.collector.transport import _SharedEybondListener


class AtIdentityStreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.reader = asyncio.StreamReader()
        self.listener = _SharedEybondListener(host="127.0.0.1", port=18899)
        self.request = build_identity_probe_request("at_text")

    async def read_identity(self, timeout=1.0):
        return await self.listener._read_identity_probe_response(
            SimpleNamespace(reader=self.reader), self.request, timeout=timeout,
        )

    async def test_valid_prefix_is_not_promoted_before_complete_line(self):
        task = asyncio.create_task(self.read_identity())
        try:
            self.reader.feed_data(b"AT+DTUPN:I200002SYN")
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            self.reader.feed_data(b"00000001\r\nNEXT-PAYLOAD")
            response, pn, source = await task
            self.assertEqual(response, b"AT+DTUPN:I200002SYN00000001\r\n")
            self.assertEqual(pn, "I200002SYN00000001")
            self.assertEqual(source, "at_dtupn")
            self.assertEqual(await self.reader.readexactly(12), b"NEXT-PAYLOAD")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_echo_and_blank_lines_share_one_reply(self):
        self.reader.feed_data(b"\r\nAT+DTUPN?\r\nAT+DTUPN:I200002SYN00000001\r\n")
        _, pn, source = await self.read_identity()
        self.assertEqual((pn, source), ("I200002SYN00000001", "at_dtupn"))

    async def test_unterminated_reply_has_no_identity_at_eof(self):
        self.reader.feed_data(b"AT+DTUPN:I200002SYN00000001")
        self.reader.feed_eof()
        _, pn, source = await self.read_identity()
        self.assertEqual((pn, source), ("", ""))

    async def test_unterminated_reply_times_out_without_accepting_prefix(self):
        self.reader.feed_data(b"AT+DTUPN:I200002SYN00000001")
        with self.assertRaises(asyncio.TimeoutError):
            await self.read_identity(timeout=0.01)

    async def test_input_limit_is_shared_across_lines(self):
        self.reader.feed_data(b"x\n" * 256 + b"AT+DTUPN:I200002SYN00000001\r\n")
        response, pn, source = await self.read_identity()
        self.assertEqual(len(response), 512)
        self.assertEqual((pn, source), ("", ""))
