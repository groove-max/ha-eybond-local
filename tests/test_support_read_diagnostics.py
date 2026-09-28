"""Bounded support reads never write, recurse, or replace the original sample."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
import zipfile

from custom_components.eybond_local.drivers.support_diagnostics import capture_support_reads
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.support.package import export_support_package
from custom_components.eybond_local.support.runtime_projection import build_support_fixture


class SupportReadDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def capture(self, read, *, timeout=1):
        session = SimpleNamespace(read_holding=AsyncMock(side_effect=read))
        result = await capture_support_reads(
            session, ((10, 1, "first"), (20, 1, "second"), (30, 1, "third")),
            timeout_seconds=timeout, source="synthetic documented map", purpose="comparison",
        )
        return result, session.read_holding.await_args_list

    async def test_only_illegal_address_allows_next_group(self):
        for error, continues in ((ModbusError("exception_code:2"), True),
                                 (ModbusError("exception_code:3"), False),
                                 (ModbusError("crc_mismatch"), False),
                                 (TimeoutError(), False), (ConnectionError("offline"), False)):
            with self.subTest(error=error):
                async def read(start, count):
                    if start == 20:
                        raise error
                    return [start]
                result, calls = await self.capture(read)
                self.assertEqual([c.args[0] for c in calls], [10, 20, 30] if continues else [10, 20])
                self.assertEqual(result["status"], "completed" if continues else "stopped_on_error")
                self.assertEqual(result["captured_ranges"][0]["words"], [10])
                self.assertEqual(len(result["range_failures"]), 1)

    async def test_one_deadline_keeps_success_and_cancels_stalled_read(self):
        cancelled = asyncio.Event()
        async def read(start, count):
            if start == 10:
                return [10]
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        result, calls = await self.capture(read, timeout=0.01)
        self.assertTrue(cancelled.is_set())
        self.assertEqual(len(calls), 2)
        self.assertEqual(result["status"], "budget_exhausted")
        self.assertEqual(result["range_failures"][0]["error"], "diagnostic_budget_exhausted")
        self.assertEqual(len(result["captured_ranges"]), 1)

    async def test_external_cancel_is_not_exported_as_success(self):
        entered = asyncio.Event()
        async def read(start, count):
            entered.set()
            await asyncio.Future()
        task = asyncio.create_task(self.capture(read))
        try:
            await asyncio.wait_for(entered.wait(), 1)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_zip_preserves_extra_observations_without_rewriting_fixture(self):
        async def read(start, count):
            return [42]
        diagnostics, _ = await self.capture(read)
        for key in ("support_read_diagnostics", "current_read_diagnostics"):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as config_dir:
                original = [{"start": 10, "count": 1, "values": [0]}]
                evidence = {"capture_kind": "synthetic", "fixture_ranges": original, key: diagnostics}
                fixture = build_support_fixture(evidence, inverter=None, collector_payload=None)
                exported = export_support_package(
                    config_dir=Path(config_dir), entry_id="support_comparison",
                    entry_title="Read-only evidence", support_bundle={}, raw_capture=evidence,
                    fixture=fixture, anonymized_fixture=None,
                )
                with zipfile.ZipFile(exported.path) as archive:
                    self.assertIsNone(archive.testzip())
                    raw = json.loads(archive.read("raw_capture.json"))
                    saved = json.loads(archive.read("fixture/raw_fixture.json"))
                self.assertEqual(raw[key], diagnostics)
                self.assertEqual(saved["ranges"], original)
