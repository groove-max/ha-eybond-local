from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.at import (
    CollectorAtCommand,
    CollectorAtError,
    CollectorAtStreamSession,
    build_at_query,
    build_at_response,
    build_at_write,
    parse_at_command,
    parse_at_response,
)
from custom_components.eybond_local.collector.at_runtime import query_runtime_collector_at_values


class _FakeWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None


class CollectorAtTests(unittest.TestCase):
    def test_build_at_query_and_write(self) -> None:
        self.assertEqual(build_at_query("WFSS"), b"AT+WFSS?\r\n")
        self.assertEqual(
            build_at_write("CLDSRVHOST1", "collector-cloud.smartess.example,18899,TCP"),
            b"AT+CLDSRVHOST1=collector-cloud.smartess.example,18899,TCP\r\n",
        )
        self.assertEqual(
            build_at_response("CLDSRVHOST1", "collector-cloud.smartess.example,18899,TCP"),
            b"AT+CLDSRVHOST1:collector-cloud.smartess.example,18899,TCP\r\n",
        )

    def test_parse_at_command(self) -> None:
        query = parse_at_command("AT+CLDSRVHOST1?\r\n")
        write = parse_at_command("AT+CLDSRVHOST1=192.168.1.50,18899,TCP\r\n")

        self.assertEqual(
            query,
            CollectorAtCommand(
                command="CLDSRVHOST1",
                operation="query",
                value="",
                raw="AT+CLDSRVHOST1?",
            ),
        )
        self.assertEqual(write.command, "CLDSRVHOST1")
        self.assertEqual(write.operation, "write")
        self.assertEqual(write.value, "192.168.1.50,18899,TCP")

    def test_parse_at_response(self) -> None:
        parsed = parse_at_response(b"AT+WFSS:-56\r\n", expected_command="WFSS")

        self.assertEqual(parsed.command, "WFSS")
        self.assertEqual(parsed.value, "-56")
        self.assertEqual(parsed.raw, "AT+WFSS:-56")

    def test_parse_at_response_rejects_command_mismatch(self) -> None:
        with self.assertRaises(CollectorAtError):
            parse_at_response("AT+SYST:20260428132224", expected_command="WFSS")

    def test_stream_session_query_reads_one_response(self) -> None:
        async def _run() -> None:
            reader = asyncio.StreamReader()
            writer = _FakeWriter()
            session = CollectorAtStreamSession(reader, writer, timeout=1.0)

            reader.feed_data(b"AT+WFSS:-55\r\n")
            response = await session.query("WFSS")

            self.assertEqual(bytes(writer.buffer), b"AT+WFSS?\r\n")
            self.assertEqual(response.command, "WFSS")
            self.assertEqual(response.value, "-55")

        asyncio.run(_run())

    def test_runtime_query_decodes_valuecloud_endpoint_family(self) -> None:
        class _Transport:
            async def async_query(self, command: str):
                if command == "CLDSRVHOST1":
                    return parse_at_response("AT+CLDSRVHOST1:iot.eybond.com,18899,TCP")
                return parse_at_response(f"AT+{command}:")

        async def _run() -> None:
            values = await query_runtime_collector_at_values(_Transport())

            self.assertEqual(values["collector_server_endpoint"], "iot.eybond.com,18899,TCP")
            self.assertEqual(values["collector_cloud_family"], "valuecloud_at")
            self.assertEqual(values["collector_cloud_family_source"], "endpoint_host")
            self.assertEqual(values["collector_cloud_family_confidence"], "high")

        asyncio.run(_run())

    def test_runtime_query_never_sends_vdtu_after_factory_endpoint_family(self) -> None:
        class _Transport:
            commands: list[str]

            def __init__(self) -> None:
                self.commands = []

            async def async_query(self, command: str):
                self.commands.append(command)
                if command == "CLDSRVHOST1":
                    return parse_at_response("AT+CLDSRVHOST1:iot.eybond.com,18899,TCP")
                if command == "VDTU":
                    raise AssertionError("VDTU must not be queried")
                return parse_at_response(f"AT+{command}:")

        async def _run() -> None:
            transport = _Transport()
            values = await query_runtime_collector_at_values(transport)

            self.assertEqual(values["collector_cloud_family"], "valuecloud_at")
            self.assertNotIn("VDTU", transport.commands)

        asyncio.run(_run())

    def test_runtime_query_never_sends_vdtu_from_family_hint(self) -> None:
        class _Transport:
            commands: list[str]

            def __init__(self) -> None:
                self.commands = []

            async def async_query(self, command: str):
                self.commands.append(command)
                if command == "VDTU":
                    raise AssertionError("VDTU must not be queried")
                return parse_at_response(f"AT+{command}:")

        async def _run() -> None:
            transport = _Transport()
            values = await query_runtime_collector_at_values(
                transport,
                collector_cloud_family="valuecloud_at",
            )

            self.assertNotIn("VDTU", transport.commands)

        asyncio.run(_run())


class AtSweepTimeoutAbortTests(unittest.TestCase):
    def test_first_timeout_aborts_the_remaining_sweep(self) -> None:
        class _DeadLinkTransport:
            def __init__(self) -> None:
                self.commands: list[str] = []

            async def async_query(self, command: str):
                self.commands.append(command)
                raise asyncio.TimeoutError()

        async def _run() -> None:
            transport = _DeadLinkTransport()
            values = await query_runtime_collector_at_values(transport)
            self.assertEqual(values, {})
            # One strike ends the sweep instead of 12 consecutive timeouts.
            self.assertEqual(len(transport.commands), 1)

        asyncio.run(_run())

    def test_command_level_errors_do_not_abort_the_sweep(self) -> None:
        class _ErroringTransport:
            def __init__(self) -> None:
                self.commands: list[str] = []

            async def async_query(self, command: str):
                self.commands.append(command)
                raise ValueError("bad response")

        async def _run() -> None:
            transport = _ErroringTransport()
            await query_runtime_collector_at_values(transport)
            self.assertGreater(len(transport.commands), 1)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
