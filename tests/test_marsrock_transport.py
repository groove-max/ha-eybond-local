"""Replay the captured RTU payloads through real stream parsers and facades.

Only the raw payloads are field evidence. FC4 envelopes and silence below are
synthetic boundary cases, NOT proof that a MarsRock callback supports either.
The collector socket is injected; no hardware, endpoint or UART is modified.
"""

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE, build_collector_request, decode_header,
)
from custom_components.eybond_local.collector.transport import (
    SharedCollectorAtTransport, SharedEybondTransport,
    _CollectorAtConnection, _CollectorConnection,
)
from custom_components.eybond_local.drivers.marsrock import MarsRockDriver
from custom_components.eybond_local.link_models import RawSerialLinkRoute
from custom_components.eybond_local.payload.modbus import ModbusError, decode_read_request
from custom_components.eybond_local.runtime.driver_detection import async_detect_inverter


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures/marsrock_read_observation.json").read_text()
)


class ReadOnlyCollectorReplay:
    """Answer captured reads; reject any accidental configuration/inverter write."""

    def __init__(self, reader, *, forward_mode="raw", corrupt_crc=False):
        self.reader = reader
        self.forward_mode = forward_mode
        self.corrupt_crc = corrupt_crc
        self.cycle = 0
        self.closed = False
        self.requests = []

    def write(self, wire):
        raw = decode_read_request(wire)
        header = None if raw is not None else decode_header(wire[:HEADER_SIZE])
        payload = wire if raw is not None else wire[HEADER_SIZE:]
        if header is not None and header.fcode == 2:
            # The cloud capture contains this framed baud-rate query between
            # raw RTU sweeps; a management reply is not an FC4 confirmation.
            assert payload == b"\x22"
            response = b"\x00\x229600\r\n"
        else:
            assert raw is not None or header.fcode == 4
            request = decode_read_request(payload)
            assert request is not None and request.function_code == 3
            assert (request.address, request.count) in ((0, 20), (59, 66))
            if header is not None:
                assert (header.devcode, header.devaddr) == (1, 255)
            mode = "raw" if raw is not None else "fc4"
            self.requests.append((mode, payload))
            if self.forward_mode != mode:
                return
            row = next(r for r in FIXTURE["records"]
                       if r["cycle"] == self.cycle and r["request_hex"] == payload.hex())
            response = bytes.fromhex(row["response_hex"])
            if self.corrupt_crc:
                response = response[:-1] + bytes([response[-1] ^ 1])
        if header is not None:
            response = build_collector_request(
                header.tid, response, devcode=header.devcode,
                collector_addr=header.devaddr, fcode=header.fcode,
            )
        # Split before the length field, within the payload and inside CRC.
        self.reader.feed_data(response[:2])
        loop = asyncio.get_running_loop()
        for part in (response[2:7], response[7:-1], response[-1:]):
            loop.call_soon(self.reader.feed_data, part)

    async def drain(self):
        pass

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.closed = True

    def get_extra_info(self, name, default=None):
        return ("192.0.2.43", 40000) if name == "peername" else default


@asynccontextmanager
async def replay_stream(*, at=True, forward_mode="raw", corrupt_crc=False):
    reader = asyncio.StreamReader()
    writer = ReadOnlyCollectorReplay(reader, forward_mode=forward_mode, corrupt_crc=corrupt_crc)
    if at:
        connection = _CollectorAtConnection(
            remote_ip_hint="192.0.2.43", write_timeout=0.5,
            raw_passthrough_bootstrap="none",
        )
        transport = SharedCollectorAtTransport(
            host="127.0.0.1", port=18899, collector_ip="192.0.2.43", request_timeout=0.1,
        )
        transport.set_negotiated_wire("at_text")
        connection_method = "_at_connection"
    else:
        connection = _CollectorConnection(
            remote_ip_hint="192.0.2.43", heartbeat_interval=3600, write_timeout=0.5,
        )
        transport = SharedEybondTransport(
            host="127.0.0.1", port=8899, collector_ip="192.0.2.43",
            request_timeout=0.1, heartbeat_interval=3600,
        )
        connection_method = "_connection"
    run = asyncio.create_task(connection.run(reader, writer))
    try:
        assert await connection.wait_until_connected(1)
        with patch.object(transport, connection_method, return_value=connection):
            yield transport, connection, writer
    finally:
        await connection.disconnect()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)


class MarsRockTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_raw_stream_runs_detection_and_three_poll_cycles(self):
        async with replay_stream() as (transport, connection, replay):
            context = await async_detect_inverter(transport, driver_hint="marsrock_grid_tie")
            self.assertIsNotNone(context)
            self.assertEqual(context.inverter.serial_number, "9900000043")
            for cycle, grid in enumerate((235, 234.7, 233.9)):
                replay.cycle = cycle
                read = await context.driver.async_read_values(transport, context.inverter)
                self.assertEqual(read.values["grid_voltage"], grid)
                self.assertEqual(len(read.values), 4)
            self.assertEqual([mode for mode, _ in replay.requests], ["raw"] * 5)
            self.assertEqual(connection.collector_info.raw_response_count, 5)
            self.assertEqual(connection.collector_info.inverter_forward_mode, "raw_serial")

    async def test_fc2_management_does_not_pin_fc4_over_raw_only_data_plane(self):
        async with replay_stream() as (transport, connection, replay):
            await connection.async_send_bridge_identity_probe(
                fcode=2, payload=b"\x22", request_timeout=0.5,
            )
            self.assertTrue(connection.mixed_frame_observed)
            self.assertEqual(connection.collector_info.inverter_forward_mode, "")
            driver = MarsRockDriver()
            inverter = await driver.async_probe(transport, driver.probe_targets[0])
            self.assertIsNotNone(inverter)
            self.assertEqual([mode for mode, _ in replay.requests], ["fc4", "raw", "raw"])
            self.assertEqual(connection.collector_info.inverter_forward_mode, "raw_serial")

    async def test_synthetic_fc4_response_works_without_changing_payload_map(self):
        async with replay_stream(at=False, forward_mode="fc4") as (transport, _, replay):
            driver = MarsRockDriver()
            inverter = await driver.async_probe(transport, driver.probe_targets[0])
            self.assertIsNotNone(inverter)
            read = await driver.async_read_values(transport, inverter)
            self.assertEqual(read.values["grid_voltage"], 235)
            self.assertEqual([mode for mode, _ in replay.requests], ["fc4"] * 3)

    async def test_framed_management_alive_but_no_fc4_is_not_a_decoder_failure(self):
        async with replay_stream(at=False, forward_mode="raw") as (transport, connection, replay):
            async def management_read():
                _, response = await transport.async_send_collector(fcode=2, payload=b"\x22")
                self.assertEqual(response, b"\x00\x229600\r\n")

            await management_read()
            driver = MarsRockDriver()
            with patch("custom_components.eybond_local.drivers.marsrock.decode_marsrock_identity") as decode:
                self.assertIsNone(await driver.async_probe(transport, driver.probe_targets[0]))
                decode.assert_not_called()
            self.assertEqual(replay.requests, [("fc4", bytes.fromhex("01030000001445c5"))] * 2)
            self.assertTrue(connection.connected)
            self.assertEqual(connection.collector_info.raw_request_count, 0)
            await management_read()
            with self.assertRaises(TypeError):
                await transport.async_send_payload(
                    bytes.fromhex("01030000001445c5"), route=RawSerialLinkRoute(protocol="modbus_rtu"),
                )
            self.assertEqual(len(replay.requests), 2)

    async def test_corrupt_raw_crc_never_becomes_an_inverter_or_fresh_values(self):
        async with replay_stream() as (transport, _, replay):
            driver = MarsRockDriver()
            inverter = await driver.async_probe(transport, driver.probe_targets[0])
            self.assertIsNotNone(inverter)
            replay.corrupt_crc = True
            with self.assertRaises(ModbusError):
                await driver.async_read_values(transport, inverter)
            self.assertIsNone(await driver.async_probe(transport, driver.probe_targets[0]))
