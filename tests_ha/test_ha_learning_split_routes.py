"""Real HA loop + loopback sockets: #62's two routes never forward writes."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.modbus import build_read_request, build_write_single_request
from custom_components.eybond_local.support.shadow_learning.backend import build_shadow_learning_seed
from custom_components.eybond_local.support.shadow_learning.proxy import (
    InProcessFailClosedShadowProxyHandler, route_status_indicates_control_write_ready,
)


@pytest.mark.allow_hosts(["127.0.0.1"])
async def test_0925_split_routes_prepare_and_intercept_without_device_writes(hass, tmp_path, monkeypatch, socket_enabled):
    device = DetectedInverter(
        "smartess_local", "smartess_local", "Synthetic 0925", "", ProbeTarget(1, 5, 5),
        variant_key="smartess_0925", profile_name="smartess_local/models/0925.json",
        register_schema_name="smartess_local/models/0925.json",
    )
    seed, blockers = build_shadow_learning_seed(
        session_id="synthetic-split", entry_id="synthetic", collector_pn="E5000020000000",
        collector_cloud_profile_key="", collector_cloud_profile_label="",
        collector_cloud_profile_source="", collector_cloud_profile_confidence="",
        collector_callback_endpoint="127.0.0.1,8899,TCP", collector_cloud_family="legacy_binary",
        effective_metadata_snapshot={"driver_key": "smartess_local", "profile_name": device.profile_name,
                                     "register_schema_name": device.register_schema_name},
        raw_capture={"driver_key": "smartess_local",
                     "probe_target": {"devcode": 1, "collector_addr": 5, "device_addr": 5},
                     "captured_ranges": [{"start": 4501, "count": 14, "collector_addr": 255,
                                          "words": [0] * 14}],
                     "fixture_ranges": [{"start": 4501, "values": [0] * 14}]},
        identified_inverter=device, inverter_target=(1, 5), inverter_device_addr=5,
        inspect_cloud_protocol=True,
    )
    assert not blockers
    connections = asyncio.Queue()
    release = asyncio.Event()

    async def cloud(reader, writer):
        await connections.put((reader, writer))
        await release.wait()
        writer.close()
        await writer.wait_closed()

    monkeypatch.setattr("custom_components.eybond_local.support.shadow_learning.proxy.async_verify_shadow_socket",
                        AsyncMock(return_value=("synthetic_verified_pn", b"")))
    cloud_server = await asyncio.start_server(cloud, "127.0.0.1", 0)
    handler = InProcessFailClosedShadowProxyHandler(
        seed=seed, upstream_host="127.0.0.1", upstream_port=cloud_server.sockets[0].getsockname()[1],
        output_path=tmp_path / "split.jsonl", expected_session_protocol="eybond_framed",
    )
    await handler.start()
    server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
    device_writer = None
    try:
        device_reader, device_writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        reader, writer = await asyncio.wait_for(connections.get(), 1)
        writer.write(build_collector_request(1, build_read_request(5, 4501, 45),
                                            devcode=1, collector_addr=255, fcode=4))
        await writer.drain()
        assert await asyncio.wait_for(reader.read(512), 1)
        assert route_status_indicates_control_write_ready(handler.status())
        assert handler.status()["protocol_diagnostics"]["captured_inverter_targets"] == ((1, 255),)
        # The config write belongs to the primary route. It is observed and
        # rejected locally, not sent to the physical collector.
        writer.write(build_collector_request(2, build_write_single_request(5, 5004, 0),
                                            devcode=1, collector_addr=5, fcode=4))
        await writer.drain()
        assert await asyncio.wait_for(reader.read(512), 1)
        assert len(handler._backend.observations_since(0)) == 1
        try:
            await asyncio.wait_for(device_reader.read(1), .05)
        except TimeoutError:
            pass
        else:
            raise AssertionError("cloud inverter traffic escaped the shadow")
        # A later read on an unproved unit stops readiness, even after success.
        writer.write(build_collector_request(3, build_read_request(6, 4501, 45),
                                            devcode=1, collector_addr=255, fcode=4))
        await writer.drain()
        async with asyncio.timeout(1):
            while not handler.status()["protocol_preparation_error"]:
                await asyncio.sleep(.005)
        assert handler.status()["protocol_preparation_error"] == "cloud_protocol_route_mismatch"
        assert not route_status_indicates_control_write_ready(handler.status())
    finally:
        release.set()
        if device_writer is not None:
            device_writer.close()
            await device_writer.wait_closed()
        server.close()
        cloud_server.close()
        await server.wait_closed()
        await cloud_server.wait_closed()
        await handler.stop()
