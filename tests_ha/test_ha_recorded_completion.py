"""All 146 saved PI30-local/Modbus-cloud commands through proxy and HA stop.

Only the provider, collector peer and endpoint IO are simulated. Never use this
offline receipt to release an actual collector's historical protection hold.
"""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("last_echo_missing", [False, True])
async def test_recorded_146_command_proxy_and_retirement(dispatch_context, monkeypatch, tmp_path, socket_enabled, last_echo_missing):
    from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
    from custom_components.eybond_local.smartess_cloud import ApiEnvelope, build_device_control_action
    from custom_components.eybond_local.support.shadow_learning.backend import ShadowLearningSeed
    from custom_components.eybond_local.support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler
    from custom_components.eybond_local.support.shadow_learning.completion_store import CompletedLearningStore

    data = json.loads((Path(__file__).parents[1]/"tests/fixtures/smartess_0925_recorded_learning.json").read_text())
    rows = [dict(zip(data["attempt_columns"], row, strict=True)) for row in data["attempts"]]
    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    bank = {block["start"] + index: value for block in data["register_ranges"]
            for index, value in enumerate(block["values"])}
    seed = ShadowLearningSeed(
        entry_id=ctx.state.entry_id, session_id=ctx.owner.removeprefix("shadow_learning:"),
        collector_pn=ctx.pn, collector_cloud_profile_key="eybond_ascii_pn_v1",
        collector_cloud_profile_label="Recorded Modbus", collector_cloud_profile_source="fixture",
        collector_cloud_profile_confidence="high", collector_callback_endpoint=ctx.state.proxy_endpoint,
        effective_metadata_snapshot={"effective_owner_key": "smartess_local",
            "profile_name": data["source_profile"], "register_schema_name": data["source_schema"]},
        command_responses={}, register_bank=bank, collector_cloud_family="legacy_binary",
    )
    queue = asyncio.Queue()
    cloud_done = asyncio.Event()
    cloud_errors = []

    async def cloud(reader, writer):
        try:
            await reader.readexactly(12)
            while (job := await queue.get()) is not None:
                item, future = job
                expected = bytes.fromhex(item["api_response_frame_hex"])
                header = decode_header(expected)
                request = build_collector_request(header.tid, bytes.fromhex(item["write_payload_hex"]),
                    devcode=header.devcode, collector_addr=header.devaddr, fcode=4)
                writer.write(request)
                await writer.drain()
                response = await reader.readexactly(len(expected))
                if response != expected:
                    future.set_exception(AssertionError("recorded_refusal_mismatch"))
                else:
                    future.set_result(response)
            await reader.read()
        except Exception as exc:
            cloud_errors.append(exc)
        finally:
            writer.close()
            await writer.wait_closed()
            cloud_done.set()

    loop = asyncio.get_running_loop()

    async def send(item):
        future = loop.create_future()
        await queue.put((item, future))
        return await future

    server = await asyncio.start_server(cloud, "127.0.0.1", 0)
    handler = InProcessFailClosedShadowProxyHandler(upstream_host="127.0.0.1",
        upstream_port=server.sockets[0].getsockname()[1], seed=seed, output_path=tmp_path/"replay.jsonl")
    await handler.start()
    proxy = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
    writer.write(build_collector_request(1, b"ping", devcode=1, collector_addr=5, fcode=1))
    await writer.drain()
    # Wait for the simulated connection to own its ledger before HTTP dispatch.
    async with asyncio.timeout(3):
        while not handler.upstream_connected:
            await asyncio.sleep(0.01)
    monkeypatch.setattr(c._runtime, "shadow_learning_completion", lambda: handler.completion, raising=False)

    async def stop(**_kwargs):
        await queue.put(None)
        await handler.stop()

    monkeypatch.setattr(c._runtime, "async_stop_shadow_learning_route", stop)
    restore = AsyncMock(return_value=ctx.state.original_endpoint)
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", restore)
    monkeypatch.setattr(c, "_async_verify_restored_collector_endpoint", AsyncMock(return_value={
        "restore_confirmed": True, "observed_endpoint": ctx.state.original_endpoint}))
    try:
        for index, item in enumerate(rows):
            action = build_device_control_action(pn=ctx.pn, sn="offline", devcode=1, devaddr=5,
                field_id=item["field_id"], value=item["requested_value"])

            def http(**kwargs):
                assert kwargs["action"] == action
                response = asyncio.run_coroutine_threadsafe(send(item), loop).result(timeout=5)
                dat = {"dat": response.hex(" "), "status": 0}
                if last_echo_missing and index == len(rows)-1:
                    dat.pop("dat")
                return ApiEnvelope(err=0, desc="ERR_NONE", dat=dat, raw={})

            await c.async_dispatch_shadow_learning_cloud_action(http,
                collector_pn=ctx.pn, route_owner_id=ctx.owner, action=action)
        assert len(handler.backend.write_observations) == 146
        assert handler.backend._register_bank == bank
        assert await c._async_cloud_learning_hold_reason()
        result = await c.async_stop_shadow_learning(request_refresh=False)
        assert await asyncio.wait_for(reader.read(), 3) == b""  # no inverter writes
        await asyncio.wait_for(cloud_done.wait(), 3)
        assert not cloud_errors
        if last_echo_missing:
            assert result["status"] == "cloud_held"
            assert len(handler.completion.commands) == 145
            assert await c._async_cloud_learning_hold_reason()
            restore.assert_not_awaited()
        else:
            assert len(handler.completion.completed_commands()) == 146
            assert not await c._async_cloud_learning_hold_reason()
            restore.assert_awaited_once_with(ctx.state.original_endpoint)
            store = CompletedLearningStore(Path(c.hass.config.config_dir))
            receipt = json.loads(next((store._root/"completed_receipts").glob("*.json")).read_text())
            assert len(receipt["commands"]) == 146
            assert all(command["protocol"] == "modbus_rtu" for command in receipt["commands"])
    finally:
        await queue.put(None)
        await handler.stop()
        writer.close(); await writer.wait_closed()
        proxy.close(); await proxy.wait_closed()
        server.close(); await server.wait_closed()
        await asyncio.wait_for(cloud_done.wait(), 3)
