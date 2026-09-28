"""Real coordinator refresh/unload around the cloud-tool preparation barrier."""
from __future__ import annotations

import asyncio

import pytest

from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("kind", ["proxy", "shadow"])
@pytest.mark.parametrize("cancel_unload", [False, True])
async def test_poll_drains_then_unload_waits_for_start_cleanup(
    hass, collector_entry, fake_runtime, monkeypatch, kind, cancel_unload,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    runtime = coordinator._runtime
    poll_entered, poll_release = asyncio.Event(), asyncio.Event()
    prepared, restoring, restored = asyncio.Event(), asyncio.Event(), asyncio.Event()
    snapshot = coordinator.data
    calls = []

    async def refresh(**kwargs):
        calls.append("poll")
        poll_entered.set()
        await poll_release.wait()
        return snapshot

    async def prepare(**kwargs):
        # Device/route I/O is represented by barriers. The coordinator wrapper,
        # refresh and HA entry unload are real. Full route rollback is unit-tested.
        assert coordinator._runtime_operation_lock.locked()
        prepared.set()
        try:
            await asyncio.Future()
        finally:
            async def cleanup():
                restoring.set()
                await restored.wait()
                assert runtime.stopped == 0
            await coordinator._run_finalization_shielded(cleanup)

    monkeypatch.setattr(runtime, "async_refresh", refresh)
    method = "proxy_capture" if kind == "proxy" else "shadow_learning"
    monkeypatch.setattr(coordinator, f"_async_start_{method}_exclusive", prepare)
    tasks = []
    try:
        poll = asyncio.create_task(coordinator._async_update_data())
        tasks.append(poll)
        await asyncio.wait_for(poll_entered.wait(), 1)
        start = asyncio.create_task(getattr(coordinator, f"async_start_{method}")())
        tasks.append(start)
        for _ in range(10):
            if coordinator._cloud_tool_preparation_task is not None:
                break
            await asyncio.sleep(0)
        assert coordinator._cloud_tool_preparation_task is start
        assert not prepared.is_set()
        assert await asyncio.wait_for(coordinator._async_update_data(), 1) is snapshot
        poll_release.set()
        await poll
        await asyncio.wait_for(prepared.wait(), 1)
        await asyncio.wait_for(coordinator.async_refresh(), 1)
        assert calls == ["poll"]
        with pytest.raises(RuntimeError, match="collector_endpoint_operation_busy"):
            await coordinator.async_start_proxy_capture()
        # Exercise ordinary entry unload, and cancellation of the coordinator's
        # teardown separately. Cancelling HA's entry state machine itself leaves
        # its entry in UNLOAD_IN_PROGRESS (a core lifecycle constraint).
        unload = asyncio.create_task(
            coordinator.async_shutdown() if cancel_unload
            else hass.config_entries.async_unload(collector_entry.entry_id)
        )
        tasks.append(unload)
        await asyncio.wait_for(restoring.wait(), 2)
        if cancel_unload:
            unload.cancel()
            await asyncio.sleep(0)
            unload.cancel()
        await asyncio.sleep(0)
        assert not unload.done()
        assert runtime.stopped == 0
        assert coordinator._runtime_operation_lock.locked()
        restored.set()
        with pytest.raises(asyncio.CancelledError):
            await start
        if cancel_unload:
            with pytest.raises(asyncio.CancelledError):
                await unload
        else:
            assert await unload
        assert runtime.stopped == 1
        assert coordinator._cloud_tool_preparation_task is None
        assert not coordinator._runtime_operation_lock.locked()
        with pytest.raises(RuntimeError, match="coordinator_stopped"):
            await coordinator.async_start_proxy_capture()
    finally:
        poll_release.set()
        restored.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await hass.config_entries.async_unload(collector_entry.entry_id)
