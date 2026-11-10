"""Unload must not wait indefinitely behind its own active runtime poll."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from custom_components.eybond_local.runtime.coordinator import lifecycle
from test_ha_setup_unload import _collector_entry


async def test_unload_drains_owned_poll_before_endpoint_finalization(hass, fake_runtime, monkeypatch):
    entry = _collector_entry(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    # Force the deadline branch deterministically, without a ten-second sleep.
    monkeypatch.setattr(lifecycle, "_RUNTIME_POLL_SHUTDOWN_GRACE_SECONDS", 0)
    entered = asyncio.Event()
    release = asyncio.Event()
    cancelled = asyncio.Event()
    finalizing = asyncio.Event()
    finish_caller = asyncio.Event()
    original_refresh = coordinator._runtime.async_refresh
    original_stop = coordinator.async_stop_shadow_learning

    async def blocked_refresh(*, poll_interval=None):
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return await original_refresh(poll_interval=poll_interval)

    async def observe_stop(**kwargs):
        finalizing.set()
        assert cancelled.is_set(), "shutdown entered route cleanup while its poll still owned the runtime lock"
        assert not coordinator._runtime_operation_lock.locked()
        return await original_stop(**kwargs)

    monkeypatch.setattr(coordinator._runtime, "async_refresh", blocked_refresh)
    # Wrapper records failures because shutdown deliberately logs cloud-tool cleanup errors.
    observed = AsyncMock(side_effect=observe_stop)
    monkeypatch.setattr(coordinator, "async_stop_shadow_learning", observed)
    async def caller():
        try:
            await coordinator.async_refresh()
        except asyncio.CancelledError:
            # A service caller may handle cancellation and keep working.
            await finish_caller.wait()

    refresh_task = asyncio.create_task(caller())
    unload_task = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        unload_task = asyncio.create_task(hass.config_entries.async_unload(entry.entry_id))
        await asyncio.wait_for(finalizing.wait(), 5)
        assert cancelled.is_set()
        assert await asyncio.wait_for(unload_task, 5)
        assert not refresh_task.done()
        finish_caller.set()
        await asyncio.gather(refresh_task, return_exceptions=True)
        assert coordinator._runtime_poll_task is None
        assert coordinator._runtime_poll_done is None
        assert coordinator._runtime.stopped == 1
    finally:
        release.set()
        finish_caller.set()
        await asyncio.gather(refresh_task, return_exceptions=True)
        if unload_task is not None:
            await asyncio.gather(unload_task, return_exceptions=True)


async def test_unload_allows_regular_poll_to_finish_during_grace(hass, fake_runtime, monkeypatch):
    entry = _collector_entry(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    entered, release, draining = asyncio.Event(), asyncio.Event(), asyncio.Event()
    after_poll, finish_caller = asyncio.Event(), asyncio.Event()
    was_cancelled = False
    snapshot = coordinator.data
    original_drain = coordinator._async_drain_runtime_poll

    async def read(**kwargs):
        nonlocal was_cancelled
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            was_cancelled = True
            raise
        return snapshot

    async def drain(**kwargs):
        draining.set()
        await original_drain(**kwargs)

    monkeypatch.setattr(coordinator._runtime, "async_refresh", read)
    monkeypatch.setattr(coordinator, "_async_drain_runtime_poll", drain)
    async def caller():
        await coordinator.async_refresh()
        after_poll.set()
        await finish_caller.wait()

    poll = asyncio.create_task(caller())
    unload = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        unload = asyncio.create_task(hass.config_entries.async_unload(entry.entry_id))
        await asyncio.wait_for(draining.wait(), 5)
        assert not poll.done()
        release.set()
        assert await asyncio.wait_for(unload, 5)
        # The caller outlives the poll and must not be joined or cancelled.
        assert after_poll.is_set()
        assert not poll.done()
        finish_caller.set()
        await poll
        assert not was_cancelled
        assert coordinator._runtime.stopped == 1
    finally:
        release.set()
        finish_caller.set()
        await asyncio.gather(*[task for task in (poll, unload) if task], return_exceptions=True)


async def test_cancelled_shutdown_joins_poll_cleanup_before_stopping_link(hass, fake_runtime, monkeypatch):
    entry = _collector_entry(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    monkeypatch.setattr(lifecycle, "_RUNTIME_POLL_SHUTDOWN_GRACE_SECONDS", 0)
    entered, cleaning, cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def read(**kwargs):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await cleaned.wait()

    monkeypatch.setattr(coordinator._runtime, "async_refresh", read)
    poll = asyncio.create_task(coordinator.async_refresh())
    shutdown = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        shutdown = asyncio.create_task(coordinator.async_shutdown())
        await asyncio.wait_for(cleaning.wait(), 5)
        shutdown.cancel()
        await asyncio.sleep(0)
        shutdown.cancel()
        assert coordinator._runtime.stopped == 0
        assert not shutdown.done()
        cleaned.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(shutdown, 5)
        await asyncio.gather(poll, return_exceptions=True)
        assert coordinator._runtime_poll_task is None
        assert coordinator._runtime.stopped == 1
        assert not coordinator._runtime_operation_lock.locked()
    finally:
        cleaned.set()
        if not poll.done():
            poll.cancel()
        await asyncio.gather(*[task for task in (poll, shutdown) if task], return_exceptions=True)
        assert await hass.config_entries.async_unload(entry.entry_id)
