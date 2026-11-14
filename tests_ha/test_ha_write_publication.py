"""Control completion must wait for real HA publication, not a queued refresh.

The production management mixin and HA coordinator/debouncer are real. Only
the device and poll boundary are synthetic; no inverter commands are sent.
"""

import asyncio
import logging
from types import SimpleNamespace

import pytest
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from custom_components.eybond_local.runtime.coordinator.management import (
    CoordinatorManagementMixin,
)


class PublicationCoordinator(CoordinatorManagementMixin, DataUpdateCoordinator):
    def __init__(self, hass):
        super().__init__(hass, logging.getLogger(__name__), name="write-publication", config_entry=None)
        self._runtime_operation_lock = asyncio.Lock()
        self.device_value = 10
        self.writes = []
        self.write_error = None
        self.read_error = None
        self.pause_read = None
        self.published = []
        self.inverter = SimpleNamespace(
            get_capability=lambda key: SimpleNamespace(key=key),
            get_capability_preset=lambda key: SimpleNamespace(key=key),
        )
        self._runtime = SimpleNamespace(
            async_write_capability=self.write,
            async_apply_preset=self.preset,
        )

    def can_expose_capability(self, capability):
        return True

    def can_expose_preset(self, preset):
        return True

    async def write(self, key, value):
        assert self._runtime_operation_lock.locked()
        self.writes.append((key, value))
        if self.write_error is not None:
            raise self.write_error
        # This boundary represents a completed runtime write AND readback.
        self.device_value = value
        return value

    async def preset(self, key):
        await self.write(key, 20)
        return {"value": 20}

    async def _async_update_data(self):
        async with self._runtime_operation_lock:
            value = self.device_value
            if self.pause_read is not None:
                started, release = self.pause_read
                self.pause_read = None
                started.set()
                await release.wait()
            if self.read_error is not None:
                raise self.read_error
            return SimpleNamespace(inverter=self.inverter, current=value)


@pytest.fixture
async def coordinator(hass):
    coordinator = PublicationCoordinator(hass)
    remove = coordinator.async_add_listener(
        lambda: coordinator.published.append(coordinator.data.current)
    )
    await coordinator.async_request_refresh()
    assert coordinator.data.current == 10
    try:
        yield coordinator
    finally:
        remove()
        await coordinator.async_shutdown()


async def apply(coordinator, operation):
    if operation == "preset":
        return await coordinator.async_apply_preset("test_preset")
    return await coordinator.async_write_capability("test_current", 20)


@pytest.mark.parametrize("operation", ["write", "preset"])
async def test_success_publishes_before_return_during_debounce_cooldown(coordinator, operation):
    result = await apply(coordinator, operation)
    assert result == ({"value": 20} if operation == "preset" else 20)
    assert coordinator.device_value == coordinator.data.current == 20
    assert coordinator.published[-1] == 20
    assert len(coordinator.writes) == 1


@pytest.mark.parametrize("operation", ["write", "preset"])
async def test_success_waits_for_post_write_refresh(coordinator, operation):
    started, release = asyncio.Event(), asyncio.Event()
    coordinator.pause_read = (started, release)
    task = asyncio.create_task(apply(coordinator, operation))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not task.done()
        assert coordinator.device_value == 20
        assert coordinator.data.current == 10
        release.set()
        await task
        assert coordinator.data.current == 20
        assert len(coordinator.writes) == 1
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_in_flight_poll_cannot_overwrite_completed_write(coordinator):
    started, release = asyncio.Event(), asyncio.Event()
    coordinator.pause_read = (started, release)
    poll = asyncio.create_task(coordinator.async_refresh())
    await asyncio.wait_for(started.wait(), timeout=1)
    write = asyncio.create_task(apply(coordinator, "write"))
    try:
        await asyncio.sleep(0)
        assert not coordinator.writes
        release.set()
        await asyncio.gather(poll, write)
        assert coordinator.data.current == coordinator.published[-1] == 20
        assert len(coordinator.writes) == 1
    finally:
        release.set()
        for task in (poll, write):
            if not task.done():
                task.cancel()
        await asyncio.gather(poll, write, return_exceptions=True)


async def test_two_writes_publish_latest_observation_without_replaying(coordinator):
    await asyncio.gather(
        coordinator.async_write_capability("test_current", 20),
        coordinator.async_write_capability("test_current", 30),
    )
    assert coordinator.data.current == coordinator.device_value == 30
    assert coordinator.writes == [("test_current", 20), ("test_current", 30)]
    assert coordinator.published[-1] == 30


@pytest.mark.parametrize("error", ["nak", "write_not_confirmed", "write_response_unconfirmed:battery_type"])
async def test_failure_is_not_optimistically_published_or_retried(coordinator, error):
    coordinator.write_error = RuntimeError(error)
    with pytest.raises(RuntimeError, match=error):
        await apply(coordinator, "write")
    assert coordinator.device_value == coordinator.data.current == 10
    assert 20 not in coordinator.published
    assert len(coordinator.writes) == 1


async def test_post_write_refresh_error_marks_ha_data_unavailable(coordinator):
    coordinator.read_error = TimeoutError()
    assert await apply(coordinator, "write") == 20
    assert not coordinator.last_update_success
    assert coordinator.data.current == 10
    assert 20 not in coordinator.published
    assert len(coordinator.writes) == 1


async def test_cancellation_releases_locks_and_does_not_retry_write(coordinator):
    started, release = asyncio.Event(), asyncio.Event()
    coordinator.pause_read = (started, release)
    task = asyncio.create_task(apply(coordinator, "write"))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not coordinator._runtime_operation_lock.locked()
        assert coordinator.data.current == 10
        await coordinator.async_refresh()
        assert coordinator.data.current == 20
        assert len(coordinator.writes) == 1
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
