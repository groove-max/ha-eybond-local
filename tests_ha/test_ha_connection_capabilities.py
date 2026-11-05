"""Connection setup stays reachable before inverter recognition (#49)."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.data_entry_flow import FlowResultType

from test_ha_config_flow import collector_entry

from custom_components.eybond_local import _ENTRY_SCHEMA_VERSION
from custom_components.eybond_local.collector.capabilities import (
    collector_capability_profile_from_entry,
    collector_profile_entry_fields,
)


@pytest.mark.parametrize("kind", ["unknown", "factory_eybond", "esp_eybond_bridge"])
@pytest.mark.parametrize("loaded", [False, True])
async def test_connection_menu_does_not_confuse_unknown_with_local_only(
    hass, collector_entry, fake_runtime, kind, loaded,
):
    """Use the real HA flow manager with no inverter or hardware identity."""
    if kind != "unknown":
        # This menu test starts with a fully persisted known collector profile.
        # Partial bridge metadata triggers an unrelated startup entity reload.
        profile_fields = collector_profile_entry_fields(
            collector_capability_profile_from_entry({"collector_kind": kind}, {}),
        )
        hass.config_entries.async_update_entry(
            collector_entry,
            data={**collector_entry.data, **profile_fields},
            options={**collector_entry.options, **profile_fields},
        )
    if loaded:
        assert await hass.config_entries.async_setup(collector_entry.entry_id)
        await hass.async_block_till_done()
        coordinator = collector_entry.runtime_data
        coordinator.data = replace(
            coordinator.data, connected=True, inverter=None,
            values={"runtime_driver_state": "driver_unbound"},
        )
        # Viewing the connection form cannot grant factory capabilities.
        assert coordinator.collector_capabilities.collector_kind == kind
        assert coordinator.collector_capabilities.cloud_connection_supported is (
            kind == "factory_eybond"
        )

    before_data = dict(collector_entry.data)
    before_options = dict(collector_entry.options)
    options = hass.config_entries.options
    result = await options.async_init(collector_entry.entry_id)
    flow_id = result["flow_id"]
    try:
        assert result["type"] is FlowResultType.MENU
        if kind == "esp_eybond_bridge":
            assert "collector_endpoint" in result["menu_options"]
            assert "connection" not in result["menu_options"]
            # A stale/direct link cannot expose cloud modes on a known ESP.
            result = await options._progress[flow_id].async_step_connection()
            assert result["type"] is FlowResultType.MENU
            assert result["step_id"] == "init"
        else:
            assert "connection" in result["menu_options"]
            result = await options.async_configure(
                flow_id, {"next_step_id": "connection"},
            )
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] == "connection"
            # Choosing a different profile only stages the existing transaction.
            result = await options.async_configure(
                flow_id, {"connection_strategy": "inbound"},
            )
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] == "strategy_transition"
            result = await options.async_configure(flow_id, {
                "advertised_server_ip": "192.0.2.10",
                "advertised_tcp_port": 8899,
                "confirm_connection_strategy_risk": False,
            })
            assert result["step_id"] == "strategy_transition"
            assert result["errors"]["confirm_connection_strategy_risk"] == (
                "connection_strategy_risk_unconfirmed"
            )
            flow = options._progress[flow_id]
            assert flow._transition_task is None
        assert collector_entry.data == before_data
        assert collector_entry.options == before_options
    finally:
        options.async_abort(flow_id)
        if loaded:
            assert await hass.config_entries.async_unload(collector_entry.entry_id)
            assert collector_entry.state is ConfigEntryState.NOT_LOADED


class _BridgeLifecycle:
    """Own the test's refresh gate and observe actual HA reload tasks."""

    def __init__(self, hass, entry):
        self.hass = hass
        self.entry = entry
        self.refresh_started = asyncio.Event()
        self.allow_refresh = asyncio.Event()
        self.cleanup_started = asyncio.Event()
        # This future contains a task, never an exception-bearing result.
        self.reload_started = hass.loop.create_future()
        self.refresh_tasks = []
        self.reload_tasks = []
        self.reload_calls = []

    async def wait_for_reload(self):
        async with asyncio.timeout(5):
            task = await asyncio.shield(self.reload_started)
            if task.done():
                return task.result()
            return await asyncio.shield(task)

    async def cleanup(self, primary_error):
        self.allow_refresh.set()
        self.cleanup_started.set()
        errors = []

        def record(error):
            if error is primary_error or any(error is old for old in errors):
                return
            errors.append(error)

        async def attempt(awaitable):
            try:
                await awaitable
            except BaseException as error:
                record(error)

        await attempt(self.hass.async_block_till_done())
        coordinator = getattr(self.entry, "runtime_data", None)
        cancelled_reload = (
            isinstance(primary_error, asyncio.CancelledError)
            and self.reload_started.done()
            and not self.reload_started.cancelled()
            and self.reload_started.result().cancelled()
        )
        if not cancelled_reload and (self.reload_started.done() or getattr(
            coordinator, "_entity_platform_reload_requested", False,
        )):
            await attempt(self.wait_for_reload())
        for task in self.reload_tasks:
            if task.cancelled() and isinstance(primary_error, asyncio.CancelledError):
                continue  # The cancelled reload was already reported by the body.
            if not task.done():
                try:
                    async with asyncio.timeout(5):
                        await asyncio.shield(task)
                except BaseException as error:
                    record(error)
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass  # Cancellation requested above; the task is joined.
                    except BaseException as error:
                        record(error)
            else:
                try:
                    task.result()  # Retrieve failures even after an early assertion.
                except BaseException as error:
                    record(error)

        async def unload():
            # HA's public unload checks state before acquiring this lock. Hold
            # it first so cleanup cannot overlap an already-running unload.
            async with self.entry.setup_lock:
                if self.entry.state is not ConfigEntryState.NOT_LOADED:
                    assert self.entry.state.recoverable
                    assert await self.hass.config_entries.async_unload(
                        self.entry.entry_id, _lock=False,
                    )

        await attempt(unload())
        await attempt(self.hass.async_block_till_done())
        if not self.reload_started.done():
            self.reload_started.cancel()
        return errors


@asynccontextmanager
async def _bridge_profile_lifecycle(hass, entry, monkeypatch):
    from conftest import FakeRuntimeManager

    lifecycle = _BridgeLifecycle(hass, entry)
    original_refresh = FakeRuntimeManager.async_refresh
    original_reload = hass.config_entries.async_reload

    async def gated_refresh(self, *, poll_interval=None):
        lifecycle.refresh_tasks.append(asyncio.current_task())
        lifecycle.refresh_started.set()
        await lifecycle.allow_refresh.wait()
        return await original_refresh(self, poll_interval=poll_interval)

    async def tracked_reload(entry_id):
        if entry_id == entry.entry_id:
            task = asyncio.current_task()
            lifecycle.reload_calls.append(entry_id)
            lifecycle.reload_tasks.append(task)
            if not lifecycle.reload_started.done():
                lifecycle.reload_started.set_result(task)
        # Awaiting this task reports the real exception and its traceback,
        # including cancellation. There is no second error-result Future.
        return await original_reload(entry_id)

    with monkeypatch.context() as patch:
        patch.setattr(FakeRuntimeManager, "async_refresh", gated_refresh)
        patch.setattr(hass.config_entries, "async_reload", tracked_reload)
        try:
            yield lifecycle
        except BaseException as primary_error:
            errors = await lifecycle.cleanup(primary_error)
            if errors:
                raise BaseExceptionGroup(
                    "Test failed and lifecycle cleanup also failed",
                    [primary_error, *errors],
                ) from None
            raise
        else:
            errors = await lifecycle.cleanup(None)
            if errors:
                raise BaseExceptionGroup("Lifecycle cleanup failed", errors)


@pytest.mark.parametrize("partial_profile", [
    pytest.param({"collector_kind": "esp_eybond_bridge"}, id="kind-only"),
    pytest.param({"collector_virtual_bridge": True}, id="legacy-bridge-flag"),
])
async def test_partial_bridge_profile_normalizes_then_completes_reload(
    hass, collector_entry, fake_runtime, monkeypatch, partial_profile,
):
    """Partial bridge metadata survives migration, normalization and a real reload."""
    hass.config_entries.async_update_entry(
        collector_entry, data={**collector_entry.data, **partial_profile},
    )
    expected_profile = {
        "collector_kind": "esp_eybond_bridge",
        "collector_virtual_bridge": True,
        "collector_bridge_kind": "esp-collector",
    }
    assert not expected_profile.keys() <= collector_entry.data.keys()
    assert not expected_profile.keys() & collector_entry.options.keys()
    identity = (collector_entry.entry_id, collector_entry.unique_id)
    collector_pn = collector_entry.data["collector_pn"]
    async with _bridge_profile_lifecycle(hass, collector_entry, monkeypatch) as lifecycle:
        assert await hass.config_entries.async_setup(collector_entry.entry_id)
        async with asyncio.timeout(5):
            await lifecycle.refresh_started.wait()
        assert collector_entry.state is ConfigEntryState.LOADED
        assert len(fake_runtime) == 1
        initial_coordinator = collector_entry.runtime_data
        assert fake_runtime[0].stopped == 0

        # Finish the device refresh after platform setup. Wait for the actual
        # reload to return, including its replacement setup, before unloading.
        lifecycle.allow_refresh.set()
        assert await lifecycle.wait_for_reload()
        await hass.async_block_till_done()

        assert lifecycle.reload_calls == [collector_entry.entry_id]
        assert collector_entry.state is ConfigEntryState.LOADED
        assert collector_entry.version == _ENTRY_SCHEMA_VERSION
        assert collector_entry.runtime_data is not initial_coordinator
        assert (collector_entry.entry_id, collector_entry.unique_id) == identity
        assert collector_entry.data["collector_pn"] == collector_pn
        for fields in (collector_entry.data, collector_entry.options):
            for key, value in expected_profile.items():
                assert fields[key] == value
        assert collector_entry.runtime_data.collector_capabilities.ha_only_required
        assert not collector_entry.runtime_data.collector_capabilities.cloud_connection_supported
        assert len(fake_runtime) == 2
        assert [runtime.started for runtime in fake_runtime] == [1, 1]
        assert [runtime.stopped for runtime in fake_runtime] == [1, 0]

    assert collector_entry.state is ConfigEntryState.NOT_LOADED
    assert not hasattr(collector_entry, "runtime_data")
    assert [runtime.stopped for runtime in fake_runtime] == [1, 1]


def _assert_lifecycle_finished(lifecycle):
    assert lifecycle.allow_refresh.is_set()
    assert lifecycle.reload_started.done()
    assert all(task.done() for task in lifecycle.reload_tasks + lifecycle.refresh_tasks)


@pytest.mark.parametrize("failure_point", ["before-setup", "after-setup", "early-assertion"])
async def test_bridge_lifecycle_cleans_up_early_failure(
    hass, collector_entry, fake_runtime, monkeypatch, failure_point,
):
    hass.config_entries.async_update_entry(
        collector_entry,
        data={**collector_entry.data, "collector_kind": "esp_eybond_bridge"},
    )
    original_setup = hass.config_entries.async_setup
    failure = AssertionError("controlled initial lifecycle failure")

    async def setup(entry_id, *args, **kwargs):
        if entry_id == collector_entry.entry_id and failure_point == "before-setup":
            raise failure
        result = await original_setup(entry_id, *args, **kwargs)
        if entry_id == collector_entry.entry_id and failure_point == "after-setup" and len(fake_runtime) == 1:
            raise failure
        return result

    monkeypatch.setattr(hass.config_entries, "async_setup", setup)
    with pytest.raises(AssertionError) as caught:
        async with _bridge_profile_lifecycle(hass, collector_entry, monkeypatch) as lifecycle:
            assert await hass.config_entries.async_setup(collector_entry.entry_id)
            raise failure
    assert caught.value is failure
    _assert_lifecycle_finished(lifecycle)
    assert collector_entry.state is ConfigEntryState.NOT_LOADED
    assert all(runtime.stopped == 1 for runtime in fake_runtime)
    if failure_point != "before-setup":
        assert len(fake_runtime) == 2
        assert lifecycle.reload_calls == [collector_entry.entry_id]


@pytest.mark.parametrize("cancelled", [False, True], ids=["exception", "cancellation"])
async def test_bridge_lifecycle_reports_reload_failure(
    hass, collector_entry, monkeypatch, cancelled,
):
    import traceback

    failure = asyncio.CancelledError("controlled reload cancellation") if cancelled else ValueError("controlled reload failure")

    async def failed_reload(entry_id):
        raise failure

    monkeypatch.setattr(hass.config_entries, "async_reload", failed_reload)
    with pytest.raises(type(failure)) as caught:
        async with _bridge_profile_lifecycle(hass, collector_entry, monkeypatch) as lifecycle:
            hass.async_create_task(hass.config_entries.async_reload(collector_entry.entry_id))
            await lifecycle.wait_for_reload()
    if not cancelled:
        assert caught.value is failure
        assert "failed_reload" in [frame.name for frame in traceback.extract_tb(caught.value.__traceback__)]
    _assert_lifecycle_finished(lifecycle)
    assert lifecycle.reload_tasks[0].cancelled() is cancelled


async def test_bridge_lifecycle_preserves_primary_and_cleanup_errors(
    hass, collector_entry, monkeypatch,
):
    primary = AssertionError("controlled test failure")
    cleanup = RuntimeError("controlled cleanup failure")
    original_drain = hass.async_block_till_done
    calls = 0

    async def drain(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise cleanup
        await original_drain(*args, **kwargs)

    monkeypatch.setattr(hass, "async_block_till_done", drain)
    with pytest.raises(BaseExceptionGroup) as caught:
        async with _bridge_profile_lifecycle(hass, collector_entry, monkeypatch) as lifecycle:
            raise primary
    assert caught.value.exceptions == (primary, cleanup)
    _assert_lifecycle_finished(lifecycle)


async def test_bridge_lifecycle_joins_reload_after_waiter_cancellation(
    hass, collector_entry, fake_runtime, monkeypatch,
):
    hass.config_entries.async_update_entry(
        collector_entry,
        data={**collector_entry.data, "collector_kind": "esp_eybond_bridge"},
    )
    import custom_components.eybond_local as integration

    inside_unload = asyncio.Event()
    allow_unload = asyncio.Event()
    original_unload = integration.async_unload_entry
    unload_calls = 0
    lifecycle = None

    async def paused_unload(hass, entry):
        nonlocal unload_calls
        if entry is collector_entry:
            unload_calls += 1
            if unload_calls == 1:
                inside_unload.set()
                await allow_unload.wait()
        return await original_unload(hass, entry)

    monkeypatch.setattr(integration, "async_unload_entry", paused_unload)

    async def exercise():
        nonlocal lifecycle
        async with _bridge_profile_lifecycle(hass, collector_entry, monkeypatch) as lifecycle:
            assert await hass.config_entries.async_setup(collector_entry.entry_id)
            lifecycle.allow_refresh.set()
            await lifecycle.wait_for_reload()

    async with asyncio.TaskGroup() as tasks:
        task = tasks.create_task(exercise())
        try:
            async with asyncio.timeout(5):
                await inside_unload.wait()
            assert collector_entry.state is ConfigEntryState.UNLOAD_IN_PROGRESS
            task.cancel()
            async with asyncio.timeout(5):
                await lifecycle.cleanup_started.wait()
            # Cleanup waits for this unload instead of starting another one.
            assert unload_calls == 1
            assert collector_entry.state is ConfigEntryState.UNLOAD_IN_PROGRESS
            allow_unload.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            allow_unload.set()
    # TaskGroup also joins exercise() if a preceding assertion fails.
    _assert_lifecycle_finished(lifecycle)
    assert len(fake_runtime) == 2
    assert unload_calls == 2
    assert [runtime.stopped for runtime in fake_runtime] == [1, 1]
    assert collector_entry.state is ConfigEntryState.NOT_LOADED
