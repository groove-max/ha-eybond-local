"""Protected command transaction: real HA coordinator, fake HTTP/device only."""

import asyncio
import threading
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context, dispatch
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("source", ["smartess", "dessmonitor", "valuecloud"])
async def test_first_http_has_durable_intent_and_hold_and_later_commands_reuse_it(dispatch_context, monkeypatch, source):
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    from custom_components.eybond_local.support.shadow_learning.session import load_shadow_learning_session_state
    ctx = dispatch_context
    c = ctx.coordinator
    state = replace(ctx.state, cloud_source=source)
    c._protected_learning_run = replace(c._protected_learning_run, source=source)
    await c._async_save_shadow_learning_session_state(state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    root = Path(c.hass.config.config_dir)
    store = CloudLearningProtectionStore(root)
    seen = []

    def http():
        persisted = load_shadow_learning_session_state(root, entry_id=c.config_entry.entry_id)
        assert persisted.cloud_commands_possible is True
        hold = store.load(ctx.pn)
        assert hold.source == source
        assert hold.entry_id == ctx.token.entry_id
        seen.append(hold)
        return "ok"

    assert await dispatch(ctx, http) == "ok"
    assert await dispatch(ctx, http) == "ok"
    assert seen[0] == seen[1]


@pytest.mark.parametrize("failure", ["intent", "hold", "after_hold", "lost_response"])
async def test_persistence_and_ownership_failures_never_escape_to_http(dispatch_context, monkeypatch, failure):
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    ctx = dispatch_context
    c = ctx.coordinator
    calls = []

    def http():
        calls.append(True)
        raise TimeoutError("lost HTTP response")

    if failure == "intent":
        monkeypatch.setattr(c, "_async_save_shadow_learning_session_state", AsyncMock(side_effect=OSError("disk full")))
    elif failure == "hold":
        monkeypatch.setattr(CloudLearningProtectionStore, "create", lambda *a, **kw: (_ for _ in ()).throw(OSError("fsync")))
    elif failure == "after_hold":
        original = CloudLearningProtectionStore.async_create
        async def create(self, **kw):
            await original(self, **kw)
            ctx.status["collector_identity_verified"] = False
        monkeypatch.setattr(CloudLearningProtectionStore, "async_create", create)
    with pytest.raises((OSError, CloudLearningProtectionRequired, TimeoutError)):
        await dispatch(ctx, http)
    assert len(calls) == (1 if failure == "lost_response" else 0)
    if failure in {"after_hold", "lost_response"}:
        assert await c._async_cloud_learning_hold_reason()


async def test_held_stop_force_clear_reload_keep_local_route_and_forbid_resuming(dispatch_context, monkeypatch):
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    await dispatch(ctx, lambda: "accepted")
    stop = AsyncMock()
    restore = AsyncMock(side_effect=AssertionError("must not restore cloud"))
    monkeypatch.setattr(c._runtime, "async_stop_shadow_learning_route", stop)
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", restore)
    result = await c.async_stop_shadow_learning(clear_failed_restore=True, request_refresh=False)
    assert result["status"] == "cloud_held"
    assert c._protected_learning_run is None
    stop.assert_awaited_once()
    restore.assert_not_awaited()
    await c.async_request_refresh()  # local runtime remains usable with the hold
    state = await c._async_active_shadow_learning_state(require_process=False)
    assert state.cloud_commands_possible and state.protection_version == 1
    assert await c.hass.config_entries.async_reload(ctx.token.entry_id)
    await c.hass.async_block_till_done()
    new = c.hass.config_entries.async_get_entry(ctx.token.entry_id).runtime_data
    assert await new._async_cloud_learning_hold_reason()
    with pytest.raises(CloudLearningProtectionRequired):
        await new.async_dispatch_shadow_learning_cloud_action(
            lambda: pytest.fail("resumed HTTP after restart"), collector_pn=ctx.pn, route_owner_id=ctx.owner,
        )
    await new.async_request_refresh()


@pytest.mark.parametrize("legacy", [False, True])
async def test_interrupted_journal_recovers_only_blocking_authority(dispatch_context, monkeypatch, legacy):
    from custom_components.eybond_local.runtime.coordinator.learning_recovery import async_preserve_interrupted_learning_hold
    ctx = dispatch_context
    c = ctx.coordinator
    state = replace(ctx.state, protection_version=0 if legacy else 1,
                    cloud_commands_possible=not legacy)
    await c._async_save_shadow_learning_session_state(state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    c._protected_learning_run = None
    await async_preserve_interrupted_learning_hold(c)
    assert await c._async_cloud_learning_hold_reason()
    assert c._protected_learning_run is None


async def test_no_command_intent_does_not_quarantine_new_preparation(dispatch_context, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator.learning_recovery import async_preserve_interrupted_learning_hold
    ctx = dispatch_context
    await ctx.coordinator._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(ctx.coordinator, "_async_active_shadow_learning_state")
    await async_preserve_interrupted_learning_hold(ctx.coordinator)
    assert not await ctx.coordinator._async_cloud_learning_hold_reason()


async def test_cancel_after_durable_intent_drains_worker_and_stop_quarantines(dispatch_context, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator import cloud_tools
    from custom_components.eybond_local.support.shadow_learning.session import save_shadow_learning_session_state
    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    written, release = threading.Event(), threading.Event()
    calls = []
    def save(**kw):
        result = save_shadow_learning_session_state(**kw)
        if kw["state"].cloud_commands_possible and kw["state"].status == "ready":
            written.set()
            assert release.wait(5)
        return result
    monkeypatch.setattr(cloud_tools, "save_shadow_learning_session_state", save)
    task = asyncio.create_task(dispatch(ctx, lambda: calls.append(True)))
    for _ in range(200):
        if written.is_set():
            break
        await asyncio.sleep(.005)
    assert written.is_set()
    task.cancel()
    await asyncio.sleep(.01)
    task.cancel()
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not calls
    restore = AsyncMock(side_effect=AssertionError("uncertain intent must not restore cloud"))
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", restore)
    result = await c.async_stop_shadow_learning(request_refresh=False)
    assert result["status"] == "cloud_held"
    restore.assert_not_awaited()


@pytest.mark.parametrize("changed", ["missing", "corrupt"])
async def test_each_command_rechecks_independent_hold(dispatch_context, monkeypatch, changed):
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    ctx = dispatch_context
    await ctx.coordinator._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(ctx.coordinator, "_async_active_shadow_learning_state")
    await dispatch(ctx, lambda: "first")
    store = CloudLearningProtectionStore(Path(ctx.coordinator.hass.config.config_dir))
    path = store._path(ctx.pn)
    if changed == "missing":
        await ctx.coordinator.hass.async_add_executor_job(path.unlink)
    else:
        await ctx.coordinator.hass.async_add_executor_job(path.write_text, "{broken")
    with pytest.raises(CloudLearningProtectionRequired):
        await dispatch(ctx, lambda: pytest.fail("second HTTP escaped without intact hold"))


@pytest.mark.parametrize("same_pn", [False, True])
@pytest.mark.parametrize("scoped", [False, True])
async def test_readded_entry_cannot_hide_legacy_intent_for_same_collector(dispatch_context, monkeypatch, same_pn, scoped):
    from custom_components.eybond_local.runtime.coordinator.learning_recovery import async_preserve_interrupted_learning_hold
    from custom_components.eybond_local.support.shadow_learning.session import save_shadow_learning_session_state
    ctx = dispatch_context
    c = ctx.coordinator
    old = replace(ctx.state, entry_id="old_removed_entry", protection_version=0,
                  collector_pn=ctx.pn if same_pn else "E5000099990002")
    await c.hass.async_add_executor_job(lambda: save_shadow_learning_session_state(
        config_dir=Path(c.hass.config.config_dir), state=old, entry_id=old.entry_id if scoped else "",
    ))
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    c._shadow_learning_session_state_loaded = False
    await async_preserve_interrupted_learning_hold(c)
    assert bool(await c._async_cloud_learning_hold_reason()) is same_pn
