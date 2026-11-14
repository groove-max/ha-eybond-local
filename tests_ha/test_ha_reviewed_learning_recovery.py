"""Historical approval -> real coordinator stop -> original endpoint recovery.

Only collector/cloud boundaries are simulated. No live study ledger is forged.
"""
import asyncio
from pathlib import Path
import threading
from unittest.mock import AsyncMock

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context
from test_ha_config_flow import collector_entry
from tests.test_reviewed_learning_recovery import history_fixture, plan_for, approve


@pytest.fixture
async def reviewed_context(dispatch_context, monkeypatch):
    ctx = dispatch_context
    c = ctx.coordinator
    ctx.store, ctx.hold, ctx.history, evidence = await c.hass.async_add_executor_job(
        lambda: history_fixture(Path(c.hass.config.config_dir), entry_id=c.config_entry.entry_id, pn=ctx.pn))
    ctx.plan = await c.hass.async_add_executor_job(lambda: plan_for(ctx.store, ctx.hold, evidence))
    await c.hass.async_add_executor_job(lambda: approve(ctx.store, ctx.plan))
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    c._shadow_learning_session_state_loaded = False
    c._protected_learning_run = None
    ctx.authority.release(ctx.token.entry_id, ctx.token)
    ctx.token = ctx.authority.adopt(ctx.token.entry_id, "shadow_learning", ctx.history[-1].route_owner_id)
    ctx.running = False
    monkeypatch.setattr(c, "_shadow_learning_process_running", lambda: ctx.running)
    monkeypatch.setattr(c._runtime, "shadow_learning_completion", lambda: None, raising=False)
    ctx.stop = AsyncMock()
    ctx.restore = AsyncMock(return_value=ctx.history[0].original_endpoint)
    ctx.verify = AsyncMock(return_value={"restore_confirmed": True, "observed_endpoint": ctx.history[0].original_endpoint})
    monkeypatch.setattr(c._runtime, "async_stop_shadow_learning_route", ctx.stop)
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", ctx.restore)
    monkeypatch.setattr(c, "_async_verify_restored_collector_endpoint", ctx.verify)
    return ctx


async def test_approved_retry_recovers_original_endpoint_through_normal_stop(reviewed_context):
    ctx = reviewed_context; c = ctx.coordinator
    assert await c._async_cloud_learning_hold_reason()
    await c.async_stop_shadow_learning(request_refresh=False)
    ctx.stop.assert_awaited_once_with(owner_id=ctx.history[-1].route_owner_id)
    ctx.restore.assert_awaited_once_with(ctx.history[0].original_endpoint)
    ctx.verify.assert_awaited_once()
    assert not await c._async_cloud_learning_hold_reason()
    assert await c._async_active_shadow_learning_state(require_process=False) is None
    assert c.data.values["cloud_learning_protection_release_method"] == "maintainer_reviewed_terminal_history"
    assert c.data.values["cloud_learning_protection_reviewed_run_count"] == 2
    assert c.data.values["cloud_learning_protection_reviewed_command_count"] == 180


@pytest.mark.parametrize("fault", ["receipt_missing", "trace_changed", "foreign_owner", "running", "receipt_corrupt"])
async def test_changed_review_never_restores_cloud(reviewed_context, fault):
    ctx = reviewed_context; c = ctx.coordinator
    if fault == "receipt_missing":
        await c.hass.async_add_executor_job(ctx.store._review_path(ctx.history[-1]).unlink)
    elif fault == "receipt_corrupt":
        await c.hass.async_add_executor_job(ctx.store._review_path(ctx.history[-1]).write_text, "{}")
    elif fault == "trace_changed":
        await c.hass.async_add_executor_job(Path(ctx.history[0].trace_path).write_text, "new evidence")
    elif fault == "running":
        ctx.running = True
    elif fault == "foreign_owner":
        ctx.authority.release(ctx.token.entry_id, ctx.token)
        foreign = ctx.authority.acquire(ctx.token.entry_id, "proxy_capture", owner_ref="foreign").token
        try:
            with pytest.raises(RuntimeError, match="operation_busy"):
                await c.async_stop_shadow_learning(request_refresh=False)
        finally:
            ctx.authority.release(foreign.entry_id, foreign)
        ctx.restore.assert_not_awaited()
        return
    result = await c.async_stop_shadow_learning(request_refresh=False)
    assert result["status"] == "cloud_held"
    ctx.restore.assert_not_awaited()
    assert await c._async_cloud_learning_hold_reason()
    assert await c.hass.async_add_executor_job(ctx.store.load, ctx.pn) == ctx.hold


@pytest.mark.parametrize("after_archive", [False, True])
async def test_partial_migration_retries_original_not_local_endpoint(reviewed_context, monkeypatch, after_archive):
    from custom_components.eybond_local.support.shadow_learning.reviewed_recovery import ReviewedLearningRecoveryStore
    ctx = reviewed_context; c = ctx.coordinator
    archive = ReviewedLearningRecoveryStore._archive_hold
    def fail(self, hold):
        if after_archive: archive(self, hold)
        raise OSError("after journal or archive rename")
    with monkeypatch.context() as m:
        m.setattr(ReviewedLearningRecoveryStore, "_archive_hold", fail)
        result = await c.async_stop_shadow_learning(request_refresh=False)
    assert result["status"] == "cloud_held"
    ctx.restore.assert_not_awaited()
    current = await c._async_active_shadow_learning_state(require_process=False)
    assert current.original_endpoint == ctx.history[0].original_endpoint
    assert current.restore_required and not current.cloud_commands_possible
    await c.async_stop_shadow_learning(request_refresh=False)
    ctx.restore.assert_awaited_once_with(ctx.history[0].original_endpoint)
    assert not await c._async_cloud_learning_hold_reason()


async def test_failed_endpoint_verification_remains_recoverable(reviewed_context):
    ctx = reviewed_context; c = ctx.coordinator
    ctx.verify.return_value = {"restore_confirmed": False}
    await c.async_stop_shadow_learning(request_refresh=False)
    state = await c._async_active_shadow_learning_state(require_process=False)
    assert state.status == "restore_failed" and state.restore_required
    assert state.original_endpoint == ctx.history[0].original_endpoint
    assert not state.cloud_commands_possible
    assert not await c._async_cloud_learning_hold_reason()
    ctx.verify.return_value = {"restore_confirmed": True, "observed_endpoint": state.original_endpoint}
    await c.async_stop_shadow_learning(request_refresh=False)
    assert await c._async_active_shadow_learning_state(require_process=False) is None


async def test_cancel_waits_for_migration_then_leaves_original_restore_obligation(reviewed_context, monkeypatch):
    from custom_components.eybond_local.support.shadow_learning.reviewed_recovery import ReviewedLearningRecoveryStore
    ctx = reviewed_context; c = ctx.coordinator
    entered, release = threading.Event(), threading.Event()
    apply = ReviewedLearningRecoveryStore.apply
    def delayed(self, **kwargs):
        entered.set()
        assert release.wait(5)
        return apply(self, **kwargs)
    monkeypatch.setattr(ReviewedLearningRecoveryStore, "apply", delayed)
    task = asyncio.create_task(c.async_stop_shadow_learning(request_refresh=False))
    try:
        async with asyncio.timeout(5):
            while not entered.is_set(): await asyncio.sleep(.01)
        task.cancel(); await asyncio.sleep(.01); task.cancel(); await asyncio.sleep(.01)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError): await task
    ctx.restore.assert_not_awaited()
    state = await c._async_active_shadow_learning_state(require_process=False)
    assert state.original_endpoint == ctx.history[0].original_endpoint and state.restore_required
    assert not state.cloud_commands_possible
    await c.async_stop_shadow_learning(request_refresh=False)
    ctx.restore.assert_awaited_once_with(ctx.history[0].original_endpoint)
