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
from tests.test_reviewed_learning_empty_retry import five_run_fixture
from tests.test_reviewed_learning_recovery import history_fixture, plan_for, approve


async def test_cold_setup_restores_five_run_history_without_review_or_sender_logs(dispatch_context, monkeypatch):
    """#94: the fourth retry remembers only the local address, not the original cloud one."""
    from custom_components.eybond_local.runtime.coordinator.cloud_tools import CoordinatorCloudToolsMixin
    ctx = dispatch_context
    c = ctx.coordinator
    root = Path(c.hass.config.config_dir)
    store, hold, states, _ = await c.hass.async_add_executor_job(
        lambda: history_fixture(root, retries=4, entry_id=c.config_entry.entry_id, pn=ctx.pn)
    )
    assert states[-1].original_endpoint == states[-1].proxy_endpoint and not states[-1].restore_required
    assert states[0].original_endpoint != states[-1].original_endpoint
    assert not store.receipt_present(states[-1])
    # Only a new coordinator sees the persisted state; never run an actual cloud action.
    monkeypatch.setattr(c, "_async_cancel_cloud_tool_preparation", AsyncMock())
    monkeypatch.setattr(c, "async_stop_shadow_learning", AsyncMock())
    ctx.authority.release(ctx.token.entry_id, ctx.token)
    restore = AsyncMock(return_value=states[0].original_endpoint)
    verify = AsyncMock(return_value={"restore_confirmed": True, "observed_endpoint": states[0].original_endpoint})
    monkeypatch.setattr(CoordinatorCloudToolsMixin, "_async_restore_proxy_capture_endpoint", restore)
    monkeypatch.setattr(CoordinatorCloudToolsMixin, "_async_verify_restored_collector_endpoint", verify)
    assert await c.hass.config_entries.async_reload(c.config_entry.entry_id)
    await c.hass.async_block_till_done()
    new = c.hass.config_entries.async_get_entry(c.config_entry.entry_id).runtime_data
    restore.assert_awaited_once_with(states[0].original_endpoint)
    verify.assert_awaited_once_with(states[0].original_endpoint)
    assert await new._async_active_shadow_learning_state(require_process=False) is None
    assert await c.hass.async_add_executor_job(store.load, hold.collector_pn) is None
    assert new._protected_learning_run is None
    assert not list((store._root / "completed_receipts").glob("*.json"))


@pytest.fixture
async def reviewed_context(dispatch_context, monkeypatch, request):
    ctx = dispatch_context
    c = ctx.coordinator
    def prepare():
        root = Path(c.hass.config.config_dir)
        scope = dict(entry_id=c.config_entry.entry_id, pn=ctx.pn)
        if getattr(request, "param", None) == "empty_retries":
            store, hold, states, _, proofs, counts = five_run_fixture(root, **scope)
            plan = store.prepare(collector_pn=ctx.pn, entry_id=hold.entry_id, study_id=hold.study_id,
                                 evidence_files=proofs, command_counts=counts)
        else:
            store, hold, states, evidence = history_fixture(root, **scope)
            plan = plan_for(store, hold, evidence)
        return store, hold, states, plan
    ctx.store, ctx.hold, ctx.history, ctx.plan = await c.hass.async_add_executor_job(prepare)
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


@pytest.mark.parametrize(("reviewed_context", "expected_runs"), [(None, 2), ("empty_retries", 5)], indirect=["reviewed_context"])
async def test_approved_retry_recovers_original_endpoint_through_normal_stop(reviewed_context, expected_runs):
    ctx = reviewed_context; c = ctx.coordinator
    assert await c._async_cloud_learning_hold_reason()
    await c.async_stop_shadow_learning(request_refresh=False)
    ctx.stop.assert_awaited_once_with(owner_id=ctx.history[-1].route_owner_id)
    ctx.restore.assert_awaited_once_with(ctx.history[0].original_endpoint)
    ctx.verify.assert_awaited_once()
    assert not await c._async_cloud_learning_hold_reason()
    assert await c._async_active_shadow_learning_state(require_process=False) is None
    assert c.data.values["cloud_learning_protection_release_method"] == "maintainer_reviewed_terminal_history"
    assert c.data.values["cloud_learning_protection_reviewed_run_count"] == expected_runs
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
    if fault == "receipt_missing":
        # No manual approval needed to return a proven original endpoint.
        assert result["status"] == "stopped"
        ctx.restore.assert_awaited_once_with(ctx.history[0].original_endpoint)
        assert c.data.values["cloud_learning_protection_release_method"] == "stopped_with_unconfirmed_delivery"
        return
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
