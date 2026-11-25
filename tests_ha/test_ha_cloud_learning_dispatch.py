"""Actual coordinator preparation owns the HTTP worker; no real cloud writes."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
import threading
from unittest.mock import AsyncMock

import pytest

from test_ha_config_flow import collector_entry


@pytest.fixture
async def dispatch_context(hass, collector_entry, fake_runtime, monkeypatch, tmp_path):
    from custom_components.eybond_local.support.shadow_learning import completion
    monkeypatch.setattr(completion, "DISPATCH_FAILURE_QUIET_SECONDS", .01)
    from custom_components.eybond_local.connection import collector_endpoint_operation as operations
    from custom_components.eybond_local.support.shadow_learning.session import build_shadow_learning_session_state
    from custom_components.eybond_local.runtime.coordinator.learning_isolation import ProtectedLearningRun
    monkeypatch.setattr(hass.config, "config_dir", str(tmp_path))
    authority = operations.CollectorEndpointOperationAuthority()
    monkeypatch.setattr(operations, "COLLECTOR_ENDPOINT_OPERATION_AUTHORITY", authority)
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    pn = coordinator.smartess_collector_pn
    owner = "shadow_learning:test_run"
    token = authority.acquire(collector_entry.entry_id, operations.OPERATION_SHADOW_LEARNING, owner_ref=owner).token
    state = build_shadow_learning_session_state(
        entry_id=collector_entry.entry_id, collector_pn=pn, route_owner_id=owner,
        trace_path="synthetic.jsonl", original_endpoint="cloud.invalid,18899,TCP",
        proxy_endpoint="192.0.2.1,18899,TCP", upstream_endpoint="cloud.invalid,18899,TCP",
        restore_required=True, started_at="2026-10-15T12:00:00Z",
        updated_at="2026-10-15T12:00:00Z", status="ready",
        protection_version=1, cloud_source="smartess",
    )
    status = dict(running=True, collector_connected=True, collector_protocol_ingress=True,
                  collector_identity_verified=True, collector_identity_pn=pn,
                  route_protocol_activity=True, upstream_connected=True, ready=True, upstream_error="")
    monkeypatch.setattr(coordinator, "_async_active_shadow_learning_state", AsyncMock(return_value=state))
    # Keep the production coordinator projection: it must preserve the proxy's
    # live identity fields rather than accidentally dropping dispatch evidence.
    monkeypatch.setattr(coordinator._runtime, "shadow_learning_route_status", lambda: dict(status))
    route_owner = {"owner": owner}
    monkeypatch.setattr(coordinator._runtime, "shadow_learning_route_owned_by",
                        lambda **kw: kw == dict(entry_id=collector_entry.entry_id, owner_id=route_owner["owner"]), raising=False)
    context = SimpleNamespace(coordinator=coordinator, state=state, token=token,
                              authority=authority, pn=pn, owner=owner,
                              status=status, route_owner=route_owner)
    coordinator._protected_learning_run = ProtectedLearningRun(
        collector_entry.entry_id, owner, pn, state.proxy_endpoint,
        coordinator._connection_spec.tcp_port, "smartess",
    )
    yield context
    # Do not let synthetic ready state pretend to be a real device restore.
    monkeypatch.setattr(coordinator, "_async_active_shadow_learning_state", AsyncMock(return_value=None))
    authority.release(collector_entry.entry_id, token)
    await hass.config_entries.async_unload(collector_entry.entry_id)


async def dispatch(context, action, **kwargs):
    return await context.coordinator.async_dispatch_shadow_learning_cloud_action(
        action, collector_pn=context.pn, route_owner_id=context.owner, **kwargs,
    )


@pytest.mark.parametrize("change", ["missing_owner", "foreign_owner", "route", "pn", "expired", "stopping", "shutdown", "hold", "identity_missing", "identity_foreign"])
async def test_dispatch_rechecks_after_await_and_never_adopts_missing_owner(dispatch_context, monkeypatch, change):
    from custom_components.eybond_local.connection import collector_endpoint_operation as operations
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    ctx = dispatch_context
    c = ctx.coordinator
    if change in {"pn", "expired", "stopping"}:
        new = {"pn": dict(collector_pn="V000405SYN94677058"),
               "expired": dict(expires_at="2000-01-01T00:00:00Z"),
               "stopping": dict(status="restoring")}[change]
        monkeypatch.setattr(c, "_async_active_shadow_learning_state", AsyncMock(return_value=replace(ctx.state, **new)))

    async def disk_check():
        await asyncio.sleep(0)
        if change in {"missing_owner", "foreign_owner"}:
            assert ctx.authority.release(ctx.token.entry_id, ctx.token)
            if change == "foreign_owner":
                ctx.authority.acquire(ctx.token.entry_id, operations.OPERATION_SHADOW_LEARNING, owner_ref="new_study")
        elif change == "route":
            ctx.route_owner["owner"] = "new_study"
        elif change == "shutdown":
            c._shutdown_complete = True
        elif change == "hold":
            raise CloudLearningProtectionRequired("cloud_learning_hold_pending")
        elif change == "identity_missing":
            ctx.status["collector_identity_verified"] = False
        elif change == "identity_foreign":
            ctx.status["collector_identity_pn"] = "V000405SYN94677058"

    monkeypatch.setattr(c, "_async_require_cloud_learning_unheld", disk_check)
    def forbidden():
        pytest.fail("HTTP must not run after scope/hold changed")
    try:
        with pytest.raises(CloudLearningProtectionRequired):
            await dispatch(ctx, forbidden)
        if change == "missing_owner":
            assert not ctx.authority.is_held(ctx.token.entry_id)
    finally:
        c._shutdown_complete = False


@pytest.mark.parametrize("worker_error", [False, True])
@pytest.mark.parametrize("stop_kind", ["preparation", "shutdown", "unload"])
async def test_stop_shutdown_and_repeated_cancel_cannot_overtake_http_worker(
    dispatch_context, monkeypatch, worker_error, stop_kind,
):
    ctx = dispatch_context
    c = ctx.coordinator
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    stop_route = AsyncMock(return_value={"status": "not_running"})
    monkeypatch.setattr(c, "_async_stop_shadow_learning_once", stop_route)

    def action():
        started.set()
        assert release.wait(5)
        finished.set()
        if worker_error:
            raise TimeoutError("synthetic lost reply")
        return "ok"

    task = asyncio.create_task(dispatch(ctx, action))
    cancel = None
    try:
        assert await asyncio.to_thread(started.wait, 2)
        with pytest.raises(RuntimeError, match="collector_endpoint_operation_busy"):
            await c.async_stop_shadow_learning(request_refresh=False)
        stop_route.assert_not_awaited()
        cleanup = (
            c._async_cancel_cloud_tool_preparation() if stop_kind == "preparation"
            else c.async_shutdown() if stop_kind == "shutdown"
            else c.hass.config_entries.async_unload(c.config_entry.entry_id)
        )
        cancel = asyncio.create_task(cleanup)
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not cancel.done()
        assert not task.done()
        assert c._cloud_tool_preparation_task is task
        assert c._runtime_operation_lock.locked()
        assert c._runtime.stopped == 0
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        if cancel is not None:
            await cancel
    assert finished.is_set()
    assert c._cloud_tool_preparation_task is None
    assert not c._runtime_operation_lock.locked()
    assert ctx.authority.owns(ctx.token)
    if stop_kind == "preparation":
        await c.async_stop_shadow_learning(request_refresh=False)
    else:
        assert c._runtime.stopped == 1
    stop_route.assert_awaited_once()


async def test_dispatch_returns_result_and_keeps_study_authority(dispatch_context):
    ctx = dispatch_context
    def action(*, value):
        assert ctx.coordinator._runtime_operation_lock.locked()
        return value + 1
    assert await dispatch(ctx, action, value=6) == 7
    assert ctx.authority.owns(ctx.token)

async def test_cancellation_during_disk_check_does_not_dispatch(dispatch_context, monkeypatch):
    ctx = dispatch_context
    entered = asyncio.Event()
    async def check():
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(ctx.coordinator, "_async_require_cloud_learning_unheld", check)
    def forbidden():
        pytest.fail("Cancelled pre-dispatch check must not launch HTTP")
    task = asyncio.create_task(dispatch(ctx, forbidden))
    try:
        await asyncio.wait_for(entered.wait(), 1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert ctx.coordinator._cloud_tool_preparation_task is None
    assert ctx.authority.owns(ctx.token)
