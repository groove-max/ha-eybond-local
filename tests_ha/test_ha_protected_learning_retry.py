"""Real HA coordinator + journals/authority; fake only device/network boundaries."""

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, PropertyMock
from pathlib import Path

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context
from test_ha_config_flow import collector_entry


@pytest.fixture
async def retry_context(dispatch_context, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator import learning_retry as retry
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    from custom_components.eybond_local.telemetry import TelemetryFreshness, TelemetryPoint, TypedTelemetryFrame

    ctx = dispatch_context
    c = ctx.coordinator
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    monkeypatch.setattr(retry, "AUTHORITY", ctx.authority)
    target = f"{c._effective_callback_server_host},{c._connection_spec.effective_advertised_tcp_port},TCP"
    ctx.previous = replace(ctx.state, status="restore_failed", cloud_commands_possible=True,
                           proxy_endpoint=target, last_restore_error="cloud_learning_hold_pending")
    await c._async_save_shadow_learning_session_state(ctx.previous)
    ctx.store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    ctx.hold = CloudLearningHold(c.config_entry.entry_id, "test_run", ctx.pn, "smartess", ctx.state.started_at)
    await c.hass.async_add_executor_job(lambda: ctx.store.create(hold=ctx.hold))
    monkeypatch.setattr(type(c), "collector_actions_enabled", PropertyMock(return_value=True))
    readiness = c.support_acquisition_readiness
    monkeypatch.setattr(type(c), "support_acquisition_readiness", PropertyMock(
        return_value=replace(readiness, active_control_learning=replace(
            readiness.active_control_learning, visible=True, can_start=True, blocker=""))))
    monkeypatch.setattr(c, "_shadow_learning_process_running", lambda: ctx.running)
    monkeypatch.setattr(c._runtime, "proxy_capture_route_running", lambda: False, raising=False)
    monkeypatch.setattr(c, "_async_preflight_proxy_capture_network", AsyncMock())
    monkeypatch.setattr(c, "_async_disconnect_collector_for_cloud_tool_route", AsyncMock())
    monkeypatch.setattr(c, "_async_wait_for_shadow_learning_ready", AsyncMock())
    monkeypatch.setattr(retry, "read_available_memory_mib", lambda: 4096)
    monkeypatch.setattr(retry, "build_shadow_learning_seed", lambda **kw: (SimpleNamespace(write_response_mode="exception"), ()))
    monkeypatch.setattr(retry, "build_shadow_learning_preflight", lambda seed: SimpleNamespace(blockers=()))
    ctx.cert = SimpleNamespace(collector_pn=ctx.pn)
    ctx.registry = SimpleNamespace(
        claimed_session_id=lambda entry: "",
        pin_owner_claim_to_current_observed_session=Mock(return_value="owned"),
        certify_permanent_owned_session=lambda entry, sid: ctx.cert if sid == "owned" else None,
        session_handle_for_owned_session=lambda *args: SimpleNamespace(listener_port=c._connection_spec.tcp_port),
        reverify_permanent_owned_session=Mock(return_value=True))
    monkeypatch.setattr(retry, "get_callback_session_registry", lambda hass: ctx.registry)
    ctx.endpoint = {"current_endpoint": target}
    monkeypatch.setattr(c._runtime, "async_get_collector_server_endpoint_state", AsyncMock(side_effect=lambda **kw: ctx.endpoint))
    ctx.snapshot = SimpleNamespace(connected=True, last_error="", inverter=object(), telemetry=TypedTelemetryFrame(
        "modbus_smg", (TelemetryPoint("battery_voltage", 52.0, TelemetryFreshness.FRESH),)))
    monkeypatch.setattr(c._runtime, "async_refresh", AsyncMock(side_effect=lambda: ctx.snapshot))
    ctx.running = False
    ctx.starts = []
    async def start(**kw):
        # Exercise exact single-use permission even with fake transport.
        await kw["protected_retry_permit"].consume(**{k:kw[k] for k in (
            "entry_id", "owner_id", "collector_pn", "listen_port", "upstream_host", "upstream_port")})
        ctx.starts.append(kw)
        ctx.running = True
        ctx.route_owner["owner"] = kw["owner_id"]
    async def stop(**kw):
        if ctx.running:
            assert kw["owner_id"] == ctx.route_owner["owner"]
        ctx.running = False
    monkeypatch.setattr(c._runtime, "async_start_shadow_learning_route", start, raising=False)
    monkeypatch.setattr(c._runtime, "async_stop_shadow_learning_route", stop, raising=False)
    ctx.start = lambda: c.async_retry_shadow_learning(confirm_cloud_outage=True, cloud_source="dessmonitor")
    ctx.restore = AsyncMock(return_value=ctx.previous.original_endpoint)
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", ctx.restore)
    monkeypatch.setattr(c, "_async_verify_restored_collector_endpoint", AsyncMock(return_value={"restore_confirmed": True}))
    return ctx


async def test_retry_keeps_hold_changes_owner_and_rejects_old_dispatch(retry_context):
    ctx = retry_context
    c = ctx.coordinator
    assert await c.async_shadow_learning_retry_available()
    result = await ctx.start()
    assert result["protected_retry"]
    ctx.registry.pin_owner_claim_to_current_observed_session.assert_called_once_with(c.config_entry.entry_id)
    assert result["session_id"] != ctx.owner
    assert not ctx.authority.owns(ctx.token)
    assert c._protected_learning_run.hold == ctx.hold
    assert await c.hass.async_add_executor_job(ctx.store.load, ctx.pn) == ctx.hold
    http = Mock(return_value="ok")
    with pytest.raises(RuntimeError):
        await c.async_dispatch_shadow_learning_cloud_action(http, collector_pn=ctx.pn, route_owner_id=ctx.owner)
    http.assert_not_called()
    assert await c.async_dispatch_shadow_learning_cloud_action(http, collector_pn=ctx.pn,
        route_owner_id=result["session_id"]) == "ok"
    stopped = await c.async_stop_shadow_learning(request_refresh=False)
    assert stopped["status"] == "stopped"
    assert not ctx.running
    ctx.restore.assert_awaited_once_with(ctx.previous.original_endpoint)
    assert not await c.async_shadow_learning_retry_available()
    assert await c.hass.async_add_executor_job(ctx.store.load, ctx.pn) is None


@pytest.mark.parametrize("change", ["endpoint", "identity", "no_trusted_session", "pending_reset", "stale_payload", "changed_after_capture", "foreign_owner", "consent"])
async def test_retry_rejects_unproven_context_without_opening_cloud(retry_context, change):
    ctx = retry_context
    if change == "endpoint": ctx.endpoint["current_endpoint"] = "cloud.invalid,18899,TCP"
    elif change == "identity": ctx.cert.collector_pn = "V000405SYN94677058"
    elif change == "no_trusted_session": ctx.registry.pin_owner_claim_to_current_observed_session.return_value = ""
    elif change == "pending_reset": ctx.endpoint["reboot_required"] = True
    elif change == "stale_payload": ctx.snapshot.connected = False
    elif change == "changed_after_capture":
        # The own-bench collector replaced a malformed framed session while
        # gathering support evidence. A valid earlier proof cannot authorize
        # the successor socket or a new cloud command/journal.
        ctx.registry.reverify_permanent_owned_session.side_effect = [True, False]
    elif change == "foreign_owner":
        ctx.authority.release(ctx.token.entry_id, ctx.token)
        ctx.authority.acquire(ctx.token.entry_id, "proxy_capture", owner_ref="foreign")
    with pytest.raises(RuntimeError):
        if change == "consent":
            await ctx.coordinator.async_retry_shadow_learning(confirm_cloud_outage=False, cloud_source="smartess")
        else:
            await ctx.start()
    assert not ctx.starts
    assert await ctx.coordinator._async_active_shadow_learning_state(require_process=False) == ctx.previous
    assert await ctx.coordinator.hass.async_add_executor_job(ctx.store.load, ctx.pn) == ctx.hold


async def test_retry_ready_failure_restores_original_endpoint(retry_context, monkeypatch):
    ctx = retry_context
    monkeypatch.setattr(ctx.coordinator, "_async_wait_for_shadow_learning_ready", AsyncMock(side_effect=TimeoutError()))
    with pytest.raises(TimeoutError):
        await ctx.start()
    state = await ctx.coordinator._async_active_shadow_learning_state(require_process=False)
    assert state is None
    ctx.restore.assert_awaited_once_with(ctx.previous.original_endpoint)
    assert not ctx.running
    assert await ctx.coordinator.hass.async_add_executor_job(ctx.store.load, ctx.pn) is None


async def test_retry_request_failure_cannot_retire_original_hold_as_no_send(retry_context, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator import learning_dispatch

    ctx = retry_context
    result = await ctx.start()
    no_send = AsyncMock(return_value=True)
    monkeypatch.setattr(learning_dispatch, "async_confirm_owned_action_not_sent", no_send)
    with pytest.raises(TimeoutError):
        await ctx.coordinator.async_dispatch_shadow_learning_cloud_action(
            Mock(side_effect=TimeoutError("synthetic connect timeout")), collector_pn=ctx.pn,
            route_owner_id=result["session_id"],
        )
    no_send.assert_not_awaited()
    assert await ctx.coordinator.hass.async_add_executor_job(ctx.store.load, ctx.pn) == ctx.hold
    assert (await ctx.coordinator.async_stop_shadow_learning(request_refresh=False))["status"] == "stopped"
    ctx.restore.assert_awaited_once_with(ctx.previous.original_endpoint)


async def test_repeated_cancel_during_journal_rollover_waits_and_keeps_new_owner(retry_context, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator import learning_retry as retry
    ctx = retry_context
    entered, release = threading.Event(), threading.Event()
    original = retry.replace_held_shadow_learning_session
    def delayed(**kw):
        entered.set()
        assert release.wait(5)
        return original(**kw)
    monkeypatch.setattr(retry, "replace_held_shadow_learning_session", delayed)
    task = asyncio.create_task(ctx.start())
    try:
        async with asyncio.timeout(5):
            while not entered.is_set(): await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError): await task
    state = await ctx.coordinator._async_active_shadow_learning_state(require_process=False)
    assert state is None
    ctx.restore.assert_awaited_once_with(ctx.previous.original_endpoint)
    assert not ctx.authority.owns(ctx.token)
    assert not ctx.starts
    assert await ctx.coordinator.hass.async_add_executor_job(ctx.store.load, ctx.pn) is None


async def test_directory_sync_error_after_journal_replace_does_not_orphan_owner(retry_context, monkeypatch):
    from custom_components.eybond_local.support.shadow_learning import session
    ctx = retry_context
    original = session._save_shadow_learning_session_state
    def after_replace(**kw):
        result = original(**kw)
        if kw["state"].status == "preflight": raise OSError("synthetic directory sync failure")
        return result
    monkeypatch.setattr(session, "_save_shadow_learning_session_state", after_replace)
    with pytest.raises(OSError): await ctx.start()
    state = await ctx.coordinator._async_active_shadow_learning_state(require_process=False)
    assert state.status == "restore_failed" and state.route_owner_id != ctx.owner
    assert not ctx.authority.owns(ctx.token)
    assert ctx.authority.adopt(state.entry_id, "shadow_learning", state.route_owner_id) is not None
    assert not ctx.starts
