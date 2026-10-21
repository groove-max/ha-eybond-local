"""Real coordinator dispatch/stop/recovery with no device or cloud IO."""
import asyncio
from dataclasses import replace
from pathlib import Path
import threading
from unittest.mock import AsyncMock

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("fault", [None, "echo_missing", "lost_response", "no_drain", "old_study", "disk", "cancelled", "restore_failure"])
@pytest.mark.parametrize("source,codec,framed", [
    (source, codec, framed) for source in ("smartess", "dessmonitor")
    for codec in ("modbus_rtu", "pi30_ascii", "eybond_g_ascii") for framed in (True, False)
] + [("smartess_refusal", "modbus_rtu", False)])
async def test_completed_stop_restores_only_fully_proven_study(dispatch_context, monkeypatch, fault, source, codec, framed):
    from tests.test_shadow_learning_completion_adapters import exchange
    from custom_components.eybond_local.support.shadow_learning.completion import CloudCommandCompletion
    from custom_components.eybond_local.support.shadow_learning.completion_store import CompletedLearningStore

    from custom_components.eybond_local.smartess_cloud import SmartEssActionRejectedError
    device_refusal = source == "smartess_refusal"
    if device_refusal:
        source = "smartess"
    ctx = dispatch_context; c = ctx.coordinator
    ctx.state = replace(ctx.state, cloud_source=source)
    c._protected_learning_run = replace(c._protected_learning_run, source=source)
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    book = CloudCommandCompletion(entry_id=ctx.state.entry_id,
        study_id=ctx.owner.removeprefix("shadow_learning:"), collector_pn=ctx.pn)
    monkeypatch.setattr(c._runtime, "shadow_learning_completion", lambda: book, raising=False)
    arguments, request, reply, envelope, carrier = exchange(source, codec, framed, pn=ctx.pn)
    if device_refusal:
        from custom_components.eybond_local.payload.modbus import build_write_multiple_request
        from custom_components.eybond_local.support.shadow_learning.protocol import ModbusRtuShadowLearningAdapter
        request = build_write_multiple_request(1, 300, [0])
        adapter = ModbusRtuShadowLearningAdapter()
        reply = adapter.build_write_exception_response(adapter.decode_write_request(request), exception_code=1)
    loop = asyncio.get_running_loop()
    drained = threading.Event()
    release_http = threading.Event()

    def observed():
        book.request(request, carrier=carrier, protocol=codec)
        if fault != "no_drain":
            book.replied(reply)
        drained.set()

    def http(**kwargs):
        assert kwargs == arguments
        loop.call_soon_threadsafe(observed)
        assert drained.wait(2)
        if fault == "cancelled":
            assert release_http.wait(2)
        if fault == "lost_response":
            raise TimeoutError("response lost")
        if device_refusal:
            raise SmartEssActionRejectedError(err=1,
                desc="ERR_FAIL" if fault == "echo_missing" else "ERR_FAIL(Read-Only Register)")
        return replace(envelope, dat={}) if fault == "echo_missing" else envelope

    if fault == "cancelled":
        task = asyncio.create_task(c.async_dispatch_shadow_learning_cloud_action(
            http, collector_pn=ctx.pn, route_owner_id=ctx.owner, **arguments))
        assert await asyncio.to_thread(drained.wait, 2)
        task.cancel(); release_http.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif fault == "lost_response":
        with pytest.raises(TimeoutError):
            await c.async_dispatch_shadow_learning_cloud_action(
                http, collector_pn=ctx.pn, route_owner_id=ctx.owner, **arguments)
    elif device_refusal:
        with pytest.raises(SmartEssActionRejectedError):
            await c.async_dispatch_shadow_learning_cloud_action(
                http, collector_pn=ctx.pn, route_owner_id=ctx.owner, **arguments)
    else:
        await c.async_dispatch_shadow_learning_cloud_action(
            http, collector_pn=ctx.pn, route_owner_id=ctx.owner, **arguments)
    if fault == "old_study":
        book.study_id = "previous_unconfirmed_study"
    if fault == "disk":
        monkeypatch.setattr(CompletedLearningStore, "record_completed",
                            lambda *a, **k: (_ for _ in ()).throw(OSError("fsync")))
    assert await c._async_cloud_learning_hold_reason()  # receipt alone isn't retirement

    async def stop(**_kwargs):
        assert book.closing
        book.closed = True

    monkeypatch.setattr(c._runtime, "async_stop_shadow_learning_route", stop)
    restore = AsyncMock(return_value=ctx.state.original_endpoint)
    verify = AsyncMock(return_value={"restore_confirmed": True, "observed_endpoint": ctx.state.original_endpoint})
    if fault == "restore_failure":
        restore.side_effect = OSError("endpoint offline")
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", restore)
    monkeypatch.setattr(c, "_async_verify_restored_collector_endpoint", verify)
    result = await c.async_stop_shadow_learning(request_refresh=False)
    if fault == "restore_failure":
        restore.assert_awaited_once()
        assert not await c._async_cloud_learning_hold_reason()
        current = await c._async_active_shadow_learning_state(require_process=False)
        assert current.status == "restore_failed" and not current.cloud_commands_possible
        assert not result.get("restore_confirmed")
    elif fault:
        assert result["status"] == "cloud_held"
        restore.assert_not_awaited()
        assert await c._async_cloud_learning_hold_reason()
    else:
        restore.assert_awaited_once_with(ctx.state.original_endpoint)
        verify.assert_awaited_once()
        assert not await c._async_cloud_learning_hold_reason()
        assert await c._async_active_shadow_learning_state(require_process=False) is None
        assert c._protected_learning_run is None
        store = CompletedLearningStore(Path(c.hass.config.config_dir))
        assert list((store._root / "completed_receipts").glob("*.json"))


@pytest.mark.parametrize("journal_corrected", [False, True])
async def test_restart_finishes_exact_completed_receipt_without_dispatch(dispatch_context, monkeypatch, journal_corrected):
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
    from custom_components.eybond_local.runtime.coordinator.learning_recovery import async_preserve_interrupted_learning_hold
    from custom_components.eybond_local.support.shadow_learning.completion_store import CompletedLearningStore
    from tests.test_shadow_learning_completion import ledger, deliver, close

    ctx = dispatch_context; c = ctx.coordinator
    state = replace(ctx.state, status="restoring", cloud_commands_possible=not journal_corrected)
    await c._async_save_shadow_learning_session_state(state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    store = CompletedLearningStore(Path(c.hass.config.config_dir))
    hold = CloudLearningHold(entry_id=state.entry_id, study_id=ctx.owner.removeprefix("shadow_learning:"),
                             collector_pn=ctx.pn, source="smartess", armed_at="2026-10-21T01:00:00Z")
    store.create(hold=hold)
    book = ledger(); deliver(book)
    store.record_completed(hold, close(book))
    assert await c._async_cloud_learning_hold_reason()
    c._protected_learning_run = None
    await async_preserve_interrupted_learning_hold(c)
    assert not await c._async_cloud_learning_hold_reason()
    current = await c._async_active_shadow_learning_state(require_process=False)
    assert current.status == "restoring" and not current.cloud_commands_possible
    assert c._protected_learning_run is None
