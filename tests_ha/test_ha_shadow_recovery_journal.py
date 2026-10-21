"""Actual HA persistence, cancellation and historical recovery receipts."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import threading

import pytest

from test_ha_config_flow import collector_entry


@pytest.fixture
async def recovery_context(hass, collector_entry, fake_runtime, tmp_path, monkeypatch):
    from custom_components.eybond_local.support.shadow_learning.session import build_shadow_learning_session_state

    monkeypatch.setattr(hass.config, "config_dir", str(tmp_path))
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    state = build_shadow_learning_session_state(
        entry_id=collector_entry.entry_id, collector_pn=coordinator.smartess_collector_pn,
        route_owner_id="shadow_learning:journal_test", trace_path="synthetic.jsonl",
        original_endpoint="cloud.invalid,18899,TCP", proxy_endpoint="192.0.2.1,8899,TCP",
        upstream_endpoint="cloud.invalid,18899,TCP", restore_required=True,
        started_at="2026-10-15T12:00:00Z", updated_at="2026-10-15T12:00:00Z", status="restore_failed",
    )
    await coordinator._async_save_shadow_learning_session_state(state)
    yield coordinator, state
    # The fake runtime cannot certify a cloud restore. Explicit synthetic cleanup
    # remains labelled discard, never endpoint_verified.
    await coordinator._async_clear_shadow_learning_session_state(outcome="explicit_discard")
    await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("worker_error", [False, True])
async def test_cancelled_save_drains_disk_worker_and_invalidates_cache(
    recovery_context, monkeypatch, worker_error,
):
    from custom_components.eybond_local.runtime.coordinator import cloud_tools
    from custom_components.eybond_local.support.shadow_learning.session import save_shadow_learning_session_state

    coordinator, state = recovery_context
    updated = replace(state, status="restoring")
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def delayed_save(**kwargs):
        started.set()
        assert release.wait(5)
        save_shadow_learning_session_state(**kwargs)
        finished.set()
        if worker_error:
            raise OSError("synthetic sync error after rename")

    monkeypatch.setattr(cloud_tools, "save_shadow_learning_session_state", delayed_save)
    task = asyncio.create_task(coordinator._async_save_shadow_learning_session_state(updated))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert finished.is_set()
    assert coordinator._shadow_learning_session_state_loaded is False
    assert await coordinator._async_active_shadow_learning_state(require_process=False) == updated


async def test_finalization_failure_preserves_state_and_has_no_success_receipt(recovery_context, monkeypatch):
    from custom_components.eybond_local.support.shadow_learning import session

    coordinator, state = recovery_context
    with monkeypatch.context() as scoped:
        def fail(*args, **kwargs):
            raise OSError("synthetic disk full")
        scoped.setattr(session, "atomic_write_record", fail)
        with pytest.raises(OSError):
            await coordinator._async_clear_shadow_learning_session_state(outcome="endpoint_verified")
    assert await coordinator._async_active_shadow_learning_state(require_process=False) == state
    diagnostics = await coordinator._async_shadow_learning_recovery_diagnostics()
    assert diagnostics["shadow_learning_recovery_disk_status"] == "current_entry"
    assert "shadow_learning_recovery_last_finalization_outcome" not in diagnostics


async def test_receipt_survives_clear_and_is_in_support_bundle(recovery_context):
    coordinator, _state = recovery_context
    await coordinator._async_clear_shadow_learning_session_state(outcome="endpoint_verified")
    diagnostics = await coordinator._async_shadow_learning_recovery_diagnostics()
    assert diagnostics["shadow_learning_recovery_disk_status"] == "missing"
    assert diagnostics["shadow_learning_recovery_last_finalization_outcome"] == "endpoint_verified"
    assert diagnostics["shadow_learning_recovery_cached_record_present"] is False
    bundle = coordinator._build_support_bundle_payload(shadow_learning_recovery=diagnostics)
    assert bundle["runtime"]["metadata"]["shadow_learning_recovery_last_finalization_outcome"] == "endpoint_verified"


async def test_foreign_legacy_journal_does_not_block_or_change_this_entry(recovery_context):
    from custom_components.eybond_local.support.shadow_learning import session

    coordinator, state = recovery_context
    root = Path(coordinator.hass.config.config_dir)
    foreign = replace(state, entry_id="other-entry", collector_pn="OTHER-COLLECTOR",
                      route_owner_id="shadow_learning:foreign-held-run")
    path = await coordinator.hass.async_add_executor_job(
        lambda: session.save_shadow_learning_session_state(config_dir=root, state=foreign),
    )
    before = await coordinator.hass.async_add_executor_job(path.read_bytes)
    coordinator._shadow_learning_session_state_loaded = False
    assert await coordinator._async_active_shadow_learning_state(require_process=False) == state
    await coordinator._async_save_shadow_learning_session_state(replace(state, status="restoring"))
    await coordinator._async_clear_shadow_learning_session_state(outcome="explicit_discard")
    assert await coordinator.hass.async_add_executor_job(path.read_bytes) == before
