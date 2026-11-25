"""Real HA lifecycle with seeded durable holds; no cloud/device commands.

These tests qualify hold consumption; the transaction suite qualifies arming.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from test_ha_config_flow import collector_entry


@pytest.fixture(autouse=True)
def isolated_config_dir(hass, tmp_path, monkeypatch):
    from custom_components.eybond_local.support.shadow_learning import completion
    monkeypatch.setattr(completion, "DISPATCH_FAILURE_QUIET_SECONDS", .01)
    monkeypatch.setattr(hass.config, "config_dir", str(tmp_path))


async def seed_hold(hass, entry):
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    store = CloudLearningProtectionStore(Path(hass.config.config_dir))
    hold = CloudLearningHold(
        entry_id=entry.entry_id, study_id="seeded_study", collector_pn=entry.data["collector_pn"],
        source="smartess", armed_at="2026-10-15T12:00:00Z",
    )
    await store.async_create(hold=hold)
    return store, hold


async def test_held_options_menu_allows_read_only_but_not_active_learning(
    hass, collector_entry, fake_runtime,
):
    store, hold = await seed_hold(hass, collector_entry)
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    options = hass.config_entries.options
    result = await options.async_init(collector_entry.entry_id)
    flow_id = result["flow_id"]
    result = await options.async_configure(flow_id, {"next_step_id": "cloud_tools"})
    assert result["step_id"] == "cloud_tools"
    result = await options.async_configure(flow_id, {"next_step_id": "shadow_learning"})
    if result["step_id"] == "shadow_learning":
        result = await options.async_configure(flow_id, {"learning_method": "read_only_evidence"})
    assert result["step_id"] == "shadow_learning_source"
    result = await options.async_configure(flow_id, {"learning_source": "smartess"})
    assert result["step_id"] == "shadow_learning_credentials"
    options.async_abort(flow_id)
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    # Entering read-only analysis gives no permission to start an active route.
    with pytest.raises(CloudLearningProtectionRequired):
        await collector_entry.runtime_data.async_start_shadow_learning()
    assert await hass.async_add_executor_job(store.load, hold.collector_pn) == hold
    await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("language,marker", [("en", "Do not reconnect"), ("ru", "Не подключайте"), ("uk", "Не підключайте")])
async def test_persistent_notice_is_localized_and_dismissal_is_not_release(
    hass, collector_entry, fake_runtime, monkeypatch, language, marker,
):
    from homeassistant.components import persistent_notification
    store, hold = await seed_hold(hass, collector_entry)
    monkeypatch.setattr(hass.config, "language", language)
    create = Mock(wraps=persistent_notification.async_create)
    monkeypatch.setattr(persistent_notification, "async_create", create)
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    calls = [call for call in create.call_args_list if "cloud_learning_hold" in call.kwargs.get("notification_id", "")]
    assert len(calls) == 1
    assert marker in calls[0].args[1]
    persistent_notification.async_dismiss(hass, calls[0].kwargs["notification_id"])
    c = collector_entry.runtime_data
    assert await c.async_shadow_learning_start_blocker() == "cloud_learning_protection_held"
    assert await hass.async_add_executor_job(store.load, hold.collector_pn) == hold
    await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("prior_status", ["ready", "restore_failed", None])
async def test_hold_survives_real_setup_stop_force_clear_and_reload(
    hass, collector_entry, fake_runtime, monkeypatch, prior_status,
):
    from custom_components.eybond_local.connection import collector_endpoint_operation as authority
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    from custom_components.eybond_local.runtime.coordinator.cloud_tools import CoordinatorCloudToolsMixin
    from custom_components.eybond_local.support.shadow_learning.session import (
        build_shadow_learning_session_state, load_shadow_learning_session_state,
        save_shadow_learning_session_state,
    )
    monkeypatch.setattr(authority, "COLLECTOR_ENDPOINT_OPERATION_AUTHORITY", authority.CollectorEndpointOperationAuthority())
    restore = AsyncMock(side_effect=AssertionError("held lifecycle must never reach restoration"))
    monkeypatch.setattr(CoordinatorCloudToolsMixin, "_async_restore_proxy_capture_endpoint", restore)
    store, hold = await seed_hold(hass, collector_entry)
    config_dir = Path(hass.config.config_dir)
    if prior_status:
        state = build_shadow_learning_session_state(
            entry_id=collector_entry.entry_id,
            route_owner_id=f"shadow_learning:{collector_entry.entry_id}:test",
            collector_pn=hold.collector_pn, trace_path=str(config_dir / "shadow.jsonl"),
            original_endpoint="cloud.invalid,18899,TCP", proxy_endpoint="192.0.2.1,18899,TCP",
            upstream_endpoint="cloud.invalid,18899,TCP", restore_required=True,
            started_at=hold.armed_at, updated_at=hold.armed_at, status=prior_status,
        )
        await hass.async_add_executor_job(lambda: save_shadow_learning_session_state(config_dir=config_dir, state=state))
    for _ in range(2):
        assert await hass.config_entries.async_setup(collector_entry.entry_id)
        await hass.async_block_till_done()
        coordinator = collector_entry.runtime_data
        assert await coordinator.async_shadow_learning_start_blocker() == "cloud_learning_protection_held"
        diagnostics = await coordinator._async_shadow_learning_recovery_diagnostics()
        assert diagnostics["cloud_learning_protection_status"] == "held"
        assert diagnostics["cloud_learning_protection_reason"] == "cloud_learning_hold_pending"
        assert hold.collector_pn not in str(diagnostics)
        bundle = coordinator._build_support_bundle_payload(shadow_learning_recovery=diagnostics)
        assert bundle["runtime"]["metadata"]["cloud_learning_protection_status"] == "held"
        with pytest.raises(CloudLearningProtectionRequired):
            await coordinator._async_require_cloud_learning_unheld()
        result = await coordinator.async_stop_shadow_learning(
            clear_failed_restore=True, raise_when_not_running=False, request_refresh=False,
        )
        assert result["status"] == "cloud_held"
        assert result["restore_confirmed"] is False
        restore.assert_not_awaited()
        if prior_status:
            saved = await hass.async_add_executor_job(load_shadow_learning_session_state, config_dir)
            assert saved.status == "restore_failed"
            assert saved.original_endpoint == state.original_endpoint
            assert saved.last_restore_error == "cloud_learning_hold_pending"
            assert authority.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.active_operation(collector_entry.entry_id)
        await hass.config_entries.async_unload(collector_entry.entry_id)
        assert await hass.async_add_executor_job(store.load, hold.collector_pn) == hold
    assert len(fake_runtime) == 2


async def test_corrupt_hold_on_cold_start_blocks_without_repairing_file(
    hass, collector_entry, fake_runtime,
):
    store, hold = await seed_hold(hass, collector_entry)
    path = store._path(hold.collector_pn)
    await hass.async_add_executor_job(path.write_text, "{broken")
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    assert await coordinator.async_shadow_learning_start_blocker() == "cloud_learning_protection_held"
    result = await coordinator.async_stop_shadow_learning(
        clear_failed_restore=True, raise_when_not_running=False, request_refresh=False,
    )
    assert result["reason"] == "cloud_learning_hold_invalid"
    assert result["restore_confirmed"] is False
    await hass.config_entries.async_unload(collector_entry.entry_id)
    assert await hass.async_add_executor_job(path.read_text) == "{broken"


async def test_missing_composition_fence_is_not_permission_to_restore(
    hass, collector_entry, fake_runtime, monkeypatch,
):
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    monkeypatch.delattr(coordinator, "_cloud_learning_mutation_fence")
    with pytest.raises(CloudLearningProtectionRequired, match="context_missing"):
        await coordinator._async_restore_proxy_capture_endpoint("cloud.invalid,18899,TCP")
    await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_late_hold_refusal_cannot_be_force_cleared(
    hass, collector_entry, fake_runtime, monkeypatch,
):
    from custom_components.eybond_local.connection import collector_endpoint_operation as authority
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    from custom_components.eybond_local.support.shadow_learning.session import build_shadow_learning_session_state
    monkeypatch.setattr(authority, "COLLECTOR_ENDPOINT_OPERATION_AUTHORITY", authority.CollectorEndpointOperationAuthority())
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    state = build_shadow_learning_session_state(
        entry_id=collector_entry.entry_id, collector_pn=collector_entry.data["collector_pn"],
        route_owner_id=f"shadow_learning:{collector_entry.entry_id}:late",
        trace_path=str(Path(hass.config.config_dir) / "shadow.jsonl"),
        original_endpoint="cloud.invalid,18899,TCP", proxy_endpoint="192.0.2.1,18899,TCP",
        upstream_endpoint="cloud.invalid,18899,TCP", restore_required=True,
        started_at="2026-10-15T12:00:00Z", updated_at="2026-10-15T12:00:00Z", status="ready",
    )
    await coordinator._async_save_shadow_learning_session_state(state)
    async def refuse_restore(_endpoint):
        await seed_hold(hass, collector_entry)
        raise CloudLearningProtectionRequired("cloud_learning_hold_pending")
    monkeypatch.setattr(coordinator, "_async_restore_proxy_capture_endpoint", refuse_restore)
    result = await coordinator.async_stop_shadow_learning(clear_failed_restore=True, request_refresh=False)
    assert result["status"] == "cloud_held"
    assert result["restore_confirmed"] is False
    saved = coordinator._cached_shadow_learning_session_state
    assert saved.status == "restore_failed"
    assert saved.original_endpoint == state.original_endpoint
    assert authority.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.active_operation(collector_entry.entry_id)
    await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_direct_and_proxy_restore_paths_are_fenced_before_io(
    hass, collector_entry, fake_runtime, monkeypatch,
):
    from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    await seed_hold(hass, collector_entry)
    stop = AsyncMock()
    read = AsyncMock(side_effect=AssertionError("must check hold before network IO"))
    monkeypatch.setattr(coordinator, "_async_stop_proxy_capture_process", stop)
    monkeypatch.setattr(coordinator, "_async_read_live_collector_server_endpoint", read)
    with pytest.raises(CloudLearningProtectionRequired):
        await coordinator._async_restore_proxy_capture_endpoint("cloud.invalid,18899,TCP")
    with pytest.raises(CloudLearningProtectionRequired):
        await coordinator._async_trigger_proxy_capture_restore(trace_path=Path("/unused"), owner_id="owner")
    result = await coordinator._async_guarded_proxy_capture_restore(
        state=SimpleNamespace(route_owner_id="owner"), prefer_proxy_restore_trigger=True,
    )
    assert result["restore_confirmed"] is False
    stop.assert_awaited_once_with(owner_id="owner")
    read.assert_not_awaited()
    with pytest.raises(CloudLearningProtectionRequired):
        await coordinator.async_start_shadow_learning()
    with pytest.raises(CloudLearningProtectionRequired):
        await coordinator.async_start_proxy_capture(confirm_redirect=True)
    with pytest.raises(CloudLearningProtectionRequired):
        async with coordinator._collector_endpoint_operation("manual_endpoint_write"):
            pytest.fail("manual mutation must not acquire permission")
    transition = await coordinator.async_run_connection_strategy_transition(target_strategy="inbound")
    assert transition.success is False
    assert transition.failure_reason == "cloud_learning_protection_held"
    await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_removal_keeps_pn_hold_and_skips_separate_restart_hook(
    hass, collector_entry, monkeypatch,
):
    from custom_components.eybond_local import async_remove_entry
    from custom_components import eybond_local as integration
    from custom_components.eybond_local.connection import removal_finalization
    from custom_components.eybond_local import passive_discovery
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningMutationFence
    store, hold = await seed_hold(hass, collector_entry)
    restart = AsyncMock()
    monkeypatch.setattr(integration, "_async_ensure_listener_entry", AsyncMock())
    monkeypatch.setattr(removal_finalization, "async_finalize_collector_entry_removal", restart)
    monkeypatch.setattr(passive_discovery, "get_passive_callback_discovery", lambda _hass: SimpleNamespace(
        take_entry_removal_ticket=lambda _entry: object(), registry=object(),
    ))
    await async_remove_entry(hass, collector_entry)
    restart.assert_not_awaited()
    assert await hass.async_add_executor_job(store.load, hold.collector_pn) == hold
    # Re-adding with another entry id cannot change this independent PN scope.
    assert await CloudLearningMutationFence(Path(hass.config.config_dir), hold.collector_pn).hold_reason()
