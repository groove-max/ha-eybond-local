"""Real HA dispatch + provider clients + correlation, with synthetic HTTP/observations.

Only device and HTTPS boundaries are simulated; no vendor/device IO is allowed.
These tests do not substitute for a successful live-cloud learning run.
"""

import asyncio
import threading
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("source", ["smartess", "dessmonitor"])
@pytest.mark.parametrize("cloud_err", [0, 4])
@pytest.mark.parametrize("failure", [None, "connect", "request", "response_headers", "response_body", "second_request"])
async def test_provider_http_outcome_preserves_exact_run_recovery(
    dispatch_context, monkeypatch, source, failure, cloud_err,
):
    from custom_components.eybond_local import cloud_http_session as http
    from custom_components.eybond_local import dessmonitor_cloud as dess
    from custom_components.eybond_local import smartess_cloud as smart
    from custom_components.eybond_local.support.shadow_learning import ShadowWriteObservation
    from custom_components.eybond_local.support.shadow_learning.dessmonitor_orchestrator import (
        async_orchestrate_dessmonitor_shadow_learning,
    )
    from custom_components.eybond_local.support.shadow_learning.orchestrator import (
        async_orchestrate_shadow_learning_settings,
    )
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    from custom_components.eybond_local.support.shadow_learning.session import load_shadow_learning_session_state

    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(replace(ctx.state, cloud_source=source))
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    c._protected_learning_run = replace(c._protected_learning_run, source=source)
    root = Path(c.hass.config.config_dir)
    store = CloudLearningProtectionStore(root)
    connection = Mock(sock=None)
    response = MagicMock(status=200, length=0, will_close=False)
    response.__enter__.return_value = response
    observations = []
    requests = []
    pending_value = None

    def fail_at(phase):
        if failure == phase:
            raise TimeoutError("synthetic connection fault")

    def connect():
        # Even DNS/TCP/TLS preparation is inside the durable dispatch transaction.
        assert load_shadow_learning_session_state(root, entry_id=c.config_entry.entry_id).cloud_commands_possible
        assert store.load(ctx.pn).source == source
        fail_at("connect")
        connection.sock = Mock()

    def request(method, target, **_kwargs):
        nonlocal pending_value
        requests.append(parse_qs(urlsplit(target).query))
        assert method == "GET"
        fail_at("request")
        if failure == "second_request" and len(requests) == 2:
            raise TimeoutError("synthetic second request fault")
        # Synthetic cloud emits the corresponding write; do not mock correlation.
        pending_value = int(requests[-1]["val"][0])

    def headers():
        fail_at("response_headers")
        return response

    def read(_limit):
        # A captured write can precede a lost HTTP response; capture is not a
        # reason to retry or continue with the next setting on an uncertain error.
        observations.append(ShadowWriteObservation(
            register=300, values=(pending_value,), function_code=6,
            devcode=2304, devaddr=1, raw_payload_hex=f"{pending_value:04x}",
        ))
        fail_at("response_body")
        return ('{"err":%d,"desc":"synthetic","dat":{}}' % cloud_err).encode()

    connection.connect.side_effect = connect
    connection.request.side_effect = request
    connection.getresponse.side_effect = headers
    response.read.side_effect = read
    factory = Mock(return_value=connection)
    monkeypatch.setattr(http, "HTTPSConnection", factory)
    provider = smart if source == "smartess" else dess
    session = http.CloudHttpSession(provider.DEFAULT_BASE_URL)

    async def dispatch_action(callable_, **kwargs):
        return await c.async_dispatch_shadow_learning_cloud_action(
            callable_, collector_pn=ctx.pn, route_owner_id=ctx.owner, **kwargs,
        )

    common = dict(
        confirm_cloud_write=True, shadow_session_state="ready", all_choice_values=True,
        observation_cursor=lambda: len(observations),
        current_observations_since=lambda cursor: tuple(observations[cursor:]),
        is_session_ready=lambda: True, http_session=session, dispatch_action=dispatch_action,
    )

    async def learn():
        if source == "smartess":
            return await async_orchestrate_shadow_learning_settings(
                settings_dat={"field": [{"id": "mode", "name": "Mode", "item": [
                    {"key": str(v), "val": label} for v, label in enumerate(("Solar", "Grid", "Battery"))
                ]}]},
                session=smart.SessionCredentials(token="synthetic", secret="synthetic"),
                pn=ctx.pn, sn="SYNTHETIC", devcode=2304, devaddr=1,
                dry_run=False, field_ids=[], include_numeric=False, **common,
            )
        return await async_orchestrate_dessmonitor_shadow_learning(
            control_fields=(dess.DessMonitorControlField(
                field_id="mode", title="Mode", choices=(("0", "Solar"), ("1", "Grid"), ("2", "Battery")),
            ),),
            session=dess.DessMonitorSession(token="synthetic", secret="synthetic"),
            identity=dess.DessMonitorDeviceIdentity(ctx.pn, "SYNTHETIC", 2304, 1), **common,
        )

    try:
        if failure:
            with pytest.raises((smart.SmartEssCloudError, dess.DessMonitorCloudError)):
                await learn()
        else:
            result = await learn()
            assert result["captured_not_applied_count"] == 3
            assert result["leaked_count"] == result["degraded_count"] == 0
            assert [item["observation"]["values"] for item in result["results"]] == [[0], [1], [2]]
    finally:
        session.close()

    expected = 3 if failure is None else 0 if failure == "connect" else 2 if failure == "second_request" else 1
    assert len(requests) == expected  # No retry, provider fallback or later action.
    factory.assert_called_once()
    connection.close.assert_called_once()
    assert c._cloud_tool_preparation_task is None
    assert not c._runtime_operation_lock.locked()

    no_request = failure == "connect"
    restore = AsyncMock(return_value=ctx.state.original_endpoint) if no_request else AsyncMock(
        side_effect=AssertionError("held run must not restore direct cloud"),
    )
    monkeypatch.setattr(c, "_async_restore_proxy_capture_endpoint", restore)
    monkeypatch.setattr(c, "_async_verify_restored_collector_endpoint", AsyncMock(return_value={
        "restore_confirmed": True, "observed_endpoint": ctx.state.original_endpoint,
    }))
    stop = AsyncMock()
    monkeypatch.setattr(c._runtime, "async_stop_shadow_learning_route", stop)
    result = await c.async_stop_shadow_learning(request_refresh=False)
    assert result["status"] == ("stopped" if no_request else "cloud_held")
    if no_request:
        restore.assert_awaited_once_with(ctx.state.original_endpoint)
        assert await c._async_active_shadow_learning_state(require_process=False) is None
    else:
        restore.assert_not_awaited()
    stop.assert_awaited_once()
    assert c._protected_learning_run is None
    assert (store.load(ctx.pn) is None) is no_request
    await c.async_request_refresh()
    assert c.last_update_success


@pytest.mark.parametrize("journal_corrected", [False, True])
async def test_interrupted_no_request_receipt_finishes_without_resuming(
    dispatch_context, monkeypatch, journal_corrected,
):
    from test_ha_cloud_learning_dispatch import dispatch
    from custom_components.eybond_local.runtime.coordinator.learning_recovery import async_preserve_interrupted_learning_hold
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    await dispatch(ctx, lambda: None)
    store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    hold = store.load(ctx.pn)
    await c._async_cloud_learning_hold_reason()  # Exercise sticky-fence retirement.
    await c.hass.async_add_executor_job(store.record_no_request, hold)
    state = await c._async_active_shadow_learning_state(require_process=False)
    if journal_corrected:
        await c._async_save_shadow_learning_session_state(replace(state, cloud_commands_possible=False))
    c._protected_learning_run = None
    await asyncio.gather(*(async_preserve_interrupted_learning_hold(c) for _ in range(3)))
    assert store.load(ctx.pn) is None
    assert not await c._async_cloud_learning_hold_reason()
    assert not (await c._async_active_shadow_learning_state(require_process=False)).cloud_commands_possible
    assert c._protected_learning_run is None  # Recovery never resumes HTTP.


async def test_cancelled_connect_is_joined_before_no_request_retirement(dispatch_context, monkeypatch):
    from custom_components.eybond_local import cloud_http_session as http
    from custom_components.eybond_local import smartess_cloud as smart
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    started, release = threading.Event(), threading.Event()
    connection = Mock(sock=None)
    def connect():
        started.set()
        assert release.wait(5)
        raise TimeoutError("synthetic connect")
    connection.connect.side_effect = connect
    monkeypatch.setattr(http, "HTTPSConnection", Mock(return_value=connection))
    session = http.CloudHttpSession(smart.DEFAULT_BASE_URL)
    store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    task = asyncio.create_task(c.async_dispatch_shadow_learning_cloud_action(
        smart.fetch_signed_action, collector_pn=ctx.pn, route_owner_id=ctx.owner,
        action="&action=ctrlDevice&id=synthetic&val=1", http_session=session,
        session=smart.SessionCredentials(token="synthetic", secret="synthetic"),
    ))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert store.load(ctx.pn) is not None
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        session.close()
    connection.request.assert_not_called()
    assert store.load(ctx.pn) is None
    assert not (await c._async_active_shadow_learning_state(require_process=False)).cloud_commands_possible


async def test_cold_setup_completes_no_request_receipt_before_endpoint_recovery(dispatch_context, monkeypatch):
    from test_ha_cloud_learning_dispatch import dispatch
    from custom_components.eybond_local.runtime.coordinator.cloud_tools import CoordinatorCloudToolsMixin
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    ctx = dispatch_context
    c = ctx.coordinator
    await c._async_save_shadow_learning_session_state(ctx.state)
    monkeypatch.delattr(c, "_async_active_shadow_learning_state")
    await dispatch(ctx, lambda: None)
    store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    hold = store.load(ctx.pn)
    # Simulate receipt persisted immediately before process loss. Skip ordinary
    # shutdown recovery so only the new coordinator sees and completes it.
    monkeypatch.setattr(c, "_async_cancel_cloud_tool_preparation", AsyncMock())
    monkeypatch.setattr(c, "async_stop_shadow_learning", AsyncMock())
    await c.hass.async_add_executor_job(store.record_no_request, hold)
    restore = AsyncMock(return_value=ctx.state.original_endpoint)
    monkeypatch.setattr(CoordinatorCloudToolsMixin, "_async_restore_proxy_capture_endpoint", restore)
    monkeypatch.setattr(CoordinatorCloudToolsMixin, "_async_verify_restored_collector_endpoint", AsyncMock(return_value={
        "restore_confirmed": True, "observed_endpoint": ctx.state.original_endpoint,
    }))
    assert await c.hass.config_entries.async_reload(ctx.token.entry_id)
    await c.hass.async_block_till_done()
    new = c.hass.config_entries.async_get_entry(ctx.token.entry_id).runtime_data
    assert not await new._async_cloud_learning_hold_reason()
    assert await new._async_active_shadow_learning_state(require_process=False) is None
    restore.assert_awaited_once_with(ctx.state.original_endpoint)
