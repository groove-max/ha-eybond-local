"""Capture actions and explanations use the same current readiness snapshot."""
import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock, PropertyMock, patch

import pytest

from custom_components.eybond_local.support.proxy_capture import build_proxy_capture_overview
from custom_components.eybond_local.support.acquisition import (
    SupportAcquisitionReadiness, SupportOperationReadiness,
)
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("action", ["refresh", "stop"])
async def test_capture_status_does_not_wait_for_inverter_poll(
    hass, collector_entry, fake_runtime, action,
):
    """Refresh (including an already-stopped capture) is a view, not a poll."""
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    flow = hass.config_entries.options._progress[first["flow_id"]]
    ready = SupportOperationReadiness(visible=True, can_start=True, blocker="")
    readiness = SupportAcquisitionReadiness(
        collector_identified=True, inverter_identified=False,
        cloud_metadata_read=ready, proxy_capture=ready, active_control_learning=ready,
    )
    options = dict(collector_entry.options)
    poll_gate = asyncio.Event()
    try:
        with patch.object(type(coordinator), "support_acquisition_readiness",
                          new_callable=PropertyMock, return_value=readiness), \
             patch.object(coordinator, "async_request_refresh", new_callable=AsyncMock,
                          side_effect=poll_gate.wait) as poll, \
             patch.object(coordinator, "async_stop_proxy_capture", new_callable=AsyncMock,
                          side_effect=RuntimeError("proxy_capture_not_running")) as stop, \
             patch.object(coordinator, "async_touch_proxy_capture_lease",
                          new_callable=AsyncMock) as touch:
            async with asyncio.timeout(1):
                result = await flow.async_step_proxy_capture({"proxy_capture_action": action})
            assert result["type"] == "form"
            assert result["step_id"] == "proxy_capture"
            assert not result["errors"]
            poll.assert_not_awaited()
            touch.assert_not_awaited()
            assert stop.await_count == (1 if action == "stop" else 0)
        assert dict(collector_entry.options) == options
    finally:
        poll_gate.set()
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_auto_allows_capture_but_not_unidentified_inverter_writes(
    hass, collector_entry, fake_runtime,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    coordinator.data = replace(
        coordinator.data, connected=True, inverter=None,
        values={"runtime_driver_state": "driver_unbound"},
    )
    try:
        for mode, allowed in (("read_only", False), ("auto", True)):
            with patch.object(type(coordinator), "control_mode", new_callable=PropertyMock,
                              return_value=mode):
                assert not coordinator.controls_enabled
                overview = build_proxy_capture_overview(
                    control_mode=mode, collector_control_allowed=coordinator.collector_actions_enabled,
                    collector_connected=True, cloud_tools_allowed=True,
                    collector_cloud_family="valuecloud_at", collector_session_protocol="at_text",
                    cloud_session_protocol="at_text", current_endpoint="cloud.example,18899,TCP",
                    upstream_endpoint="cloud.example,18899,TCP", target_endpoint="192.0.2.10,18899,TCP",
                )
                assert overview.can_start is allowed
                assert overview.blocking_reason == ("" if allowed else "collector_control_disabled")
    finally:
        await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("connected", [False, True])
async def test_capture_form_does_not_mix_cached_instructions_with_live_actions(
    hass, collector_entry, fake_runtime, connected,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    flow = hass.config_entries.options._progress[first["flow_id"]]
    overview = build_proxy_capture_overview(
        control_mode="auto", collector_connected=connected, cloud_tools_allowed=True,
        collector_session_protocol="eybond_framed", cloud_session_protocol="eybond_framed",
        current_endpoint="cloud.example.test,18899,TCP",
        upstream_endpoint="cloud.example.test,18899,TCP",
        target_endpoint="192.0.2.10,18899,TCP",
    )
    cached = {**coordinator.data.values,
        "proxy_capture_status": "blocked" if connected else "ready",
        "proxy_capture_can_reconnect_for_start": connected,
        "proxy_capture_can_start": not connected,
        "proxy_capture_can_stop": False,
        "proxy_capture_redirect_required": True,
        "proxy_capture_blocking_reason": "collector_not_connected" if connected else "",
        "proxy_capture_target_endpoint": "192.0.2.99,18899,TCP",
    }
    coordinator.data = replace(coordinator.data, values=cached)
    try:
        with patch.object(type(coordinator), "proxy_capture_overview", new_callable=PropertyMock,
                          side_effect=[overview] + [replace(overview, can_start=not overview.can_start)] * 5):
            result = flow._show_proxy_capture_form(coordinator)
        actions = result["data_schema"].schema["proxy_capture_action"].config["options"]
        assert [item["value"] for item in actions] == ["start", "refresh"]
        label = actions[0]["label"].lower()
        plan = result["description_placeholders"]["proxy_capture_user_plan"].lower()
        assert ("reconnect" in label) is not connected
        assert ("reconnect and start capture" in plan) is not connected
        assert result["description_placeholders"]["proxy_capture_target_endpoint"] == overview.target_endpoint
        assert result["description_placeholders"]["proxy_capture_blocking_reason"] == (
            "the collector is not connected" if not connected else "Not applicable"
        )
        assert coordinator.data.values is cached  # A render must not mutate runtime state.
        assert cached["proxy_capture_can_reconnect_for_start"] is connected
        for marker in result["data_schema"].schema:
            if marker == "proxy_capture_action":
                assert marker.default() == ("start" if connected else "refresh")
    finally:
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("status,label,can_stop,critical,blocker,actions", [
    ("running", "Running", True, False, "session_active", ["stop", "reset_timer", "refresh"]),
    ("restoring", "Restoring collector connection", False, True, "session_active", ["refresh"]),
    ("blocked", "Not ready", False, False, "collector_control_not_allowed", ["refresh"]),
])
async def test_capture_form_live_state_overrides_stale_start_permission(
    hass, collector_entry, fake_runtime, status, label, can_stop, critical, blocker, actions,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    flow = hass.config_entries.options._progress[first["flow_id"]]
    overview = build_proxy_capture_overview(
        control_mode="auto", collector_connected=True, cloud_tools_allowed=True,
        collector_session_protocol="eybond_framed", cloud_session_protocol="eybond_framed",
        current_endpoint="cloud.example.test,18899,TCP",
        upstream_endpoint="cloud.example.test,18899,TCP",
        target_endpoint="192.0.2.10,18899,TCP",
    )
    overview = replace(
        overview, status=status, can_start=False, can_stop=can_stop,
        critical_phase=critical, blocking_reason=blocker,
    )
    cached = {**coordinator.data.values, "proxy_capture_can_start": True,
              "proxy_capture_status": "ready", "proxy_capture_blocking_reason": "",
              "proxy_capture_can_reconnect_for_start": True,
              "proxy_trace_live_log": "existing traffic evidence"}
    coordinator.data = replace(coordinator.data, values=cached)
    try:
        with patch.object(type(coordinator), "proxy_capture_overview", new_callable=PropertyMock,
                          return_value=overview):
            result = flow._show_proxy_capture_form(coordinator)
        options = result["data_schema"].schema["proxy_capture_action"].config["options"]
        assert [item["value"] for item in options] == actions
        placeholders = result["description_placeholders"]
        assert placeholders["proxy_capture_status_label"] == label
        assert "reconnect and start capture" not in placeholders["proxy_capture_user_plan"].lower()
        if can_stop:
            assert "stop capture" in placeholders["proxy_capture_user_plan"].lower()
            assert placeholders["proxy_capture_live_log"] == "existing traffic evidence"
        assert coordinator.data.values is cached
    finally:
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_failed_capture_start_renders_current_disconnected_state(
    hass, collector_entry, fake_runtime,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    flow = hass.config_entries.options._progress[first["flow_id"]]
    overview = build_proxy_capture_overview(
        control_mode="auto", collector_connected=True, cloud_tools_allowed=True,
        collector_session_protocol="eybond_framed", cloud_session_protocol="eybond_framed",
        current_endpoint="cloud.example.test,18899,TCP",
        upstream_endpoint="cloud.example.test,18899,TCP",
        target_endpoint="192.0.2.10,18899,TCP",
    )
    ready = SupportOperationReadiness(visible=True, can_start=True, blocker="")
    readiness = SupportAcquisitionReadiness(
        collector_identified=True, inverter_identified=False,
        cloud_metadata_read=ready, proxy_capture=ready, active_control_learning=ready,
    )
    try:
        with patch.object(type(coordinator), "proxy_capture_overview", new_callable=PropertyMock,
                          return_value=overview) as current, \
             patch.object(type(coordinator), "support_acquisition_readiness",
                          new_callable=PropertyMock, return_value=readiness), \
             patch.object(coordinator, "async_start_proxy_capture", new_callable=AsyncMock) as start:
            async def fail_preflight(**kwargs):
                current.return_value = replace(
                    overview, status="blocked", can_start=False, collector_connected=False,
                    can_reconnect_for_start=True, blocking_reason="collector_not_connected",
                )
                raise RuntimeError("cloud_tool_collector_not_connected")

            start.side_effect = fail_preflight
            result = await flow.async_step_proxy_capture({"proxy_capture_action": "start"})
        start.assert_awaited_once()
        assert result["errors"] == {"base": "proxy_capture_action_failed"}
        actions = result["data_schema"].schema["proxy_capture_action"].config["options"]
        assert "reconnect" in actions[0]["label"].lower()
        assert "reconnect and start capture" in result["description_placeholders"]["proxy_capture_user_plan"].lower()
    finally:
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)
