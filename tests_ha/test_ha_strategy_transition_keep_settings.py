"""A failed mode change must not replace unrelated options with an empty dict.

Drive the actual HA options manager and reload lifecycle. Only the physical
transition is stubbed: no collector commands, endpoint changes or live HA server.
"""

from unittest.mock import AsyncMock

import pytest

from custom_components.eybond_local.connection.strategy_transition import (
    StrategyTransitionResult,
)
from synthetic import SYNTHETIC_SERVER_IP
from test_ha_config_flow import collector_entry  # noqa: F401


@pytest.mark.parametrize("failure_kind", ["exception", "typed_result"])
@pytest.mark.parametrize("action", ["keep_settings", "cancel"])
@pytest.mark.parametrize("update_while_failed", [False, True])
async def test_failed_mode_change_preserves_latest_options_and_data(
    hass, collector_entry, fake_runtime, monkeypatch,
    failure_kind, action, update_while_failed,
):
    saved_options = {
        **collector_entry.options,
        "driver_hint": "modbus_smg",
        "poll_mode": "manual",
        "poll_interval": 40,
        "control_mode": "read_only",
        "collector_original_server_endpoint": "cloud.example.invalid:18899",
        # Opaque options must survive too; this does not install an experimental
        # map or require the device-specific catalog from an unmerged PR.
        "device_catalog_entry_key": "explicit_test_map",
        "future_option": {"enabled": True},
    }
    hass.config_entries.async_update_entry(collector_entry, options=saved_options)
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()

    async def fail_without_device_io(**kwargs):
        assert kwargs["target_strategy"] == "inbound"
        assert kwargs["option_payload"] == {}
        if failure_kind == "exception":
            raise RuntimeError("transition_session_unavailable")
        return StrategyTransitionResult(
            success=False, target_strategy="inbound",
            failure_reason="transition_session_unavailable",
        )

    transition = AsyncMock(side_effect=fail_without_device_io)
    monkeypatch.setattr(
        collector_entry.runtime_data,
        "async_run_connection_strategy_transition", transition,
    )
    options = hass.config_entries.options
    result = await options.async_init(collector_entry.entry_id)
    flow_id = result["flow_id"]
    result = await options.async_configure(flow_id, {"next_step_id": "connection"})
    assert result["step_id"] == "connection"
    result = await options.async_configure(flow_id, {"connection_strategy": "inbound"})
    assert result["step_id"] == "strategy_transition"
    result = await options.async_configure(flow_id, {
        "advertised_server_ip": SYNTHETIC_SERVER_IP,
        "advertised_tcp_port": 8899,
        "confirm_connection_strategy_risk": True,
    })
    for _ in range(5):
        if result["type"] not in ("progress", "progress_done"):
            break
        await hass.async_block_till_done()
        result = await options.async_configure(flow_id)
    assert result["step_id"] == "strategy_transition_failed", result

    if update_while_failed:
        # A different legitimate options save can complete while the user reads
        # the failure menu. The action must use the current entry, not a snapshot
        # captured before starting the failed transition.
        hass.config_entries.async_update_entry(collector_entry, options={
            **collector_entry.options, "poll_interval": 60,
            "latest_option": "must_survive",
        })
        await hass.async_block_till_done()
    options_before = dict(collector_entry.options)
    data_before = dict(collector_entry.data)
    runtime_count = len(fake_runtime)

    result = await options.async_configure(flow_id, {
        "next_step_id": f"strategy_transition_{action}",
    })
    assert result["type"] == "create_entry"
    await hass.async_block_till_done()
    assert collector_entry.options == options_before
    assert collector_entry.data == data_before
    assert len(fake_runtime) == runtime_count, "keeping unchanged options must not reload"
    transition.assert_awaited_once()

    assert await hass.config_entries.async_reload(collector_entry.entry_id)
    await hass.async_block_till_done()
    assert collector_entry.options == options_before
    assert collector_entry.data == data_before
    assert await hass.config_entries.async_unload(collector_entry.entry_id)
    await hass.async_block_till_done()
