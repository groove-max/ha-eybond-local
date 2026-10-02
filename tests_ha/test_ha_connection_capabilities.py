"""Connection setup stays reachable before inverter recognition (#49)."""

from dataclasses import replace

import pytest
from homeassistant.data_entry_flow import FlowResultType

from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("kind", ["unknown", "factory_eybond", "esp_eybond_bridge"])
@pytest.mark.parametrize("loaded", [False, True])
async def test_connection_menu_does_not_confuse_unknown_with_local_only(
    hass, collector_entry, fake_runtime, kind, loaded,
):
    """Use the real HA flow manager with no inverter or hardware identity."""
    if kind != "unknown":
        hass.config_entries.async_update_entry(
            collector_entry, data={**collector_entry.data, "collector_kind": kind},
        )
    if loaded:
        assert await hass.config_entries.async_setup(collector_entry.entry_id)
        await hass.async_block_till_done()
        coordinator = collector_entry.runtime_data
        coordinator.data = replace(
            coordinator.data, connected=True, inverter=None,
            values={"runtime_driver_state": "driver_unbound"},
        )
        # Viewing the connection form cannot grant factory capabilities.
        assert coordinator.collector_capabilities.collector_kind == kind
        assert coordinator.collector_capabilities.cloud_connection_supported is (
            kind == "factory_eybond"
        )

    before_data = dict(collector_entry.data)
    before_options = dict(collector_entry.options)
    options = hass.config_entries.options
    result = await options.async_init(collector_entry.entry_id)
    flow_id = result["flow_id"]
    try:
        assert result["type"] is FlowResultType.MENU
        if kind == "esp_eybond_bridge":
            assert "collector_endpoint" in result["menu_options"]
            assert "connection" not in result["menu_options"]
            # A stale/direct link cannot expose cloud modes on a known ESP.
            result = await options._progress[flow_id].async_step_connection()
            assert result["type"] is FlowResultType.MENU
            assert result["step_id"] == "init"
        else:
            assert "connection" in result["menu_options"]
            result = await options.async_configure(
                flow_id, {"next_step_id": "connection"},
            )
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] == "connection"
            # Choosing a different profile only stages the existing transaction.
            result = await options.async_configure(
                flow_id, {"connection_strategy": "inbound"},
            )
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] == "strategy_transition"
            result = await options.async_configure(flow_id, {
                "advertised_server_ip": "192.0.2.10",
                "advertised_tcp_port": 8899,
                "confirm_connection_strategy_risk": False,
            })
            assert result["step_id"] == "strategy_transition"
            assert result["errors"]["confirm_connection_strategy_risk"] == (
                "connection_strategy_risk_unconfirmed"
            )
            flow = options._progress[flow_id]
            assert flow._transition_task is None
        assert collector_entry.data == before_data
        assert collector_entry.options == before_options
    finally:
        options.async_abort(flow_id)
        if loaded:
            await hass.config_entries.async_unload(collector_entry.entry_id)
