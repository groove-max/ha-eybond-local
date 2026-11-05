"""Connection setup stays reachable before inverter recognition (#49)."""

import asyncio
from dataclasses import replace

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.data_entry_flow import FlowResultType

from test_ha_config_flow import collector_entry

from custom_components.eybond_local import _ENTRY_SCHEMA_VERSION
from custom_components.eybond_local.collector.capabilities import (
    collector_capability_profile_from_entry,
    collector_profile_entry_fields,
)


@pytest.mark.parametrize("kind", ["unknown", "factory_eybond", "esp_eybond_bridge"])
@pytest.mark.parametrize("loaded", [False, True])
async def test_connection_menu_does_not_confuse_unknown_with_local_only(
    hass, collector_entry, fake_runtime, kind, loaded,
):
    """Use the real HA flow manager with no inverter or hardware identity."""
    if kind != "unknown":
        # This menu test starts with a fully persisted known collector profile.
        # Partial bridge metadata triggers an unrelated startup entity reload.
        profile_fields = collector_profile_entry_fields(
            collector_capability_profile_from_entry({"collector_kind": kind}, {}),
        )
        hass.config_entries.async_update_entry(
            collector_entry,
            data={**collector_entry.data, **profile_fields},
            options={**collector_entry.options, **profile_fields},
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
            assert await hass.config_entries.async_unload(collector_entry.entry_id)
            assert collector_entry.state is ConfigEntryState.NOT_LOADED


@pytest.mark.parametrize("partial_profile", [
    pytest.param({"collector_kind": "esp_eybond_bridge"}, id="kind-only"),
    pytest.param({"collector_virtual_bridge": True}, id="legacy-bridge-flag"),
])
async def test_partial_bridge_profile_normalizes_then_completes_reload(
    hass, collector_entry, fake_runtime, monkeypatch, partial_profile,
):
    """Partial bridge metadata survives migration, normalization and a real reload."""
    from conftest import FakeRuntimeManager

    hass.config_entries.async_update_entry(
        collector_entry, data={**collector_entry.data, **partial_profile},
    )
    expected_profile = {
        "collector_kind": "esp_eybond_bridge",
        "collector_virtual_bridge": True,
        "collector_bridge_kind": "esp-collector",
    }
    assert not expected_profile.keys() <= collector_entry.data.keys()
    assert not expected_profile.keys() & collector_entry.options.keys()
    identity = (collector_entry.entry_id, collector_entry.unique_id)
    collector_pn = collector_entry.data["collector_pn"]
    refresh_started = asyncio.Event()
    allow_refresh = asyncio.Event()
    reload_finished = hass.loop.create_future()
    reload_calls = []
    original_refresh = FakeRuntimeManager.async_refresh
    original_reload = hass.config_entries.async_reload

    async def gated_refresh(self, *, poll_interval=None):
        refresh_started.set()
        await allow_refresh.wait()
        return await original_refresh(self, poll_interval=poll_interval)

    async def tracked_reload(entry_id):
        if entry_id == collector_entry.entry_id:
            reload_calls.append(entry_id)
        result = await original_reload(entry_id)
        if entry_id == collector_entry.entry_id and not reload_finished.done():
            reload_finished.set_result(result)
        return result

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", gated_refresh)
    monkeypatch.setattr(hass.config_entries, "async_reload", tracked_reload)

    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    try:
        async with asyncio.timeout(5):
            await refresh_started.wait()
        assert collector_entry.state is ConfigEntryState.LOADED
        assert len(fake_runtime) == 1
        initial_coordinator = collector_entry.runtime_data
        assert fake_runtime[0].stopped == 0

        # Finish the device refresh after platform setup. Wait for the actual
        # reload to return, including its replacement setup, before unloading.
        allow_refresh.set()
        async with asyncio.timeout(5):
            assert await reload_finished
        await hass.async_block_till_done()

        assert reload_calls == [collector_entry.entry_id]
        assert collector_entry.state is ConfigEntryState.LOADED
        assert collector_entry.version == _ENTRY_SCHEMA_VERSION
        assert collector_entry.runtime_data is not initial_coordinator
        assert (collector_entry.entry_id, collector_entry.unique_id) == identity
        assert collector_entry.data["collector_pn"] == collector_pn
        for fields in (collector_entry.data, collector_entry.options):
            for key, value in expected_profile.items():
                assert fields[key] == value
        assert collector_entry.runtime_data.collector_capabilities.ha_only_required
        assert not collector_entry.runtime_data.collector_capabilities.cloud_connection_supported
        assert len(fake_runtime) == 2
        assert [runtime.started for runtime in fake_runtime] == [1, 1]
        assert [runtime.stopped for runtime in fake_runtime] == [1, 0]
    finally:
        assert await hass.config_entries.async_unload(collector_entry.entry_id)
        await hass.async_block_till_done()

    assert collector_entry.state is ConfigEntryState.NOT_LOADED
    assert not hasattr(collector_entry, "runtime_data")
    assert [runtime.stopped for runtime in fake_runtime] == [1, 1]
