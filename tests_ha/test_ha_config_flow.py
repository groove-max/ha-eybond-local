"""Config flow and options flow driven by the REAL Home Assistant flow manager.

These catch the class of bug the stub suite structurally cannot: unknown handler,
invalid flow, and `Handler ... doesn't support step ...`.
"""

from __future__ import annotations

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import (
    CONF_ENTRY_ROLE,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
)

from synthetic import (
    SYNTHETIC_COLLECTOR_IP,
    SYNTHETIC_COLLECTOR_PN,
    SYNTHETIC_SERVER_IP,
)


async def test_user_config_flow_reaches_first_step(hass: HomeAssistant) -> None:
    """The real flow manager starts our user flow and returns a usable step.

    No network discovery is performed: the host-interface boundary is faked and
    the first step is a form/menu that asks the user something.
    """

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    # Real HA rejects an unknown handler before we get here; reaching a step also
    # proves the declared step exists on the handler.
    assert result["step_id"]
    assert result.get("errors") in (None, {})


async def test_user_flow_step_is_servable_twice(hass: HomeAssistant) -> None:
    """Re-entering the flow is stable (no handler/step registration drift)."""

    first = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    second = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )

    assert first["step_id"] == second["step_id"]
    for flow in hass.config_entries.flow.async_progress():
        hass.config_entries.flow.async_abort(flow["flow_id"])


async def test_unknown_step_is_not_silently_accepted(hass: HomeAssistant) -> None:
    """Guard the guard: the real flow manager rejects a nonexistent step."""

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    with pytest.raises(Exception):
        await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=None, step_id="definitely_not_a_step"
        )


@pytest.fixture
def collector_entry(hass: HomeAssistant) -> MockConfigEntry:
    """A real, fully-formed collector entry with a synthetic durable identity."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Collector",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=3,
        data={
            "connection_type": "eybond",
            "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
            "control_mode": "read_only",
            "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external",
            "proxy_enabled": False,
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    return entry


async def test_options_flow_reaches_first_step(
    hass: HomeAssistant, collector_entry: MockConfigEntry
) -> None:
    """The real flow manager serves our options flow's first step.

    This is what catches `Handler ... doesn't support step ...`: HA resolves the
    options handler from the entry and invokes its declared initial step.
    """

    result = await hass.config_entries.options.async_init(collector_entry.entry_id)

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    assert result["step_id"]


async def test_listener_entry_gets_its_own_options_flow(hass: HomeAssistant) -> None:
    """The listener role resolves to its dedicated options handler, for real."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Local — Discovery",
        unique_id=f"{DOMAIN}:listener",
        version=3,
        data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    assert result["step_id"]


async def test_options_flow_entry_is_not_loaded_requirement(
    hass: HomeAssistant, collector_entry: MockConfigEntry
) -> None:
    """Options flow init does not require a started runtime (no device needed)."""

    assert collector_entry.state is ConfigEntryState.NOT_LOADED
    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)


async def test_runtime_options_commit_strategy_to_data_with_one_reload(
    hass: HomeAssistant, collector_entry: MockConfigEntry, fake_runtime
) -> None:
    """The real options manager keeps strategy canonical and reloads once."""

    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    assert collector_entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 1
    assert collector_entry.runtime_data._suppress_entry_reload_count == 0

    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert result["type"] is FlowResultType.MENU
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"next_step_id": "runtime"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "runtime"

    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            "poll_mode": "auto",
            "control_mode": "read_only",
            "connection_strategy": "inbound",
            "connection": {
                "server_ip": SYNTHETIC_SERVER_IP,
                "collector_ip": SYNTHETIC_COLLECTOR_IP,
                "tcp_port": 8899,
                "udp_port": 58899,
                "discovery_target": "192.0.2.255",
                "discovery_interval": 3,
                "heartbeat_interval": 60,
                "driver_hint": "auto",
            },
        },
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()

    assert collector_entry.data["connection_strategy"] == "inbound"
    assert "connection_strategy" not in collector_entry.options
    assert collector_entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 2, "runtime options must schedule exactly one reload"

    await hass.config_entries.async_unload(collector_entry.entry_id)
    await hass.async_block_till_done()
