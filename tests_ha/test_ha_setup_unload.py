"""Full setup -> platform forwarding -> unload through the REAL Home Assistant.

Only the device/transport boundary is faked (`create_runtime_manager`, the
factory seam the coordinator already uses). Everything else is genuine:
`async_setup_entry` is OUR code, Home Assistant forwards the real entity
platforms, and Home Assistant performs the unload.
"""

from __future__ import annotations

import asyncio

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import (
    CONF_ENTRY_ROLE,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
    PLATFORMS,
)
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


def _collector_entry(hass: HomeAssistant) -> MockConfigEntry:
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
            "collector_operation_mode": "home_assistant_only",
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


async def test_setup_forwards_platforms_then_unloads(
    hass: HomeAssistant, fake_runtime
) -> None:
    """The real entry lifecycle: HA sets us up, forwards platforms, unloads us."""

    entry = _collector_entry(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    # Our real async_setup_entry constructed the runtime through the factory seam
    # and started it exactly once (no device, no socket).
    assert len(fake_runtime) == 1
    assert fake_runtime[0].started == 1
    # A real coordinator object is attached as runtime data.
    assert entry.runtime_data is not None

    # Home Assistant really forwarded the entity platforms and registered
    # entities for this entry.
    registry = er.async_get(hass)
    entities = er.async_entries_for_config_entry(registry, entry.entry_id)
    assert entities, "no entities were registered by the real platform forwarding"
    registered_platforms = {entity.domain for entity in entities}
    assert registered_platforms <= set(PLATFORMS)

    # Unload through Home Assistant.
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.NOT_LOADED
    assert fake_runtime[0].stopped >= 1


async def test_unload_leaves_no_pending_tasks_or_listeners(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Setup/unload must not leak background tasks, timers or listeners."""

    entry = _collector_entry(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.NOT_LOADED
    # The entry's own unload hooks are all drained by HA.
    assert not entry.update_listeners

    # No task started by this entry is still pending. (Tasks belonging to the
    # test harness itself are excluded by only looking at not-done tasks after a
    # full block_till_done.)
    pending = [
        task
        for task in asyncio.all_tasks()
        if not task.done() and task is not asyncio.current_task()
    ]
    leaked = [task for task in pending if DOMAIN in repr(task.get_coro())]
    assert not leaked, f"leaked integration tasks after unload: {leaked}"


async def test_setup_and_unload_is_repeatable(
    hass: HomeAssistant, fake_runtime
) -> None:
    """A reload cycle works: the entry can be set up again after unload."""

    entry = _collector_entry(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    # A second, independent runtime was constructed and started.
    assert len(fake_runtime) == 2
    assert fake_runtime[1].started == 1

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED


async def test_reload_through_home_assistant(hass: HomeAssistant, fake_runtime) -> None:
    """`async_reload` (the update-listener path) works end to end."""

    entry = _collector_entry(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 2

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_listener_entry_sets_up_and_unloads_without_runtime(
    hass: HomeAssistant, fake_runtime
) -> None:
    """The listener/bootstrap role loads with no coordinator and no platforms."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Local — Discovery",
        unique_id=f"{DOMAIN}:listener",
        version=3,
        data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
    )
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    # No device runtime is constructed for the bootstrap entry.
    assert not fake_runtime
    assert entry.runtime_data is None

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED
