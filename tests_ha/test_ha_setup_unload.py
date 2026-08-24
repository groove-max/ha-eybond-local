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
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import (
    CONF_ENTRY_ROLE,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
    ENTRY_ROLE_PENDING_COLLECTOR,
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


def _persisted_anenji_entry(hass: HomeAssistant) -> MockConfigEntry:
    """Return an offline entry with a previously confirmed model surface."""

    entry = _collector_entry(hass)
    data = dict(entry.data)
    data.update(
        {
            "detected_driver": "modbus_smg",
            "detected_model": "Anenji ANJ-11KW-48V-WIFI-P",
            "detected_serial": "92B32500004401",
            "detection_confidence": "high",
        }
    )
    options = dict(entry.options)
    options["effective_metadata_snapshot"] = {
        "effective_owner_key": "modbus_smg",
        "variant_key": "anenji_anj_11kw_48v_wifi_p",
        "profile_name": "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        "register_schema_name": "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        "confidence": "high",
    }
    hass.config_entries.async_update_entry(entry, data=data, options=options)
    return entry


async def test_component_setup_removes_obsolete_pending_entry_without_runtime(
    hass: HomeAssistant,
    fake_runtime,
) -> None:
    """A beta2 incomplete draft is deleted, never loaded or promoted."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Setup Pending",
        unique_id="pending:01TEST00000000000000000000",
        version=5,
        data={
            CONF_ENTRY_ROLE: ENTRY_ROLE_PENDING_COLLECTOR,
            "collector_pn": "",
            "collector_ip": "203.0.113.10",
        },
        options={},
    )
    entry.add_to_hass(hass)

    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    assert hass.config_entries.async_get_entry(entry.entry_id) is None
    assert fake_runtime == []


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


async def test_offline_persisted_model_creates_model_specific_entities_before_refresh(
    hass: HomeAssistant,
    fake_runtime,
    caplog,
) -> None:
    """A fast disconnected refresh cannot erase the setup metadata surface."""

    entry = _persisted_anenji_entry(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    registry = er.async_get(hass)
    unique_ids = {
        entity.unique_id
        for entity in er.async_entries_for_config_entry(registry, entry.entry_id)
    }
    for key in (
        "pv1_voltage",
        "pv1_current",
        "pv1_power",
        "pv2_voltage",
        "pv2_current",
        "pv2_power",
    ):
        assert f"{entry.entry_id}_{key}" in unique_ids
    assert "Timed out waiting to finalize EyeBond expert entity migration" not in caplog.text

    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    reloaded_unique_ids = {
        entity.unique_id
        for entity in er.async_entries_for_config_entry(registry, entry.entry_id)
    }
    assert unique_ids <= reloaded_unique_ids
    assert "Timed out waiting to finalize EyeBond expert entity migration" not in caplog.text

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_setup_unlinks_a_foreign_inverter_child_from_current_collector(
    hass: HomeAssistant,
    fake_runtime,
) -> None:
    """A re-added inverter removes foreign rows through the real HA registry."""

    entry = _persisted_anenji_entry(hass)
    foreign_entry = MockConfigEntry(domain="test", title="Old entry")
    foreign_entry.add_to_hass(hass)
    registry = dr.async_get(hass)
    collector_identifier = (DOMAIN, f"{entry.entry_id}:collector")
    registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={collector_identifier},
        name="Current collector",
    )
    foreign_identifier = (DOMAIN, foreign_entry.entry_id)
    stale = registry.async_get_or_create(
        config_entry_id=foreign_entry.entry_id,
        identifiers={foreign_identifier},
        name="Old inverter",
        via_device=collector_identifier,
    )
    assert hass.config_entries.async_get_entry(foreign_entry.entry_id) is foreign_entry
    assert stale is not None

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    retained_foreign = registry.async_get_device(identifiers={foreign_identifier})
    assert retained_foreign is not None
    assert retained_foreign.via_device_id is None
    assert retained_foreign.config_entries == {foreign_entry.entry_id}
    canonical = registry.async_get_device(identifiers={(DOMAIN, entry.entry_id)})
    assert canonical is not None
    collector = registry.async_get_device(identifiers={collector_identifier})
    assert collector is not None
    assert canonical.via_device_id == collector.id

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


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


async def test_collector_first_entry_is_enriched_by_runtime_detection(
    hass: HomeAssistant,
    fake_runtime,
    monkeypatch,
) -> None:
    """Late runtime identity reloads collector-only platforms exactly once."""

    from conftest import FakeRuntimeManager
    from custom_components.eybond_local.models import (
        CollectorInfo,
        DetectedInverter,
        ProbeTarget,
        RuntimeSnapshot,
    )

    first_refresh_started = asyncio.Event()
    allow_detection = asyncio.Event()

    async def _detected_refresh(
        self,
        *,
        poll_interval: float | None = None,
    ) -> RuntimeSnapshot:
        del self, poll_interval
        first_refresh_started.set()
        await allow_detection.wait()
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(
                remote_ip=SYNTHETIC_COLLECTOR_IP,
                collector_pn=SYNTHETIC_COLLECTOR_PN,
            ),
            inverter=DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(
                    devcode=1,
                    collector_addr=1,
                    device_addr=1,
                ),
            ),
            values={"runtime_detection_status": "autodetected_high_confidence"},
        )

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", _detected_refresh)
    entry = _collector_entry(hass)
    assert entry.data.get("driver_hint") == "auto"
    assert not entry.data.get("detected_model")
    assert not entry.data.get("detected_serial")

    assert await hass.config_entries.async_setup(entry.entry_id)
    await first_refresh_started.wait()

    # Startup is deliberately not held open waiting for inverter detection.
    # The first platform pass is therefore collector-only, and no model entity
    # may be created speculatively from a driver hint.
    assert entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 1
    registry = er.async_get(hass)
    first_unique_ids = {
        entity.unique_id
        for entity in er.async_entries_for_config_entry(registry, entry.entry_id)
    }
    assert f"{entry.entry_id}_collector_pn" in first_unique_ids
    assert f"{entry.entry_id}_driver_key" not in first_unique_ids

    # Runtime detection completes after the platforms are initialized. The
    # coordinator persists the identity and schedules one state-gated reload;
    # the second setup then materializes driver-specific entities from the
    # persisted identity instead of polling in the setup path.
    allow_detection.set()
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 2
    assert entry.data["detected_model"] == "SMG 6200"
    assert entry.data["detected_serial"] == "92632500000001"
    assert entry.data["driver_hint"] == "auto"
    assert entry.data["detected_driver"] == "modbus_smg"
    assert entry.data["detection_confidence"] == "high"
    reloaded_unique_ids = {
        entity.unique_id
        for entity in er.async_entries_for_config_entry(registry, entry.entry_id)
    }
    assert f"{entry.entry_id}_driver_key" in reloaded_unique_ids

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


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
