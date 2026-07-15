"""Config entry migration through the REAL Home Assistant entry lifecycle.

`async_migrate_entry` is invoked by Home Assistant itself (not called directly),
so this exercises the genuine version gate, `async_update_entry(version=...)`
handling, and post-migration entry state.
"""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local import _ENTRY_SCHEMA_VERSION
from custom_components.eybond_local.const import DOMAIN
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


def _legacy_v1_entry(hass: HomeAssistant) -> MockConfigEntry:
    """A version-1 entry as shipped before the explicit connection axes."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Legacy Collector",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=1,
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
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    return entry


async def test_legacy_entry_migrates_through_real_lifecycle(
    hass: HomeAssistant, fake_runtime
) -> None:
    """HA runs our migration and the entry lands on the current schema."""

    entry = _legacy_v1_entry(hass)
    assert entry.version == 1

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    # Home Assistant performed the migration as part of setup.
    assert entry.version == _ENTRY_SCHEMA_VERSION
    assert entry.state is ConfigEntryState.LOADED

    data = entry.data
    # The three explicit connection axes are now persisted.
    assert data["connection_strategy"] in ("inbound", "callback_on_demand")
    assert data["endpoint_control_policy"] in ("external", "integration_managed")
    assert data["proxy_enabled"] is False

    # Durable identity survives migration untouched.
    assert data["collector_pn"] == SYNTHETIC_COLLECTOR_PN
    assert entry.unique_id == f"collector:{SYNTHETIC_COLLECTOR_PN}"

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_v3_conflicting_strategy_is_canonicalized_with_real_ha(
    hass: HomeAssistant, fake_runtime
) -> None:
    """HA preserves the old effective value, then removes the options shadow."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Conflicting legacy strategy",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=3,
        data={
            "connection_type": "eybond",
            "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "collector_operation_mode": "home_assistant_only",
            "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external",
            "proxy_enabled": False,
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
            "control_mode": "read_only",
        },
        # Before schema v4 options won. Preserve that effective behavior once,
        # then remove this duplicate source of truth.
        options={
            "connection_strategy": "inbound",
            "poll_interval": 30,
            "poll_mode": "auto",
        },
    )
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.version == _ENTRY_SCHEMA_VERSION
    assert entry.data["connection_strategy"] == "inbound"
    assert "connection_strategy" not in entry.options
    assert entry.state is ConfigEntryState.LOADED

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_migration_does_not_derive_ownership_from_address(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Ownership/identity must never be derived from hostname or peer IP.

    Two legacy entries behind the SAME address but with DIFFERENT durable PNs
    must migrate to two independent identities.
    """

    first = MockConfigEntry(
        domain=DOMAIN,
        title="Collector A",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=1,
        data={
            "connection_type": "eybond",
            "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,  # same address ...
            "collector_pn": SYNTHETIC_COLLECTOR_PN,  # ... different identity
            "collector_operation_mode": "home_assistant_only",
        },
    )
    first.add_to_hass(hass)

    from synthetic import SYNTHETIC_OTHER_COLLECTOR_PN

    second = MockConfigEntry(
        domain=DOMAIN,
        title="Collector B",
        unique_id=f"collector:{SYNTHETIC_OTHER_COLLECTOR_PN}",
        version=1,
        data={
            "connection_type": "eybond",
            "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,  # same address ...
            "collector_pn": SYNTHETIC_OTHER_COLLECTOR_PN,  # ... different identity
            "collector_operation_mode": "home_assistant_only",
        },
    )
    second.add_to_hass(hass)

    # Setting up the component makes Home Assistant load BOTH entries itself.
    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    assert first.state is ConfigEntryState.LOADED
    assert second.state is ConfigEntryState.LOADED
    assert first.version == _ENTRY_SCHEMA_VERSION
    assert second.version == _ENTRY_SCHEMA_VERSION

    assert first.data["collector_pn"] != second.data["collector_pn"]
    assert first.unique_id != second.unique_id
    # The shared address did not collapse or cross-bind the two identities.
    assert first.data["collector_ip"] == second.data["collector_ip"]

    for entry in (first, second):
        await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_future_schema_version_is_refused(hass: HomeAssistant) -> None:
    """An entry newer than this code refuses to migrate rather than corrupt."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="From the future",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=_ENTRY_SCHEMA_VERSION + 1,
        data={"connection_type": "eybond", "collector_pn": SYNTHETIC_COLLECTOR_PN},
    )
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.MIGRATION_ERROR
