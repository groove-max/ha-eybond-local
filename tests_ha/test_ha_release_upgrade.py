"""Representative released-entry upgrades through actual HA lifecycle.

v0.1.53 created schema1 entries and v0.3.0-beta.5 used schema5 (verified against
their tags). Both used entry-id-based sensor unique IDs. These synthetic shapes
test preservation during current setup/reload, not old HA binaries, physical
devices or every possible historic entry. Learning recovery has separate tests.
"""

from __future__ import annotations

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local import _ENTRY_SCHEMA_VERSION
from custom_components.eybond_local.const import DOMAIN
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("schema", [1, 5], ids=["v0.1.53", "v0.3.0-beta.5"])
async def test_release_upgrade_preserves_entity_ids_options_and_user_customization(
    hass, fake_runtime, schema,
):
    """A normal update/reload must not require replacing automation targets."""

    data = {
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
        "detected_driver": "modbus_smg",
        "detected_model": "SMG 6200",
        "detected_serial": "92632500000001",
        "detection_confidence": "high",
    }
    options = {"poll_mode": "manual", "poll_interval": 45}
    if schema == 5:
        data.update({
            "connection_strategy": "inbound",
            "endpoint_control_policy": "external",
            "proxy_enabled": False,
        })
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Existing installation",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=schema,
        data=data,
        options=options,
    )
    entry.add_to_hass(hass)
    old_entry_id = entry.entry_id
    registry = er.async_get(hass)
    old_entities = {}
    for key in ("battery_voltage", "grid_voltage"):
        disabled = er.RegistryEntryDisabler.USER if key == "grid_voltage" else None
        entity = registry.async_get_or_create(
            "sensor", DOMAIN, f"{entry.entry_id}_{key}",
            config_entry=entry, suggested_object_id=f"my_existing_{key}",
            disabled_by=disabled,
        )
        registry.async_update_entity(entity.entity_id, name=f"My {key}")
        old_entities[key] = (entity.entity_id, disabled)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    def assert_preserved():
        assert entry.entry_id == old_entry_id
        assert entry.unique_id == f"collector:{SYNTHETIC_COLLECTOR_PN}"
        assert entry.version == _ENTRY_SCHEMA_VERSION
        assert entry.state is ConfigEntryState.LOADED
        assert entry.data["collector_pn"] == SYNTHETIC_COLLECTOR_PN
        assert entry.data["collector_ip"] == SYNTHETIC_COLLECTOR_IP
        assert entry.data["tcp_port"] == 8899
        assert entry.data["udp_port"] == 58899
        assert entry.data["control_mode"] == "read_only"
        assert entry.options["poll_mode"] == "manual"
        assert entry.options["poll_interval"] == 45
        for key, (entity_id, disabled) in old_entities.items():
            current = registry.async_get(entity_id)
            assert current is not None
            assert current.config_entry_id == old_entry_id
            assert current.unique_id == f"{old_entry_id}_{key}"
            assert current.name == f"My {key}"
            assert current.disabled_by == disabled
            matching = [e for e in er.async_entries_for_config_entry(registry, old_entry_id)
                        if e.unique_id == current.unique_id]
            assert len(matching) == 1
            assert matching[0].entity_id == entity_id

    assert_preserved()
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert_preserved()
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert fake_runtime
    assert all(runtime.stopped >= runtime.started for runtime in fake_runtime)
