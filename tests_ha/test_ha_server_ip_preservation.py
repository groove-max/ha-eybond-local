"""Preserve explicit routed/NAT callback settings across real HA lifecycle."""
from unittest.mock import patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.integration_metadata import _async_self_heal_server_ip
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("override", [False, True])
async def test_setup_and_reload_do_not_persist_transient_network_choices(
    hass, collector_entry, fake_runtime, override,
):
    data = dict(collector_entry.data, server_ip="192.0.2.94", collector_ip="198.51.100.20")
    options = dict(collector_entry.options, advertised_server_ip="203.0.113.8", advertised_tcp_port=18899)
    if override:
        options["server_ip"] = "192.0.2.95"
    hass.config_entries.async_update_entry(collector_entry, data=data, options=options)
    with patch("custom_components.eybond_local.network_interfaces.get_ipv4_interfaces", return_value=[]), patch(
        "custom_components.eybond_local.runtime.link.common._default_local_ip", return_value="172.30.32.1",
    ):
        assert await hass.config_entries.async_setup(collector_entry.entry_id)
        await hass.async_block_till_done()
        assert dict(collector_entry.data)["server_ip"] == data["server_ip"]
        assert dict(collector_entry.options) == options
        assert await hass.config_entries.async_reload(collector_entry.entry_id)
        await hass.async_block_till_done()
        assert collector_entry.data["server_ip"] == "192.0.2.94"
        assert dict(collector_entry.options) == options
        assert await hass.config_entries.async_unload(collector_entry.entry_id)
        await hass.async_block_till_done()


async def test_missing_address_waits_for_lan_then_initializes_once(hass):
    entry = MockConfigEntry(domain="eybond_local", data={"collector_ip": "192.0.2.4"},
                            options={"collector_ip": "198.51.100.4", "server_ip": ""})
    entry.add_to_hass(hass)
    with patch("custom_components.eybond_local.network_interfaces.get_ipv4_interfaces", return_value=[]):
        await _async_self_heal_server_ip(hass, entry)
    assert "server_ip" not in entry.data
    assert entry.options["server_ip"] == ""
    interfaces = [
        {"ip": "192.0.2.20", "prefixlen": "24"},
        {"ip": "198.51.100.20", "prefixlen": "24"},
    ]
    with patch("custom_components.eybond_local.network_interfaces.get_ipv4_interfaces", return_value=interfaces):
        await _async_self_heal_server_ip(hass, entry)
    # Options collector target takes precedence over obsolete entry data.
    assert entry.data["server_ip"] == entry.options["server_ip"] == "198.51.100.20"
    with patch("custom_components.eybond_local.runtime.link.resolve_server_ip") as resolve:
        await _async_self_heal_server_ip(hass, entry)
        resolve.assert_not_called()


async def test_non_eybond_entry_is_not_rewritten(hass):
    entry = MockConfigEntry(domain="eybond_local", data={"connection_type": "modbus_tcp"})
    entry.add_to_hass(hass)
    with patch("custom_components.eybond_local.runtime.link.resolve_server_ip") as resolve:
        await _async_self_heal_server_ip(hass, entry)
        resolve.assert_not_called()
