"""Real HA config-entry updates must not reload an enriched PN on heartbeat."""

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local import _async_update_listener
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.passive_discovery import PassiveCallbackDiscovery


@pytest.mark.parametrize("initial_protocol", ["", "eybond_framed"])
async def test_heartbeat_prefix_retains_identity_and_only_backfills_once(
    hass: HomeAssistant, initial_protocol: str,
) -> None:
    full_pn = "V001107SYN82291016"
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=f"collector:{full_pn}",
        title=f"Collector PN {full_pn}",
        data={"collector_pn": full_pn, "connection_mode": "callback_listener",
              "tcp_port": 18899, "collector_session_protocol": initial_protocol},
    )
    entry.add_to_hass(hass)
    remove_listener = entry.add_update_listener(_async_update_listener)
    discovery = PassiveCallbackDiscovery(hass)
    try:
        with patch.object(hass.config_entries, "async_reload", new_callable=AsyncMock) as reload:
            for pn, source in (
                ("V001107SYN8229", "framed_heartbeat"),
                ("V001107SYN8229", "framed_heartbeat"),
                (full_pn, "fc2_parameter_2"),
            ):
                discovery._maybe_upgrade_existing_entry_from_session(entry, port=18899, session={
                    "collector_pn": pn, "peer_ip": "192.0.2.8",
                    "collector_identity_source": source,
                    "protocol_shape": "eybond_framed", "state": "routed_framed",
                })
                await hass.async_block_till_done()
                assert entry.data["collector_pn"] == full_pn
                assert entry.unique_id == f"collector:{full_pn}"
                assert entry.title == f"Collector PN {full_pn}"
                assert entry.data["collector_session_protocol"] == "eybond_framed"
                # A real protocol migration is still allowed; a PN prefix alone
                # is not a configuration change and must not trigger a reload.
                assert reload.await_count == (0 if initial_protocol else 1)
    finally:
        remove_listener()
