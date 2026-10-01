"""Shared PI30 label migration through real HA setup, refresh and reload."""
from dataclasses import replace

import pytest
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.runtime.hub.common import _inverter_identities_conflict
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("saved_metadata", [False, True])
async def test_vmii_legacy_label_preserves_entities_and_user_names(
    hass, fake_runtime, monkeypatch, saved_metadata,
):
    from conftest import FakeRuntimeManager

    target = ProbeTarget(0x0994, 1, 0)
    replies = {
        "QPI": "PI30", "QMN": "VMII-NXPW5KW", "QID": "99000000000054",
        "QPIRI": "230.0 26.9 230.0 50.0 26.9 6200 6200 48.0 46.0 42.0 55.2 54.6 2 030 030 1 0 1 1 01 0 0 54.0 0 1",
        "QFLAG": "EabkuxzDjvy", "QMOD": "L",
        "QPIWS": "00000000000000000000000000000000",
        "QPIGS": "232.1 50.0 232.1 50.0 0440 0227 007 429 54.60 000 100 0043 00.0 000.0 00.00 00000 00010101 00 00 00000 110",
        "Q1": "00 00 00 000 042 030 043 00 00 000 0030 0000 13",
        "QVFW": "VERFW:00021.12", "QVFW2": "VERFW2:00000.00",
        "QVFW3": "NAK", "QET": "NAK", "QLT": "NAK", "QT": "NAK",
    }

    class ReadOnlyTransport(FixtureTransport):
        async def async_send_payload(self, payload, *, route):
            assert payload[:-3].decode("ascii").startswith("Q"), "No automatic inverter writes"
            return await super().async_send_payload(payload, route=route)

    transport = ReadOnlyTransport(
        registers=None,
        command_responses={(target.devcode, target.collector_addr, k): v for k, v in replies.items()},
        probe_target=target,
    )
    driver = Pi30Driver()
    inverter = await driver.async_probe(transport, target)
    assert inverter.model_name == "PI30 VMII-NXPW5KW"
    # Even without a trustworthy serial, a declared display-name migration
    # must not trigger the different-physical-inverter guard.
    legacy = replace(inverter, model_name="PowMr 4.2kW", serial_number="")
    assert not _inverter_identities_conflict(legacy, replace(inverter, serial_number=""))
    assert _inverter_identities_conflict(legacy, replace(inverter, model_name="Unrelated model", serial_number=""))

    live = False

    def seed(self, _driver, binding):
        self.initial_binding = binding

    async def refresh(self, *, poll_interval=None):
        if not live:
            return RuntimeSnapshot(connected=False, values={})
        values = (await driver.async_read_values(transport, inverter)).values
        return RuntimeSnapshot(
            connected=True, inverter=inverter, values=values,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
        )

    monkeypatch.setattr(FakeRuntimeManager, "set_initial_inverter_binding", seed, raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        title="My installation",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": "full",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": "pi30", "detected_model": "PowMr 4.2kW",
            "detected_serial": "", "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto"} | ({
            "effective_metadata_snapshot": {
                "effective_owner_key": "pi30", "variant_key": "vmii_nxpw5kw",
                "profile_name": inverter.profile_name,
                "register_schema_name": inverter.register_schema_name, "confidence": "high",
            },
        } if saved_metadata else {}),
    )
    entry.add_to_hass(hass)
    devices = dr.async_get(hass)
    old_device = devices.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, entry.entry_id)},
        name="PowMr 4.2kW", model="PowMr 4.2kW",
    )
    devices.async_update_device(old_device.id, name_by_user="My Victor")
    entities = er.async_get(hass)
    old_entity = entities.async_get_or_create(
        "select", DOMAIN, f"{entry.entry_id}_select_output_source_priority",
        config_entry=entry, device_id=old_device.id, suggested_object_id="my_output_priority",
    )
    entities.async_update_entity(old_entity.entity_id, name="My priority")

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert fake_runtime[-1].initial_binding.profile_name == inverter.profile_name
    assert fake_runtime[-1].initial_binding.variant_key == "vmii_nxpw5kw"

    def inverter_entities():
        return {e.unique_id: e.entity_id for e in er.async_entries_for_config_entry(entities, entry.entry_id)
                if e.device_id == old_device.id}

    before = inverter_entities()
    assert before[f"{entry.entry_id}_select_output_source_priority"] == old_entity.entity_id
    assert f"{entry.entry_id}_battery_voltage" in before
    live = True
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert entry.data["detected_model"] == inverter.model_name
    assert entry.data["control_mode"] == "full"
    assert devices.async_get(old_device.id).model == inverter.model_name
    assert devices.async_get(old_device.id).name_by_user == "My Victor"
    assert inverter_entities() == before
    assert entities.async_get(old_entity.entity_id).name == "My priority"

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert inverter_entities() == before
    assert devices.async_get(old_device.id).name_by_user == "My Victor"
    assert entities.async_get(old_entity.entity_id).name == "My priority"
    owned = dr.async_entries_for_config_entry(devices, entry.entry_id)
    assert len([d for d in owned if (DOMAIN, entry.entry_id) in d.identifiers]) == 1
    assert await hass.config_entries.async_unload(entry.entry_id)
