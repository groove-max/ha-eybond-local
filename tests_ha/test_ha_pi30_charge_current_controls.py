"""Device-advertised PI30 controls materialize only with explicit Full Control."""
from dataclasses import replace

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "auto", "full"])
async def test_advertised_currents_survive_setup_reload_without_writes(hass, fake_runtime, monkeypatch, mode):
    from conftest import FakeRuntimeManager

    target = ProbeTarget(0x0994, 1, 0)
    replies = {
        "QPI": "PI30", "QID": "NAK", "QSID": "NAK", "QMN": "NAK",
        "QPIRI": "220.0 14.5 220.0 60.0 13.9 3200 3000 24.0 25.3 24.0 28.7 27.0 2 10 050 1 0 2 6 01 0 0 00.0 0 1",
        "QFLAG": "EabxzDjkuvy", "QMCHGCR": "010 020 030 040 050 060 070 080 090 100 110",
        "QMUCHGCR": "002 010 020 030 040 050 060 070 080",
        "MCHGC040": "ACK", "MCHGC050": "ACK", "MUCHGC002": "ACK", "MUCHGC010": "ACK",
    }
    transport = FixtureTransport(
        registers=None, probe_target=target,
        command_responses={(target.devcode, 1, key): value for key, value in replies.items()},
    )
    inverter = await Pi30Driver().async_probe(transport, target)
    assert inverter is not None

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=True, inverter=inverter,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            values={**inverter.details, "runtime_detection_status": "autodetected_high_confidence"},
        )

    writes = []
    allow_writes = False

    async def write(self, key, value):
        assert allow_writes, "Setup, Full Control and reload must not send writes"
        writes.append((key, value))
        return await Pi30Driver().async_write_capability(transport, inverter, key, value)

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", write)
    entry = MockConfigEntry(
        domain=DOMAIN, title="PI30 test", version=3, unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "control_mode": mode, "driver_hint": "auto", "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external", "detected_driver": "pi30",
            "detected_model": inverter.model_name, "detected_serial": "", "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto", "effective_metadata_snapshot": {
            "effective_owner_key": "pi30", "variant_key": inverter.variant_key,
            "profile_name": inverter.profile_name, "register_schema_name": inverter.register_schema_name,
            "confidence": "high",
        }},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    registry = er.async_get(hass)
    for reload in (False, True):
        if reload:
            assert await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done()
        for key, expected in (("max_charging_current", "50 A"), ("max_ac_charging_current", "10 A")):
            entity_id = registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_{key}")
            if mode != "full":
                assert entity_id is None
            else:
                assert entity_id is not None
                state = hass.states.get(entity_id)
                assert state.state == expected
                assert "120 A" not in state.attributes["options"]
    if mode == "full":
        allow_writes = True
        for key, options in (("max_charging_current", ("40 A", "50 A")),
                             ("max_ac_charging_current", ("2 A", "10 A"))):
            entity_id = registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_{key}")
            for option in options:
                await hass.services.async_call("select", "select_option", {
                    "entity_id": entity_id, "option": option,
                }, blocking=True)
                await entry.runtime_data.async_refresh()
                await hass.async_block_till_done()
                assert hass.states.get(entity_id).state == option
        assert len(writes) == 4
        allow_writes = False
        key = "max_charging_current"
        entity_id = registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_{key}")
        original = inverter
        cap = next(c for c in inverter.capabilities if c.key == key)
        inverter = replace(inverter, capabilities=tuple(
            replace(c, choices=cap.choices[:5]) if c.key == key else c for c in inverter.capabilities
        ))
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert hass.states.get(entity_id).attributes["options"] == ["10 A", "20 A", "30 A", "40 A", "50 A"]
        inverter = replace(inverter, capabilities=tuple(c for c in inverter.capabilities if c.key != key))
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert hass.states.get(entity_id).state == "unavailable"
        inverter = original
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_{key}") == entity_id
        assert hass.states.get(entity_id).state == "50 A"
        registry.async_update_entity(entity_id, disabled_by=er.RegistryEntryDisabler.USER)
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
        assert registry.async_get(entity_id).disabled_by is er.RegistryEntryDisabler.USER
    else:
        with pytest.raises(PermissionError):
            await entry.runtime_data.async_write_capability("max_charging_current", "40 A")
        assert writes == []
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
