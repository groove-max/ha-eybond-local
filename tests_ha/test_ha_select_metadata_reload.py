"""A metadata reload must leave controls on the current runtime (#72)."""

import asyncio
from types import SimpleNamespace

import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers import entity_platform, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.runtime.coordinator.entity_reload import CoordinatorEntityReloadMixin
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "auto", "full"])
@pytest.mark.parametrize("saved_fallback", [False, True])
async def test_controls_follow_runtime_after_metadata_reload(
    hass, fake_runtime, monkeypatch, caplog, mode, saved_fallback,
):
    """Real HA setup/reload/services, with only device reads/writes replaced."""
    from conftest import FakeRuntimeManager

    name = "modbus_smg/models/smg_6200.json"
    profile = load_driver_profile(name)
    keys = {"output_source_priority", "input_voltage_range", "buzzer_mode", "charge_source_priority"}
    values = {key: profile.get_capability(key).choices[0].label for key in keys}
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic SMG",
        serial_number="SYNTHETIC72", probe_target=ProbeTarget(1, 255, 1), variant_key="default",
        profile_name=name, register_schema_name=name, capabilities=profile.capabilities,
        capability_groups=profile.groups, capability_presets=(),
    )
    writes = []
    ready = False

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=not self.stopped,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            inverter=inverter if ready else None,
            values={**values, "load_power": 100, "runtime_detection_status": "autodetected_high_confidence" if ready else "driver_unbound"},
        )

    async def write(self, key, value):
        if self.stopped:
            raise ConnectionError("collector_not_connected")
        writes.append((self, key, value))
        values[key] = value
        return value

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", write)
    entry = MockConfigEntry(
        domain=DOMAIN, title="Synthetic SMG", version=5,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": "modbus_smg", "detected_model": "Synthetic SMG",
            "detected_serial": "SYNTHETIC72", "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    if saved_fallback:
        hass.config_entries.async_update_entry(entry, options={**entry.options, "effective_metadata_snapshot": {
            "effective_owner_key": "modbus_smg", "variant_key": "fallback",
            "profile_name": "modbus_smg/family_fallback.json",
            "register_schema_name": name, "confidence": "high",
        }})
    registry = er.async_get(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    first_coordinator = entry.runtime_data
    first_runtime = fake_runtime[-1]

    ready = True
    await first_coordinator.async_refresh()
    async with asyncio.timeout(5):
        while (
            getattr(entry, "runtime_data", first_coordinator) is first_coordinator
            or entry.state is not ConfigEntryState.LOADED
        ):
            await asyncio.sleep(0)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert first_runtime.stopped == 1

    assert not writes, "Identification and reload must not write to the inverter"
    first_ids = {}
    for reload_again in (False, True):
        if reload_again:
            assert await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done()
        for key in sorted(keys):
            entity_id = registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_{key}")
            if mode == "read_only":
                assert entity_id is None
                continue
            assert entity_id is not None
            assert entity_id == first_ids.setdefault(key, entity_id)
            entities = [p.entities[entity_id] for p in entity_platform.async_get_platforms(hass, DOMAIN)
                        if p.domain == "select" and entity_id in p.entities]
            assert len(entities) == 1
            assert entities[0].coordinator is entry.runtime_data
            # Fresh external (panel/cloud) read, followed by a local HA write.
            values[key] = profile.get_capability(key).choices[0].label
            await entry.runtime_data.async_refresh()
            assert hass.states.get(entity_id).state == values[key]
            target = profile.get_capability(key).choices[-1].label
            await hass.services.async_call(
                "select", "select_option", {"entity_id": entity_id, "option": target}, blocking=True,
            )
            assert writes[-1] == (fake_runtime[-1], key, target)
            assert hass.states.get(entity_id).state == target
    assert len(writes) == (0 if mode == "read_only" else 8)
    assert "does not generate unique IDs" not in caplog.text
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize("state", list(ConfigEntryState))
def test_live_addition_requires_an_active_entry_generation(state):
    owner = CoordinatorEntityReloadMixin()
    owner.config_entry = SimpleNamespace(state=state, runtime_data=owner)
    owner._shutdown_complete = False
    owner._entity_platform_reload_requested = False
    owner._entity_platform_reload_dispatched = False
    assert owner.can_add_live_entities == (state in {
        ConfigEntryState.SETUP_IN_PROGRESS, ConfigEntryState.LOADED,
    })
    owner.config_entry.state = ConfigEntryState.LOADED
    for flag in ("_shutdown_complete", "_entity_platform_reload_requested", "_entity_platform_reload_dispatched"):
        setattr(owner, flag, True)
        assert not owner.can_add_live_entities
        setattr(owner, flag, False)
    owner.config_entry.runtime_data = object()
    assert not owner.can_add_live_entities
    del owner.config_entry.runtime_data
    assert not owner.can_add_live_entities
