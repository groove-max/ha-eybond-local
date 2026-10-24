"""First SMG identification must not race the old select platform (#66)."""

import asyncio

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "auto", "full"])
async def test_late_smg_identity_creates_selects_once(
    hass, fake_runtime, monkeypatch, caplog, mode,
):
    """Use the real coordinator/reload/registry; fake only device polling."""
    from conftest import FakeRuntimeManager

    name = "modbus_smg/models/smg_6200.json"
    profile = load_driver_profile(name)
    keys = {
        "output_source_priority", "input_voltage_range", "buzzer_mode",
        "turn_on_mode", "charge_source_priority",
    }
    values = {
        key: profile.get_capability(key).choices[0].label for key in keys
    }
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Anenji 6200", serial_number="SYNTHETIC66",
        probe_target=ProbeTarget(1, 255, 1), variant_key="default",
        profile_name=name, register_schema_name=name,
        capabilities=profile.capabilities, capability_groups=profile.groups,
        capability_presets=profile.presets,
    )
    ready = False

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(
                remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN,
            ),
            inverter=inverter if ready else None,
            values={**values, "runtime_detection_status": (
                "autodetected_high_confidence" if ready else "driver_unbound"
            )},
        )

    async def unexpected_write(*args, **kwargs):
        pytest.fail("Identification and entity reload must never write settings")

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", unexpected_write)
    entry = MockConfigEntry(
        domain=DOMAIN, title="EyeBond Collector", version=3,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    registry = er.async_get(hass)

    def select_ids():
        return {
            key: registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_{key}")
            for key in keys
        }

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert len(fake_runtime) == 1
    assert all(value is None for value in select_ids().values())

    ready = True
    await entry.runtime_data.async_refresh()
    # The real coordinator dispatches its identity reload on the next loop tick.
    async with asyncio.timeout(5):
        while len(fake_runtime) < 2:
            await asyncio.sleep(0)
    await hass.async_block_till_done()
    assert len(fake_runtime) == 2
    assert entry.state is ConfigEntryState.LOADED
    assert entry.data["detected_driver"] == "modbus_smg"
    assert entry.data["control_mode"] == mode
    assert "does not generate unique IDs" not in caplog.text
    first_ids = select_ids()
    if mode == "read_only":
        assert all(value is None for value in first_ids.values())
    else:
        assert all(first_ids.values())
        assert len(set(first_ids.values())) == len(keys)
        for key, entity_id in first_ids.items():
            assert hass.states.get(entity_id).state == values[key]

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert select_ids() == first_ids
    assert "does not generate unique IDs" not in caplog.text
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED
