"""Real HA lifecycle for the read-only Hopewind string-inverter pack."""
from __future__ import annotations

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["auto", "full"])
@pytest.mark.parametrize("already_detected", [False, True])
async def test_hopewind_setup_reload_and_missing_block_remain_read_only(
        hass, fake_runtime, monkeypatch, mode, already_detected):
    from conftest import FakeRuntimeManager

    class ReadOnlyTransport(FixtureTransport):
        async def async_send_payload(self, payload, *, route):
            assert payload[1] in (3, 4), "Read-only acquisition must never write"
            return await super().async_send_payload(payload, route=route)

    registers = {r: 0 for r in range(40500, 40651)} | {
        40646: 15, 40647: 400, 40546: 1, 40547: 8, 40500: 4300,
        40538: 4999, 40539: 73, 40541: 76, 40544: 372,
        40548: 1930, 40550: 1849, 40551: 62,
    }
    transport = ReadOnlyTransport(registers=registers, input_registers={},
        command_responses=None, probe_target=ProbeTarget(1, 255, 1))
    driver = ModbusCatalogDriver()
    inverter = await driver.async_probe(transport, ProbeTarget(1, 255, 1))
    assert inverter is not None

    def seed_binding(self, driver, binding):
        assert binding.register_schema_name == "hopewind_0237/base.json"
        assert not binding.profile_name and not binding.capabilities
        self.initial_binding = binding

    async def refresh(self, *, poll_interval=None):
        binding = getattr(self, "initial_binding", inverter)
        read = await driver.async_read_values(transport, binding)
        return RuntimeSnapshot(connected=True, inverter=binding, values=read.values,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN))

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "set_initial_inverter_binding", seed_binding, raising=False)
    data = {"connection_type": "eybond", "connection_mode": "known_ip",
        "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
        "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
        "driver_hint": "auto", "control_mode": mode, "connection_strategy": "callback_on_demand",
        "endpoint_control_policy": "external", "proxy_enabled": False}
    if already_detected:
        data.update(detected_driver=driver.key, detected_model=inverter.model_name,
                    detected_serial="", detection_confidence="medium")
    entry = MockConfigEntry(domain=DOMAIN, version=5, data=data,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}", options={"poll_interval": 30, "poll_mode": "auto"})
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    registry = er.async_get(hass)
    assert entry.options.get("effective_metadata_snapshot", {}).get("surface_key") == "hopewind_0237_read_only", (dict(entry.data), dict(entry.options), entry.runtime_data.data.inverter)

    def sensor_id(key):
        return registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")

    for _ in range(2):
        for key, expected in {"inverter_ac_power": 730, "pv_power": 760,
                              "grid_frequency": 49.99, "pv_energy_total": 40650.81}.items():
            entity_id = sensor_id(key)
            assert entity_id is not None, key + " " + str([e.unique_id for e in registry.entities.values()])
            state = hass.states.get(entity_id)
            assert state is not None, (key, registry.async_get(entity_id))
            assert float(state.state) == expected, key
        assert sensor_id("output_power") is None
        assert sensor_id("battery_power") is None
        assert sensor_id("grid_power") is None
        assert not entry.runtime_data.data.inverter.capabilities
        metadata = entry.options["effective_metadata_snapshot"]
        assert metadata["surface_key"] == "hopewind_0237_read_only"
        assert metadata["register_schema_name"] == "hopewind_0237/base.json"
        assert metadata["profile_name"] == ""
        assert registry.async_get(sensor_id("pv8_input_voltage")).disabled_by is er.RegistryEntryDisabler.INTEGRATION
        assert hass.states.get(sensor_id("pv_energy_total")).attributes["unit_of_measurement"] == "kWh"
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
    del transport._registers[40538]
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(sensor_id("grid_frequency")).state in ("unknown", "unavailable")
    assert float(hass.states.get(sensor_id("rated_power")).state) == 15000
    assert await hass.config_entries.async_unload(entry.entry_id)
