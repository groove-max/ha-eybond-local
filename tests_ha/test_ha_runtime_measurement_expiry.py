"""Actual HA entity availability/energy after an inverter-only read outage."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers import entity_registry as er

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.derived_energy import (
    DerivedEnergyCycleDescription, DerivedEnergyDescription, DerivedEnergyVariant,
)
from custom_components.eybond_local.drivers.read_result import DriverReadMode, DriverReadResult
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.hub import snapshot as snapshot_module
from custom_components.eybond_local.sensor import EybondDerivedEnergyCycleSensor, EybondDerivedEnergySensor
from tests import test_hub as hub_tests
from test_ha_setup_unload import _collector_entry


@pytest.fixture
async def expiring_runtime(hass, fake_runtime, monkeypatch):
    from conftest import FakeRuntimeManager

    hub = hub_tests.HubEvidenceAdmissionAndRecoveryTests().make_hub()
    now = [100.0]
    monkeypatch.setattr(snapshot_module, "monotonic", lambda: now[0])
    reading = AsyncMock(return_value=DriverReadResult({"battery_voltage": 52.3, "output_power": 1600}))
    monkeypatch.setattr(hub._driver, "async_read_values", reading)

    async def refresh(self, *, poll_interval=None):
        return await hub.async_refresh(poll_interval=10)

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = _collector_entry(hass)
    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, "driver_hint": "srne_modbus", "detected_driver": "srne_modbus",
              "detected_model": hub._inverter.model_name, "detection_confidence": "high"},
        options={"poll_interval": 10, "poll_mode": "manual"},
    )
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    yield entry, hub, now, reading
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_live_collector_does_not_keep_stale_inverter_entities_available(hass, expiring_runtime):
    entry, hub, now, reading = expiring_runtime
    registry = er.async_get(hass)
    voltage = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_battery_voltage")
    power = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_output_power")
    assert voltage and power
    assert float(hass.states.get(voltage).state) == 52.3
    reading.side_effect = ModbusError("request_timeout")
    now[0] = 159
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert float(hass.states.get(voltage).state) == 52.3
    now[0] = 160
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hub._last_snapshot.connected
    assert hass.states.get(voltage).state == "unavailable"
    assert hass.states.get(power).state == "unavailable"
    assert hub._link_manager.reset_calls == 0

    reading.side_effect = None
    reading.return_value = DriverReadResult({"battery_voltage": 52.4}, mode=DriverReadMode.DELTA)
    now[0] = 180
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert float(hass.states.get(voltage).state) == 52.4
    assert hass.states.get(power).state == "unavailable"


@pytest.mark.parametrize("cycle", [None, "daily", "monthly"])
async def test_expired_power_resets_energy_sample_without_erasing_total(
    expiring_runtime, monkeypatch, cycle,
):
    entry, hub, now, reading = expiring_runtime
    description = DerivedEnergyDescription(
        key="expiry_test_energy", name="Expiry Test Energy", icon="mdi:flash",
        variants=(DerivedEnergyVariant(("output_power",), "passthrough"),),
    )
    if cycle is None:
        entity = EybondDerivedEnergySensor(entry.runtime_data, description)
    else:
        entity = EybondDerivedEnergyCycleSensor(
            entry.runtime_data,
            DerivedEnergyCycleDescription(
                key=f"expiry_test_{cycle}", name="Expiry Test", icon="mdi:flash",
                source_key=description.key, cycle=cycle,
            ), description,
        )
    monkeypatch.setattr(entity, "async_write_ha_state", Mock())
    timestamp = datetime.now(timezone.utc)
    entity._accumulator.accumulate(1600, timestamp - timedelta(seconds=10))
    entity._handle_coordinator_update()
    assert entity.available
    total = entity.native_value
    assert total > 0

    reading.side_effect = ModbusError("request_timeout")
    now[0] = 160
    await entry.runtime_data.async_refresh()
    entity._handle_coordinator_update()
    assert not entity.available
    assert entity._accumulator._prev_time is None
    assert entity.native_value == total

    reading.side_effect = None
    now[0] = 18100
    await entry.runtime_data.async_refresh()
    entity._handle_coordinator_update()
    assert entity.available
    assert entity.native_value == total  # first fresh sample must not bridge the gap
