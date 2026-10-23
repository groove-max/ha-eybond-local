"""Unavailable power must neither erase energy nor integrate through a gap."""

from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest
from homeassistant.core import State
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import restore_state as rs
from homeassistant.helpers.json import json_bytes
from pytest_homeassistant_custom_component.common import MockConfigEntry, mock_restore_cache

from custom_components.eybond_local.canonical_telemetry import project_canonical_telemetry
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("restored_state", ["12.5", "unavailable"])
async def test_export_gap_reload_recovery_and_day_rollover(hass, fake_runtime, monkeypatch, restored_state):
    from conftest import FakeRuntimeManager
    from custom_components.eybond_local import sensor as platform

    now = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(platform, "dt_util", SimpleNamespace(now=lambda: now))
    inverter = DetectedInverter("pi30", "pi30", "Synthetic PI30", "", ProbeTarget(0x0994, 1, 0))
    export = None
    connected = True

    async def refresh(self, *, poll_interval=None):
        values = {"grid_voltage": 230, "output_power": 200, "pv_power": 1200,
                  "battery_power": 0, "solar_feed_to_grid_enabled": True}
        if export is not None:
            values["solar_feed_to_grid_power"] = export
        frame = project_canonical_telemetry(fold_driver_telemetry(
            TypedTelemetryFrame.empty(), driver_key="pi30", values=values, replace=True,
        ))
        return RuntimeSnapshot(
            connected=connected, inverter=inverter, telemetry=frame,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
        )

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(domain=DOMAIN, version=5, title=inverter.model_name,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}", data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "pi30", "control_mode": "read_only",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": "pi30", "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        }, options={"poll_interval": 30, "poll_mode": "manual"})
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    daily = registry.async_get_or_create(
        "sensor", DOMAIN, f"{entry.entry_id}_estimated_grid_export_energy_daily",
        config_entry=entry, suggested_object_id="retained_export", disabled_by=None,
    )
    mock_restore_cache(hass, [State(daily.entity_id, restored_state, {
        "period_key": now.date().isoformat(),
        "unit_of_measurement": "kWh",
    })])
    if restored_state == "unavailable":
        rs.async_get(hass).last_states[daily.entity_id].extra_data = rs.RestoredExtraData({
            "accumulated_kwh": 12.5, "period_key": now.date().isoformat(),
        })

    async def update():
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        return hass.states.get(daily.entity_id)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    state = await update()
    assert state.state == "unavailable"
    # Exercise HA's actual removal/restore cache, not just the helper function.
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    state = await update()
    assert state.state == "unavailable"

    export = 1000
    now += timedelta(minutes=10)
    assert float((await update()).state) == 12.5
    now += timedelta(minutes=30)
    assert float((await update()).state) == 13
    export = None
    now += timedelta(minutes=5)
    assert (await update()).state == "unavailable"
    # A cold restart serializes extra restore data separately from the HA
    # state, whose unavailable form deliberately omits custom attributes.
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    cache = rs.async_get(hass)
    stored = cache.last_states[daily.entity_id]
    assert stored.state.state == "unavailable"
    payload = json.loads(json_bytes(stored.as_dict()))
    assert payload["extra_data"]["accumulated_kwh"] == 13
    cache.last_states[daily.entity_id] = rs.StoredState.from_dict(payload)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    export = 1000
    now += timedelta(minutes=10)
    assert float((await update()).state) == 13  # no interpolation across the gap
    connected = False
    assert (await update()).state == "unavailable"
    connected = True
    now += timedelta(minutes=5)
    assert float((await update()).state) == 13
    export = 0
    assert float((await update()).state) == 13  # genuine zero is available
    now += timedelta(days=1)
    assert float((await update()).state) == 0
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize("value", [None, True, "bad", "nan", "inf", -1])
def test_invalid_retained_counter_is_not_restored(value):
    from custom_components.eybond_local.sensor import _restored_energy_total
    assert _restored_energy_total(State("sensor.test", "unavailable"), {"accumulated_kwh": value}) is None
