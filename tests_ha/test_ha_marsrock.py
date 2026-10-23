"""Manual MarsRock entities, truthful naming and failed-read recovery in HA."""

import json
from pathlib import Path

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.marsrock import MarsRockDriver
from custom_components.eybond_local.models import CollectorInfo, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import (
    ModbusError, crc16_modbus, parse_read_holding_response,
)
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "full"])
async def test_marsrock_manual_setup_read_failure_reload(hass, fake_runtime, monkeypatch, mode):
    from conftest import FakeRuntimeManager

    fixture = json.loads((Path(__file__).parents[1] / "tests/fixtures/marsrock_read_observation.json").read_text())
    monkeypatch.setattr("custom_components.eybond_local.drivers.marsrock.SUPPORT_READ_SPACING_SECONDS", 0)

    class Transport:
        fail = False
        cycle = 0

        async def async_send_payload(self, payload, *, route, request_timeout=None):
            assert route == MarsRockDriver.probe_targets[0].link_route
            assert payload[1] == 3, "Neither setup nor full mode authorizes writes"
            if self.fail:
                raise ModbusError("synthetic_read_failure")
            row = next((r for r in fixture["records"] if r["cycle"] == self.cycle
                        and bytes.fromhex(r["request_hex"]) == payload), None)
            if row is not None:
                return bytes.fromhex(row["response_hex"])
            start, count = int.from_bytes(payload[2:4], "big"), int.from_bytes(payload[4:6], "big")
            if count == 1 and start in (60, 94, 99, 107, 110):
                live = next(r for r in fixture["records"] if r["cycle"] == self.cycle and r["start"] == 59)
                words = parse_read_holding_response(bytes.fromhex(live["response_hex"]), slave_id=1, count=66)
                body = bytes([1, 3, 2]) + words[start - 59].to_bytes(2, "big")
            else:
                assert (start, count) in ((151, 10), (271, 7), (305, 2), (331, 2), (451, 5))
                body = bytes([1, 0x83, 2])
            return body + crc16_modbus(body).to_bytes(2, "little")

    driver, transport = MarsRockDriver(), Transport()
    inverter = await driver.async_probe(transport, driver.probe_targets[0])
    assert inverter is not None

    async def refresh(self, *, poll_interval=None):
        try:
            read = await driver.async_read_values(transport, inverter)
        except ModbusError:
            return RuntimeSnapshot(connected=False, inverter=inverter)
        return RuntimeSnapshot(connected=True, inverter=inverter,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            telemetry=fold_driver_telemetry(TypedTelemetryFrame.empty(), driver_key=driver.key,
                                           values=read.values, replace=True))

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)

    async def capture(self):
        # Real coordinator export must hold the same operation lock as polling.
        assert entry.runtime_data._runtime_operation_lock.locked()
        return await driver.async_capture_support_evidence(transport, inverter)

    monkeypatch.setattr(FakeRuntimeManager, "async_capture_support_evidence", capture)
    entry = MockConfigEntry(domain=DOMAIN, version=5, title=inverter.model_name,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}", data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": driver.key, "control_mode": mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": driver.key, "detected_model": inverter.model_name,
            "detected_serial": inverter.serial_number, "detection_confidence": "high",
        }, options={"poll_interval": 30, "poll_mode": "manual"})
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def entity_id(key):
        return registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")

    expected = {"grid_voltage": 235, "dc_input_voltage": 20.5,
                "generation_power": 0.6, "generation_energy_total": 1.2}
    ids = {key: entity_id(key) for key in expected}
    for key, value in expected.items():
        assert ids[key] is not None
        assert float(hass.states.get(ids[key]).state) == value
    assert hass.states.get(ids["generation_energy_total"]).attributes["state_class"] == "total_increasing"
    assert registry.async_get(ids["generation_power"]).original_name == "Generation Power (Protocol)"
    for key in ("output_power", "pv_power", "battery_power", "grid_frequency", "inverter_temperature",
                "estimated_load_energy_daily", "estimated_pv_energy_daily"):
        assert entity_id(key) is None
    inverter_device = registry.async_get(ids["grid_voltage"]).device_id
    assert not [e for e in registry.entities.values() if e.device_id == inverter_device
                and e.domain in {"number", "select", "switch"}]
    assert not inverter.capabilities

    _, evidence = await entry.runtime_data._async_build_support_package_payloads(
        integration_build_values={},
    )
    assert evidence["status"] == "completed"
    assert len(evidence["candidate_comparisons"]) == 5
    assert len(evidence["range_failures"]) == 5
    assert len(evidence["fixture_ranges"]) == 2
    assert not entry.runtime_data._runtime_operation_lock.locked()
    assert all(entity_id(key) is None for key in ("inverter_temperature", "limiter_power", "dc_input_current"))

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert {key: entity_id(key) for key in expected} == ids
    transport.fail = True
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert all(hass.states.get(i).state == "unavailable" for i in ids.values())
    transport.fail, transport.cycle = False, 1
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert float(hass.states.get(ids["generation_power"]).state) == 0
    assert float(hass.states.get(ids["grid_voltage"]).state) == 234.7
    assert float(hass.states.get(ids["generation_energy_total"]).state) == 1.2
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
