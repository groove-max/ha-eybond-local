"""SRNE short-read fallback reaches real HA entities without filling holes."""
from __future__ import annotations

from homeassistant.helpers import entity_registry as er
import pytest

from custom_components.eybond_local.canonical_telemetry import project_canonical_telemetry
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.srne import SrneModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import crc16_modbus, decode_read_request
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("mode", ["read_only", "full"])
async def test_partial_dc_telemetry_survives_reload_and_failed_battery_removes_values(
    hass, collector_entry, fake_runtime, monkeypatch, mode,
):
    from conftest import FakeRuntimeManager

    registers = {r: 0 for start, count in ((53, 20), (516, 4), (528, 18))
                 for r in range(start, start + count)} | {
        **{53 + offset: ord(char) for offset, char in enumerate("SR-TEST".ljust(20))},
        256: 93, 257: 287, 258: (-106) & 0xFFFF,
        263: 2500, 264: 15, 265: 375, 267: 2, 270: 300,
    }

    class ReadOnlyTransport(FixtureTransport):
        async def async_send_payload(self, payload, *, route):
            assert payload[1] == 3
            request = decode_read_request(payload)
            assert request is not None
            if any(address not in self._registers
                   for address in range(request.address, request.address + request.count)):
                # Replay a real Modbus illegal-address response. A missing
                # FixtureTransport key alone is an offline-fixture error, not
                # evidence that would authorize the runtime fallback.
                frame = bytes((request.slave_id, 0x83, 2))
                return frame + crc16_modbus(frame).to_bytes(2, "little")
            return await super().async_send_payload(payload, route=route)

    driver = SrneModbusDriver()
    link = ReadOnlyTransport(registers=registers, input_registers={},
        command_responses=None, probe_target=ProbeTarget(1, 255, 1))
    inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
    assert inverter is not None

    async def refresh(self, **kwargs):
        values = (await driver.async_read_values(link, inverter)).values
        telemetry = project_canonical_telemetry(fold_driver_telemetry(
            TypedTelemetryFrame.empty(), driver_key=driver.key, values=values, replace=True,
        ))
        return RuntimeSnapshot(connected=True, inverter=inverter, telemetry=telemetry,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN))

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    hass.config_entries.async_update_entry(collector_entry, data={**collector_entry.data, "control_mode": mode})
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    # First live detection schedules an entry reload to construct inverter
    # platforms; refresh the replacement coordinator after that lifecycle.
    await collector_entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def state(key):
        entity_id = registry.async_get_entity_id("sensor", DOMAIN, f"{collector_entry.entry_id}_{key}")
        assert entity_id is not None, (key, dict(collector_entry.data), dict(collector_entry.options),
                                      collector_entry.runtime_data.data.inverter)
        return hass.states.get(entity_id)

    try:
        for _ in range(2):
            await collector_entry.runtime_data.async_refresh()
            await hass.async_block_till_done()
            assert state("battery_voltage").state == "28.7", (
                collector_entry.runtime_data.data.connected,
                collector_entry.runtime_data.data.runtime_value("battery_voltage"),
            )
            assert float(state("battery_current").state) == -10.6
            assert "pv2_input_voltage" not in collector_entry.runtime_data.data.values
            assert state("pv2_input_voltage").state in ("unknown", "unavailable")
            assert not collector_entry.runtime_data.data.inverter.capabilities
            assert collector_entry.options["effective_metadata_snapshot"]["surface_key"] == "srne_modbus_read_only"
            assert await hass.config_entries.async_reload(collector_entry.entry_id)
            await hass.async_block_till_done()
        del link._registers[257]
        await collector_entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert state("battery_voltage").state in ("unknown", "unavailable")
        assert state("battery_current").state in ("unknown", "unavailable")
    finally:
        assert await hass.config_entries.async_unload(collector_entry.entry_id)
