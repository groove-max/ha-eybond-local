"""MUST control admission and BMS availability through real HA lifecycle."""
from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.must import MustPvPh18Driver, _support_capture_ranges
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.schema import entity_kind_for_capability
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "auto", "full"])
@pytest.mark.parametrize("upgrade", [False, True])
@pytest.mark.parametrize("has_bms", [False, True])
async def test_must_unverified_controls_and_bms(hass, fake_runtime, monkeypatch, mode, upgrade, has_bms):
    from conftest import FakeRuntimeManager

    registers = {reg: 0 for start, count in _support_capture_ranges("must_pv_ph18/base.json")
                 for reg in range(start, start + count)} | {
        20000: int.from_bytes(b"PV", "big"), 20001: 3300,
        20101: 1, 20109: 1, 20125: 100, 20143: 0, 25205: 512,
        25210: 1, 25211: 1, 25212: 1,
        109: 512, 110: 65526, 111: 25, 112: 0, 113: 73,
    }
    if not has_bms:
        registers.update(dict.fromkeys(range(109, 114), 0))
    clock = [100.0]
    capture_read_counts = []

    class ReadOnlyTransport(FixtureTransport):
        requests = None

        async def async_send_payload(self, payload, *, route):
            assert payload[1] == 3, "Setup/reload/support export must never write"
            self.requests.append((int.from_bytes(payload[2:4], "big"), int.from_bytes(payload[4:6], "big")))
            return await super().async_send_payload(payload, route=route)

    driver = MustPvPh18Driver()
    target = ProbeTarget(1, 255, 4)
    transport = ReadOnlyTransport(registers=registers, command_responses=None, probe_target=target)
    transport.requests = []
    inverter = await driver.async_probe(transport, target)
    assert inverter is not None

    def seed_binding(self, driver, binding):
        self.initial_binding = binding

    async def refresh(self, *, poll_interval=None):
        binding = getattr(self, "initial_binding", inverter)
        if not hasattr(self, "bms_state"):
            self.bms_state = {}
        read = await driver.async_read_values(transport, binding, runtime_state=self.bms_state,
                                              now_monotonic=clock[0])
        return RuntimeSnapshot(connected=True, inverter=binding, values=read.diagnostics,
            telemetry=fold_driver_telemetry(TypedTelemetryFrame.empty(), driver_key=driver.key,
                                           values=read.values, replace=True),
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN))

    async def capture(self):
        before = transport.requests.count((109, 5))
        evidence = await driver.async_capture_support_evidence(transport, getattr(self, "initial_binding", inverter))
        capture_read_counts.append(transport.requests.count((109, 5)) - before)
        return evidence

    async def unexpected_write(*args, **kwargs):
        pytest.fail("Blocked controls or automatic setup must not reach the write transport")

    monkeypatch.setattr(FakeRuntimeManager, "set_initial_inverter_binding", seed_binding, raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_capture_support_evidence", capture)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", unexpected_write)
    entry = MockConfigEntry(domain=DOMAIN, version=5,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={"connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": mode, "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external", "proxy_enabled": False,
            "detected_driver": driver.key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high"},
        options={"poll_interval": 30, "poll_mode": "auto"} | ({
            "effective_metadata_snapshot": {
                "effective_owner_key": driver.key, "confidence": "high", "variant_key": "pv3300",
                "profile_name": "must_pv_ph18/base.json", "register_schema_name": "must_pv_ph18/pv3300.json",
            },
        } if upgrade else {}))
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    old_id = None
    if upgrade:
        old_id = registry.async_get_or_create("select", DOMAIN, f"{entry.entry_id}_select_energy_use_mode",
            config_entry=entry, suggested_object_id="existing_must_priority").entity_id
        registry.async_update_entity(old_id, name="My priority")
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    for _ in range(2):
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        capabilities = entry.runtime_data.data.inverter.capabilities
        assert len(capabilities) == 27 and all(not c.tested for c in capabilities)
        for capability in capabilities:
            kind = entity_kind_for_capability(capability)
            entity_id = registry.async_get_entity_id(kind, DOMAIN, f"{entry.entry_id}_{kind}_{capability.key}")
            if mode == "full":
                assert entity_id is not None, capability.key
                assert hass.states.get(entity_id) is not None
            else:
                assert entity_id is None, capability.key
                with pytest.raises(PermissionError, match="capability_control_disabled"):
                    await entry.runtime_data.async_write_capability(capability.key, 1)
        if mode == "full" and upgrade:
            assert registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_energy_use_mode") == old_id
            assert registry.async_get(old_id).name == "My priority"
        assert (109, 5) in transport.requests
        for key, value in {"battery_soc": "73", "bms_battery_voltage": "51.2",
                           "bms_battery_current": "-1.0", "bms_battery_temperature": "25"}.items():
            entity_id = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")
            assert entity_id is not None
            assert hass.states.get(entity_id).state == (value if has_bms else "unavailable")
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()

    soc_id = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_battery_soc")
    registry.async_update_entity(soc_id, name="My BMS SOC")
    # Losing BMS data must withdraw old readings without losing the inverter.
    transport._registers.update(dict.fromkeys(range(109, 114), 0))
    clock[0] += 61
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(soc_id).state == "unavailable"
    voltage_id = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_battery_voltage")
    assert hass.states.get(voltage_id).state == "51.2"
    # Valid zero percent is distinct from an absent BMS; recovery is automatic.
    transport._registers.update({109: 512, 110: 65526, 111: 25, 112: 0, 113: 0})
    clock[0] += 61
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(soc_id).state == "0"
    assert registry.async_get(soc_id).name == "My BMS SOC"
    archive_path = Path(await entry.runtime_data.async_export_support_package())

    def inspect():
        with ZipFile(archive_path) as archive:
            return json.loads(archive.read("raw_capture.json"))

    evidence = await hass.async_add_executor_job(inspect)
    assert evidence["bms_read_diagnostics"]["captured_ranges"] == [
        {"start": 109, "count": 5, "words": [512, 65526, 25, 0, 0]}]
    # Export also refreshes runtime telemetry; the diagnostic capture itself
    # must send exactly one BMS request, independently of that normal refresh.
    assert capture_read_counts == [1]
    assert registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_battery_percent") is None
    assert await hass.config_entries.async_unload(entry.entry_id)
