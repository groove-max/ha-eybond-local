"""Real HA lifecycle for 09C1: family identity, read-only, failed group removal."""

from __future__ import annotations

import json
from pathlib import Path
import zipfile

import pytest

from homeassistant.helpers import device_registry as dr, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.eybond_09c1 import Eybond09C1Driver
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["auto", "full"])
@pytest.mark.parametrize("already_detected", [False, True])
async def test_09c1_entities_reload_and_optional_failure(
    hass, fake_runtime, monkeypatch, mode, already_detected,
):
    from conftest import FakeRuntimeManager

    class Transport:
        connected = True
        fail = False
        optional_fail = False

        async def async_send_payload(self, payload, *, route, request_timeout=None):
            assert (route.devcode, route.collector_addr) == (1, 255)
            if self.fail or (self.optional_fail and payload != b"Q1\r"):
                raise TimeoutError("synthetic_read_timeout")
            return {
                b"Q1\r": b"(232.0 241.0 229.0 025 49.9 52.4 31.0 00100001\r",
                b"QF\r": b"(50.1\r", b"PV?\r": b"(3215 123 0 002180\r",
                b"F\r": b"(230.0 12K 48.00 50.0\r",
                b"G?\r": b"(Normal 04  \r",
            }[payload]

    driver, transport = Eybond09C1Driver(), Transport()
    inverter = await driver.async_probe(transport, ProbeTarget(1, 255, 1))
    assert inverter is not None

    async def refresh(self, *, poll_interval=None):
        try:
            read = await driver.async_read_values(transport, inverter)
        except TimeoutError:
            return RuntimeSnapshot(connected=False, inverter=inverter, values={})
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            inverter=inverter, values=dict(read.values),
        )

    async def unexpected_write(*args, **kwargs):
        pytest.fail("09C1 read-only detection/runtime must never send a write")

    async def capture(self):
        return await driver.async_capture_support_evidence(transport, inverter)

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_capture_support_evidence", capture)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", unexpected_write, raising=False)
    entry = MockConfigEntry(
        domain=DOMAIN, title=inverter.model_name, version=3,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "collector_operation_mode": "home_assistant_only",
            "tcp_port": 8899, "udp_port": 58899, "driver_hint": "auto",
            "control_mode": mode, "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external", "proxy_enabled": False,
            **({"detected_driver": driver.key, "detected_model": inverter.model_name,
                "detected_serial": "", "detection_confidence": "high"} if already_detected else {}),
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def sensor_id(key):
        return registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")

    expected = {
        "grid_voltage": "232.0", "output_voltage": "229.0", "load_percent": "25.0",
        "grid_frequency": "49.9", "output_frequency": "50.1", "battery_voltage": "52.4",
        "temperature": "31.0", "pv_voltage": "321.5", "pv_current": "12.3",
        "protocol_id": "EYBOND_09C1", "operating_mode": "Bypass",
    }
    identities = {key: sensor_id(key) for key in expected}
    for key, value in expected.items():
        assert identities[key] is not None, key
        assert hass.states.get(identities[key]).state == value, key
    metadata = entry.options["effective_metadata_snapshot"]
    assert metadata["surface_key"] == "eybond_09c1_read_only"
    assert metadata["register_schema_name"] == inverter.register_schema_name
    assert metadata["profile_name"] == ""
    device_id = registry.async_get(identities["grid_voltage"]).device_id
    device = dr.async_get(hass).async_get(device_id)
    assert device.serial_number is None
    assert device.model == "EyeBond 09C1 family"
    entities = [entity for entity in er.async_entries_for_config_entry(registry, entry.entry_id)
                if entity.device_id == device_id]
    assert all(entity.domain not in {"select", "number", "switch", "text", "time"} for entity in entities)
    assert not any(entity.unique_id.endswith("_sync_inverter_clock") for entity in entities)
    for key in ("battery_soc", "output_power", "pv_power", "energy_total", "serial_number"):
        assert sensor_id(key) is None, key

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert {key: sensor_id(key) for key in expected} == identities
    assert registry.async_get(identities["grid_voltage"]).device_id == device_id

    transport.optional_fail = True
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(identities["grid_frequency"]).state == "49.9"
    for key in ("output_frequency", "pv_voltage", "pv_current"):
        assert hass.states.get(identities[key]).state == "unavailable", key
    transport.fail = True
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    for key in ("grid_voltage", "battery_voltage", "load_percent"):
        assert hass.states.get(identities[key]).state == "unavailable", key
    transport.fail = transport.optional_fail = False
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    for key, value in expected.items():
        assert hass.states.get(identities[key]).state == value, key
    archive_path = Path(await entry.runtime_data.async_export_support_package())

    def inspect_archive():
        with zipfile.ZipFile(archive_path) as archive:
            return json.loads(archive.read("raw_capture.json"))

    evidence = await hass.async_add_executor_job(inspect_archive)
    assert evidence["capture_kind"] == "09c1_read_only"
    assert evidence["failures"] == {}
    assert set(evidence["responses_hex"]) == {"Q1", "QF", "PV?", "F", "G?"}
    assert bytes.fromhex(evidence["responses_hex"]["F"]) == b"(230.0 12K 48.00 50.0\r"
    assert await hass.config_entries.async_unload(entry.entry_id)
