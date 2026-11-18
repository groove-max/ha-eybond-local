"""Q1 manual selector, actual HA entities, support export and reload lifecycle."""

from io import BytesIO
import json
from pathlib import Path
import zipfile

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.eybond_q1 import EybondQ1Driver
from custom_components.eybond_local.flows.common.connection_form import driver_selector
from custom_components.eybond_local.models import CollectorInfo, RuntimeSnapshot
from custom_components.eybond_local.payload.q1 import STATUS_REQUEST, Q1Error
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "full"])
async def test_manual_q1_entities_archive_failure_and_reload(hass, fake_runtime, monkeypatch, mode):
    from conftest import FakeRuntimeManager

    response = b"(249.6 000.0 234.5 000 60.0 2.34 30.0 10000000\r"
    assert driver_selector()("eybond_q1") == "eybond_q1"
    assert next(option["label"] for option in driver_selector().config["options"]
                if option["value"] == "eybond_q1") == "Q1-compatible (experimental; read-only)"

    class Transport:
        connected = True
        fail = False

        async def async_send_payload(self, payload, *, route, request_timeout=None):
            assert payload == STATUS_REQUEST, "No additional dialect/setting commands"
            assert route == EybondQ1Driver.probe_targets[0].link_route
            return response[:-1] if self.fail else response

    driver, transport = EybondQ1Driver(), Transport()
    inverter = await driver.async_probe(transport, driver.probe_targets[0])
    assert inverter is not None

    async def refresh(self, *, poll_interval=None):
        try:
            read = await driver.async_read_values(transport, inverter)
        except Q1Error:
            return RuntimeSnapshot(connected=False, inverter=inverter)
        return RuntimeSnapshot(
            connected=True, inverter=inverter,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            telemetry=fold_driver_telemetry(TypedTelemetryFrame.empty(), driver_key=driver.key,
                                           values=read.values, replace=True),
        )

    async def capture(self):
        assert entry.runtime_data._runtime_operation_lock.locked()
        return await driver.async_capture_support_evidence(transport, inverter)

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_capture_support_evidence", capture)
    entry = MockConfigEntry(domain=DOMAIN, version=5, title=inverter.model_name,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}", data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": driver.key, "control_mode": mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": driver.key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        }, options={"poll_interval": 30, "poll_mode": "manual"})
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def entity_id(key):
        return registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")

    expected = {"grid_voltage": 249.6, "output_voltage": 234.5, "load_percent": 0,
                "q1_frequency": 60, "temperature": 30}
    ids = {key: entity_id(key) for key in expected}
    for key, value in expected.items():
        assert ids[key] is not None
        assert float(hass.states.get(ids[key]).state) == value
    assert hass.states.get(ids["q1_frequency"]).attributes["unit_of_measurement"] == "Hz"
    inverter_device = registry.async_get(ids["output_voltage"]).device_id
    assert not [e for e in registry.entities.values() if e.device_id == inverter_device
                and e.domain in {"number", "select", "switch", "binary_sensor"}]
    for key in ("battery_voltage", "battery_soc", "output_power", "pv_power", "battery_power",
                "grid_frequency", "output_frequency", "operating_mode",
                "estimated_load_energy_daily", "estimated_pv_energy_daily"):
        assert entity_id(key) is None

    path = Path(await entry.runtime_data.async_export_support_package())
    data = await hass.async_add_executor_job(path.read_bytes)
    with zipfile.ZipFile(BytesIO(data)) as archive:
        raw = json.loads(archive.read("raw_capture.json"))
        assert raw["capture_kind"] == "q1_read_only"
        assert raw["responses_hex"]["Q1"] == response.hex()
        evidence = raw["q1_status_evidence"]
        assert evidence["battery_voltage_unscaled"] == 2.34
        assert evidence["status_flags"] == "10000000"
        assert evidence["field_map_verification"] == "experimental_replay_only"
        assert SYNTHETIC_COLLECTOR_PN not in archive.read("support_bundle.json").decode()

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert {key: entity_id(key) for key in expected} == ids
    transport.fail = True
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert all(hass.states.get(i).state == "unavailable" for i in ids.values())
    transport.fail = False
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert float(hass.states.get(ids["output_voltage"]).state) == 234.5
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
