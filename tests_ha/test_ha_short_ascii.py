"""The qualified short-ASCII baseline uses the real HA entity lifecycle."""

from __future__ import annotations

import pytest

from homeassistant.helpers import device_registry as dr, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["auto", "full"])
@pytest.mark.parametrize("already_detected", [False, True])
async def test_family_telemetry_reload_and_read_failure_without_guessed_identity_or_controls(
    hass, fake_runtime, monkeypatch, mode, already_detected,
) -> None:
    from conftest import FakeRuntimeManager

    class Transport:
        fail = False
        connected = True
        clock = 0.0
        rb = b"NAK\r"

        async def async_send_payload(self, payload, *, route, request_timeout=None):
            if self.fail:
                raise TimeoutError("synthetic_read_timeout")
            assert route.devcode == 767 and route.collector_addr == 255
            assert payload[-2:] == b"\x01\r"
            if payload[:-2] == b"RB" and isinstance(self.rb, Exception):
                raise self.rb
            body = b"230.0 04 03 115.0 013 60.0 13.2 35.0 1000010" + bytes(3)
            return {
                b"MP": b"\x01" + bytes(range(36)) + b"\r",
                b"MD": b"S2-8127-260101-V7.00  \x00\r",
                b"Q1": b"\x01" + body + sum(body).to_bytes(2, "big") + b"\r",
                b"RB": self.rb,
                b"F": b"#115.0 105 48.00 60.0\r",
            }.get(payload[:-2], b"NAK\r")

    driver, transport = EybondShortAsciiDriver(), Transport()
    inverter = await driver.async_probe(transport, ProbeTarget(767, 255, 1))
    assert inverter is not None

    async def refresh(self, *, poll_interval=None):
        if not hasattr(self, "short_ascii_state"):
            self.short_ascii_state = {}
        transport.clock += 1
        try:
            read = await driver.async_read_values(
                transport, inverter, runtime_state=self.short_ascii_state,
                now_monotonic=transport.clock,
            )
        except TimeoutError:
            # The real hub converts an exhausted transport recovery into an
            # offline snapshot; reproduce that boundary, not HA internals.
            return RuntimeSnapshot(connected=False, inverter=inverter, values={})
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            inverter=inverter, values=dict(read.values) | {
                key: inverter.details[key] for key in ("protocol_id", "short_ascii_firmware")
            },
        )

    async def unexpected_write(*args, **kwargs):
        pytest.fail("The short-ASCII read-only baseline must not send inverter writes")

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
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
            "detected_driver": driver.key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={
            "poll_interval": 30, "poll_mode": "auto",
        },
    )
    entry.add_to_hass(hass)
    if not already_detected:
        hass.config_entries.async_update_entry(entry, data={
            key: value for key, value in entry.data.items()
            if key not in {"detected_driver", "detected_model", "detected_serial", "detection_confidence"}
        })
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def sensor_id(key):
        return registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")

    expected = {
        "grid_voltage": "230.0", "output_voltage": "115.0", "load_percent": "13.0",
        "output_frequency": "60.0", "battery_reference_voltage": "13.2", "temperature": "35.0",
        "protocol_id": "EYBOND_SHORT_ASCII", "short_ascii_firmware": "S2-8127-260101-V7.00",
    }
    identities = {key: sensor_id(key) for key in expected}
    metadata = entry.options["effective_metadata_snapshot"]
    assert metadata["surface_key"] == "eybond_short_ascii_read_only"
    assert metadata["profile_name"] == ""
    assert metadata["register_schema_name"] == inverter.register_schema_name
    for key, value in expected.items():
        assert identities[key] is not None, key
        assert hass.states.get(identities[key]).state == value, key
    reference_entity = registry.async_get(identities["battery_reference_voltage"])
    assert reference_entity.entity_category.value == "diagnostic"
    device_id = reference_entity.device_id
    inverter_device = dr.async_get(hass).async_get(device_id)
    assert inverter_device.serial_number is None
    assert inverter_device.model == "EyeBond Short-ASCII family"
    entities = [entity for entity in er.async_entries_for_config_entry(registry, entry.entry_id)
                if entity.device_id == device_id]
    assert all(entity.domain not in {"select", "number", "switch", "text", "time"} for entity in entities)
    assert not any(entity.unique_id.endswith("_sync_inverter_clock") for entity in entities)
    assert registry.async_get(sensor_id("battery_soc")).disabled_by is er.RegistryEntryDisabler.INTEGRATION
    assert registry.async_get(sensor_id("pv_power")).disabled_by is er.RegistryEntryDisabler.INTEGRATION
    for key in ("battery_voltage", "grid_frequency", "output_power", "serial_number"):
        assert sensor_id(key) is None, key

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert {key: sensor_id(key) for key in expected} == identities
    assert registry.async_get(identities["battery_reference_voltage"]).device_id == device_id
    assert not entry.data.get("detected_serial")

    # Once the runtime reports the read/recovery failed, old live measurements
    # must not remain available in HA.
    transport.fail = True
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert {key: sensor_id(key) for key in expected} == identities
    assert registry.async_get(identities["battery_reference_voltage"]).device_id == device_id
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    for key in ("grid_voltage", "output_voltage", "load_percent", "battery_reference_voltage", "temperature"):
        assert hass.states.get(identities[key]).state == "unavailable", key
    transport.fail = False
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    for key, value in expected.items():
        assert hass.states.get(identities[key]).state == value, key

    # Optional BMS entities are opt-in; exercise their real HA availability
    # without replacing the driver or turning a missing RB into a Q1 failure.
    def bms_reply(voltage, soc):
        body = bytearray(37)
        body[:2] = voltage.to_bytes(2, "big")
        body[2] = soc
        body[17:19] = (333).to_bytes(2, "big")
        body[23:25] = b"\x01\x01"
        return b"\x01" + body + bytes([sum(body) & 255]) + b"\r"

    bms_keys = ("battery_soc", "bms_total_voltage", "bms_min_cell_voltage")
    bms_ids = {key: sensor_id(key) for key in bms_keys}
    available_id = registry.async_get_entity_id(
        "binary_sensor", DOMAIN, f"{entry.entry_id}_binary_sensor_short_ascii_bms_data_available",
    )
    for entity_id in (*bms_ids.values(), available_id):
        registry.async_update_entity(entity_id, disabled_by=None)
    transport.rb = bms_reply(520, 80)
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()

    async def check_bms(reply, availability, values):
        transport.rb = reply
        transport.clock += 31
        # At most one optional query per cycle; allow the other group its turn.
        for _ in range(2):
            await entry.runtime_data.async_refresh()
            await hass.async_block_till_done()
        assert hass.states.get(available_id).state == availability
        for key, state in zip(bms_keys, values, strict=True):
            assert hass.states.get(bms_ids[key]).state == state, key
        assert hass.states.get(identities["grid_voltage"]).state == "230.0"

    await check_bms(bms_reply(520, 80), "on", ("80", "52.0", "3.33"))
    await check_bms(bms_reply(0, 0), "off", ("unavailable",) * 3)
    await check_bms(b"NAK\r", "unavailable", ("unavailable",) * 3)
    await check_bms(TimeoutError(), "unavailable", ("unavailable",) * 3)
    await check_bms(bms_reply(520, 0), "on", ("0", "52.0", "3.33"))
    assert {key: sensor_id(key) for key in bms_keys} == bms_ids
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
