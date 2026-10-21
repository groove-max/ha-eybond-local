"""External setting changes update real HA sensors/selects/switches, read-only."""

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.canonical_telemetry import project_canonical_telemetry
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("control_mode", ["read_only", "full"])
@pytest.mark.parametrize("firmware_scoped", [False, True])
async def test_external_settings_refresh_without_writes(hass, fake_runtime, monkeypatch, control_mode, firmware_scoped):
    from conftest import FakeRuntimeManager

    target = ProbeTarget(0x0994, 1, 0)
    replies = {
        "QPI": "PI30", "QID": "99000000000054",
        "QPIRI": "230.0 26.9 230.0 50.0 26.9 6200 6200 48.0 46.0 42.0 55.2 54.6 2 030 030 1 0 1 1 01 0 0 54.0 0 1",
        "QFLAG": "ExDa",
        "QMCHGCR": "010 020 030 040", "QMUCHGCR": "002 010 020 030",
        "QMOD": "L", "QPIWS": "00000000000000000000000000000000", "QET": "NAK",
        "QPIGS": "232.1 50.0 232.1 50.0 0440 0227 007 429 54.60 000 100 0043 00.0 000.0 00.00 00000 00010101 00 00 00000 110",
        "Q1": "00 00 00 000 042 030 043 00 00 000 0030 0000 13",
    }
    if firmware_scoped:
        replies.update(QMN="VMII-NXPW5KW", QVFW="VERFW:00031.07")
        replies["QPIRI"] += " 50.0 22 51.2"
    transport = FixtureTransport(
        registers=None, probe_target=target,
        command_responses={(target.devcode, 1, key): value for key, value in replies.items()},
    )
    commands = []
    original_send = transport.async_send_payload

    async def send(payload, *, route):
        command = payload[:-3].decode("ascii")
        assert command.startswith("Q"), "Refreshing settings must never write"
        commands.append(command)
        return await original_send(payload, route=route)

    monkeypatch.setattr(transport, "async_send_payload", send)
    driver = Pi30Driver()
    inverter = await driver.async_probe(transport, target)
    assert inverter is not None
    if firmware_scoped:
        assert inverter.profile_name == "pi30_ascii/models/vmii_6200_31_07.json"
    state = {}
    values = dict(inverter.details)
    frame = fold_driver_telemetry(
        TypedTelemetryFrame.empty(), driver_key="pi30", values=values, replace=True,
    )
    now = 100.0

    async def refresh(self, *, poll_interval=None):
        nonlocal frame
        read = await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=10, now_monotonic=now,
        )
        values.update(read.values)
        for key in read.removed_keys:
            values.pop(key, None)
        frame = fold_driver_telemetry(
            frame, driver_key="pi30", values=read.values,
            replace=False, removed_keys=read.removed_keys,
        )
        return RuntimeSnapshot(
            connected=True, inverter=inverter,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            telemetry=project_canonical_telemetry(frame), values={
                **values, **read.diagnostics,
                "runtime_detection_status": "autodetected_high_confidence",
            },
        )

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="PI30 settings test",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "pi30", "control_mode": control_mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": "pi30", "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={
            "poll_interval": 10, "poll_mode": "manual",
            "effective_metadata_snapshot": {
                "effective_owner_key": "pi30", "variant_key": inverter.variant_key,
                "profile_name": inverter.profile_name,
                "register_schema_name": inverter.register_schema_name, "confidence": "high",
            },
        },
    )
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    keys = ("output_source_priority", "charger_source_priority", "battery_bulk_voltage")
    # Seed user-enabled diagnostic sensors before setup. Enabling them afterward
    # schedules an HA platform reload; don't race that with an explicit reload.
    for key in keys:
        registry.async_get_or_create(
            "sensor", DOMAIN, f"{entry.entry_id}_{key}",
            config_entry=entry, disabled_by=None,
        )
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()

    def entity_id(domain, key):
        prefix = f"{domain}_" if domain != "sensor" else ""
        found = registry.async_get_entity_id(domain, DOMAIN, f"{entry.entry_id}_{prefix}{key}")
        assert found is not None
        return found

    fields = replies["QPIRI"].split()
    if firmware_scoped:
        assert hass.states.get(entity_id("sensor", "charger_source_priority")).state == "Solar + Utility"
        if control_mode == "full":
            control = hass.states.get(entity_id("select", "charger_source_priority"))
            assert control.state == "Solar + Utility"
            assert control.attributes["options"] == ["Solar first", "Solar + Utility", "Only solar charging permitted"]
    fields[16], fields[17], fields[10] = "2", "2" if firmware_scoped else "3", "56.4"
    transport._command_responses[(target.devcode, 1, "QPIRI")] = " ".join(fields)
    transport._command_responses[(target.devcode, 1, "QFLAG")] = "EaDx"
    commands.clear()
    now = 160.0
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert commands.count("QPIRI") == 1
    now = 170.0
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert commands.count("QFLAG") == 1
    expected = {"output_source_priority": "SBU first", "charger_source_priority": "Only solar charging permitted"}
    for key, label in expected.items():
        assert hass.states.get(entity_id("sensor", key)).state == label
        if control_mode == "full":
            assert hass.states.get(entity_id("select", key)).state == label
    assert float(hass.states.get(entity_id("sensor", "battery_bulk_voltage")).state) == 56.4
    if control_mode == "full":
        assert hass.states.get(entity_id("switch", "buzzer_enabled")).state == "on"
        assert hass.states.get(entity_id("switch", "lcd_backlight_enabled")).state == "off"
    assert all(command.startswith("Q") for command in commands)
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
