"""PI30 partial reads, expiry and recovery through real HA measurement entities."""

from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.canonical_telemetry import project_canonical_telemetry
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


async def test_failed_mode_read_keeps_live_voltage_then_expires_and_recovers(
    hass, fake_runtime, monkeypatch,
):
    from conftest import FakeRuntimeManager

    target = ProbeTarget(0x0994, 1, 0)
    replies = {
        "QPI": "PI30", "QID": "99000000000054",
        "QPIRI": "230.0 26.9 230.0 50.0 26.9 6200 6200 48.0 46.0 42.0 55.2 54.6 2 030 030 1 0 1 1 01 0 0 54.0 0 1",
        "QMOD": "L", "QPIWS": "00000000000000000000000000000000", "QET": "NAK",
        "QPIGS": "232.1 50.0 232.1 50.0 0440 0227 007 429 54.60 000 100 0043 00.0 000.0 00.00 00000 00010101 00 00 00000 110",
        "Q1": "00 00 00 000 042 030 043 00 00 000 0030 0000 13",
    }
    transport = FixtureTransport(
        registers=None,
        command_responses={(target.devcode, target.collector_addr, k): v for k, v in replies.items()},
        probe_target=target,
    )
    mode_reply = "L"
    original_send = transport.async_send_payload

    async def send(payload, *, route):
        command = payload[:-3].decode("ascii")
        assert command.startswith("Q"), "Polling must not write"
        if command == "QMOD":
            replacement = FixtureTransport(
                registers=None, probe_target=target,
                command_responses={(target.devcode, target.collector_addr, command): mode_reply},
            )
            return await replacement.async_send_payload(payload, route=route)
        return await original_send(payload, route=route)

    monkeypatch.setattr(transport, "async_send_payload", send)
    driver = Pi30Driver()
    inverter = await driver.async_probe(transport, target)
    assert inverter is not None
    state = {}
    frame = TypedTelemetryFrame.empty()
    now = 100.0

    async def refresh(self, *, poll_interval=None):
        nonlocal frame
        read = await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=10, now_monotonic=now,
        )
        # Fold raw points before projecting aliases, as the production hub does.
        frame = fold_driver_telemetry(
            frame, driver_key=driver.key, values=read.values,
            replace=False, removed_keys=read.removed_keys,
        )
        return RuntimeSnapshot(
            connected=True, inverter=inverter, telemetry=project_canonical_telemetry(frame),
            values=read.diagnostics,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
        )

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title=inverter.model_name,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "pi30", "control_mode": "read_only",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": driver.key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={"poll_interval": 10, "poll_mode": "manual"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def reading(key):
        entity_id = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_{key}")
        assert entity_id is not None
        return hass.states.get(entity_id).state

    initial_mode = reading("operating_mode")
    assert initial_mode not in {"unknown", "unavailable"}
    for now, mode_reply, expired in ((110, "NAK", False), (220, "NAK", True), (230, "L", False)):
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert float(reading("battery_voltage")) == 54.6
        assert reading("operating_mode") == ("unavailable" if expired else initial_mode)
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
