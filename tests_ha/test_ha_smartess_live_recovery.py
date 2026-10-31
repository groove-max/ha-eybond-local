"""Required 0925 polling repairs saved skips through the real HA coordinator."""

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.collector.metadata import build_collector_metadata_routes
from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.smartess_local import SmartEssLocalDriver
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.modbus import crc16_modbus, decode_read_request
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.telemetry import TelemetryFreshness
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("optional_skip", [False, True])
async def test_saved_live_skip_recovers_and_stays_removed_after_reload(
    hass, fake_runtime, monkeypatch, optional_skip,
):
    from conftest import FakeRuntimeManager

    now = 100.0
    soc = 75
    fail_live = False
    calls = []

    class Transport:
        async def async_send_payload(self, payload, *, route):
            request = decode_read_request(payload)
            assert request is not None, "read-only recovery must never write"
            calls.append((request.address, request.count))
            if request.address == 4501 and fail_live:
                raise TimeoutError("temporary device silence")
            words = []
            for address in range(request.address, request.address + request.count):
                # Only the physical-device boundary is synthetic. The driver
                # decodes its actual little-endian 0925 live words.
                value = soc if address == 4507 else 0
                if 4501 <= address <= 4561:
                    value = ((value & 255) << 8) | (value >> 8)
                words.append(value)
            body = bytes((request.slave_id, request.function_code, len(words) * 2))
            body += b"".join(word.to_bytes(2, "big") for word in words)
            return body + crc16_modbus(body).to_bytes(2, "little")

    class Link:
        connected = True
        transport = Transport()
        collector_info = CollectorInfo(
            collector_pn=SYNTHETIC_COLLECTOR_PN, remote_ip=SYNTHETIC_COLLECTOR_IP,
        )

        async def async_try_connect(self, **kwargs):
            return True

        async def async_ensure_connected(self, **kwargs):
            return None

        def collector_metadata_routes(self):
            return build_collector_metadata_routes()

        def listener_diagnostics(self):
            return {}

    hub = EybondHub(connection=EybondConnectionSpec(
        server_ip=SYNTHETIC_SERVER_IP, collector_ip=SYNTHETIC_COLLECTOR_IP,
        collector_pn=SYNTHETIC_COLLECTOR_PN, tcp_port=8899, udp_port=58899,
        discovery_target=SYNTHETIC_COLLECTOR_IP, discovery_interval=30,
        heartbeat_interval=60, request_timeout=5,
    ))
    hub._link_manager = Link()
    hub._driver = SmartEssLocalDriver()
    inverter = DetectedInverter(
        driver_key="smartess_local", protocol_family="smartess_local",
        model_name="Synthetic 0925", serial_number="", variant_key="smartess_0925",
        probe_target=ProbeTarget(1, 5, 5),
        profile_name=hub._driver.profile_name,
        register_schema_name=hub._driver.register_schema_name,
        capabilities=hub._driver.write_capabilities,
    )
    hub._inverter = inverter
    hub._accept_inverter_binding_identity()
    read_values = hub._driver.async_read_values

    async def timed_read(transport, inverter, **kwargs):
        return await read_values(transport, inverter, **{**kwargs, "now_monotonic": now})

    async def refresh(self, *, poll_interval=None):
        return await hub.async_refresh(poll_interval=10)

    monkeypatch.setattr(hub._driver, "async_read_values", timed_read)
    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(
        FakeRuntimeManager, "set_persistent_unsupported_commands",
        lambda self, commands: hub.set_persistent_unsupported_commands(commands), raising=False,
    )
    expected = ["register:5004"] if optional_skip else []
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="Synthetic 0925 recovery",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "smartess_local", "control_mode": "read_only",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": inverter.driver_key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={
            "poll_interval": 30, "poll_mode": "auto",
            "driver_unsupported_commands": ["block:live", *expected],
            "driver_unsupported_commands_version": 2,
        },
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.options["driver_unsupported_commands"] == expected
    assert hub._persistent_unsupported_commands == tuple(expected)
    assert hub._last_snapshot.runtime_value("battery_capacity") == 75
    assert (4501, 14) in calls

    async def poll():
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        return hub._last_snapshot.telemetry.point("battery_capacity")

    now, soc = 110, 80
    point = await poll()
    assert point.value == 80 and point.freshness is TelemetryFreshness.FRESH
    now, fail_live = 120, True
    point = await poll()
    assert point.value == 80 and point.freshness is TelemetryFreshness.CARRIED
    now, soc, fail_live = 130, 85, False
    point = await poll()
    assert point.value == 85 and point.freshness is TelemetryFreshness.FRESH

    # Recreate per-session state and reload HA from the corrected saved options.
    hub._reset_runtime_read_state()
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.options["driver_unsupported_commands"] == expected
    assert hub._last_snapshot.runtime_value("battery_capacity") == 85
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
