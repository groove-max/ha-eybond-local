"""Real G-ASCII driver/hub recovery through native HA entities, no devices."""

from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.eybond_g_ascii import EybondGAsciiDriver
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.telemetry import TelemetryFreshness
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP
from tests.test_g_ascii_poll_recovery import CORE, CORE_REPLIES
from tests.test_hub import _FakeLinkManager


async def test_silent_core_is_carried_then_recovers_without_reconnect(
    hass, fake_runtime, monkeypatch,
):
    from conftest import FakeRuntimeManager

    class Transport:
        connected = True
        silent = False
        reject_mode = False

        def __init__(self):
            self.commands = []

        async def async_send_payload(self, payload, *, route):
            command = payload[:-1].decode("ascii")
            self.commands.append(command)
            assert command in CORE or command.startswith(("G", "F", "S", "B", "T", "D"))
            if self.silent:
                raise TimeoutError()
            if self.reject_mode and command == "GMOD":
                return b"NAK\r"
            return b"(" + CORE_REPLIES.get(command, "").encode("ascii") + b"\r"

    transport = Transport()
    hub = EybondHub(connection=EybondConnectionSpec(
        server_ip=SYNTHETIC_SERVER_IP, collector_ip=SYNTHETIC_COLLECTOR_IP,
        collector_pn=SYNTHETIC_COLLECTOR_PN, tcp_port=8899, udp_port=58899,
        discovery_target="192.0.2.255", discovery_interval=30,
        heartbeat_interval=60, request_timeout=5.0,
    ))
    hub._link_manager = _FakeLinkManager()
    hub._link_manager.transport = transport
    hub._link_manager.collector_info.collector_pn = SYNTHETIC_COLLECTOR_PN
    hub._driver = EybondGAsciiDriver()
    inverter = DetectedInverter(
        driver_key=hub._driver.key, protocol_family="eybond_g_ascii",
        model_name="EyeBond G-ASCII family", serial_number="",
        probe_target=ProbeTarget(0x0994, 1, 0),
        register_schema_name="eybond_g_ascii/base.json",
    )
    hub._inverter = inverter
    hub._accept_inverter_binding_identity()

    async def refresh(self, *, poll_interval=None):
        return await hub.async_refresh(poll_interval=10)

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title=inverter.model_name,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "eybond_g_ascii", "control_mode": "read_only",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": inverter.driver_key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={"poll_interval": 10, "poll_mode": "manual"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    registry = er.async_get(hass)
    entity_id = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_battery_voltage")
    assert entity_id is not None
    assert float(hass.states.get(entity_id).state) == 26.4
    last_success = hub._last_success_monotonic

    transport.silent = True
    transport.commands.clear()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert transport.commands == [*CORE, *CORE]  # Existing same-socket retry, no optional sweep.
    assert hub._last_snapshot.connected
    assert hub._last_snapshot.last_error == "request_timeout"
    assert hub._last_snapshot.telemetry.point("battery_voltage").freshness is TelemetryFreshness.CARRIED
    assert hub._last_success_monotonic == last_success
    assert hub._link_manager.reset_calls == 0
    assert float(hass.states.get(entity_id).state) == 26.4

    transport.silent = False
    transport.reject_mode = True  # One unsupported core query must not discard other data.
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hub._last_snapshot.last_error is None
    assert hub._last_snapshot.telemetry.point("battery_voltage").freshness is TelemetryFreshness.FRESH
    assert hub._last_success_monotonic > last_success
    assert "runtime_payload_error" not in hub._last_snapshot.values
    assert hub._link_manager.reset_calls == 0
    assert float(hass.states.get(entity_id).state) == 26.4
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
