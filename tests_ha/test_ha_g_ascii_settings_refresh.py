"""Native HA number publication across periodic reads and write confirmation."""

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.eybond_g_ascii import EybondGAsciiDriver
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.telemetry import TelemetryFreshness
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP
from tests.test_g_ascii_poll_recovery import CORE_REPLIES
from tests.test_hub import _FakeLinkManager


@pytest.mark.parametrize("control_mode", ["read_only", "full"])
async def test_external_and_local_settings_have_real_readback(hass, fake_runtime, monkeypatch, control_mode):
    from conftest import FakeRuntimeManager

    profile_name = "eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json"
    profile = load_driver_profile(profile_name)
    cap = profile.get_capability("max_charging_current_setting")
    replies = {**CORE_REPLIES, "CHGC???": "030"}
    commands = []
    now = 100.0

    class Transport:
        connected = True
        async def async_send_payload(self, payload, *, route):
            command = payload[:-1].decode("ascii")
            commands.append(command)
            reply = replies.get(command, "")
            if isinstance(reply, Exception):
                raise reply
            return b"(" + reply.encode("ascii") + b"\r"

    hub = EybondHub(connection=EybondConnectionSpec(
        server_ip=SYNTHETIC_SERVER_IP, collector_ip=SYNTHETIC_COLLECTOR_IP,
        collector_pn=SYNTHETIC_COLLECTOR_PN, tcp_port=8899, udp_port=58899,
        discovery_target="192.0.2.255", discovery_interval=30,
        heartbeat_interval=60, request_timeout=5.0,
    ))
    hub._link_manager = _FakeLinkManager()
    hub._link_manager.transport = Transport()
    hub._link_manager.collector_info.collector_pn = SYNTHETIC_COLLECTOR_PN
    hub._driver = EybondGAsciiDriver()
    read_values = hub._driver.async_read_values

    async def timed_read(transport, inverter, **kwargs):
        kwargs["now_monotonic"] = now
        return await read_values(transport, inverter, **kwargs)

    monkeypatch.setattr(hub._driver, "async_read_values", timed_read)
    inverter = DetectedInverter(
        driver_key=hub._driver.key, protocol_family="eybond_g_ascii",
        model_name="Settings test", serial_number="",
        probe_target=ProbeTarget(0x0994, 255, 0),
        register_schema_name="eybond_g_ascii/base.json", profile_name=profile_name,
        capabilities=(cap,),
    )
    hub._inverter = inverter
    hub._accept_inverter_binding_identity()

    async def refresh(self, *, poll_interval=None):
        return await hub.async_refresh(poll_interval=10)

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="G-ASCII settings test",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "eybond_g_ascii", "control_mode": control_mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "detected_driver": inverter.driver_key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={
            "poll_interval": 10, "poll_mode": "manual",
            "effective_metadata_snapshot": {
                "effective_owner_key": inverter.driver_key, "variant_key": inverter.variant_key,
                "profile_name": inverter.profile_name,
                "register_schema_name": inverter.register_schema_name, "confidence": "high",
            },
        },
    )
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    if control_mode == "full":
        registry.async_get_or_create(
            "number", DOMAIN, f"{entry.entry_id}_number_{cap.key}",
            config_entry=entry, disabled_by=None,
        )
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    def current():
        return hub._last_snapshot.runtime_value(cap.value_key)

    def assert_number(value):
        if control_mode == "full":
            entity_id = registry.async_get_entity_id("number", DOMAIN, f"{entry.entry_id}_number_{cap.key}")
            assert entity_id is not None
            assert float(hass.states.get(entity_id).state) == value

    assert current() == 30
    assert_number(30)
    poll_entity = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_collector_poll_current_interval_seconds")
    assert poll_entity is not None
    assert hass.states.get(poll_entity).attributes["manual_backoff_active"] is False
    replies["CHGC???"] = "040"  # panel/cloud edit, not a local write
    commands.clear()
    now = 110.0
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert "CHGC???" not in commands
    assert current() == 30
    assert hub._last_snapshot.telemetry.point(cap.value_key).freshness is TelemetryFreshness.CARRIED
    now = 160.0
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert current() == 40
    assert_number(40)
    assert hub._last_snapshot.telemetry.point(cap.value_key).freshness is TelemetryFreshness.FRESH

    if control_mode == "full":
        now = 161.0
        command = f"{cap.command}050"
        replies[command] = "ACK"
        replies["CHGC???"] = TimeoutError()
        # An ACK plus the old cached value must never confirm a write.
        with pytest.raises(RuntimeError, match="readback|confirm"):
            await hub.async_write_capability(cap.key, 50)
        assert commands.count(command) == 1
        assert current() is None
        replies["CHGC???"] = "050"
        now = 230.0
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert_number(50)

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
