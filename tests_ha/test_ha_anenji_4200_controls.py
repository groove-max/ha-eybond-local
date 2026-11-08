"""Upgrade the #88 control surface in real HA without duplicate writers or I/O."""

from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


async def test_upgrade_replaces_numeric_controls_and_applies_live_conditions(hass, fake_runtime, monkeypatch):
    from conftest import FakeRuntimeManager

    name = "modbus_smg/models/anenji_4200_protocol_1.json"
    profile = load_driver_profile(name)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Anenji 4200 (Protocol 1)", serial_number="SYNTHETIC88",
        probe_target=ProbeTarget(1, 255, 1), variant_key="anenji_4200_protocol_1",
        profile_name=name, register_schema_name=name, capabilities=profile.capabilities,
        capability_groups=profile.groups, capability_presets=profile.presets,
    )
    values = {
        "runtime_detection_status": "autodetected_high_confidence",
        "operating_mode": "Standby", "battery_type": "Li4", "battery_voltage": 26.8,
        "battery_connected": True, "output_source_priority": "SUF",
        "output_rating_voltage": 230, "output_rating_voltage_setting": "230 V",
        "output_rating_frequency": 50, "output_rating_frequency_setting": "50 Hz",
        "battery_equalization_time": 60, "battery_equalization_mode": False,
        "battery_equalization_enabled": False,
    }

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=True, inverter=inverter, values=dict(values),
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
        )

    async def unexpected_write(*args, **kwargs):
        raise AssertionError("Profile migration/availability changes must not write to hardware")

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", unexpected_write, raising=False)
    entry = MockConfigEntry(
        domain=DOMAIN, title=inverter.model_name, version=3,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": "full",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "proxy_enabled": False, "detected_driver": "modbus_smg",
            "detected_model": inverter.model_name, "detected_serial": inverter.serial_number,
            "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto", "effective_metadata_snapshot": {
            "effective_owner_key": "modbus_smg", "variant_key": inverter.variant_key,
            "profile_name": name, "register_schema_name": name, "confidence": "high",
        }},
    )
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    legacy = [
        registry.async_get_or_create(
            domain, DOMAIN, f"{entry.entry_id}_{domain}_{key}", config_entry=entry,
        ).entity_id
        for domain, key in (
            ("number", "output_rating_voltage"), ("number", "output_rating_frequency"),
            ("select", "output_voltage_select"), ("select", "output_frequency_select"),
        )
    ]
    retained = registry.async_get_or_create(
        "switch", DOMAIN, f"{entry.entry_id}_switch_lcd_backlight_mode",
        config_entry=entry, disabled_by=er.RegistryEntryDisabler.USER,
    )
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    for entity_id in legacy:
        assert registry.async_get(entity_id) is None
    assert registry.async_get(retained.entity_id).disabled_by is er.RegistryEntryDisabler.USER

    ids = {}
    for domain, key in (
        ("select", "output_rating_voltage"), ("select", "output_rating_frequency"),
        ("number", "battery_equalization_time"), ("switch", "battery_equalization_mode"),
        ("select", "battery_type"), ("button", "equalization_activate_now"),
    ):
        entity_id = registry.async_get_entity_id(domain, DOMAIN, f"{entry.entry_id}_{domain}_{key}")
        assert entity_id is not None
        if key in ("battery_type", "equalization_activate_now"):
            assert registry.async_get(entity_id).disabled_by is er.RegistryEntryDisabler.INTEGRATION
        ids[key] = entity_id
        registry.async_update_entity(entity_id, disabled_by=None)
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert hass.states.get(ids["output_rating_voltage"]).state == "230 V"
    assert hass.states.get(ids["output_rating_frequency"]).state == "50 Hz"
    assert hass.states.get(ids["battery_type"]).state == "Li4"
    assert hass.states.get(ids["equalization_activate_now"]).state == "unavailable"
    assert hass.states.get(ids["battery_equalization_time"]).state == "unavailable"
    assert hass.states.get(ids["battery_equalization_mode"]).state == "unavailable"
    for mode in ("Line", "Fault", "Power On", None, "Standby"):
        values["operating_mode"] = mode
        values["configuration_safe_mode"] = True  # Broader flag is NOT proof of Standby.
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        expected = "230 V" if mode == "Standby" else "unavailable"
        assert hass.states.get(ids["output_rating_voltage"]).state == expected
    values["battery_type"] = "Flooded"
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert float(hass.states.get(ids["battery_equalization_time"]).state) == 60
    assert hass.states.get(ids["battery_equalization_mode"]).state == "off"
    assert hass.states.get(ids["equalization_activate_now"]).state == "unavailable"
    for battery in ("Flooded", "User", "AGM", "Li2", "Li4", "LiB", None):
        for enabled in (False, True, None):
            values.update(battery_type=battery, battery_equalization_enabled=enabled)
            await entry.runtime_data.async_refresh()
            await hass.async_block_till_done()
            allowed = battery in ("Flooded", "User") and enabled is True
            assert (hass.states.get(ids["equalization_activate_now"]).state != "unavailable") is allowed
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert registry.async_get(retained.entity_id).disabled_by is er.RegistryEntryDisabler.USER
    assert registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_output_rating_voltage") == ids["output_rating_voltage"]
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
