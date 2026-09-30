"""Generic SMG profiles materialize through real HA without automatic writes."""

from __future__ import annotations

import asyncio
import pytest

from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("protocol,legacy_snapshot", [(1, False), (2, False), (11, False), (11, True)])
@pytest.mark.parametrize("mode", ["auto", "full"])
async def test_protocol_control_policy_survives_setup_and_reload(
    hass, fake_runtime, monkeypatch, protocol, legacy_snapshot, mode,
) -> None:
    from conftest import FakeRuntimeManager

    name = f"modbus_smg/protocols/communication_protocol_{protocol}.json"
    profile = load_driver_profile(name)
    model = f"SMG Protocol {protocol} (Unverified Variant)"
    variant = f"protocol_{protocol}_family_fallback"
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg", model_name=model,
        serial_number="", probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
        variant_key=variant, profile_name=name, register_schema_name=name,
        capabilities=profile.capabilities, capability_groups=profile.groups,
        capability_presets=profile.presets,
        details={"protocol_number": protocol, "rated_power": 6200},
    )

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            inverter=inverter,
            values={
                "runtime_detection_status": "autodetected_high_confidence",
                "output_source_priority": profile.get_capability("output_source_priority").choices[0].label,
                "battery_voltage": 49.5,
                "battery_bulk_voltage": 51.6,
            },
        )

    async def unexpected_write(*args, **kwargs):
        pytest.fail("Selecting a generic protocol or Full Control must not write to an inverter")

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", unexpected_write, raising=False)
    entry = MockConfigEntry(
        domain=DOMAIN, title=model, version=3,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "collector_operation_mode": "home_assistant_only",
            "tcp_port": 8899, "udp_port": 58899, "driver_hint": "auto",
            "control_mode": mode, "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external", "proxy_enabled": False,
            "detected_driver": "modbus_smg", "detected_model": model,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={
            "poll_interval": 30, "poll_mode": "auto",
            "effective_metadata_snapshot": {
                "effective_owner_key": "modbus_smg", "variant_key": variant,
                "profile_name": name, "register_schema_name": name, "confidence": "high",
            },
        },
    )
    entry.add_to_hass(hass)
    if legacy_snapshot:
        # Upgrade an existing read-only family entry, not just a fresh installation.
        data = dict(entry.data, detected_model="SMG Family (Unverified Variant)")
        options = dict(entry.options, effective_metadata_snapshot={
            "effective_owner_key": "modbus_smg", "variant_key": "family_fallback",
            "profile_name": "", "register_schema_name": "modbus_smg/base.json",
            "confidence": "medium",
        })
        hass.config_entries.async_update_entry(entry, data=data, options=options)
    assert await hass.config_entries.async_setup(entry.entry_id)
    if legacy_snapshot:
        async with asyncio.timeout(5):
            while len(fake_runtime) < 2:
                await asyncio.sleep(0)
    await hass.async_block_till_done()
    assert entry.data["detected_model"] == model
    assert entry.options["effective_metadata_snapshot"]["profile_name"] == name
    registry = er.async_get(hass)
    select_id = registry.async_get_entity_id("select", DOMAIN, f"{entry.entry_id}_select_output_source_priority")
    number_id = registry.async_get_entity_id("number", DOMAIN, f"{entry.entry_id}_number_battery_bulk_voltage")
    if mode == "auto":
        assert select_id is None
        assert number_id is None
    else:
        assert select_id is not None
        assert number_id is not None
        # Advanced numeric settings exist but require explicit entity activation.
        assert registry.async_get(number_id).disabled_by is er.RegistryEntryDisabler.INTEGRATION
        registry.async_update_entity(number_id, disabled_by=None)
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert hass.states.get(select_id).state == profile.get_capability("output_source_priority").choices[0].label
        assert float(hass.states.get(number_id).state) == 51.6
        # User choice remains authoritative across reload, even in Full Control.
        registry.async_update_entity(number_id, disabled_by=er.RegistryEntryDisabler.USER)
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    if mode == "full":
        assert registry.async_get(number_id).disabled_by is er.RegistryEntryDisabler.USER
        assert registry.async_get_entity_id("number", DOMAIN, f"{entry.entry_id}_number_battery_bulk_voltage") == number_id
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize("mode", ["read_only", "auto", "full"])
@pytest.mark.parametrize("legacy_snapshot", [False, True])
async def test_anj6200_upgrades_old_smg_selector_without_writes(hass, fake_runtime, monkeypatch, mode, legacy_snapshot):
    """Existing #51 entry keeps its identity, but not the stale SMG mode table."""
    from conftest import FakeRuntimeManager

    name = "modbus_smg/models/anenji_anj_6200_48pl.json"
    profile = load_driver_profile(name)
    cap = profile.get_capability("output_source_priority")
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Anenji ANJ-6200-48PL", serial_number="SYNTHETIC51",
        probe_target=ProbeTarget(1, 255, 1), variant_key="anenji_anj_6200_48pl",
        profile_name=name, register_schema_name=name, capabilities=profile.capabilities,
        capability_groups=profile.groups, capability_presets=profile.presets,
    )

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            inverter=inverter,
            values={"runtime_detection_status": "autodetected_high_confidence",
                    "output_source_priority": cap.choices[1].label, "battery_voltage": 51.5},
        )

    async def unexpected_write(*args, **kwargs):
        pytest.fail("A catalog upgrade or Full Control selection must not send writes")

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", unexpected_write, raising=False)
    entry = MockConfigEntry(
        domain=DOMAIN, version=3, title=inverter.model_name,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": mode,
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "proxy_enabled": False, "detected_driver": "modbus_smg",
            "detected_model": inverter.model_name, "detected_serial": inverter.serial_number,
            "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto", "effective_metadata_snapshot": {
            "effective_owner_key": "modbus_smg", "variant_key": "default",
            "profile_name": "modbus_smg/models/smg_6200.json",
            "register_schema_name": "modbus_smg/models/smg_6200.json", "confidence": "high",
            "surface_key": "smg_6200_full", "catalog_version": "before-issue-51",
            "candidate_keys": ["anenji_anj_6200_48pl"],
        }},
    )
    entry.add_to_hass(hass)
    if legacy_snapshot:
        snapshot = dict(entry.options["effective_metadata_snapshot"])
        for key in ("catalog_version", "surface_key", "candidate_keys"):
            snapshot.pop(key, None)
        hass.config_entries.async_update_entry(entry, options=dict(entry.options, effective_metadata_snapshot=snapshot))
    registry = er.async_get(hass)
    unique_id = f"{entry.entry_id}_select_output_source_priority"
    previous_id = None
    if mode == "full":
        previous_id = registry.async_get_or_create(
            "select", DOMAIN, unique_id, config_entry=entry,
            suggested_object_id="existing_anj_output_source_priority",
            original_name="Output Source Priority",
        ).entity_id
    assert await hass.config_entries.async_setup(entry.entry_id)
    if legacy_snapshot:
        async with asyncio.timeout(5):
            while len(fake_runtime) < 2:
                await asyncio.sleep(0)
    await hass.async_block_till_done()
    select_id = registry.async_get_entity_id("select", DOMAIN, unique_id)
    assert entry.options["effective_metadata_snapshot"]["profile_name"] == name
    if mode != "read_only":
        assert select_id is not None
        if previous_id:
            assert select_id == previous_id
        assert hass.states.get(select_id).attributes["options"] == [c.label for c in cap.choices]
        assert hass.states.get(select_id).state == cap.choices[1].label
    else:
        assert select_id is None
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert registry.async_get_entity_id("select", DOMAIN, unique_id) == select_id
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
