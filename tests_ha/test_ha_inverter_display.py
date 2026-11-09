"""Real HA flow/registry/lifecycle checks; the device transport is synthetic."""

from copy import deepcopy
from unittest.mock import patch

import pytest
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import device_registry as dr, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import (
    DOMAIN, CONF_INVERTER_DISPLAY_MANUFACTURER as BRAND, CONF_INVERTER_DISPLAY_MODEL as MODEL,
)
from custom_components.eybond_local.runtime.coordinator.device_registry import _entry_device
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.fixture
def display_entry(hass):
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="Synthetic collector",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={"connection_type": "eybond", "connection_mode": "known_ip",
              "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
              "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
              "driver_hint": "auto", "control_mode": "read_only",
              "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
              "detected_driver": "modbus_smg", "detected_model": "Sandisolar SD-11KP48V-WIFI",
              "detected_serial": "synthetic-serial"},
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    return entry


async def display_form(hass, entry):
    menu = await hass.config_entries.options.async_init(entry.entry_id)
    assert "inverter_display" in menu["menu_options"]
    return await hass.config_entries.options.async_configure(
        menu["flow_id"], {"next_step_id": "inverter_display"},
    )


async def save_labels(hass, entry, values):
    form = await display_form(hass, entry)
    result = await hass.config_entries.options.async_configure(form["flow_id"], values)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()


@pytest.mark.parametrize("custom_name", [False, True])
@pytest.mark.parametrize("control_mode", ["read_only", "full"])
async def test_save_clear_reload_preserve_identity_and_never_restart_for_labels(
    hass, display_entry, fake_runtime, custom_name, control_mode,
):
    entry = display_entry
    hass.config_entries.async_update_entry(entry, options={**entry.options, "control_mode": control_mode})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    registry = dr.async_get(hass)
    device = _entry_device(registry, entry.entry_id, (DOMAIN, entry.entry_id))
    assert device is not None
    original_model = device.model
    if custom_name:
        registry.async_update_device(device.id, name_by_user="My inverter")
    entities = er.async_get(hass)

    def entity_ids():
        return {item.unique_id: item.entity_id for item in er.async_entries_for_config_entry(entities, entry.entry_id)}

    before_ids = entity_ids()
    assert before_ids
    before_data = deepcopy(dict(entry.data))
    before_options = deepcopy(dict(entry.options))
    before_metadata = coordinator.effective_metadata
    manager = fake_runtime[-1]
    with patch.object(hass.config_entries, "async_reload", wraps=hass.config_entries.async_reload) as reload:
        for _ in range(2):  # A no-op save must not leave a suppression token.
            await save_labels(hass, entry, {BRAND: " Anenji ", MODEL: "ANJ-11KW-48V-WIFI-P"})
            assert entry.runtime_data is coordinator
            assert manager.stopped == 0
            assert dict(entry.data) == before_data
            assert dict(entry.options) == before_options | {BRAND: "Anenji", MODEL: "ANJ-11KW-48V-WIFI-P"}
            assert coordinator.effective_metadata == before_metadata
            current = registry.async_get(device.id)
            assert current.manufacturer == "Anenji"
            assert current.model == "ANJ-11KW-48V-WIFI-P"
            assert current.name_by_user == ("My inverter" if custom_name else None)
            assert entity_ids() == before_ids
        reload.assert_not_called()

        # The ordinary next options change still reloads normally.
        hass.config_entries.async_update_entry(entry, options={**entry.options, "poll_interval": 45})
        await hass.async_block_till_done()
        reload.assert_called_once_with(entry.entry_id)
    assert manager.stopped == 1
    assert entry.runtime_data is not coordinator
    assert registry.async_get(device.id).model == "ANJ-11KW-48V-WIFI-P"
    assert entity_ids() == before_ids

    manager = fake_runtime[-1]
    with patch.object(hass.config_entries, "async_reload", wraps=hass.config_entries.async_reload) as reload:
        # Missing optional fields and whitespace both clear, not restore defaults.
        await save_labels(hass, entry, {BRAND: "  "})
        await save_labels(hass, entry, {})
        reload.assert_not_called()
    assert manager.stopped == 0
    assert BRAND not in entry.options and MODEL not in entry.options
    current = registry.async_get(device.id)
    assert current.model == original_model
    assert current.manufacturer == "OEM / EyeBond"
    assert current.name_by_user == ("My inverter" if custom_name else None)
    assert entity_ids() == before_ids
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_unloaded_entry_can_save_and_reset_labels(hass, display_entry, fake_runtime):
    entry = display_entry
    before = dict(entry.data)
    await save_labels(hass, entry, {BRAND: "Anenji", MODEL: "ANJ"})
    assert not fake_runtime
    assert dict(entry.data) == before
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    registry = dr.async_get(hass)
    device = _entry_device(registry, entry.entry_id, (DOMAIN, entry.entry_id))
    assert device.model == "ANJ" and device.manufacturer == "Anenji"
    assert await hass.config_entries.async_unload(entry.entry_id)
    await save_labels(hass, entry, {})
    assert BRAND not in entry.options and MODEL not in entry.options


async def test_invalid_label_is_localized_and_does_not_commit(hass, display_entry):
    entry = display_entry
    before = dict(entry.options)
    form = await display_form(hass, entry)
    assert form["description_placeholders"]["detected_model"] == entry.data["detected_model"]
    result = await hass.config_entries.options.async_configure(
        form["flow_id"], {BRAND: "Anenji", MODEL: "a" * 129},
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {MODEL: "invalid_inverter_display_label"}
    assert dict(entry.options) == before


async def test_label_alone_cannot_identify_an_inverter(hass, display_entry, fake_runtime):
    entry = display_entry
    hass.config_entries.async_update_entry(entry, data={
        key: value for key, value in entry.data.items() if not key.startswith("detected_")
    })
    await save_labels(hass, entry, {BRAND: "Anenji", MODEL: "ANJ"})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert not entry.runtime_data.has_inverter_identity
    assert _entry_device(dr.async_get(hass), entry.entry_id, (DOMAIN, entry.entry_id)) is None
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_open_form_uses_latest_options_and_cancel_does_not_save(hass, display_entry):
    entry = display_entry
    form = await display_form(hass, entry)
    hass.config_entries.async_update_entry(entry, options={**entry.options, "poll_interval": 47})
    result = await hass.config_entries.options.async_configure(form["flow_id"], {BRAND: "Anenji"})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options["poll_interval"] == 47
    assert entry.options[BRAND] == "Anenji"
    form = await display_form(hass, entry)
    before = dict(entry.options)
    hass.config_entries.options.async_abort(form["flow_id"])
    assert dict(entry.options) == before


@pytest.mark.parametrize("mode", ["read_only", "full"])
async def test_live_technical_profile_and_controls_are_unchanged(
    hass, display_entry, fake_runtime, monkeypatch, mode,
):
    from conftest import FakeRuntimeManager
    from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
    from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot

    entry = display_entry
    profile_path = "modbus_smg/models/sandisolar_sd_11kp48v_wifi.json"
    profile = load_driver_profile(profile_path)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name=entry.data["detected_model"], serial_number="synthetic-serial",
        probe_target=ProbeTarget(1, 255, 1), profile_name=profile_path,
        register_schema_name=profile_path, capabilities=profile.capabilities,
        capability_groups=profile.groups,
    )

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(connected=True, inverter=inverter,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN))

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    hass.config_entries.async_update_entry(entry, options={**entry.options, "control_mode": mode})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    # Complete the normal first-live-profile reload before testing a cosmetic
    # change. That reload is independent of, and precedes, the display flow.
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    entity_registry = er.async_get(hass)

    def entities():
        return {item.unique_id: (item.entity_id, item.disabled_by) for item in
                er.async_entries_for_config_entry(entity_registry, entry.entry_id)}

    before = entities()
    controls = {key for key in before if "_number_" in key or "_select_" in key or "_switch_" in key}
    if mode == "full":
        assert controls
    else:
        assert not controls
    technical = coordinator.effective_metadata
    capabilities = coordinator.data.inverter.capabilities
    before_bundle = coordinator._build_support_bundle_payload(integration_build_values={})
    form = await display_form(hass, entry)
    assert form["description_placeholders"]["profile"] == profile_path
    await hass.config_entries.options.async_configure(form["flow_id"], {BRAND: "Anenji", MODEL: "ANJ"})
    await hass.async_block_till_done()
    await coordinator.async_refresh()
    await hass.async_block_till_done()
    assert coordinator is entry.runtime_data
    assert fake_runtime[-1].stopped == 0
    assert coordinator.effective_metadata == technical
    assert coordinator.data.inverter.capabilities == capabilities
    assert entities() == before
    bundle = coordinator._build_support_bundle_payload(integration_build_values={})
    assert bundle["source_metadata"] == before_bundle["source_metadata"]
    assert bundle["runtime"]["inverter"] == before_bundle["runtime"]["inverter"]
    assert bundle["inverter_display"]["source"] == "user"
    assert bundle["inverter_display"]["overrides"] == {"manufacturer": "Anenji", "model": "ANJ"}
    assert await hass.config_entries.async_unload(entry.entry_id)
