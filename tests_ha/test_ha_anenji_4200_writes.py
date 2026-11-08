"""Real HA services -> existing coordinator/hub -> SMG wire, synthetic inverter."""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.button import EybondCapabilityButton
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.coordinator.management import CoordinatorManagementMixin
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from custom_components.eybond_local.select import EybondCapabilitySelect

PROFILE = "modbus_smg/models/anenji_4200_protocol_1.json"


class ControlCoordinator(CoordinatorManagementMixin, DataUpdateCoordinator):
    def __init__(self, hass, hub):
        super().__init__(hass, logging.getLogger(__name__), name="anenji-write", config_entry=None)
        self.config_entry = MockConfigEntry(domain="eybond_local")
        self._runtime_operation_lock = asyncio.Lock()
        self._runtime = hub

    def can_expose_capability(self, capability):
        return True  # Full Control; default-disabled entities tested in the lifecycle suite.

    def capability_enabled_by_default(self, capability):
        return True  # Represents explicit user activation in this service-level test.

    def inverter_device_info(self):
        return None

    async def _async_update_data(self):
        return await self._runtime.async_refresh()


@pytest.fixture
async def controls(hass):
    profile = load_driver_profile(PROFILE)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Synthetic Anenji 4200", serial_number="SYNTHETIC88",
        probe_target=ProbeTarget(1, 255, 1), capabilities=profile.capabilities,
        variant_key="anenji_4200_protocol_1", profile_name=PROFILE, register_schema_name=PROFILE,
    )

    class Device(FixtureTransport):
        def __init__(self):
            registers = dict.fromkeys(range(800), 0)
            registers.update({171: 0x3501, 184: 1, 322: 6, 324: 272})
            super().__init__(registers=registers, command_responses=None, probe_target=inverter.probe_target)
            self.writes, self.reject = [], False

        def _handle_write_single(self, payload):
            self.writes.append(payload)
            if self.reject:
                raise ModbusError("exception_code:3")
            response = super()._handle_write_single(payload)
            register = int.from_bytes(payload[2:4], "big")
            if register == 322:
                # Device-owned side effect, never restored by HA automatically.
                self._registers[324] = 282
            elif register == 425:
                self._registers[425] = 0  # Momentary action, no persistent-state assertion.
            return response

        def _handle_write_multiple(self, payload):
            raise AssertionError("These two controls must use FC06, not FC16")

    device = Device()
    hub = HubManagementMixin()
    hub._inverter, hub._driver = inverter, SmgModbusDriver()
    hub._runtime_read_state, hub._write_blockers = {}, {}
    hub._link_manager = SimpleNamespace(transport=device)
    hub._async_ensure_connected = AsyncMock()

    async def refresh():
        result = await hub._driver.async_read_values(device, inverter)
        return RuntimeSnapshot(connected=True, inverter=inverter, values=result.values)

    hub.async_refresh = refresh
    coordinator = ControlCoordinator(hass, hub)
    await coordinator.async_refresh()
    entities = []
    for domain, cls, key in (
        ("select", EybondCapabilitySelect, "battery_type"),
        ("button", EybondCapabilityButton, "equalization_activate_now"),
    ):
        assert await async_setup_component(hass, domain, {})
        entity = cls(coordinator, profile.get_capability(key))
        entity.entity_id = f"{domain}.synthetic88_{key}"
        await hass.data[domain].async_add_entities([entity])
        entities.append(entity)
    await hass.async_block_till_done()
    try:
        yield SimpleNamespace(device=device, hub=hub, coordinator=coordinator,
                              select=entities[0], button=entities[1])
    finally:
        for entity in entities:
            await entity.async_remove()
        await coordinator.async_shutdown()


async def test_type_write_refreshes_device_side_effects_and_eq_requires_enabled_mode(hass, controls):
    c = controls
    assert hass.states.get(c.select.entity_id).state == "Li4"
    assert hass.states.get(c.button.entity_id).state == "unavailable"
    assert c.device.writes == []  # Setup/availability never apply a setting.
    await hass.services.async_call("select", "select_option",
        {"entity_id": c.select.entity_id, "option": "Flooded"}, blocking=True)
    assert hass.states.get(c.select.entity_id).state == "Flooded"
    assert c.coordinator.data.runtime_value("battery_bulk_voltage") == 28.2
    assert len(c.device.writes) == 1
    assert c.device.writes[0][:6] == bytes.fromhex("010601420001")
    assert hass.states.get(c.button.entity_id).state == "unavailable"
    # HA skips an unavailable target; the runtime must also reject a direct call.
    await hass.services.async_call("button", "press", {"entity_id": c.button.entity_id}, blocking=True)
    with pytest.raises(ValueError, match="capability_not_editable"):
        await c.coordinator.async_write_capability("equalization_activate_now", None)
    assert len(c.device.writes) == 1
    c.device._registers[313] = 1
    await c.coordinator.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(c.button.entity_id).state != "unavailable"
    await hass.services.async_call("button", "press", {"entity_id": c.button.entity_id}, blocking=True)
    assert len(c.device.writes) == 2
    assert c.device.writes[1][:6] == bytes.fromhex("010601a90001")
    assert c.device._registers[425] == 0


async def test_write_guard_rechecks_type_after_ui_was_available(hass, controls):
    c = controls
    c.device._registers.update({322: 1, 313: 1})
    await c.coordinator.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(c.button.entity_id).state != "unavailable"
    c.device._registers[322] = 6  # Cloud/LCD changed type after the last UI refresh.
    with pytest.raises((ValueError, HomeAssistantError), match="capability_not_editable"):
        await hass.services.async_call("button", "press", {"entity_id": c.button.entity_id}, blocking=True)
    assert c.device.writes == []


async def test_rejected_type_is_not_published_or_retried_with_fc16(hass, controls):
    c = controls
    c.device.reject = True
    with pytest.raises((ValueError, HomeAssistantError), match="Modbus exception 03"):
        await hass.services.async_call("select", "select_option",
            {"entity_id": c.select.entity_id, "option": "Flooded"}, blocking=True)
    assert len(c.device.writes) == 1
    assert hass.states.get(c.select.entity_id).state == "Li4"
    assert c.device._registers[322] == 6
    assert c.hub._write_blockers == {}
