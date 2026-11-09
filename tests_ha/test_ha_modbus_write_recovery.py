"""Real HA services recover after uncertain writes; only device IO is synthetic."""

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
from custom_components.eybond_local.number import EybondCapabilityNumber
from custom_components.eybond_local.runtime.coordinator.management import CoordinatorManagementMixin
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from custom_components.eybond_local.select import EybondCapabilitySelect


class RecoveryCoordinator(CoordinatorManagementMixin, DataUpdateCoordinator):
    def __init__(self, hass, hub, snapshot):
        super().__init__(hass, logging.getLogger(__name__), name="recovery-test", config_entry=None)
        self.config_entry = MockConfigEntry(domain="eybond_local")
        self._runtime_operation_lock = asyncio.Lock()
        self._runtime = hub
        self.snapshot = snapshot

    def can_expose_capability(self, capability):
        return True

    def capability_enabled_by_default(self, capability):
        return True

    def inverter_device_info(self):
        return None

    async def _async_update_data(self):
        async with self._runtime_operation_lock:
            return self.snapshot


@pytest.mark.parametrize("reply", [b"", b"\x01\x06\x01\xa9"])
@pytest.mark.parametrize("following_write_also_short", [False, True])
async def test_eq_error_releases_lock_for_next_select_without_resending(hass, reply, following_write_also_short):
    name = "modbus_smg/models/anenji_4200_protocol_1.json"
    profile = load_driver_profile(name)
    target = ProbeTarget(1, 255, 1)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic Anenji",
        serial_number="synthetic", probe_target=target, profile_name=name,
        register_schema_name=name, capabilities=profile.capabilities,
    )

    class Transport(FixtureTransport):
        writes = None

        async def async_send_payload(self, payload, *, route):
            if payload[1] in (6, 16):
                self.writes.append(payload)
                if int.from_bytes(payload[2:4], "big") == 425:
                    return reply
                if following_write_also_short and len(self.writes) == 2:
                    return reply
            return await super().async_send_payload(payload, route=route)

    transport = Transport(registers={322: 1}, command_responses=None, probe_target=target)
    transport.writes = []
    snapshot = RuntimeSnapshot(connected=True, inverter=inverter, values={
        "operating_mode": "Standby", "battery_type": "Flooded",
        "battery_equalization_enabled": True,
    })
    hub = HubManagementMixin()
    hub._inverter, hub._driver = inverter, SmgModbusDriver()
    hub._async_ensure_connected = AsyncMock()
    hub.async_refresh = AsyncMock(return_value=snapshot)
    hub._runtime_read_state, hub._write_blockers = {}, {}
    hub._link_manager = SimpleNamespace(transport=transport)
    coordinator = RecoveryCoordinator(hass, hub, snapshot)
    await coordinator.async_refresh()
    button = EybondCapabilityButton(coordinator, inverter.get_capability("equalization_activate_now"))
    select = EybondCapabilitySelect(coordinator, inverter.get_capability("battery_type"))
    button.entity_id, select.entity_id = "button.synthetic_eq", "select.synthetic_battery"
    for domain, entity in (("button", button), ("select", select)):
        assert await async_setup_component(hass, domain, {})
        await hass.data[domain].async_add_entities([entity])
    await hass.async_block_till_done()
    try:
        with pytest.raises((ValueError, HomeAssistantError), match="write_response_unconfirmed") as caught:
            await asyncio.wait_for(hass.services.async_call(
                "button", "press", {"entity_id": button.entity_id}, blocking=True,
            ), timeout=2)
        assert "FC06, register 425" in str(caught.value)
        assert f"{len(reply)} response bytes" in str(caught.value)
        assert "was not resent" in str(caught.value)
        assert not coordinator._runtime_operation_lock.locked()
        assert hub._write_blockers == {}
        assert len(transport.writes) == 1
        assert hass.states.get(select.entity_id).state == "Flooded"
        if following_write_also_short:
            with pytest.raises((ValueError, HomeAssistantError), match="register 322"):
                await asyncio.wait_for(hass.services.async_call(
                    "select", "select_option", {"entity_id": select.entity_id, "option": "Flooded"},
                    blocking=True,
                ), timeout=2)
            assert len(transport.writes) == 2
            assert not coordinator._runtime_operation_lock.locked()
            assert hub._write_blockers == {}
        # A new explicit service call succeeds once the synthetic device replies.
        await asyncio.wait_for(hass.services.async_call(
            "select", "select_option", {"entity_id": select.entity_id, "option": "Flooded"},
            blocking=True,
        ), timeout=2)
        assert len(transport.writes) == (3 if following_write_also_short else 2)
        assert int.from_bytes(transport.writes[-1][2:4], "big") == 322
        assert not coordinator._runtime_operation_lock.locked()
        assert hub._write_blockers == {}
    finally:
        await button.async_remove()
        await select.async_remove()
        await coordinator.async_shutdown()


async def test_hidden_equalization_keeps_runtime_value_but_ha_omits_unavailable_attributes(hass):
    profile = load_driver_profile("modbus_smg/models/anenji_4200_protocol_1.json")
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic Anenji",
        serial_number="synthetic", probe_target=ProbeTarget(1, 255, 1), capabilities=profile.capabilities,
    )
    cap = inverter.get_capability("battery_equalization_voltage")
    snapshot = RuntimeSnapshot(connected=True, inverter=inverter, values={
        "battery_type": "Li4", cap.value_key: 29.2,
    })
    coordinator = RecoveryCoordinator(hass, SimpleNamespace(), snapshot)
    await coordinator.async_refresh()
    assert await async_setup_component(hass, "number", {})
    entity = EybondCapabilityNumber(coordinator, cap)
    entity.entity_id = "number.synthetic_eq_voltage"
    await hass.data["number"].async_add_entities([entity])
    await hass.async_block_till_done()
    try:
        state = hass.states.get(entity.entity_id)
        assert state.state == "unavailable"
        # HA deliberately omits extra_state_attributes for unavailable entities.
        # Do not enable a writable entity merely to show a read-only value.
        assert "current_value" not in state.attributes
        assert entity.extra_state_attributes["current_value"] == 29.2
        assert entity.extra_state_attributes["status"] == "hidden"
        assert not cap.runtime_state(snapshot.runtime_values()).editable
    finally:
        await entity.async_remove()
        await coordinator.async_shutdown()
