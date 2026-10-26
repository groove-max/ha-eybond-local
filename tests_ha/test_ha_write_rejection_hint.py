"""A real HA number service preserves model advice and the observed value.

HA entity/services, coordinator write serialization and hub classification are
real. Only the inverter IO and its pre-write snapshot are synthetic.
"""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.drivers.modbus_write_error import ModbusWriteErrorMixin
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.number import EybondCapabilityNumber
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.coordinator.management import CoordinatorManagementMixin
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from custom_components.eybond_local.telemetry import TelemetryFreshness, TelemetryPoint, TypedTelemetryFrame

KEY = "op1_offgrid_soc_protection_value"
GRID = "low_dc_protection_soc_grid_mode"


class HintCoordinator(CoordinatorManagementMixin, DataUpdateCoordinator):
    def __init__(self, hass, hub, snapshot):
        super().__init__(hass, logging.getLogger(__name__), name="hint-test", config_entry=None)
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
        return self.snapshot


@pytest.mark.parametrize("fresh", [True, False])
async def test_number_service_reports_hint_without_changing_limits_or_state(hass, fresh):
    profile = load_driver_profile("modbus_smg/models/sandisolar_sd_11kp48v_wifi.json")
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Synthetic Sandisolar", serial_number="synthetic",
        probe_target=ProbeTarget(1, 2, 1), capabilities=profile.capabilities,
    )
    snapshot = RuntimeSnapshot(
        connected=True, inverter=inverter,
        telemetry=TypedTelemetryFrame("modbus_smg", (
            TelemetryPoint(KEY, 18, TelemetryFreshness.FRESH),
            TelemetryPoint(GRID, 20, TelemetryFreshness.FRESH if fresh else TelemetryFreshness.CARRIED),
        )),
    )
    hub = HubManagementMixin()
    hub._inverter = inverter
    hub._driver = ModbusWriteErrorMixin()
    hub._driver.async_write_capability = AsyncMock(side_effect=ModbusError("exception_code:3"))
    hub._async_ensure_connected = AsyncMock()
    hub.async_refresh = AsyncMock(return_value=snapshot)
    hub._runtime_read_state = {}
    hub._write_blockers = {}
    hub._link_manager = SimpleNamespace(transport=object())
    coordinator = HintCoordinator(hass, hub, snapshot)
    await coordinator.async_refresh()
    assert await async_setup_component(hass, "number", {})
    entity = EybondCapabilityNumber(coordinator, inverter.get_capability(KEY))
    entity.entity_id = "number.synthetic_sandisolar_op1"
    await hass.data["number"].async_add_entities([entity])
    await hass.async_block_till_done()
    try:
        state = hass.states.get(entity.entity_id)
        assert float(state.state) == 18
        assert (state.attributes["min"], state.attributes["max"]) == (0, 100)
        assert "1 percentage point" in state.attributes["write_rejection_hint"]
        with pytest.raises((ValueError, HomeAssistantError)) as caught:
            await hass.services.async_call(
                "number", "set_value", {"entity_id": entity.entity_id, "value": 25},
                blocking=True,
            )
        message = str(caught.value)
        assert "Modbus exception 03" in message
        assert "not a confirmed cause" in message
        assert ("Grid Mode' = 20%" in message) is fresh
        assert float(hass.states.get(entity.entity_id).state) == 18
        hub._driver.async_write_capability.assert_awaited_once()
        assert hub._write_blockers == {}
    finally:
        await entity.async_remove()
        await coordinator.async_shutdown()
