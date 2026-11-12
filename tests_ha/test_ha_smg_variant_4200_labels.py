"""Issue #94: actual HA selects receive the driver's model-scoped enum labels."""

from types import SimpleNamespace

from homeassistant.setup import async_setup_component

from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.select import EybondCapabilitySelect
from test_ha_anenji_4200_writes import ControlCoordinator

PROFILE = "modbus_smg/models/smg_variant_4200.json"


async def test_config_enum_labels_survive_full_refresh_without_unknown_or_writes(hass):
    profile = load_driver_profile(PROFILE)
    target = ProbeTarget(1, 255, 1)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic 4200",
        serial_number="SYNTHETIC94", probe_target=target, variant_key="smg_variant_4200",
        profile_name=PROFILE, register_schema_name=PROFILE, capabilities=profile.capabilities,
    )

    class ReadOnlyDevice(FixtureTransport):
        def _handle_write_single(self, payload):
            raise AssertionError("enum read-back must not change settings")

        def _handle_write_multiple(self, payload):
            raise AssertionError("enum read-back must not change settings")

    registers = dict.fromkeys(range(800), 0)
    registers.update({171: 0x7801, 184: 11, 322: 2, 338: 0})
    device = ReadOnlyDevice(registers=registers, command_responses=None, probe_target=target)

    async def refresh():
        result = await SmgModbusDriver().async_read_values(device, inverter)
        return RuntimeSnapshot(connected=True, inverter=inverter, values=result.values)

    coordinator = ControlCoordinator(hass, SimpleNamespace(async_refresh=refresh))
    entities = []
    try:
        await coordinator.async_refresh()
        assert await async_setup_component(hass, "select", {})
        for key in ("battery_type", "automatic_mains_output_enabled"):
            entity = EybondCapabilitySelect(coordinator, profile.get_capability(key))
            entity.entity_id = f"select.synthetic94_{key}"
            await hass.data["select"].async_add_entities([entity])
            entities.append(entity)
        await hass.async_block_till_done()
        assert [hass.states.get(e.entity_id).state for e in entities] == ["USER", "Manual Boot Required"]
        # Simulate a device/cloud change, then a normal complete config poll.
        device._registers.update({322: 6, 338: 1})
        await coordinator.async_refresh()
        await hass.async_block_till_done()
        assert [hass.states.get(e.entity_id).state for e in entities] == ["Li4", "Auto Output"]
        for entity in entities:
            state = hass.states.get(entity.entity_id)
            assert state.state in state.attributes["options"]
    finally:
        for entity in entities:
            await entity.async_remove()
        await coordinator.async_shutdown()
