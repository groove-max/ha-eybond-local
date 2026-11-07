"""Real HA entities follow the same editability rules as the write guard."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from custom_components.eybond_local.models import (
    CapabilityCondition, DetectedInverter, ProbeTarget, RuntimeSnapshot, WriteCapability,
)
from custom_components.eybond_local.number import EybondCapabilityNumber
from custom_components.eybond_local.select import EybondCapabilitySelect
from custom_components.eybond_local.switch import EybondCapabilitySwitch


@pytest.mark.parametrize("entity_type,kind", [
    (EybondCapabilityNumber, "u16"),
    (EybondCapabilitySelect, "enum"),
    (EybondCapabilitySwitch, "bool"),
])
async def test_editability_controls_availability_without_treating_warnings_as_blocks(
    hass, entity_type, kind,
):
    cap = WriteCapability(
        key="setting", register=320, value_kind=kind, note="Synthetic",
        enum_map={0: "Off", 1: "On"} if kind == "enum" else None,
        editable_if=(CapabilityCondition(
            key="operating_mode", operator="eq", value="Standby", effect="disable",
        ),),
    )
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Synthetic", serial_number="SYNTHETIC",
        probe_target=ProbeTarget(1, 255, 1), capabilities=(cap,),
    )
    coordinator = SimpleNamespace(
        config_entry=SimpleNamespace(entry_id="synthetic-editability"),
        capability_enabled_by_default=lambda _cap: True,
        can_expose_capability=lambda _cap: True,
        data=RuntimeSnapshot(connected=True, inverter=inverter, values={}),
    )
    entity = entity_type(coordinator, cap)
    for mode in (None, "Line", "Power On", "Fault"):
        coordinator.data = replace(coordinator.data, values={"operating_mode": mode})
        assert not entity.available
    coordinator.data = replace(coordinator.data, values={"operating_mode": "Standby", "setting": 0})
    assert entity.available  # Zero/off is a valid current value.
    coordinator.data = replace(coordinator.data, values={
        "operating_mode": "Standby", "capability_block_reason_setting": "Interlock",
    })
    assert not entity.available
    coordinator.data = replace(coordinator.data, values={
        "operating_mode": "Standby", "capability_hidden_reason_setting": "Not applicable",
    })
    assert not entity.available
    warning_cap = replace(cap, editable_if=(replace(cap.editable_if[0], effect="warn"),))
    warning_entity = entity_type(coordinator, warning_cap)
    coordinator.data = replace(coordinator.data, values={"operating_mode": "Line"})
    assert warning_entity.available
    coordinator.data = replace(coordinator.data, connected=False)
    assert not warning_entity.available
    coordinator.data = replace(coordinator.data, connected=True, inverter=None)
    assert not warning_entity.available
    coordinator.data = replace(coordinator.data, inverter=replace(inverter, capabilities=()))
    assert not warning_entity.available
