"""Generated MUST controls retain their wire function through real HA activation.

Only the device/runtime boundary is synthetic. Profile generation, selection,
activation, entry reload, entities, services and MUST Modbus encoding run for real.
No cloud learning session, socket or physical inverter is used.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.must import (
    MustPvPh18Driver,
    _support_capture_ranges,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import decode_write_request
from custom_components.eybond_local.support.shadow_learning.overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)
from custom_components.eybond_local.support.shadow_learning.review_model import (
    build_activation_selection,
)
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


_BASE_PROFILE = "must_pv_ph18/pv3300.json"
# Deliberately outside the built-in control map: these are synthetic registers,
# not a proposal to write these addresses on a real PV3300.
_LEVEL_REGISTER = 20250
_MODE_REGISTER = 20251


def _correlation(target: ProbeTarget) -> dict:
    """One FC06 numeric setting and one single-word FC16 enum setting."""

    matched = []
    for register, function, field_id, title, value, label in (
        (_LEVEL_REGISTER, 6, "synthetic_level", "Synthetic learned level", 20, ""),
        (_MODE_REGISTER, 16, "synthetic_mode", "Synthetic learned mode", 3, "Mode A"),
        (_MODE_REGISTER, 16, "synthetic_mode", "Synthetic learned mode", 4, "Mode B"),
    ):
        matched.append(
            {
                "sequence_index": len(matched),
                "field_id": field_id,
                "field_name": title,
                "requested_value": str(value),
                "value_label": label,
                "value_source": "choice" if label else "current",
                "requested_at": "2026-10-02T10:00:00+00:00",
                "observation": {
                    "function_code": function,
                    "register": register,
                    "values": [value],
                    "devcode": target.devcode,
                    "devaddr": target.collector_addr,
                    "unit": target.device_addr,
                    "timestamp": "2026-10-02T10:00:00+00:00",
                },
            }
        )
    return {
        "matched": matched,
        "matched_count": len(matched),
        "unmatched_attempt_count": 0,
        "unmatched_write_count": 0,
    }


@pytest.mark.parametrize("mode", ["read_only", "auto", "full"])
async def test_generated_must_overlay_activation_and_modbus_service_writes(
    hass, fake_runtime, monkeypatch, tmp_path, mode
):
    from conftest import FakeRuntimeManager

    hass.config.config_dir = str(tmp_path)
    registers = {
        register: 0
        for start, count in _support_capture_ranges("must_pv_ph18/base.json")
        for register in range(start, start + count)
    } | {
        20000: int.from_bytes(b"PV", "big"),
        20001: 3300,
        20101: 1,
        20109: 3,
        20125: 300,
        20132: 800,
        20143: 2,
        25205: 512,
        25210: 1,
        25211: 1,
        25212: 1,
        **dict.fromkeys(range(109, 114), 0),
        _LEVEL_REGISTER: 20,
        _MODE_REGISTER: 3,
    }
    allow_writes = False
    write_frames: list[bytes] = []
    runtime_writes: list[tuple[str, object]] = []

    class SyntheticTransport(FixtureTransport):
        async def async_send_payload(self, payload, *, route):
            if payload[1] != 3:
                assert allow_writes, "Generation/setup/activation/reload must never write"
                assert payload[1] in (6, 16)
                write_frames.append(bytes(payload))
            return await super().async_send_payload(payload, route=route)

    driver = MustPvPh18Driver()
    target = ProbeTarget(1, 255, 4)
    transport = SyntheticTransport(
        registers=registers, command_responses=None, probe_target=target
    )
    inverter = await driver.async_probe(transport, target)
    assert inverter is not None
    assert inverter.profile_name == _BASE_PROFILE
    assert not {_LEVEL_REGISTER, _MODE_REGISTER} & {
        capability.register for capability in inverter.capabilities
    }
    collector = CollectorInfo(
        remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN
    )

    def seed_binding(self, bound_driver, binding):
        self.initial_binding = binding

    def set_overlay_applier(self, callback):
        self.overlay_applier = callback

    async def refresh(self, *, poll_interval=None):
        binding = getattr(self, "initial_binding", inverter)
        if self.overlay_applier is not None:
            # Use the real coordinator hook, just as the runtime hub does when
            # publishing a snapshot. Never inject learned capabilities by hand.
            binding = self.overlay_applier(binding, collector)
        self.initial_binding = binding
        if not hasattr(self, "driver_state"):
            self.driver_state = {}
        read = await driver.async_read_values(
            transport, binding, runtime_state=self.driver_state, now_monotonic=100.0
        )
        return RuntimeSnapshot(
            connected=True,
            inverter=binding,
            collector=collector,
            values=read.diagnostics,
            telemetry=fold_driver_telemetry(
                TypedTelemetryFrame.empty(),
                driver_key=driver.key,
                values=read.values,
                replace=True,
            ),
        )

    async def write(self, key, value):
        assert allow_writes, "Read-only and automatic lifecycle must not reach runtime writes"
        runtime_writes.append((key, value))
        return await driver.async_write_capability(
            transport, self.initial_binding, key, value, runtime_state=self.driver_state
        )

    monkeypatch.setattr(FakeRuntimeManager, "set_initial_inverter_binding", seed_binding, raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "set_inverter_overlay_applier", set_overlay_applier, raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", write)

    entry = MockConfigEntry(
        domain=DOMAIN,
        version=5,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond",
            "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
            "control_mode": mode,
            "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external",
            "proxy_enabled": False,
            "detected_driver": driver.key,
            "detected_model": inverter.model_name,
            "detected_serial": "",
            "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()

    def generate():
        result = generate_shadow_learning_overlay_drafts(
            config_dir=Path(hass.config.config_dir),
            source_profile_name=_BASE_PROFILE,
            source_schema_name=_BASE_PROFILE,
            session_manifest={
                "session_id": "synthetic-must-write-functions",
                "collector_pn": SYNTHETIC_COLLECTOR_PN,
                "devcode": target.devcode,
                "devaddr": target.collector_addr,
                "write_response_mode": "exception",
            },
            correlation=_correlation(target),
        )
        return result, json.loads(result.profile_path.read_text(encoding="utf-8"))

    draft, raw_profile = await hass.async_add_executor_job(generate)
    assert draft.generated_capability_count == 2
    assert draft.skipped_duplicate_count == 0
    raw_controls = {item["register"]: item for item in raw_profile["capabilities"]}
    expected_functions = {_LEVEL_REGISTER: 6, _MODE_REGISTER: 16}
    keys = {register: item["key"] for register, item in raw_controls.items()}
    for register, function in expected_functions.items():
        assert raw_controls[register]["write_function"] == function
        assert raw_controls[register]["word_count"] == 1
        assert raw_controls[register]["enabled_default"] is False

    # Merely generating a draft neither activates it nor creates writable entities.
    assert not entry.runtime_data.effective_metadata.device_scoped_overlay_active
    assert not set(keys.values()) & {
        capability.key for capability in entry.runtime_data.data.inverter.capabilities
    }
    for register, kind in ((_LEVEL_REGISTER, "number"), (_MODE_REGISTER, "select")):
        assert registry.async_get_entity_id(
            kind, DOMAIN, f"{entry.entry_id}_{kind}_{keys[register]}"
        ) is None
    assert runtime_writes == write_frames == []

    selection = build_activation_selection(
        review_model=draft.manifest["review_model"],
        selections={"controls": {key: {"enabled": True} for key in keys.values()}},
    )
    previous_coordinator = entry.runtime_data
    previous_runtime = fake_runtime[-1]
    activation = await previous_coordinator.async_activate_device_scoped_overlay(
        profile_name=draft.manifest["output"]["profile_name"],
        register_schema_name=draft.manifest["output"]["schema_name"],
        selection=selection,
    )
    await hass.async_block_till_done()
    assert set(activation["selected_control_keys"]) == set(keys.values())
    assert entry.runtime_data is not previous_coordinator
    assert fake_runtime[-1] is not previous_runtime
    assert previous_runtime.stopped == 1

    entity_ids = {}
    # Check both the activation-triggered reload and a later ordinary reload.
    for reload_index in range(2):
        coordinator = entry.runtime_data
        await coordinator.async_refresh()
        await hass.async_block_till_done()
        assert coordinator.effective_metadata.device_scoped_overlay_active
        assert coordinator.effective_profile_name == activation["profile_name"]
        loaded_profile = await hass.async_add_executor_job(
            load_driver_profile, activation["profile_name"]
        )
        for register, kind in ((_LEVEL_REGISTER, "number"), (_MODE_REGISTER, "select")):
            key = keys[register]
            capability = coordinator.data.inverter.get_capability(key)
            assert loaded_profile.get_capability(key).write_function == expected_functions[register]
            assert capability.write_function == expected_functions[register]
            assert capability.is_device_scoped_experimental
            assert capability.tested is False
            entity_id = registry.async_get_entity_id(
                kind, DOMAIN, f"{entry.entry_id}_{kind}_{key}"
            )
            if mode == "read_only":
                assert entity_id is None
                with pytest.raises(PermissionError, match="capability_control_disabled"):
                    await coordinator.async_write_capability(key, 25 if kind == "number" else 4)
            else:
                assert entity_id is not None
                assert registry.async_get(entity_id).disabled_by is None
                assert hass.states.get(entity_id) is not None
                assert hass.states.get(entity_id).state != "unavailable"
                assert entity_id == entity_ids.setdefault(register, entity_id)
        assert runtime_writes == write_frames == []
        if reload_index == 0:
            assert await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.runtime_data is not coordinator

    if mode != "read_only":
        for kind, register, function, native, raw in (
            ("number", _LEVEL_REGISTER, 6, 25, 25),
            ("number", _LEVEL_REGISTER, 6, 20, 20),
            ("select", _MODE_REGISTER, 16, "Mode B", 4),
            ("select", _MODE_REGISTER, 16, "Mode A", 3),
        ):
            before = len(write_frames)
            allow_writes = True
            try:
                await hass.services.async_call(
                    kind,
                    "set_value" if kind == "number" else "select_option",
                    {
                        "entity_id": entity_ids[register],
                        "value" if kind == "number" else "option": native,
                    },
                    blocking=True,
                )
            finally:
                allow_writes = False
            await hass.async_block_till_done()
            assert len(write_frames) == before + 1
            assert len(runtime_writes) == before + 1
            assert runtime_writes[-1][0] == keys[register]
            request = decode_write_request(write_frames[-1])
            assert request is not None
            assert request.function_code == function
            assert request.slave_id == target.device_addr
            assert request.address == register
            assert request.values == (raw,)
            assert transport._registers[register] == raw

    before_unload = len(write_frames)
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert len(write_frames) == before_unload
    assert before_unload == (0 if mode == "read_only" else 4)
