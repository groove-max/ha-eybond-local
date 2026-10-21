"""Real overlay activation/reload and HA services; only PI30 wire is synthetic."""

import json
from pathlib import Path

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.pi30 import build_request
from custom_components.eybond_local.support.shadow_learning.pi30_protocol import Pi30ShadowLearningAdapter
from custom_components.eybond_local.support.shadow_learning.overlay_generator import generate_shadow_learning_overlay_drafts
from custom_components.eybond_local.support.shadow_learning.review_model import build_activation_selection
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("mode", ["read_only", "full"])
@pytest.mark.parametrize("kind,commands,options,labels,selected,service,data,expected", [
    ("switch", ("PDa", "PEa"), ("0", "1"), ("Off", "On"), "PEa", "turn_on", {}, "on"),
    ("select", ("POP00", "POP02"), ("48", "49"), ("First", "Second"), "POP02",
     "select_option", {"option": "Second"}, "Second"),
    ("number", ("PCVV27.9", "PCVV28.9"), ("27.9", "28.9"), ("", ""), "PCVV28.5",
     "set_value", {"value": 28.5}, "28.5"),
])
async def test_native_overlay_activation_reload_and_service(
    hass, fake_runtime, monkeypatch, tmp_path, mode, kind, commands, options, labels,
    selected, service, data, expected,
):
    from conftest import FakeRuntimeManager

    hass.config.config_dir = str(tmp_path)
    target = ProbeTarget(0x0994, 1, 0)
    fields = "230.0 26.9 230.0 50.0 26.9 6200 6200 24.0 23.0 21.0 27.9 26.8 2 030 030 1 0 1 1 01 0 0 27.0 0 1".split()
    replies = {
        "QPI": "PI30", "QID": "99000000000054", "QPIRI": " ".join(fields), "QFLAG": "ExDa",
        "QMCHGCR": "010 020 030", "QMUCHGCR": "002 010 020 030", "QMOD": "L",
        "QPIWS": "00000000000000000000000000000000", "QET": "NAK",
        "QPIGS": "232.1 50.0 232.1 50.0 0440 0227 007 429 26.60 000 100 0043 00.0 000.0 00.00 00000 00010101 00 00 00000 110",
        "Q1": "00 00 00 000 042 030 043 00 00 000 0030 0000 13",
    }
    transport = FixtureTransport(registers=None, probe_target=target,
        command_responses={(target.devcode, 1, key): value for key, value in replies.items()})
    allow_writes = False
    writes = []
    original_send = transport.async_send_payload

    async def send(payload, *, route):
        command = payload[:-3].decode("ascii")
        if not command.startswith("Q"):
            assert allow_writes, "Setup, activation and reload must never write"
            assert command == selected
            writes.append(command)
            replies["QFLAG"] = "EaxD"
            fields[16], fields[10] = "2", "28.5"
            replies["QPIRI"] = " ".join(fields)
            return build_request("(ACK")
        if command in replies:
            return build_request("(" + replies[command])
        return await original_send(payload, route=route)

    monkeypatch.setattr(transport, "async_send_payload", send)
    driver = Pi30Driver()
    inverter = await driver.async_probe(transport, target)
    assert inverter is not None
    collector = CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN)

    async def refresh(self, *, poll_interval=None):
        binding = getattr(self, "initial_binding", inverter)
        if self.overlay_applier:
            binding = self.overlay_applier(binding, collector)
        self.initial_binding = binding
        if not hasattr(self, "driver_state"):
            self.driver_state, self.values, self.now = {}, {}, 100.0
        self.now += 70
        result = await driver.async_read_values(
            transport, binding, runtime_state=self.driver_state, now_monotonic=self.now,
        )
        self.values.update(result.values)
        for key in result.removed_keys:
            self.values.pop(key, None)
        return RuntimeSnapshot(connected=True, inverter=binding, collector=collector, values=dict(self.values))

    async def write(self, key, value):
        assert allow_writes
        return await driver.async_write_capability(
            transport, self.initial_binding, key, value, runtime_state=self.driver_state,
        )

    monkeypatch.setattr(FakeRuntimeManager, "set_initial_inverter_binding",
                        lambda self, driver, binding: setattr(self, "initial_binding", binding), raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "set_inverter_overlay_applier",
                        lambda self, callback: setattr(self, "overlay_applier", callback), raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", write)
    entry = MockConfigEntry(domain=DOMAIN, version=5, unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={"connection_type": "eybond", "connection_mode": "known_ip",
              "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
              "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
              "driver_hint": "pi30", "control_mode": mode, "connection_strategy": "callback_on_demand",
              "endpoint_control_policy": "external", "detected_driver": "pi30",
              "detected_model": inverter.model_name, "detected_serial": "", "detection_confidence": "high"},
        options={"poll_interval": 30, "poll_mode": "auto"})
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    adapter = Pi30ShadowLearningAdapter()
    matched = []
    for command, value, label in zip(commands, options, labels):
        item = {"sequence_index": len(matched), "field_id": "synthetic_setting", "field_name": "Learned setting",
                "requested_value": value, "value_label": label,
                "value_source": "numeric_probe" if kind == "number" else "choice",
                "observation": adapter.write_observation(frame=build_request(command),
                    devcode=target.devcode, devaddr=target.collector_addr).to_json_dict()}
        if kind == "number":
            item["numeric_evidence"] = {"kind": "number", "unit": "V"}
        matched.append(item)

    def generate():
        return generate_shadow_learning_overlay_drafts(config_dir=Path(hass.config.config_dir),
            source_profile_name=inverter.profile_name, source_schema_name=inverter.register_schema_name,
            session_manifest={"session_id": "synthetic-native-learning", "collector_pn": SYNTHETIC_COLLECTOR_PN,
                              "devcode": target.devcode, "devaddr": target.collector_addr},
            correlation={"matched": matched})

    draft = await hass.async_add_executor_job(generate)
    raw = json.loads(await hass.async_add_executor_job(draft.profile_path.read_text))
    assert draft.generated_capability_count == 1
    key = raw["capabilities"][0]["key"]
    assert not entry.runtime_data.effective_metadata.device_scoped_overlay_active
    selection = build_activation_selection(review_model=draft.manifest["review_model"],
        selections={"controls": {key: {"enabled": True}}})
    await entry.runtime_data.async_activate_device_scoped_overlay(
        profile_name=draft.manifest["output"]["profile_name"],
        register_schema_name=draft.manifest["output"]["schema_name"], selection=selection)
    await hass.async_block_till_done()
    registry = er.async_get(hass)
    identity = None
    for _ in range(2):
        assert entry.runtime_data.effective_metadata.device_scoped_overlay_active
        for _ in range(3):
            await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        entity_id = registry.async_get_entity_id(kind, DOMAIN, f"{entry.entry_id}_{kind}_{key}")
        if mode == "read_only":
            assert entity_id is None
            with pytest.raises(PermissionError, match="capability_control_disabled"):
                await entry.runtime_data.async_write_capability(key, True)
        else:
            assert entity_id is not None
            assert hass.states.get(entity_id).state not in ("unknown", "unavailable")
            assert identity in (None, entity_id)
            identity = entity_id
        assert writes == []
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
    if mode == "full":
        allow_writes = True
        await hass.services.async_call(kind, service, {"entity_id": identity, **data}, blocking=True)
        await hass.async_block_till_done()
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert writes == [selected]
        assert hass.states.get(identity).state == expected
    assert await hass.config_entries.async_unload(entry.entry_id)
