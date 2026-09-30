"""Exact-model output modes must not inherit another SMG model's enum."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers.smg import SmgModbusDriver, _decode_block
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.effective_metadata_snapshot import build_effective_metadata_snapshot_from_runtime
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import ModbusError, crc16_modbus
from custom_components.eybond_local.schema import capability_write_exposure_allowed

NAME = "modbus_smg/models/anenji_anj_6200_48pl.json"
OLD = "modbus_smg/models/smg_6200.json"


def registers():
    fixture = json.loads((ROOT / "tests/fixtures/smg_protocol_11_7904.json").read_text())
    result = {r["start"] + i: word for r in fixture["ranges"] for i, word in enumerate(r["values"])}
    result.update({171: 0x2300, 184: 2, 301: 2, 643: 6200})
    return result


class RecordingTransport(FixtureTransport):
    def __init__(self, values=None, *, reject=False):
        super().__init__(registers=registers() if values is None else values,
                         command_responses=None, probe_target=ProbeTarget(1, 255, 1))
        self.requests = []
        self.reject = reject

    async def async_send_forward(self, payload, *, devcode, collector_addr):
        self.requests.append(payload)
        if self.reject and payload[1] in (6, 16):
            response = bytes((payload[0], payload[1] | 0x80, 3))
            return response + crc16_modbus(response).to_bytes(2, "little")
        return await super().async_send_forward(payload, devcode=devcode, collector_addr=collector_addr)


class Anj6200PriorityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.force = patch("custom_components.eybond_local.metadata.device_catalog_loader.FORCE_UNSUPPORTED_MODELS", False)
        self.force.start()
        self.addCleanup(self.force.stop)
        self.driver = SmgModbusDriver()
        self.target = ProbeTarget(1, 255, 1)

    async def test_exact_model_selects_dedicated_surface_and_invalidates_old_binding(self):
        transport = RecordingTransport()
        inverter = await self.driver.async_probe(transport, self.target)
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.model_name, "Anenji ANJ-6200-48PL")
        self.assertEqual(inverter.variant_key, "anenji_anj_6200_48pl")
        self.assertEqual(inverter.profile_name, NAME)
        self.assertEqual(inverter.register_schema_name, NAME)
        snapshot = build_effective_metadata_snapshot_from_runtime(inverter=inverter, confidence="high")
        self.assertTrue(snapshot.is_valid)
        old = replace(snapshot, surface_key="smg_6200_full", variant_key="default",
                      profile_name=OLD, register_schema_name=OLD,
                      descriptor_revisions=("anenji_anj_6200_48pl:old",))
        self.assertFalse(old.is_valid)
        self.assertTrue(all(request[1] == 3 for request in transport.requests))

    async def test_each_mode_reads_and_writes_the_same_native_value(self):
        transport = RecordingTransport()
        inverter = await self.driver.async_probe(transport, self.target)
        cap = load_driver_profile(NAME).get_capability("output_source_priority")
        self.assertEqual({c.value for c in cap.choices}, {1, 2, 3, 4})
        for choice in cap.choices:
            with self.subTest(value=choice.value):
                await self.driver.async_write_capability(transport, inverter, cap.key, choice.label)
                self.assertEqual(transport._registers[301], choice.value)
                values = (await self.driver.async_read_values(transport, inverter)).values
                self.assertEqual(values[cap.key], choice.label)

    async def test_rejection_remains_a_failure_with_no_fallback_write(self):
        transport = RecordingTransport(reject=True)
        inverter = await self.driver.async_probe(transport, self.target)
        transport.requests.clear()
        with self.assertRaisesRegex(ModbusError, "exception_code:3"):
            await self.driver.async_write_capability(transport, inverter, "output_source_priority", 3)
        writes = [r for r in transport.requests if r[1] in (6, 16)]
        self.assertEqual(len(writes), 1)
        self.assertEqual(transport._registers[301], 2)

    async def test_old_modes_and_unknown_values_are_rejected_before_transport(self):
        transport = RecordingTransport()
        inverter = await self.driver.async_probe(transport, self.target)
        transport.requests.clear()
        for value in (0, 5, "Utility First (UTI)", "Solar First (SOL)"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "unsupported_enum"):
                await self.driver.async_write_capability(transport, inverter, "output_source_priority", value)
        self.assertEqual(transport.requests, [])

    def test_read_enum_and_selector_agree_and_no_other_surface_changes(self):
        profile = load_driver_profile(NAME)
        previous = load_driver_profile(OLD)
        schema = load_register_schema(NAME)
        previous_schema = load_register_schema(OLD)
        cap = profile.get_capability("output_source_priority")
        spec = next(s for s in schema.spec_set("config") if s.key == cap.key)
        self.assertTrue(cap.tested)
        self.assertEqual(cap.provenance, "verified")
        self.assertEqual(spec.register, 301)
        for choice in cap.choices:
            self.assertEqual(_decode_block(301, [choice.value], (spec,))[cap.key], choice.label)
        self.assertEqual(schema.blocks, previous_schema.blocks)
        self.assertEqual(
            [c for c in profile.capabilities if c.key != cap.key],
            [c for c in previous.capabilities if c.key != cap.key],
        )
        self.assertEqual({c.value for c in previous.get_capability(cap.key).choices}, {0, 1, 2, 3})
        for mode, expected in (("auto", True), ("read_only", False), ("full", True)):
            self.assertEqual(capability_write_exposure_allowed(
                cap, control_mode=mode, detection_confidence="high",
                variant_key="anenji_anj_6200_48pl", profile_source_scope="builtin",
                schema_source_scope="builtin", profile_name=NAME,
            ), expected)

    async def test_smg6200_and_unknown_protocol2_keep_their_own_profiles(self):
        for protocol, model, expected in ((1, 0x1E00, OLD), (2, 0x9999, "modbus_smg/protocols/communication_protocol_2.json")):
            data = registers() | {184: protocol, 171: model, 235: 0, 236: 0}
            inverter = await self.driver.async_probe(RecordingTransport(data), self.target)
            self.assertIsNotNone(inverter)
            self.assertEqual(inverter.profile_name, expected)
            if protocol == 2:
                self.assertFalse(load_driver_profile(expected).get_capability("output_source_priority").tested)
