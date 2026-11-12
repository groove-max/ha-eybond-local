"""Issue #94: model config labels must agree with writable enum options."""

import json
from pathlib import Path
import tempfile
import unittest

from custom_components.eybond_local.drivers.smg import SmgModbusDriver, _decode_block
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.metadata.register_schema_loader import (
    _parse_spec, load_register_schema, load_register_schema_raw, set_external_register_schema_roots,
)
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget

PROFILE = "modbus_smg/models/smg_variant_4200.json"
KEYS = ("battery_type", "automatic_mains_output_enabled")


class SmgVariant4200LabelTests(unittest.IsolatedAsyncioTestCase):
    def test_every_model_enum_value_matches_options_and_runtime_mirror(self):
        schema, profile = load_register_schema(PROFILE), load_driver_profile(PROFILE)
        for key in KEYS:
            capability = profile.get_capability(key)
            config = next(spec for spec in schema.spec_set("config") if spec.key == key)
            runtime = next(spec for spec in schema.spec_set("runtime") if spec.key == key)
            for raw, label in capability.enum_value_map.items():
                with self.subTest(key=key, raw=raw):
                    self.assertEqual(_decode_block(config.register, [raw], (config,))[key], label)
                    self.assertEqual(_decode_block(runtime.register, [raw], (runtime,))[key], label)
                    self.assertIn(label, capability.enum_options)

    def test_override_does_not_change_base_or_unrelated_config_specs(self):
        base = load_register_schema("modbus_smg/base.json")
        schema = load_register_schema(PROFILE)
        before = {s.key: s for s in base.spec_set("config")}
        after = {s.key: s for s in schema.spec_set("config")}
        self.assertEqual(before.keys(), after.keys())
        self.assertEqual(len(before), 30)
        # Existing model enum tables already customize some other base specs.
        # Compare against the original config definitions with those tables,
        # not against unrelated generic-model labels.
        original = load_register_schema_raw("modbus_smg/base.json")["spec_sets"]["config"]
        for raw_spec in original:
            key = raw_spec["key"]
            if key not in KEYS:
                self.assertEqual(after[key], _parse_spec(raw_spec, schema.enum_tables))
        self.assertEqual(_decode_block(322, [2], (before["battery_type"],))["battery_type"], "User")
        self.assertEqual(_decode_block(338, [0], (before["automatic_mains_output_enabled"],))[
            "automatic_mains_output_enabled"], "Disabled")
        self.assertEqual(schema.enum_tables["enabled_disabled"], base.enum_tables["enabled_disabled"])

    def test_learned_builtin_parent_inherits_fix_without_manual_overlay(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "learned.json"
            path.write_text(json.dumps({"extends": "builtin:" + PROFILE,
                                        "schema_key": "synthetic_learned94"}), encoding="utf-8")
            set_external_register_schema_roots((Path(tmp),))
            try:
                learned = load_register_schema("learned.json")
                for key, register, raw, expected in (
                    ("battery_type", 322, 2, "USER"),
                    ("automatic_mains_output_enabled", 338, 0, "Manual Boot Required"),
                ):
                    spec = next(s for s in learned.spec_set("config") if s.key == key)
                    self.assertEqual(_decode_block(register, [raw], (spec,))[key], expected)
            finally:
                set_external_register_schema_roots(())

    async def test_actual_driver_full_poll_uses_model_labels_without_writes(self):
        profile = load_driver_profile(PROFILE)
        target = ProbeTarget(1, 255, 1)
        inverter = DetectedInverter(
            driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic 4200",
            serial_number=None, probe_target=target, variant_key="smg_variant_4200",
            profile_name=PROFILE, register_schema_name=PROFILE, capabilities=profile.capabilities,
        )

        class ReadOnlyDevice(FixtureTransport):
            def _handle_write_single(self, payload):
                raise AssertionError("poll must not write")

            def _handle_write_multiple(self, payload):
                raise AssertionError("poll must not write")

        registers = dict.fromkeys(range(800), 0)
        registers.update({171: 0x7801, 184: 11, 322: 2, 338: 0})
        device = ReadOnlyDevice(registers=registers, command_responses=None, probe_target=target)
        result = await SmgModbusDriver().async_read_values(device, inverter)
        self.assertEqual(result.values["battery_type"], "USER")
        self.assertEqual(result.values["automatic_mains_output_enabled"], "Manual Boot Required")
