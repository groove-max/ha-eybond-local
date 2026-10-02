"""Learned Modbus operations must retain the observed wire contract.

All register addresses/frames below are synthetic, not an Ongrid Switch map.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.drivers.must import MustPvPh18Driver
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.local_metadata import local_profiles_root
from custom_components.eybond_local.metadata.profile_loader import (
    clear_profile_loader_cache,
    load_driver_profile,
    set_external_profile_roots,
)
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.modbus import decode_write_request
from custom_components.eybond_local.schema import capability_write_exposure_allowed
from custom_components.eybond_local.support.shadow_learning.overlay_generator import (
    _build_learned_capabilities,
    generate_shadow_learning_overlay_drafts,
)
from custom_components.eybond_local.support.shadow_learning.valuecloud_orchestrator import (
    build_valuecloud_learning_plan,
)


def _correlation(function: int = 6) -> dict:
    return {
        "matched": [
            {
                "sequence_index": value,
                "field_id": "synthetic_switch",
                "field_name": "Synthetic switch",
                "requested_value": str(value),
                "value_label": label,
                "value_source": "choice",
                "observation": {
                    "register": 60000,
                    "function_code": function,
                    "values": [value],
                    "unit": 4,
                    "devcode": 1,
                    "devaddr": 255,
                },
            }
            for value, label in ((0, "Deactivate"), (1, "Enable"))
        ],
    }


def _build(correlation: dict):
    return _build_learned_capabilities(
        source_profile=load_driver_profile("must_pv_ph18/pv3300.json"),
        correlation=correlation,
        session_manifest={"session_id": "synthetic-write-contract"},
    )


class LearnedModbusWriteContractTests(unittest.TestCase):
    def test_preserves_observed_function_in_capability_and_support_summary(self):
        for function in (6, 16):
            with self.subTest(function=function):
                capabilities, summary = _build(_correlation(function))
                self.assertEqual(len(capabilities), 1)
                self.assertEqual(capabilities[0]["write_function"], function)
                self.assertEqual(summary["generated"][0]["write_function"], function)
                self.assertFalse(capabilities[0]["tested"])
                self.assertFalse(capabilities[0]["enabled_default"])

    def test_incomplete_or_conflicting_observations_never_guess_a_function(self):
        cases = [
            {}, {"function_code": None}, {"function_code": 0},
            {"function_code": True}, {"function_code": "6"},
            {"function_code": 6.0}, {"function_code": 5},
            {"function_code": 16},  # Conflicts with the other FC06 sample.
            {"values": []}, {"values": None}, {"values": [1, 2]},
            {"values": [1, 2, 3]}, {"values": ["1"]},
            {"values": [True]}, {"values": [1.5]},
            {"values": [-1]}, {"values": [65536]}, {"values": {"0": 1}},
        ]
        for change in cases:
            with self.subTest(change=change):
                correlation = _correlation()
                observation = correlation["matched"][1]["observation"]
                if not change:
                    observation.pop("function_code")
                else:
                    observation.update(change)
                original = deepcopy(correlation)
                capabilities, summary = _build(correlation)
                self.assertEqual(capabilities, [])
                self.assertEqual(summary["generated"], [])
                self.assertEqual(summary["skipped"], [])
                self.assertEqual(summary["rejected"][0]["reason"],
                                 "unproven_modbus_write_contract")
                self.assertEqual(correlation, original, "Support evidence must not be rewritten")

    def test_fc16_width_must_be_consistent_and_representable(self):
        for first, second, reason in (
            ([0], [0, 1], "unproven_modbus_write_contract"),
            ([0, 0], [0, 1], "unsupported_modbus_write_shape"),
            ([0, 0, 0], [0, 0, 1], "unproven_modbus_write_contract"),
        ):
            with self.subTest(first=first, second=second):
                correlation = _correlation(16)
                for row, words in zip(correlation["matched"], (first, second)):
                    row["observation"]["values"] = words
                capabilities, summary = _build(correlation)
                self.assertEqual(capabilities, [])
                self.assertEqual(summary["rejected"][0]["reason"], reason)

    def test_invalid_field_does_not_discard_other_controls(self):
        correlation = _correlation()
        invalid = deepcopy(correlation["matched"][0])
        invalid["field_id"] = "missing_function"
        invalid["observation"]["register"] = 60001
        invalid["observation"].pop("function_code")
        correlation["matched"].append(invalid)
        capabilities, summary = _build(correlation)
        self.assertEqual(len(capabilities), 1)
        self.assertEqual(capabilities[0]["register"], 60000)
        self.assertEqual(summary["rejected"][0]["register"], 60001)

    def test_fc16_two_word_numeric_capture_does_not_break_valid_neighbor(self):
        correlation = _correlation()
        row = _correlation(16)["matched"][0]
        row.update(requested_value="65537", value_source="current", value_label="")
        row["field_id"] = "synthetic_multiword"
        row["observation"].update(register=60001, values=[1, 1])
        correlation["matched"].append(row)
        capabilities, summary = _build(correlation)
        self.assertEqual(len(capabilities), 1)
        self.assertEqual(summary["rejected"][0]["reason"], "unsupported_modbus_write_shape")
        self.assertEqual(summary["rejected"][0]["register"], 60001)
        try:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                result = generate_shadow_learning_overlay_drafts(
                    config_dir=root, source_profile_name="must_pv_ph18/pv3300.json",
                    source_schema_name="must_pv_ph18/pv3300.json",
                    session_manifest={"session_id": "synthetic-mixed-widths"},
                    correlation=correlation,
                )
                set_external_profile_roots((local_profiles_root(root),))
                clear_profile_loader_cache()
                profile = load_driver_profile(result.profile_path.relative_to(local_profiles_root(root)).as_posix())
                self.assertIn(60000, {c.register for c in profile.capabilities})
                self.assertNotIn(60001, {c.register for c in profile.capabilities})
                self.assertEqual(result.manifest["skipped_controls"][0]["register"], 60001)
        finally:
            set_external_profile_roots(())
            clear_profile_loader_cache()

    def test_rejected_contract_is_archived_separately_from_builtin_duplicates(self):
        correlation = _correlation()
        correlation["matched"][1]["observation"].pop("function_code")
        with tempfile.TemporaryDirectory() as temp:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp), source_profile_name="must_pv_ph18/pv3300.json",
                source_schema_name="must_pv_ph18/pv3300.json",
                session_manifest={"session_id": "synthetic-incomplete-capture"},
                correlation=correlation,
            )
        self.assertEqual(result.generated_capability_count, 0)
        self.assertEqual(result.skipped_duplicate_count, 0)
        self.assertEqual(result.manifest["skipped_duplicates"], [])
        self.assertEqual(result.manifest["skipped_controls"][0]["reason"],
                         "unproven_modbus_write_contract")

    def test_builtin_controls_stay_deduplicated(self):
        correlation = _correlation()
        for row in correlation["matched"]:
            row["observation"]["register"] = 20125
        capabilities, summary = _build(correlation)
        self.assertEqual(capabilities, [])
        self.assertEqual(summary["skipped"][0]["reason"], "register_already_mapped")

    def test_valuecloud_ongrid_metadata_plans_both_legacy_actions_without_writes(self):
        # Metadata shape from #46, deliberately no inferred local register.
        field = {
            "id": "shutdown_work", "name": "Ongrid Switch", "readwrite": "RW",
            "detailsId": None, "controlItemId": None, "datatype": 3,
            "item": [{"key": "0", "val": "Deactivate"}, {"key": "1", "val": "Enable"}],
        }
        plan = build_valuecloud_learning_plan(
            {"groups": []}, control_strategy={"fields": [field]},
            device_ctrl={"fields": [field]}, max_fields=40,
        )
        self.assertEqual([(row["field_id"], row["value"], row["value_label"]) for row in plan],
                         [("shutdown_work", "0", "Deactivate"), ("shutdown_work", "1", "Enable")])
        self.assertTrue(all(row["action"] == "valuecloud_legacy_ctrlDevice" for row in plan))
        self.assertTrue(all("register" not in row for row in plan))


class LearnedModbusWireReplayTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        set_external_profile_roots(())
        clear_profile_loader_cache()
        super().tearDown()

    async def test_smg_honors_existing_override_without_changing_legacy_default(self):
        profile = load_driver_profile("modbus_smg/models/smg_variant_4200.json")
        target = ProbeTarget(1, 255, 4)
        inverter = DetectedInverter(
            driver_key="modbus_smg", protocol_family="modbus_smg",
            model_name="Synthetic SMG variant", serial_number="",
            probe_target=target, capabilities=profile.capabilities,
        )
        driver = SmgModbusDriver()
        writes = []

        class RecordingTransport(FixtureTransport):
            async def async_send_payload(self, payload, *, route):
                write = decode_write_request(payload)
                if write is not None:
                    writes.append(write)
                return await super().async_send_payload(payload, route=route)

        transport = RecordingTransport(registers={322: 0, 303: 0},
                                       command_responses=None, probe_target=target)
        for key, declared, expected in (("battery_type", 6, 6), ("buzzer_mode", None, 16)):
            with self.subTest(key=key):
                capability = profile.get_capability(key)
                self.assertEqual(capability.write_function, declared)
                await driver.async_write_capability(transport, inverter, key, 0)
                self.assertEqual(writes[-1].function_code, expected)
                self.assertEqual(writes[-1].address, capability.register)
        self.assertEqual(len(writes), 2)

    async def test_generated_and_reloaded_controls_replay_fc06_and_fc16(self):
        for driver, profile_name, schema_name in (
            (MustPvPh18Driver(), "must_pv_ph18/pv3300.json", "must_pv_ph18/pv3300.json"),
            (SmgModbusDriver(), "smg_modbus.json", "modbus_smg/models/smg_6200.json"),
        ):
            for function in (6, 16):
                with self.subTest(driver=driver.key, function=function), tempfile.TemporaryDirectory() as temp:
                    config_dir = Path(temp)
                    result = generate_shadow_learning_overlay_drafts(
                        config_dir=config_dir, source_profile_name=profile_name,
                        source_schema_name=schema_name,
                        session_manifest={"session_id": "synthetic-write-contract", "collector_pn": "E5000020000000"},
                        correlation=_correlation(function),
                    )
                    set_external_profile_roots((local_profiles_root(config_dir),))
                    clear_profile_loader_cache()
                    local_name = result.profile_path.relative_to(local_profiles_root(config_dir)).as_posix()
                    for _ in range(2):
                        profile = load_driver_profile(local_name)
                        capability = next(c for c in profile.capabilities if c.register == 60000)
                        self.assertEqual(capability.write_function, function)
                        self.assertFalse(capability.tested)
                        for mode in ("auto", "full", "read_only"):
                            self.assertEqual(capability_write_exposure_allowed(
                                capability, control_mode=mode, detection_confidence="high",
                                device_scoped_overlay_active=True,
                                selected_control_keys=frozenset({capability.key}),
                            ), mode != "read_only")
                        self.assertFalse(capability_write_exposure_allowed(
                            capability, control_mode="auto", detection_confidence="high",
                            device_scoped_overlay_active=True, selected_control_keys=frozenset(),
                        ))
                        target = ProbeTarget(1, 255, 4)
                        inverter = DetectedInverter(
                            driver_key=driver.key, protocol_family=profile.protocol_family,
                            model_name="Synthetic learned control", serial_number="",
                            probe_target=target, capabilities=profile.capabilities,
                        )
                        writes = []

                        class RecordingTransport(FixtureTransport):
                            async def async_send_payload(self, payload, *, route):
                                write = decode_write_request(payload)
                                if write is not None:
                                    writes.append(write)
                                return await super().async_send_payload(payload, route=route)

                        transport = RecordingTransport(
                            registers={60000: 0}, command_responses=None, probe_target=target,
                        )
                        for label, raw in (("Enable", 1), ("Deactivate", 0)):
                            self.assertEqual(await driver.async_write_capability(
                                transport, inverter, capability.key, label,
                            ), label)
                            self.assertEqual(transport._registers[60000], raw)
                        self.assertEqual([(w.function_code, w.slave_id, w.address, tuple(w.values)) for w in writes],
                                         [(function, 4, 60000, (1,)), (function, 4, 60000, (0,))])
                        clear_profile_loader_cache()


if __name__ == "__main__":
    unittest.main()
