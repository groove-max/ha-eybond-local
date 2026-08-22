from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.read_learning_binder import (  # noqa: E402
    BIND_STATUS_AMBIGUOUS,
    BIND_STATUS_ENUM_LABEL,
    BIND_STATUS_NO_MATCH,
    BIND_STATUS_SKIPPED_ZERO,
    BIND_STATUS_UNIQUE,
    ENUM_STATUS_AMBIGUOUS,
    ENUM_STATUS_NO_TABLE_MATCH,
    ENUM_STATUS_UNIQUE,
    ObservedControlEnumEvidence,
    bind_cloud_labels_to_registers,
    match_enum_bindings,
    normalize_enum_label,
)
from custom_components.eybond_local.support.shadow_learning.read_evidence import (  # noqa: E402
    LearnedReadActivationContext,
    ShadowReadRegisterEvidence,
    ShadowReadRoute,
    read_register_evidence_from_map,
    validate_learned_read_activation,
)


_ROUTE = ShadowReadRoute(devcode=2376, collector_addr=1, device_addr=1)


def _register_evidence(
    registers: dict[int | str, int | list[int]],
    *,
    function: int = 3,
    route: ShadowReadRoute = _ROUTE,
) -> tuple[ShadowReadRegisterEvidence, ...]:
    return tuple(
        ShadowReadRegisterEvidence(
            route=route,
            function=function,
            register=int(register),
            samples=tuple(samples if isinstance(samples, list) else [samples]),
        )
        for register, samples in sorted(registers.items(), key=lambda item: int(item[0]))
    )


def _enum_authority(authority: dict[int, str]) -> dict[tuple[int, int], str]:
    return {(3, register): table for register, table in authority.items()}


def _corpus_registers() -> tuple[ShadowReadRegisterEvidence, ...]:
    """Registers shaped like the live SMG 6200 seed snapshot."""

    return _register_evidence({
        "205": [2305],   # inverter voltage 230.5
        "207": [5004],   # inverter frequency 50.04
        "210": [2297],   # output voltage 229.7
        "212": [5000],   # output frequency 50.00
        "215": [531],    # battery voltage 53.1
        "216": [65501],  # battery current -3.5 (s16 -35 / 10)
        "219": [197],    # pv voltage 19.7
        "225": [2],      # load percent 2
        "226": [27],     # dcdc temperature 27
        "227": [32],     # inverter temperature 32
        "320": [2300],   # output rating voltage 230.0
        "643": [6200],   # rated power
    })


def _sensor(cloud_id: str, par: str, val: str, unit: str = "") -> dict[str, str]:
    return {"id": cloud_id, "par": par, "val": val, "unit": unit}


class ReadLearningBinderTests(unittest.TestCase):
    def test_unique_pins_for_distinct_quantities(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[
                _sensor("bt_eybond_read_28", "Battery Voltage", "53.1", "V"),
                _sensor("sy_eybond_read_38", "DC Module Termperature", "27", "°C"),
                _sensor("pv_eybond_read_32", "PV Voltage", "19.7", "V"),
            ],
            register_evidence=_corpus_registers(),
        )

        by_title = {binding.title: binding for binding in report.bindings}
        battery = by_title["Battery Voltage"]
        self.assertEqual(battery.status, BIND_STATUS_UNIQUE)
        self.assertEqual(battery.register, 215)
        self.assertEqual(battery.candidates[0].divisor, 10)
        self.assertEqual(battery.decimals, 1)

        temperature = by_title["DC Module Termperature"]
        self.assertEqual(temperature.status, BIND_STATUS_UNIQUE)
        self.assertEqual(temperature.register, 226)
        self.assertEqual(temperature.candidates[0].divisor, 1)

        pv = by_title["PV Voltage"]
        self.assertEqual(pv.status, BIND_STATUS_UNIQUE)
        self.assertEqual(pv.register, 219)

    def test_negative_value_binds_via_signed_interpretation(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("bt_eybond_read_29", "Battery Current", "-3.5", "A")],
            register_evidence=_corpus_registers(),
        )

        binding = report.bindings[0]
        self.assertEqual(binding.status, BIND_STATUS_UNIQUE)
        self.assertEqual(binding.register, 216)
        self.assertTrue(binding.candidates[0].signed)
        self.assertEqual(binding.candidates[0].divisor, 10)

    def test_shared_value_is_ambiguous_with_candidate_list_not_a_guess(self) -> None:
        # Output frequency 50.00 reconstructs from BOTH 212 (5000) and 320?
        # No — from 212 and the rating register family; build an explicit clash.
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("bc_eybond_read_25", "Output frequency", "50.00", "Hz")],
            register_evidence=_register_evidence({"212": [5000], "321": [5000]}),
        )

        binding = report.bindings[0]
        self.assertEqual(binding.status, BIND_STATUS_AMBIGUOUS)
        self.assertIsNone(binding.register)
        self.assertEqual(
            sorted(candidate.register for candidate in binding.candidates),
            [212, 321],
        )

    def test_zero_values_are_skipped_not_bound(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("gd_eybond_read_15", "Grid Voltage", "0.0", "V")],
            register_evidence=_corpus_registers(),
        )

        self.assertEqual(report.bindings[0].status, BIND_STATUS_SKIPPED_ZERO)

    def test_enum_label_is_deferred_to_enum_learner(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("sy_eybond_read_14", "Operating mode", "Off-Grid Mode")],
            register_evidence=_corpus_registers(),
        )

        self.assertEqual(report.bindings[0].status, BIND_STATUS_ENUM_LABEL)

    def test_unreconstructable_value_is_no_match(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("x", "Phantom Power", "123.4", "W")],
            register_evidence=_register_evidence({"205": [2305]}),
        )

        self.assertEqual(report.bindings[0].status, BIND_STATUS_NO_MATCH)

    def test_non_finite_values_do_not_crash_the_run(self) -> None:
        # 'nan'/'inf' parse as floats but round() on them raises; a single such
        # cloud value must NOT take down the whole binding pass (the learning
        # run treats read-label binding as best-effort and supplemental).
        report = bind_cloud_labels_to_registers(
            sensors=[
                _sensor("a", "Broken NaN", "nan", "W"),
                _sensor("b", "Broken Inf", "inf", "V"),
                _sensor("c", "Broken NegInf", "-inf", "A"),
                _sensor("d", "Output rating voltage", "230.0", "V"),
            ],
            register_evidence=_corpus_registers(),
        )

        statuses = {b.title: b.status for b in report.bindings}
        self.assertEqual(statuses["Broken NaN"], "not_numeric")
        self.assertEqual(statuses["Broken Inf"], "not_numeric")
        self.assertEqual(statuses["Broken NegInf"], "not_numeric")
        # The valid sensor in the same batch still binds.
        self.assertEqual(statuses["Output rating voltage"], "unique")

    def test_report_serializes_with_counts(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[
                _sensor("bt_eybond_read_28", "Battery Voltage", "53.1", "V"),
                _sensor("gd_eybond_read_15", "Grid Voltage", "0.0", "V"),
            ],
            register_evidence=_corpus_registers(),
        )

        payload = report.to_json_dict()
        self.assertEqual(payload["sensor_count"], 2)
        self.assertEqual(payload["unique_count"], 1)
        self.assertEqual(payload["register_count"], len(_corpus_registers()))
        self.assertEqual(payload["bindings"][0]["candidates"][0]["register"], 215)

    def test_string_or_int_register_keys_both_accepted(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("bt", "Battery Voltage", "53.1", "V")],
            register_evidence=_register_evidence({215: [531]}),
        )

        self.assertEqual(report.bindings[0].register, 215)

    def test_same_register_in_two_functions_is_ambiguous_not_merged(self) -> None:
        evidence = _register_evidence({215: [531]}) + _register_evidence(
            {215: [531]}, function=4
        )

        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("bt", "Battery Voltage", "53.1", "V")],
            register_evidence=evidence,
        )

        binding = report.bindings[0]
        self.assertEqual(binding.status, BIND_STATUS_AMBIGUOUS)
        self.assertEqual(
            [(item.function, item.register) for item in binding.candidates],
            [(3, 215), (4, 215)],
        )

    def test_address_only_mapping_is_rejected_at_typed_boundary(self) -> None:
        with self.assertRaises(TypeError):
            bind_cloud_labels_to_registers(
                sensors=[_sensor("bt", "Battery Voltage", "53.1", "V")],
                register_evidence={"215": [531]},  # type: ignore[arg-type]
            )

    def test_read_evidence_and_activation_context_roundtrip_strictly(self) -> None:
        item = _register_evidence({215: [531]})[0]
        parsed = read_register_evidence_from_map(
            {"register_series": [item.to_record()]}
        )
        self.assertEqual(parsed, (item,))
        self.assertEqual(parsed[0].to_record(), item.to_record())

        context = LearnedReadActivationContext(
            collector_pn="E5000020000000",
            driver_key="modbus_smg",
            register_schema_name="modbus_smg/models/smg_6200.json",
            route=_ROUTE,
        )
        self.assertEqual(
            LearnedReadActivationContext.from_record(context.to_record()),
            context,
        )
        for poisoned in (
            {"register_series": [{**item.to_record(), "function": True}]},
            {"register_series": [{**item.to_record(), "device_addr": "1"}]},
            {"register_series": [{**item.to_record(), "samples": [531, 531]}]},
            {"register_series": [{**item.to_record(), "extra": 1}]},
        ):
            with self.subTest(poisoned=poisoned):
                self.assertEqual(read_register_evidence_from_map(poisoned), ())

    def test_activation_requires_selected_key_and_exact_current_context(self) -> None:
        context = LearnedReadActivationContext(
            collector_pn="E5000020000000",
            driver_key="modbus_smg",
            register_schema_name="modbus_smg/models/smg_6200.json",
            route=_ROUTE,
        )
        manifest = {
            "source_schema_name": context.register_schema_name,
            "output": {
                "profile_name": "learned/profile.json",
                "schema_name": "learned/schema.json",
            },
            "learned_read_sensors": [
                {
                    "key": "learned_read_fc3_215",
                    "function": 3,
                    "register": 215,
                    "spec_set": "live",
                }
            ],
            "learned_read_context": context.to_record(),
        }
        schema_record = {
            "driver_key": context.driver_key,
            "draft_of": f"builtin:{context.register_schema_name}",
            "shadow_learning_overlay": manifest,
            "spec_sets": {
                "live": [
                    {
                        "key": "learned_read_fc3_215",
                        "function": 3,
                        "register": 215,
                    }
                ]
            },
            "measurement_descriptions": [
                {"key": "learned_read_fc3_215", "learned": True}
            ],
            "learned_read_locations": [[3, 215]],
        }
        self.assertEqual(
            validate_learned_read_activation(
                manifest=manifest,
                register_schema_record=schema_record,
                profile_name="learned/profile.json",
                register_schema_name="learned/schema.json",
                selected_read_keys=("learned_read_fc3_215",),
                current_context=context,
            ),
            context,
        )

        foreign = LearnedReadActivationContext(
            collector_pn=context.collector_pn,
            driver_key=context.driver_key,
            register_schema_name=context.register_schema_name,
            route=ShadowReadRoute(devcode=2376, collector_addr=1, device_addr=2),
        )
        for kwargs in (
            {"selected_read_keys": ("learned_read_fc3_999",)},
            {"current_context": foreign},
            {"manifest": {**manifest, "learned_read_context": {}}},
            {"manifest": {**manifest, "output": {}}},
        ):
            with self.subTest(kwargs=kwargs):
                call = {
                    "manifest": manifest,
                    "register_schema_record": schema_record,
                    "profile_name": "learned/profile.json",
                    "register_schema_name": "learned/schema.json",
                    "selected_read_keys": ("learned_read_fc3_215",),
                    "current_context": context,
                }
                call.update(kwargs)
                with self.assertRaises((TypeError, ValueError)):
                    validate_learned_read_activation(**call)

        for poisoned_schema in (
            {**schema_record, "shadow_learning_overlay": {}},
            {**schema_record, "driver_key": "foreign_driver"},
            {
                **schema_record,
                "spec_sets": {
                    "live": [
                        {
                            "key": "learned_read_fc3_215",
                            "function": 4,
                            "register": 215,
                        }
                    ]
                },
            },
            {**schema_record, "measurement_descriptions": []},
            {**schema_record, "learned_read_locations": [[4, 215]]},
        ):
            with self.subTest(poisoned_schema=poisoned_schema):
                with self.assertRaises(ValueError):
                    validate_learned_read_activation(
                        manifest=manifest,
                        register_schema_record=poisoned_schema,
                        profile_name="learned/profile.json",
                        register_schema_name="learned/schema.json",
                        selected_read_keys=("learned_read_fc3_215",),
                        current_context=context,
                    )



class ReadEnumMatcherTests(unittest.TestCase):
    def _enum_label_report(
        self,
        title: str,
        value: str,
        *,
        cloud_id: str = "sy_eybond_read_14",
    ) -> dict:
        return {
            "bindings": [
                {
                    "cloud_id": cloud_id,
                    "title": title,
                    "cloud_value": value,
                    "status": BIND_STATUS_ENUM_LABEL,
                }
            ]
        }

    def test_inverts_known_table_to_unique_register(self) -> None:
        # SMG seed: register 201 holds 3, mode_names maps 3 -> "Off-Grid".
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Off-Grid Mode"),
            register_evidence=_register_evidence({"201": [3], "215": [531]}),
            enum_tables={"mode_names": {"0": "Power On", "2": "Line", "3": "Off-Grid"}},
        )

        binding = result["bindings"][0]
        self.assertEqual(binding["status"], ENUM_STATUS_UNIQUE)
        self.assertEqual(binding["candidates"][0]["register"], 201)
        self.assertEqual(binding["candidates"][0]["raw_value"], 3)
        self.assertEqual(binding["candidates"][0]["enum_table"], "mode_names")
        self.assertEqual(result["unique_count"], 1)

    def test_exact_label_match_beats_containment(self) -> None:
        # "Line" must not also hit "Line Saving" containment when an exact hit exists.
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Line"),
            register_evidence=_register_evidence({"201": [2], "300": [5]}),
            enum_tables={
                "mode_names": {"2": "Line"},
                "output_mode": {"5": "Line Saving"},
            },
        )

        binding = result["bindings"][0]
        self.assertEqual(binding["status"], ENUM_STATUS_UNIQUE)
        self.assertEqual(binding["candidates"][0]["register"], 201)
        self.assertTrue(
            all(candidate["match_kind"] == "exact" for candidate in binding["candidates"])
        )

    def test_value_in_many_registers_is_ambiguous(self) -> None:
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Off-Grid"),
            register_evidence=_register_evidence({"201": [3], "303": [3]}),
            enum_tables={"mode_names": {"3": "Off-Grid"}},
        )

        self.assertEqual(result["bindings"][0]["status"], ENUM_STATUS_AMBIGUOUS)

    def test_schema_register_table_authority_rejects_foreign_same_raw_register(self) -> None:
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Off-Grid Mode"),
            register_evidence=_register_evidence({"201": [3], "331": [3]}),
            enum_tables={
                "mode_names": {"3": "Off-Grid"},
                "charge_source_priority": {"3": "PV Only"},
            },
            register_enum_tables=_enum_authority(
                {201: "mode_names", 331: "charge_source_priority"}
            ),
        )

        binding = result["bindings"][0]
        self.assertEqual(binding["status"], ENUM_STATUS_UNIQUE)
        self.assertEqual(
            [candidate["register"] for candidate in binding["candidates"]], [201]
        )

    def test_generic_off_label_does_not_match_off_grid_mode(self) -> None:
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Off-Grid Mode"),
            register_evidence=_register_evidence({"201": [3], "300": [0]}),
            enum_tables={
                "mode_names": {"3": "Off-Grid"},
                "power_saving_mode": {"0": "Off", "1": "On"},
            },
            register_enum_tables=_enum_authority(
                {201: "mode_names", 300: "power_saving_mode"}
            ),
        )

        candidates = result["bindings"][0]["candidates"]
        self.assertEqual([candidate["register"] for candidate in candidates], [201])
        self.assertTrue(
            all(candidate["enum_table"] == "mode_names" for candidate in candidates)
        )

    def test_unknown_label_is_no_table_match(self) -> None:
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Quantum Mode"),
            register_evidence=_register_evidence({"201": [3]}),
            enum_tables={"mode_names": {"3": "Off-Grid"}},
        )

        self.assertEqual(result["bindings"][0]["status"], ENUM_STATUS_NO_TABLE_MATCH)

    def test_same_session_control_enum_bridges_provider_vocabulary(self) -> None:
        evidence = ObservedControlEnumEvidence(
            provider_id="smartess",
            session_id="shadow-session-1",
            semantic_key="charger_source_priority",
            cloud_field_id="bat_eybond_ctrl_75",
            enum_table="charge_source_priority",
            provider_field_ordinal=75,
            register=331,
            devcode=2376,
            collector_addr=1,
            device_addr=1,
            value_labels=(
                (0, "Utility charging is preferred"),
                (3, "Only PV charging is allowed"),
            ),
        )

        result = match_enum_bindings(
            read_bindings=self._enum_label_report(
                "Charger Source Priority",
                "Only PV charging is allowed",
                cloud_id="sy_eybond_read_75",
            ),
            register_evidence=_register_evidence({"331": [3]}),
            enum_tables={
                "charge_source_priority": {
                    "0": "Utility Priority",
                    "3": "PV Only",
                }
            },
            register_enum_tables=_enum_authority({331: "charge_source_priority"}),
            control_enum_evidence=(evidence,),
            session_id="shadow-session-1",
        )

        binding = result["bindings"][0]
        self.assertEqual(binding["status"], ENUM_STATUS_UNIQUE)
        self.assertEqual(binding["method"], "same_session_control_enum")
        self.assertEqual(binding["value_source"], "seed_bank_and_observed_control")
        self.assertEqual(binding["candidates"][0]["register"], 331)
        self.assertEqual(binding["candidates"][0]["raw_value"], 3)
        self.assertEqual(
            binding["candidates"][0]["cloud_control_field_id"],
            "bat_eybond_ctrl_75",
        )

    def test_control_enum_bridge_fails_closed_on_each_identity_axis(self) -> None:
        base = {
            "provider_id": "smartess",
            "session_id": "shadow-session-1",
            "semantic_key": "charger_source_priority",
            "cloud_field_id": "bat_eybond_ctrl_75",
            "enum_table": "charge_source_priority",
            "provider_field_ordinal": 75,
            "register": 331,
            "devcode": 2376,
            "collector_addr": 1,
            "device_addr": 1,
            "value_labels": ((0, "Utility"), (3, "Only PV charging is allowed")),
        }
        cases = (
            ({"session_id": "foreign-session"}, "shadow-session-1", "sy_eybond_read_75", "Charger Source Priority", {331: [3]}, {331: "charge_source_priority"}),
            ({"provider_field_ordinal": 76}, "shadow-session-1", "sy_eybond_read_75", "Charger Source Priority", {331: [3]}, {331: "charge_source_priority"}),
            ({"semantic_key": "output_priority"}, "shadow-session-1", "sy_eybond_read_75", "Charger Source Priority", {331: [3]}, {331: "charge_source_priority"}),
            ({}, "shadow-session-1", "sy_eybond_read_75", "Charger Source Priority", {331: [2]}, {331: "charge_source_priority"}),
            ({}, "shadow-session-1", "sy_eybond_read_75", "Charger Source Priority", {331: [3]}, {331: "mode_names"}),
            ({}, "shadow-session-1", "sy_eybond_read_74", "Charger Source Priority", {331: [3]}, {331: "charge_source_priority"}),
            ({}, "shadow-session-1", "sy_eybond_read_75", "Output priority", {331: [3]}, {331: "charge_source_priority"}),
        )
        for overrides, session_id, cloud_id, title, registers, authority in cases:
            with self.subTest(overrides=overrides, cloud_id=cloud_id, title=title):
                evidence = ObservedControlEnumEvidence(**{**base, **overrides})
                result = match_enum_bindings(
                    read_bindings=self._enum_label_report(
                        title,
                        "Only PV charging is allowed",
                        cloud_id=cloud_id,
                    ),
                    register_evidence=_register_evidence(registers),
                    enum_tables={
                        "charge_source_priority": {
                            "0": "Utility Priority",
                            "3": "PV Only",
                        },
                        "mode_names": {"3": "Off-Grid"},
                    },
                    register_enum_tables=_enum_authority(authority),
                    control_enum_evidence=(evidence,),
                    session_id=session_id,
                )
                self.assertEqual(
                    result["bindings"][0]["status"],
                    ENUM_STATUS_NO_TABLE_MATCH,
                )

    def test_control_enum_evidence_direct_constructor_is_strict(self) -> None:
        valid = {
            "provider_id": "smartess",
            "session_id": "shadow-session-1",
            "semantic_key": "charger_source_priority",
            "cloud_field_id": "bat_eybond_ctrl_75",
            "enum_table": "charge_source_priority",
            "provider_field_ordinal": 75,
            "register": 331,
            "devcode": 2376,
            "collector_addr": 1,
            "device_addr": 1,
            "value_labels": ((0, "Utility"), (3, "PV only")),
        }
        for field, invalid in (
            ("provider_id", " smartess"),
            ("session_id", ""),
            ("register", True),
            ("provider_field_ordinal", 0),
            ("value_labels", ((3, "PV only"),)),
            ("value_labels", ((3, "PV only"), (3, "Duplicate"))),
        ):
            with self.subTest(field=field, invalid=invalid):
                with self.assertRaises((TypeError, ValueError)):
                    ObservedControlEnumEvidence(**{**valid, field: invalid})

    def test_numeric_bindings_are_ignored_by_enum_matcher(self) -> None:
        result = match_enum_bindings(
            read_bindings={
                "bindings": [
                    {"title": "Battery Voltage", "status": BIND_STATUS_UNIQUE, "cloud_value": "53.1"}
                ]
            },
            register_evidence=_register_evidence({"215": [531]}),
            enum_tables={"mode_names": {"3": "Off-Grid"}},
        )

        self.assertEqual(result["bindings"], [])

    def test_label_normalization(self) -> None:
        self.assertEqual(normalize_enum_label("Off-Grid Mode"), "offgridmode")
        self.assertEqual(normalize_enum_label("  UTI "), "uti")


if __name__ == "__main__":
    unittest.main()
