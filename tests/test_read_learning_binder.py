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
    bind_cloud_labels_to_registers,
    match_enum_bindings,
    normalize_enum_label,
)


def _corpus_registers() -> dict[str, list[int]]:
    """Registers shaped like the live SMG 6200 seed snapshot."""

    return {
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
    }


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
            registers=_corpus_registers(),
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
            registers=_corpus_registers(),
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
            registers={"212": [5000], "321": [5000]},
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
            registers=_corpus_registers(),
        )

        self.assertEqual(report.bindings[0].status, BIND_STATUS_SKIPPED_ZERO)

    def test_enum_label_is_deferred_to_enum_learner(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("sy_eybond_read_14", "Operating mode", "Off-Grid Mode")],
            registers=_corpus_registers(),
        )

        self.assertEqual(report.bindings[0].status, BIND_STATUS_ENUM_LABEL)

    def test_unreconstructable_value_is_no_match(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("x", "Phantom Power", "123.4", "W")],
            registers={"205": [2305]},
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
            registers=_corpus_registers(),
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
            registers=_corpus_registers(),
        )

        payload = report.to_json_dict()
        self.assertEqual(payload["sensor_count"], 2)
        self.assertEqual(payload["unique_count"], 1)
        self.assertEqual(payload["register_count"], len(_corpus_registers()))
        self.assertEqual(payload["bindings"][0]["candidates"][0]["register"], 215)

    def test_string_or_int_register_keys_both_accepted(self) -> None:
        report = bind_cloud_labels_to_registers(
            sensors=[_sensor("bt", "Battery Voltage", "53.1", "V")],
            registers={215: [531]},
        )

        self.assertEqual(report.bindings[0].register, 215)



class ReadEnumMatcherTests(unittest.TestCase):
    def _enum_label_report(self, title: str, value: str) -> dict:
        return {
            "bindings": [
                {
                    "cloud_id": "sy_eybond_read_14",
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
            registers={"201": [3], "215": [531]},
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
            registers={"201": [2], "300": [5]},
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
            registers={"201": [3], "303": [3]},
            enum_tables={"mode_names": {"3": "Off-Grid"}},
        )

        self.assertEqual(result["bindings"][0]["status"], ENUM_STATUS_AMBIGUOUS)

    def test_unknown_label_is_no_table_match(self) -> None:
        result = match_enum_bindings(
            read_bindings=self._enum_label_report("Operating mode", "Quantum Mode"),
            registers={"201": [3]},
            enum_tables={"mode_names": {"3": "Off-Grid"}},
        )

        self.assertEqual(result["bindings"][0]["status"], ENUM_STATUS_NO_TABLE_MATCH)

    def test_numeric_bindings_are_ignored_by_enum_matcher(self) -> None:
        result = match_enum_bindings(
            read_bindings={
                "bindings": [
                    {"title": "Battery Voltage", "status": BIND_STATUS_UNIQUE, "cloud_value": "53.1"}
                ]
            },
            registers={"215": [531]},
            enum_tables={"mode_names": {"3": "Off-Grid"}},
        )

        self.assertEqual(result["bindings"], [])

    def test_label_normalization(self) -> None:
        self.assertEqual(normalize_enum_label("Off-Grid Mode"), "offgridmode")
        self.assertEqual(normalize_enum_label("  UTI "), "uti")


if __name__ == "__main__":
    unittest.main()
