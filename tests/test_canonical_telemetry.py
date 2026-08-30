from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.canonical_telemetry import (
    apply_canonical_measurements,
    canonical_measurements_for_driver,
    project_canonical_telemetry,
)
from custom_components.eybond_local.telemetry import (
    TelemetryFreshness,
    TelemetryOrigin,
    TypedTelemetryFrame,
    fold_driver_telemetry,
)


class CanonicalTelemetryTests(unittest.TestCase):
    @staticmethod
    def _typed_frame(
        driver_key: str,
        values: dict[str, object],
    ) -> TypedTelemetryFrame:
        return fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key=driver_key,
            values=values,
            replace=True,
        )

    def test_pi30_canonical_measurements_expose_common_core_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("pi30")
        }

        self.assertIn("grid_voltage", keys)
        self.assertIn("grid_frequency", keys)
        self.assertIn("output_power", keys)
        self.assertIn("pv_voltage", keys)
        self.assertIn("pv_current", keys)
        self.assertIn("pv_power", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("pv_to_battery_power", keys)
        self.assertIn("pv_to_grid_power", keys)
        self.assertIn("battery_to_home_power", keys)
        self.assertIn("grid_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_pi18_canonical_measurements_expose_common_flow_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("pi18")
        }

        self.assertIn("output_power", keys)
        self.assertIn("pv_voltage", keys)
        self.assertIn("pv_power", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_eybond_g_ascii_canonical_measurements_expose_card_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("eybond_g_ascii")
        }

        self.assertIn("output_power", keys)
        self.assertIn("load_percent", keys)
        self.assertIn("battery_percent", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("pv_voltage", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("pv_to_battery_power", keys)
        self.assertIn("pv_to_grid_power", keys)
        self.assertIn("battery_to_home_power", keys)
        self.assertIn("grid_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_srne_canonical_measurements_expose_card_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("srne_modbus")
        }

        self.assertIn("pv_power", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("pv_to_battery_power", keys)
        self.assertIn("pv_to_grid_power", keys)
        self.assertIn("battery_to_home_power", keys)
        self.assertIn("grid_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_must_canonical_measurements_expose_card_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("must_pv_ph18")
        }

        self.assertIn("pv_power", keys)
        # battery_power is a native schema measurement (register 25273),
        # not a canonical alias, since the 25273/25274 fix.
        self.assertNotIn("battery_power", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_apply_canonical_measurements_builds_srne_card_values(self) -> None:
        values = {
            "grid_voltage": 229.0,
            "output_power": 900,
            "pv1_input_power": 620,
            "pv2_input_power": 480,
            "charge_power": 200.0,
        }

        apply_canonical_measurements("srne_modbus", values)

        self.assertEqual(values["pv_power"], 1100.0)
        self.assertEqual(values["battery_power"], 200.0)
        self.assertEqual(values["pv_to_home_power"], 900.0)
        self.assertEqual(values["pv_to_battery_power"], 200.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_sums_single_string_srne_pv(self) -> None:
        values = {
            "output_power": 400,
            "pv1_input_power": 350,
            "charge_power": 0.0,
        }

        apply_canonical_measurements("srne_modbus", values)

        self.assertEqual(values["pv_power"], 350)

    def test_apply_canonical_measurements_builds_must_card_values(self) -> None:
        # battery_power is a native schema key since the 25273/25274 fix
        # (25273 is the vendor's Batt power; 25274 is battery current that
        # the third-party map mislabeled "Battery_Load").
        values = {
            "grid_voltage": 231.0,
            "output_power": 1200,
            "grid_power": 700.0,
            "pv_charging_power": 500,
            "battery_power": -300.0,
        }

        apply_canonical_measurements("must_pv_ph18", values)

        self.assertEqual(values["pv_power"], 500)
        self.assertEqual(values["battery_power"], -300.0)
        self.assertEqual(values["pv_to_home_power"], 500.0)
        self.assertEqual(values["battery_to_home_power"], 300.0)
        self.assertEqual(values["grid_to_home_power"], 400.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_builds_modbus_catalog_card_values(self) -> None:
        values = {
            "grid_voltage": 230.0,
            "output_power": 900.0,
            "pv_input_voltage": 350.0,
            "pv_input_current": 4.0,
            "battery_voltage": 26.4,
            "battery_current": 10.0,
            "battery_percent": 78,
        }

        apply_canonical_measurements("modbus_catalog", values, variant_key="aohai_fsa")

        self.assertEqual(values["pv_power"], 1400.0)
        self.assertEqual(values["battery_power"], 264.0)
        self.assertEqual(values["pv_to_home_power"], 900.0)
        self.assertEqual(values["pv_to_battery_power"], 264.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)

    def test_canonical_entity_surface_respects_variant_gate(self) -> None:
        aohai = {
            d.key
            for d in canonical_measurements_for_driver(
                "modbus_catalog", variant_key="aohai_fsa"
            )
        }
        other = {
            d.key
            for d in canonical_measurements_for_driver(
                "modbus_catalog", variant_key="other_pack"
            )
        }
        legacy = {
            d.key for d in canonical_measurements_for_driver("modbus_catalog")
        }

        self.assertIn("pv_power", aohai)
        self.assertIn("battery_power", aohai)
        # A different pack must not grow never-populated canonical entities.
        self.assertNotIn("pv_power", other)
        self.assertNotIn("battery_power", other)
        # Ungated flow descriptions stay available to every pack.
        self.assertIn("pv_to_home_power", other)
        # No variant context keeps the legacy driver-wide surface.
        self.assertIn("pv_power", legacy)

    def test_modbus_catalog_variants_do_not_leak_to_other_packs(self) -> None:
        # A future pack served by the same generic driver may reuse key names
        # with different semantics; Aohai's V*I computes must not apply.
        values = {
            "output_power": 900.0,
            "pv_input_voltage": 350.0,
            "pv_input_current": 4.0,
            "battery_voltage": 26.4,
            "battery_current": 10.0,
        }

        apply_canonical_measurements("modbus_catalog", values, variant_key="other_pack")

        self.assertNotIn("pv_power", values)
        self.assertNotIn("battery_power", values)

    def test_apply_canonical_measurements_builds_pi30_common_values(self) -> None:
        values = {
            "input_voltage": 230.0,
            "input_frequency": 50.0,
            "output_active_power": 1400,
            "pv_input_voltage": 118.0,
            "pv_input_current": 8.5,
            "pv_input_power": 1003,
            "battery_voltage": 51.2,
            "battery_charge_current": 12.0,
            "battery_discharge_current": 0.0,
        }

        apply_canonical_measurements("pi30", values)

        self.assertEqual(values["grid_voltage"], 230.0)
        self.assertEqual(values["grid_frequency"], 50.0)
        self.assertEqual(values["output_power"], 1400)
        self.assertEqual(values["pv_voltage"], 118.0)
        self.assertEqual(values["pv_current"], 8.5)
        self.assertEqual(values["pv_power"], 1003)
        self.assertEqual(values["battery_power"], 614.4)
        self.assertEqual(values["pv_to_home_power"], 1003.0)
        self.assertEqual(values["pv_to_battery_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_home_power"], 397.0)
        self.assertEqual(values["grid_to_battery_power"], 614.4)

    def test_typed_projection_uses_the_same_pi30_values_with_provenance(self) -> None:
        raw = {
            "input_voltage": 230.0,
            "input_frequency": 50.0,
            "output_active_power": 1400,
            "pv_input_voltage": 118.0,
            "pv_input_current": 8.5,
            "pv_input_power": 1003,
            "battery_voltage": 51.2,
            "battery_charge_current": 12.0,
            "battery_discharge_current": 0.0,
        }
        legacy = dict(raw)
        apply_canonical_measurements("pi30", legacy)

        projected = project_canonical_telemetry(self._typed_frame("pi30", raw))

        self.assertEqual(projected.value("battery_power"), legacy["battery_power"])
        self.assertEqual(
            projected.value("grid_to_battery_power"),
            legacy["grid_to_battery_power"],
        )
        battery = projected.point("battery_power")
        self.assertIs(battery.origin, TelemetryOrigin.CANONICAL)
        self.assertEqual(
            battery.source_keys,
            (
                "battery_voltage",
                "battery_charge_current",
                "battery_discharge_current",
            ),
        )
        flow = projected.point("grid_to_battery_power")
        self.assertIs(flow.origin, TelemetryOrigin.CANONICAL)
        self.assertIn("battery_power", flow.source_keys)
        self.assertIs(flow.freshness, TelemetryFreshness.FRESH)

    def test_typed_flow_provenance_tracks_optional_route_evidence(self) -> None:
        projected = project_canonical_telemetry(
            self._typed_frame(
                "modbus_smg",
                {
                    "operating_mode": "Mains",
                    "output_power": 1601.0,
                    "pv_power": 1711.0,
                    "battery_power": 1730.0,
                    "grid_power": 204.0,
                    "grid_voltage": 242.3,
                    "pv_charging_power": 1730.0,
                    "power_flow_charge_source_state": "PV",
                },
            )
        )

        flow = projected.point("pv_to_home_power")
        self.assertEqual(flow.value, 0.0)
        self.assertTrue(
            {
                "operating_mode",
                "output_power",
                "pv_power",
                "battery_power",
                "grid_power",
                "grid_voltage",
                "pv_charging_power",
                "power_flow_charge_source_state",
            }.issubset(flow.source_keys)
        )
        self.assertIs(flow.freshness, TelemetryFreshness.FRESH)

    def test_typed_projection_propagates_carried_freshness_through_chain(self) -> None:
        first = self._typed_frame(
            "pi30",
            {
                "output_active_power": 1400,
                "pv_input_power": 1003,
                "battery_voltage": 51.2,
                "battery_charge_current": 12.0,
                "battery_discharge_current": 0.0,
            },
        )
        delta = fold_driver_telemetry(
            first,
            driver_key="pi30",
            values={"output_active_power": 1410},
            replace=False,
        )

        projected = project_canonical_telemetry(delta)

        self.assertIs(
            projected.point("output_power").freshness,
            TelemetryFreshness.FRESH,
        )
        self.assertIs(
            projected.point("battery_power").freshness,
            TelemetryFreshness.CARRIED,
        )
        self.assertIs(
            projected.point("grid_to_battery_power").freshness,
            TelemetryFreshness.CARRIED,
        )

    def test_native_canonical_key_remains_driver_owned(self) -> None:
        frame = self._typed_frame(
            "modbus_smg",
            {
                "output_power": 1750,
                "pv_power": 920,
                "battery_average_power": -315.0,
            },
        )

        projected = project_canonical_telemetry(frame)

        native = projected.point("pv_power")
        self.assertIs(native.origin, TelemetryOrigin.DRIVER)
        self.assertEqual(native.source_keys, ())
        derived = projected.point("battery_power")
        self.assertIs(derived.origin, TelemetryOrigin.CANONICAL)
        self.assertEqual(derived.source_keys, ("battery_average_power",))

    def test_typed_projection_respects_variant_gate_and_strict_context(self) -> None:
        frame = self._typed_frame(
            "modbus_catalog",
            {
                "pv_input_voltage": 350.0,
                "pv_input_current": 4.0,
            },
        )

        aohai = project_canonical_telemetry(frame, variant_key="aohai_fsa")
        other = project_canonical_telemetry(frame, variant_key="other_pack")

        self.assertEqual(aohai.value("pv_power"), 1400.0)
        self.assertIsNone(other.point("pv_power"))
        for variant in ("", " padded", 7, object()):
            with self.subTest(variant=variant):
                with self.assertRaises((TypeError, ValueError)):
                    project_canonical_telemetry(
                        frame,
                        variant_key=variant,  # type: ignore[arg-type]
                    )

    def test_apply_canonical_measurements_preserves_native_smg_values(self) -> None:
        values = {
            "grid_voltage": 228.0,
            "output_power": 1750,
            "pv_power": 920,
            "battery_average_power": -315.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["grid_voltage"], 228.0)
        self.assertEqual(values["output_power"], 1750)
        self.assertEqual(values["pv_power"], 920)
        self.assertEqual(values["battery_power"], -315.0)

    def test_apply_canonical_measurements_builds_flow_split_from_direct_pv_charge(self) -> None:
        # The charger's own measurement proves the route but is not a second
        # PV source. Every derived route remains bounded by the measured PV,
        # grid, battery, and load budgets even when the registers disagree.
        values = {
            "output_power": 109.0,
            "pv_power": 492.0,
            "battery_average_power": 629.0,
            "grid_power": 203.0,
            "pv_charging_power": 666.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["battery_power"], 629.0)
        self.assertEqual(values["pv_to_home_power"], 0.0)
        self.assertEqual(values["pv_to_battery_power"], 492.0)
        self.assertEqual(values["pv_to_grid_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 94.0)
        self.assertEqual(values["grid_to_home_power"], 109.0)

    def test_pv_only_charging_is_not_reattributed_to_grid_when_pv_power_under_reads(
        self,
    ) -> None:
        # Field regression (SMG 6200, charge source "PV Only", 2026-07-04):
        # pv_power read 1718 W while the PV charger logged 1786 W into the
        # battery and the load drew 781 W.  The derived-headroom clamp sent
        # the 850 W shortfall to grid_to_battery, accumulating 1.17 kWh of
        # fake daily grid import; the grid meter recorded ~0.23 kWh.
        values = {
            "output_power": 781.0,
            "pv_power": 1718.0,
            "battery_average_power": 1787.0,
            "grid_power": 30.0,
            "pv_charging_power": 1786.0,
            "grid_voltage": 230.0,
            "charge_source_priority": "PV Only",
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["pv_to_home_power"], 0.0)
        self.assertEqual(values["pv_to_battery_power"], 1718.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)
        self.assertEqual(values["grid_to_home_power"], 30.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)

    def test_field_regression_pv_routes_never_double_count_total_pv_power(self) -> None:
        # Issue #13, Sandisolar Protocol 4: the old split published 1601 W
        # PV->home plus 1730 W PV->battery from only 1711 W total PV.
        values = {
            "operating_mode": "Mains",
            "output_power": 1601.0,
            "pv_power": 1711.0,
            "battery_average_power": 1730.0,
            "grid_power": 204.0,
            "grid_voltage": 242.3,
            "pv_charging_power": 1730.0,
            "power_flow_charge_source_state": "PV",
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["pv_to_home_power"], 0.0)
        self.assertEqual(values["pv_to_battery_power"], 1711.0)
        self.assertEqual(values["pv_to_grid_power"], 0.0)
        self.assertEqual(values["grid_to_home_power"], 204.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)
        self.assertLessEqual(
            values["pv_to_home_power"]
            + values["pv_to_battery_power"]
            + values["pv_to_grid_power"],
            values["pv_power"],
        )
        self.assertLessEqual(
            values["grid_to_home_power"] + values["grid_to_battery_power"],
            values["grid_power"],
        )
        self.assertLessEqual(
            values["pv_to_home_power"]
            + values["battery_to_home_power"]
            + values["grid_to_home_power"],
            values["output_power"],
        )
        self.assertLessEqual(
            values["pv_to_battery_power"] + values["grid_to_battery_power"],
            values["battery_average_power"],
        )

    def test_apply_canonical_measurements_routes_residual_grid_import_to_home(self) -> None:
        values = {
            "output_power": 85.0,
            "pv_power": 0.0,
            "battery_average_power": 36.0,
            "grid_power": 142.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["pv_to_home_power"], 0.0)
        self.assertEqual(values["pv_to_battery_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 36.0)
        self.assertEqual(values["grid_to_home_power"], 85.0)

    def test_apply_canonical_measurements_builds_pi18_flow_values(self) -> None:
        values = {
            "output_active_power": 950,
            "pv_input_voltage": 120.0,
            "pv_input_power": 600,
            "battery_voltage": 51.2,
            "battery_charge_current": 0.0,
            "battery_discharge_current": 4.0,
        }

        apply_canonical_measurements("pi18", values)

        self.assertEqual(values["output_power"], 950)
        self.assertEqual(values["pv_power"], 600)
        self.assertEqual(values["battery_power"], -204.8)
        self.assertEqual(values["pv_to_home_power"], 600.0)
        self.assertEqual(values["battery_to_home_power"], 204.8)
        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_builds_eybond_g_ascii_card_values(self) -> None:
        values = {
            "eybond_g_ascii_operating_mode_code": "B",
            "grid_voltage": 0.0,
            "output_active_power": 904.0,
            "output_load_percentage": 25.0,
            "battery_voltage": 27.5,
            "battery_current": 15.8,
            "battery_capacity": 100.0,
            "pv_input_voltage": 183.1,
            "pv_power": 972.0,
        }

        apply_canonical_measurements("eybond_g_ascii", values)

        self.assertEqual(values["output_power"], 904.0)
        self.assertEqual(values["load_percent"], 25.0)
        self.assertEqual(values["battery_percent"], 100.0)
        self.assertEqual(values["pv_voltage"], 183.1)
        self.assertEqual(values["battery_power"], 434.5)
        self.assertEqual(values["pv_to_home_power"], 904.0)
        self.assertEqual(values["pv_to_battery_power"], 68.0)
        self.assertEqual(values["pv_to_grid_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_uses_eybond_g_ascii_pv_charge_current_fallback(self) -> None:
        values = {
            "eybond_g_ascii_operating_mode_code": "B",
            "output_active_power": 85.0,
            "battery_voltage": 27.0,
            "battery_current": 0.0,
            "pv_charging_current": 14.71,
            "pv_power": 420.0,
        }

        apply_canonical_measurements("eybond_g_ascii", values)

        self.assertEqual(values["battery_power"], 397.17)
        self.assertEqual(values["pv_to_battery_power"], 335.0)

    def test_apply_canonical_measurements_signs_eybond_g_ascii_discharge_power(self) -> None:
        values = {
            "eybond_g_ascii_operating_mode_code": "0",
            "output_active_power": 700.0,
            "battery_voltage": 25.6,
            "battery_current": 4.0,
            "pv_power": 0.0,
        }

        apply_canonical_measurements("eybond_g_ascii", values)

        self.assertEqual(values["battery_power"], -102.4)
        self.assertEqual(values["battery_to_home_power"], 102.4)

    def test_apply_canonical_measurements_keeps_eybond_g_ascii_line_flow_available(self) -> None:
        for mode_code in ("L", "4", "LINE", "UTILITY", "MAINS"):
            with self.subTest(mode_code=mode_code):
                values = {
                    "eybond_g_ascii_operating_mode_code": mode_code,
                    "grid_voltage": 228.9,
                    "output_active_power": 0.0,
                    "battery_voltage": 26.4,
                    "battery_current": 0.6,
                    "pv_power": 0.0,
                }

                apply_canonical_measurements("eybond_g_ascii", values)

                self.assertEqual(values["battery_power"], 15.84)
                self.assertEqual(values["pv_to_home_power"], 0.0)
                self.assertEqual(values["pv_to_battery_power"], 0.0)
                self.assertEqual(values["battery_to_home_power"], 0.0)
                self.assertEqual(values["grid_to_home_power"], 0.0)
                self.assertEqual(values["grid_to_battery_power"], 15.84)

    def test_apply_canonical_measurements_does_not_guess_eybond_g_ascii_battery_sign(self) -> None:
        values = {
            "eybond_g_ascii_operating_mode_code": "X",
            "output_active_power": 700.0,
            "battery_voltage": 25.6,
            "battery_current": 4.0,
            "pv_power": 0.0,
        }

        apply_canonical_measurements("eybond_g_ascii", values)

        self.assertNotIn("battery_power", values)
        self.assertNotIn("battery_to_home_power", values)

    def test_apply_canonical_measurements_does_not_infer_grid_flow_when_grid_voltage_is_zero(self) -> None:
        values = {
            "grid_voltage": 0.0,
            "output_active_power": 950,
            "pv_input_voltage": 120.0,
            "pv_input_power": 600,
            "battery_voltage": 51.2,
            "battery_charge_current": 0.0,
            "battery_discharge_current": 4.0,
        }

        apply_canonical_measurements("pi18", values)

        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_ignores_measured_grid_noise_when_grid_is_absent(self) -> None:
        values = {
            "grid_voltage": 0.0,
            "output_power": 85.0,
            "pv_power": 0.0,
            "battery_average_power": 13.0,
            "grid_power": 13.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)


if __name__ == "__main__":
    unittest.main()
