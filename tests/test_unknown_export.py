"""An unmeasured export is not zero and not the unallocated PV balance."""

import unittest

from custom_components.eybond_local.canonical_telemetry import (
    apply_canonical_measurements, project_canonical_telemetry,
)
from custom_components.eybond_local.derived_energy import (
    compute_derived_power, derived_energy_descriptions_for_keys,
)
from custom_components.eybond_local.payload.pi30 import parse_qpigs
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry


class UnknownExportTests(unittest.TestCase):
    def test_qpigs21_does_not_measure_export(self):
        values = parse_qpigs(
            "231.3 49.9 231.3 49.9 0346 0218 005 426 53.70 000 100 "
            "0040 02.9 405.8 00.00 00000 10010111 00 00 01203 110"
        )
        values["solar_feed_to_grid_enabled"] = False
        apply_canonical_measurements("pi30", values)
        self.assertNotIn("pv_to_grid_power", values)
        self.assertAlmostEqual(values["pv_power"], 1176.8)
        self.assertEqual(values["pv_to_home_power"], 218)
        description = next(d for d in derived_energy_descriptions_for_keys({"pv_to_grid_power"})
                           if d.key == "estimated_grid_export_energy")
        self.assertIsNone(compute_derived_power(values, description))

    def test_only_measured_or_disconnected_grid_establishes_export(self):
        base = {"output_power": 200, "pv_power": 1200, "battery_power": 0, "grid_voltage": 230}
        for extra, expected in (
            ({}, None), ({"solar_feed_to_grid_enabled": True}, None),
            ({"pv_power": 200}, None), ({"grid_voltage": 0}, 0),
            ({"grid_power": 50}, 0), ({"grid_power": -500}, 500),
            ({"solar_feed_to_grid_power": 0}, 0),
            ({"solar_feed_to_grid_power": 700}, 700),
        ):
            with self.subTest(extra=extra):
                values = base | extra
                apply_canonical_measurements("pi30", values)
                self.assertEqual(values.get("pv_to_grid_power"), expected)
                self.assertIn("pv_to_home_power", values)

    def test_expired_measurement_cannot_survive_in_projection(self):
        frame = TypedTelemetryFrame.empty()
        for extra, removed, expected in (
            ({"solar_feed_to_grid_power": 300}, (), 300),
            ({}, ("solar_feed_to_grid_power",), None),
            ({"solar_feed_to_grid_power": 400}, (), 400),
        ):
            frame = fold_driver_telemetry(
                frame, driver_key="pi30", replace=False, removed_keys=frozenset(removed),
                values={"output_power": 200, "pv_power": 1200, "battery_power": 0,
                        "grid_voltage": 230} | extra,
            )
            point = project_canonical_telemetry(frame).point("pv_to_grid_power")
            self.assertEqual(point.value if point else None, expected)
