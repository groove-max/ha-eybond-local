"""Labelled AC-load estimate: load% × rated VA, never measured active power."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_optional import _rb
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_estimates import estimated_ac_load_values
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.short_ascii import parse_q1


class EstimatedAcLoadUnitTests(unittest.TestCase):
    def test_estimate_is_percent_times_rated_va(self):
        self.assertEqual(
            estimated_ac_load_values({
                "load_percent": 13,
                "short_ascii_rated_voltage": 115,
                "short_ascii_rated_current": 105,
            }),
            {"estimated_ac_load_power": 1569.8},
        )

    def test_estimate_omitted_without_ratings_or_load_percent(self):
        self.assertEqual(estimated_ac_load_values({"load_percent": 13}), {})
        self.assertEqual(
            estimated_ac_load_values({
                "short_ascii_rated_voltage": 115, "short_ascii_rated_current": 105,
            }),
            {},
        )
        self.assertEqual(estimated_ac_load_values({}), {})

    def test_zero_load_percent_is_zero_estimate_not_missing(self):
        self.assertEqual(
            estimated_ac_load_values({
                "load_percent": 0,
                "short_ascii_rated_voltage": 115,
                "short_ascii_rated_current": 105,
            }),
            {"estimated_ac_load_power": 0.0},
        )


class EstimatedAcLoadDriverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {"RB": _rb(), "F": b"#115.0 105 48.00 60.0\r"}
        self.transport = _Transport(responses)
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(767, 255, 1))
        self.state = {}
        self.transport.requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def test_estimate_labelled_load_percent_kept_no_grid_freq_or_measured_load_power(self):
        first = await self.read(0)
        self.assertEqual(first.values["load_percent"], 13)
        self.assertNotIn("estimated_ac_load_power", first.values)
        self.assertNotIn("grid_frequency", first.values)
        self.assertNotIn("load_power", first.values)

        second = await self.read(1)
        self.assertEqual(second.values["load_percent"], 13)
        self.assertEqual(second.values["estimated_ac_load_power"], 1569.8)
        self.assertEqual(second.values["output_frequency"], 60)
        self.assertNotIn("grid_frequency", second.values)
        for key in ("load_power", "load_power_from_percent", "load_power_source",
                    "output_power", "output_active_power", "rated_apparent_power"):
            self.assertNotIn(key, second.values)

        schema = load_register_schema(self.driver.register_schema_name)
        description = schema.measurement_description("estimated_ac_load_power")
        self.assertEqual(description.name, "Estimated AC Load Power")
        self.assertTrue(description.enabled_default)
        self.assertEqual(description.unit, "W")
        self.assertEqual(description.device_class, "power")
        keys = {item.key for item in schema.measurement_descriptions}
        self.assertNotIn("grid_frequency", keys)
        self.assertNotIn("load_power", keys)
        self.assertIn("load_percent", keys)
        self.assertIn("output_frequency", keys)

    async def test_q1_parser_still_does_not_alias_or_invent_power(self):
        values = parse_q1(_responses()["Q1"])
        self.assertEqual(values["load_percent"], 13)
        self.assertEqual(values["output_frequency"], 60)
        for key in ("grid_frequency", "estimated_ac_load_power", "load_power"):
            self.assertNotIn(key, values)

    async def test_estimate_withdraws_when_f_ratings_expire(self):
        await self.read(0)
        await self.read(1)
        self.assertEqual((await self.read(2)).values["estimated_ac_load_power"], 1569.8)
        expired = await self.read(902)
        self.assertNotIn("short_ascii_rated_voltage", expired.values)
        self.assertNotIn("estimated_ac_load_power", expired.values)
        self.assertEqual(expired.values["load_percent"], 13)


if __name__ == "__main__":
    unittest.main()
