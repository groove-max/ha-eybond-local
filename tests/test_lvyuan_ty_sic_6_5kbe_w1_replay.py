"""Replay checks for the LVYUAN TY-SIC-6.5KBE-W1 pack.

The fixtures are sanitized numeric words taken from a passive BLE capture of the
vendor app (2026-08-09) plus one live read of the same unit (2026-11-10). They
carry no serial, account, address or collection id.

They exist because the original fingerprint pinned a user-settable charge
threshold (word 16648 == 480 V) and the installer later changed it, which made a
live unit with a healthy transport fail detection. These tests replay the two
observed configurations and a synthetic 96 V negative case so the matcher cannot
silently regress to a settable value.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.canonical_telemetry import (  # noqa: E402
    _CANONICAL_TELEMETRY,
    _compute_variant,
    canonical_measurements_for_driver,
)
from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver  # noqa: E402
from custom_components.eybond_local.drivers.read_result import (  # noqa: E402
    DriverReadMode,
    DriverReadResult,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
AUGUST = json.loads(
    (FIXTURES / "lvyuan_ty_sic_6_5kbe_w1_august_registers.json").read_text(encoding="utf-8")
)
NOVEMBER = json.loads(
    (FIXTURES / "lvyuan_ty_sic_6_5kbe_w1_live_november_registers.json").read_text(encoding="utf-8")
)
NEGATIVE_96V = json.loads(
    (FIXTURES / "lvyuan_ty_sic_6_5kbe_w1_96v_negative_registers.json").read_text(encoding="utf-8")
)

IDENTITY_START = 16640
IDENTITY_COUNT = 16


def _target() -> ProbeTarget:
    return ProbeTarget(devcode=23711, collector_addr=1, device_addr=1)


def _registers(fixture: dict) -> dict[int, int]:
    """Expand a fixture into the flat register map the transport replays."""

    registers: dict[int, int] = {}
    for start, count, words in (
        (fixture["mains_start"], len(fixture["mains"]), fixture["mains"]),
        (fixture["output_start"], len(fixture["output"]), fixture["output"]),
        (fixture["battery_start"], len(fixture["battery"]), fixture["battery"]),
    ):
        for offset, word in enumerate(words):
            registers[start + offset] = word

    # The energy blocks are read as (start, count) spans that also cover the
    # zero words around each counter, so replay every word of the span.
    for start, count in ((445, 4), (453, 2), (545, 2), (551, 2)):
        for offset in range(count):
            registers[start + offset] = 0
    for key, word in (
        ("energy_daily_a", 448),
        ("energy_total_a", 454),
        ("energy_daily_b", 546),
        ("energy_total_b", 552),
    ):
        registers[word] = fixture[key]

    registers[fixture["input_power_start"]] = fixture["input_power"][0]

    # The rating block is what the probe action reads. Everything the vendor app
    # does not poll stays absent, so a matcher that needs it would see nothing.
    for offset in range(IDENTITY_COUNT):
        registers[IDENTITY_START + offset] = 0
    for raw, value in fixture["identity"].items():
        registers[int(raw)] = value
    return registers


def _transport(fixture: dict) -> FixtureTransport:
    return FixtureTransport(
        registers=_registers(fixture),
        input_registers=None,
        command_responses=None,
        probe_target=_target(),
    )


class LvyuanIdentityReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_august_capture_resolves_the_exact_model(self) -> None:
        driver = ModbusCatalogDriver()

        inverter = await driver.async_probe(_transport(AUGUST), _target())

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "lvyuan_ty_sic_6_5kbe_w1")
        self.assertEqual(
            inverter.register_schema_name, "lvyuan_ty_sic_6_5kbe_w1/base.json"
        )

    async def test_live_unit_with_edited_thresholds_still_resolves(self) -> None:
        """The regression that broke detection on the real inverter.

        Both fixtures describe the same unit. The user-settable charge
        thresholds differ, because the installer changed them between August
        and November. Detection must not depend on those words.
        """

        driver = ModbusCatalogDriver()

        inverter = await driver.async_probe(_transport(NOVEMBER), _target())

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "lvyuan_ty_sic_6_5kbe_w1")

        # The two fixtures are deliberately NOT identical. Pinning the whole
        # rating block equal would hide exactly the drift that broke the
        # previous fingerprint, so assert the stable word and the drift
        # separately.
        self.assertEqual(
            AUGUST["identity"]["16646"], 6, "power-class word must be the 6 kW class"
        )
        self.assertEqual(
            NOVEMBER["identity"]["16646"], 6, "power class must not drift"
        )
        moved = {
            word
            for word in set(AUGUST["identity"]) | set(NOVEMBER["identity"])
            if AUGUST["identity"].get(word) != NOVEMBER["identity"].get(word)
        }
        self.assertEqual(
            moved,
            {"16645", "16648", "16649", "16651"},
            "the threshold block is what moved; the matcher must survive it",
        )
        # 16650 stayed put in this pair of reads, which is precisely why it is
        # NOT evidence of a stable fingerprint: it is one of the settable words
        # and simply was not edited between these two observations.
        self.assertIn("16650", AUGUST["identity"])
        self.assertIn("16648", moved)

    async def test_detection_is_labelled_provisional_and_never_exact(self) -> None:
        """The pack must never be sold as an automatic exact-model match.

        No stable non-settable per-model discriminator was captured, so the
        binding is explicitly provisional: the resolution is reported as a
        family match at medium confidence, and the provisional flag rides
        along in the detection details.
        """

        driver = ModbusCatalogDriver()

        inverter = await driver.async_probe(_transport(AUGUST), _target())
        assert inverter is not None

        detection = inverter.details["catalog_detection"]
        self.assertEqual(detection["resolution"], "family")
        self.assertNotEqual(detection["resolution"], "exact")
        self.assertEqual(detection["confidence"], "medium")
        self.assertNotEqual(detection["confidence"], "high")
        self.assertIs(detection["provisional_identity"], True)

    async def test_96v_platform_is_refused_despite_the_same_power_class(self) -> None:
        """Word 16646 reads 6 on any 6 kW unit, so it cannot stand alone.

        The negative fixture keeps the power class AND keeps the settable
        charge words inside the 500-600 V window the old fingerprint pinned,
        but its measured battery platform is 96 V instead of 48 V. The
        matcher must not claim this model: the platform voltage is a
        measurement, not a user setting, so it cannot be edited away.
        """

        driver = ModbusCatalogDriver()

        inverter = await driver.async_probe(_transport(NEGATIVE_96V), _target())

        self.assertIsNone(inverter)


class LvyuanTelemetryReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_confirmed_telemetry_decodes_from_the_capture(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport(AUGUST)

        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        result = await driver.async_read_values(transport, inverter)
        self.assertIs(type(result), DriverReadResult)
        self.assertIs(result.mode, DriverReadMode.FULL)
        values = result.values

        # Mains and output are direct FC3 words divided by ten.
        self.assertEqual(values["mains_voltage"], 228.0)
        self.assertEqual(values["mains_current"], 8.57)
        self.assertEqual(values["mains_frequency"], 49.99)
        self.assertEqual(values["output_voltage"], 229.0)
        self.assertEqual(values["output_active_power"], 1821)
        self.assertEqual(values["output_apparent_power"], 1826)
        self.assertEqual(values["load_percent"], 28)

        # 33290 is exposed as input active power, not as an unattributed word.
        self.assertEqual(values["input_active_power"], 1863)

        # Counters decode as hundredths of a kWh.
        self.assertEqual(values["energy_counter_448"], 14.16)
        self.assertEqual(values["energy_counter_454"], 87.71)
        self.assertEqual(values["energy_counter_546"], 14.08)
        self.assertEqual(values["energy_counter_552"], 70.50)


class LvyuanCanonicalSurfaceTests(unittest.TestCase):
    """Register 33290 must stay a provisional measurement, not grid power."""

    def test_input_active_power_is_not_aliased_onto_grid_power(self) -> None:
        descriptions = canonical_measurements_for_driver(
            "modbus_catalog",
            variant_key="lvyuan_ty_sic_6_5kbe_w1",
        )
        keys = {description.key for description in descriptions}

        # The pack publishes 33290 as its own measurement...
        self.assertNotIn(
            "grid_power",
            keys,
            "33290 has no captured export cycle, so its sign convention is "
            "unobserved and it must not be published as canonical grid power",
        )

    def test_driver_wide_surface_does_not_need_the_alias_either(self) -> None:
        descriptions = canonical_measurements_for_driver("modbus_catalog")
        keys = {description.key for description in descriptions}

        # Solis still owns the canonical grid-power key on the driver-wide
        # surface; this assertion exists so a future LVYUAN re-introduction is
        # caught here rather than in a live statistics change.
        self.assertIn("grid_power", keys)

    def test_battery_power_is_charge_positive(self) -> None:
        """Register 129 is discharge-positive on the wire; canonical is
        charge-positive.

        Measured on the live unit on 2026-11-11:
          +29.8 A -> AC input 0 W while the load drew 1400 W (pack supplying)
           -6.4 A -> AC input 1839 W against a 1488 W load (pack absorbing;
                     the +351 W surplus matches |52.9 V * -6.4 A| = 339 W)

        A plain multiply therefore reported a charging pack as discharging.
        """
        variant = _lvyuan_battery_power_variant()
        self.assertIsNotNone(variant, "the pack must publish battery_power")

        # A raw +29.8 A reading means the pack is SUPPLYING, so the canonical
        # value must come out negative (charge > 0 is the canonical sign).
        discharging = _compute_variant(
            variant, {"battery_voltage": 52.5, "battery_current": 29.8}
        )
        self.assertIsNotNone(discharging)
        self.assertLess(
            discharging,
            0,
            "a discharge-positive wire reading must not surface as charging",
        )
        self.assertAlmostEqual(discharging, -1564.5, places=1)

        # A raw negative current is the pack ABSORBING: canonical -> positive.
        charging = _compute_variant(
            variant, {"battery_voltage": 52.9, "battery_current": -6.4}
        )
        self.assertIsNotNone(charging)
        self.assertGreater(
            charging,
            0,
            "an absorbing pack must surface as charging (positive)",
        )
        self.assertAlmostEqual(charging, 338.56, places=1)

        # Zero stays zero, and a missing input stays unavailable.
        self.assertEqual(
            _compute_variant(variant, {"battery_voltage": 53.3, "battery_current": 0.0}),
            0.0,
        )
        self.assertIsNone(
            _compute_variant(variant, {"battery_voltage": 53.3})
        )


def _lvyuan_battery_power_variant():
    """Return the canonical battery_power variant bound to the LVYUAN pack."""
    for spec in _CANONICAL_TELEMETRY:
        if spec.description.key != "battery_power":
            continue
        for variant in spec.variants:
            if "modbus_catalog" not in variant.driver_keys:
                continue
            if "lvyuan_ty_sic_6_5kbe_w1" in variant.variant_keys:
                return variant
    return None


if __name__ == "__main__":
    unittest.main()
