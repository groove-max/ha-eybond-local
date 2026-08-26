from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import (
    DetectedInverter,
    DriverMatch,
    ProbeTarget,
)
from custom_components.eybond_local.runtime.driver_detection import (
    DetectedDriverContext,
    resolve_catalog_driver_candidate_overlap,
)


_TARGET = ProbeTarget(devcode=1, collector_addr=0xFF, device_addr=1)


def _context(
    driver_key: str,
    *,
    target: ProbeTarget = _TARGET,
    variant_key: str = "default",
    profile_name: str = "",
    register_schema_name: str = "",
    details: dict[str, object] | None = None,
) -> DetectedDriverContext:
    inverter = DetectedInverter(
        driver_key=driver_key,
        protocol_family=driver_key,
        model_name=driver_key,
        serial_number="",
        probe_target=target,
        variant_key=variant_key,
        profile_name=profile_name,
        register_schema_name=register_schema_name,
        details={} if details is None else details,
    )
    return DetectedDriverContext(
        driver=SimpleNamespace(key=driver_key),
        inverter=inverter,
        match=DriverMatch(
            driver_key=driver_key,
            protocol_family=driver_key,
            model_name=driver_key,
            serial_number="",
            probe_target=target,
            variant_key=variant_key,
        ),
    )


def _kevolt_context(*, resolution: str = "exact") -> DetectedDriverContext:
    return _context(
        "modbus_catalog",
        variant_key="deye_3ph_high_80kw",
        profile_name="modbus_catalog/deye_3ph_high_80kw.json",
        register_schema_name="deye_3ph_high_80kw/base.json",
        details={
            "catalog_detection": {
                "resolution": resolution,
                "surface_key": "deye_3ph_high_80kw_untested",
                "confidence": "high",
            }
        },
    )


class DriverCandidateSelectionTests(unittest.TestCase):
    """Catalog precedence resolves only a proven same-route overlap."""

    def test_exact_kevolt_fingerprint_supersedes_smg_in_any_order(self) -> None:
        kevolt = _kevolt_context()
        smg = _context("modbus_smg")

        for candidates in ((smg, kevolt), (kevolt, smg)):
            with self.subTest(order=[item.match.driver_key for item in candidates]):
                selection = resolve_catalog_driver_candidate_overlap(candidates)
                self.assertIsNotNone(selection)
                assert selection is not None
                self.assertIs(selection.context, kevolt)
                self.assertEqual(selection.catalog_entry_key, "deye_3ph_high_80kw")
                self.assertEqual(selection.superseded_protocols, ("modbus_smg",))

    def test_non_exact_fingerprint_remains_ambiguous(self) -> None:
        self.assertIsNone(
            resolve_catalog_driver_candidate_overlap(
                (_kevolt_context(resolution="family"), _context("modbus_smg"))
            )
        )

    def test_different_physical_target_remains_ambiguous(self) -> None:
        other_target = ProbeTarget(devcode=1, collector_addr=0xFE, device_addr=1)
        self.assertIsNone(
            resolve_catalog_driver_candidate_overlap(
                (
                    _kevolt_context(),
                    _context("modbus_smg", target=other_target),
                )
            )
        )

    def test_unlisted_competitor_remains_ambiguous(self) -> None:
        self.assertIsNone(
            resolve_catalog_driver_candidate_overlap(
                (_kevolt_context(), _context("srne_modbus"))
            )
        )

    def test_malformed_candidate_identity_fails_closed(self) -> None:
        malformed = _context("modbus_smg")
        malformed.match.driver_key = "modbus_catalog"
        self.assertIsNone(
            resolve_catalog_driver_candidate_overlap(
                (_kevolt_context(), malformed)
            )
        )


if __name__ == "__main__":
    unittest.main()
