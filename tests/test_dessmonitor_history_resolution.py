from __future__ import annotations

import ast
import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorDeviceIdentity,
)
from custom_components.eybond_local.dessmonitor_history import (  # noqa: E402
    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    DessMonitorHistoryPoint,
    DessMonitorHistorySeries,
)
from custom_components.eybond_local.dessmonitor_history_resolution import (  # noqa: E402
    DESSMONITOR_RESOLVED_HISTORY_AUTHORITY,
    DessMonitorResolvedHistoryPoint,
    DessMonitorResolvedHistorySeries,
    resolve_dessmonitor_history_time_basis,
)
from custom_components.eybond_local.dessmonitor_time_basis import (  # noqa: E402
    DessMonitorDeviceTimeBasis,
)


FULL_PN = "E50000200000000001"
SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "dessmonitor_history_resolution.py"
)


def _identity(*, pn: str = FULL_PN) -> DessMonitorDeviceIdentity:
    return DessMonitorDeviceIdentity(
        pn=pn,
        sn="92632511100118",
        devcode=2376,
        devaddr=1,
    )


def _series(
    *,
    identity: DessMonitorDeviceIdentity | None = None,
) -> DessMonitorHistorySeries:
    return DessMonitorHistorySeries(
        identity=identity or _identity(),
        source_action=DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
        series_key="pv_voltage",
        title="PV Voltage",
        unit="V",
        requested_date="2026-08-22",
        precision_minutes=5,
        points=(
            DessMonitorHistoryPoint("2026-08-22 10:00:00", "123.4"),
            DessMonitorHistoryPoint("2026-08-22 10:05:00", "124.1"),
        ),
    )


def _basis(
    *,
    identity: DessMonitorDeviceIdentity | None = None,
    offset_seconds: int = 3 * 60 * 60,
) -> DessMonitorDeviceTimeBasis:
    return DessMonitorDeviceTimeBasis(
        identity=identity or _identity(),
        offset_seconds=offset_seconds,
    )


class DessMonitorResolvedHistoryTests(unittest.TestCase):
    def test_resolution_preserves_local_time_and_derives_exact_utc(self) -> None:
        resolved = resolve_dessmonitor_history_time_basis(
            _series(),
            _basis(),
        )

        self.assertEqual(
            resolved.points[0].device_local_timestamp,
            "2026-08-22 10:00:00",
        )
        self.assertEqual(
            resolved.points[0].utc_timestamp,
            "2026-08-22T07:00:00+00:00",
        )
        self.assertEqual(resolved.points[0].value, "123.4")
        self.assertEqual(resolved.point_count, 2)

    def test_roundtrip_is_json_safe_and_stays_mapping_unproven(self) -> None:
        original = resolve_dessmonitor_history_time_basis(
            _series(),
            _basis(offset_seconds=-5 * 60 * 60),
        )
        record = original.to_record()
        parsed = DessMonitorResolvedHistorySeries.from_record(
            json.loads(json.dumps(record))
        )

        self.assertEqual(
            record["authority"],
            DESSMONITOR_RESOLVED_HISTORY_AUTHORITY,
        )
        self.assertEqual(record["local_mapping"], "unproven")
        self.assertIs(record["local_mapping_proven"], False)
        self.assertEqual(
            record["points"][0]["utc_timestamp"],
            "2026-08-22T15:00:00+00:00",
        )
        self.assertEqual(parsed, original)
        self.assertEqual(parsed.to_record(), record)

    def test_identity_mismatch_and_duck_inputs_fail_before_resolution(self) -> None:
        foreign_identity = _identity(pn="V00102000000000001")
        with self.assertRaisesRegex(ValueError, "identity_mismatch"):
            resolve_dessmonitor_history_time_basis(
                _series(),
                _basis(identity=foreign_identity),
            )
        with self.assertRaises(TypeError):
            resolve_dessmonitor_history_time_basis(  # type: ignore[arg-type]
                object(),
                _basis(),
            )
        with self.assertRaises(TypeError):
            resolve_dessmonitor_history_time_basis(  # type: ignore[arg-type]
                _series(),
                object(),
            )

    def test_direct_constructor_rejects_forged_or_misaligned_points(self) -> None:
        source = _series()
        basis = _basis()
        valid = resolve_dessmonitor_history_time_basis(source, basis)

        with self.assertRaises(ValueError):
            DessMonitorResolvedHistorySeries(
                source_series=source,
                time_basis=basis,
                points=valid.points[:1],
            )
        forged = DessMonitorResolvedHistoryPoint(
            device_local_timestamp="2026-08-22 10:00:00",
            utc_timestamp="2026-08-22T06:00:00+00:00",
            value="123.4",
        )
        with self.assertRaisesRegex(ValueError, "point_mismatch"):
            DessMonitorResolvedHistorySeries(
                source_series=source,
                time_basis=basis,
                points=(forged, valid.points[1]),
            )
        with self.assertRaises(ValueError):
            DessMonitorResolvedHistoryPoint(
                device_local_timestamp="2026-08-22 10:00:00",
                utc_timestamp="2026-08-22T10:00:00+03:00",
                value="123.4",
            )

    def test_parser_rejects_forged_authority_mapping_and_derived_count(self) -> None:
        class _DuckAuthority:
            def __eq__(self, _other):
                return True

        for key, value in (
            ("authority", _DuckAuthority()),
            ("local_mapping", "proven"),
            ("local_mapping_proven", True),
            ("point_count", 99),
        ):
            with self.subTest(key=key):
                record = resolve_dessmonitor_history_time_basis(
                    _series(),
                    _basis(),
                ).to_record()
                record[key] = value
                self.assertIsNone(
                    DessMonitorResolvedHistorySeries.from_record(record)
                )


class DessMonitorResolvedHistoryArchitectureTests(unittest.TestCase):
    def test_resolution_is_neutral_and_cannot_mint_local_bindings(self) -> None:
        source = SOURCE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")

        forbidden_imports = {
            "drivers",
            "runtime",
            "flows",
            "read_learning_binder",
            "overlay_generator",
        }
        self.assertFalse(
            any(
                any(part in imported for part in forbidden_imports)
                for imported in imports
            )
        )
        for forbidden in (
            "register_address",
            "driver_key",
            "read_bindings",
            "write_capability",
            "activation",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn("provider_identity_bound_time_resolution", source)
        self.assertIn('"local_mapping_proven": False', source)


if __name__ == "__main__":
    unittest.main()
