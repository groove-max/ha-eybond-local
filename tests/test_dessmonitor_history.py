from __future__ import annotations

import ast
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorApiEnvelope,
    DessMonitorCloudError,
    DessMonitorDeviceIdentity,
    DessMonitorSession,
)
import custom_components.eybond_local.dessmonitor_history as history_module  # noqa: E402
from custom_components.eybond_local.dessmonitor_history import (  # noqa: E402
    DESSMONITOR_HISTORY_AUTHORITY,
    DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    DESSMONITOR_HISTORY_TIME_BASIS,
    DessMonitorHistoryPoint,
    DessMonitorHistorySeries,
    fetch_key_parameter_history,
    fetch_sole_chart_history,
    parse_key_parameter_history,
    parse_sole_chart_history,
)


FULL_PN = "E50000200000000001"
SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "dessmonitor_history.py"
)


def _identity() -> DessMonitorDeviceIdentity:
    return DessMonitorDeviceIdentity(
        pn=FULL_PN,
        sn="92632511100118",
        devcode=2376,
        devaddr=1,
    )


def _series() -> DessMonitorHistorySeries:
    return DessMonitorHistorySeries(
        identity=_identity(),
        source_action=DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
        series_key="PV_VOLTAGE",
        title="PV Voltage",
        unit="V",
        requested_date="2026-08-22",
        precision_minutes=0,
        points=(
            DessMonitorHistoryPoint(
                device_local_timestamp="2026-08-22 10:00:00",
                value="123.40",
            ),
            DessMonitorHistoryPoint(
                device_local_timestamp="2026-08-22 10:05:00",
                value="124.10",
            ),
        ),
    )


class DessMonitorHistoryModelTests(unittest.TestCase):
    def test_roundtrip_is_json_safe_and_explicitly_unresolved(self) -> None:
        original = _series()

        record = original.to_record()
        parsed = DessMonitorHistorySeries.from_record(
            json.loads(json.dumps(record))
        )

        self.assertEqual(record["authority"], DESSMONITOR_HISTORY_AUTHORITY)
        self.assertEqual(record["time_basis"], DESSMONITOR_HISTORY_TIME_BASIS)
        self.assertEqual(record["timezone_offset"], "")
        self.assertEqual(record["local_mapping"], "unproven")
        self.assertIs(record["local_mapping_proven"], False)
        self.assertNotIn("utc", record["points"][0])
        self.assertEqual(parsed, original)
        self.assertEqual(parsed.to_record(), record)

    def test_direct_constructors_reject_malformed_time_value_and_shape(self) -> None:
        with self.assertRaises(ValueError):
            DessMonitorHistoryPoint(
                device_local_timestamp="2026-08-22T10:00:00+00:00",
                value="1",
            )
        for value in ("NaN", "Infinity", " 1", "value"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    DessMonitorHistoryPoint(
                        device_local_timestamp="2026-08-22 10:00:00",
                        value=value,
                    )
        with self.assertRaises(TypeError):
            DessMonitorHistoryPoint(  # type: ignore[arg-type]
                device_local_timestamp="2026-08-22 10:00:00",
                value=1,
            )
        with self.assertRaises(ValueError):
            DessMonitorHistorySeries(
                identity=_identity(),
                source_action=DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
                series_key="PV_VOLTAGE",
                requested_date="2026-08-22",
                points=(
                    DessMonitorHistoryPoint(
                        "2026-08-23 00:00:00",
                        "1",
                    ),
                ),
            )
        with self.assertRaises(TypeError):
            DessMonitorHistorySeries(  # type: ignore[arg-type]
                identity=_identity(),
                source_action=DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
                series_key="pv_voltage",
                requested_date="2026-08-22",
                points=(),
                precision_minutes=True,
            )
        with self.assertRaises(ValueError):
            DessMonitorHistorySeries(
                identity=_identity(),
                source_action=DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
                series_key="PV_VOLTAGE",
                requested_date="2026-08-22",
                points=tuple(reversed(_series().points)),
            )

    def test_parser_rejects_forged_authority_and_derived_counts(self) -> None:
        class _DuckAuthority:
            def __eq__(self, _other):
                return True

        for key, value in (
            ("authority", _DuckAuthority()),
            ("time_basis", "utc"),
            ("timezone_offset", "+03:00"),
            ("local_mapping_proven", True),
            ("point_count", 99),
        ):
            with self.subTest(key=key):
                record = _series().to_record()
                record[key] = value
                self.assertIsNone(DessMonitorHistorySeries.from_record(record))


class DessMonitorHistoryParserTests(unittest.TestCase):
    def test_key_parameter_parser_sorts_and_skips_malformed_rows(self) -> None:
        series = parse_key_parameter_history(
            {
                "parameter": [
                    {"val": "124.1", "ts": "2026-08-22 10:05:00"},
                    {"val": "not-a-number", "ts": "2026-08-22 10:02:00"},
                    {"val": "123.4", "ts": "2026-08-22 10:00:00"},
                    {"val": "99", "ts": "2026-08-23 00:00:00"},
                ]
            },
            identity=_identity(),
            parameter="PV_VOLTAGE",
            requested_date="2026-08-22",
        )

        self.assertEqual(
            [item.device_local_timestamp for item in series.points],
            ["2026-08-22 10:00:00", "2026-08-22 10:05:00"],
        )
        self.assertEqual([item.value for item in series.points], ["123.4", "124.1"])
        self.assertEqual(series.precision_minutes, 0)

    def test_chart_parser_preserves_provider_label_unit_and_naive_time(self) -> None:
        series = parse_sole_chart_history(
            {
                "optional": "pv_voltage",
                "name": "PV Voltage",
                "uint": "V",
                "rets": [
                    {"key": "2026-08-22 00:00:55", "val": "218.0000"},
                    {"key": "2026-08-22 00:05:56", "val": "219.0000"},
                ],
            },
            identity=_identity(),
            requested_date="2026-08-22",
            precision_minutes=5,
        )

        self.assertEqual(series.series_key, "pv_voltage")
        self.assertEqual(series.title, "PV Voltage")
        self.assertEqual(series.unit, "V")
        self.assertEqual(series.precision_minutes, 5)
        self.assertEqual(
            series.points[0].device_local_timestamp,
            "2026-08-22 00:00:55",
        )

    def test_duplicate_or_unbounded_provider_rows_fail_closed(self) -> None:
        duplicate = [
            {"val": "1", "ts": "2026-08-22 10:00:00"},
            {"val": "2", "ts": "2026-08-22 10:00:00"},
        ]
        with self.assertRaisesRegex(
            DessMonitorCloudError,
            "timestamp_ambiguous",
        ):
            parse_key_parameter_history(
                {"parameter": duplicate},
                identity=_identity(),
                parameter="PV_VOLTAGE",
                requested_date="2026-08-22",
            )

        oversized = [
            {"val": "1", "ts": "2026-08-22 10:00:00"}
        ] * (history_module._MAX_HISTORY_POINTS + 1)
        with self.assertRaisesRegex(DessMonitorCloudError, "limit_exceeded"):
            parse_key_parameter_history(
                {"parameter": oversized},
                identity=_identity(),
                parameter="PV_VOLTAGE",
                requested_date="2026-08-22",
            )


class DessMonitorHistoryFetchTests(unittest.TestCase):
    def test_key_parameter_fetch_uses_only_official_read_action(self) -> None:
        captured: list[str] = []

        def fetch(*, action, **_kwargs):
            captured.append(action)
            return DessMonitorApiEnvelope(
                err=0,
                desc="ERR_NONE",
                dat={
                    "parameter": [
                        {"val": "123.4", "ts": "2026-08-22 10:00:00"}
                    ]
                },
            )

        with patch.object(history_module, "fetch_signed_action", side_effect=fetch):
            series = fetch_key_parameter_history(
                session=DessMonitorSession(token="token", secret="secret"),
                identity=_identity(),
                parameter="PV_VOLTAGE",
                requested_date="2026-08-22",
            )

        query = parse_qs(captured[0].removeprefix("&"))
        self.assertEqual(query["action"], [DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER])
        self.assertEqual(query["parameter"], ["PV_VOLTAGE"])
        self.assertEqual(query["date"], ["2026-08-22"])
        self.assertNotIn("ctrlDevice", captured[0])
        self.assertEqual(series.point_count, 1)

    def test_chart_fetch_uses_one_bounded_same_day_window(self) -> None:
        captured: list[str] = []

        def fetch(*, action, **_kwargs):
            captured.append(action)
            return DessMonitorApiEnvelope(
                err=0,
                desc="ERR_NONE",
                dat={
                    "optional": "pv_voltage",
                    "name": "PV Voltage",
                    "uint": "V",
                    "rets": [],
                },
            )

        with patch.object(history_module, "fetch_signed_action", side_effect=fetch):
            series = fetch_sole_chart_history(
                session=DessMonitorSession(token="token", secret="secret"),
                identity=_identity(),
                requested_date="2026-08-22",
                precision_minutes=5,
            )

        query = parse_qs(captured[0].removeprefix("&"))
        self.assertEqual(query["action"], [DESSMONITOR_HISTORY_SOURCE_SOLE_CHART])
        self.assertEqual(query["precision"], ["5"])
        self.assertEqual(query["sdate"], ["2026-08-22 00:00:00"])
        self.assertEqual(query["edate"], ["2026-08-22 23:59:59"])
        self.assertEqual(series.point_count, 0)


class DessMonitorHistoryArchitectureTests(unittest.TestCase):
    def test_history_module_has_no_local_binding_or_runtime_dependency(self) -> None:
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")

        forbidden = {
            "drivers",
            "runtime",
            "flows",
            "read_learning_binder",
            "overlay_generator",
        }
        self.assertFalse(
            any(any(part in imported for part in forbidden) for imported in imports)
        )
        fields = {
            node.target.id
            for node in ast.walk(tree)
            if isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
        }
        self.assertFalse(
            fields
            & {
                "register",
                "register_address",
                "driver_key",
                "read_bindings",
                "write_capability",
                "activation",
            }
        )


if __name__ == "__main__":
    unittest.main()
