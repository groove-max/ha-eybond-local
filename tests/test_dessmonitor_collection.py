from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import custom_components.eybond_local.dessmonitor_collection as collection_module  # noqa: E402
from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorApiEnvelope,
    DessMonitorCloudError,
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    DessMonitorSession,
    DessMonitorTelemetryField,
)
from custom_components.eybond_local.dessmonitor_collection import (  # noqa: E402
    DESSMONITOR_COLLECTION_AUTHORITY,
    DESSMONITOR_COLLECTION_STATUS_COMPLETE,
    DESSMONITOR_COLLECTION_STATUS_PARTIAL,
    DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
    DESSMONITOR_COLLECTION_STATUS_UNAVAILABLE,
    DessMonitorHistoryCollection,
    fetch_read_only_evidence_with_history,
)
from custom_components.eybond_local.dessmonitor_history import (  # noqa: E402
    DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    DessMonitorHistoryPoint,
    DessMonitorHistorySeries,
)
from custom_components.eybond_local.dessmonitor_history_resolution import (  # noqa: E402
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
    / "dessmonitor_collection.py"
)


def _identity() -> DessMonitorDeviceIdentity:
    return DessMonitorDeviceIdentity(
        pn=FULL_PN,
        sn="92632511100118",
        devcode=2376,
        devaddr=1,
    )


def _bundle() -> DessMonitorEvidenceBundle:
    return DessMonitorEvidenceBundle(
        identity=_identity(),
        telemetry_fields=(),
        chart_fields=(),
        key_parameters=(
            DessMonitorTelemetryField(
                field_id="pv_voltage",
                title="PV Voltage",
                value="230.0",
                unit="V",
                section="",
                source_action="querySPKeyParameters",
            ),
            DessMonitorTelemetryField(
                field_id="battery_voltage",
                title="Battery Voltage",
                value="52.1",
                unit="V",
                section="",
                source_action="querySPKeyParameters",
            ),
        ),
        control_fields=(),
    )


def _basis() -> DessMonitorDeviceTimeBasis:
    return DessMonitorDeviceTimeBasis(
        identity=_identity(),
        offset_seconds=3 * 60 * 60,
    )


def _history(
    source_action: str,
    series_key: str,
    *,
    title: str = "PV Voltage",
    unit: str = "V",
) -> DessMonitorHistorySeries:
    return DessMonitorHistorySeries(
        identity=_identity(),
        source_action=source_action,
        series_key=series_key,
        title=title,
        unit=unit,
        requested_date="2026-08-23",
        precision_minutes=(
            0
            if source_action == DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER
            else 5
        ),
        points=(
            DessMonitorHistoryPoint(
                device_local_timestamp="2026-08-23 00:05:00",
                value="230.0",
            ),
        ),
    )


def _collection() -> DessMonitorHistoryCollection:
    basis = _basis()
    source = _history(
        DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
        "pv_voltage",
    )
    return DessMonitorHistoryCollection(
        identity=_identity(),
        time_basis=basis,
        requested_date="2026-08-23",
        attempted_series_count=1,
        failed_series_count=0,
        budget_exhausted=False,
        series=(resolve_dessmonitor_history_time_basis(source, basis),),
    )


class DessMonitorHistoryCollectionModelTests(unittest.TestCase):
    def test_roundtrip_is_json_safe_read_only_and_unproven(self) -> None:
        original = _collection()
        record = original.to_record()
        parsed = DessMonitorHistoryCollection.from_record(
            json.loads(json.dumps(record))
        )

        self.assertEqual(record["authority"], DESSMONITOR_COLLECTION_AUTHORITY)
        self.assertIs(record["read_only"], True)
        self.assertIs(record["local_mapping_proven"], False)
        self.assertIs(record["activation_allowed"], False)
        self.assertEqual(record["status"], DESSMONITOR_COLLECTION_STATUS_COMPLETE)
        self.assertEqual(record["point_count"], 1)
        self.assertEqual(parsed, original)
        self.assertEqual(parsed.to_record(), record)

    def test_time_basis_unavailable_has_one_exact_empty_shape(self) -> None:
        collection = DessMonitorHistoryCollection(
            identity=_identity(),
            time_basis=None,
            requested_date="",
            attempted_series_count=0,
            failed_series_count=0,
            budget_exhausted=False,
            series=(),
        )

        self.assertEqual(
            collection.status,
            DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
        )
        with self.assertRaises(ValueError):
            DessMonitorHistoryCollection(
                identity=_identity(),
                time_basis=None,
                requested_date="2026-08-23",
                attempted_series_count=1,
                failed_series_count=1,
                budget_exhausted=False,
                series=(),
            )
        with self.assertRaisesRegex(ValueError, "empty_attempt"):
            DessMonitorHistoryCollection(
                identity=_identity(),
                time_basis=_basis(),
                requested_date="2026-08-23",
                attempted_series_count=0,
                failed_series_count=0,
                budget_exhausted=False,
                series=(),
            )

    def test_parser_rejects_forged_authority_flags_status_and_counts(self) -> None:
        for key, value in (
            ("authority", object()),
            ("read_only", False),
            ("local_mapping_proven", True),
            ("activation_allowed", True),
            ("budget_exhausted", object()),
            ("status", "proven"),
            ("collected_series_count", 99),
            ("point_count", 99),
        ):
            with self.subTest(key=key):
                record = _collection().to_record()
                record[key] = value
                self.assertIsNone(DessMonitorHistoryCollection.from_record(record))


class DessMonitorHistoryCollectionFetchTests(unittest.TestCase):
    def test_single_login_collects_chart_and_bounded_labeled_parameters(self) -> None:
        session = DessMonitorSession(token="token", secret="secret")
        seen_sessions: list[DessMonitorSession] = []

        def chart(*, session, requested_date, **_kwargs):
            seen_sessions.append(session)
            self.assertEqual(requested_date, "2026-08-23")
            return _history(
                DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
                "sole_pv",
            )

        def parameter(*, session, parameter, requested_date, **_kwargs):
            seen_sessions.append(session)
            self.assertEqual(requested_date, "2026-08-23")
            return _history(
                DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
                parameter,
                title=parameter,
                unit="",
            )

        with (
            patch.object(
                collection_module,
                "login_with_password",
                return_value=(
                    DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat={}),
                    session,
                ),
            ) as login,
            patch.object(
                collection_module,
                "fetch_read_only_evidence_for_session",
                side_effect=lambda **kwargs: (
                    seen_sessions.append(kwargs["session"]) or _bundle()
                ),
            ),
            patch.object(
                collection_module,
                "fetch_device_time_basis",
                side_effect=lambda **kwargs: (
                    seen_sessions.append(kwargs["session"]) or _basis()
                ),
            ),
            patch.object(
                collection_module,
                "fetch_sole_chart_history",
                side_effect=chart,
            ),
            patch.object(
                collection_module,
                "fetch_key_parameter_history",
                side_effect=parameter,
            ),
        ):
            bundle, collection = fetch_read_only_evidence_with_history(
                username="account",
                password="password",
                collector_pn=FULL_PN,
                max_history_series=3,
                utc_now=datetime(2026, 8, 22, 22, 30, tzinfo=timezone.utc),
            )

        self.assertEqual(login.call_count, 1)
        self.assertTrue(seen_sessions)
        self.assertTrue(all(item is session for item in seen_sessions))
        self.assertEqual(bundle, _bundle())
        self.assertEqual(collection.status, DESSMONITOR_COLLECTION_STATUS_COMPLETE)
        self.assertEqual(collection.requested_date, "2026-08-23")
        self.assertEqual(collection.attempted_series_count, 3)
        self.assertEqual(collection.collected_series_count, 3)
        self.assertEqual(
            [item.source_series.title for item in collection.series[1:]],
            ["PV Voltage", "Battery Voltage"],
        )

    def test_individual_history_failure_preserves_metadata_and_partial_result(self) -> None:
        session = DessMonitorSession(token="token", secret="secret")
        calls = 0

        def parameter(*, parameter, **_kwargs):
            nonlocal calls
            calls += 1
            if parameter == "battery_voltage":
                raise DessMonitorCloudError("history_unavailable")
            return _history(
                DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
                parameter,
                title=parameter,
                unit="",
            )

        with (
            patch.object(
                collection_module,
                "login_with_password",
                return_value=(
                    DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat={}),
                    session,
                ),
            ),
            patch.object(
                collection_module,
                "fetch_read_only_evidence_for_session",
                return_value=_bundle(),
            ),
            patch.object(
                collection_module,
                "fetch_device_time_basis",
                return_value=_basis(),
            ),
            patch.object(
                collection_module,
                "fetch_sole_chart_history",
                return_value=_history(
                    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
                    "sole_pv",
                ),
            ),
            patch.object(
                collection_module,
                "fetch_key_parameter_history",
                side_effect=parameter,
            ),
        ):
            bundle, collection = fetch_read_only_evidence_with_history(
                username="account",
                password="password",
                collector_pn=FULL_PN,
                max_history_series=3,
                utc_now=datetime(2026, 8, 23, tzinfo=timezone.utc),
            )

        self.assertEqual(bundle, _bundle())
        self.assertEqual(calls, 2)
        self.assertEqual(collection.status, DESSMONITOR_COLLECTION_STATUS_PARTIAL)
        self.assertEqual(collection.attempted_series_count, 3)
        self.assertEqual(collection.failed_series_count, 1)
        self.assertEqual(collection.collected_series_count, 2)

    def test_time_basis_failure_skips_history_but_preserves_metadata(self) -> None:
        session = DessMonitorSession(token="token", secret="secret")
        with (
            patch.object(
                collection_module,
                "login_with_password",
                return_value=(
                    DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat={}),
                    session,
                ),
            ),
            patch.object(
                collection_module,
                "fetch_read_only_evidence_for_session",
                return_value=_bundle(),
            ),
            patch.object(
                collection_module,
                "fetch_device_time_basis",
                side_effect=DessMonitorCloudError("timezone_unavailable"),
            ),
            patch.object(collection_module, "fetch_sole_chart_history") as chart,
            patch.object(collection_module, "fetch_key_parameter_history") as key,
        ):
            bundle, collection = fetch_read_only_evidence_with_history(
                username="account",
                password="password",
                collector_pn=FULL_PN,
            )

        self.assertEqual(bundle, _bundle())
        self.assertEqual(
            collection.status,
            DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
        )
        chart.assert_not_called()
        key.assert_not_called()

    def test_history_budget_stops_before_unstarted_requests(self) -> None:
        session = DessMonitorSession(token="token", secret="secret")
        with (
            patch.object(
                collection_module,
                "login_with_password",
                return_value=(
                    DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat={}),
                    session,
                ),
            ),
            patch.object(
                collection_module,
                "fetch_read_only_evidence_for_session",
                return_value=_bundle(),
            ),
            patch.object(
                collection_module,
                "fetch_device_time_basis",
                return_value=_basis(),
            ),
            patch.object(collection_module, "fetch_sole_chart_history") as chart,
            patch.object(collection_module, "fetch_key_parameter_history") as key,
            patch.object(collection_module, "monotonic", side_effect=(0.0, 0.0, 2.0)),
        ):
            bundle, collection = fetch_read_only_evidence_with_history(
                username="account",
                password="password",
                collector_pn=FULL_PN,
                history_budget_seconds=1.0,
                utc_now=datetime(2026, 8, 23, tzinfo=timezone.utc),
            )

        self.assertEqual(bundle, _bundle())
        self.assertEqual(
            collection.status,
            DESSMONITOR_COLLECTION_STATUS_UNAVAILABLE,
        )
        self.assertIs(collection.budget_exhausted, True)
        self.assertEqual(collection.attempted_series_count, 0)
        chart.assert_not_called()
        key.assert_not_called()

    def test_malformed_resolution_is_supplemental_not_a_metadata_failure(self) -> None:
        session = DessMonitorSession(token="token", secret="secret")
        real_resolver = resolve_dessmonitor_history_time_basis

        def resolve(series, basis):
            if series.source_action == DESSMONITOR_HISTORY_SOURCE_SOLE_CHART:
                raise ValueError("malformed_resolved_series")
            return real_resolver(series, basis)

        with (
            patch.object(
                collection_module,
                "login_with_password",
                return_value=(
                    DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat={}),
                    session,
                ),
            ),
            patch.object(
                collection_module,
                "fetch_read_only_evidence_for_session",
                return_value=_bundle(),
            ),
            patch.object(
                collection_module,
                "fetch_device_time_basis",
                return_value=_basis(),
            ),
            patch.object(
                collection_module,
                "fetch_sole_chart_history",
                return_value=_history(
                    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
                    "sole_pv",
                ),
            ),
            patch.object(
                collection_module,
                "fetch_key_parameter_history",
                side_effect=lambda *, parameter, **_kwargs: _history(
                    DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
                    parameter,
                    title=parameter,
                    unit="",
                ),
            ),
            patch.object(
                collection_module,
                "resolve_dessmonitor_history_time_basis",
                side_effect=resolve,
            ),
        ):
            bundle, collection = fetch_read_only_evidence_with_history(
                username="account",
                password="password",
                collector_pn=FULL_PN,
                max_history_series=3,
                utc_now=datetime(2026, 8, 23, tzinfo=timezone.utc),
            )

        self.assertEqual(bundle, _bundle())
        self.assertEqual(collection.status, DESSMONITOR_COLLECTION_STATUS_PARTIAL)
        self.assertEqual(collection.attempted_series_count, 3)
        self.assertEqual(collection.failed_series_count, 1)
        self.assertEqual(collection.collected_series_count, 2)

    def test_invalid_clock_fails_before_login(self) -> None:
        with patch.object(collection_module, "login_with_password") as login:
            with self.assertRaises(ValueError):
                fetch_read_only_evidence_with_history(
                    username="account",
                    password="password",
                    collector_pn=FULL_PN,
                    utc_now=datetime(2026, 8, 23),
                )
        login.assert_not_called()


class DessMonitorHistoryCollectionArchitectureTests(unittest.TestCase):
    def test_collection_is_read_only_and_has_no_runtime_activation_dependency(self) -> None:
        source = SOURCE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")

        for forbidden in (
            "runtime",
            "flows",
            "drivers",
            "read_learning_binder",
            "overlay_generator",
        ):
            self.assertFalse(any(forbidden in item for item in imports))
        for forbidden in (
            "ctrlDevice",
            "write_capability",
            "read_bindings",
            "activation=True",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn("bounded_read_only_history_collection", source)
        self.assertIn('"activation_allowed": False', source)


if __name__ == "__main__":
    unittest.main()
