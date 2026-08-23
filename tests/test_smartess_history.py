from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
import types
import unittest
from urllib.parse import parse_qs
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_history_evidence import (  # noqa: E402
    CLOUD_HISTORY_STATUS_COMPLETE,
    CLOUD_HISTORY_STATUS_UNAVAILABLE,
    CLOUD_HISTORY_STATUS_TIME_BASIS_UNAVAILABLE,
    CloudHistoryCollection,
)
from custom_components.eybond_local.support.smartess_history import (  # noqa: E402
    SMARTESS_HISTORY_FAILURE_ACTION_REJECTED,
    SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE,
    SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
    SMARTESS_HISTORY_KEYS_ACTION,
    SMARTESS_HISTORY_SOURCE_ACTION,
    SMARTESS_HISTORY_TIME_BASIS_ACTION,
    SmartEssHistoryFetchResult,
    fetch_smartess_evidence_with_history,
)
from custom_components.eybond_local.smartess_cloud import (  # noqa: E402
    SmartEssActionRejectedError,
    SmartEssCloudError,
)


FULL_PN = "E50000253884199645"


def _bundle() -> dict:
    return {
        "request": {
            "params": {
                "pn": FULL_PN,
                "sn": "92632511100118",
                "devcode": 2376,
                "devaddr": 1,
            }
        },
        "normalized": {},
    }


def _collect_with_keys(
    key_dat: object,
    *,
    transient_key_failures: int = 0,
    source_dat: object | None = None,
    source_exception: Exception | None = None,
) -> tuple[SmartEssHistoryFetchResult, list[str]]:
    session = object()
    actions: list[str] = []
    key_attempts = 0

    def signed_action(*, action, **_kwargs):
        nonlocal key_attempts
        params = parse_qs(action.lstrip("&"), keep_blank_values=True)
        action_name = params["action"][0]
        actions.append(action_name)
        if action_name == SMARTESS_HISTORY_TIME_BASIS_ACTION:
            return types.SimpleNamespace(
                dat=[
                    {
                        "pn": FULL_PN,
                        "sn": "92632511100118",
                        "devcode": 2376,
                        "devaddr": 1,
                        "timezone": 7200,
                    }
                ]
            )
        if action_name == SMARTESS_HISTORY_KEYS_ACTION:
            key_attempts += 1
            if key_attempts <= transient_key_failures:
                raise SmartEssCloudError("network_error:timed out")
            return types.SimpleNamespace(dat=key_dat)
        if source_exception is not None:
            raise source_exception
        return types.SimpleNamespace(
            dat=(
                {
                    "parameter": [
                        {"ts": "2026-08-23 12:00:00", "val": "1.00"},
                    ]
                }
                if source_dat is None
                else source_dat
            )
        )

    with patch(
        "custom_components.eybond_local.support.smartess_history.login_for_control_discovery",
        return_value=({}, session),
    ), patch(
        "custom_components.eybond_local.support.smartess_history.fetch_device_bundle_for_collector_with_session",
        return_value=_bundle(),
    ), patch(
        "custom_components.eybond_local.support.smartess_history.fetch_signed_action",
        side_effect=signed_action,
    ), patch(
        "custom_components.eybond_local.support.smartess_history.sleep"
    ):
        result = fetch_smartess_evidence_with_history(
            username="user",
            password="secret",
            collector_pn=FULL_PN,
            utc_now=datetime(2026, 8, 23, 10, 30, tzinfo=timezone.utc),
        )
    return result, actions


class SmartEssHistoryTests(unittest.TestCase):
    def test_one_session_collects_exact_daily_history_and_utc_basis(self) -> None:
        session = object()
        actions: list[str] = []
        details: list[tuple[str, int, int]] = []

        def signed_action(*, action, session: object, **_kwargs):
            self.assertIs(session, cloud_session)
            actions.append(action)
            params = parse_qs(action.lstrip("&"), keep_blank_values=True)
            action_name = params["action"][0]
            if action_name == SMARTESS_HISTORY_TIME_BASIS_ACTION:
                return types.SimpleNamespace(
                    dat=[
                        {
                            "pn": FULL_PN,
                            "sn": "92632511100118",
                            "devcode": 2376,
                            "devaddr": 1,
                            "timezone": 7200,
                        }
                    ]
                )
            if action_name == SMARTESS_HISTORY_KEYS_ACTION:
                return types.SimpleNamespace(
                    dat={
                        "keys": [
                            "PV_OUTPUT_POWER",
                            "LOAD_ACTIVE_POWER",
                        ]
                    }
                )
            self.assertEqual(action_name, SMARTESS_HISTORY_SOURCE_ACTION)
            self.assertEqual(params["pn"], [FULL_PN])
            self.assertEqual(params["sn"], ["92632511100118"])
            self.assertEqual(params["devcode"], ["2376"])
            self.assertEqual(params["devaddr"], ["1"])
            self.assertEqual(params["date"], ["2026-08-23"])
            return types.SimpleNamespace(
                dat={
                    "parameter": [
                        {"ts": "2026-08-23 12:00:00", "val": "1.00"},
                        {"ts": "2026-08-23 12:05:00", "val": "1.25"},
                    ]
                }
            )

        cloud_session = session
        with patch(
            "custom_components.eybond_local.support.smartess_history.login_for_control_discovery",
            return_value=({}, session),
        ) as login, patch(
            "custom_components.eybond_local.support.smartess_history.fetch_device_bundle_for_collector_with_session",
            return_value=_bundle(),
        ) as metadata, patch(
            "custom_components.eybond_local.support.smartess_history.fetch_signed_action",
            side_effect=signed_action,
        ):
            result = fetch_smartess_evidence_with_history(
                username="user",
                password="secret",
                collector_pn=FULL_PN,
                utc_now=datetime(2026, 8, 23, 10, 30, tzinfo=timezone.utc),
                progress_detail=lambda *args: details.append(args),
            )

        bundle = result.bundle
        collection = result.history_collection
        self.assertEqual(bundle, _bundle())
        self.assertEqual(result.failure_stage, "")
        self.assertEqual(result.failure_code, "")
        self.assertEqual(collection.status, CLOUD_HISTORY_STATUS_COMPLETE)
        self.assertEqual(collection.requested_date, "2026-08-23")
        self.assertEqual(collection.timezone_offset_seconds, 7200)
        self.assertEqual(collection.collected_series_count, 2)
        self.assertEqual(collection.point_count, 4)
        self.assertEqual(
            collection.series[0].points[0].utc_timestamp,
            "2026-08-23T10:00:00+00:00",
        )
        self.assertEqual(
            collection.series[0].points[1].utc_timestamp,
            "2026-08-23T10:05:00+00:00",
        )
        self.assertEqual(details[-1], (SMARTESS_HISTORY_SOURCE_ACTION, 2, 2))
        self.assertEqual(login.call_count, 1)
        self.assertEqual(metadata.call_count, 1)
        self.assertEqual(len(actions), 4)
        self.assertIsNotNone(
            CloudHistoryCollection.from_record(collection.to_record())
        )

    def test_live_key_parameter_row_forms_collect_history(self) -> None:
        for key_dat in (
            [{"e0": "PV_OUTPUT_POWER", "e1": "PV Power", "e3": "kW"}],
            {"field": [{"field": "PV_OUTPUT_POWER"}]},
            {"pars": [{"e0": "PV_OUTPUT_POWER"}]},
            {
                "parameters": [
                    {
                        "id": "PV_OUTPUT_POWER",
                        "name": "PV Power",
                        "unit": "kW",
                    }
                ]
            },
            {"dat": [{"id": "PV_OUTPUT_POWER"}]},
        ):
            with self.subTest(key_dat=key_dat):
                result, actions = _collect_with_keys(key_dat)

                self.assertEqual(
                    result.history_collection.status,
                    CLOUD_HISTORY_STATUS_COMPLETE,
                )
                self.assertEqual(
                    result.history_collection.series[0].series_key,
                    "PV_OUTPUT_POWER",
                )
                self.assertEqual(result.failure_stage, "")
                self.assertEqual(result.failure_code, "")
                self.assertIn(SMARTESS_HISTORY_SOURCE_ACTION, actions)

    def test_transient_key_failure_is_retried_without_losing_history(self) -> None:
        result, actions = _collect_with_keys(
            [{"e0": "PV_OUTPUT_POWER"}],
            transient_key_failures=2,
        )

        self.assertEqual(
            actions.count(SMARTESS_HISTORY_KEYS_ACTION),
            3,
        )
        self.assertEqual(
            result.history_collection.status,
            CLOUD_HISTORY_STATUS_COMPLETE,
        )
        self.assertEqual(result.failure_code, "")

    def test_malformed_present_key_response_is_typed_not_silent(self) -> None:
        malformed = (
            object(),
            {"unknown": []},
            {"keys": [" PV_OUTPUT_POWER"]},
            [{"e0": object()}],
            {"field": [{"name": "PV Power"}]},
            {"keys": ["PV_OUTPUT_POWER"], "dat": []},
        )
        for key_dat in malformed:
            with self.subTest(key_dat=key_dat):
                result, actions = _collect_with_keys(key_dat)

                self.assertEqual(
                    result.history_collection.status,
                    CLOUD_HISTORY_STATUS_UNAVAILABLE,
                )
                self.assertEqual(
                    result.failure_stage,
                    SMARTESS_HISTORY_KEYS_ACTION,
                )
                self.assertEqual(
                    result.failure_code,
                    SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE,
                )
                self.assertNotIn(SMARTESS_HISTORY_SOURCE_ACTION, actions)

    def test_valid_empty_key_response_has_distinct_reason(self) -> None:
        result, actions = _collect_with_keys({"keys": []})

        self.assertEqual(
            result.history_collection.status,
            CLOUD_HISTORY_STATUS_UNAVAILABLE,
        )
        self.assertEqual(result.failure_stage, SMARTESS_HISTORY_KEYS_ACTION)
        self.assertEqual(
            result.failure_code,
            SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
        )
        self.assertNotIn(SMARTESS_HISTORY_SOURCE_ACTION, actions)

    def test_empty_series_response_is_invalid_not_a_collected_series(self) -> None:
        result, actions = _collect_with_keys(
            {"keys": ["PV_OUTPUT_POWER"]},
            source_dat={"parameter": []},
        )

        collection = result.history_collection
        self.assertEqual(collection.status, CLOUD_HISTORY_STATUS_UNAVAILABLE)
        self.assertEqual(collection.attempted_series_count, 1)
        self.assertEqual(collection.failed_series_count, 1)
        self.assertEqual(collection.collected_series_count, 0)
        self.assertEqual(result.failure_stage, SMARTESS_HISTORY_SOURCE_ACTION)
        self.assertEqual(
            result.failure_code,
            SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE,
        )
        self.assertEqual(actions.count(SMARTESS_HISTORY_SOURCE_ACTION), 1)

    def test_action_rejection_is_distinct_and_provider_text_is_not_logged(self) -> None:
        with self.assertLogs(
            "custom_components.eybond_local.support.smartess_history",
            level="INFO",
        ) as captured:
            result, actions = _collect_with_keys(
                {"keys": ["PV_OUTPUT_POWER"]},
                source_exception=SmartEssActionRejectedError(
                    err=7,
                    desc="provider-secret-description",
                ),
            )

        self.assertEqual(
            result.failure_code,
            SMARTESS_HISTORY_FAILURE_ACTION_REJECTED,
        )
        self.assertEqual(result.history_collection.failed_series_count, 1)
        self.assertEqual(actions.count(SMARTESS_HISTORY_SOURCE_ACTION), 1)
        joined = "\n".join(captured.output)
        self.assertIn("series=PV_OUTPUT_POWER", joined)
        self.assertIn("error=action_rejected", joined)
        self.assertNotIn("provider-secret-description", joined)

    def test_diagnostics_boundary_contains_no_bundle_or_credentials(self) -> None:
        result, _actions = _collect_with_keys({"keys": []})
        diagnostics = result.to_diagnostics_record()

        self.assertEqual(
            set(diagnostics),
            {
                "provider_id",
                "source_id",
                "history_status",
                "failure_stage",
                "failure_code",
                "attempted_series_count",
                "failed_series_count",
                "collected_series_count",
                "point_count",
                "budget_exhausted",
            },
        )
        self.assertNotIn(FULL_PN, str(diagnostics))
        self.assertNotIn("secret", str(diagnostics))
        with self.assertRaises((TypeError, ValueError)):
            SmartEssHistoryFetchResult(
                bundle=result.bundle,
                history_collection=result.history_collection,
                failure_stage=object(),  # type: ignore[arg-type]
                failure_code=SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
            )
        with self.assertRaises(ValueError):
            SmartEssHistoryFetchResult(
                bundle=result.bundle,
                history_collection=result.history_collection,
            )

    def test_missing_exact_time_basis_fails_closed_without_series_calls(self) -> None:
        session = object()
        actions: list[str] = []

        def signed_action(*, action, **_kwargs):
            actions.append(action)
            raise SmartEssCloudError("missing_time_basis")

        with patch(
            "custom_components.eybond_local.support.smartess_history.login_for_control_discovery",
            return_value=({}, session),
        ), patch(
            "custom_components.eybond_local.support.smartess_history.fetch_device_bundle_for_collector_with_session",
            return_value=_bundle(),
        ), patch(
            "custom_components.eybond_local.support.smartess_history.fetch_signed_action",
            side_effect=signed_action,
        ):
            result = fetch_smartess_evidence_with_history(
                username="user",
                password="secret",
                collector_pn=FULL_PN,
                utc_now=datetime(2026, 8, 23, 10, 30, tzinfo=timezone.utc),
            )

        collection = result.history_collection
        self.assertEqual(
            collection.status,
            CLOUD_HISTORY_STATUS_TIME_BASIS_UNAVAILABLE,
        )
        self.assertEqual(
            result.failure_stage,
            SMARTESS_HISTORY_TIME_BASIS_ACTION,
        )
        self.assertEqual(result.failure_code, "unexpected")
        self.assertEqual(collection.series, ())
        self.assertEqual(len(actions), 1)


if __name__ == "__main__":
    unittest.main()
