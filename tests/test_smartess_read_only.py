from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_read_only_workflow import (  # noqa: E402
    ReadOnlyEvidenceWorkflowRunner,
)
from custom_components.eybond_local.support.cloud_history_evidence import (  # noqa: E402
    CLOUD_HISTORY_STATUS_COMPLETE,
    CLOUD_HISTORY_STATUS_UNAVAILABLE,
    CloudHistoryCollection,
    CloudHistoryIdentity,
    CloudHistoryPoint,
    CloudHistorySeries,
)
from custom_components.eybond_local.support.cloud_semantic_evidence import (  # noqa: E402
    CLOUD_FIELD_KIND_KEY_PARAMETER,
)
from custom_components.eybond_local.support.smartess_read_only import (  # noqa: E402
    SmartEssReadOnlyEvidenceOperation,
)
from custom_components.eybond_local.support.smartess_history import (  # noqa: E402
    SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
    SMARTESS_HISTORY_KEYS_ACTION,
    SmartEssHistoryFetchResult,
)
from custom_components.eybond_local.support.smartess_semantics import (  # noqa: E402
    build_smartess_semantic_report,
)


FULL_PN = "E50000200000000001"
SHORT_PN = "E5000020000000"


def _bundle() -> dict:
    return {
        "normalized": {
            "device_detail": {
                "sections": {
                    "pv_": [
                        {
                            "id": "pv_voltage",
                            "par": "PV Voltage",
                            "val": "123.4",
                            "unit": "V",
                        }
                    ]
                }
            },
            "device_settings": {
                "fields": [
                    {
                        "cloud_id": "output_priority",
                        "title": "Output Source Priority",
                        "has_current_value": True,
                        "current_value": "SBU",
                        "unit": "",
                    }
                ]
            },
        }
    }


def _history_collection() -> CloudHistoryCollection:
    identity = CloudHistoryIdentity(
        pn=FULL_PN,
        sn="SN-1",
        devcode=2376,
        devaddr=1,
    )
    series = CloudHistorySeries(
        provider_id="smartess",
        source_id="smartess",
        source_action="queryDeviceKeyParameterOneDay",
        field_kind=CLOUD_FIELD_KIND_KEY_PARAMETER,
        identity=identity,
        series_key="PV_OUTPUT_POWER",
        title="PV Power",
        unit="kW",
        requested_date="2026-08-23",
        precision_minutes=0,
        timezone_offset_seconds=7200,
        points=(
            CloudHistoryPoint(
                device_local_timestamp="2026-08-23 12:00:00",
                utc_timestamp="2026-08-23T10:00:00+00:00",
                value="1.25",
            ),
        ),
    )
    return CloudHistoryCollection(
        provider_id="smartess",
        source_id="smartess",
        identity=identity,
        requested_date="2026-08-23",
        timezone_offset_seconds=7200,
        attempted_series_count=1,
        failed_series_count=0,
        budget_exhausted=False,
        series=(series,),
    )


def _history_fetch_result() -> SmartEssHistoryFetchResult:
    return SmartEssHistoryFetchResult(
        bundle=_bundle(),
        history_collection=_history_collection(),
    )


def _unavailable_history_fetch_result() -> SmartEssHistoryFetchResult:
    available = _history_collection()
    return SmartEssHistoryFetchResult(
        bundle=_bundle(),
        history_collection=CloudHistoryCollection(
            provider_id="smartess",
            source_id="smartess",
            identity=available.identity,
            requested_date=available.requested_date,
            timezone_offset_seconds=available.timezone_offset_seconds,
            attempted_series_count=0,
            failed_series_count=0,
            budget_exhausted=False,
            series=(),
        ),
        failure_stage=SMARTESS_HISTORY_KEYS_ACTION,
        failure_code=SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
    )


async def _executor(fn, *args):
    return fn(*args)


class SmartEssReadOnlyTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_only_path_fetches_metadata_without_active_authority(self) -> None:
        bundle = _bundle()
        evidence = {"device_identity": {"pn": FULL_PN, "sn": "SN-1"}}
        start_route = AsyncMock()
        on_learning = Mock()
        identities: list[dict] = []

        with patch(
            "custom_components.eybond_local.support.smartess_read_only.fetch_smartess_evidence_with_history",
            return_value=SmartEssHistoryFetchResult(
                bundle=bundle,
                history_collection=_history_collection(),
            ),
        ) as fetch, patch(
            "custom_components.eybond_local.support.smartess_read_only.build_smartess_device_bundle_cloud_evidence",
            return_value=evidence,
        ) as build:
            outcome = await ReadOnlyEvidenceWorkflowRunner(
                SmartEssReadOnlyEvidenceOperation()
            ).async_run(
                executor=_executor,
                collector_pn=SHORT_PN,
                username="user",
                password="secret",
                fallback_identity={"pn": "FOREIGN"},
                max_fields=10,
                progress=lambda *_args, **_kwargs: None,
                orchestrator_callbacks={"write": object()},
                on_identity=identities.append,
                start_shadow_route=start_route,
                on_learning=on_learning,
            )

        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(fetch.call_args.kwargs["username"], "user")
        self.assertEqual(fetch.call_args.kwargs["password"], "secret")
        self.assertEqual(fetch.call_args.kwargs["collector_pn"], SHORT_PN)
        build.assert_called_once()
        start_route.assert_not_awaited()
        on_learning.assert_not_called()
        self.assertEqual(identities[0]["pn"], FULL_PN)
        self.assertEqual(outcome.result["source"], "smartess")
        self.assertTrue(outcome.result["metadata_only"])
        self.assertEqual(outcome.result["planned_write_count"], 0)
        self.assertEqual(outcome.result["executed_result_count"], 0)
        self.assertEqual(outcome.result["semantic_candidate_count"], 1)
        self.assertEqual(outcome.result["semantic_unit_conflict_count"], 1)
        self.assertEqual(outcome.result["control_metadata_count"], 1)
        self.assertEqual(
            outcome.result["history_status"],
            CLOUD_HISTORY_STATUS_COMPLETE,
        )
        self.assertEqual(outcome.result["history_series_count"], 1)
        self.assertEqual(outcome.result["history_point_count"], 1)
        self.assertEqual(outcome.result["history_failure_stage"], "")
        self.assertEqual(outcome.result["history_failure_code"], "")
        self.assertEqual(
            outcome.metadata_evidence["history_collection"],
            _history_collection().to_record(),
        )
        self.assertEqual(
            outcome.metadata_evidence["history_diagnostics"]["history_status"],
            CLOUD_HISTORY_STATUS_COMPLETE,
        )
        self.assertNotIn(
            FULL_PN,
            str(outcome.metadata_evidence["history_diagnostics"]),
        )
        self.assertIsNone(outcome.read_bindings)

    async def test_foreign_cloud_identity_is_rejected(self) -> None:
        with patch(
            "custom_components.eybond_local.support.smartess_read_only.fetch_smartess_evidence_with_history",
            return_value=_history_fetch_result(),
        ), patch(
            "custom_components.eybond_local.support.smartess_read_only.build_smartess_device_bundle_cloud_evidence",
            return_value={"device_identity": {"pn": "FOREIGN"}},
        ):
            with self.assertRaisesRegex(
                RuntimeError, "smartess_read_only_identity_mismatch"
            ):
                await SmartEssReadOnlyEvidenceOperation().async_collect(
                    executor=_executor,
                    collector_pn=SHORT_PN,
                    username="user",
                    password="secret",
                    max_fields=10,
                    progress=lambda *_args, **_kwargs: None,
                )

    async def test_unavailable_history_keeps_safe_provider_reason(self) -> None:
        history_fetch = _unavailable_history_fetch_result()
        with patch(
            "custom_components.eybond_local.support.smartess_read_only.fetch_smartess_evidence_with_history",
            return_value=history_fetch,
        ), patch(
            "custom_components.eybond_local.support.smartess_read_only.build_smartess_device_bundle_cloud_evidence",
            return_value={"device_identity": {"pn": FULL_PN, "sn": "SN-1"}},
        ):
            outcome = await SmartEssReadOnlyEvidenceOperation().async_collect(
                executor=_executor,
                collector_pn=SHORT_PN,
                username="user",
                password="secret",
                max_fields=10,
                progress=lambda *_args, **_kwargs: None,
            )

        self.assertEqual(
            outcome.result["history_status"],
            CLOUD_HISTORY_STATUS_UNAVAILABLE,
        )
        self.assertEqual(
            outcome.result["history_failure_stage"],
            SMARTESS_HISTORY_KEYS_ACTION,
        )
        self.assertEqual(
            outcome.result["history_failure_code"],
            SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
        )
        self.assertEqual(
            outcome.metadata_evidence["history_diagnostics"]["failure_code"],
            SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
        )

    def test_semantics_are_hint_only_and_bounded_to_known_sections(self) -> None:
        report = build_smartess_semantic_report(_bundle())
        record = report.to_record()
        self.assertEqual(record["authority"], "semantic_hint_only")
        self.assertIs(record["local_mapping_proven"], False)
        self.assertEqual(record["recognized_count"], 1)
        self.assertEqual(record["control_metadata_count"], 1)
        self.assertEqual(len(record["observations"]), 2)
        self.assertNotIn("register_address", str(record))


if __name__ == "__main__":
    unittest.main()
