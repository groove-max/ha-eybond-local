from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_local_coverage import (  # noqa: E402
    CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
    CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
    CLOUD_LOCAL_STATUS_NOT_OBSERVED,
    CLOUD_LOCAL_STATUS_VALUE_UNKNOWN,
    CloudLocalCoverageItem,
    CloudLocalCoverageReport,
    build_cloud_local_coverage_report,
)
from custom_components.eybond_local.support.cloud_semantic_evidence import (  # noqa: E402
    CLOUD_FIELD_KIND_READING,
    CLOUD_FIELD_KIND_SETTING,
    CloudSemanticEvidenceReport,
    classify_cloud_semantic_observation,
)
from custom_components.eybond_local.telemetry import (  # noqa: E402
    TelemetryFreshness,
    TelemetryPoint,
    TypedTelemetryFrame,
)


def _observation(
    title: str,
    *,
    field_id: str,
    kind: str = CLOUD_FIELD_KIND_READING,
):
    return classify_cloud_semantic_observation(
        field_kind=kind,
        field_id=field_id,
        title=title,
        value="1",
        observed_unit="V" if "Voltage" in title else "",
        source_action="querySPDeviceLastData",
    )


class CloudLocalCoverageTests(unittest.TestCase):
    def test_presence_is_reported_without_values_or_mapping_authority(self) -> None:
        semantic_report = CloudSemanticEvidenceReport(
            provider_id="smartess",
            source_id="dessmonitor",
            observations=(
                _observation("PV Voltage", field_id="pv_now"),
                _observation("PV Voltage", field_id="pv_chart"),
                _observation("Battery Voltage", field_id="battery"),
                _observation("Output Active Power", field_id="power"),
                _observation(
                    "Charger Source Priority",
                    field_id="setting",
                    kind=CLOUD_FIELD_KIND_SETTING,
                ),
            ),
        )
        telemetry = TypedTelemetryFrame(
            driver_key="smg",
            points=(
                TelemetryPoint(
                    key="pv_voltage",
                    value=123.4,
                    freshness=TelemetryFreshness.FRESH,
                ),
                TelemetryPoint(
                    key="battery_voltage",
                    value=51.2,
                    freshness=TelemetryFreshness.CARRIED,
                ),
            ),
        )

        report = build_cloud_local_coverage_report(semantic_report, telemetry)

        self.assertEqual(report.available_count, 2)
        self.assertEqual(report.not_observed_count, 1)
        self.assertEqual(
            tuple(item.status for item in report.items),
            (
                CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
                CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
                CLOUD_LOCAL_STATUS_NOT_OBSERVED,
            ),
        )
        self.assertEqual(report.items[0].cloud_field_count, 2)
        record = report.to_record()
        self.assertEqual(record["authority"], "runtime_semantic_presence_only")
        self.assertIs(record["local_mapping_proven"], False)
        self.assertNotIn("123.4", str(record))
        self.assertNotIn("51.2", str(record))
        self.assertNotIn("register", str(record).casefold())

    def test_typed_unknown_is_not_counted_as_available(self) -> None:
        semantic_report = CloudSemanticEvidenceReport(
            provider_id="smartess",
            source_id="dessmonitor",
            observations=(_observation("PV Voltage", field_id="pv"),),
        )
        telemetry = TypedTelemetryFrame(
            driver_key="smg",
            points=(
                TelemetryPoint(
                    key="pv_voltage",
                    value=None,
                    freshness=TelemetryFreshness.FRESH,
                ),
            ),
        )

        report = build_cloud_local_coverage_report(semantic_report, telemetry)

        self.assertEqual(report.available_count, 0)
        self.assertEqual(report.unknown_value_count, 1)
        self.assertEqual(report.items[0].status, CLOUD_LOCAL_STATUS_VALUE_UNKNOWN)

    def test_report_roundtrip_and_forged_authority_fail_closed(self) -> None:
        report = CloudLocalCoverageReport(
            driver_key="pi30",
            items=(
                CloudLocalCoverageItem(
                    semantic_key="pv_voltage",
                    cloud_field_count=1,
                    status=CLOUD_LOCAL_STATUS_NOT_OBSERVED,
                ),
            ),
        )
        record = report.to_record()
        parsed = CloudLocalCoverageReport.from_record(
            json.loads(json.dumps(record))
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.to_record(), record)

        for key, value in (
            ("schema_version", True),
            ("authority", "local_register_mapping"),
            ("local_mapping_proven", True),
            ("available_count", 99),
        ):
            malformed = dict(record)
            malformed[key] = value
            with self.subTest(key=key):
                self.assertIsNone(CloudLocalCoverageReport.from_record(malformed))

    def test_direct_constructors_reject_ducks_and_impossible_shapes(self) -> None:
        invalid_items = (
            {"semantic_key": " pv_voltage"},
            {"cloud_field_count": True},
            {"status": "available_fresh"},
            {"status": CLOUD_LOCAL_STATUS_AVAILABLE_FRESH, "local_freshness": "carried"},
            {"status": CLOUD_LOCAL_STATUS_VALUE_UNKNOWN, "local_value_kind": "number"},
        )
        defaults = {
            "semantic_key": "pv_voltage",
            "cloud_field_count": 1,
            "status": CLOUD_LOCAL_STATUS_NOT_OBSERVED,
            "local_freshness": "",
            "local_origin": "",
            "local_value_kind": "",
        }
        for override in invalid_items:
            with self.subTest(override=override):
                with self.assertRaises((TypeError, ValueError)):
                    CloudLocalCoverageItem(**(defaults | override))

        with self.assertRaises(TypeError):
            CloudLocalCoverageReport(
                driver_key="pi30",
                items=(SimpleNamespace(semantic_key="pv_voltage"),),  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            build_cloud_local_coverage_report(  # type: ignore[arg-type]
                SimpleNamespace(), TypedTelemetryFrame.empty()
            )
        with self.assertRaises(TypeError):
            build_cloud_local_coverage_report(  # type: ignore[arg-type]
                CloudSemanticEvidenceReport(
                    provider_id="smartess",
                    source_id="dessmonitor",
                    observations=(),
                ),
                SimpleNamespace(),
            )


if __name__ == "__main__":
    unittest.main()
