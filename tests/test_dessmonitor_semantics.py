from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorControlField,
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    DessMonitorTelemetryField,
)
from custom_components.eybond_local.support.cloud_semantic_evidence import (  # noqa: E402
    CLOUD_FIELD_KIND_CHART,
    CLOUD_FIELD_KIND_KEY_PARAMETER,
    CLOUD_FIELD_KIND_READING,
    CLOUD_FIELD_KIND_SETTING,
    CLOUD_SEMANTIC_STATUS_RECOGNIZED,
    CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
    CLOUD_SEMANTIC_STATUS_UNKNOWN,
)
from custom_components.eybond_local.support.dessmonitor_semantics import (  # noqa: E402
    build_dessmonitor_semantic_report,
)


class DessMonitorSemanticAdapterTests(unittest.TestCase):
    def test_all_provider_groups_are_adapted_without_register_authority(self) -> None:
        bundle = DessMonitorEvidenceBundle(
            identity=DessMonitorDeviceIdentity(
                pn="E50000200000000001",
                sn="92632511100118",
                devcode=2376,
                devaddr=1,
            ),
            telemetry_fields=(
                DessMonitorTelemetryField(
                    field_id="pv_voltage",
                    title="PV Voltage",
                    value="123.4",
                    unit="V",
                    section="pv_",
                    source_action="querySPDeviceLastData",
                ),
            ),
            chart_fields=(
                DessMonitorTelemetryField(
                    field_id="grid_voltage",
                    title="Grid Voltage",
                    value="",
                    unit="A",
                    section="",
                    source_action="queryDeviceChartField",
                ),
            ),
            key_parameters=(
                DessMonitorTelemetryField(
                    field_id="vendor_datum",
                    title="Vendor Datum",
                    value="on",
                    unit="",
                    section="",
                    source_action="querySPKeyParameters",
                ),
            ),
            control_fields=(
                DessMonitorControlField(
                    field_id="charger_priority",
                    title="Charger Source Priority",
                    current_value="Solar first",
                ),
            ),
        )

        report = build_dessmonitor_semantic_report(bundle)

        self.assertEqual(report.provider_id, "smartess")
        self.assertEqual(report.source_id, "dessmonitor")
        self.assertEqual(
            tuple(item.field_kind for item in report.observations),
            (
                CLOUD_FIELD_KIND_READING,
                CLOUD_FIELD_KIND_CHART,
                CLOUD_FIELD_KIND_KEY_PARAMETER,
                CLOUD_FIELD_KIND_SETTING,
            ),
        )
        self.assertEqual(
            tuple(item.status for item in report.observations),
            (
                CLOUD_SEMANTIC_STATUS_RECOGNIZED,
                CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
                CLOUD_SEMANTIC_STATUS_UNKNOWN,
                CLOUD_SEMANTIC_STATUS_RECOGNIZED,
            ),
        )
        self.assertEqual(report.control_metadata_count, 1)
        rendered = report.to_record()
        self.assertIs(rendered["local_mapping_proven"], False)
        self.assertNotIn("register", str(rendered).casefold())

    def test_adapter_rejects_duck_bundle(self) -> None:
        with self.assertRaises(TypeError):
            build_dessmonitor_semantic_report(object())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
