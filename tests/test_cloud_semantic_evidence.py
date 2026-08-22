from __future__ import annotations

import json
from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_semantic_evidence import (  # noqa: E402
    CLOUD_FIELD_KIND_READING,
    CLOUD_SEMANTIC_STATUS_RECOGNIZED,
    CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
    CLOUD_SEMANTIC_STATUS_UNKNOWN,
    CloudSemanticEvidenceReport,
    CloudSemanticObservation,
    classify_cloud_semantic_observation,
)


class CloudSemanticEvidenceTests(unittest.TestCase):
    def _classify(
        self,
        *,
        title: str = "PV Voltage",
        unit: str = "V",
    ) -> CloudSemanticObservation:
        return classify_cloud_semantic_observation(
            field_kind=CLOUD_FIELD_KIND_READING,
            field_id="sy_eybond_read_2",
            title=title,
            value="123.4",
            observed_unit=unit,
            source_action="querySPDeviceLastData",
        )

    def test_known_title_is_only_a_semantic_hint(self) -> None:
        observation = self._classify()

        self.assertEqual(observation.status, CLOUD_SEMANTIC_STATUS_RECOGNIZED)
        self.assertEqual(observation.semantic_key, "pv_voltage")
        self.assertEqual(observation.canonical_title, "PV Voltage")
        self.assertEqual(observation.expected_unit, "V")
        record = observation.to_record()
        self.assertEqual(record["local_mapping"], "unproven")
        for forbidden in (
            "register",
            "register_key",
            "driver_key",
            "writable",
            "enabled",
        ):
            self.assertNotIn(forbidden, record)

    def test_catalog_alias_and_equivalent_temperature_unit_are_recognized(self) -> None:
        observation = self._classify(
            title="INV Module Termperature",
            unit="℃",
        )

        self.assertEqual(observation.status, CLOUD_SEMANTIC_STATUS_RECOGNIZED)
        self.assertEqual(observation.semantic_key, "inv_module_temperature")
        self.assertEqual(observation.expected_unit, "°C")

    def test_known_title_with_conflicting_unit_is_not_presented_as_recognized(self) -> None:
        observation = self._classify(unit="A")

        self.assertEqual(observation.status, CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT)
        self.assertEqual(observation.semantic_key, "pv_voltage")
        self.assertEqual(observation.observed_unit, "A")
        self.assertEqual(observation.expected_unit, "V")

    def test_unknown_title_mints_no_semantic_key(self) -> None:
        observation = self._classify(title="Uncatalogued Cloud Datum", unit="x")

        self.assertEqual(observation.status, CLOUD_SEMANTIC_STATUS_UNKNOWN)
        self.assertEqual(observation.semantic_key, "")
        self.assertEqual(observation.canonical_title, "")
        self.assertEqual(observation.expected_unit, "")

    def test_report_is_json_stable_and_fail_closed_on_authority_forgery(self) -> None:
        report = CloudSemanticEvidenceReport(
            provider_id="smartess",
            source_id="dessmonitor",
            observations=(
                self._classify(),
                self._classify(unit="A"),
                self._classify(title="Unknown", unit=""),
            ),
        )
        record = report.to_record()
        parsed = CloudSemanticEvidenceReport.from_record(
            json.loads(json.dumps(record))
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.to_record(), record)
        self.assertEqual(parsed.recognized_count, 1)
        self.assertEqual(parsed.unit_conflict_count, 1)
        self.assertEqual(parsed.unknown_count, 1)
        for key, value in (
            ("schema_version", True),
            ("authority", "local_register_mapping"),
            ("local_mapping_proven", True),
            ("recognized_count", 99),
        ):
            malformed = dict(record)
            malformed[key] = value
            with self.subTest(key=key):
                self.assertIsNone(
                    CloudSemanticEvidenceReport.from_record(malformed)
                )
        with_extra = dict(record)
        with_extra["register"] = 404
        self.assertIsNone(CloudSemanticEvidenceReport.from_record(with_extra))

    def test_direct_constructors_reject_ducks_padded_tokens_and_false_conflicts(self) -> None:
        with self.assertRaises(TypeError):
            CloudSemanticEvidenceReport(
                provider_id="smartess",
                source_id="dessmonitor",
                observations=(types.SimpleNamespace(),),  # type: ignore[arg-type]
            )
        with self.assertRaises(ValueError):
            CloudSemanticEvidenceReport(
                provider_id=" smartess ",
                source_id="dessmonitor",
                observations=(),
            )
        with self.assertRaises(ValueError):
            CloudSemanticObservation(
                field_kind=CLOUD_FIELD_KIND_READING,
                field_id="id",
                title="PV Voltage",
                value="1",
                observed_unit="V",
                source_action="querySPDeviceLastData",
                status=CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
                semantic_key="pv_voltage",
                canonical_title="PV Voltage",
                semantic_kind="read",
                expected_unit="V",
            )
        with self.assertRaises(ValueError):
            CloudSemanticObservation(
                field_kind=CLOUD_FIELD_KIND_READING,
                field_id="id",
                title="PV Voltage",
                value="1",
                observed_unit="A",
                source_action="querySPDeviceLastData",
                status=CLOUD_SEMANTIC_STATUS_RECOGNIZED,
                semantic_key="pv_voltage",
                canonical_title="PV Voltage",
                semantic_kind="read",
                expected_unit="V",
            )

    def test_classification_rejects_non_string_provider_values_without_coercion(self) -> None:
        for field_name, value in (
            ("title", object()),
            ("field_id", 7),
            ("observed_unit", b"V"),
            ("value", None),
        ):
            kwargs = {
                "field_kind": CLOUD_FIELD_KIND_READING,
                "field_id": "id",
                "title": "PV Voltage",
                "value": "1",
                "observed_unit": "V",
                "source_action": "querySPDeviceLastData",
            }
            kwargs[field_name] = value
            with self.subTest(field_name=field_name):
                with self.assertRaises(TypeError):
                    classify_cloud_semantic_observation(**kwargs)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
