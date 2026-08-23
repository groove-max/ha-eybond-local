from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_history_evidence import (  # noqa: E402
    CLOUD_HISTORY_STATUS_COMPLETE,
    CloudHistoryCollection,
    CloudHistoryIdentity,
    CloudHistoryPoint,
    CloudHistorySeries,
)
from custom_components.eybond_local.support.cloud_semantic_evidence import (  # noqa: E402
    CLOUD_FIELD_KIND_KEY_PARAMETER,
)


def _identity() -> CloudHistoryIdentity:
    return CloudHistoryIdentity(
        pn="E50000200000000001",
        sn="90000000000001",
        devcode=2376,
        devaddr=1,
    )


def _series() -> CloudHistorySeries:
    return CloudHistorySeries(
        provider_id="smartess",
        source_id="smartess",
        source_action="queryDeviceKeyParameterOneDay",
        field_kind=CLOUD_FIELD_KIND_KEY_PARAMETER,
        identity=_identity(),
        series_key="PV_OUTPUT_POWER",
        title="PV Power",
        unit="kW",
        requested_date="2026-08-23",
        precision_minutes=5,
        timezone_offset_seconds=7200,
        points=(
            CloudHistoryPoint(
                device_local_timestamp="2026-08-23 12:00:00",
                utc_timestamp="2026-08-23T10:00:00+00:00",
                value="1.25",
            ),
            CloudHistoryPoint(
                device_local_timestamp="2026-08-23 12:05:00",
                utc_timestamp="2026-08-23T10:05:00+00:00",
                value="1.50",
            ),
        ),
    )


def _collection() -> CloudHistoryCollection:
    return CloudHistoryCollection(
        provider_id="smartess",
        source_id="smartess",
        identity=_identity(),
        requested_date="2026-08-23",
        timezone_offset_seconds=7200,
        attempted_series_count=1,
        failed_series_count=0,
        budget_exhausted=False,
        series=(_series(),),
    )


class CloudHistoryEvidenceTests(unittest.TestCase):
    def test_json_roundtrip_is_byte_stable_and_observation_only(self) -> None:
        collection = _collection()
        record = collection.to_record()
        parsed = CloudHistoryCollection.from_record(
            json.loads(json.dumps(record))
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.to_record(), record)
        self.assertEqual(parsed.status, CLOUD_HISTORY_STATUS_COMPLETE)
        self.assertIs(record["read_only"], True)
        self.assertIs(record["local_mapping_proven"], False)
        self.assertIs(record["activation_allowed"], False)
        self.assertEqual(
            record["series"][0]["field_kind"],
            CLOUD_FIELD_KIND_KEY_PARAMETER,
        )
        self.assertNotIn("register", str(record).casefold())

    def test_direct_constructors_reject_ducks_and_unknown_field_kind(self) -> None:
        values = dict(
            provider_id="smartess",
            source_id="smartess",
            source_action="queryDeviceKeyParameterOneDay",
            field_kind=CLOUD_FIELD_KIND_KEY_PARAMETER,
            identity=_identity(),
            series_key="PV_OUTPUT_POWER",
            title="PV Power",
            unit="kW",
            requested_date="2026-08-23",
            precision_minutes=5,
            timezone_offset_seconds=7200,
            points=(),
        )
        with self.assertRaises(ValueError):
            CloudHistorySeries(**{**values, "field_kind": "telemetry"})
        with self.assertRaises(TypeError):
            CloudHistorySeries(**{**values, "identity": object()})
        with self.assertRaises(TypeError):
            CloudHistoryCollection(
                provider_id="smartess",
                source_id="smartess",
                identity=_identity(),
                requested_date="2026-08-23",
                timezone_offset_seconds=7200,
                attempted_series_count=1,
                failed_series_count=0,
                budget_exhausted=False,
                series=[_series()],
            )

    def test_parser_rejects_tampered_authority_counts_and_field_kind(self) -> None:
        record = _collection().to_record()
        for mutate in (
            lambda value: value.update(authority="mapping_proven"),
            lambda value: value.update(point_count=999),
            lambda value: value["series"][0].update(field_kind="reading"),
            lambda value: value["series"][0].update(local_mapping_proven=True),
        ):
            with self.subTest(mutate=mutate):
                candidate = json.loads(json.dumps(record))
                mutate(candidate)
                self.assertIsNone(CloudHistoryCollection.from_record(candidate))

    def test_collection_rejects_cross_provider_or_foreign_identity_series(self) -> None:
        series_values = {
            name: getattr(_series(), name)
            for name in _series().__dataclass_fields__
        }
        for field, value in (
            ("source_id", "dessmonitor"),
            (
                "identity",
                CloudHistoryIdentity(
                    pn="FOREIGN",
                    sn="90000000000001",
                    devcode=2376,
                    devaddr=1,
                ),
            ),
        ):
            with self.subTest(field=field):
                foreign = CloudHistorySeries(
                    **{**series_values, field: value}
                )
                with self.assertRaises(ValueError):
                    CloudHistoryCollection(
                        provider_id="smartess",
                        source_id="smartess",
                        identity=_identity(),
                        requested_date="2026-08-23",
                        timezone_offset_seconds=7200,
                        attempted_series_count=1,
                        failed_series_count=0,
                        budget_exhausted=False,
                        series=(foreign,),
                    )


if __name__ == "__main__":
    unittest.main()
