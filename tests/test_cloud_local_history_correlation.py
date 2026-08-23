from __future__ import annotations

import ast
import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_history import (  # noqa: E402
    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
)
from custom_components.eybond_local.drivers.local_register_evidence import (  # noqa: E402
    LocalRegisterBlockObservation,
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
)
from custom_components.eybond_local.drivers.local_register_series import (  # noqa: E402
    LocalRegisterSnapshotSeries,
)
from custom_components.eybond_local.metadata.register_schema_models import (  # noqa: E402
    RegisterSchemaMetadata,
)
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    load_register_schema,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.models import (  # noqa: E402
    MeasurementDescription,
    ProbeTarget,
    RegisterValueSpec,
)
from custom_components.eybond_local.support.cloud_local_history_correlation import (  # noqa: E402
    CLOUD_LOCAL_HISTORY_CORRELATION_AUTHORITY,
    CLOUD_LOCAL_HISTORY_REVIEW_AUTHORITY,
    CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES,
    CLOUD_LOCAL_HISTORY_REVIEW_STATUS_NO_ELIGIBLE,
    CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS,
    CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES,
    CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION,
    CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE,
    CLOUD_LOCAL_HISTORY_STATUS_UNIQUE,
    CloudLocalHistoryCorrelationReport,
    CloudLocalHistoryReview,
    build_cloud_local_history_correlation_report,
    build_cloud_local_history_review,
)
from custom_components.eybond_local.support.cloud_history_evidence import (  # noqa: E402
    CloudHistoryCollection,
    CloudHistoryIdentity,
    CloudHistoryPoint,
    CloudHistorySeries,
)
from custom_components.eybond_local.support.cloud_semantic_evidence import (  # noqa: E402
    CLOUD_FIELD_KIND_CHART,
    CLOUD_FIELD_KIND_KEY_PARAMETER,
)
from custom_components.eybond_local.support.cloud_local_history_draft import (  # noqa: E402
    CLOUD_LOCAL_READ_DRAFT_AUTHORITY,
    CloudLocalReadDraftItem,
    CloudLocalReadDraftPlan,
    build_cloud_local_read_draft_plan,
)
from custom_components.eybond_local.support.cloud_local_history_draft_writer import (  # noqa: E402
    CLOUD_LOCAL_READ_DRAFT_ARTIFACT_AUTHORITY,
    generate_inactive_cloud_local_read_schema_draft,
)
from custom_components.eybond_local.support.cloud_local_history_representability import (  # noqa: E402
    CLOUD_LOCAL_HISTORY_REPRESENTABILITY_AUTHORITY,
    REPRESENTABILITY_STATUS_ALREADY_AVAILABLE,
    REPRESENTABILITY_STATUS_AMBIGUOUS,
    REPRESENTABILITY_STATUS_DRIVER_MISMATCH,
    REPRESENTABILITY_STATUS_REGISTER_CONFLICT,
    REPRESENTABILITY_STATUS_REPRESENTABLE,
    REPRESENTABILITY_STATUS_ROUTE_MISMATCH,
    CloudLocalHistoryRepresentabilityReview,
    LocalRegisterOverlayContext,
    SchemaRegisterLocation,
    build_cloud_local_history_representability_review,
    build_local_register_overlay_context,
)


FULL_PN = "E50000200000000001"
SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "support"
    / "cloud_local_history_correlation.py"
)
REPRESENTABILITY_SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "support"
    / "cloud_local_history_representability.py"
)


def _identity(*, pn: str = FULL_PN) -> CloudHistoryIdentity:
    return CloudHistoryIdentity(
        pn=pn,
        sn="92632511100118",
        devcode=2376,
        devaddr=1,
    )


def _cloud_history(
    values: tuple[str, ...] = ("230.0", "231.0", "232.0", "233.0", "234.0"),
    *,
    title: str = "PV Voltage",
    series_key: str = "pv_voltage",
    unit: str = "V",
    pn: str = FULL_PN,
    source_id: str = "dessmonitor",
    source_action: str = DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    field_kind: str = CLOUD_FIELD_KIND_CHART,
) -> CloudHistorySeries:
    points = tuple(
        CloudHistoryPoint(
            device_local_timestamp=f"2026-08-22 10:{index * 5:02d}:00",
            utc_timestamp=f"2026-08-22T10:{index * 5:02d}:00+00:00",
            value=value,
        )
        for index, value in enumerate(values)
    )
    return CloudHistorySeries(
        provider_id="smartess",
        source_id=source_id,
        field_kind=field_kind,
        identity=_identity(pn=pn),
        source_action=source_action,
        series_key=series_key,
        title=title,
        unit=unit,
        requested_date="2026-08-22",
        precision_minutes=5,
        timezone_offset_seconds=0,
        points=points,
    )


def _local_series(
    primary: tuple[int, ...] = (2300, 2310, 2320, 2330, 2340),
    *,
    secondary: tuple[int, ...] = (17, 19, 23, 29, 31),
    driver_key: str = "smg_modbus",
    start: int = 100,
    pn: str = FULL_PN,
) -> LocalRegisterSnapshotSeries:
    snapshots: list[LocalRegisterSnapshot] = []
    plan = LocalRegisterReadPlan(
        devcode=2376,
        collector_addr=1,
        device_addr=1,
        function=3,
        start=start,
        count=2,
    )
    for index, (first, second) in enumerate(zip(primary, secondary, strict=True)):
        minute = index * 5
        snapshots.append(
            LocalRegisterSnapshot(
                collector_pn=pn,
                driver_key=driver_key,
                started_at=f"2026-08-22T10:{minute:02d}:05+00:00",
                completed_at=f"2026-08-22T10:{minute:02d}:15+00:00",
                planned_block_count=1,
                failed_block_count=0,
                blocks=(
                    LocalRegisterBlockObservation(
                        plan=plan,
                        observed_at=(
                            f"2026-08-22T10:{minute:02d}:10+00:00"
                        ),
                        values=(first, second),
                    ),
                ),
            )
        )
    return LocalRegisterSnapshotSeries(
        collector_pn=pn,
        driver_key=driver_key,
        sample_interval_seconds=300,
        snapshots=tuple(snapshots),
    )


def _history_collection(
    *series: CloudHistorySeries,
) -> CloudHistoryCollection:
    if not series:
        series = (_cloud_history(),)
    first = series[0]
    return CloudHistoryCollection(
        provider_id=first.provider_id,
        source_id=first.source_id,
        identity=first.identity,
        requested_date=first.requested_date,
        timezone_offset_seconds=first.timezone_offset_seconds,
        attempted_series_count=len(series),
        failed_series_count=0,
        budget_exhausted=False,
        series=tuple(series),
    )


def _overlay_context(
    *,
    pn: str = FULL_PN,
    driver_key: str = "smg_modbus",
    devcode: int = 2376,
    collector_addr: int = 1,
    device_addr: int = 1,
    claimed: tuple[SchemaRegisterLocation, ...] = (),
    semantics: tuple[str, ...] = (),
) -> LocalRegisterOverlayContext:
    return LocalRegisterOverlayContext(
        collector_pn=pn,
        driver_key=driver_key,
        register_schema_name="modbus_smg/models/smg_6200.json",
        devcode=devcode,
        collector_addr=collector_addr,
        device_addr=device_addr,
        claimed_locations=claimed,
        existing_semantic_keys=semantics,
    )


def _report(
    *,
    cloud_values: tuple[str, ...] = (
        "230.0",
        "231.0",
        "232.0",
        "233.0",
        "234.0",
    ),
    primary: tuple[int, ...] = (2300, 2310, 2320, 2330, 2340),
    secondary: tuple[int, ...] = (17, 19, 23, 29, 31),
) -> CloudLocalHistoryCorrelationReport:
    return build_cloud_local_history_correlation_report(
        _cloud_history(cloud_values),
        _local_series(primary, secondary=secondary),
        alignment_tolerance_seconds=30,
    )


class CloudLocalHistoryCorrelationTests(unittest.TestCase):
    def test_unique_exact_scaled_candidate_is_still_unproven(self) -> None:
        report = _report()

        self.assertEqual(report.status, CLOUD_LOCAL_HISTORY_STATUS_UNIQUE)
        self.assertEqual(report.candidate_count, 1)
        candidate = report.candidates[0]
        self.assertEqual(candidate.location.register, 100)
        self.assertEqual(candidate.divisor, 10)
        self.assertIs(candidate.signed, False)
        self.assertEqual(candidate.aligned_sample_count, 5)
        self.assertEqual(candidate.distinct_cloud_value_count, 5)
        record = report.to_record()
        self.assertEqual(
            record["authority"],
            CLOUD_LOCAL_HISTORY_CORRELATION_AUTHORITY,
        )
        self.assertEqual(record["local_mapping"], "candidate_not_proven")
        self.assertIs(record["local_mapping_proven"], False)

    def test_roundtrip_recomputes_the_exact_derived_verdict(self) -> None:
        original = _report()
        record = original.to_record()
        parsed = CloudLocalHistoryCorrelationReport.from_record(
            json.loads(json.dumps(record))
        )

        self.assertEqual(parsed, original)
        self.assertEqual(parsed.to_record(), record)

    def test_duplicate_register_series_stays_ambiguous(self) -> None:
        report = _report(
            secondary=(2300, 2310, 2320, 2330, 2340),
        )

        self.assertEqual(report.status, CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS)
        self.assertEqual(
            [item.location.register for item in report.candidates],
            [100, 101],
        )

    def test_insufficient_samples_and_static_values_never_mint_candidates(self) -> None:
        short = build_cloud_local_history_correlation_report(
            _cloud_history(("230.0", "231.0", "232.0")),
            _local_series(
                (2300, 2310, 2320),
                secondary=(17, 19, 23),
            ),
            alignment_tolerance_seconds=30,
        )
        static = _report(
            cloud_values=("230.0",) * 5,
            primary=(2300,) * 5,
        )

        self.assertEqual(
            short.status,
            CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES,
        )
        self.assertEqual(short.candidates, ())
        self.assertEqual(
            static.status,
            CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION,
        )
        self.assertEqual(static.candidates, ())

    def test_changing_but_nonmatching_values_report_no_exact_candidate(self) -> None:
        report = _report(primary=(100, 200, 300, 400, 500))

        self.assertEqual(
            report.status,
            CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE,
        )
        self.assertEqual(report.candidates, ())

    def test_negative_values_require_observed_signed_encoding(self) -> None:
        report = _report(
            cloud_values=("-1", "-2", "-3", "-4", "-5"),
            primary=(65535, 65534, 65533, 65532, 65531),
        )

        self.assertEqual(report.status, CLOUD_LOCAL_HISTORY_STATUS_UNIQUE)
        self.assertEqual(report.candidates[0].divisor, 1)
        self.assertIs(report.candidates[0].signed, True)

    def test_smartess_kw_history_uses_explicit_power_scale_for_local_watts(
        self,
    ) -> None:
        report = build_cloud_local_history_correlation_report(
            _cloud_history(
                values=("0.230", "0.231", "0.232", "0.233", "0.234"),
                title="PV Power",
                series_key="PV_OUTPUT_POWER",
                unit="kW",
                source_id="smartess",
                source_action="queryDeviceKeyParameterOneDay",
                field_kind=CLOUD_FIELD_KIND_KEY_PARAMETER,
            ),
            _local_series((230, 231, 232, 233, 234)),
            alignment_tolerance_seconds=30,
        )

        self.assertEqual(report.status, CLOUD_LOCAL_HISTORY_STATUS_UNIQUE)
        self.assertEqual(report.semantic.status, "unit_conflict")
        self.assertEqual(report.semantic.expected_unit, "W")
        self.assertEqual(report.candidates[0].divisor, 1000)

    def test_foreign_identity_or_unknown_semantic_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "identity_mismatch"):
            build_cloud_local_history_correlation_report(
                _cloud_history(pn="V0000000000001"),
                _local_series(),
                alignment_tolerance_seconds=30,
            )
        with self.assertRaisesRegex(ValueError, "semantic_untrusted"):
            build_cloud_local_history_correlation_report(
                _cloud_history(title="Quantum Flux Reading"),
                _local_series(),
                alignment_tolerance_seconds=30,
            )

    def test_forged_status_candidate_or_policy_record_is_rejected(self) -> None:
        original = _report()
        with self.assertRaisesRegex(ValueError, "derived_verdict_mismatch"):
            CloudLocalHistoryCorrelationReport(
                cloud_history=original.cloud_history,
                local_series=original.local_series,
                semantic=original.semantic,
                alignment_tolerance_seconds=30,
                status=CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE,
                candidates=(),
            )

        for key, value in (
            ("authority", object()),
            ("local_mapping", "proven"),
            ("local_mapping_proven", True),
            ("minimum_aligned_samples", 1),
            ("minimum_distinct_values", 1),
            ("candidate_count", 99),
        ):
            with self.subTest(key=key):
                record = original.to_record()
                record[key] = value
                self.assertIsNone(
                    CloudLocalHistoryCorrelationReport.from_record(record)
                )

        forged_semantic = original.to_record()
        forged_semantic["semantic"]["semantic_key"] = "battery_voltage"
        self.assertIsNone(
            CloudLocalHistoryCorrelationReport.from_record(forged_semantic)
        )


class CloudLocalHistoryReviewTests(unittest.TestCase):
    def test_review_derives_compact_revalidated_candidate_verdict(self) -> None:
        review = build_cloud_local_history_review(
            _history_collection(_cloud_history()),
            _local_series(),
        )

        self.assertEqual(
            review.status,
            CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES,
        )
        self.assertEqual(review.reviewed_series_count, 1)
        self.assertEqual(review.unique_candidate_count, 1)
        self.assertEqual(review.ambiguous_candidate_count, 0)
        self.assertEqual(review.skipped_series_count, 0)
        self.assertEqual(review.reports[0].alignment_tolerance_seconds, 150)

        record = review.to_record()
        self.assertEqual(record["authority"], CLOUD_LOCAL_HISTORY_REVIEW_AUTHORITY)
        self.assertIs(record["read_only"], True)
        self.assertIs(record["local_mapping_proven"], False)
        self.assertIs(record["activation_allowed"], False)
        self.assertNotIn("local_series", record["verdicts"][0])
        self.assertNotIn("cloud_history", record["verdicts"][0])
        parsed = CloudLocalHistoryReview.from_record(
            json.loads(json.dumps(record))
        )
        self.assertEqual(parsed, review)
        self.assertEqual(parsed.to_record(), record)

    def test_unknown_semantic_is_skipped_without_becoming_a_candidate(self) -> None:
        review = build_cloud_local_history_review(
            _history_collection(
                _cloud_history(title="Quantum Flux Reading"),
            ),
            _local_series(),
        )

        self.assertEqual(
            review.status,
            CLOUD_LOCAL_HISTORY_REVIEW_STATUS_NO_ELIGIBLE,
        )
        self.assertEqual(review.reviewed_series_count, 0)
        self.assertEqual(review.skipped_series_count, 1)
        self.assertEqual(review.reports, ())

    def test_foreign_identity_and_forged_verdict_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "identity_mismatch"):
            build_cloud_local_history_review(
                _history_collection(
                    _cloud_history(pn="V0000000000001"),
                ),
                _local_series(),
            )

        review = build_cloud_local_history_review(
            _history_collection(_cloud_history()),
            _local_series(),
        )
        with self.assertRaisesRegex(ValueError, "verdict_mismatch"):
            CloudLocalHistoryReview(
                history_collection=review.history_collection,
                local_series=review.local_series,
                reports=(),
            )
        for mutate in (
            lambda record: record.update({"authority": "mapping_authority"}),
            lambda record: record.update({"activation_allowed": True}),
            lambda record: record["verdicts"][0].update(
                {"status": CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE}
            ),
            lambda record: record.update({"unique_candidate_count": 99}),
        ):
            with self.subTest(mutate=mutate):
                record = review.to_record()
                mutate(record)
                self.assertIsNone(CloudLocalHistoryReview.from_record(record))


class CloudLocalHistoryRepresentabilityTests(unittest.TestCase):
    def _review(
        self,
        *,
        secondary: tuple[int, ...] = (17, 19, 23, 29, 31),
    ) -> CloudLocalHistoryReview:
        return build_cloud_local_history_review(
            _history_collection(_cloud_history()),
            _local_series(secondary=secondary),
        )

    def test_exact_full_route_is_review_only_and_roundtrips(self) -> None:
        review = self._review()
        result = build_cloud_local_history_representability_review(
            review,
            _overlay_context(),
        )

        self.assertEqual(
            result.decisions[0].status,
            REPRESENTABILITY_STATUS_REPRESENTABLE,
        )
        self.assertEqual(result.representable_count, 1)
        record = result.to_record()
        self.assertEqual(
            record["authority"],
            CLOUD_LOCAL_HISTORY_REPRESENTABILITY_AUTHORITY,
        )
        self.assertIs(record["draft_generation_allowed"], False)
        self.assertIs(record["activation_allowed"], False)
        parsed = CloudLocalHistoryRepresentabilityReview.from_record(
            json.loads(json.dumps(record)),
            review=review,
        )
        self.assertEqual(parsed, result)
        self.assertEqual(parsed.to_record(), record)

    def test_route_driver_schema_and_ambiguity_fail_closed_separately(self) -> None:
        review = self._review()
        cases = (
            (
                _overlay_context(device_addr=2),
                REPRESENTABILITY_STATUS_ROUTE_MISMATCH,
            ),
            (
                _overlay_context(driver_key="other_driver"),
                REPRESENTABILITY_STATUS_DRIVER_MISMATCH,
            ),
            (
                _overlay_context(semantics=("pv_voltage",)),
                REPRESENTABILITY_STATUS_ALREADY_AVAILABLE,
            ),
            (
                _overlay_context(
                    claimed=(SchemaRegisterLocation(function=3, register=100),)
                ),
                REPRESENTABILITY_STATUS_REGISTER_CONFLICT,
            ),
        )
        for context, expected in cases:
            with self.subTest(expected=expected):
                result = build_cloud_local_history_representability_review(
                    review,
                    context,
                )
                self.assertEqual(result.decisions[0].status, expected)

        ambiguous = build_cloud_local_history_representability_review(
            self._review(secondary=(2300, 2310, 2320, 2330, 2340)),
            _overlay_context(),
        )
        self.assertEqual(
            ambiguous.decisions[0].status,
            REPRESENTABILITY_STATUS_AMBIGUOUS,
        )

    def test_foreign_identity_or_forged_verdict_is_rejected(self) -> None:
        review = self._review()
        with self.assertRaisesRegex(ValueError, "identity_mismatch"):
            build_cloud_local_history_representability_review(
                review,
                _overlay_context(pn="V0000000000001"),
            )

        result = build_cloud_local_history_representability_review(
            review,
            _overlay_context(),
        )
        record = result.to_record()
        record["decisions"][0]["status"] = REPRESENTABILITY_STATUS_ROUTE_MISMATCH
        self.assertIsNone(
            CloudLocalHistoryRepresentabilityReview.from_record(
                record,
                review=review,
            )
        )

    def test_context_builder_preserves_schema_claims_and_exact_types(self) -> None:
        schema = RegisterSchemaMetadata(
            key="test",
            title="Test",
            driver_key="smg_modbus",
            protocol_family="modbus",
            source_name="modbus_smg/test.json",
            source_path="/tmp/test.json",
            source_scope="builtin",
            blocks=(),
            spec_sets={
                "live": (
                    RegisterValueSpec(
                        key="existing",
                        register=100,
                        word_count=2,
                        function=4,
                    ),
                )
            },
            enum_tables={},
            bit_labels={},
            scalar_registers={},
            measurement_descriptions=(
                MeasurementDescription(
                    key="existing",
                    name="PV Voltage",
                    translation_key="pv_voltage",
                ),
            ),
            binary_sensor_descriptions=(),
        )
        context = build_local_register_overlay_context(
            collector_pn=FULL_PN,
            driver_key="smg_modbus",
            probe_target=ProbeTarget(
                devcode=2376,
                collector_addr=1,
                device_addr=1,
            ),
            register_schema_name="modbus_smg/test.json",
            register_schema=schema,
        )

        self.assertEqual(
            context.claimed_locations,
            (
                SchemaRegisterLocation(function=4, register=100),
                SchemaRegisterLocation(function=4, register=101),
            ),
        )
        self.assertEqual(context.existing_semantic_keys, ("existing", "pv_voltage"))
        self.assertEqual(
            LocalRegisterOverlayContext.from_record(context.to_record()),
            context,
        )
        with self.assertRaisesRegex(TypeError, "probe_target_invalid"):
            build_local_register_overlay_context(
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                probe_target=object(),
                register_schema_name="modbus_smg/test.json",
                register_schema=schema,
            )

    def test_representability_module_has_no_draft_or_activation_path(self) -> None:
        source = REPRESENTABILITY_SOURCE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")
        for forbidden in ("runtime", "flows", "overlay_generator"):
            self.assertFalse(any(forbidden in item for item in imports))
        for forbidden in (
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "write_text",
            "async_update_entry",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn('"draft_generation_allowed": False', source)
        self.assertIn('"activation_allowed": False', source)


class CloudLocalReadDraftPlanTests(unittest.TestCase):
    def _representability(
        self,
        *,
        collection: CloudHistoryCollection | None = None,
        local_series: LocalRegisterSnapshotSeries | None = None,
        context: LocalRegisterOverlayContext | None = None,
    ) -> CloudLocalHistoryRepresentabilityReview:
        exact_local_series = local_series or _local_series()
        review = build_cloud_local_history_review(
            collection or _history_collection(),
            exact_local_series,
        )
        return build_cloud_local_history_representability_review(
            review,
            context or _overlay_context(),
        )

    def test_plan_preserves_exact_route_transform_and_stays_inactive(self) -> None:
        plan = build_cloud_local_read_draft_plan(self._representability())

        self.assertEqual(plan.item_count, 1)
        item = plan.items[0]
        self.assertEqual(item.series_key, "pv_voltage")
        self.assertEqual(item.semantic_key, "pv_voltage")
        self.assertEqual(item.location.devcode, 2376)
        self.assertEqual(item.location.collector_addr, 1)
        self.assertEqual(item.location.device_addr, 1)
        self.assertEqual(item.location.function, 3)
        self.assertEqual(item.location.register, 100)
        self.assertEqual(item.candidate.divisor, 10)
        self.assertIs(item.candidate.signed, False)

        record = plan.to_record()
        self.assertEqual(record["authority"], CLOUD_LOCAL_READ_DRAFT_AUTHORITY)
        self.assertIs(record["read_only"], True)
        self.assertEqual(record["local_mapping"], "candidate_not_proven")
        self.assertIs(record["local_mapping_proven"], False)
        self.assertIs(record["draft_generation_allowed"], True)
        self.assertIs(record["activation_allowed"], False)

    def test_roundtrip_recomputes_plan_and_rejects_tampering(self) -> None:
        plan = build_cloud_local_read_draft_plan(self._representability())
        record = json.loads(json.dumps(plan.to_record()))

        representability = plan.representability
        parsed = CloudLocalReadDraftPlan.from_record(
            record,
            representability=representability,
        )
        self.assertEqual(parsed, plan)
        self.assertEqual(parsed.to_record(), record)

        for mutate in (
            lambda value: value["items"][0]["candidate"]["location"].__setitem__(
                "register", 101
            ),
            lambda value: value.__setitem__("activation_allowed", True),
            lambda value: value.__setitem__("local_mapping_proven", True),
            lambda value: value.__setitem__("item_count", 2),
            lambda value: value.__setitem__("extra", "forged"),
        ):
            tampered = json.loads(json.dumps(record))
            mutate(tampered)
            self.assertIsNone(
                CloudLocalReadDraftPlan.from_record(
                    tampered,
                    representability=representability,
                )
            )

    def test_direct_constructor_rejects_duck_and_forged_items(self) -> None:
        representability = self._representability()
        valid = build_cloud_local_read_draft_plan(representability)
        item = valid.items[0]

        with self.assertRaises(TypeError):
            CloudLocalReadDraftPlan(
                representability=representability,
                items=(object(),),
            )
        with self.assertRaises(ValueError):
            CloudLocalReadDraftPlan(
                representability=representability,
                items=(),
            )
        with self.assertRaises(ValueError):
            CloudLocalReadDraftItem(
                source_action=item.source_action,
                series_key="foreign_series",
                semantic=item.semantic,
                candidate=item.candidate,
            )

    def test_nonrepresentable_candidate_never_enters_plan(self) -> None:
        representability = self._representability(
            context=_overlay_context(
                claimed=(SchemaRegisterLocation(function=3, register=100),),
            )
        )

        plan = build_cloud_local_read_draft_plan(representability)

        self.assertEqual(plan.items, ())
        self.assertIs(plan.draft_generation_allowed, False)
        self.assertIs(plan.to_record()["activation_allowed"], False)

    def test_cross_series_location_collision_is_reviewed_but_not_draftable(
        self,
    ) -> None:
        pv = _cloud_history(
            title="PV Voltage",
            series_key="pv_voltage",
        )
        battery = _cloud_history(
            title="Battery Voltage",
            series_key="battery_voltage",
        )
        representability = self._representability(
            collection=_history_collection(pv, battery),
        )

        self.assertEqual(representability.representable_count, 2)
        plan = build_cloud_local_read_draft_plan(representability)
        self.assertEqual(plan.items, ())
        self.assertIs(plan.draft_generation_allowed, False)


class CloudLocalReadDraftWriterTests(unittest.TestCase):
    SOURCE_SCHEMA = "modbus_smg/models/smg_6200.json"

    def _plan(self, *, exact_context: bool = True) -> CloudLocalReadDraftPlan:
        cloud = _cloud_history(
            values=("10.0", "11.0", "12.0", "13.0", "14.0"),
            title="AC charging current",
            series_key="ac_charging_current",
            unit="A",
        )
        local = _local_series(
            primary=(100, 110, 120, 130, 140),
            driver_key="modbus_smg",
            start=65000,
        )
        review = build_cloud_local_history_review(
            _history_collection(cloud),
            local,
        )
        if exact_context:
            schema = load_register_schema(self.SOURCE_SCHEMA)
            context = build_local_register_overlay_context(
                collector_pn=FULL_PN,
                driver_key="modbus_smg",
                probe_target=ProbeTarget(
                    devcode=2376,
                    collector_addr=1,
                    device_addr=1,
                ),
                register_schema_name=self.SOURCE_SCHEMA,
                register_schema=schema,
            )
        else:
            context = _overlay_context(
                driver_key="modbus_smg",
                claimed=(),
                semantics=(),
            )
            context = LocalRegisterOverlayContext(
                collector_pn=context.collector_pn,
                driver_key=context.driver_key,
                register_schema_name=self.SOURCE_SCHEMA,
                devcode=context.devcode,
                collector_addr=context.collector_addr,
                device_addr=context.device_addr,
                claimed_locations=context.claimed_locations,
                existing_semantic_keys=context.existing_semantic_keys,
            )
        representability = build_cloud_local_history_representability_review(
            review,
            context,
        )
        return build_cloud_local_read_draft_plan(representability)

    def test_writer_creates_deterministic_inactive_valid_schema(self) -> None:
        plan = self._plan()
        self.assertEqual(plan.item_count, 1)

        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            artifact = generate_inactive_cloud_local_read_schema_draft(
                config_dir=config_dir,
                source_schema_name=self.SOURCE_SCHEMA,
                plan=plan,
            )
            repeated = generate_inactive_cloud_local_read_schema_draft(
                config_dir=config_dir,
                source_schema_name=self.SOURCE_SCHEMA,
                plan=plan,
            )
            raw = json.loads(artifact.schema_path.read_text(encoding="utf-8"))

            self.assertEqual(repeated.schema_path, artifact.schema_path)
            self.assertNotEqual(artifact.schema_name, self.SOURCE_SCHEMA)
            self.assertIn("learned/dessmonitor_review/", artifact.schema_name)
            self.assertEqual(
                raw["dessmonitor_read_draft"]["authority"],
                CLOUD_LOCAL_READ_DRAFT_ARTIFACT_AUTHORITY,
            )
            self.assertIs(
                raw["dessmonitor_read_draft"]["local_mapping_proven"],
                False,
            )
            self.assertIs(
                raw["dessmonitor_read_draft"]["activation_allowed"],
                False,
            )
            spec = raw["spec_sets"]["aux_config"][0]
            self.assertEqual(spec["function"], 3)
            self.assertEqual(spec["register"], 65000)
            self.assertEqual(spec["divisor"], 10)
            self.assertEqual(spec["decimals"], 1)
            measurement = raw["measurement_descriptions"][0]
            self.assertEqual(measurement["translation_key"], "ac_charging_current")
            self.assertIs(measurement["enabled_default"], False)
            self.assertTrue(measurement["key"].startswith("learned_read_"))

            external_root = (
                config_dir / "eybond_local" / "register_schemas"
            )
            set_external_register_schema_roots((external_root,))
            try:
                loaded = load_register_schema(artifact.schema_name)
            finally:
                set_external_register_schema_roots(())
            learned_spec = loaded.spec_set("aux_config")[-1]
            self.assertEqual(learned_spec.function, 3)
            self.assertEqual(learned_spec.register, 65000)
            self.assertEqual(learned_spec.divisor, 10)

    def test_writer_rebuilds_schema_context_before_any_write(self) -> None:
        forged = self._plan(exact_context=False)

        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            with self.assertRaisesRegex(ValueError, "schema_context_changed"):
                generate_inactive_cloud_local_read_schema_draft(
                    config_dir=config_dir,
                    source_schema_name=self.SOURCE_SCHEMA,
                    plan=forged,
                )
            self.assertFalse((config_dir / "eybond_local").exists())

    def test_existing_different_artifact_is_never_overwritten(self) -> None:
        plan = self._plan()

        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            artifact = generate_inactive_cloud_local_read_schema_draft(
                config_dir=config_dir,
                source_schema_name=self.SOURCE_SCHEMA,
                plan=plan,
            )
            artifact.schema_path.write_text("{}\n", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                generate_inactive_cloud_local_read_schema_draft(
                    config_dir=config_dir,
                    source_schema_name=self.SOURCE_SCHEMA,
                    plan=plan,
                )


class CloudLocalHistoryCorrelationArchitectureTests(unittest.TestCase):
    def test_review_correlator_has_no_runtime_or_activation_dependency(self) -> None:
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
            "read_learning_binder",
            "overlay_generator",
            "device_scoped_overlay",
        ):
            self.assertFalse(any(forbidden in item for item in imports))
        for forbidden in (
            "async_activate_device_scoped_overlay",
            "generate_shadow_learning_overlay_drafts",
            "write_capability",
            "read_bindings",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn("review_candidate_only", source)
        self.assertIn('"local_mapping_proven": False', source)


if __name__ == "__main__":
    unittest.main()
