"""Pure presentation helpers for read-only cloud evidence review."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ...dessmonitor_collection import (
    DESSMONITOR_COLLECTION_STATUS_COMPLETE,
    DESSMONITOR_COLLECTION_STATUS_PARTIAL,
    DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
    DessMonitorHistoryCollection,
)
from ...drivers.local_register_series import LocalRegisterSnapshotSeries
from ...support.cloud_local_coverage import (
    CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
    CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
    CloudLocalCoverageReport,
)
from ...support.cloud_local_history_correlation import (
    CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES,
    CLOUD_LOCAL_HISTORY_STATUS_UNIQUE,
    CloudLocalHistoryReview,
    build_cloud_local_history_review,
)
from ...support.cloud_local_history_draft import (
    CloudLocalReadDraftPlan,
    build_cloud_local_read_draft_plan,
)
from ...support.cloud_local_history_representability import (
    CloudLocalHistoryRepresentabilityReview,
    LocalRegisterOverlayContext,
    REPRESENTABILITY_STATUS_ALREADY_AVAILABLE,
    REPRESENTABILITY_STATUS_DRIVER_MISMATCH,
    REPRESENTABILITY_STATUS_REGISTER_CONFLICT,
    REPRESENTABILITY_STATUS_REPRESENTABLE,
    REPRESENTABILITY_STATUS_ROUTE_MISMATCH,
    build_cloud_local_history_representability_review,
)
from ...support.cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_SETTING,
    CLOUD_SEMANTIC_STATUS_RECOGNIZED,
    CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
    CloudSemanticEvidenceReport,
)


Translation = Callable[..., str]


def cloud_metadata_review_fields(evidence: object) -> list[dict[str, str]]:
    """Return deduplicated, credential-free metadata for read-only review."""

    if not isinstance(evidence, dict):
        return []
    semantic_report = CloudSemanticEvidenceReport.from_record(
        evidence.get("semantic_report")
    )
    if semantic_report is not None:
        coverage_report = CloudLocalCoverageReport.from_record(
            evidence.get("local_coverage")
        )
        coverage_by_key = {
            item.semantic_key: item.status
            for item in (
                coverage_report.items if coverage_report is not None else ()
            )
        }
        output: list[dict[str, str]] = []
        seen: set[tuple[str, str, str]] = set()
        for observation in semantic_report.observations:
            key = (
                observation.field_id,
                observation.title.casefold(),
                observation.observed_unit,
            )
            if key in seen:
                continue
            seen.add(key)
            output.append(
                {
                    "field_id": observation.field_id,
                    "title": observation.title,
                    "unit": observation.observed_unit,
                    "value": observation.value,
                    "kind": observation.field_kind,
                    "semantic_status": observation.status,
                    "semantic_key": observation.semantic_key,
                    "canonical_title": observation.canonical_title,
                    "expected_unit": observation.expected_unit,
                    "local_status": coverage_by_key.get(
                        observation.semantic_key,
                        "",
                    ),
                }
            )
        return output

    output = []
    seen: set[tuple[str, str, str]] = set()
    for group, kind in (
        ("telemetry_fields", "reading"),
        ("chart_fields", "chart"),
        ("key_parameters", "key_parameter"),
        ("control_fields", "setting"),
    ):
        rows = evidence.get(group)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            field_id = str(row.get("field_id") or "").strip()
            title = str(row.get("title") or "").strip()
            unit = str(row.get("unit") or "").strip()
            value = str(
                row.get("value") or row.get("current_value") or ""
            ).strip()
            if not title:
                continue
            key = (field_id, title.casefold(), unit)
            if key in seen:
                continue
            seen.add(key)
            output.append(
                {
                    "field_id": field_id,
                    "title": title,
                    "unit": unit,
                    "value": value,
                    "kind": kind,
                    "semantic_status": "",
                    "semantic_key": "",
                    "canonical_title": "",
                    "expected_unit": "",
                    "local_status": "",
                }
            )
    return output


def cloud_metadata_semantic_candidate_count(
    fields: list[dict[str, str]],
) -> int:
    """Count recognized read hints without treating settings as sensors."""

    return sum(
        item.get("semantic_status") == CLOUD_SEMANTIC_STATUS_RECOGNIZED
        and item.get("kind") != CLOUD_FIELD_KIND_SETTING
        for item in fields
    )


def cloud_history_collection(evidence: object) -> DessMonitorHistoryCollection | None:
    """Return only an exact, internally consistent DESS history record."""

    if not isinstance(evidence, dict):
        return None
    return DessMonitorHistoryCollection.from_record(
        evidence.get("history_collection")
    )


def cloud_history_summary(
    collection: DessMonitorHistoryCollection | None,
    translate: Translation,
) -> str:
    """Render bounded history availability without exposing raw evidence."""

    if collection is None:
        return ""
    placeholders = {
        "count": str(collection.collected_series_count),
        "points": str(collection.point_count),
    }
    if collection.status == DESSMONITOR_COLLECTION_STATUS_COMPLETE:
        return translate(
            "common.dynamic.cloud_learning_history_complete",
            "Historical evidence includes {count} series ({points} points).",
            placeholders,
        )
    if collection.status == DESSMONITOR_COLLECTION_STATUS_PARTIAL:
        return translate(
            "common.dynamic.cloud_learning_history_partial",
            "Historical evidence includes {count} series ({points} points); "
            "some series were unavailable.",
            placeholders,
        )
    if collection.status == DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE:
        return translate(
            "common.dynamic.cloud_learning_history_time_unavailable",
            "Historical data was skipped because the device time zone could not "
            "be confirmed.",
        )
    return translate(
        "common.dynamic.cloud_learning_history_unavailable",
        "Historical data was unavailable for this check.",
    )


def metadata_with_cloud_local_history_review(
    evidence: dict[str, Any],
    local_series: object,
) -> dict[str, Any]:
    """Attach only an exact review aggregate; never preserve a stale record."""

    detached = dict(evidence)
    detached.pop("local_history_review", None)
    if type(local_series) is not LocalRegisterSnapshotSeries:
        return detached
    collection = cloud_history_collection(detached)
    if collection is None:
        return detached
    try:
        review = build_cloud_local_history_review(collection, local_series)
    except (TypeError, ValueError):
        return detached
    detached["local_history_review"] = review.to_record()
    return detached


def metadata_with_cloud_local_history_representability(
    evidence: dict[str, Any],
    context: object,
) -> dict[str, Any]:
    """Attach a fresh current-context verdict, never a stale compatibility claim."""

    detached = dict(evidence)
    detached.pop("local_history_representability", None)
    if type(context) is not LocalRegisterOverlayContext:
        return detached
    review = cloud_local_history_review(detached)
    if review is None:
        return detached
    try:
        representability = build_cloud_local_history_representability_review(
            review,
            context,
        )
    except (TypeError, ValueError):
        return detached
    detached["local_history_representability"] = representability.to_record()
    return detached


def metadata_with_cloud_local_history_draft_plan(
    evidence: dict[str, Any],
) -> dict[str, Any]:
    """Attach a recomputed inactive-draft plan or remove any stale claim."""

    detached = dict(evidence)
    detached.pop("local_history_draft_plan", None)
    representability = cloud_local_history_representability(detached)
    if representability is None:
        return detached
    try:
        plan = build_cloud_local_read_draft_plan(representability)
    except (TypeError, ValueError):
        return detached
    detached["local_history_draft_plan"] = plan.to_record()
    return detached


def cloud_local_history_review(
    evidence: object,
) -> CloudLocalHistoryReview | None:
    """Return only an exact, recomputed review-only correlation aggregate."""

    if not isinstance(evidence, dict):
        return None
    return CloudLocalHistoryReview.from_record(
        evidence.get("local_history_review")
    )


def cloud_local_history_representability(
    evidence: object,
) -> CloudLocalHistoryRepresentabilityReview | None:
    """Parse compatibility only against the exact recomputed review beside it."""

    review = cloud_local_history_review(evidence)
    if review is None or not isinstance(evidence, dict):
        return None
    return CloudLocalHistoryRepresentabilityReview.from_record(
        evidence.get("local_history_representability"),
        review=review,
    )


def cloud_local_history_draft_plan(
    evidence: object,
) -> CloudLocalReadDraftPlan | None:
    """Parse only a plan derived from the exact adjacent review/context."""

    representability = cloud_local_history_representability(evidence)
    if representability is None or not isinstance(evidence, dict):
        return None
    return CloudLocalReadDraftPlan.from_record(
        evidence.get("local_history_draft_plan"),
        representability=representability,
    )


def cloud_local_history_review_summary(
    review: CloudLocalHistoryReview | None,
    translate: Translation,
) -> str:
    """Render candidate counts without presenting them as proven mappings."""

    if review is None:
        return ""
    placeholders = {
        "reviewed": str(review.reviewed_series_count),
        "skipped": str(review.skipped_series_count),
        "unique": str(review.unique_candidate_count),
        "ambiguous": str(review.ambiguous_candidate_count),
    }
    if review.status == CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES:
        return translate(
            "common.dynamic.cloud_learning_local_history_candidates",
            "Local comparison found {unique} unique and {ambiguous} ambiguous "
            "candidate series. They are review hints only; no mapping was enabled.",
            placeholders,
        )
    if review.reviewed_series_count:
        return translate(
            "common.dynamic.cloud_learning_local_history_no_candidate",
            "Local comparison reviewed {reviewed} series but found no exact "
            "candidate. No mapping was enabled.",
            placeholders,
        )
    return translate(
        "common.dynamic.cloud_learning_local_history_no_eligible",
        "The historical fields were not eligible for an exact local comparison. "
        "No mapping was enabled.",
        placeholders,
    )


def cloud_local_history_representability_summary(
    review: CloudLocalHistoryRepresentabilityReview | None,
    translate: Translation,
) -> str:
    """Summarize context compatibility without promising a generated mapping."""

    if review is None:
        return ""
    placeholders = {
        "representable": str(review.representable_count),
        "available": str(review.already_available_count),
        "incompatible": str(review.incompatible_count),
        "inconclusive": str(review.inconclusive_count),
    }
    if review.representable_count:
        return translate(
            "common.dynamic.cloud_learning_local_history_representable",
            "{representable} candidate(s) preserve the current driver and exact "
            "local route. They remain review-only; no schema draft was created.",
            placeholders,
        )
    if review.already_available_count:
        return translate(
            "common.dynamic.cloud_learning_local_history_already_available",
            "The compatible field(s) are already available locally. No schema "
            "draft was created.",
            placeholders,
        )
    if review.incompatible_count:
        return translate(
            "common.dynamic.cloud_learning_local_history_incompatible",
            "The candidate(s) do not fit the current driver, route, or schema. "
            "No schema draft was created.",
            placeholders,
        )
    return translate(
        "common.dynamic.cloud_learning_local_history_not_representable_yet",
        "The comparison is still inconclusive for the current local context. "
        "No schema draft was created.",
        placeholders,
    )


def cloud_local_history_representability_markdown(
    review: CloudLocalHistoryRepresentabilityReview | None,
    translate: Translation,
) -> str:
    """Render bounded per-series compatibility reasons for expert review."""

    if review is None or not review.decisions:
        return ""
    status_text = {
        REPRESENTABILITY_STATUS_REPRESENTABLE: (
            "common.dynamic.cloud_learning_representability_route_exact",
            "exact current route; adapter-compatible, but not activated",
        ),
        REPRESENTABILITY_STATUS_ALREADY_AVAILABLE: (
            "common.dynamic.cloud_learning_representability_already_available",
            "already available in the effective local schema",
        ),
        REPRESENTABILITY_STATUS_REGISTER_CONFLICT: (
            "common.dynamic.cloud_learning_representability_register_conflict",
            "the same FC/register is already claimed by another local field",
        ),
        REPRESENTABILITY_STATUS_ROUTE_MISMATCH: (
            "common.dynamic.cloud_learning_representability_route_mismatch",
            "the candidate belongs to a different local route",
        ),
        REPRESENTABILITY_STATUS_DRIVER_MISMATCH: (
            "common.dynamic.cloud_learning_representability_driver_mismatch",
            "the active driver changed after the local series was captured",
        ),
    }
    lines: list[str] = []
    for decision in review.decisions[:8]:
        key, default = status_text.get(
            decision.status,
            (
                "common.dynamic.cloud_learning_representability_inconclusive",
                "more evidence is required",
            ),
        )
        lines.append(
            f"- {decision.semantic_key}: "
            + translate(key, default)
        )
    heading = translate(
        "common.dynamic.cloud_learning_representability_heading",
        "Current local-context compatibility — review only",
    )
    return f"**{heading}**\n" + "\n".join(lines)


def cloud_local_history_review_markdown(
    review: CloudLocalHistoryReview | None,
    translate: Translation,
) -> str:
    """Render a bounded technical review without offering activation."""

    if review is None or not review.reports:
        return ""
    lines: list[str] = []
    for report in review.reports[:8]:
        if not report.candidates:
            continue
        title = (
            report.semantic.canonical_title or report.semantic.title
        ).replace("\n", " ")
        if report.status == CLOUD_LOCAL_HISTORY_STATUS_UNIQUE:
            candidate = report.candidates[0]
            lines.append(
                "- "
                + translate(
                    "common.dynamic.cloud_learning_local_history_unique_line",
                    "{title}: exact review candidate — FC{function}, register "
                    "{register}, scale ÷{divisor}{signed}.",
                    {
                        "title": title,
                        "function": str(candidate.location.function),
                        "register": str(candidate.location.register),
                        "divisor": str(candidate.divisor),
                        "signed": (
                            translate(
                                "common.dynamic.cloud_learning_local_history_signed",
                                ", signed",
                            )
                            if candidate.signed
                            else ""
                        ),
                    },
                )
            )
            continue
        lines.append(
            "- "
            + translate(
                "common.dynamic.cloud_learning_local_history_ambiguous_line",
                "{title}: {count} exact candidates; more evidence is required.",
                {
                    "title": title,
                    "count": str(report.candidate_count),
                },
            )
        )
    if not lines:
        return ""
    heading = translate(
        "common.dynamic.cloud_learning_local_history_heading",
        "Local comparison candidates — review only",
    )
    return f"**{heading}**\n" + "\n".join(lines)


def cloud_metadata_review_markdown(
    fields: list[dict[str, str]],
    translate: Translation,
) -> str:
    """Render grouped semantic hints without claiming a local mapping."""

    recognized = [
        item
        for item in fields
        if item.get("kind") != CLOUD_FIELD_KIND_SETTING
        and item.get("semantic_status") == CLOUD_SEMANTIC_STATUS_RECOGNIZED
    ]
    locally_available = [
        item
        for item in recognized
        if item.get("local_status")
        in {
            CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
            CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
        }
    ]
    recognized_unmapped = [
        item for item in recognized if item not in locally_available
    ]
    conflicts = [
        item
        for item in fields
        if item.get("kind") != CLOUD_FIELD_KIND_SETTING
        and item.get("semantic_status") == CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT
    ]
    settings = [item for item in fields if item.get("kind") == CLOUD_FIELD_KIND_SETTING]
    other = [
        item
        for item in fields
        if item not in recognized and item not in conflicts and item not in settings
    ]

    lines: list[str] = []
    visible_count = 0
    recognized_groups = (
        (
            locally_available,
            "common.dynamic.cloud_learning_metadata_local_heading",
            "Already available locally ({count})",
        ),
        (
            recognized_unmapped,
            "common.dynamic.cloud_learning_metadata_recognized_heading",
            "Recognized in the cloud — local source not mapped ({count})",
        ),
    )
    if recognized and not any(item.get("local_status") for item in recognized):
        recognized_groups = (
            (
                recognized,
                "common.dynamic.cloud_learning_metadata_recognized_heading",
                "Recognized cloud readings — local source not checked ({count})",
            ),
        )
    groups = recognized_groups + (
        (
            conflicts,
            "common.dynamic.cloud_learning_metadata_unit_conflict_heading",
            "Recognized fields with a unit mismatch ({count})",
        ),
        (
            settings,
            "common.dynamic.cloud_learning_metadata_settings_heading",
            "Cloud setting descriptions — read only ({count})",
        ),
        (
            other,
            "common.dynamic.cloud_learning_metadata_other_heading",
            "Other cloud fields ({count})",
        ),
    )
    for items, heading_key, heading_default in groups:
        if not items or visible_count >= 80:
            continue
        if lines:
            lines.append("")
        lines.append(
            "**"
            + translate(
                heading_key,
                heading_default,
                {"count": str(len(items))},
            )
            + "**"
        )
        for item in items[: 80 - visible_count]:
            visible_count += 1
            lines.append(cloud_metadata_review_line(item, translate))
    if len(fields) > visible_count:
        lines.append("")
        lines.append(
            translate(
                "common.dynamic.cloud_learning_metadata_more",
                "…and {count} more field(s) in the support evidence.",
                {"count": str(len(fields) - visible_count)},
            )
        )
    return "\n".join(lines)


def cloud_metadata_review_line(
    item: dict[str, str],
    translate: Translation,
) -> str:
    """Render one bounded field already parsed by the typed report."""

    title = item["title"].replace("\n", " ")
    canonical_title = item.get("canonical_title", "").replace("\n", " ")
    if canonical_title and canonical_title.casefold() != title.casefold():
        title = f"{canonical_title} ({title})"
    value = item["value"].replace("\n", " ")
    unit = item["unit"].replace("\n", " ")
    suffix = ""
    if value:
        suffix = f" — {value}{(' ' + unit) if unit else ''}"
    elif unit:
        suffix = f" — {unit}"
    if item.get("semantic_status") == CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT:
        suffix += " — " + translate(
            "common.dynamic.cloud_learning_metadata_unit_conflict_detail",
            "cloud unit {observed_unit}; expected {expected_unit}",
            {
                "observed_unit": unit,
                "expected_unit": item.get("expected_unit", ""),
            },
        )
    return f"- {title}{suffix}"


__all__ = [
    "cloud_local_history_representability",
    "cloud_local_history_representability_markdown",
    "cloud_local_history_representability_summary",
    "cloud_local_history_review",
    "cloud_local_history_review_markdown",
    "cloud_local_history_review_summary",
    "cloud_history_collection",
    "cloud_history_summary",
    "cloud_metadata_review_fields",
    "cloud_metadata_review_line",
    "cloud_metadata_review_markdown",
    "cloud_metadata_semantic_candidate_count",
    "metadata_with_cloud_local_history_review",
    "metadata_with_cloud_local_history_representability",
]
