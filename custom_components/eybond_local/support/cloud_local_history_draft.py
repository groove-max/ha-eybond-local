"""Build an inactive local-read draft plan from review-only evidence.

The plan is deliberately weaker than a learned mapping.  It preserves the
exact DESSMonitor/local-history review and current route/schema compatibility,
then selects only unique, non-conflicting candidates that can be represented
by an *inactive* draft.  It never writes metadata, activates an overlay, or
grants a candidate local-mapping authority.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from .cloud_local_history_correlation import (
    CLOUD_LOCAL_HISTORY_STATUS_UNIQUE,
    CloudLocalHistoryCandidate,
    LocalRegisterLocation,
)
from .cloud_local_history_representability import (
    REPRESENTABILITY_STATUS_REPRESENTABLE,
    CloudLocalHistoryRepresentabilityReview,
)
from .cloud_semantic_evidence import CloudSemanticObservation


CLOUD_LOCAL_READ_DRAFT_SCHEMA_VERSION = 1
CLOUD_LOCAL_READ_DRAFT_AUTHORITY = "inactive_review_draft_plan_only"
CLOUD_LOCAL_READ_DRAFT_SOURCE = "dessmonitor"


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True)
class CloudLocalReadDraftItem:
    """One exact, still-unproven candidate for an inactive local draft."""

    source_action: str
    series_key: str
    semantic: CloudSemanticObservation
    candidate: CloudLocalHistoryCandidate

    def __post_init__(self) -> None:
        _required_token(
            self.source_action,
            "cloud_local_read_draft_source_action_invalid",
        )
        _required_token(
            self.series_key,
            "cloud_local_read_draft_series_key_invalid",
        )
        if type(self.semantic) is not CloudSemanticObservation:
            raise TypeError("cloud_local_read_draft_semantic_invalid")
        if type(self.candidate) is not CloudLocalHistoryCandidate:
            raise TypeError("cloud_local_read_draft_candidate_invalid")
        if (
            self.semantic.source_action != self.source_action
            or self.semantic.field_id != self.series_key
        ):
            raise ValueError("cloud_local_read_draft_source_mismatch")

    @property
    def semantic_key(self) -> str:
        return self.semantic.semantic_key

    @property
    def location(self) -> LocalRegisterLocation:
        return self.candidate.location

    def to_record(self) -> dict[str, Any]:
        return {
            "source_action": self.source_action,
            "series_key": self.series_key,
            "semantic": self.semantic.to_record(),
            "candidate": self.candidate.to_record(),
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudLocalReadDraftItem | None":
        if type(record) is not dict or set(record) != {
            "source_action",
            "series_key",
            "semantic",
            "candidate",
        }:
            return None
        semantic = CloudSemanticObservation.from_record(record["semantic"])
        candidate = CloudLocalHistoryCandidate.from_record(record["candidate"])
        if semantic is None or candidate is None:
            return None
        try:
            item = cls(
                source_action=record["source_action"],
                series_key=record["series_key"],
                semantic=semantic,
                candidate=candidate,
            )
        except (TypeError, ValueError):
            return None
        if item.to_record() != record:
            return None
        return item


@dataclass(frozen=True, slots=True)
class CloudLocalReadDraftPlan:
    """Revalidatable plan that permits only generation of an inactive draft."""

    representability: CloudLocalHistoryRepresentabilityReview
    items: tuple[CloudLocalReadDraftItem, ...]

    def __post_init__(self) -> None:
        if (
            type(self.representability)
            is not CloudLocalHistoryRepresentabilityReview
        ):
            raise TypeError("cloud_local_read_draft_representability_invalid")
        if type(self.items) is not tuple or any(
            type(item) is not CloudLocalReadDraftItem for item in self.items
        ):
            raise TypeError("cloud_local_read_draft_items_invalid")
        if self.items != _draft_items(self.representability):
            raise ValueError("cloud_local_read_draft_items_mismatch")

    @property
    def item_count(self) -> int:
        return len(self.items)

    @property
    def draft_generation_allowed(self) -> bool:
        return bool(self.items)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_LOCAL_READ_DRAFT_SCHEMA_VERSION,
            "authority": CLOUD_LOCAL_READ_DRAFT_AUTHORITY,
            "source_id": CLOUD_LOCAL_READ_DRAFT_SOURCE,
            "read_only": True,
            "local_mapping": "candidate_not_proven",
            "local_mapping_proven": False,
            "draft_generation_allowed": self.draft_generation_allowed,
            "activation_allowed": False,
            "items": [item.to_record() for item in self.items],
            "item_count": self.item_count,
        }

    @classmethod
    def from_record(
        cls,
        record: object,
        *,
        representability: CloudLocalHistoryRepresentabilityReview,
    ) -> "CloudLocalReadDraftPlan | None":
        if (
            type(record) is not dict
            or type(representability) is not CloudLocalHistoryRepresentabilityReview
        ):
            return None
        try:
            plan = build_cloud_local_read_draft_plan(representability)
        except (TypeError, ValueError):
            return None
        # Exact equality closes every authority flag and rejects forged items,
        # counts, route axes, transforms, semantics, and extra fields.
        if plan.to_record() != record:
            return None
        return plan


def build_cloud_local_read_draft_plan(
    representability: CloudLocalHistoryRepresentabilityReview,
) -> CloudLocalReadDraftPlan:
    """Derive a non-authoritative inactive-draft plan from an exact review."""

    if type(representability) is not CloudLocalHistoryRepresentabilityReview:
        raise TypeError("cloud_local_read_draft_representability_invalid")
    return CloudLocalReadDraftPlan(
        representability=representability,
        items=_draft_items(representability),
    )


def _draft_items(
    representability: CloudLocalHistoryRepresentabilityReview,
) -> tuple[CloudLocalReadDraftItem, ...]:
    proposals: list[CloudLocalReadDraftItem] = []
    reports = representability.review.reports
    decisions = representability.decisions
    if len(reports) != len(decisions):
        raise ValueError("cloud_local_read_draft_decision_count_mismatch")

    for report, decision in zip(reports, decisions, strict=True):
        source = report.cloud_history
        if (
            decision.source_action != source.source_action
            or decision.series_key != source.series_key
            or decision.semantic_key != report.semantic.semantic_key
        ):
            raise ValueError("cloud_local_read_draft_decision_mismatch")
        if decision.status != REPRESENTABILITY_STATUS_REPRESENTABLE:
            continue
        if (
            report.status != CLOUD_LOCAL_HISTORY_STATUS_UNIQUE
            or len(report.candidates) != 1
        ):
            raise ValueError("cloud_local_read_draft_candidate_not_unique")
        proposals.append(
            CloudLocalReadDraftItem(
                source_action=source.source_action,
                series_key=source.series_key,
                semantic=report.semantic,
                candidate=report.candidates[0],
            )
        )

    # A draft cannot safely represent two cloud semantics through the same word,
    # nor one semantic through two candidate words.  Such candidates remain in
    # the underlying review for an expert, but neither is promoted into the plan.
    location_counts = Counter(item.location for item in proposals)
    semantic_counts = Counter(item.semantic_key for item in proposals)
    source_counts = Counter(
        (item.source_action, item.series_key) for item in proposals
    )
    return tuple(
        item
        for item in proposals
        if location_counts[item.location] == 1
        and semantic_counts[item.semantic_key] == 1
        and source_counts[(item.source_action, item.series_key)] == 1
    )


__all__ = [
    "CLOUD_LOCAL_READ_DRAFT_AUTHORITY",
    "CLOUD_LOCAL_READ_DRAFT_SCHEMA_VERSION",
    "CLOUD_LOCAL_READ_DRAFT_SOURCE",
    "CloudLocalReadDraftItem",
    "CloudLocalReadDraftPlan",
    "build_cloud_local_read_draft_plan",
]
