"""Review whether cloud/local candidates preserve the active local route.

This module is deliberately downstream of temporal correlation and upstream of
any learned-overlay adapter.  It can say that a unique candidate is expressible
against the current driver/schema context, but it never creates a schema draft,
persists a mapping, or grants activation authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..collector_identity import pn_is_same_identity, validated_collector_pn
from ..metadata.register_schema_models import RegisterSchemaMetadata
from ..models import MeasurementDescription, ProbeTarget, RegisterValueSpec
from .cloud_local_history_correlation import (
    CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS,
    CLOUD_LOCAL_HISTORY_STATUS_UNIQUE,
    CloudLocalHistoryReview,
)


CLOUD_LOCAL_HISTORY_REPRESENTABILITY_SCHEMA_VERSION = 1
CLOUD_LOCAL_HISTORY_REPRESENTABILITY_AUTHORITY = (
    "current_context_review_only"
)

REPRESENTABILITY_STATUS_REPRESENTABLE = "representable_current_context"
REPRESENTABILITY_STATUS_ALREADY_AVAILABLE = "already_available"
REPRESENTABILITY_STATUS_REGISTER_CONFLICT = "register_conflict"
REPRESENTABILITY_STATUS_ROUTE_MISMATCH = "route_mismatch"
REPRESENTABILITY_STATUS_DRIVER_MISMATCH = "driver_mismatch"
REPRESENTABILITY_STATUS_AMBIGUOUS = "ambiguous_candidate"
REPRESENTABILITY_STATUS_INCONCLUSIVE = "inconclusive_candidate"

_REPRESENTABILITY_STATUSES = frozenset(
    {
        REPRESENTABILITY_STATUS_REPRESENTABLE,
        REPRESENTABILITY_STATUS_ALREADY_AVAILABLE,
        REPRESENTABILITY_STATUS_REGISTER_CONFLICT,
        REPRESENTABILITY_STATUS_ROUTE_MISMATCH,
        REPRESENTABILITY_STATUS_DRIVER_MISMATCH,
        REPRESENTABILITY_STATUS_AMBIGUOUS,
        REPRESENTABILITY_STATUS_INCONCLUSIVE,
    }
)


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


def _bounded_int(
    value: object,
    *,
    minimum: int,
    maximum: int,
    reason: str,
) -> int:
    if type(value) is not int:
        raise TypeError(reason)
    if value < minimum or value > maximum:
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True, order=True)
class SchemaRegisterLocation:
    """One exact Modbus address already claimed by the effective schema."""

    function: int
    register: int

    def __post_init__(self) -> None:
        if type(self.function) is not int:
            raise TypeError("cloud_local_schema_function_invalid")
        if self.function not in {3, 4}:
            raise ValueError("cloud_local_schema_function_invalid")
        _bounded_int(
            self.register,
            minimum=0,
            maximum=0xFFFF,
            reason="cloud_local_schema_register_invalid",
        )

    def to_record(self) -> dict[str, int]:
        return {"function": self.function, "register": self.register}

    @classmethod
    def from_record(cls, record: object) -> "SchemaRegisterLocation | None":
        if type(record) is not dict or set(record) != {"function", "register"}:
            return None
        try:
            return cls(
                function=record["function"],
                register=record["register"],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class LocalRegisterOverlayContext:
    """Strict read-only snapshot of the current route and schema claims."""

    collector_pn: str
    driver_key: str
    register_schema_name: str
    devcode: int
    collector_addr: int
    device_addr: int
    claimed_locations: tuple[SchemaRegisterLocation, ...]
    existing_semantic_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.collector_pn) is not str:
            raise TypeError("cloud_local_context_collector_pn_invalid")
        if validated_collector_pn(self.collector_pn) != self.collector_pn:
            raise ValueError("cloud_local_context_collector_pn_invalid")
        _required_token(self.driver_key, "cloud_local_context_driver_invalid")
        _required_token(
            self.register_schema_name,
            "cloud_local_context_schema_invalid",
        )
        _bounded_int(
            self.devcode,
            minimum=0,
            maximum=0xFFFF,
            reason="cloud_local_context_devcode_invalid",
        )
        for value in (self.collector_addr, self.device_addr):
            _bounded_int(
                value,
                minimum=0,
                maximum=0xFF,
                reason="cloud_local_context_address_invalid",
            )
        if type(self.claimed_locations) is not tuple:
            raise TypeError("cloud_local_context_locations_invalid")
        if any(
            type(location) is not SchemaRegisterLocation
            for location in self.claimed_locations
        ):
            raise TypeError("cloud_local_context_location_invalid")
        if self.claimed_locations != tuple(sorted(set(self.claimed_locations))):
            raise ValueError("cloud_local_context_locations_invalid")
        if type(self.existing_semantic_keys) is not tuple:
            raise TypeError("cloud_local_context_semantics_invalid")
        for key in self.existing_semantic_keys:
            _required_token(key, "cloud_local_context_semantic_invalid")
        if self.existing_semantic_keys != tuple(
            sorted(set(self.existing_semantic_keys))
        ):
            raise ValueError("cloud_local_context_semantics_invalid")

    def to_record(self) -> dict[str, Any]:
        return {
            "collector_pn": self.collector_pn,
            "driver_key": self.driver_key,
            "register_schema_name": self.register_schema_name,
            "probe_target": {
                "devcode": self.devcode,
                "collector_addr": self.collector_addr,
                "device_addr": self.device_addr,
            },
            "claimed_locations": [
                location.to_record() for location in self.claimed_locations
            ],
            "existing_semantic_keys": list(self.existing_semantic_keys),
        }

    @classmethod
    def from_record(cls, record: object) -> "LocalRegisterOverlayContext | None":
        if type(record) is not dict or set(record) != {
            "collector_pn",
            "driver_key",
            "register_schema_name",
            "probe_target",
            "claimed_locations",
            "existing_semantic_keys",
        }:
            return None
        target = record["probe_target"]
        if type(target) is not dict or set(target) != {
            "devcode",
            "collector_addr",
            "device_addr",
        }:
            return None
        if type(record["claimed_locations"]) is not list:
            return None
        locations: list[SchemaRegisterLocation] = []
        for raw_location in record["claimed_locations"]:
            location = SchemaRegisterLocation.from_record(raw_location)
            if location is None:
                return None
            locations.append(location)
        if type(record["existing_semantic_keys"]) is not list:
            return None
        try:
            context = cls(
                collector_pn=record["collector_pn"],
                driver_key=record["driver_key"],
                register_schema_name=record["register_schema_name"],
                devcode=target["devcode"],
                collector_addr=target["collector_addr"],
                device_addr=target["device_addr"],
                claimed_locations=tuple(locations),
                existing_semantic_keys=tuple(record["existing_semantic_keys"]),
            )
        except (TypeError, ValueError):
            return None
        if context.to_record() != record:
            return None
        return context


def build_local_register_overlay_context(
    *,
    collector_pn: str,
    driver_key: str,
    probe_target: ProbeTarget,
    register_schema_name: str,
    register_schema: RegisterSchemaMetadata,
) -> LocalRegisterOverlayContext:
    """Build a strict context only from exact runtime/schema model types."""

    if type(probe_target) is not ProbeTarget:
        raise TypeError("cloud_local_context_probe_target_invalid")
    if type(register_schema) is not RegisterSchemaMetadata:
        raise TypeError("cloud_local_context_register_schema_invalid")
    _required_token(driver_key, "cloud_local_context_driver_invalid")
    _required_token(register_schema_name, "cloud_local_context_schema_invalid")
    schema_driver_key = _required_token(
        register_schema.driver_key,
        "cloud_local_context_schema_driver_invalid",
    )
    schema_source_name = _required_token(
        register_schema.source_name,
        "cloud_local_context_schema_name_invalid",
    )
    if schema_driver_key != driver_key:
        raise ValueError("cloud_local_context_schema_driver_mismatch")
    if schema_source_name != register_schema_name:
        raise ValueError("cloud_local_context_schema_name_mismatch")

    locations: set[SchemaRegisterLocation] = set()
    if type(register_schema.spec_sets) is not dict:
        raise TypeError("cloud_local_context_spec_sets_invalid")
    for set_key, specs in register_schema.spec_sets.items():
        _required_token(set_key, "cloud_local_context_spec_set_key_invalid")
        if type(specs) is not tuple:
            raise TypeError("cloud_local_context_spec_set_invalid")
        for spec in specs:
            if type(spec) is not RegisterValueSpec:
                raise TypeError("cloud_local_context_spec_invalid")
            _bounded_int(
                spec.word_count,
                minimum=1,
                maximum=0x10000,
                reason="cloud_local_context_word_count_invalid",
            )
            if spec.register + spec.word_count > 0x10000:
                raise ValueError("cloud_local_context_register_range_invalid")
            for register in range(spec.register, spec.register + spec.word_count):
                locations.add(
                    SchemaRegisterLocation(
                        function=spec.function,
                        register=register,
                    )
                )

    semantic_keys: set[str] = set()
    if type(register_schema.measurement_descriptions) is not tuple:
        raise TypeError("cloud_local_context_measurements_invalid")
    for description in register_schema.measurement_descriptions:
        if type(description) is not MeasurementDescription:
            raise TypeError("cloud_local_context_measurement_invalid")
        key = _required_token(
            description.key,
            "cloud_local_context_semantic_invalid",
        )
        semantic_keys.add(key)
        translation_key = description.translation_key
        if translation_key is not None:
            semantic_keys.add(
                _required_token(
                    translation_key,
                    "cloud_local_context_semantic_invalid",
                )
            )

    return LocalRegisterOverlayContext(
        collector_pn=collector_pn,
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        devcode=probe_target.devcode,
        collector_addr=probe_target.collector_addr,
        device_addr=probe_target.device_addr,
        claimed_locations=tuple(sorted(locations)),
        existing_semantic_keys=tuple(sorted(semantic_keys)),
    )


@dataclass(frozen=True, slots=True)
class CloudLocalHistoryRepresentabilityDecision:
    """One recomputed compatibility verdict for one reviewed cloud series."""

    source_action: str
    series_key: str
    semantic_key: str
    status: str

    def __post_init__(self) -> None:
        _required_token(self.source_action, "cloud_local_decision_source_invalid")
        _required_token(self.series_key, "cloud_local_decision_series_invalid")
        _required_token(self.semantic_key, "cloud_local_decision_semantic_invalid")
        if type(self.status) is not str:
            raise TypeError("cloud_local_decision_status_invalid")
        if self.status not in _REPRESENTABILITY_STATUSES:
            raise ValueError("cloud_local_decision_status_invalid")

    def to_record(self) -> dict[str, str]:
        return {
            "source_action": self.source_action,
            "series_key": self.series_key,
            "semantic_key": self.semantic_key,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class CloudLocalHistoryRepresentabilityReview:
    """Current-context compatibility review with zero mapping authority."""

    review: CloudLocalHistoryReview
    context: LocalRegisterOverlayContext
    decisions: tuple[CloudLocalHistoryRepresentabilityDecision, ...]

    def __post_init__(self) -> None:
        if type(self.review) is not CloudLocalHistoryReview:
            raise TypeError("cloud_local_representability_review_invalid")
        if type(self.context) is not LocalRegisterOverlayContext:
            raise TypeError("cloud_local_representability_context_invalid")
        if not pn_is_same_identity(
            self.review.local_series.collector_pn,
            self.context.collector_pn,
        ):
            raise ValueError("cloud_local_representability_identity_mismatch")
        if type(self.decisions) is not tuple or any(
            type(decision) is not CloudLocalHistoryRepresentabilityDecision
            for decision in self.decisions
        ):
            raise TypeError("cloud_local_representability_decisions_invalid")
        if self.decisions != _representability_decisions(self.review, self.context):
            raise ValueError("cloud_local_representability_verdict_mismatch")

    @property
    def representable_count(self) -> int:
        return sum(
            item.status == REPRESENTABILITY_STATUS_REPRESENTABLE
            for item in self.decisions
        )

    @property
    def already_available_count(self) -> int:
        return sum(
            item.status == REPRESENTABILITY_STATUS_ALREADY_AVAILABLE
            for item in self.decisions
        )

    @property
    def incompatible_count(self) -> int:
        return sum(
            item.status
            in {
                REPRESENTABILITY_STATUS_REGISTER_CONFLICT,
                REPRESENTABILITY_STATUS_ROUTE_MISMATCH,
                REPRESENTABILITY_STATUS_DRIVER_MISMATCH,
            }
            for item in self.decisions
        )

    @property
    def inconclusive_count(self) -> int:
        return len(self.decisions) - (
            self.representable_count
            + self.already_available_count
            + self.incompatible_count
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_LOCAL_HISTORY_REPRESENTABILITY_SCHEMA_VERSION,
            "authority": CLOUD_LOCAL_HISTORY_REPRESENTABILITY_AUTHORITY,
            "read_only": True,
            "local_mapping": "candidate_not_proven",
            "local_mapping_proven": False,
            "draft_generation_allowed": False,
            "activation_allowed": False,
            "context": self.context.to_record(),
            "decisions": [item.to_record() for item in self.decisions],
            "representable_count": self.representable_count,
            "already_available_count": self.already_available_count,
            "incompatible_count": self.incompatible_count,
            "inconclusive_count": self.inconclusive_count,
        }

    @classmethod
    def from_record(
        cls,
        record: object,
        *,
        review: CloudLocalHistoryReview,
    ) -> "CloudLocalHistoryRepresentabilityReview | None":
        if type(review) is not CloudLocalHistoryReview or type(record) is not dict:
            return None
        context = LocalRegisterOverlayContext.from_record(record.get("context"))
        if context is None:
            return None
        try:
            candidate = build_cloud_local_history_representability_review(
                review,
                context,
            )
        except (TypeError, ValueError):
            return None
        if candidate.to_record() != record:
            return None
        return candidate


def build_cloud_local_history_representability_review(
    review: CloudLocalHistoryReview,
    context: LocalRegisterOverlayContext,
) -> CloudLocalHistoryRepresentabilityReview:
    """Recompute full-route compatibility without producing an adapter draft."""

    if type(review) is not CloudLocalHistoryReview:
        raise TypeError("cloud_local_representability_review_invalid")
    if type(context) is not LocalRegisterOverlayContext:
        raise TypeError("cloud_local_representability_context_invalid")
    return CloudLocalHistoryRepresentabilityReview(
        review=review,
        context=context,
        decisions=_representability_decisions(review, context),
    )


def _representability_decisions(
    review: CloudLocalHistoryReview,
    context: LocalRegisterOverlayContext,
) -> tuple[CloudLocalHistoryRepresentabilityDecision, ...]:
    claimed = set(context.claimed_locations)
    semantics = set(context.existing_semantic_keys)
    decisions: list[CloudLocalHistoryRepresentabilityDecision] = []
    for report in review.reports:
        source = report.cloud_history.source_series
        status = REPRESENTABILITY_STATUS_INCONCLUSIVE
        if report.status == CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS:
            status = REPRESENTABILITY_STATUS_AMBIGUOUS
        elif report.status == CLOUD_LOCAL_HISTORY_STATUS_UNIQUE:
            candidate = report.candidates[0]
            location = candidate.location
            if review.local_series.driver_key != context.driver_key:
                status = REPRESENTABILITY_STATUS_DRIVER_MISMATCH
            elif (
                location.devcode != context.devcode
                or location.collector_addr != context.collector_addr
                or location.device_addr != context.device_addr
            ):
                status = REPRESENTABILITY_STATUS_ROUTE_MISMATCH
            elif report.semantic.semantic_key in semantics:
                status = REPRESENTABILITY_STATUS_ALREADY_AVAILABLE
            elif SchemaRegisterLocation(
                function=location.function,
                register=location.register,
            ) in claimed:
                status = REPRESENTABILITY_STATUS_REGISTER_CONFLICT
            else:
                status = REPRESENTABILITY_STATUS_REPRESENTABLE
        decisions.append(
            CloudLocalHistoryRepresentabilityDecision(
                source_action=source.source_action,
                series_key=source.series_key,
                semantic_key=report.semantic.semantic_key,
                status=status,
            )
        )
    return tuple(decisions)


__all__ = [
    "CLOUD_LOCAL_HISTORY_REPRESENTABILITY_AUTHORITY",
    "CloudLocalHistoryRepresentabilityDecision",
    "CloudLocalHistoryRepresentabilityReview",
    "LocalRegisterOverlayContext",
    "REPRESENTABILITY_STATUS_ALREADY_AVAILABLE",
    "REPRESENTABILITY_STATUS_AMBIGUOUS",
    "REPRESENTABILITY_STATUS_DRIVER_MISMATCH",
    "REPRESENTABILITY_STATUS_INCONCLUSIVE",
    "REPRESENTABILITY_STATUS_REGISTER_CONFLICT",
    "REPRESENTABILITY_STATUS_REPRESENTABLE",
    "REPRESENTABILITY_STATUS_ROUTE_MISMATCH",
    "SchemaRegisterLocation",
    "build_cloud_local_history_representability_review",
    "build_local_register_overlay_context",
]
