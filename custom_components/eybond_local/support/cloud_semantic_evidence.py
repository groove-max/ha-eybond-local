"""Typed, provider-neutral semantic hints from read-only cloud evidence.

These models deliberately describe only what a cloud field *means*.  They do
not contain a register address, driver key, writable capability, or activation
flag.  A recognized title therefore remains an unproven hint until an
independent local-learning boundary correlates it with local wire evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..metadata.semantic_titles_loader import resolve_semantic_title


CLOUD_SEMANTIC_SCHEMA_VERSION = 1

CLOUD_SEMANTIC_AUTHORITY_HINT_ONLY = "semantic_hint_only"
CLOUD_LOCAL_MAPPING_UNPROVEN = "unproven"

CLOUD_SEMANTIC_STATUS_RECOGNIZED = "recognized"
CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT = "unit_conflict"
CLOUD_SEMANTIC_STATUS_UNKNOWN = "unknown"

CLOUD_FIELD_KIND_READING = "reading"
CLOUD_FIELD_KIND_CHART = "chart"
CLOUD_FIELD_KIND_KEY_PARAMETER = "key_parameter"
CLOUD_FIELD_KIND_SETTING = "setting"

_SEMANTIC_STATUSES = frozenset(
    {
        CLOUD_SEMANTIC_STATUS_RECOGNIZED,
        CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
        CLOUD_SEMANTIC_STATUS_UNKNOWN,
    }
)
_FIELD_KINDS = frozenset(
    {
        CLOUD_FIELD_KIND_READING,
        CLOUD_FIELD_KIND_CHART,
        CLOUD_FIELD_KIND_KEY_PARAMETER,
        CLOUD_FIELD_KIND_SETTING,
    }
)
_SEMANTIC_KINDS = frozenset({"read", "write", "both"})
_MAX_OBSERVATIONS = 512
_MAX_TEXT_LENGTH = 512


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip() or len(value) > _MAX_TEXT_LENGTH:
        raise ValueError(reason)
    return value


def _optional_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if value != value.strip() or len(value) > _MAX_TEXT_LENGTH:
        raise ValueError(reason)
    return value


def _closed_token(value: object, allowed: frozenset[str], reason: str) -> str:
    normalized = _required_token(value, reason)
    if normalized not in allowed:
        raise ValueError(reason)
    return normalized


def _unit_key(value: str) -> str:
    """Normalize only presentation-equivalent unit spellings."""

    aliases = {
        "℃": "°c",
        "°c": "°c",
        "celsius": "°c",
        "percent": "%",
    }
    normalized = " ".join(value.casefold().split())
    return aliases.get(normalized, normalized)


@dataclass(frozen=True, slots=True)
class CloudSemanticObservation:
    """One cloud field and its bounded semantic-classification verdict."""

    field_kind: str
    field_id: str
    title: str
    value: str
    observed_unit: str
    source_action: str
    status: str
    semantic_key: str = ""
    canonical_title: str = ""
    semantic_kind: str = ""
    expected_unit: str = ""
    device_class: str = ""
    state_class: str = ""

    def __post_init__(self) -> None:
        _closed_token(
            self.field_kind,
            _FIELD_KINDS,
            "cloud_semantic_field_kind_invalid",
        )
        _optional_token(self.field_id, "cloud_semantic_field_id_invalid")
        _required_token(self.title, "cloud_semantic_title_invalid")
        _optional_token(self.value, "cloud_semantic_value_invalid")
        _optional_token(self.observed_unit, "cloud_semantic_observed_unit_invalid")
        _required_token(self.source_action, "cloud_semantic_source_action_invalid")
        _closed_token(
            self.status,
            _SEMANTIC_STATUSES,
            "cloud_semantic_status_invalid",
        )
        for value, reason in (
            (self.semantic_key, "cloud_semantic_key_invalid"),
            (self.canonical_title, "cloud_semantic_canonical_title_invalid"),
            (self.semantic_kind, "cloud_semantic_kind_invalid"),
            (self.expected_unit, "cloud_semantic_expected_unit_invalid"),
            (self.device_class, "cloud_semantic_device_class_invalid"),
            (self.state_class, "cloud_semantic_state_class_invalid"),
        ):
            _optional_token(value, reason)

        if self.status == CLOUD_SEMANTIC_STATUS_UNKNOWN:
            if any(
                (
                    self.semantic_key,
                    self.canonical_title,
                    self.semantic_kind,
                    self.expected_unit,
                    self.device_class,
                    self.state_class,
                )
            ):
                raise ValueError("cloud_semantic_unknown_has_classification")
            return

        _required_token(self.semantic_key, "cloud_semantic_key_invalid")
        _required_token(
            self.canonical_title,
            "cloud_semantic_canonical_title_invalid",
        )
        _closed_token(
            self.semantic_kind,
            _SEMANTIC_KINDS,
            "cloud_semantic_kind_invalid",
        )
        if (
            self.status == CLOUD_SEMANTIC_STATUS_RECOGNIZED
            and self.observed_unit
            and self.expected_unit
            and _unit_key(self.observed_unit) != _unit_key(self.expected_unit)
        ):
            raise ValueError("cloud_semantic_recognized_unit_conflict")
        if self.status == CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT:
            if not self.observed_unit or not self.expected_unit:
                raise ValueError("cloud_semantic_unit_conflict_incomplete")
            if _unit_key(self.observed_unit) == _unit_key(self.expected_unit):
                raise ValueError("cloud_semantic_unit_conflict_false")

    def to_record(self) -> dict[str, str]:
        """Serialize the hint without minting local-mapping authority."""

        return {
            "field_kind": self.field_kind,
            "field_id": self.field_id,
            "title": self.title,
            "value": self.value,
            "observed_unit": self.observed_unit,
            "source_action": self.source_action,
            "status": self.status,
            "semantic_key": self.semantic_key,
            "canonical_title": self.canonical_title,
            "semantic_kind": self.semantic_kind,
            "expected_unit": self.expected_unit,
            "device_class": self.device_class,
            "state_class": self.state_class,
            "local_mapping": CLOUD_LOCAL_MAPPING_UNPROVEN,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudSemanticObservation | None":
        """Parse persisted/transient evidence without raising."""

        if type(record) is not dict:
            return None
        if not all(type(key) is str for key in record):
            return None
        if (
            type(record.get("local_mapping")) is not str
            or record.get("local_mapping") != CLOUD_LOCAL_MAPPING_UNPROVEN
        ):
            return None
        expected_keys = {
            "field_kind",
            "field_id",
            "title",
            "value",
            "observed_unit",
            "source_action",
            "status",
            "semantic_key",
            "canonical_title",
            "semantic_kind",
            "expected_unit",
            "device_class",
            "state_class",
            "local_mapping",
        }
        if set(record) != expected_keys:
            return None
        try:
            return cls(
                field_kind=record["field_kind"],
                field_id=record["field_id"],
                title=record["title"],
                value=record["value"],
                observed_unit=record["observed_unit"],
                source_action=record["source_action"],
                status=record["status"],
                semantic_key=record["semantic_key"],
                canonical_title=record["canonical_title"],
                semantic_kind=record["semantic_kind"],
                expected_unit=record["expected_unit"],
                device_class=record["device_class"],
                state_class=record["state_class"],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class CloudSemanticEvidenceReport:
    """One bounded report of semantic hints from a single cloud source."""

    provider_id: str
    source_id: str
    observations: tuple[CloudSemanticObservation, ...]

    def __post_init__(self) -> None:
        _required_token(self.provider_id, "cloud_semantic_provider_invalid")
        _required_token(self.source_id, "cloud_semantic_source_invalid")
        if type(self.observations) is not tuple:
            raise TypeError("cloud_semantic_observations_invalid")
        if len(self.observations) > _MAX_OBSERVATIONS:
            raise ValueError("cloud_semantic_observations_limit_exceeded")
        for observation in self.observations:
            if type(observation) is not CloudSemanticObservation:
                raise TypeError("cloud_semantic_observation_invalid")

    @property
    def recognized_count(self) -> int:
        return sum(
            item.status == CLOUD_SEMANTIC_STATUS_RECOGNIZED
            for item in self.observations
        )

    @property
    def unit_conflict_count(self) -> int:
        return sum(
            item.status == CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT
            for item in self.observations
        )

    @property
    def read_candidate_count(self) -> int:
        return sum(
            item.status == CLOUD_SEMANTIC_STATUS_RECOGNIZED
            and item.field_kind != CLOUD_FIELD_KIND_SETTING
            for item in self.observations
        )

    @property
    def unknown_count(self) -> int:
        return sum(
            item.status == CLOUD_SEMANTIC_STATUS_UNKNOWN
            for item in self.observations
        )

    @property
    def control_metadata_count(self) -> int:
        return sum(
            item.field_kind == CLOUD_FIELD_KIND_SETTING
            for item in self.observations
        )

    def to_record(self) -> dict[str, Any]:
        """Serialize hints with an explicit non-authoritative marker."""

        return {
            "schema_version": CLOUD_SEMANTIC_SCHEMA_VERSION,
            "authority": CLOUD_SEMANTIC_AUTHORITY_HINT_ONLY,
            "local_mapping_proven": False,
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "observations": [item.to_record() for item in self.observations],
            "recognized_count": self.recognized_count,
            "read_candidate_count": self.read_candidate_count,
            "unit_conflict_count": self.unit_conflict_count,
            "unknown_count": self.unknown_count,
            "control_metadata_count": self.control_metadata_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudSemanticEvidenceReport | None":
        """Fail closed when a record claims more authority than a hint."""

        if type(record) is not dict:
            return None
        if not all(type(key) is str for key in record):
            return None
        expected_keys = {
            "schema_version",
            "authority",
            "local_mapping_proven",
            "provider_id",
            "source_id",
            "observations",
            "recognized_count",
            "read_candidate_count",
            "unit_conflict_count",
            "unknown_count",
            "control_metadata_count",
        }
        if set(record) != expected_keys:
            return None
        if (
            type(record.get("schema_version")) is not int
            or record.get("schema_version") != CLOUD_SEMANTIC_SCHEMA_VERSION
            or type(record.get("authority")) is not str
            or record.get("authority") != CLOUD_SEMANTIC_AUTHORITY_HINT_ONLY
            or record.get("local_mapping_proven") is not False
        ):
            return None
        rows = record.get("observations")
        if type(rows) is not list:
            return None
        parsed: list[CloudSemanticObservation] = []
        for row in rows:
            observation = CloudSemanticObservation.from_record(row)
            if observation is None:
                return None
            parsed.append(observation)
        try:
            report = cls(
                provider_id=record.get("provider_id"),
                source_id=record.get("source_id"),
                observations=tuple(parsed),
            )
        except (TypeError, ValueError):
            return None
        for key, expected in (
            ("recognized_count", report.recognized_count),
            ("read_candidate_count", report.read_candidate_count),
            ("unit_conflict_count", report.unit_conflict_count),
            ("unknown_count", report.unknown_count),
            ("control_metadata_count", report.control_metadata_count),
        ):
            if type(record.get(key)) is not int or record.get(key) != expected:
                return None
        return report


def classify_cloud_semantic_observation(
    *,
    field_kind: str,
    field_id: str,
    title: str,
    value: str,
    observed_unit: str,
    source_action: str,
) -> CloudSemanticObservation:
    """Classify one already-normalized cloud field by title and unit only."""

    # Validate the provider boundary before invoking the catalog loader, whose
    # public lookup intentionally accepts broad presentation input.
    _closed_token(field_kind, _FIELD_KINDS, "cloud_semantic_field_kind_invalid")
    _optional_token(field_id, "cloud_semantic_field_id_invalid")
    _required_token(title, "cloud_semantic_title_invalid")
    _optional_token(value, "cloud_semantic_value_invalid")
    _optional_token(observed_unit, "cloud_semantic_observed_unit_invalid")
    _required_token(source_action, "cloud_semantic_source_action_invalid")

    entry = resolve_semantic_title(title)
    if entry is None:
        return CloudSemanticObservation(
            field_kind=field_kind,
            field_id=field_id,
            title=title,
            value=value,
            observed_unit=observed_unit,
            source_action=source_action,
            status=CLOUD_SEMANTIC_STATUS_UNKNOWN,
        )

    status = CLOUD_SEMANTIC_STATUS_RECOGNIZED
    if (
        observed_unit
        and entry.unit
        and _unit_key(observed_unit) != _unit_key(entry.unit)
    ):
        status = CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT
    return CloudSemanticObservation(
        field_kind=field_kind,
        field_id=field_id,
        title=title,
        value=value,
        observed_unit=observed_unit,
        source_action=source_action,
        status=status,
        semantic_key=entry.semantic_key,
        canonical_title=entry.canonical_title,
        semantic_kind=entry.kind,
        expected_unit=entry.unit,
        device_class=entry.device_class,
        state_class=entry.state_class,
    )


__all__ = [
    "CLOUD_FIELD_KIND_CHART",
    "CLOUD_FIELD_KIND_KEY_PARAMETER",
    "CLOUD_FIELD_KIND_READING",
    "CLOUD_FIELD_KIND_SETTING",
    "CLOUD_LOCAL_MAPPING_UNPROVEN",
    "CLOUD_SEMANTIC_AUTHORITY_HINT_ONLY",
    "CLOUD_SEMANTIC_SCHEMA_VERSION",
    "CLOUD_SEMANTIC_STATUS_RECOGNIZED",
    "CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT",
    "CLOUD_SEMANTIC_STATUS_UNKNOWN",
    "CloudSemanticEvidenceReport",
    "CloudSemanticObservation",
    "classify_cloud_semantic_observation",
]
