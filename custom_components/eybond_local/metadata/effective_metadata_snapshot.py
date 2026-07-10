"""Serializable last-known effective metadata snapshot helpers.

This module is intentionally additive: it defines snapshot vocabulary and
normalization helpers only. Runtime behavior is unchanged until downstream
tasks explicitly wire persistence and fallback usage.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

_KNOWN_CONFIDENCE_VALUES = frozenset({"none", "low", "medium", "high"})

_EFFECTIVE_OWNER_KEY_KEYS = (
    "effective_owner_key",
    "owner_key",
    "driver_key",
)
_EFFECTIVE_OWNER_NAME_KEYS = (
    "effective_owner_name",
    "owner_name",
)
_VARIANT_KEY_KEYS = (
    "variant_key",
    "inverter_variant_key",
)
_PROFILE_NAME_KEYS = ("profile_name",)
_REGISTER_SCHEMA_NAME_KEYS = (
    "register_schema_name",
    "schema_name",
)
_CONFIDENCE_KEYS = (
    "confidence",
    "detection_confidence",
)
_GENERATION_KEYS = (
    "generation",
    "snapshot_generation",
    "version",
)
_GENERATED_AT_KEYS = (
    "generated_at",
    "generated_at_utc",
    "timestamp",
    "updated_at",
)
_CANDIDATE_KEYS_KEYS = ("candidate_keys", "device_candidates")
_RESOLUTION_LEVEL_KEYS = ("resolution_level", "resolution")
_SURFACE_KEY_KEYS = ("surface_key",)
_EVIDENCE_FINGERPRINT_KEYS = ("evidence_fingerprint",)
_CATALOG_VERSION_KEYS = ("catalog_version",)
_DESCRIPTOR_REVISIONS_KEYS = ("descriptor_revisions",)


@dataclass(frozen=True, slots=True)
class EffectiveMetadataSnapshot:
    """Normalized snapshot of one last-known effective metadata state."""

    effective_owner_key: str = ""
    effective_owner_name: str = ""
    variant_key: str = ""
    profile_name: str = ""
    register_schema_name: str = ""
    confidence: str = "none"
    candidate_keys: tuple[str, ...] = ()
    resolution_level: str = ""
    surface_key: str = ""
    evidence_fingerprint: str = ""
    catalog_version: str = ""
    descriptor_revisions: tuple[str, ...] = ()
    generation: int = 0
    generated_at: str = ""

    @property
    def is_empty(self) -> bool:
        """Return true when the snapshot carries no usable metadata payload."""

        return (
            not self.effective_owner_key
            and not self.effective_owner_name
            and not self.variant_key
            and not self.profile_name
            and not self.register_schema_name
            and self.confidence == "none"
            and not self.candidate_keys
            and not self.resolution_level
            and not self.surface_key
            and not self.evidence_fingerprint
            and not self.catalog_version
            and not self.descriptor_revisions
            and self.generation == 0
            and not self.generated_at
        )

    @property
    def is_valid(self) -> bool:
        """Return whether the snapshot has enough metadata to be consumed safely."""

        base_valid = bool(
            self.effective_owner_key
            and self.profile_name
            and self.register_schema_name
            and self.confidence != "none"
        )
        if not base_valid:
            return False
        if not self.catalog_version:
            return True
        return _catalog_surface_matches(self)

    def as_dict(self) -> dict[str, Any]:
        """Serialize this snapshot into one plain dictionary payload."""

        return effective_metadata_snapshot_to_dict(self)


def build_effective_metadata_snapshot(
    *,
    effective_owner_key: Any = "",
    effective_owner_name: Any = "",
    variant_key: Any = "",
    profile_name: Any = "",
    register_schema_name: Any = "",
    confidence: Any = "none",
    candidate_keys: Any = (),
    resolution_level: Any = "",
    surface_key: Any = "",
    evidence_fingerprint: Any = "",
    catalog_version: Any = "",
    descriptor_revisions: Any = (),
    generation: Any = 0,
    generated_at: Any = "",
) -> EffectiveMetadataSnapshot:
    """Build one normalized snapshot from metadata-like field values."""

    return EffectiveMetadataSnapshot(
        effective_owner_key=_normalize_text(effective_owner_key),
        effective_owner_name=_normalize_text(effective_owner_name),
        variant_key=_normalize_text(variant_key),
        profile_name=_normalize_text(profile_name),
        register_schema_name=_normalize_text(register_schema_name),
        confidence=_normalize_confidence(confidence),
        candidate_keys=_normalize_text_tuple(candidate_keys),
        resolution_level=_normalize_text(resolution_level),
        surface_key=_normalize_text(surface_key),
        evidence_fingerprint=_normalize_text(evidence_fingerprint),
        catalog_version=_normalize_text(catalog_version),
        descriptor_revisions=_normalize_text_tuple(descriptor_revisions),
        generation=_normalize_generation(generation),
        generated_at=_normalize_generated_at(generated_at),
    )


def build_effective_metadata_snapshot_from_runtime(
    *,
    inverter: Any = None,
    selection: Any = None,
    confidence: Any = "none",
    generation: Any = 0,
    generated_at: Any = "",
) -> EffectiveMetadataSnapshot:
    """Build one snapshot from runtime inverter and effective-selection objects."""

    resolution = _runtime_resolution_metadata(inverter)
    return build_effective_metadata_snapshot(
        effective_owner_key=_first_non_empty(
            getattr(selection, "effective_owner_key", ""),
            getattr(inverter, "driver_key", ""),
        ),
        effective_owner_name=getattr(selection, "effective_owner_name", ""),
        variant_key=getattr(inverter, "variant_key", ""),
        profile_name=_first_non_empty(
            getattr(inverter, "profile_name", ""),
            getattr(selection, "profile_name", ""),
        ),
        register_schema_name=_first_non_empty(
            getattr(inverter, "register_schema_name", ""),
            getattr(selection, "register_schema_name", ""),
        ),
        confidence=confidence,
        candidate_keys=resolution.get("candidate_keys", ()),
        resolution_level=resolution.get("resolution", ""),
        surface_key=resolution.get("surface_key", ""),
        evidence_fingerprint=resolution.get("evidence_fingerprint", ""),
        catalog_version=resolution.get("catalog_version", ""),
        descriptor_revisions=resolution.get("descriptor_revisions", ()),
        generation=generation,
        generated_at=generated_at,
    )


def effective_metadata_snapshot_from_dict(
    raw: Mapping[str, Any] | None,
) -> EffectiveMetadataSnapshot:
    """Deserialize one snapshot from plain persisted data safely."""

    if not isinstance(raw, Mapping):
        return EffectiveMetadataSnapshot()

    return build_effective_metadata_snapshot(
        effective_owner_key=_first_key_value(raw, _EFFECTIVE_OWNER_KEY_KEYS),
        effective_owner_name=_first_key_value(raw, _EFFECTIVE_OWNER_NAME_KEYS),
        variant_key=_first_key_value(raw, _VARIANT_KEY_KEYS),
        profile_name=_first_key_value(raw, _PROFILE_NAME_KEYS),
        register_schema_name=_first_key_value(raw, _REGISTER_SCHEMA_NAME_KEYS),
        confidence=_first_key_value(raw, _CONFIDENCE_KEYS),
        candidate_keys=_first_key_value(raw, _CANDIDATE_KEYS_KEYS),
        resolution_level=_first_key_value(raw, _RESOLUTION_LEVEL_KEYS),
        surface_key=_first_key_value(raw, _SURFACE_KEY_KEYS),
        evidence_fingerprint=_first_key_value(raw, _EVIDENCE_FINGERPRINT_KEYS),
        catalog_version=_first_key_value(raw, _CATALOG_VERSION_KEYS),
        descriptor_revisions=_first_key_value(raw, _DESCRIPTOR_REVISIONS_KEYS),
        generation=_first_key_value(raw, _GENERATION_KEYS),
        generated_at=_first_key_value(raw, _GENERATED_AT_KEYS),
    )


def effective_metadata_snapshot_to_dict(
    snapshot: EffectiveMetadataSnapshot,
) -> dict[str, Any]:
    """Serialize one snapshot into a stable plain-dict payload."""

    return {
        "effective_owner_key": snapshot.effective_owner_key,
        "effective_owner_name": snapshot.effective_owner_name,
        "variant_key": snapshot.variant_key,
        "profile_name": snapshot.profile_name,
        "register_schema_name": snapshot.register_schema_name,
        "confidence": snapshot.confidence,
        "candidate_keys": list(snapshot.candidate_keys),
        "resolution_level": snapshot.resolution_level,
        "surface_key": snapshot.surface_key,
        "evidence_fingerprint": snapshot.evidence_fingerprint,
        "catalog_version": snapshot.catalog_version,
        "descriptor_revisions": list(snapshot.descriptor_revisions),
        "generation": snapshot.generation,
        "generated_at": snapshot.generated_at,
    }


def _first_non_empty(*values: Any) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return ""


def _first_key_value(raw: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in raw:
            return raw[key]
    return ""


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return ""
    return ""


def _normalize_text_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(
        normalized
        for item in value
        if (normalized := _normalize_text(item))
    )


def _runtime_resolution_metadata(inverter: Any) -> Mapping[str, Any]:
    details = getattr(inverter, "details", None)
    if not isinstance(details, Mapping):
        return {}
    direct = details.get("catalog_detection")
    if isinstance(direct, Mapping):
        return direct
    device_catalog = details.get("device_catalog")
    if isinstance(device_catalog, Mapping):
        compiled = device_catalog.get("compiled_resolution")
        if isinstance(compiled, Mapping):
            return compiled
    return {}


def _catalog_surface_matches(snapshot: EffectiveMetadataSnapshot) -> bool:
    if not snapshot.surface_key:
        return False
    try:
        from .compiled_detection_catalog import load_compiled_detection_catalog

        catalog = load_compiled_detection_catalog()
    except (ImportError, OSError, ValueError):
        return False
    if catalog.catalog_version != snapshot.catalog_version:
        return False
    surface = catalog.surfaces.get(snapshot.surface_key)
    if surface is None:
        return False
    if surface.driver_key != snapshot.effective_owner_key:
        return False
    if snapshot.candidate_keys:
        expected_revisions = tuple(
            f"{key}:{catalog.devices[key].revision}"
            for key in snapshot.candidate_keys
            if key in catalog.devices
        )
        if len(expected_revisions) != len(snapshot.candidate_keys):
            return False
        if (
            snapshot.descriptor_revisions
            and snapshot.descriptor_revisions != expected_revisions
        ):
            return False
    from .profile_loader import canonical_driver_profile_name

    return (
        surface.variant_key == snapshot.variant_key
        and canonical_driver_profile_name(surface.profile_name)
        == canonical_driver_profile_name(snapshot.profile_name)
        and surface.register_schema_name == snapshot.register_schema_name
    )


def _normalize_confidence(value: Any) -> str:
    normalized = _normalize_text(value).lower()
    if normalized in _KNOWN_CONFIDENCE_VALUES:
        return normalized
    return "none"


def _normalize_generation(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(value, 0)

    normalized = _normalize_text(value)
    if not normalized:
        return 0
    try:
        parsed = int(normalized)
    except ValueError:
        return 0
    return max(parsed, 0)


def _normalize_generated_at(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()

    normalized = _normalize_text(value)
    if not normalized:
        return ""

    # Accept legacy UTC Z suffix and emit canonical ISO format.
    candidate = normalized.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate).isoformat()
    except ValueError:
        return ""


__all__ = [
    "EffectiveMetadataSnapshot",
    "build_effective_metadata_snapshot",
    "build_effective_metadata_snapshot_from_runtime",
    "effective_metadata_snapshot_from_dict",
    "effective_metadata_snapshot_to_dict",
]
