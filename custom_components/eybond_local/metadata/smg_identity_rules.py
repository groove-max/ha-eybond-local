"""Load and score declarative SMG identity rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Mapping

from .smg_identity_anchor_catalog_loader import load_smg_identity_anchor_catalog


SMG_IDENTITY_RULE_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "smg_identity_rules.json"
)

_ALLOWED_CONFIDENCE_VALUES = frozenset({"high", "medium", "low"})
_MISSING = object()
_NO_EQUALS = object()


@dataclass(frozen=True, slots=True)
class SmgIdentityEvidence:
    """Evidence used to score SMG identity candidates."""

    protocol_family: str = "modbus_smg"
    anchors: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SmgIdentityRuleRequirement:
    """One required SMG identity anchor condition."""

    anchor_key: str
    equals: object | None = None
    one_of: tuple[object, ...] = ()
    min_value: int | float | None = None
    max_value: int | float | None = None
    known_enum: bool = False


@dataclass(frozen=True, slots=True)
class SmgIdentityVariantRule:
    """One declarative SMG variant rule."""

    variant_key: str
    required: tuple[SmgIdentityRuleRequirement, ...]
    preferred: tuple[str, ...]
    confidence: str
    read_only: bool = False
    provisional: bool = False
    family_only: bool = False


@dataclass(frozen=True, slots=True)
class SmgIdentityRuleCatalog:
    """Declarative SMG variant rules."""

    protocol_family: str
    rules: tuple[SmgIdentityVariantRule, ...]


@dataclass(frozen=True, slots=True)
class SmgIdentityCandidate:
    """One scored SMG variant candidate."""

    variant_key: str
    score: int
    confidence: str
    read_only: bool
    provisional: bool
    reasons: tuple[str, ...]


@lru_cache(maxsize=None)
def load_smg_identity_rule_catalog() -> SmgIdentityRuleCatalog:
    """Load the built-in SMG identity rule catalog."""

    raw = json.loads(SMG_IDENTITY_RULE_CATALOG_PATH.read_text(encoding="utf-8"))
    catalog = SmgIdentityRuleCatalog(
        protocol_family=str(raw.get("protocol_family", "modbus_smg")).strip() or "modbus_smg",
        rules=tuple(
            _parse_rule(item)
            for item in raw.get("rules", [])
            if isinstance(item, dict)
        ),
    )
    _validate_catalog(catalog)
    return catalog


def clear_smg_identity_rule_catalog_cache() -> None:
    """Clear cached SMG identity rule metadata."""

    load_smg_identity_rule_catalog.cache_clear()


def score_smg_identity_candidates(
    evidence: SmgIdentityEvidence | Mapping[str, object],
) -> tuple[SmgIdentityCandidate, ...]:
    """Score SMG identity candidates for one evidence snapshot."""

    evidence_snapshot = _coerce_evidence(evidence)
    catalog = load_smg_identity_rule_catalog()
    scored_candidates = tuple(
        candidate
        for candidate in (
            _score_rule(rule, evidence_snapshot, catalog)
            for rule in catalog.rules
        )
        if candidate is not None
    )
    return tuple(sorted(scored_candidates, key=lambda candidate: candidate.score, reverse=True))


def _coerce_evidence(
    evidence: SmgIdentityEvidence | Mapping[str, object],
) -> SmgIdentityEvidence:
    if isinstance(evidence, SmgIdentityEvidence):
        return evidence

    if not isinstance(evidence, Mapping):
        raise TypeError("smg_identity_rules:unsupported_evidence_type")

    protocol_family = str(evidence.get("protocol_family", "modbus_smg")).strip() or "modbus_smg"
    anchors = evidence.get("anchors")
    if isinstance(anchors, Mapping):
        return SmgIdentityEvidence(protocol_family=protocol_family, anchors=dict(anchors))

    return SmgIdentityEvidence(
        protocol_family=protocol_family,
        anchors={
            str(key).strip(): value
            for key, value in evidence.items()
            if str(key).strip() not in {"protocol_family", "anchors"}
        },
    )


def _score_rule(
    rule: SmgIdentityVariantRule,
    evidence: SmgIdentityEvidence,
    catalog: SmgIdentityRuleCatalog,
) -> SmgIdentityCandidate | None:
    if evidence.protocol_family != catalog.protocol_family:
        return None

    if rule.family_only and evidence.protocol_family != catalog.protocol_family:
        return None

    anchors = _normalized_anchors(evidence.anchors)
    reasons: list[str] = []
    score = 0

    for requirement in rule.required:
        actual = anchors.get(requirement.anchor_key, _MISSING)
        if actual is _MISSING:
            return None
        if not _is_requirement_match(requirement, actual):
            return None
        score += 1000
        reasons.append(f"required_anchor:{requirement.anchor_key}={_format_reason_value(actual)}")

    for anchor_key in rule.preferred:
        actual = anchors.get(anchor_key, _MISSING)
        if actual is _MISSING:
            continue
        score += 100
        reasons.append(f"preferred_anchor:{anchor_key}={_format_reason_value(actual)}")

    if rule.family_only:
        score += 1
        reasons.append("family_fallback_variant")
    if rule.read_only:
        reasons.append("read_only_variant")
    if rule.provisional:
        reasons.append("provisional_variant")
    if rule.confidence == "high":
        reasons.append("confirmed_variant")
    elif rule.confidence == "medium" and not rule.family_only:
        reasons.append("unverified_variant")

    return SmgIdentityCandidate(
        variant_key=rule.variant_key,
        score=score,
        confidence=rule.confidence,
        read_only=rule.read_only,
        provisional=rule.provisional,
        reasons=tuple(reasons),
    )


def _parse_rule(raw: dict[str, object]) -> SmgIdentityVariantRule:
    return SmgIdentityVariantRule(
        variant_key=str(raw["variant_key"]).strip(),
        required=tuple(
            _parse_requirement(item)
            for item in raw.get("required", [])
            if isinstance(item, dict)
        ),
        preferred=tuple(
            str(anchor_key).strip()
            for anchor_key in raw.get("preferred", [])
            if str(anchor_key).strip()
        ),
        confidence=str(raw.get("confidence", "medium")).strip().lower(),
        read_only=bool(raw.get("read_only", False)),
        provisional=bool(raw.get("provisional", False)),
        family_only=bool(raw.get("family_only", False)),
    )


def _parse_requirement(raw: dict[str, object]) -> SmgIdentityRuleRequirement:
    equals = raw.get("equals", _NO_EQUALS)
    one_of_raw = raw.get("one_of", [])
    if isinstance(one_of_raw, list):
        one_of = tuple(one_of_raw)
    else:
        one_of = ()

    min_value_raw = raw.get("min", _NO_EQUALS)
    max_value_raw = raw.get("max", _NO_EQUALS)

    min_value: int | float | None
    max_value: int | float | None
    if isinstance(min_value_raw, (int, float)):
        min_value = min_value_raw
    else:
        min_value = None
    if isinstance(max_value_raw, (int, float)):
        max_value = max_value_raw
    else:
        max_value = None

    return SmgIdentityRuleRequirement(
        anchor_key=str(raw["anchor_key"]).strip(),
        equals=None if equals is _NO_EQUALS else equals,
        one_of=one_of,
        min_value=min_value,
        max_value=max_value,
        known_enum=bool(raw.get("known_enum", False)),
    )


def _validate_catalog(catalog: SmgIdentityRuleCatalog) -> None:
    if not catalog.protocol_family:
        raise ValueError("smg_identity_rule_catalog:missing_protocol_family")

    if not catalog.rules:
        raise ValueError("smg_identity_rule_catalog:missing_rules")

    anchor_catalog = load_smg_identity_anchor_catalog()
    known_anchor_keys = set(anchor_catalog.anchors)
    seen_rule_keys: set[str] = set()

    for rule in catalog.rules:
        if not rule.variant_key:
            raise ValueError("smg_identity_rule_catalog:invalid_rule_key")
        if rule.variant_key in seen_rule_keys:
            raise ValueError(f"smg_identity_rule_catalog:duplicate_rule:{rule.variant_key}")
        seen_rule_keys.add(rule.variant_key)

        if rule.confidence not in _ALLOWED_CONFIDENCE_VALUES:
            raise ValueError(
                f"smg_identity_rule_catalog:invalid_confidence:{rule.variant_key}:{rule.confidence}"
            )

        for requirement in rule.required:
            if requirement.anchor_key not in known_anchor_keys:
                raise ValueError(
                    f"smg_identity_rule_catalog:unknown_required_anchor:{rule.variant_key}:{requirement.anchor_key}"
                )

        for anchor_key in rule.preferred:
            if anchor_key not in known_anchor_keys:
                raise ValueError(
                    f"smg_identity_rule_catalog:unknown_preferred_anchor:{rule.variant_key}:{anchor_key}"
                )

        if rule.variant_key == "family_fallback":
            if not rule.family_only:
                raise ValueError("smg_identity_rule_catalog:family_fallback_requires_family_only")
            if not rule.read_only:
                raise ValueError("smg_identity_rule_catalog:family_fallback_requires_read_only")
            if not rule.provisional:
                raise ValueError("smg_identity_rule_catalog:family_fallback_requires_provisional")


def _normalized_anchors(anchors: Mapping[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in anchors.items():
        normalized_key = str(key).strip()
        if normalized_key:
            normalized[normalized_key] = value
    return normalized


def _format_reason_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _is_requirement_match(requirement: SmgIdentityRuleRequirement, actual: object) -> bool:
    if requirement.equals is not None and actual != requirement.equals:
        return False

    if requirement.one_of and actual not in requirement.one_of:
        return False

    if requirement.known_enum and not _is_known_enum_value(actual):
        return False

    if requirement.min_value is not None or requirement.max_value is not None:
        if not isinstance(actual, (int, float)):
            return False
        if requirement.min_value is not None and actual < requirement.min_value:
            return False
        if requirement.max_value is not None and actual > requirement.max_value:
            return False

    return True


def _is_known_enum_value(value: object) -> bool:
    return isinstance(value, str) and not value.startswith("Unknown")
