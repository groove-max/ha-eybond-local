"""Shared PI-family identity and metadata resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .pi_family_catalog_loader import load_pi_family_catalog


_PI_MODEL_NUMBER_ALIASES: dict[str, str] = {
    "VMII-NXPW5KW": "PowMr 4.2kW",
}


@dataclass(frozen=True, slots=True)
class PiProtocolIdentity:
    """Resolved PI-family identity for one detected device."""

    protocol_id: str
    family_key: str
    model_name: str
    variant_key: str = "default"


@dataclass(frozen=True, slots=True)
class PiMetadataNames:
    """Effective profile and schema names selected for one PI-family device."""

    profile_name: str
    register_schema_name: str


def classify_pi_protocol(protocol_id: str) -> str | None:
    """Return the canonical PI family key for one protocol identifier."""

    normalized = protocol_id.strip().upper()
    if not normalized:
        return None
    return load_pi_family_catalog().protocol_families.get(normalized)


def build_pi_model_name(protocol_id: str, values: dict[str, Any]) -> str:
    """Build a user-facing model name from probe values."""

    model_number = values.get("model_number")
    if isinstance(model_number, str):
        normalized_model_number = model_number.strip()
        if normalized_model_number:
            return _PI_MODEL_NUMBER_ALIASES.get(
                normalized_model_number.upper(),
                normalized_model_number,
            )

    rated_power = values.get("output_rating_active_power")
    if isinstance(rated_power, int) and rated_power > 0:
        return f"{protocol_id} {rated_power}"

    return protocol_id


def resolve_pi_identity(protocol_id: str, values: dict[str, Any]) -> PiProtocolIdentity | None:
    """Resolve one PI-family identity from a raw protocol identifier and probe values."""

    normalized_protocol_id = protocol_id.strip().upper()
    family_key = classify_pi_protocol(normalized_protocol_id)
    if family_key is None:
        return None

    model_name = build_pi_model_name(normalized_protocol_id, values)
    variant_key = (
        resolve_pi30_variant(normalized_protocol_id, values, model_name)
        if family_key in {"pi30", "pi41"}
        else "default"
    )
    return PiProtocolIdentity(
        protocol_id=normalized_protocol_id,
        family_key=family_key,
        model_name=model_name,
        variant_key=variant_key,
    )


def resolve_pi30_variant(protocol_id: str, values: dict[str, Any], model_name: str) -> str:
    """Return the known PI30-family variant key for one model candidate set."""

    normalized_protocol_id = protocol_id.strip().upper()
    candidates = set(_pi_identity_candidates(values, model_name))
    for variant in load_pi_family_catalog().pi30_variants:
        if _variant_matches(variant, normalized_protocol_id, values, candidates):
            return variant.key
    return "default"


def resolve_pi30_metadata_names(
    values: dict[str, Any],
    model_name: str,
    *,
    default_profile_name: str,
    default_register_schema_name: str,
) -> PiMetadataNames:
    """Resolve effective PI30 profile/schema names for one detected device."""

    candidates = set(_pi_identity_candidates(values, model_name))
    protocol_id = str(values.get("protocol_id", "")).strip().upper()
    for variant in load_pi_family_catalog().pi30_variants:
        if _variant_matches(variant, protocol_id, values, candidates):
            return PiMetadataNames(
                profile_name=variant.profile_name,
                register_schema_name=variant.register_schema_name,
            )

    return PiMetadataNames(
        profile_name=default_profile_name,
        register_schema_name=default_register_schema_name,
    )


def _pi_identity_candidates(values: dict[str, Any], model_name: str) -> tuple[str, ...]:
    candidates: list[str] = []

    model_number = values.get("model_number")
    if isinstance(model_number, str) and model_number.strip():
        candidates.append(model_number.strip().lower())
    if model_name.strip():
        candidates.append(model_name.strip().lower())

    return tuple(dict.fromkeys(candidates))


def _variant_matches(
    variant,
    protocol_id: str,
    values: dict[str, Any],
    candidates: set[str],
) -> bool:
    return any(
        _match_variant_rule(rule, protocol_id, values, candidates)
        for rule in variant.rules
    )


def _match_variant_rule(
    rule,
    protocol_id: str,
    values: dict[str, Any],
    candidates: set[str],
) -> bool:
    if rule.protocol_ids and protocol_id not in rule.protocol_ids:
        return False
    if rule.model_candidates and not candidates.intersection(rule.model_candidates):
        return False

    operating_mode_code = str(values.get("operating_mode_code", "")).strip().upper()
    if rule.qmod_codes and operating_mode_code not in rule.qmod_codes:
        return False

    if rule.qflag_contains_any:
        enabled = str(values.get("capability_flags_enabled", "")).lower()
        disabled = str(values.get("capability_flags_disabled", "")).lower()
        if not any(flag in enabled or flag in disabled for flag in rule.qflag_contains_any):
            return False

    qpiri_field_count = values.get("qpiri_field_count")
    if rule.min_qpiri_fields is not None and (
        not isinstance(qpiri_field_count, int) or qpiri_field_count < rule.min_qpiri_fields
    ):
        return False

    qpigs_field_count = values.get("qpigs_field_count")
    if rule.min_qpigs_fields is not None and (
        not isinstance(qpigs_field_count, int) or qpigs_field_count < rule.min_qpigs_fields
    ):
        return False

    qpiws_bit_count = values.get("qpiws_bit_count")
    if rule.min_qpiws_bits is not None and (
        not isinstance(qpiws_bit_count, int) or qpiws_bit_count < rule.min_qpiws_bits
    ):
        return False

    return True