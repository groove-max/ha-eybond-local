"""Load declarative PI-family catalog metadata from JSON files."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


PI_FAMILY_CATALOG_PATH = Path(__file__).resolve().parents[1] / "protocol_catalogs" / "pi_family.json"


@dataclass(frozen=True, slots=True)
class PiVariantMatchRule:
    """One declarative predicate used to match a PI-family variant."""

    protocol_ids: tuple[str, ...]
    model_candidates: tuple[str, ...]
    qmod_codes: tuple[str, ...]
    qflag_contains_any: tuple[str, ...]
    min_qpiri_fields: int | None
    min_qpigs_fields: int | None
    min_qpiws_bits: int | None


@dataclass(frozen=True, slots=True)
class PiVariantCatalogEntry:
    """One declarative PI-family variant mapping entry."""

    key: str
    rules: tuple[PiVariantMatchRule, ...]
    profile_name: str
    register_schema_name: str


@dataclass(frozen=True, slots=True)
class PiFamilyCatalog:
    """Declarative PI-family code and variant mappings."""

    protocol_families: dict[str, str]
    pi30_variants: tuple[PiVariantCatalogEntry, ...]


@lru_cache(maxsize=None)
def load_pi_family_catalog() -> PiFamilyCatalog:
    """Load the built-in PI-family catalog."""

    raw = json.loads(PI_FAMILY_CATALOG_PATH.read_text(encoding="utf-8"))
    return PiFamilyCatalog(
        protocol_families={
            str(protocol_id).strip().upper(): str(family_key)
            for protocol_id, family_key in raw.get("protocol_families", {}).items()
        },
        pi30_variants=tuple(
            PiVariantCatalogEntry(
                key=str(item["key"]),
                rules=_parse_variant_rules(item),
                profile_name=str(item["profile_name"]),
                register_schema_name=str(item["register_schema_name"]),
            )
            for item in raw.get("pi30_variants", [])
        ),
    )


def clear_pi_family_catalog_cache() -> None:
    """Clear cached PI-family catalog metadata."""

    load_pi_family_catalog.cache_clear()


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _parse_variant_rules(raw: dict[str, object]) -> tuple[PiVariantMatchRule, ...]:
    any_rules = raw.get("match_any")
    if isinstance(any_rules, list) and any_rules:
        return tuple(_parse_variant_rule(item) for item in any_rules if isinstance(item, dict))
    return (_parse_variant_rule(raw),)


def _parse_variant_rule(raw: dict[str, object]) -> PiVariantMatchRule:
    return PiVariantMatchRule(
        protocol_ids=tuple(
            str(protocol_id).strip().upper()
            for protocol_id in raw.get("protocol_ids", [])
            if str(protocol_id).strip()
        ),
        model_candidates=tuple(
            str(candidate).strip().lower()
            for candidate in raw.get("model_candidates", [])
            if str(candidate).strip()
        ),
        qmod_codes=tuple(
            str(code).strip().upper()
            for code in raw.get("qmod_codes", [])
            if str(code).strip()
        ),
        qflag_contains_any=tuple(
            str(flag).strip().lower()
            for flag in raw.get("qflag_contains_any", [])
            if str(flag).strip()
        ),
        min_qpiri_fields=_optional_int(raw.get("min_qpiri_fields")),
        min_qpigs_fields=_optional_int(raw.get("min_qpigs_fields")),
        min_qpiws_bits=_optional_int(raw.get("min_qpiws_bits")),
    )