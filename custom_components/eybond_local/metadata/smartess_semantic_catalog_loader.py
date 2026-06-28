"""Load declarative SmartESS semantic alias and binding metadata from JSON files."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


SMARTESS_SEMANTIC_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "smartess_semantic_catalog.json"
)

SMARTESS_CLOUD_ONLY_REASON = (
    "No current 0925 root-map or PI30-family local evidence is linked to this field."
)


def _normalize_title(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").split())


@dataclass(frozen=True, slots=True)
class SmartEssSemanticConsumerBinding:
    """One consumer-specific binding block for a SmartESS semantic entry."""

    titles: tuple[str, ...] = ()
    title_aliases: tuple[str, ...] = ()
    cloud_field_ids: tuple[str, ...] = ()
    asset_ids: tuple[str, ...] = ()
    asset_registers: tuple[int, ...] = ()
    bucket: str = ""
    source: str = ""
    reason: str = ""
    profile_keys: tuple[str, ...] = ()
    register_keys: tuple[str, ...] = ()
    measurement_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SmartEssSemanticEntry:
    """One canonical SmartESS semantic mapping entry."""

    semantic_key: str
    canonical_title: str
    title_aliases: tuple[str, ...] = ()
    notes: str = ""
    smartess_cloud: SmartEssSemanticConsumerBinding = SmartEssSemanticConsumerBinding()
    smg_bridge: SmartEssSemanticConsumerBinding = SmartEssSemanticConsumerBinding()

    @property
    def all_titles(self) -> tuple[str, ...]:
        return (self.canonical_title, *self.title_aliases)

    @property
    def normalized_titles(self) -> tuple[str, ...]:
        seen: set[str] = set()
        ordered: list[str] = []
        for title in self.all_titles:
            normalized = _normalize_title(title)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
        return tuple(ordered)


@dataclass(frozen=True, slots=True)
class SmartEssSemanticCatalog:
    """Declarative SmartESS semantic catalog."""

    catalog_version: int
    description: str
    cloud_id_semantics: str
    entries: dict[str, SmartEssSemanticEntry]
    aliases: dict[str, str]
    smartess_cloud_aliases: dict[str, str]

    def resolve(self, value: str) -> SmartEssSemanticEntry | None:
        normalized = _normalize_title(value)
        if not normalized:
            return None
        semantic_key = self.aliases.get(normalized, normalized)
        return self.entries.get(semantic_key)

    def resolve_smartess_cloud(self, value: str) -> SmartEssSemanticEntry | None:
        normalized = _normalize_title(value)
        if not normalized:
            return None
        semantic_key = self.smartess_cloud_aliases.get(normalized, normalized)
        return self.entries.get(semantic_key)


@lru_cache(maxsize=None)
def load_smartess_semantic_catalog() -> SmartEssSemanticCatalog:
    """Load the built-in SmartESS semantic catalog."""

    raw = json.loads(SMARTESS_SEMANTIC_CATALOG_PATH.read_text(encoding="utf-8"))
    entries = tuple(
        _parse_entry(item)
        for item in raw.get("entries", [])
        if isinstance(item, dict)
    )
    catalog = SmartEssSemanticCatalog(
        catalog_version=int(raw.get("catalog_version", 0)),
        description=str(raw.get("description", "")).strip(),
        cloud_id_semantics=str(raw.get("cloud_id_semantics", "")).strip(),
        entries=_keyed_entries(entries),
        aliases={},
        smartess_cloud_aliases={},
    )
    aliases = _build_alias_map(catalog.entries)
    smartess_cloud_aliases = _build_smartess_cloud_alias_map(catalog.entries)
    catalog = SmartEssSemanticCatalog(
        catalog_version=catalog.catalog_version,
        description=catalog.description,
        cloud_id_semantics=catalog.cloud_id_semantics,
        entries=catalog.entries,
        aliases=aliases,
        smartess_cloud_aliases=smartess_cloud_aliases,
    )
    _validate_catalog(catalog)
    return catalog


def clear_smartess_semantic_catalog_cache() -> None:
    """Clear cached SmartESS semantic catalog metadata."""

    load_smartess_semantic_catalog.cache_clear()


def resolve_smartess_semantic_entry(value: str) -> SmartEssSemanticEntry | None:
    """Resolve one SmartESS semantic entry by semantic key, title, or alias."""

    return load_smartess_semantic_catalog().resolve(value)


def resolve_smartess_cloud_entry(value: str) -> SmartEssSemanticEntry | None:
    """Resolve one SmartESS semantic entry by cloud-normalization titles only."""

    return load_smartess_semantic_catalog().resolve_smartess_cloud(value)


def resolve_smartess_cloud_classification(value: str) -> dict[str, object]:
    """Resolve SmartESS cloud classification metadata from the canonical catalog."""

    entry = resolve_smartess_cloud_entry(value)
    if entry is None:
        return {
            "bucket": "cloud_only",
            "source": "cloud_payload_only",
            "reason": SMARTESS_CLOUD_ONLY_REASON,
        }

    binding = entry.smartess_cloud
    bucket = binding.bucket.strip() or "cloud_only"
    source = binding.source.strip() or "cloud_payload_only"
    reason = binding.reason.strip()
    if bucket == "cloud_only" and source == "cloud_payload_only" and not reason:
        reason = SMARTESS_CLOUD_ONLY_REASON
    classification: dict[str, object] = {
        "bucket": bucket,
        "source": source,
        "reason": reason,
    }
    if binding.asset_registers:
        classification["asset_register"] = binding.asset_registers[0]
    return classification


def _parse_entry(raw: dict[str, object]) -> SmartEssSemanticEntry:
    return SmartEssSemanticEntry(
        semantic_key=str(raw["semantic_key"]).strip(),
        canonical_title=str(raw["canonical_title"]).strip(),
        title_aliases=tuple(
            str(item).strip()
            for item in raw.get("title_aliases", [])
            if str(item).strip()
        ),
        notes=str(raw.get("notes", "")).strip(),
        smartess_cloud=_parse_binding(raw.get("smartess_cloud")),
        smg_bridge=_parse_binding(raw.get("smg_bridge")),
    )


def _parse_binding(raw: object) -> SmartEssSemanticConsumerBinding:
    if not isinstance(raw, dict):
        return SmartEssSemanticConsumerBinding()
    return SmartEssSemanticConsumerBinding(
        titles=tuple(str(item).strip() for item in raw.get("titles", []) if str(item).strip()),
        title_aliases=tuple(
            str(item).strip()
            for item in raw.get("title_aliases", [])
            if str(item).strip()
        ),
        cloud_field_ids=tuple(str(item).strip() for item in raw.get("cloud_field_ids", []) if str(item).strip()),
        asset_ids=tuple(str(item).strip() for item in raw.get("asset_ids", []) if str(item).strip()),
        asset_registers=tuple(int(item) for item in raw.get("asset_registers", [])),
        bucket=str(raw.get("bucket", "")).strip(),
        source=str(raw.get("source", "")).strip(),
        reason=str(raw.get("reason", "")).strip(),
        profile_keys=tuple(str(item).strip() for item in raw.get("profile_keys", []) if str(item).strip()),
        register_keys=tuple(str(item).strip() for item in raw.get("register_keys", []) if str(item).strip()),
        measurement_keys=tuple(str(item).strip() for item in raw.get("measurement_keys", []) if str(item).strip()),
    )


def _keyed_entries(entries: tuple[SmartEssSemanticEntry, ...]) -> dict[str, SmartEssSemanticEntry]:
    keyed: dict[str, SmartEssSemanticEntry] = {}
    for entry in entries:
        if not entry.semantic_key:
            raise ValueError("smartess_semantic_catalog:invalid_semantic_key")
        if entry.semantic_key in keyed:
            raise ValueError(f"smartess_semantic_catalog:duplicate_semantic_key:{entry.semantic_key}")
        keyed[entry.semantic_key] = entry
    return keyed


def _build_alias_map(entries: dict[str, SmartEssSemanticEntry]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for semantic_key, entry in entries.items():
        aliases[semantic_key] = semantic_key
        for normalized in entry.normalized_titles:
            existing = aliases.get(normalized)
            if existing is not None and existing != semantic_key:
                raise ValueError(
                    f"smartess_semantic_catalog:duplicate_alias:{normalized}:{existing}:{semantic_key}"
                )
            aliases[normalized] = semantic_key
    return aliases


def _build_smartess_cloud_alias_map(entries: dict[str, SmartEssSemanticEntry]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for semantic_key, entry in entries.items():
        binding = entry.smartess_cloud
        if not _binding_has_cloud_metadata(binding):
            continue
        aliases[semantic_key] = semantic_key
        titles = binding.titles or (entry.canonical_title,)
        for title in (*titles, *binding.title_aliases, *binding.cloud_field_ids):
            normalized = _normalize_title(title)
            if not normalized:
                continue
            existing = aliases.get(normalized)
            if existing is not None and existing != semantic_key:
                raise ValueError(
                    f"smartess_semantic_catalog:duplicate_cloud_alias:{normalized}:{existing}:{semantic_key}"
                )
            aliases[normalized] = semantic_key
    return aliases


def _binding_has_cloud_metadata(binding: SmartEssSemanticConsumerBinding) -> bool:
    return bool(
        binding.titles
        or binding.title_aliases
        or binding.cloud_field_ids
        or binding.asset_ids
        or binding.asset_registers
        or binding.bucket
        or binding.source
        or binding.reason
        or binding.profile_keys
        or binding.register_keys
    )


def _validate_catalog(catalog: SmartEssSemanticCatalog) -> None:
    if catalog.catalog_version < 1:
        raise ValueError("smartess_semantic_catalog:invalid_catalog_version")
    if not catalog.description:
        raise ValueError("smartess_semantic_catalog:missing_description")
    if not catalog.cloud_id_semantics:
        raise ValueError("smartess_semantic_catalog:missing_cloud_id_semantics")
    if "not local modbus" not in catalog.cloud_id_semantics.lower():
        raise ValueError("smartess_semantic_catalog:missing_cloud_id_modbus_distinction")
    if not catalog.entries:
        raise ValueError("smartess_semantic_catalog:missing_entries")

    for semantic_key, entry in catalog.entries.items():
        if entry.semantic_key != semantic_key:
            raise ValueError(f"smartess_semantic_catalog:semantic_key_mismatch:{semantic_key}")
        if not entry.canonical_title:
            raise ValueError(f"smartess_semantic_catalog:missing_canonical_title:{semantic_key}")
        if not entry.normalized_titles:
            raise ValueError(f"smartess_semantic_catalog:missing_titles:{semantic_key}")
        _validate_binding(semantic_key, "smartess_cloud", entry.smartess_cloud)
        _validate_binding(semantic_key, "smg_bridge", entry.smg_bridge)


def _validate_binding(
    semantic_key: str,
    binding_name: str,
    binding: SmartEssSemanticConsumerBinding,
) -> None:
    for title in (*binding.titles, *binding.title_aliases):
        if title != str(title).strip():
            raise ValueError(
                f"smartess_semantic_catalog:invalid_title:{binding_name}:{semantic_key}"
            )
    for field_id in binding.cloud_field_ids:
        if field_id != str(field_id).strip():
            raise ValueError(
                f"smartess_semantic_catalog:invalid_cloud_field_id:{binding_name}:{semantic_key}"
            )
    for asset_id in binding.asset_ids:
        if asset_id != str(asset_id).strip():
            raise ValueError(
                f"smartess_semantic_catalog:invalid_asset_id:{binding_name}:{semantic_key}"
            )
    if binding_name == "smartess_cloud" and binding.asset_registers:
        if not binding.bucket.strip():
            raise ValueError(f"smartess_semantic_catalog:missing_bucket:{binding_name}:{semantic_key}")
        if not binding.source.strip():
            raise ValueError(f"smartess_semantic_catalog:missing_source:{binding_name}:{semantic_key}")
        if not binding.reason.strip():
            raise ValueError(f"smartess_semantic_catalog:missing_reason:{binding_name}:{semantic_key}")
