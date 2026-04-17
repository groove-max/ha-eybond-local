"""Load declarative SmartESS local protocol catalog metadata from JSON files."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


SMARTESS_PROTOCOL_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "smartess_local.json"
)


@dataclass(frozen=True, slots=True)
class SmartEssProtocolCatalogEntry:
    """One known SmartESS local protocol asset mapping entry."""

    asset_id: str
    profile_key: str
    raw_profile_name: str = ""
    raw_register_schema_name: str = ""
    profile_name: str = ""
    register_schema_name: str = ""
    proto_name: str = ""
    proto_version: str = ""
    device_addresses: tuple[int, ...] = ()
    write_one_function_code: int | None = None
    write_more_function_code: int | None = None
    system_info_function_codes: tuple[int, ...] = ()
    system_setting_function_codes: tuple[int, ...] = ()
    root_count: int = 0
    system_info_group_count: int = 0
    system_info_segment_count: int = 0
    system_setting_group_count: int = 0
    system_setting_segment_count: int = 0
    system_info_groups: tuple[str, ...] = ()
    system_setting_groups: tuple[str, ...] = ()

    @property
    def asset_name(self) -> str:
        return f"{self.asset_id}.json"


@dataclass(frozen=True, slots=True)
class SmartEssProtocolCatalog:
    """Declarative SmartESS local protocol catalog."""

    protocols: dict[str, SmartEssProtocolCatalogEntry]


@lru_cache(maxsize=None)
def load_smartess_protocol_catalog() -> SmartEssProtocolCatalog:
    """Load the built-in SmartESS local protocol catalog."""

    raw = json.loads(SMARTESS_PROTOCOL_CATALOG_PATH.read_text(encoding="utf-8"))
    entries = tuple(
        _parse_protocol_entry(item)
        for item in raw.get("protocols", [])
        if isinstance(item, dict)
    )
    return SmartEssProtocolCatalog(
        protocols={entry.asset_id: entry for entry in entries},
    )


def clear_smartess_protocol_catalog_cache() -> None:
    """Clear cached SmartESS local protocol catalog metadata."""

    load_smartess_protocol_catalog.cache_clear()


def resolve_smartess_protocol_catalog_entry(
    *,
    asset_id: str = "",
    profile_key: str = "",
) -> SmartEssProtocolCatalogEntry | None:
    """Resolve one SmartESS catalog entry by asset id or profile key."""

    normalized_asset_id = str(asset_id).strip()
    normalized_profile_key = str(profile_key).strip()
    catalog = load_smartess_protocol_catalog()

    if normalized_asset_id:
        entry = catalog.protocols.get(normalized_asset_id)
        if entry is not None:
            return entry

    if normalized_profile_key:
        for entry in catalog.protocols.values():
            if entry.profile_key == normalized_profile_key:
                return entry

    return None


def _parse_protocol_entry(raw: dict[str, object]) -> SmartEssProtocolCatalogEntry:
    return SmartEssProtocolCatalogEntry(
        asset_id=str(raw["asset_id"]).strip(),
        profile_key=str(raw["profile_key"]).strip(),
        raw_profile_name=str(raw.get("raw_profile_name", "")).strip(),
        raw_register_schema_name=str(raw.get("raw_register_schema_name", "")).strip(),
        profile_name=str(raw.get("profile_name", "")).strip(),
        register_schema_name=str(raw.get("register_schema_name", "")).strip(),
        proto_name=str(raw.get("proto_name", "")).strip(),
        proto_version=str(raw.get("proto_version", "")).strip(),
        device_addresses=tuple(int(item) for item in raw.get("device_addresses", [])),
        write_one_function_code=_optional_int(raw.get("write_one_function_code")),
        write_more_function_code=_optional_int(raw.get("write_more_function_code")),
        system_info_function_codes=tuple(
            int(item) for item in raw.get("system_info_function_codes", [])
        ),
        system_setting_function_codes=tuple(
            int(item) for item in raw.get("system_setting_function_codes", [])
        ),
        root_count=int(raw.get("root_count", 0)),
        system_info_group_count=int(raw.get("system_info_group_count", 0)),
        system_info_segment_count=int(raw.get("system_info_segment_count", 0)),
        system_setting_group_count=int(raw.get("system_setting_group_count", 0)),
        system_setting_segment_count=int(raw.get("system_setting_segment_count", 0)),
        system_info_groups=tuple(str(item) for item in raw.get("system_info_groups", [])),
        system_setting_groups=tuple(
            str(item) for item in raw.get("system_setting_groups", [])
        ),
    )


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)