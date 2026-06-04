"""Load declarative collector cloud profile catalog metadata from JSON files."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


COLLECTOR_CLOUD_PROFILE_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "collector_cloud_profiles.json"
)


@dataclass(frozen=True, slots=True)
class CollectorCloudProfileCatalogEntry:
    """One declarative collector cloud profile entry."""

    family: str
    default_host: str
    known_hosts: tuple[str, ...]
    known_ports: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CollectorCloudProfileCatalog:
    """Declarative collector cloud profile catalog with lookup indexes."""

    profiles: dict[str, CollectorCloudProfileCatalogEntry]
    families_by_host: dict[str, str]
    families_by_port: dict[int, str]
    default_hosts: dict[str, str]


@lru_cache(maxsize=None)
def load_collector_cloud_profile_catalog() -> CollectorCloudProfileCatalog:
    """Load the built-in collector cloud profile catalog."""

    raw = json.loads(COLLECTOR_CLOUD_PROFILE_CATALOG_PATH.read_text(encoding="utf-8"))
    entries = tuple(
        _parse_profile_entry(item)
        for item in raw.get("profiles", [])
        if isinstance(item, dict)
    )

    profiles: dict[str, CollectorCloudProfileCatalogEntry] = {}
    families_by_host: dict[str, str] = {}
    families_by_port: dict[int, str] = {}
    default_hosts: dict[str, str] = {}

    for entry in entries:
        family = entry.family
        if not family:
            continue

        profiles[family] = entry
        if entry.default_host:
            default_hosts[family] = entry.default_host

        for host in entry.known_hosts:
            if host:
                families_by_host.setdefault(host, family)

        for port in entry.known_ports:
            families_by_port.setdefault(port, family)

    return CollectorCloudProfileCatalog(
        profiles=profiles,
        families_by_host=families_by_host,
        families_by_port=families_by_port,
        default_hosts=default_hosts,
    )


def clear_collector_cloud_profile_catalog_cache() -> None:
    """Clear cached collector cloud profile catalog metadata."""

    load_collector_cloud_profile_catalog.cache_clear()


def resolve_collector_cloud_family_by_host(host: object) -> str:
    """Resolve one known collector cloud family by endpoint host."""

    normalized_host = str(host or "").strip().lower()
    if not normalized_host:
        return ""
    catalog = load_collector_cloud_profile_catalog()
    return catalog.families_by_host.get(normalized_host, "")


def resolve_collector_cloud_family_by_port(port: object) -> str:
    """Resolve one known collector cloud family by endpoint port."""

    try:
        normalized_port = int(port)
    except (TypeError, ValueError):
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.families_by_port.get(normalized_port, "")


def resolve_collector_cloud_default_host(cloud_family: object) -> str:
    """Resolve one known default cloud host for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.default_hosts.get(normalized_family, "")


def _parse_profile_entry(raw: dict[str, object]) -> CollectorCloudProfileCatalogEntry:
    known_hosts = tuple(
        str(item).strip().lower()
        for item in raw.get("known_hosts", [])
        if str(item).strip()
    )
    known_ports = tuple(
        int(item)
        for item in raw.get("known_ports", [])
        if _is_int_like(item)
    )
    return CollectorCloudProfileCatalogEntry(
        family=str(raw.get("family", "")).strip().lower(),
        default_host=str(raw.get("default_host", "")).strip().lower(),
        known_hosts=known_hosts,
        known_ports=known_ports,
    )


def _is_int_like(value: object) -> bool:
    try:
        int(value)
    except (TypeError, ValueError):
        return False
    return True
