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
    provider: str
    label: str
    default_host: str
    default_port: int
    default_protocol: str
    known_hosts: tuple[str, ...]
    known_ports: tuple[int, ...]
    endpoint_write_format: str
    session_protocol: str
    identity_strategy: str
    raw_passthrough_bootstrap: str
    raw_passthrough_frame_format: str
    raw_passthrough_min_interval_ms: int


@dataclass(frozen=True, slots=True)
class CollectorCloudProfileCatalog:
    """Declarative collector cloud profile catalog with lookup indexes."""

    profiles: dict[str, CollectorCloudProfileCatalogEntry]
    providers: dict[str, str]
    families_by_host: dict[str, str]
    families_by_port: dict[int, str]
    default_hosts: dict[str, str]
    default_ports: dict[str, int]
    default_protocols: dict[str, str]
    endpoint_write_formats: dict[str, str]
    session_protocols: dict[str, str]
    identity_strategies: dict[str, str]
    raw_passthrough_bootstraps: dict[str, str]
    raw_passthrough_frame_formats: dict[str, str]
    raw_passthrough_min_interval_ms: dict[str, int]


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
    providers: dict[str, str] = {}
    families_by_host: dict[str, str] = {}
    families_by_port: dict[int, str] = {}
    default_hosts: dict[str, str] = {}
    default_ports: dict[str, int] = {}
    default_protocols: dict[str, str] = {}
    endpoint_write_formats: dict[str, str] = {}
    session_protocols: dict[str, str] = {}
    identity_strategies: dict[str, str] = {}
    raw_passthrough_bootstraps: dict[str, str] = {}
    raw_passthrough_frame_formats: dict[str, str] = {}
    raw_passthrough_min_interval_ms: dict[str, int] = {}

    for entry in entries:
        family = entry.family
        if not family:
            continue

        profiles[family] = entry
        if entry.provider:
            providers[family] = entry.provider
        if entry.default_host:
            default_hosts[family] = entry.default_host
        if entry.default_port:
            default_ports[family] = entry.default_port
        if entry.default_protocol:
            default_protocols[family] = entry.default_protocol
        if entry.endpoint_write_format:
            endpoint_write_formats[family] = entry.endpoint_write_format
        if entry.session_protocol:
            session_protocols[family] = entry.session_protocol
        if entry.identity_strategy:
            identity_strategies[family] = entry.identity_strategy
        if entry.raw_passthrough_bootstrap:
            raw_passthrough_bootstraps[family] = entry.raw_passthrough_bootstrap
        if entry.raw_passthrough_frame_format:
            raw_passthrough_frame_formats[family] = entry.raw_passthrough_frame_format
        if entry.raw_passthrough_min_interval_ms > 0:
            raw_passthrough_min_interval_ms[family] = entry.raw_passthrough_min_interval_ms

        for host in entry.known_hosts:
            if host:
                families_by_host.setdefault(host, family)

        for port in entry.known_ports:
            families_by_port.setdefault(port, family)

    return CollectorCloudProfileCatalog(
        profiles=profiles,
        providers=providers,
        families_by_host=families_by_host,
        families_by_port=families_by_port,
        default_hosts=default_hosts,
        default_ports=default_ports,
        default_protocols=default_protocols,
        endpoint_write_formats=endpoint_write_formats,
        session_protocols=session_protocols,
        identity_strategies=identity_strategies,
        raw_passthrough_bootstraps=raw_passthrough_bootstraps,
        raw_passthrough_frame_formats=raw_passthrough_frame_formats,
        raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
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


def resolve_collector_cloud_provider(cloud_family: object) -> str:
    """Resolve the cloud-account provider for one collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.providers.get(normalized_family, "")


def resolve_collector_cloud_default_host(cloud_family: object) -> str:
    """Resolve one known default cloud host for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.default_hosts.get(normalized_family, "")


def resolve_collector_cloud_default_port(cloud_family: object) -> int:
    """Resolve one known default cloud port for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return 0

    catalog = load_collector_cloud_profile_catalog()
    return int(catalog.default_ports.get(normalized_family, 0) or 0)


def resolve_collector_cloud_default_protocol(cloud_family: object) -> str:
    """Resolve one known default cloud protocol for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.default_protocols.get(normalized_family, "")


def resolve_collector_cloud_endpoint_write_format(cloud_family: object) -> str:
    """Resolve the CLDSRVHOST1 write shape for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.endpoint_write_formats.get(normalized_family, "")


def resolve_collector_cloud_session_protocol(cloud_family: object) -> str:
    """Resolve the callback session protocol for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.session_protocols.get(normalized_family, "")


def resolve_collector_cloud_identity_strategy(cloud_family: object) -> str:
    """Resolve the collector identity strategy for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.identity_strategies.get(normalized_family, "")


def resolve_collector_cloud_raw_passthrough_bootstrap(cloud_family: object) -> str:
    """Resolve the raw inverter payload bootstrap mode for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.raw_passthrough_bootstraps.get(normalized_family, "")


def resolve_collector_cloud_raw_passthrough_frame_format(cloud_family: object) -> str:
    """Resolve the raw inverter payload frame format for a collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return ""

    catalog = load_collector_cloud_profile_catalog()
    return catalog.raw_passthrough_frame_formats.get(normalized_family, "")


def resolve_collector_cloud_raw_passthrough_min_interval_ms(cloud_family: object) -> int:
    """Resolve the minimum delay between raw passthrough requests."""

    normalized_family = str(cloud_family or "").strip().lower()
    if not normalized_family:
        return 0

    catalog = load_collector_cloud_profile_catalog()
    return int(catalog.raw_passthrough_min_interval_ms.get(normalized_family, 0) or 0)


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
        provider=str(raw.get("provider", "")).strip().lower(),
        label=str(raw.get("label", "")).strip(),
        default_host=str(raw.get("default_host", "")).strip().lower(),
        default_port=int(raw.get("default_port", 0) or 0) if _is_int_like(raw.get("default_port", 0)) else 0,
        default_protocol=str(raw.get("default_protocol", "")).strip().upper(),
        known_hosts=known_hosts,
        known_ports=known_ports,
        endpoint_write_format=str(raw.get("endpoint_write_format", "")).strip().lower(),
        session_protocol=str(raw.get("session_protocol", "")).strip().lower(),
        identity_strategy=str(raw.get("identity_strategy", "")).strip().lower(),
        raw_passthrough_bootstrap=str(raw.get("raw_passthrough_bootstrap", "")).strip().lower(),
        raw_passthrough_frame_format=str(
            raw.get("raw_passthrough_frame_format", "")
        ).strip().lower(),
        raw_passthrough_min_interval_ms=(
            int(raw.get("raw_passthrough_min_interval_ms", 0) or 0)
            if _is_int_like(raw.get("raw_passthrough_min_interval_ms", 0))
            else 0
        ),
    )


def _is_int_like(value: object) -> bool:
    try:
        int(value)
    except (TypeError, ValueError):
        return False
    return True
