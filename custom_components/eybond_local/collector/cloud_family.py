"""Classification helpers for collector cloud callback families."""

from __future__ import annotations

from dataclasses import dataclass

from ..collector_endpoint import inspect_collector_server_endpoint
from ..metadata.collector_cloud_profile_catalog_loader import (
    resolve_collector_cloud_default_host,
    resolve_collector_cloud_family_by_host,
    resolve_collector_cloud_family_by_port,
)

COLLECTOR_CLOUD_FAMILY_UNKNOWN = "unknown"
COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY = "legacy_binary"
COLLECTOR_CLOUD_FAMILY_SMARTESS_AT = "smartess_at"

COLLECTOR_CLOUD_FAMILY_SOURCE_TRANSPORT_SNIFF = "transport_sniff"
COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT = "explicit_endpoint_port"
COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST = "endpoint_host"

_CONFIDENCE_RANK = {
    "": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}

_SOURCE_RANK = {
    "": 0,
    COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST: 1,
    COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT: 1,
    COLLECTOR_CLOUD_FAMILY_SOURCE_TRANSPORT_SNIFF: 2,
}


@dataclass(frozen=True, slots=True)
class CollectorCloudFamilyObservation:
    """One normalized collector cloud-family signal with provenance."""

    family: str = COLLECTOR_CLOUD_FAMILY_UNKNOWN
    source: str = ""
    confidence: str = ""

    @property
    def known(self) -> bool:
        return self.family not in {"", COLLECTOR_CLOUD_FAMILY_UNKNOWN}


def collector_cloud_family_observation_from_transport_sniff(
    *,
    at_traffic: bool,
) -> CollectorCloudFamilyObservation:
    """Classify one collector session from the first sniffed callback chunk."""

    family = (
        COLLECTOR_CLOUD_FAMILY_SMARTESS_AT
        if at_traffic
        else COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY
    )
    return CollectorCloudFamilyObservation(
        family=family,
        source=COLLECTOR_CLOUD_FAMILY_SOURCE_TRANSPORT_SNIFF,
        confidence="high",
    )


def collector_cloud_family_observation_from_endpoint(
    endpoint: object,
) -> CollectorCloudFamilyObservation:
    """Best-effort classification from one callback endpoint shape."""

    try:
        parsed = inspect_collector_server_endpoint(
            str(endpoint or ""),
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return CollectorCloudFamilyObservation()

    normalized_host = str(parsed.host or "").strip().lower()

    family = resolve_collector_cloud_family_by_host(normalized_host)
    if family:
        return CollectorCloudFamilyObservation(
            family=family,
            source=COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST,
            confidence="high" if parsed.has_explicit_port else "low",
        )

    if not parsed.has_explicit_port:
        return CollectorCloudFamilyObservation()

    family = resolve_collector_cloud_family_by_port(parsed.port)
    if family:
        return CollectorCloudFamilyObservation(
            family=family,
            source=COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT,
            confidence="medium",
        )

    return CollectorCloudFamilyObservation()


def collector_cloud_family_observation_from_collector(
    collector: object | None,
) -> CollectorCloudFamilyObservation:
    """Read one existing collector-side family observation."""

    if collector is None:
        return CollectorCloudFamilyObservation()

    family = str(getattr(collector, "collector_cloud_family", "") or "").strip()
    if not family:
        return CollectorCloudFamilyObservation()

    return CollectorCloudFamilyObservation(
        family=family,
        source=str(getattr(collector, "collector_cloud_family_source", "") or "").strip(),
        confidence=str(getattr(collector, "collector_cloud_family_confidence", "") or "").strip(),
    )


def default_collector_cloud_host(cloud_family: str) -> str:
    """Return a known default upstream cloud host for one collector family."""

    return resolve_collector_cloud_default_host(cloud_family)


def select_preferred_collector_cloud_family(
    *observations: CollectorCloudFamilyObservation,
) -> CollectorCloudFamilyObservation:
    """Return the strongest known family observation from the given candidates."""

    selected = CollectorCloudFamilyObservation()
    selected_rank = (-1, -1)

    for observation in observations:
        if not observation.known:
            continue
        rank = (
            _CONFIDENCE_RANK.get(observation.confidence, 0),
            _SOURCE_RANK.get(observation.source, 0),
        )
        if rank > selected_rank:
            selected = observation
            selected_rank = rank

    return selected


def apply_collector_cloud_family_observation(
    collector: object | None,
    observation: CollectorCloudFamilyObservation,
) -> None:
    """Persist the preferred family observation onto one mutable collector object."""

    if collector is None:
        return

    selected = select_preferred_collector_cloud_family(
        collector_cloud_family_observation_from_collector(collector),
        observation,
    )
    if not selected.known:
        return

    setattr(collector, "collector_cloud_family", selected.family)
    setattr(collector, "collector_cloud_family_source", selected.source)
    setattr(collector, "collector_cloud_family_confidence", selected.confidence)
