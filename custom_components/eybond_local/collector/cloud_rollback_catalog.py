"""Catalog adapter for CP2B.2 cloud rollback endpoint selection.

Turns the EXISTING collector cloud profile catalog into writable rollback
endpoint choices and builds the typed, immutable
``CloudRollbackSelection`` objects the transition authority consumes.

Reuses the existing catalog loader, the existing endpoint formatter and the
existing provider-neutral parser -- it adds NO second parser and never infers a
cloud endpoint from a hostname shape, a collector kind or a peer IP. The user's
explicit choice (a confirmed CP2B.1 candidate, a catalog key, or a manually
entered endpoint) is the only authority; a cloud family only shapes the write
format of an endpoint the user has already chosen.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..collector_endpoint import (
    format_collector_server_endpoint_for_cloud_profile,
    normalize_collector_server_endpoint,
)
from ..connection.strategy_transition_context import (
    CLOUD_PROVENANCE_EXPLICIT_USER,
    ROLLBACK_SELECTION_CATALOG,
    ROLLBACK_SELECTION_CONFIRMED_CANDIDATE,
    ROLLBACK_SELECTION_MANUAL,
    CloudRollbackEndpoint,
    CloudRollbackSelection,
)
from ..metadata.collector_cloud_profile_catalog_loader import (
    load_collector_cloud_profile_catalog,
)


@dataclass(frozen=True, slots=True)
class CloudRollbackCatalogOption:
    """One writable cloud rollback choice presented to the user."""

    key: str
    label: str
    provider: str
    endpoint: str


def writable_cloud_rollback_catalog_options() -> tuple[CloudRollbackCatalogOption, ...]:
    """Every catalog profile that can form a writable endpoint, with a stable key.

    A profile is offered only when it has a default host AND the formatted
    endpoint is a valid writable rollback target (via ``CloudRollbackEndpoint``).
    The write format (host_only / host_port / host_port_protocol) is the
    profile's own -- it shapes an endpoint the user explicitly picks, never
    auto-selects one.
    """

    catalog = load_collector_cloud_profile_catalog()
    options: list[CloudRollbackCatalogOption] = []
    for family, profile in sorted(catalog.profiles.items()):
        if not profile.default_host:
            continue
        try:
            endpoint = format_collector_server_endpoint_for_cloud_profile(
                server_host=profile.default_host,
                cloud_family=family,
            )
        except ValueError:
            continue
        try:
            CloudRollbackEndpoint(endpoint, CLOUD_PROVENANCE_EXPLICIT_USER)
        except (ValueError, TypeError):
            continue
        options.append(
            CloudRollbackCatalogOption(
                key=family,
                label=profile.label or family,
                provider=profile.provider,
                endpoint=endpoint,
            )
        )
    return tuple(options)


def cloud_rollback_selection_from_catalog_key(key: object) -> CloudRollbackSelection | None:
    """Build a catalog selection from a stable key; ``None`` for an unknown/stale key.

    Fail-closed: an arbitrary, removed or non-string key never yields a selection.
    """

    if type(key) is not str or not key:
        return None
    for option in writable_cloud_rollback_catalog_options():
        if option.key == key:
            try:
                return CloudRollbackSelection(
                    endpoint=CloudRollbackEndpoint(
                        option.endpoint, CLOUD_PROVENANCE_EXPLICIT_USER
                    ),
                    selection_kind=ROLLBACK_SELECTION_CATALOG,
                    catalog_profile_key=key,
                    user_confirmed=True,
                )
            except (ValueError, TypeError):
                return None
    return None


def cloud_rollback_selection_from_manual(endpoint_raw: object) -> CloudRollbackSelection | None:
    """Build a manual selection from a raw endpoint string; ``None`` if malformed.

    Validation + normalization go through the EXISTING provider-neutral parser
    (shape-preserving), so ``host``, ``host,port`` and ``host,port,protocol`` are
    all accepted and a malformed value is a caller-handleable ``None`` (never a
    raised 500).
    """

    if type(endpoint_raw) is not str:
        return None
    raw = endpoint_raw.strip()
    if not raw:
        return None
    try:
        normalized = normalize_collector_server_endpoint(
            raw,
            require_explicit_port=False,
            require_explicit_protocol=False,
            preserve_shape=True,
        )
    except ValueError:
        return None
    try:
        return CloudRollbackSelection(
            endpoint=CloudRollbackEndpoint(normalized, CLOUD_PROVENANCE_EXPLICIT_USER),
            selection_kind=ROLLBACK_SELECTION_MANUAL,
            user_confirmed=True,
        )
    except (ValueError, TypeError):
        return None


def cloud_rollback_selection_from_candidate(
    endpoint: object,
) -> CloudRollbackSelection | None:
    """Build a confirmed-candidate selection from a CP2B.1 resolver endpoint.

    ``None`` unless ``endpoint`` is a KNOWN ``CloudRollbackEndpoint`` whose
    provenance is one of the resolver candidate provenances (the model rejects
    the reserved explicit-user provenance here).
    """

    if type(endpoint) is not CloudRollbackEndpoint or not endpoint.known:
        return None
    try:
        return CloudRollbackSelection(
            endpoint=endpoint,
            selection_kind=ROLLBACK_SELECTION_CONFIRMED_CANDIDATE,
            candidate_provenance=endpoint.provenance,
            user_confirmed=True,
        )
    except (ValueError, TypeError):
        return None


ROLLBACK_SELECTION_VALID = ""
ROLLBACK_SELECTION_INVALID = "invalid"
ROLLBACK_SELECTION_STALE = "stale"


def validate_cloud_rollback_selection(
    selection: object,
    *,
    confirmed_candidate: object = None,
) -> str:
    """Rebuild and verify a selection at the execution trust boundary.

    A frozen dataclass proves only its own shape.  It does not prove that a
    catalog key names the endpoint carried beside it, or that a confirmed
    candidate is still the fact the read model currently exposes.  Rebuilding
    through the public constructors closes both substitution and TOCTOU holes.
    """

    if type(selection) is not CloudRollbackSelection:
        return ROLLBACK_SELECTION_INVALID

    rebuilt: CloudRollbackSelection | None
    if selection.selection_kind == ROLLBACK_SELECTION_CATALOG:
        rebuilt = cloud_rollback_selection_from_catalog_key(
            selection.catalog_profile_key
        )
    elif selection.selection_kind == ROLLBACK_SELECTION_MANUAL:
        rebuilt = cloud_rollback_selection_from_manual(selection.endpoint_value)
    elif selection.selection_kind == ROLLBACK_SELECTION_CONFIRMED_CANDIDATE:
        if (
            type(confirmed_candidate) is not CloudRollbackEndpoint
            or not confirmed_candidate.known
        ):
            return ROLLBACK_SELECTION_STALE
        rebuilt = cloud_rollback_selection_from_candidate(confirmed_candidate)
        if rebuilt != selection:
            return ROLLBACK_SELECTION_STALE
    else:  # defensive: the model currently makes this unreachable
        return ROLLBACK_SELECTION_INVALID

    if rebuilt is None or rebuilt != selection:
        return ROLLBACK_SELECTION_INVALID
    return ROLLBACK_SELECTION_VALID
