"""Collector callback transport profile resolution.

The cloud endpoint family describes how a collector calls back to its server,
but the local inverter runtime can further constrain the payload transport. For
example, a known SMG runtime still uses the legacy framed EyeBond tunnel even
when the collector's original cloud endpoint belongs to the SmartESS AT family.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ..const import (
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_DRIVER_HINT,
    DRIVER_HINT_AUTO,
)
from ..metadata.collector_cloud_profile_catalog_loader import (
    load_collector_cloud_profile_catalog,
    resolve_collector_cloud_identity_strategy,
    resolve_collector_cloud_raw_passthrough_bootstrap,
    resolve_collector_cloud_raw_passthrough_frame_format,
    resolve_collector_cloud_raw_passthrough_min_interval_ms,
    resolve_collector_cloud_session_protocol,
)
from .cloud_family import collector_cloud_family_observation_from_endpoint
from .capabilities import (
    COLLECTOR_KIND_ENTRY_KEY,
    COLLECTOR_KIND_ESP_EYBOND_BRIDGE,
)


CONFIRMED_COLLECTOR_SESSION_PROTOCOLS: frozenset[str] = frozenset(
    {"at_text", "eybond_framed"}
)


@dataclass(frozen=True, slots=True)
class CollectorTransportProfile:
    """Resolved callback transport metadata for one collector runtime."""

    cloud_family: str
    runtime_owner_key: str
    session_protocol: str
    identity_strategy: str
    raw_passthrough_bootstrap: str
    raw_passthrough_frame_format: str
    raw_passthrough_min_interval_ms: int


def known_collector_cloud_family(value: object) -> str:
    """Return a known collector cloud family key or an empty string."""

    family = str(value or "").strip().lower()
    if not family:
        return ""
    if family in load_collector_cloud_profile_catalog().profiles:
        return family
    return ""


def normalize_collector_session_protocol(value: object) -> str:
    """Return a confirmed collector callback protocol or an empty string.

    Listener byte-shape diagnostics may temporarily report broader values such
    as ``eybond_framed_or_binary`` before routing/ownership is known. Those
    values are useful diagnostics, but they are not safe to persist as runtime
    protocol decisions.
    """

    protocol = str(value or "").strip().lower()
    if protocol in CONFIRMED_COLLECTOR_SESSION_PROTOCOLS:
        return protocol
    return ""


def collector_session_protocol_from_inventory_state(
    *,
    state: object,
    protocol_shape: object,
) -> str:
    """Return the best confirmed protocol from a passive listener session."""

    normalized_state = str(state or "").strip().lower()
    if normalized_state == "routed_at_text":
        return "at_text"
    if normalized_state == "routed_framed":
        return "eybond_framed"
    return normalize_collector_session_protocol(protocol_shape)


def runtime_owner_key_from_entry_context(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> str:
    """Return the best known local inverter runtime owner from config context."""

    for source in (options, data):
        driver_hint = str(source.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO) or DRIVER_HINT_AUTO).strip().lower()
        if driver_hint and driver_hint != DRIVER_HINT_AUTO:
            return driver_hint

    for source in (options, data):
        snapshot = source.get("effective_metadata_snapshot")
        if not isinstance(snapshot, Mapping):
            continue
        owner_key = str(snapshot.get("effective_owner_key", "") or "").strip().lower()
        if owner_key:
            return owner_key

    return ""


def collector_cloud_family_from_entry_context(
    data: Mapping[str, object],
    options: Mapping[str, object],
    *,
    extra_endpoints: tuple[object, ...] = (),
) -> str:
    """Resolve collector cloud family from durable and runtime entry context."""

    for source in (data, options):
        family = known_collector_cloud_family(source.get(CONF_COLLECTOR_CLOUD_FAMILY, ""))
        if family:
            return family

    for source in (options, data):
        family = known_collector_cloud_family(
            source.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY, "")
        )
        if family:
            return family

    for endpoint in extra_endpoints:
        family = _known_family_from_endpoint(endpoint)
        if family:
            return family

    for source in (options, data):
        family = _known_family_from_endpoint(
            source.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT, "")
        )
        if family:
            return family

    return ""


def resolve_collector_transport_profile(
    *,
    cloud_family: object,
    runtime_owner_key: object = "",
    virtual_bridge: bool = False,
) -> CollectorTransportProfile:
    """Resolve callback session protocol and identity strategy.

    ``virtual_bridge`` is collector metadata/capability only. It must not choose
    inverter payload transport: the live SessionHandle adapter negotiation owns
    that decision.

    The ``runtime_owner_key`` (the local inverter driver key) is retained ONLY as
    a diagnostic provenance field; it must never influence ``session_protocol``,
    ``identity_strategy``, or any wire/adapter selection. Driver keys deciding
    transport was the exact coupling removed in the phase-2 refactor. The cloud
    family remains a legacy pre-live-observation hint that a live SessionHandle
    always overrides.
    """

    normalized_family = known_collector_cloud_family(cloud_family)
    normalized_owner = str(runtime_owner_key or "").strip().lower()
    return CollectorTransportProfile(
        cloud_family=normalized_family,
        runtime_owner_key=normalized_owner,
        session_protocol=resolve_collector_cloud_session_protocol(normalized_family),
        identity_strategy=resolve_collector_cloud_identity_strategy(normalized_family),
        raw_passthrough_bootstrap=resolve_collector_cloud_raw_passthrough_bootstrap(
            normalized_family
        ),
        raw_passthrough_frame_format=resolve_collector_cloud_raw_passthrough_frame_format(
            normalized_family
        ),
        raw_passthrough_min_interval_ms=resolve_collector_cloud_raw_passthrough_min_interval_ms(
            normalized_family
        ),
    )


# A live-observed callback session protocol IMPLIES its transport profile. This
# protocol -> (identity strategy, raw-passthrough bootstrap/frame/interval) map is
# transport POLICY and lives here, in the transport-profile authority, never in
# the runtime coordinator. ``reset_min_interval`` selects between keeping the
# cloud-family base interval (AT text) and clearing it (framed).
_OBSERVED_SESSION_PROTOCOL_PROFILES: dict[str, dict[str, object]] = {
    "at_text": {
        "identity_strategy": "at_dtupn",
        "raw_passthrough_bootstrap": "uart_write_same_value",
        "raw_passthrough_frame_format": "transparent",
        "reset_min_interval": False,
    },
    "eybond_framed": {
        "identity_strategy": "framed_heartbeat_then_fc2_pn",
        "raw_passthrough_bootstrap": "",
        "raw_passthrough_frame_format": "",
        "reset_min_interval": True,
    },
}


def apply_observed_collector_session_protocol(
    base: CollectorTransportProfile,
    observed_protocol: object,
) -> CollectorTransportProfile:
    """Return the transport profile a LIVE-observed session protocol implies.

    Live observation is always stronger than the cloud-family bootstrap hint, so
    a confirmed observed protocol replaces the base profile's protocol-specific
    fields. Returns ``base`` unchanged when there is no observation, the
    observation matches the base, or the observed protocol is unknown. The
    runtime coordinator passes the observed protocol; the map itself is owned
    here so the neutral runtime holds no protocol->policy table.
    """

    protocol = str(observed_protocol or "").strip().lower()
    if not protocol or protocol == base.session_protocol:
        return base
    override = _OBSERVED_SESSION_PROTOCOL_PROFILES.get(protocol)
    if override is None:
        return base
    return CollectorTransportProfile(
        cloud_family=base.cloud_family,
        runtime_owner_key=base.runtime_owner_key,
        session_protocol=protocol,
        identity_strategy=str(override["identity_strategy"]),
        raw_passthrough_bootstrap=str(override["raw_passthrough_bootstrap"]),
        raw_passthrough_frame_format=str(override["raw_passthrough_frame_format"]),
        raw_passthrough_min_interval_ms=(
            0 if override["reset_min_interval"] else base.raw_passthrough_min_interval_ms
        ),
    )


def framed_collector_transport_profile(
    *,
    cloud_family: object = "",
    runtime_owner_key: object = "",
) -> CollectorTransportProfile:
    """Return the framed FC transport profile (the esp-bridge session)."""

    return CollectorTransportProfile(
        cloud_family=known_collector_cloud_family(cloud_family),
        runtime_owner_key=str(runtime_owner_key or "").strip().lower(),
        session_protocol="eybond_framed",
        identity_strategy="framed_heartbeat_then_fc2_pn",
        raw_passthrough_bootstrap="",
        raw_passthrough_frame_format="",
        raw_passthrough_min_interval_ms=0,
    )


def entry_context_is_virtual_bridge(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> bool:
    for source in (options, data):
        collector_kind = str(source.get(COLLECTOR_KIND_ENTRY_KEY, "") or "").strip()
        if collector_kind == COLLECTOR_KIND_ESP_EYBOND_BRIDGE:
            return True
        if bool(source.get("collector_virtual_bridge")):
            return True
        bridge_kind = str(source.get("collector_bridge_kind", "") or "").strip().lower()
        if bridge_kind == "esp-collector":
            return True
    return False


def resolve_collector_transport_profile_from_entry_context(
    data: Mapping[str, object],
    options: Mapping[str, object],
    *,
    extra_endpoints: tuple[object, ...] = (),
) -> CollectorTransportProfile:
    """Resolve one transport profile from config-entry style mappings."""

    cloud_family = collector_cloud_family_from_entry_context(
        data,
        options,
        extra_endpoints=extra_endpoints,
    )
    runtime_owner_key = runtime_owner_key_from_entry_context(data, options)
    return resolve_collector_transport_profile(
        cloud_family=cloud_family,
        runtime_owner_key=runtime_owner_key,
        virtual_bridge=entry_context_is_virtual_bridge(data, options),
    )


def _known_family_from_endpoint(endpoint: object) -> str:
    observation = collector_cloud_family_observation_from_endpoint(endpoint)
    return known_collector_cloud_family(observation.family)
