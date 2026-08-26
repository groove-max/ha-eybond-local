"""Typed readiness projection for support-evidence acquisition.

Support tools exist to explain an unsupported device, so their availability
must never depend on that device already having an inverter driver.  This
module keeps the three operation classes separate:

* metadata-only cloud reads need only a durable collector identity;
* proxy capture additionally needs a known vendor-cloud side and a safe route;
* active control correlation has the same route precondition and adds its own
  consent/preflight inside the active workflow.

The projection is transient and read-only.  It neither persists a collector
kind nor turns cloud evidence into local driver authority.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..collector_identity import validated_collector_pn


SUPPORT_BLOCKER_COLLECTOR_IDENTITY = "collector_identity_unavailable"
SUPPORT_BLOCKER_LOCAL_BRIDGE = "collector_has_no_vendor_cloud_side"
SUPPORT_BLOCKER_CLOUD_PROVIDER = "cloud_provider_unavailable"
SUPPORT_BLOCKER_OPERATING_PROFILE = "operating_profile_requires_cloud_and_ha"

SUPPORT_ACQUISITION_BLOCKERS = frozenset(
    {
        "",
        SUPPORT_BLOCKER_COLLECTOR_IDENTITY,
        SUPPORT_BLOCKER_LOCAL_BRIDGE,
        SUPPORT_BLOCKER_CLOUD_PROVIDER,
        SUPPORT_BLOCKER_OPERATING_PROFILE,
    }
)


def _strict_bool(value: object) -> bool:
    if type(value) is not bool:
        raise TypeError("support_acquisition_boolean_invalid")
    return value


def _strict_provider(value: object) -> str:
    if type(value) is not str:
        raise TypeError("support_acquisition_provider_invalid")
    if value != value.strip():
        raise ValueError("support_acquisition_provider_invalid")
    return value


@dataclass(frozen=True, slots=True)
class SupportOperationReadiness:
    """One operation's visibility and immediate start permission."""

    visible: bool
    can_start: bool
    blocker: str

    def __post_init__(self) -> None:
        visible = _strict_bool(self.visible)
        can_start = _strict_bool(self.can_start)
        if type(self.blocker) is not str:
            raise TypeError("support_acquisition_blocker_invalid")
        if (
            self.blocker != self.blocker.strip()
            or self.blocker not in SUPPORT_ACQUISITION_BLOCKERS
        ):
            raise ValueError("support_acquisition_blocker_invalid")
        if can_start and (not visible or self.blocker):
            raise ValueError("support_acquisition_readiness_contradiction")
        if not can_start and not self.blocker:
            raise ValueError("support_acquisition_blocker_required")


@dataclass(frozen=True, slots=True)
class SupportAcquisitionReadiness:
    """Operation readiness independent of local inverter recognition."""

    collector_identified: bool
    inverter_identified: bool
    cloud_metadata_read: SupportOperationReadiness
    proxy_capture: SupportOperationReadiness
    active_control_learning: SupportOperationReadiness

    def __post_init__(self) -> None:
        _strict_bool(self.collector_identified)
        _strict_bool(self.inverter_identified)
        for operation in (
            self.cloud_metadata_read,
            self.proxy_capture,
            self.active_control_learning,
        ):
            if type(operation) is not SupportOperationReadiness:
                raise TypeError("support_acquisition_operation_invalid")


def _blocked(reason: str, *, visible: bool = False) -> SupportOperationReadiness:
    return SupportOperationReadiness(
        visible=visible,
        can_start=False,
        blocker=reason,
    )


def _ready() -> SupportOperationReadiness:
    return SupportOperationReadiness(visible=True, can_start=True, blocker="")


def resolve_support_acquisition_readiness(
    *,
    collector_pn: object,
    inverter_identified: bool,
    virtual_bridge: bool,
    cloud_provider: object,
    cloud_provider_supported: bool,
    cloud_route_allowed: bool,
) -> SupportAcquisitionReadiness:
    """Resolve support operations from collector facts, never driver support."""

    inverter_known = _strict_bool(inverter_identified)
    bridge = _strict_bool(virtual_bridge)
    provider_supported = _strict_bool(cloud_provider_supported)
    route_allowed = _strict_bool(cloud_route_allowed)
    provider = _strict_provider(cloud_provider)
    collector_identified = bool(
        type(collector_pn) is str
        and collector_pn
        and validated_collector_pn(collector_pn) == collector_pn
    )

    if not collector_identified:
        blocker = SUPPORT_BLOCKER_COLLECTOR_IDENTITY
        metadata = _blocked(blocker)
        proxy = _blocked(blocker)
        active = _blocked(blocker)
    elif bridge:
        blocker = SUPPORT_BLOCKER_LOCAL_BRIDGE
        metadata = _blocked(blocker)
        proxy = _blocked(blocker)
        active = _blocked(blocker)
    else:
        # A provider can be selected explicitly for a metadata-only API read;
        # absence of a catalog-resolved provider must not hide that safe path.
        metadata = _ready()
        if not provider or not provider_supported:
            proxy = _blocked(SUPPORT_BLOCKER_CLOUD_PROVIDER)
            active = _blocked(SUPPORT_BLOCKER_CLOUD_PROVIDER)
        elif not route_allowed:
            proxy = _blocked(SUPPORT_BLOCKER_OPERATING_PROFILE, visible=True)
            active = _blocked(SUPPORT_BLOCKER_OPERATING_PROFILE, visible=True)
        else:
            proxy = _ready()
            active = _ready()

    return SupportAcquisitionReadiness(
        collector_identified=collector_identified,
        inverter_identified=inverter_known,
        cloud_metadata_read=metadata,
        proxy_capture=proxy,
        active_control_learning=active,
    )


__all__ = [
    "SUPPORT_ACQUISITION_BLOCKERS",
    "SUPPORT_BLOCKER_CLOUD_PROVIDER",
    "SUPPORT_BLOCKER_COLLECTOR_IDENTITY",
    "SUPPORT_BLOCKER_LOCAL_BRIDGE",
    "SUPPORT_BLOCKER_OPERATING_PROFILE",
    "SupportAcquisitionReadiness",
    "SupportOperationReadiness",
    "resolve_support_acquisition_readiness",
]
