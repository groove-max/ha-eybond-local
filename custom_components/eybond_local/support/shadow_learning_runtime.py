"""Typed read boundary for shadow-learning runtime state.

The options flow must never navigate through coordinator/runtime/link private
objects to inspect an active learning route.  These immutable models are the
single public projection the coordinator exposes across that boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .cloud_evidence import CloudEvidenceRecord
from .shadow_learning import ShadowWriteObservation


@dataclass(frozen=True, slots=True)
class ShadowLearningRouteStatus:
    """Sanitized live status of the active shadow-learning route."""

    running: bool = False
    collector_connected: bool = False
    collector_protocol_ingress: bool = False
    route_protocol_activity: bool = False
    upstream_connected: bool = False
    ready: bool = False
    upstream_error: str = ""

    def __post_init__(self) -> None:
        for field_name in (
            "running",
            "collector_connected",
            "collector_protocol_ingress",
            "route_protocol_activity",
            "upstream_connected",
            "ready",
        ):
            if type(getattr(self, field_name)) is not bool:
                raise TypeError(f"shadow_learning_route_{field_name}_must_be_bool")
        if type(self.upstream_error) is not str:
            raise TypeError("shadow_learning_route_upstream_error_must_be_str")
        if self.upstream_error != self.upstream_error.strip():
            raise ValueError("shadow_learning_route_upstream_error_not_normalized")

    @classmethod
    def from_mapping(
        cls,
        raw: Mapping[str, object] | None,
    ) -> ShadowLearningRouteStatus:
        """Sanitize one lower-layer status mapping into the strict public model."""

        if not isinstance(raw, Mapping):
            return cls()

        def _exact_bool(key: str) -> bool:
            value = raw.get(key)
            return value if type(value) is bool else False

        raw_error = raw.get("upstream_error")
        upstream_error = (
            raw_error.strip()
            if type(raw_error) is str
            else ""
        )
        return cls(
            running=_exact_bool("running"),
            collector_connected=_exact_bool("collector_connected"),
            collector_protocol_ingress=_exact_bool(
                "collector_protocol_ingress"
            ),
            route_protocol_activity=_exact_bool("route_protocol_activity"),
            upstream_connected=_exact_bool("upstream_connected"),
            ready=_exact_bool("ready"),
            upstream_error=upstream_error,
        )

    def to_mapping(self) -> dict[str, object]:
        """Return the compatibility mapping used by existing safety predicates."""

        return {
            "running": self.running,
            "collector_connected": self.collector_connected,
            "collector_protocol_ingress": self.collector_protocol_ingress,
            "route_protocol_activity": self.route_protocol_activity,
            "upstream_connected": self.upstream_connected,
            "ready": self.ready,
            "upstream_error": self.upstream_error,
        }


@dataclass(frozen=True, slots=True)
class ShadowLearningRuntimeView:
    """Public, read-only shadow-learning state exposed by the coordinator."""

    route_status: ShadowLearningRouteStatus = ShadowLearningRouteStatus()
    cloud_evidence: CloudEvidenceRecord | None = None
    write_observations: tuple[ShadowWriteObservation, ...] = ()

    def __post_init__(self) -> None:
        if type(self.route_status) is not ShadowLearningRouteStatus:
            raise TypeError("shadow_learning_runtime_route_status_invalid")
        if (
            self.cloud_evidence is not None
            and type(self.cloud_evidence) is not CloudEvidenceRecord
        ):
            raise TypeError("shadow_learning_runtime_cloud_evidence_invalid")
        if type(self.write_observations) is not tuple:
            raise TypeError("shadow_learning_runtime_write_observations_must_be_tuple")
        if any(
            type(observation) is not ShadowWriteObservation
            for observation in self.write_observations
        ):
            raise TypeError("shadow_learning_runtime_write_observation_invalid")


__all__ = ["ShadowLearningRouteStatus", "ShadowLearningRuntimeView"]
