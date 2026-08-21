"""Cohesive public boundary for shadow-learning runtime access."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..support.cloud_evidence import CloudEvidenceRecord
from ..support.shadow_learning import ShadowWriteObservation
from ..support.shadow_learning.runtime import (
    ShadowLearningRouteStatus,
    ShadowLearningRuntimeView,
)


class ShadowLearningRuntimeFacade:
    """Expose shadow-learning state and observations without leaking internals."""

    def __init__(
        self,
        *,
        runtime: object,
        cloud_evidence_provider: Callable[[], CloudEvidenceRecord | None],
    ) -> None:
        if not callable(cloud_evidence_provider):
            raise TypeError("shadow_learning_cloud_evidence_provider_required")
        self._runtime = runtime
        self._cloud_evidence_provider = cloud_evidence_provider

    @property
    def view(self) -> ShadowLearningRuntimeView:
        """Return one strict, immutable snapshot for UI consumers."""

        route_status_provider = getattr(
            self._runtime,
            "shadow_learning_route_status",
            None,
        )
        try:
            raw_status = (
                route_status_provider()
                if callable(route_status_provider)
                else None
            )
        except Exception:
            raw_status = None
        route_status = ShadowLearningRouteStatus.from_mapping(raw_status)

        observations_provider = getattr(
            self._runtime,
            "shadow_learning_write_observations",
            None,
        )
        try:
            candidate_observations = (
                tuple(observations_provider())
                if callable(observations_provider)
                else ()
            )
        except Exception:
            candidate_observations = ()
        observations = (
            candidate_observations
            if all(
                type(observation) is ShadowWriteObservation
                for observation in candidate_observations
            )
            else ()
        )

        try:
            candidate_evidence = self._cloud_evidence_provider()
        except Exception:
            candidate_evidence = None
        evidence = (
            candidate_evidence
            if type(candidate_evidence) is CloudEvidenceRecord
            else None
        )

        # Live route status is safety-critical. Optional evidence or observation
        # projection failures must fail closed in their own field; they must
        # never erase a valid route status and turn an active proxy into an
        # apparent all-false session.
        return ShadowLearningRuntimeView(
            route_status=route_status,
            cloud_evidence=evidence,
            write_observations=observations,
        )

    async def async_capture_support_evidence(self) -> dict[str, object]:
        """Capture learning seed evidence through the runtime contract."""

        capture = getattr(self._runtime, "async_capture_support_evidence", None)
        if not callable(capture):
            return {}
        evidence = await capture()
        return dict(evidence) if isinstance(evidence, dict) else {}

    def observation_cursor(self) -> int:
        """Return the active learning route's current observation cursor."""

        provider = getattr(
            self._runtime,
            "shadow_learning_observation_cursor",
            None,
        )
        if not callable(provider):
            return 0
        cursor = provider()
        if type(cursor) is not int or cursor < 0:
            raise RuntimeError("shadow_learning_observation_cursor_invalid")
        return cursor

    def observations_since(
        self,
        cursor: int,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return exact observations captured at or after one cursor."""

        self._require_cursor(cursor)
        provider = getattr(
            self._runtime,
            "shadow_learning_observations_since",
            None,
        )
        if not callable(provider):
            return ()
        observations = provider(cursor)
        return self._validated_observations(observations)

    async def async_wait_for_observations_since(
        self,
        cursor: int,
        timeout_seconds: float,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Wait for exact observations through the public runtime boundary."""

        self._require_cursor(cursor)
        self._require_timeout(timeout_seconds)
        provider = getattr(
            self._runtime,
            "async_wait_for_shadow_learning_observations_since",
            None,
        )
        if not callable(provider):
            return ()
        observations = await provider(
            cursor,
            timeout_seconds=timeout_seconds,
        )
        return self._validated_observations(observations)

    def read_map_snapshot(self) -> dict[str, Any]:
        """Return a detached read-map snapshot through the runtime contract."""

        provider = getattr(
            self._runtime,
            "shadow_learning_read_map_snapshot",
            None,
        )
        if not callable(provider):
            return {}
        read_map = provider()
        return dict(read_map) if isinstance(read_map, dict) else {}

    @staticmethod
    def _validated_observations(
        observations: object,
    ) -> tuple[ShadowWriteObservation, ...]:
        if type(observations) is not tuple:
            raise TypeError("shadow_learning_observations_must_be_tuple")
        return ShadowLearningRuntimeView(
            write_observations=observations
        ).write_observations

    @staticmethod
    def _require_cursor(cursor: object) -> None:
        if type(cursor) is not int or cursor < 0:
            raise ValueError("shadow_learning_observation_cursor_invalid")

    @staticmethod
    def _require_timeout(timeout_seconds: object) -> None:
        if (
            type(timeout_seconds) not in (int, float)
            or type(timeout_seconds) is bool
            or timeout_seconds < 0
        ):
            raise ValueError("shadow_learning_observation_timeout_invalid")


__all__ = ["ShadowLearningRuntimeFacade"]
