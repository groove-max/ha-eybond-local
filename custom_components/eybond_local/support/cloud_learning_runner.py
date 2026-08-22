"""Neutral contracts shared by all cloud-learning implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any


ExecutorJob = Callable[..., Awaitable[Any]]
ProgressCallback = Callable[..., None]
IdentityCallback = Callable[[dict[str, Any]], None]
LearningCallback = Callable[[], None]
StartShadowRouteCallback = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class CloudLearningOutcome:
    """Normalized result of one exact cloud-learning engine run."""

    identity: dict[str, Any]
    result: dict[str, Any]
    read_bindings: dict[str, Any] | None = None
    metadata_evidence: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if type(self.identity) is not dict:
            raise TypeError("cloud_learning_identity_invalid")
        if type(self.result) is not dict:
            raise TypeError("cloud_learning_result_invalid")
        for value, reason in (
            (self.read_bindings, "cloud_learning_read_bindings_invalid"),
            (self.metadata_evidence, "cloud_learning_metadata_evidence_invalid"),
        ):
            if value is not None and type(value) is not dict:
                raise TypeError(reason)
        # Detach the outcome from mutable provider-owned containers while
        # preserving the exact, already-normalized mapping shape.
        object.__setattr__(self, "identity", dict(self.identity))
        object.__setattr__(self, "result", dict(self.result))
        if self.read_bindings is not None:
            object.__setattr__(self, "read_bindings", dict(self.read_bindings))
        if self.metadata_evidence is not None:
            object.__setattr__(
                self,
                "metadata_evidence",
                dict(self.metadata_evidence),
            )


class CloudLearningRunner(ABC):
    """One API-specific transient learning implementation."""

    provider_id: str = ""
    source_id: str = ""

    @abstractmethod
    async def async_run(
        self,
        *,
        executor: ExecutorJob,
        collector_pn: str,
        username: str,
        password: str,
        fallback_identity: dict[str, Any] | None,
        max_fields: int,
        progress: ProgressCallback,
        orchestrator_callbacks: Mapping[str, Any],
        on_identity: IdentityCallback,
        start_shadow_route: StartShadowRouteCallback,
        on_learning: LearningCallback,
    ) -> CloudLearningOutcome:
        """Run one bounded engine-owned operation and return normalized evidence."""


__all__ = [
    "CloudLearningOutcome",
    "CloudLearningRunner",
    "ExecutorJob",
    "IdentityCallback",
    "LearningCallback",
    "ProgressCallback",
    "StartShadowRouteCallback",
]
