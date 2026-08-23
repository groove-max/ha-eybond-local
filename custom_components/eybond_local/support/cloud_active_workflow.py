"""Source-neutral lifecycle for active cloud/local correlation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..collector_identity import pn_is_same_identity
from .cloud_learning_runner import CloudLearningOutcome, CloudLearningRunner


ACTIVE_CORRELATION_NO_SAFE_CONTROLS = "cloud_active_no_safe_controls"


@dataclass(frozen=True, slots=True)
class ControlDiscoveryTimeoutPolicy:
    """Separate a bounded metadata sweep from one control dispatch."""

    metadata_request: float = 30.0
    action_request: float = 15.0

    def __post_init__(self) -> None:
        for value in (self.metadata_request, self.action_request):
            if type(value) not in {int, float}:
                raise TypeError("control_discovery_timeout_invalid")
            if value <= 0:
                raise ValueError("control_discovery_timeout_invalid")


DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY = ControlDiscoveryTimeoutPolicy()


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


class CloudActiveCorrelationOperation(ABC):
    """Provider operation executed inside the common active lifecycle."""

    provider_id: str = ""
    source_id: str = ""

    @abstractmethod
    async def async_correlate(
        self,
        *,
        executor,
        collector_pn: str,
        username: str,
        password: str,
        fallback_identity,
        max_fields: int,
        progress,
        orchestrator_callbacks,
        adopt_identity,
        start_shadow_route,
        on_learning,
    ) -> CloudLearningOutcome:
        """Run one provider-specific active correlation operation."""


class ActiveCorrelationWorkflowRunner(CloudLearningRunner):
    """Enforce one route, one identity and one learning phase per run."""

    def __init__(self, operation: CloudActiveCorrelationOperation) -> None:
        if not isinstance(operation, CloudActiveCorrelationOperation):
            raise TypeError("cloud_active_operation_invalid")
        self._operation = operation
        self.provider_id = _required_token(
            operation.provider_id,
            "cloud_active_provider_invalid",
        )
        self.source_id = _required_token(
            operation.source_id,
            "cloud_active_source_invalid",
        )

    async def async_run(
        self,
        *,
        executor,
        collector_pn,
        username,
        password,
        fallback_identity,
        max_fields,
        progress,
        orchestrator_callbacks,
        on_identity,
        start_shadow_route,
        on_learning,
    ) -> CloudLearningOutcome:
        route_started = False
        learning_started = False
        adopted_identity: dict | None = None

        async def _start_route_once() -> None:
            nonlocal route_started
            if route_started:
                raise RuntimeError("cloud_active_route_already_started")
            route_started = True
            await start_shadow_route()

        def _adopt_identity_once(identity: object) -> None:
            nonlocal adopted_identity
            if adopted_identity is not None:
                raise RuntimeError("cloud_active_identity_already_adopted")
            if type(identity) is not dict:
                raise TypeError("cloud_active_identity_invalid")
            pn = identity.get("pn")
            if type(pn) is not str or not pn or pn != pn.strip():
                raise ValueError("cloud_active_identity_invalid")
            adopted_identity = dict(identity)
            on_identity(dict(identity))

        def _start_learning_once() -> None:
            nonlocal learning_started
            if learning_started:
                raise RuntimeError("cloud_active_learning_already_started")
            learning_started = True
            on_learning()

        outcome = await self._operation.async_correlate(
            executor=executor,
            collector_pn=collector_pn,
            username=username,
            password=password,
            fallback_identity=fallback_identity,
            max_fields=max_fields,
            progress=progress,
            orchestrator_callbacks=orchestrator_callbacks,
            adopt_identity=_adopt_identity_once,
            start_shadow_route=_start_route_once,
            on_learning=_start_learning_once,
        )
        self._validate_outcome(
            outcome,
            route_started=route_started,
            learning_started=learning_started,
            adopted_identity=adopted_identity,
        )
        return outcome

    def _validate_outcome(
        self,
        outcome: object,
        *,
        route_started: bool,
        learning_started: bool,
        adopted_identity: dict | None,
    ) -> None:
        if not route_started:
            raise ValueError("cloud_active_route_not_started")
        if not learning_started:
            raise ValueError("cloud_active_learning_not_started")
        if type(adopted_identity) is not dict:
            raise ValueError("cloud_active_identity_not_adopted")
        if type(outcome) is not CloudLearningOutcome:
            raise TypeError("cloud_active_outcome_invalid")
        outcome_pn = outcome.identity.get("pn")
        adopted_pn = adopted_identity.get("pn")
        if (
            type(outcome_pn) is not str
            or type(adopted_pn) is not str
            or not pn_is_same_identity(outcome_pn, adopted_pn)
        ):
            raise ValueError("cloud_active_outcome_identity_mismatch")
        result = outcome.result
        if result.get("source") != self.source_id:
            raise ValueError("cloud_active_result_source_mismatch")
        if result.get("metadata_only") is not False:
            raise ValueError("cloud_active_result_authority_invalid")
        for key in (
            "planned_write_count",
            "executed_result_count",
            "sent_count",
            "leaked_count",
            "degraded_count",
        ):
            value = result.get(key)
            if type(value) is not int or value < 0:
                raise ValueError("cloud_active_result_count_invalid")


__all__ = [
    "ACTIVE_CORRELATION_NO_SAFE_CONTROLS",
    "ActiveCorrelationWorkflowRunner",
    "CloudActiveCorrelationOperation",
    "ControlDiscoveryTimeoutPolicy",
    "DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY",
]
