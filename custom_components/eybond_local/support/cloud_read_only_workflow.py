"""Source-neutral read-only cloud-evidence workflow."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .cloud_learning_runner import CloudLearningOutcome, CloudLearningRunner


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


class CloudReadOnlyEvidenceOperation(ABC):
    """Provider operation that can only return passive cloud evidence."""

    provider_id: str = ""
    source_id: str = ""

    @abstractmethod
    async def async_collect(
        self,
        *,
        executor,
        collector_pn: str,
        username: str,
        password: str,
        max_fields: int,
        progress,
    ) -> CloudLearningOutcome:
        """Collect one bounded source-owned read-only evidence bundle."""


class ReadOnlyEvidenceWorkflowRunner(CloudLearningRunner):
    """Enforce the zero-route, zero-write boundary around one API operation."""

    def __init__(self, operation: CloudReadOnlyEvidenceOperation) -> None:
        if not isinstance(operation, CloudReadOnlyEvidenceOperation):
            raise TypeError("cloud_read_only_operation_invalid")
        self._operation = operation
        self.provider_id = _required_token(
            operation.provider_id,
            "cloud_read_only_provider_invalid",
        )
        self.source_id = _required_token(
            operation.source_id,
            "cloud_read_only_source_invalid",
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
        # These active-workflow inputs are deliberately not passed to the API
        # operation. A read-only implementation therefore cannot accidentally
        # open a route, dispatch a control probe or claim a fallback identity.
        del fallback_identity
        del orchestrator_callbacks
        del start_shadow_route
        del on_learning

        progress(0.10, "fetching")
        outcome = await self._operation.async_collect(
            executor=executor,
            collector_pn=collector_pn,
            username=username,
            password=password,
            max_fields=max_fields,
            progress=progress,
        )
        self._validate_outcome(outcome)
        on_identity(dict(outcome.identity))
        progress(0.82, "building")
        return outcome

    def _validate_outcome(self, outcome: object) -> None:
        if type(outcome) is not CloudLearningOutcome:
            raise TypeError("cloud_read_only_outcome_invalid")
        if outcome.read_bindings is not None:
            raise ValueError("cloud_read_only_local_binding_forbidden")
        identity = outcome.identity
        pn = identity.get("pn") if type(identity) is dict else None
        if type(pn) is not str or not pn or pn != pn.strip():
            raise ValueError("cloud_read_only_identity_invalid")
        result = outcome.result
        if result.get("source") != self.source_id:
            raise ValueError("cloud_read_only_result_source_mismatch")
        if result.get("metadata_only") is not True:
            raise ValueError("cloud_read_only_result_authority_invalid")
        for key in (
            "planned_write_count",
            "executed_result_count",
            "sent_count",
            "leaked_count",
            "degraded_count",
        ):
            if type(result.get(key)) is not int or result.get(key) != 0:
                raise ValueError("cloud_read_only_result_write_claim_invalid")


__all__ = [
    "CloudReadOnlyEvidenceOperation",
    "ReadOnlyEvidenceWorkflowRunner",
]
