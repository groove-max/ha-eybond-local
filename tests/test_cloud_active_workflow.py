from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_active_workflow import (  # noqa: E402
    ActiveCorrelationWorkflowRunner,
    CloudActiveCorrelationOperation,
)
from custom_components.eybond_local.support.cloud_learning_runner import (  # noqa: E402
    CloudLearningOutcome,
)


def _outcome(**result_overrides) -> CloudLearningOutcome:
    result = {
        "source": "test_source",
        "metadata_only": False,
        "planned_write_count": 1,
        "executed_result_count": 1,
        "sent_count": 1,
        "leaked_count": 0,
        "degraded_count": 0,
    }
    result.update(result_overrides)
    return CloudLearningOutcome(
        identity={"pn": "E50000200000000001"},
        result=result,
    )


class _Operation(CloudActiveCorrelationOperation):
    provider_id = "test_provider"
    source_id = "test_source"

    def __init__(
        self,
        outcome: object,
        *,
        route_calls: int = 1,
        identity_calls: int = 1,
        learning_calls: int = 1,
    ) -> None:
        self.outcome = outcome
        self.route_calls = route_calls
        self.identity_calls = identity_calls
        self.learning_calls = learning_calls

    async def async_correlate(
        self,
        *,
        adopt_identity,
        start_shadow_route,
        on_learning,
        **_kwargs,
    ):
        for _ in range(self.route_calls):
            await start_shadow_route()
        for _ in range(self.identity_calls):
            adopt_identity({"pn": "E50000200000000001"})
        for _ in range(self.learning_calls):
            on_learning()
        return self.outcome


class ActiveCorrelationWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, operation: _Operation):
        route = AsyncMock()
        learning = Mock()
        identities: list[dict] = []
        outcome = await ActiveCorrelationWorkflowRunner(operation).async_run(
            executor=AsyncMock(),
            collector_pn="E5000020000000",
            username="user",
            password="secret",
            fallback_identity=None,
            max_fields=10,
            progress=lambda *_args, **_kwargs: None,
            orchestrator_callbacks={},
            on_identity=identities.append,
            start_shadow_route=route,
            on_learning=learning,
        )
        return outcome, route, learning, identities

    async def test_success_runs_each_lifecycle_boundary_once(self) -> None:
        outcome, route, learning, identities = await self._run(
            _Operation(_outcome())
        )

        self.assertIsInstance(outcome, CloudLearningOutcome)
        route.assert_awaited_once()
        learning.assert_called_once()
        self.assertEqual(identities, [{"pn": "E50000200000000001"}])

    async def test_duplicate_or_missing_lifecycle_boundary_is_rejected(self) -> None:
        cases = (
            _Operation(_outcome(), route_calls=0),
            _Operation(_outcome(), route_calls=2),
            _Operation(_outcome(), identity_calls=0),
            _Operation(_outcome(), identity_calls=2),
            _Operation(_outcome(), learning_calls=0),
            _Operation(_outcome(), learning_calls=2),
        )
        for operation in cases:
            with self.subTest(operation=operation):
                with self.assertRaises((RuntimeError, ValueError)):
                    await self._run(operation)

    async def test_malformed_success_outcome_is_rejected(self) -> None:
        malformed = (
            object(),
            _outcome(source="foreign"),
            _outcome(metadata_only=True),
            _outcome(planned_write_count=-1),
            _outcome(sent_count=True),
            CloudLearningOutcome(
                identity={"pn": "FOREIGN"},
                result=_outcome().result,
            ),
        )
        for value in malformed:
            with self.subTest(value=value):
                with self.assertRaises((TypeError, ValueError)):
                    await self._run(_Operation(value))

    def test_duck_operation_is_rejected(self) -> None:
        class Duck:
            provider_id = "test_provider"
            source_id = "test_source"

            async def async_correlate(self, **_kwargs):
                return _outcome()

        with self.assertRaises(TypeError):
            ActiveCorrelationWorkflowRunner(Duck())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
