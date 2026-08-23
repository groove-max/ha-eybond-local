from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_learning_runner import (  # noqa: E402
    CloudLearningOutcome,
)
from custom_components.eybond_local.support.cloud_read_only_workflow import (  # noqa: E402
    CloudReadOnlyEvidenceOperation,
    ReadOnlyEvidenceWorkflowRunner,
)


def _outcome(**result_overrides) -> CloudLearningOutcome:
    result = {
        "source": "test_source",
        "metadata_only": True,
        "planned_write_count": 0,
        "executed_result_count": 0,
        "sent_count": 0,
        "leaked_count": 0,
        "degraded_count": 0,
    }
    result.update(result_overrides)
    return CloudLearningOutcome(
        identity={"pn": "E50000200000000001"},
        result=result,
        read_bindings=None,
    )


class _Operation(CloudReadOnlyEvidenceOperation):
    provider_id = "test_provider"
    source_id = "test_source"

    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        self.calls = 0

    async def async_collect(self, **_kwargs):
        self.calls += 1
        return self.outcome


class ReadOnlyEvidenceWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, operation: _Operation):
        start_route = AsyncMock()
        on_learning = Mock()
        identities: list[dict] = []
        progress: list[tuple[float, str]] = []
        outcome = await ReadOnlyEvidenceWorkflowRunner(operation).async_run(
            executor=AsyncMock(),
            collector_pn="E5000020000000",
            username="user",
            password="secret",
            fallback_identity={"pn": "FOREIGN"},
            max_fields=10,
            progress=lambda fraction, stage, **_kwargs: progress.append(
                (fraction, stage)
            ),
            orchestrator_callbacks={"write": object()},
            on_identity=identities.append,
            start_shadow_route=start_route,
            on_learning=on_learning,
        )
        return outcome, start_route, on_learning, identities, progress

    async def test_success_cannot_open_route_or_call_active_learning(self) -> None:
        operation = _Operation(_outcome())
        outcome, start_route, on_learning, identities, progress = await self._run(
            operation
        )

        self.assertIsInstance(outcome, CloudLearningOutcome)
        self.assertEqual(operation.calls, 1)
        start_route.assert_not_awaited()
        on_learning.assert_not_called()
        self.assertEqual(identities, [{"pn": "E50000200000000001"}])
        self.assertEqual(progress, [(0.10, "fetching"), (0.82, "building")])

    async def test_invalid_outcomes_fail_before_identity_adoption(self) -> None:
        malformed = (
            object(),
            CloudLearningOutcome(
                identity={"pn": "E50000200000000001"},
                result=_outcome().result,
                read_bindings={"register": 1},
            ),
            _outcome(source="foreign"),
            _outcome(metadata_only=False),
            _outcome(planned_write_count=1),
            CloudLearningOutcome(identity={"pn": " padded "}, result=_outcome().result),
        )
        for value in malformed:
            with self.subTest(value=value):
                operation = _Operation(value)
                with self.assertRaises((TypeError, ValueError)):
                    await self._run(operation)

    def test_duck_operation_is_rejected(self) -> None:
        class Duck:
            provider_id = "test_provider"
            source_id = "test_source"

            async def async_collect(self, **_kwargs):
                return _outcome()

        with self.assertRaises(TypeError):
            ReadOnlyEvidenceWorkflowRunner(Duck())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
