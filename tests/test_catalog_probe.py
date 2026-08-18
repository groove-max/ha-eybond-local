from __future__ import annotations

from time import monotonic
import unittest

from custom_components.eybond_local.drivers.catalog_probe import (
    ProbeDeadline,
    async_walk_detection_dag,
)
from custom_components.eybond_local.metadata.compiled_detection_catalog import (
    PROBE_ACTION_MODBUS_READ,
    load_compiled_detection_catalog,
)


class ProbeDeadlineTests(unittest.TestCase):
    def test_remaining_budget_clamps_required_action_timeout(self) -> None:
        deadline = ProbeDeadline(10.0)
        deadline._started = monotonic() - 8.5

        self.assertLessEqual(deadline.action_timeout(4.0), 1.6)

    def test_optional_action_requires_full_configured_timeout_budget(self) -> None:
        deadline = ProbeDeadline(10.0)
        deadline._started = monotonic() - 8.5

        self.assertFalse(deadline.has_optional_budget(2.0))
        self.assertTrue(deadline.has_optional_budget(1.0))


class RequiredActionContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_required_actions_run_before_early_no_match(self) -> None:
        catalog = load_compiled_detection_catalog()
        protocol = catalog.protocols["modbus_smg"]
        evidence: dict[str, object] = {}
        executed: list[str] = []

        async def _execute(action) -> str:
            executed.append(action.key)
            for field in action.evidence_fields:
                evidence[field.key] = 0
            return "executed"

        result = await async_walk_detection_dag(
            protocol=protocol,
            tree=catalog.decision_trees[protocol.key],
            evidence=evidence,
            execute_action=_execute,
            supported_kinds=frozenset({PROBE_ACTION_MODBUS_READ}),
        )

        self.assertEqual(
            executed,
            ["modbus_smg.identity.171", "modbus_smg.identity.184"],
        )
        self.assertEqual(result.executed_actions, tuple(executed))
