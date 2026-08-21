from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    CallbackSessionRegistry,
)
from custom_components.eybond_local.const import (  # noqa: E402
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
)
from custom_components.eybond_local.runtime.coordinator_strategy import (  # noqa: E402
    CoordinatorStrategyTransitionMixin,
)
from custom_components.eybond_local.runtime.hub_lifecycle import (  # noqa: E402
    HubLifecycleMixin,
)


ENTRY_ID = "entry-transition-bootstrap"
FULL_PN = "E5000025SYN0000000001"
FOREIGN_PN = "V001020SYN0000000002"
SESSION_ID = "listener-8899-2"


def _session(session_id: str, collector_pn: str) -> dict[str, object]:
    return {
        "session_id": session_id,
        "peer_ip": "203.0.113.10",
        "listener_port": 8899,
        "collector_pn": collector_pn,
        "state": "routed_framed",
        "protocol_shape": "eybond_framed",
        "collector_identity_source": "fc2_parameter_2",
    }


class _Runtime:
    def __init__(
        self,
        *,
        inventory: list[dict[str, object]],
        result: object = True,
        session: dict[str, object] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.inventory = inventory
        self.result = result
        self.session = session
        self.error = error
        self.calls: list[float] = []

    async def async_ensure_collector_management_session(
        self,
        *,
        timeout: float,
    ) -> object:
        self.calls.append(timeout)
        if self.error is not None:
            raise self.error
        if self.session is not None:
            self.inventory.append(self.session)
        return self.result


def _subject(*, current_strategy: str, runtime: object):
    subject = CoordinatorStrategyTransitionMixin()
    subject.connection_strategy = current_strategy
    subject._runtime = runtime
    return subject


def _registry(inventory: list[dict[str, object]]) -> CallbackSessionRegistry:
    registry = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
    registry.claim_identity(ENTRY_ID, FULL_PN)
    return registry


class StrategyTransitionSessionBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_trusted_session_is_pinned_without_callback(self) -> None:
        inventory = [_session(SESSION_ID, FULL_PN)]
        runtime = _Runtime(
            inventory=inventory,
            error=AssertionError("callback bootstrap must not run"),
        )
        subject = _subject(
            current_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            runtime=runtime,
        )

        selected = await subject._async_prepare_strategy_transition_management_session(
            registry=_registry(inventory),
            entry_id=ENTRY_ID,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            timeout=60.0,
        )

        self.assertEqual(selected, SESSION_ID)
        self.assertEqual(runtime.calls, [])

    async def test_idle_callback_entry_uses_runtime_then_pins_exact_session(self) -> None:
        inventory: list[dict[str, object]] = []
        runtime = _Runtime(
            inventory=inventory,
            session=_session(SESSION_ID, FULL_PN),
        )
        registry = _registry(inventory)
        subject = _subject(
            current_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            runtime=runtime,
        )

        selected = await subject._async_prepare_strategy_transition_management_session(
            registry=registry,
            entry_id=ENTRY_ID,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            timeout=60.0,
        )

        self.assertEqual(selected, SESSION_ID)
        self.assertEqual(runtime.calls, [60.0])
        self.assertEqual(registry.claimed_session_id(ENTRY_ID), SESSION_ID)
        self.assertIsNotNone(
            registry.session_handle_for_owned_session(ENTRY_ID, SESSION_ID)
        )

    async def test_foreign_session_after_callback_is_never_pinned(self) -> None:
        inventory: list[dict[str, object]] = []
        runtime = _Runtime(
            inventory=inventory,
            session=_session(SESSION_ID, FOREIGN_PN),
        )
        registry = _registry(inventory)
        subject = _subject(
            current_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            runtime=runtime,
        )

        selected = await subject._async_prepare_strategy_transition_management_session(
            registry=registry,
            entry_id=ENTRY_ID,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            timeout=60.0,
        )

        self.assertEqual(selected, "")
        self.assertEqual(registry.claimed_session_id(ENTRY_ID), "")
        self.assertIsNone(
            registry.session_handle_for_owned_session(ENTRY_ID, SESSION_ID)
        )

    async def test_false_or_non_bool_runtime_result_fails_closed(self) -> None:
        for result in (False, 1, "connected", object()):
            with self.subTest(result=result):
                inventory: list[dict[str, object]] = []
                runtime = _Runtime(
                    inventory=inventory,
                    result=result,
                    session=_session(SESSION_ID, FULL_PN),
                )
                registry = _registry(inventory)
                subject = _subject(
                    current_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                    runtime=runtime,
                )

                selected = await subject._async_prepare_strategy_transition_management_session(
                    registry=registry,
                    entry_id=ENTRY_ID,
                    target_strategy=CONNECTION_STRATEGY_INBOUND,
                    timeout=60.0,
                )

                self.assertEqual(selected, "")
                self.assertEqual(registry.claimed_session_id(ENTRY_ID), "")

    async def test_other_direction_never_bootstraps_callback(self) -> None:
        inventory: list[dict[str, object]] = []
        runtime = _Runtime(
            inventory=inventory,
            error=AssertionError("inbound transition must not send callback"),
        )
        subject = _subject(
            current_strategy=CONNECTION_STRATEGY_INBOUND,
            runtime=runtime,
        )

        selected = await subject._async_prepare_strategy_transition_management_session(
            registry=_registry(inventory),
            entry_id=ENTRY_ID,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            timeout=60.0,
        )

        self.assertEqual(selected, "")
        self.assertEqual(runtime.calls, [])

    async def test_cancellation_from_runtime_bootstrap_propagates(self) -> None:
        inventory: list[dict[str, object]] = []
        runtime = _Runtime(
            inventory=inventory,
            error=asyncio.CancelledError(),
        )
        subject = _subject(
            current_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            runtime=runtime,
        )

        with self.assertRaises(asyncio.CancelledError):
            await subject._async_prepare_strategy_transition_management_session(
                registry=_registry(inventory),
                entry_id=ENTRY_ID,
                target_strategy=CONNECTION_STRATEGY_INBOUND,
                timeout=60.0,
            )


class StrategyTransitionBootstrapArchitectureTests(unittest.TestCase):
    def test_facade_serializes_bootstrap_and_transition_with_runtime_polling(self) -> None:
        source = inspect.getsource(
            CoordinatorStrategyTransitionMixin.async_run_connection_strategy_transition
        )

        lock = source.index("await self._runtime_operation_lock.acquire()")
        bootstrap = source.index(
            "await self._async_prepare_strategy_transition_management_session("
        )
        probe = source.index("SilentSessionIdentityProbeChannel(")
        authority = source.index("return await async_run_strategy_transition(")
        release = source.index("self._runtime_operation_lock.release()")

        self.assertLess(lock, bootstrap)
        self.assertLess(bootstrap, probe)
        self.assertLess(probe, authority)
        self.assertGreater(release, authority)

    def test_bootstrap_reuses_runtime_and_has_no_parallel_trigger_or_matcher(self) -> None:
        source = inspect.getsource(
            CoordinatorStrategyTransitionMixin._async_prepare_strategy_transition_management_session
        )

        self.assertIn("async_ensure_collector_management_session", source)
        self.assertNotIn("async_trigger_reverse_discovery", source)
        self.assertNotIn("match_callback", source)
        self.assertNotIn("peer_ip", source)
        self.assertNotIn("observed_sessions_per_socket", source)


class RuntimeManagementSessionBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_runtime_boundary_uses_lifecycle_connect_without_heartbeat(self) -> None:
        hub = HubLifecycleMixin()
        connect = AsyncMock(return_value=True)
        hub._async_try_connect_for_session_lifecycle = connect

        connected = await hub.async_ensure_collector_management_session(timeout=60.0)

        self.assertTrue(connected)
        connect.assert_awaited_once_with(timeout=60.0, require_heartbeat=False)

    async def test_public_runtime_boundary_rejects_truthy_non_bool_result(self) -> None:
        hub = HubLifecycleMixin()
        hub._async_try_connect_for_session_lifecycle = AsyncMock(return_value=1)

        connected = await hub.async_ensure_collector_management_session(timeout=60.0)

        self.assertFalse(connected)


if __name__ == "__main__":
    unittest.main()
