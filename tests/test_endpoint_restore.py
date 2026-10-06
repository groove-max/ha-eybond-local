"""Read-first recovery, uncertain writes and apply-state safety."""

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from custom_components.eybond_local.collector.management import CollectorManagementCommandError
from custom_components.eybond_local.runtime.endpoint_restore import restore_collector_endpoint


ORIGINAL = "iot.eybond.com,18899,TCP"
LOCAL = "192.0.2.1,18899,TCP"


def state(endpoint=ORIGINAL, pending="0"):
    return {"current_endpoint": endpoint, "reboot_required": pending}


class EndpointRestoreTests(unittest.IsolatedAsyncioTestCase):
    def runtime(self, *reads):
        return SimpleNamespace(
            async_get_collector_server_endpoint_state=AsyncMock(side_effect=reads),
            async_disconnect_collector_connections=AsyncMock(),
            async_set_collector_server_endpoint=AsyncMock(return_value={"apply_performed": True}),
            async_apply_collector_changes=AsyncMock(return_value={"status": "applied"}),
        )

    async def restore(self, runtime):
        return await restore_collector_endpoint(runtime, ORIGINAL, timeout=10)

    async def test_already_restored_does_not_write_apply_or_disconnect(self):
        runtime = self.runtime(state())
        self.assertEqual(await self.restore(runtime), ORIGINAL)
        runtime.async_set_collector_server_endpoint.assert_not_awaited()
        runtime.async_apply_collector_changes.assert_not_awaited()
        runtime.async_disconnect_collector_connections.assert_not_awaited()
        runtime.async_get_collector_server_endpoint_state.assert_awaited_once_with(
            timeout=10, require_heartbeat=False,
        )

    async def test_pending_or_unknown_apply_only_applies_existing_endpoint(self):
        for pending in ("1", ""):
            with self.subTest(pending=pending):
                runtime = self.runtime(state(pending=pending), state(pending="" if not pending else "0"))
                self.assertEqual(await self.restore(runtime), ORIGINAL)
                runtime.async_set_collector_server_endpoint.assert_not_awaited()
                runtime.async_apply_collector_changes.assert_awaited_once_with(
                    timeout=10, require_heartbeat=False,
                )
                runtime.async_disconnect_collector_connections.assert_awaited_once()

    async def test_different_endpoint_writes_once_then_reconnects_and_reads(self):
        runtime = self.runtime(state(LOCAL), state())
        self.assertEqual(await self.restore(runtime), ORIGINAL)
        runtime.async_set_collector_server_endpoint.assert_awaited_once_with(
            ORIGINAL, apply_changes=True, timeout=10, require_heartbeat=False,
        )
        runtime.async_apply_collector_changes.assert_not_awaited()
        runtime.async_disconnect_collector_connections.assert_awaited_once()

    async def test_initial_read_timeout_retries_only_read_on_new_session(self):
        runtime = self.runtime(TimeoutError(), state())
        self.assertEqual(await self.restore(runtime), ORIGINAL)
        runtime.async_disconnect_collector_connections.assert_awaited_once()
        runtime.async_set_collector_server_endpoint.assert_not_awaited()

    async def test_repeated_read_failure_never_writes(self):
        runtime = self.runtime(TimeoutError(), TimeoutError())
        with self.assertRaises(TimeoutError):
            await self.restore(runtime)
        runtime.async_set_collector_server_endpoint.assert_not_awaited()
        runtime.async_apply_collector_changes.assert_not_awaited()
        runtime.async_disconnect_collector_connections.assert_awaited_once()

    async def test_session_replacement_is_a_failed_observation_not_confirmation(self):
        from custom_components.eybond_local.collector.management import CollectorManagementTransportError

        runtime = self.runtime(
            CollectorManagementTransportError("collector_management_session_changed"),
            state(),
        )
        self.assertEqual(await self.restore(runtime), ORIGINAL)
        runtime.async_disconnect_collector_connections.assert_awaited_once()
        runtime.async_set_collector_server_endpoint.assert_not_awaited()

    async def test_explicit_rejection_is_not_treated_as_lost_ack(self):
        runtime = self.runtime(state(LOCAL))
        runtime.async_set_collector_server_endpoint.side_effect = CollectorManagementCommandError("rejected")
        with self.assertRaises(CollectorManagementCommandError):
            await self.restore(runtime)
        runtime.async_disconnect_collector_connections.assert_not_awaited()
        runtime.async_get_collector_server_endpoint_state.assert_awaited_once()

    async def test_write_timeout_can_be_resolved_only_by_independent_applied_read(self):
        for observed, error in (
            (state(), None),
            (state(LOCAL), "restore_live_endpoint_mismatch"),
            (state(pending="1"), "restore_apply_unconfirmed"),
            (state(pending=""), "restore_apply_unconfirmed"),
        ):
            with self.subTest(observed=observed):
                runtime = self.runtime(state(LOCAL), observed)
                runtime.async_set_collector_server_endpoint.side_effect = TimeoutError()
                if error:
                    with self.assertRaisesRegex(RuntimeError, error):
                        await self.restore(runtime)
                else:
                    self.assertEqual(await self.restore(runtime), ORIGINAL)
                runtime.async_set_collector_server_endpoint.assert_awaited_once()
                runtime.async_apply_collector_changes.assert_not_awaited()

    async def test_apply_timeout_does_not_repeat_apply(self):
        for pending in ("0", "1", ""):
            with self.subTest(pending=pending):
                runtime = self.runtime(state(pending="1"), state(pending=pending))
                runtime.async_apply_collector_changes.side_effect = TimeoutError()
                if pending == "0":
                    self.assertEqual(await self.restore(runtime), ORIGINAL)
                else:
                    with self.assertRaisesRegex(RuntimeError, "restore_apply_unconfirmed"):
                        await self.restore(runtime)
                runtime.async_apply_collector_changes.assert_awaited_once()
                runtime.async_set_collector_server_endpoint.assert_not_awaited()

    async def test_ack_cannot_override_pending_apply_or_mismatched_endpoint(self):
        for observed in (state(pending="1"), state(LOCAL)):
            runtime = self.runtime(state(LOCAL), observed)
            with self.assertRaises(RuntimeError):
                await self.restore(runtime)

    async def test_invalid_read_is_not_permission_to_write(self):
        for observed in ({}, None, state(""), state(" " + ORIGINAL), state(pending=True)):
            runtime = self.runtime(observed)
            with self.assertRaises(RuntimeError):
                await self.restore(runtime)
            runtime.async_set_collector_server_endpoint.assert_not_awaited()

    async def test_cancellation_propagates_without_retry(self):
        runtime = self.runtime(asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.restore(runtime)
        runtime.async_disconnect_collector_connections.assert_not_awaited()
        runtime = self.runtime(state(LOCAL))
        runtime.async_set_collector_server_endpoint.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.restore(runtime)
        runtime.async_disconnect_collector_connections.assert_not_awaited()
