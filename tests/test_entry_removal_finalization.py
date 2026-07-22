from __future__ import annotations

import asyncio
import ast
from dataclasses import fields
import inspect
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.removal_finalization import (
    CollectorRemovalSessionTicket,
    REMOVAL_RESTART_CONFIRMED,
    REMOVAL_SESSION_CONFLICT,
    async_finalize_collector_entry_removal,
)
from custom_components.eybond_local.connection.callback_ledger import (
    CallbackTriggerInhibitedError,
    CallbackTriggerLedger,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.passive_discovery import PassiveCallbackDiscovery


PN = "E50000253884199645"
OTHER_PN = "V90110737282291016"


def _session(*, session_id: str, pn: str, peer_ip: str = "192.168.1.1"):
    return {
        "session_id": session_id,
        "peer_ip": peer_ip,
        "listener_port": 8899,
        "listener_bind_host": "0.0.0.0",
        "collector_pn": pn,
        "collector_identity_source": "fc2_parameter_2",
        "protocol_shape": "eybond_framed",
        "state": "routed_framed",
    }


class RemovalTicketBoundaryTests(unittest.TestCase):
    def test_ticket_is_strict_and_strong(self) -> None:
        valid = CollectorRemovalSessionTicket(
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            session_id="listener-8899-e500",
            listener_host="0.0.0.0",
            listener_port=8899,
        )
        self.assertEqual(valid.inventory_key, "8899:listener-8899-e500")

        invalid = (
            {"collector_pn": f" {PN}"},
            {"identity_source": "framed_heartbeat"},
            {"session_id": object()},
            {"listener_host": " 0.0.0.0"},
            {"listener_port": True},
            {"listener_port": 0},
        )
        base = {
            "collector_pn": PN,
            "identity_source": "fc2_parameter_2",
            "session_id": "listener-8899-e500",
            "listener_host": "0.0.0.0",
            "listener_port": 8899,
        }
        for change in invalid:
            with self.subTest(change=change), self.assertRaises((TypeError, ValueError)):
                CollectorRemovalSessionTicket(**(base | change))

    def test_ticket_contains_no_route_or_peer_selector(self) -> None:
        self.assertEqual(
            {field.name for field in fields(CollectorRemovalSessionTicket)},
            {
                "collector_pn",
                "identity_source",
                "session_id",
                "listener_host",
                "listener_port",
            },
        )

        import custom_components.eybond_local.connection.removal_finalization as module

        source = inspect.getsource(module)
        self.assertNotIn("peer_ip", source)
        self.assertNotIn("collector_ip", source)
        self.assertNotIn("release_collector_connections", source)


class RemovalQuarantineArchitectureTests(unittest.TestCase):
    def test_quarantine_resumes_only_after_full_entry_setup_succeeds(self) -> None:
        import custom_components.eybond_local as integration

        claim_source = inspect.getsource(
            integration._register_entry_callback_session_claim
        )
        self.assertNotIn("resume_entry_sessions", claim_source)

        source = inspect.getsource(integration.async_setup_entry)
        tree = ast.parse(source)
        resume_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "resume_entry_sessions"
        ]
        self.assertEqual(len(resume_calls), 1)
        setup_try = next(node for node in ast.walk(tree) if isinstance(node, ast.Try))
        terminal_handler_line = max(
            handler.end_lineno or handler.lineno for handler in setup_try.handlers
        )
        self.assertGreater(resume_calls[0].lineno, terminal_handler_line)


class RemovalFinalizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_restarts_only_the_exact_ticketed_session(self) -> None:
        sessions = [
            _session(session_id="listener-8899-v901", pn=OTHER_PN),
            _session(session_id="listener-8899-e500", pn=PN),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        restarted: list[str] = []

        class _Channel:
            def __init__(self, **kwargs) -> None:
                self._session_provider = kwargs["session_id_provider"]
                self._handle_provider = kwargs["handle_provider"]

            async def async_send_restart(self) -> None:
                sid = self._session_provider()
                handle = self._handle_provider()
                assert handle is not None
                assert handle.session_id == sid == "listener-8899-e500"
                restarted.append(sid)
                sessions[:] = [row for row in sessions if row["session_id"] != sid]

            async def async_close(self) -> None:
                return None

        ticket = CollectorRemovalSessionTicket(
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            session_id="listener-8899-e500",
            listener_host="0.0.0.0",
            listener_port=8899,
        )
        module = "custom_components.eybond_local.connection.removal_finalization"
        with patch(f"{module}.ObservedSessionRestartChannel", _Channel):
            result = await async_finalize_collector_entry_removal(ticket, registry)

        self.assertEqual(result.status, REMOVAL_RESTART_CONFIRMED)
        self.assertTrue(result.restarted)
        self.assertTrue(result.disconnect_observed)
        self.assertEqual(restarted, ["listener-8899-e500"])
        self.assertEqual(sessions[0]["session_id"], "listener-8899-v901")
        self.assertEqual(registry.owner_for_pn(PN), "")
        self.assertEqual(registry.owner_for_pn(OTHER_PN), "")

    async def test_foreign_owner_refuses_before_channel_construction(self) -> None:
        sessions = [_session(session_id="listener-8899-e500", pn=PN)]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim_identity("another-entry", PN)
        ticket = CollectorRemovalSessionTicket(
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            session_id="listener-8899-e500",
            listener_host="0.0.0.0",
            listener_port=8899,
        )
        module = "custom_components.eybond_local.connection.removal_finalization"
        with patch(f"{module}.ObservedSessionRestartChannel") as channel:
            result = await async_finalize_collector_entry_removal(ticket, registry)

        self.assertEqual(result.status, REMOVAL_SESSION_CONFLICT)
        channel.assert_not_called()
        self.assertEqual(registry.owner_for_pn(PN), "another-entry")

    async def test_cancellation_closes_channel_and_releases_temporary_owner(self) -> None:
        sessions = [_session(session_id="listener-8899-e500", pn=PN)]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        entered = asyncio.Event()
        closed = asyncio.Event()

        class _Channel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                entered.set()
                await asyncio.Event().wait()

            async def async_close(self) -> None:
                await asyncio.sleep(0)
                closed.set()

        ticket = CollectorRemovalSessionTicket(
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            session_id="listener-8899-e500",
            listener_host="0.0.0.0",
            listener_port=8899,
        )
        module = "custom_components.eybond_local.connection.removal_finalization"
        with patch(f"{module}.ObservedSessionRestartChannel", _Channel):
            task = asyncio.create_task(
                async_finalize_collector_entry_removal(ticket, registry)
            )
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(closed.is_set())
        self.assertEqual(registry.owner_for_pn(PN), "")

    async def test_restart_window_blocks_concurrent_callback_trigger(self) -> None:
        sessions = [_session(session_id="listener-8899-e500", pn=PN)]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        ledger = CallbackTriggerLedger()
        entered = asyncio.Event()
        release = asyncio.Event()

        class _Channel:
            def __init__(self, **_kwargs) -> None:
                return None

            async def async_send_restart(self) -> None:
                entered.set()
                await release.wait()
                sessions.clear()

            async def async_close(self) -> None:
                return None

        ticket = CollectorRemovalSessionTicket(
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            session_id="listener-8899-e500",
            listener_host="0.0.0.0",
            listener_port=8899,
        )
        module = "custom_components.eybond_local.connection.removal_finalization"
        with (
            patch(f"{module}.ObservedSessionRestartChannel", _Channel),
            patch(f"{module}.get_callback_trigger_ledger", return_value=ledger),
        ):
            task = asyncio.create_task(
                async_finalize_collector_entry_removal(ticket, registry)
            )
            await entered.wait()
            with self.assertRaises(CallbackTriggerInhibitedError):
                with ledger.callback_send_scope():
                    self.fail("callback send entered the removal silence window")
            release.set()
            result = await task

        self.assertEqual(result.status, REMOVAL_RESTART_CONFIRMED)
        # The inhibitor is released on the terminal path.
        with ledger.callback_send_scope():
            pass


class RemovalLifecycleIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_remove_consumes_unload_ticket_before_evidence_cleanup(self) -> None:
        from custom_components.eybond_local import async_remove_entry
        from custom_components.eybond_local.connection.removal_finalization import (
            CollectorRemovalFinalizationResult,
        )

        with tempfile.TemporaryDirectory() as config_dir:
            class _Hass:
                def __init__(self) -> None:
                    self.data = {}
                    self.config = types.SimpleNamespace(path=lambda: config_dir)

                async def async_add_executor_job(self, call):
                    return call()

            hass = _Hass()
            discovery = PassiveCallbackDiscovery(hass)
            session = _session(session_id="listener-8899-e500", pn=PN)

            class _Listener:
                def discovered_collector_sessions(self):
                    return (session,)

            discovery._listeners[8899] = _Listener()
            hass.data[DOMAIN] = {
                "passive_callback_discovery": discovery,
                "callback_session_registry": discovery.registry,
            }
            discovery.registry.claim("entry-e500", collector_pn=PN)
            discovery.retire_entry_sessions("entry-e500")
            discovery.registry.release("entry-e500")

            entry = types.SimpleNamespace(
                entry_id="entry-e500",
                data={"collector_pn": PN},
            )
            finalized = AsyncMock(
                return_value=CollectorRemovalFinalizationResult(
                    status=REMOVAL_RESTART_CONFIRMED,
                    restarted=True,
                    disconnect_observed=True,
                )
            )
            with (
                patch(
                    "custom_components.eybond_local.connection.removal_finalization.async_finalize_collector_entry_removal",
                    finalized,
                ),
                patch(
                    "custom_components.eybond_local.support.cloud_evidence.remove_cloud_evidence_for_entry",
                    return_value=[],
                ),
                patch(
                    "custom_components.eybond_local._async_ensure_listener_entry",
                    new=AsyncMock(),
                ),
            ):
                await async_remove_entry(hass, entry)

            finalized.assert_awaited_once()
            ticket, passed_registry = finalized.await_args.args
            self.assertEqual(ticket.session_id, "listener-8899-e500")
            self.assertIs(passed_registry, discovery.registry)
            self.assertIsNone(
                discovery.take_entry_removal_ticket("entry-e500")
            )


if __name__ == "__main__":
    unittest.main()
