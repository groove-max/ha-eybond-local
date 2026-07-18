"""The ONE verified connection-strategy transition authority (Batch 8).

Unit-proves the orchestrator in ``connection/strategy_transition.py`` against
the REAL pieces it reuses: the real ``CallbackSessionRegistry`` (the entry's
own durable claim), the real recovery engine (baseline, causality lease, the
exactly-one-``set>server`` gate), the real ``RecoveryTerminalInput`` /
``merge_recovery_contract`` writer. Only the collector-side effects (endpoint
write / reboot / trigger datagram) are scripted fakes at the OS boundary.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.callback_ledger import (  # noqa: E402
    CallbackTriggerLedger,
)
from custom_components.eybond_local.connection.recovery_contract import (  # noqa: E402
    RECOVERY_CONTRACT_KEY,
    RecoveryContract,
)
from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    CallbackSessionRegistry,
)
from custom_components.eybond_local.connection.strategy_transition import (  # noqa: E402
    TRANSITION_CALLBACK_ROUTE_REQUIRED,
    TRANSITION_ENDPOINT_REQUIRED,
    TRANSITION_ENDPOINT_WRITE_FAILED,
    TRANSITION_INBOUND_RECOVERED_INSTEAD,
    TRANSITION_NOT_REQUIRED,
    TRANSITION_ROLLBACK_ENDPOINT_UNAVAILABLE,
    TRANSITION_SESSION_UNAVAILABLE,
    async_run_strategy_transition,
)
from custom_components.eybond_local.onboarding.strategy_verification import (  # noqa: E402
    CallbackRecoveryRoute,
)
from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)
from dataclasses import replace  # noqa: E402

# Synthetic identities only.
FULL_PN = "V001020SYN62344022"
OTHER_FULL_PN = "V000405SYN94677058"
OLD_SESSION = "sock-1"
NEW_SESSION = "sock-2"
ENTRY_ID = "entry-under-transition"
TS = "2026-07-17T10:00:00+00:00"

FAST_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    inbound_strong_identity_timeout=1.0,
    inbound_restart_disconnect_timeout=1.0,
    inbound_reconnect_timeout=0.6,
    callback_recovery_session_wait=0.8,
    callback_causality_lease_wait=0.5,
)


def _session(session_id: str, pn: str, state: str = "identified") -> dict[str, object]:
    return {
        "session_id": session_id,
        "peer_ip": "203.0.113.10",
        "listener_port": 18899,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": "eybond_framed",
        "collector_identity_source": "fc2_parameter_2",
    }


def _route() -> CallbackRecoveryRoute:
    return CallbackRecoveryRoute(
        bind_ip="192.168.1.50",
        trigger_target_ip="203.0.113.10",
        trigger_udp_port=58899,
        advertised_ha_host="198.51.100.20",  # a PUBLIC NAT address, verbatim
        advertised_ha_port=18899,
        listener_port=18899,
    )


class _Sender:
    """Scripted trigger edge: records the route, feeds the ledger, answers."""

    def __init__(self, *, ledger, on_send=None) -> None:
        self._ledger = ledger
        self._on_send = on_send
        self.routes: list[CallbackRecoveryRoute] = []

    async def async_send(self, route) -> None:
        self.routes.append(route)
        self._ledger.record(
            target=route.trigger_target_ip, source="strategy_transition_test"
        )
        if self._on_send is not None:
            self._on_send()


class _Harness:
    """One transition test bench around the REAL registry + REAL contract."""

    def __init__(self, *, current_strategy: str) -> None:
        self.inventory: list[dict[str, object]] = [_session(OLD_SESSION, FULL_PN)]
        self.registry = CallbackSessionRegistry(
            sessions_source=lambda: tuple(self.inventory)
        )
        self.registry.claim_session(ENTRY_ID, session_id=OLD_SESSION)
        self.registry.promote_claim_to_full_pn(ENTRY_ID, FULL_PN)
        self.ledger = CallbackTriggerLedger()
        self.current_strategy = current_strategy
        self.write_calls: list[str] = []
        self.reboot_calls = 0
        self.written_provenance: list[str] = []
        self.restored_provenance: list[str] = []
        self.committed: dict[str, object] | None = None
        self.committed_options: dict[str, object] = {}
        self.commit_terminals: list[object] = []
        self.entry_data: dict[str, object] = {"collector_pn": FULL_PN}
        self.commit_refusal_override: str | None = None
        self.reconnect_after_restart = True
        # --- write-ahead ordering instrumentation (Blocker 3/4/6) ----------
        # ONE ordered event log across every side-effecting hook, plus the
        # typed states handed to the persistence hooks, so a test can assert
        # the exact order and inject a crash at any boundary.
        self.events: list[str] = []
        self.pending_states: list[object] = []
        self.confirmed_states: list[object] = []
        self.persist_pending_error: BaseException | None = None
        self.persist_pending_refusal: object = None
        self.persist_confirmed_error: BaseException | None = None
        # The single persisted key (mirrors CONF_STRATEGY_TRANSITION_STATE in
        # entry.data): last write wins, exactly one record at a time.
        self.persisted_state_record: object = None

    # --- collector-side scripted effects ---------------------------------
    def _drop_old_add_new(self, pn: str = FULL_PN) -> None:
        self.inventory[:] = [s for s in self.inventory if s["session_id"] != OLD_SESSION]
        if self.reconnect_after_restart:
            self.inventory.append(_session(NEW_SESSION, pn))

    async def write_endpoint(self, endpoint: str):
        self.events.append("endpoint_write")
        self.write_calls.append(endpoint)
        self._drop_old_add_new()
        return {"status": "applied", "readback_endpoint": endpoint}

    async def write_endpoint_no_reconnect(self, endpoint: str):
        self.events.append("endpoint_write")
        self.write_calls.append(endpoint)
        self.inventory[:] = [
            s for s in self.inventory if s["session_id"] != OLD_SESSION
        ]
        return {"status": "applied", "readback_endpoint": endpoint}

    async def reboot(self):
        self.events.append("reboot")
        self.reboot_calls += 1
        self._drop_old_add_new()
        return {"status": "reboot_requested"}

    # --- persistence hooks -------------------------------------------------
    def on_written(self, value: str) -> None:
        self.written_provenance.append(value)

    def on_restored(self, value: str) -> None:
        self.events.append("on_restored")
        self.restored_provenance.append(value)

    def persist_pending(self, state):
        # WRITE-AHEAD hook: the authority MUST call this before the first side
        # effect. An injected error/refusal proves the authority stops here.
        # Mirrors the coordinator: the ONE persisted key holds ``to_record()``.
        self.events.append("persist_pending")
        self.pending_states.append(state)
        if self.persist_pending_error is not None:
            raise self.persist_pending_error
        self.persisted_state_record = state.to_record()
        return self.persist_pending_refusal

    def persist_confirmed(self, state):
        # ONE durable write at the confirmed-restore boundary (overwrites the
        # single persisted key, exactly as the coordinator's one entry-data
        # write does).
        self.events.append("persist_confirmed")
        self.confirmed_states.append(state)
        if self.persist_confirmed_error is not None:
            raise self.persist_confirmed_error
        self.persisted_state_record = state.to_record()

    async def commit(self, updates, terminal, option_updates) -> str:
        self.events.append("commit")
        if self.commit_refusal_override is not None:
            return self.commit_refusal_override
        data = dict(self.entry_data)
        data.update(updates)
        from custom_components.eybond_local.onboarding.recovery_terminalization import (
            merge_recovery_contract,
        )

        refusal = merge_recovery_contract(data, terminal)
        if refusal:
            return refusal
        self.entry_data = data
        self.committed = dict(updates)
        self.committed_options = dict(option_updates or {})
        self.commit_terminals.append(terminal)
        return ""

    # --- run ---------------------------------------------------------------
    async def run(self, *, target: str, **kwargs):
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            StrategyTransitionRecoveryState,
        )

        base = dict(
            target_strategy=target,
            current_strategy=self.current_strategy,
            collector_pn=FULL_PN,
            owner_id=ENTRY_ID,
            registry=self.registry,
            claimed_session_id=lambda: self.registry.claimed_session_id(ENTRY_ID),
            live_wire=lambda: "eybond_framed",
            clock=lambda: TS,
            commit=self.commit,
            policy=FAST_POLICY,
            ledger=self.ledger,
            poll_interval=0.02,
            # A valid pre-built TYPED recovery state (startable pending phase)
            # by default -- exactly what the coordinator builds before any side
            # effect, its route MATCHING ``_route()``. The preflight tests
            # override ``recovery_state`` with None / a foreign / a wrong-type
            # value. The write-ahead + confirmed persistence hooks default to
            # the harness recorders.
            recovery_state=StrategyTransitionRecoveryState.create(
                collector_pn=FULL_PN,
                now=TS,
                trigger_target_host="203.0.113.10",
                trigger_udp_port=58899,
                advertised_host="198.51.100.20",
                advertised_port=18899,
                trigger_bind_host="192.168.1.50",
                listener_bind_host="192.168.1.50",
                local_listener_port=18899,
            ),
            persist_pending=self.persist_pending,
            persist_confirmed=self.persist_confirmed,
        )
        base.update(kwargs)
        return await async_run_strategy_transition(**base)


class CallbackToInboundTransitionTests(unittest.IsolatedAsyncioTestCase):
    """B. callback_on_demand -> inbound."""

    async def test_write_then_single_restart_then_proof_then_strategy(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        prepared: list[int] = []

        async def _prepare(port: int) -> None:
            prepared.append(port)

        result = await h.run(
            target="inbound",
            inbound_endpoint="198.51.100.20:18899",  # explicit PUBLIC NAT host
            endpoint_needs_write=True,
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
            prepare_listener=_prepare,
            local_listener_port=8899,
            on_endpoint_written=h.on_written,
        )

        self.assertTrue(result.success, result.failure_reason)
        # The explicit endpoint went to the collector VERBATIM — no local-IP
        # substitution, no peer-IP derivation.
        self.assertEqual(h.write_calls, ["198.51.100.20:18899"])
        # The LOCAL listener bind, never the advertised/forwarded port.
        self.assertEqual(prepared, [8899])
        # Exactly ONE apply/restart lifecycle: the write IS the restart.
        self.assertEqual(h.reboot_calls, 0)
        # Zero UDP in the inbound path.
        self.assertEqual(h.ledger.snapshot_generation(), 0)
        # Provenance was earned at write-confirm time.
        self.assertEqual(h.written_provenance, ["198.51.100.20:18899"])
        # The strategy landed ONLY via the success commit, with honest axes.
        assert h.committed is not None
        self.assertEqual(h.committed["connection_strategy"], "inbound")
        self.assertEqual(h.committed["endpoint_control_policy"], "integration_managed")
        self.assertEqual(
            h.committed["endpoint_written_value"], "198.51.100.20:18899"
        )
        # The REAL RecoveryContract carries the inbound proof.
        contract = RecoveryContract.from_entry_data(h.entry_data)
        assert contract is not None
        self.assertTrue(contract.inbound_verified)
        # The entry's claim followed the collector onto the NEW socket.
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), NEW_SESSION)

    async def test_same_endpoint_without_write_keeps_policy_external(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        result = await h.run(
            target="inbound",
            inbound_endpoint="192.168.1.50:18899",
            endpoint_needs_write=False,
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
            on_endpoint_written=h.on_written,
        )
        self.assertTrue(result.success, result.failure_reason)
        # No write happened: one plain reboot, no provenance, and the commit
        # deliberately carries NO endpoint_control_policy key (external stays
        # external; integration_managed can never appear retroactively).
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 1)
        self.assertEqual(h.written_provenance, [])
        assert h.committed is not None
        self.assertEqual(h.committed["connection_strategy"], "inbound")
        self.assertNotIn("endpoint_control_policy", h.committed)
        self.assertFalse(result.endpoint_written)

    async def test_missing_endpoint_is_typed_and_touches_nothing(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        result = await h.run(
            target="inbound",
            inbound_endpoint="",
            endpoint_needs_write=True,
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_ENDPOINT_REQUIRED)
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 0)
        self.assertIsNone(h.committed)


class InboundToCallbackTransitionTests(unittest.IsolatedAsyncioTestCase):
    """C. inbound -> callback_on_demand."""

    async def test_restore_then_one_trigger_then_proof_then_strategy(self) -> None:
        h = _Harness(current_strategy="inbound")
        # Restore drops the old socket and the collector does NOT come back on
        # its own (it now points at the vendor cloud); only the single
        # set>server brings it in.
        sender = _Sender(
            ledger=h.ledger,
            on_send=lambda: h.inventory.append(_session(NEW_SESSION, FULL_PN)),
        )
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertTrue(result.success, result.failure_reason)
        # The restore used ONLY the saved provenance endpoint, exactly once.
        self.assertEqual(h.write_calls, ["vendor.example.net:5074"])
        self.assertEqual(h.reboot_calls, 0)
        self.assertEqual(h.restored_provenance, ["vendor.example.net:5074"])
        self.assertTrue(result.endpoint_restored)
        # Exactly ONE logical set>server sequence.
        self.assertEqual(len(sender.routes), 1)
        self.assertEqual(h.ledger.snapshot_generation(), 1)
        # The advertised HA endpoint travelled VERBATIM (public NAT address).
        self.assertEqual(sender.routes[0].advertised_ha_host, "198.51.100.20")
        self.assertEqual(sender.routes[0].advertised_ha_port, 18899)
        # Success commit: strategy + honestly-external policy.
        assert h.committed is not None
        self.assertEqual(h.committed["connection_strategy"], "callback_on_demand")
        self.assertEqual(h.committed["endpoint_control_policy"], "external")
        contract = RecoveryContract.from_entry_data(h.entry_data)
        assert contract is not None
        self.assertTrue(contract.callback_verified)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), NEW_SESSION)

    async def test_missing_rollback_endpoint_is_typed_before_any_side_effect(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="",
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(
            result.failure_reason, TRANSITION_ROLLBACK_ENDPOINT_UNAVAILABLE
        )
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 0)
        self.assertEqual(sender.routes, [])
        self.assertIsNone(h.committed)

    async def test_external_policy_needs_no_restore_and_keeps_policy_untouched(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False  # gone to its external endpoint
        sender = _Sender(
            ledger=h.ledger,
            on_send=lambda: h.inventory.append(_session(NEW_SESSION, FULL_PN)),
        )
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            restore_endpoint="",
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertTrue(result.success, result.failure_reason)
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 1)
        self.assertEqual(h.restored_provenance, [])
        assert h.committed is not None
        self.assertNotIn("endpoint_control_policy", h.committed)

    async def test_missing_route_is_typed_input_required(self) -> None:
        h = _Harness(current_strategy="inbound")
        result = await h.run(
            target="callback_on_demand",
            callback_route=None,
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_CALLBACK_ROUTE_REQUIRED)
        self.assertEqual(h.reboot_calls, 0)
        self.assertIsNone(h.committed)


class TransitionFailureMatrixTests(unittest.IsolatedAsyncioTestCase):
    """D. Partial failures: earned facts survive, the strategy never moves."""

    async def test_preflight_no_session_touches_nothing(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        h.registry.release(ENTRY_ID)
        result = await h.run(
            target="inbound",
            inbound_endpoint="198.51.100.20:18899",
            endpoint_needs_write=True,
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_SESSION_UNAVAILABLE)
        self.assertEqual(h.write_calls, [])
        self.assertIsNone(h.committed)

    async def test_endpoint_write_failure_is_typed_and_earns_nothing(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")

        async def _failing_write(endpoint: str):
            raise RuntimeError("collector_write_failed")

        result = await h.run(
            target="inbound",
            inbound_endpoint="198.51.100.20:18899",
            endpoint_needs_write=True,
            write_endpoint=_failing_write,
            reboot=h.reboot,
            on_endpoint_written=h.on_written,
        )
        self.assertFalse(result.success)
        # The engine types a failing restart step itself: the honest reason is
        # its own restart_not_confirmed (already localized), not a re-label.
        self.assertEqual(result.failure_reason, "restart_not_confirmed")
        # The write did NOT confirm: no provenance was earned.
        self.assertEqual(h.written_provenance, [])
        self.assertFalse(result.endpoint_written)
        self.assertIsNone(h.committed)

    async def test_confirmed_write_with_failed_recovery_keeps_provenance(
        self,
    ) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        h.reconnect_after_restart = False  # the collector never comes back

        result = await h.run(
            target="inbound",
            inbound_endpoint="198.51.100.20:18899",
            endpoint_needs_write=True,
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
            on_endpoint_written=h.on_written,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, "inbound_reconnect_timeout")
        # The write really happened and stays honestly recorded...
        self.assertTrue(result.endpoint_written)
        self.assertEqual(result.endpoint_written_value, "198.51.100.20:18899")
        self.assertEqual(h.written_provenance, ["198.51.100.20:18899"])
        # ...but the strategy did NOT move (no commit at all).
        self.assertIsNone(h.committed)
        self.assertNotIn(RECOVERY_CONTRACT_KEY, h.entry_data)

    async def test_confirmed_restore_with_failed_callback_keeps_external_fact(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)  # trigger sent, nobody answers
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, "callback_recovery_timeout")
        # The restore is a confirmed FACT (policy external persisted by the
        # hook) even though the proof failed; strategy stays inbound.
        self.assertTrue(result.endpoint_restored)
        self.assertEqual(h.restored_provenance, ["vendor.example.net:5074"])
        self.assertIsNone(h.committed)
        self.assertEqual(len(sender.routes), 1)

    async def test_inbound_recovered_instead_merges_fact_without_axis_change(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = True  # endpoint still points here
        sender = _Sender(ledger=h.ledger)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(
            result.failure_reason, TRANSITION_INBOUND_RECOVERED_INSTEAD
        )
        # The autonomous reconnect preempted the callback: ZERO datagrams.
        self.assertEqual(sender.routes, [])
        # The honestly-earned inbound proof was merged WITHOUT axis changes.
        self.assertEqual(h.committed, {})
        contract = RecoveryContract.from_entry_data(h.entry_data)
        assert contract is not None
        self.assertTrue(contract.inbound_verified)
        self.assertFalse(contract.callback_verified)

    async def test_commit_refusal_surfaces_and_strategy_stays(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        h.commit_refusal_override = "recovery_contract_conflict"
        result = await h.run(
            target="inbound",
            inbound_endpoint="192.168.1.50:18899",
            endpoint_needs_write=False,
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, "recovery_contract_conflict")

    async def test_cancellation_propagates_without_commit(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        h.reconnect_after_restart = False  # would wait out the window

        task = asyncio.get_running_loop().create_task(
            h.run(
                target="inbound",
                inbound_endpoint="192.168.1.50:18899",
                endpoint_needs_write=False,
                reboot=h.reboot,
            )
        )
        await asyncio.sleep(0.1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertIsNone(h.committed)

    async def test_not_required_and_no_session_are_preflight_only(self) -> None:
        h = _Harness(current_strategy="inbound")
        result = await h.run(target="inbound", reboot=h.reboot)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_NOT_REQUIRED)
        self.assertEqual(h.reboot_calls, 0)


class NatAndForeignCollectorTests(unittest.IsolatedAsyncioTestCase):
    """E. Two collectors behind one peer IP: only the PN distinguishes them."""

    async def test_foreign_session_on_same_peer_never_proves_the_transition(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False

        def _answer_with_foreign_then_ours() -> None:
            # Both collectors sit behind 203.0.113.10. The FOREIGN one answers
            # first; the engine must keep waiting for OUR identity.
            h.inventory.append(_session("foreign-sock", OTHER_FULL_PN))
            h.inventory.append(_session(NEW_SESSION, FULL_PN))

        sender = _Sender(ledger=h.ledger, on_send=_answer_with_foreign_then_ours)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            reboot=h.reboot,
        )
        self.assertTrue(result.success, result.failure_reason)
        # The claim moved to OUR session, never the foreign one.
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), NEW_SESSION)

    async def test_only_foreign_answer_times_out_without_claiming_it(self) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False
        sender = _Sender(
            ledger=h.ledger,
            on_send=lambda: h.inventory.append(
                _session("foreign-sock", OTHER_FULL_PN)
            ),
        )
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, "callback_recovery_timeout")
        # The foreign session was never adopted: the claim still names the
        # old (closed) socket, not the foreign one.
        self.assertNotEqual(
            h.registry.claimed_session_id(ENTRY_ID), "foreign-sock"
        )
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), OLD_SESSION)
        self.assertIsNone(h.committed)


if __name__ == "__main__":
    unittest.main()


class CorrectiveBlockerTests(unittest.IsolatedAsyncioTestCase):
    """Batch 8 corrective follow-up: each test pins one architectural blocker.

    Written to FAIL on the original WIP (fake handoff string, inventory-string
    wire, unguarded lease window, arbitrary commit payload, swallowed commit
    refusal, NAT listener/advertised mixing, flat restore partial state).
    """

    # -- 1. fake prepared handoff -----------------------------------------
    async def test_permanent_owner_capability_is_registry_certified(self) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False
        certify_calls: list[tuple[str, str]] = []

        real_registry = h.registry

        class _Spy:
            def __getattr__(self, name):
                return getattr(real_registry, name)

            def certify_permanent_owned_session(self, owner, session_id):
                certify_calls.append((owner, session_id))
                return real_registry.certify_permanent_owned_session(
                    owner, session_id
                )

        h.registry = _Spy()
        sender = _Sender(
            ledger=h.ledger,
            on_send=lambda: h.inventory.append(_session(NEW_SESSION, FULL_PN)),
        )
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            reboot=h.reboot,
        )
        self.assertTrue(result.success, result.failure_reason)
        # The capability came from the REGISTRY as a TYPED object: exact
        # owner + exact session (certified at least once; re-verified again
        # right before commit).
        self.assertIn((ENTRY_ID, NEW_SESSION), certify_calls)
        # The outcome carries the typed certification, NOT a handoff owner.
        outcome = result.outcome
        from custom_components.eybond_local.connection.session_registry import (
            PermanentOwnedSessionCertification,
        )
        self.assertIsInstance(
            outcome.owner_certification, PermanentOwnedSessionCertification
        )
        self.assertEqual(outcome.handoff_owner, "")
        # The terminal carries NO prepared-onboarding-handoff owner.
        self.assertEqual(len(h.commit_terminals), 1)
        self.assertEqual(h.commit_terminals[0].prepared_handoff_owner, "")

    async def test_owner_string_without_certification_is_refused(self) -> None:
        # A registry that refuses to certify (e.g. the claim's socket is not
        # the live session) must yield a typed failure, not a proof.
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False

        real_registry = h.registry

        class _RefusingRegistry:
            def __getattr__(self, name):
                return getattr(real_registry, name)

            def certify_permanent_owned_session(self, owner, session_id):
                return None

        h.registry = _RefusingRegistry()
        sender = _Sender(
            ledger=h.ledger,
            on_send=lambda: h.inventory.append(_session(NEW_SESSION, FULL_PN)),
        )
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        self.assertIsNone(h.committed)

    async def test_registry_certification_rejects_foreign_and_closed(self) -> None:
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        inventory = [
            _session(OLD_SESSION, FULL_PN),
            _session("foreign-sock", OTHER_FULL_PN),
            _session("closed-sock", FULL_PN, state="closed"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        registry.claim_session(ENTRY_ID, session_id=OLD_SESSION)
        registry.promote_claim_to_full_pn(ENTRY_ID, FULL_PN)

        # Exact owner + exact live same-PN session -> durable PN.
        self.assertEqual(
            registry.certify_owner_reconnected_session(ENTRY_ID, OLD_SESSION),
            FULL_PN,
        )
        # A session the claim does not hold is refused (no PN lookup).
        self.assertEqual(
            registry.certify_owner_reconnected_session(ENTRY_ID, "foreign-sock"),
            "",
        )
        # A closed session is refused even when the claim names it.
        registry.release(ENTRY_ID)
        registry.claim_session(ENTRY_ID, session_id="closed-sock")
        self.assertEqual(
            registry.certify_owner_reconnected_session(ENTRY_ID, "closed-sock"),
            "",
        )
        # An unknown owner is refused.
        self.assertEqual(
            registry.certify_owner_reconnected_session("nobody", OLD_SESSION),
            "",
        )

    # -- 2. trusted wire authority ----------------------------------------
    async def test_wire_authority_requires_trusted_session_handle(self) -> None:
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )
        from custom_components.eybond_local.connection.strategy_transition import (
            trusted_transition_wire,
        )

        def _raw(session_id, pn, state, shape="eybond_framed"):
            return {
                "session_id": session_id,
                "peer_ip": "203.0.113.10",
                "listener_port": 18899,
                "collector_pn": pn,
                "state": state,
                "protocol_shape": shape,
                "collector_identity_source": "fc2_parameter_2",
            }

        # UNTRUSTED (identity-mismatch) framed-looking session: the sniffed
        # shape / inventory protocol string must NEVER become the authority.
        inventory = [_raw(OLD_SESSION, FULL_PN, "route_identity_mismatch")]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        registry.claim_session(ENTRY_ID, session_id=OLD_SESSION)
        registry.promote_claim_to_full_pn(ENTRY_ID, FULL_PN)
        # The claim is pinned to OLD_SESSION (claim_session), so the 3-way
        # (owner / claim.session_id / handle.session_id) match can hold.
        self.assertEqual(trusted_transition_wire(registry, ENTRY_ID, OLD_SESSION), "")

        # A routed-state/shape CONFLICT negotiates fail-closed: no authority.
        inventory[0] = _raw(OLD_SESSION, FULL_PN, "routed_framed", shape="at_text")
        self.assertEqual(trusted_transition_wire(registry, ENTRY_ID, OLD_SESSION), "")

        # Routed (trusted) framed session -> framed authority.
        inventory[0] = _raw(OLD_SESSION, FULL_PN, "routed_framed")
        self.assertEqual(
            trusted_transition_wire(registry, ENTRY_ID, OLD_SESSION), "eybond_framed"
        )

        # A DIFFERENT session id than the claim pins -> no authority (no
        # fallback to another same-PN session).
        inventory.append(_raw(NEW_SESSION, FULL_PN, "routed_framed"))
        self.assertEqual(trusted_transition_wire(registry, ENTRY_ID, NEW_SESSION), "")
        inventory.pop()

        # Routed AT session -> at_text authority.
        inventory[0] = _raw(
            OLD_SESSION, FULL_PN, "routed_at_text", shape="at_text"
        )
        self.assertEqual(
            trusted_transition_wire(registry, ENTRY_ID, OLD_SESSION), "at_text"
        )

        # A foreign-PN routed session the claim does not pin is NOT authority.
        inventory[0] = _raw("foreign-sock", OTHER_FULL_PN, "routed_framed")
        self.assertEqual(
            trusted_transition_wire(registry, ENTRY_ID, "foreign-sock"), ""
        )

    # -- 4. atomic lease (unit shape: guard must precede every await) ------
    async def test_transition_lease_registry_is_atomic_and_cancel_safe(self) -> None:
        # The lease helper the coordinator uses: acquire is synchronous
        # (atomic between awaits), double-acquire refuses, release is safe
        # after cancellation, and entries do not block each other.
        from custom_components.eybond_local.connection.strategy_transition import (
            StrategyTransitionLease,
        )

        lease = StrategyTransitionLease()
        self.assertTrue(lease.acquire("entry-a"))
        self.assertFalse(lease.acquire("entry-a"))  # second attempt refused
        self.assertTrue(lease.acquire("entry-b"))  # other entries independent
        lease.release("entry-a")
        self.assertTrue(lease.acquire("entry-a"))
        lease.release("entry-a")
        lease.release("entry-a")  # idempotent
        lease.release("entry-b")

    # -- 5. commit payload trust boundary ----------------------------------
    async def test_option_payload_cannot_smuggle_axes(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        result = await h.run(
            target="inbound",
            inbound_endpoint="198.51.100.20:18899",
            endpoint_needs_write=False,
            reboot=h.reboot,
            option_payload={"connection_strategy": "callback_on_demand"},
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, "transition_payload_forbidden")
        self.assertIsNone(h.committed)

    async def test_option_payload_forbidden_keys_matrix(self) -> None:
        for forbidden in (
            "connection_strategy",
            "endpoint_control_policy",
            "endpoint_written_value",
            "endpoint_written_at",
            "collector_pn",
            "recovery_contract",
        ):
            h = _Harness(current_strategy="callback_on_demand")
            result = await h.run(
                target="inbound",
                inbound_endpoint="198.51.100.20:18899",
                endpoint_needs_write=False,
                reboot=h.reboot,
                option_payload={forbidden: "x", "poll_mode": "auto"},
            )
            with self.subTest(forbidden=forbidden):
                self.assertFalse(result.success)
                self.assertEqual(
                    result.failure_reason, "transition_payload_forbidden"
                )
                self.assertIsNone(h.committed)

    async def test_legacy_operation_mode_is_the_only_axis_payload(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        result = await h.run(
            target="inbound",
            inbound_endpoint="192.168.1.50:18899",
            endpoint_needs_write=False,
            reboot=h.reboot,
            legacy_operation_mode="home_assistant_only",
            option_payload={"poll_mode": "auto"},
        )
        self.assertTrue(result.success, result.failure_reason)
        assert h.committed is not None
        self.assertEqual(
            h.committed.get("collector_operation_mode"), "home_assistant_only"
        )
        # The target strategy is still the AUTHORITY's decision.
        self.assertEqual(h.committed["connection_strategy"], "inbound")
        self.assertEqual(h.committed_options.get("poll_mode"), "auto")
        self.assertEqual(
            h.committed_options.get("collector_operation_mode"),
            "home_assistant_only",
        )

    # -- 6. commit refusal must surface in inbound_recovered ---------------
    async def test_inbound_recovered_commit_refusal_is_terminal_failure(self) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = True  # collector comes back on its own
        # A malformed existing contract makes the merge refuse.
        h.entry_data[RECOVERY_CONTRACT_KEY] = {"malformed": True}
        sender = _Sender(ledger=h.ledger)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",
            reboot=h.reboot,
        )
        self.assertFalse(result.success)
        # The persistence refusal is the terminal reason -- never masked by
        # the inbound_recovered_instead label.
        self.assertNotEqual(
            result.failure_reason, TRANSITION_INBOUND_RECOVERED_INSTEAD
        )
        self.assertTrue(result.failure_reason)
        # The malformed record was left byte-for-byte untouched.
        self.assertEqual(h.entry_data[RECOVERY_CONTRACT_KEY], {"malformed": True})

    # -- 7. NAT: local listener vs advertised endpoint ---------------------
    async def test_prepare_listener_binds_local_port_never_advertised(self) -> None:
        h = _Harness(current_strategy="callback_on_demand")
        prepared_ports: list[int] = []

        async def _prepare(port: int) -> None:
            prepared_ports.append(port)

        result = await h.run(
            target="inbound",
            # Public NAT endpoint: forwarded port 18899 -> local 8899.
            inbound_endpoint="public.example:18899",
            endpoint_needs_write=True,
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
            prepare_listener=_prepare,
            local_listener_port=8899,
            on_endpoint_written=h.on_written,
        )
        self.assertTrue(result.success, result.failure_reason)
        # The integration prepared its LOCAL listener port, and never tried
        # to bind the advertised/forwarded port locally.
        self.assertEqual(prepared_ports, [8899])
        # The collector still received the PUBLIC endpoint verbatim.
        self.assertEqual(h.write_calls, ["public.example:18899"])

    # -- 3. restore partial state must not stay silently flat --------------
    async def test_restore_then_timeout_persists_confirmed_unproven_state(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        )

        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)  # nobody answers the one trigger
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        self.assertTrue(result.endpoint_restored)
        # The entry is NOT left as a silently-flat "inbound + external": at the
        # confirmed-restore boundary ONE durable write advanced the recovery
        # state to the confirmed-unproven phase (policy already external), so
        # the canonical strategy no longer silently claims inbound health.
        self.assertEqual(len(h.confirmed_states), 1)
        self.assertEqual(
            h.confirmed_states[0].phase, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN
        )
        self.assertEqual(result.degraded_state, "callback_restore_unproven")
        # Write-ahead happened BEFORE the restore, confirmed AFTER it.
        self.assertEqual(
            h.events,
            ["persist_pending", "endpoint_write", "persist_confirmed", "on_restored"],
        )

    async def test_restore_then_cancellation_keeps_confirmed_state(self) -> None:
        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)

        task = asyncio.get_running_loop().create_task(
            h.run(
                target="callback_on_demand",
                callback_route=_route(),
                trigger_sender=sender,
                endpoint_control_policy="integration_managed",
                restore_endpoint="vendor.example.net:5074",
                write_endpoint=h.write_endpoint_no_reconnect,
                reboot=h.reboot,
                on_endpoint_restored=h.on_restored,
            )
        )
        await asyncio.sleep(0.3)  # let the restore confirm, then cancel
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # The restore had already CONFIRMED: the confirmed-unproven state was
        # persisted at that boundary and is NOT deleted by the cancellation
        # (CancelledError still propagates unchanged). Nothing is rolled back.
        self.assertEqual(h.restored_provenance, ["vendor.example.net:5074"])
        self.assertEqual(len(h.pending_states), 1)
        self.assertEqual(len(h.confirmed_states), 1)
        self.assertNotIn("commit", h.events)

    async def test_recovered_after_restore_has_honest_reason(self) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = True  # comes back DESPITE the restore
        sender = _Sender(ledger=h.ledger)

        async def _restore_but_reconnect(endpoint: str):
            h.events.append("endpoint_write")
            h.write_calls.append(endpoint)
            h._drop_old_add_new()
            return {"status": "applied", "readback_endpoint": endpoint}

        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=_restore_but_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        # After a CONFIRMED restore the user must never be told the endpoint
        # still points at Home Assistant.
        self.assertEqual(
            result.failure_reason, "transition_inbound_recovered_after_restore"
        )
        # The restore DID confirm (endpoint external), so the confirmed-unproven
        # state was persisted -- but the lone autonomous reconnect proves no
        # durable inbound future, so the commit carries no strategy and the
        # state is deliberately NOT cleared.
        self.assertEqual(len(h.confirmed_states), 1)
        self.assertIsNotNone(h.committed)
        self.assertNotIn("connection_strategy", h.committed)


class RegistryPermanentCapabilityTests(unittest.IsolatedAsyncioTestCase):
    """Blocker 5/6: typed permanent-owner capability + session-pinned handle."""

    def _registry(self, inventory):
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        reg = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        return reg

    async def test_certification_is_typed_and_reverified(self) -> None:
        from custom_components.eybond_local.connection.session_registry import (
            PermanentOwnedSessionCertification,
        )

        inventory = [_session(OLD_SESSION, FULL_PN)]
        reg = self._registry(inventory)
        reg.claim_session(ENTRY_ID, session_id=OLD_SESSION)
        reg.promote_claim_to_full_pn(ENTRY_ID, FULL_PN)

        cert = reg.certify_permanent_owned_session(ENTRY_ID, OLD_SESSION)
        self.assertIsInstance(cert, PermanentOwnedSessionCertification)
        self.assertEqual(cert.owner_id, ENTRY_ID)
        self.assertEqual(cert.session_id, OLD_SESSION)
        self.assertEqual(cert.collector_pn, FULL_PN)
        # Re-verify accepts the live capability...
        self.assertTrue(reg.reverify_permanent_owned_session(cert))
        # ...but rejects a forged look-alike (strict type)...
        class _Forged:
            owner_id = ENTRY_ID
            session_id = OLD_SESSION
            collector_pn = FULL_PN

        forged = _Forged()
        self.assertFalse(reg.reverify_permanent_owned_session(forged))
        # ...and rejects a STALE one after the claim retargets to a new socket
        # (a certification names the EXACT session it was issued for).
        inventory.append(_session(NEW_SESSION, FULL_PN))
        self.assertTrue(reg.pin_owner_claim_to_session(ENTRY_ID, NEW_SESSION))
        self.assertFalse(reg.reverify_permanent_owned_session(cert))
        self.assertIsNone(
            reg.certify_permanent_owned_session(ENTRY_ID, OLD_SESSION)
        )

    async def test_certification_rejects_foreign_and_unpinned(self) -> None:
        inventory = [
            _session(OLD_SESSION, FULL_PN),
            _session("foreign", OTHER_FULL_PN),
        ]
        reg = self._registry(inventory)
        reg.claim_session(ENTRY_ID, session_id=OLD_SESSION)
        reg.promote_claim_to_full_pn(ENTRY_ID, FULL_PN)
        # A session the claim does not pin is refused (no PN lookup).
        self.assertIsNone(reg.certify_permanent_owned_session(ENTRY_ID, "foreign"))
        # An unknown owner is refused.
        self.assertIsNone(reg.certify_permanent_owned_session("nobody", OLD_SESSION))

    async def test_pin_then_session_handle_for_owned_session(self) -> None:
        raw = {
            "session_id": OLD_SESSION,
            "peer_ip": "203.0.113.10",
            "listener_port": 18899,
            "collector_pn": FULL_PN,
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }
        inventory = []
        reg = self._registry(inventory)
        # A durable by-PN claim with NO observed session yet (session_id empty).
        reg.claim(ENTRY_ID, collector_pn=FULL_PN)
        inventory.append(raw)  # the collector now dials in
        # The claim is not pinned to the socket, so the exact resolver is None.
        self.assertIsNone(reg.session_handle_for_owned_session(ENTRY_ID, OLD_SESSION))
        # The explicit pin op records the session id...
        self.assertTrue(reg.pin_owner_claim_to_session(ENTRY_ID, OLD_SESSION))
        handle = reg.session_handle_for_owned_session(ENTRY_ID, OLD_SESSION)
        self.assertIsNotNone(handle)
        self.assertEqual(handle.session_id, OLD_SESSION)
        # A different session id is never returned for this claim.
        self.assertIsNone(reg.session_handle_for_owned_session(ENTRY_ID, "other"))
        # Pinning a foreign/absent session id fails.
        self.assertFalse(reg.pin_owner_claim_to_session(ENTRY_ID, "absent"))


class AtomicRestoreAndInboundRecoveredTests(unittest.IsolatedAsyncioTestCase):
    """Blocker 2/4: restore-boundary persistence + inbound_recovered_after_restore."""

    async def test_inbound_recovered_after_restore_is_typed_and_keeps_state(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = True  # collector comes back on its own
        sender = _Sender(ledger=h.ledger)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            # The restore drops the old socket; the collector then autonomously
            # re-dials (write_endpoint adds the new session) -> inbound_recovered.
            write_endpoint=h.write_endpoint,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        # After a CONFIRMED restore the reason is the after-restore variant,
        # never the "still points at HA" one.
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_INBOUND_RECOVERED_AFTER_RESTORE,
        )

        self.assertEqual(
            result.failure_reason, TRANSITION_INBOUND_RECOVERED_AFTER_RESTORE
        )
        self.assertTrue(result.endpoint_restored)
        # The inbound_recovered commit carries NO connection_strategy, so the
        # coordinator commit would NOT clear the recovery state.
        self.assertIsNotNone(h.committed)
        self.assertNotIn("connection_strategy", h.committed)

    async def test_preflight_state_failure_before_restore_has_no_side_effects(
        self,
    ) -> None:
        # Blocker 2: if the typed recovery state could not be built/validated
        # BEFORE the physical restore, the restore is REFUSED here -- no
        # endpoint write, no reboot, no UDP trigger, no commit.
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_RECOVERY_STATE_UNAVAILABLE,
        )

        h = _Harness(current_strategy="inbound")
        writes: list[str] = []
        restored: list[str] = []

        async def _write_endpoint(endpoint: str):
            writes.append(endpoint)
            return {"status": "applied", "readback_endpoint": endpoint}

        sender = _Sender(ledger=h.ledger)
        gen_before = h.ledger.snapshot_generation()
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=_write_endpoint,
            reboot=h.reboot,
            on_endpoint_restored=restored.append,
            recovery_state=None,  # forced build failure
        )
        self.assertFalse(result.success)
        self.assertEqual(
            result.failure_reason, TRANSITION_RECOVERY_STATE_UNAVAILABLE
        )
        # ZERO management writes, ZERO restore confirmations, ZERO UDP, and the
        # write-ahead hook was NEVER reached (validation fails before it).
        self.assertEqual(writes, [])
        self.assertEqual(restored, [])
        self.assertEqual(sender.routes, [])
        self.assertEqual(h.ledger.snapshot_generation(), gen_before)
        self.assertEqual(h.pending_states, [])
        self.assertEqual(h.events, [])
        # No commit (data/options byte-for-byte unchanged).
        self.assertIsNone(h.committed)


class WriteAheadOrderingTests(unittest.IsolatedAsyncioTestCase):
    """Blocker 2/3/4/6: typed boundary + write-ahead ORDER + crash/cancel.

    The authority owns the order: persist_pending BEFORE the first side effect,
    persist_confirmed as ONE local write AFTER a confirmed restore, terminal
    commit LAST. The event log proves the order; injected hook errors prove the
    crash boundaries.
    """

    def _valid_state(self, **overrides):
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            StrategyTransitionRecoveryState,
        )

        base = dict(
            collector_pn=FULL_PN,
            now=TS,
            trigger_target_host="203.0.113.10",
            trigger_udp_port=58899,
            advertised_host="198.51.100.20",
            advertised_port=18899,
            trigger_bind_host="192.168.1.50",
            listener_bind_host="192.168.1.50",
            local_listener_port=18899,
        )
        base.update(overrides)
        return StrategyTransitionRecoveryState.create(**base)

    def _reconnecting_sender(self, h):
        return _Sender(
            ledger=h.ledger,
            on_send=lambda: h.inventory.append(_session(NEW_SESSION, FULL_PN)),
        )

    def _assert_no_side_effects(self, h, sender, gen_before):
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 0)
        self.assertEqual(sender.routes, [])
        self.assertEqual(h.ledger.snapshot_generation(), gen_before)
        self.assertEqual(h.pending_states, [])
        self.assertEqual(h.confirmed_states, [])
        self.assertEqual(h.events, [])
        self.assertIsNone(h.committed)

    # -- success ORDER: restore path ---------------------------------------
    async def test_restore_success_order_is_pending_write_confirmed_commit(
        self,
    ) -> None:
        import json

        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_PENDING,
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        )

        h = _Harness(current_strategy="inbound")
        sender = self._reconnecting_sender(h)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertTrue(result.success, result.failure_reason)
        # THE exact order: write-ahead intent, THEN the restore, THEN the ONE
        # confirmed write, THEN (proof) the terminal strategy commit.
        self.assertEqual(
            h.events,
            [
                "persist_pending",
                "endpoint_write",
                "persist_confirmed",
                "on_restored",
                "commit",
            ],
        )
        # The pending state is JSON-safe, byte-stable and in the startable phase.
        pend = h.pending_states[0]
        self.assertEqual(pend.phase, RECOVERY_PHASE_PENDING)
        rec = pend.to_record()
        self.assertEqual(json.loads(json.dumps(rec)), rec)
        # The confirmed write advanced ONLY the phase; identity + route are the
        # same durable capability.
        conf = h.confirmed_states[0]
        self.assertEqual(conf.phase, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN)
        self.assertEqual(conf.collector_pn, pend.collector_pn)
        self.assertEqual(conf.callback_route(), pend.callback_route())
        # The terminal commit carries the strategy (the coordinator uses that to
        # clear the state); success = the whole order completed.
        assert h.committed is not None
        self.assertEqual(h.committed["connection_strategy"], "callback_on_demand")

    # -- success ORDER: reboot path at ALREADY-external policy --------------
    async def test_reboot_path_writes_ahead_before_reboot(self) -> None:
        # Blocker 3: write-ahead intent is needed before the FIRST destructive
        # side effect of ANY inbound->callback transition, INCLUDING a reboot at
        # already-external policy (no restore happens, so no persist_confirmed).
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False  # only the trigger brings it back
        sender = self._reconnecting_sender(h)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",  # already external -> reboot
            reboot=h.reboot,
            write_endpoint=h.write_endpoint,
            on_endpoint_restored=h.on_restored,
        )
        self.assertTrue(result.success, result.failure_reason)
        self.assertEqual(h.events, ["persist_pending", "reboot", "commit"])
        self.assertEqual(len(h.pending_states), 1)  # write-ahead still happened
        self.assertEqual(h.confirmed_states, [])  # nothing was restored
        self.assertEqual(h.write_calls, [])  # reboot, not endpoint write

    # -- Blocker 2: TYPED boundary rejects everything that is not the type --
    async def test_untyped_or_mismatched_state_rejected_with_no_side_effects(
        self,
    ) -> None:
        from types import SimpleNamespace

        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_RECOVERY_STATE_INVALID,
        )
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        )

        valid = self._valid_state()
        cases = {
            # A TRUTHY dict that round-trips to a valid state is STILL not the
            # type -- truthiness is never enough.
            "raw_dict": valid.to_record(),
            # A fully duck-typed look-alike (right attrs + callback_route()) is
            # rejected on the exact-type check before any attribute is trusted.
            "duck": SimpleNamespace(
                collector_pn=FULL_PN,
                target_strategy="callback_on_demand",
                phase="transition_pending",
                callback_route=lambda: _route(),
            ),
            # Same shape, foreign durable PN.
            "foreign_pn": self._valid_state(collector_pn=OTHER_FULL_PN),
            # Same PN, a route that differs by ONE field (advertised port).
            "foreign_route": self._valid_state(advertised_port=9999),
            # Right type + identity + route, but not the startable phase.
            "wrong_phase": valid.with_phase(
                RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=TS
            ),
        }
        for name, bad in cases.items():
            with self.subTest(case=name):
                h = _Harness(current_strategy="inbound")
                sender = _Sender(ledger=h.ledger)
                gen_before = h.ledger.snapshot_generation()
                result = await h.run(
                    target="callback_on_demand",
                    callback_route=_route(),
                    trigger_sender=sender,
                    endpoint_control_policy="integration_managed",
                    restore_endpoint="vendor.example.net:5074",
                    write_endpoint=h.write_endpoint_no_reconnect,
                    reboot=h.reboot,
                    on_endpoint_restored=h.on_restored,
                    recovery_state=bad,
                )
                self.assertFalse(result.success)
                self.assertEqual(
                    result.failure_reason, TRANSITION_RECOVERY_STATE_INVALID
                )
                self._assert_no_side_effects(h, sender, gen_before)

    # -- Blocker 4: persist_pending refusal/crash stops EVERYTHING ----------
    async def test_persist_pending_raise_blocks_all_side_effects(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_PERSIST_PENDING_FAILED,
        )

        h = _Harness(current_strategy="inbound")
        h.persist_pending_error = RuntimeError("write-ahead store is down")
        sender = _Sender(ledger=h.ledger)
        gen_before = h.ledger.snapshot_generation()
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_PERSIST_PENDING_FAILED)
        # The hook WAS reached (write-ahead attempted) but nothing followed it.
        self.assertEqual(h.events, ["persist_pending"])
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 0)
        self.assertEqual(sender.routes, [])
        self.assertEqual(h.ledger.snapshot_generation(), gen_before)
        self.assertIsNone(h.committed)

    async def test_persist_pending_refusal_blocks_all_side_effects(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_PERSIST_PENDING_FAILED,
        )

        h = _Harness(current_strategy="inbound")
        h.persist_pending_refusal = "store_refused"  # non-empty return == refusal
        sender = _Sender(ledger=h.ledger)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_PERSIST_PENDING_FAILED)
        self.assertEqual(h.events, ["persist_pending"])
        self.assertEqual(h.write_calls, [])
        self.assertIsNone(h.committed)

    async def test_missing_persist_pending_hook_refuses_before_side_effects(
        self,
    ) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_RECOVERY_STATE_UNAVAILABLE,
        )

        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)
        gen_before = h.ledger.snapshot_generation()
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
            persist_pending=None,  # no way to establish durable intent
        )
        self.assertFalse(result.success)
        self.assertEqual(
            result.failure_reason, TRANSITION_RECOVERY_STATE_UNAVAILABLE
        )
        self._assert_no_side_effects(h, sender, gen_before)

    # -- Blocker 6: crash at the persist_confirmed boundary -----------------
    async def test_crash_at_persist_confirmed_keeps_pending_and_never_commits(
        self,
    ) -> None:
        h = _Harness(current_strategy="inbound")
        h.persist_confirmed_error = RuntimeError("crash right after restore")
        sender = _Sender(ledger=h.ledger)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        self.assertFalse(result.success)
        # The restore physically HAPPENED before the crash...
        self.assertEqual(h.write_calls, ["vendor.example.net:5074"])
        self.assertTrue(result.endpoint_restored)
        # ...the write-ahead pending state survives, the confirmed write was
        # ATTEMPTED, and NO terminal strategy was ever committed.
        self.assertEqual(len(h.pending_states), 1)
        self.assertEqual(len(h.confirmed_states), 1)
        self.assertIsNone(h.committed)
        self.assertNotIn("commit", h.events)

    # -- Batch 8A.1: persisted phase survives re-read from entry.data -------
    async def test_persisted_phase_records_carry_and_reload_exact_phase(
        self,
    ) -> None:
        import json

        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_PENDING,
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
            StrategyTransitionRecoveryState,
        )

        # A restore that confirms but never proves callback: persist_pending
        # writes the pending phase, persist_confirmed overwrites with the
        # confirmed-unproven phase (nobody answers the trigger -> no commit).
        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)
        await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
        )
        # persist_pending saved transition_pending.
        self.assertEqual(
            h.pending_states[0].to_record()["phase"], RECOVERY_PHASE_PENDING
        )
        # persist_confirmed saved callback_restore_confirmed_unproven.
        self.assertEqual(
            h.confirmed_states[0].to_record()["phase"],
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        )
        # The single persisted entry-data record now holds the CONFIRMED phase,
        # and re-reading it (JSON round-trip -> from_record) preserves it
        # exactly -- never silently reverting to pending.
        reloaded = StrategyTransitionRecoveryState.from_record(
            json.loads(json.dumps(h.persisted_state_record))
        )
        self.assertIsNotNone(reloaded)
        self.assertEqual(
            reloaded.phase, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN
        )

    # -- Blocker 4 (8A.1): integration-managed restore REQUIRES the hook ----
    async def test_missing_persist_confirmed_refuses_restore_before_side_effects(
        self,
    ) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            TRANSITION_PERSIST_CONFIRMED_UNAVAILABLE,
        )

        h = _Harness(current_strategy="inbound")
        sender = _Sender(ledger=h.ledger)
        gen_before = h.ledger.snapshot_generation()
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="integration_managed",  # restore path
            restore_endpoint="vendor.example.net:5074",
            write_endpoint=h.write_endpoint_no_reconnect,
            reboot=h.reboot,
            on_endpoint_restored=h.on_restored,
            persist_confirmed=None,  # no way to persist the confirmed phase
        )
        self.assertFalse(result.success)
        self.assertEqual(
            result.failure_reason, TRANSITION_PERSIST_CONFIRMED_UNAVAILABLE
        )
        # Refused BEFORE persist_pending and BEFORE any side effect: entry data
        # untouched, write-ahead never attempted, zero endpoint/reboot/UDP/commit.
        self.assertEqual(h.events, [])
        self.assertIsNone(h.persisted_state_record)
        self.assertEqual(h.pending_states, [])
        self.assertEqual(h.write_calls, [])
        self.assertEqual(h.reboot_calls, 0)
        self.assertEqual(sender.routes, [])
        self.assertEqual(h.ledger.snapshot_generation(), gen_before)
        self.assertIsNone(h.committed)

    async def test_reboot_path_does_not_require_persist_confirmed(self) -> None:
        # The already-external reboot path changes no persistent endpoint, so a
        # confirmed-restore hook is NOT required: it still succeeds with none.
        h = _Harness(current_strategy="inbound")
        h.reconnect_after_restart = False
        sender = self._reconnecting_sender(h)
        result = await h.run(
            target="callback_on_demand",
            callback_route=_route(),
            trigger_sender=sender,
            endpoint_control_policy="external",  # reboot path
            reboot=h.reboot,
            write_endpoint=h.write_endpoint,
            on_endpoint_restored=h.on_restored,
            persist_confirmed=None,
        )
        self.assertTrue(result.success, result.failure_reason)
        self.assertEqual(h.events, ["persist_pending", "reboot", "commit"])
        self.assertEqual(h.confirmed_states, [])


class OptionPayloadAllowlistTests(unittest.IsolatedAsyncioTestCase):
    """Blocker 8: allowlist, not blacklist."""

    async def test_orthogonal_keys_pass_topology_and_unknown_refused(self) -> None:
        # Allowed orthogonal keys pass through.
        h = _Harness(current_strategy="callback_on_demand")
        result = await h.run(
            target="inbound",
            inbound_endpoint="192.168.1.50:18899",
            endpoint_needs_write=False,
            reboot=h.reboot,
            option_payload={
                "poll_mode": "auto",
                "poll_interval": 30,
                "control_mode": "auto",
            },
        )
        self.assertTrue(result.success, result.failure_reason)
        self.assertEqual(h.committed_options.get("poll_mode"), "auto")

        # A topology field is refused (not on the allowlist).
        for smuggled in ("server_ip", "tcp_port", "udp_port", "advertised_server_ip", "some_future_axis"):
            h2 = _Harness(current_strategy="callback_on_demand")
            res2 = await h2.run(
                target="inbound",
                inbound_endpoint="192.168.1.50:18899",
                endpoint_needs_write=False,
                reboot=h2.reboot,
                option_payload={"poll_mode": "auto", smuggled: "x"},
            )
            with self.subTest(smuggled=smuggled):
                self.assertFalse(res2.success)
                self.assertEqual(res2.failure_reason, "transition_payload_forbidden")
                self.assertIsNone(h2.committed)
