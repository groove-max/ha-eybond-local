"""The shared callback identity transaction: link + identity, nothing else.

These pin the contract the whole batch rests on:

* the transaction proves WHICH collector answered by READING its PN on the
  socket it claimed -- it never infers identity from a driver sweep, and it never
  runs one (a driver sweep here is what used to outlive the session);
* trigger provenance is PER ATTEMPT, so two concurrent attempts (and the
  runtime) cannot confirm each other;
* every failure and cancellation leaves the registry exactly as it was.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, suppress
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.callback_ledger import (
    get_callback_trigger_ledger,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.onboarding.callback_identity import (
    CallbackIdentityRequest,
    IDENTITY_CONFLICT,
    IDENTITY_MISMATCH,
    IDENTITY_OK,
    IDENTITY_TIMEOUT,
    IDENTITY_TRIGGER_INTERFERENCE,
    IDENTITY_TRIGGER_NOT_SENT,
    IDENTITY_UNVERIFIED,
    async_run_callback_identity_transaction,
)

# Synthetic identities only.
FULL_PN = "V001020SYN62344022"
SHORT_PN = "V001020SYN6"  # a real prefix of FULL_PN
OTHER_FULL_PN = "V000405SYN94677058"
NAT_IP = "192.0.2.55"
SERVER_IP = "192.0.2.10"


def _observed(session_id, pn="", *, state="routed_framed", shape="eybond_framed", port=18899, peer_ip=NAT_IP):
    return {
        "session_id": session_id,
        "peer_ip": peer_ip,
        "peer_port": 51000,
        "listener_port": port,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": shape,
        "collector_identity_source": "fc2_parameter_2" if pn else "",
    }


class _Live:
    """Session inventory that reveals the collector's answer AFTER the baseline.

    A session that already exists when the baseline is snapshotted can never be
    an answer to a trigger sent afterwards -- that is the invariant under test,
    so the fixture must not hand the answer over too early. Read 1 is the
    baseline; from read 2 the collector is in. This models inbound too, where
    nothing is sent and the collector dials in on its own.
    """

    def __init__(self, initial=()):
        self.sessions = list(initial)
        self._pending: list = []
        self._reads = 0

    def arm(self, *sessions):
        self._pending = list(sessions)
        self._reads = 0

    def __call__(self):
        self._reads += 1
        if self._pending and self._reads > 1:
            self.sessions.extend(self._pending)
            self._pending = []
        return tuple(self.sessions)


class _FakeHass:
    def __init__(self, registry):
        self.data = {"eybond_local": {"callback_session_registry": registry}}


class _Reader:
    """Stands in for the authoritative on-session PN read."""

    def __init__(self, *, pn=FULL_PN, source="fc2_parameter_2", error=None, on_read=None):
        self.pn = pn
        self.source = source
        self.error = error
        self.on_read = on_read
        self.calls: list[dict] = []

    async def async_read_full_pn(self, **kwargs):
        self.calls.append(kwargs)
        if self.on_read is not None:
            self.on_read(kwargs)
        if self.error is not None:
            raise self.error
        return (self.pn, self.source)


class _Sender:
    """Stands in for the production trigger facade: records like it does."""

    def __init__(self, *, sends=1, on_send=None):
        self.sends = sends
        self.on_send = on_send
        self.calls = 0

    async def async_send(self, request):
        self.calls += 1
        for _ in range(self.sends):
            get_callback_trigger_ledger().record(
                target=request.target_ip, source="test_attempt"
            )
        if self.on_send is not None:
            self.on_send()


def _foreign_send(ledger):
    """Try one trigger send from a genuinely FOREIGN causality context.

    A fresh contextvars.Context (not a thread: asyncio.to_thread copies the
    current context, so a threaded send is still the attempt's own).
    """

    import contextvars

    from custom_components.eybond_local.connection.callback_ledger import (
        CallbackTriggerInhibitedError,
    )

    def _send():
        try:
            with ledger.callback_send_scope():
                return "passed"
        except CallbackTriggerInhibitedError:
            return "refused"

    return contextvars.Context().run(_send)


async def _both(*coros, timeout=2.0):
    """gather() with a hard deadline that cancels the others on any failure.

    A concurrency test must finish or fail in a second or two: a bare gather on a
    rendezvous hangs the whole run instead of reporting the bug.
    """

    tasks = [asyncio.ensure_future(coro) for coro in coros]
    try:
        return await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=False), timeout=timeout
        )
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task


@contextmanager
def _no_probe_scope():
    """Neutralize the passive-discovery scope (not under test here)."""

    @contextmanager
    def _scope(_hass, _scope_id):
        yield set()

    with patch(
        "custom_components.eybond_local.passive_discovery.active_callback_probe_scope",
        new=_scope,
    ):
        yield


def _request(**overrides):
    base = {
        "server_ip": SERVER_IP,
        "tcp_port": 18899,
        "udp_port": 58899,
        "target_ip": NAT_IP,
        "session_wait_timeout": 0.5,
    }
    base.update(overrides)
    return CallbackIdentityRequest(**base)


class CallbackIdentityTransactionTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, sessions, *, reader=None, sender=None, request=None, initial=()):
        live = _Live(initial)
        live.arm(*sessions)
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        with _no_probe_scope():
            outcome = await async_run_callback_identity_transaction(
                hass,
                request or _request(),
                reader=reader if reader is not None else _Reader(),
                sender=sender if sender is not None else _Sender(),
            )
        return outcome, registry

    # 1 -----------------------------------------------------------------
    async def test_success_yields_full_pn_and_certified_handoff(self) -> None:
        sessions = [_observed("s-new", FULL_PN)]
        outcome, registry = await self._run(sessions)

        self.assertEqual(outcome.result, IDENTITY_OK)
        self.assertTrue(outcome.confirmed)
        self.assertEqual(outcome.collector_pn, FULL_PN)
        self.assertEqual(outcome.session_id, "s-new")
        self.assertEqual(outcome.session_protocol, "eybond_framed")
        # The registry -- not the caller -- certifies the identity.
        self.assertEqual(registry.owner_for_pn(FULL_PN), outcome.handoff_owner)
        self.assertEqual(
            registry.prepared_handoff_identity(outcome.handoff_owner, FULL_PN), FULL_PN
        )
        self.assertEqual(registry.claimed_session_id(outcome.handoff_owner), "s-new")

    # 2 -----------------------------------------------------------------
    async def test_short_pn_is_enriched_to_full(self) -> None:
        # The socket only ever advertised a SHORT pn; the authoritative read
        # returns the full one. The durable identity must be the full PN.
        sessions = [_observed("s-new", SHORT_PN)]
        outcome, registry = await self._run(sessions, reader=_Reader(pn=FULL_PN))

        self.assertTrue(outcome.confirmed)
        self.assertEqual(outcome.collector_pn, FULL_PN)
        self.assertEqual(registry.claimed_identity(outcome.handoff_owner), FULL_PN)
        self.assertEqual(
            registry.prepared_handoff_identity(outcome.handoff_owner, FULL_PN), FULL_PN
        )

    # 3 -----------------------------------------------------------------
    async def test_framed_session_reads_pn_via_fc2_parameter_2(self) -> None:
        reader = _Reader(pn=FULL_PN, source="fc2_parameter_2")
        sessions = [_observed("s-new", FULL_PN, state="routed_framed", shape="eybond_framed")]
        outcome, _registry = await self._run(sessions, reader=reader)

        self.assertTrue(outcome.confirmed)
        self.assertEqual(len(reader.calls), 1)
        # The wire came from the OBSERVED session, and the read was pinned to
        # exactly the claimed socket (never an IP/PN scan).
        self.assertEqual(reader.calls[0]["session_protocol"], "eybond_framed")
        self.assertEqual(reader.calls[0]["session_id"], "s-new")
        self.assertEqual(reader.calls[0]["listener_port"], 18899)
        self.assertEqual(outcome.identity_source, "fc2_parameter_2")

    # 4 -----------------------------------------------------------------
    async def test_at_session_reads_pn_via_dtupn(self) -> None:
        reader = _Reader(pn=FULL_PN, source="at_dtupn")
        sessions = [_observed("s-new", FULL_PN, state="routed_at_text", shape="at_text")]
        outcome, _registry = await self._run(sessions, reader=reader)

        self.assertTrue(outcome.confirmed)
        self.assertEqual(reader.calls[0]["session_protocol"], "at_text")
        self.assertEqual(outcome.session_protocol, "at_text")
        self.assertEqual(outcome.identity_source, "at_dtupn")

    # 5 -----------------------------------------------------------------
    async def test_no_driver_sweep_is_ever_invoked(self) -> None:
        # The whole point of the batch. If anything in the transaction reached a
        # detector, this explodes.
        import custom_components.eybond_local.onboarding.factory as factory

        def _boom(*_a, **_k):
            raise AssertionError(
                "the identity transaction must never build a detector or sweep drivers"
            )

        sessions = [_observed("s-new", FULL_PN)]
        with patch.object(factory, "create_onboarding_manager", side_effect=_boom):
            outcome, _registry = await self._run(sessions)
        self.assertTrue(outcome.confirmed)

    async def test_transaction_module_imports_no_detection_symbol(self) -> None:
        # Static proof, not just runtime: detection must not even be reachable.
        source = (
            REPO_ROOT
            / "custom_components/eybond_local/onboarding/callback_identity.py"
        ).read_text()
        code = "\n".join(
            line for line in source.splitlines() if not line.strip().startswith("#")
        )
        body = code.split('"""', 2)[-1]  # drop the module docstring
        for banned in (
            "async_auto_detect",
            "async_deep_detect",
            "create_onboarding_manager",
            "driver_detection",
            "link_sweep",
            "DRIVER_HINT",
        ):
            self.assertNotIn(banned, body, f"{banned} must not be reachable here")

    # 6 -----------------------------------------------------------------
    async def test_exactly_one_trigger_per_attempt(self) -> None:
        sender = _Sender(sends=1)
        sessions = [_observed("s-new", FULL_PN)]
        outcome, _registry = await self._run(sessions, sender=sender)

        self.assertTrue(outcome.confirmed)
        self.assertEqual(sender.calls, 1)

    async def test_more_than_one_own_trigger_is_refused(self) -> None:
        # A sender that fans out (the old sweep did) is not a valid attempt: we
        # could no longer say which trigger the answer belongs to.
        sender = _Sender(sends=2)
        sessions = [_observed("s-new", FULL_PN)]
        outcome, registry = await self._run(sessions, sender=sender)

        self.assertEqual(outcome.result, IDENTITY_TRIGGER_NOT_SENT)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")

    # 7 -----------------------------------------------------------------
    async def test_inbound_sends_zero_triggers(self) -> None:
        sender = _Sender(sends=1)
        sessions = [_observed("s-new", FULL_PN)]
        outcome, _registry = await self._run(
            sessions,
            sender=sender,
            request=_request(strategy="inbound", session_wait_timeout=0.5),
        )

        # Home Assistant never dials out on inbound ...
        self.assertEqual(sender.calls, 0)
        # ... and a session that appears on its own still identifies normally.
        self.assertTrue(outcome.confirmed)
        self.assertEqual(outcome.collector_pn, FULL_PN)

    # 8 -----------------------------------------------------------------
    async def test_two_parallel_attempts_are_serialized_and_both_succeed(self) -> None:
        # Two correct attempts started at once. The causality lease runs them ONE
        # AFTER ANOTHER on clean windows, so BOTH succeed. Under the old
        # overlapping-window accounting they poisoned each other and both failed
        # with interference -- "correct concurrent usage always fails" was the bug.
        live = _Live()
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        sends: list[str] = []
        owners_at_send: list[str] = []

        class _Sequential:
            def __init__(self, name, session):
                self.name = name
                self.session = session

            async def async_send(self, request):
                sends.append(self.name)
                # Whoever is sending must be the sole holder of causality.
                owners_at_send.append(get_callback_trigger_ledger().causality_owner())
                get_callback_trigger_ledger().record(
                    target=request.target_ip, source="test_attempt"
                )
                await asyncio.sleep(0)  # let the other attempt try to butt in
                live.sessions.append(self.session)

        async def _attempt(name, pn, session_id):
            with _no_probe_scope():
                return await async_run_callback_identity_transaction(
                    hass,
                    _request(session_wait_timeout=0.5),
                    reader=_Reader(pn=pn),
                    sender=_Sequential(name, _observed(session_id, pn)),
                )

        first, second = await _both(
            _attempt("a", FULL_PN, "s-a"), _attempt("b", OTHER_FULL_PN, "s-b")
        )

        # BOTH succeeded -- serialized, not mutually destroyed.
        self.assertTrue(first.confirmed, first.result)
        self.assertTrue(second.confirmed, second.result)
        self.assertEqual({first.collector_pn, second.collector_pn}, {FULL_PN, OTHER_FULL_PN})
        self.assertNotEqual(first.handoff_owner, second.handoff_owner)
        # Each send happened under its own exclusive lease, and the two windows
        # were different attempts -- never open at the same time.
        self.assertEqual(len(sends), 2)
        self.assertTrue(all(owners_at_send), owners_at_send)
        self.assertNotEqual(owners_at_send[0], owners_at_send[1])
        # Nobody holds causality once both are done.
        self.assertEqual(get_callback_trigger_ledger().causality_owner(), "")

    async def test_staggered_race_b_waits_and_cannot_claim_as_late_callback(self) -> None:
        # A triggers; B tries to start; A's collector only dials in LATER. B must
        # not open its own causality window while A's is open, and must never
        # claim the session A caused.
        live = _Live()
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        a_triggered = asyncio.Event()
        b_tried = asyncio.Event()
        windows: list[str] = []

        class _LateCollector:
            """A's trigger; its collector answers only after B has tried to start."""

            async def async_send(self, request):
                windows.append("a")
                get_callback_trigger_ledger().record(
                    target=request.target_ip, source="test_attempt"
                )
                a_triggered.set()
                # Bounded: a hung rendezvous must FAIL fast, never wedge the loop.
                await asyncio.wait_for(b_tried.wait(), timeout=1.0)
                live.sessions.append(_observed("s-a", FULL_PN))  # the LATE callback

        class _BSender:
            async def async_send(self, request):
                windows.append("b")
                get_callback_trigger_ledger().record(
                    target=request.target_ip, source="test_attempt"
                )
                live.sessions.append(_observed("s-b", OTHER_FULL_PN))

        async def _a():
            with _no_probe_scope():
                return await async_run_callback_identity_transaction(
                    hass,
                    _request(session_wait_timeout=1.0),
                    reader=_Reader(pn=FULL_PN),
                    sender=_LateCollector(),
                )

        async def _b():
            await asyncio.wait_for(a_triggered.wait(), timeout=1.0)
            with _no_probe_scope():
                task = asyncio.create_task(
                    async_run_callback_identity_transaction(
                        hass,
                        _request(session_wait_timeout=1.0, lease_wait_timeout=10.0),
                        reader=_Reader(pn=OTHER_FULL_PN),
                        sender=_BSender(),
                    )
                )
                try:
                    await asyncio.sleep(0.05)
                    # A still owns causality; B has NOT opened a window of its own.
                    self.assertNotIn("b", windows)
                    self.assertNotEqual(
                        get_callback_trigger_ledger().causality_owner(), ""
                    )
                    b_tried.set()
                    return await asyncio.wait_for(task, timeout=2.0)
                finally:
                    # If anything above blew up, do not leave B running.
                    b_tried.set()
                    if not task.done():
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task

        a_outcome, b_outcome = await _both(_a(), _b())

        # A owns the late session its own trigger caused ...
        self.assertTrue(a_outcome.confirmed, a_outcome.result)
        self.assertEqual(a_outcome.collector_pn, FULL_PN)
        self.assertEqual(a_outcome.session_id, "s-a")
        # ... B only opened its window after A's closed ...
        self.assertEqual(windows, ["a", "b"])
        # ... and B never claimed A's session: by B's baseline it already existed.
        self.assertNotEqual(b_outcome.session_id, "s-a")
        self.assertEqual(registry.owner_for_pn(FULL_PN), a_outcome.handoff_owner)

    # 9 -----------------------------------------------------------------
    async def test_another_runtimes_trigger_is_not_taken_as_our_own(self) -> None:
        # Our own trigger goes out AND the runtime fires one during our window.
        # The appearing session is no longer attributable to us.
        sessions = [_observed("s-new", FULL_PN)]

        def _foreign_runtime_trigger():
            # Recorded with no attempt context -- exactly like runtime/link.py.
            get_callback_trigger_ledger().record(
                target="192.0.2.99", source="runtime_callback_on_demand", attempt_id=""
            )

        sender = _Sender(sends=1, on_send=_foreign_runtime_trigger)
        outcome, registry = await self._run(sessions, sender=sender)

        self.assertEqual(outcome.result, IDENTITY_TRIGGER_INTERFERENCE)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")

    # 10 ----------------------------------------------------------------
    async def test_foreign_pre_existing_session_is_never_claimed(self) -> None:
        # A stranger's session already exists; our trigger produces nothing new.
        # The stranger must not be claimed, promoted or handed off.
        reader = _Reader(pn=OTHER_FULL_PN)
        outcome, registry = await self._run(
            [], reader=reader, initial=[_observed("s-foreign", OTHER_FULL_PN)]
        )

        self.assertEqual(outcome.result, IDENTITY_TIMEOUT)
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")
        self.assertEqual(registry.diagnostics()["claim_count"], 0)
        # The stranger's socket was never even read.
        self.assertEqual(reader.calls, [])

    # 11 ----------------------------------------------------------------
    async def test_two_collectors_behind_one_nat_ip_stay_distinct(self) -> None:
        # Same peer IP, two collectors. Identity is the PN and only the PN.
        live = _Live()
        live.arm(_observed("s-a", FULL_PN, peer_ip=NAT_IP))
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        with _no_probe_scope():
            first = await async_run_callback_identity_transaction(
                hass, _request(), reader=_Reader(pn=FULL_PN), sender=_Sender()
            )
        self.assertTrue(first.confirmed)
        self.assertEqual(first.collector_pn, FULL_PN)

        # The second collector dials in from the SAME NAT IP.
        live.arm(_observed("s-b", OTHER_FULL_PN, peer_ip=NAT_IP))
        with _no_probe_scope():
            second = await async_run_callback_identity_transaction(
                hass, _request(), reader=_Reader(pn=OTHER_FULL_PN), sender=_Sender()
            )
        self.assertTrue(second.confirmed)
        self.assertEqual(second.collector_pn, OTHER_FULL_PN)

        # Two distinct owners, two distinct identities, one peer IP.
        self.assertNotEqual(first.handoff_owner, second.handoff_owner)
        self.assertEqual(registry.owner_for_pn(FULL_PN), first.handoff_owner)
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), second.handoff_owner)

    # 12 ----------------------------------------------------------------
    async def test_timeout_is_typed_and_claims_nothing(self) -> None:
        outcome, registry = await self._run([])
        self.assertEqual(outcome.result, IDENTITY_TIMEOUT)
        self.assertFalse(outcome.confirmed)
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    async def test_identity_mismatch_against_expected_pn_is_typed(self) -> None:
        # Passive discovery said A; the socket authoritatively reads as B.
        sessions = [_observed("s-new", OTHER_FULL_PN)]
        outcome, registry = await self._run(
            sessions,
            reader=_Reader(pn=OTHER_FULL_PN),
            request=_request(expected_pn=FULL_PN, session_wait_timeout=0.5),
        )
        self.assertEqual(outcome.result, IDENTITY_MISMATCH)
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    async def test_session_claimed_by_another_owner_is_typed(self) -> None:
        live = _Live()
        registry = CallbackSessionRegistry(sessions_source=live)
        registry.claim_session("someone-else", session_id="s-new")
        live.arm(_observed("s-new", FULL_PN))  # arm AFTER setup's own reads
        hass = _FakeHass(registry)
        with _no_probe_scope():
            outcome = await async_run_callback_identity_transaction(
                hass, _request(), reader=_Reader(), sender=_Sender()
            )

        self.assertEqual(outcome.result, IDENTITY_CONFLICT)
        # The other owner's claim is untouched.
        self.assertEqual(registry.claimed_session_id("someone-else"), "s-new")

    async def test_trigger_not_sent_is_not_called_interference(self) -> None:
        # Our datagram never went out (an inhibited window / socket error). That
        # is OUR failure -- naming it interference sends users hunting a phantom.
        sessions = [_observed("s-new", FULL_PN)]
        outcome, registry = await self._run(sessions, sender=_Sender(sends=0))

        self.assertEqual(outcome.result, IDENTITY_TRIGGER_NOT_SENT)
        self.assertNotEqual(outcome.result, IDENTITY_TRIGGER_INTERFERENCE)
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    async def test_unreadable_identity_is_typed_and_claims_nothing(self) -> None:
        sessions = [_observed("s-new", FULL_PN)]
        outcome, registry = await self._run(
            sessions, reader=_Reader(error=ConnectionError("collector_not_connected"))
        )
        self.assertEqual(outcome.result, IDENTITY_UNVERIFIED)
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    async def test_untrusted_wire_never_guesses_a_frame(self) -> None:
        # An unroutable/ambiguous socket: we must not guess which bytes to write.
        sessions = [_observed("s-new", FULL_PN, state="parked_waiting_for_identity", shape="unknown")]
        reader = _Reader()
        outcome, registry = await self._run(sessions, reader=reader)

        self.assertEqual(outcome.result, IDENTITY_UNVERIFIED)
        self.assertEqual(reader.calls, [])  # nothing was written to that socket
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    # 13 ----------------------------------------------------------------
    async def test_claim_is_released_after_every_failure(self) -> None:
        for label, kwargs in (
            ("read_error", {"reader": _Reader(error=RuntimeError("boom"))}),
            ("empty_pn", {"reader": _Reader(pn="")}),
            ("timeout", {"reader": _Reader(), "sessions": []}),
        ):
            with self.subTest(failure=label):
                sessions = kwargs.pop("sessions", [_observed("s-new", FULL_PN)])
                outcome, registry = await self._run(sessions, **kwargs)
                self.assertFalse(outcome.confirmed)
                self.assertEqual(
                    registry.diagnostics()["claim_count"],
                    0,
                    f"{label} leaked a claim",
                )

    # 14 ----------------------------------------------------------------
    async def test_cancellation_is_not_swallowed_and_releases_the_claim(self) -> None:
        live = _Live()
        live.arm(_observed("s-new", FULL_PN))
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)

        class _CancellingReader:
            calls = 0

            async def async_read_full_pn(self, **_kwargs):
                type(self).calls += 1
                raise asyncio.CancelledError()

        with _no_probe_scope():
            with self.assertRaises(asyncio.CancelledError):
                await async_run_callback_identity_transaction(
                    hass, _request(), reader=_CancellingReader(), sender=_Sender()
                )

        self.assertEqual(_CancellingReader.calls, 1)
        # Cancellation still released the transient claim.
        self.assertEqual(registry.diagnostics()["claim_count"], 0)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")

    async def test_cancelled_trigger_send_is_not_swallowed(self) -> None:
        class _CancellingSender:
            async def async_send(self, _request):
                raise asyncio.CancelledError()

        registry = CallbackSessionRegistry(sessions_source=lambda: ())
        hass = _FakeHass(registry)
        with _no_probe_scope():
            with self.assertRaises(asyncio.CancelledError):
                await async_run_callback_identity_transaction(
                    hass, _request(), reader=_Reader(), sender=_CancellingSender()
                )
        self.assertEqual(registry.diagnostics()["claim_count"], 0)


class CallbackTriggerSequenceSemanticsTests(unittest.IsolatedAsyncioTestCase):
    """One logical callback-trigger SEQUENCE per attempt -- not one datagram.

    The wire contract is three compatible ``set>server`` payload variants sent by
    one logical send (collectors differ in which they accept). That is deliberate
    and undocumented-elsewhere, so the attempt contract counts the SEQUENCE the
    facade performs, never the datagrams underneath it. What must not happen is a
    second sequence: detector fan-out, a broadcast scan, or a continuous
    announcer.
    """

    async def test_transaction_invokes_the_trigger_facade_exactly_once(self) -> None:
        import custom_components.eybond_local.collector.discovery as discovery

        calls: list[dict] = []

        async def _facade(**kwargs):
            calls.append(kwargs)
            get_callback_trigger_ledger().record(
                target=kwargs.get("target_ip", ""), source=kwargs.get("source", "")
            )
            live.sessions.append(_observed("s-new", FULL_PN))
            return None

        live = _Live()
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        with _no_probe_scope(), patch.object(
            discovery, "async_send_callback_trigger", new=_facade
        ):
            outcome = await async_run_callback_identity_transaction(
                hass, _request(session_wait_timeout=0.5), reader=_Reader(pn=FULL_PN)
            )

        self.assertTrue(outcome.confirmed, outcome.result)
        # ONE logical sequence for the attempt.
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["target_ip"], NAT_IP)

    async def test_the_sequence_may_still_send_its_compatible_wire_variants(self) -> None:
        # The payload builder emits the three compatible set>server variants; that
        # is INSIDE one logical sequence and must not be broken by the attempt
        # contract. Proven at the payload layer so the wire behaviour is pinned.
        from custom_components.eybond_local.collector.discovery import (
            build_discovery_messages,
        )

        messages = build_discovery_messages("192.0.2.10", 18899)
        self.assertGreater(len(messages), 1, "compatible variants must be preserved")
        for message in messages:
            self.assertIn(b"set>server", message if isinstance(message, bytes) else message.encode())

    async def test_no_fan_out_scan_or_continuous_announcer(self) -> None:
        # A second sequence in any shape is what made attempts unattributable.
        import custom_components.eybond_local.collector.discovery as discovery

        def _boom(name):
            def _fail(*_a, **_k):
                raise AssertionError(f"{name} must never run inside an identity attempt")

            return _fail

        live = _Live()
        live.arm(_observed("s-new", FULL_PN))
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        with _no_probe_scope(), patch.object(
            discovery, "async_send_callback_trigger_replies", new=_boom("fan-out scan")
        ), patch.object(
            discovery, "DiscoveryAnnouncer", new=_boom("continuous announcer")
        ), patch.object(
            discovery, "async_probe_target_replies", new=_boom("raw replies probe")
        ):
            outcome = await async_run_callback_identity_transaction(
                hass, _request(session_wait_timeout=0.5), reader=_Reader(), sender=_Sender()
            )
        self.assertTrue(outcome.confirmed, outcome.result)


class CallbackIdentityBudgetPolicyTests(unittest.TestCase):
    def test_budgets_come_from_the_central_policy_not_magic_numbers(self) -> None:
        import custom_components.eybond_local.onboarding.callback_identity as ci
        from custom_components.eybond_local.onboarding.timeouts import (
            DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        )

        self.assertGreater(
            DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_identity_session_wait, 0
        )
        self.assertGreater(
            DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_causality_lease_wait, 0
        )
        # No module-level wait budget survives beside the policy.
        self.assertFalse(hasattr(ci, "_SESSION_WAIT_TIMEOUT"))

    def test_production_request_carries_no_hardcoded_override(self) -> None:
        request = CallbackIdentityRequest(server_ip="", tcp_port=0, udp_port=0)
        # 0 = "resolve from the policy at call time".
        self.assertEqual(request.session_wait_timeout, 0.0)
        self.assertEqual(request.lease_wait_timeout, 0.0)


class CausalityLeaseSafetyTests(unittest.IsolatedAsyncioTestCase):
    """The lease must never be stranded, and never observable half-taken.

    The earlier design parked a worker thread on a blocking Lock.acquire: a
    cancelled waiter left that worker running, it could win the lease afterwards,
    and nobody would ever release it. It also published the owner AFTER acquiring,
    leaving a window in which the lease was held but callback_send_scope saw no
    owner and let a foreign trigger through.
    """

    def setUp(self) -> None:
        self.ledger = get_callback_trigger_ledger()
        self.assertEqual(self.ledger.causality_owner(), "", "leaked from a prior test")

    async def _hold(self, attempt_id, released):
        async with self.ledger.causality_lease(attempt_id, timeout=1.0):
            await asyncio.wait_for(released.wait(), timeout=1.0)

    async def test_cancelling_a_waiter_strands_nothing(self) -> None:
        released = asyncio.Event()
        holder = asyncio.ensure_future(self._hold("owner-a", released))
        try:
            await asyncio.sleep(0.01)
            self.assertEqual(self.ledger.causality_owner(), "owner-a")

            async def _queued():
                async with self.ledger.causality_lease("owner-b", timeout=1.0):
                    raise AssertionError("must not have acquired")

            waiter = asyncio.ensure_future(_queued())
            await asyncio.sleep(0.05)
            waiter.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(waiter, timeout=1.0)

            # A owns it still: the cancelled waiter took nothing on its way out.
            self.assertEqual(self.ledger.causality_owner(), "owner-a")
        finally:
            released.set()
            await asyncio.wait_for(holder, timeout=1.0)
        self.assertEqual(self.ledger.causality_owner(), "")

    async def test_next_attempt_acquires_after_a_cancellation(self) -> None:
        released = asyncio.Event()
        holder = asyncio.ensure_future(self._hold("owner-a", released))
        try:
            await asyncio.sleep(0.01)

            async def _queued():
                async with self.ledger.causality_lease("owner-b", timeout=1.0):
                    pass

            waiter = asyncio.ensure_future(_queued())
            await asyncio.sleep(0.05)
            waiter.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(waiter, timeout=1.0)
        finally:
            released.set()
            await asyncio.wait_for(holder, timeout=1.0)

        # The lease is genuinely free -- not silently stolen by the dead waiter.
        async with self.ledger.causality_lease("owner-c", timeout=1.0) as attempt:
            self.assertEqual(self.ledger.causality_owner(), "owner-c")
            self.assertEqual(attempt.attempt_id, "owner-c")
        self.assertEqual(self.ledger.causality_owner(), "")

    async def test_cancellation_at_the_moment_of_acquisition(self) -> None:
        # Cancel repeatedly while the lease is FREE, so the cancel lands around
        # the acquire itself. Nothing may be left holding it.
        for _ in range(20):
            async def _grab():
                async with self.ledger.causality_lease("owner-x", timeout=1.0):
                    await asyncio.sleep(0.02)

            task = asyncio.ensure_future(_grab())
            await asyncio.sleep(0)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1.0)
            self.assertEqual(self.ledger.causality_owner(), "")

        async with self.ledger.causality_lease("owner-after", timeout=1.0):
            self.assertEqual(self.ledger.causality_owner(), "owner-after")
        self.assertEqual(self.ledger.causality_owner(), "")

    async def test_exception_inside_the_owner_scope_releases(self) -> None:
        with self.assertRaises(RuntimeError):
            async with self.ledger.causality_lease("owner-boom", timeout=1.0):
                raise RuntimeError("boom")
        self.assertEqual(self.ledger.causality_owner(), "")
        # And the attempt bookkeeping went with it.
        async with self.ledger.causality_lease("owner-boom", timeout=1.0) as attempt:
            self.assertEqual(attempt.own_sends, 0)

    async def test_empty_attempt_id_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            async with self.ledger.causality_lease("", timeout=1.0):
                pass
        self.assertEqual(self.ledger.causality_owner(), "")

    async def test_duplicate_attempt_id_fails_fast_while_that_id_holds_the_lease(self) -> None:
        # The case that matters: id A re-entering while A still owns the lease.
        # It can NEVER be granted, so it must fail immediately -- behind the
        # "is it free?" gate this check was unreachable and the caller instead sat
        # out the whole lease timeout and got a misleading "busy".
        loop = asyncio.get_running_loop()
        async with self.ledger.causality_lease("owner-dup", timeout=1.0):
            started = loop.time()
            with self.assertRaises(ValueError) as ctx:
                async with self.ledger.causality_lease("owner-dup", timeout=5.0):
                    raise AssertionError("must not have acquired")
            elapsed = loop.time() - started
            self.assertIn("callback_attempt_duplicate", str(ctx.exception))
            # Immediate, not "after the 5s timeout".
            self.assertLess(elapsed, 0.5, "duplicate must fail fast, not wait")
            # A's own lease is untouched by the refusal.
            self.assertEqual(self.ledger.causality_owner(), "owner-dup")
        self.assertEqual(self.ledger.causality_owner(), "")

    async def test_duplicate_attempt_id_refused_in_parallel_too(self) -> None:
        released = asyncio.Event()
        holder = asyncio.ensure_future(self._hold("owner-par", released))
        try:
            await asyncio.sleep(0.01)
            self.assertEqual(self.ledger.causality_owner(), "owner-par")

            async def _same_id():
                async with self.ledger.causality_lease("owner-par", timeout=5.0):
                    raise AssertionError("must not have acquired")

            with self.assertRaises(ValueError):
                await asyncio.wait_for(_same_id(), timeout=1.0)
        finally:
            released.set()
            await asyncio.wait_for(holder, timeout=1.0)
        self.assertEqual(self.ledger.causality_owner(), "")

    async def test_a_different_owner_waits_instead_of_failing(self) -> None:
        # The contrast: a DIFFERENT id is a legitimate queued attempt, not a bug.
        released = asyncio.Event()
        holder = asyncio.ensure_future(self._hold("owner-first", released))
        try:
            await asyncio.sleep(0.01)

            async def _other():
                async with self.ledger.causality_lease("owner-second", timeout=1.0):
                    return self.ledger.causality_owner()

            waiter = asyncio.ensure_future(_other())
            await asyncio.sleep(0.05)
            self.assertFalse(waiter.done(), "a different owner must QUEUE, not fail")
            released.set()
            self.assertEqual(await asyncio.wait_for(waiter, timeout=1.0), "owner-second")
        finally:
            released.set()
            await asyncio.wait_for(holder, timeout=1.0)

    async def test_no_sender_slips_between_acquire_and_owner_publication(self) -> None:
        # Atomicity is STRUCTURAL here: the owner string IS the lease, and both
        # the gate and the acquire touch it under the one mutex, so "held but
        # unowned" cannot be represented. What IS observable -- and what the old
        # publish-after-acquire design got wrong -- is that the instant an attempt
        # holds causality, every foreign sender is refused.
        #
        # A fresh contextvars.Context is what "foreign" means: a thread is NOT,
        # because asyncio.to_thread copies the current context -- and that is
        # correct in production, an attempt's own threaded send is its own send.
        self.assertEqual(_foreign_send(self.ledger), "passed")  # free -> passes

        async with self.ledger.causality_lease("owner-a", timeout=1.0):
            self.assertEqual(self.ledger.causality_owner(), "owner-a")
            # Held -> refused, with the honest reason, from the very first moment.
            for _ in range(5):
                self.assertEqual(_foreign_send(self.ledger), "refused")
            # ... while the OWNER's own sends still pass (same attempt context).
            with self.ledger.callback_send_scope():
                pass

    async def test_no_sender_slips_between_owner_clearing_and_release(self) -> None:
        # The mirror: clearing the owner IS the release, so there is no
        # "unowned but still locked" tail either -- a sender is refused right up
        # to the release and passes immediately after, with nothing in between.
        for index in range(25):
            async with self.ledger.causality_lease(f"rel-{index}", timeout=1.0):
                self.assertEqual(_foreign_send(self.ledger), "refused")
            # Released: free immediately, and no stale owner is left behind.
            self.assertEqual(self.ledger.causality_owner(), "")
            self.assertEqual(_foreign_send(self.ledger), "passed")

    def test_owner_and_gate_share_one_mutex(self) -> None:
        # The structural half of the two tests above: if acquire/release/gate ever
        # stop sharing _state_lock, a half-taken lease becomes representable again.
        import inspect

        from custom_components.eybond_local.connection import callback_ledger

        for name in (
            "_try_acquire_causality",
            "_release_causality",
            "callback_send_scope",
        ):
            source = inspect.getsource(
                getattr(callback_ledger.CallbackTriggerLedger, name)
            )
            self.assertIn("self._state_lock", source, f"{name} must use the one mutex")
            self.assertIn("_causality_owner", source, f"{name} must gate on the owner")


class CausalLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """A causal window is trigger -> late session, not trigger -> sendto returns.

    Refusing a *send* while somebody holds the lease was never enough: a datagram
    that went out a second BEFORE their lease still produces a TCP session inside
    their window. So every operation that can cause a callback session owns the
    lease from before its first trigger until its own terminal point.
    """

    def setUp(self) -> None:
        self.ledger = get_callback_trigger_ledger()
        self.assertEqual(self.ledger.causality_owner(), "", "leaked from a prior test")

    # A + B + C ---------------------------------------------------------------
    async def test_manual_waits_for_a_runtime_attempt_and_never_adopts_its_session(self) -> None:
        live = _Live()
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        runtime_done = asyncio.Event()
        events: list[str] = []

        async def _runtime_attempt():
            """The runtime's window: trigger -> bounded wait -> terminal."""

            async with self.ledger.causality_lease("runtime_callback:1", timeout=1.0):
                events.append("runtime:trigger")
                get_callback_trigger_ledger().record(
                    target=NAT_IP, source="runtime_callback_on_demand"
                )
                # Its collector dials back LATE -- inside the runtime's own window.
                await asyncio.sleep(0.05)
                live.sessions.append(_observed("s-runtime", OTHER_FULL_PN))
                events.append("runtime:session")
                runtime_done.set()
                await asyncio.sleep(0.02)  # still its window
            events.append("runtime:terminal")

        async def _manual():
            await asyncio.wait_for(runtime_done.wait(), timeout=1.0)
            # A: the runtime still owns causality -> we cannot even baseline yet.
            self.assertEqual(self.ledger.causality_owner(), "runtime_callback:1")

            def _answer():
                live.sessions.append(_observed("s-manual", FULL_PN))

            with _no_probe_scope():
                outcome = await async_run_callback_identity_transaction(
                    hass,
                    _request(session_wait_timeout=0.5, lease_wait_timeout=2.0),
                    reader=_Reader(pn=FULL_PN),
                    sender=_Sender(sends=1, on_send=_answer),
                )
            events.append("manual:terminal")
            return outcome

        _runtime, outcome = await _both(_runtime_attempt(), _manual(), timeout=2.0)

        # C: the manual window opened only after the runtime reached its terminal.
        self.assertEqual(
            events,
            ["runtime:trigger", "runtime:session", "runtime:terminal", "manual:terminal"],
        )
        # B: the runtime's late session is in the manual baseline -> never its answer.
        self.assertTrue(outcome.confirmed, outcome.result)
        self.assertEqual(outcome.collector_pn, FULL_PN)
        self.assertEqual(outcome.session_id, "s-manual")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "", "adopted the runtime's session")

    # D ------------------------------------------------------------------------
    async def test_inbound_verification_waits_for_a_callback_attempt_then_owns_silence(self) -> None:
        order: list[str] = []
        attempt_done = asyncio.Event()

        async def _callback_attempt():
            async with self.ledger.causality_lease("callback_verification:1", timeout=1.0):
                order.append("callback:window")
                attempt_done.set()
                await asyncio.sleep(0.05)
            order.append("callback:terminal")

        async def _inbound_verification():
            await asyncio.wait_for(attempt_done.wait(), timeout=1.0)
            # Same coordinator: it must QUEUE behind the callback attempt.
            async with self.ledger.causality_lease("inbound_verification:1", timeout=2.0):
                order.append("inbound:window")
                async with self.ledger.inhibit_callback_triggers():
                    order.append("inbound:silent")
                    # E: nothing may trigger inside the verification window.
                    self.assertEqual(_foreign_send(self.ledger), "refused")

        await _both(_callback_attempt(), _inbound_verification(), timeout=2.0)

        self.assertEqual(
            order,
            ["callback:window", "callback:terminal", "inbound:window", "inbound:silent"],
        )
        self.assertEqual(self.ledger.causality_owner(), "")

    # E ------------------------------------------------------------------------
    async def test_callback_transaction_waits_for_inbound_verification(self) -> None:
        live = _Live()
        registry = CallbackSessionRegistry(sessions_source=live)
        hass = _FakeHass(registry)
        verifying = asyncio.Event()
        release = asyncio.Event()
        order: list[str] = []

        async def _inbound_verification():
            async with self.ledger.causality_lease("inbound_verification:2", timeout=1.0):
                async with self.ledger.inhibit_callback_triggers():
                    order.append("inbound:window")
                    verifying.set()
                    await asyncio.wait_for(release.wait(), timeout=1.0)
            order.append("inbound:terminal")

        async def _attempt():
            await asyncio.wait_for(verifying.wait(), timeout=1.0)

            def _answer():
                order.append("callback:trigger")
                live.sessions.append(_observed("s-new", FULL_PN))

            task = asyncio.ensure_future(
                async_run_callback_identity_transaction(
                    hass,
                    _request(session_wait_timeout=0.5, lease_wait_timeout=2.0),
                    reader=_Reader(pn=FULL_PN),
                    sender=_Sender(sends=1, on_send=_answer),
                )
            )
            try:
                await asyncio.sleep(0.05)
                # It has NOT triggered: it is queued behind the verification.
                self.assertNotIn("callback:trigger", order)
                release.set()
                with _no_probe_scope():
                    return await asyncio.wait_for(task, timeout=1.5)
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

        _v, outcome = await _both(_inbound_verification(), _attempt(), timeout=2.0)

        # It only triggered once the verification's window closed.
        self.assertEqual(order[:2], ["inbound:window", "inbound:terminal"])
        self.assertIn("callback:trigger", order)
        self.assertTrue(outcome.confirmed, outcome.result)

    # F ------------------------------------------------------------------------
    async def test_every_high_level_failure_mode_releases_the_lease(self) -> None:
        # cancellation
        async def _cancelled():
            async with self.ledger.causality_lease("op-cancel", timeout=1.0):
                await asyncio.sleep(5)

        task = asyncio.ensure_future(_cancelled())
        await asyncio.sleep(0.02)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1.0)
        self.assertEqual(self.ledger.causality_owner(), "")

        # exception
        with self.assertRaises(RuntimeError):
            async with self.ledger.causality_lease("op-error", timeout=1.0):
                raise RuntimeError("boom")
        self.assertEqual(self.ledger.causality_owner(), "")

        # timeout inside the window
        async with self.ledger.causality_lease("op-timeout", timeout=1.0):
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.Event().wait(), timeout=0.01)
        self.assertEqual(self.ledger.causality_owner(), "")


if __name__ == "__main__":
    unittest.main()
