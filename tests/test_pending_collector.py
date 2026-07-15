"""Pending-collector lifecycle invariants (Part 2).

A pending entry is a collector saved BEFORE its durable full PN is known. It is
explicitly not a collector runtime: no coordinator, no platforms, no devices, no
endpoint write, and no session claim by address. Two lifecycles, chosen only by
the canonical connection_strategy in entry.data:

* inbound -- fully passive, ZERO UDP, user-confirmed candidate binding;
* callback_on_demand -- exactly ONE bounded attempt per async_setup_entry, with
  Home Assistant owning retry/backoff via ConfigEntryNotReady.
"""

from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest
from contextlib import contextmanager
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local import const as C  # noqa: E402
from custom_components.eybond_local.connection import connection_policy as cp  # noqa: E402
from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    CallbackSessionRegistry,
)
from custom_components.eybond_local.onboarding.callback_matching import (  # noqa: E402
    MATCH_IDENTITY_AMBIGUOUS,
    MATCH_IDENTITY_MISMATCH,
    MATCH_TIMEOUT,
    MATCH_TRIGGER_INTERFERENCE,
    match_callback_answer,
)

# Synthetic identifiers only (tests/test_no_real_identifiers.py allowlist).
FULL_PN = "E5000099990001"
OTHER_FULL_PN = "E5000099990002"
# 9 chars: BELOW the registry's 10-char prefix-match minimum, so it is NOT the
# same identity as FULL_PN -- used for the "weak evidence" cases.
SHORT_PN = "E50000999"
# 12 chars: a VALID short prefix of FULL_PN, i.e. the same durable identity.
SHORT_PN_PREFIX = "E50000999900"
NAT_IP = "192.0.2.55"


def _observed(session_id, pn, *, peer_ip=NAT_IP, state="routed_framed", source="at_dtupn"):
    return {
        "session_id": session_id,
        "peer_ip": peer_ip,
        "listener_port": 18899,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": "",
        "collector_identity_source": source,
    }


def _pending_entry(entry_id="pending-1", *, strategy=C.CONNECTION_STRATEGY_INBOUND, address=""):
    return types.SimpleNamespace(
        entry_id=entry_id,
        unique_id=f"pending:01TEST{entry_id}",
        version=C.__dict__.get("_unused", 4),
        data={
            C.CONF_ENTRY_ROLE: C.ENTRY_ROLE_PENDING_COLLECTOR,
            C.CONF_PENDING_ID: f"01TEST{entry_id}",
            C.CONF_CONNECTION_STRATEGY: strategy,
            C.CONF_PENDING_ADDRESS_HINT: address,
            C.CONF_COLLECTOR_IP: address,
            C.CONF_COLLECTOR_PN: "",
        },
        options={},
    )


class _FakeConfigEntries:
    def __init__(self, entries=None):
        self._entries = list(entries or [])
        self.updates: list[dict] = []
        self.reloads: list[str] = []

    def async_entries(self, _domain):
        return list(self._entries)

    def async_update_entry(self, entry, **kwargs):
        self.updates.append(dict(kwargs))
        for key, value in kwargs.items():
            if key in ("data", "options"):
                setattr(entry, key, dict(value))
            else:
                setattr(entry, key, value)
        return True

    async def async_reload(self, entry_id):
        self.reloads.append(entry_id)
        return True


def _fake_hass(entries=None, registry=None):
    data = {}
    if registry is not None:
        data[C.DOMAIN] = {"callback_session_registry": registry}
    return types.SimpleNamespace(
        config_entries=_FakeConfigEntries(entries),
        data=data,
    )


class PendingRoleInvariantTests(unittest.TestCase):
    def test_pending_entry_is_not_a_collector_backed_entry(self) -> None:
        entry = _pending_entry()
        # It must never be treated as a normal collector entry: no session claim,
        # and NOT reported as a broken identity_binding_required entry.
        self.assertFalse(cp.is_collector_backed_callback_entry(entry.data, entry.options))
        self.assertFalse(cp.collector_identity_binding_required(entry.data, entry.options))
        self.assertTrue(cp.is_pending_collector_entry(entry.data, entry.options))

    def test_normal_pn_less_entry_is_still_binding_required(self) -> None:
        # The pending exemption must not weaken the durable-PN rule for a NORMAL
        # collector entry.
        self.assertTrue(cp.collector_identity_binding_required({}, {}))

    def test_pending_strategy_is_canonical_from_data(self) -> None:
        entry = _pending_entry(strategy=C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND)
        from custom_components.eybond_local.pending_collector import pending_entry_strategy

        self.assertEqual(
            pending_entry_strategy(entry), C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        )
        # ... and options never hold a strategy copy.
        self.assertNotIn(C.CONF_CONNECTION_STRATEGY, entry.options)

    def test_pending_unique_id_is_not_derived_from_address(self) -> None:
        first = _pending_entry("a", address=NAT_IP)
        second = _pending_entry("b", address=NAT_IP)
        # Two pending entries behind ONE NAT/peer IP stay distinct identities.
        self.assertNotEqual(first.unique_id, second.unique_id)
        for entry in (first, second):
            self.assertTrue(entry.unique_id.startswith(C.PENDING_UNIQUE_ID_PREFIX))
            self.assertNotIn(NAT_IP, entry.unique_id)


class InboundCandidateTests(unittest.TestCase):
    def test_only_strong_full_pn_sessions_are_offered(self) -> None:
        from custom_components.eybond_local.pending_collector import list_inbound_candidates

        sessions = [
            _observed("s1", FULL_PN),
            _observed("s2", SHORT_PN, source="framed_heartbeat"),  # weak -> excluded
            _observed("s3", "", source="framed_heartbeat"),  # no PN -> excluded
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        hass = _fake_hass(registry=registry)

        candidates = list_inbound_candidates(hass)

        self.assertEqual([c["collector_pn"] for c in candidates], [FULL_PN])

    def test_two_collectors_behind_one_ip_are_offered_separately(self) -> None:
        from custom_components.eybond_local.pending_collector import list_inbound_candidates

        sessions = [
            _observed("s1", FULL_PN, peer_ip=NAT_IP),
            _observed("s2", OTHER_FULL_PN, peer_ip=NAT_IP),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        candidates = list_inbound_candidates(_fake_hass(registry=registry))

        # Distinguished by PN, never collapsed by the shared peer IP.
        self.assertEqual(
            sorted(c["collector_pn"] for c in candidates), sorted([FULL_PN, OTHER_FULL_PN])
        )

    def test_claimed_session_is_not_offered(self) -> None:
        from custom_components.eybond_local.pending_collector import list_inbound_candidates

        sessions = [_observed("s1", FULL_PN)]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-existing", collector_pn=FULL_PN)

        self.assertEqual(list_inbound_candidates(_fake_hass(registry=registry)), [])


class PromotionTests(unittest.TestCase):
    """Promotion is only ever allowed on a registry-certified identity."""

    def _registry_with_prepared_handoff(self, owner, pn, session_id="s1"):
        sessions = [_observed(session_id, pn)]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session(owner, session_id=session_id)
        registry.promote_claim_to_full_pn(owner, pn)
        registry.prepare_handoff(owner, pn)
        return registry

    def test_promotion_is_atomic_and_clears_pending_fields(self) -> None:
        from custom_components.eybond_local.pending_collector import (
            async_promote_pending_entry,
        )

        owner = "pending_attempt:abc"
        registry = self._registry_with_prepared_handoff(owner, FULL_PN)
        entry = _pending_entry(strategy=C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND, address=NAT_IP)
        hass = _fake_hass([entry], registry=registry)

        async_promote_pending_entry(
            hass,
            entry,
            collector_pn=FULL_PN,
            evidence=C.CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
            handoff_owner=owner,
        )

        # ONE update carried unique_id + data together (no half-promoted state).
        self.assertEqual(len(hass.config_entries.updates), 1)
        self.assertEqual(hass.config_entries.updates[0]["unique_id"], f"collector:{FULL_PN}")

        self.assertEqual(entry.data[C.CONF_COLLECTOR_PN], FULL_PN)
        self.assertEqual(entry.data[C.CONF_ENTRY_ROLE], "")
        # The user's canonical strategy survives promotion.
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        )
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY_EVIDENCE],
            C.CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
        )
        for key in (C.CONF_PENDING_ID, C.CONF_PENDING_ADDRESS_HINT, C.CONF_PENDING_LAST_ATTEMPT_RESULT):
            self.assertNotIn(key, entry.data)
        self.assertTrue(cp.is_collector_backed_callback_entry(entry.data, entry.options))
        self.assertFalse(cp.collector_identity_binding_required(entry.data, entry.options))

    def test_promotion_without_handoff_owner_is_refused(self) -> None:
        # A caller may not promote on a PN it merely believes in.
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        registry = self._registry_with_prepared_handoff("pending_attempt:abc", FULL_PN)
        entry = _pending_entry()
        hass = _fake_hass([entry], registry=registry)

        with self.assertRaises(PendingPromotionError):
            async_promote_pending_entry(hass, entry, collector_pn=FULL_PN)
        self.assertEqual(hass.config_entries.updates, [])

    def test_promotion_without_prepared_handoff_is_refused(self) -> None:
        # The owner claimed the session but never prepared the handoff.
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        sessions = [_observed("s1", FULL_PN)]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session("pending_attempt:abc", session_id="s1")
        registry.promote_claim_to_full_pn("pending_attempt:abc", FULL_PN)
        # NO prepare_handoff.
        entry = _pending_entry()
        hass = _fake_hass([entry], registry=registry)

        with self.assertRaises(PendingPromotionError):
            async_promote_pending_entry(
                hass, entry, collector_pn=FULL_PN, handoff_owner="pending_attempt:abc"
            )
        self.assertEqual(hass.config_entries.updates, [])

    def test_promotion_on_a_different_identity_than_the_handoff_is_refused(self) -> None:
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        owner = "pending_attempt:abc"
        registry = self._registry_with_prepared_handoff(owner, FULL_PN)
        entry = _pending_entry()
        hass = _fake_hass([entry], registry=registry)

        # The owner's certified identity is FULL_PN; promoting OTHER_FULL_PN on it
        # must fail closed.
        with self.assertRaises(PendingPromotionError):
            async_promote_pending_entry(
                hass, entry, collector_pn=OTHER_FULL_PN, handoff_owner=owner
            )
        self.assertEqual(hass.config_entries.updates, [])

    def test_pn_collision_leaves_pending_entry_unchanged(self) -> None:
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        owner = "pending_attempt:abc"
        registry = self._registry_with_prepared_handoff(owner, FULL_PN)
        existing = types.SimpleNamespace(
            entry_id="normal-1",
            unique_id=f"collector:{FULL_PN}",
            data={C.CONF_COLLECTOR_PN: FULL_PN},
            options={},
        )
        entry = _pending_entry(address=NAT_IP)
        before = dict(entry.data)
        hass = _fake_hass([existing, entry], registry=registry)

        with self.assertRaises(PendingPromotionError) as ctx:
            async_promote_pending_entry(
                hass, entry, collector_pn=FULL_PN, handoff_owner=owner
            )

        self.assertEqual(ctx.exception.reason, "already_configured")
        # Fail closed: NOTHING was mutated and no second collector entry appeared.
        self.assertEqual(entry.data, before)
        self.assertEqual(hass.config_entries.updates, [])

    def test_collision_is_detected_by_identity_not_string(self) -> None:
        # An existing entry stored the SHORT PN; the freshly-read FULL PN is the
        # same collector, so promotion must still collide (a string compare on
        # unique_id would have created a duplicate).
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        owner = "pending_attempt:abc"
        registry = self._registry_with_prepared_handoff(owner, FULL_PN)
        existing = types.SimpleNamespace(
            entry_id="normal-1",
            unique_id=f"collector:{SHORT_PN_PREFIX}",
            data={C.CONF_COLLECTOR_PN: SHORT_PN_PREFIX},
            options={},
        )
        entry = _pending_entry()
        hass = _fake_hass([existing, entry], registry=registry)

        with self.assertRaises(PendingPromotionError) as ctx:
            async_promote_pending_entry(
                hass, entry, collector_pn=FULL_PN, handoff_owner=owner
            )
        self.assertEqual(ctx.exception.reason, "already_configured")
        self.assertEqual(hass.config_entries.updates, [])

    def test_pn_only_claim_without_session_cannot_promote(self) -> None:
        # A claim taken by PN alone never observed this collector on the wire for
        # this attempt, so it must not be able to promote an entry.
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        registry = CallbackSessionRegistry(sessions_source=lambda: [])
        registry.claim("pending_attempt:abc", collector_pn=FULL_PN)
        registry.prepare_handoff("pending_attempt:abc", FULL_PN)
        entry = _pending_entry()
        hass = _fake_hass([entry], registry=registry)

        with self.assertRaises(PendingPromotionError):
            async_promote_pending_entry(
                hass, entry, collector_pn=FULL_PN, handoff_owner="pending_attempt:abc"
            )
        self.assertEqual(hass.config_entries.updates, [])

    def test_promotion_persists_the_certified_full_pn_not_the_short_input(self) -> None:
        # The caller offers a short prefix; the registry certifies the full,
        # session-derived identity, and THAT is what the entry gets.
        from custom_components.eybond_local.pending_collector import (
            async_promote_pending_entry,
        )

        owner = "pending_attempt:abc"
        registry = self._registry_with_prepared_handoff(owner, FULL_PN)
        entry = _pending_entry()
        hass = _fake_hass([entry], registry=registry)

        async_promote_pending_entry(
            hass, entry, collector_pn=SHORT_PN_PREFIX, handoff_owner=owner
        )

        self.assertEqual(entry.data[C.CONF_COLLECTOR_PN], FULL_PN)
        self.assertEqual(
            hass.config_entries.updates[0]["unique_id"], f"collector:{FULL_PN}"
        )

    def test_promotion_requires_a_durable_full_pn(self) -> None:
        from custom_components.eybond_local.pending_collector import (
            PendingPromotionError,
            async_promote_pending_entry,
        )

        entry = _pending_entry()
        hass = _fake_hass([entry])
        with self.assertRaises(PendingPromotionError):
            async_promote_pending_entry(hass, entry, collector_pn="", handoff_owner="x")
        self.assertEqual(hass.config_entries.updates, [])

    def test_release_only_touches_its_own_attempt_claim(self) -> None:
        from custom_components.eybond_local.pending_collector import (
            release_pending_attempt_claim,
        )

        sessions = [_observed("s1", FULL_PN), _observed("s2", OTHER_FULL_PN)]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-other", collector_pn=OTHER_FULL_PN)
        registry.claim_session("pending_attempt:mine", session_id="s1")
        hass = _fake_hass(registry=registry)

        release_pending_attempt_claim(hass, "pending_attempt:mine")

        # Only ours is gone; the other entry's ownership is untouched.
        self.assertEqual(registry.claimed_identity("pending_attempt:mine"), "")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "entry-other")


class RegistryHandoffProofTests(unittest.TestCase):
    """The registry publicly certifies a prepared handoff; no private map reads."""

    def _registry(self, pn=FULL_PN, session_id="s1"):
        sessions = [_observed(session_id, pn)]
        return CallbackSessionRegistry(sessions_source=lambda: sessions)

    def test_prepared_session_claim_is_certified(self) -> None:
        registry = self._registry()
        registry.claim_session("owner", session_id="s1")
        registry.promote_claim_to_full_pn("owner", FULL_PN)
        registry.prepare_handoff("owner", FULL_PN)

        self.assertEqual(registry.prepared_handoff_identity("owner", FULL_PN), FULL_PN)

    def test_unprepared_claim_is_not_certified(self) -> None:
        registry = self._registry()
        registry.claim_session("owner", session_id="s1")
        registry.promote_claim_to_full_pn("owner", FULL_PN)
        self.assertEqual(registry.prepared_handoff_identity("owner", FULL_PN), "")

    def test_pn_only_claim_without_a_session_is_never_certified(self) -> None:
        # claim(collector_pn=...) proves nothing was seen on the wire for this
        # attempt, so it must never be promotable even once prepared.
        registry = CallbackSessionRegistry(sessions_source=lambda: [])
        registry.claim("owner", collector_pn=FULL_PN)
        registry.prepare_handoff("owner", FULL_PN)

        self.assertEqual(registry.prepared_handoff_identity("owner", FULL_PN), "")

    def test_certified_identity_is_the_claims_full_pn_not_the_callers_short_one(self) -> None:
        registry = self._registry()
        registry.claim_session("owner", session_id="s1")
        registry.promote_claim_to_full_pn("owner", FULL_PN)
        registry.prepare_handoff("owner", FULL_PN)

        # The caller offers a SHORT prefix; the registry answers with the full,
        # session-derived identity, and that is what must be persisted.
        certified = registry.prepared_handoff_identity("owner", SHORT_PN_PREFIX)
        self.assertEqual(certified, FULL_PN)

    def test_foreign_identity_is_not_certified(self) -> None:
        registry = self._registry()
        registry.claim_session("owner", session_id="s1")
        registry.promote_claim_to_full_pn("owner", FULL_PN)
        registry.prepare_handoff("owner", FULL_PN)
        self.assertEqual(registry.prepared_handoff_identity("owner", OTHER_FULL_PN), "")

    def test_unknown_owner_is_not_certified(self) -> None:
        registry = self._registry()
        self.assertEqual(registry.prepared_handoff_identity("nobody", FULL_PN), "")


class CallbackAttemptMatchingTests(unittest.TestCase):
    """One shared matcher decides what answered THIS attempt."""

    def _views(self, *sessions):
        return [
            {
                "session_id": s["session_id"],
                "collector_pn": s["collector_pn"],
                "state": s["state"],
                "has_strong_identity": s["collector_identity_source"] == "at_dtupn",
            }
            for s in sessions
        ]

    def _match(self, sessions, *, baseline=(), result_pn="", expected_pn="", gen=(0, 0)):
        return match_callback_answer(
            self._views(*sessions),
            baseline_session_ids=frozenset(baseline),
            result_pn=result_pn,
            expected_pn=expected_pn,
            trigger_generation_before=gen[0],
            trigger_generation_after=gen[1],
        )

    def test_matching_pn_is_accepted(self) -> None:
        match = self._match(
            [_observed("s2", FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 1)
        )
        self.assertTrue(match.confirmed)
        self.assertEqual((match.session_id, match.collector_pn), ("s2", FULL_PN))

    def test_foreign_new_strong_pn_is_not_accepted(self) -> None:
        # THE regression: the probe reached FULL_PN, but a DIFFERENT collector
        # dialed in. It must never be bound.
        match = self._match(
            [_observed("s2", OTHER_FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 1)
        )
        self.assertFalse(match.confirmed)
        self.assertEqual(match.result, MATCH_IDENTITY_MISMATCH)
        self.assertEqual(match.collector_pn, "")

    def test_two_distinct_new_identities_are_ambiguous(self) -> None:
        match = self._match(
            [_observed("s2", FULL_PN), _observed("s3", OTHER_FULL_PN)],
            baseline={"s1"},
            result_pn=FULL_PN,
            gen=(0, 1),
        )
        self.assertEqual(match.result, MATCH_IDENTITY_AMBIGUOUS)
        self.assertEqual(match.collector_pn, "")

    def test_no_detector_identity_fails_closed(self) -> None:
        # The probe confirmed no durable PN -> a strong new session is NOT proof.
        match = self._match([_observed("s2", FULL_PN)], baseline={"s1"}, gen=(0, 1))
        self.assertFalse(match.confirmed)
        self.assertEqual(match.collector_pn, "")

    def test_concurrent_trigger_is_interference(self) -> None:
        # Someone else triggered while we waited: a new session is no longer
        # attributable to OUR trigger.
        match = self._match(
            [_observed("s2", FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 2)
        )
        self.assertEqual(match.result, MATCH_TRIGGER_INTERFERENCE)

    def test_trigger_generation_exactly_one_is_required(self) -> None:
        # 1 == our own trigger -> matching may proceed.
        match = self._match(
            [_observed("s2", FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 1)
        )
        self.assertTrue(match.confirmed)

    def test_trigger_generation_zero_is_provenance_failure(self) -> None:
        # Our own trigger never went out, so nothing that appeared can be OUR
        # answer -- a coincidental dial-in must not be claimed as proof.
        match = self._match(
            [_observed("s2", FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 0)
        )
        self.assertEqual(match.result, MATCH_TRIGGER_INTERFERENCE)
        self.assertEqual(match.collector_pn, "")

    def test_trigger_generation_two_is_interference(self) -> None:
        # Someone else triggered concurrently: not attributable to us.
        match = self._match(
            [_observed("s2", FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 2)
        )
        self.assertEqual(match.result, MATCH_TRIGGER_INTERFERENCE)
        self.assertEqual(match.collector_pn, "")

    def test_pre_trigger_session_is_not_an_answer(self) -> None:
        match = self._match(
            [_observed("s1", FULL_PN)], baseline={"s1"}, result_pn=FULL_PN, gen=(0, 1)
        )
        self.assertEqual(match.result, MATCH_TIMEOUT)

    def test_weak_new_session_is_not_an_answer(self) -> None:
        match = self._match(
            [_observed("s2", SHORT_PN, source="framed_heartbeat")],
            result_pn=FULL_PN,
            gen=(0, 1),
        )
        self.assertEqual(match.result, MATCH_TIMEOUT)

    def test_closed_new_session_is_not_an_answer(self) -> None:
        match = self._match(
            [_observed("s2", FULL_PN, state="closed_disconnected")],
            result_pn=FULL_PN,
            gen=(0, 1),
        )
        self.assertEqual(match.result, MATCH_TIMEOUT)

    def test_route_identity_mismatch_session_is_not_an_answer(self) -> None:
        match = self._match(
            [_observed("s2", FULL_PN, state="route_identity_mismatch")],
            result_pn=FULL_PN,
            gen=(0, 1),
        )
        self.assertEqual(match.result, MATCH_TIMEOUT)

    def test_expected_identity_gates_the_probe_result(self) -> None:
        # Manual verification context: the probe reached a DIFFERENT collector.
        match = self._match(
            [_observed("s2", OTHER_FULL_PN)],
            baseline={"s1"},
            result_pn=OTHER_FULL_PN,
            expected_pn=FULL_PN,
            gen=(0, 1),
        )
        self.assertEqual(match.result, MATCH_IDENTITY_MISMATCH)

    def test_short_expected_pn_enriches_to_full(self) -> None:
        match = self._match(
            [_observed("s2", FULL_PN)],
            baseline={"s1"},
            result_pn=FULL_PN,
            expected_pn="E50000999900",
            gen=(0, 1),
        )
        self.assertTrue(match.confirmed)
        self.assertEqual(match.collector_pn, FULL_PN)

    def test_identity_matching_delegates_to_the_registry(self) -> None:
        from custom_components.eybond_local.onboarding.pending_attempt import (
            pending_attempt_matches_identity,
        )

        self.assertTrue(pending_attempt_matches_identity("E50000999900", FULL_PN))
        self.assertFalse(pending_attempt_matches_identity(FULL_PN, OTHER_FULL_PN))


class PendingActiveProbeScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_callback_attempt_owns_one_passive_discovery_scope(self) -> None:
        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )
        from custom_components.eybond_local.onboarding.pending_attempt import (
            async_run_pending_callback_attempt,
        )

        entry = _pending_entry(
            strategy=C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            address=NAT_IP,
        )
        hass = _fake_hass(registry=CallbackSessionRegistry(sessions_source=lambda: ()))
        events: list[tuple[str, str]] = []

        @contextmanager
        def _scope(_hass, scope_id):
            events.append(("begin", scope_id))
            retained: set[str] = set()
            try:
                yield retained
            finally:
                events.append(("end", scope_id))

        class _Detector:
            async def async_auto_detect(self, **_kwargs):
                get_callback_trigger_ledger().record(
                    target=NAT_IP, source="pending_scope_test"
                )
                return ()

        with patch(
            "custom_components.eybond_local.passive_discovery."
            "active_callback_probe_scope",
            new=_scope,
        ), patch(
            "custom_components.eybond_local.onboarding.pending_attempt."
            "create_onboarding_manager",
            return_value=_Detector(),
        ):
            await async_run_pending_callback_attempt(hass, entry)

        self.assertEqual([event for event, _scope_id in events], ["begin", "end"])
        self.assertEqual(events[0][1], events[1][1])
        self.assertTrue(events[0][1].startswith("pending_callback:"))


if __name__ == "__main__":
    unittest.main()
