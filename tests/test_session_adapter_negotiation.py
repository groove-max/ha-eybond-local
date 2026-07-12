"""Phase 2 tests: real session ownership + live adapter negotiation.

These lock in the fix for the remote-ESP-behind-NAT bug: an entry persisted with
``collector_session_protocol=at_text`` in front of a live FRAMED session must use
the framed adapter (so PI30/SMG detection proceeds) instead of waiting on the AT
transport and reporting collector_offline. Transport ownership is by durable full
PN, never peer IP; short PNs only enrich the full PN.
"""

from __future__ import annotations

import asyncio
import unittest
import types

from custom_components.eybond_local.connection.session_handle import (
    ADAPTER_AT_COMMANDS,
    ADAPTER_INVERTER_FRAMED_FC4,
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    ADAPTER_FRAMED_COLLECTOR_COMMANDS,
    ADAPTER_FRAMED_FORWARD,
    ADAPTER_RAW_PASSTHROUGH,
    ConfirmedWireBinding,
    WIRE_AT_TEXT,
    WIRE_FRAMED,
    WIRE_UNKNOWN,
    negotiate_session_adapters,
    negotiate_wire,
)
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.link_transport import select_payload_route
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
    SESSION_STATE_CLOSED,
    reconcile_pn,
)
from custom_components.eybond_local.runtime.hub import _reconcile_durable_collector_pn
from custom_components.eybond_local.runtime.link import (
    EybondRuntimeLinkManager,
    _UnavailablePayloadTransport,
    _callback_identity_status_values,
)


FULL_PN = "PNALPHA-FULL-0001"
SHORT_PN = "PNALPHA-FU"  # a 10-char prefix of FULL_PN (the framed heartbeat PN)
OTHER_FULL_PN = "PNBETA-FULL-0002"


def _observed(session_id, pn, *, peer_ip="203.0.113.9", state="", shape="", source=""):
    return {
        "session_id": session_id,
        "peer_ip": peer_ip,
        "listener_port": 18899,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": shape,
        "collector_identity_source": source,
    }


class _FakeTransport:
    """Payload-transport double exposing only the public session facade."""

    def __init__(self, sessions):
        self._listener = object()  # opaque; the link must not read its internals
        self._sessions = tuple(dict(s) for s in sessions)
        self.connected = True
        self.collector_info = types.SimpleNamespace(remote_ip="", heartbeat_fresh=False)

    def observed_collector_sessions(self):
        return self._sessions

    def select_payload_route(self, route, *, payload_family=""):
        return route


class _FakeAtTransport:
    connected = True
    collector_info = types.SimpleNamespace(remote_ip="")

    def select_payload_route(self, route, *, payload_family=""):
        return RawSerialLinkRoute(protocol=payload_family)


def _bare_link(*, collector_pn, collector_ip, persisted_protocol, sessions):
    link = object.__new__(EybondRuntimeLinkManager)
    link._collector_pn = collector_pn
    link._collector_ip = collector_ip
    link._collector_session_protocol = persisted_protocol
    link._transport = _FakeTransport(sessions)
    link._at_transport = _FakeAtTransport()
    link._unavailable_payload_transport = _UnavailablePayloadTransport()
    link._auxiliary_transports = {}
    link._auxiliary_at_transports = {}
    link._auxiliary_listener_ports = set()
    link._runtime_claim_pn = None
    link._confirmed_wire_binding = None
    link._session_registry = CallbackSessionRegistry(
        sessions_source=link._iter_observed_sessions,
    )
    return link


class SessionHandleNegotiationTests(unittest.TestCase):
    def test_framed_byte_shape_negotiates_framed_forward(self) -> None:
        handle = negotiate_session_adapters(
            _observed("s1", FULL_PN, shape="eybond_framed_or_binary", source="framed_heartbeat")
        )
        self.assertEqual(handle.wire, WIRE_FRAMED)
        self.assertTrue(handle.supports(ADAPTER_FRAMED_FORWARD))
        self.assertTrue(handle.supports(ADAPTER_FRAMED_COLLECTOR_COMMANDS))
        self.assertFalse(handle.supports(ADAPTER_AT_COMMANDS))

    def test_routed_state_takes_precedence_only_when_not_contradicted(self) -> None:
        # Even if a stale shape hints AT, the routed framed state is authoritative
        # only when the observation is not an impossible framed-vs-AT conflict.
        self.assertEqual(
            negotiate_wire(state="routed_framed", protocol_shape="unknown"),
            WIRE_FRAMED,
        )
        handle = negotiate_session_adapters(
            _observed(
                "s-conflict",
                FULL_PN,
                state="routed_at_text",
                shape="eybond_framed",
                source="at_dtupn",
            )
        )
        self.assertEqual(handle.wire, WIRE_UNKNOWN)
        self.assertIn("wire_conflict", handle.conflict)
        self.assertFalse(handle.supports(ADAPTER_RAW_PASSTHROUGH))

    def test_framed_shape_with_at_identity_source_stays_framed(self) -> None:
        handle = negotiate_session_adapters(
            _observed("s1", FULL_PN, shape="eybond_framed", source="at_dtupn")
        )
        self.assertEqual(handle.wire_framing, WIRE_FRAMED)
        self.assertIn("at_dtupn", handle.identity_sources)
        self.assertEqual(handle.inverter_forward_adapter, ADAPTER_INVERTER_FRAMED_FC4)
        self.assertFalse(handle.supports(ADAPTER_INVERTER_RAW_PASSTHROUGH))

    def test_at_text_negotiates_raw_passthrough_and_at_commands(self) -> None:
        handle = negotiate_session_adapters(
            _observed("s2", FULL_PN, state="routed_at_text", shape="at_text", source="at_dtupn")
        )
        self.assertEqual(handle.wire, WIRE_AT_TEXT)
        self.assertTrue(handle.supports(ADAPTER_AT_COMMANDS))
        self.assertTrue(handle.supports(ADAPTER_RAW_PASSTHROUGH))
        self.assertFalse(handle.supports(ADAPTER_FRAMED_FORWARD))
        self.assertEqual(
            handle.inverter_forward_adapter,
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
        )

    def test_closed_listener_session_is_not_unclaimed_discovery_candidate(self) -> None:
        registry = CallbackSessionRegistry(
            sessions_source=lambda: [
                _observed(
                    "s-closed",
                    FULL_PN,
                    state="closed_disconnected",
                    shape="eybond_framed",
                    source="framed_heartbeat",
                )
            ]
        )

        observed = registry.observed_sessions()

        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0].state, SESSION_STATE_CLOSED)
        self.assertEqual(registry.list_unclaimed_sessions(), ())

    def test_unknown_shape_is_unobserved(self) -> None:
        handle = negotiate_session_adapters(_observed("s3", FULL_PN))
        self.assertEqual(handle.wire, WIRE_UNKNOWN)
        self.assertFalse(handle.observed)
        self.assertEqual(negotiate_session_adapters(None).available_adapters, frozenset())


class RegistrySessionHandleTests(unittest.TestCase):
    def test_handle_for_pn_reflects_live_framed_wire(self) -> None:
        sessions = [
            _observed("s1", FULL_PN, shape="eybond_framed_or_binary", source="framed_heartbeat")
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        handle = registry.session_handle_for_pn(FULL_PN)
        self.assertIsNotNone(handle)
        self.assertEqual(handle.wire, WIRE_FRAMED)

    def test_handle_for_entry_after_claim(self) -> None:
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entryA", collector_pn=FULL_PN)
        handle = registry.session_handle_for_entry("entryA")
        self.assertIsNotNone(handle)
        self.assertEqual(handle.collector_pn, FULL_PN)
        self.assertEqual(handle.wire, WIRE_FRAMED)

    def test_two_collectors_one_ip_have_distinct_handles_by_pn(self) -> None:
        # Same NAT/public IP, two collectors, different wires -- resolved by PN.
        sessions = [
            _observed("s1", FULL_PN, peer_ip="198.51.100.7", shape="eybond_framed_or_binary"),
            _observed("s2", OTHER_FULL_PN, peer_ip="198.51.100.7", state="routed_at_text", shape="at_text"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        self.assertEqual(registry.session_handle_for_pn(FULL_PN).wire, WIRE_FRAMED)
        self.assertEqual(registry.session_handle_for_pn(OTHER_FULL_PN).wire, WIRE_AT_TEXT)


class LinkLiveWireSelectionTests(unittest.TestCase):
    def test_link_obtains_handle_via_registry_not_listener_internals(self) -> None:
        # The link must go through the registry ownership API; it must not read
        # the listener's private _session_inventory. The fake transport exposes
        # only observed_collector_sessions() and an opaque _listener.
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="at_text",
            sessions=[
                _observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")
            ],
        )
        self.assertIsInstance(link._session_registry, CallbackSessionRegistry)
        # After negotiation the registry owns this entry's durable identity.
        self.assertFalse(link._uses_at_text_payload())
        self.assertEqual(
            link._session_registry.owner_for_pn(FULL_PN), "runtime"
        )

    def test_framed_live_session_overrides_stale_at_text_hint(self) -> None:
        # THE BUG: persisted at_text, live framed session -> must use framed.
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="at_text",
            sessions=[
                _observed(
                    "s1", FULL_PN, state="routed_framed",
                    shape="eybond_framed_or_binary", source="framed_heartbeat",
                )
            ],
        )
        self.assertFalse(link._uses_at_text_payload())
        handle = link.session_handle
        self.assertEqual(handle.wire, WIRE_FRAMED)
        self.assertTrue(handle.supports(ADAPTER_FRAMED_FORWARD))
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        route = select_payload_route(
            link.transport,
            EybondLinkRoute(devcode=0x0994, collector_addr=1),
            payload_family="pi30_ascii",
        )
        self.assertIsInstance(route, EybondLinkRoute)

    def test_at_text_live_session_uses_at_wire(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",  # stale, opposite of live
            sessions=[
                _observed("s2", FULL_PN, state="routed_at_text", shape="at_text", source="at_dtupn")
            ],
        )
        self.assertTrue(link._uses_at_text_payload())
        self.assertEqual(
            link._inverter_forward_adapter(),
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
        )
        route = select_payload_route(
            link.transport,
            EybondLinkRoute(devcode=0x0994, collector_addr=1),
            payload_family="pi30_ascii",
        )
        self.assertIsInstance(route, RawSerialLinkRoute)
        self.assertEqual(route.protocol, "pi30_ascii")

    def test_conflicting_live_session_does_not_silently_select_raw_passthrough(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="at_text",
            sessions=[
                _observed(
                    "s1",
                    FULL_PN,
                    state="routed_at_text",
                    shape="eybond_framed",
                    source="at_dtupn",
                )
            ],
        )
        self.assertTrue(link.session_handle.conflict)
        # Conflict is fail-closed to no live adapter; only the documented legacy
        # fallback remains until the listener stops producing contradictory state.
        self.assertEqual(link.session_handle.inverter_forward_adapter, "none")
        self.assertFalse(link.connected)
        with self.assertRaises(TypeError):
            select_payload_route(
                link.transport,
                EybondLinkRoute(devcode=0x0994, collector_addr=1),
                payload_family="pi30_ascii",
            )

    def test_unobserved_session_falls_back_to_persisted_hint(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN, collector_ip="", persisted_protocol="at_text", sessions=[]
        )
        self.assertTrue(link._uses_at_text_payload())
        link_framed = _bare_link(
            collector_pn=FULL_PN, collector_ip="", persisted_protocol="eybond_framed", sessions=[]
        )
        self.assertFalse(link_framed._uses_at_text_payload())

    def test_peer_ip_does_not_leak_another_collectors_wire(self) -> None:
        # Two collectors on the same peer IP: the link for FULL_PN must negotiate
        # its OWN framed session, not the other collector's AT session.
        sessions = [
            _observed("s1", FULL_PN, peer_ip="198.51.100.7", state="routed_framed",
                      shape="eybond_framed_or_binary", source="framed_heartbeat"),
            _observed("s2", OTHER_FULL_PN, peer_ip="198.51.100.7", state="routed_at_text",
                      shape="at_text", source="at_dtupn"),
        ]
        link = _bare_link(
            collector_pn=FULL_PN, collector_ip="", persisted_protocol="at_text", sessions=sessions
        )
        self.assertFalse(link._uses_at_text_payload())
        self.assertEqual(link.session_handle.collector_pn, FULL_PN)

    def test_short_pn_heartbeat_matches_full_pn_entry(self) -> None:
        # Remote ESP behind NAT: only the short heartbeat PN is on the wire, but
        # the entry's durable identity is the full PN. It must still resolve.
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="at_text",
            sessions=[
                _observed("s1", SHORT_PN, state="routed_framed",
                          shape="eybond_framed_or_binary", source="framed_heartbeat")
            ],
        )
        self.assertFalse(link._uses_at_text_payload())

    def test_mismatch_same_prefix_session_cannot_override_claimed_routed_session(self) -> None:
        # The claimed session is a live, routed FRAMED session (full PN). A
        # separate route-identity-mismatch session shares the PN prefix and
        # sniffed AT-shaped bytes. It must NOT become the runtime wire truth:
        # the framed routed session wins.
        sessions = [
            _observed("s-routed", FULL_PN, state="routed_framed",
                      shape="eybond_framed_or_binary", source="at_dtupn"),
            _observed("s-mismatch", SHORT_PN, state="route_identity_mismatch",
                      shape="at_text", source="framed_heartbeat"),
        ]
        link = _bare_link(
            collector_pn=FULL_PN, collector_ip="", persisted_protocol="at_text", sessions=sessions
        )
        self.assertFalse(link._uses_at_text_payload())
        self.assertEqual(link.session_handle.wire, WIRE_FRAMED)

    def test_pending_identity_session_does_not_override_claimed_routed_session(self) -> None:
        # A not-yet-routed (waiting_for_route_identity) same-prefix session must
        # not win over the claimed routed AT session.
        sessions = [
            _observed("s-routed", FULL_PN, state="routed_at_text",
                      shape="at_text", source="at_dtupn"),
            _observed("s-pending", SHORT_PN, state="waiting_for_route_identity",
                      shape="eybond_framed_or_binary", source="framed_heartbeat"),
        ]
        link = _bare_link(
            collector_pn=FULL_PN, collector_ip="", persisted_protocol="eybond_framed", sessions=sessions
        )
        self.assertTrue(link._uses_at_text_payload())
        self.assertEqual(link.session_handle.wire, WIRE_AT_TEXT)

    def test_claim_conflict_does_not_cache_failed_runtime_claim(self) -> None:
        # Defensive hardening: if the runtime-scoped registry ever rejects the
        # claim, the link must not remember the PN as successfully claimed. That
        # would freeze the handle as unknown until the PN changes.
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="at_text",
            sessions=[
                _observed("s1", FULL_PN, state="routed_framed",
                          shape="eybond_framed_or_binary", source="framed_heartbeat")
            ],
        )
        link._session_registry.claim("other-entry", collector_pn=FULL_PN)

        handle = link.session_handle

        self.assertEqual(handle.wire, WIRE_UNKNOWN)
        self.assertNotEqual(link._runtime_claim_pn, FULL_PN)


class PnStabilityTests(unittest.TestCase):
    def test_short_heartbeat_does_not_downgrade_durable_full_pn(self) -> None:
        pn, conflict = _reconcile_durable_collector_pn(FULL_PN, SHORT_PN)
        self.assertEqual(pn, FULL_PN)
        self.assertFalse(conflict)

    def test_longer_same_identity_pn_enriches_durable(self) -> None:
        pn, conflict = _reconcile_durable_collector_pn(SHORT_PN, FULL_PN)
        self.assertEqual(pn, FULL_PN)
        self.assertFalse(conflict)

    def test_different_full_pn_is_identity_conflict_keeps_durable(self) -> None:
        pn, conflict = _reconcile_durable_collector_pn(FULL_PN, OTHER_FULL_PN)
        self.assertEqual(pn, FULL_PN)
        self.assertTrue(conflict)

    def test_no_durable_uses_observed(self) -> None:
        pn, conflict = _reconcile_durable_collector_pn("", SHORT_PN)
        self.assertEqual(pn, SHORT_PN)
        self.assertFalse(conflict)


class RegistryPnReconciliationTests(unittest.TestCase):
    """Phase 5: short/full PN reconciliation has a single home in the registry."""

    def test_reconcile_pn_enriches_short_to_full(self) -> None:
        self.assertEqual(reconcile_pn(SHORT_PN, FULL_PN), FULL_PN)
        self.assertEqual(reconcile_pn(FULL_PN, SHORT_PN), FULL_PN)

    def test_reconcile_pn_keeps_current_on_identity_conflict(self) -> None:
        # A genuinely different PN must not silently switch identity.
        self.assertEqual(reconcile_pn(FULL_PN, OTHER_FULL_PN), FULL_PN)

    def test_reconcile_pn_handles_empty(self) -> None:
        self.assertEqual(reconcile_pn("", FULL_PN), FULL_PN)
        self.assertEqual(reconcile_pn(FULL_PN, ""), FULL_PN)

    def test_transport_and_link_helpers_delegate_to_registry(self) -> None:
        # The transport/link connection-level helpers must resolve identically to
        # the registry's single reconciliation function.
        from custom_components.eybond_local.collector.transport import (
            _prefer_more_complete_identity,
        )
        from custom_components.eybond_local.runtime.link import (
            _prefer_more_complete_collector_pn,
        )

        for a, b in ((SHORT_PN, FULL_PN), (FULL_PN, SHORT_PN), (FULL_PN, OTHER_FULL_PN)):
            self.assertEqual(_prefer_more_complete_identity(a, b), reconcile_pn(a, b))
            self.assertEqual(_prefer_more_complete_collector_pn(a, b), reconcile_pn(a, b))


def _set_sessions(link, sessions) -> None:
    """Replace the fake transport's observed sessions (models socket lifecycle)."""

    link._transport._sessions = tuple(dict(s) for s in sessions)


def _reconcilable(link) -> None:
    """Add the persisted-profile fields the reconcile method reads."""

    link._collector_identity_strategy = "framed_heartbeat_then_fc2_pn"
    link._collector_raw_passthrough_bootstrap = ""
    link._collector_raw_passthrough_frame_format = ""
    link._collector_raw_passthrough_min_interval_ms = 0


def _observe(link) -> None:
    """Model an explicit session-observation event (poll / owned-session monitor).

    Binding adoption is a lifecycle step, never a side effect of a read, so tests
    must adopt explicitly instead of relying on an accessor to latch.
    """

    link._adopt_trusted_live_binding()


class SessionHandoverLifecycleTests(unittest.TestCase):
    """Same-PN reconnect is a session handover, never a wire/profile change.

    These lock in the fix for the ~15-minute PI30 reconnect that flapped
    eybond_framed -> at_text -> eybond_framed and re-onboarded for ~17s. A trusted
    live wire is adopted into an immutable ConfirmedWireBinding (durable wire
    facts only, no socket metadata); it survives the transient gap so cloud-family
    bootstrap can never fill it with at_text.
    """

    # A. Same-PN framed replacement keeps framed_fc4 throughout, never at_text.
    def test_same_pn_framed_replacement_keeps_framed_adapter(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)  # a poll observed the trusted framed session
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertEqual(link._owned_observed_session_protocol(), "eybond_framed")

        # Old socket closes; the new same-PN socket is present but not yet routed
        # (the real handover window). The confirmed wire must survive.
        _set_sessions(link, [_observed("s2", FULL_PN, state="waiting_for_route_identity")])
        _observe(link)  # observation during the gap must not change the binding
        self.assertFalse(link._uses_at_text_payload())
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertEqual(link._owned_observed_session_protocol(), "eybond_framed")

        # New socket becomes routed framed: still framed, no flap.
        _set_sessions(link, [_observed("s2", FULL_PN, state="routed_framed", source="framed_heartbeat")])
        _observe(link)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)

    # B. Full gap (no session observed) + cloud-family bootstrap cannot switch to
    # at_text: the confirmed wire holds and the reconcile refuses the guess.
    def test_gap_holds_confirmed_wire_and_reconcile_refuses_bootstrap(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _reconcilable(link)
        _observe(link)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertTrue(link.has_confirmed_wire_binding())

        # Transient gap: nothing observed at all.
        _set_sessions(link, [])
        self.assertEqual(link._owned_observed_session_protocol(), "eybond_framed")
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)

        # The coordinator's cloud-family bootstrap (smartess_at -> at_text) tries
        # to reconcile during the gap. It must be a no-op: no rebuild, no
        # downgrade of the confirmed live wire.
        changed = asyncio.run(
            link.async_reconcile_collector_session_profile(
                collector_session_protocol="at_text",
                collector_identity_strategy="at_dtupn",
                reason="refresh",
            )
        )
        self.assertFalse(changed)
        self.assertEqual(link._collector_session_protocol, "eybond_framed")
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)

    # Conflict + attempted bootstrap reconcile: a live conflict blocks the rebuild
    # entirely; conflict preserved, configured protocol unchanged, no bootstrap.
    def test_live_conflict_blocks_bootstrap_reconcile(self) -> None:
        from custom_components.eybond_local.connection.session_handle import ADAPTER_NONE

        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _reconcilable(link)
        _observe(link)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)

        # A contradictory live observation appears (real conflict).
        _set_sessions(
            link,
            [_observed("s2", FULL_PN, state="routed_at_text", shape="eybond_framed", source="at_dtupn")],
        )
        self.assertTrue(link._live_session_handle().conflict)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_NONE)  # fail-closed

        # Bootstrap reconcile during the conflict must be blocked.
        changed = asyncio.run(
            link.async_reconcile_collector_session_profile(
                collector_session_protocol="at_text",
                collector_identity_strategy="at_dtupn",
                reason="refresh",
            )
        )
        self.assertFalse(changed)
        self.assertEqual(link._collector_session_protocol, "eybond_framed")
        # Conflict is still surfaced; bootstrap not applied.
        self.assertTrue(link._live_session_handle().conflict)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_NONE)

    # A genuine live framed->at_text change (positive live evidence) IS adopted
    # once observed, and updates the confirmed binding.
    def test_genuine_live_framed_to_at_text_change_is_adopted(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertTrue(link.confirmed_wire_binding.uses_framed_wire)

        # A genuinely observed at_text session (positive live evidence).
        _set_sessions(link, [_observed("s2", FULL_PN, state="routed_at_text", source="at_dtupn")])
        self.assertEqual(link._raw_live_observed_protocol(), "at_text")
        # Live observed session routes directly; adoption updates the binding.
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_RAW_PASSTHROUGH)
        _observe(link)
        self.assertTrue(link.confirmed_wire_binding.uses_at_text_wire)
        self.assertEqual(
            link.confirmed_wire_binding.inverter_forward_adapter,
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
        )

    # The confirmed binding must NOT carry stale socket metadata.
    def test_confirmed_binding_has_no_socket_metadata(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[
                _observed(
                    "sock-42", FULL_PN, peer_ip="203.0.113.9",
                    state="routed_framed", source="framed_heartbeat",
                )
            ],
        )
        _observe(link)
        binding = link.confirmed_wire_binding
        self.assertIsNotNone(binding)
        # Durable wire facts only.
        self.assertEqual(binding.collector_pn, FULL_PN)
        self.assertTrue(binding.uses_framed_wire)
        self.assertEqual(binding.inverter_forward_adapter, ADAPTER_INVERTER_FRAMED_FC4)
        # No transient socket identity is retained anywhere on the binding.
        for attr in ("session_id", "peer_ip", "listener_port", "state", "observed"):
            self.assertFalse(hasattr(binding, attr), attr)

    # Reading accessors/diagnostics must NOT create or change the binding.
    def test_reads_do_not_mutate_binding(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        # A trusted session is observable, but until an explicit adoption event
        # the binding stays None no matter how many reads happen.
        for _ in range(3):
            link._inverter_forward_adapter()
            link._owned_observed_session_protocol()
            link._effective_wire_binding()
            link._raw_live_observed_protocol()
        self.assertIsNone(link._confirmed_wire_binding)
        self.assertFalse(link.has_confirmed_wire_binding())

        # Only the explicit lifecycle step creates it.
        _observe(link)
        self.assertIsNotNone(link._confirmed_wire_binding)

    # E. Two collectors behind one peer IP stay independent: replacing one PN's
    # session never leaks the other's wire, and peer IP is never identity.
    def test_two_collectors_one_peer_ip_do_not_leak_wire(self) -> None:
        peer = "198.51.100.7"
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[
                _observed("a1", FULL_PN, peer_ip=peer, state="routed_framed", source="framed_heartbeat"),
                _observed("b1", OTHER_FULL_PN, peer_ip=peer, state="routed_at_text", source="at_dtupn"),
            ],
        )
        _observe(link)
        # Our entry negotiates its OWN framed wire, not the neighbour's at_text.
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertEqual(link.confirmed_wire_binding.collector_pn, FULL_PN)

        # Our socket enters a handover gap while the neighbour's at_text session
        # is still live on the same peer IP: we keep framed, never adopt at_text.
        _set_sessions(
            link,
            [
                _observed("a2", FULL_PN, peer_ip=peer, state="waiting_for_route_identity"),
                _observed("b1", OTHER_FULL_PN, peer_ip=peer, state="routed_at_text", source="at_dtupn"),
            ],
        )
        _observe(link)  # must not adopt the neighbour's at_text
        self.assertFalse(link._uses_at_text_payload())
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertEqual(link._owned_observed_session_protocol(), "eybond_framed")
        self.assertTrue(link.confirmed_wire_binding.uses_framed_wire)

    # C. Delayed close of the OLD socket after the NEW one is accepted must not
    # drop the new session's ownership (registry-level index/ownership).
    def test_old_close_after_new_accept_keeps_new_session(self) -> None:
        sessions = [
            _observed("old", FULL_PN, state="routed_framed", source="framed_heartbeat"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim("entry-1", collector_pn=FULL_PN)

        # New socket accepted for the same PN.
        sessions.append(_observed("new", FULL_PN, state="routed_framed", source="framed_heartbeat"))
        # Delayed close callback for the OLD socket arrives: it becomes terminal.
        sessions[0]["state"] = SESSION_STATE_CLOSED

        owned = registry.owned_session_location("entry-1")
        self.assertIsNotNone(owned)
        self.assertEqual(owned.session_id, "new")
        handle = registry.session_handle_for_entry("entry-1")
        self.assertTrue(handle.observed)
        self.assertEqual(handle.session_id, "new")

    # F. Diagnostics: a same-collector handover reports "reconnecting"; a real
    # route_identity_mismatch reports "conflict"; a merely-identified FOREIGN
    # session is unresolved/unowned, never a conflict.
    def test_diagnostics_reconnecting_vs_conflict(self) -> None:
        identified = [{"collector_identity_masked": True, "state": "waiting_for_route_identity"}]

        handover = _callback_identity_status_values(
            pending_count=1,
            recent_count=1,
            duplicate_peer_ip_count=0,
            sessions=identified,
            expects_collector_identity=True,
            owned_session_observed=True,
            handover_in_progress=True,
        )
        self.assertEqual(handover["collector_callback_identity_status"], "reconnecting")
        self.assertEqual(handover["collector_callback_identity_mismatch_count"], 0)

        # A foreign identified session (we own nothing here) is NOT a conflict.
        foreign = _callback_identity_status_values(
            pending_count=1,
            recent_count=1,
            duplicate_peer_ip_count=0,
            sessions=identified,
            expects_collector_identity=True,
            owned_session_observed=False,
            handover_in_progress=False,
        )
        self.assertEqual(foreign["collector_callback_identity_status"], "unresolved")
        self.assertEqual(foreign["collector_callback_identity_mismatch_count"], 0)
        self.assertEqual(foreign["collector_callback_foreign_identified_session_count"], 1)

        # Only positive evidence (route_identity_mismatch) is a conflict.
        real_mismatch = _callback_identity_status_values(
            pending_count=1,
            recent_count=1,
            duplicate_peer_ip_count=0,
            sessions=[{"state": "route_identity_mismatch"}],
            expects_collector_identity=True,
            owned_session_observed=True,
            handover_in_progress=False,
        )
        self.assertEqual(real_mismatch["collector_callback_identity_status"], "conflict")

    # Two PNs on one peer IP BEFORE any confirmed binding: a foreign identified
    # session must not create a conflict for the second entry.
    def test_foreign_pn_before_confirmed_binding_is_not_conflict(self) -> None:
        status = _callback_identity_status_values(
            pending_count=1,
            recent_count=1,
            duplicate_peer_ip_count=1,
            sessions=[{"collector_identity_masked": True, "state": "waiting_for_route_identity"}],
            expects_collector_identity=True,
            owned_session_observed=False,
            handover_in_progress=False,
        )
        self.assertNotEqual(status["collector_callback_identity_status"], "conflict")
        self.assertEqual(status["collector_callback_identity_mismatch_count"], 0)

    # G. Existing invariants: nothing is confirmed before the first observation,
    # and short/full PN remain one identity (no duplicate) across a handover.
    def test_no_confirmed_wire_before_first_observation(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="",
            sessions=[],
        )
        # Never observed: no binding is invented; reports no observed protocol.
        self.assertEqual(link._owned_observed_session_protocol(), "")
        self.assertIsNone(getattr(link, "_confirmed_wire_binding", None))
        self.assertFalse(link.has_confirmed_wire_binding())

    def test_short_full_pn_remain_one_identity_across_handover(self) -> None:
        # Confirm on the full PN, then a heartbeat reports only the short PN: it
        # is the same collector, so the confirmed framed wire is preserved.
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        _set_sessions(link, [_observed("s2", SHORT_PN, state="waiting_for_route_identity")])
        _observe(link)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)
        self.assertEqual(link._owned_observed_session_protocol(), "eybond_framed")
        self.assertEqual(link.confirmed_wire_binding.collector_pn, FULL_PN)


class ConfirmedWireBindingInvariantTests(unittest.TestCase):
    """A confirmed binding requires a durable entry PN and an identified live PN."""

    def _handle(self, pn, *, state="routed_framed", shape="", source="framed_heartbeat"):
        return negotiate_session_adapters(_observed("s1", pn, state=state, shape=shape, source=source))

    # A. Observed wire but NO live PN -> no binding (unidentified session).
    def test_observed_wire_without_live_pn_creates_no_binding(self) -> None:
        handle = self._handle("")  # routed framed, but collector_pn empty
        self.assertTrue(handle.observed)
        self.assertIsNone(ConfirmedWireBinding.from_handle(handle, collector_pn=FULL_PN))

    # B. No durable entry PN -> no binding, even for a fully identified session.
    def test_no_durable_pn_creates_no_binding(self) -> None:
        handle = self._handle(FULL_PN)
        self.assertIsNone(ConfirmedWireBinding.from_handle(handle, collector_pn=""))
        # And at the link level: an entry without a durable PN never confirms.
        link = _bare_link(
            collector_pn="",
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)
        self.assertIsNone(link._confirmed_wire_binding)
        self.assertFalse(link.has_confirmed_wire_binding())

    # A conflict / unknown wire never confirms.
    def test_conflict_or_unknown_wire_creates_no_binding(self) -> None:
        conflict = self._handle(FULL_PN, state="routed_at_text", shape="eybond_framed")
        self.assertTrue(conflict.conflict)
        self.assertIsNone(ConfirmedWireBinding.from_handle(conflict, collector_pn=FULL_PN))
        unknown = self._handle(FULL_PN, state="waiting_for_route_identity")
        self.assertFalse(unknown.observed)
        self.assertIsNone(ConfirmedWireBinding.from_handle(unknown, collector_pn=FULL_PN))

    # C. Durable full PN + live short PN -> binding stores the full PN.
    def test_durable_full_pn_and_live_short_pn_bind_with_full_pn(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", SHORT_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)
        binding = link.confirmed_wire_binding
        self.assertIsNotNone(binding)
        self.assertEqual(binding.collector_pn, FULL_PN)  # preferred full PN, not short
        self.assertTrue(binding.uses_framed_wire)

    # D. A foreign live PN never replaces an existing confirmed binding.
    def test_foreign_live_pn_does_not_replace_binding(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)
        self.assertEqual(link.confirmed_wire_binding.collector_pn, FULL_PN)

        # A different collector (foreign full PN) appears on the shared listener.
        _set_sessions(link, [_observed("x1", OTHER_FULL_PN, state="routed_at_text", source="at_dtupn")])
        _observe(link)
        # Our binding is untouched: still FULL_PN, still framed.
        self.assertEqual(link.confirmed_wire_binding.collector_pn, FULL_PN)
        self.assertTrue(link.confirmed_wire_binding.uses_framed_wire)


class HandoverLifecycleEvidenceTests(unittest.TestCase):
    """`reconnecting` requires an owned pending socket, not just a gap."""

    def _framed_link(self):
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="eybond_framed",
            sessions=[_observed("s1", FULL_PN, state="routed_framed", source="framed_heartbeat")],
        )
        _observe(link)  # confirm a framed binding
        return link

    # A. Confirmed binding but NO socket -> offline/idle, NOT reconnecting forever.
    def test_binding_without_socket_is_not_reconnecting(self) -> None:
        link = self._framed_link()
        _set_sessions(link, [])
        self.assertFalse(link._has_owned_pending_session())
        self.assertFalse(link._handover_in_progress())

    # B. Confirmed binding + owned pending socket -> reconnecting.
    def test_binding_with_owned_pending_socket_is_reconnecting(self) -> None:
        link = self._framed_link()
        _set_sessions(link, [_observed("s2", FULL_PN, state="waiting_for_route_identity")])
        self.assertTrue(link._has_owned_pending_session())
        self.assertTrue(link._handover_in_progress())

    # C. Confirmed binding + active trusted socket -> not reconnecting (ok/active).
    def test_binding_with_active_trusted_socket_is_ok(self) -> None:
        link = self._framed_link()
        _set_sessions(link, [_observed("s2", FULL_PN, state="routed_framed", source="framed_heartbeat")])
        _observe(link)
        self.assertFalse(link._handover_in_progress())
        self.assertTrue(link._live_session_handle().observed)
        self.assertEqual(link._inverter_forward_adapter(), ADAPTER_INVERTER_FRAMED_FC4)

    # D. Positive route_identity_mismatch is a conflict (not reconnecting).
    def test_route_identity_mismatch_is_conflict(self) -> None:
        status = _callback_identity_status_values(
            pending_count=1,
            recent_count=1,
            duplicate_peer_ip_count=0,
            sessions=[{"state": "route_identity_mismatch"}],
            expects_collector_identity=True,
            owned_session_observed=True,
            handover_in_progress=False,
        )
        self.assertEqual(status["collector_callback_identity_status"], "conflict")


class ProductionBindingAdoptionLifecycleTests(unittest.TestCase):
    """The monitor adopts wire evidence when one pending socket becomes routed."""

    def test_same_socket_pending_to_routed_adopts_confirmed_binding(self) -> None:
        link = _bare_link(
            collector_pn=FULL_PN,
            collector_ip="",
            persisted_protocol="at_text",
            sessions=[
                _observed(
                    "same-socket",
                    FULL_PN,
                    state="waiting_for_route_identity",
                )
            ],
        )

        # The accepted socket exists, but it has not established a trusted wire.
        link._reconcile_owned_session_binding_observation()
        self.assertIsNone(link.confirmed_wire_binding)
        socket_fingerprint = link._current_owned_session_fingerprint()

        # Listener routing completes on that exact socket.  session_id and port
        # stay unchanged, but the independent wire-observation lifecycle must
        # now adopt the framed binding.
        _set_sessions(
            link,
            [
                _observed(
                    "same-socket",
                    FULL_PN,
                    state="routed_framed",
                    source="framed_heartbeat",
                )
            ],
        )
        self.assertEqual(link._current_owned_session_fingerprint(), socket_fingerprint)
        link._reconcile_owned_session_binding_observation()

        binding = link.confirmed_wire_binding
        self.assertIsNotNone(binding)
        self.assertTrue(binding.uses_framed_wire)
        self.assertEqual(
            binding.inverter_forward_adapter,
            ADAPTER_INVERTER_FRAMED_FC4,
        )


if __name__ == "__main__":
    unittest.main()
