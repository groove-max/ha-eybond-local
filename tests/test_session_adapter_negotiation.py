"""Phase 2 tests: real session ownership + live adapter negotiation.

These lock in the fix for the remote-ESP-behind-NAT bug: an entry persisted with
``collector_session_protocol=at_text`` in front of a live FRAMED session must use
the framed adapter (so PI30/SMG detection proceeds) instead of waiting on the AT
transport and reporting collector_offline. Transport ownership is by durable full
PN, never peer IP; short PNs only enrich the full PN.
"""

from __future__ import annotations

import unittest
import types

from custom_components.eybond_local.connection.session_handle import (
    ADAPTER_AT_COMMANDS,
    ADAPTER_INVERTER_FRAMED_FC4,
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    ADAPTER_FRAMED_COLLECTOR_COMMANDS,
    ADAPTER_FRAMED_FORWARD,
    ADAPTER_RAW_PASSTHROUGH,
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


if __name__ == "__main__":
    unittest.main()
