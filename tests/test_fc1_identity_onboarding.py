"""FC1-only identity uses the real listener, callback and claimed-session reader."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import unittest

import test_callback_identity_production_wire as wire
from custom_components.eybond_local.collector.identity_probe import (
    PROBE_FRAMED_FC1, build_identity_probe_request, parse_identity_probe_response,
)
from custom_components.eybond_local.collector.protocol import encode_header
from custom_components.eybond_local.connection.callback_identity import (
    CallbackIdentityRequest, OnboardingWireProbeIntent,
    async_run_callback_identity_transaction,
)


class Fc1IdentityOnboardingTests(wire.ProductionWireHarness):
    async def _start(self, *, silent=False):
        service = wire._framed_service(udp_port=0, pn=wire.FULL_PN)
        service._scenario = replace(
            service._scenario, fc1_full_pn=True,
            first_heartbeat_delay=3600 if silent else 0,
            fc2_query_modes={2: "timeout", 5: "timeout", 14: "timeout"},
        )
        await service.start()
        self.addAsyncCleanup(service.stop)
        return CallbackIdentityRequest(
            server_ip="127.0.0.1", tcp_port=self._tcp_port,
            udp_port=service._udp_transport.get_extra_info("sockname")[1],
            target_ip="127.0.0.1", session_wait_timeout=3,
        )

    async def _run(self, request):
        return await asyncio.wait_for(
            async_run_callback_identity_transaction(self._hass, request), timeout=15,
        )

    async def test_full_heartbeat_without_fc2_is_certified_by_challenge(self):
        outcome = await self._run(await self._start())
        self.assertTrue(outcome.identity_certified, outcome.result)
        self.assertEqual(outcome.collector_pn, wire.FULL_PN)
        self.assertEqual(outcome.identity_source, "fc1_identity_challenge")
        self._assert_certified_handoff_transfers_socket(outcome)

    async def test_silent_collector_explicit_framed_choice_uses_fc1(self):
        request = await self._start(silent=True)
        first = await self._run(request)
        self.assertFalse(first.identity_certified)
        self.assertIsNotNone(first.silent_bootstrap_offer)
        outcome = await self._run(replace(
            request, bootstrap_probe=OnboardingWireProbeIntent(
                protocol="eybond_framed", session_id=first.silent_bootstrap_offer.session_id,
            ),
        ))
        self.assertTrue(outcome.identity_certified, outcome.result)
        self.assertEqual(outcome.session_id, first.silent_bootstrap_offer.session_id)
        self.assertEqual(outcome.collector_pn, wire.FULL_PN)
        self._assert_certified_handoff_transfers_socket(outcome)

    async def test_valid_foreign_fc1_identity_is_not_adopted(self):
        request = await self._start()
        outcome = await self._run(replace(request, expected_pn=wire.FULL_PN[:-1] + "9"))
        self.assertFalse(outcome.identity_certified)
        self.assertEqual(self._registry.owner_for_pn(wire.FULL_PN), "")


class Fc1IdentityPayloadTests(unittest.TestCase):
    def test_prefix_wrong_tid_wrong_function_and_failed_fc2_are_not_identity(self):
        request = build_identity_probe_request("eybond_framed", probe_kind=PROBE_FRAMED_FC1)
        for tid, fc, payload in (
            (1, 1, wire.FULL_PN[:14].encode()),
            (2, 1, wire.FULL_PN.encode()),
            (1, 2, wire.FULL_PN.encode()),
        ):
            with self.subTest(tid=tid, fc=fc, payload=payload):
                self.assertEqual(parse_identity_probe_response(
                    request, encode_header(tid, 258, 8 + len(payload), 255, fc) + payload,
                ), ("", ""))
        request = build_identity_probe_request("eybond_framed")
        payload = b"\x01\x02" + wire.FULL_PN.encode()
        self.assertEqual(parse_identity_probe_response(
            request, encode_header(1, 1, 8 + len(payload), 1, 2) + payload,
        ), ("", ""))


if __name__ == "__main__":
    unittest.main()
