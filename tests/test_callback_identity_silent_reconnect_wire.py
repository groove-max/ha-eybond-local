"""Real loopback TCP/UDP: old silent peer, delayed callback, explicit identity."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import socket
import unittest

import test_callback_identity_production_wire as wire
from custom_components.eybond_local.connection.callback_identity import (
    CallbackIdentityRequest,
    IDENTITY_SESSION_SILENT,
    OnboardingWireProbeIntent,
    async_run_callback_identity_transaction,
)


class SilentReconnectWireTests(wire.ProductionWireHarness):
    async def test_closed_baseline_needs_another_trigger_inside_the_same_attempt(self):
        # #75: the collector ignores a trigger while its old socket is open.
        # Closing that socket does NOT reconnect it; only another UDP does.
        # A different collector behind the same relay must remain untouched.
        other_reader, other_writer = await asyncio.open_connection("127.0.0.1", self._tcp_port)
        self.addAsyncCleanup(other_writer.wait_closed)
        self.addCleanup(other_writer.close)
        udp_port = wire._free_port(socket.SOCK_DGRAM)
        service = wire.FakeCollectorService(
            listen_ip="127.0.0.1", udp_port=udp_port,
            tcp_bind_ip="127.0.0.1", heartbeat_interval=30, connect_timeout=2,
            udp_reply="ACK", scenario=wire.resolve_scenario(
                preset="collector_only", profile=wire.CollectorProfile(pn=wire.FULL_PN),
                first_heartbeat_delay=3600,
            ),
        )
        await service.start()
        self.addAsyncCleanup(service.stop)
        await service._ensure_reverse_tcp("127.0.0.1", self._tcp_port)
        async with asyncio.timeout(3):
            while len(self._listener.silent_pending_collector_sessions()) != 2:
                await asyncio.sleep(0.01)
        old_ids = {row["session_id"] for row in self._listener.silent_pending_collector_sessions()}
        request = CallbackIdentityRequest(
            server_ip="127.0.0.1", tcp_port=self._tcp_port,
            udp_port=udp_port, target_ip="127.0.0.1", session_wait_timeout=6,
        )
        async with asyncio.TaskGroup() as tasks:
            transaction = tasks.create_task(async_run_callback_identity_transaction(self._hass, request))
            async with asyncio.timeout(3):
                while getattr(service, "discovery_rx_count", 0) < 1:
                    await asyncio.sleep(0.01)
            await service._close_tcp_only()
        outcome = transaction.result()
        self.assertEqual(outcome.result, IDENTITY_SESSION_SILENT)
        self.assertEqual(service.discovery_rx_count, 2)
        offer = outcome.silent_bootstrap_offer
        self.assertIsNotNone(offer)
        self.assertNotIn(offer.session_id, old_ids)
        self.assertFalse(outcome.identity_certified)
        self.assertEqual(self._registry.diagnostics()["claim_count"], 0)
        confirmed = await async_run_callback_identity_transaction(
            self._hass, replace(request, bootstrap_probe=OnboardingWireProbeIntent.for_offer(
                offer, protocol="eybond_framed",
            )),
        )
        self.assertTrue(confirmed.identity_certified, confirmed.result)
        self.assertEqual(confirmed.session_id, offer.session_id)
        self.assertEqual(confirmed.collector_pn, wire.FULL_PN)
        self.assertEqual(service.discovery_rx_count, 2)  # continuation sends nothing
        self._assert_certified_handoff_transfers_socket(confirmed)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(other_reader.read(1), 0.05)

    async def test_loopback_relay_peer_gets_live_offer_and_exact_framed_query(self):
        # Both sockets have the same loopback peer, as with a TCP relay. The
        # pre-existing socket must never receive a probe or acquire ownership.
        old_reader, old_writer = await asyncio.open_connection("127.0.0.1", self._tcp_port)
        self.addAsyncCleanup(old_writer.wait_closed)
        self.addCleanup(old_writer.close)
        async with asyncio.timeout(3):
            while not self._listener.silent_pending_collector_sessions():
                await asyncio.sleep(0.01)
        old_ids = {
            row["session_id"] for row in self._listener.silent_pending_collector_sessions()
        }

        udp_port = wire._free_port(socket.SOCK_DGRAM)
        service = wire.FakeCollectorService(
            listen_ip="127.0.0.1", udp_port=udp_port, tcp_bind_ip="127.0.0.1",
            heartbeat_interval=30, connect_timeout=2, udp_reply="ACK",
            scenario=wire.resolve_scenario(
                preset="collector_only", profile=wire.CollectorProfile(pn=wire.FULL_PN),
                first_heartbeat_delay=3600, reverse_connect_delay=0.6,
            ),
        )
        await service.start()
        self.addAsyncCleanup(service.stop)
        request = CallbackIdentityRequest(
            server_ip="127.0.0.1", tcp_port=self._tcp_port,
            udp_port=udp_port, target_ip="127.0.0.1", session_wait_timeout=10,
        )
        async with asyncio.timeout(8):
            outcome = await async_run_callback_identity_transaction(self._hass, request)
            self.assertEqual(outcome.result, IDENTITY_SESSION_SILENT)
            self.assertFalse(outcome.identity_certified)
            offer = outcome.silent_bootstrap_offer
            self.assertIsNotNone(offer)
            self.assertNotIn(offer.session_id, old_ids)
            self.assertIn(offer.session_id, {
                row["session_id"] for row in self._listener.silent_pending_collector_sessions()
            })
            self.assertEqual(self._registry.diagnostics()["claim_count"], 0)

            confirmed = await async_run_callback_identity_transaction(
                self._hass, replace(request, bootstrap_probe=OnboardingWireProbeIntent.for_offer(
                    offer, protocol="eybond_framed",
                )),
            )
        self.assertTrue(confirmed.identity_certified, confirmed.result)
        self.assertEqual(confirmed.session_id, offer.session_id)
        self.assertEqual(confirmed.collector_pn, wire.FULL_PN)
        self.assertEqual(confirmed.identity_source, "fc2_parameter_2")
        self.assertEqual(service.discovery_rx_count, 1)  # zero-send continuation
        self._assert_certified_handoff_transfers_socket(confirmed)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(old_reader.read(1), 0.05)


if __name__ == "__main__":
    unittest.main()
