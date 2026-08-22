"""Tests for the collector-metadata boundary (Phase 4 + ownership hardening).

Covers the wire-layer readers (structured outcomes on the REAL production
readers, not just synthetic route thunks), the metadata health model, and the
runtime service: cadence, merge precedence, force-liveness, dirty/invalidation,
collector-only bootstrap, dead-channel learning (delivered-but-empty, NOT
timeout), transport-vs-command outcome, durable-identity-aware cache, and the
generation pre/postflight guard.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.at import CollectorAtResponse  # noqa: E402
from custom_components.eybond_local.collector.at_runtime import (  # noqa: E402
    read_runtime_collector_at_values,
)
from custom_components.eybond_local.collector.metadata import (  # noqa: E402
    AT_METADATA_CHANNEL,
    FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
    FRAMED_METADATA_CHANNEL,
    CollectorMetadataRoute,
    CollectorMetadataRouteSet,
    async_read_at_metadata,
    async_read_framed_metadata,
    build_collector_metadata_routes,
)
from custom_components.eybond_local.collector.metadata_result import (  # noqa: E402
    OUTCOME_COMMAND_ERROR,
    OUTCOME_EMPTY,
    OUTCOME_PARTIAL,
    OUTCOME_SUCCESS,
    OUTCOME_TRANSPORT_ERROR,
    CollectorMetadataChannelReadResult,
)
from custom_components.eybond_local.runtime.collector_metadata import (  # noqa: E402
    STATUS_CACHED,
    STATUS_COMMAND_ERROR,
    STATUS_EMPTY,
    STATUS_FRESH,
    STATUS_SKIPPED_DEAD,
    STATUS_STALE_GENERATION,
    STATUS_TRANSPORT_ERROR,
    CollectorMetadataService,
)
from custom_components.eybond_local.runtime.metadata_health import (  # noqa: E402
    UNSUPPORTED_METADATA_CHANNEL_STRIKES,
    CollectorMetadataHealth,
)


# --- reader test doubles -----------------------------------------------------


class _ProgrammableAtTransport:
    """AT transport whose per-command behavior is scripted."""

    def __init__(self, script: dict[str, object]) -> None:
        # value: str -> blank/real value; "TIMEOUT"/"OSERROR" -> raise
        self._script = dict(script)
        self.queries: list[str] = []

    async def async_query(self, command: str) -> CollectorAtResponse:
        self.queries.append(command)
        behavior = self._script.get(command, "")
        if behavior == "TIMEOUT":
            raise asyncio.TimeoutError()
        if behavior == "OSERROR":
            raise OSError("socket down")
        return CollectorAtResponse(command=command, value=str(behavior), raw=f"AT+{command}:{behavior}")


class _FramedTransport:
    """Framed FC transport whose per-parameter behavior is scripted."""

    def __init__(self, responses: dict[int, bytes], *, raise_on: dict[int, Exception] | None = None) -> None:
        self._responses = dict(responses)
        self._raise_on = dict(raise_on or {})

    async def async_send_collector(self, *, fcode, payload=b"", devcode=0, collector_addr=1):
        parameter = payload[0]
        if parameter in self._raise_on:
            raise self._raise_on[parameter]
        if parameter in self._responses:
            return (None, self._responses[parameter])
        # well-formed "unsupported" -> non-zero code
        return (None, bytes((1, parameter)))


# --- service route helpers ---------------------------------------------------


def _reader(values=None, *, outcome=None, calls=None, raise_exc=None):
    values = dict(values or {})
    resolved = outcome if outcome is not None else (OUTCOME_SUCCESS if values else OUTCOME_EMPTY)

    async def _read():
        if calls is not None:
            calls.append(1)
        if raise_exc is not None:
            raise raise_exc
        return CollectorMetadataChannelReadResult(
            values=values if resolved in (OUTCOME_SUCCESS, OUTCOME_PARTIAL) else {},
            outcome=resolved,
        )

    return _read


def _framed_route(reader, *, generation=0):
    return CollectorMetadataRoute(
        channel_id=FRAMED_METADATA_CHANNEL, reader=reader, provenance="live", generation=generation
    )


def _at_route(reader, *, generation=0):
    return CollectorMetadataRoute(
        channel_id=AT_METADATA_CHANNEL, reader=reader, provenance="live", generation=generation
    )


def _bootstrap_route(reader, *, generation=0):
    return CollectorMetadataRoute(
        channel_id=FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
        reader=reader,
        provenance="live",
        generation=generation,
        is_bootstrap=True,
    )


def _routeset(*, framed=None, at=None, bootstrap=None, generation=0, provenance="live", identity=""):
    return CollectorMetadataRouteSet(
        generation=generation,
        provenance=provenance,
        identity=identity,
        framed=framed,
        at=at,
        bootstrap=bootstrap,
    )


# --- production reader outcome tests (item 2) --------------------------------


class AtReaderOutcomeTests(unittest.TestCase):
    def test_first_command_timeout_is_transport_error_no_values(self) -> None:
        transport = _ProgrammableAtTransport({"DTUPN": "TIMEOUT"})
        result = asyncio.run(read_runtime_collector_at_values(transport))
        self.assertEqual(result.outcome, OUTCOME_TRANSPORT_ERROR)
        self.assertTrue(result.timed_out)
        self.assertFalse(result.has_values)
        self.assertFalse(result.is_strike)
        self.assertEqual(result.safe_error_code, "at_response_timeout")
        # The sweep ended on the first timeout (not twelve).
        self.assertEqual(len(transport.queries), 1)

    def test_partial_then_timeout_is_partial_fresh_no_strike(self) -> None:
        transport = _ProgrammableAtTransport({"DTUPN": "E123", "ATVER": "TIMEOUT"})
        result = asyncio.run(read_runtime_collector_at_values(transport))
        self.assertEqual(result.outcome, OUTCOME_PARTIAL)
        self.assertTrue(result.is_fresh)
        self.assertFalse(result.is_strike)
        self.assertEqual(result.values.get("collector_pn"), "E123")

    def test_disconnect_is_transport_error_no_strike(self) -> None:
        transport = _ProgrammableAtTransport({"DTUPN": "OSERROR"})
        result = asyncio.run(read_runtime_collector_at_values(transport))
        self.assertEqual(result.outcome, OUTCOME_TRANSPORT_ERROR)
        self.assertFalse(result.is_strike)
        self.assertEqual(result.safe_error_code, "OSError")

    def test_all_answered_blank_is_empty_strike(self) -> None:
        transport = _ProgrammableAtTransport({})  # every command -> blank value
        result = asyncio.run(read_runtime_collector_at_values(transport))
        self.assertEqual(result.outcome, OUTCOME_EMPTY)
        self.assertTrue(result.is_strike)
        self.assertFalse(result.has_values)
        self.assertGreater(len(transport.queries), 1)  # whole sweep delivered

    def test_real_values_are_success(self) -> None:
        transport = _ProgrammableAtTransport({"DTUPN": "E123", "ATVER": "2.05"})
        result = asyncio.run(read_runtime_collector_at_values(transport))
        self.assertEqual(result.outcome, OUTCOME_SUCCESS)
        self.assertTrue(result.is_fresh)
        self.assertEqual(result.values.get("collector_pn"), "E123")

class FramedReaderOutcomeTests(unittest.TestCase):
    def test_delivery_failure_is_transport_error(self) -> None:
        transport = _FramedTransport({}, raise_on={2: OSError("down"), 5: OSError("down")})
        result = asyncio.run(async_read_framed_metadata(transport))
        self.assertEqual(result.outcome, OUTCOME_TRANSPORT_ERROR)
        self.assertFalse(result.has_values)

    def test_partial_success_when_some_answer(self) -> None:
        transport = _FramedTransport(
            {6: b"\x00\x06esp-collector/0.1.5/ESP32"},
            raise_on={2: TimeoutError()},
        )
        result = asyncio.run(async_read_framed_metadata(transport))
        self.assertEqual(result.outcome, OUTCOME_PARTIAL)
        self.assertEqual(result.values.get("collector_hardware_version"), "esp-collector/0.1.5/ESP32")

    def test_all_answered_is_success(self) -> None:
        transport = _FramedTransport({6: b"\x00\x06esp-collector/0.1.5/ESP32"})
        result = asyncio.run(async_read_framed_metadata(transport))
        self.assertEqual(result.outcome, OUTCOME_SUCCESS)


class MetadataHealthModelTests(unittest.TestCase):
    def test_empty_strikes_then_dead_then_revived(self) -> None:
        health = CollectorMetadataHealth()
        for _ in range(UNSUPPORTED_METADATA_CHANNEL_STRIKES - 1):
            health.record_empty(AT_METADATA_CHANNEL)
            self.assertFalse(health.is_dead(AT_METADATA_CHANNEL))
        health.record_empty(AT_METADATA_CHANNEL)
        self.assertTrue(health.is_dead(AT_METADATA_CHANNEL))
        health.record_alive(AT_METADATA_CHANNEL)
        self.assertFalse(health.is_dead(AT_METADATA_CHANNEL))

    def test_seed_and_clear(self) -> None:
        health = CollectorMetadataHealth()
        health.seed_dead((AT_METADATA_CHANNEL,))
        self.assertTrue(health.is_dead(AT_METADATA_CHANNEL))
        self.assertEqual(
            health.failure_count(AT_METADATA_CHANNEL),
            UNSUPPORTED_METADATA_CHANNEL_STRIKES,
        )
        self.assertEqual(health.dead_channels(), (AT_METADATA_CHANNEL,))
        health.clear()
        self.assertFalse(health.is_dead(AT_METADATA_CHANNEL))


# --- service tests -----------------------------------------------------------


class CadenceAndMergeTests(unittest.TestCase):
    def test_framed_cadence_skips_within_interval(self) -> None:
        async def _run():
            calls = []
            service = CollectorMetadataService()
            routes = _routeset(framed=_framed_route(_reader({"a": 1}, calls=calls)))
            await service.async_refresh(routes, poll_interval=10.0)
            first = len(calls)
            result = await service.async_refresh(routes, poll_interval=10.0)
            return first, len(calls), result

        first, second, result = asyncio.run(_run())
        self.assertEqual(first, 1)
        self.assertEqual(second, 1)
        self.assertIn(FRAMED_METADATA_CHANNEL, result.used_cached_channels)

    def test_dual_channel_merge_at_overrides_framed(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            routes = _routeset(
                framed=_framed_route(_reader({"collector_server_endpoint": "fc", "fc_only": 1})),
                at=_at_route(_reader({"collector_server_endpoint": "at", "at_only": 2})),
            )
            return await service.async_refresh(routes, poll_interval=10.0)

        result = asyncio.run(_run())
        self.assertEqual(result.merged_values["collector_server_endpoint"], "at")
        self.assertEqual(result.merged_values["fc_only"], 1)
        self.assertEqual(result.merged_values["at_only"], 2)

    def test_force_liveness_reReads_framed(self) -> None:
        async def _run():
            calls = []
            service = CollectorMetadataService()
            routes = _routeset(framed=_framed_route(_reader({"a": 1}, calls=calls)))
            await service.async_refresh(routes, poll_interval=10.0)
            await service.async_refresh(routes, poll_interval=10.0, force_liveness=True)
            return len(calls)

        self.assertEqual(asyncio.run(_run()), 2)

    def test_invalidate_forces_reRead(self) -> None:
        async def _run():
            calls = []
            service = CollectorMetadataService()
            routes = _routeset(framed=_framed_route(_reader({"a": 1}, calls=calls)))
            await service.async_refresh(routes, poll_interval=10.0)
            service.invalidate()
            await service.async_refresh(routes, poll_interval=10.0)
            return len(calls)

        self.assertEqual(asyncio.run(_run()), 2)


class BootstrapTests(unittest.TestCase):
    def test_bootstrap_reads_identity_then_skips_once_known(self) -> None:
        async def _run():
            calls = []
            service = CollectorMetadataService()
            routes = _routeset(
                bootstrap=_bootstrap_route(
                    _reader({"collector_hardware_version": "esp-collector/0.1.5/ESP32"}, calls=calls)
                ),
            )
            result = await service.async_refresh(routes, poll_interval=10.0)
            first = len(calls)
            service._framed_bootstrap_last_attempt = -1000.0
            await service.async_refresh(routes, poll_interval=10.0)
            return first, len(calls), result

        first, second, result = asyncio.run(_run())
        self.assertEqual(first, 1)
        self.assertEqual(second, 1)
        self.assertEqual(
            result.merged_values["collector_hardware_version"], "esp-collector/0.1.5/ESP32"
        )


class DeadChannelTests(unittest.TestCase):
    def test_at_channel_learned_dead_on_empty_then_revived(self) -> None:
        async def _run():
            calls = []
            service = CollectorMetadataService()
            routes = _routeset(at=_at_route(_reader({}, outcome=OUTCOME_EMPTY, calls=calls)))
            for _ in range(UNSUPPORTED_METADATA_CHANNEL_STRIKES):
                service.at_last_attempt_monotonic = -1000.0
                service.dirty = True
                await service.async_refresh(routes, poll_interval=10.0)
            learned = len(calls)
            # Dead: skipped even with cadence forced open.
            service.at_last_attempt_monotonic = -1000.0
            service.dirty = True
            result = await service.async_refresh(routes, poll_interval=10.0)
            after_dead = len(calls)
            # Explicit recheck revives.
            service.clear_channel_health()
            service.at_last_attempt_monotonic = -1000.0
            service.dirty = True
            await service.async_refresh(routes, poll_interval=10.0)
            after_revive = len(calls)
            return learned, after_dead, after_revive, result

        learned, after_dead, after_revive, result = asyncio.run(_run())
        self.assertEqual(learned, UNSUPPORTED_METADATA_CHANNEL_STRIKES)
        self.assertEqual(after_dead, learned)
        self.assertEqual(result.channel_status[AT_METADATA_CHANNEL], STATUS_SKIPPED_DEAD)
        self.assertEqual(after_revive, learned + 1)

    def test_transport_error_preserves_cache_and_stages_no_strike(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            good = _routeset(at=_at_route(_reader({"collector_link_status": "up"})))
            await service.async_refresh(good, poll_interval=10.0)
            service.at_last_attempt_monotonic = -1000.0
            service.dirty = True
            bad = _routeset(at=_at_route(_reader({}, outcome=OUTCOME_TRANSPORT_ERROR)))
            result = await service.async_refresh(bad, poll_interval=10.0)
            return service.merged_values(), result, service.at_channel_disabled()

        merged, result, disabled = asyncio.run(_run())
        self.assertEqual(merged.get("collector_link_status"), "up")
        self.assertEqual(result.channel_status[AT_METADATA_CHANNEL], STATUS_TRANSPORT_ERROR)
        self.assertFalse(disabled)

    def test_command_error_stages_no_strike(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            routes = _routeset(at=_at_route(_reader({}, outcome=OUTCOME_COMMAND_ERROR)))
            for _ in range(UNSUPPORTED_METADATA_CHANNEL_STRIKES + 1):
                service.at_last_attempt_monotonic = -1000.0
                service.dirty = True
                await service.async_refresh(routes, poll_interval=10.0)
            return service.at_channel_disabled()

        self.assertFalse(asyncio.run(_run()))

    def test_reader_raise_is_treated_as_transport_error(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            routes = _routeset(at=_at_route(_reader({}, raise_exc=ConnectionError("boom"))))
            result = await service.async_refresh(routes, poll_interval=10.0)
            return result, service.at_channel_disabled()

        result, disabled = asyncio.run(_run())
        self.assertEqual(result.channel_status[AT_METADATA_CHANNEL], STATUS_TRANSPORT_ERROR)
        self.assertFalse(disabled)


class GenerationGuardTests(unittest.TestCase):
    def test_stale_generation_postflight_discards_result(self) -> None:
        async def _run():
            generation = {"value": 1}
            service = CollectorMetadataService(generation_provider=lambda: generation["value"])

            async def _slow():
                generation["value"] = 2
                return CollectorMetadataChannelReadResult(
                    values={"collector_server_endpoint": "stale"}, outcome=OUTCOME_SUCCESS
                )

            routes = _routeset(framed=_framed_route(_slow, generation=1), generation=1)
            result = await service.async_refresh(routes, poll_interval=10.0)
            return result, service.merged_values()

        result, merged = asyncio.run(_run())
        self.assertEqual(result.channel_status[FRAMED_METADATA_CHANNEL], STATUS_STALE_GENERATION)
        self.assertNotIn("collector_server_endpoint", merged)

    def test_generation_preflight_skips_second_reader(self) -> None:
        async def _run():
            generation = {"value": 5}
            service = CollectorMetadataService(generation_provider=lambda: generation["value"])
            framed_calls = []
            at_calls = []

            async def _framed():
                framed_calls.append(1)
                generation["value"] = 6  # session moves on after the framed read
                return CollectorMetadataChannelReadResult(values={"a": 1}, outcome=OUTCOME_SUCCESS)

            routes = _routeset(
                framed=_framed_route(_framed, generation=5),
                at=_at_route(_reader({"b": 2}, calls=at_calls)),
                generation=5,
            )
            result = await service.async_refresh(routes, poll_interval=10.0)
            return framed_calls, at_calls, result

        framed_calls, at_calls, result = asyncio.run(_run())
        self.assertEqual(len(framed_calls), 1)
        self.assertEqual(at_calls, [])  # stale route never queried
        self.assertEqual(result.channel_status[AT_METADATA_CHANNEL], STATUS_STALE_GENERATION)


class IdentityAwareCacheTests(unittest.TestCase):
    def test_same_pn_reconnect_preserves_cache(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            r1 = _routeset(framed=_framed_route(_reader({"x": 1})), identity="PN-FULL-1")
            await service.async_refresh(r1, poll_interval=10.0)
            # reconnect, same PN, cadence not due -> cache preserved
            r2 = _routeset(framed=_framed_route(_reader({"x": 2})), identity="PN-FULL-1")
            result = await service.async_refresh(r2, poll_interval=10.0)
            return result.merged_values

        merged = asyncio.run(_run())
        self.assertEqual(merged.get("x"), 1)  # cached, not re-read

    def test_short_to_full_pn_enrichment_preserves_cache(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            # Synthetic short PN (exactly the 10-char prefix-match minimum) that is
            # a prefix of the fuller allowlisted synthetic PN read later.
            r1 = _routeset(framed=_framed_route(_reader({"x": 1})), identity="V000000000")
            await service.async_refresh(r1, poll_interval=10.0)
            short_identity = service.identity
            r2 = _routeset(framed=_framed_route(_reader({"x": 2})), identity="V0000000000001")
            result = await service.async_refresh(r2, poll_interval=10.0)
            return short_identity, service.identity, result.merged_values

        short_identity, full_identity, merged = asyncio.run(_run())
        self.assertEqual(merged.get("x"), 1)  # cache preserved across enrichment
        # identity enriched to the fuller PN (not cleared)
        self.assertTrue(full_identity)

    def test_different_pn_invalidates_cache_and_health(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            r1 = _routeset(
                framed=_framed_route(_reader({"x": 1})),
                at=_at_route(_reader({}, outcome=OUTCOME_EMPTY)),
                identity="PN-DEVICE-A",
            )
            # drive AT dead for device A
            for _ in range(UNSUPPORTED_METADATA_CHANNEL_STRIKES):
                service.at_last_attempt_monotonic = -1000.0
                service.dirty = True
                await service.async_refresh(r1, poll_interval=10.0)
            was_dead = service.at_channel_disabled()
            # device swap: different durable PN
            r2 = _routeset(framed=_framed_route(_reader({"y": 9})), identity="PN-DEVICE-B")
            result = await service.async_refresh(r2, poll_interval=10.0)
            return was_dead, service.at_channel_disabled(), result.merged_values

        was_dead, still_dead, merged = asyncio.run(_run())
        self.assertTrue(was_dead)
        self.assertFalse(still_dead)  # health cleared on identity change
        self.assertNotIn("x", merged)  # device A's values gone
        self.assertEqual(merged.get("y"), 9)

    def test_provisional_pn_less_session_does_not_overwrite_durable_cache(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            r1 = _routeset(framed=_framed_route(_reader({"x": 1})), identity="PN-DURABLE")
            await service.async_refresh(r1, poll_interval=10.0)
            # a PN-less provisional session must not overwrite the durable cache
            service.dirty = True
            r2 = _routeset(framed=_framed_route(_reader({"x": 999})), identity="")
            result = await service.async_refresh(r2, poll_interval=10.0)
            return result.merged_values, service.identity

        merged, identity = asyncio.run(_run())
        self.assertEqual(merged.get("x"), 1)  # durable cache preserved
        self.assertEqual(identity, "PN-DURABLE")

    def test_conflict_does_not_publish_cache_as_new_device_metadata(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            r1 = _routeset(framed=_framed_route(_reader({"x": 1})), identity="PN-DURABLE")
            await service.async_refresh(r1, poll_interval=10.0)
            conflict = CollectorMetadataRouteSet(provenance="conflict", identity="")
            result = await service.async_refresh(conflict, poll_interval=10.0)
            # cache preserved internally, but not published in the conflict result
            return result.merged_values, service.merged_values()

        published, internal = asyncio.run(_run())
        self.assertEqual(published, {})
        self.assertEqual(internal.get("x"), 1)


class AuthoritativeOverlayTests(unittest.TestCase):
    def test_overlay_survives_next_cadence_gated_sweep(self) -> None:
        async def _run():
            calls = []
            service = CollectorMetadataService()
            routes = _routeset(
                framed=_framed_route(_reader({"collector_server_endpoint": "swept"}, calls=calls))
            )
            await service.async_refresh(routes, poll_interval=10.0)
            service.apply_authoritative_values({"collector_server_endpoint": "written"})
            result = await service.async_refresh(routes, poll_interval=10.0)
            return result.merged_values["collector_server_endpoint"], len(calls)

        endpoint, calls = asyncio.run(_run())
        self.assertEqual(endpoint, "written")
        self.assertEqual(calls, 1)


class DiagnosticsTests(unittest.TestCase):
    def test_diagnostics_report_counts_and_dead_channels_without_secrets(self) -> None:
        async def _run():
            service = CollectorMetadataService()
            service.seed_dead_channels((AT_METADATA_CHANNEL,))
            routes = _routeset(
                framed=_framed_route(_reader({"collector_server_endpoint": "secret"}), generation=3),
                at=_at_route(_reader({}, outcome=OUTCOME_EMPTY), generation=3),
                generation=3,
                identity="PN-X",
            )
            await service.async_refresh(routes, poll_interval=10.0)
            return service.diagnostics(routes)

        diag = asyncio.run(_run())
        self.assertEqual(diag["route_provenance"], "live")
        self.assertEqual(diag["session_generation"], 3)
        self.assertTrue(diag["identity_known"])
        dead = {entry["channel_id"] for entry in diag["dead_channels"]}
        self.assertIn(AT_METADATA_CHANNEL, dead)
        for entry in diag["dead_channels"]:
            self.assertIn("threshold", entry)
            self.assertIn("consecutive_failures", entry)
        framed_row = next(r for r in diag["routes"] if r["channel_id"] == FRAMED_METADATA_CHANNEL)
        self.assertIn("attempted_commands", framed_row)
        self.assertIn("outcome", framed_row)
        # No secrets anywhere in the diagnostics payload.
        self.assertNotIn("secret", str(diag))


class RouteBuilderTests(unittest.TestCase):
    def test_builder_omits_bootstrap_when_framed_present(self) -> None:
        routes = build_collector_metadata_routes(
            framed_transport=object(), at_transport=object(), bootstrap_transport=object()
        )
        self.assertIsNotNone(routes.framed)
        self.assertIsNotNone(routes.at)
        self.assertIsNone(routes.bootstrap)

    def test_builder_offers_bootstrap_only_without_framed(self) -> None:
        routes = build_collector_metadata_routes(
            at_transport=object(), bootstrap_transport=object(), identity="PN"
        )
        self.assertIsNone(routes.framed)
        self.assertIsNotNone(routes.bootstrap)
        self.assertEqual(routes.identity, "PN")


if __name__ == "__main__":
    unittest.main()
