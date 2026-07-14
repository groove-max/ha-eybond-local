"""Runtime collector-metadata service: cadence, cache, merge, channel health.

The generic runtime owns WHEN to read collector telemetry and how to combine the
channels; it does NOT own the wire. The wire-specific read contracts live in
``collector.metadata`` (bound to owned transports by the link route authority),
and the FC/AT parameter definitions stay in ``collector.parameter_registry`` /
``collector.at_runtime``.

This service holds:

* the framed cache (FC sweep + param-6 bootstrap + authoritative management
  overlay) and the AT supplemental cache;
* last-refresh / last-attempt timestamps and the dual-channel refresh interval;
* the dirty flag and force-liveness selection;
* per-channel duration + structured outcome;
* its OWN metadata channel health (``CollectorMetadataHealth``) -- deliberately
  separate from the driver's unsupported-command negative cache, so an inverter
  command success never commits an AT-metadata strike and vice versa;
* the durable collector identity (PN) the cache/health belongs to, so a device
  swap invalidates and a short/full enrichment or same-PN reconnect preserves;
* a generation preflight + postflight guard so a stale-route result never
  overwrites a newer session's cache and a stale route is never even queried.

Merge precedence is preserved: AT supplemental values override framed values on
identical keys.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable

from ..collector.metadata import (
    AT_METADATA_CHANNEL,
    FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
    FRAMED_METADATA_CHANNEL,
    CollectorMetadataRoute,
    CollectorMetadataRouteSet,
)
from ..collector.metadata_result import (
    OUTCOME_COMMAND_ERROR,
    OUTCOME_EMPTY,
    OUTCOME_PARTIAL,
    OUTCOME_SUCCESS,
    OUTCOME_TRANSPORT_ERROR,
    CollectorMetadataChannelReadResult,
)
from ..connection.session_registry import pn_is_same_identity, reconcile_pn
from .metadata_health import CollectorMetadataHealth

# Channel status codes surfaced in diagnostics (safe, typed -- never payloads).
STATUS_FRESH = "fresh"
STATUS_CACHED = "cached"
STATUS_EMPTY = "empty"
STATUS_TRANSPORT_ERROR = "transport_error"
STATUS_COMMAND_ERROR = "command_error"
STATUS_SKIPPED_DEAD = "skipped_dead"
STATUS_STALE_GENERATION = "stale_generation"

_MINIMUM_REFRESH_INTERVAL = 30.0
_CONFLICT_PROVENANCE = "conflict"


@dataclass(frozen=True)
class CollectorMetadataRefreshResult:
    """Immutable normalized result of one metadata refresh."""

    merged_values: dict[str, object]
    fresh: bool
    fresh_channels: tuple[str, ...]
    channel_status: dict[str, str]
    channel_duration_ms: dict[str, int]
    used_cached_channels: tuple[str, ...]
    errors: dict[str, str]
    route_provenance: str
    framed_duration_ms: int = 0
    at_duration_ms: int = 0
    partial_channels: tuple[str, ...] = ()


class CollectorMetadataService:
    """Own the cadence/cache/merge/health/identity state for collector telemetry."""

    def __init__(
        self,
        *,
        generation_provider: Callable[[], int] | None = None,
        dead_threshold: int | None = None,
    ) -> None:
        self._generation_provider = generation_provider
        self._health = (
            CollectorMetadataHealth(dead_threshold=dead_threshold)
            if dead_threshold is not None
            else CollectorMetadataHealth()
        )

        self._framed_values: dict[str, object] = {}
        self._at_values: dict[str, object] = {}
        self._framed_last_refresh = 0.0
        self._framed_bootstrap_last_attempt = 0.0
        self._at_last_refresh = 0.0
        self._at_last_attempt = 0.0
        self._dirty = True
        self._last_fresh = False

        # Durable identity (PN) this cache/health belongs to. Never a peer IP.
        self._identity = ""
        self._identity_transitions = 0

        # Diagnostics-only snapshot of the last refresh.
        self._channel_status: dict[str, str] = {}
        self._channel_duration_ms: dict[str, int] = {}
        self._channel_error: dict[str, str] = {}
        self._channel_results: dict[str, CollectorMetadataChannelReadResult] = {}
        self._route_provenance = "unavailable"
        self._last_generation = 0
        self._last_session_id = ""

    # -- state accessors (single source of truth; hub exposes thin delegates) --

    @property
    def framed_values(self) -> dict[str, object]:
        return self._framed_values

    @framed_values.setter
    def framed_values(self, value: dict[str, object]) -> None:
        self._framed_values = dict(value or {})

    @property
    def at_values(self) -> dict[str, object]:
        return self._at_values

    @at_values.setter
    def at_values(self, value: dict[str, object]) -> None:
        self._at_values = dict(value or {})

    @property
    def dirty(self) -> bool:
        return self._dirty

    @dirty.setter
    def dirty(self, value: bool) -> None:
        self._dirty = bool(value)

    @property
    def framed_last_refresh_monotonic(self) -> float:
        return self._framed_last_refresh

    @framed_last_refresh_monotonic.setter
    def framed_last_refresh_monotonic(self, value: float) -> None:
        self._framed_last_refresh = float(value)

    @property
    def at_last_attempt_monotonic(self) -> float:
        return self._at_last_attempt

    @at_last_attempt_monotonic.setter
    def at_last_attempt_monotonic(self, value: float) -> None:
        self._at_last_attempt = float(value)

    @property
    def last_read_fresh(self) -> bool:
        return self._last_fresh

    @property
    def identity(self) -> str:
        return self._identity

    def merged_values(self) -> dict[str, object]:
        """Return framed base metadata merged with AT supplemental (AT wins)."""

        values = dict(self._framed_values)
        values.update(self._at_values)
        return values

    def invalidate(self) -> None:
        """Drop cached values so the next refresh reads them live."""

        self._framed_values.clear()
        self._at_values.clear()
        self._framed_last_refresh = 0.0
        self._framed_bootstrap_last_attempt = 0.0
        self._at_last_refresh = 0.0
        self._at_last_attempt = 0.0
        self._dirty = True

    def apply_authoritative_values(self, mapping: dict[str, object]) -> None:
        """Overlay authoritative values (management action result) into the cache.

        A collector-management action (endpoint write / apply / reboot) knows the
        effective collector state better than a soon-to-run telemetry sweep; the
        result is overlaid into the framed cache and the framed refresh timestamp
        is bumped so the next cadence-gated sweep does not immediately clobber it.
        """

        if not mapping:
            return
        self._framed_values.update(mapping)
        self._framed_last_refresh = self._now()

    # -- metadata channel health (own store; NOT the driver command cache) ----

    def at_channel_disabled(self) -> bool:
        """Return whether the AT metadata channel is empirically known dead."""

        return self._health.is_dead(AT_METADATA_CHANNEL)

    def dead_channels(self) -> tuple[str, ...]:
        """Return the persisted dead-channel set (for config-entry persistence)."""

        return self._health.dead_channels()

    def seed_dead_channels(self, channels: tuple[str, ...] | list[str]) -> None:
        """Seed the persisted dead-channel set (from the config entry)."""

        self._health.seed_dead(channels)

    def clear_channel_health(self) -> None:
        """Explicit recheck: forget the metadata channel health entirely."""

        self._health.clear()

    # -- refresh ---------------------------------------------------------------

    async def async_refresh(
        self,
        routes: CollectorMetadataRouteSet,
        *,
        poll_interval: float | None,
        force_liveness: bool = False,
    ) -> CollectorMetadataRefreshResult:
        """Refresh collector metadata over the routed channels.

        ``force_liveness`` guarantees one real command exchange this call (the
        cheap framed read when a framed channel is routed, otherwise the AT read)
        without forcing the full sweep out of cache.
        """

        now = self._now()
        refresh_interval = max(float(poll_interval or 0.0) * 3.0, _MINIMUM_REFRESH_INTERVAL)
        force_refresh = bool(self._dirty)
        start_generation = int(routes.generation)

        self._channel_status = {}
        self._channel_duration_ms = {}
        self._channel_error = {}
        self._channel_results = {}
        self._route_provenance = routes.provenance
        self._last_generation = start_generation
        self._last_session_id = routes.session_id
        fresh_channels: list[str] = []
        partial_channels: list[str] = []
        used_cached: list[str] = []
        framed_ms = 0
        at_ms = 0

        # A contradictory wire never queries, and the last-known cache is NOT
        # published as this (unknown) device's metadata.
        if routes.provenance == _CONFLICT_PROVENANCE:
            self._dirty = False
            self._last_fresh = False
            return self._build_result(routes, {}, (), (), (), 0, 0)

        # Durable-identity reconciliation. Returns whether reads may write the
        # durable-identity cache (a PN-less provisional session may not overwrite
        # an identified device's cache).
        writable = self._reconcile_identity(routes)
        if not writable:
            # Provisional session for an already-identified device: do not query
            # or overwrite; show the durable device's last-known metadata.
            self._dirty = False
            self._last_fresh = False
            return self._build_result(
                routes, self.merged_values(), (), (), (), 0, 0
            )

        framed_routed = routes.framed is not None
        force_fc = force_refresh or (force_liveness and framed_routed)
        force_at = force_refresh or (force_liveness and not framed_routed)

        # -- framed FC=2 metadata sweep ---------------------------------------
        if routes.framed is not None:
            due = (
                force_fc
                or not self._framed_values
                or now - self._framed_last_refresh >= refresh_interval
            )
            if due:
                result, status, ms = await self._run_channel(routes.framed, start_generation)
                framed_ms = max(framed_ms, ms)
                if status == STATUS_FRESH:
                    self._framed_values = dict(result.values)
                    self._framed_last_refresh = now
                    fresh_channels.append(FRAMED_METADATA_CHANNEL)
                    if result.outcome == OUTCOME_PARTIAL:
                        partial_channels.append(FRAMED_METADATA_CHANNEL)
                self._record_channel(FRAMED_METADATA_CHANNEL, result, status)
            else:
                self._mark_cached(FRAMED_METADATA_CHANNEL, used_cached)

        # -- framed param-6 hardware bootstrap (collector-only, AT wire) ------
        if routes.bootstrap is not None and routes.framed is None:
            bootstrap_due = now - self._framed_bootstrap_last_attempt >= refresh_interval
            identity_missing = "collector_hardware_version" not in self._framed_values
            if identity_missing and (force_refresh or bootstrap_due):
                self._framed_bootstrap_last_attempt = now
                result, status, ms = await self._run_channel(
                    routes.bootstrap, start_generation
                )
                framed_ms = max(framed_ms, ms)
                if status == STATUS_FRESH:
                    self._framed_values.update(result.values)
                    fresh_channels.append(FRAMED_HARDWARE_BOOTSTRAP_CHANNEL)
                    if result.outcome == OUTCOME_PARTIAL:
                        partial_channels.append(FRAMED_HARDWARE_BOOTSTRAP_CHANNEL)
                self._record_channel(FRAMED_HARDWARE_BOOTSTRAP_CHANNEL, result, status)

        # -- AT-text supplemental metadata sweep ------------------------------
        if routes.at is not None:
            if self._health.is_dead(AT_METADATA_CHANNEL):
                self._channel_status[AT_METADATA_CHANNEL] = STATUS_SKIPPED_DEAD
            else:
                at_stale = (
                    not self._at_values
                    or now - self._at_last_refresh >= refresh_interval
                )
                at_attempt_due = now - self._at_last_attempt >= refresh_interval
                if force_at or (at_stale and at_attempt_due):
                    self._at_last_attempt = now
                    result, status, ms = await self._run_channel(
                        routes.at, start_generation
                    )
                    at_ms = max(at_ms, ms)
                    if status == STATUS_FRESH:
                        self._at_values = dict(result.values)
                        self._at_last_refresh = now
                        fresh_channels.append(AT_METADATA_CHANNEL)
                        if result.outcome == OUTCOME_PARTIAL:
                            partial_channels.append(AT_METADATA_CHANNEL)
                        # A live answer resets ONLY this channel's health.
                        self._health.record_alive(AT_METADATA_CHANNEL)
                    elif status == STATUS_EMPTY:
                        # Delivered over a live link, no metadata -> dead-channel
                        # strike. A transport/command error stages NO strike.
                        self._health.record_empty(AT_METADATA_CHANNEL)
                    self._record_channel(AT_METADATA_CHANNEL, result, status)
                else:
                    self._mark_cached(AT_METADATA_CHANNEL, used_cached)

        self._dirty = False
        self._last_fresh = bool(fresh_channels)
        self._channel_duration_ms = {
            channel: ms
            for channel, ms in (
                (FRAMED_METADATA_CHANNEL, framed_ms),
                (AT_METADATA_CHANNEL, at_ms),
            )
            if ms
        }

        return self._build_result(
            routes,
            self.merged_values(),
            tuple(fresh_channels),
            tuple(used_cached),
            tuple(partial_channels),
            framed_ms,
            at_ms,
        )

    def _build_result(
        self,
        routes: CollectorMetadataRouteSet,
        merged: dict[str, object],
        fresh_channels: tuple[str, ...],
        used_cached: tuple[str, ...],
        partial_channels: tuple[str, ...],
        framed_ms: int,
        at_ms: int,
    ) -> CollectorMetadataRefreshResult:
        return CollectorMetadataRefreshResult(
            merged_values=merged,
            fresh=self._last_fresh,
            fresh_channels=fresh_channels,
            channel_status=dict(self._channel_status),
            channel_duration_ms=dict(self._channel_duration_ms),
            used_cached_channels=used_cached,
            errors=dict(self._channel_error),
            route_provenance=routes.provenance,
            framed_duration_ms=framed_ms,
            at_duration_ms=at_ms,
            partial_channels=partial_channels,
        )

    # -- identity --------------------------------------------------------------

    def _reconcile_identity(self, routes: CollectorMetadataRouteSet) -> bool:
        """Reconcile durable identity; return whether reads may write the cache.

        * incoming empty (provisional/PN-less): never changes durable identity;
          writable ONLY if there is no durable identity yet (a fresh collector-
          only bootstrap). If a durable identity is already known, a provisional
          session must not overwrite its cache.
        * incoming == durable (short/full enrichment): keep cache, enrich PN.
        * incoming != durable: device swap -> invalidate caches + clear health,
          adopt the new identity. Old values never appear in the new result.
        """

        incoming = str(routes.identity or "").strip()
        if not incoming:
            return not self._identity
        if not self._identity:
            self._identity = incoming
            return True
        if pn_is_same_identity(self._identity, incoming):
            self._identity = reconcile_pn(self._identity, incoming)
            return True
        # Durable identity change.
        self._identity = incoming
        self._identity_transitions += 1
        self.invalidate()
        self._health.clear()
        return True

    # -- internals -------------------------------------------------------------

    async def _run_channel(
        self,
        route: CollectorMetadataRoute,
        start_generation: int,
    ) -> tuple[CollectorMetadataChannelReadResult, str, int]:
        """Run one channel reader with generation pre/postflight guards.

        Preflight: if the owned session already moved on, the stale route is not
        queried at all. Postflight: a result observed after a generation change
        is discarded so it never overwrites the newer session's cache.
        ``asyncio.CancelledError`` is never swallowed.
        """

        if self._generation_advanced(start_generation):
            return (
                CollectorMetadataChannelReadResult(outcome=OUTCOME_EMPTY),
                STATUS_STALE_GENERATION,
                0,
            )
        started = self._now()
        try:
            result = await route.reader()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - defensive; readers report structured
            ms = self._elapsed_ms(started)
            return (
                CollectorMetadataChannelReadResult.transport_error(type(exc).__name__),
                STATUS_TRANSPORT_ERROR,
                ms,
            )
        ms = self._elapsed_ms(started)
        if self._generation_advanced(start_generation):
            return result, STATUS_STALE_GENERATION, ms
        return result, self._status_for_outcome(result.outcome), ms

    @staticmethod
    def _status_for_outcome(outcome: str) -> str:
        if outcome in (OUTCOME_SUCCESS, OUTCOME_PARTIAL):
            return STATUS_FRESH
        if outcome == OUTCOME_TRANSPORT_ERROR:
            return STATUS_TRANSPORT_ERROR
        if outcome == OUTCOME_COMMAND_ERROR:
            return STATUS_COMMAND_ERROR
        return STATUS_EMPTY

    def _generation_advanced(self, start_generation: int) -> bool:
        if self._generation_provider is None:
            return False
        try:
            return int(self._generation_provider()) != int(start_generation)
        except Exception:  # pragma: no cover - defensive
            return False

    def _record_channel(
        self,
        channel_id: str,
        result: CollectorMetadataChannelReadResult,
        status: str,
    ) -> None:
        self._channel_status[channel_id] = status
        self._channel_results[channel_id] = result
        if result.safe_error_code and status in (
            STATUS_TRANSPORT_ERROR,
            STATUS_COMMAND_ERROR,
        ):
            self._channel_error[channel_id] = result.safe_error_code

    def _mark_cached(self, channel_id: str, used_cached: list[str]) -> None:
        self._channel_status[channel_id] = STATUS_CACHED
        used_cached.append(channel_id)

    # -- diagnostics -----------------------------------------------------------

    def diagnostics(self, routes: CollectorMetadataRouteSet | None = None) -> dict[str, object]:
        """Return non-sensitive metadata diagnostics.

        Never includes endpoint values, Wi-Fi credentials, raw AT payloads, or
        session peer IP -- only channel routing, provenance, generation, session
        ownership presence, per-channel outcome/duration/error-code/command
        counts/partial flag, cache age + key counts, dirty state, per-channel
        consecutive failure count, the dead reason/threshold, and the identity
        transition count.
        """

        now = self._maybe_now()
        route_rows: list[dict[str, object]] = []
        if routes is not None:
            for role, route in (
                ("framed", routes.framed),
                ("at", routes.at),
                ("bootstrap", routes.bootstrap),
            ):
                if route is None:
                    continue
                result = self._channel_results.get(route.channel_id)
                route_rows.append(
                    {
                        "role": role,
                        "channel_id": route.channel_id,
                        "provenance": route.provenance,
                        "session_generation": route.generation,
                        "has_session_id": bool(route.session_id),
                        "status": self._channel_status.get(route.channel_id, ""),
                        "outcome": (result.outcome if result else ""),
                        "duration_ms": self._channel_duration_ms.get(route.channel_id, 0),
                        "error_code": self._channel_error.get(route.channel_id, ""),
                        "attempted_commands": (result.attempted_commands if result else 0),
                        "successful_commands": (result.successful_commands if result else 0),
                        "failed_commands": (result.failed_commands if result else 0),
                        "partial": bool(result and result.outcome == OUTCOME_PARTIAL),
                        "timed_out": bool(result and result.timed_out),
                        "consecutive_failures": self._health.failure_count(route.channel_id),
                    }
                )

        dead_channels: list[dict[str, object]] = [
            {
                "channel_id": channel,
                "reason": "empirically_unanswered",
                "consecutive_failures": self._health.failure_count(channel),
                "threshold": self._health.threshold,
            }
            for channel in self._health.dead_channels()
        ]

        return {
            "routes": route_rows,
            "route_provenance": (routes.provenance if routes else self._route_provenance),
            "session_generation": (routes.generation if routes else self._last_generation),
            "identity_known": bool(self._identity),
            "identity_transitions": self._identity_transitions,
            "refresh": {
                "last_read_fresh": self._last_fresh,
                "channel_status": dict(self._channel_status),
                "channel_duration_ms": dict(self._channel_duration_ms),
                "channel_error": dict(self._channel_error),
            },
            "cache": {
                "dirty": self._dirty,
                "framed_cached_keys": len(self._framed_values),
                "at_cached_keys": len(self._at_values),
                "framed_age_seconds": self._age(now, self._framed_last_refresh),
                "at_age_seconds": self._age(now, self._at_last_refresh),
            },
            "dead_channels": dead_channels,
        }

    @staticmethod
    def _age(now: float | None, stamp: float) -> float | None:
        if now is None or stamp <= 0.0:
            return None
        return max(0.0, round(now - stamp, 3))

    def _elapsed_ms(self, started: float) -> int:
        return int(round((self._now() - started) * 1000.0))

    @staticmethod
    def _now() -> float:
        return asyncio.get_running_loop().time()

    @staticmethod
    def _maybe_now() -> float | None:
        """Return loop time, or ``None`` when called outside a running loop."""

        try:
            return asyncio.get_running_loop().time()
        except RuntimeError:
            return None
