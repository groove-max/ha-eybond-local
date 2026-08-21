"""Polling and runtime-refresh lifecycle for EybondLocalCoordinator."""

from __future__ import annotations

import asyncio
from datetime import timedelta
import logging
import math

from homeassistant.components import persistent_notification

from ...connection.connection_policy import collector_identity_binding_required
from ...const import (
    CONF_CONNECTION_TYPE,
    CONF_DRIVER_HINT,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_MODE,
    DOMAIN,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from ...drivers.registry import poll_policy_for_driver_key
from ...models import RuntimeSnapshot
from .poll_projection import (
    COLLECTOR_POLL_CONTEXT_RUNTIME as _COLLECTOR_POLL_CONTEXT_RUNTIME,
    POLL_FIXED_RATE_MIN_DELAY_SECONDS as _POLL_FIXED_RATE_MIN_DELAY_SECONDS,
    POLL_NOTIFICATION_COOLDOWN_SECONDS as _POLL_NOTIFICATION_COOLDOWN_SECONDS,
    POLL_OVERRUN_RATIO as _POLL_OVERRUN_RATIO,
    POLL_STABLE_STREAK_THRESHOLD as _POLL_STABLE_STREAK_THRESHOLD,
    POLL_UTILIZATION_WARNING_RATIO as _POLL_UTILIZATION_WARNING_RATIO,
    RUNTIME_DRIVER_STATE_DRIVER_BOUND as _RUNTIME_DRIVER_STATE_DRIVER_BOUND,
    clamp_poll_interval_seconds as _clamp_poll_interval_seconds,
    is_clean_runtime_poll_cycle as _is_clean_runtime_poll_cycle,
    poll_context_for_runtime_driver_state as _poll_context_for_runtime_driver_state,
    poll_non_runtime_retry_interval_seconds as _poll_non_runtime_retry_interval_seconds,
    poll_recommended_interval_seconds as _poll_recommended_interval_seconds,
    runtime_driver_state_from_snapshot as _runtime_driver_state_from_snapshot,
    snapshot_reconnect_count as _snapshot_reconnect_count,
)
from .tooling_projection import localized_runtime_text as _localized_runtime_text
from ..poll_scheduler import PollDecision, PollScheduler, clamp_interval, normalize_poll_mode

logger = logging.getLogger(__name__)

_HIDDEN_HA_ONLY_COLLECTOR_VALUE_KEYS: frozenset[str] = frozenset(
    {"collector_udp_reply", "collector_udp_reply_from"}
)
_UNSUPPORTED_COMMANDS_OPTION_KEY = "driver_unsupported_commands"
_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY = "driver_unsupported_commands_version"
_UNSUPPORTED_COMMANDS_OPTION_VERSION = 2
_METADATA_DEAD_CHANNELS_OPTION_KEY = "collector_metadata_dead_channels"
_METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY = "collector_metadata_dead_channels_version"
_METADATA_DEAD_CHANNELS_OPTION_VERSION = 1
_LEGACY_METADATA_CHANNEL_PREFIX = "collector:"


class CoordinatorPollingMixin:
    """Own the coordinator polling loop and runtime snapshot publication."""

    def _prune_collector_values_for_connection(self, snapshot: RuntimeSnapshot) -> None:
        """Hide diagnostics that do not apply while the collector routes to HA."""

        if not self.collector_uses_home_assistant_route:
            return
        for key in _HIDDEN_HA_ONLY_COLLECTOR_VALUE_KEYS:
            snapshot.values.pop(key, None)

    async def _async_update_data(self) -> RuntimeSnapshot:
        if getattr(self, "_shutdown_complete", False) and self.data is not None:
            # A refresh queued before shutdown (debounced request, connection
            # watcher, write follow-up) must not drive the stopped link.
            return self.data
        if self._diagnostic_active and self.data is not None:
            # A diagnostic command run holds the shared transport. Skip the live
            # poll so it does not contend on the bus; return the last snapshot.
            return self.data
        async with self._runtime_operation_lock:
            if self._diagnostic_active and self.data is not None:
                return self.data
            self._ensure_poll_scheduler()
            self._configure_poll_scheduler_from_options()
            poll_interval = self._current_poll_cycle_interval_seconds()
            previous_runtime_driver_state = (
                _runtime_driver_state_from_snapshot(self.data)
                if isinstance(self.data, RuntimeSnapshot)
                else ""
            )
            previous_reconnect_count = (
                _snapshot_reconnect_count(self.data)
                if isinstance(self.data, RuntimeSnapshot)
                else 0
            )
            loop = asyncio.get_running_loop()
            cycle_started = loop.time()
            previous_started = float(
                getattr(self, "_poll_last_cycle_started_monotonic", 0.0) or 0.0
            )
            self._poll_last_cycle_started_monotonic = cycle_started
            snapshot = await self._async_update_data_with_runtime_lock(
                poll_interval_seconds=poll_interval
            )
            cycle_duration = max(0.0, loop.time() - cycle_started)
            self._update_poll_scheduler_policy_from_snapshot(snapshot)
            runtime_driver_state = _runtime_driver_state_from_snapshot(snapshot)
            poll_context = _poll_context_for_runtime_driver_state(runtime_driver_state)
            runtime_poll_success = _is_clean_runtime_poll_cycle(
                previous_runtime_driver_state=previous_runtime_driver_state,
                runtime_driver_state=runtime_driver_state,
                previous_reconnect_count=previous_reconnect_count,
                reconnect_count=_snapshot_reconnect_count(snapshot),
            )
            decision = self._poll_scheduler.observe(
                cycle_duration,
                success=runtime_poll_success,
            )
            next_poll_interval = self._next_poll_cycle_interval_seconds(
                current_interval=poll_interval,
                duration_seconds=cycle_duration,
                poll_context=poll_context,
                decision=decision,
            )
            start_interval = (
                max(0.0, cycle_started - previous_started)
                if previous_started > 0.0
                else None
            )
            self._record_poll_cycle_metrics(
                snapshot,
                poll_interval_seconds=poll_interval,
                duration_seconds=cycle_duration,
                start_interval_seconds=start_interval,
                decision=decision,
                runtime_driver_state=runtime_driver_state,
                poll_context=poll_context,
                next_interval_seconds=next_poll_interval,
                clean_runtime_poll=runtime_poll_success,
            )
            self._sync_fixed_rate_poll_update_interval(
                snapshot,
                poll_interval_seconds=next_poll_interval,
                duration_seconds=cycle_duration,
                scheduler_mode=decision.mode,
            )
            self._maybe_persist_unsupported_commands(snapshot)
            self._maybe_persist_metadata_dead_channels()
            return snapshot

    def _configured_poll_interval_seconds(self) -> int:
        config_entry = getattr(self, "config_entry", None)
        options = getattr(config_entry, "options", {}) or {}
        return _clamp_poll_interval_seconds(
            options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )

    def _configured_poll_mode(self) -> str:
        config_entry = getattr(self, "config_entry", None)
        options = getattr(config_entry, "options", {}) or {}
        if CONF_POLL_MODE not in options:
            return POLL_MODE_MANUAL
        return normalize_poll_mode(options.get(CONF_POLL_MODE, DEFAULT_POLL_MODE))

    def _ensure_poll_scheduler(self) -> None:
        if isinstance(getattr(self, "_poll_scheduler", None), PollScheduler):
            return
        config_entry = getattr(self, "config_entry", None)
        options = getattr(config_entry, "options", {}) or {}
        driver_key = str(
            getattr(self, "_poll_scheduler_driver_key", "")
            or options.get(CONF_DRIVER_HINT, "auto")
            or "auto"
        )
        self._poll_scheduler_driver_key = driver_key
        self._poll_scheduler = PollScheduler(
            policy=poll_policy_for_driver_key(
                driver_key, inverter=self._detected_inverter_for_poll_policy()
            ),
            mode=self._configured_poll_mode(),
            manual_interval=self._configured_poll_interval_seconds(),
        )

    def _configure_poll_scheduler_from_options(self) -> None:
        self._ensure_poll_scheduler()
        self._poll_scheduler.configure(
            mode=self._configured_poll_mode(),
            manual_interval=self._configured_poll_interval_seconds(),
        )
        if self._configured_poll_mode() != POLL_MODE_AUTO:
            self._poll_non_runtime_retry_interval_seconds = 0

    def _current_poll_cycle_interval_seconds(self) -> float:
        self._ensure_poll_scheduler()
        scheduler_interval = self._poll_scheduler.current_interval()
        if self._configured_poll_mode() != POLL_MODE_AUTO:
            return scheduler_interval
        retry_interval = float(
            getattr(self, "_poll_non_runtime_retry_interval_seconds", 0.0) or 0.0
        )
        if retry_interval <= 0.0:
            return scheduler_interval
        policy = getattr(self._poll_scheduler, "policy", poll_policy_for_driver_key(""))
        return clamp_interval(
            retry_interval,
            minimum=policy.min_auto_interval,
            maximum=policy.max_auto_interval,
        )

    def _next_poll_cycle_interval_seconds(
        self,
        *,
        current_interval: float,
        duration_seconds: float,
        poll_context: str,
        decision: PollDecision,
    ) -> float:
        if decision.mode != POLL_MODE_AUTO:
            self._poll_non_runtime_retry_interval_seconds = 0
            return decision.effective_interval
        if poll_context == _COLLECTOR_POLL_CONTEXT_RUNTIME:
            self._poll_non_runtime_retry_interval_seconds = 0
            return decision.effective_interval
        retry_interval = _poll_non_runtime_retry_interval_seconds(
            current_interval=current_interval,
            observed_duration=duration_seconds,
            decision=decision,
        )
        self._poll_non_runtime_retry_interval_seconds = retry_interval
        return retry_interval

    def _detected_inverter_for_poll_policy(self):
        """Return the bound detected inverter for model-specific policy, if known.

        A catalog driver may pick a model-specific policy from the detected
        inverter; the runtime just forwards it. ``None`` before identity is known
        (or before the runtime is attached).
        """

        runtime = getattr(self, "_runtime", None)
        return getattr(runtime, "detected_inverter", None)

    def _update_poll_scheduler_policy_from_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return
        driver_key = str(values.get("driver_key") or "").strip()
        if not driver_key:
            return
        # Resolve the policy with the detected inverter (a catalog driver may pick
        # a model-specific policy). Reconfigure when the RESOLVED policy changed,
        # not only when the driver key changed: the same driver can start with a
        # family/default policy (no identity yet) and later resolve a
        # model-specific policy for the SAME driver key once the model is known.
        resolved = poll_policy_for_driver_key(
            driver_key, inverter=self._detected_inverter_for_poll_policy()
        )
        scheduler = getattr(self, "_poll_scheduler", None)
        current_policy = getattr(scheduler, "policy", None)
        if (
            driver_key == getattr(self, "_poll_scheduler_driver_key", "")
            and resolved == current_policy
        ):
            # Same driver AND same resolved policy: nothing to do (avoid a
            # needless reconfigure). configure() preserves duration samples, but
            # skipping it entirely keeps the fast path allocation-free.
            return
        self._poll_scheduler_driver_key = driver_key
        # configure() updates the policy while preserving accumulated observations.
        self._poll_scheduler.configure(policy=resolved)

    def _record_poll_cycle_metrics(
        self,
        snapshot: RuntimeSnapshot,
        *,
        poll_interval_seconds: float,
        duration_seconds: float | None = None,
        start_interval_seconds: float | None = None,
        decision: PollDecision | None = None,
        runtime_driver_state: str | None = None,
        poll_context: str | None = None,
        next_interval_seconds: float | None = None,
        clean_runtime_poll: bool | None = None,
    ) -> None:
        """Publish poll-pipeline utilization and protect against stable overruns."""

        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return
        driver_duration_ms = values.get("collector_poll_duration_ms")
        if duration_seconds is None:
            try:
                duration = max(0.0, float(driver_duration_ms) / 1000.0)
            except (TypeError, ValueError):
                return
        else:
            try:
                duration = max(0.0, float(duration_seconds))
            except (TypeError, ValueError):
                return
        interval = clamp_interval(poll_interval_seconds)

        if not runtime_driver_state:
            runtime_driver_state = _runtime_driver_state_from_snapshot(snapshot)
        if not poll_context:
            poll_context = _poll_context_for_runtime_driver_state(runtime_driver_state)
        runtime_poll = poll_context == _COLLECTOR_POLL_CONTEXT_RUNTIME
        # Cycles that bound the driver or recovered the connection measure that
        # recovery work, not the normal poll cost: keep them out of the duration
        # statistics, the warning streaks, and the scheduler alike.
        clean_poll = runtime_poll if clean_runtime_poll is None else bool(clean_runtime_poll)

        if clean_poll:
            self._poll_duration_max_seconds = max(
                float(getattr(self, "_poll_duration_max_seconds", 0.0) or 0.0),
                duration,
            )
            current_ewma = float(getattr(self, "_poll_duration_ewma_seconds", 0.0) or 0.0)
            if current_ewma <= 0.0:
                self._poll_duration_ewma_seconds = duration
            else:
                self._poll_duration_ewma_seconds = (
                    current_ewma * 0.7 + duration * 0.3
                )
            recent = list(getattr(self, "_poll_recent_durations_seconds", []) or [])
            recent.append(duration)
            self._poll_recent_durations_seconds = recent
            if len(self._poll_recent_durations_seconds) > 20:
                self._poll_recent_durations_seconds = self._poll_recent_durations_seconds[-20:]

        next_interval = (
            clamp_interval(next_interval_seconds)
            if next_interval_seconds is not None
            else (
                clamp_interval(decision.effective_interval)
                if decision is not None
                else interval
            )
        )
        utilization_ratio = duration / float(interval) if interval > 0 else 0.0
        if runtime_poll and clean_poll and utilization_ratio >= _POLL_OVERRUN_RATIO:
            self._collector_poll_overrun_streak = (
                int(getattr(self, "_collector_poll_overrun_streak", 0) or 0) + 1
            )
        else:
            self._collector_poll_overrun_streak = 0

        if runtime_poll and clean_poll and utilization_ratio >= _POLL_UTILIZATION_WARNING_RATIO:
            self._collector_poll_high_utilization_streak = (
                int(getattr(self, "_collector_poll_high_utilization_streak", 0) or 0) + 1
            )
        else:
            self._collector_poll_high_utilization_streak = 0

        recent_peak = max(self._poll_recent_durations_seconds[-5:] or [duration])
        recommended = (
            int(math.ceil(decision.recommended_interval))
            if decision is not None
            else _poll_recommended_interval_seconds(
                current_interval=interval,
                observed_duration=recent_peak,
            )
        )
        if duration_seconds is not None:
            try:
                values["collector_driver_poll_duration_ms"] = int(driver_duration_ms)
            except (TypeError, ValueError):
                values.pop("collector_driver_poll_duration_ms", None)
        if start_interval_seconds is not None:
            values["collector_poll_start_interval_ms"] = int(
                round(max(0.0, start_interval_seconds) * 1000.0)
            )
        values.update(
            {
                "collector_poll_interval_configured_seconds": self._configured_poll_interval_seconds(),
                "collector_poll_manual_interval_seconds": self._configured_poll_interval_seconds(),
                "collector_poll_mode": (
                    decision.mode if decision is not None else self._configured_poll_mode()
                ),
                "collector_poll_policy_driver_key": getattr(
                    self,
                    "_poll_scheduler_driver_key",
                    "",
                ),
                "collector_poll_policy_min_interval_seconds": (
                    decision.policy_min_interval
                    if decision is not None
                    else getattr(
                        getattr(self, "_poll_scheduler", None),
                        "policy",
                        poll_policy_for_driver_key(""),
                    ).min_auto_interval
                ),
                "collector_poll_policy_max_interval_seconds": (
                    decision.policy_max_interval
                    if decision is not None
                    else getattr(
                        getattr(self, "_poll_scheduler", None),
                        "policy",
                        poll_policy_for_driver_key(""),
                    ).max_auto_interval
                ),
                "runtime_driver_state": runtime_driver_state,
                "collector_identity_binding_required": self._identity_binding_required_flag(),
                "collector_poll_context": poll_context,
                "collector_poll_current_interval_seconds": interval,
                "collector_poll_next_interval_seconds": next_interval,
                "collector_poll_target_start_interval_seconds": next_interval,
                "collector_poll_duration_ms": int(round(duration * 1000.0)),
                "collector_poll_duration_avg_ms": int(
                    round(self._poll_duration_ewma_seconds * 1000.0)
                ),
                "collector_poll_duration_max_ms": int(
                    round(self._poll_duration_max_seconds * 1000.0)
                ),
                "collector_poll_utilization_percent": int(round(utilization_ratio * 100.0)),
                "collector_poll_overrun_streak": self._collector_poll_overrun_streak,
                "collector_poll_high_utilization_streak": self._collector_poll_high_utilization_streak,
                "collector_poll_recommended_min_interval_seconds": recommended,
            }
        )
        detection_retry_interval = float(
            getattr(self, "_poll_non_runtime_retry_interval_seconds", 0.0) or 0.0
        )
        if (
            poll_context != _COLLECTOR_POLL_CONTEXT_RUNTIME
            and detection_retry_interval > 0.0
        ):
            values["collector_poll_detection_retry_interval_seconds"] = int(
                math.ceil(detection_retry_interval)
            )
        else:
            values.pop("collector_poll_detection_retry_interval_seconds", None)

        if (
            self._configured_poll_mode() == POLL_MODE_MANUAL
            and runtime_poll
            and self._collector_poll_high_utilization_streak
            >= _POLL_STABLE_STREAK_THRESHOLD
        ):
            self._notify_poll_high_utilization(
                poll_interval=int(math.ceil(interval)),
                recommended_interval=recommended,
                utilization_ratio=utilization_ratio,
            )

        if (
            runtime_poll
            and clean_poll
            and utilization_ratio < _POLL_UTILIZATION_WARNING_RATIO
        ):
            self._poll_normal_utilization_streak = (
                int(getattr(self, "_poll_normal_utilization_streak", 0) or 0) + 1
            )
            if self._poll_normal_utilization_streak >= _POLL_STABLE_STREAK_THRESHOLD:
                self._dismiss_poll_high_utilization_notification()
        elif runtime_poll and clean_poll:
            self._poll_normal_utilization_streak = 0

    def _sync_fixed_rate_poll_update_interval(
        self,
        snapshot: RuntimeSnapshot,
        *,
        poll_interval_seconds: float,
        duration_seconds: float,
        scheduler_mode: str = POLL_MODE_MANUAL,
    ) -> None:
        """Keep configured poll interval as start-to-start target.

        Home Assistant's DataUpdateCoordinator sleeps ``update_interval`` after
        ``_async_update_data`` completes.  Without compensating for the refresh
        duration, a configured 10s poll with a 5s refresh becomes a ~15s
        start-to-start cadence.  Store the configured poll interval as the user
        target and make HA's internal post-refresh delay the remaining time.
        """

        interval = clamp_interval(poll_interval_seconds)
        try:
            duration = max(0.0, float(duration_seconds))
        except (TypeError, ValueError):
            duration = 0.0
        delay = max(_POLL_FIXED_RATE_MIN_DELAY_SECONDS, float(interval) - duration)
        self.update_interval = timedelta(seconds=delay)
        values = getattr(snapshot, "values", None)
        if isinstance(values, dict):
            values["collector_poll_effective_update_delay_ms"] = int(
                round(delay * 1000.0)
            )
            values["collector_poll_effective_update_delay_seconds"] = round(delay, 3)
            values["collector_poll_scheduler_mode"] = "fixed_rate"

    def _poll_high_utilization_notification_id(self) -> str:
        return f"{DOMAIN}_poll_interval_high_utilization_{self.config_entry.entry_id}"

    def _dismiss_poll_high_utilization_notification(self) -> None:
        """Retract the warning once polling is sustainably back within budget."""

        if not getattr(self, "_poll_notification_active", False):
            return
        self._poll_notification_active = False
        try:
            persistent_notification.async_dismiss(
                self.hass,
                self._poll_high_utilization_notification_id(),
            )
        except Exception:
            logger.debug(
                "Failed to dismiss poll high-utilization notification",
                exc_info=True,
            )

    def _notify_poll_high_utilization(
        self,
        *,
        poll_interval: int,
        recommended_interval: int,
        utilization_ratio: float,
    ) -> None:
        self._poll_normal_utilization_streak = 0
        now = asyncio.get_running_loop().time()
        if (
            float(getattr(self, "_poll_last_notification_monotonic", 0.0) or 0.0)
            > 0.0
            and now - float(getattr(self, "_poll_last_notification_monotonic", 0.0) or 0.0)
            < _POLL_NOTIFICATION_COOLDOWN_SECONDS
        ):
            return
        self._poll_last_notification_monotonic = now
        # Marked active only when a notification is actually created, so a
        # later dismiss never targets a notification that was throttled away.
        self._poll_notification_active = True
        persistent_notification.async_create(
            self.hass,
            _localized_runtime_text(
                self.hass,
                "poll_interval_high_utilization_body",
                poll_interval=poll_interval,
                recommended_interval=recommended_interval,
                utilization_percent=int(round(utilization_ratio * 100.0)),
            ),
            title=_localized_runtime_text(
                self.hass,
                "poll_interval_high_utilization_title",
            ),
            notification_id=self._poll_high_utilization_notification_id(),
        )

    async def _async_update_data_with_runtime_lock(
        self,
        *,
        poll_interval_seconds: float | None = None,
    ) -> RuntimeSnapshot:
        """Refresh runtime data while holding the shared transport operation lock."""

        self._ensure_poll_scheduler()
        poll_interval = float(
            poll_interval_seconds
            if poll_interval_seconds is not None
            else self._poll_scheduler.current_interval()
        )
        # Per-phase wall-clock timing: poll cycles have repeatedly turned out
        # to be dominated by phases nobody suspected; the breakdown makes the
        # next "why is the cycle 60s" question a sensor read, not a tcpdump.
        _phase_timings: dict[str, int] = {}
        _loop = asyncio.get_running_loop()

        async def _timed(phase: str, coro):
            phase_started = _loop.time()
            try:
                return await coro
            finally:
                _phase_timings[phase] = _phase_timings.get(phase, 0) + int(
                    round((_loop.time() - phase_started) * 1000.0)
                )

        await _timed("network_reconcile", self._async_reconcile_network(reason="refresh"))
        await _timed(
            "session_profile",
            self._async_reconcile_collector_session_profile(reason="refresh"),
        )
        snapshot = await _timed(
            "runtime_refresh",
            self._runtime.async_refresh(poll_interval=poll_interval),
        )
        if bool(getattr(snapshot, "connected", False)):
            event = getattr(self, "_runtime_connected_event", None)
            if event is not None:
                event.set()
        snapshot = await _timed(
            "snapshot_profile",
            self._async_prepare_runtime_snapshot_profile(snapshot),
        )
        if await _timed(
            "session_profile",
            self._async_reconcile_collector_session_profile(
                reason="post_refresh_profile_discovery"
            ),
        ):
            snapshot = await _timed(
                "runtime_refresh",
                self._runtime.async_refresh(poll_interval=poll_interval),
            )
            if bool(getattr(snapshot, "connected", False)):
                event = getattr(self, "_runtime_connected_event", None)
                if event is not None:
                    event.set()
            snapshot = await _timed(
                "snapshot_profile",
                self._async_prepare_runtime_snapshot_profile(snapshot),
            )
        await _timed(
            "endpoint_reconcile",
            self._async_reconcile_managed_collector_endpoint(snapshot),
        )
        # Persist the confirmed live wire (durable, PN-validated) so a same-PN
        # restart can bootstrap it. Pure entry-data write; no reload.
        self._persist_confirmed_session_protocol_from_runtime()
        snapshot.values["collector_poll_phase_breakdown"] = ", ".join(
            f"{phase}={elapsed_ms}ms"
            for phase, elapsed_ms in sorted(
                _phase_timings.items(), key=lambda item: -item[1]
            )
        )
        snapshot.values["connection_type"] = self.config_entry.data.get(CONF_CONNECTION_TYPE, "eybond")
        snapshot.values["collector_operation_mode"] = self.collector_operation_mode
        snapshot.values["detection_confidence"] = self.detection_confidence
        snapshot.values["control_mode"] = self.control_mode
        snapshot.values["controls_enabled"] = self.controls_enabled
        snapshot.values["control_policy_reason"] = self.controls_reason
        snapshot.values["control_policy_summary"] = self.controls_summary
        write_exposure_context = self._write_exposure_context()
        snapshot.values["effective_variant_key"] = write_exposure_context["variant_key"]
        snapshot.values["effective_profile_name"] = write_exposure_context["profile_name"]
        snapshot.values["effective_profile_source_scope"] = write_exposure_context[
            "profile_source_scope"
        ]
        snapshot.values["effective_schema_source_scope"] = write_exposure_context[
            "schema_source_scope"
        ]
        snapshot.values["effective_device_scoped_overlay_active"] = write_exposure_context[
            "device_scoped_overlay_active"
        ]
        snapshot.values["effective_device_scoped_overlay_scope"] = write_exposure_context[
            "device_scoped_overlay_scope"
        ]
        # Store as a sorted list, not the raw frozenset: snapshot values are serialized
        # to JSON for the support package, and a frozenset is not JSON-serializable
        # (it raised "Object of type frozenset is not JSON serializable" and blocked
        # every export once a selection existed). The reader accepts list/tuple/set.
        _selected_control_keys = write_exposure_context["selected_control_keys"]
        snapshot.values["effective_device_scoped_overlay_selected_control_keys"] = (
            sorted(_selected_control_keys) if _selected_control_keys is not None else None
        )
        snapshot.values["effective_capabilities_experimental"] = write_exposure_context[
            "effective_capabilities_experimental"
        ]
        # Diagnostics: what the device-overlay merge decided this cycle, and the resulting
        # inverter capability picture. Surfaced into the support package so the merge is
        # observable on-device instead of inferred (the support bundle does not otherwise
        # serialize inverter.capabilities).
        _runtime_inverter = getattr(snapshot, "inverter", None)
        _runtime_capabilities = tuple(getattr(_runtime_inverter, "capabilities", ()) or ())
        _learned_capability_keys = sorted(
            str(getattr(capability, "key", ""))
            for capability in _runtime_capabilities
            if getattr(capability, "is_device_scoped_experimental", False)
        )
        if _selected_control_keys is None:
            _exposed_learned_capability_keys = (
                _learned_capability_keys
                if write_exposure_context["device_scoped_overlay_active"]
                else []
            )
        else:
            _exposed_learned_capability_keys = [
                key for key in _learned_capability_keys if key in _selected_control_keys
            ]
        snapshot.values["effective_overlay_merge_status"] = self._device_overlay_merge_status
        snapshot.values["effective_inverter_capability_count"] = len(_runtime_capabilities)
        snapshot.values["effective_inverter_all_learned_capability_keys"] = _learned_capability_keys
        snapshot.values["effective_inverter_exposed_learned_capability_keys"] = (
            _exposed_learned_capability_keys
        )
        snapshot.values["effective_inverter_learned_capability_keys"] = _learned_capability_keys
        snapshot.values.update(self._support_workflow_values(snapshot))
        snapshot.values.update(self._collector_onboarding_values(snapshot))
        snapshot.values.update(self._tooling_values)
        snapshot.values.update(await self._proxy_capture_values(snapshot))
        self._prune_collector_values_for_connection(snapshot)
        from ...integration_sensor_precision import (
            _async_self_heal_sensor_display_precision,
        )

        await _async_self_heal_sensor_display_precision(self.hass, self.config_entry)
        self._sync_inverter_protocol_ambiguity_notification()
        self.async_sync_device_registry(snapshot)
        return snapshot

    def _sync_inverter_protocol_ambiguity_notification(self) -> None:
        """Tell the user when runtime deliberately remains collector-only."""

        notification_id = (
            f"{DOMAIN}_inverter_protocol_ambiguous_{self.config_entry.entry_id}"
        )
        candidates = self.inverter_protocol_candidates
        if len(candidates) > 1:
            self._inverter_protocol_notification_active = True
            persistent_notification.async_create(
                self.hass,
                _localized_runtime_text(
                    self.hass,
                    "inverter_protocol_ambiguous_body",
                    count=len(candidates),
                ),
                title=_localized_runtime_text(
                    self.hass,
                    "inverter_protocol_ambiguous_title",
                ),
                notification_id=notification_id,
            )
            return
        if not getattr(self, "_inverter_protocol_notification_active", False):
            return
        self._inverter_protocol_notification_active = False
        persistent_notification.async_dismiss(self.hass, notification_id)

    async def _async_prepare_runtime_snapshot_profile(
        self,
        snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """Persist runtime-learned profile facts before transport-profile reconcile."""

        snapshot = await self._async_reconcile_proxy_capture_session(snapshot)
        snapshot = await self._async_reconcile_shadow_learning_session(snapshot)
        await self._async_restore_collector_original_endpoint_from_registry(snapshot)
        await self._async_remember_collector_server_endpoint(snapshot)
        await self._async_remember_runtime_identity(snapshot)
        # Keep self.data aligned with the fresh snapshot before helpers that
        # inspect coordinator state instead of the local snapshot argument.
        self.data = snapshot
        self._sync_collector_capability_profile()
        self._configure_reverse_discovery_mode()
        await self._async_warm_effective_metadata_cache()
        collector_cloud_family = self.collector_cloud_family
        if collector_cloud_family:
            snapshot.values["collector_cloud_family"] = collector_cloud_family
        collector_cloud_profile = self.collector_cloud_profile
        if collector_cloud_profile.known:
            snapshot.set_collector_cloud_profile(collector_cloud_profile)
        return snapshot

    def _maybe_persist_unsupported_commands(self, snapshot: RuntimeSnapshot) -> None:
        """Persist the empirically learned unsupported-command set on change."""

        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return
        raw = values.get("driver_unsupported_commands")
        if not isinstance(raw, str) or not raw.strip():
            return
        commands = sorted(
            command.strip()
            for command in raw.split(",")
            if command.strip()
        )
        stored = self.config_entry.options.get(_UNSUPPORTED_COMMANDS_OPTION_KEY)
        stored_version = self.config_entry.options.get(_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY)
        if (
            stored_version == _UNSUPPORTED_COMMANDS_OPTION_VERSION
            and isinstance(stored, (list, tuple))
            and sorted(stored) == commands
        ):
            return
        set_unsupported = getattr(
            self._runtime,
            "set_persistent_unsupported_commands",
            None,
        )
        if callable(set_unsupported):
            set_unsupported(tuple(commands))
        options = dict(self.config_entry.options)
        options[_UNSUPPORTED_COMMANDS_OPTION_KEY] = commands
        options[_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY] = (
            _UNSUPPORTED_COMMANDS_OPTION_VERSION
        )
        self._async_update_entry_without_reload(options=options)
        logger.info(
            "Persisted unsupported inverter commands for this device: %s",
            ", ".join(commands),
        )

    def _maybe_persist_metadata_dead_channels(self) -> None:
        """Persist the metadata dead-channel set + migrate legacy driver keys.

        The dead set is read from the runtime (its own metadata health store),
        NOT from the driver negative cache. The same pass performs the one-time
        legacy migration: any ``collector:`` key still riding the driver option
        is stripped from it. Idempotent -- a settled entry produces no write.
        """

        runtime = getattr(self, "_runtime", None)
        getter = getattr(runtime, "collector_metadata_dead_channels", None)
        if not callable(getter):
            return
        try:
            current = sorted(
                {str(channel).strip() for channel in getter() if str(channel).strip()}
            )
        except Exception:  # pragma: no cover - defensive
            return

        options = dict(self.config_entry.options)
        changed = False

        # One-time migration: strip legacy ``collector:`` keys out of the driver
        # negative cache option so metadata verdicts stop riding it.
        stored_driver = options.get(_UNSUPPORTED_COMMANDS_OPTION_KEY)
        if isinstance(stored_driver, (list, tuple)):
            kept = [
                key
                for key in (str(command).strip() for command in stored_driver)
                if key and not key.startswith(_LEGACY_METADATA_CHANNEL_PREFIX)
            ]
            had = [key for key in (str(c).strip() for c in stored_driver) if key]
            if len(kept) != len(had):
                options[_UNSUPPORTED_COMMANDS_OPTION_KEY] = kept
                changed = True

        stored_meta = options.get(_METADATA_DEAD_CHANNELS_OPTION_KEY)
        stored_meta_version = options.get(_METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY)
        already = (
            stored_meta_version == _METADATA_DEAD_CHANNELS_OPTION_VERSION
            and isinstance(stored_meta, (list, tuple))
            and sorted(str(c).strip() for c in stored_meta if str(c).strip()) == current
        )
        if not already:
            if current:
                options[_METADATA_DEAD_CHANNELS_OPTION_KEY] = current
                options[_METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY] = (
                    _METADATA_DEAD_CHANNELS_OPTION_VERSION
                )
                changed = True
            elif (
                _METADATA_DEAD_CHANNELS_OPTION_KEY in options
                or _METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY in options
            ):
                options.pop(_METADATA_DEAD_CHANNELS_OPTION_KEY, None)
                options.pop(_METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY, None)
                changed = True

        if changed:
            self._async_update_entry_without_reload(options=options)
            logger.info(
                "Persisted collector metadata dead channels for this device: %s",
                ", ".join(current) or "(none)",
            )

    async def async_recheck_supported_commands(self) -> None:
        """Forget the learned unsupported-command set and re-probe everything.

        Clears BOTH the inverter command negative cache and the metadata channel
        health (their separate stores), and removes both persisted options.
        """

        clear_cache = getattr(self._runtime, "clear_unsupported_command_cache", None)
        if callable(clear_cache):
            clear_cache()
        options = dict(self.config_entry.options)
        removed_commands = options.pop(_UNSUPPORTED_COMMANDS_OPTION_KEY, None) is not None
        removed_version = (
            options.pop(_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY, None) is not None
        )
        removed_meta = options.pop(_METADATA_DEAD_CHANNELS_OPTION_KEY, None) is not None
        removed_meta_version = (
            options.pop(_METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY, None) is not None
        )
        if removed_commands or removed_version or removed_meta or removed_meta_version:
            self._async_update_entry_without_reload(options=options)
        await self.async_request_refresh()

    def _identity_binding_required_flag(self) -> bool:
        """Whether this entry has no durable collector PN to own a session.

        Surfaced as a live snapshot value so the runtime state agrees with the
        support bundle's ``entry_axis_diagnostics.collector_identity_binding_required``.
        This is a pure DIAGNOSTIC flag: it does NOT feed the poll scheduler or the
        runtime_driver_state that drives polling, so no poll behavior changes.
        """

        try:
            return collector_identity_binding_required(
                self.config_entry.data, self.config_entry.options
            )
        except Exception:  # pragma: no cover - diagnostics must never break polling
            return False

    def _on_collector_connection_established(self, remote_ip: str) -> None:
        """Refresh immediately when the collector dials back in.

        Without this the reconnected collector sits idle until the next
        scheduled poll, which after failed detection cycles can be more than
        a minute away (non-runtime retry backoff).
        """

        if getattr(self, "_shutdown_complete", False):
            return
        event = getattr(self, "_runtime_connected_event", None)
        if event is not None:
            # The watcher belongs to the runtime transport, not passive
            # inventory. Reaching it means the callback socket has actually
            # been activated as a payload connection.
            event.set()
        snapshot = self.data if isinstance(self.data, RuntimeSnapshot) else None
        if snapshot is not None:
            runtime_driver_state = _runtime_driver_state_from_snapshot(snapshot)
            if snapshot.connected and runtime_driver_state == _RUNTIME_DRIVER_STATE_DRIVER_BOUND:
                return
        invalidator = getattr(self._runtime, "invalidate_collector_runtime_values", None)
        if callable(invalidator):
            invalidator()
        logger.debug(
            "Collector connection from %s while not bound; requesting immediate refresh",
            remote_ip,
        )
        self.hass.async_create_task(self.async_request_refresh())

    def _publish_runtime_intermediate_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        """Publish collector state while the runtime is still probing the inverter."""

        if getattr(self, "_shutdown_complete", False):
            return
        snapshot.values["connection_type"] = self.config_entry.data.get(
            CONF_CONNECTION_TYPE,
            "eybond",
        )
        snapshot.values["collector_operation_mode"] = self.collector_operation_mode
        snapshot.values["detection_confidence"] = self.detection_confidence
        snapshot.values["control_mode"] = self.control_mode
        snapshot.values["controls_enabled"] = self.controls_enabled
        snapshot.values["control_policy_reason"] = self.controls_reason
        snapshot.values["control_policy_summary"] = self.controls_summary
        self.data = snapshot
        self.async_set_updated_data(snapshot)



__all__ = ["CoordinatorPollingMixin"]
