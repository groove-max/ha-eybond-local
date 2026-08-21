"""Extracted EyeBond options-flow lifecycle: ShadowLearningRunMixin."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult

from ...collector.transport import _finish_cleanup_on_cancel
from ..common.presentation import _smartess_credential_schema_fields
from ..common.translation import with_translation_bundle as _with_translation_bundle
from .shared import (
    _BOOLEAN_SELECTOR,
    CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED,
    CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE,
    CONTROL_DISCOVERY_FAILURE_SAFETY_STOP,
)
from ...runtime.shadow_learning_facade import ShadowLearningRuntimeFacade
from ...support.cloud_evidence_providers import resolve_cloud_evidence_provider
from ...support.shadow_learning.overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)

logger = logging.getLogger(__name__)


CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS = 40


class ShadowLearningRunMixin:
    """ShadowLearningRun lifecycle."""

    @_with_translation_bundle
    async def async_step_shadow_learning(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 1: intro and consent.

        Replaces the former technical action dropdown (now
        ``async_step_shadow_learning_advanced``) with one linear user-facing
        workflow: intro/consent -> credentials -> progress -> review -> result.
        No live cloud operation runs until the user gives explicit consent.
        """
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "shadow_learning_title",
                    "Expand support for this device",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        if not (
            (
                self._cloud_tool_new_operations_allowed()
                and self._collector_capabilities().shadow_learning
            )
            or self._shadow_learning_lifecycle_active(coordinator)
        ):
            return await self._async_cloud_tools_unavailable()

        errors: dict[str, str] = {}
        consent = bool(
            (user_input or {}).get("shadow_learning_confirm_cloud_write", False)
        )
        if user_input is not None:
            if consent:
                # Start a fresh wizard pass: drop ALL of the previous run's
                # result state, not just credentials. Otherwise a run that fails
                # early (e.g. a preflight blocker) would still show the prior
                # run's overlay/controls/read counts as if they were its own.
                self._reset_control_discovery_run_state()
                self._shadow_learning_state["wizard_consent"] = True
                return await self.async_step_shadow_learning_credentials()
            errors["shadow_learning_confirm_cloud_write"] = "required"

        return self.async_show_form(
            step_id="shadow_learning",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        "shadow_learning_confirm_cloud_write",
                        default=consent,
                    ): _BOOLEAN_SELECTOR,
                }
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_intro_hint",
                "Home Assistant will briefly sign in to {cloud_provider_label} to "
                "find which settings it can control on this device. Your login is "
                "used only for this check and is not saved.\n\n"
                "⚠️ Before you continue, fully CLOSE the {cloud_app_label} mobile "
                "app. If it stays open it competes with this check for the device "
                "and can disrupt the scan or interfere with the inverter.\n\n"
                "Confirm below to continue.",
            ),
        )

    @_with_translation_bundle
    async def async_step_shadow_learning_credentials(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 2: cloud credentials.

        Asks only for the cloud username/password. Credentials are held in
        transient flow state for the current run only and are never written to
        the config entry or its options.
        """
        coordinator = self._coordinator()
        if coordinator is None or not bool(
            self._shadow_learning_state.get("wizard_consent")
        ):
            # Credentials are unreachable without a coordinator and prior consent.
            return await self.async_step_shadow_learning()

        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        username = str(defaults.get("username") or "").strip()
        password = str(defaults.get("password") or "").strip()
        if user_input is not None:
            if not username:
                errors["username"] = "required"
            if not password:
                errors["password"] = "required"
            if not errors:
                # Transient only — used by the automatic runner (EYB-REF-041)
                # and dropped at the result step; never persisted to the entry.
                self._shadow_learning_state["wizard_credentials"] = {
                    "username": username,
                    "password": password,
                }
                return await self.async_step_shadow_learning_progress()

        return self.async_show_form(
            step_id="shadow_learning_credentials",
            data_schema=vol.Schema(
                _smartess_credential_schema_fields(
                    required=True,
                    username_default=username,
                    password_default="",
                )
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_credentials_hint",
                "Enter your {cloud_provider_label} username and password. They "
                "are used only for this one check and are not saved.",
            ),
        )

    def _set_control_discovery_progress(
        self, fraction: float, stage: str, *, done: int = 0, total: int = 0
    ) -> None:
        """Advance the guided control-discovery progress bar.

        Records the latest fraction and drives the determinate progress bar via
        ``async_update_progress`` when the running Home Assistant core supports it
        (older cores just show the spinner). The progress step label stays static
        — only the bar animates — because re-rendering the dialog to update text
        visibly flickers. ``stage``/``done``/``total`` are accepted for call-site
        readability and future use.
        """

        clamped = max(0.0, min(1.0, float(fraction)))
        self._shadow_learning_state["progress"] = {
            "fraction": clamped,
            "stage": str(stage),
            "done": int(done),
            "total": int(total),
        }
        update = getattr(self, "async_update_progress", None)
        if callable(update):
            with suppress(Exception):
                update(clamped)

    @_with_translation_bundle
    async def async_step_shadow_learning_progress(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 3: automatic progress.

        Shows ``async_show_progress`` while the automatic discovery runner works,
        rendering a determinate progress bar (on cores that support it) and a
        stage-specific label that updates as the runner advances, then moves on
        to the review screen.
        """
        coordinator = self._coordinator()
        if coordinator is None or not self._shadow_learning_state.get(
            "wizard_credentials"
        ):
            # Progress is unreachable until consent + credentials are gathered.
            return await self.async_step_shadow_learning()

        pipeline = self._shadow_learning_state.get("wizard_progress_task")
        if pipeline is None:
            # Fresh run: start at the review overview page once it completes.
            self._shadow_learning_state.pop("review_phase", None)
            # Prime the determinate bar at an explicit 0% before the first real
            # update. HA's progress bar mis-renders its fill on the very first
            # non-zero value (the scale only draws correctly from the second
            # update on); seeding 0% makes that sacrificial first render an empty
            # bar, so the first visible fill (the next update) renders correctly.
            self._set_control_discovery_progress(0.0, "starting")
            pipeline = self.hass.async_create_task(self._async_run_control_discovery())
            self._shadow_learning_state["wizard_progress_task"] = pipeline

        if pipeline.done():
            self._shadow_learning_state["wizard_progress_task"] = None
            return self.async_show_progress_done(
                next_step_id="shadow_learning_review",
            )

        # The dialog renders once (progress_task is the pipeline itself, so HA
        # only re-runs this step when discovery finishes). The label is therefore
        # static — the live feedback is the determinate bar, which the runner
        # advances via async_update_progress as each stage completes. Re-rendering
        # the dialog on a timer to animate the label visibly flickers, so we don't.
        fraction = max(
            0.0,
            min(
                1.0,
                float(
                    dict(self._shadow_learning_state.get("progress") or {}).get(
                        "fraction"
                    )
                    or 0.0
                ),
            ),
        )
        update = getattr(self, "async_update_progress", None)
        if callable(update):
            with suppress(Exception):
                update(fraction)
        return self.async_show_progress(
            step_id="shadow_learning_progress",
            progress_action="shadow_learning",
            progress_task=pipeline,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_progress_status",
                "Checking which extra device features are available…",
            ),
        )

    async def _async_run_control_discovery(self) -> None:
        """Run the automatic control-discovery pipeline for the guided wizard.

        Executed as the progress-step background task. In one pass — and with no
        preview-plan, manual field-id, numeric-value, or action-sequencing step —
        it performs: preflight -> start the fail-closed shadow session -> fetch
        cloud settings -> build a bounded automatic plan -> run learning ->
        generate the device-scoped overlay draft -> stop the session and restore
        the collector endpoint -> publish support artifacts.

        Fail-closed: any failure attempts to stop the shadow session and restore
        the endpoint, records the error in flow state, and preserves whatever
        trace/support evidence already exists. This coroutine never raises, so the
        progress step always advances to the review screen.
        """

        coordinator = self._coordinator()
        if coordinator is None:
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": "coordinator_not_loaded",
            }
            return None

        credentials = dict(self._shadow_learning_state.get("wizard_credentials") or {})
        username = str(credentials.get("username") or "").strip()
        password = str(credentials.get("password") or "")
        if not username or not password:
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": "credentials_required",
            }
            return None

        try:
            await self._async_execute_control_discovery(
                coordinator,
                username=username,
                password=password,
            )
        except asyncio.CancelledError:
            # HA's flow manager cancels this progress task when the user closes
            # the options dialog mid-scan. CancelledError does NOT subclass
            # Exception, so it would otherwise skip the fail-closed cleanup and
            # leave the collector redirected to the local proxy until the
            # session lease expires. Run the cleanup (which never raises) even
            # while being cancelled, then propagate the cancellation.
            self._shadow_learning_state["discovery"] = {
                "status": "cancelled",
                "reason": "control_discovery_cancelled",
            }
            await _finish_cleanup_on_cancel(
                self._async_control_discovery_failsafe_stop(coordinator)
            )
            raise
        except Exception as exc:
            # Fail-closed cleanup: stop the shadow session and restore the
            # collector endpoint, then surface the failure in flow state.
            progress = dict(self._shadow_learning_state.get("progress") or {})
            logger.error(
                "Control discovery failed entry=%s provider=%s stage=%s exception_type=%s",
                getattr(self._config_entry, "entry_id", ""),
                str(getattr(coordinator, "cloud_evidence_provider", "") or ""),
                str(progress.get("stage") or "unknown"),
                type(exc).__name__,
            )
            await self._async_control_discovery_failsafe_stop(coordinator)
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": self._control_discovery_failure_reason(exc),
            }
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.control_discovery_failed",
                "Control discovery could not finish. The temporary cloud "
                "connection was closed if it had been opened.",
            )
            # Preserve whatever trace/support evidence already exists.
            with suppress(Exception):
                self._publish_shadow_learning_artifacts(coordinator)
        return None

    async def _async_execute_control_discovery(
        self,
        coordinator,
        *,
        username: str,
        password: str,
    ) -> None:
        """Run the automatic discovery happy path; raise on any failure.

        Fail-closed cleanup after a failure is owned by the caller
        (``_async_run_control_discovery``); this method only stops the session
        itself on its own successful exit.
        """

        # The progress step already primed the bar at 0%. Give the frontend a
        # brief moment to actually paint that empty bar before the first non-zero
        # value, so the determinate scale renders correctly from the start instead
        # of mis-drawing the very first fill (an HA progress-dialog quirk).
        await asyncio.sleep(0.5)
        self._set_control_discovery_progress(0.01, "preflight")
        preflight_started = time.monotonic()
        preflight = await self._build_shadow_learning_preflight_snapshot(coordinator)
        preflight = dict(preflight)
        preflight["duration_ms"] = int(
            round((time.monotonic() - preflight_started) * 1000.0)
        )
        self._shadow_learning_state["preflight"] = preflight
        self._set_control_discovery_progress(0.03, "preflight")
        if not bool(preflight.get("can_start")):
            blockers = preflight.get("blockers") or []
            if not isinstance(blockers, list):
                blockers = []
            raise RuntimeError(
                "shadow_learning_preflight_blocked:"
                + ",".join(str(item) for item in blockers)
                if blockers
                else "shadow_learning_preflight_blocked"
            )

        provider = self._control_discovery_cloud_provider(coordinator)
        provider_impl = resolve_cloud_evidence_provider(provider)
        if not provider_impl.control_discovery_available:
            raise RuntimeError(
                self._tr(
                    "common.dynamic.control_discovery_provider_not_supported",
                    "Automatic control discovery is not available for "
                    "{cloud_provider_label} yet. Local read-only support and "
                    "support packages still work.",
                    {
                        "cloud_provider_label": self._control_discovery_cloud_provider_label(
                            coordinator
                        )
                    },
                )
            )

        # The active provider owns login/fetch/parse/action/orchestrate. It logs
        # in before redirecting the collector, then fetches device-bound
        # metadata and performs the control sweep through that same cloud
        # session. This avoids both wasting the short-lived E500 proxy socket on
        # authentication and creating a competing pre-proxy collector session.
        shadow_runtime = self._shadow_learning_runtime(coordinator)
        runner = provider_impl.control_discovery_runner()

        async def _start_shadow_route() -> None:
            self._set_control_discovery_progress(0.10, "connecting")
            session = await coordinator.async_start_shadow_learning(
                allow_ack_writes=False
            )
            self._shadow_learning_state["session"] = dict(session or {})
            self._publish_shadow_learning_artifacts(coordinator)

        outcome = await runner.async_run(
            executor=self.hass.async_add_executor_job,
            collector_pn=str(coordinator.smartess_collector_pn or ""),
            username=username,
            password=password,
            fallback_identity=self._shadow_learning_cloud_identity(coordinator),
            max_fields=CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS,
            progress=self._set_control_discovery_progress,
            orchestrator_callbacks=self._shadow_learning_orchestrator_callbacks(
                coordinator,
                shadow_runtime,
            ),
            on_identity=lambda identity: self._on_control_discovery_identity(
                coordinator, identity
            ),
            start_shadow_route=_start_shadow_route,
            on_learning=self._on_control_discovery_learning,
        )
        identity = outcome.identity
        result = outcome.result
        read_bindings = outcome.read_bindings

        self._shadow_learning_state["orchestration"] = result
        plan = result.get("plan") if isinstance(result, dict) else None
        if isinstance(plan, list):
            self._shadow_learning_state["plan"] = {
                "source": f"{provider}_orchestration_plan",
                "items": plan,
                "count": len(plan),
            }
        self._shadow_learning_state["session"] = {
            **dict(self._shadow_learning_state.get("session") or {}),
            "status": "degraded"
            if (
                int(result.get("degraded_count") or 0) > 0
                or int(result.get("leaked_count") or 0) > 0
            )
            else "ready",
        }
        self._publish_shadow_learning_artifacts(coordinator)

        orchestration = dict(self._shadow_learning_state.get("orchestration") or {})
        planned_count = int(orchestration.get("planned_write_count") or 0)
        executed_count = int(orchestration.get("executed_result_count") or 0)
        leaked_count = int(orchestration.get("leaked_count") or 0)
        degraded_count = int(orchestration.get("degraded_count") or 0)
        if leaked_count > 0:
            # SAFETY: at least one control write was accepted by the cloud (ERR_NONE) and did
            # not have a matching local proxy write observation -- proof the write bypassed our
            # proxy and may have reached the REAL inverter. The run was hard-stopped at the
            # first such write, but a live change may already have been applied to the hardware.
            # Do not build or offer a partial overlay from a safety-aborted run; let the caller
            # perform the fail-closed stop/restore path and surface this as an error.
            raise RuntimeError(CONTROL_DISCOVERY_FAILURE_SAFETY_STOP)
        if degraded_count > 0:
            raise RuntimeError(CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED)
        if planned_count > 0 and executed_count < planned_count:
            raise RuntimeError(CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE)

        self._set_control_discovery_progress(0.88, "building")
        correlation = result.get("correlation")
        read_map = result.get("read_map")
        if isinstance(correlation, dict):
            await self._async_generate_control_discovery_overlay(
                coordinator,
                identity=identity,
                correlation=correlation,
                read_map=read_map if isinstance(read_map, dict) else None,
                read_bindings=read_bindings,
            )

        # Success path: stop the session and restore the endpoint, then publish
        # the final artifact bundle. (Failure cleanup is owned by the caller.)
        self._set_control_discovery_progress(0.95, "finalizing")
        await self._async_control_discovery_stop(coordinator)
        self._publish_shadow_learning_artifacts(coordinator)
        self._set_control_discovery_progress(1.0, "finalizing")
        self._shadow_learning_state["status"] = self._tr(
            "common.dynamic.control_discovery_done",
            "Control discovery finished. The temporary cloud connection is closed.",
        )
        found_controls = int(
            dict(self._shadow_learning_state.get("overlay") or {}).get(
                "generated_capability_count"
            )
            or 0
        )
        sent_count = int(orchestration.get("sent_count") or 0)
        read_map_for_result = orchestration.get("read_map")
        read_event_count = (
            int(read_map_for_result.get("read_event_count") or 0)
            if isinstance(read_map_for_result, dict)
            else 0
        )
        # A run that found nothing AND transmitted no probes at all did not actually
        # observe the device -- it stalled on the connection (e.g. the collector never
        # reconnected through the temporary proxy). That is a retryable error, not a
        # genuine "this device has no controls" result, so surface it as a failure with a
        # clear retry hint instead of the misleading "nothing found this time" message.
        if found_controls == 0 and sent_count == 0 and read_event_count == 0:
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": self._tr(
                    "common.dynamic.control_discovery_run_incomplete",
                    "The device could not be probed this time (the temporary cloud "
                    "connection did not come up). Please try the scan again.",
                ),
                "found_controls": 0,
            }
        else:
            self._shadow_learning_state["discovery"] = {
                "status": "ok",
                "found_controls": found_controls,
            }
        return None

    def _on_control_discovery_identity(
        self, coordinator, identity: dict[str, Any]
    ) -> None:
        """Record the resolved cloud identity + publish artifacts (flow UI state)."""

        self._shadow_learning_state["identity"] = identity
        self._publish_shadow_learning_artifacts(coordinator)

    def _on_control_discovery_learning(self) -> None:
        """Mark the shadow session as learning (flow UI state)."""

        self._shadow_learning_state["session"] = {
            **dict(self._shadow_learning_state.get("session") or {}),
            "status": "learning",
        }

    def _shadow_learning_orchestrator_callbacks(
        self,
        coordinator,
        shadow_runtime: ShadowLearningRuntimeFacade | None = None,
    ) -> dict[str, Any]:
        """Return shared shadow-observation callbacks for provider runners."""

        def _on_test_progress(done: int, total: int) -> None:
            fraction = 0.30 + 0.55 * (done / total) if total > 0 else 0.30
            self._set_control_discovery_progress(
                fraction,
                "testing",
                done=min(done + 1, total) if total else 0,
                total=total,
            )

        shadow_runtime = (
            shadow_runtime
            if type(shadow_runtime) is ShadowLearningRuntimeFacade
            else self._shadow_learning_runtime(coordinator)
        )
        return {
            "observation_cursor": (
                shadow_runtime.observation_cursor
                if shadow_runtime is not None
                else None
            ),
            "current_observations_since": (
                shadow_runtime.observations_since
                if shadow_runtime is not None
                else None
            ),
            "wait_for_observations_since": (
                shadow_runtime.async_wait_for_observations_since
                if shadow_runtime is not None
                else None
            ),
            "is_session_ready": lambda: self._shadow_learning_route_accepts_control(
                coordinator
            ),
            "wait_until_session_ready": lambda: (
                self._async_wait_for_shadow_learning_control_route(coordinator)
            ),
            "read_map_snapshot": (
                shadow_runtime.read_map_snapshot if shadow_runtime is not None else None
            ),
            "on_progress": _on_test_progress,
        }

    async def _async_generate_control_discovery_overlay(
        self,
        coordinator,
        *,
        identity: dict[str, Any],
        correlation: dict[str, Any],
        read_map: dict[str, Any] | None = None,
        read_bindings: dict[str, Any] | None = None,
    ) -> None:
        """Generate the inactive device-scoped overlay draft from correlation evidence."""

        session = dict(self._shadow_learning_state.get("session") or {})
        session_manifest = {
            "session_id": str(
                session.get("session_id")
                or session.get("trace_path")
                or datetime.now().strftime("%Y%m%dT%H%M%S")
            ),
            "collector_pn": coordinator.smartess_collector_pn,
            "cloud_pn": str(identity.get("pn") or ""),
            "cloud_sn": str(identity.get("sn") or ""),
            "devcode": identity.get("devcode"),
            "devaddr": identity.get("devaddr"),
        }
        result = await self.hass.async_add_executor_job(
            lambda: generate_shadow_learning_overlay_drafts(
                config_dir=Path(self.hass.config.config_dir),
                source_profile_name=str(coordinator.effective_profile_name or ""),
                source_schema_name=str(
                    coordinator.effective_register_schema_name or ""
                ),
                session_manifest=session_manifest,
                correlation=correlation,
                read_map=read_map,
                read_bindings=read_bindings,
                overwrite=False,
            )
        )
        self._shadow_learning_state["overlay"] = {
            "profile_path": str(result.profile_path),
            "schema_path": str(result.schema_path),
            "generated_capability_count": int(result.generated_capability_count),
            "generated_read_count": int(result.generated_read_count),
            "skipped_duplicate_count": int(result.skipped_duplicate_count),
            "manifest": dict(result.manifest),
            "profile_name": str(
                result.manifest.get("output", {}).get("profile_name") or ""
            ),
            "schema_name": str(
                result.manifest.get("output", {}).get("schema_name") or ""
            ),
        }
        self._publish_shadow_learning_artifacts(coordinator)
        return None

    async def _async_control_discovery_stop(self, coordinator) -> dict[str, Any]:
        """Stop the shadow session and restore the endpoint on the success path."""

        stop = getattr(coordinator, "async_stop_shadow_learning", None)
        if not callable(stop):
            return {}
        result = await stop(reason="control_discovery_done")
        merged = {
            **dict(self._shadow_learning_state.get("session") or {}),
            **(dict(result) if isinstance(result, dict) else {}),
            "status": "stopped",
        }
        self._shadow_learning_state["session"] = merged
        return dict(result) if isinstance(result, dict) else {}

    async def _async_control_discovery_failsafe_stop(self, coordinator) -> None:
        """Best-effort fail-closed stop + endpoint restore after a discovery failure.

        Tolerant of an already-stopped or never-started session: it never raises,
        so it is safe to call regardless of how far the pipeline progressed.
        """

        stop = getattr(coordinator, "async_stop_shadow_learning", None)
        if not callable(stop):
            return None
        try:
            result = await stop(
                reason="control_discovery_failed",
                raise_when_not_running=False,
            )
        except Exception as exc:
            logger.warning(
                "Control discovery fail-closed stop failed for entry %s: %s",
                getattr(self._config_entry, "entry_id", ""),
                exc,
            )
            return None
        if isinstance(result, dict):
            self._shadow_learning_state["session"] = {
                **dict(self._shadow_learning_state.get("session") or {}),
                **result,
            }
        return None
