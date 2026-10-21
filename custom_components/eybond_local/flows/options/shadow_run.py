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
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from ...collector.transport import _finish_cleanup_on_cancel
from ...collector_identity import pn_is_same_identity, validated_collector_pn
from ...drivers.local_register_evidence import LocalRegisterSnapshot
from ..common.presentation import _smartess_credential_schema_fields
from ..common.translation import with_translation_bundle as _with_translation_bundle
from .shared import (
    _BOOLEAN_SELECTOR,
    CONTROL_DISCOVERY_FAILURE_GENERIC,
    CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED,
    CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE,
    CONTROL_DISCOVERY_FAILURE_SAFETY_STOP,
    control_discovery_cloud_failure_reason,
)
from ...runtime.shadow_learning_facade import ShadowLearningRuntimeFacade
from ...support.cloud_active_workflow import ACTIVE_CORRELATION_NO_SAFE_CONTROLS
from ...support.cloud_local_coverage import build_cloud_local_coverage_report
from ...support.cloud_learning_engines import (
    default_cloud_learning_method,
)
from ...support.cloud_learning_models import (
    LEARNING_METHOD_ACTIVE_CORRELATION,
    LEARNING_METHOD_READ_ONLY_EVIDENCE,
)
from ...support.cloud_semantic_evidence import CloudSemanticEvidenceReport
from ...support.shadow_learning.overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)
from ...telemetry import TypedTelemetryFrame
from .shadow_metadata_review import (
    metadata_with_cloud_local_history_draft_plan,
    metadata_with_cloud_local_history_representability,
    metadata_with_cloud_local_history_review,
)

logger = logging.getLogger(__name__)


CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS = 40


def _learning_failure_location(exc: Exception) -> str:
    """Return code coordinates only, never exception text, locals or URLs."""
    location = "unknown"
    trace = exc.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if "eybond_local" in path.parts:
            location = f"{path.name}:{code.co_name}:{trace.tb_lineno}"
        trace = trace.tb_next
    return location


def _learning_management_failure(exc: Exception) -> dict[str, object]:
    """Preserve the primary typed failure even if endpoint recovery fails too."""
    from ...collector.management import CollectorManagementTransportError

    if not isinstance(exc, CollectorManagementTransportError):
        return {}
    calls = []
    trace = exc.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if "eybond_local" in path.parts:
            calls.append(f"{path.name}:{code.co_name}:{trace.tb_lineno}")
        trace = trace.tb_next
    return {"management_failure": {
        "error_code": exc.diagnostic_code,
        "failed_request": exc.request_diagnostics,
        "call_path": calls[-12:],
    }}


def _metadata_with_local_coverage(
    metadata_evidence: dict[str, Any],
    telemetry: object,
    *,
    local_register_snapshot: object = None,
    local_register_series: object = None,
    local_register_context: object = None,
    expected_collector_pn: object = "",
) -> dict[str, Any]:
    """Attach exact typed local evidence without minting cloud bindings."""

    detached = dict(metadata_evidence)
    # Coverage belongs to the current local driver, not to the cloud source or
    # an earlier run. Collector-only entries have a valid empty telemetry frame
    # but no driver against which semantic presence can be checked.
    detached.pop("local_coverage", None)
    if (
        type(local_register_snapshot) is LocalRegisterSnapshot
        and type(expected_collector_pn) is str
        and bool(expected_collector_pn)
        and validated_collector_pn(expected_collector_pn)
        == expected_collector_pn
        and pn_is_same_identity(
            expected_collector_pn,
            local_register_snapshot.collector_pn,
        )
    ):
        detached["local_register_snapshot"] = (
            local_register_snapshot.to_record()
        )
    detached = metadata_with_cloud_local_history_review(
        detached,
        local_register_series,
    )
    detached = metadata_with_cloud_local_history_representability(
        detached,
        local_register_context,
    )
    detached = metadata_with_cloud_local_history_draft_plan(detached)
    semantic_report = CloudSemanticEvidenceReport.from_record(
        detached.get("semantic_report")
    )
    if (
        semantic_report is None
        or type(telemetry) is not TypedTelemetryFrame
        or not telemetry.driver_key
    ):
        return detached
    detached["local_coverage"] = build_cloud_local_coverage_report(
        semantic_report,
        telemetry,
    ).to_record()
    return detached


class ShadowLearningRunMixin:
    """ShadowLearningRun lifecycle."""

    async def _async_control_discovery_restore_pending(
        self, coordinator, *, allow_read_only_selection: bool = False,
    ) -> bool:
        """Resolve durable recovery before every wizard entry, including stale forms."""

        blocker = getattr(coordinator, "async_shadow_learning_start_blocker", None)
        blocker_reason = await blocker() if callable(blocker) else ""
        if blocker_reason == "cloud_learning_protection_held":
            self._shadow_learning_state["protection_held"] = True
            retry_check = getattr(coordinator, "async_shadow_learning_retry_available", None)
            retry_available = await retry_check() is True if callable(retry_check) else False
            self._shadow_learning_state["protected_retry_available"] = retry_available
            if allow_read_only_selection or self._shadow_learning_state.get(
                "wizard_method"
            ) == LEARNING_METHOD_READ_ONLY_EVIDENCE or retry_available:
                return False
            self._shadow_learning_state.pop("wizard_credentials", None)
            self._shadow_learning_state.pop("wizard_consent", None)
            self._shadow_learning_state.pop("wizard_outage_consent", None)
            if self._shadow_learning_state.get("discovery", {}).get("status") == "ok":
                return True
            self._shadow_learning_state["discovery"] = {
                "status": "error", "reason": blocker_reason,
            }
            return True
        # A reviewed zero-request receipt can retire the hold while an old form
        # is still open. Presentation follows the fresh coordinator decision.
        self._shadow_learning_state.pop("protection_held", None)
        self._shadow_learning_state.pop("protected_retry_available", None)
        if self._shadow_learning_state.get("discovery", {}).get("reason") == "cloud_learning_protection_held":
            self._shadow_learning_state["discovery"] = {
                "status": "error", "reason": CONTROL_DISCOVERY_FAILURE_GENERIC,
            }
        pending = (
            blocker_reason == "shadow_learning_restore_pending"
        ) or bool(getattr(coordinator, "shadow_learning_restore_pending", False))
        if pending:
            self._shadow_learning_state.pop("wizard_credentials", None)
            self._shadow_learning_state.pop("wizard_consent", None)
            self._shadow_learning_state["discovery"] = {
                "status": "error", "reason": "shadow_learning_restore_pending",
            }
        return pending

    @staticmethod
    def _control_discovery_failure_reason(
        exc: Exception,
        *,
        cloud_error_code: object = "",
    ) -> str:
        """Reduce internal exceptions to a closed user-facing reason set."""

        reason = str(exc).strip()
        if reason == "shadow_learning_cloud_protocol_not_confirmed":
            return "control_discovery_cloud_protocol_unconfirmed"
        if reason == (
            "shadow_learning_protocol_preparation_failed:"
            "cloud_payload_modbus_requires_register_seed"
        ):
            return "control_discovery_cloud_register_seed_required"
        if reason == "cloud_learning_protection_held":
            return "cloud_learning_protection_held"
        if reason == "shadow_learning_restore_pending" or (
            reason.startswith("shadow_learning_preflight_blocked:")
            and "shadow_learning_restore_pending" in reason.split(":", 1)[1].split(",")
        ):
            return "shadow_learning_restore_pending"
        if reason in {
            ACTIVE_CORRELATION_NO_SAFE_CONTROLS,
            CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED,
            CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE,
            CONTROL_DISCOVERY_FAILURE_SAFETY_STOP,
        }:
            return reason
        cloud_reason = control_discovery_cloud_failure_reason(cloud_error_code)
        if cloud_reason:
            return cloud_reason
        return CONTROL_DISCOVERY_FAILURE_GENERIC

    def _control_discovery_error_detail(self) -> str:
        """Return a localized explanation without leaking internal exceptions."""

        discovery = self._shadow_learning_state.get("discovery")
        reason = (
            str(discovery.get("reason") or "") if isinstance(discovery, dict) else ""
        )
        reason = reason.strip()
        if self._shadow_learning_state.get("cloud_protocol_evidence") or self._shadow_learning_state.get("protocol_diagnostics"):
            return self._tr("common.dynamic.cloud_protocol_problem",
                "Cloud protocol compatibility or learning readiness was not confirmed. "
                "After connection recovery, use the read-only profile check if offered. "
                "Otherwise download a Support Archive. Your current profile is unchanged; "
                "do not choose an arbitrary map. Any learning protection remains active.")
        if reason == CONTROL_DISCOVERY_FAILURE_GENERIC:
            from ...support.shadow_learning.protocol_mismatch import CloudModbusRead

            if CloudModbusRead.from_mapping(self._shadow_learning_state.get("cloud_modbus_read")) is not None:
                reason = "control_discovery_cloud_register_seed_required"
        if reason == "cloud_learning_protection_held":
            diagnostics = discovery.get("diagnostics") or {}
            primary = diagnostics.get("primary_failure_reason") if isinstance(diagnostics, dict) else None
            # Show the actual run failure alongside, not instead of, the hold.
            # Only the closed cloud-error vocabulary can enter this message.
            if type(primary) is str and primary in {
                "control_discovery_cloud_auth_failed", "control_discovery_cloud_rate_limited",
                "control_discovery_cloud_unavailable", "control_discovery_cloud_timeout",
                "control_discovery_cloud_network", "control_discovery_cloud_unexpected",
            }:
                reason = primary
        defaults = {
            "control_discovery_cloud_protocol_unconfirmed": (
                "No matching cloud read confirmed the PI30 protocol during preparation. "
                "No test controls were sent. Download a Support Archive and check the "
                "connection/restoration status below before trying again."
            ),
            "control_discovery_cloud_register_seed_required": (
                "Modbus reads were observed from the cloud while local polling uses PI30. "
                "No test controls were sent. Once the previous connection is restored, "
                "use the Modbus compatibility check if offered, or share a Support Archive. A successful register read "
                "alone does not confirm a compatible map."
            ),
            "cloud_learning_protection_held": (
                "Direct cloud reconnection remains blocked. Use a protected retry "
                "only when offered by the wizard, or download a Support Archive "
                "for recovery review. Read-only cloud analysis remains available."
            ),
            "shadow_learning_restore_pending": (
                "Home Assistant could not confirm restoration of the previous "
                "collector connection. Open Expand device support → Analyze cloud "
                "data and device capabilities, then retry restoring the connection "
                "before starting another check. Download a Support Archive if "
                "restoration keeps failing."
            ),
            ACTIVE_CORRELATION_NO_SAFE_CONTROLS: (
                "The selected cloud API did not expose any safe controls that "
                "could be checked for this device. Nothing was changed."
            ),
            CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED: (
                "The temporary cloud connection ended before all capabilities "
                "could be checked. The check stopped. See the connection and "
                "protection status below before taking another action."
            ),
            CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE: (
                "The device check ended before all planned capabilities were tested."
            ),
            CONTROL_DISCOVERY_FAILURE_SAFETY_STOP: (
                "The safety check stopped the scan because a command may have "
                "bypassed the local learning route. Check the inverter before trying again."
            ),
            "control_discovery_cloud_auth_failed": (
                "The cloud service rejected the login. Check the username and password."
            ),
            "control_discovery_cloud_rate_limited": (
                "The cloud service temporarily limited requests. Wait a little and try again."
            ),
            "control_discovery_cloud_unavailable": (
                "The cloud service could not provide the device data needed for this check."
            ),
            "control_discovery_cloud_timeout": (
                "The cloud service did not respond in time. Try again later."
            ),
            "control_discovery_cloud_network": (
                "Home Assistant could not reach the cloud service. Check its internet connection."
            ),
            "control_discovery_cloud_unexpected": (
                "The cloud service returned an unexpected response."
            ),
            CONTROL_DISCOVERY_FAILURE_GENERIC: (
                "The check stopped. See the connection and protection status "
                "below; this message does not confirm cloud restoration."
            ),
        }
        normalized = reason if reason in defaults else CONTROL_DISCOVERY_FAILURE_GENERIC
        return self._tr(
            f"common.dynamic.{normalized}",
            defaults[normalized],
        )

    async def _async_control_discovery_retry_restore(self, coordinator) -> bool:
        """Retry the existing restoration transaction without cloud probes."""

        try:
            result = await coordinator.async_stop_shadow_learning(
                reason="user_restore_retry", raise_when_not_running=False,
            )
        except Exception as exc:
            logger.warning(
                "Shadow-learning connection recovery failed: %s", type(exc).__name__,
            )
            result = {}
        self._shadow_learning_state["session"] = {
            **dict(self._shadow_learning_state.get("session") or {}), **result,
        }
        confirmed = result.get("restore_confirmed") is True
        self._shadow_learning_state["discovery"] = {
            "status": "error",
            "reason": CONTROL_DISCOVERY_FAILURE_GENERIC if confirmed
            else "shadow_learning_restore_pending",
        }
        return confirmed

    @_with_translation_bundle
    async def async_step_shadow_learning(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided support expansion — step 1: choose the product goal."""
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
                    "Ensure the entry is loaded, then try again.",
                ),
            )

        if await self._async_control_discovery_restore_pending(
            coordinator, allow_read_only_selection=True,
        ):
            return await self.async_step_shadow_learning_result()

        readiness = self._support_acquisition_readiness()
        if not (
            readiness.cloud_metadata_read.can_start
            or readiness.active_control_learning.can_start
            or self._shadow_learning_lifecycle_active(coordinator)
        ):
            return await self._async_cloud_tools_unavailable()

        methods = self._control_discovery_learning_methods(coordinator)
        if not methods:
            return await self._async_cloud_tools_unavailable()

        if len(methods) == 1:
            self._shadow_learning_state["wizard_method"] = methods[0].method_id
            return await self.async_step_shadow_learning_source()

        errors: dict[str, str] = {}
        default_method = default_cloud_learning_method(
            self._control_discovery_cloud_provider(coordinator)
        )
        selected = (user_input or {}).get("learning_method", default_method)
        if user_input is not None:
            if type(selected) is not str or not any(
                method.method_id == selected for method in methods
            ):
                errors["learning_method"] = "invalid_selection"
            else:
                self._reset_control_discovery_run_state()
                self._shadow_learning_state.pop("wizard_source", None)
                self._shadow_learning_state["wizard_method"] = selected
                return await self.async_step_shadow_learning_source()

        options = [
            SelectOptionDict(
                value=method.method_id,
                label=self._control_discovery_method_option_label(method.method_id),
            )
            for method in methods
        ]
        return self.async_show_form(
            step_id="shadow_learning",
            data_schema=vol.Schema(
                {
                    vol.Required("learning_method", default=default_method): SelectSelector(
                        SelectSelectorConfig(
                            options=options,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.cloud_learning_method_intro",
                "Choose whether Home Assistant should analyze cloud data without "
                "changing the device or actively verify additional local controls.",
            ),
        )

    def _control_discovery_method_option_label(self, method_id: str) -> str:
        labels = {
            LEARNING_METHOD_READ_ONLY_EVIDENCE: self._tr(
                "common.dynamic.cloud_learning_method_read_only",
                "Analyze device data (diagnostics)",
            ),
            LEARNING_METHOD_ACTIVE_CORRELATION: self._tr(
                "common.dynamic.cloud_learning_method_active",
                "Verify additional local controls (advanced)",
            ),
        }
        return labels.get(method_id, method_id)

    @_with_translation_bundle
    async def async_step_shadow_learning_source(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Choose one API source compatible with the selected product goal."""

        coordinator = self._coordinator()
        if coordinator is not None and await self._async_control_discovery_restore_pending(coordinator):
            return await self.async_step_shadow_learning_result()
        if coordinator is None or not self._control_discovery_learning_method(
            coordinator
        ):
            return await self.async_step_shadow_learning()
        sources = self._control_discovery_learning_sources(coordinator)
        if not sources:
            return await self.async_step_shadow_learning()
        if len(sources) == 1:
            self._shadow_learning_state["wizard_source"] = sources[0].source_id
            return await self._async_continue_after_learning_source(coordinator)

        method_id = self._control_discovery_learning_method(coordinator)
        default_source = self._control_discovery_default_learning_source(
            coordinator,
            method_id,
        )
        selected = (user_input or {}).get("learning_source", default_source)
        errors: dict[str, str] = {}
        if user_input is not None:
            if type(selected) is not str or not any(
                source.source_id == selected for source in sources
            ):
                errors["learning_source"] = "invalid_selection"
            else:
                self._reset_control_discovery_run_state()
                self._shadow_learning_state["wizard_method"] = method_id
                self._shadow_learning_state["wizard_source"] = selected
                return await self._async_continue_after_learning_source(coordinator)

        options = [
            SelectOptionDict(
                value=source.source_id,
                label=self._control_discovery_source_option_label(source.source_id),
            )
            for source in sources
        ]
        return self.async_show_form(
            step_id="shadow_learning_source",
            data_schema=vol.Schema(
                {
                    vol.Required("learning_source", default=default_source): SelectSelector(
                        SelectSelectorConfig(
                            options=options,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.cloud_learning_source_intro",
                "Choose the cloud service Home Assistant should use for this check.",
            ),
        )

    async def _async_continue_after_learning_source(self, coordinator) -> ConfigFlowResult:
        engine = self._control_discovery_learning_engine(coordinator)
        if not engine.available or engine.method is None:
            return await self.async_step_shadow_learning()
        if engine.method.requires_control_consent:
            return await self.async_step_shadow_learning_consent()
        return await self.async_step_shadow_learning_credentials()

    def _control_discovery_source_option_label(self, source_id: str) -> str:
        labels = {
            "smartclient": self._tr(
                "common.dynamic.cloud_learning_source_smartclient",
                "SmartClient / ShineMonitor",
            ),
            "smartess": self._tr(
                "common.dynamic.cloud_learning_source_smartess",
                "SmartESS API",
            ),
            "dessmonitor": self._tr(
                "common.dynamic.cloud_learning_source_dessmonitor",
                "DESSMonitor API",
            ),
            "valuecloud": self._tr(
                "common.dynamic.cloud_learning_source_valuecloud",
                "ValueCloud API",
            ),
        }
        return labels.get(source_id, source_id)

    @_with_translation_bundle
    async def async_step_shadow_learning_consent(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Consent to temporary routing, bounded cloud probes, and interception."""

        coordinator = self._coordinator()
        if coordinator is not None and await self._async_control_discovery_restore_pending(coordinator):
            return await self.async_step_shadow_learning_result()
        if coordinator is None:
            return await self.async_step_shadow_learning()
        engine = self._control_discovery_learning_engine(coordinator)
        if not engine.available or engine.method is None:
            return await self.async_step_shadow_learning()
        if not engine.method.requires_control_consent:
            return await self.async_step_shadow_learning_credentials()
        method_id = engine.method.method_id
        source_id = engine.source.source_id

        errors: dict[str, str] = {}
        consent = bool(
            (user_input or {}).get("shadow_learning_confirm_cloud_write", False)
        )
        outage_consent = (user_input or {}).get("shadow_learning_confirm_cloud_outage") is True
        if user_input is not None:
            if consent and outage_consent:
                self._reset_control_discovery_run_state()
                self._shadow_learning_state["wizard_method"] = method_id
                self._shadow_learning_state["wizard_source"] = source_id
                self._shadow_learning_state["wizard_consent"] = True
                self._shadow_learning_state["wizard_outage_consent"] = True
                return await self.async_step_shadow_learning_credentials()
            if not consent:
                errors["shadow_learning_confirm_cloud_write"] = "required"
            elif not outage_consent:
                errors["shadow_learning_confirm_cloud_outage"] = "required"
        return self.async_show_form(
            step_id="shadow_learning_consent",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        "shadow_learning_confirm_cloud_write",
                        default=consent,
                    ): _BOOLEAN_SELECTOR,
                    vol.Required("shadow_learning_confirm_cloud_outage", default=outage_consent): _BOOLEAN_SELECTOR,
                }
            ),
            errors=errors,
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_intro_hint",
                "Home Assistant will briefly sign in to {cloud_provider_label} to "
                "check which settings can be learned locally. Close the "
                "{cloud_app_label} app before continuing so it does not compete "
                "with the check.",
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
        if coordinator is not None and await self._async_control_discovery_restore_pending(coordinator):
            return await self.async_step_shadow_learning_result()
        engine = (
            self._control_discovery_learning_engine(coordinator)
            if coordinator is not None
            else None
        )
        if (
            coordinator is None
            or engine is None
            or not engine.available
            or engine.method is None
        ):
            return await self.async_step_shadow_learning()
        requires_consent = engine.method.requires_control_consent
        if (
            requires_consent
            and not (self._shadow_learning_state.get("wizard_consent")
                     and self._shadow_learning_state.get("wizard_outage_consent"))
        ):
            # Credentials are unreachable without a coordinator and prior consent.
            return await self.async_step_shadow_learning()

        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        username = str(defaults.get("username") or "").strip()
        # Whitespace can be part of the cloud password and its signed hash.
        password = str(defaults.get("password") or "")
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
                "Enter the username and password for {cloud_provider_label}. They "
                "are used only for this check and are not saved.",
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
        normalized_stage = str(stage)
        prior = self._shadow_learning_state.get("progress")
        prior_fraction = prior.get("fraction") if isinstance(prior, dict) else None
        # Provider callbacks and executor-thread detail callbacks can arrive at
        # adjacent lifecycle boundaries.  A new run may explicitly reset to
        # zero, but every other update is monotonic so stale stage fractions can
        # never make Home Assistant's determinate bar move backwards.
        resetting = normalized_stage == "starting" and clamped == 0.0
        if (
            not resetting
            and type(prior_fraction) in {int, float}
            and clamped < float(prior_fraction)
        ):
            clamped = float(prior_fraction)
        self._shadow_learning_state["progress"] = {
            "fraction": clamped,
            "stage": normalized_stage,
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

        Fail-closed: failures from an active-learning engine stop its shadow
        session and restore the endpoint.  Metadata-only engines never touch that
        lifecycle, including on cancellation or failure.  Available evidence is
        preserved.  Only cancellation is re-raised; ordinary failures advance to
        the review/result screen.
        """

        for key in ("cloud_modbus_read", "modbus_profile_check", "cloud_protocol_evidence", "protocol_diagnostics"):
            self._shadow_learning_state.pop(key, None)

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
            if self._control_discovery_requires_shadow_route(coordinator):
                await _finish_cleanup_on_cancel(
                    self._async_control_discovery_failsafe_stop(coordinator)
                )
            raise
        except Exception as exc:
            # Fail-closed cleanup: stop the shadow session and restore the
            # collector endpoint, then surface the failure in flow state.
            from ...support.shadow_learning.protocol_mismatch import CloudProtocolMismatchError

            self._shadow_learning_state.pop("cloud_modbus_read", None)
            if isinstance(exc, CloudProtocolMismatchError):
                if exc.cloud_read is not None:
                    self._shadow_learning_state["cloud_modbus_read"] = exc.cloud_read.to_mapping()
                if exc.evidence is not None:
                    self._shadow_learning_state["cloud_protocol_evidence"] = exc.evidence.to_mapping()
                self._shadow_learning_state["protocol_diagnostics"] = exc.diagnostics
            progress = dict(self._shadow_learning_state.get("progress") or {})
            source_id = self._control_discovery_learning_source(coordinator)
            cloud_error_code = ""
            provider_diagnostics = {}
            if str(progress.get("stage") or "") in {"fetching", "testing"}:
                engine = self._control_discovery_learning_engine(coordinator)
                cloud_error_code = engine.classify_error(exc)
                provider_diagnostics = engine.error_diagnostics(exc)
            failure_reason = self._control_discovery_failure_reason(
                exc,
                cloud_error_code=cloud_error_code,
            )
            primary_failure_reason = failure_reason
            failure_location = _learning_failure_location(exc)
            management_failure = _learning_management_failure(exc)
            request_stage = provider_diagnostics.get(
                "request_stage", str(getattr(exc, "stage", "") or "unknown")
            )
            provider_reason = provider_diagnostics.get(
                "provider_reason", str(getattr(exc, "reason_code", "") or "unknown")
            )
            logger.error(
                "Control discovery failed entry=%s source=%s stage=%s "
                "request_stage=%s provider_reason=%s exception_type=%s "
                "cloud_error_code=%s failure_reason=%s http_status=%s location=%s management=%s",
                getattr(self._config_entry, "entry_id", ""),
                source_id,
                str(progress.get("stage") or "unknown"),
                request_stage,
                provider_reason,
                type(exc).__name__,
                cloud_error_code or "not_cloud_classified",
                failure_reason,
                provider_diagnostics.get("http_status", "unknown"),
                failure_location,
                management_failure,
            )
            recovery_pending = False
            if self._control_discovery_requires_shadow_route(coordinator):
                await self._async_control_discovery_failsafe_stop(coordinator)
                session = self._shadow_learning_state.get("session") or {}
                if (
                    getattr(coordinator, "shadow_learning_restore_pending", False)
                    or session.get("restore_confirmed") is False
                    or session.get("status") in {"restore_failed", "restore_unconfirmed"}
                ):
                    recovery_pending = True
                    failure_reason = (
                        "cloud_learning_protection_held"
                        if self._shadow_learning_state.get("protection_held")
                        else "shadow_learning_restore_pending"
                    )
            self._shadow_learning_state["discovery"] = {
                "status": "error",
                "reason": failure_reason,
                # Keep workflow-owned context in the archive too, not only in
                # the Core log. Never persist exception text, request URLs or
                # arbitrary provider attributes (which may contain credentials).
                "diagnostics": {
                    "primary_failure_reason": primary_failure_reason,
                    "failure_location": failure_location,
                    **management_failure,
                    "recovery_pending": recovery_pending,
                    **provider_diagnostics,
                    "learning_source": source_id,
                    "stage": str(progress.get("stage") or "unknown"),
                    "exception_category": next(
                        (
                            error_type.__name__
                            for error_type in (
                                TimeoutError, ConnectionError, ValueError,
                                TypeError, RuntimeError,
                            )
                            if isinstance(exc, error_type)
                        ),
                        "Exception",
                    ),
                },
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
        source_id = self._control_discovery_learning_source(coordinator)
        learning_engine = self._control_discovery_learning_engine(coordinator)
        if not learning_engine.available or learning_engine.method is None:
            raise RuntimeError("cloud_learning_source_unavailable")

        self._set_control_discovery_progress(0.01, "preflight")
        if learning_engine.method.requires_shadow_route:
            preflight_started = time.monotonic()
            preflight = await self._build_shadow_learning_preflight_snapshot(coordinator)
            preflight = dict(preflight)
            preflight["duration_ms"] = int(
                round((time.monotonic() - preflight_started) * 1000.0)
            )
        else:
            collector_pn = getattr(coordinator, "smartess_collector_pn", "")
            trusted_pn = (
                type(collector_pn) is str
                and bool(collector_pn)
                and validated_collector_pn(collector_pn) == collector_pn
            )
            preflight = {
                "can_start": trusted_pn,
                "blockers": [] if trusted_pn else ["collector_identity_unavailable"],
                "collector_pn": collector_pn if trusted_pn else "",
                "metadata_only": True,
                "duration_ms": 0,
            }
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
        run_collector_pn = preflight.get("collector_pn")
        if (
            type(run_collector_pn) is not str
            or not run_collector_pn
            or validated_collector_pn(run_collector_pn) != run_collector_pn
        ):
            raise RuntimeError("shadow_learning_collector_identity_invalid")

        # The engine owns login/fetch/parse and, only when declared, the exact
        # provider-specific ordering around a temporary route.  Metadata-only
        # engines receive inert route callbacks so an implementation mistake
        # fails closed instead of mutating the collector endpoint.
        requires_shadow_route = learning_engine.method.requires_shadow_route
        shadow_runtime = (
            self._shadow_learning_runtime(coordinator)
            if requires_shadow_route
            else None
        )
        runner = learning_engine.learning_runner()
        local_register_snapshot: LocalRegisterSnapshot | None = None
        if learning_engine.evidence_capabilities.local_register_snapshot:
            capture_local = getattr(
                coordinator,
                "async_capture_local_register_snapshot",
                None,
            )
            if callable(capture_local):
                self._set_control_discovery_progress(0.06, "capturing_local")
                try:
                    candidate_snapshot = await capture_local()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # supplemental read-only evidence
                    logger.debug(
                        "Local register snapshot unavailable for cloud learning: %s",
                        type(exc).__name__,
                    )
                else:
                    if type(candidate_snapshot) is LocalRegisterSnapshot:
                        local_register_snapshot = candidate_snapshot

        current_collector_pn = getattr(coordinator, "smartess_collector_pn", "")
        if (
            type(current_collector_pn) is not str
            or validated_collector_pn(current_collector_pn)
            != current_collector_pn
            or not pn_is_same_identity(run_collector_pn, current_collector_pn)
        ):
            raise RuntimeError("shadow_learning_collector_identity_changed")

        run_route_owner_id = ""

        async def _start_shadow_route() -> None:
            nonlocal run_route_owner_id
            if not requires_shadow_route:
                raise RuntimeError("cloud_learning_shadow_route_forbidden")
            start = (coordinator.async_retry_shadow_learning
                     if self._shadow_learning_state.get("protected_retry_available") is True
                     else coordinator.async_start_shadow_learning)
            session = await start(
                allow_ack_writes=False,
                confirm_cloud_outage=self._shadow_learning_state.get("wizard_outage_consent") is True,
                cloud_source=self._control_discovery_learning_source(coordinator),
            )
            self._shadow_learning_state["session"] = dict(session or {})
            run_route_owner_id = (session or {}).get("session_id", "")
            self._publish_shadow_learning_artifacts(coordinator)

        async def _dispatch_owned_action(callable_, **kwargs):
            dispatch = getattr(coordinator, "async_dispatch_shadow_learning_cloud_action", None)
            if not requires_shadow_route or not callable(dispatch):
                # No fallback to an unowned HTTP action in the HA product path.
                raise RuntimeError("cloud_learning_hold_dispatch_context_missing")
            return await dispatch(
                callable_, collector_pn=run_collector_pn,
                route_owner_id=run_route_owner_id, **kwargs,
            )

        outcome = await runner.async_run(
            executor=self.hass.async_add_executor_job,
            collector_pn=run_collector_pn,
            username=username,
            password=password,
            fallback_identity=(
                self._shadow_learning_cloud_identity(coordinator)
                if requires_shadow_route
                else {}
            ),
            max_fields=CONTROL_DISCOVERY_AUTOMATIC_MAX_FIELDS,
            progress=self._set_control_discovery_progress,
            orchestrator_callbacks=(
                {
                    **self._shadow_learning_orchestrator_callbacks(coordinator, shadow_runtime),
                    "dispatch_action": _dispatch_owned_action,
                }
                if requires_shadow_route
                else {}
            ),
            on_identity=lambda identity: self._on_control_discovery_identity(
                coordinator, identity
            ),
            start_shadow_route=_start_shadow_route,
            on_learning=(
                self._on_control_discovery_learning
                if requires_shadow_route
                else self._forbid_metadata_only_learning
            ),
        )
        current_collector_pn = getattr(coordinator, "smartess_collector_pn", "")
        if (
            type(current_collector_pn) is not str
            or validated_collector_pn(current_collector_pn)
            != current_collector_pn
            or not pn_is_same_identity(run_collector_pn, current_collector_pn)
        ):
            raise RuntimeError("shadow_learning_collector_identity_changed")
        identity = outcome.identity
        result = dict(outcome.result)
        read_bindings = outcome.read_bindings
        metadata_evidence = outcome.metadata_evidence
        if result.get("metadata_only") is True:
            unavailable_actions = (
                metadata_evidence.get("unavailable_actions")
                if isinstance(metadata_evidence, dict)
                else None
            )
            logger.info(
                "Cloud read-only evidence ready entry=%s source=%s "
                "metadata_fields=%d semantic_candidates=%d semantic_unknown=%d "
                "unavailable_actions=%d history_status=%s history_series=%d "
                "history_points=%d history_failures=%d",
                getattr(self._config_entry, "entry_id", ""),
                source_id,
                int(result.get("metadata_field_count") or 0),
                int(result.get("semantic_candidate_count") or 0),
                int(result.get("semantic_unknown_count") or 0),
                len(unavailable_actions)
                if isinstance(unavailable_actions, list)
                else 0,
                str(result.get("history_status") or ""),
                int(result.get("history_series_count") or 0),
                int(result.get("history_point_count") or 0),
                int(result.get("history_failed_series_count") or 0),
            )
        if isinstance(metadata_evidence, dict):
            try:
                local_register_series = getattr(
                    coordinator,
                    "latest_local_register_series",
                    None,
                )
            except Exception:  # supplemental review evidence
                local_register_series = None
            try:
                local_register_context = getattr(
                    coordinator,
                    "local_register_overlay_context",
                    None,
                )
            except Exception:  # supplemental current-context review
                local_register_context = None
            metadata_evidence = _metadata_with_local_coverage(
                metadata_evidence,
                getattr(getattr(coordinator, "data", None), "telemetry", None),
                local_register_snapshot=local_register_snapshot,
                local_register_series=local_register_series,
                local_register_context=local_register_context,
                expected_collector_pn=run_collector_pn,
            )
            self._shadow_learning_state["cloud_metadata"] = metadata_evidence
            # The published runtime/support artifact is the orchestration
            # record, so replace its provider-owned evidence with the detached,
            # locally enriched record.  This keeps the review and exported
            # evidence on one exact snapshot without mutating the outcome.
            result["metadata_evidence"] = metadata_evidence

        if (self._shadow_learning_state.get("session") or {}).get("protected_retry") is True:
            from ...support.shadow_learning.retry_evidence import qualify_retry_evidence
            result = qualify_retry_evidence(result)
        self._shadow_learning_state["orchestration"] = result
        plan = result.get("plan") if isinstance(result, dict) else None
        if isinstance(plan, list):
            self._shadow_learning_state["plan"] = {
                "source": f"{source_id}_orchestration_plan",
                "items": plan,
                "count": len(plan),
            }
        if learning_engine.method.requires_shadow_route:
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
        if learning_engine.method.requires_shadow_route:
            await self._async_control_discovery_stop(coordinator)
        self._publish_shadow_learning_artifacts(coordinator)
        self._set_control_discovery_progress(1.0, "finalizing")
        if requires_shadow_route:
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.control_discovery_done",
                "Control discovery finished. The temporary cloud connection is closed.",
            )
        else:
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.cloud_learning_metadata_done",
                "The read-only device analysis finished.",
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
        metadata_field_count = (
            int(metadata_evidence.get("metadata_field_count") or 0)
            if isinstance(metadata_evidence, dict)
            else 0
        )
        # A run that found nothing AND transmitted no probes at all did not actually
        # observe the device -- it stalled on the connection (e.g. the collector never
        # reconnected through the temporary proxy). That is a retryable error, not a
        # genuine "this device has no controls" result, so surface it as a failure with a
        # clear retry hint instead of the misleading "nothing found this time" message.
        if (
            found_controls == 0
            and sent_count == 0
            and read_event_count == 0
            and metadata_field_count == 0
        ):
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
                "found_metadata": metadata_field_count,
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

    @staticmethod
    def _forbid_metadata_only_learning() -> None:
        """Reject an active-learning callback from a metadata-only engine."""

        raise RuntimeError("cloud_learning_control_probe_forbidden")

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
        try:
            learned_read_context = coordinator.shadow_learning_read_context
        except Exception:  # active read learning must fail closed on context drift
            learned_read_context = None
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
                learned_read_context=learned_read_context,
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
        """Stop interception, retaining the cloud hold after any test command."""

        stop = getattr(coordinator, "async_stop_shadow_learning", None)
        if not callable(stop):
            return {}
        result = await stop(reason="control_discovery_done")
        merged = {
            **dict(self._shadow_learning_state.get("session") or {}),
            **(dict(result) if isinstance(result, dict) else {}),
        }
        if merged.get("status") == "cloud_held":
            self._shadow_learning_state["session"] = merged
            self._shadow_learning_state["protection_held"] = True
            return dict(result)
        if (
            merged.get("restore_confirmed") is False
            or merged.get("status") in {"restore_failed", "restore_unconfirmed"}
        ):
            merged["status"] = "restore_failed"
            self._shadow_learning_state["session"] = merged
            raise RuntimeError("shadow_learning_restore_pending")
        merged["status"] = "stopped"
        self._shadow_learning_state["session"] = merged
        return dict(result) if isinstance(result, dict) else {}

    async def _async_control_discovery_failsafe_stop(self, coordinator) -> None:
        """Best-effort stop, preserving protection after a discovery failure.

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
            if result.get("status") == "cloud_held":
                self._shadow_learning_state["protection_held"] = True
            self._shadow_learning_state["session"] = {
                **dict(self._shadow_learning_state.get("session") or {}),
                **result,
            }
        return None
