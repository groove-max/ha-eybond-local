"""Extracted EyeBond options-flow lifecycle: ShadowLearningReviewMixin."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from ..common.translation import with_translation_bundle as _with_translation_bundle
from .shared import (
    _BOOLEAN_SELECTOR,
    CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED,
    CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE,
    CONTROL_DISCOVERY_FAILURE_SAFETY_STOP,
    control_discovery_cloud_failure_reason,
)
from ...support.shadow_learning.review_model import (
    build_activation_selection,
    default_learned_control_label,
)
from ...support.cloud_local_coverage import (
    CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
    CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
)
from ...dessmonitor_collection import DessMonitorHistoryCollection
from ...support.cloud_learning_engines import resolve_cloud_learning_engine
from .shadow_metadata_review import (
    cloud_history_collection,
    cloud_history_summary,
    cloud_local_history_representability,
    cloud_local_history_representability_markdown,
    cloud_local_history_representability_summary,
    cloud_local_history_draft_plan,
    cloud_local_history_review,
    cloud_local_history_review_markdown,
    cloud_local_history_review_summary,
    cloud_metadata_review_fields,
    cloud_metadata_review_markdown,
    cloud_metadata_semantic_candidate_count,
)
from .shadow_inactive_draft import async_generate_inactive_read_draft

CONTROL_DISCOVERY_RESULT_ACTION_ACTIVATE = "activate_selected"


CONTROL_DISCOVERY_RESULT_ACTION_INACTIVE_DRAFT = "create_inactive_read_draft"


CONTROL_DISCOVERY_RESULT_ACTION_SUPPORT = "create_support_package"


CONTROL_DISCOVERY_RESULT_ACTION_RETRY = "retry"


CONTROL_DISCOVERY_RESULT_ACTION_DONE = "done"


CONTROL_DISCOVERY_FAILURE_GENERIC = "control_discovery_failure_generic"


def _coerce_int(value: Any) -> int | None:
    """Best-effort int coercion; returns ``None`` for empty/non-numeric values."""

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class ShadowLearningReviewMixin:
    """ShadowLearningReview lifecycle."""

    _CONTROL_DISCOVERY_RUN_STATE_KEYS = (
        "activation",
        "cloud_metadata",
        "discovery",
        "identity",
        "inactive_read_draft",
        "orchestration",
        "overlay",
        "preflight",
        "progress",
        "read_bindings",
        "review_phase",
        "review_selections",
        "session",
        "status",
        "support_package_path",
    )

    @_with_translation_bundle
    async def async_step_shadow_learning_review(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 4: review what was found.

        Shown as two pages on the same step. Page one is a read-only overview of
        everything discovered for this device: the new controls that can be added
        and the controls already supported by Home Assistant (so the user sees the
        full picture, including ones left off). Page two lets the user rename and
        enable the new controls. Normal-risk controls default to enabled; risky or
        uncertain ones default to disabled.

        Choices are stored in transient flow state (``review_selections``); the
        discovered evidence (``review_model.learned_all``) is never mutated, so
        disabled controls are preserved for the support package and an edited
        label never overwrites the developer-facing field name.
        """
        coordinator = self._coordinator()
        if coordinator is None:
            return await self.async_step_shadow_learning()

        new_controls = self._control_discovery_review_controls()
        already_controls = self._control_discovery_already_supported_controls()
        new_reads = self._control_discovery_review_read_sensors()
        already_reads = self._control_discovery_already_supported_read_sensors()
        inconclusive_reads = self._control_discovery_inconclusive_read_sensors()
        metadata_fields = self._control_discovery_metadata_fields()

        if (
            metadata_fields
            and not new_controls
            and not already_controls
            and not new_reads
            and not already_reads
            and not inconclusive_reads
        ):
            source_id = self._shadow_learning_state.get("wizard_source")
            learning_engine = resolve_cloud_learning_engine(
                source_id
                if type(source_id) is str and source_id == source_id.strip()
                else ""
            )
            collection_supported = bool(
                learning_engine.available
                and learning_engine.source.capabilities.local_register_series
                and callable(
                    getattr(coordinator, "start_local_register_collection", None)
                )
            )
            collection_status = self._local_register_observation_status(coordinator)
            collection_can_start = collection_supported and not collection_status.active
            errors: dict[str, str] = {}
            if user_input is not None:
                requested = user_input.get("start_local_register_observation", False)
                if type(requested) is not bool:
                    errors["start_local_register_observation"] = "invalid_selection"
                elif requested and collection_can_start:
                    try:
                        self._start_local_register_observation(coordinator)
                    except (TypeError, ValueError, RuntimeError):
                        errors["base"] = "local_register_collection_unavailable"
                elif requested:
                    errors["base"] = "local_register_collection_unavailable"
                if not errors:
                    return await self.async_step_shadow_learning_result()
            metadata_count = len(metadata_fields)
            semantic_candidate_count = self._control_discovery_semantic_candidate_count(
                metadata_fields
            )
            local_available_count = sum(
                item.get("local_status")
                in {
                    CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
                    CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
                }
                for item in metadata_fields
            )
            history_collection = self._control_discovery_history_collection()
            history_series_count = (
                history_collection.collected_series_count
                if history_collection is not None
                else 0
            )
            history_point_count = (
                history_collection.point_count
                if history_collection is not None
                else 0
            )
            placeholders = {
                "cloud_metadata_count": str(metadata_count),
                "cloud_semantic_candidate_count": str(semantic_candidate_count),
                "cloud_local_available_count": str(local_available_count),
                "cloud_history_series_count": str(history_series_count),
                "cloud_history_point_count": str(history_point_count),
                "cloud_history_summary": self._control_discovery_history_summary(
                    history_collection
                ),
                "cloud_local_history_review_summary": (
                    self._control_discovery_local_history_review_summary()
                ),
                "local_register_observation_summary": (
                    self._local_register_observation_summary(coordinator)
                ),
                "cloud_metadata_overview": self._control_discovery_metadata_markdown(
                    metadata_fields
                ),
            }
            return self.async_show_form(
                step_id="shadow_learning_review",
                data_schema=vol.Schema(
                    {
                        vol.Optional(
                            "start_local_register_observation",
                            default=False,
                        ): _BOOLEAN_SELECTOR,
                    }
                    if collection_can_start
                    else {}
                ),
                errors=errors,
                description_placeholders=self._control_discovery_placeholders(
                    coordinator,
                    "common.dynamic.cloud_learning_metadata_overview_intro",
                    "{cloud_provider_label} returned {cloud_metadata_count} "
                    "metadata field(s). Home Assistant recognized "
                    "{cloud_semantic_candidate_count} reading(s); "
                    "{cloud_local_available_count} are already available "
                    "locally. {cloud_history_summary} This read-only evidence "
                    "did not add entities or controls. "
                    "{cloud_local_history_review_summary} "
                    "{local_register_observation_summary}"
                    "\n\n{cloud_metadata_overview}",
                    hint_placeholders=placeholders,
                    extra=placeholders,
                ),
            )

        # Nothing discovered at all (or discovery failed earlier): skip the
        # redundant "nothing found" page entirely and go straight to the detailed
        # result screen, which explains empty vs failed and offers retry / support
        # / return. Showing an intermediate empty page first was pure friction.
        if (
            not new_controls
            and not already_controls
            and not new_reads
            and not already_reads
            and not inconclusive_reads
        ):
            return await self.async_step_shadow_learning_result()

        # Page one: read-only overview of everything found.
        if str(self._shadow_learning_state.get("review_phase") or "overview") != "edit":
            if user_input is not None:
                if new_controls or new_reads:
                    self._shadow_learning_state["review_phase"] = "edit"
                    return await self.async_step_shadow_learning_review()
                return await self.async_step_shadow_learning_result()
            new_count = len(new_controls)
            existing_count = len(already_controls)
            new_read_count = len(new_reads)
            existing_read_count = len(already_reads)
            inconclusive_read_count = len(inconclusive_reads)
            on_count = sum(
                1 for control in new_controls if bool(control.get("enabled_by_default"))
            )
            read_on_count = sum(
                1 for sensor in new_reads if bool(sensor.get("enabled_by_default"))
            )
            overview_placeholders = {
                "control_discovery_count": str(
                    new_count + existing_count + new_read_count + existing_read_count
                ),
                "control_discovery_new_count": str(new_count),
                "control_discovery_existing_count": str(existing_count),
                "control_discovery_new_read_count": str(new_read_count),
                "control_discovery_existing_read_count": str(existing_read_count),
                "control_discovery_inconclusive_read_count": str(
                    inconclusive_read_count
                ),
                "control_discovery_on_count": str(on_count),
                "control_discovery_off_count": str(new_count - on_count),
                "control_discovery_read_on_count": str(read_on_count),
                "control_discovery_read_off_count": str(new_read_count - read_on_count),
                "control_discovery_overview": self._control_discovery_overview_markdown(
                    new_controls,
                    already_controls,
                    new_reads,
                    already_reads,
                    inconclusive_reads,
                ),
            }
            return self.async_show_form(
                step_id="shadow_learning_review",
                data_schema=vol.Schema({}),
                errors={},
                description_placeholders=self._control_discovery_placeholders(
                    coordinator,
                    "common.dynamic.control_discovery_overview_intro",
                    "Found {control_discovery_count} supported item(s): "
                    "{control_discovery_new_count} new control(s), "
                    "{control_discovery_existing_count} existing control(s), "
                    "{control_discovery_new_read_count} new sensor(s), and "
                    "{control_discovery_existing_read_count} existing sensor(s). "
                    "Another {control_discovery_inconclusive_read_count} cloud "
                    "field(s) could not be linked safely in this run. "
                    "Continue to review the results.\n\n"
                    "{control_discovery_overview}",
                    hint_placeholders=overview_placeholders,
                    extra=overview_placeholders,
                ),
            )

        # Page two: pick which new controls to add. Each option is labelled with
        # the control's friendly name (the entity is named that automatically —
        # there is no rename field), and the descriptions live on the overview.
        prior = self._control_discovery_prior_selections()
        prior_reads = self._control_discovery_prior_read_selections()
        if user_input is not None:
            self._store_control_discovery_selections(
                new_controls, new_reads, user_input
            )
            self._shadow_learning_state.pop("review_phase", None)
            return await self.async_step_shadow_learning_result()

        control_options = [
            SelectOptionDict(
                value=str(control.get("key") or ""),
                label=self._control_discovery_control_label(control),
            )
            for control in new_controls
            if str(control.get("key") or "")
        ]
        read_options = [
            SelectOptionDict(
                value=str(sensor.get("key") or ""),
                label=self._control_discovery_read_sensor_label(sensor),
            )
            for sensor in new_reads
            if str(sensor.get("key") or "")
        ]
        default_enabled = self._control_discovery_default_enabled_keys(
            new_controls, prior
        )
        default_enabled_reads = self._control_discovery_default_enabled_read_keys(
            new_reads, prior_reads
        )
        review_placeholders = {
            "control_discovery_count": str(len(new_controls)),
            "control_discovery_on_count": str(len(default_enabled)),
            "control_discovery_off_count": str(
                len(new_controls) - len(default_enabled)
            ),
            "control_discovery_read_count": str(len(new_reads)),
            "control_discovery_read_on_count": str(len(default_enabled_reads)),
            "control_discovery_read_off_count": str(
                len(new_reads) - len(default_enabled_reads)
            ),
        }
        schema_fields: dict[Any, Any] = {}
        if control_options:
            schema_fields[vol.Optional("enabled_controls", default=default_enabled)] = (
                SelectSelector(
                    SelectSelectorConfig(
                        options=control_options,
                        multiple=True,
                        mode=SelectSelectorMode.LIST,
                    )
                )
            )
        if read_options:
            schema_fields[
                vol.Optional("enabled_read_sensors", default=default_enabled_reads)
            ] = SelectSelector(
                SelectSelectorConfig(
                    options=read_options,
                    multiple=True,
                    mode=SelectSelectorMode.LIST,
                )
            )
        return self.async_show_form(
            step_id="shadow_learning_review",
            data_schema=vol.Schema(schema_fields),
            errors={},
            description_placeholders=self._control_discovery_placeholders(
                coordinator,
                "common.dynamic.control_discovery_review_intro",
                "Choose the extra controls and read sensors you want to add to Home "
                "Assistant. Risky controls are left off — enable them only if "
                "you know what they do.",
                hint_placeholders=review_placeholders,
                extra=review_placeholders,
            ),
        )

    def _control_discovery_review_controls(self) -> list[dict[str, Any]]:
        """Return the discovered controls (``review_model.learned_all``) to review.

        Reads the deterministic review model embedded in the generated overlay
        manifest (EYB-REF-042). Returns copies so callers cannot mutate the stored
        evidence, and an empty list when no overlay/review model exists (e.g. a
        failed or empty discovery run).
        """

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        learned_all = review_model.get("learned_all")
        if not isinstance(learned_all, list):
            return []
        return [dict(entry) for entry in learned_all if isinstance(entry, dict)]

    def _control_discovery_metadata_fields(self) -> list[dict[str, str]]:
        """Return deduplicated, credential-free metadata for read-only review."""
        return cloud_metadata_review_fields(
            self._shadow_learning_state.get("cloud_metadata")
        )

    def _control_discovery_history_collection(
        self,
    ) -> DessMonitorHistoryCollection | None:
        """Return only an exact, internally consistent DESS history record."""
        return cloud_history_collection(
            self._shadow_learning_state.get("cloud_metadata")
        )

    def _control_discovery_history_summary(
        self,
        collection: DessMonitorHistoryCollection | None,
    ) -> str:
        """Render bounded history availability without exposing raw evidence."""
        return cloud_history_summary(collection, self._tr)

    def _control_discovery_local_history_review_summary(self) -> str:
        """Describe exact review candidates without claiming a mapping."""

        evidence = self._shadow_learning_state.get("cloud_metadata")
        review_summary = cloud_local_history_review_summary(
            cloud_local_history_review(evidence),
            self._tr,
        )
        representability_summary = cloud_local_history_representability_summary(
            cloud_local_history_representability(evidence),
            self._tr,
        )
        return " ".join(
            part for part in (review_summary, representability_summary) if part
        )

    @staticmethod
    def _control_discovery_semantic_candidate_count(
        fields: list[dict[str, str]],
    ) -> int:
        """Count recognized read hints without treating settings as sensors."""

        return cloud_metadata_semantic_candidate_count(fields)

    def _control_discovery_metadata_markdown(
        self, fields: list[dict[str, str]]
    ) -> str:
        """Render grouped semantic hints without claiming a local mapping."""

        metadata_markdown = cloud_metadata_review_markdown(fields, self._tr)
        evidence = self._shadow_learning_state.get("cloud_metadata")
        review_markdown = cloud_local_history_review_markdown(
            cloud_local_history_review(evidence),
            self._tr,
        )
        representability_markdown = (
            cloud_local_history_representability_markdown(
                cloud_local_history_representability(evidence),
                self._tr,
            )
        )
        return "\n\n".join(
            part
            for part in (
                metadata_markdown,
                review_markdown,
                representability_markdown,
            )
            if part
        )

    def _control_discovery_review_read_sensors(self) -> list[dict[str, Any]]:
        """Return generated learned read sensors available for review."""

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        learned_all = review_model.get("learned_read_all")
        if not isinstance(learned_all, list):
            return []
        return [dict(entry) for entry in learned_all if isinstance(entry, dict)]

    def _reset_control_discovery_run_state(self) -> None:
        """Drop every result key from the previous discovery run plus credentials."""

        for key in self._CONTROL_DISCOVERY_RUN_STATE_KEYS:
            self._shadow_learning_state.pop(key, None)
        self._shadow_learning_state.pop("wizard_consent", None)
        self._shadow_learning_state.pop("wizard_credentials", None)
        self._shadow_learning_state.pop("wizard_progress_task", None)

    def _control_discovery_failed(self) -> bool:
        """Return whether the discovery run ended in an error (vs a genuine empty result)."""

        discovery = self._shadow_learning_state.get("discovery")
        return (
            isinstance(discovery, dict)
            and str(discovery.get("status") or "") == "error"
        )

    @staticmethod
    def _control_discovery_failure_reason(
        exc: Exception,
        *,
        cloud_error_code: object = "",
    ) -> str:
        """Reduce internal exceptions to a closed user-facing reason set."""

        reason = str(exc).strip()
        if reason in {
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
        defaults = {
            CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED: (
                "The temporary cloud connection ended before all capabilities "
                "could be checked. Home Assistant stopped safely and restored "
                "the collector connection."
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
                "Home Assistant stopped safely and restored the collector connection."
            ),
        }
        normalized = reason if reason in defaults else CONTROL_DISCOVERY_FAILURE_GENERIC
        return self._tr(
            f"common.dynamic.{normalized}",
            defaults[normalized],
        )

    def _control_discovery_already_supported_controls(self) -> list[dict[str, Any]]:
        """Return controls discovered but already supported by the base schema.

        These come from the overlay manifest's ``skipped_duplicates`` (registers
        already mapped by Home Assistant). They are shown read-only on the review
        overview so the user sees the full picture of what was found, not only the
        new additions.
        """

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        skipped = manifest.get("skipped_duplicates")
        if not isinstance(skipped, list):
            return []
        return [dict(entry) for entry in skipped if isinstance(entry, dict)]

    def _control_discovery_already_supported_read_sensors(self) -> list[dict[str, Any]]:
        """Return learned read candidates skipped because they are already covered."""

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        skipped = review_model.get("read_excluded_by_policy")
        if not isinstance(skipped, list):
            return []
        return [dict(entry) for entry in skipped if isinstance(entry, dict)]

    def _control_discovery_inconclusive_read_sensors(self) -> list[dict[str, Any]]:
        """Return observed cloud fields that were not safe to bind this run."""

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        rows = review_model.get("read_inconclusive")
        if not isinstance(rows, list):
            return []
        return [dict(entry) for entry in rows if isinstance(entry, dict)]

    def _control_discovery_overview_markdown(
        self,
        new_controls: list[dict[str, Any]],
        already_controls: list[dict[str, Any]],
        new_reads: list[dict[str, Any]] | None = None,
        already_reads: list[dict[str, Any]] | None = None,
        inconclusive_reads: list[dict[str, Any]] | None = None,
    ) -> str:
        """Render a readable, non-technical overview of everything discovered.

        A markdown bullet list (which renders reliably in the flow form) grouped
        into new controls — each with its friendly type and suggested state — and
        controls already in Home Assistant.
        """

        def clean(value: Any) -> str:
            return str(value or "").replace("\n", " ").strip()

        lines: list[str] = []
        new_reads = list(new_reads or [])
        already_reads = list(already_reads or [])
        inconclusive_reads = list(inconclusive_reads or [])
        if new_controls:
            heading = self._tr(
                "common.dynamic.control_discovery_overview_new_heading",
                "New controls found ({count})",
                {"count": str(len(new_controls))},
            )
            lines.append(f"**{heading}**")
            for control in new_controls:
                name = clean(self._control_discovery_control_label(control))
                type_label = clean(
                    self._control_discovery_type_label(
                        str(control.get("value_kind") or "")
                    )
                )
                status = clean(self._control_discovery_status_note(control))
                lines.append(f"- {name} — {type_label} · {status}")
        if already_controls:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_existing_heading",
                "Already in Home Assistant ({count})",
                {"count": str(len(already_controls))},
            )
            lines.append(f"**{heading}**")
            for control in already_controls:
                name = clean(control.get("field_name") or control.get("field_id"))
                lines.append(f"- {name}")
        if new_reads:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_new_reads_heading",
                "New read sensors found ({count})",
                {"count": str(len(new_reads))},
            )
            lines.append(f"**{heading}**")
            for sensor in new_reads:
                name = clean(self._control_discovery_read_sensor_label(sensor))
                type_label = self._tr(
                    "common.dynamic.control_discovery_overview_read_sensor_type",
                    "Sensor",
                )
                suggested = self._tr(
                    "common.dynamic.control_discovery_overview_read_sensor_suggested",
                    "Suggested on",
                )
                lines.append(f"- {name} — {type_label} · {suggested}")
        if already_reads:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_existing_reads_heading",
                "Read sensors already in Home Assistant ({count})",
                {"count": str(len(already_reads))},
            )
            lines.append(f"**{heading}**")
            for sensor in already_reads:
                name = clean(self._control_discovery_read_sensor_label(sensor))
                lines.append(f"- {name}")
        if inconclusive_reads:
            if lines:
                lines.append("")
            heading = self._tr(
                "common.dynamic.control_discovery_overview_inconclusive_reads_heading",
                "Cloud fields not linked in this run ({count})",
                {"count": str(len(inconclusive_reads))},
            )
            lines.append(f"**{heading}**")
            for sensor in inconclusive_reads:
                name = clean(self._control_discovery_read_sensor_label(sensor))
                reason = clean(
                    self._control_discovery_inconclusive_read_reason(sensor)
                )
                lines.append(f"- {name} — {reason}")
        return "\n".join(lines)

    def _control_discovery_inconclusive_read_reason(
        self,
        sensor: dict[str, Any],
    ) -> str:
        """Return a localized explanation for one non-promotable read field."""

        reason = str(sensor.get("reason") or "unresolved").strip()
        translations = {
            "value_zero": (
                "control_discovery_read_reason_value_zero",
                "no active value during this check",
            ),
            "multiple_registers": (
                "control_discovery_read_reason_multiple_registers",
                "several registers had the same value",
            ),
            "enum_ambiguous": (
                "control_discovery_read_reason_enum_ambiguous",
                "the state could not be matched safely",
            ),
            "enum_no_match": (
                "control_discovery_read_reason_enum_no_match",
                "the state is not in the known value tables",
            ),
            "no_register_match": (
                "control_discovery_read_reason_no_register_match",
                "no matching register was observed",
            ),
            "not_numeric": (
                "control_discovery_read_reason_not_numeric",
                "the value needs a known state table",
            ),
            "unresolved": (
                "control_discovery_read_reason_unresolved",
                "not enough evidence in this check",
            ),
        }
        key, default = translations.get(reason, translations["unresolved"])
        return self._tr(f"common.dynamic.{key}", default)

    def _control_discovery_prior_selections(self) -> dict[str, dict[str, Any]]:
        """Return any previously stored per-control selections, keyed by control key."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        controls = selections.get("controls")
        return controls if isinstance(controls, dict) else {}

    def _control_discovery_prior_read_selections(self) -> dict[str, dict[str, Any]]:
        """Return previously stored per-read-sensor selections."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        read_sensors = selections.get("read_sensors")
        return read_sensors if isinstance(read_sensors, dict) else {}

    def _control_discovery_default_enabled_keys(
        self,
        controls: list[dict[str, Any]],
        prior: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Return the control keys that should be pre-selected on the edit page.

        Honours a prior selection when the user revisits the screen, otherwise
        falls back to the review model's ``enabled_by_default`` decision
        (normal-risk on, high-risk/uncertain off).
        """

        enabled: list[str] = []
        for control in controls:
            key = str(control.get("key") or "")
            if not key:
                continue
            saved = prior.get(key)
            saved = saved if isinstance(saved, dict) else {}
            if "enabled" in saved:
                is_on = bool(saved.get("enabled"))
            else:
                is_on = bool(control.get("enabled_by_default"))
            if is_on:
                enabled.append(key)
        return enabled

    def _control_discovery_default_enabled_read_keys(
        self,
        read_sensors: list[dict[str, Any]],
        prior: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Return read sensor keys pre-selected on the edit page."""

        enabled: list[str] = []
        for sensor in read_sensors:
            key = str(sensor.get("key") or "")
            if not key:
                continue
            saved = prior.get(key)
            saved = saved if isinstance(saved, dict) else {}
            if "enabled" in saved:
                is_on = bool(saved.get("enabled"))
            else:
                is_on = bool(sensor.get("enabled_by_default"))
            if is_on:
                enabled.append(key)
        return enabled

    def _store_control_discovery_selections(
        self,
        controls: list[dict[str, Any]],
        read_sensors: list[dict[str, Any]],
        user_input: dict[str, Any],
    ) -> None:
        """Persist the user's per-control name + enable choices into flow state.

        Stores choices keyed by the discovered control key under
        ``review_selections`` for the activation / support-package steps. This is
        additive flow state: it never edits the overlay manifest, so the discovered
        evidence (including disabled controls and the original field names) stays
        intact.
        """

        selected_raw = user_input.get("enabled_controls")
        selected = (
            {str(key) for key in selected_raw}
            if isinstance(selected_raw, (list, tuple, set))
            else set()
        )
        selected_reads_raw = user_input.get("enabled_read_sensors")
        selected_reads = (
            {str(key) for key in selected_reads_raw}
            if isinstance(selected_reads_raw, (list, tuple, set))
            else set()
        )
        stored: dict[str, dict[str, Any]] = {}
        enabled_by_user: list[str] = []
        excluded_by_user: list[str] = []
        for control in controls:
            key = str(control.get("key") or "")
            if not key:
                continue
            # The friendly discovered name is used as-is (no rename field); it
            # becomes the entity's label when activated.
            label = self._control_discovery_control_label(control)
            enabled = key in selected
            stored[key] = {
                "key": key,
                "register": _coerce_int(control.get("register")) or 0,
                "field_id": str(control.get("field_id") or ""),
                "value_kind": str(control.get("value_kind") or ""),
                "risk_level": str(control.get("risk_level") or ""),
                "label": label,
                "default_label": label,
                "enabled": enabled,
                "enabled_by_default": bool(control.get("enabled_by_default")),
            }
            if enabled:
                enabled_by_user.append(key)
            else:
                excluded_by_user.append(key)
        stored_reads: dict[str, dict[str, Any]] = {}
        read_enabled_by_user: list[str] = []
        read_excluded_by_user: list[str] = []
        for sensor in read_sensors:
            key = str(sensor.get("key") or "")
            if not key:
                continue
            label = self._control_discovery_read_sensor_label(sensor)
            enabled = key in selected_reads
            stored_reads[key] = {
                "key": key,
                "register": _coerce_int(sensor.get("register")) or 0,
                "kind": str(sensor.get("kind") or ""),
                "spec_set": str(sensor.get("spec_set") or ""),
                "label": label,
                "default_label": label,
                "enabled": enabled,
                "enabled_by_default": bool(sensor.get("enabled_by_default")),
            }
            if enabled:
                read_enabled_by_user.append(key)
            else:
                read_excluded_by_user.append(key)
        self._shadow_learning_state["review_selections"] = {
            "controls": stored,
            "read_sensors": stored_reads,
            "enabled_by_user": enabled_by_user,
            "excluded_by_user": excluded_by_user,
            "read_enabled_by_user": read_enabled_by_user,
            "read_excluded_by_user": read_excluded_by_user,
        }

    def _control_discovery_control_label(self, control: dict[str, Any]) -> str:
        """Return the discovered default label for one control."""

        default_label = str(control.get("default_label") or "").strip()
        if default_label:
            return default_label
        return default_learned_control_label(
            field_name=str(control.get("field_name") or ""),
            field_id=str(control.get("field_id") or ""),
            register=_coerce_int(control.get("register")),
        )

    def _control_discovery_read_sensor_label(self, sensor: dict[str, Any]) -> str:
        """Return the discovered default label for one learned read sensor."""

        default_label = str(sensor.get("default_label") or "").strip()
        if default_label:
            return default_label
        field_name = str(sensor.get("field_name") or "").strip()
        if field_name:
            return field_name
        register = _coerce_int(sensor.get("register"))
        if register is not None and register > 0:
            return f"Discovered sensor {register}"
        return "Discovered sensor"

    def _control_discovery_type_label(self, value_kind: str) -> str:
        """Map an internal value kind to a friendly, non-technical control type."""

        kind = str(value_kind or "").strip().lower()
        if kind == "bool":
            return self._tr("common.dynamic.control_discovery_type_switch", "Switch")
        if kind == "enum":
            return self._tr("common.dynamic.control_discovery_type_select", "Option")
        if kind == "action":
            return self._tr("common.dynamic.control_discovery_type_button", "Button")
        if kind in {"u16", "u32_high_first", "u32_low_first"}:
            return self._tr("common.dynamic.control_discovery_type_number", "Number")
        return self._tr("common.dynamic.control_discovery_type_other", "Setting")

    def _control_discovery_status_note(self, control: dict[str, Any]) -> str:
        """Return a short, non-technical suggested-state note for one control."""

        risk = str(control.get("risk_level") or "").strip().lower()
        if risk == "high":
            return self._tr(
                "common.dynamic.control_discovery_status_high",
                "Risky — off by default",
            )
        if risk == "uncertain" or not bool(control.get("enabled_by_default")):
            return self._tr(
                "common.dynamic.control_discovery_status_uncertain",
                "Needs a check — off by default",
            )
        return self._tr(
            "common.dynamic.control_discovery_status_normal",
            "Suggested on",
        )

    @_with_translation_bundle
    async def async_step_shadow_learning_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Guided control-discovery wizard — step 5: final result.

        The discovery session is already stopped by this point and the transient
        credentials gathered for the run are dropped here, so nothing sensitive
        is retained after the wizard ends.

        The action selector adapts to the run's outcome:

        - **Apply the selected parameters** — only when the user actually turned
          on at least one discovered control. It activates the device-scoped
          learned overlay with exactly that selection (and labels) via
          ``build_activation_selection`` + ``async_activate_device_scoped_overlay``.
          On success it CONFIRMS on the same screen (does not bounce to the menu);
        - **Try the scan again** — shown in place of Apply when the run failed;
          restarts the guided wizard;
        - **Create an inactive local sensor draft** — only when the DESSMonitor
          history review produced exact, non-colliding local candidates. It writes
          a review artifact but never activates entities or reloads the entry;
        - **Download the support package** — always offered; confirms in place;
        - **Return to the menu** — leaves the wizard.

        Activation is never automatic. After any in-place action the default
        selection becomes "Return to the menu" so the next submit leaves cleanly.
        """
        coordinator = self._coordinator()
        # Drop transient credentials once the wizard reaches its end.
        self._shadow_learning_state.pop("wizard_credentials", None)
        if coordinator is None:
            return await self.async_step_shadow_learning()

        controls = self._control_discovery_review_controls()
        # Failure is decided ONLY by the run's status (discovery.status=="error").
        # error_detail falls back to the last status line, which on success holds
        # the success message — OR-ing it in here turned a successful-but-empty
        # run into a failure screen that printed the success text as the error.
        failed = self._control_discovery_failed()
        error_detail = self._control_discovery_error_detail() if failed else ""
        selected_count = self._control_discovery_enabled_selection_count()
        read_count = self._control_discovery_enabled_read_selection_count()
        metadata_count = len(self._control_discovery_metadata_fields())
        inactive_draft_plan = cloud_local_history_draft_plan(
            self._shadow_learning_state.get("cloud_metadata")
        )
        inactive_draft_count = (
            inactive_draft_plan.item_count
            if inactive_draft_plan is not None
            else 0
        )
        can_generate_inactive_draft = inactive_draft_count > 0 and not failed
        # Learned read sensors are applied with the schema overlay regardless of
        # control selection, so selected read sensors make activation worthwhile
        # on their own.
        can_activate = (bool(controls) and selected_count > 0) or read_count > 0

        errors: dict[str, str] = {}
        notice = ""
        if user_input is not None:
            raw_action = user_input.get(
                "result_action", CONTROL_DISCOVERY_RESULT_ACTION_DONE
            )
            action = raw_action if type(raw_action) is str else ""
            if action == CONTROL_DISCOVERY_RESULT_ACTION_RETRY:
                # Re-run the guided wizard from the consent step (it resets the
                # run's transient state); credentials are re-gathered there.
                for key in (
                    "discovery",
                    "progress",
                    "review_phase",
                    "review_selections",
                    "wizard_progress_task",
                ):
                    self._shadow_learning_state.pop(key, None)
                return await self.async_step_shadow_learning()
            if action == CONTROL_DISCOVERY_RESULT_ACTION_ACTIVATE and can_activate:
                error = await self._async_control_discovery_activate_selection(
                    coordinator
                )
                if error is None:
                    # Confirm and STAY on this screen -- applying must not bounce
                    # the user straight back to the menu.
                    if read_count > 0 and selected_count > 0:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_applied_both",
                            "✓ The selected controls and {read_count} read "
                            "sensor(s) were added to Home Assistant.",
                            {"read_count": str(read_count)},
                        )
                    elif read_count > 0:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_applied_reads",
                            "✓ {read_count} read sensor(s) were added to Home Assistant.",
                            {"read_count": str(read_count)},
                        )
                    else:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_applied",
                            "✓ The selected control(s) were added to Home Assistant.",
                        )
                else:
                    errors["base"] = error
            elif action == CONTROL_DISCOVERY_RESULT_ACTION_INACTIVE_DRAFT:
                if can_generate_inactive_draft:
                    error = (
                        await self._async_control_discovery_generate_inactive_read_draft(
                            coordinator
                        )
                    )
                    if error is None:
                        notice = self._tr(
                            "common.dynamic.control_discovery_result_notice_inactive_draft",
                            "✓ An inactive local sensor draft was saved for review. "
                            "Nothing was added to Home Assistant.",
                        )
                    else:
                        errors["base"] = error
                else:
                    errors["result_action"] = "invalid_selection"
            elif action == CONTROL_DISCOVERY_RESULT_ACTION_SUPPORT:
                error = await self._async_control_discovery_export_support(coordinator)
                if error is None:
                    notice = self._tr(
                        "common.dynamic.control_discovery_result_notice_support",
                        "✓ Support package saved.",
                    )
                else:
                    errors["base"] = error
            elif action == CONTROL_DISCOVERY_RESULT_ACTION_DONE:
                return await self.async_step_init()
            else:
                errors["result_action"] = "invalid_selection"

        # "Apply the selected parameters" shows only when the user turned on at
        # least one discovered control; on a failed run it is replaced by "Try the
        # scan again". Support + Return are always offered.
        action_options: list[SelectOptionDict] = []
        if can_activate:
            action_options.append(
                SelectOptionDict(
                    value=CONTROL_DISCOVERY_RESULT_ACTION_ACTIVATE,
                    label=self._tr(
                        "common.dynamic.control_discovery_result_action_activate",
                        "Apply the selected parameters",
                    ),
                )
            )
        elif failed:
            action_options.append(
                SelectOptionDict(
                    value=CONTROL_DISCOVERY_RESULT_ACTION_RETRY,
                    label=self._tr(
                        "common.dynamic.control_discovery_result_action_retry",
                        "Try the scan again",
                    ),
                )
            )
        if can_generate_inactive_draft:
            action_options.append(
                SelectOptionDict(
                    value=CONTROL_DISCOVERY_RESULT_ACTION_INACTIVE_DRAFT,
                    label=self._tr(
                        "common.dynamic.control_discovery_result_action_inactive_draft",
                        "Create an inactive local sensor draft",
                    ),
                )
            )
        action_options.append(
            SelectOptionDict(
                value=CONTROL_DISCOVERY_RESULT_ACTION_SUPPORT,
                label=self._tr(
                    "common.dynamic.control_discovery_result_action_support",
                    "Download the support package",
                ),
            )
        )
        action_options.append(
            SelectOptionDict(
                value=CONTROL_DISCOVERY_RESULT_ACTION_DONE,
                label=self._tr(
                    "common.dynamic.control_discovery_result_action_done",
                    "Return to the menu",
                ),
            )
        )
        # After a successful action default to leaving; otherwise to the primary
        # (apply / retry / support) option.
        default_action = (
            CONTROL_DISCOVERY_RESULT_ACTION_DONE
            if notice
            else action_options[0]["value"]
        )

        if controls:
            if can_activate:
                body_key = "common.dynamic.control_discovery_result_intro"
                body_default = (
                    "Discovery finished and the temporary cloud connection is "
                    "closed. {control_discovery_selected_count} control(s) are "
                    "turned on. Apply them, download the support package, or return "
                    "to the menu."
                )
            else:
                body_key = "common.dynamic.control_discovery_result_intro_none_selected"
                body_default = (
                    "Discovery finished and the temporary cloud connection is "
                    "closed. You did not turn on any of the discovered controls, so "
                    "there is nothing to apply. Download the support package, or "
                    "return to the menu."
                )
            hint_placeholders = {
                "control_discovery_selected_count": str(selected_count)
            }
        elif read_count > 0:
            # No controls to review, but read sensors were learned: activation is
            # still worthwhile, so offer Apply instead of a dead-end.
            body_key = "common.dynamic.control_discovery_result_reads_only"
            body_default = (
                "Discovery finished and the temporary cloud connection is "
                "closed. {control_discovery_read_count} read sensor(s) were "
                "discovered. Apply them, download the support package, or return "
                "to the menu."
            )
            hint_placeholders = {"control_discovery_read_count": str(read_count)}
        elif failed:
            body_key = "common.dynamic.control_discovery_result_failed"
            body_default = (
                "The check couldn't finish ({control_discovery_error}). Try the "
                "scan again, download the support package so the developer can see "
                "what happened, or return to the menu."
            )
            hint_placeholders = {
                "control_discovery_error": error_detail or "unknown error"
            }
        elif metadata_count > 0:
            semantic_candidate_count = self._control_discovery_semantic_candidate_count(
                self._control_discovery_metadata_fields()
            )
            history_collection = self._control_discovery_history_collection()
            body_key = "common.dynamic.cloud_learning_metadata_result"
            body_default = (
                "The read-only check found {cloud_metadata_count} metadata "
                "field(s), including {cloud_semantic_candidate_count} recognized "
                "semantic candidate(s). {cloud_history_summary} No local register "
                "mapping was proven, so "
                "no entities or controls were added. "
                "{cloud_local_history_review_summary} "
                "{local_register_observation_summary} Download the support package "
                "or return to the menu."
            )
            hint_placeholders = {
                "cloud_metadata_count": str(metadata_count),
                "cloud_semantic_candidate_count": str(semantic_candidate_count),
                "cloud_history_series_count": str(
                    history_collection.collected_series_count
                    if history_collection is not None
                    else 0
                ),
                "cloud_history_point_count": str(
                    history_collection.point_count
                    if history_collection is not None
                    else 0
                ),
                "cloud_history_summary": self._control_discovery_history_summary(
                    history_collection
                ),
                "cloud_local_history_review_summary": (
                    self._control_discovery_local_history_review_summary()
                ),
                "local_register_observation_summary": (
                    self._local_register_observation_summary(coordinator)
                ),
            }
        else:
            body_key = "common.dynamic.control_discovery_result_empty_with_support"
            body_default = (
                "The check has finished and the temporary cloud connection is "
                "closed. No controls were found to add this time. Download the "
                "support package so the developer can inspect what happened, or "
                "return to the menu."
            )
            hint_placeholders = {}

        placeholders = self._control_discovery_placeholders(
            coordinator,
            body_key,
            body_default,
            hint_placeholders=hint_placeholders,
            extra=hint_placeholders,
        )
        if controls and read_count > 0:
            # Controls path already has its intro; note the learned reads too.
            read_line = self._tr(
                "common.dynamic.control_discovery_result_reads_note",
                "Plus {control_discovery_read_count} read sensor(s) were "
                "discovered and will be added on Apply.",
                {"control_discovery_read_count": str(read_count)},
            )
            placeholders["control_discovery_hint"] = (
                f"{placeholders.get('control_discovery_hint', '')}\n\n{read_line}"
            )
        if notice:
            placeholders["control_discovery_hint"] = (
                f"{notice}\n\n{placeholders.get('control_discovery_hint', '')}"
            )
        elif can_generate_inactive_draft:
            draft_line = self._tr(
                "common.dynamic.cloud_learning_inactive_draft_available",
                "{count} exact-route candidate(s) can be saved as an inactive "
                "local sensor draft for review. This does not add entities.",
                {"count": str(inactive_draft_count)},
            )
            placeholders["control_discovery_hint"] = (
                f"{placeholders.get('control_discovery_hint', '')}\n\n{draft_line}"
            )
        return self.async_show_form(
            step_id="shadow_learning_result",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        "result_action",
                        default=default_action,
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=action_options,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    ),
                }
            ),
            errors=errors,
            description_placeholders=placeholders,
        )

    def _control_discovery_enabled_selection_count(self) -> int:
        """Return how many discovered controls the user has turned on."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        enabled = selections.get("enabled_by_user")
        return len(enabled) if isinstance(enabled, list) else 0

    def _control_discovery_enabled_read_selection_count(self) -> int:
        """Return how many discovered read sensors the user has turned on."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        enabled = selections.get("read_enabled_by_user")
        if isinstance(enabled, list):
            return len(enabled)
        # Backward-compatible fallback for packages/runs created before read
        # review selections existed.
        return int(
            dict(self._shadow_learning_state.get("overlay") or {}).get(
                "generated_read_count"
            )
            or 0
        )

    def _control_discovery_review_selection_payload(
        self,
        overlay: dict[str, Any],
    ) -> dict[str, Any]:
        """Return the user's reviewed control selection without activating it."""

        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        selected_controls = selections.get("controls")
        selected_reads = selections.get("read_sensors")
        if (not isinstance(selected_controls, dict) or not selected_controls) and (
            not isinstance(selected_reads, dict) or not selected_reads
        ):
            return {}

        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        return build_activation_selection(
            review_model=review_model,
            selections=selections,
        )

    async def _async_control_discovery_activate_selection(
        self, coordinator
    ) -> str | None:
        """Activate the user-selected discovered controls for this device.

        Builds the activation selection from the review model
        (``overlay.manifest.review_model``, EYB-REF-042) and the user's review
        choices (``review_selections``, EYB-REF-043) and activates the
        device-scoped learned overlay with exactly those controls (EYB-REF-044),
        so runtime exposes only what the user turned on. Returns ``None`` on
        success or an error code for the result form on failure.

        The discovered evidence is read-only here: ``build_activation_selection``
        never mutates the overlay manifest, so ``learned_all`` (including the
        disabled controls) stays intact for the support package.
        """

        overlay = self._shadow_learning_state.get("overlay")
        overlay = overlay if isinstance(overlay, dict) else {}
        profile_name = str(overlay.get("profile_name") or "").strip()
        schema_name = str(overlay.get("schema_name") or "").strip()
        manifest = overlay.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        review_model = manifest.get("review_model")
        review_model = review_model if isinstance(review_model, dict) else {}
        selections = self._shadow_learning_state.get("review_selections")
        selections = selections if isinstance(selections, dict) else {}
        try:
            if not profile_name or not schema_name:
                raise RuntimeError("shadow_learning_overlay_unavailable")
            selection = build_activation_selection(
                review_model=review_model,
                selections=selections,
            )
            activation = await coordinator.async_activate_device_scoped_overlay(
                profile_name=profile_name,
                register_schema_name=schema_name,
                selection=selection,
            )
            self._shadow_learning_state["activation"] = dict(activation)
            self._publish_shadow_learning_artifacts(coordinator)
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.shadow_learning_status_overlay_activated",
                "Discovered controls activated for this device and reload requested.",
            )
            return None
        except Exception as exc:  # noqa: BLE001 - surfaced to the user as a form error
            self._shadow_learning_state["status"] = str(exc)
            return "shadow_learning_failed"

    async def _async_control_discovery_generate_inactive_read_draft(
        self,
        coordinator,
    ) -> str | None:
        """Write one review artifact without activating or reloading it."""

        state = await async_generate_inactive_read_draft(
            hass=self.hass,
            coordinator=coordinator,
            metadata=self._shadow_learning_state.get("cloud_metadata"),
        )
        if state is None:
            return "shadow_learning_failed"
        self._shadow_learning_state["inactive_read_draft"] = state
        return None

    async def _async_control_discovery_export_support(self, coordinator) -> str | None:
        """Export a support package from the guided result screen.

        Mirrors the advanced path's support-only export: publishes the current
        UX artifacts and writes a sanitized support archive without re-running
        any live SmartESS operation. Returns ``None`` on success or an error code
        on failure.
        """

        try:
            self._publish_shadow_learning_artifacts(coordinator)
            path = await coordinator.async_export_support_package_with_cloud_refresh(
                smartess_username="",
                smartess_password="",
                wants_refresh=False,
            )
            self._shadow_learning_state["support_package_path"] = str(path)
            self._shadow_learning_state["status"] = self._tr(
                "common.dynamic.shadow_learning_status_support_exported",
                "Support package exported without running control discovery.",
            )
            return None
        except Exception as exc:  # noqa: BLE001 - surfaced to the user as a form error
            self._shadow_learning_state["status"] = str(exc)
            return "shadow_learning_failed"

    def _control_discovery_placeholders(
        self,
        coordinator,
        hint_key: str,
        hint_default: str,
        *,
        hint_placeholders: dict[str, Any] | None = None,
        extra: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Build wizard description placeholders plus one step-specific hint.

        Reuses the existing shadow-learning placeholder set so the status table
        renders, and adds ``control_discovery_hint`` for the guided steps. The
        hint string lives in ``flow_translations``; wiring it into the rendered
        step templates (``translations/*.json``) is a follow-up.

        ``hint_placeholders`` lets a dynamic step (the review screen) format the
        hint with run-specific values (e.g. the discovered control count and a
        summary table). ``extra`` exposes those same values as standalone
        placeholders for templates that prefer to render them separately.
        """

        placeholders = dict(self._shadow_learning_placeholders(coordinator))
        placeholders["control_discovery_hint"] = self._tr(
            hint_key,
            hint_default,
            {**placeholders, **(hint_placeholders or {})},
        )
        if extra:
            placeholders.update(extra)
        return placeholders
