"""Options-flow UX for one coordinator-owned local register observation."""

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

from ...drivers.local_register_series import (
    DEFAULT_LOCAL_REGISTER_SERIES_INTERVAL_SECONDS,
    DEFAULT_LOCAL_REGISTER_SERIES_SAMPLE_COUNT,
    LocalRegisterSeriesPlan,
)
from ...support.local_register_collection import (
    LOCAL_REGISTER_COLLECTION_STATE_CANCELLED,
    LOCAL_REGISTER_COLLECTION_STATE_COMPLETE,
    LOCAL_REGISTER_COLLECTION_STATE_FAILED,
    LOCAL_REGISTER_COLLECTION_STATE_IDLE,
    LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
    LocalRegisterCollectionStatus,
)
from ..common.translation import with_translation_bundle as _with_translation_bundle


LOCAL_REGISTER_OBSERVATION_ACTION_CANCEL = "cancel"
LOCAL_REGISTER_OBSERVATION_ACTION_RESTART = "restart"
LOCAL_REGISTER_OBSERVATION_ACTION_DONE = "done"


class LocalRegisterObservationOptionsMixin:
    """Present and control the retained read-only evidence task."""

    @staticmethod
    def _local_register_observation_status(coordinator) -> LocalRegisterCollectionStatus:
        status = getattr(coordinator, "local_register_collection_status", None)
        return (
            status
            if type(status) is LocalRegisterCollectionStatus
            else LocalRegisterCollectionStatus.idle()
        )

    def _local_register_observation_visible(self, coordinator=None) -> bool:
        coordinator = coordinator or self._coordinator()
        if coordinator is None:
            return False
        return (
            self._local_register_observation_status(coordinator).state
            != LOCAL_REGISTER_COLLECTION_STATE_IDLE
        )

    @staticmethod
    def _local_register_observation_plan() -> LocalRegisterSeriesPlan:
        return LocalRegisterSeriesPlan(
            sample_count=DEFAULT_LOCAL_REGISTER_SERIES_SAMPLE_COUNT,
            sample_interval_seconds=DEFAULT_LOCAL_REGISTER_SERIES_INTERVAL_SECONDS,
        )

    def _start_local_register_observation(
        self,
        coordinator,
    ) -> LocalRegisterCollectionStatus:
        start = getattr(coordinator, "start_local_register_collection", None)
        if not callable(start):
            raise RuntimeError("local_register_collection_unavailable")
        result = start(self._local_register_observation_plan())
        if type(result) is not LocalRegisterCollectionStatus:
            raise TypeError("local_register_collection_status_invalid")
        return result

    def _local_register_observation_summary(
        self,
        coordinator,
    ) -> str:
        status = self._local_register_observation_status(coordinator)
        plan = status.plan or self._local_register_observation_plan()
        placeholders = {
            "done": str(status.completed_sample_count),
            "total": str(plan.sample_count),
            "minutes": str(max(1, plan.duration_seconds // 60)),
        }
        if status.state == LOCAL_REGISTER_COLLECTION_STATE_RUNNING:
            return self._tr(
                "common.dynamic.local_register_observation_running",
                "Local read-only observation is running ({done}/{total} snapshots).",
                placeholders,
            )
        if status.state == LOCAL_REGISTER_COLLECTION_STATE_COMPLETE:
            return self._tr(
                "common.dynamic.local_register_observation_complete",
                "Local read-only observation is complete ({total} snapshots).",
                placeholders,
            )
        if status.state == LOCAL_REGISTER_COLLECTION_STATE_FAILED:
            return self._tr(
                "common.dynamic.local_register_observation_failed",
                "Local read-only observation stopped before it could finish.",
                placeholders,
            )
        if status.state == LOCAL_REGISTER_COLLECTION_STATE_CANCELLED:
            return self._tr(
                "common.dynamic.local_register_observation_cancelled",
                "Local read-only observation was cancelled.",
                placeholders,
            )
        return self._tr(
            "common.dynamic.local_register_observation_available",
            "Optionally observe local readings for about {minutes} minutes in the "
            "background. This only gathers review evidence and adds no entities "
            "or controls.",
            placeholders,
        )

    @_with_translation_bundle
    async def async_step_local_register_observation(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Show, cancel, or restart the retained background observation."""

        coordinator = self._coordinator()
        if coordinator is None:
            return await self.async_step_init()
        status = self._local_register_observation_status(coordinator)
        if status.state == LOCAL_REGISTER_COLLECTION_STATE_IDLE:
            return await self.async_step_init()

        errors: dict[str, str] = {}
        if user_input is not None:
            action = user_input.get("local_register_observation_action")
            if type(action) is not str:
                errors["local_register_observation_action"] = "invalid_selection"
            elif action == LOCAL_REGISTER_OBSERVATION_ACTION_DONE:
                return await self.async_step_init()
            elif action == LOCAL_REGISTER_OBSERVATION_ACTION_CANCEL and status.active:
                cancel = getattr(
                    coordinator,
                    "async_cancel_local_register_collection",
                    None,
                )
                if not callable(cancel):
                    errors["base"] = "local_register_collection_unavailable"
                else:
                    await cancel()
                    status = self._local_register_observation_status(coordinator)
            elif action == LOCAL_REGISTER_OBSERVATION_ACTION_RESTART and not status.active:
                try:
                    status = self._start_local_register_observation(coordinator)
                except (TypeError, ValueError, RuntimeError):
                    errors["base"] = "local_register_collection_unavailable"
            else:
                errors["local_register_observation_action"] = "invalid_selection"

        options: list[SelectOptionDict] = []
        if status.active:
            options.append(
                SelectOptionDict(
                    value=LOCAL_REGISTER_OBSERVATION_ACTION_CANCEL,
                    label=self._tr(
                        "common.dynamic.local_register_observation_action_cancel",
                        "Stop observation",
                    ),
                )
            )
        else:
            options.append(
                SelectOptionDict(
                    value=LOCAL_REGISTER_OBSERVATION_ACTION_RESTART,
                    label=self._tr(
                        "common.dynamic.local_register_observation_action_restart",
                        "Start a new observation",
                    ),
                )
            )
        options.append(
            SelectOptionDict(
                value=LOCAL_REGISTER_OBSERVATION_ACTION_DONE,
                label=self._tr(
                    "common.dynamic.local_register_observation_action_done",
                    "Return to the menu",
                ),
            )
        )
        return self.async_show_form(
            step_id="local_register_observation",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        "local_register_observation_action",
                        default=options[0]["value"],
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=options,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders={
                "local_register_observation_summary": (
                    self._local_register_observation_summary(coordinator)
                )
            },
        )


__all__ = ["LocalRegisterObservationOptionsMixin"]
