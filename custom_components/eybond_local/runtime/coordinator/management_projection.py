"""Collector-management availability projections."""

from __future__ import annotations

from ...const import CONF_PROXY_CAPTURE_DURATION_MINUTES
from .tooling_projection import coerce_proxy_capture_duration_minutes as _coerce_proxy_capture_duration_minutes

_PENDING_MANAGED_ENDPOINT_SYNC_STATUSES: frozenset[str] = frozenset(
    {"applied", "waiting_for_collector", "cooldown"}
)


class CoordinatorManagementProjectionMixin:
    """Expose management availability without performing collector writes."""

    @property
    def proxy_capture_configured_duration_minutes(self) -> int:
        """Return the configured proxy capture duration in minutes."""

        return _coerce_proxy_capture_duration_minutes(
            self.config_entry.options.get(
                CONF_PROXY_CAPTURE_DURATION_MINUTES,
                self.config_entry.data.get(CONF_PROXY_CAPTURE_DURATION_MINUTES),
            )
        )

    @property
    def proxy_capture_remaining_seconds(self) -> int:
        """Return the last published active proxy capture remaining time."""

        values = self._proxy_capture_runtime_values()
        try:
            return max(0, int(float(values.get("proxy_capture_remaining_seconds") or 0)))
        except (TypeError, ValueError):
            return 0

    @property
    def proxy_capture_remaining_minutes(self) -> int:
        """Return remaining proxy capture minutes rounded up for UI controls."""

        seconds = self.proxy_capture_remaining_seconds
        if seconds <= 0:
            return 0
        return max(1, (seconds + 59) // 60)

    @property
    def proxy_capture_display_duration_minutes(self) -> int:
        """Return the number shown by runtime/UI controls."""

        if self.proxy_capture_overview.can_stop and self.proxy_capture_remaining_minutes > 0:
            return _coerce_proxy_capture_duration_minutes(self.proxy_capture_remaining_minutes)
        return self.proxy_capture_configured_duration_minutes

    def proxy_capture_duration_availability_reason(self) -> str | None:
        """Return why the proxy timer setting is temporarily unavailable."""

        overview = self.proxy_capture_overview
        if overview.critical_phase:
            return "proxy_capture_critical_phase"
        if overview.can_start or overview.can_stop:
            return None
        return str(overview.blocking_reason or "proxy_capture_not_ready")

    def _raise_if_high_level_collector_actions_disabled(self) -> None:
        """Reject high-level collector actions when the current write policy blocks them."""

        if not self.collector_actions_enabled:
            raise PermissionError(
                f"collector_control_disabled:{self.control_mode}:{self.controls_reason}"
            )

        lock_code = self.collector_configuration_lock_code()
        if lock_code is not None:
            raise RuntimeError(lock_code)

    def collector_endpoint_sync_lock_code(self) -> str | None:
        """Return one lock code while a managed endpoint change is still applying."""

        sync_status = str(
            self.data.values.get("collector_operation_endpoint_sync_status") or ""
        ).strip()
        if sync_status in _PENDING_MANAGED_ENDPOINT_SYNC_STATUSES:
            return "collector_endpoint_sync_pending"
        return None

    def collector_endpoint_sync_lock_reason(self) -> str | None:
        """Return a user-facing reason while a managed endpoint change is applying."""

        if self.collector_endpoint_sync_lock_code() is None:
            return None
        return (
            "Collector is applying the new connection endpoint. "
            "Wait for the collector to restart and reconnect."
        )

    def collector_configuration_lock_code(self) -> str | None:
        """Return one lock code while collector callback actions must stay blocked."""

        overview = self.proxy_capture_overview
        overview_status = str(getattr(overview, "status", "") or "").strip()
        if overview_status in {"starting", "stopping", "restoring"}:
            return "collector_configuration_proxy_transition_active"
        if overview_status == "running":
            return "collector_configuration_proxy_session_active"
        return self.collector_endpoint_sync_lock_code()

    def collector_configuration_lock_reason(self) -> str | None:
        """Return a user-facing reason while collector callback actions must stay blocked."""

        lock_code = self.collector_configuration_lock_code()
        if lock_code == "collector_configuration_proxy_transition_active":
            return (
                "Proxy capture is changing the collector callback. "
                "Wait for the transition to finish."
            )
        if lock_code == "collector_configuration_proxy_session_active":
            return "Stop proxy capture before changing collector callback actions."
        if lock_code == "collector_endpoint_sync_pending":
            return self.collector_endpoint_sync_lock_reason()
        return None

    def collector_management_capabilities(self):
        """Return the CURRENT collector-management capabilities from the runtime.

        Recomputed from the negotiated live wire each call, so it reflects a live
        handover/adoption immediately without a config-entry reload. Wire choice
        is the runtime's negotiated authority -- never collector kind, cloud
        provider or hostname.
        """

        getter = getattr(self._runtime, "collector_management_capabilities", None)
        if not callable(getter):
            return None
        try:
            return getter()
        except Exception:  # pragma: no cover - defensive
            return None

    def collector_management_action_available(self, action: str) -> bool:
        """Return whether one management ACTION is supported by the live adapter.

        ``action`` in {write_endpoint, apply_changes, reboot, read_endpoint_state}.
        Conflict/unknown/unavailable -> all False (fail closed).
        """

        capabilities = self.collector_management_capabilities()
        if capabilities is None:
            return False
        return bool(getattr(capabilities, action, False))



__all__ = ["CoordinatorManagementProjectionMixin"]
