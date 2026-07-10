"""Planning helpers for collector proxy capture UX and runtime gating."""

from __future__ import annotations

from dataclasses import dataclass

from .proxy_trace import ProxyCaptureSessionState


@dataclass(frozen=True, slots=True)
class ProxyCaptureOverview:
    """One normalized proxy capture runtime view for UI and coordinator state."""

    status: str
    status_label: str
    summary: str
    blocking_reason: str
    can_start: bool
    can_stop: bool
    critical_phase: bool
    redirect_required: bool
    collector_connected: bool
    current_endpoint: str
    upstream_endpoint: str
    target_endpoint: str
    masked_endpoint: str
    latest_trace_path: str
    latest_manifest_path: str


def build_proxy_capture_overview(
    *,
    control_mode: str,
    collector_control_allowed: bool = True,
    collector_proxy_capture_allowed: bool = True,
    collector_connected: bool,
    collector_cloud_family: str = "",
    current_endpoint: str,
    upstream_endpoint: str = "",
    target_endpoint: str,
    active_state: ProxyCaptureSessionState | None = None,
    latest_trace_path: str = "",
    latest_manifest_path: str = "",
) -> ProxyCaptureOverview:
    """Build one user-facing proxy capture readiness overview."""

    normalized_current = str(current_endpoint or "").strip()
    normalized_upstream = str(upstream_endpoint or "").strip()
    normalized_target = str(target_endpoint or "").strip()
    normalized_family = str(collector_cloud_family or "").strip().lower()
    normalized_trace_path = str(getattr(active_state, "trace_path", "") or latest_trace_path or "").strip()
    normalized_manifest_path = "" if active_state is not None else str(latest_manifest_path or "").strip()

    if active_state is not None:
        status = str(active_state.status or "running").strip() or "running"
        critical_phase = status in {"starting", "restoring", "stopping"}
        return ProxyCaptureOverview(
            status=status,
            status_label=_status_label(status),
            summary=_running_summary(status),
            blocking_reason="session_active",
            can_start=False,
            can_stop=not critical_phase,
            critical_phase=critical_phase,
            redirect_required=bool(active_state.restore_required),
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=str(active_state.original_endpoint or normalized_current).strip(),
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    redirect_required = bool(normalized_current and normalized_target and normalized_current != normalized_target)
    masked_endpoint = normalized_current or normalized_target

    if not normalized_target:
        return ProxyCaptureOverview(
            status="blocked",
            status_label=_status_label("blocked"),
            summary="Proxy capture target endpoint is not available for this entry.",
            blocking_reason="target_endpoint_unavailable",
            can_start=False,
            can_stop=False,
            critical_phase=False,
            redirect_required=redirect_required,
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=masked_endpoint,
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    if not normalized_current:
        return ProxyCaptureOverview(
            status="blocked",
            status_label=_status_label("blocked"),
            summary="The current collector callback endpoint is not available yet.",
            blocking_reason="current_endpoint_unavailable",
            can_start=False,
            can_stop=False,
            critical_phase=False,
            redirect_required=redirect_required,
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=masked_endpoint,
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    if not collector_proxy_capture_allowed:
        return ProxyCaptureOverview(
            status="blocked",
            status_label=_status_label("blocked"),
            summary="This collector has no SmartESS cloud callback side, so proxy capture is not available.",
            blocking_reason="collector_proxy_capture_unavailable",
            can_start=False,
            can_stop=False,
            critical_phase=False,
            redirect_required=redirect_required,
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=masked_endpoint,
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    if not normalized_upstream:
        return ProxyCaptureOverview(
            status="blocked",
            status_label=_status_label("blocked"),
            summary="The original upstream callback endpoint is not available, so proxy capture cannot be routed safely.",
            blocking_reason="upstream_endpoint_unavailable",
            can_start=False,
            can_stop=False,
            critical_phase=False,
            redirect_required=redirect_required,
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=masked_endpoint,
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    if not collector_connected:
        return ProxyCaptureOverview(
            status="blocked",
            status_label=_status_label("blocked"),
            summary="The collector is not connected, so proxy capture cannot start yet.",
            blocking_reason="collector_not_connected",
            can_start=False,
            can_stop=False,
            critical_phase=False,
            redirect_required=redirect_required,
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=masked_endpoint,
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    if redirect_required and not collector_control_allowed:
        return ProxyCaptureOverview(
            status="blocked",
            status_label=_status_label("blocked"),
            summary=(
                "Proxy capture requires a temporary callback redirect, but the current control policy "
                "does not allow collector-side changes."
            ),
            blocking_reason="collector_control_disabled",
            can_start=False,
            can_stop=False,
            critical_phase=False,
            redirect_required=redirect_required,
            collector_connected=collector_connected,
            current_endpoint=normalized_current,
            upstream_endpoint=normalized_upstream,
            target_endpoint=normalized_target,
            masked_endpoint=masked_endpoint,
            latest_trace_path=normalized_trace_path,
            latest_manifest_path=normalized_manifest_path,
        )

    if redirect_required:
        if normalized_family == "legacy_binary":
            summary = (
                "Collector proxy capture is ready. Starting a session will temporarily redirect the legacy binary callback profile."
            )
        elif normalized_family == "smartess_at":
            summary = (
                "Collector proxy capture is ready. Starting a session will temporarily redirect the AT bootstrap callback profile."
            )
        else:
            summary = (
                "Collector proxy capture is ready. Starting a session will temporarily redirect the callback endpoint."
            )
    else:
        summary = "Collector proxy capture is ready. No callback redirect is required for this session."
    return ProxyCaptureOverview(
        status="ready",
        status_label=_status_label("ready"),
        summary=summary,
        blocking_reason="",
        can_start=True,
        can_stop=False,
        critical_phase=False,
        redirect_required=redirect_required,
        collector_connected=collector_connected,
        current_endpoint=normalized_current,
        upstream_endpoint=normalized_upstream,
        target_endpoint=normalized_target,
        masked_endpoint=masked_endpoint,
        latest_trace_path=normalized_trace_path,
        latest_manifest_path=normalized_manifest_path,
    )


def _status_label(status: str) -> str:
    return {
        "ready": "Ready",
        "blocked": "Blocked",
        "starting": "Starting",
        "running": "Running",
        "stopping": "Stopping",
        "restoring": "Restoring",
    }.get(str(status or "").strip(), "Unknown")


def _running_summary(status: str) -> str:
    normalized = str(status or "").strip()
    if normalized == "starting":
        return "Proxy capture is starting and the collector callback endpoint may still be changing."
    if normalized == "restoring":
        return "Proxy capture is restoring the collector callback endpoint."
    if normalized == "stopping":
        return "Proxy capture is stopping and finalizing the trace artifact."
    return "Proxy capture is active and recording collector traffic."
