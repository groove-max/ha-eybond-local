"""Proxy capture and shadow-learning lifecycle for the runtime coordinator."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import ipaddress
import logging
from pathlib import Path
from typing import Any

from homeassistant.components import persistent_notification

from ...const import (
    CONF_COLLECTOR_IP,
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    DEFAULT_COLLECTOR_IP,
    DOMAIN,
    LOCAL_METADATA_DIR,
)
from ...metadata.collector_cloud_profile_catalog_loader import (
    resolve_collector_cloud_provider,
    resolve_collector_cloud_session_protocol,
)
from ...models import RuntimeSnapshot
from ...support.cloud_evidence_providers import (
    CloudEvidenceContext,
    cloud_evidence_provider_supported,
    resolve_cloud_evidence_provider,
)
from ...support.download import sign_proxy_capture_download_url
from ...support.memory_guard import read_available_memory_mib, shadow_learning_memory_blocker
from ...support.proxy_capture import (
    build_proxy_capture_overview,
    resolve_proxy_wire_mode,
)
from ...support.proxy_session import (
    build_proxy_capture_restore_trigger_path,
    build_proxy_capture_trace_path,
    inspect_proxy_capture_start_status,
    open_proxy_trace_output_file,
    summarize_proxy_capture_trace,
)
from ...support.proxy_trace import (
    build_proxy_capture_lease_deadline,
    build_proxy_capture_session_state,
    build_proxy_trace_manifest,
    clear_proxy_capture_session_state,
    export_proxy_trace_bundle,
    export_proxy_trace_manifest,
    load_latest_proxy_trace_manifest,
    load_proxy_capture_session_state,
    proxy_capture_restore_guard_reason,
    proxy_capture_session_is_active,
    proxy_capture_session_is_expired,
    refresh_proxy_capture_session_lease,
    save_proxy_capture_session_state,
)
from ...support.shadow_learning_backend import (
    build_shadow_learning_preflight,
    build_shadow_learning_seed,
    build_shadow_learning_trace_path,
)
from ...support.shadow_learning_proxy import route_status_indicates_control_ready
from ...support.shadow_learning_session import (
    build_shadow_learning_lease_deadline,
    build_shadow_learning_session_state,
    clear_shadow_learning_session_state,
    load_shadow_learning_session_state,
    save_shadow_learning_session_state,
    shadow_learning_session_is_active,
    shadow_learning_session_is_expired,
    shadow_learning_session_timestamp,
)
from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY
from .endpoint_projection import (
    local_source_ip_for_target as _local_source_ip_for_target,
    normalize_preserved_collector_server_endpoint as _normalize_preserved_collector_server_endpoint,
    parse_collector_server_endpoint as _parse_collector_server_endpoint,
    resolve_collector_server_endpoint as _resolve_collector_server_endpoint,
)
from .tooling_projection import (
    CloudToolEndpointContext as _CloudToolEndpointContext,
    PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS as _PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS,
    bounded_shadow_learning_artifact_path as _bounded_shadow_learning_artifact_path,
    coerce_proxy_capture_duration_minutes as _coerce_proxy_capture_duration_minutes,
    localized_runtime_text as _localized_runtime_text,
    proxy_capture_notification_id as _proxy_capture_notification_id,
    proxy_capture_remaining_seconds as _proxy_capture_remaining_seconds,
    proxy_capture_state_wire_mode as _proxy_capture_state_wire_mode,
)
from ..shadow_learning_facade import ShadowLearningRuntimeFacade

logger = logging.getLogger(__name__)


class CoordinatorCloudToolsMixin:
    """Own the shared proxy/shadow endpoint transaction lifecycle."""

    async def _async_disconnect_collector_for_cloud_tool_route(
        self,
        *,
        reason: str,
    ) -> None:
        """Hand the next collector callback to the active proxy/shadow route."""

        disconnect = getattr(
            self._runtime,
            "async_disconnect_collector_connections",
            None,
        )
        if not callable(disconnect):
            raise RuntimeError("collector_disconnect_unavailable")
        await disconnect(reason=reason)

    async def async_start_proxy_capture(
        self,
        *,
        anonymized: bool = True,
        confirm_redirect: bool = False,
        duration_minutes: int | None = None,
    ) -> dict[str, object]:
        """Start one live collector proxy capture session."""

        endpoint_sync_lock_code = self.collector_endpoint_sync_lock_code()
        if endpoint_sync_lock_code is not None:
            raise RuntimeError(endpoint_sync_lock_code)

        active_shadow_state = await self._async_active_shadow_learning_state(require_process=False)
        if active_shadow_state is not None and shadow_learning_session_is_active(active_shadow_state):
            raise RuntimeError("shadow_learning_route_running")
        if self._shadow_learning_process_running():
            raise RuntimeError("shadow_learning_route_running")
        if self._proxy_capture_process_running():
            raise RuntimeError("proxy_capture_already_running")
        if not self.collector_cloud_tools_allowed:
            raise RuntimeError("operating_profile_requires_cloud_and_ha")
        if not self.collector_capabilities.proxy_capture:
            raise RuntimeError("collector_proxy_capture_unavailable")
        if not self.collector_actions_enabled:
            raise RuntimeError("collector_control_disabled")

        # CP2C: own the ONE per-entry endpoint-operation authority for the WHOLE
        # active proxy mode. The exact live endpoint read below happens only after
        # acquiring this authority: reconnecting a callback session and changing
        # parameter 21 therefore cannot race another endpoint-owning operation.
        _proxy_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        route_owner_id = f"proxy_capture:{self.config_entry.entry_id}:{_proxy_timestamp}"
        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
            COLLECTOR_ENDPOINT_OPERATION_BUSY as _OP_BUSY,
            OPERATION_PROXY_CAPTURE as _OP_PROXY,
        )

        _op_acquire = _OP_AUTHORITY.acquire(
            self.config_entry.entry_id, _OP_PROXY, owner_ref=route_owner_id
        )
        if not _op_acquire.acquired:
            raise RuntimeError(_OP_BUSY)
        _op_hold = False
        try:
            endpoint_context = await self._async_prepare_cloud_tool_endpoint_context()
            overview = self._proxy_capture_overview_for_live_context(endpoint_context)
            if not overview.can_start:
                raise RuntimeError(
                    str(overview.blocking_reason or "proxy_capture_not_ready")
                )
            if overview.redirect_required and not confirm_redirect:
                raise ValueError("proxy_capture_redirect_requires_confirmation")

            upstream_host, upstream_port, _upstream_protocol = (
                _resolve_collector_server_endpoint(
                    endpoint_context.upstream_endpoint,
                    cloud_family=self.collector_cloud_family,
                )
            )
            target_host, target_port, _target_protocol = (
                _resolve_collector_server_endpoint(
                    endpoint_context.target_endpoint,
                    cloud_family=self.collector_cloud_family,
                )
            )
            configured_duration_minutes = _coerce_proxy_capture_duration_minutes(
                duration_minutes
                if duration_minutes is not None
                else self.proxy_capture_configured_duration_minutes
            )
            if configured_duration_minutes != self.proxy_capture_configured_duration_minutes:
                await self.async_set_proxy_capture_duration_minutes(configured_duration_minutes)

            cloud_session_protocol = resolve_collector_cloud_session_protocol(
                self.collector_cloud_family
            )
            proxy_wire_mode = resolve_proxy_wire_mode(
                self.collector_session_protocol,
                cloud_session_protocol,
            )
            if not proxy_wire_mode:
                raise RuntimeError("proxy_capture_wire_unsupported")
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            trace_path = await self.hass.async_add_executor_job(
                lambda: build_proxy_capture_trace_path(
                    config_dir=Path(self.hass.config.config_dir),
                    entry_id=self.config_entry.entry_id,
                    collector_pn=self.smartess_collector_pn,
                    timestamp=timestamp,
                )
            )
            restore_trigger_path = build_proxy_capture_restore_trigger_path(trace_path)
            try:
                await self.hass.async_add_executor_job(restore_trigger_path.unlink)
            except FileNotFoundError:
                pass
            started_at = datetime.now(timezone.utc).isoformat()
            state = build_proxy_capture_session_state(
                entry_id=self.config_entry.entry_id,
                route_owner_id=route_owner_id,
                collector_pn=self.smartess_collector_pn,
                trace_path=str(trace_path),
                original_endpoint=overview.current_endpoint,
                proxy_endpoint=overview.target_endpoint,
                restore_required=overview.redirect_required,
                anonymized=anonymized,
                started_at=started_at,
                expires_at=build_proxy_capture_lease_deadline(
                    lease_seconds=configured_duration_minutes * 60,
                ),
                status="starting",
                proxy_wire_mode=proxy_wire_mode,
            )
            endpoint_mutation_started = False
            try:
                # FIRST persistence lives INSIDE the finalized region: from here
                # the mode may OWN the endpoint, so a cancel during/after this save
                # is cleaned up by the shielded finalization below -- never a
                # persisted session left with a free authority.
                await self._async_save_proxy_capture_session_state(state)
                # No await between the save returning and this flag, so the "owned"
                # transition is atomic with the persistence.
                _op_hold = True
                self._publish_tooling_values(
                    **self._proxy_capture_overview_runtime_values(active_state=state),
                    proxy_trace_saved_result_path="",
                    proxy_trace_saved_result_download_url="",
                    proxy_trace_manifest_download_url="",
                    local_metadata_status="Starting collector proxy capture",
                )
                await self._async_preflight_proxy_capture_network(
                    target_host=target_host,
                    target_port=target_port,
                    upstream_host=upstream_host,
                    upstream_port=upstream_port,
                    validate_upstream=False,
                )

                async def _async_open_proxy_trace_output(path: Path):
                    return await self.hass.async_add_executor_job(
                        open_proxy_trace_output_file,
                        path,
                    )

                async def _async_close_proxy_trace_output(output):
                    await self.hass.async_add_executor_job(output.close)

                await self._runtime.async_start_proxy_capture_route(
                    owner_id=route_owner_id,
                    entry_id=self.config_entry.entry_id,
                    collector_ip=self._proxy_capture_collector_ip(),
                    collector_pn=self.smartess_collector_pn,
                    expected_session_protocol=cloud_session_protocol,
                    proxy_wire_mode=proxy_wire_mode,
                    listen_port=target_port,
                    upstream_host=upstream_host,
                    upstream_port=upstream_port,
                    output_path=trace_path,
                    masked_endpoint=endpoint_context.upstream_endpoint,
                    restore_trigger_path=restore_trigger_path,
                    async_open_output=_async_open_proxy_trace_output,
                    async_close_output=_async_close_proxy_trace_output,
                )
                if overview.redirect_required:
                    # From this exact point the wire call may have changed the
                    # collector even if cancellation/error prevents a result.
                    # Earlier failures only need route/state cleanup; writing the
                    # old endpoint before this boundary would be an unnecessary
                    # and potentially disruptive collector mutation.
                    endpoint_mutation_started = True
                    await self._runtime.async_set_collector_server_endpoint(
                        overview.target_endpoint,
                        apply_changes=True,
                    )
                await self._async_disconnect_collector_for_cloud_tool_route(
                    reason="proxy_capture_start"
                )
                await self._async_wait_for_proxy_capture_reconnect(trace_path)
                running_state = build_proxy_capture_session_state(
                    entry_id=state.entry_id,
                    route_owner_id=state.route_owner_id,
                    collector_pn=state.collector_pn,
                    trace_path=state.trace_path,
                    original_endpoint=state.original_endpoint,
                    proxy_endpoint=state.proxy_endpoint,
                    restore_required=state.restore_required,
                    anonymized=state.anonymized,
                    started_at=state.started_at,
                    expires_at=state.expires_at,
                    status="running",
                    proxy_wire_mode=_proxy_capture_state_wire_mode(state),
                )
                await self._async_save_proxy_capture_session_state(running_state)
            except BaseException as exc:
                # MANDATORY cancellation-safe cleanup (catches CancelledError too).
                # ONE shielded finalization boundary runs to completion even under
                # REPEATED cancellation: restore the endpoint, stop the route by
                # exact owner id, then clear+release (confirmed) OR persist a
                # recoverable state and hold the token.
                error_text = str(exc or "").strip()
                error_code = error_text.split(":", 1)[0] if error_text else type(exc).__name__
                _finalized, _pending_cancel = await self._run_finalization_shielded(
                    lambda: self._finalize_proxy_capture_start_cleanup(
                        state=state,
                        overview=overview,
                        endpoint_mutation_started=endpoint_mutation_started,
                        entry_id=self.config_entry.entry_id,
                        token=_op_acquire.token,
                    )
                )
                # The finalization now owns the token decision (released OR held),
                # so the outer finally must never release it again. The original
                # ``exc`` (including a CancelledError) is re-raised below, so an
                # extra cancel absorbed during finalization needs no separate
                # re-raise here.
                _op_hold = True
                try:
                    await self.async_request_refresh()
                except Exception as refresh_exc:
                    logger.warning(
                        "Proxy capture failure refresh failed for entry %s: %s",
                        self.config_entry.entry_id,
                        refresh_exc,
                    )
                self._publish_tooling_values(
                    **self._proxy_capture_overview_runtime_values(),
                    proxy_capture_start_error=error_text,
                    proxy_capture_start_error_code=error_code,
                    proxy_capture_start_error_type=type(exc).__name__,
                    local_metadata_status="Collector proxy capture failed to start",
                )
                # Re-raise the ORIGINAL (including CancelledError) only AFTER the
                # mandatory finalization above.
                raise

            await self.async_request_refresh()
            self._publish_tooling_values(
                **self._proxy_capture_overview_runtime_values(active_state=running_state),
                local_metadata_status="Collector proxy capture running",
            )
            # Already held since the first session persistence.
            return {
                "status": "running",
                "trace_path": str(trace_path),
                "redirect_required": overview.redirect_required,
                "masked_endpoint": overview.masked_endpoint,
                "duration_minutes": configured_duration_minutes,
            }
        finally:
            if not _op_hold:
                _OP_AUTHORITY.release(
                    self.config_entry.entry_id, _op_acquire.token
                )

    async def async_stop_proxy_capture(
        self,
        *,
        reason: str = "stopped",
        prefer_proxy_restore_trigger: bool = True,
        request_refresh: bool = True,
    ) -> dict[str, object]:
        """Serialize and stop one live collector proxy capture session."""

        lock = getattr(self, "_collector_endpoint_terminalization_lock", None)
        if lock is None:
            # Bare coordinator test harnesses bypass __init__.  Production
            # instances always take the initialized branch above.
            lock = asyncio.Lock()
            self._collector_endpoint_terminalization_lock = lock
        async with lock:
            return await self._async_stop_proxy_capture_once(
                reason=reason,
                prefer_proxy_restore_trigger=prefer_proxy_restore_trigger,
                request_refresh=request_refresh,
            )

    async def _async_stop_proxy_capture_once(
        self,
        *,
        reason: str,
        prefer_proxy_restore_trigger: bool,
        request_refresh: bool,
    ) -> dict[str, object]:
        """Stop one live collector proxy capture session and finalize its manifest."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None:
            raise RuntimeError("proxy_capture_not_running")

        # CP2C: stop is a CONTINUATION of the same ownership. Adopt the lease from
        # the persisted route owner id (re-acquiring it if a reload/restart lost
        # the in-memory token), so the restore runs under owned control. The lease
        # is released ONLY after the session state is cleared -- a stop that fails
        # before that keeps honest ownership for a retry, never silently dropping.
        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
            COLLECTOR_ENDPOINT_OPERATION_BUSY as _OP_BUSY,
            OPERATION_PROXY_CAPTURE as _OP_PROXY,
        )

        _op_token = _OP_AUTHORITY.adopt(
            self.config_entry.entry_id, _OP_PROXY, state.route_owner_id
        )
        # Ownership must be proven BEFORE the first state/wire mutation. adopt
        # returns None only when a FOREIGN operation owns the entry or the
        # persisted route owner id is empty/invalid -> refuse fail-closed with
        # zero state writes, route stop, restore or endpoint writes.
        if _op_token is None:
            raise RuntimeError(_OP_BUSY)

        config_dir = Path(self.hass.config.config_dir)
        stopping_state = build_proxy_capture_session_state(
            entry_id=state.entry_id,
            route_owner_id=state.route_owner_id,
            collector_pn=state.collector_pn,
            trace_path=state.trace_path,
            original_endpoint=state.original_endpoint,
            proxy_endpoint=state.proxy_endpoint,
            restore_required=state.restore_required,
            anonymized=state.anonymized,
            started_at=state.started_at,
            expires_at=state.expires_at,
            status="stopping",
            proxy_wire_mode=_proxy_capture_state_wire_mode(state),
        )
        await self._async_save_proxy_capture_session_state(stopping_state)
        self._publish_tooling_values(
            **self._proxy_capture_overview_runtime_values(active_state=stopping_state),
            local_metadata_status=self._proxy_capture_local_status(reason, phase="stopping")
        )

        restore_info = await self._async_guarded_proxy_capture_restore(
            state=state,
            prefer_proxy_restore_trigger=prefer_proxy_restore_trigger,
        )
        restored_endpoint = str(restore_info.get("restored_endpoint") or "")
        restore_confirmed = bool(restore_info.get("restore_confirmed"))
        restore_mode = str(restore_info.get("restore_mode") or "")
        restore_skipped_reason = str(restore_info.get("restore_skipped_reason") or "")
        restore_error = str(restore_info.get("restore_error") or "")
        current_endpoint = str(restore_info.get("current_endpoint") or "")

        if (
            state.restore_required
            and state.original_endpoint
            and restore_mode
            in {"proxy_trigger", "proxy_trigger_then_direct", "direct"}
        ):
            restoring_state = build_proxy_capture_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                restore_required=state.restore_required,
                anonymized=state.anonymized,
                started_at=state.started_at,
                expires_at=state.expires_at,
                status="restoring",
                proxy_wire_mode=_proxy_capture_state_wire_mode(state),
            )
            await self._async_save_proxy_capture_session_state(restoring_state)
            self._publish_tooling_values(
                **self._proxy_capture_overview_runtime_values(
                    active_state=restoring_state,
                    current_endpoint=current_endpoint,
                ),
                local_metadata_status=self._proxy_capture_local_status(reason, phase="stopping"),
            )

        trace_path = Path(state.trace_path)
        result_status = self._proxy_capture_result_status(reason, restore_confirmed=restore_confirmed)
        manifest_path = await self.hass.async_add_executor_job(
            lambda: export_proxy_trace_manifest(
                config_dir=config_dir,
                manifest=build_proxy_trace_manifest(
                    source="collector_proxy_capture",
                    trace_path=str(trace_path),
                    entry_id=self.config_entry.entry_id,
                    collector_pn=self.smartess_collector_pn,
                    anonymized=state.anonymized,
                    session={
                        "started_at": state.started_at,
                        "stopped_at": datetime.now(timezone.utc).isoformat(),
                        "original_endpoint": state.original_endpoint,
                        "proxy_endpoint": state.proxy_endpoint,
                        "current_endpoint": current_endpoint,
                        "restore_required": state.restore_required,
                        "restored_endpoint": restored_endpoint,
                        "restore_confirmed": restore_confirmed,
                        "restore_mode": restore_mode,
                        "restore_skipped_reason": restore_skipped_reason,
                        "restore_error": restore_error,
                        "final_status": result_status,
                    },
                    summary=summarize_proxy_capture_trace(trace_path),
                ),
            )
        )
        bundle_path = await self.hass.async_add_executor_job(
            lambda: export_proxy_trace_bundle(
                manifest_path=manifest_path,
                overwrite=True,
            )
        )
        download_url = sign_proxy_capture_download_url(
            self.hass,
            self.config_entry.entry_id,
            bundle_path.name,
        )
        if restore_confirmed:
            # Restore CONFIRMED: the mode no longer owns the endpoint -> clear the
            # session AND release the exact token as ONE cancellation-safe critical
            # step. Shielded, so a cancel can never split them into the forbidden
            # "record cleared + authority still held" pair.
            async def _clear_and_release() -> None:
                await self._async_clear_proxy_capture_session_state()
                if _op_token is not None:
                    _OP_AUTHORITY.release(self.config_entry.entry_id, _op_token)

            _cleared, _pending_cancel = await self._run_finalization_shielded(
                _clear_and_release
            )
            if _pending_cancel is not None:
                # The atomic clear+release already completed (record absent AND
                # authority free); now propagate the caller's cancellation.
                raise _pending_cancel
        else:
            # Restore NOT confirmed: keep a recoverable restoring state and HOLD
            # the token (never restore-unconfirmed + free authority). A retry stop
            # adopts the SAME token and repeats the cleanup; release happens only
            # after a confirmed restore.
            restoring_state = build_proxy_capture_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                restore_required=state.restore_required,
                anonymized=state.anonymized,
                started_at=state.started_at,
                expires_at=state.expires_at,
                status="restoring",
                proxy_wire_mode=_proxy_capture_state_wire_mode(state),
            )
            await self._async_save_proxy_capture_session_state(restoring_state)
        if request_refresh:
            await self.async_request_refresh()
        final_proxy_values = self._proxy_capture_overview_runtime_values(current_endpoint=current_endpoint)
        final_proxy_values["proxy_trace_path"] = str(trace_path)
        final_proxy_values["proxy_trace_manifest_path"] = str(manifest_path)
        self._publish_tooling_values(
            **final_proxy_values,
            proxy_trace_saved_result_path=str(bundle_path),
            proxy_trace_saved_result_download_url=download_url,
            proxy_trace_manifest_download_url=download_url,
            local_metadata_status=self._proxy_capture_local_status(reason, phase="finished"),
        )
        persistent_notification.async_create(
            self.hass,
            _localized_runtime_text(
                self.hass,
                "proxy_capture_notification_body" if download_url else "proxy_capture_notification_body_no_link",
                download_url=download_url,
                saved_path=str(bundle_path),
            ),
            title=_localized_runtime_text(self.hass, "proxy_capture_notification_title"),
            notification_id=_proxy_capture_notification_id(
                self.config_entry.entry_id,
                # One physical capture may need more than one restore attempt.
                # Keep one notification per trace so a retry replaces the prior
                # result instead of presenting duplicate "capture ready" cards.
                trace_path,
            ),
        )
        if not restore_confirmed:
            self._notify_proxy_capture_restore_unconfirmed()
        return {
            "status": result_status,
            "trace_path": str(trace_path),
            "manifest_path": str(manifest_path),
            "saved_result_path": str(bundle_path),
            "saved_result_download_url": download_url,
            "restored_endpoint": restored_endpoint,
            "restore_mode": restore_mode,
            "restore_skipped_reason": restore_skipped_reason,
            "restore_error": restore_error,
            "current_endpoint": current_endpoint,
        }

    async def async_start_shadow_learning(
        self,
        *,
        output_path: Path | None = None,
        raw_capture: dict[str, Any] | None = None,
        allow_ack_writes: bool = False,
    ) -> dict[str, object]:
        """Start one fail-closed shadow-learning runtime session."""

        if not self.collector_cloud_tools_allowed:
            raise RuntimeError("shadow_learning_requires_cloud_and_ha_profile")
        if not self.collector_actions_enabled:
            raise RuntimeError("shadow_learning_collector_control_disabled")
        active_proxy_state = await self._async_active_proxy_capture_state(require_process=False)
        if active_proxy_state is not None and proxy_capture_session_is_active(active_proxy_state):
            raise RuntimeError("proxy_capture_route_running")
        if self._runtime.proxy_capture_route_running():
            raise RuntimeError("proxy_capture_route_running")
        if self._shadow_learning_process_running():
            raise RuntimeError("shadow_learning_already_running")

        add_executor_job = getattr(
            getattr(self, "hass", None),
            "async_add_executor_job",
            None,
        )
        if callable(add_executor_job):
            available_mib = await add_executor_job(read_available_memory_mib)
        else:
            available_mib = read_available_memory_mib()
        memory_blocker = shadow_learning_memory_blocker(available_mib)
        if memory_blocker:
            raise RuntimeError(f"shadow_learning_preflight_blocked:{memory_blocker}")

        if raw_capture is None and self.data.connected:
            try:
                raw_capture = await self._runtime.async_capture_support_evidence()
            except Exception as exc:
                logger.debug("Shadow-learning support capture unavailable: %s", exc)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        trace_path = (
            Path(output_path)
            if output_path is not None
            else await self.hass.async_add_executor_job(
                lambda: build_shadow_learning_trace_path(
                    config_dir=Path(self.hass.config.config_dir),
                    entry_id=self.config_entry.entry_id,
                    collector_pn=self.smartess_collector_pn,
                    timestamp=timestamp,
                )
            )
        )

        session_id = f"{self.config_entry.entry_id}_{timestamp}"
        route_owner_id = f"shadow_learning:{session_id}"

        # Own the ONE per-entry endpoint-operation authority for the WHOLE active
        # shadow-learning transaction. A new Cloud + HA session may temporarily
        # redirect the endpoint, so reconcile/proxy/endpoint actions cannot
        # interfere with the learning proxy. Acquired fail-closed BEFORE the
        # first persistence/wire effect; the persisted route owner lets
        # reload/restart adopt the same token.
        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
            COLLECTOR_ENDPOINT_OPERATION_BUSY as _OP_BUSY,
            OPERATION_SHADOW_LEARNING as _OP_SHADOW,
        )

        _op_acquire = _OP_AUTHORITY.acquire(
            self.config_entry.entry_id, _OP_SHADOW, owner_ref=route_owner_id
        )
        if not _op_acquire.acquired:
            raise RuntimeError(_OP_BUSY)
        _op_hold = False
        try:
            endpoint_context = await self._async_prepare_cloud_tool_endpoint_context()
            seed, blockers = build_shadow_learning_seed(
                session_id=session_id,
                entry_id=self.config_entry.entry_id,
                collector_pn=self.smartess_collector_pn,
                collector_cloud_family=self.collector_cloud_family,
                raw_passthrough_frame_format=self.collector_raw_passthrough_frame_format,
                collector_cloud_profile_key=self.collector_cloud_profile_key,
                collector_cloud_profile_label=self.collector_cloud_profile_label,
                collector_cloud_profile_source=self.collector_cloud_profile_source,
                collector_cloud_profile_confidence=self.collector_cloud_profile_confidence,
                collector_callback_endpoint=endpoint_context.target_endpoint,
                effective_metadata_snapshot=self.shadow_learning_effective_metadata,
                raw_capture=raw_capture,
                write_response_mode="ack" if allow_ack_writes else "exception",
                allow_ack_writes=allow_ack_writes,
            )
            if blockers:
                raise RuntimeError(
                    "shadow_learning_preflight_blocked:" + ",".join(blockers)
                )
            preflight = build_shadow_learning_preflight(seed)
            if not preflight.can_start:
                raise RuntimeError("shadow_learning_preflight_blocked:" + ",".join(preflight.blockers))

            callback_endpoint = endpoint_context.target_endpoint
            upstream_endpoint = endpoint_context.upstream_endpoint

            _callback_host, callback_port, _callback_protocol = _resolve_collector_server_endpoint(
                callback_endpoint,
                cloud_family=self.collector_cloud_family,
            )
            upstream_host, upstream_port, _upstream_protocol = _resolve_collector_server_endpoint(
                upstream_endpoint,
                cloud_family=self.collector_cloud_family,
            )
            await self._async_preflight_proxy_capture_network(
                target_host=self._effective_callback_server_host,
                target_port=callback_port,
                upstream_host=upstream_host,
                upstream_port=upstream_port,
            )

            # Shadow learning and proxy capture share one product contract:
            # temporarily route cloud traffic through the HA listener under the
            # per-entry endpoint authority, then restore the exact cloud route.
            # The persisted state makes restoration recoverable across reloads.
            restore_endpoint = endpoint_context.current_endpoint
            restore_required = restore_endpoint != callback_endpoint
            started_at = shadow_learning_session_timestamp()
            expires_at = build_shadow_learning_lease_deadline(
                lease_seconds=self.proxy_capture_configured_duration_minutes * 60,
            )
            state = build_shadow_learning_session_state(
                entry_id=self.config_entry.entry_id,
                route_owner_id=route_owner_id,
                collector_pn=self.smartess_collector_pn,
                trace_path=str(trace_path),
                original_endpoint=restore_endpoint,
                proxy_endpoint=callback_endpoint,
                upstream_endpoint=upstream_endpoint,
                restore_required=restore_required,
                started_at=started_at,
                expires_at=expires_at,
                updated_at=started_at,
                status="preflight",
            )
            endpoint_mutation_started = False
            try:
                # FIRST persistence lives INSIDE the finalized region: from here
                # the mode may OWN the endpoint, so a cancel during/after this save
                # is cleaned up by the shielded finalization below -- never a
                # persisted session left with a free authority.
                await self._async_save_shadow_learning_session_state(state)
                # No await between the save returning and this flag, so the "owned"
                # transition is atomic with the persistence.
                _op_hold = True
                self._publish_tooling_values(
                    shadow_learning_session_status="preflight",
                    shadow_learning_session_ready=False,
                    shadow_learning_trace_path=str(trace_path),
                    shadow_learning_proxy_endpoint=callback_endpoint,
                    shadow_learning_upstream_endpoint=upstream_endpoint,
                    local_metadata_status="Starting shadow-learning route",
                )
                starting_state = build_shadow_learning_session_state(
                    entry_id=state.entry_id,
                    route_owner_id=state.route_owner_id,
                    collector_pn=state.collector_pn,
                    trace_path=state.trace_path,
                    original_endpoint=state.original_endpoint,
                    proxy_endpoint=state.proxy_endpoint,
                    upstream_endpoint=state.upstream_endpoint,
                    restore_required=state.restore_required,
                    started_at=state.started_at,
                    expires_at=state.expires_at,
                    updated_at=shadow_learning_session_timestamp(),
                    restore_attempt_count=state.restore_attempt_count,
                    last_restore_attempt_at=state.last_restore_attempt_at,
                    last_restore_error=state.last_restore_error,
                    status="starting",
                )
                await self._async_save_shadow_learning_session_state(starting_state)
                await self._runtime.async_start_shadow_learning_route(
                    owner_id=state.route_owner_id,
                    entry_id=state.entry_id,
                    collector_ip=self._proxy_capture_collector_ip(),
                    collector_pn=self.smartess_collector_pn,
                    expected_session_protocol=resolve_collector_cloud_session_protocol(
                        self.collector_cloud_family
                    ),
                    listen_port=callback_port,
                    upstream_host=upstream_host,
                    upstream_port=upstream_port,
                    output_path=trace_path,
                    seed=seed,
                )

                min_ready_sequence = 0
                if restore_required:
                    # From here the write may have changed the collector even if
                    # cancellation prevents a result. The shielded finalizer
                    # therefore owns the mandatory restore.
                    endpoint_mutation_started = True
                    await self._runtime.async_set_collector_server_endpoint(
                        callback_endpoint,
                        apply_changes=True,
                    )
                await self._async_disconnect_collector_for_cloud_tool_route(
                    reason="shadow_learning_start"
                )

                await self._async_wait_for_shadow_learning_ready(
                    trace_path=trace_path,
                    timeout_seconds=75.0,
                    min_collector_connection_sequence=min_ready_sequence,
                )
                ready_state = build_shadow_learning_session_state(
                    entry_id=state.entry_id,
                    route_owner_id=state.route_owner_id,
                    collector_pn=state.collector_pn,
                    trace_path=state.trace_path,
                    original_endpoint=state.original_endpoint,
                    proxy_endpoint=state.proxy_endpoint,
                    upstream_endpoint=state.upstream_endpoint,
                    restore_required=state.restore_required,
                    started_at=state.started_at,
                    expires_at=state.expires_at,
                    updated_at=shadow_learning_session_timestamp(),
                    restore_attempt_count=state.restore_attempt_count,
                    last_restore_attempt_at=state.last_restore_attempt_at,
                    last_restore_error=state.last_restore_error,
                    status="ready",
                )
                await self._async_save_shadow_learning_session_state(ready_state)
            except BaseException as exc:
                # MANDATORY cancellation-safe cleanup (catches CancelledError too).
                # ONE shielded finalization boundary runs to completion even under
                # REPEATED cancellation: stop the route by exact owner id, then
                # atomically clear the tentative state and release ownership.
                _released, _pending_cancel = await self._run_finalization_shielded(
                    lambda: self._finalize_shadow_learning_start_cleanup(
                        state=state,
                        endpoint_mutation_started=endpoint_mutation_started,
                        entry_id=self.config_entry.entry_id,
                        token=_op_acquire.token,
                    )
                )
                # The finalization now owns the token decision (released OR held),
                # so the outer finally must never release it again. The original
                # ``exc`` is re-raised below, so an extra cancel absorbed during
                # finalization needs no separate re-raise here.
                _op_hold = True
                try:
                    await self.async_request_refresh()
                except Exception as refresh_exc:
                    logger.warning(
                        "Shadow-learning failure refresh failed for entry %s: %s",
                        self.config_entry.entry_id,
                        refresh_exc,
                    )
                self._publish_tooling_values(
                    shadow_learning_session_status=(
                        "failed" if _released else "restore_failed"
                    ),
                    shadow_learning_session_ready=False,
                    local_metadata_status=(
                        "Shadow-learning route failed to start"
                        if _released
                        else "Shadow-learning endpoint restore requires attention"
                    ),
                )
                # Re-raise the ORIGINAL (including CancelledError) only AFTER the
                # mandatory finalization above.
                raise

            await self.async_request_refresh()
            self._publish_tooling_values(
                shadow_learning_session_status="ready",
                shadow_learning_session_ready=True,
                shadow_learning_trace_path=str(trace_path),
                shadow_learning_proxy_endpoint=callback_endpoint,
                shadow_learning_upstream_endpoint=upstream_endpoint,
                local_metadata_status="Shadow-learning route ready",
            )
            # Already held since the first session persistence.
            return {
                "status": "ready",
                "session_id": state.route_owner_id,
                "trace_path": str(trace_path),
                "collector_callback_endpoint": callback_endpoint,
                "upstream_endpoint": upstream_endpoint,
                "write_response_mode": seed.write_response_mode,
                "restore_required": restore_required,
            }
        finally:
            if not _op_hold:
                _OP_AUTHORITY.release(
                    self.config_entry.entry_id, _op_acquire.token
                )

    def publish_shadow_learning_artifacts(
        self,
        *,
        plan: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        correlation: dict[str, Any] | None = None,
        trace_path: str = "",
        profile_draft_path: str = "",
        schema_draft_path: str = "",
        activation: dict[str, Any] | None = None,
        session_id: str = "",
        device_scope: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish one sanitized shadow-learning artifact bundle for support export."""

        from ...support.package import build_shadow_learning_runtime_values

        config_dir = Path(self.hass.config.config_dir).resolve()
        normalized_trace_path = _bounded_shadow_learning_artifact_path(
            config_dir=config_dir,
            value=trace_path,
            relative_root=Path(LOCAL_METADATA_DIR) / "shadow_learning_traces",
        )
        normalized_profile_path = _bounded_shadow_learning_artifact_path(
            config_dir=config_dir,
            value=profile_draft_path,
            relative_root=Path(LOCAL_METADATA_DIR) / "profiles",
        )
        normalized_schema_path = _bounded_shadow_learning_artifact_path(
            config_dir=config_dir,
            value=schema_draft_path,
            relative_root=Path(LOCAL_METADATA_DIR) / "register_schemas",
        )
        published_values = build_shadow_learning_runtime_values(
            plan=plan,
            orchestration=orchestration,
            correlation=correlation,
            trace_path=normalized_trace_path,
            profile_draft_path=normalized_profile_path,
            schema_draft_path=normalized_schema_path,
            activation=activation,
            session_id=session_id,
            device_scope=device_scope,
        )
        self._publish_tooling_values(**published_values)
        return dict(published_values["shadow_learning_artifacts"])

    async def async_stop_shadow_learning(
        self,
        *,
        reason: str = "stopped",
        request_refresh: bool = True,
        raise_when_not_running: bool = True,
        clear_failed_restore: bool = False,
    ) -> dict[str, object]:
        """Serialize and stop one in-process shadow-learning session."""

        lock = getattr(self, "_collector_endpoint_terminalization_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._collector_endpoint_terminalization_lock = lock
        async with lock:
            return await self._async_stop_shadow_learning_once(
                reason=reason,
                request_refresh=request_refresh,
                raise_when_not_running=raise_when_not_running,
                clear_failed_restore=clear_failed_restore,
            )

    async def _async_stop_shadow_learning_once(
        self,
        *,
        reason: str,
        request_refresh: bool,
        raise_when_not_running: bool,
        clear_failed_restore: bool,
    ) -> dict[str, object]:
        """Stop one in-process shadow-learning session and restore collector endpoint."""

        state = await self._async_active_shadow_learning_state(require_process=False)
        if state is None and not self._shadow_learning_process_running():
            if raise_when_not_running:
                raise RuntimeError("shadow_learning_not_running")
            return {"status": "not_running"}

        route_owner_id = str(getattr(state, "route_owner_id", "") or "")
        # CP2C: stop continues the same ownership. Adopt the lease from the
        # persisted route owner id (re-acquiring after a reload/restart) so the
        # restore runs under owned control; it is released ONLY when the session
        # is finalized/cleared. A failed restore keeps the degraded session AND
        # its ownership for a retry -- never silently dropped.
        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
            COLLECTOR_ENDPOINT_OPERATION_BUSY as _OP_BUSY,
            OPERATION_SHADOW_LEARNING as _OP_SHADOW,
        )

        _op_token = _OP_AUTHORITY.adopt(
            self.config_entry.entry_id, _OP_SHADOW, route_owner_id
        )
        # With a live session, ownership must be proven BEFORE the first mutation:
        # adopt returns None only for a FOREIGN owner or an invalid persisted owner
        # id -> refuse fail-closed, zero mutation. (An orphan process without a
        # session has no owner id and is cleaned up without a lease.)
        if state is not None and _op_token is None:
            raise RuntimeError(_OP_BUSY)
        if state is not None:
            stopping_state = build_shadow_learning_session_state(
                entry_id=state.entry_id,
                route_owner_id=route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                upstream_endpoint=state.upstream_endpoint,
                restore_required=state.restore_required,
                started_at=state.started_at,
                expires_at=state.expires_at,
                updated_at=shadow_learning_session_timestamp(),
                restore_attempt_count=state.restore_attempt_count,
                last_restore_attempt_at=state.last_restore_attempt_at,
                last_restore_error=state.last_restore_error,
                status="restoring",
            )
            await self._async_save_shadow_learning_session_state(stopping_state)

        if route_owner_id:
            await self._runtime.async_stop_shadow_learning_route(
                owner_id=route_owner_id,
            )
        else:
            await self._runtime.async_stop_shadow_learning_route()

        restored_endpoint = ""
        restore_confirmed = True
        restore_error = ""
        restore_attempt_at = ""
        if state is not None and state.restore_required and state.original_endpoint:
            restore_attempt_at = shadow_learning_session_timestamp()
            try:
                restored_endpoint = await self._async_restore_proxy_capture_endpoint(
                    state.original_endpoint
                )
                verification = (
                    await self._async_verify_restored_collector_endpoint(
                        state.original_endpoint
                    )
                )
                restore_confirmed = bool(
                    verification.get("restore_confirmed")
                )
                restored_endpoint = str(
                    verification.get("observed_endpoint") or restored_endpoint
                )
                restore_error = str(
                    verification.get("restore_error") or ""
                )
                if not restore_confirmed:
                    self._notify_proxy_capture_restore_unconfirmed()
            except Exception as exc:
                restore_confirmed = False
                restore_error = str(exc)
                logger.warning(
                    "Shadow-learning restore failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
                self._notify_proxy_capture_restore_unconfirmed()

        if restore_confirmed or clear_failed_restore or state is None or not state.restore_required:
            # Mode finalized: clear the session AND release the exact token as ONE
            # cancellation-safe critical step (never "record cleared + authority
            # still held").
            async def _clear_and_release() -> None:
                await self._async_clear_shadow_learning_session_state()
                if _op_token is not None:
                    _OP_AUTHORITY.release(self.config_entry.entry_id, _op_token)

            _cleared, _pending_cancel = await self._run_finalization_shielded(
                _clear_and_release
            )
            if _pending_cancel is not None:
                # The atomic clear+release already completed (record absent AND
                # authority free); now propagate the caller's cancellation.
                raise _pending_cancel
        else:
            failed_state = build_shadow_learning_session_state(
                entry_id=state.entry_id,
                route_owner_id=route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                upstream_endpoint=state.upstream_endpoint,
                restore_required=state.restore_required,
                started_at=state.started_at,
                expires_at=state.expires_at,
                updated_at=shadow_learning_session_timestamp(),
                restore_attempt_count=state.restore_attempt_count + 1,
                last_restore_attempt_at=restore_attempt_at,
                last_restore_error=restore_error,
                status="restore_failed",
            )
            await self._async_save_shadow_learning_session_state(failed_state)
        if request_refresh:
            await self.async_request_refresh()
        self._publish_tooling_values(
            shadow_learning_session_status="stopped" if restore_confirmed else "restore_failed",
            shadow_learning_session_ready=False,
            local_metadata_status="Shadow-learning route stopped",
        )
        return {
            "status": "stopped" if restore_confirmed else "restore_unconfirmed",
            "reason": str(reason or "stopped"),
            "restored_endpoint": restored_endpoint,
            "restore_confirmed": restore_confirmed,
        }

    async def async_touch_proxy_capture_lease(self, *, extend: bool = True) -> str:
        """Publish active proxy-session countdown values and optionally refresh the lease."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None or not proxy_capture_session_is_active(state):
            return ""
        if self._proxy_capture_state_needs_reconcile(state):
            self._cancel_proxy_capture_deadline_refresh()
            await self.async_request_refresh()
            return ""

        published_state = state
        if extend:
            published_state = refresh_proxy_capture_session_lease(
                state,
                lease_seconds=self.proxy_capture_configured_duration_minutes * 60,
            )
            await self._async_save_proxy_capture_session_state(published_state)
        self._schedule_proxy_capture_deadline_refresh(published_state.expires_at)
        self._publish_tooling_values(
            **self._proxy_capture_overview_runtime_values(active_state=published_state)
        )
        return published_state.expires_at

    async def async_set_proxy_capture_duration_minutes(self, value: object) -> int:
        """Persist proxy capture duration and update the active session deadline explicitly."""

        duration_minutes = _coerce_proxy_capture_duration_minutes(value)
        options = dict(self.config_entry.options)
        if options.get(CONF_PROXY_CAPTURE_DURATION_MINUTES) != duration_minutes:
            options[CONF_PROXY_CAPTURE_DURATION_MINUTES] = duration_minutes
            self._async_update_entry_without_reload(options=options)

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is not None and proxy_capture_session_is_active(state):
            if self._proxy_capture_state_needs_reconcile(state):
                self._cancel_proxy_capture_deadline_refresh()
                await self.async_request_refresh()
                self._publish_tooling_values(**self._proxy_capture_timer_runtime_values(None))
                return duration_minutes
            updated_state = build_proxy_capture_session_state(
                entry_id=state.entry_id,
                route_owner_id=state.route_owner_id,
                collector_pn=state.collector_pn,
                trace_path=state.trace_path,
                original_endpoint=state.original_endpoint,
                proxy_endpoint=state.proxy_endpoint,
                restore_required=state.restore_required,
                anonymized=state.anonymized,
                started_at=state.started_at,
                expires_at=build_proxy_capture_lease_deadline(
                    lease_seconds=duration_minutes * 60,
                ),
                status=state.status,
                proxy_wire_mode=_proxy_capture_state_wire_mode(state),
            )
            await self._async_save_proxy_capture_session_state(updated_state)
            self._schedule_proxy_capture_deadline_refresh(updated_state.expires_at)
            self._publish_tooling_values(
                **self._proxy_capture_overview_runtime_values(active_state=updated_state)
            )
        else:
            self._cancel_proxy_capture_deadline_refresh()
            self._publish_tooling_values(**self._proxy_capture_timer_runtime_values(None))
        return duration_minutes

    @property
    def cloud_evidence_provider(self) -> str:
        """Return the account/cloud provider used for support cloud evidence.

        Provider RESOLUTION (cloud family -> provider id) is catalog-driven and
        provider-neutral; the coordinator only reads the resolved id.
        """

        return resolve_collector_cloud_provider(self.collector_cloud_family)

    @property
    def cloud_evidence_export_available(self) -> bool:
        """Return whether provider-specific cloud evidence can be attempted.

        Asks the neutral provider REGISTRY whether the resolved provider is
        supported (no hardcoded allow-list in the coordinator) and a collector PN
        is available. Deliberately lightweight -- it never resolves effective
        metadata, so it is safe to call from sync config-flow form rendering.
        """

        return bool(self.smartess_collector_pn) and cloud_evidence_provider_supported(
            self.cloud_evidence_provider
        )

    def _cloud_evidence_provider_impl(self):
        """Return the neutral cloud-evidence provider implementation (registry)."""

        return resolve_cloud_evidence_provider(self.cloud_evidence_provider)

    def _cloud_evidence_context(self) -> CloudEvidenceContext:
        """Assemble the neutral cloud-evidence context from live runtime/config state.

        A bag of already-resolved DATA the active provider may read; the
        coordinator interprets none of it. No peer IP / hostname / collector kind.
        """

        hass = getattr(self, "hass", None)
        config = getattr(hass, "config", None)
        # ``config_dir`` is only consumed by export/draft (which always have a live
        # hass); a resolve-only path (draft-plan property) may run without one.
        config_dir = Path(str(getattr(config, "config_dir", "") or "."))
        collector = getattr(self.data, "collector", None)
        entry_data = getattr(self.config_entry, "data", {}) or {}
        # Gather the explicit protocol hints (data, not policy) once, here, so the
        # provider receives normalized fields instead of the raw collector /
        # config-entry mapping.
        protocol_asset_id = str(
            getattr(collector, "smartess_protocol_asset_id", "")
            or entry_data.get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "")
            or ""
        ).strip()
        protocol_profile_key = str(
            getattr(collector, "smartess_protocol_profile_key", "")
            or entry_data.get(CONF_SMARTESS_PROFILE_KEY, "")
            or ""
        ).strip()
        return CloudEvidenceContext(
            config_dir=config_dir,
            entry_id=self.config_entry.entry_id,
            collector_pn=self.smartess_collector_pn,
            protocol_asset_id=protocol_asset_id,
            protocol_profile_key=protocol_profile_key,
            effective_owner_key=self.effective_owner_key,
            effective_profile_name=self.effective_profile_name,
            effective_register_schema_name=self.effective_register_schema_name,
            effective_profile_path=str(
                getattr(self.effective_profile_metadata, "source_path", "") or ""
            ),
            effective_register_schema_path=str(
                getattr(self.effective_register_schema_metadata, "source_path", "") or ""
            ),
        )

    def _cloud_evidence_draft_candidate(self, kind: str):
        """Return the neutral draft candidate of one kind, or None."""

        return self._cloud_evidence_provider_impl().draft_candidate(
            self._cloud_evidence_context(),
            self._latest_smartess_cloud_evidence_record(),
            kind,
        )

    @property
    def smartess_cloud_evidence_path(self) -> str:
        """Return the latest saved SmartESS cloud evidence path for this entry."""

        record = self._latest_smartess_cloud_evidence_record()
        return str(record.path) if record is not None else ""

    @property
    def latest_proxy_trace_path(self) -> str:
        """Return the latest saved proxy trace data path for this entry."""

        values = self._proxy_capture_runtime_values()
        return str(values.get("proxy_trace_path") or "").strip()

    @property
    def latest_proxy_trace_manifest_path(self) -> str:
        """Return the latest saved proxy trace manifest path for this entry."""

        values = self._proxy_capture_runtime_values()
        return str(values.get("proxy_trace_manifest_path") or "").strip()

    @property
    def proxy_capture_overview(self):
        """Return one normalized proxy capture runtime overview."""

        snapshot = self.data
        state = self._active_proxy_capture_state()
        values = self._proxy_capture_runtime_values()
        return build_proxy_capture_overview(
            control_mode=self.control_mode,
            collector_control_allowed=self.collector_actions_enabled,
            collector_proxy_capture_allowed=self.collector_capabilities.proxy_capture,
            collector_connected=bool(snapshot.connected),
            cloud_tools_allowed=self.collector_cloud_tools_allowed,
            collector_cloud_family=self.collector_cloud_family,
            collector_session_protocol=self.collector_session_protocol,
            cloud_session_protocol=resolve_collector_cloud_session_protocol(
                self.collector_cloud_family
            ),
            current_endpoint=str(
                values.get("collector_server_endpoint")
                or snapshot.collector_server_endpoint
                or ""
            ),
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            target_endpoint=self.proxy_capture_target_endpoint,
            active_state=state,
            latest_trace_path=self.latest_proxy_trace_path,
            latest_manifest_path=self.latest_proxy_trace_manifest_path,
        )

    async def _async_recover_proxy_capture_state(self) -> None:
        """Best-effort restore collector callback state after an interrupted session."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None or not proxy_capture_session_is_active(state):
            return
        logger.warning(
            "Recovering interrupted proxy capture for entry %s with state %s",
            self.config_entry.entry_id,
            state.status,
        )
        try:
            await self.async_stop_proxy_capture(
                reason="recovered_after_restart",
                prefer_proxy_restore_trigger=False,
                request_refresh=False,
            )
        except Exception as exc:
            # CP2C: a failed recovery must NOT unconditionally clear the session.
            # The recoverable state stays and the authority stays owned by this
            # route owner (the stop already re-adopted it / holds it), so another
            # endpoint operation gets busy and a later recovery/stop can finish the
            # restore. Clearing here would strand the endpoint with a free
            # authority.
            logger.warning("Proxy capture recovery failed for entry %s: %s", self.config_entry.entry_id, exc)
            self._notify_proxy_capture_restore_unconfirmed()

    async def _async_recover_shadow_learning_state(self) -> None:
        """Best-effort restore collector callback state after interrupted shadow learning."""

        state = await self._async_active_shadow_learning_state(require_process=False)
        recoverable_status = str(getattr(state, "status", "") or "").strip()
        if state is None or (
            not shadow_learning_session_is_active(state)
            and recoverable_status != "restore_failed"
        ):
            return
        logger.warning(
            "Recovering interrupted shadow-learning session for entry %s with state %s",
            self.config_entry.entry_id,
            state.status,
        )
        try:
            stop_reason = (
                "expired_lease"
                if shadow_learning_session_is_expired(state)
                else "recovered_after_restart"
            )
            await self.async_stop_shadow_learning(
                reason=stop_reason,
                request_refresh=False,
                raise_when_not_running=False,
            )
        except Exception as exc:
            logger.warning(
                "Shadow-learning recovery failed for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            self._notify_proxy_capture_restore_unconfirmed()

    async def _async_stop_proxy_capture_process(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active shared-ingress proxy capture route when it exists."""

        stop_route = getattr(self._runtime, "async_stop_proxy_capture_route", None)
        if stop_route is not None:
            if owner_id or force:
                await stop_route(owner_id=owner_id, force=force)
            else:
                await stop_route()

    async def _async_restore_proxy_capture_endpoint(self, endpoint: str) -> str:
        """Restore one collector callback endpoint captured before proxy redirect."""

        _parse_collector_server_endpoint(endpoint)
        disconnect = getattr(
            self._runtime,
            "async_disconnect_collector_connections",
            None,
        )
        if callable(disconnect):
            await disconnect(reason="collector_endpoint_restore")
        result = await self._runtime.async_set_collector_server_endpoint(
            endpoint,
            apply_changes=True,
            timeout=(
                DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait
            ),
            require_heartbeat=False,
        )
        return str(result.get("readback_endpoint") or endpoint)

    async def _async_verify_restored_collector_endpoint(
        self,
        expected_endpoint: str,
    ) -> dict[str, object]:
        """Verify a restored endpoint on a newly usable entry-owned session.

        A write/apply response proves only that the collector acknowledged the
        command.  It does not prove that parameter 21 survived the ensuing
        disconnect.  The runtime connection boundary sends at most one callback
        request, waits for the session owned by this entry's durable PN claim,
        then reads the endpoint from that exact live management session.
        """

        try:
            normalized_expected = _normalize_preserved_collector_server_endpoint(
                expected_endpoint
            )
            expected_route = _resolve_collector_server_endpoint(
                normalized_expected,
                cloud_family=self.collector_cloud_family,
            )
        except (TypeError, ValueError) as exc:
            return {
                "restore_confirmed": False,
                "observed_endpoint": "",
                "restore_error": f"restore_expected_endpoint_invalid:{exc}",
            }

        try:
            result = await self._runtime.async_get_collector_server_endpoint_state(
                timeout=(
                    DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait
                ),
                # The management read below is the active liveness proof.  A
                # fully-silent collector must not be rejected merely because it
                # emits no unsolicited heartbeat on the restored callback.
                require_heartbeat=False,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Unable to verify restored collector endpoint for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            return {
                "restore_confirmed": False,
                "observed_endpoint": "",
                "restore_error": f"restore_live_endpoint_unavailable:{exc}",
            }

        if type(result) is not dict:
            return {
                "restore_confirmed": False,
                "observed_endpoint": "",
                "restore_error": "restore_live_endpoint_result_invalid",
            }
        raw_observed = result.get("current_endpoint")
        if (
            type(raw_observed) is not str
            or not raw_observed
            or raw_observed != raw_observed.strip()
        ):
            return {
                "restore_confirmed": False,
                "observed_endpoint": "",
                "restore_error": "restore_live_endpoint_unavailable",
            }
        try:
            normalized_observed = _normalize_preserved_collector_server_endpoint(
                raw_observed
            )
            observed_route = _resolve_collector_server_endpoint(
                normalized_observed,
                cloud_family=self.collector_cloud_family,
            )
        except (TypeError, ValueError) as exc:
            return {
                "restore_confirmed": False,
                "observed_endpoint": "",
                "restore_error": f"restore_live_endpoint_invalid:{exc}",
            }
        if observed_route != expected_route:
            return {
                "restore_confirmed": False,
                "observed_endpoint": normalized_observed,
                "restore_error": "restore_live_endpoint_mismatch",
            }
        return {
            "restore_confirmed": True,
            "observed_endpoint": normalized_observed,
            "restore_error": "",
        }

    async def _async_read_live_collector_server_endpoint(self) -> str:
        """Return the latest collector endpoint, preferring a direct live management read."""

        fallback = self.data.collector_server_endpoint
        try:
            result = await self._runtime.async_get_collector_server_endpoint_state()
        except Exception as exc:
            logger.warning(
                "Unable to read live collector endpoint for proxy capture safeguard on entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            return fallback
        return str(result.get("current_endpoint") or fallback or "").strip()

    async def _async_prepare_cloud_tool_endpoint_context(
        self,
    ) -> _CloudToolEndpointContext:
        """Resolve the exact live route shared by proxy and shadow learning.

        Both tools are temporary endpoint transactions. They must start from one
        runtime-owned collector session, preserve the endpoint read from that
        exact session, route through one HA callback target, and restore the
        preserved endpoint on every terminal path.
        """

        try:
            result = await self._runtime.async_get_collector_server_endpoint_state()
        except Exception as exc:
            raise RuntimeError("cloud_tool_collector_not_connected") from exc
        if type(result) is not dict:
            raise RuntimeError("cloud_tool_current_endpoint_unavailable")
        raw_current = result.get("current_endpoint")
        if (
            type(raw_current) is not str
            or not raw_current
            or raw_current != raw_current.strip()
        ):
            raise RuntimeError("cloud_tool_current_endpoint_unavailable")
        try:
            current_endpoint = _normalize_preserved_collector_server_endpoint(
                raw_current
            )
            _resolve_collector_server_endpoint(
                current_endpoint,
                cloud_family=self.collector_cloud_family,
            )
        except ValueError as exc:
            raise RuntimeError("cloud_tool_current_endpoint_unavailable") from exc

        # Publish the exact live read before resolving upstream/target properties;
        # both properties intentionally consume this snapshot and therefore make
        # the same decision for proxy capture and shadow learning.
        self._publish_snapshot_values(
            collector_server_endpoint=current_endpoint,
        )
        upstream_endpoint = self.proxy_capture_upstream_endpoint
        target_endpoint = self.proxy_capture_target_endpoint
        if not upstream_endpoint:
            raise RuntimeError("cloud_tool_upstream_endpoint_unavailable")
        if not target_endpoint:
            raise RuntimeError("cloud_tool_target_endpoint_unavailable")
        try:
            _resolve_collector_server_endpoint(
                upstream_endpoint,
                cloud_family=self.collector_cloud_family,
            )
            _resolve_collector_server_endpoint(
                target_endpoint,
                cloud_family=self.collector_cloud_family,
            )
        except ValueError as exc:
            raise RuntimeError("cloud_tool_endpoint_context_invalid") from exc
        return _CloudToolEndpointContext(
            current_endpoint=current_endpoint,
            upstream_endpoint=upstream_endpoint,
            target_endpoint=target_endpoint,
        )

    def _proxy_capture_overview_for_live_context(
        self,
        context: _CloudToolEndpointContext,
    ):
        """Build proxy readiness from the same exact endpoint context as shadow."""

        return build_proxy_capture_overview(
            control_mode=self.control_mode,
            collector_control_allowed=self.collector_actions_enabled,
            collector_proxy_capture_allowed=self.collector_capabilities.proxy_capture,
            collector_connected=True,
            cloud_tools_allowed=self.collector_cloud_tools_allowed,
            collector_cloud_family=self.collector_cloud_family,
            collector_session_protocol=self.collector_session_protocol,
            cloud_session_protocol=resolve_collector_cloud_session_protocol(
                self.collector_cloud_family
            ),
            current_endpoint=context.current_endpoint,
            upstream_endpoint=context.upstream_endpoint,
            target_endpoint=context.target_endpoint,
            active_state=None,
            latest_trace_path=self.latest_proxy_trace_path,
            latest_manifest_path=self.latest_proxy_trace_manifest_path,
        )

    async def _async_preflight_proxy_capture_network(
        self,
        *,
        target_host: str,
        target_port: int,
        upstream_host: str,
        upstream_port: int,
        validate_upstream: bool = True,
    ) -> None:
        """Fail early when the temporary cloud route is clearly unsafe.

        Proxy capture reserves its real upstream connection in the in-process
        handler before changing the collector endpoint. Its caller disables the
        throwaway upstream probe so the cloud sees only the session that will
        carry collector traffic. Shadow learning retains the explicit upstream
        reachability preflight.
        """

        await self._async_validate_proxy_capture_target(target_host=target_host, target_port=target_port)
        if validate_upstream:
            await self._async_validate_proxy_capture_upstream(
                upstream_host=upstream_host,
                upstream_port=upstream_port,
            )

    async def _async_validate_proxy_capture_upstream(self, *, upstream_host: str, upstream_port: int) -> None:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(upstream_host, upstream_port),
                timeout=5.0,
            )
        except Exception as exc:
            raise RuntimeError(f"proxy_capture_upstream_unreachable:{type(exc).__name__}:{exc}") from exc
        writer.close()
        await writer.wait_closed()
        del reader

    async def _async_validate_proxy_capture_target(self, *, target_host: str, target_port: int) -> None:
        try:
            target_ip = ipaddress.ip_address(target_host)
        except ValueError:
            return
        if target_ip.is_loopback or target_ip.is_unspecified:
            raise RuntimeError("proxy_capture_target_not_reachable_from_collector_lan:loopback_or_unspecified")

        collector_ip = str(self.config_entry.data.get(CONF_COLLECTOR_IP) or "").strip()
        if not collector_ip or collector_ip == DEFAULT_COLLECTOR_IP:
            return
        try:
            collector_addr = ipaddress.ip_address(collector_ip)
        except ValueError:
            return
        if not (target_ip.is_private and collector_addr.is_private):
            return

        source_ip = await self.hass.async_add_executor_job(_local_source_ip_for_target, collector_ip)
        if not source_ip:
            return
        if source_ip != target_host:
            raise RuntimeError(
                "proxy_capture_target_not_reachable_from_collector_lan:"
                f"target={target_host}:{target_port}:source={source_ip}:"
                "use_collector_callback_endpoint_override_or_external_transport"
            )

    async def _async_wait_for_proxy_capture_reconnect(self, trace_path: Path) -> None:
        deadline = asyncio.get_running_loop().time() + 75.0
        identity_verified = False
        while asyncio.get_running_loop().time() < deadline:
            if not self._proxy_capture_process_running():
                raise RuntimeError("proxy_capture_route_stopped")
            status = await self.hass.async_add_executor_job(
                lambda: inspect_proxy_capture_start_status(trace_path)
            )
            upstream_error = str(status.get("upstream_error") or "")
            if upstream_error:
                raise RuntimeError(f"proxy_capture_upstream_connect_failed:{upstream_error}")
            if status.get("identity_mismatch"):
                raise RuntimeError("proxy_capture_collector_identity_mismatch")
            identity_verified = bool(status.get("identity_verified"))
            if (
                status.get("connected")
                and identity_verified
                and status.get("operational_activity")
            ):
                return
            await asyncio.sleep(1.0)
        if identity_verified:
            raise TimeoutError("proxy_capture_cloud_session_not_ready")
        raise TimeoutError("proxy_capture_collector_identity_timeout")

    async def _async_trigger_proxy_capture_restore(
        self,
        *,
        trace_path: Path,
        owner_id: str,
    ) -> bool:
        trigger_path = build_proxy_capture_restore_trigger_path(trace_path)
        await self.hass.async_add_executor_job(
            lambda: trigger_path.write_text(
                datetime.now(timezone.utc).isoformat() + "\n",
                encoding="utf-8",
            )
        )
        deadline = asyncio.get_running_loop().time() + 20.0
        while asyncio.get_running_loop().time() < deadline:
            status = await self.hass.async_add_executor_job(
                lambda: inspect_proxy_capture_start_status(trace_path)
            )
            if status.get("restore_acknowledged"):
                try:
                    await self.hass.async_add_executor_job(trigger_path.unlink)
                except FileNotFoundError:
                    pass
                await self._async_stop_proxy_capture_process(owner_id=owner_id)
                return True
            if status.get("restore_missing"):
                break
            await asyncio.sleep(0.5)
        await self._async_stop_proxy_capture_process(owner_id=owner_id)
        return False

    async def _async_best_effort_restore_after_start_failure(self, endpoint: str) -> tuple[bool, str]:
        try:
            await self._async_restore_proxy_capture_endpoint(endpoint)
            verification = await self._async_verify_restored_collector_endpoint(
                endpoint
            )
            if verification.get("restore_confirmed"):
                return True, ""
            error = str(
                verification.get("restore_error")
                or "restore_live_endpoint_unconfirmed"
            )
            self._notify_proxy_capture_restore_unconfirmed()
            return False, error
        except Exception as exc:
            logger.warning("Proxy capture start rollback failed for entry %s: %s", self.config_entry.entry_id, exc)
            self._notify_proxy_capture_restore_unconfirmed()
            return False, str(exc)

    async def _run_finalization_shielded(self, factory):
        """Run a finalization coroutine to completion even under REPEATED cancellation.

        The mandatory endpoint-ownership cleanup of an interrupted long-lived
        start/stop MUST NOT be interrupted half-way -- a cancel landing between
        "route stopped / endpoint restored" and "state cleared + token released"
        would strand the collector. This creates ONE task and awaits it through
        ``asyncio.shield`` in a loop, so a cancel of the caller (even more than
        once) cannot break the cleanup mid-way; it always runs to the end. It is
        NOT fire-and-forget: the task is always awaited here.

        Returns ``(result, pending_cancel)`` where ``pending_cancel`` is the
        absorbed ``CancelledError`` (or ``None``). The caller MUST re-raise it once
        its own state/token invariant is finalized, so the original cancellation
        still propagates.
        """

        task = asyncio.ensure_future(factory())
        pending_cancel: BaseException | None = None
        while True:
            try:
                return await asyncio.shield(task), pending_cancel
            except asyncio.CancelledError as cancel_exc:
                # Absorb the caller's cancel (even repeated) but remember it so the
                # caller can re-raise once the cleanup has fully finished.
                pending_cancel = cancel_exc
                if task.done():
                    # The cleanup itself finished; its result/exception stands.
                    return task.result(), pending_cancel
                # A (repeated) cancel arrived mid-cleanup -> keep awaiting it.
                continue

    async def _finalize_proxy_capture_start_cleanup(
        self,
        *,
        state,
        overview,
        endpoint_mutation_started: bool,
        entry_id: str,
        token,
    ) -> bool:
        """The ONE mandatory cleanup for an interrupted proxy-capture start.

        Runs (always, idempotently) inside a shielded boundary: restore the
        endpoint if a redirect may have been applied, stop the route by the EXACT
        owner id even when the route-start await never returned, then EITHER clear
        the session and release the exact token (confirmed restore) OR persist a
        recoverable ``restoring`` state and keep the token. Returns True when it
        released the token, False when it kept ownership. Never leaves a persisted
        session with a free authority, nor a cleared session with a held token.
        """

        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
        )

        # Blocker 1: a route-start await that was cancelled before returning may
        # still have created the route, so stop it by the EXACT owner id
        # unconditionally (idempotent when no such route exists).  The route is
        # stopped BEFORE the restore transaction so the callback session used
        # for live postcondition verification belongs to the normal runtime,
        # never to the temporary proxy handler.
        try:
            await self._async_stop_proxy_capture_process(owner_id=state.route_owner_id)
        except Exception as exc:  # best-effort: a missing route must not abort cleanup
            logger.warning(
                "Proxy capture start cleanup route-stop failed for entry %s: %s",
                entry_id,
                exc,
            )
        restored = not (overview.redirect_required and endpoint_mutation_started)
        if overview.redirect_required and endpoint_mutation_started:
            if overview.current_endpoint:
                restored, _restore_error = (
                    await self._async_best_effort_restore_after_start_failure(
                        overview.current_endpoint
                    )
                )
            else:
                restored = False
        if restored:
            # Confirmed back to the original endpoint: clear the session AND
            # release the exact token as ONE critical step (this whole coroutine
            # runs shielded, so the pair cannot be split by a cancel).
            await self._async_clear_proxy_capture_session_state()
            _OP_AUTHORITY.release(entry_id, token)
            return True
        restoring_state = build_proxy_capture_session_state(
            entry_id=state.entry_id,
            route_owner_id=state.route_owner_id,
            collector_pn=state.collector_pn,
            trace_path=state.trace_path,
            original_endpoint=state.original_endpoint,
            proxy_endpoint=state.proxy_endpoint,
            restore_required=state.restore_required,
            anonymized=state.anonymized,
            started_at=state.started_at,
            expires_at=state.expires_at,
            status="restoring",
            proxy_wire_mode=_proxy_capture_state_wire_mode(state),
        )
        await self._async_save_proxy_capture_session_state(restoring_state)
        return False

    async def _finalize_shadow_learning_start_cleanup(
        self,
        *,
        state,
        endpoint_mutation_started: bool,
        entry_id: str,
        token,
    ) -> bool:
        """The ONE mandatory cleanup for an interrupted shadow-learning start.

        Restore the original cloud endpoint when a redirect may have started,
        stop the route by exact owner id unconditionally, then either clear the
        tentative state and release the exact token or persist a recoverable
        restore-failed state while retaining ownership.
        """

        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
        )

        # Release the temporary route before reconnecting through the normal
        # entry-owned runtime for endpoint write + live postcondition read.
        try:
            await self._runtime.async_stop_shadow_learning_route(
                owner_id=state.route_owner_id,
            )
        except Exception as exc:  # best-effort: a missing route must not abort cleanup
            logger.warning(
                "Shadow-learning start cleanup route-stop failed for entry %s: %s",
                entry_id,
                exc,
            )
        restored = not (state.restore_required and endpoint_mutation_started)
        restore_error = ""
        if state.restore_required and endpoint_mutation_started:
            if state.original_endpoint:
                restored, restore_error = (
                    await self._async_best_effort_restore_after_start_failure(
                        state.original_endpoint
                    )
                )
            else:
                restored = False
                restore_error = "shadow_learning_original_endpoint_unavailable"
        if restored:
            await self._async_clear_shadow_learning_session_state()
            _OP_AUTHORITY.release(entry_id, token)
            return True
        attempt_at = shadow_learning_session_timestamp()
        restore_failed_state = build_shadow_learning_session_state(
            entry_id=state.entry_id,
            route_owner_id=state.route_owner_id,
            collector_pn=state.collector_pn,
            trace_path=state.trace_path,
            original_endpoint=state.original_endpoint,
            proxy_endpoint=state.proxy_endpoint,
            upstream_endpoint=state.upstream_endpoint,
            restore_required=state.restore_required,
            started_at=state.started_at,
            expires_at=state.expires_at,
            updated_at=attempt_at,
            restore_attempt_count=state.restore_attempt_count + 1,
            last_restore_attempt_at=attempt_at,
            last_restore_error=restore_error,
            status="restore_failed",
        )
        await self._async_save_shadow_learning_session_state(restore_failed_state)
        return False

    async def _async_reconcile_proxy_capture_session(
        self,
        snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """Auto-stop abandoned proxy sessions on lease expiry or after proxy loss."""

        state = await self._async_active_proxy_capture_state(require_process=False)
        if state is None or not proxy_capture_session_is_active(state):
            return snapshot

        stop_reason = ""
        if proxy_capture_session_is_expired(state):
            stop_reason = "expired_lease"
        elif (
            state.status == "running"
            and not self._proxy_capture_process_running()
        ):
            stop_reason = "interrupted_process_exit"

        if not stop_reason:
            return snapshot

        logger.warning(
            "Stopping proxy capture for entry %s due to %s",
            self.config_entry.entry_id,
            stop_reason,
        )
        await self.async_stop_proxy_capture(
            reason=stop_reason,
            prefer_proxy_restore_trigger=stop_reason == "expired_lease",
            request_refresh=False,
        )
        self._ensure_poll_scheduler()
        return await self._runtime.async_refresh(
            poll_interval=self._poll_scheduler.current_interval()
        )

    async def _async_reconcile_shadow_learning_session(
        self,
        snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """Auto-stop abandoned shadow-learning sessions on lease expiry or route interruption."""

        state = await self._async_active_shadow_learning_state(require_process=False)
        if state is None or not shadow_learning_session_is_active(state):
            return snapshot

        stop_reason = ""
        if shadow_learning_session_is_expired(state):
            stop_reason = "expired_lease"
        elif (
            state.status
            in {
                "waiting_for_collector",
                "connecting_upstream",
                "ready",
                "learning",
                "degraded",
            }
            and not self._shadow_learning_process_running()
        ):
            stop_reason = "interrupted_process_exit"

        if not stop_reason:
            return snapshot

        logger.warning(
            "Stopping shadow learning for entry %s due to %s",
            self.config_entry.entry_id,
            stop_reason,
        )
        await self.async_stop_shadow_learning(
            reason=stop_reason,
            request_refresh=False,
            raise_when_not_running=False,
        )
        self._ensure_poll_scheduler()
        return await self._runtime.async_refresh(
            poll_interval=self._poll_scheduler.current_interval()
        )

    def _proxy_capture_process_running(self) -> bool:
        route_running = getattr(self._runtime, "proxy_capture_route_running", None)
        return bool(route_running is not None and route_running())

    def _shadow_learning_process_running(self) -> bool:
        route_running = getattr(self._runtime, "shadow_learning_route_running", None)
        return bool(route_running is not None and route_running())

    def _shadow_learning_route_status(self) -> dict[str, object]:
        route_status = getattr(self._runtime, "shadow_learning_route_status", None)
        if route_status is None:
            return {
                "running": self._shadow_learning_process_running(),
                "collector_connected": False,
                "collector_connection_sequence": 0,
                "collector_protocol_ingress": False,
                "route_protocol_activity": False,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }
        status = route_status()
        if not isinstance(status, dict):
            return {
                "running": self._shadow_learning_process_running(),
                "collector_connected": False,
                "collector_connection_sequence": 0,
                "collector_protocol_ingress": False,
                "route_protocol_activity": False,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }
        return {
            "running": bool(status.get("running")),
            "collector_connected": bool(status.get("collector_connected")),
            "collector_connection_sequence": int(status.get("collector_connection_sequence") or 0),
            "collector_protocol_ingress": bool(status.get("collector_protocol_ingress")),
            "route_protocol_activity": bool(status.get("route_protocol_activity")),
            "upstream_connected": bool(status.get("upstream_connected")),
            "ready": bool(status.get("ready")),
            "upstream_error": str(status.get("upstream_error") or ""),
        }

    @property
    def shadow_learning_runtime(self) -> ShadowLearningRuntimeFacade:
        """Return the cohesive public facade for shadow-learning consumers."""

        return ShadowLearningRuntimeFacade(
            runtime=self._runtime,
            cloud_evidence_provider=self._latest_smartess_cloud_evidence_record,
        )

    async def _async_wait_for_shadow_learning_ready(
        self,
        *,
        trace_path: Path,
        timeout_seconds: float,
        min_collector_connection_sequence: int = 0,
    ) -> None:
        del trace_path
        deadline = asyncio.get_running_loop().time() + max(float(timeout_seconds), 1.0)
        phase = "waiting_for_collector"
        while asyncio.get_running_loop().time() < deadline:
            if not self._shadow_learning_process_running():
                raise RuntimeError("shadow_learning_route_stopped")
            status = self._shadow_learning_route_status()
            upstream_error = str(status.get("upstream_error") or "")
            if upstream_error:
                raise RuntimeError(f"shadow_learning_upstream_connect_failed:{upstream_error}")
            # Return the instant the collector has reconnected to our proxy and is speaking our
            # protocol -- the same moment proxy capture keys off. Do NOT wait for the full
            # ``ready`` flag: it additionally requires the short-lived upstream proxy->cloud
            # socket, which connects on demand, so waiting for it false-timed-out here and
            # triggered a premature restore of the collector back to the real server. This is the
            # SAME predicate the per-write control gate uses, so start and gate never disagree.
            if route_status_indicates_control_ready(status):
                if (
                    min_collector_connection_sequence > 0
                    and int(status.get("collector_connection_sequence") or 0)
                    <= min_collector_connection_sequence
                ):
                    await asyncio.sleep(1.0)
                    continue
                return
            collector_connected = bool(status.get("collector_connected"))
            next_phase = "connecting_upstream" if collector_connected else "waiting_for_collector"
            if next_phase != phase:
                phase = next_phase
                state = await self._async_active_shadow_learning_state(require_process=False)
                if state is not None:
                    await self._async_save_shadow_learning_session_state(
                        build_shadow_learning_session_state(
                            entry_id=state.entry_id,
                            route_owner_id=state.route_owner_id,
                            collector_pn=state.collector_pn,
                            trace_path=state.trace_path,
                            original_endpoint=state.original_endpoint,
                            proxy_endpoint=state.proxy_endpoint,
                            upstream_endpoint=state.upstream_endpoint,
                            restore_required=state.restore_required,
                            started_at=state.started_at,
                            expires_at=state.expires_at,
                            updated_at=shadow_learning_session_timestamp(),
                            restore_attempt_count=state.restore_attempt_count,
                            last_restore_attempt_at=state.last_restore_attempt_at,
                            last_restore_error=state.last_restore_error,
                            status=phase,
                        )
                    )
                self._publish_tooling_values(
                    shadow_learning_session_status=phase,
                    shadow_learning_session_ready=False,
                    local_metadata_status=(
                        "Shadow-learning waiting for collector"
                        if phase == "waiting_for_collector"
                        else "Shadow-learning connecting upstream"
                    ),
                )
            await asyncio.sleep(1.0)
        raise TimeoutError("shadow_learning_collector_reconnect_timeout")

    async def _async_guarded_proxy_capture_restore(
        self,
        *,
        state,
        prefer_proxy_restore_trigger: bool,
    ) -> dict[str, object]:
        """Restore the collector callback only while the proxy still owns the endpoint."""

        route_stopped = False

        async def _stop_route_once() -> None:
            nonlocal route_stopped
            if route_stopped:
                return
            await self._async_stop_proxy_capture_process(
                owner_id=state.route_owner_id
            )
            route_stopped = True

        current_endpoint = await self._async_read_live_collector_server_endpoint()
        restore_skipped_reason = proxy_capture_restore_guard_reason(
            state,
            current_endpoint=current_endpoint,
        )
        if not state.restore_required or not state.original_endpoint:
            await _stop_route_once()
            return {
                "current_endpoint": current_endpoint,
                "restored_endpoint": current_endpoint,
                "restore_confirmed": True,
                "restore_mode": "not_required",
                "restore_skipped_reason": "",
            }

        # A positively observed endpoint different from the proxy route may be
        # an external/user change.  Never overwrite it automatically; stop the
        # temporary route and accept it only if a live read proves it is exactly
        # the original endpoint.  "Unavailable" is not evidence of an external
        # change and must continue into the owned restore transaction.
        if restore_skipped_reason == "current_endpoint_changed":
            await _stop_route_once()
            verification = await self._async_verify_restored_collector_endpoint(
                state.original_endpoint
            )
            return {
                "current_endpoint": current_endpoint,
                "restored_endpoint": str(
                    verification.get("observed_endpoint") or current_endpoint
                ),
                "restore_confirmed": bool(
                    verification.get("restore_confirmed")
                ),
                "restore_mode": "skipped_verified",
                "restore_skipped_reason": restore_skipped_reason,
                "restore_error": str(verification.get("restore_error") or ""),
            }

        proxy_restore_acknowledged = False
        if prefer_proxy_restore_trigger and self._proxy_capture_process_running():
            proxy_restore_acknowledged = (
                await self._async_trigger_proxy_capture_restore(
                    trace_path=Path(state.trace_path),
                    owner_id=state.route_owner_id,
                )
            )
            if proxy_restore_acknowledged:
                verification = await self._async_verify_restored_collector_endpoint(
                    state.original_endpoint
                )
                if verification.get("restore_confirmed"):
                    return {
                        "current_endpoint": current_endpoint,
                        "restored_endpoint": str(
                            verification.get("observed_endpoint") or ""
                        ),
                        "restore_confirmed": True,
                        "restore_mode": "proxy_trigger",
                        "restore_skipped_reason": "",
                        "restore_error": "",
                    }
                # ACK without a post-apply read is not terminal. Continue through
                # the normal runtime callback path and perform an explicit
                # restore+verify transaction.

            current_endpoint = await self._async_read_live_collector_server_endpoint()
            restore_skipped_reason = proxy_capture_restore_guard_reason(
                state,
                current_endpoint=current_endpoint,
            )
            if restore_skipped_reason == "current_endpoint_changed":
                verification = await self._async_verify_restored_collector_endpoint(
                    state.original_endpoint
                )
                return {
                    "current_endpoint": current_endpoint,
                    "restored_endpoint": str(
                        verification.get("observed_endpoint") or current_endpoint
                    ),
                    "restore_confirmed": bool(
                        verification.get("restore_confirmed")
                    ),
                    "restore_mode": "skipped_verified",
                    "restore_skipped_reason": restore_skipped_reason,
                    "restore_error": str(verification.get("restore_error") or ""),
                }

        # The direct transaction must use the normal entry-owned runtime, never
        # a socket still reserved by the temporary proxy route.
        await _stop_route_once()
        try:
            restored_endpoint = await self._async_restore_proxy_capture_endpoint(state.original_endpoint)
        except Exception as exc:
            logger.warning("Proxy capture direct restore failed for entry %s: %s", self.config_entry.entry_id, exc)
            return {
                "current_endpoint": current_endpoint,
                "restored_endpoint": current_endpoint,
                "restore_confirmed": False,
                "restore_mode": "direct_failed",
                "restore_skipped_reason": "",
                "restore_error": str(exc),
            }

        verification = await self._async_verify_restored_collector_endpoint(
            state.original_endpoint
        )
        return {
            "current_endpoint": current_endpoint,
            "restored_endpoint": str(
                verification.get("observed_endpoint") or restored_endpoint
            ),
            "restore_confirmed": bool(verification.get("restore_confirmed")),
            "restore_mode": (
                "proxy_trigger_then_direct"
                if proxy_restore_acknowledged
                else "direct"
            ),
            "restore_skipped_reason": "",
            "restore_error": str(verification.get("restore_error") or ""),
        }

    def _proxy_capture_result_status(self, reason: str, *, restore_confirmed: bool) -> str:
        normalized_reason = str(reason or "stopped").strip() or "stopped"
        if restore_confirmed:
            return {
                "expired_lease": "expired_stopped",
                "recovered_after_restart": "recovered_after_restart",
                "interrupted_process_exit": "recovered_after_process_exit",
            }.get(normalized_reason, "stopped")
        return {
            "expired_lease": "expired_restore_unconfirmed",
            "recovered_after_restart": "recovered_after_restart_restore_unconfirmed",
            "interrupted_process_exit": "recovered_after_process_exit_restore_unconfirmed",
        }.get(normalized_reason, "stopped_restore_unconfirmed")

    def _proxy_capture_local_status(self, reason: str, *, phase: str) -> str:
        normalized_reason = str(reason or "stopped").strip() or "stopped"
        if phase == "stopping":
            return "Stopping collector proxy capture"
        return {
            "recovered_after_restart": "Recovered interrupted collector proxy capture",
            "interrupted_process_exit": "Recovered interrupted collector proxy capture",
        }.get(normalized_reason, "Collector proxy capture stopped")

    def _notify_proxy_capture_restore_unconfirmed(self) -> None:
        persistent_notification.async_create(
            self.hass,
            _localized_runtime_text(self.hass, "proxy_capture_restore_unconfirmed_body"),
            title=_localized_runtime_text(self.hass, "proxy_capture_restore_unconfirmed_title"),
            notification_id=f"{DOMAIN}_proxy_capture_restore_unconfirmed_{self.config_entry.entry_id}",
        )

    def _latest_proxy_trace_record(self):
        """Return the latest proxy-trace manifest record for this entry."""

        return load_latest_proxy_trace_manifest(
            Path(self.hass.config.config_dir),
            entry_id=self.config_entry.entry_id,
            collector_pn=self.smartess_collector_pn,
        )

    async def _async_latest_proxy_trace_record(self):
        """Return the latest proxy-trace manifest record for this entry without blocking."""

        return await self.hass.async_add_executor_job(
            lambda: load_latest_proxy_trace_manifest(
                Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                collector_pn=self.smartess_collector_pn,
            )
        )

    def _active_proxy_capture_state(self, *, require_process: bool = True):
        """Return the last persisted proxy capture session state cached by async paths."""

        del require_process
        cached_state = getattr(self, "_cached_proxy_capture_session_state", None)
        if cached_state is not None:
            return cached_state
        return None

    async def _async_active_proxy_capture_state(self, *, require_process: bool = True):
        """Return the persisted active proxy capture state when it belongs to this entry."""

        del require_process
        state = await self.hass.async_add_executor_job(
            lambda: load_proxy_capture_session_state(Path(self.hass.config.config_dir))
        )
        if state is None:
            self._cached_proxy_capture_session_state = None
            return None
        if state.entry_id and state.entry_id != self.config_entry.entry_id:
            self._cached_proxy_capture_session_state = None
            return None
        collector_pn = self.smartess_collector_pn
        if collector_pn and state.collector_pn and state.collector_pn != collector_pn:
            self._cached_proxy_capture_session_state = None
            return None
        self._cached_proxy_capture_session_state = state
        return state

    async def _async_save_proxy_capture_session_state(self, state) -> None:
        """Persist one proxy capture session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: save_proxy_capture_session_state(
                config_dir=Path(self.hass.config.config_dir),
                state=state,
            )
        )
        self._cached_proxy_capture_session_state = state
        if proxy_capture_session_is_active(state):
            self._schedule_proxy_capture_deadline_refresh(state.expires_at)
        else:
            self._cancel_proxy_capture_deadline_refresh()

    async def _async_clear_proxy_capture_session_state(self) -> None:
        """Delete persisted proxy capture session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: clear_proxy_capture_session_state(Path(self.hass.config.config_dir))
        )

        self._cached_proxy_capture_session_state = None
        self._cancel_proxy_capture_deadline_refresh()
        self._clear_proxy_capture_session_runtime_values()

    async def _async_active_shadow_learning_state(self, *, require_process: bool = True):
        """Return the persisted active shadow-learning state when it belongs to this entry."""

        del require_process
        if self._shadow_learning_session_state_loaded:
            # Authoritative in-memory cache: save/clear keep it fresh and this
            # coordinator is the only writer, so skip the per-refresh disk read.
            return self._cached_shadow_learning_session_state
        state = await self.hass.async_add_executor_job(
            lambda: load_shadow_learning_session_state(Path(self.hass.config.config_dir))
        )
        self._shadow_learning_session_state_loaded = True
        if state is None:
            self._cached_shadow_learning_session_state = None
            return None
        if state.entry_id and state.entry_id != self.config_entry.entry_id:
            self._cached_shadow_learning_session_state = None
            return None
        collector_pn = self.smartess_collector_pn
        if collector_pn and state.collector_pn and state.collector_pn != collector_pn:
            self._cached_shadow_learning_session_state = None
            return None
        self._cached_shadow_learning_session_state = state
        return state

    async def _async_save_shadow_learning_session_state(self, state) -> None:
        """Persist one shadow-learning session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: save_shadow_learning_session_state(
                config_dir=Path(self.hass.config.config_dir),
                state=state,
            )
        )
        self._cached_shadow_learning_session_state = state
        self._shadow_learning_session_state_loaded = True

    async def _async_clear_shadow_learning_session_state(self) -> None:
        """Delete persisted shadow-learning session state without blocking the event loop."""

        await self.hass.async_add_executor_job(
            lambda: clear_shadow_learning_session_state(Path(self.hass.config.config_dir))
        )
        self._cached_shadow_learning_session_state = None
        self._shadow_learning_session_state_loaded = True

    def _clear_proxy_capture_session_runtime_values(self) -> None:
        """Drop stale transient proxy-session values from both cache and current snapshot."""

        snapshot_values = getattr(self.data, "values", None)
        for key in _PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS:
            self._tooling_values.pop(key, None)
            if isinstance(snapshot_values, dict):
                snapshot_values.pop(key, None)

    def _proxy_capture_runtime_values(self) -> dict[str, Any]:
        """Return current proxy-capture UI values with snapshot data preferred over tooling cache."""

        values = dict(getattr(self, "_tooling_values", {}))
        values.update(getattr(self.data, "values", {}) or {})
        return values

    def _proxy_capture_timer_runtime_values(self, state=None) -> dict[str, Any]:
        """Return proxy capture duration and countdown runtime values."""

        remaining_seconds = 0
        if state is not None:
            remaining_seconds = _proxy_capture_remaining_seconds(getattr(state, "expires_at", ""))
        remaining_minutes = max(1, (remaining_seconds + 59) // 60) if remaining_seconds > 0 else 0
        return {
            CONF_PROXY_CAPTURE_DURATION_MINUTES: self.proxy_capture_configured_duration_minutes,
            "proxy_capture_remaining_seconds": remaining_seconds,
            "proxy_capture_remaining_minutes": remaining_minutes,
        }

    def _proxy_capture_overview_runtime_values(
        self,
        *,
        active_state=None,
        current_endpoint: str = "",
    ) -> dict[str, Any]:
        """Build immediate proxy-capture runtime values for transition-aware entity UX."""

        snapshot = self.data
        runtime_values = self._proxy_capture_runtime_values()
        overview = build_proxy_capture_overview(
            control_mode=self.control_mode,
            collector_control_allowed=self.collector_actions_enabled,
            collector_proxy_capture_allowed=self.collector_capabilities.proxy_capture,
            collector_connected=bool(snapshot.connected),
            cloud_tools_allowed=self.collector_cloud_tools_allowed,
            collector_cloud_family=self.collector_cloud_family,
            current_endpoint=str(
                current_endpoint
                or runtime_values.get("collector_server_endpoint")
                or snapshot.collector_server_endpoint
                or ""
            ),
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            target_endpoint=self.proxy_capture_target_endpoint,
            active_state=active_state,
            latest_trace_path=self.latest_proxy_trace_path,
            latest_manifest_path=self.latest_proxy_trace_manifest_path,
        )
        values: dict[str, Any] = {
            "proxy_capture_status": overview.status,
            "proxy_capture_status_label": overview.status_label,
            "proxy_capture_summary": overview.summary,
            "proxy_capture_blocking_reason": overview.blocking_reason,
            "proxy_capture_can_start": overview.can_start,
            "proxy_capture_can_stop": overview.can_stop,
            "proxy_capture_critical_phase": overview.critical_phase,
            "proxy_capture_redirect_required": overview.redirect_required,
            "proxy_capture_collector_cloud_family": self.collector_cloud_family,
            "proxy_capture_current_endpoint": overview.current_endpoint,
            "proxy_capture_target_endpoint": overview.target_endpoint,
            "proxy_capture_masked_endpoint": overview.masked_endpoint,
            "proxy_trace_path": overview.latest_trace_path,
            "proxy_trace_manifest_path": overview.latest_manifest_path,
        }
        values.update(self._proxy_capture_timer_runtime_values(active_state))
        if active_state is not None:
            values.update(
                {
                    "proxy_capture_session_status": str(active_state.status or "").strip(),
                    "proxy_capture_session_started_at": str(active_state.started_at or "").strip(),
                    "proxy_capture_session_expires_at": str(active_state.expires_at or "").strip(),
                    "proxy_capture_session_anonymized": bool(active_state.anonymized),
                }
            )
        return values

    async def _async_proxy_trace_manifest_download_details(self, manifest_path: str) -> tuple[str, str]:
        """Return the saved ZIP bundle path and published URL for one proxy capture."""

        normalized_manifest_path = str(manifest_path or "").strip()
        if not normalized_manifest_path:
            return "", ""
        if normalized_manifest_path == self._proxy_trace_download_manifest_path:
            bundle_path = self._proxy_trace_download_details[0]
            if bundle_path:
                return (
                    bundle_path,
                    sign_proxy_capture_download_url(
                        self.hass,
                        self.config_entry.entry_id,
                        Path(bundle_path).name,
                    ),
                )
            return "", ""

        def _build_download_details() -> str:
            path = Path(normalized_manifest_path)
            if not path.exists():
                return ""
            bundle_path = export_proxy_trace_bundle(
                manifest_path=path,
                overwrite=True,
            )
            return str(bundle_path)

        try:
            bundle_path = await self.hass.async_add_executor_job(
                _build_download_details
            )
        except OSError:
            return "", ""

        self._proxy_trace_download_manifest_path = normalized_manifest_path
        relative_url = (
            sign_proxy_capture_download_url(
                self.hass,
                self.config_entry.entry_id,
                Path(bundle_path).name,
            )
            if bundle_path
            else ""
        )
        self._proxy_trace_download_details = (bundle_path, relative_url)
        return self._proxy_trace_download_details

    def _cancel_proxy_capture_deadline_refresh(self) -> None:
        """Cancel one scheduled deadline-triggered refresh if it exists."""

        handle = getattr(self, "_proxy_capture_deadline_refresh_handle", None)
        if handle is not None:
            handle.cancel()
        self._proxy_capture_deadline_refresh_handle = None

    async def _async_request_proxy_capture_deadline_refresh(self) -> None:
        """Ask the coordinator to reconcile proxy state when the lease expires."""

        self._proxy_capture_deadline_refresh_handle = None
        try:
            await self.async_request_refresh()
        except Exception as exc:
            logger.warning(
                "Proxy capture deadline refresh failed for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )

    def _schedule_proxy_capture_deadline_refresh(self, expires_at: object) -> None:
        """Schedule one coordinator refresh for the active proxy-capture deadline."""

        self._cancel_proxy_capture_deadline_refresh()
        deadline = parse_proxy_capture_session_timestamp(expires_at)
        if deadline is None:
            return

        loop = getattr(self.hass, "loop", None)
        if loop is None or not hasattr(loop, "call_later"):
            return

        delay = max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())

        def _trigger_refresh() -> None:
            create_task = getattr(self.hass, "async_create_task", None)
            coroutine = self._async_request_proxy_capture_deadline_refresh()
            if create_task is not None:
                create_task(coroutine)
            else:
                asyncio.create_task(coroutine)

        self._proxy_capture_deadline_refresh_handle = loop.call_later(delay, _trigger_refresh)

    def _proxy_capture_state_needs_reconcile(self, state: object | None) -> bool:
        """Return whether one interactive proxy action should first reconcile stale state."""

        if state is None or not proxy_capture_session_is_active(state):
            return False
        if proxy_capture_session_is_expired(state):
            return True
        status = str(getattr(state, "status", "") or "").strip()
        return status == "running" and not self._proxy_capture_process_running()

    def _proxy_capture_collector_ip(self) -> str:
        """Return the collector IP used to route proxy capture on shared ingress."""

        configured_ip = str(self.config_entry.data.get(CONF_COLLECTOR_IP) or "").strip()
        if configured_ip and configured_ip != DEFAULT_COLLECTOR_IP:
            return configured_ip
        collector = getattr(self.data, "collector", None)
        return str(getattr(collector, "remote_ip", "") or "").strip()


__all__ = ["CoordinatorCloudToolsMixin"]
