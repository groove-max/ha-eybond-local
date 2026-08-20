"""Runtime snapshot and support-payload projections for the coordinator."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..const import (
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
)
from ..drivers.registry import support_marker as driver_support_marker
from ..metadata.collector_cloud_profile_catalog_loader import resolve_collector_cloud_session_protocol
from ..models import RuntimeSnapshot
from ..schema import build_runtime_ui_schema
from ..support.bundle import build_support_bundle_payload
from ..support.collector_registry import get_collector_registry_record
from ..support.proxy_capture import build_proxy_capture_overview
from ..support.proxy_session import inspect_proxy_capture_trace
from ..support.runtime_projection import (
    build_collector_support_payload,
    build_inverter_support_payload,
)
from ..support.workflow import build_support_workflow_state
from .coordinator_tooling_projection import integration_build_runtime_values as _integration_build_runtime_values


class CoordinatorSnapshotProjectionMixin:
    """Publish coordinator-owned state without creating a second state store."""

    def _publish_tooling_values(self, **values: Any) -> None:
        """Publish in-memory tooling results into coordinator snapshot values."""

        if getattr(self, "_shutdown_complete", False):
            return
        self._tooling_values.update(values)
        snapshot = self.data
        snapshot.values.update(self._tooling_values)
        publish = getattr(self, "async_set_updated_data", None)
        if publish is not None:
            publish(snapshot)

    def _publish_snapshot_values(self, **values: Any) -> None:
        """Publish transient runtime values into the live coordinator snapshot only."""

        if getattr(self, "_shutdown_complete", False):
            return
        snapshot = self.data
        for key, value in values.items():
            if key == "collector_server_endpoint":
                snapshot.set_collector_server_endpoint("" if value is None else value)
                continue
            if value is None:
                snapshot.values.pop(key, None)
            else:
                snapshot.values[key] = value
        publish = getattr(self, "async_set_updated_data", None)
        if publish is not None:
            publish(snapshot)

    def invalidate_collector_runtime_values(self) -> None:
        """Invalidate cached collector-side runtime values before a forced refresh."""

        invalidator = getattr(self._runtime, "invalidate_collector_runtime_values", None)
        if callable(invalidator):
            invalidator()

    def _support_workflow_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return user-facing support workflow guidance for the current entry."""

        snapshot = snapshot or self.data
        metadata = self.effective_metadata
        collector = snapshot.collector
        marker = self._driver_support_marker(snapshot.inverter, metadata)
        workflow = build_support_workflow_state(
            has_inverter=snapshot.inverter is not None,
            variant_key=getattr(snapshot.inverter, "variant_key", ""),
            profile_name=metadata.profile_name,
            effective_owner_key=metadata.effective_owner_key,
            support_marker_workflow=marker.workflow if marker is not None else None,
            effective_owner_name=metadata.effective_owner_name,
            smartess_family_name=metadata.smartess_family_name,
            detection_confidence=self.detection_confidence,
            profile_source_scope=getattr(metadata.profile_metadata, "source_scope", ""),
            schema_source_scope=getattr(metadata.register_schema_metadata, "source_scope", ""),
            smartess_protocol_asset_id=(
                getattr(collector, "smartess_protocol_asset_id", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "") or "")
            ),
            smartess_profile_key=(
                getattr(collector, "smartess_protocol_profile_key", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_PROFILE_KEY, "") or "")
            ),
            smartess_collector_version=(
                getattr(collector, "smartess_collector_version", "")
                or str(self.config_entry.data.get(CONF_SMARTESS_COLLECTOR_VERSION, "") or "")
            ),
        )
        return {
            "support_workflow_level": workflow["level"],
            "support_workflow_level_label": workflow["level_label"],
            "support_workflow_summary": workflow["summary"],
            "support_workflow_next_action": workflow["next_action"],
            "support_workflow_primary_action": workflow["primary_action"],
            "support_workflow_step_1": workflow["step_1"],
            "support_workflow_step_2": workflow["step_2"],
            "support_workflow_step_3": workflow["step_3"],
            "support_workflow_plan": workflow["plan"],
            "support_workflow_advanced_hint": workflow["advanced_hint"],
        }

    def _collector_original_endpoint_runtime_values(
        self,
        *,
        include_registry: bool = False,
        registry_lookup: tuple[str, Any | None] | None = None,
    ) -> dict[str, Any]:
        """Return non-sensitive diagnostics for preserved original endpoint state."""

        options = getattr(self.config_entry, "options", {}) or {}
        remembered_endpoint = self._normalized_remembered_collector_server_endpoint()
        values: dict[str, Any] = {
            "collector_original_endpoint_known": bool(remembered_endpoint),
            "collector_original_endpoint_profile_key": str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY, "") or ""
            ).strip(),
            "collector_original_endpoint_source": str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE, "") or ""
            ).strip(),
            "collector_original_endpoint_observed_at": str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT, "") or ""
            ).strip(),
        }
        if not include_registry:
            return values

        collector_pn = self._preferred_collector_pn(self.data)
        if registry_lookup is not None:
            registry_status, record = registry_lookup
        else:
            registry_status = "unavailable"
            record = None
        if collector_pn and registry_lookup is None:
            try:
                record = get_collector_registry_record(
                    config_dir=Path(self.hass.config.path()),
                    collector_pn=collector_pn,
                )
                registry_status = "found" if record is not None else "missing"
            except Exception as exc:  # pragma: no cover - defensive diagnostics only
                registry_status = f"error:{type(exc).__name__}"
        values["collector_registry_record_status"] = registry_status
        values["collector_registry_record_pn_known"] = bool(collector_pn)
        if record is not None:
            values.update(
                {
                    "collector_registry_original_endpoint": record.original_endpoint_raw,
                    "collector_registry_cloud_profile_key": record.cloud_profile_key,
                    "collector_registry_source": record.source,
                    "collector_registry_observed_at": record.observed_at,
                    "collector_registry_last_seen_ip": record.last_seen_ip,
                }
            )
        return values

    def _collector_transport_profile_runtime_values(self) -> dict[str, Any]:
        """Return diagnostics that compare resolved profile and live link state."""

        profile = self.collector_transport_profile
        values: dict[str, Any] = {
            "collector_resolved_cloud_family": profile.cloud_family,
            "collector_resolved_runtime_owner_key": profile.runtime_owner_key,
            "collector_resolved_session_protocol": profile.session_protocol,
            "collector_resolved_identity_strategy": profile.identity_strategy,
        }
        connection = getattr(self, "_connection_spec", None)
        if connection is not None:
            values.update(
                {
                    "collector_connection_cloud_family": str(
                        getattr(connection, "collector_cloud_family", "") or ""
                    ),
                    "collector_connection_session_protocol": str(
                        getattr(
                            connection,
                            "collector_configured_session_protocol",
                            "",
                        )
                        or ""
                    ),
                    "collector_connection_identity_strategy": str(
                        getattr(connection, "collector_identity_strategy", "") or ""
                    ),
                }
            )
        runtime = getattr(self, "_runtime", None)
        link_diagnostics = getattr(runtime, "listener_diagnostics", None)
        if callable(link_diagnostics):
            try:
                diagnostics = link_diagnostics()
            except Exception as exc:  # pragma: no cover - defensive diagnostics only
                values["collector_runtime_link_diagnostics_error"] = type(exc).__name__
            else:
                values["collector_runtime_link_session_protocol"] = str(
                    diagnostics.get("collector_configured_session_protocol") or ""
                )
                values["collector_runtime_link_identity_strategy"] = str(
                    diagnostics.get("collector_callback_identity_strategy") or ""
                )
        return values

    def _collector_onboarding_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return compact collector-side onboarding status helpers for entity UX."""

        snapshot = snapshot or self.data
        support_label = str(snapshot.values.get("support_workflow_level_label") or "").strip()
        return {
            "collector_onboarding_status": support_label or "Unknown",
            **self._collector_original_endpoint_runtime_values(),
            **self._collector_transport_profile_runtime_values(),
        }

    async def _proxy_capture_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return user-facing proxy capture status helpers for diagnostics UX."""

        snapshot = snapshot or self.data
        state = await self._async_active_proxy_capture_state(require_process=False)
        record = await self._async_latest_proxy_trace_record()
        trace_path = str(getattr(state, "trace_path", "") or "").strip()
        if not trace_path and record is not None:
            trace = record.payload.get("trace") if isinstance(record.payload, dict) else None
            if isinstance(trace, dict):
                trace_path = str(trace.get("path") or "").strip()
        manifest_path = "" if state is not None or record is None else str(record.path)
        trace_details = await self.hass.async_add_executor_job(
            lambda: inspect_proxy_capture_trace(Path(trace_path))
        ) if trace_path else {
            "exists": False,
            "line_count": 0,
            "kind_summary": "",
            "recent_kinds": "",
            "recent_events": "",
            "live_log": "",
            "last_timestamp": "",
        }
        overview = build_proxy_capture_overview(
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
            current_endpoint=snapshot.collector_server_endpoint,
            upstream_endpoint=self.proxy_capture_upstream_endpoint,
            target_endpoint=self.proxy_capture_target_endpoint,
            active_state=state,
            latest_trace_path=trace_path,
            latest_manifest_path=manifest_path,
        )
        manifest_download_path, manifest_download_url = await self._async_proxy_trace_manifest_download_details(
            overview.latest_manifest_path
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
            "proxy_trace_saved_result_path": manifest_download_path,
            "proxy_trace_saved_result_download_url": manifest_download_url,
            "proxy_trace_manifest_download_url": manifest_download_url,
            "proxy_trace_line_count": trace_details.get("line_count", 0),
            "proxy_trace_kind_summary": str(trace_details.get("kind_summary") or ""),
            "proxy_trace_recent_kinds": str(trace_details.get("recent_kinds") or ""),
            "proxy_trace_recent_events": str(trace_details.get("recent_events") or ""),
            "proxy_trace_live_log": str(trace_details.get("live_log") or ""),
            "proxy_trace_last_timestamp": str(trace_details.get("last_timestamp") or ""),
        }
        values.update(self._proxy_capture_timer_runtime_values(state))
        if state is not None:
            values["proxy_capture_session_status"] = state.status
            values["proxy_capture_session_started_at"] = state.started_at
            values["proxy_capture_session_expires_at"] = state.expires_at
            values["proxy_capture_session_anonymized"] = state.anonymized
        return values

    def _build_support_bundle_payload(
        self,
        *,
        integration_build_values: Mapping[str, object] | None = None,
        collector_registry_lookup: tuple[str, Any | None] | None = None,
    ) -> dict[str, Any]:
        inverter = self.data.inverter
        metadata = self.effective_metadata
        smartess_protocol = metadata.smartess_protocol
        values = self.data.runtime_values()
        values.update(integration_build_values or _integration_build_runtime_values())
        values.update(self._collector_transport_profile_runtime_values())
        values.update(
            self._collector_original_endpoint_runtime_values(
                include_registry=True,
                registry_lookup=collector_registry_lookup,
            )
        )
        cloud_evidence_record = self._latest_smartess_cloud_evidence_record()
        cloud_evidence = None
        if cloud_evidence_record is not None:
            cloud_evidence = cloud_evidence_record.payload
            values["cloud_evidence_path"] = str(cloud_evidence_record.path)
        inverter_payload = None
        if inverter is not None:
            values["ui_schema"] = build_runtime_ui_schema(
                inverter,
                self.data.runtime_values(),
            )
            inverter_payload = build_inverter_support_payload(inverter)
        marker = self._driver_support_marker(inverter, metadata)
        return build_support_bundle_payload(
            entry_id=self.config_entry.entry_id,
            entry_title=self._support_context_title(),
            connected=self.data.connected,
            collector=build_collector_support_payload(
                self.data.collector,
                self.collector_cloud_profile,
            ),
            inverter=inverter_payload,
            values=values,
            telemetry=self.data.telemetry,
            data=dict(self.config_entry.data),
            options=dict(self.config_entry.options),
            profile_name=metadata.profile_name,
            register_schema_name=metadata.register_schema_name,
            variant_key=getattr(inverter, "variant_key", ""),
            effective_owner_key=metadata.effective_owner_key,
            effective_owner_name=metadata.effective_owner_name,
            smartess_family_name=metadata.smartess_family_name,
            raw_profile_name=metadata.raw_profile_name,
            raw_register_schema_name=metadata.raw_register_schema_name,
            smartess_protocol_asset_id=getattr(smartess_protocol, "asset_id", ""),
            smartess_profile_key=getattr(smartess_protocol, "profile_key", ""),
            support_marker=marker.as_payload() if marker is not None else None,
            cloud_evidence=cloud_evidence,
        )

    @staticmethod
    def _driver_support_marker(inverter: Any, metadata: Any):
        """Resolve the authoritative driver support marker for this identity.

        The owning driver (by ``inverter.driver_key``) decides any special
        support state; the runtime only projects the neutral marker. Returns
        ``None`` when there is no inverter or no special state.
        """

        if inverter is None:
            return None
        return driver_support_marker(
            getattr(inverter, "driver_key", ""),
            variant_key=str(getattr(inverter, "variant_key", "") or ""),
            profile_name=str(getattr(metadata, "profile_name", "") or ""),
        )


__all__ = ["CoordinatorSnapshotProjectionMixin"]
