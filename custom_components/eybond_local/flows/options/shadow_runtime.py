"""Extracted EyeBond options-flow lifecycle: ShadowLearningRuntimeMixin."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Any

from ...runtime.shadow_learning_facade import ShadowLearningRuntimeFacade
from ...support.cloud_learning_engines import (
    CloudLearningEngine,
    compatible_cloud_learning_methods_for_provider,
    compatible_cloud_learning_sources_for_method,
    compatible_cloud_learning_sources_for_method_any_provider,
    default_cloud_learning_source_for_method,
    default_cloud_learning_source_for_method_any_provider,
    resolve_cloud_learning_selection,
    supported_cloud_learning_methods,
)
from ...support.cloud_learning_models import (
    LEARNING_METHOD_ACTIVE_CORRELATION,
    LEARNING_METHOD_READ_ONLY_EVIDENCE,
    CloudApiSource,
    CloudLearningMethod,
    CloudLearningSelection,
)
from ...support.memory_guard import (
    read_available_memory_mib,
    shadow_learning_memory_blocker,
)
from ...support.shadow_learning.backend import (
    build_shadow_learning_preflight,
    build_shadow_learning_seed,
)
from ...support.shadow_learning.proxy import route_status_indicates_control_write_ready
from ...support.shadow_learning.runtime import (
    ShadowLearningRouteStatus,
    ShadowLearningRuntimeView,
)

logger = logging.getLogger(__name__)


_SHADOW_CONTROL_ROUTE_WINDOW_WAIT = 65.0


class ShadowLearningRuntimeMixin:
    """ShadowLearningRuntime lifecycle."""

    @staticmethod
    def _control_discovery_cloud_provider(coordinator) -> str:
        # The coordinator resolves cloud family through the catalog.  The flow
        # consumes that resolved provider id verbatim: no substring heuristic
        # and no legacy SmartESS default may become a second policy authority.
        return (
            str(getattr(coordinator, "cloud_evidence_provider", "") or "")
            .strip()
            .lower()
        )

    def _control_discovery_learning_method(self, coordinator) -> str:
        """Return only an exact transient method compatible with the provider."""

        compatible = self._control_discovery_learning_methods(coordinator)
        selected = self._shadow_learning_state.get("wizard_method")
        if type(selected) is str and selected == selected.strip() and any(
            method.method_id == selected for method in compatible
        ):
            return selected
        return ""

    def _control_discovery_learning_methods(
        self, coordinator
    ) -> tuple[CloudLearningMethod, ...]:
        provider = self._control_discovery_cloud_provider(coordinator)
        candidates = (
            compatible_cloud_learning_methods_for_provider(provider)
            if provider
            else supported_cloud_learning_methods()
        )
        readiness = self._support_acquisition_readiness()
        allowed: set[str] = set()
        if readiness.cloud_metadata_read.can_start:
            allowed.add(LEARNING_METHOD_READ_ONLY_EVIDENCE)
        if (
            readiness.active_control_learning.can_start
            or self._shadow_learning_lifecycle_active(coordinator)
        ):
            allowed.add(LEARNING_METHOD_ACTIVE_CORRELATION)
        return tuple(
            method for method in candidates if method.method_id in allowed
        )

    def _control_discovery_learning_source(self, coordinator) -> str:
        """Return only an exact transient source for the selected method."""

        compatible = self._control_discovery_learning_sources(coordinator)
        selected = self._shadow_learning_state.get("wizard_source")
        if type(selected) is str and selected == selected.strip() and any(
            source.source_id == selected for source in compatible
        ):
            return selected
        return ""

    def _control_discovery_learning_sources(
        self, coordinator
    ) -> tuple[CloudApiSource, ...]:
        """Return sources compatible with the trusted provider and method."""

        provider = self._control_discovery_cloud_provider(coordinator)
        method_id = self._control_discovery_learning_method(coordinator)
        if provider:
            return compatible_cloud_learning_sources_for_method(
                provider,
                method_id,
            )
        if (
            method_id == LEARNING_METHOD_READ_ONLY_EVIDENCE
            and self._support_acquisition_readiness().cloud_metadata_read.can_start
        ):
            return compatible_cloud_learning_sources_for_method_any_provider(
                method_id
            )
        return ()

    def _control_discovery_default_learning_source(
        self,
        coordinator,
        method_id: str,
    ) -> str:
        """Return a source default without inventing a missing provider."""

        provider = self._control_discovery_cloud_provider(coordinator)
        if provider:
            return default_cloud_learning_source_for_method(provider, method_id)
        return default_cloud_learning_source_for_method_any_provider(method_id)

    def _control_discovery_learning_selection(
        self, coordinator
    ) -> CloudLearningSelection | None:
        method_id = self._control_discovery_learning_method(coordinator)
        source_id = self._control_discovery_learning_source(coordinator)
        if not method_id or not source_id:
            return None
        selection = CloudLearningSelection(
            method_id=method_id,
            source_id=source_id,
        )
        engine = resolve_cloud_learning_selection(selection)
        if not engine.available:
            return None
        provider = self._control_discovery_cloud_provider(coordinator)
        if provider and engine.source.provider_id != provider:
            return None
        if not provider and method_id != LEARNING_METHOD_READ_ONLY_EVIDENCE:
            return None
        return selection

    def _control_discovery_learning_engine(self, coordinator) -> CloudLearningEngine:
        return resolve_cloud_learning_selection(
            self._control_discovery_learning_selection(coordinator)
        )

    def _control_discovery_learning_source_label(self, coordinator) -> str:
        engine = self._control_discovery_learning_engine(coordinator)
        source_id = engine.source.source_id
        labels = {
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
        return labels.get(source_id, engine.source.label)

    def _control_discovery_requires_shadow_route(self, coordinator) -> bool:
        """Return whether the exact selected engine owns a temporary route.

        Cleanup is a mutating endpoint operation.  It must therefore be gated by
        the same typed engine capability that authorizes route creation; a
        metadata-only source must never reach the shadow-session stop path even
        when its HTTP request fails or its flow task is cancelled.
        """

        engine = self._control_discovery_learning_engine(coordinator)
        return bool(
            engine.available
            and engine.method is not None
            and engine.method.requires_shadow_route
        )

    def _control_discovery_cloud_provider_label(self, coordinator) -> str:
        engine = self._control_discovery_learning_engine(coordinator)
        provider = (
            engine.source.provider_id
            if engine.available
            else self._control_discovery_cloud_provider(coordinator)
        )
        if provider == "valuecloud":
            return "ValueCloud"
        if provider == "smartess":
            return "SmartESS"
        return provider or self._tr("common.dynamic.cloud_provider", "cloud service")

    def _control_discovery_cloud_app_label(self, coordinator) -> str:
        engine = self._control_discovery_learning_engine(coordinator)
        provider = (
            engine.source.provider_id
            if engine.available
            else self._control_discovery_cloud_provider(coordinator)
        )
        if provider == "valuecloud":
            return "SmartValue"
        if provider == "smartess":
            return "SmartESS"
        return self._control_discovery_cloud_provider_label(coordinator)

    @staticmethod
    def _preflight_effective_metadata(coordinator) -> dict[str, Any]:
        """Return the effective metadata the preflight validates.

        Delegates to the coordinator so this preview preflight and the actual
        learning start path (``async_start_shadow_learning``) share ONE fallback
        implementation. They used to each carry their own copy and drifted: the
        preview fell back to live metadata while the start path passed the raw
        (empty, for a partial tier) persisted snapshot, so learning previewed as
        startable and then failed with ``missing_effective_metadata_snapshot``.
        """

        return coordinator.shadow_learning_effective_metadata

    async def _build_shadow_learning_preflight_snapshot(
        self, coordinator
    ) -> dict[str, Any]:
        connected = bool(getattr(coordinator.data, "connected", False))
        raw_capture = None
        if connected:
            with suppress(Exception):
                shadow_runtime = self._shadow_learning_runtime(coordinator)
                if shadow_runtime is not None:
                    raw_capture = await shadow_runtime.async_capture_support_evidence()
        seed, blockers = build_shadow_learning_seed(
            session_id=f"{self._config_entry.entry_id}_preview",
            entry_id=self._config_entry.entry_id,
            collector_pn=coordinator.smartess_collector_pn,
            collector_cloud_family=coordinator.collector_cloud_family,
            raw_passthrough_frame_format=getattr(
                coordinator,
                "collector_raw_passthrough_frame_format",
                "",
            ),
            collector_cloud_profile_key=coordinator.collector_cloud_profile_key,
            collector_cloud_profile_label=coordinator.collector_cloud_profile_label,
            collector_cloud_profile_source=coordinator.collector_cloud_profile_source,
            collector_cloud_profile_confidence=coordinator.collector_cloud_profile_confidence,
            collector_callback_endpoint=coordinator.collector_callback_target_endpoint,
            effective_metadata_snapshot=self._preflight_effective_metadata(coordinator),
            raw_capture=raw_capture,
        )
        preflight = build_shadow_learning_preflight(seed)
        effective_blockers = list(blockers or preflight.blockers)
        can_start = bool(preflight.can_start)
        if not connected:
            # The register seed can only be captured from a LIVE collector. When it is offline
            # the seed is empty and the only blocker is the cryptic "missing_register_seed";
            # surface the real cause so the user knows to bring the collector back online rather
            # than suspecting a code regression.
            effective_blockers = ["collector_not_connected"] + [
                blocker
                for blocker in effective_blockers
                if blocker != "missing_register_seed"
            ]
        # Memory guard: the scan spins up a cloud sign-in + proxy capture +
        # correlation pass. On a memory-tight host that spike can push the box
        # into the OOM killer (and a watchdog reset). Refuse up front rather than
        # risk taking the whole appliance down. Unknown memory (non-Linux) skips
        # the guard.
        available_mib = await self.hass.async_add_executor_job(
            read_available_memory_mib
        )
        memory_blocker = shadow_learning_memory_blocker(available_mib)
        if memory_blocker:
            effective_blockers = [memory_blocker] + effective_blockers
            can_start = False
        route_status = self._shadow_learning_route_status(coordinator)
        return {
            "can_start": can_start,
            "blockers": effective_blockers,
            "protocol_adapter_key": str(seed.protocol_adapter_key or ""),
            "protocol_adapter_supported": bool(
                seed.protocol_adapter_key
                and not any(
                    str(blocker).startswith("unsupported_shadow_learning_protocol:")
                    for blocker in effective_blockers
                )
            ),
            "collector_pn": coordinator.smartess_collector_pn,
            "profile_name": str(coordinator.effective_profile_name or ""),
            "schema_name": str(coordinator.effective_register_schema_name or ""),
            "shadow_session_state": self._shadow_learning_session_state(coordinator),
            "shadow_session_active": route_status.running,
            "shadow_session_ready": route_status.ready,
            "shadow_session_running": route_status.running,
            "shadow_session_collector_connected": route_status.collector_connected,
            "shadow_session_upstream_connected": route_status.upstream_connected,
        }

    @staticmethod
    def _shadow_learning_runtime(
        coordinator,
    ) -> ShadowLearningRuntimeFacade | None:
        try:
            facade = getattr(coordinator, "shadow_learning_runtime", None)
        except Exception:
            return None
        return facade if type(facade) is ShadowLearningRuntimeFacade else None

    @staticmethod
    def _shadow_learning_runtime_view(
        coordinator,
    ) -> ShadowLearningRuntimeView:
        facade = ShadowLearningRuntimeMixin._shadow_learning_runtime(coordinator)
        if facade is None:
            return ShadowLearningRuntimeView()
        try:
            view = facade.view
        except Exception:
            return ShadowLearningRuntimeView()
        return (
            view
            if type(view) is ShadowLearningRuntimeView
            else ShadowLearningRuntimeView()
        )

    def _shadow_learning_route_status(
        self,
        coordinator,
    ) -> ShadowLearningRouteStatus:
        return self._shadow_learning_runtime_view(coordinator).route_status

    def _shadow_learning_cloud_identity(self, coordinator) -> dict[str, Any] | None:
        record = self._cached_cloud_evidence_record(coordinator)
        if record is None:
            return None
        identity = record.payload.get("device_identity")
        if not isinstance(identity, dict):
            return None
        pn = str(identity.get("pn") or "").strip()
        sn = str(identity.get("sn") or "").strip()
        devcode = identity.get("devcode")
        devaddr = identity.get("devaddr")
        if not pn or not sn or devcode is None or devaddr is None:
            return None
        return {
            "pn": pn,
            "sn": sn,
            "devcode": int(devcode),
            "devaddr": int(devaddr),
        }

    def _cached_cloud_evidence_record(self, coordinator):
        return self._shadow_learning_runtime_view(coordinator).cloud_evidence

    def _publish_shadow_learning_artifacts(self, coordinator) -> dict[str, Any]:
        """Publish the current UX artifact state into support-package runtime values."""

        publish = getattr(coordinator, "publish_shadow_learning_artifacts", None)
        if not callable(publish):
            return {}
        state = dict(self._shadow_learning_state or {})
        plan = dict(state.get("plan") or {})
        orchestration = dict(state.get("orchestration") or {})
        if not orchestration:
            discovery = state.get("discovery")
            if isinstance(discovery, dict) and discovery:
                orchestration = {
                    "source": "cloud_learning_runner",
                    "status": str(discovery.get("status") or ""),
                    "reason": str(discovery.get("reason") or ""),
                    "preflight": dict(state.get("preflight") or {}),
                }
        correlation = orchestration.get("correlation")
        if not isinstance(correlation, dict):
            correlation = {}
        session = dict(state.get("session") or {})
        identity = dict(state.get("identity") or {})
        overlay = dict(state.get("overlay") or {})
        activation = dict(state.get("activation") or {})
        device_scope = {
            "collector_pn": str(
                getattr(coordinator, "smartess_collector_pn", "") or ""
            ),
            "cloud_pn": str(identity.get("pn") or ""),
            "cloud_sn": str(identity.get("sn") or ""),
            "devcode": identity.get("devcode"),
            "devaddr": identity.get("devaddr"),
        }
        overlay_manifest = overlay.get("manifest")
        if isinstance(overlay_manifest, dict):
            manifest_scope = overlay_manifest.get("scope")
            if isinstance(manifest_scope, dict):
                device_scope.update(manifest_scope)
            elif manifest_scope:
                device_scope["scope"] = str(manifest_scope)
        if not activation and overlay:
            activation = {
                "status": "draft_generated",
                "active": False,
                "scope": device_scope.get("scope") or "device",
                "activation_scope": device_scope,
                "profile_name": str(overlay.get("profile_name") or ""),
                "register_schema_name": str(overlay.get("schema_name") or ""),
            }
            review_selection = self._control_discovery_review_selection_payload(overlay)
            if review_selection:
                activation.update(review_selection)
                activation["status"] = "review_selected"
        return publish(
            plan=plan,
            orchestration=orchestration,
            correlation=correlation,
            trace_path=str(session.get("trace_path") or ""),
            profile_draft_path=str(overlay.get("profile_path") or ""),
            schema_draft_path=str(overlay.get("schema_path") or ""),
            activation=activation,
            session_id=str(
                session.get("session_id") or session.get("trace_path") or ""
            ),
            device_scope=device_scope,
        )

    def _shadow_learning_session_state(self, coordinator) -> str:
        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        explicit_state = (
            str(values.get("shadow_learning_session_status") or "").strip().lower()
        )
        route_status = self._shadow_learning_route_status(coordinator)

        # Route status is authoritative for live execution readiness.
        if route_status.ready:
            if explicit_state == "learning":
                return "learning"
            return "ready"
        if route_status.running:
            if (
                route_status.collector_connected
                and not route_status.route_protocol_activity
            ):
                return "waiting_for_collector"
            if (
                route_status.route_protocol_activity
                and not route_status.upstream_connected
            ):
                return "connecting_upstream"
            return "degraded"

        if explicit_state in {
            "preflight",
            "starting",
            "restoring",
            "restore_failed",
            "failed",
            "stopped",
        }:
            return explicit_state
        return "stopped"

    def _shadow_learning_route_accepts_control(self, coordinator) -> bool:
        """Return whether SmartESS control commands may be sent through the route.

        SAFETY-CRITICAL. A ``ctrlDevice`` is delivered cloud -> the collector's
        MAIN (param-21) link. That write reaches the inverter UNPROXIED unless the
        param-21 link currently terminates on our proxy. The only real-time signal
        for that is ``collector_connected`` -- the live collector->proxy socket,
        which is STABLE for the duration of a scan (it is the separate *upstream*
        proxy->cloud socket that is short-lived, not this one). If the collector
        disconnects from the learning route or its endpoint changes away from
        Home Assistant mid-scan, ``collector_connected`` drops and control must
        stop immediately. A "reached us once" signal is NOT acceptable here: it
        stays true after a route loss and could let probing continue outside the
        learning proxy.
        """

        status = self._shadow_learning_route_status(coordinator)
        if not status.running:
            return False
        if status.upstream_error:
            return False
        # SAFETY: start readiness and write readiness are deliberately different.
        # Start only needs a collector->proxy reconnect; an actual ctrlDevice must
        # also have a live proxy->cloud upstream socket, otherwise SmartESS may
        # deliver the command over the real-server route and bypass our shadow.
        return route_status_indicates_control_write_ready(status.to_mapping())

    async def _async_wait_for_shadow_learning_control_route(
        self,
        coordinator,
    ) -> bool:
        """Wait for one safe live shadow-route window before a cloud request."""

        started_at = asyncio.get_running_loop().time()
        deadline = started_at + _SHADOW_CONTROL_ROUTE_WINDOW_WAIT
        initial_status = self._shadow_learning_route_status(coordinator)
        logger.warning(
            "Shadow control route wait started running=%s collector=%s ingress=%s "
            "activity=%s upstream=%s ready=%s upstream_error=%s",
            initial_status.running,
            initial_status.collector_connected,
            initial_status.collector_protocol_ingress,
            initial_status.route_protocol_activity,
            initial_status.upstream_connected,
            initial_status.ready,
            bool(initial_status.upstream_error),
        )
        while asyncio.get_running_loop().time() < deadline:
            if self._shadow_learning_route_accepts_control(coordinator):
                logger.warning(
                    "Shadow control route wait ready elapsed=%.3f",
                    asyncio.get_running_loop().time() - started_at,
                )
                return True
            status = self._shadow_learning_route_status(coordinator)
            if not status.running or status.upstream_error:
                logger.warning(
                    "Shadow control route wait terminal elapsed=%.3f running=%s "
                    "collector=%s ingress=%s activity=%s upstream=%s ready=%s "
                    "upstream_error=%s",
                    asyncio.get_running_loop().time() - started_at,
                    status.running,
                    status.collector_connected,
                    status.collector_protocol_ingress,
                    status.route_protocol_activity,
                    status.upstream_connected,
                    status.ready,
                    bool(status.upstream_error),
                )
                return False
            await asyncio.sleep(0.25)
        status = self._shadow_learning_route_status(coordinator)
        logger.warning(
            "Shadow control route wait timeout elapsed=%.3f running=%s collector=%s "
            "ingress=%s activity=%s upstream=%s ready=%s upstream_error=%s",
            asyncio.get_running_loop().time() - started_at,
            status.running,
            status.collector_connected,
            status.collector_protocol_ingress,
            status.route_protocol_activity,
            status.upstream_connected,
            status.ready,
            bool(status.upstream_error),
        )
        return False

    def _shadow_learning_placeholders(self, coordinator) -> dict[str, str]:
        state = dict(self._shadow_learning_state or {})
        preflight = dict(state.get("preflight") or {})
        plan = dict(state.get("plan") or {})
        orchestration = dict(state.get("orchestration") or {})
        correlation = dict(orchestration.get("correlation") or {})
        overlay = dict(state.get("overlay") or {})
        activation = dict(state.get("activation") or {})
        session = dict(state.get("session") or {})
        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}

        learned_summary = overlay.get("manifest", {}).get("learned_capabilities", [])
        destructive_count = 0
        action_count = 0
        if isinstance(learned_summary, list):
            for item in learned_summary:
                if not isinstance(item, dict):
                    continue
                if str(item.get("value_kind") or "") == "action":
                    action_count += 1
                if str(item.get("safety_class") or "") == "destructive":
                    destructive_count += 1

        blockers = preflight.get("blockers") or []
        if not isinstance(blockers, list):
            blockers = []
        can_start = bool(preflight.get("can_start"))
        session_state = str(
            self._shadow_learning_session_state(coordinator) or ""
        ).strip()
        if not session_state:
            session_state = str(
                session.get("status") or preflight.get("shadow_session_state") or ""
            ).strip()
        return {
            "cloud_provider": self._control_discovery_cloud_provider(coordinator),
            "cloud_provider_label": self._control_discovery_learning_source_label(
                coordinator
            ),
            "cloud_app_label": self._control_discovery_cloud_app_label(coordinator),
            "shadow_learning_status": str(
                state.get("status")
                or self._tr("common.dynamic.not_run_yet", "Not run yet")
            ),
            "shadow_learning_preflight": self._tr(
                "common.dynamic.shadow_learning_preflight_summary",
                "Can start: {can_start}; blockers: {blockers}",
                {
                    "can_start": self._tr("common.dynamic.yes", "Yes")
                    if can_start
                    else self._tr("common.dynamic.no", "No"),
                    "blockers": ", ".join(blockers)
                    or self._tr("common.dynamic.none", "None"),
                },
            ),
            "shadow_learning_session_state": session_state
            or self._tr("common.dynamic.not_run_yet", "Not run yet"),
            "shadow_learning_trace_path": str(
                session.get("trace_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "shadow_learning_plan_count": str(len(plan.get("items") or [])),
            "shadow_learning_found_controls": str(
                int(overlay.get("generated_capability_count") or 0)
            ),
            "shadow_learning_found_actions": str(action_count),
            "shadow_learning_found_destructive": str(destructive_count),
            "shadow_learning_unmatched_fields": str(
                int(correlation.get("unmatched_attempt_count") or 0)
            ),
            "shadow_learning_overlay_profile_path": str(
                overlay.get("profile_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "shadow_learning_overlay_schema_path": str(
                overlay.get("schema_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "shadow_learning_support_package_path": str(
                state.get("support_package_path")
                or values.get("support_package_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "shadow_learning_activation_scope": str(
                activation.get("scope")
                or values.get("effective_device_scoped_overlay_scope")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "shadow_learning_activation_status": (
                self._tr("common.dynamic.yes", "Yes")
                if bool(values.get("effective_device_scoped_overlay_active"))
                else self._tr("common.dynamic.no", "No")
            ),
            "shadow_learning_warning": self._tr(
                "common.dynamic.shadow_learning_warning",
                "Control discovery is advanced and optional. It briefly uses {cloud_provider_label} to test which settings Home Assistant can control. Testing all option values or numeric settings can trigger cloud-side actions and requires explicit preview and confirmation.",
                {
                    "cloud_provider_label": self._control_discovery_cloud_provider_label(
                        coordinator
                    )
                },
            ),
        }
