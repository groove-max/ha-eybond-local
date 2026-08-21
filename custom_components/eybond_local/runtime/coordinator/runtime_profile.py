"""Runtime effective-metadata and managed-endpoint reconciliation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from typing import Any

from ...connection.connection_policy import may_auto_manage_endpoint
from ...const import CONF_DETECTION_CONFIDENCE
from ...metadata.effective_metadata import resolve_effective_metadata_selection
from ...metadata.effective_metadata_snapshot import build_effective_metadata_snapshot_from_runtime
from ...models import RuntimeSnapshot
from ...support.shadow_learning_session import shadow_learning_session_is_active
from .endpoint_projection import (
    normalize_preserved_collector_server_endpoint as _normalize_preserved_collector_server_endpoint,
    resolve_collector_server_endpoint as _resolve_collector_server_endpoint,
)

logger = logging.getLogger(__name__)

_COLLECTOR_HA_PRIMARY_RECONCILE_COOLDOWN_SECONDS = 300.0


class CoordinatorRuntimeProfileMixin:
    """Reconcile runtime metadata and integration-managed endpoint facts."""

    def _support_context_title(self) -> str:
        """Return the support artifact title, preferring confirmed inverter identity."""

        inverter = self.data.inverter
        model_name = str(getattr(inverter, "model_name", "") or "").strip()
        serial_number = str(getattr(inverter, "serial_number", "") or "").strip()
        if model_name and serial_number:
            return f"{model_name} ({serial_number})"
        if model_name:
            return model_name
        return str(self.config_entry.title or "").strip() or "EyeBond Local"

    def _build_runtime_effective_metadata_snapshot(
        self,
        snapshot: RuntimeSnapshot,
        *,
        entry_data: dict[str, Any],
        current_snapshot,
    ):
        """Return one persisted snapshot only when live runtime identity is confirmed."""

        inverter = snapshot.inverter
        if inverter is None:
            return None

        model_name = str(getattr(inverter, "model_name", "") or "").strip()
        serial_number = str(getattr(inverter, "serial_number", "") or "").strip()
        if not (model_name or serial_number):
            return None

        # Persist only when runtime supplied concrete metadata, not driver defaults alone.
        profile_name = str(getattr(inverter, "profile_name", "") or "").strip()
        register_schema_name = str(
            getattr(inverter, "register_schema_name", "") or ""
        ).strip()
        if not profile_name or not register_schema_name:
            return None

        effective_selection = resolve_effective_metadata_selection(
            inverter=inverter,
            driver=self.current_driver,
            collector=snapshot.collector,
            entry_data=entry_data,
            entry_options=self.config_entry.options,
        )
        confidence = str(
            entry_data.get(CONF_DETECTION_CONFIDENCE)
            or self.detection_confidence
            or "none"
        ).strip()
        stable_snapshot = build_effective_metadata_snapshot_from_runtime(
            inverter=inverter,
            selection=effective_selection,
            confidence=confidence,
            generation=current_snapshot.generation,
            generated_at=current_snapshot.generated_at,
        )
        if not stable_snapshot.is_valid:
            return None
        if (
            stable_snapshot.effective_owner_key == current_snapshot.effective_owner_key
            and stable_snapshot.effective_owner_name == current_snapshot.effective_owner_name
            and stable_snapshot.variant_key == current_snapshot.variant_key
            and stable_snapshot.profile_name == current_snapshot.profile_name
            and stable_snapshot.register_schema_name == current_snapshot.register_schema_name
            and stable_snapshot.confidence == current_snapshot.confidence
            and stable_snapshot.candidate_keys == current_snapshot.candidate_keys
            and stable_snapshot.resolution_level == current_snapshot.resolution_level
            and stable_snapshot.surface_key == current_snapshot.surface_key
            and stable_snapshot.evidence_fingerprint == current_snapshot.evidence_fingerprint
            and stable_snapshot.catalog_version == current_snapshot.catalog_version
            and stable_snapshot.descriptor_revisions == current_snapshot.descriptor_revisions
        ):
            return None

        new_snapshot = build_effective_metadata_snapshot_from_runtime(
            inverter=inverter,
            selection=effective_selection,
            confidence=confidence,
            generation=max(int(current_snapshot.generation), 0) + 1,
            generated_at=datetime.now(timezone.utc),
        )
        if not new_snapshot.is_valid:
            return None
        return new_snapshot

    def _endpoint_effective_parts(self, endpoint: str) -> tuple[str, int, str]:
        try:
            return _resolve_collector_server_endpoint(
                endpoint,
                cloud_family=self.collector_cloud_family,
            )
        except ValueError:
            return "", 0, ""

    async def _async_reconcile_managed_collector_endpoint(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Keep collector parameter 21 aligned when the integration manages it.

        The automatic per-poll write/restore is gated on the explicit
        ``endpoint_control_policy`` axis. Under ``external`` the integration
        never silently writes, restores, or auto-heals the endpoint -- it only
        surfaces the endpoint as diagnostic state. Only ``integration_managed``
        (the integration previously wrote the endpoint through an explicit user
        action) may reconcile it automatically. Explicit user actions (the
        verified connection-strategy transition and the low-level bind/rollback
        services) are separate and are not gated here.

        An active shadow-learning route temporarily OWNS the collector endpoint
        (it is the live-wire safety boundary of the scan), so the per-poll
        reconcile must not touch the endpoint while it runs, regardless of the
        policy axis. This is route ownership, not operation-mode coupling: it is
        a no-op that never writes or restores.
        """

        snapshot.values.pop("collector_operation_endpoint_sync_error", None)
        if await self._async_shadow_learning_owns_endpoint():
            snapshot.values["collector_operation_endpoint_sync_status"] = "shadow_learning_active"
            return
        if not may_auto_manage_endpoint(self.endpoint_control_policy):
            self._collector_operation_pending_target_endpoint = ""
            snapshot.values["collector_operation_endpoint_sync_status"] = "external_not_managed"
            return
        current_endpoint = snapshot.collector_server_endpoint
        current_parts = self._endpoint_effective_parts(current_endpoint)
        pending_target_endpoint = str(
            getattr(self, "_collector_operation_pending_target_endpoint", "") or ""
        ).strip()
        pending_target_parts = self._endpoint_effective_parts(pending_target_endpoint)
        # integration_managed always keeps the collector pointed at Home
        # Assistant -- that is what "the integration manages this endpoint" means.
        # The legacy collector_operation_mode and the hostname "looks like a local
        # callback" heuristic are no longer consulted, and the endpoint is never
        # auto-restored here: restoring the previous endpoint is an explicit user
        # action (the rollback service/button), which also flips the axis back to
        # external.
        target_endpoint = self.collector_callback_target_endpoint
        if not target_endpoint:
            self._collector_operation_pending_target_endpoint = ""
            snapshot.values["collector_operation_endpoint_sync_status"] = "target_unavailable"
            return

        await self._async_prepare_home_assistant_callback_listener(target_endpoint)

        target_parts = self._endpoint_effective_parts(target_endpoint)
        pending_matches_target = bool(
            pending_target_parts[0] and pending_target_parts == target_parts
        )
        if pending_matches_target and not snapshot.connected:
            snapshot.values["collector_operation_endpoint_sync_status"] = "waiting_for_collector"
            return

        if current_parts == target_parts and current_parts[0]:
            self._collector_operation_pending_target_endpoint = ""
            snapshot.values["collector_operation_endpoint_sync_status"] = "aligned"
            return

        if pending_matches_target and snapshot.connected and not current_endpoint:
            self._collector_operation_pending_target_endpoint = ""
            snapshot.set_collector_server_endpoint(pending_target_endpoint)
            snapshot.values["collector_operation_endpoint_sync_status"] = "aligned"
            return

        if not snapshot.connected:
            snapshot.values["collector_operation_endpoint_sync_status"] = "waiting_for_collector"
            return

        try:
            normalized_current = _normalize_preserved_collector_server_endpoint(current_endpoint)
        except ValueError:
            normalized_current = current_endpoint
        signature = (normalized_current, target_endpoint)
        now = asyncio.get_running_loop().time()
        if (
            signature == self._ha_primary_reconcile_last_signature
            and now - self._ha_primary_reconcile_last_attempt_monotonic
            < _COLLECTOR_HA_PRIMARY_RECONCILE_COOLDOWN_SECONDS
        ):
            snapshot.values["collector_operation_endpoint_sync_status"] = "cooldown"
            return

        # CP2C: the automatic reconcile is best-effort. When ANOTHER endpoint
        # operation owns the entry (a transition, proxy capture, shadow learning,
        # a manual write or bind/rollback), SILENTLY skip the write with an honest
        # diagnostic status and no cooldown stamp (so it retries once free) --
        # never breaking the refresh and never a second endpoint owner.
        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as _OP_AUTHORITY,
            OPERATION_RECONCILE_ENDPOINT as _OP_RECONCILE,
        )

        _op = _OP_AUTHORITY.acquire(self.config_entry.entry_id, _OP_RECONCILE)
        if not _op.acquired:
            snapshot.values["collector_operation_endpoint_sync_status"] = (
                "operation_busy"
            )
            return
        self._ha_primary_reconcile_last_signature = signature
        self._ha_primary_reconcile_last_attempt_monotonic = now
        try:
            result = await self._runtime.async_set_collector_server_endpoint(
                target_endpoint,
                apply_changes=True,
            )
        except Exception as exc:
            snapshot.values["collector_operation_endpoint_sync_status"] = "failed"
            snapshot.values["collector_operation_endpoint_sync_error"] = str(exc)
            logger.warning(
                "Failed to align collector callback endpoint for Home Assistant only mode: current=%s target=%s error=%s",
                current_endpoint or "unknown",
                target_endpoint,
                exc,
            )
            return
        finally:
            _OP_AUTHORITY.release(self.config_entry.entry_id, _op.token)

        snapshot.set_collector_server_endpoint(
            str(
                result.get("readback_endpoint")
                or result.get("requested_endpoint")
                or target_endpoint
            )
        )
        self._collector_operation_pending_target_endpoint = (
            snapshot.collector_server_endpoint
        )
        snapshot.values["collector_operation_endpoint_sync_status"] = str(
            result.get("status") or "applied"
        )

    async def _async_shadow_learning_owns_endpoint(self) -> bool:
        """Return whether an active shadow-learning route owns the collector endpoint.

        New learning sessions start from HA-only and do not rewrite the endpoint,
        but the active learning proxy still owns its route as the safety boundary
        of the scan. Per-poll reconcile must leave that endpoint alone: a
        concurrent realignment could move traffic outside the learning proxy.
        This is route ownership (like the callback-session registry), not a
        collector_operation_mode decision, and it never writes or restores here.
        """

        if self._shadow_learning_process_running():
            return True
        try:
            state = await self._async_active_shadow_learning_state(require_process=False)
        except Exception:
            return False
        return bool(state is not None and shadow_learning_session_is_active(state))


__all__ = ["CoordinatorRuntimeProfileMixin"]
