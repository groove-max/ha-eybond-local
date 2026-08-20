"""Network and collector-session profile reconciliation."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class CoordinatorNetworkReconcileMixin:
    """Reconcile runtime links while preserving the runtime manager authority."""

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Reconcile listener bind/discovery state after HA or network readiness changes."""

        changed = await self._async_reconcile_network(reason=reason)
        if changed:
            if self.collector_callback_listener_required:
                await self._async_prepare_home_assistant_callback_listener(
                    self.collector_callback_target_endpoint
                )
            await self.async_request_refresh()
        return changed

    async def _async_reconcile_network(self, *, reason: str) -> bool:
        reconcile = getattr(self._runtime, "async_reconcile_network", None)
        if reconcile is None:
            return False
        changed = bool(await reconcile(reason=reason))
        if changed:
            self._ha_primary_reconcile_last_signature = ("", "")
            logger.warning(
                "Reconciled EyeBond listener network state for entry %s after %s",
                self.config_entry.entry_id,
                reason or "network_change",
            )
        return changed

    async def _async_reconcile_collector_session_profile(self, *, reason: str) -> bool:
        """Align dialect metadata with an independently confirmed live wire.

        Cloud family never chooses the wire. Once a live or persisted PN-bound
        observation establishes the protocol, provider metadata may refine that
        confirmed protocol's forwarding dialect. The live session remains the
        sole transport authority:
        a real framed<->at_text change is applied in place by the link's
        SessionHandle wire negotiation (no destructive rebuild), and this per-
        poll profile computation must never tear transports down.
        """

        has_confirmed_binding = getattr(
            self._runtime, "has_confirmed_wire_binding", None
        )
        if callable(has_confirmed_binding):
            try:
                if has_confirmed_binding():
                    return False
            except Exception:  # pragma: no cover - defensive runtime inspection
                pass

        protocol = self.collector_session_protocol
        identity_strategy = self.collector_identity_strategy
        raw_passthrough_bootstrap = self.collector_raw_passthrough_bootstrap
        raw_passthrough_frame_format = self.collector_raw_passthrough_frame_format
        raw_passthrough_min_interval_ms = self.collector_raw_passthrough_min_interval_ms
        if (
            not protocol
            and not identity_strategy
            and not raw_passthrough_bootstrap
            and not raw_passthrough_frame_format
            and raw_passthrough_min_interval_ms <= 0
        ):
            return False

        reconcile = getattr(self._runtime, "async_reconcile_collector_session_profile", None)
        if reconcile is None:
            return False

        changed = bool(
            await reconcile(
                collector_session_protocol=protocol,
                collector_identity_strategy=identity_strategy,
                collector_raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                collector_raw_passthrough_frame_format=raw_passthrough_frame_format,
                collector_raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
                reason=reason,
            )
        )
        if changed:
            logger.warning(
                "Reconciled EyeBond collector session profile for entry %s after %s: protocol=%s identity=%s raw_bootstrap=%s raw_frame=%s raw_min_interval_ms=%s",
                self.config_entry.entry_id,
                reason or "collector_session_profile_change",
                protocol or "unknown",
                identity_strategy or "unknown",
                raw_passthrough_bootstrap or "unknown",
                raw_passthrough_frame_format or "unknown",
                raw_passthrough_min_interval_ms,
            )
        return changed



__all__ = ["CoordinatorNetworkReconcileMixin"]
