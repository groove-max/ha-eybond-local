"""Setup, exact-session activation, and shutdown lifecycle."""

from __future__ import annotations

import asyncio
import logging

from homeassistant.components import persistent_notification

from ...const import CONF_COLLECTOR_PN, DOMAIN

logger = logging.getLogger(__name__)


class CoordinatorLifecycleMixin:
    """Own coordinator startup and teardown sequencing."""

    async def async_setup(self) -> None:
        """Start the underlying hub."""

        self._configure_reverse_discovery_mode()
        self._configure_callback_ownership()
        await self._runtime.async_start()
        await self._async_activate_owned_callback_session_on_setup()
        if self.collector_callback_listener_required:
            await self._async_prepare_home_assistant_callback_listener(
                self.collector_callback_target_endpoint
            )
        await self._async_recover_proxy_capture_state()
        await self._async_recover_shadow_learning_state()
        await self._async_warm_smartess_cloud_evidence_cache()
        await self._async_warm_effective_metadata_cache()

    async def _async_activate_owned_callback_session_on_setup(self) -> bool:
        """Consume an exact permanent handoff before background refresh starts.

        A config-flow recovery may complete its registry handoff before this
        coordinator exists.  Starting the runtime alone does not consume the
        parked socket, while the initial HA refresh is intentionally allowed to
        continue in the background.  Activate the exact claimed session here,
        while setup still has sole access to the runtime, so that the first
        refresh cannot race it or fall back to a new callback attempt.

        A normal cold start carries only a durable PN claim and therefore has no
        session id to activate.  This method then returns immediately.  It never
        sends UDP and never substitutes a same-PN session.
        """

        entry_id = str(getattr(self.config_entry, "entry_id", "") or "").strip()
        try:
            from ...passive_discovery import get_callback_session_registry

            registry = get_callback_session_registry(self.hass)
        except Exception:
            # A minimal standalone/test runtime may not install the domain
            # discovery service.  It has no permanent handoff to consume.
            registry = None
        if registry is None or not entry_id:
            return False
        session_id = str(registry.claimed_session_id(entry_id) or "").strip()
        if not session_id:
            return False
        certification = registry.certify_permanent_owned_session(
            entry_id,
            session_id,
        )
        if certification is None:
            return False
        activated = await self._runtime.async_activate_claimed_session(
            expected_session_id=session_id,
            timeout=5.0,
        )
        completed = bool(
            activated
            and registry.reverify_permanent_owned_session(certification)
        )
        if completed:
            # Ephemeral runtime receipt: the exact recovery-certified socket was
            # consumed by the payload transport during this setup.  A first
            # metadata refresh may immediately request a legitimate entry
            # reload, so a later instantaneous ``connected`` check cannot be the
            # proof of this already-completed handoff.
            self._setup_callback_activation_receipt = (
                certification.owner_id,
                certification.session_id,
                certification.collector_pn,
            )
            self._runtime_connected_event.set()
        return completed

    async def async_activate_proven_callback_session(
        self,
        certification: object,
        *,
        timeout: float = 5.0,
    ) -> bool:
        """Adopt the exact recovery-certified socket into the runtime transport.

        ``ConfigEntryState.LOADED`` only means that HA created the coordinator;
        setup intentionally permits its first live refresh to continue in the
        background.  A strategy transition needs a stronger postcondition: the
        exact session certified by recovery must be the socket the payload
        runtime has actually activated.  This method provides that boundary and
        never emits a callback trigger or searches for a same-PN substitute.
        """

        from ...connection.session_registry import PermanentOwnedSessionCertification
        from ...passive_discovery import get_callback_session_registry

        if type(certification) is not PermanentOwnedSessionCertification:
            return False
        entry_id = str(self.config_entry.entry_id or "").strip()
        registry = get_callback_session_registry(self.hass)
        if registry is None or certification.owner_id != entry_id:
            return False
        receipt = getattr(self, "_setup_callback_activation_receipt", None)
        if receipt == (
            certification.owner_id,
            certification.session_id,
            certification.collector_pn,
        ):
            return True
        if not registry.reverify_permanent_owned_session(certification):
            return False

        async with self._runtime_operation_lock:
            activated = await self._runtime.async_activate_claimed_session(
                expected_session_id=certification.session_id,
                timeout=max(0.0, float(timeout)),
            )
        return bool(
            activated
            and registry.reverify_permanent_owned_session(certification)
        )

    async def async_ensure_callback_runtime_ready(
        self,
        *,
        timeout: float,
    ) -> bool:
        """Load-only recovery postcondition: payload connected to the owned PN."""

        from ...passive_discovery import get_callback_session_registry

        entry_id = str(self.config_entry.entry_id or "").strip()
        collector_pn = str(
            self.config_entry.data.get(CONF_COLLECTOR_PN, "") or ""
        ).strip()
        registry = get_callback_session_registry(self.hass)
        if (
            registry is None
            or not entry_id
            or not collector_pn
            or registry.owner_for_pn(collector_pn) != entry_id
        ):
            return False
        try:
            await asyncio.wait_for(
                self._runtime_connected_event.wait(),
                timeout=max(0.0, float(timeout)),
            )
        except asyncio.TimeoutError:
            return False
        return bool(
            registry.owner_for_pn(collector_pn) == entry_id
        )

    async def async_shutdown(self) -> None:
        """Stop the underlying hub."""

        async with self._shutdown_lock:
            if self._shutdown_complete:
                return
            self._shutdown_complete = True
            if self._entry_loaded_reload_unsub is not None:
                self._entry_loaded_reload_unsub()
                self._entry_loaded_reload_unsub = None
            if self._component_loaded_reload_unsub is not None:
                self._component_loaded_reload_unsub()
                self._component_loaded_reload_unsub = None
            if getattr(self, "_inverter_protocol_notification_active", False):
                self._inverter_protocol_notification_active = False
                persistent_notification.async_dismiss(
                    self.hass,
                    f"{DOMAIN}_inverter_protocol_ambiguous_{self.config_entry.entry_id}",
                )
            await self._async_cancel_diagnostic_run()
            await self._support_package_flight.cancel()
            self._cancel_proxy_capture_deadline_refresh()
            set_snapshot_observer = getattr(
                self._runtime,
                "set_runtime_snapshot_observer",
                None,
            )
            if callable(set_snapshot_observer):
                set_snapshot_observer(None)
            set_overlay_applier = getattr(
                self._runtime,
                "set_inverter_overlay_applier",
                None,
            )
            if callable(set_overlay_applier):
                set_overlay_applier(None)
            set_connection_watcher = getattr(
                self._runtime,
                "set_collector_connection_watcher",
                None,
            )
            if callable(set_connection_watcher):
                set_connection_watcher(None)
            try:
                await self.async_stop_shadow_learning(
                    reason="shutdown",
                    request_refresh=False,
                    raise_when_not_running=False,
                )
            except Exception as exc:
                logger.warning(
                    "Shadow learning shutdown cleanup failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
            await self._async_stop_proxy_capture_process(force=True)
            await self._runtime.async_stop()
        # Base-class teardown (debouncer shutdown, unschedule refresh) must
        # run too, or a queued request_refresh can still drive a poll against
        # the stopped link.
        await super().async_shutdown()



__all__ = ["CoordinatorLifecycleMixin"]
