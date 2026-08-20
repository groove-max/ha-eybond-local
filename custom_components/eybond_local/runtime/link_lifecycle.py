"""LinkLifecycleMixin ownership slice for the runtime link."""

from __future__ import annotations

from .link_common import (
    logger,
)


class LinkLifecycleMixin:
    """Methods owned by LinkLifecycleMixin."""

    async def async_start(self) -> None:
        """Start the active link transport and its discovery loop."""

        await self._rebuild_if_server_ip_changed(reason="runtime_start")
        self._listener_status = "starting"
        try:
            await self._start_all_transports()
        except Exception as exc:
            self._started = False
            self._record_listener_error(exc)
            await self._stop_all_transports()
            raise

        self._started = True
        self._listener_status = "listening"
        self._listener_last_error = ""
        self._start_owned_session_monitor()
        # Phase 3: no continuous announcer. callback_on_demand sends a one-shot
        # trigger per connect attempt (async_try_connect); nothing runs here.
        await self._announcer.stop()

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Re-resolve the collector-facing host and rebuild listeners if it changed."""

        was_started = self._started
        changed = await self._rebuild_if_server_ip_changed(reason=reason)
        if not changed or not was_started:
            return changed

        self._listener_status = "starting"
        try:
            await self._start_all_transports()
        except Exception as exc:
            self._started = False
            self._record_listener_error(exc)
            await self._stop_all_transports()
            raise

        self._listener_status = "listening"
        self._listener_last_error = ""
        self._started = True
        await self._announcer.stop()
        return True

    async def async_reconcile_collector_session_profile(
        self,
        *,
        collector_session_protocol: str,
        collector_identity_strategy: str,
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
        reason: str = "collector_session_profile_change",
    ) -> bool:
        """Rebuild transports when the resolved callback session profile changes."""

        normalized_protocol = str(collector_session_protocol or "").strip().lower()
        normalized_strategy = str(collector_identity_strategy or "").strip().lower()
        normalized_raw_bootstrap = str(collector_raw_passthrough_bootstrap or "").strip().lower()
        normalized_raw_frame = str(collector_raw_passthrough_frame_format or "").strip().lower()
        normalized_raw_min_interval_ms = max(
            0,
            int(collector_raw_passthrough_min_interval_ms or 0),
        )
        if (
            normalized_protocol == self._configured_collector_session_protocol
            and normalized_strategy == self._collector_identity_strategy
            and normalized_raw_bootstrap == self._collector_raw_passthrough_bootstrap
            and normalized_raw_frame == self._collector_raw_passthrough_frame_format
            and normalized_raw_min_interval_ms == self._collector_raw_passthrough_min_interval_ms
        ):
            return False

        # A live conflict (contradictory wire observation) blocks any profile
        # rebuild until new NON-contradictory positive live evidence appears.
        # Tearing transports down on top of a conflict would destroy a working
        # listener and act on evidence we have explicitly rejected. Preserve
        # the conflict until a positive wire observation resolves it.
        live = self._live_session_handle()
        if live.conflict:
            logger.debug(
                "Ignoring session-profile reconcile after %s: live session is in "
                "an unresolved wire conflict (%s); preserving the confirmed wire",
                reason or "collector_session_profile_change",
                live.conflict,
            )
            return False

        # Live session handover is NOT a profile change. The confirmed wire
        # binding is the authority: a reconcile request whose protocol
        # contradicts it is untrusted configuration, not wire evidence. Tearing
        # transports down for it caused framed->at_text->framed flapping and
        # needless re-onboarding. Rebuild only when no wire has been confirmed
        # yet or when the requested protocol is positively observed live.
        # Steady-state live wire changes go through set_negotiated_wire.
        binding = self._effective_wire_binding()
        confirmed_protocol = binding.session_protocol if binding is not None else ""
        if (
            confirmed_protocol
            and normalized_protocol
            and normalized_protocol != confirmed_protocol
            and self._raw_live_observed_protocol() != normalized_protocol
        ):
            logger.debug(
                "Ignoring session-profile reconcile after %s: requested protocol %s "
                "contradicts the confirmed live wire %s with no live evidence "
                "(transient reconnect handover, not a profile change)",
                reason or "collector_session_profile_change",
                normalized_protocol or "unknown",
                confirmed_protocol,
            )
            return False

        logger.warning(
            "EyeBond callback session profile changed after %s: protocol %s -> %s, identity %s -> %s, raw_bootstrap %s -> %s, raw_frame %s -> %s, raw_min_interval_ms %s -> %s; rebuilding transport",
            reason or "collector_session_profile_change",
            self._configured_collector_session_protocol or "unknown",
            normalized_protocol or "unknown",
            self._collector_identity_strategy or "unknown",
            normalized_strategy or "unknown",
            self._collector_raw_passthrough_bootstrap or "unknown",
            normalized_raw_bootstrap or "unknown",
            self._collector_raw_passthrough_frame_format or "unknown",
            normalized_raw_frame or "unknown",
            self._collector_raw_passthrough_min_interval_ms,
            normalized_raw_min_interval_ms,
        )
        was_started = self._started
        if was_started:
            await self._announcer.stop()
            await self._stop_all_transports()

        self._configured_collector_session_protocol = normalized_protocol
        self._collector_identity_strategy = normalized_strategy
        self._collector_raw_passthrough_bootstrap = normalized_raw_bootstrap
        self._collector_raw_passthrough_frame_format = normalized_raw_frame
        self._collector_raw_passthrough_min_interval_ms = normalized_raw_min_interval_ms
        self._rebuild_link(self._effective_server_ip)
        self._listener_rebind_count += 1

        if not was_started:
            return True

        self._listener_status = "starting"
        try:
            await self._start_all_transports()
        except Exception as exc:
            self._started = False
            self._record_listener_error(exc)
            await self._stop_all_transports()
            raise

        self._listener_status = "listening"
        self._listener_last_error = ""
        self._started = True
        await self._announcer.stop()
        return True

    async def async_stop(self) -> None:
        """Stop discovery and the active link transport."""

        await self._stop_owned_session_monitor()
        await self.async_stop_proxy_capture_route(force=True)
        await self.async_stop_shadow_learning_route(force=True)
        await self._announcer.stop()
        await self._stop_all_transports()
        self._started = False
        self._listener_status = "stopped"

    async def async_ensure_callback_listener(self, port: int) -> None:
        """Ensure one auxiliary callback listener is available for collector redirects."""

        requested_port = int(port or 0)
        if requested_port <= 0 or requested_port == self._tcp_port:
            return

        if requested_port not in self._auxiliary_listener_ports:
            self._auxiliary_listener_ports.add(requested_port)
            payload_transport, at_transport = self._build_transport_pair(
                self._listener_bind_host,
                requested_port,
            )
            self._auxiliary_transports[requested_port] = payload_transport
            self._auxiliary_at_transports[requested_port] = at_transport
            self._apply_collector_connection_watcher()

        try:
            await self._auxiliary_transports[requested_port].start()
            await self._auxiliary_at_transports[requested_port].start()
        except Exception as exc:
            self._record_listener_error(exc)
            raise
