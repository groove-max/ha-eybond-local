"""LinkCallbackMixin ownership slice for the runtime link."""

from __future__ import annotations

from .common import (
    CALLBACK_STATE_CLAIMED_BY_OTHER,
    CALLBACK_STATE_CONNECTED,
    CALLBACK_STATE_IDENTITY_MISMATCH,
    CALLBACK_STATE_LISTENER_ERROR,
    CALLBACK_STATE_LISTENER_UNAVAILABLE,
    CALLBACK_STATE_TIMEOUT,
    CallbackSessionRegistry,
    _CALLBACK_TRIGGER_TIMEOUT,
    _callback_state_message,
    async_send_callback_trigger,
    asyncio,
    logger,
    pn_is_same_identity,
)


class LinkCallbackMixin:
    """Methods owned by LinkCallbackMixin."""

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        """Send one explicit UDP bootstrap probe without enabling background discovery."""

        target_ip = str(self._collector_ip or self._discovery_target or "").strip()
        if not target_ip:
            raise RuntimeError("collector_discovery_target_unavailable")

        advertised_port = int(port or self._configured_advertised_tcp_port or self._tcp_port)
        probe = await async_send_callback_trigger(
            bind_ip=self._effective_server_ip,
            advertised_server_ip=self.effective_advertised_server_ip,
            advertised_server_port=advertised_port,
            target_ip=target_ip,
            udp_port=self._udp_port,
            timeout=float(timeout),
            source="runtime_manual_trigger",
        )
        self._announcer.last_reply = probe.reply
        self._announcer.last_reply_from = probe.reply_from
        return {
            "status": "reply_received" if probe.reply else "probe_sent",
            "target_ip": probe.target_ip,
            "advertised_endpoint": (
                f"{self.effective_advertised_server_ip}:{advertised_port}"
            ),
            "message": probe.message,
            "reply": probe.reply,
            "reply_from": probe.reply_from,
            "local_port": probe.local_port,
        }

    def set_callback_ownership(
        self,
        registry: CallbackSessionRegistry | None,
        entry_id: str,
    ) -> None:
        """Inject the domain callback-session registry + this entry id.

        The domain registry (the one passive discovery feeds from EVERY shared
        listener in the process) is the single transport-ownership authority:
        when it is installed, the runtime resolves its owned live SessionHandle,
        the exact claimed session id, and the listener port the collector
        actually dialed from it -- under the REAL config entry id claimed at
        setup. It never reads listener internals; ownership stays PN/session
        based (peer IP is never a key). Without a domain registry (standalone
        hubs, unit tests) the runtime falls back to its own listener-scoped
        registry, so the two ownership paths are never active at the same time.
        """

        self._callback_ownership_registry = registry
        self._callback_entry_id = str(entry_id or "").strip()

    async def _send_callback_trigger(self) -> None:
        """Send exactly ONE UDP callback trigger for a callback_on_demand attempt.

        This is the one-shot replacement for the old continuous
        ``DiscoveryAnnouncer`` loop: one datagram per connect attempt, never a
        repeating N-second broadcast. The bounded wait for the inbound session
        happens after this returns. ``collector_ip``/``discovery_target`` are only
        the UDP target here, not identity.
        """

        target_ip = str(self._collector_ip or self._discovery_target or "").strip()
        if not target_ip:
            # No UDP target to poke; the collector may still dial in on its own,
            # and the bounded wait handles that.
            return
        advertised_port = int(self._configured_advertised_tcp_port or self._tcp_port)
        self._callback_trigger_count += 1
        try:
            probe = await async_send_callback_trigger(
                bind_ip=self._effective_server_ip,
                advertised_server_ip=self.effective_advertised_server_ip,
                advertised_server_port=advertised_port,
                target_ip=target_ip,
                udp_port=self._udp_port,
                source="runtime_callback_on_demand",
                timeout=_CALLBACK_TRIGGER_TIMEOUT,
            )
        except Exception as exc:  # pragma: no cover - defensive UDP send guard
            logger.debug("EyeBond one-shot callback trigger send failed: %s", exc)
            return
        self._announcer.last_reply = probe.reply
        self._announcer.last_reply_from = probe.reply_from

    def _callback_ownership_owner_for_pn(self, collector_pn: str) -> str:
        registry = self._callback_ownership_registry
        if registry is None:
            return ""
        try:
            return str(registry.owner_for_pn(collector_pn) or "")
        except Exception:
            return ""

    def _callback_listener_ready(self) -> bool:
        """Return whether a one-shot callback trigger has a ready listener."""

        return bool(self._started and self._listener_status == "listening")

    def _observed_foreign_session_exists(self, collector_pn: str) -> bool:
        """Return whether an inbound session with a NON-matching PN is observed."""

        from ...collector_identity import pn_is_same_identity

        for session in self._session_registry.observed_sessions():
            observed_pn = str(session.collector_pn or "").strip()
            if observed_pn and not pn_is_same_identity(collector_pn, observed_pn):
                return True
        return False

    def _classify_callback_failure(self) -> tuple[str, str]:
        """Classify why a callback_on_demand attempt did not connect (typed)."""

        if not self._started or self._listener_status == "error":
            detail = str(self._listener_last_error or "").strip()
            if detail:
                return CALLBACK_STATE_LISTENER_ERROR, detail
            return CALLBACK_STATE_LISTENER_UNAVAILABLE, self._listener_status
        collector_pn = str(self._collector_pn or "").strip()
        if collector_pn:
            if self._matching_live_session_exists(collector_pn):
                # Our collector's session is here but we did not connect: when a
                # DIFFERENT entry owns this identity in the domain registry, the
                # claim (not the network) is what blocked us -> typed conflict.
                owner = self._callback_ownership_owner_for_pn(collector_pn)
                if owner and self._callback_entry_id and owner != self._callback_entry_id:
                    return CALLBACK_STATE_CLAIMED_BY_OTHER, owner
                return CALLBACK_STATE_TIMEOUT, "session_not_yet_connected"
            if self._observed_foreign_session_exists(collector_pn):
                return CALLBACK_STATE_IDENTITY_MISMATCH, ""
        return CALLBACK_STATE_TIMEOUT, ""

    def _matching_live_session_exists(self, collector_pn: str) -> bool:
        """Return whether ANY live session of this durable identity is observed.

        Ownership-independent on purpose: classification must see the session
        even when a different entry owns it (that is exactly the
        claimed-by-other-entry outcome). Domain registry first (all shared
        listeners), else this runtime's own listener-scoped view.
        """

        from ...collector_identity import pn_is_same_identity

        registry = getattr(self, "_callback_ownership_registry", None)
        if registry is not None:
            try:
                sessions = registry.observed_sessions_per_socket()
            except Exception:
                sessions = ()
            for session in sessions:
                state = str(getattr(session, "state", "") or "").strip().lower()
                if state.startswith("closed"):
                    continue
                if pn_is_same_identity(
                    collector_pn, str(getattr(session, "collector_pn", "") or "")
                ):
                    return True
        # This runtime's own listeners are real observations too (the domain
        # registry may not cover every listener in test/standalone setups).
        for session in self._session_registry.observed_sessions():
            if pn_is_same_identity(
                collector_pn, str(session.collector_pn or "")
            ):
                return True
        return False

    def _record_callback_state(self, state: str, detail: str = "") -> None:
        self._last_callback_state = state
        self._last_callback_detail = str(detail or "")

    def _note_callback_failure(self) -> None:
        # Only meaningful for callback_on_demand; and only a real failure when we
        # are not connected (a heartbeat-only timeout keeps the CONNECTED state).
        if not self._reverse_discovery_enabled or self.connected:
            return
        state, detail = self._classify_callback_failure()
        self._record_callback_state(state, detail)

    def _note_callback_connected(self) -> None:
        if self._reverse_discovery_enabled and self.connected:
            self._record_callback_state(CALLBACK_STATE_CONNECTED)

    def callback_trigger_diagnostics(self) -> dict[str, object]:
        """Return typed callback_on_demand trigger/outcome diagnostics."""

        return {
            "collector_callback_on_demand": bool(self._reverse_discovery_enabled),
            "collector_callback_trigger_count": self._callback_trigger_count,
            "collector_callback_state": self._last_callback_state,
            "collector_callback_state_detail": self._last_callback_detail,
            "collector_callback_state_message": _callback_state_message(
                self._last_callback_state
            ),
        }

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        """Control whether UDP reverse discovery may redirect the collector."""

        was_enabled = self._reverse_discovery_enabled
        self._reverse_discovery_enabled = bool(enabled)
        if was_enabled and not self._reverse_discovery_enabled:
            announcer = self._announcer
            announcer.last_reply = ""
            announcer.last_reply_from = ""
            if getattr(announcer, "running", False):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
                loop.create_task(
                    announcer.stop(),
                    name="eybond_stop_reverse_discovery_announcer",
                )
