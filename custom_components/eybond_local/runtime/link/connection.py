"""LinkConnectionMixin ownership slice for the runtime link."""

from __future__ import annotations

from .common import (
    ADAPTER_INVERTER_AT_MIXED,
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    _RUNTIME_CAUSALITY_LEASE_WAIT,
    asyncio,
    logger,
    pn_is_same_identity,
)


class LinkConnectionMixin:
    """Methods owned by LinkConnectionMixin."""

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        """Drop current collector sockets without restarting discovery."""

        logger.warning(
            "Disconnecting collector runtime connections after %s remote=%s configured_collector_ip=%s",
            reason or "runtime_disconnect",
            self.collector_info.remote_ip or "unknown",
            self._collector_ip or "unknown",
        )
        await self._disconnect_all_transports()

    async def _async_follow_owned_session_listener(self) -> None:
        """Attach a transport facade to the listener the owned session lives on.

        The primary configured tcp_port stays the callback target/fallback, but
        it must not limit ownership of an already-accepted PN session: when the
        domain registry shows this entry's collector on a DIFFERENT shared
        listener port, bring up the auxiliary facade for that port so the claim
        can activate exactly that socket. The port comes ONLY from a live
        observed owned session (never from the endpoint hostname, peer IP, or
        collector type), and the facade attaches to the already-running shared
        listener -- it does not open arbitrary ports.
        """

        session = self._owned_domain_session()
        if session is None:
            return
        port = int(getattr(session, "listener_port", 0) or 0)
        if port <= 0 or port == self._tcp_port:
            return
        already_prepared = port in self._auxiliary_listener_ports
        try:
            await self.async_ensure_callback_listener(port)
        except Exception as exc:
            logger.debug(
                "Could not attach facade to owned-session listener %s: %s",
                port,
                exc,
            )
            return
        if not already_prepared:
            logger.info(
                "Following owned collector session %s to listener port %s (primary %s)",
                str(getattr(session, "session_id", "") or "unknown"),
                port,
                self._tcp_port,
            )
            # New facades must receive the negotiated wire + exact claim target.
            self._apply_live_wire_to_transports()

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        """Try to ensure a live collector connection without raising on timeout."""

        if self._route_lease is not None and not self.connected:
            # Proxy/shadow owns the post-redirect reconnect. Runtime must not
            # send a callback trigger concurrently and create a second framed
            # HA session that races the new cloud session for the shared
            # listener.
            return False

        # Transport ownership end-to-end: if the domain registry shows this
        # entry's owned session on another already-running shared listener,
        # attach the facade there BEFORE waiting, so inbound entries connect to
        # the socket the collector actually opened (no UDP involved).
        # A weak location must be challenged while it is still parked on the
        # shared listener. Starting the auxiliary transport first registers a
        # PN owner and can route that socket into a live connection before the
        # exact-session channel gets a chance to upgrade it. Follow immediately
        # only for an already-strong location; the weak path follows after the
        # successful challenge below.
        if not self._owned_session_requires_identity_upgrade():
            await self._async_follow_owned_session_listener()
        self._apply_live_wire_to_transports()

        # An inbound entry never sends set>server, but its already-open socket
        # can still volunteer only a weak heartbeat prefix.  Upgrade that exact
        # owned session in place before transport activation.  This is the
        # trigger-free counterpart of the callback causality path below: the
        # registry supplies the socket identity, and FC=2/AT+DTUPN supplies the
        # strong collector identity.  Peer IP and weak PN prefixes remain
        # ineligible for routing.
        if (
            not self._reverse_discovery_enabled
            and not self.connected
            and self._owned_session_requires_identity_upgrade()
        ):
            deadline = asyncio.get_running_loop().time() + max(
                0.0, float(timeout)
            )
            upgraded = await self._async_upgrade_owned_session_without_trigger(
                timeout=max(0.0, deadline - asyncio.get_running_loop().time())
            )
            if not upgraded:
                return False
            timeout = max(0.0, deadline - asyncio.get_running_loop().time())

        # A registry-owned exact session is already causally certified for this
        # entry. It may still be parked while the freshly-created transport
        # facade starts, so ``self.connected`` can be false even though the
        # physical socket is present and claimable. Activate/wait for that exact
        # socket BEFORE considering a new callback trigger. If it cannot be used,
        # fail this attempt closed; a later attempt may trigger only after the
        # registry no longer exposes the stale owned session. Never overwrite a
        # proven handoff with another set>server sequence merely because a
        # co-located foreign socket delayed facade activation.
        if (
            self._reverse_discovery_enabled
            and not self.connected
            and self._claimed_session_id()
        ):
            return await self._async_await_callback_session(
                timeout=timeout,
                require_heartbeat=require_heartbeat,
            )

        # callback_on_demand: send exactly ONE UDP callback trigger for this
        # attempt, then bounded-wait for the inbound session. inbound entries
        # have _reverse_discovery_enabled=False and never reach this, so they
        # never send a UDP trigger -- they only claim/wait for an already-inbound
        # session.
        if self._reverse_discovery_enabled and not self.connected:
            if not self._callback_listener_ready():
                self._note_callback_failure()
                return False
            # THE causal window of a callback_on_demand connect is trigger ->
            # session, not the datagram alone: the collector dials back seconds
            # later. Take the shared lease before the trigger and hold it across
            # the wait below, so no other attempt can snapshot a baseline while
            # OUR late session is still in flight and adopt it as its own answer.
            # Refusing the send while somebody else owns causality is not enough:
            # a datagram sent just before their lease still produces a session
            # inside their window.
            return await self._async_callback_connect_within_causality(
                timeout=timeout, require_heartbeat=require_heartbeat
            )

        return await self._async_await_callback_session(
            timeout=timeout, require_heartbeat=require_heartbeat
        )

    async def _async_upgrade_owned_session_without_trigger(
        self, *, timeout: float
    ) -> bool:
        """Strongly identify one already-owned inbound session without UDP.

        The session id and listener port come only from the ownership registry.
        The read-only identity query is pinned to that exact socket.  Failure,
        timeout, a foreign PN, or an unavailable probe channel leaves the weak
        session parked and cannot fall through to route/peer matching.
        """

        expected_pn = str(self._collector_pn or "").strip()
        session = self._owned_domain_session()
        session_id = str(
            getattr(session, "session_id", "")
            if session is not None
            else self._live_session_handle().session_id
        ).strip()
        challenge = self._identity_challenge_candidate()
        if not expected_pn or not session_id or not challenge:
            return False

        from ...collector.silent_session_probe import (
            SilentSessionIdentityProbeChannel,
        )

        probe_channel = SilentSessionIdentityProbeChannel(
            host=self._listener_bind_host,
            port=self._identity_challenge_listener_port(),
        )
        try:
            await probe_channel.async_open()
            if not probe_channel.available:
                return False
            self._active_identity_challenge_protocol = challenge
            try:
                identified_pn = await asyncio.wait_for(
                    probe_channel.async_identify_exact_session(
                        session_id,
                        session_protocol=challenge,
                    ),
                    timeout=max(0.0, float(timeout)),
                )
            except asyncio.TimeoutError:
                return False
            if not identified_pn or not pn_is_same_identity(
                expected_pn, identified_pn
            ):
                return False

            await self._async_follow_owned_session_listener()
            self._apply_live_wire_to_transports()
            return self._claimed_session_id() == session_id
        finally:
            self._active_identity_challenge_protocol = ""
            await probe_channel.async_close()

    async def async_activate_claimed_session(
        self,
        *,
        expected_session_id: str,
        timeout: float,
    ) -> bool:
        """Activate exactly one registry-certified callback socket, without UDP.

        Strategy-recovery has already established causality, strong identity and
        permanent ownership.  This boundary performs only the missing transport
        handoff: attach the runtime facade to the listener, route the exact
        claimed session into it and verify that the same claim is still active.
        It deliberately does not call :meth:`async_try_connect`, because losing
        the claim must fail closed instead of silently sending a new set>server.
        """

        expected = str(expected_session_id or "").strip()
        if not expected:
            return False
        if self._domain_ownership_active():
            registry = self._callback_ownership_registry
            if (
                registry.claimed_session_id(self._callback_entry_id) != expected
                or registry.session_handle_for_owned_session(
                    self._callback_entry_id,
                    expected,
                )
                is None
            ):
                return False
        elif self._claimed_session_id() != expected:
            return False

        self._activation_session_id = expected
        try:
            await self._async_follow_owned_session_listener()
            self._apply_live_wire_to_transports()
            if self._claimed_session_id() != expected:
                return False
            activated = await self._async_await_callback_session(
                timeout=max(0.0, float(timeout)),
                require_heartbeat=False,
            )
            return bool(
                activated
                and self.connected
                and self._claimed_session_id() == expected
            )
        finally:
            self._activation_session_id = ""
            # Restore the ordinary dynamic provider after the one exact
            # handoff has either completed or failed.
            self._apply_live_wire_to_transports()

    def _callback_attempt_seq(self) -> str:
        """A unique id for ONE runtime callback attempt.

        Deliberately opaque: never a peer IP, hostname, endpoint or PN. It only
        has to be unique per attempt so the coordinator can tell attempts apart.
        """

        self._callback_attempt_counter = getattr(self, "_callback_attempt_counter", 0) + 1
        return f"{id(self):x}:{self._callback_attempt_counter}"

    async def _async_callback_connect_within_causality(
        self, *, timeout: float, require_heartbeat: bool
    ) -> bool:
        """Own causality for ONE runtime callback_on_demand connect attempt.

        The lease is released only once this attempt reaches its terminal point
        (connected, or the bounded wait gave up), so a late session is always
        attributable to it and never to whoever ran next.
        """

        from ...connection.callback_ledger import (
            CallbackCausalityBusyError,
            get_callback_trigger_ledger,
        )

        attempt_id = f"runtime_callback:{self._callback_attempt_seq()}"
        try:
            async with get_callback_trigger_ledger().causality_lease(
                attempt_id, timeout=_RUNTIME_CAUSALITY_LEASE_WAIT
            ):
                challenge = self._identity_challenge_candidate()
                probe_channel = None
                deadline = asyncio.get_running_loop().time() + max(
                    0.0, float(timeout)
                )
                try:
                    baseline: frozenset[str] = frozenset()
                    from ...collector.silent_session_probe import (
                        SilentSessionIdentityProbeChannel,
                    )

                    probe_port = self._identity_challenge_listener_port()
                    probe_channel = SilentSessionIdentityProbeChannel(
                        host=self._listener_bind_host,
                        port=probe_port,
                    )
                    await probe_channel.async_open()
                    if probe_channel.available:
                        baseline = frozenset(
                            observation.session_id
                            for observation in (
                                probe_channel.snapshot_session_observations()
                            )
                            if observation.session_id
                        )
                        self._active_identity_challenge_protocol = challenge

                    await self._send_callback_trigger()

                    if probe_channel is not None and probe_channel.available:
                        result = await self._session_identity_negotiator.async_negotiate(
                            channel=probe_channel,
                            expected_pn=self._collector_pn,
                            baseline_session_ids=baseline,
                            deadline=deadline,
                            preferred_protocol=challenge,
                        )
                        if not result.identified:
                            # Candidate ordering is permission, never evidence.
                            # A timeout, foreign PN or ambiguous socket set must
                            # never fall through to route/peer matching.
                            self._note_callback_failure()
                            return False
                        # The exact-session challenge wrote only a strong PN
                        # into the public inventory. The ordinary registry now
                        # negotiates/owns/routes that socket; the cloud candidate
                        # itself never reaches transport authority.
                        await self._async_follow_owned_session_listener()
                        self._apply_live_wire_to_transports()
                    else:
                        # The challenge path is selected only while no trusted
                        # live identity exists.  If its exact-session channel is
                        # unavailable, fail closed instead of letting the
                        # transport claim by a weak PN prefix or peer route.
                        self._note_callback_failure()
                        return False

                    remaining = max(
                        0.0, deadline - asyncio.get_running_loop().time()
                    )
                    return await self._async_await_callback_session(
                        timeout=remaining,
                        require_heartbeat=require_heartbeat,
                    )
                finally:
                    self._active_identity_challenge_protocol = ""
                    if probe_channel is not None:
                        await probe_channel.async_close()
        except CallbackCausalityBusyError:
            # Someone else owns causality (an onboarding attempt, an inbound
            # verification). We did NOT trigger, so this is not a collector
            # failure: stay silent and let Home Assistant retry.
            logger.debug("Runtime callback deferred: causality is owned elsewhere")
            return False

    def _identity_challenge_candidate(self) -> str:
        """Return a non-endpoint-changing challenge hint when no wire is trusted.

        The candidate is metadata, not evidence. A live handle or a confirmed
        binding always owns the established path and suppresses this bootstrap.
        """

        if self._owned_session_requires_identity_upgrade():
            # A weak owned location is deliberately visible so this exact
            # session can be upgraded. Its observed byte shape may select the
            # non-mutating query dialect; durable metadata is only a fallback.
            session = self._owned_domain_session()
            shape = str(
                getattr(session, "protocol_shape", "") or ""
            ).strip().lower()
            if shape in ("eybond_framed", "eybond_framed_or_binary"):
                return "eybond_framed"
            if shape == "at_text":
                return "at_text"
            handle = self._live_session_handle()
            if handle.uses_framed_wire:
                return "eybond_framed"
            if handle.uses_at_text_wire:
                return "at_text"
            return self._configured_identity_challenge_protocol

        handle = self._live_session_handle()
        if handle.observed and not handle.conflict:
            return ""
        if self._effective_wire_binding() is not None:
            return ""
        return self._configured_identity_challenge_protocol

    def _identity_challenge_listener_port(self) -> int:
        """Return the listener that owns the exact session being challenged.

        The domain registry is the location authority across all shared
        listeners. A callback can remain parked on a listener different from
        this entry's primary port; probing the primary listener in that case
        can never upgrade the owned weak session. Fall back to the primary port
        only when no valid owned location is currently available.
        """

        session = self._owned_domain_session()
        listener_port = getattr(session, "listener_port", 0) if session else 0
        if type(listener_port) is int and 0 < listener_port <= 65535:
            return listener_port
        return self._tcp_port

    async def _async_await_callback_session(
        self, *, timeout: float, require_heartbeat: bool
    ) -> bool:
        """Bounded wait for the session, then adopt it. No trigger is sent here."""

        if self._inverter_forward_adapter() in (
            ADAPTER_INVERTER_AT_MIXED,
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
        ):
            if self.active_collector_at_transport is None:
                ok = await self._async_wait_for_at_connection(timeout=timeout)
                if not ok:
                    self._note_callback_failure()
                    return False

            await self._announcer.stop()
            self._note_callback_connected()
            # A freshly-connected session is positive live evidence: adopt its
            # trusted wire now so the confirmed binding survives the next gap.
            self._adopt_trusted_live_binding()
            return self.connected

        if not self.connected:
            ok = await self._async_wait_for_payload_connection(timeout=timeout)
            if not ok:
                self._note_callback_failure()
                return False

        # The callback session itself connected; heartbeat is a separate concern.
        self._note_callback_connected()
        # A freshly-connected session is positive live evidence: adopt its
        # trusted wire now so the confirmed binding survives the next gap.
        self._adopt_trusted_live_binding()

        if require_heartbeat:
            liveness_ok = await self._async_wait_for_payload_liveness(
                timeout=min(timeout, 1.5)
            )
            if not liveness_ok:
                return False

        await self._announcer.stop()
        return self.connected

    async def async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        """Ensure a live collector connection or raise a standard transport error."""

        ok = await self.async_try_connect(
            timeout=timeout,
            require_heartbeat=require_heartbeat,
        )
        if not ok:
            if require_heartbeat and self.connected:
                raise ConnectionError("collector_heartbeat_timeout")
            raise ConnectionError("collector_not_connected")

    async def async_reset_connection(self, *, reason: str = "") -> None:
        collector = self.collector_info
        logger.warning(
            "Resetting collector runtime connection after %s remote=%s configured_collector_ip=%s collector_pn=%s heartbeat_devcode=%s last_devcode=%s",
            reason or "runtime_error",
            collector.remote_ip or "unknown",
            self._collector_ip or "unknown",
            collector.collector_pn or "unknown",
            f"0x{collector.heartbeat_devcode:04X}" if collector.heartbeat_devcode is not None else "unknown",
            f"0x{collector.last_devcode:04X}" if collector.last_devcode is not None else "unknown",
        )
        await self._disconnect_all_transports()
