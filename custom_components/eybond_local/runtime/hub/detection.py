"""HubDetectionMixin ownership slice for the runtime hub."""

from __future__ import annotations

from .common import (
    DRIVER_DETECTION_FULL_SCAN,
    DRIVER_HINT_AUTO,
    DetectedDriverContext,
    DriverSweepNoMatch,
    RuntimeInverterCandidate,
    RuntimeLinkBaudChannel,
    _inverter_identities_conflict,
    _inverter_identity_is_present,
    _inverter_identity_signature,
    async_detect_inverter,
    async_detect_inverter_candidates,
    async_run_link_baud_sweep,
    asyncio,
    catalog_link_baud_hints,
    collector_capability_profile_from_runtime,
    default_runtime_driver_sweep_seconds,
    driver_keys_for_link_baud,
    logger,
    monotonic,
)


class HubDetectionMixin:
    """Methods owned by HubDetectionMixin."""

    async def _async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        """Ensure there is an active collector connection, retrying discovery if needed."""

        ok = await self._async_try_connect_for_session_lifecycle(
            timeout=timeout,
            require_heartbeat=require_heartbeat,
        )
        if ok:
            return
        if require_heartbeat and self._link_manager.connected:
            await self._async_recover_heartbeat_timeout(timeout=timeout)
            return
        raise ConnectionError("collector_not_connected")

    async def _async_recover_heartbeat_timeout(self, *, timeout: float) -> None:
        """Drop a stale connected socket and wait for a fresh heartbeat."""

        self._record_recovery_attempt(reason="collector_heartbeat_timeout")
        await self._link_manager.async_reset_connection(reason="collector_heartbeat_timeout")
        await self._link_manager.async_ensure_connected(
            timeout=timeout,
            require_heartbeat=True,
        )

    async def _async_attempt_runtime_link_baud_sweep(
        self,
        *,
        detection_generation: int,
    ):
        """Try alternate inverter UART speeds once on one owned ESP session.

        This is the runtime successor to the old onboarding UART sweep.
        It is deliberately unavailable to the default first-match strategy and
        is entered only after a full driver sweep proved total UART silence.
        """

        if self._driver_detection_strategy != DRIVER_DETECTION_FULL_SCAN:
            return None
        if detection_generation != self._owned_session_generation():
            return None
        if self._link_baud_sweep_generation == detection_generation:
            return None

        transport = self._link_manager.transport
        hardware = self._collector_metadata_service.merged_values().get(
            "collector_hardware_version"
        )
        if type(hardware) is not str or hardware != hardware.strip():
            return None
        if not collector_capability_profile_from_runtime(
            hardware_version=hardware
        ).uart_runtime_speed_change:
            self._link_baud_sweep_generation = detection_generation
            return None

        # The automatic scan and every user-facing collector mutation share ONE
        # per-entry authority.  Acquire before the first management read and hold
        # it through a possible cancellation-safe restore.  A busy operation is
        # skipped without consuming this session generation, so a later refresh
        # can retry after the active owner finishes.
        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY,
            OPERATION_RUNTIME_LINK_BAUD_SWEEP,
        )

        entry_id = getattr(self, "_collector_operation_entry_id", "")
        acquired = COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.acquire(
            entry_id,
            OPERATION_RUNTIME_LINK_BAUD_SWEEP,
        )
        if not acquired.acquired:
            return None
        token = acquired.token
        self._link_baud_sweep_generation = detection_generation

        try:
            adapter = self._collector_management_adapter(active_only=True)
            channel = RuntimeLinkBaudChannel(
                adapter,
                request_timeout=self._connection.request_timeout,
            )

            def same_session() -> bool:
                return self._owned_session_generation() == detection_generation

            async def read_baud() -> int | None:
                if not same_session():
                    return None
                return await channel.async_read_current_baud()

            async def set_baud(baud: int) -> bool:
                if not same_session():
                    return False
                return await channel.async_set_baud(baud)

            sweep_deadline = monotonic() + default_runtime_driver_sweep_seconds()

            def remaining_seconds() -> float:
                return max(0.0, sweep_deadline - monotonic())

            async def run_sweep(baud: int):
                if not same_session():
                    return None
                allowed = driver_keys_for_link_baud(baud)
                if not allowed:
                    return None
                try:
                    return await async_detect_inverter_candidates(
                        transport,
                        driver_hint=DRIVER_HINT_AUTO,
                        allowed_driver_keys=allowed,
                        remaining_seconds=remaining_seconds,
                    )
                except DriverSweepNoMatch:
                    return None

            outcome = await async_run_link_baud_sweep(
                candidate_bauds=catalog_link_baud_hints(),
                read_baud=read_baud,
                set_baud=set_baud,
                run_sweep=run_sweep,
                admit=lambda: same_session() and remaining_seconds() > 0,
            )
            if outcome.matched:
                logger.info(
                    "Runtime full scan matched inverter after UART speed change "
                    "original=%s matched=%s",
                    outcome.original_baud,
                    outcome.matched_baud,
                )
                return outcome.scan
            return None
        finally:
            if token is not None:
                COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.release(entry_id, token)

    async def _async_detect_driver(self) -> str:
        detection_generation = self._owned_session_generation()
        existing_driver_key = str(
            getattr(self._inverter, "driver_key", "") or ""
        ).strip()
        detection_hint = existing_driver_key or self._driver_hint
        if (
            detection_hint == DRIVER_HINT_AUTO
            and len(self._inverter_protocol_candidates) > 1
            and self._inverter_protocol_candidate_generation == detection_generation
        ):
            # One complete all-driver sweep already proved the ambiguity on this
            # exact owned session.  Do not repeat an expensive wire sweep every
            # poll while waiting for explicit user intent.
            return "inverter_protocol_ambiguous"

        self._record_inverter_detection_probe_log(
            (),
            budget_exhausted=False,
            generation=detection_generation,
        )
        detect_all_candidates = bool(
            detection_hint == DRIVER_HINT_AUTO
            and self._driver_detection_strategy == DRIVER_DETECTION_FULL_SCAN
        )
        detection_task = asyncio.create_task(
            (
                async_detect_inverter_candidates(
                    self._link_manager.transport,
                    driver_hint=DRIVER_HINT_AUTO,
                )
                if detect_all_candidates
                else async_detect_inverter(
                    self._link_manager.transport,
                    driver_hint=detection_hint,
                )
            ),
            name="eybond_inverter_detection",
        )
        wait_for_session_change = getattr(
            self._link_manager,
            "async_wait_for_owned_session_change",
            None,
        )
        session_change_task: asyncio.Task[None] | None = None
        if callable(wait_for_session_change):
            session_change_task = asyncio.create_task(
                wait_for_session_change(detection_generation),
                name="eybond_detection_session_guard",
            )
        try:
            if session_change_task is None:
                detection_result = await detection_task
            else:
                done, _pending = await asyncio.wait(
                    (detection_task, session_change_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if session_change_task in done and detection_task not in done:
                    detection_task.cancel()
                    try:
                        await detection_task
                    except asyncio.CancelledError:
                        pass
                    logger.info(
                        "Discarding inverter detection after owned collector session changed"
                    )
                    return "collector_session_changed"
                detection_result = await detection_task
        except DriverSweepNoMatch as exc:
            self._record_inverter_detection_probe_log(
                exc.probe_log,
                budget_exhausted=False,
                generation=detection_generation,
            )
            if not (detect_all_candidates and exc.silent):
                return str(exc)
            detection_result = await self._async_attempt_runtime_link_baud_sweep(
                detection_generation=detection_generation,
            )
            if detection_result is None:
                return str(exc)
        except RuntimeError as exc:
            return str(exc)
        finally:
            if session_change_task is not None:
                session_change_task.cancel()
                try:
                    await session_change_task
                except asyncio.CancelledError:
                    pass

        if detect_all_candidates:
            self._record_inverter_detection_probe_log(
                detection_result.probe_log,
                budget_exhausted=detection_result.budget_exhausted,
                generation=detection_generation,
            )
            contexts = tuple(detection_result.candidates)
        else:
            context = detection_result
            self._record_inverter_detection_probe_log(
                context.inverter.details.get("probe_log", ()),
                budget_exhausted=False,
                generation=detection_generation,
            )

        if self._owned_session_generation() != detection_generation:
            return "collector_session_changed"

        if detect_all_candidates:
            if len(contexts) > 1:
                self._inverter_protocol_candidates = tuple(
                    RuntimeInverterCandidate(
                        driver_key=context.match.driver_key,
                        protocol_family=context.match.protocol_family,
                        model_name=context.match.model_name,
                        serial_number=context.match.serial_number,
                    )
                    for context in contexts
                )
                self._inverter_protocol_candidate_generation = detection_generation
                logger.warning(
                    "Inverter answered on multiple protocols: %s; waiting for user selection",
                    ", ".join(
                        candidate.driver_key
                        for candidate in self._inverter_protocol_candidates
                    ),
                )
                return "inverter_protocol_ambiguous"
            if not contexts:
                return "no_supported_driver_matched"
            context: DetectedDriverContext = contexts[0]

        self._inverter_protocol_candidates = ()
        self._inverter_protocol_candidate_generation = -1

        # Identity-conflict guard: a durable/provisional binding is sticky. When
        # live detection reports a DIFFERENT full identity, report the conflict
        # and keep the durable identity -- never silently swap it. This runs on
        # the deferred provisional refresh and on any post-reset re-detection.
        previous = self._inverter
        if (
            _inverter_identity_is_present(previous)
            and _inverter_identity_is_present(context.inverter)
            and _inverter_identities_conflict(previous, context.inverter)
        ):
            self._inverter_identity_conflict = (
                f"{_inverter_identity_signature(previous)}"
                f" != {_inverter_identity_signature(context.inverter)}"
            )
            # Terminal for the provisional refresh: stop retrying and keep durable.
            self._inverter_binding_needs_live_detection_refresh = False
            self._inverter_binding_refresh_attempts = 0
            logger.warning(
                "Runtime inverter identity conflict: durable=%s live=%s; keeping durable identity",
                _inverter_identity_signature(previous),
                _inverter_identity_signature(context.inverter),
            )
            return "inverter_identity_conflict"

        self._inverter_identity_conflict = ""
        # The raw driver log may contain exception text.  Its sanitized copy is
        # already stored above; do not retain the raw form in the bound model or
        # support-bundle inverter payload.
        context.inverter.details.pop("probe_log", None)
        self._driver = context.driver
        self._inverter = context.inverter
        self._accept_inverter_binding_identity()
        self._inverter_binding_needs_live_detection_refresh = False
        self._inverter_binding_refresh_attempts = 0
        # The overlay merge is applied in _build_snapshot (every refresh, once the
        # collector identity is populated), not here -- at detection the collector is
        # not yet identified, so the device-scope match would fail and never retry.
        self._reset_runtime_read_state()
        self._write_blockers.clear()
        logger.info(
            "Detected inverter driver=%s protocol=%s serial=%s confidence=%s",
            context.inverter.driver_key,
            context.inverter.protocol_family,
            context.inverter.serial_number,
            context.match.confidence,
        )
        observer = self._inverter_detection_observer
        if observer is not None:
            try:
                observer(context.driver, context.inverter)
            except Exception:
                logger.debug("Runtime inverter detection observer failed", exc_info=True)
        return ""
