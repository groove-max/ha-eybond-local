"""LinkCloudRoutesMixin ownership slice for the runtime link."""

from __future__ import annotations

from .common import (
    InProcessFailClosedShadowProxyHandler,
    InProcessProxyCaptureHandler,
    RouteLease,
    ShadowLearningSeed,
    ShadowWriteObservation,
    SharedProxyCaptureRoute,
    replace,
)


class LinkCloudRoutesMixin:
    """Methods owned by LinkCloudRoutesMixin."""

    async def async_start_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        proxy_wire_mode: str = "transparent",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        masked_endpoint: str = "",
        restore_trigger_path=None,
        async_open_output=None,
        async_close_output=None,
    ) -> None:
        """Route one collector's callback connection through the in-process proxy."""

        if proxy_wire_mode != "transparent":
            raise ValueError("proxy_wire_mode_unsupported")
        if (
            type(expected_session_protocol) is not str
            or expected_session_protocol != expected_session_protocol.strip()
            or expected_session_protocol.lower() not in {"at_text", "eybond_framed"}
        ):
            raise ValueError("proxy_expected_session_protocol_invalid")

        normalized_owner_id = self._normalize_route_owner_id(
            mode="proxy_capture",
            owner_id=owner_id,
            entry_id=entry_id,
            output_path=output_path,
        )
        await self._acquire_route_lease(
            mode="proxy_capture",
            owner_id=normalized_owner_id,
            entry_id=entry_id,
            collector_ip=collector_ip,
            listen_port=listen_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
        )
        handler: InProcessProxyCaptureHandler | None = None
        route: SharedProxyCaptureRoute | None = None
        try:
            handler = InProcessProxyCaptureHandler(
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                output_path=output_path,
                expected_collector_pn=collector_pn,
                masked_endpoint=masked_endpoint,
                restore_trigger_path=restore_trigger_path,
                async_open_output=async_open_output,
                async_close_output=async_close_output,
            )
            await handler.start()
            route = SharedProxyCaptureRoute(
                host=self._listener_bind_host,
                port=int(listen_port),
                collector_ip=collector_ip,
                collector_pn=collector_pn,
                expected_session_protocol=expected_session_protocol,
                handler=handler.handle_client,
            )
            await route.start()
            self._proxy_capture_handler = handler
            self._proxy_capture_route = route
            await self._set_route_lease_state(normalized_owner_id, "running")
        except BaseException as exc:
            self._record_listener_error(exc)
            try:
                if route is not None:
                    await route.stop()
            finally:
                try:
                    if handler is not None:
                        await handler.stop()
                finally:
                    await self._release_route_lease(
                        mode="proxy_capture",
                        owner_id=normalized_owner_id,
                    )
            raise

    async def async_start_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        seed: ShadowLearningSeed,
    ) -> None:
        """Route one collector callback connection through the fail-closed shadow proxy."""

        if (
            type(expected_session_protocol) is not str
            or expected_session_protocol != expected_session_protocol.strip()
            or expected_session_protocol.lower() not in {"at_text", "eybond_framed"}
        ):
            raise ValueError("shadow_expected_session_protocol_invalid")

        normalized_owner_id = self._normalize_route_owner_id(
            mode="shadow_learning",
            owner_id=owner_id,
            entry_id=entry_id,
            output_path=output_path,
        )
        await self._acquire_route_lease(
            mode="shadow_learning",
            owner_id=normalized_owner_id,
            entry_id=entry_id,
            collector_ip=collector_ip,
            listen_port=listen_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
        )
        handler: InProcessFailClosedShadowProxyHandler | None = None
        route: SharedProxyCaptureRoute | None = None
        try:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                seed=seed,
                output_path=output_path,
            )
            await handler.start()
            route = SharedProxyCaptureRoute(
                host=self._listener_bind_host,
                port=int(listen_port),
                collector_ip=collector_ip,
                collector_pn=collector_pn,
                expected_session_protocol=expected_session_protocol,
                handler=handler.handle_client,
            )
            await route.start()
        except Exception as exc:
            self._record_listener_error(exc)
            try:
                if route is not None:
                    await route.stop()
            finally:
                try:
                    if handler is not None:
                        await handler.stop()
                finally:
                    await self._release_route_lease(
                        mode="shadow_learning",
                        owner_id=normalized_owner_id,
                    )
            raise
        self._shadow_learning_handler = handler
        self._shadow_learning_route = route
        await self._set_route_lease_state(normalized_owner_id, "running")

    async def async_stop_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process proxy route, if any."""

        await self._begin_route_stop(
            mode="proxy_capture",
            owner_id=owner_id,
            force=force,
        )
        route = self._proxy_capture_route
        handler = self._proxy_capture_handler
        self._proxy_capture_route = None
        self._proxy_capture_handler = None
        try:
            if route is not None:
                await route.stop()
            if handler is not None:
                await handler.stop()
        finally:
            await self._release_route_lease(
                mode="proxy_capture",
                owner_id=owner_id,
                force=force,
            )

    async def async_stop_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process shadow-learning route, if any."""

        await self._begin_route_stop(
            mode="shadow_learning",
            owner_id=owner_id,
            force=force,
        )
        route = self._shadow_learning_route
        handler = self._shadow_learning_handler
        self._shadow_learning_route = None
        self._shadow_learning_handler = None
        try:
            if route is not None:
                await route.stop()
            if handler is not None:
                await handler.stop()
        finally:
            await self._release_route_lease(
                mode="shadow_learning",
                owner_id=owner_id,
                force=force,
            )

    @property
    def route_lease(self) -> RouteLease | None:
        """Return the current exclusive callback-route lease."""

        return self._route_lease

    @staticmethod
    def _normalize_route_owner_id(
        *,
        mode: str,
        owner_id: str,
        entry_id: str,
        output_path: object,
    ) -> str:
        normalized = str(owner_id or "").strip()
        if normalized:
            return normalized
        return f"{mode}:{str(entry_id or '').strip()}:{str(output_path)}"

    async def _acquire_route_lease(
        self,
        *,
        mode: str,
        owner_id: str,
        entry_id: str,
        collector_ip: str,
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
    ) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is not None:
                raise RuntimeError(f"{current.mode}_route_running")
            if mode != "proxy_capture" and self.proxy_capture_route_running():
                raise RuntimeError("proxy_capture_route_running")
            if mode != "shadow_learning" and self.shadow_learning_route_running():
                raise RuntimeError("shadow_learning_route_running")
            self._route_lease = RouteLease(
                mode=mode,
                owner_id=owner_id,
                entry_id=str(entry_id or "").strip(),
                collector_ip=str(collector_ip or "").strip(),
                listen_port=int(listen_port),
                upstream_host=str(upstream_host or "").strip(),
                upstream_port=int(upstream_port),
                state="starting",
            )

    async def _set_route_lease_state(self, owner_id: str, state: str) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is None or current.owner_id != owner_id:
                raise RuntimeError("route_lease_owner_mismatch")
            self._route_lease = replace(current, state=str(state or "").strip())

    async def _begin_route_stop(
        self,
        *,
        mode: str,
        owner_id: str,
        force: bool,
    ) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is None:
                return
            if current.mode != mode:
                if force:
                    return
                raise RuntimeError(f"{current.mode}_route_running")
            normalized_owner_id = str(owner_id or "").strip()
            if normalized_owner_id and normalized_owner_id != current.owner_id and not force:
                raise RuntimeError("route_lease_owner_mismatch")
            self._route_lease = replace(current, state="stopping")

    async def _release_route_lease(
        self,
        *,
        mode: str,
        owner_id: str,
        force: bool = False,
    ) -> None:
        async with self._route_lease_lock:
            current = self._route_lease
            if current is None or current.mode != mode:
                return
            normalized_owner_id = str(owner_id or "").strip()
            if normalized_owner_id and normalized_owner_id != current.owner_id and not force:
                raise RuntimeError("route_lease_owner_mismatch")
            self._route_lease = None

    def proxy_capture_route_running(self) -> bool:
        """Return whether an in-process proxy route is currently active."""

        handler = self._proxy_capture_handler
        return bool(handler is not None and handler.running)

    def shadow_learning_route_running(self) -> bool:
        """Return whether an in-process shadow-learning route is currently active."""

        handler = self._shadow_learning_handler
        return bool(handler is not None and handler.running)

    def shadow_learning_route_ready(self) -> bool:
        """Return whether the active shadow route has collector and upstream connectivity."""

        handler = self._shadow_learning_handler
        return bool(handler is not None and handler.ready)

    def shadow_learning_route_status(self) -> dict[str, object]:
        """Return status details for the active shadow route."""

        handler = self._shadow_learning_handler
        if handler is None:
            return {
                "running": False,
                "collector_connected": False,
                "collector_protocol_ingress": False,
                "route_protocol_activity": False,
                "upstream_connected": False,
                "ready": False,
                "upstream_error": "",
            }
        return dict(handler.status())

    def shadow_learning_write_observations(
        self,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return observations from the active route without exposing its handler."""

        handler = self._shadow_learning_handler
        if handler is None:
            return ()
        return tuple(handler.write_observations)

    def shadow_learning_observation_cursor(self) -> int:
        """Return the active route's observation tail without exposing its handler."""

        handler = self._shadow_learning_handler
        if handler is None:
            return 0
        return handler.observation_cursor()

    def shadow_learning_observations_since(
        self,
        cursor: int,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return observations from one validated active-route cursor."""

        if type(cursor) is not int or cursor < 0:
            raise ValueError("shadow_learning_observation_cursor_invalid")
        handler = self._shadow_learning_handler
        if handler is None:
            return ()
        return tuple(handler.observations_since(cursor))

    async def async_wait_for_shadow_learning_observations_since(
        self,
        cursor: int,
        *,
        timeout_seconds: float,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Wait for active-route observations without exposing its handler."""

        if type(cursor) is not int or cursor < 0:
            raise ValueError("shadow_learning_observation_cursor_invalid")
        if (
            type(timeout_seconds) not in (int, float)
            or type(timeout_seconds) is bool
            or timeout_seconds < 0
        ):
            raise ValueError("shadow_learning_observation_timeout_invalid")
        handler = self._shadow_learning_handler
        if handler is None:
            return ()
        return tuple(
            await handler.wait_for_observations_since(
                cursor,
                timeout_seconds=float(timeout_seconds),
            )
        )

    def shadow_learning_read_map_snapshot(self) -> dict[str, object]:
        """Return a detached read-map snapshot from the active route."""

        handler = self._shadow_learning_handler
        if handler is None:
            return {}
        read_map = handler.read_map
        return dict(read_map) if isinstance(read_map, dict) else {}
