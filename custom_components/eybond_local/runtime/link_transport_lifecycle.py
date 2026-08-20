"""LinkTransportLifecycleMixin ownership slice for the runtime link."""

from __future__ import annotations

from .link_common import (
    CollectorListenerBindError,
    DEFAULT_REQUEST_TIMEOUT,
    DiscoveryAnnouncer,
    SharedCollectorAtTransport,
    SharedEybondTransport,
    _DEFAULT_LISTENER_BIND_HOST,
    logger,
    resolve_server_ip,
)


class LinkTransportLifecycleMixin:
    """Methods owned by LinkTransportLifecycleMixin."""

    async def _start_all_transports(self) -> None:
        for transport in self._payload_transports():
            await transport.start()
        for transport in self._at_transports():
            await transport.start()

    async def _stop_all_transports(self) -> None:
        for transport in reversed(self._at_transports()):
            await transport.stop()
        for transport in reversed(self._payload_transports()):
            await transport.stop()

    async def _disconnect_all_transports(self) -> None:
        for transport in reversed(self._at_transports()):
            await transport.disconnect()
        for transport in reversed(self._payload_transports()):
            await transport.disconnect()

    def _rebuild_link(self, server_ip: str) -> None:
        """Create the transport/discovery pair for one collector-facing IP."""

        effective_target = self._collector_ip or self._discovery_target
        effective_advertised_server_ip = self._configured_advertised_server_ip or server_ip
        effective_advertised_tcp_port = self._configured_advertised_tcp_port or self._tcp_port
        self._effective_server_ip = server_ip
        self._listener_bind_host = _DEFAULT_LISTENER_BIND_HOST
        self._transport, self._at_transport = self._build_transport_pair(
            self._listener_bind_host,
            self._tcp_port,
        )
        self._auxiliary_transports = {}
        self._auxiliary_at_transports = {}
        for port in sorted(self._auxiliary_listener_ports):
            payload_transport, at_transport = self._build_transport_pair(
                self._listener_bind_host,
                port,
            )
            self._auxiliary_transports[port] = payload_transport
            self._auxiliary_at_transports[port] = at_transport
        self._announcer = DiscoveryAnnouncer(
            bind_ip=server_ip,
            advertised_server_ip=effective_advertised_server_ip,
            advertised_server_port=effective_advertised_tcp_port,
            target_ip=effective_target,
            udp_port=self._udp_port,
            interval=float(self._discovery_interval),
        )
        self._apply_collector_connection_watcher()

    def _build_transport_pair(
        self,
        bind_host: str,
        port: int,
    ) -> tuple[SharedEybondTransport, SharedCollectorAtTransport]:
        payload_transport = SharedEybondTransport(
            host=bind_host,
            port=port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            heartbeat_interval=float(self._heartbeat_interval),
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            # Only a CONFIRMED protocol is handed to the shared listener.
            # "" means passive observation only.
            collector_session_protocol=self._confirmed_session_protocol(),
            collector_identity_strategy=self._collector_identity_strategy,
            collector_raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
            collector_raw_passthrough_min_interval_ms=(
                self._collector_raw_passthrough_min_interval_ms
            ),
        )
        at_transport = SharedCollectorAtTransport(
            host=bind_host,
            port=port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            # Only a CONFIRMED protocol is handed to the shared listener.
            # "" means passive observation only.
            collector_session_protocol=self._confirmed_session_protocol(),
            collector_identity_strategy=self._collector_identity_strategy,
            collector_raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
            collector_raw_passthrough_min_interval_ms=(
                self._collector_raw_passthrough_min_interval_ms
            ),
        )
        return payload_transport, at_transport

    async def _rebuild_if_server_ip_changed(self, *, reason: str) -> bool:
        resolved_server_ip = resolve_server_ip(
            self._configured_server_ip,
            collector_ip=self._collector_ip,
        )
        if resolved_server_ip == self._effective_server_ip:
            return False

        logger.warning(
            "EyeBond advertised listener IP changed from %s to %s after %s; rebuilding transport",
            self._effective_server_ip or "unknown",
            resolved_server_ip or "unknown",
            reason or "network_change",
        )
        await self._announcer.stop()
        await self._stop_all_transports()
        self._rebuild_link(resolved_server_ip)
        self._listener_rebind_count += 1
        return True

    def _record_listener_error(self, exc: Exception) -> None:
        self._listener_status = "error"
        if isinstance(exc, CollectorListenerBindError):
            self._listener_last_error = str(exc.error)
            return
        self._listener_last_error = str(exc)
