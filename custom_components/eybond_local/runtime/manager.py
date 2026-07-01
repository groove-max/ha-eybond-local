"""Generic runtime-manager contract for connection-specific runtime branches."""

from __future__ import annotations

from typing import Any, Protocol

from ..models import RuntimeSnapshot


class RuntimeManager(Protocol):
    """Runtime orchestration contract shared by all future connection branches."""

    @property
    def effective_server_ip(self) -> str:
        ...

    @property
    def effective_advertised_server_ip(self) -> str:
        ...

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        ...

    async def async_start(self) -> None:
        ...

    async def async_stop(self) -> None:
        ...

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        ...

    def listener_diagnostics(self) -> dict[str, object]:
        ...

    async def async_refresh(self, *, poll_interval: float | None = None) -> RuntimeSnapshot:
        ...

    def invalidate_collector_runtime_values(self) -> None:
        ...

    async def async_write_capability(self, capability_key: str, value: Any) -> Any:
        ...

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        ...

    async def async_set_collector_server_endpoint(
        self,
        endpoint: str,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        ...

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        ...

    async def async_ensure_callback_listener(self, port: int) -> None:
        ...

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        ...

    async def async_start_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path: Any,
        masked_endpoint: str = "",
        restore_trigger_path: Any = None,
        async_open_output: Any = None,
        async_close_output: Any = None,
    ) -> None:
        ...

    async def async_stop_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        ...

    def proxy_capture_route_running(self) -> bool:
        ...

    async def async_start_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path: Any,
        seed: Any,
    ) -> None:
        ...

    async def async_stop_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        ...

    def shadow_learning_route_running(self) -> bool:
        ...

    def shadow_learning_route_ready(self) -> bool:
        ...

    def shadow_learning_route_status(self) -> dict[str, object]:
        ...

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        ...

    async def async_apply_collector_changes(self) -> dict[str, object]:
        ...

    async def async_reboot_collector(self) -> dict[str, object]:
        ...

    async def async_rollback_collector_server_endpoint(
        self,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        ...

    async def async_get_collector_server_endpoint_state(self) -> dict[str, object]:
        ...

    async def async_capture_support_evidence(self) -> dict[str, object]:
        ...
