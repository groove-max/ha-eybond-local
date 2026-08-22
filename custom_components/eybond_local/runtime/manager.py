"""Generic runtime-manager contract for connection-specific runtime branches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ..models import RuntimeSnapshot
from ..support.shadow_learning import ShadowWriteObservation


@dataclass(frozen=True, slots=True)
class RuntimeInverterCandidate:
    """One inverter protocol observed on the entry-owned collector session.

    This is a read model, not persisted identity and not a recovery proof.  It
    exists so post-entry UX can present an ambiguity without exposing driver
    objects or transport handles outside the runtime boundary.
    """

    driver_key: str
    protocol_family: str
    model_name: str
    serial_number: str

    def __post_init__(self) -> None:
        for name in ("driver_key", "protocol_family", "model_name", "serial_number"):
            value = getattr(self, name)
            if type(value) is not str:
                raise TypeError(f"{name}_must_be_string")
            if value != value.strip():
                raise ValueError(f"{name}_must_be_normalized")
        if not self.driver_key:
            raise ValueError("driver_key_required")


class RuntimeManager(Protocol):
    """Runtime orchestration contract shared by all future connection branches."""

    @property
    def effective_server_ip(self) -> str:
        ...

    @property
    def effective_advertised_server_ip(self) -> str:
        ...

    @property
    def listener_bind_host(self) -> str:
        """The ACTUAL local TCP bind host of the shared callback listener.

        Distinct from ``effective_server_ip`` / the UDP trigger bind: a repair's
        shared TCP listener must be borrowed on exactly this host to refcount-
        share the runtime's own listener.
        """
        ...

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        ...

    @property
    def inverter_protocol_candidates(self) -> tuple[RuntimeInverterCandidate, ...]:
        """Return all protocols proven on the current owned session."""
        ...

    def set_callback_ownership(self, registry: object, entry_id: str) -> None:
        """Bind the domain session registry and durable entry identity."""
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

    async def async_ensure_collector_management_session(
        self,
        *,
        timeout: float,
    ) -> bool:
        """Ensure one live collector session through the configured runtime path."""
        ...

    async def async_activate_claimed_session(
        self,
        *,
        expected_session_id: str,
        timeout: float,
    ) -> bool:
        """Activate one exact registry-claimed callback session without UDP."""
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
        timeout: float = 5.0,
        require_heartbeat: bool = True,
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
        expected_session_protocol: str = "",
        proxy_wire_mode: str = "transparent",
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
        expected_session_protocol: str = "",
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

    def shadow_learning_write_observations(
        self,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return the exact observations captured by the active learning route."""
        ...

    def shadow_learning_observation_cursor(self) -> int:
        """Return the current tail cursor of the active learning route."""
        ...

    def shadow_learning_observations_since(
        self,
        cursor: int,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return observations captured at or after an exact cursor."""
        ...

    async def async_wait_for_shadow_learning_observations_since(
        self,
        cursor: int,
        *,
        timeout_seconds: float,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Wait a bounded interval for observations at or after a cursor."""
        ...

    def shadow_learning_read_map_snapshot(self) -> dict[str, Any]:
        """Return a detached snapshot of reads observed by the learning route."""
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

    async def async_get_collector_server_endpoint_state(
        self,
        *,
        timeout: float = 5.0,
        require_heartbeat: bool = True,
    ) -> dict[str, object]:
        ...

    async def async_query_collector_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        ...

    async def async_set_collector_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        ...

    async def async_set_collector_uart_baudrate(self, baudrate: str) -> str:
        ...

    def collector_management_capabilities(self) -> Any:
        ...

    def collector_management_diagnostics(self) -> dict[str, object]:
        ...

    def collector_metadata_diagnostics(self) -> dict[str, object]:
        ...

    async def async_capture_support_evidence(self) -> dict[str, object]:
        ...
