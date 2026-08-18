"""Single registry of connection branches for onboarding and runtime selection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable, cast

from .confirmed_session_protocol import ConfirmedSessionProtocolEvidence
from .models import ConnectionSpec, ConnectionType, EybondConnectionSpec
from .ui import (
    ConnectionDisplayMetadata,
    ConnectionFormLayout,
    EYBOND_CONNECTION_DISPLAY_METADATA,
    EYBOND_CONNECTION_FORM_LAYOUT,
    build_eybond_auto_values,
    build_eybond_manual_base_values,
    build_eybond_runtime_option_values,
)
from ..const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONF_DISCOVERY_INTERVAL,
    CONF_DISCOVERY_TARGET,
    CONF_HEARTBEAT_INTERVAL,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONNECTION_TYPE_EYBOND,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
)
from ..collector.transport_profile import (
    apply_observed_collector_session_protocol,
    resolve_collector_transport_profile_from_entry_context,
)


def _optional_int(value: object) -> int:
    """Return one optional integer field, treating blanks as zero."""

    if value in (None, ""):
        return 0
    return int(value)


@dataclass(frozen=True, slots=True)
class ConnectionBranch:
    """Metadata and constructors for one connection-type branch."""

    connection_type: ConnectionType
    spec_type: type[ConnectionSpec]
    form_layout: ConnectionFormLayout
    display: ConnectionDisplayMetadata
    build_connection_spec: Callable[
        [Mapping[str, object], Mapping[str, object]],
        ConnectionSpec,
    ]
    build_auto_values: Callable[..., dict[str, Any]]
    build_manual_base_values: Callable[..., dict[str, Any]]
    build_runtime_option_values: Callable[..., dict[str, Any]]


def _build_eybond_connection_spec(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> EybondConnectionSpec:
    confirmed_evidence = ConfirmedSessionProtocolEvidence.from_entry(
        data,
        options,
        entry_pn=str(data.get(CONF_COLLECTOR_PN, "") or ""),
    )
    transport_profile = resolve_collector_transport_profile_from_entry_context(
        data,
        options,
    )
    if confirmed_evidence is not None:
        # The validated, PN-bound live observation chooses the wire. Cloud
        # metadata may only refine the dialect of that already-confirmed wire.
        transport_profile = apply_observed_collector_session_protocol(
            transport_profile,
            confirmed_evidence.protocol,
        )
    return EybondConnectionSpec(
        server_ip=str(options.get(CONF_SERVER_IP, data.get(CONF_SERVER_IP, ""))),
        advertised_server_ip=str(
            options.get(
                CONF_ADVERTISED_SERVER_IP,
                data.get(CONF_ADVERTISED_SERVER_IP, ""),
            )
            or ""
        ),
        tcp_port=int(options.get(CONF_TCP_PORT, data.get(CONF_TCP_PORT, DEFAULT_TCP_PORT))),
        advertised_tcp_port=_optional_int(
            options.get(
                CONF_ADVERTISED_TCP_PORT,
                data.get(CONF_ADVERTISED_TCP_PORT, 0),
            )
        ),
        udp_port=int(options.get(CONF_UDP_PORT, data.get(CONF_UDP_PORT, DEFAULT_UDP_PORT))),
        collector_ip=str(options.get(CONF_COLLECTOR_IP, data.get(CONF_COLLECTOR_IP, DEFAULT_COLLECTOR_IP))),
        collector_pn=str(data.get(CONF_COLLECTOR_PN, "") or ""),
        collector_cloud_family=transport_profile.cloud_family,
        collector_configured_session_protocol=transport_profile.session_protocol,
        collector_identity_strategy=transport_profile.identity_strategy,
        collector_raw_passthrough_bootstrap=transport_profile.raw_passthrough_bootstrap,
        collector_raw_passthrough_frame_format=transport_profile.raw_passthrough_frame_format,
        collector_raw_passthrough_min_interval_ms=(
            transport_profile.raw_passthrough_min_interval_ms
        ),
        confirmed_session_protocol_evidence=confirmed_evidence,
        discovery_target=str(
            options.get(
                CONF_DISCOVERY_TARGET,
                data.get(CONF_DISCOVERY_TARGET, DEFAULT_DISCOVERY_TARGET),
            )
        ),
        discovery_interval=int(
            options.get(
                CONF_DISCOVERY_INTERVAL,
                data.get(CONF_DISCOVERY_INTERVAL, DEFAULT_DISCOVERY_INTERVAL),
            )
        ),
        heartbeat_interval=int(
            options.get(
                CONF_HEARTBEAT_INTERVAL,
                data.get(CONF_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL),
            )
        ),
        request_timeout=DEFAULT_REQUEST_TIMEOUT,
    )


_CONNECTION_BRANCHES: dict[str, ConnectionBranch] = {
    CONNECTION_TYPE_EYBOND: ConnectionBranch(
        connection_type=CONNECTION_TYPE_EYBOND,
        spec_type=EybondConnectionSpec,
        form_layout=EYBOND_CONNECTION_FORM_LAYOUT,
        display=EYBOND_CONNECTION_DISPLAY_METADATA,
        build_connection_spec=_build_eybond_connection_spec,
        build_auto_values=build_eybond_auto_values,
        build_manual_base_values=build_eybond_manual_base_values,
        build_runtime_option_values=build_eybond_runtime_option_values,
    ),
}


def supported_connection_types() -> tuple[ConnectionType, ...]:
    """Return supported connection types in stable registration order."""

    return tuple(cast(ConnectionType, connection_type) for connection_type in _CONNECTION_BRANCHES)


def get_connection_branch(connection_type: str) -> ConnectionBranch:
    """Return the registered branch metadata for one connection type."""

    branch = _CONNECTION_BRANCHES.get(connection_type)
    if branch is None:
        raise ValueError(f"unsupported_connection_type:{connection_type}")
    return branch


def get_connection_branch_for_spec(connection: ConnectionSpec) -> ConnectionBranch:
    """Return the branch metadata matching one typed connection spec."""

    branch = get_connection_branch(connection.type)
    if not isinstance(connection, branch.spec_type):
        raise ValueError(
            f"connection_spec_branch_mismatch:{branch.connection_type}:{type(connection).__name__}"
        )
    return branch
