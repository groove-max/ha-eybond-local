"""Connection-type models for future multi-link onboarding/runtime support."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from ..const import (
    CONF_CONNECTION_TYPE,
    CONNECTION_TYPE_EYBOND,
)


ConnectionType = Literal["eybond"]


@dataclass(frozen=True, slots=True)
class ConnectionSpec:
    """Base connection metadata shared by all future link types."""

    type: ConnectionType


@dataclass(frozen=True, slots=True)
class EybondConnectionSpec(ConnectionSpec):
    """Physical/discovery settings for one EyeBond collector-based link."""

    server_ip: str
    advertised_server_ip: str
    tcp_port: int
    advertised_tcp_port: int
    udp_port: int
    collector_ip: str
    discovery_target: str
    discovery_interval: int
    heartbeat_interval: int
    request_timeout: float

    def __init__(
        self,
        *,
        server_ip: str,
        advertised_server_ip: str = "",
        tcp_port: int,
        advertised_tcp_port: int = 0,
        udp_port: int,
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_interval: int,
        heartbeat_interval: int,
        request_timeout: float,
    ) -> None:
        object.__setattr__(self, "type", "eybond")
        object.__setattr__(self, "server_ip", server_ip)
        object.__setattr__(self, "advertised_server_ip", advertised_server_ip)
        object.__setattr__(self, "tcp_port", int(tcp_port))
        object.__setattr__(self, "advertised_tcp_port", int(advertised_tcp_port or 0))
        object.__setattr__(self, "udp_port", int(udp_port))
        object.__setattr__(self, "collector_ip", collector_ip)
        object.__setattr__(self, "discovery_target", discovery_target)
        object.__setattr__(self, "discovery_interval", int(discovery_interval))
        object.__setattr__(self, "heartbeat_interval", int(heartbeat_interval))
        object.__setattr__(self, "request_timeout", float(request_timeout))

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the endpoint IP that will be advertised to the collector."""

        return self.advertised_server_ip or self.server_ip

    @property
    def effective_advertised_tcp_port(self) -> int:
        """Return the endpoint TCP port that will be advertised to the collector."""

        return self.advertised_tcp_port or self.tcp_port


def resolve_connection_type(data: Mapping[str, object]) -> ConnectionType:
    """Return the effective connection type for stored config-entry data."""

    connection_type = str(data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND) or CONNECTION_TYPE_EYBOND)
    from .branch_registry import get_connection_branch

    branch = get_connection_branch(connection_type)
    return branch.connection_type


def build_connection_spec(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> ConnectionSpec:
    """Build one typed connection spec from config-entry data and options."""

    connection_type = resolve_connection_type(data)
    from .branch_registry import get_connection_branch

    branch = get_connection_branch(connection_type)
    return branch.build_connection_spec(data, options)


def build_connection_spec_from_values(
    connection_type: str,
    values: Mapping[str, object],
) -> ConnectionSpec:
    """Build one typed connection spec from branch-local values alone."""

    raw_data = dict(values)
    raw_data[CONF_CONNECTION_TYPE] = connection_type or CONNECTION_TYPE_EYBOND
    return build_connection_spec(raw_data, {})
