"""Validation helpers for collector callback endpoint strings."""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import re

from .metadata.collector_cloud_profile_catalog_loader import (
    load_collector_cloud_profile_catalog,
)

_HOSTNAME_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9])?$")

DEFAULT_COLLECTOR_SERVER_PORT = 18899
LEGACY_BINARY_COLLECTOR_SERVER_PORT = 502
DEFAULT_COLLECTOR_SERVER_PROTOCOL = "TCP"


@dataclass(frozen=True, slots=True)
class CollectorServerEndpointParts:
    """Structured collector callback endpoint with raw-shape metadata."""

    host: str
    port: int
    protocol: str
    has_explicit_port: bool
    has_explicit_protocol: bool

    def render(self, *, preserve_shape: bool = False) -> str:
        """Render the endpoint either canonically or in its original compact shape."""

        if preserve_shape:
            if not self.has_explicit_port:
                return self.host
            if not self.has_explicit_protocol:
                return f"{self.host},{self.port}"
        return f"{self.host},{self.port},{self.protocol}"


def validate_collector_server_host(server_host: str) -> str:
    """Validate one collector callback host as IPv4 or DNS hostname."""

    host = str(server_host or "").strip()
    if not host or not host.isascii() or "," in host:
        raise ValueError("collector_server_host_invalid")

    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None

    if address is not None:
        if address.version != 4:
            raise ValueError("collector_server_host_invalid")
        return host

    if len(host) > 253 or host.startswith(".") or host.endswith("."):
        raise ValueError("collector_server_host_invalid")

    labels = host.split(".")
    if any(not label or _HOSTNAME_LABEL_RE.fullmatch(label) is None for label in labels):
        raise ValueError("collector_server_host_invalid")

    return host


def validate_collector_server_port(server_port: int | str) -> int:
    """Validate one collector callback TCP port."""

    try:
        port = int(server_port)
    except (TypeError, ValueError) as exc:
        raise ValueError("collector_server_port_invalid") from exc

    if not 1 <= port <= 65535:
        raise ValueError("collector_server_port_invalid")
    return port


def validate_collector_server_protocol(
    server_protocol: str,
    *,
    require_tcp: bool = False,
) -> str:
    """Validate one collector callback protocol token."""

    protocol = str(server_protocol or "").strip().upper()
    if not protocol or not protocol.isascii() or "," in protocol:
        raise ValueError("collector_server_protocol_invalid")
    if require_tcp and protocol != "TCP":
        raise ValueError("collector_server_protocol_tcp_required")
    return protocol


def default_collector_server_port(*, cloud_family: str = "") -> int:
    """Return the semantic default callback port for one collector cloud family."""

    normalized_family = str(cloud_family or "").strip().lower()
    if normalized_family:
        catalog = load_collector_cloud_profile_catalog()
        profile = catalog.profiles.get(normalized_family)
        if profile is not None and profile.known_ports:
            return int(profile.known_ports[0])

    if normalized_family == "legacy_binary":
        return LEGACY_BINARY_COLLECTOR_SERVER_PORT
    return DEFAULT_COLLECTOR_SERVER_PORT


def format_collector_server_endpoint(
    *,
    server_host: str,
    server_port: int = DEFAULT_COLLECTOR_SERVER_PORT,
    server_protocol: str = DEFAULT_COLLECTOR_SERVER_PROTOCOL,
    include_port: bool = True,
    include_protocol: bool = True,
    require_tcp: bool = False,
) -> str:
    """Return one normalized collector callback endpoint string."""

    host = validate_collector_server_host(server_host)
    port = validate_collector_server_port(server_port)
    protocol = validate_collector_server_protocol(server_protocol, require_tcp=require_tcp)
    if not include_port:
        return host
    if not include_protocol:
        return f"{host},{port}"
    return f"{host},{port},{protocol}"


def inspect_collector_server_endpoint(
    endpoint: str,
    *,
    require_explicit_port: bool = True,
    require_explicit_protocol: bool = True,
    require_tcp: bool = False,
) -> CollectorServerEndpointParts:
    """Parse one callback endpoint and keep whether port/protocol were explicit."""

    raw_parts = [part.strip() for part in str(endpoint or "").split(",")]
    if len(raw_parts) == 1:
        if require_explicit_port or not raw_parts[0]:
            raise ValueError("collector_server_endpoint_invalid")
        host = raw_parts[0]
        protocol_text = DEFAULT_COLLECTOR_SERVER_PROTOCOL
        has_explicit_port = False
        has_explicit_protocol = False
    elif len(raw_parts) == 2:
        if any(not part for part in raw_parts):
            raise ValueError("collector_server_endpoint_invalid")
        if require_explicit_protocol:
            raise ValueError("collector_server_endpoint_invalid")
        host, port_text = raw_parts
        protocol_text = DEFAULT_COLLECTOR_SERVER_PROTOCOL
        has_explicit_port = True
        has_explicit_protocol = False
    elif len(raw_parts) == 3:
        if any(not part for part in raw_parts):
            raise ValueError("collector_server_endpoint_invalid")
        host, port_text, protocol_text = raw_parts
        has_explicit_port = True
        has_explicit_protocol = True
    else:
        raise ValueError("collector_server_endpoint_invalid")

    normalized_host = validate_collector_server_host(host)
    if has_explicit_port:
        normalized_port = validate_collector_server_port(port_text)
    else:
        catalog = load_collector_cloud_profile_catalog()
        normalized_port = default_collector_server_port(
            cloud_family=catalog.families_by_host.get(normalized_host.lower(), "")
        )
    normalized_protocol = validate_collector_server_protocol(
        protocol_text,
        require_tcp=require_tcp,
    )
    return CollectorServerEndpointParts(
        host=normalized_host,
        port=normalized_port,
        protocol=normalized_protocol,
        has_explicit_port=has_explicit_port,
        has_explicit_protocol=has_explicit_protocol,
    )


def parse_collector_server_endpoint(
    endpoint: str,
    *,
    require_explicit_port: bool = True,
    require_explicit_protocol: bool = True,
    require_tcp: bool = False,
) -> tuple[str, int, str]:
    """Parse and validate one collector callback endpoint string."""

    parsed = inspect_collector_server_endpoint(
        endpoint,
        require_explicit_port=require_explicit_port,
        require_explicit_protocol=require_explicit_protocol,
        require_tcp=require_tcp,
    )
    return parsed.host, parsed.port, parsed.protocol


def resolve_collector_server_endpoint(
    endpoint: str,
    *,
    require_explicit_port: bool = True,
    require_explicit_protocol: bool = True,
    require_tcp: bool = False,
    cloud_family: str = "",
) -> tuple[str, int, str]:
    """Resolve one endpoint into runtime host, port and protocol semantics."""

    parsed = inspect_collector_server_endpoint(
        endpoint,
        require_explicit_port=require_explicit_port,
        require_explicit_protocol=require_explicit_protocol,
        require_tcp=require_tcp,
    )
    port = parsed.port
    if not parsed.has_explicit_port:
        port = default_collector_server_port(cloud_family=cloud_family)
    return parsed.host, port, parsed.protocol


def normalize_collector_server_endpoint(
    endpoint: str,
    *,
    require_explicit_port: bool = True,
    require_explicit_protocol: bool = True,
    require_tcp: bool = False,
    preserve_shape: bool = False,
) -> str:
    """Normalize one collector callback endpoint string after validation."""

    parsed = inspect_collector_server_endpoint(
        endpoint,
        require_explicit_port=require_explicit_port,
        require_explicit_protocol=require_explicit_protocol,
        require_tcp=require_tcp,
    )
    return parsed.render(preserve_shape=preserve_shape)
