"""Collector-endpoint projections shared by coordinator lifecycles."""

from __future__ import annotations

from datetime import datetime, timezone
import ipaddress
import socket

from ...collector.cloud_family import (
    COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY,
    COLLECTOR_CLOUD_FAMILY_UNKNOWN,
    collector_cloud_family_observation_from_endpoint,
    default_collector_cloud_host,
)
from ...collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    DEFAULT_COLLECTOR_SERVER_PROTOCOL,
    default_collector_server_port,
    format_collector_server_endpoint_for_cloud_profile,
    format_collector_server_endpoint as format_runtime_collector_server_endpoint,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint as normalize_runtime_collector_server_endpoint,
    parse_collector_server_endpoint as parse_runtime_collector_server_endpoint,
    resolve_collector_server_endpoint as resolve_runtime_collector_server_endpoint,
)
from ...const import (
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
)


def format_collector_server_endpoint(
    *,
    server_host: str,
    server_port: int,
    server_protocol: str,
    include_port: bool = True,
    include_protocol: bool = True,
) -> str:
    """Normalize the collector parameter 21 endpoint payload."""

    return format_runtime_collector_server_endpoint(
        server_host=server_host,
        server_port=server_port,
        server_protocol=server_protocol,
        include_port=include_port,
        include_protocol=include_protocol,
    )


def parse_collector_server_endpoint(endpoint: str) -> tuple[str, int, str]:
    """Parse one collector endpoint string like host,port,TCP."""

    return parse_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
    )


def resolve_collector_server_endpoint(
    endpoint: str,
    *,
    cloud_family: str = "",
) -> tuple[str, int, str]:
    """Resolve one collector endpoint into effective host/port/protocol semantics."""

    return resolve_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        cloud_family=cloud_family,
    )


def collector_server_endpoints_equal(
    left: str,
    right: str,
    *,
    cloud_family: str = "",
) -> bool:
    """Return whether two collector endpoints resolve to the same target."""

    try:
        return resolve_collector_server_endpoint(
            left,
            cloud_family=cloud_family,
        ) == resolve_collector_server_endpoint(
            right,
            cloud_family=cloud_family,
        )
    except ValueError:
        return str(left or "").strip() == str(right or "").strip()


def normalize_preserved_collector_server_endpoint(endpoint: str) -> str:
    """Normalize one callback endpoint while keeping its compact raw shape."""

    return normalize_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        preserve_shape=True,
    )


def known_collector_cloud_family(value: object) -> str:
    """Return a concrete collector cloud family, ignoring unknown placeholders."""

    family = str(value or "").strip()
    if family in {"", COLLECTOR_CLOUD_FAMILY_UNKNOWN}:
        return ""
    return family


def known_collector_cloud_profile_value(value: object) -> str:
    """Return one exact normalized collector cloud-profile metadata value."""

    if type(value) is not str or value != value.strip():
        return ""
    return value


def collector_cloud_family_from_endpoint_shape(endpoint: object) -> str:
    """Infer a callback family from endpoint syntax when stronger evidence is absent."""

    observation = collector_cloud_family_observation_from_endpoint(endpoint)
    family = known_collector_cloud_family(observation.family)
    if family:
        return family

    try:
        parsed = inspect_collector_server_endpoint(
            str(endpoint or ""),
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return ""

    if not parsed.has_explicit_port:
        return COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY
    return ""


def collector_original_endpoint_source_options(
    *,
    endpoint: str,
    profile_key: str,
    source: str,
    observed_at: str | None = None,
) -> dict[str, str]:
    """Return option metadata for one preserved original cloud endpoint."""

    normalized_endpoint = str(endpoint or "").strip()
    if not normalized_endpoint:
        return {}

    normalized_profile_key = str(profile_key or "").strip().lower()
    normalized_source = str(source or "").strip() or "runtime_observed"
    timestamp = str(observed_at or "").strip() or datetime.now(timezone.utc).isoformat()
    return {
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: normalized_endpoint,
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: normalized_profile_key,
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: normalized_source,
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT: timestamp,
    }


def format_home_assistant_collector_endpoint(
    *,
    server_host: str,
    template_endpoint: str = "",
    cloud_family: str = "",
) -> str:
    """Build a proxy endpoint mirroring the template's port shape."""

    server_port = default_collector_server_port(cloud_family=cloud_family)
    server_protocol = DEFAULT_COLLECTOR_SERVER_PROTOCOL
    if template_endpoint:
        try:
            _host, server_port, server_protocol = resolve_collector_server_endpoint(
                template_endpoint,
                cloud_family=cloud_family,
            )
        except ValueError:
            server_port = DEFAULT_COLLECTOR_SERVER_PORT
            server_protocol = DEFAULT_COLLECTOR_SERVER_PROTOCOL
    return format_collector_server_endpoint_for_cloud_profile(
        server_host=server_host,
        cloud_family=cloud_family,
        server_port=server_port,
        server_protocol=server_protocol,
        template_endpoint=template_endpoint,
        require_tcp=True,
    )


def default_cloud_upstream_endpoint(
    *,
    cloud_family: str,
    template_endpoint: str = "",
) -> str:
    """Build a family-default upstream endpoint when the original is unknown."""

    normalized_family = str(cloud_family or "").strip().lower()
    default_host = default_collector_cloud_host(normalized_family)
    if not default_host:
        return ""

    return format_collector_server_endpoint_for_cloud_profile(
        server_host=default_host,
        cloud_family=normalized_family,
        server_port=None,
        server_protocol=DEFAULT_COLLECTOR_SERVER_PROTOCOL,
        template_endpoint=template_endpoint,
        require_tcp=True,
    )


def _private_ipv4_host(host: str) -> ipaddress.IPv4Address | None:
    try:
        address = ipaddress.ip_address(str(host or "").strip())
    except ValueError:
        return None
    if address.version != 4 or not address.is_private:
        return None
    return address


def same_ipv4_24(left: str, right: str) -> bool:
    left_address = _private_ipv4_host(left)
    right_address = _private_ipv4_host(right)
    if left_address is None or right_address is None:
        return False
    return ipaddress.ip_network(f"{left_address}/24", strict=False) == ipaddress.ip_network(
        f"{right_address}/24",
        strict=False,
    )


def local_source_ip_for_target(target_ip: str) -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((target_ip, 9))
            return str(sock.getsockname()[0] or "")
    except OSError:
        return ""


__all__ = [
    "collector_cloud_family_from_endpoint_shape",
    "collector_original_endpoint_source_options",
    "collector_server_endpoints_equal",
    "default_cloud_upstream_endpoint",
    "format_collector_server_endpoint",
    "format_home_assistant_collector_endpoint",
    "known_collector_cloud_family",
    "known_collector_cloud_profile_value",
    "local_source_ip_for_target",
    "normalize_preserved_collector_server_endpoint",
    "parse_collector_server_endpoint",
    "resolve_collector_server_endpoint",
    "same_ipv4_24",
]
