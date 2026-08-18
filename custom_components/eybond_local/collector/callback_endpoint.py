"""Build Home Assistant callback endpoints from neutral endpoint/cloud facts."""

from __future__ import annotations

from .cloud_family import collector_cloud_family_observation_from_endpoint
from ..collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PROTOCOL,
    default_collector_server_port,
    format_collector_server_endpoint_for_cloud_profile,
    resolve_collector_server_endpoint,
)


def home_assistant_callback_endpoint(
    *,
    server_host: str,
    listener_port: int,
    template_endpoint: str = "",
    cloud_family: str = "",
) -> str:
    """Build the Home Assistant callback endpoint for a collector.

    The callback target always carries this entry's listener port.  The
    collector-reported endpoint template shapes only protocol/format: its port
    belongs to the vendor cloud (or proxy capture) and is never inherited.
    """

    normalized_template = str(template_endpoint or "").strip()
    normalized_family = str(cloud_family or "").strip().lower()
    server_protocol = DEFAULT_COLLECTOR_SERVER_PROTOCOL
    if normalized_template:
        if not normalized_family:
            observed = collector_cloud_family_observation_from_endpoint(
                normalized_template
            ).family
            if observed and observed != "unknown":
                normalized_family = observed
        try:
            _host, _template_port, server_protocol = resolve_collector_server_endpoint(
                normalized_template,
                require_explicit_port=False,
                require_explicit_protocol=False,
                cloud_family=normalized_family,
            )
        except ValueError:
            server_protocol = DEFAULT_COLLECTOR_SERVER_PROTOCOL
    server_port = (
        int(listener_port)
        if int(listener_port or 0) > 0
        else default_collector_server_port(cloud_family=normalized_family)
    )
    return format_collector_server_endpoint_for_cloud_profile(
        server_host=server_host,
        cloud_family=normalized_family,
        server_port=server_port,
        server_protocol=server_protocol,
        template_endpoint=normalized_template,
        require_tcp=True,
    )


__all__ = ["home_assistant_callback_endpoint"]
