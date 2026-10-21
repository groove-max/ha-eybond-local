"""One catalog-backed permanent route for learning, retry and dispatch proof."""

from ...collector_endpoint import resolve_collector_endpoint_write_shape


def learning_listener_port(coordinator) -> int:
    """Keep explicit NAT mappings; honor a host-only collector's implicit port."""
    spec = coordinator._connection_spec
    shape = resolve_collector_endpoint_write_shape(
        cloud_family=getattr(coordinator, "collector_cloud_family", ""),
        template_endpoint=getattr(getattr(coordinator, "data", None), "collector_server_endpoint", ""),
    )
    if shape.port_is_fixed and getattr(spec, "effective_advertised_tcp_port", None) != shape.fixed_port:
        return shape.fixed_port
    return spec.tcp_port


def learning_target_endpoint(coordinator) -> str:
    shape = resolve_collector_endpoint_write_shape(
        cloud_family=coordinator.collector_cloud_family,
        template_endpoint=getattr(getattr(coordinator, "data", None), "collector_server_endpoint", ""),
    )
    host = coordinator._effective_callback_server_host
    if shape.port_is_fixed:
        return host
    return f"{host},{coordinator._connection_spec.effective_advertised_tcp_port},TCP"
