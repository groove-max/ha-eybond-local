from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.models import (
    EybondConnectionSpec,
    build_connection_spec,
    build_connection_spec_from_values,
    resolve_connection_type,
)
from custom_components.eybond_local.const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_CONNECTION_TYPE,
    CONF_DISCOVERY_TARGET,
    CONF_HEARTBEAT_INTERVAL,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
)


class ConnectionModelsTests(unittest.TestCase):
    def test_eybond_connection_spec_coerces_numeric_fields(self) -> None:
        spec = EybondConnectionSpec(
            server_ip="192.168.1.50",
            advertised_server_ip="203.0.113.10",
            tcp_port=8899.0,
            advertised_tcp_port=9889.0,
            udp_port=58899.0,
            collector_ip="192.168.1.14",
            discovery_target="192.168.1.255",
            discovery_interval=30.0,
            heartbeat_interval=60.0,
            request_timeout=5,
        )

        self.assertEqual(spec.type, "eybond")
        self.assertEqual(spec.advertised_server_ip, "203.0.113.10")
        self.assertEqual(spec.tcp_port, 8899)
        self.assertEqual(spec.advertised_tcp_port, 9889)
        self.assertEqual(spec.udp_port, 58899)
        self.assertEqual(spec.discovery_interval, 30)
        self.assertEqual(spec.heartbeat_interval, 60)
        self.assertEqual(spec.request_timeout, 5.0)

    def test_eybond_connection_spec_falls_back_to_listener_endpoint_when_advertised_override_is_empty(self) -> None:
        spec = EybondConnectionSpec(
            server_ip="192.168.1.50",
            tcp_port=8899,
            udp_port=58899,
            discovery_interval=30,
            heartbeat_interval=60,
            request_timeout=5,
        )

        self.assertEqual(spec.effective_advertised_server_ip, "192.168.1.50")
        self.assertEqual(spec.effective_advertised_tcp_port, 8899)

    def test_build_connection_spec_defaults_to_eybond_for_legacy_entry_data(self) -> None:
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_ADVERTISED_SERVER_IP: "203.0.113.10",
                CONF_TCP_PORT: 8899,
                CONF_ADVERTISED_TCP_PORT: 9889,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.14",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )

        self.assertIsInstance(spec, EybondConnectionSpec)
        self.assertEqual(spec.type, "eybond")
        self.assertEqual(spec.server_ip, "192.168.1.50")
        self.assertEqual(spec.effective_advertised_server_ip, "203.0.113.10")
        self.assertEqual(spec.effective_advertised_tcp_port, 9889)

    def test_resolve_connection_type_reads_explicit_type(self) -> None:
        connection_type = resolve_connection_type({CONF_CONNECTION_TYPE: "eybond"})
        self.assertEqual(connection_type, "eybond")

    def test_build_connection_spec_from_values_uses_branch_aware_builder(self) -> None:
        spec = build_connection_spec_from_values(
            "eybond",
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.14",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
        )

        self.assertIsInstance(spec, EybondConnectionSpec)
        self.assertEqual(spec.type, "eybond")
        self.assertEqual(spec.server_ip, "192.168.1.50")
        self.assertEqual(spec.effective_advertised_server_ip, "192.168.1.50")
        self.assertEqual(spec.effective_advertised_tcp_port, 8899)


if __name__ == "__main__":
    unittest.main()
