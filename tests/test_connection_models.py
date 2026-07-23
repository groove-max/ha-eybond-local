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
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_TYPE,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_HINT,
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
                CONF_COLLECTOR_PN: "E5000020000000",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )

        self.assertIsInstance(spec, EybondConnectionSpec)
        self.assertEqual(spec.type, "eybond")
        self.assertEqual(spec.server_ip, "192.168.1.50")
        self.assertEqual(spec.collector_pn, "E5000020000000")
        self.assertEqual(spec.collector_cloud_family, "smartess_at")
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertEqual(spec.collector_identity_strategy, "")
        self.assertEqual(spec.effective_advertised_server_ip, "203.0.113.10")
        self.assertEqual(spec.effective_advertised_tcp_port, 9889)

    def test_build_connection_spec_metadata_is_wire_neutral(self) -> None:
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.55",
                CONF_COLLECTOR_PN: "E50000200000009777",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DRIVER_HINT: "modbus_smg",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )

        self.assertEqual(spec.collector_cloud_family, "smartess_at")
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertEqual(spec.collector_identity_strategy, "")

    def test_build_connection_spec_virtual_bridge_does_not_choose_wire(self) -> None:
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 18899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "203.0.113.175",
                CONF_COLLECTOR_PN: "V001020SYN62344022",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DRIVER_HINT: "auto",
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )

        self.assertEqual(spec.collector_cloud_family, "smartess_at")
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertEqual(spec.collector_identity_strategy, "")

    def test_build_connection_spec_virtual_bridge_driver_hint_does_not_select_framed(self) -> None:
        # Neither the driver hint nor the collector/bridge kind may pick the
        # transport. On an esp-collector bridge with a modbus_smg hint the
        # protocol remains unset until a live session proves it.
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 18899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "203.0.113.175",
                CONF_COLLECTOR_PN: "V001020SYN62344022",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DRIVER_HINT: "modbus_smg",
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )

        self.assertEqual(spec.collector_cloud_family, "smartess_at")
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertEqual(spec.collector_identity_strategy, "")

    def test_build_connection_spec_recovers_cloud_family_from_remembered_endpoint_options(self) -> None:
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.98",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.2.209",
                CONF_COLLECTOR_PN: "A0000000000001",
                CONF_DRIVER_HINT: "auto",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: "dtu_ess.eybond.com,18899,TCP",
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: "smartess_at",
            },
        )

        self.assertEqual(spec.collector_cloud_family, "smartess_at")
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertEqual(spec.collector_identity_strategy, "")

    def test_build_connection_spec_bootstrap_ignores_effective_owner_key(self) -> None:
        # An effective owner key (driver) from the metadata snapshot must not
        # pick the transport either.
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.55",
                CONF_COLLECTOR_PN: "E50000200000009777",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DRIVER_HINT: "auto",
                "effective_metadata_snapshot": {
                    "effective_owner_key": "modbus_smg",
                },
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )

        self.assertEqual(spec.collector_cloud_family, "smartess_at")
        self.assertEqual(spec.collector_configured_session_protocol, "")

    def test_build_connection_spec_keeps_data_driver_when_options_driver_is_auto(self) -> None:
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.55",
                CONF_COLLECTOR_PN: "E50000200000009777",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DRIVER_HINT: "modbus_smg",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {CONF_DRIVER_HINT: "auto"},
        )

        # Data-vs-options driver precedence is unrelated to wire authority.
        self.assertEqual(spec.collector_configured_session_protocol, "")

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

    def test_confirmed_protocol_read_only_with_live_provenance(self) -> None:
        # A confirmed protocol is carried ONLY when its provenance source is
        # exactly ``live_session`` and a durable PN is present.
        base = {
            CONF_SERVER_IP: "192.168.1.50",
            CONF_TCP_PORT: 8899,
            CONF_UDP_PORT: 58899,
            CONF_COLLECTOR_IP: "192.168.1.55",
            CONF_COLLECTOR_PN: "E50000200000009777",
            CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
            CONF_DISCOVERY_TARGET: "192.168.1.255",
            CONF_HEARTBEAT_INTERVAL: 60,
            "collector_confirmed_session_protocol": "eybond_framed",
            "collector_confirmed_session_protocol_pn": "E50000200000009777",
            "collector_confirmed_session_protocol_source": "live_session",
        }
        spec = build_connection_spec(base, {})
        # Confirmed evidence is a VALIDATED value object, not loose strings on the
        # spec. A direct spec field for the confirmed protocol no longer exists.
        evidence = spec.confirmed_session_protocol_evidence
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.protocol, "eybond_framed")
        self.assertEqual(evidence.collector_pn, "E50000200000009777")
        self.assertEqual(evidence.source, "live_session")
        self.assertEqual(
            spec.collector_configured_session_protocol,
            "eybond_framed",
        )
        self.assertEqual(
            spec.collector_identity_strategy,
            "framed_heartbeat_then_fc2_pn",
        )

    def test_confirmed_protocol_without_live_provenance_is_fail_closed(self) -> None:
        # Migration is fail-closed: a persisted protocol whose provenance is NOT
        # live_session (or missing) yields NO validated evidence at all.
        for source in ("", "cloud_family", "inferred", "unknown"):
            data = {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.55",
                CONF_COLLECTOR_PN: "E50000200000009777",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
                "collector_confirmed_session_protocol": "eybond_framed",
                "collector_confirmed_session_protocol_pn": "E50000200000009777",
                "collector_confirmed_session_protocol_source": source,
            }
            spec = build_connection_spec(data, {})
            self.assertIsNone(
                spec.confirmed_session_protocol_evidence, f"source={source!r}"
            )

    def test_confirmed_at_wire_allows_cloud_dialect_refinement(self) -> None:
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.55",
                CONF_COLLECTOR_PN: "E50000200000009777",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
                "collector_confirmed_session_protocol": "at_text",
                "collector_confirmed_session_protocol_pn": "E50000200000009777",
                "collector_confirmed_session_protocol_source": "live_session",
            },
            {},
        )

        self.assertEqual(spec.collector_configured_session_protocol, "at_text")
        self.assertEqual(spec.collector_identity_strategy, "at_dtupn")
        self.assertEqual(spec.collector_raw_passthrough_frame_format, "transparent")

    def test_legacy_inferred_session_protocol_is_ignored(self) -> None:
        # The legacy field has no provenance. It neither becomes confirmed
        # evidence nor supplies a preliminary runtime protocol.
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "192.168.1.50",
                CONF_TCP_PORT: 8899,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "192.168.1.55",
                CONF_COLLECTOR_PN: "E50000200000009777",
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DISCOVERY_TARGET: "192.168.1.255",
                CONF_HEARTBEAT_INTERVAL: 60,
                "collector_session_protocol": "at_text",
            },
            {},
        )
        self.assertIsNone(spec.confirmed_session_protocol_evidence)
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertFalse(hasattr(spec, "collector_session_protocol"))


if __name__ == "__main__":
    unittest.main()
