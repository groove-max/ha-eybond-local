from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.entry import (
    build_detected_entry_settings,
    build_manual_entry_settings,
    build_runtime_option_settings,
    persisted_branch_setting_keys,
    runtime_option_setting_keys,
    with_driver_hint,
)


class ConnectionEntryTests(unittest.TestCase):
    def test_persisted_branch_setting_keys_cover_eybond_branch_fields(self) -> None:
        self.assertEqual(
            persisted_branch_setting_keys("eybond"),
            (
                "server_ip",
                "collector_ip",
                "driver_hint",
                "tcp_port",
                "advertised_server_ip",
                "advertised_tcp_port",
                "udp_port",
                "discovery_target",
                "discovery_interval",
                "heartbeat_interval",
            ),
        )

    def test_build_detected_entry_settings_uses_branch_defaults_and_overrides(self) -> None:
        values = build_detected_entry_settings(
            "eybond",
            server_ip="192.168.1.50",
            collector_ip="192.168.1.55",
            default_broadcast="192.168.1.255",
            overrides={"driver_hint": "modbus_smg"},
        )

        self.assertEqual(values["server_ip"], "192.168.1.50")
        self.assertEqual(values["collector_ip"], "192.168.1.55")
        self.assertEqual(values["discovery_target"], "192.168.1.255")
        self.assertEqual(values["driver_hint"], "modbus_smg")

    def test_build_manual_entry_settings_filters_to_branch_keys(self) -> None:
        values = build_manual_entry_settings(
            "eybond",
            {
                "server_ip": "192.168.1.50",
                "collector_ip": "192.168.1.55",
                "tcp_port": 8899,
                "advertised_server_ip": "203.0.113.10",
                "advertised_tcp_port": 9443,
                "udp_port": 58899,
                "discovery_target": "192.168.1.255",
                "discovery_interval": 3,
                "heartbeat_interval": 60,
                "driver_hint": "auto",
                "unexpected": "drop-me",
            },
        )

        self.assertNotIn("unexpected", values)
        self.assertEqual(values["advertised_server_ip"], "203.0.113.10")
        self.assertEqual(values["advertised_tcp_port"], 9443)
        self.assertEqual(values["udp_port"], 58899)

    def test_runtime_option_setting_keys_match_runtime_form_fields(self) -> None:
        self.assertEqual(
            runtime_option_setting_keys("eybond"),
            (
                "server_ip",
                "collector_ip",
                "tcp_port",
                "advertised_server_ip",
                "advertised_tcp_port",
                "udp_port",
                "discovery_target",
                "discovery_interval",
                "heartbeat_interval",
                "driver_hint",
            ),
        )

    def test_build_runtime_option_settings_filters_to_runtime_keys(self) -> None:
        values = build_runtime_option_settings(
            "eybond",
            {
                "server_ip": "192.168.1.50",
                "collector_ip": "192.168.1.55",
                "tcp_port": 8899,
                "advertised_server_ip": "203.0.113.10",
                "advertised_tcp_port": 9443,
                "udp_port": 58899,
                "discovery_target": "192.168.1.255",
                "discovery_interval": 3,
                "heartbeat_interval": 60,
                "driver_hint": "auto",
                "poll_interval": 10,
            },
        )

        self.assertNotIn("poll_interval", values)
        self.assertEqual(values["advertised_tcp_port"], 9443)
        self.assertEqual(values["driver_hint"], "auto")

    def test_with_driver_hint_overrides_existing_value(self) -> None:
        values = with_driver_hint({"driver_hint": "auto", "server_ip": "192.168.1.50"}, driver_hint="pi30")

        self.assertEqual(values["driver_hint"], "pi30")
        self.assertEqual(values["server_ip"], "192.168.1.50")


if __name__ == "__main__":
    unittest.main()
