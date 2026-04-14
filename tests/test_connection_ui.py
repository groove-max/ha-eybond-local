from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.ui import (
    EYBOND_CONNECTION_DISPLAY_METADATA,
    EYBOND_CONNECTION_FORM_LAYOUT,
    build_eybond_auto_values,
    build_eybond_manual_base_values,
    build_eybond_runtime_option_values,
)


class ConnectionUiTests(unittest.TestCase):
    def test_eybond_form_layout_exposes_auto_manual_and_runtime_fields(self) -> None:
        self.assertEqual(tuple(field.key for field in EYBOND_CONNECTION_FORM_LAYOUT.auto_fields), ("server_ip",))
        self.assertIn("collector_ip", tuple(field.key for field in EYBOND_CONNECTION_FORM_LAYOUT.manual_fields))
        self.assertIn(
            "advertised_server_ip",
            tuple(field.key for field in EYBOND_CONNECTION_FORM_LAYOUT.manual_advanced_fields),
        )
        self.assertIn(
            "advertised_tcp_port",
            tuple(field.key for field in EYBOND_CONNECTION_FORM_LAYOUT.runtime_fields),
        )
        self.assertIn("driver_hint", tuple(field.key for field in EYBOND_CONNECTION_FORM_LAYOUT.runtime_fields))
        self.assertEqual(EYBOND_CONNECTION_FORM_LAYOUT.auto_fields[0].label, "Home Assistant IP")
        self.assertEqual(EYBOND_CONNECTION_FORM_LAYOUT.auto_fields[0].validation_kind, "ipv4")
        self.assertEqual(
            next(field for field in EYBOND_CONNECTION_FORM_LAYOUT.manual_advanced_fields if field.key == "discovery_target").validation_kind,
            "ipv4",
        )
        self.assertEqual(EYBOND_CONNECTION_DISPLAY_METADATA.integration_name, "EyeBond Local")
        self.assertEqual(EYBOND_CONNECTION_DISPLAY_METADATA.pending_entry_title, "EyeBond Setup Pending")

    def test_build_eybond_auto_values_uses_branch_defaults(self) -> None:
        values = build_eybond_auto_values(
            server_ip="192.168.1.50",
            default_broadcast="192.168.1.255",
        )

        self.assertEqual(values["server_ip"], "192.168.1.50")
        self.assertEqual(values["discovery_target"], "192.168.1.255")
        self.assertEqual(values["tcp_port"], 8899)

    def test_build_eybond_manual_base_values_reuses_stored_defaults(self) -> None:
        values = build_eybond_manual_base_values(
            server_ip="192.168.1.50",
            default_broadcast="192.168.1.255",
            stored_defaults={
                "tcp_port": 9000,
                "advertised_server_ip": "203.0.113.10",
                "advertised_tcp_port": 9443,
                "driver_hint": "modbus_smg",
            },
            collector_ip="192.168.1.55",
            driver_hint="auto",
        )

        self.assertEqual(values["collector_ip"], "192.168.1.55")
        self.assertEqual(values["tcp_port"], 9000)
        self.assertEqual(values["advertised_server_ip"], "203.0.113.10")
        self.assertEqual(values["advertised_tcp_port"], "9443")
        self.assertEqual(values["driver_hint"], "modbus_smg")

    def test_build_eybond_runtime_option_values_prefers_options(self) -> None:
        values = build_eybond_runtime_option_values(
            data={"server_ip": "192.168.1.50", "driver_hint": "auto"},
            options={
                "server_ip": "192.168.1.60",
                "advertised_server_ip": "203.0.113.10",
                "advertised_tcp_port": 9443,
                "driver_hint": "pi30",
            },
            default_server_ip="192.168.1.50",
            default_broadcast="192.168.1.255",
        )

        self.assertEqual(values["server_ip"], "192.168.1.60")
        self.assertEqual(values["advertised_server_ip"], "203.0.113.10")
        self.assertEqual(values["advertised_tcp_port"], "9443")
        self.assertEqual(values["driver_hint"], "pi30")


if __name__ == "__main__":
    unittest.main()
