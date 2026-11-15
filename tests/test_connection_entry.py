from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.entry import (
    apply_catalog_choice,
    apply_driver_choice,
    build_detected_entry_settings,
    build_manual_entry_settings,
    build_runtime_option_settings,
    persisted_branch_setting_keys,
    resolve_catalog_entry_key,
    resolve_driver_hint,
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

class DriverChoicePersistenceTests(unittest.TestCase):
    """One place decides which of data/options wins and how a choice is saved."""

    def test_resolve_driver_hint_prefers_options_over_data(self) -> None:
        self.assertEqual(
            resolve_driver_hint({"driver_hint": "modbus_smg"}, {"driver_hint": "pi30"}),
            "pi30",
        )
        self.assertEqual(
            resolve_driver_hint({"driver_hint": "modbus_smg"}, {}),
            "modbus_smg",
        )
        self.assertEqual(resolve_driver_hint({}, {}), "auto")
        self.assertEqual(
            resolve_driver_hint({"driver_hint": "  "}, {"driver_hint": None}),
            "auto",
        )

    def test_resolve_catalog_entry_key_prefers_options_over_data(self) -> None:
        key = "lvyuan_ty_sic_6_5kbe_w1"
        self.assertEqual(
            resolve_catalog_entry_key(
                {"device_catalog_entry_key": key},
                {"device_catalog_entry_key": "other"},
            ),
            "other",
        )
        self.assertEqual(
            resolve_catalog_entry_key({"device_catalog_entry_key": key}, {}),
            key,
        )
        self.assertEqual(resolve_catalog_entry_key({}, {}), "")

    def test_named_driver_choice_is_written_to_both_sources(self) -> None:
        data = {"driver_hint": "auto", "server_ip": "192.168.1.50"}
        options: dict[str, object] = {}

        apply_driver_choice(data, options, driver_hint="modbus_catalog")

        self.assertEqual(data["driver_hint"], "modbus_catalog")
        self.assertEqual(options["driver_hint"], "modbus_catalog")
        self.assertEqual(data["server_ip"], "192.168.1.50")

    def test_automatic_driver_choice_clears_both_sources(self) -> None:
        data = {"driver_hint": "modbus_smg"}
        options = {"driver_hint": "modbus_catalog"}

        apply_driver_choice(data, options, driver_hint="auto")

        self.assertNotIn("driver_hint", data)
        self.assertNotIn("driver_hint", options)
        self.assertEqual(resolve_driver_hint(data, options), "auto")

    def test_selected_catalog_map_is_stored_once_and_dropped_from_data(self) -> None:
        data = {"device_catalog_entry_key": "stale"}
        options: dict[str, object] = {}

        apply_catalog_choice(data, options, catalog_entry_key="lvyuan_ty_sic_6_5kbe_w1")

        self.assertEqual(
            options["device_catalog_entry_key"], "lvyuan_ty_sic_6_5kbe_w1"
        )
        self.assertNotIn("device_catalog_entry_key", data)
        self.assertEqual(
            resolve_catalog_entry_key(data, options), "lvyuan_ty_sic_6_5kbe_w1"
        )

    def test_released_catalog_map_is_cleared_from_both_sources(self) -> None:
        data = {"device_catalog_entry_key": "lvyuan_ty_sic_6_5kbe_w1"}
        options = {"device_catalog_entry_key": "lvyuan_ty_sic_6_5kbe_w1"}

        apply_catalog_choice(data, options, catalog_entry_key="")

        self.assertNotIn("device_catalog_entry_key", data)
        self.assertNotIn("device_catalog_entry_key", options)
        self.assertEqual(resolve_catalog_entry_key(data, options), "")


if __name__ == "__main__":
    unittest.main()
