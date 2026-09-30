"""User-supplied scan routes are explicit and bounded, never subnet expansion."""
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.eybond_local.onboarding.discovery_addresses import parse_known_collector_ips


class DiscoveryAddressTests(unittest.TestCase):
    def test_empty_and_stable_deduplication(self):
        self.assertEqual(parse_known_collector_ips("  "), ())
        self.assertEqual(parse_known_collector_ips("192.0.2.7, 198.51.100.9\n192.0.2.7"),
                         ("192.0.2.7", "198.51.100.9"))

    def test_routes_need_no_same_subnet_assumption(self):
        self.assertEqual(parse_known_collector_ips("10.9.0.2, 192.0.2.7"),
                         ("10.9.0.2", "192.0.2.7"))

    def test_networks_hostnames_ports_and_special_addresses_rejected(self):
        for value in ("192.0.2.0/24", "collector.local", "192.0.2.7:58899",
                      "::1", "0.0.0.0", "127.0.0.1", "255.255.255.255",
                      "224.0.0.1", "192.0.2.1-20", "192.0.2.01", None, [], "1" * 513):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_known_collector_ips(value)

    def test_local_host_and_interface_broadcast_rejected(self):
        for value in ("192.0.2.10", "192.0.2.255"):
            with self.assertRaises(ValueError):
                parse_known_collector_ips(value, excluded=("192.0.2.10", "192.0.2.255"))

    def test_bounded_distinct_targets(self):
        eight = ", ".join(f"192.0.2.{n}" for n in range(1, 9))
        self.assertEqual(len(parse_known_collector_ips(eight + ", 192.0.2.1")), 8)
        with self.assertRaises(ValueError):
            parse_known_collector_ips(eight + ", 192.0.2.9")


if __name__ == "__main__":
    unittest.main()
