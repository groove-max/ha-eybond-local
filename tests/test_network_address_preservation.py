"""#82: a transient host inventory cannot replace explicit callback intent."""
from __future__ import annotations

import json
import subprocess
import unittest
from unittest.mock import patch

from custom_components.eybond_local import network_interfaces as network
from custom_components.eybond_local.runtime.link.common import resolve_server_ip


def interface(name, ip, prefix=24):
    return {"ifname": name, "addr_info": [{
        "family": "inet", "scope": "global", "local": ip, "prefixlen": prefix,
    }]}


class NetworkAddressTests(unittest.TestCase):
    def test_configured_address_survives_partial_inventory_and_changed_default(self):
        # Different subnets, including a routed collector and an AP-mode target.
        for configured, collector in (("192.0.2.94", "198.51.100.20"),
                                      ("192.0.2.94", "192.0.2.20")):
            with self.subTest(collector=collector), patch.object(
                network, "get_ipv4_interfaces", return_value=[
                    {"name": "wlan0", "ip": "203.0.113.4", "prefixlen": "24"},
                ],
            ) as inventory, patch(
                "custom_components.eybond_local.runtime.link.common._default_local_ip",
                return_value="172.30.32.1",
            ) as fallback:
                self.assertEqual(resolve_server_ip(configured, collector_ip=collector), configured)
                inventory.assert_not_called()
                fallback.assert_not_called()

    def test_filtered_interfaces_are_shared_by_flow_and_runtime(self):
        raw = [interface(name, f"172.30.{i}.1") for i, name in enumerate(
            ("docker0", "hassio", "hassio@if4", "veth123", "br-123", "virbr0", "cni0", "flannel.1")
        )]
        raw += [interface("br0", "192.0.2.20"), interface("wg0", "198.51.100.20")]
        with patch.object(network.subprocess, "check_output", return_value=json.dumps(raw)), patch(
            "custom_components.eybond_local.runtime.link.common._default_local_ip",
            return_value="172.30.32.1",
        ):
            self.assertEqual([i["name"] for i in network.get_ipv4_interfaces()], ["br0", "wg0"])
            self.assertEqual(resolve_server_ip("", collector_ip="198.51.100.8"), "198.51.100.20")
            self.assertEqual(resolve_server_ip("", collector_ip="203.0.113.8"), "192.0.2.20")

    def test_empty_or_internal_only_inventory_cannot_reintroduce_default_ip(self):
        for raw in ([], [interface("hassio", "172.30.32.1")]):
            for oneline in ("", "4: hassio inet 172.30.32.1/23 scope global hassio\n"):
                with self.subTest(raw=raw, oneline=oneline), patch.object(
                    network.subprocess, "check_output", side_effect=[json.dumps(raw), oneline],
                ), patch.object(network, "get_local_ip", return_value="172.30.32.1") as fallback:
                    self.assertEqual(resolve_server_ip("", collector_ip="198.51.100.20"), "")
                    fallback.assert_not_called()

    def test_known_internal_json_remains_excluded_if_busybox_fallback_fails(self):
        with patch.object(network.subprocess, "check_output", side_effect=[
            json.dumps([interface("docker0", "172.17.0.1")]), OSError("unavailable"),
        ]), patch.object(network, "get_local_ip", return_value="172.17.0.1") as fallback:
            self.assertEqual(network.get_ipv4_interfaces(), [])
            fallback.assert_not_called()

    def test_busybox_filter_preserves_lan_vlan_and_vpn_interfaces(self):
        lines = (
            "2: docker0 inet 172.17.0.1/16 scope global docker0\n"
            "3: hassio@if4 inet 172.30.32.1/23 scope global hassio\n"
            "4: eth0.20@if2 inet 192.0.2.10/24 brd 192.0.2.255 scope global eth0.20\n"
            "5: tailscale0 inet 100.64.1.2/32 scope global tailscale0\n"
        )
        with patch.object(network.subprocess, "check_output", side_effect=[
            subprocess.CalledProcessError(1, "ip -j"), lines,
        ]):
            self.assertEqual([i["ip"] for i in network.get_ipv4_interfaces()],
                             ["192.0.2.10", "100.64.1.2"])

    def test_missing_enumeration_can_still_supply_initial_default(self):
        with patch.object(network.subprocess, "check_output", side_effect=FileNotFoundError), patch.object(
            network, "get_local_ip", return_value="192.0.2.20",
        ):
            self.assertEqual(network.get_ipv4_interfaces(), [
                {"name": "default", "ip": "192.0.2.20", "label": "192.0.2.20"},
            ])

    def test_malformed_json_shape_uses_oneline_without_crashing(self):
        for raw in ({"error": "unavailable"}, [None]):
            with self.subTest(raw=raw), patch.object(network.subprocess, "check_output", side_effect=[
                json.dumps(raw), "2: eth0 inet 192.0.2.10/24 scope global eth0\n",
            ]):
                self.assertEqual(network.get_ipv4_interfaces()[0]["ip"], "192.0.2.10")


if __name__ == "__main__":
    unittest.main()
