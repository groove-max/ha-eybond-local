from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import CollectorCloudProfile
from custom_components.eybond_local.support.runtime_projection import (
    build_collector_support_payload,
    build_inverter_support_payload,
    build_support_fixture,
    metadata_source_payload,
)


def _collector() -> SimpleNamespace:
    return SimpleNamespace(
        remote_ip="192.0.2.10",
        remote_port=18899,
        connection_count=2,
        connection_replace_count=1,
        disconnect_count=3,
        pending_request_drop_count=4,
        last_disconnect_reason="peer_closed",
        discovery_restart_count=5,
        last_discovery_reason="manual",
        collector_pn="E50000200000000001",
        profile_key="eybond_ascii_pn_v1",
        profile_name="EyeBond ASCII PN v1",
        last_udp_reply="ok",
        last_udp_reply_from="192.0.2.10",
        last_devcode=0x0994,
        smartess_collector_version="8.50.12.3",
        smartess_protocol_raw_id="0925",
        smartess_protocol_asset_id="0925",
        smartess_protocol_asset_name="Cloud AT",
        smartess_protocol_suffix="",
        smartess_protocol_profile_key="smartess_at",
        smartess_protocol_name="Cloud AT",
        smartess_device_address=1,
    )


def _inverter() -> SimpleNamespace:
    return SimpleNamespace(
        driver_key="pi30",
        protocol_family="pi30",
        model_name="PI30 inverter",
        variant_key="default",
        serial_number="55355535553555",
        profile_name="pi30/base.json",
        register_schema_name="pi30/base.json",
        probe_target=SimpleNamespace(devcode=0x0994, collector_addr=1, device_addr=0),
        details={"rated_power": 5000},
    )


class RuntimeSupportProjectionTests(unittest.TestCase):
    def test_collector_payload_uses_one_exact_cloud_profile(self) -> None:
        profile = CollectorCloudProfile(
            key="valuecloud_at",
            label="ValueCloud AT",
            source="transport_sniff",
            confidence="high",
        )

        payload = build_collector_support_payload(_collector(), profile)

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["collector_pn"], "E50000200000000001")
        self.assertEqual(payload["collector_cloud_profile_key"], profile.key)
        self.assertEqual(payload["collector_cloud_profile_source"], profile.source)
        with self.assertRaises(TypeError):
            build_collector_support_payload(_collector(), object())  # type: ignore[arg-type]

    def test_inverter_payload_is_detached_from_runtime_details(self) -> None:
        inverter = _inverter()

        payload = build_inverter_support_payload(inverter)
        inverter.details["rated_power"] = 6000

        self.assertEqual(payload["details"]["rated_power"], 5000)
        self.assertEqual(payload["probe_target"]["devcode"], 0x0994)

    def test_support_fixture_prefers_bound_inverter_route(self) -> None:
        fixture = build_support_fixture(
            {"fixture_ranges": [{"start": 0, "values": [1, 2]}]},
            inverter=_inverter(),
            collector_payload=build_collector_support_payload(
                _collector(),
                CollectorCloudProfile(),
            ),
        )

        self.assertIsNotNone(fixture)
        assert fixture is not None
        self.assertEqual(fixture["name"], "pi30_support_capture")
        self.assertEqual(fixture["probe_target"]["collector_addr"], 1)
        self.assertEqual(fixture["collector"]["collector_pn"], "E50000200000000001")

    def test_generic_fixture_selects_the_most_complete_capture(self) -> None:
        fixture = build_support_fixture(
            {
                "capture_kind": "generic_register_dump",
                "captures": [
                    {
                        "driver_key": "short",
                        "fixture_ranges": [{"start": 0, "values": [1]}],
                        "range_failures": [],
                        "probe_target": {"devcode": 1},
                    },
                    {
                        "driver_key": "complete",
                        "fixture_ranges": [
                            {"start": 0, "values": [1]},
                            {"start": 10, "values": [2]},
                        ],
                        "range_failures": ["one"],
                        "probe_target": {"devcode": 2},
                    },
                ],
            },
            inverter=None,
            collector_payload=None,
        )

        self.assertIsNotNone(fixture)
        assert fixture is not None
        self.assertEqual(fixture["name"], "complete_support_capture")
        self.assertEqual(fixture["probe_target"], {"devcode": 2})
        self.assertEqual(len(fixture["ranges"]), 2)

    def test_empty_capture_and_missing_metadata_remain_empty(self) -> None:
        self.assertIsNone(
            build_support_fixture({}, inverter=None, collector_payload=None)
        )
        self.assertIsNone(metadata_source_payload(None))
        self.assertEqual(
            metadata_source_payload(
                SimpleNamespace(
                    source_name="builtin",
                    source_scope="integration",
                    source_path="profiles/base.json",
                )
            ),
            {
                "name": "builtin",
                "scope": "integration",
                "path": "profiles/base.json",
            },
        )


class RuntimeProjectionArchitectureTests(unittest.TestCase):
    def test_coordinator_delegates_and_does_not_redefine_projection_helpers(self) -> None:
        path = REPO_ROOT / "custom_components/eybond_local/runtime/coordinator.py"
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        coordinator = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "EybondLocalCoordinator"
        )
        method_names = {
            node.name for node in coordinator.body if isinstance(node, ast.FunctionDef)
        }

        self.assertTrue(
            all(
                name in source
                for name in {
                    "build_collector_support_payload",
                    "build_inverter_support_payload",
                    "build_support_fixture",
                    "metadata_source_payload",
                }
            )
        )
        self.assertTrue(
            {
                "_collector_payload",
                "_inverter_payload",
                "_build_support_fixture",
                "_best_generic_capture",
                "_metadata_source_payload",
            }.isdisjoint(method_names)
        )

    def test_projection_module_has_no_runtime_or_home_assistant_dependency(self) -> None:
        path = (
            REPO_ROOT
            / "custom_components/eybond_local/support/runtime_projection.py"
        )
        source = path.read_text(encoding="utf-8")

        self.assertNotIn("homeassistant", source)
        self.assertNotIn("runtime.coordinator", source)
        self.assertNotIn("config_flow", source)


if __name__ == "__main__":
    unittest.main()
