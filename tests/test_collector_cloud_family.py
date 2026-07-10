from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.cloud_family import (  # noqa: E402
    COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY,
    COLLECTOR_CLOUD_FAMILY_SMARTESS_AT,
    COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST,
    COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT,
    COLLECTOR_CLOUD_FAMILY_UNKNOWN,
    collector_cloud_family_observation_from_endpoint,
    default_collector_cloud_host,
)


class CollectorCloudFamilyTests(unittest.TestCase):
    def test_classifies_host_only_legacy_endpoint(self) -> None:
        observation = collector_cloud_family_observation_from_endpoint("ess.eybond.com")

        self.assertEqual(observation.family, COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY)
        self.assertEqual(observation.source, COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST)
        self.assertEqual(observation.confidence, "low")

    def test_classifies_host_only_smartess_endpoint(self) -> None:
        observation = collector_cloud_family_observation_from_endpoint("dtu_ess.eybond.com")

        self.assertEqual(observation.family, COLLECTOR_CLOUD_FAMILY_SMARTESS_AT)
        self.assertEqual(observation.source, COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST)
        self.assertEqual(observation.confidence, "low")

    def test_classifies_host_only_smartvalue_endpoint(self) -> None:
        observation = collector_cloud_family_observation_from_endpoint("m2m.eybond.com")

        self.assertEqual(observation.family, "smartvalue_at")
        self.assertEqual(observation.source, COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST)
        self.assertEqual(observation.confidence, "low")

    def test_classifies_host_only_valuecloud_endpoint(self) -> None:
        observation = collector_cloud_family_observation_from_endpoint("iot.eybond.com")

        self.assertEqual(observation.family, "valuecloud_at")
        self.assertEqual(observation.source, COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST)
        self.assertEqual(observation.confidence, "low")

    def test_explicit_known_host_wins_over_shared_18899_port(self) -> None:
        smartvalue = collector_cloud_family_observation_from_endpoint(
            "m2m.eybond.com,18899,TCP"
        )
        valuecloud = collector_cloud_family_observation_from_endpoint(
            "iot.eybond.com,18899,TCP"
        )

        self.assertEqual(smartvalue.family, "smartvalue_at")
        self.assertEqual(valuecloud.family, "valuecloud_at")
        self.assertEqual(smartvalue.source, COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST)
        self.assertEqual(valuecloud.source, COLLECTOR_CLOUD_FAMILY_SOURCE_ENDPOINT_HOST)
        self.assertEqual(smartvalue.confidence, "high")
        self.assertEqual(valuecloud.confidence, "high")

    def test_classifies_explicit_legacy_port(self) -> None:
        observation = collector_cloud_family_observation_from_endpoint("collector.local,502,TCP")

        self.assertEqual(observation.family, COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY)
        self.assertEqual(observation.source, COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT)
        self.assertEqual(observation.confidence, "medium")

    def test_classifies_explicit_smartess_ports(self) -> None:
        observation_18899 = collector_cloud_family_observation_from_endpoint("collector.local,18899,TCP")
        observation_38899 = collector_cloud_family_observation_from_endpoint("collector.local,38899,TCP")

        self.assertEqual(observation_18899.family, COLLECTOR_CLOUD_FAMILY_SMARTESS_AT)
        self.assertEqual(observation_38899.family, COLLECTOR_CLOUD_FAMILY_SMARTESS_AT)
        self.assertEqual(observation_18899.source, COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT)
        self.assertEqual(observation_38899.source, COLLECTOR_CLOUD_FAMILY_SOURCE_EXPLICIT_ENDPOINT_PORT)

    def test_unknown_endpoint_is_non_fatal(self) -> None:
        observation = collector_cloud_family_observation_from_endpoint("unknown.example")

        self.assertEqual(observation.family, COLLECTOR_CLOUD_FAMILY_UNKNOWN)
        self.assertEqual(observation.source, "")
        self.assertEqual(observation.confidence, "")

    def test_default_cloud_host_compatibility_wrapper(self) -> None:
        self.assertEqual(default_collector_cloud_host("legacy_binary"), "ess.eybond.com")
        self.assertEqual(default_collector_cloud_host("smartess_at"), "dtu_ess.eybond.com")
        self.assertEqual(default_collector_cloud_host("smartvalue_at"), "m2m.eybond.com")
        self.assertEqual(default_collector_cloud_host("valuecloud_at"), "iot.eybond.com")
        self.assertEqual(default_collector_cloud_host("unknown"), "")


if __name__ == "__main__":
    unittest.main()
