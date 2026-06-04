from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.collector_cloud_profile_catalog_loader import (  # noqa: E402
    load_collector_cloud_profile_catalog,
    resolve_collector_cloud_default_host,
    resolve_collector_cloud_family_by_host,
    resolve_collector_cloud_family_by_port,
)


class CollectorCloudProfileCatalogLoaderTests(unittest.TestCase):
    def test_loads_known_profile_families(self) -> None:
        catalog = load_collector_cloud_profile_catalog()

        self.assertEqual(
            set(catalog.profiles),
            {"legacy_binary", "smartess_at", "smartvalue_at"},
        )

    def test_loads_legacy_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        legacy = catalog.profiles["legacy_binary"]

        self.assertEqual(legacy.default_host, "ess.eybond.com")
        self.assertEqual(legacy.known_hosts, ("ess.eybond.com",))
        self.assertEqual(legacy.known_ports, (502,))

    def test_loads_smartess_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        smartess = catalog.profiles["smartess_at"]

        self.assertEqual(smartess.default_host, "dtu_ess.eybond.com")
        self.assertEqual(smartess.known_hosts, ("dtu_ess.eybond.com",))
        self.assertEqual(smartess.known_ports, (18899, 38899))

    def test_loads_smartvalue_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        smartvalue = catalog.profiles["smartvalue_at"]

        self.assertEqual(smartvalue.default_host, "m2m.eybond.com")
        self.assertEqual(smartvalue.known_hosts, ("m2m.eybond.com",))
        self.assertEqual(smartvalue.known_ports, ())

    def test_resolves_known_families_by_host(self) -> None:
        self.assertEqual(resolve_collector_cloud_family_by_host("ess.eybond.com"), "legacy_binary")
        self.assertEqual(resolve_collector_cloud_family_by_host("DTU_ESS.EYBOND.COM"), "smartess_at")
        self.assertEqual(resolve_collector_cloud_family_by_host("M2M.EYBOND.COM"), "smartvalue_at")

    def test_resolves_known_families_by_port(self) -> None:
        self.assertEqual(resolve_collector_cloud_family_by_port(502), "legacy_binary")
        self.assertEqual(resolve_collector_cloud_family_by_port("18899"), "smartess_at")
        self.assertEqual(resolve_collector_cloud_family_by_port(38899), "smartess_at")

    def test_unknown_values_are_safe(self) -> None:
        self.assertEqual(resolve_collector_cloud_family_by_host("unknown.example"), "")
        self.assertEqual(resolve_collector_cloud_family_by_port(65535), "")
        self.assertEqual(resolve_collector_cloud_family_by_port("not-a-port"), "")
        self.assertEqual(resolve_collector_cloud_default_host(""), "")
        self.assertEqual(resolve_collector_cloud_default_host("unknown"), "")

    def test_resolves_known_default_hosts(self) -> None:
        self.assertEqual(resolve_collector_cloud_default_host("legacy_binary"), "ess.eybond.com")
        self.assertEqual(resolve_collector_cloud_default_host("SMARTESS_AT"), "dtu_ess.eybond.com")
        self.assertEqual(resolve_collector_cloud_default_host("smartvalue_at"), "m2m.eybond.com")


if __name__ == "__main__":
    unittest.main()
