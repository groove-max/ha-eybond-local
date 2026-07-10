from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.collector_cloud_profile_catalog_loader import (  # noqa: E402
    load_collector_cloud_profile_catalog,
    resolve_collector_cloud_default_port,
    resolve_collector_cloud_default_protocol,
    resolve_collector_cloud_endpoint_write_format,
    resolve_collector_cloud_identity_strategy,
    resolve_collector_cloud_provider,
    resolve_collector_cloud_raw_passthrough_bootstrap,
    resolve_collector_cloud_raw_passthrough_frame_format,
    resolve_collector_cloud_session_protocol,
    resolve_collector_cloud_default_host,
    resolve_collector_cloud_family_by_host,
    resolve_collector_cloud_family_by_port,
)


class CollectorCloudProfileCatalogLoaderTests(unittest.TestCase):
    def test_loads_known_profile_families(self) -> None:
        catalog = load_collector_cloud_profile_catalog()

        self.assertEqual(
            set(catalog.profiles),
            {"legacy_binary", "smartess_at", "smartvalue_at", "valuecloud_at"},
        )

    def test_loads_legacy_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        legacy = catalog.profiles["legacy_binary"]

        self.assertEqual(legacy.default_host, "ess.eybond.com")
        self.assertEqual(legacy.provider, "smartess")
        self.assertEqual(legacy.label, "SmartESS legacy ESS")
        self.assertEqual(legacy.default_port, 502)
        self.assertEqual(legacy.default_protocol, "TCP")
        self.assertEqual(legacy.known_hosts, ("ess.eybond.com",))
        self.assertEqual(legacy.known_ports, (502,))
        self.assertEqual(legacy.endpoint_write_format, "host_only")
        self.assertEqual(legacy.session_protocol, "eybond_framed")
        self.assertEqual(legacy.identity_strategy, "framed_heartbeat_then_fc2_pn")

    def test_loads_smartess_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        smartess = catalog.profiles["smartess_at"]

        self.assertEqual(smartess.default_host, "dtu_ess.eybond.com")
        self.assertEqual(smartess.provider, "smartess")
        self.assertEqual(smartess.label, "SmartESS DTU ESS")
        self.assertEqual(smartess.default_port, 18899)
        self.assertEqual(smartess.default_protocol, "TCP")
        self.assertEqual(smartess.known_hosts, ("dtu_ess.eybond.com",))
        self.assertEqual(smartess.known_ports, (18899, 38899))
        self.assertEqual(smartess.endpoint_write_format, "host_port_protocol")
        self.assertEqual(smartess.session_protocol, "at_text")
        self.assertEqual(smartess.identity_strategy, "at_dtupn")
        self.assertEqual(smartess.raw_passthrough_bootstrap, "uart_write_same_value")
        self.assertEqual(smartess.raw_passthrough_frame_format, "transparent")

    def test_loads_smartvalue_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        smartvalue = catalog.profiles["smartvalue_at"]

        self.assertEqual(smartvalue.default_host, "m2m.eybond.com")
        self.assertEqual(smartvalue.provider, "smartvalue")
        self.assertEqual(smartvalue.label, "SmartValue AT")
        self.assertEqual(smartvalue.default_port, 18899)
        self.assertEqual(smartvalue.default_protocol, "TCP")
        self.assertEqual(smartvalue.known_hosts, ("m2m.eybond.com",))
        self.assertEqual(smartvalue.known_ports, ())
        self.assertEqual(smartvalue.endpoint_write_format, "host_port_protocol")
        self.assertEqual(smartvalue.session_protocol, "at_text")
        self.assertEqual(smartvalue.identity_strategy, "at_dtupn")
        self.assertEqual(smartvalue.raw_passthrough_bootstrap, "uart_write_same_value")
        self.assertEqual(smartvalue.raw_passthrough_frame_format, "transparent")

    def test_loads_valuecloud_profile_details(self) -> None:
        catalog = load_collector_cloud_profile_catalog()
        valuecloud = catalog.profiles["valuecloud_at"]

        self.assertEqual(valuecloud.default_host, "iot.eybond.com")
        self.assertEqual(valuecloud.provider, "valuecloud")
        self.assertEqual(valuecloud.label, "SmartValue iot.eybond.com AT")
        self.assertEqual(valuecloud.default_port, 18899)
        self.assertEqual(valuecloud.default_protocol, "TCP")
        self.assertEqual(valuecloud.known_hosts, ("iot.eybond.com",))
        self.assertEqual(valuecloud.known_ports, ())
        self.assertEqual(valuecloud.endpoint_write_format, "host_port_protocol")
        self.assertEqual(valuecloud.session_protocol, "at_text")
        self.assertEqual(valuecloud.identity_strategy, "at_dtupn")
        self.assertEqual(valuecloud.raw_passthrough_bootstrap, "none")
        self.assertEqual(valuecloud.raw_passthrough_frame_format, "plain_line")
        self.assertEqual(valuecloud.raw_passthrough_min_interval_ms, 0)

    def test_resolves_known_families_by_host(self) -> None:
        self.assertEqual(resolve_collector_cloud_family_by_host("ess.eybond.com"), "legacy_binary")
        self.assertEqual(resolve_collector_cloud_family_by_host("DTU_ESS.EYBOND.COM"), "smartess_at")
        self.assertEqual(resolve_collector_cloud_family_by_host("M2M.EYBOND.COM"), "smartvalue_at")
        self.assertEqual(resolve_collector_cloud_family_by_host("IOT.EYBOND.COM"), "valuecloud_at")

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
        self.assertEqual(resolve_collector_cloud_default_port("unknown"), 0)
        self.assertEqual(resolve_collector_cloud_default_protocol("unknown"), "")
        self.assertEqual(resolve_collector_cloud_endpoint_write_format("unknown"), "")
        self.assertEqual(resolve_collector_cloud_session_protocol("unknown"), "")
        self.assertEqual(resolve_collector_cloud_identity_strategy("unknown"), "")
        self.assertEqual(resolve_collector_cloud_provider("unknown"), "")
        self.assertEqual(resolve_collector_cloud_raw_passthrough_bootstrap("unknown"), "")
        self.assertEqual(resolve_collector_cloud_raw_passthrough_frame_format("unknown"), "")

    def test_resolves_known_providers(self) -> None:
        self.assertEqual(resolve_collector_cloud_provider("legacy_binary"), "smartess")
        self.assertEqual(resolve_collector_cloud_provider("SMARTESS_AT"), "smartess")
        self.assertEqual(resolve_collector_cloud_provider("smartvalue_at"), "smartvalue")
        self.assertEqual(resolve_collector_cloud_provider("valuecloud_at"), "valuecloud")

    def test_resolves_known_default_hosts(self) -> None:
        self.assertEqual(resolve_collector_cloud_default_host("legacy_binary"), "ess.eybond.com")
        self.assertEqual(resolve_collector_cloud_default_host("SMARTESS_AT"), "dtu_ess.eybond.com")
        self.assertEqual(resolve_collector_cloud_default_host("smartvalue_at"), "m2m.eybond.com")
        self.assertEqual(resolve_collector_cloud_default_host("valuecloud_at"), "iot.eybond.com")

    def test_resolves_known_default_ports_protocols_and_write_formats(self) -> None:
        self.assertEqual(resolve_collector_cloud_default_port("legacy_binary"), 502)
        self.assertEqual(resolve_collector_cloud_default_port("SMARTESS_AT"), 18899)
        self.assertEqual(resolve_collector_cloud_default_port("smartvalue_at"), 18899)
        self.assertEqual(resolve_collector_cloud_default_port("valuecloud_at"), 18899)
        self.assertEqual(resolve_collector_cloud_default_protocol("legacy_binary"), "TCP")
        self.assertEqual(resolve_collector_cloud_default_protocol("smartess_at"), "TCP")
        self.assertEqual(
            resolve_collector_cloud_endpoint_write_format("legacy_binary"),
            "host_only",
        )
        self.assertEqual(
            resolve_collector_cloud_endpoint_write_format("smartess_at"),
            "host_port_protocol",
        )
        self.assertEqual(
            resolve_collector_cloud_session_protocol("legacy_binary"),
            "eybond_framed",
        )
        self.assertEqual(
            resolve_collector_cloud_session_protocol("smartess_at"),
            "at_text",
        )
        self.assertEqual(
            resolve_collector_cloud_identity_strategy("legacy_binary"),
            "framed_heartbeat_then_fc2_pn",
        )
        self.assertEqual(
            resolve_collector_cloud_identity_strategy("smartess_at"),
            "at_dtupn",
        )


if __name__ == "__main__":
    unittest.main()
