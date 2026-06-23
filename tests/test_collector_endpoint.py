from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector_endpoint import (  # noqa: E402
    default_collector_server_port,
    default_collector_server_protocol,
    format_collector_server_endpoint_for_cloud_profile,
    format_collector_server_endpoint,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint,
    parse_collector_server_endpoint,
    resolve_collector_server_endpoint,
)
from custom_components.eybond_local.metadata.collector_cloud_profile_catalog_loader import (  # noqa: E402
    resolve_collector_cloud_family_by_host,
)


class CollectorEndpointTests(unittest.TestCase):
    def test_default_port_is_catalog_backed_with_compatibility_fallback(self) -> None:
        self.assertEqual(default_collector_server_port(cloud_family="legacy_binary"), 502)
        self.assertEqual(default_collector_server_port(cloud_family="smartess_at"), 18899)
        self.assertEqual(default_collector_server_port(cloud_family="SMARTESS_AT"), 18899)
        self.assertEqual(default_collector_server_port(cloud_family="smartvalue_at"), 18899)
        self.assertEqual(default_collector_server_port(cloud_family="unknown_family"), 18899)
        self.assertEqual(default_collector_server_protocol(cloud_family="legacy_binary"), "TCP")
        self.assertEqual(default_collector_server_protocol(cloud_family="smartess_at"), "TCP")
        self.assertEqual(default_collector_server_protocol(cloud_family="unknown_family"), "TCP")

    def test_format_requires_ipv4_or_hostname_and_tcp(self) -> None:
        self.assertEqual(
            format_collector_server_endpoint(
                server_host="collector.example",
                server_port=18899,
                server_protocol="tcp",
                require_tcp=True,
            ),
            "collector.example,18899,TCP",
        )

        with self.assertRaisesRegex(ValueError, "collector_server_host_invalid"):
            format_collector_server_endpoint(
                server_host="http://bad-host",
                server_port=18899,
                server_protocol="TCP",
                require_tcp=True,
            )

        with self.assertRaisesRegex(ValueError, "collector_server_protocol_tcp_required"):
            format_collector_server_endpoint(
                server_host="collector.example",
                server_port=18899,
                server_protocol="UDP",
                require_tcp=True,
            )

    def test_parse_can_require_explicit_protocol(self) -> None:
        self.assertEqual(
            parse_collector_server_endpoint(
                "10.0.0.25,18899,TCP",
                require_explicit_port=True,
                require_explicit_protocol=True,
                require_tcp=True,
            ),
            ("10.0.0.25", 18899, "TCP"),
        )

        with self.assertRaisesRegex(ValueError, "collector_server_endpoint_invalid"):
            parse_collector_server_endpoint(
                "10.0.0.25,18899",
                require_explicit_port=True,
                require_explicit_protocol=True,
                require_tcp=True,
            )

        self.assertEqual(
            parse_collector_server_endpoint(
                "collector-cloud.smartess.example,18899,TCP",
                require_explicit_port=True,
                require_explicit_protocol=True,
                require_tcp=True,
            ),
            ("collector-cloud.smartess.example", 18899, "TCP"),
        )

    def test_format_uses_cloud_profile_endpoint_shape(self) -> None:
        self.assertEqual(
            format_collector_server_endpoint_for_cloud_profile(
                server_host="192.168.1.50",
                cloud_family="legacy_binary",
                require_tcp=True,
            ),
            "192.168.1.50",
        )
        self.assertEqual(
            format_collector_server_endpoint_for_cloud_profile(
                server_host="192.168.1.50",
                cloud_family="smartess_at",
                require_tcp=True,
            ),
            "192.168.1.50,18899,TCP",
        )
        self.assertEqual(
            format_collector_server_endpoint_for_cloud_profile(
                server_host="192.168.1.50",
                cloud_family="smartvalue_at",
                require_tcp=True,
            ),
            "192.168.1.50,18899,TCP",
        )

    def test_format_can_infer_legacy_shape_from_template_endpoint(self) -> None:
        self.assertEqual(
            format_collector_server_endpoint_for_cloud_profile(
                server_host="192.168.1.50",
                template_endpoint="ess.eybond.com",
                require_tcp=True,
            ),
            "192.168.1.50",
        )

    def test_format_unknown_profile_preserves_template_shape(self) -> None:
        self.assertEqual(
            format_collector_server_endpoint_for_cloud_profile(
                server_host="192.168.1.50",
                template_endpoint="custom.example,38899",
                require_tcp=True,
            ),
            "192.168.1.50,38899",
        )
        self.assertEqual(
            format_collector_server_endpoint_for_cloud_profile(
                server_host="192.168.1.50",
                require_tcp=True,
            ),
            "192.168.1.50,18899,TCP",
        )

    def test_parse_can_default_host_only_endpoint_to_legacy_cloud_port(self) -> None:
        self.assertEqual(
            parse_collector_server_endpoint(
                "ess.eybond.com",
                require_explicit_port=False,
                require_explicit_protocol=False,
                require_tcp=True,
            ),
            ("ess.eybond.com", 502, "TCP"),
        )

        parsed = inspect_collector_server_endpoint(
            "ess.eybond.com",
            require_explicit_port=False,
            require_explicit_protocol=False,
            require_tcp=True,
        )
        self.assertFalse(parsed.has_explicit_port)
        self.assertFalse(parsed.has_explicit_protocol)
        self.assertEqual(parsed.render(preserve_shape=True), "ess.eybond.com")
        self.assertEqual(parsed.render(preserve_shape=False), "ess.eybond.com,502,TCP")

    def test_parse_preserves_smartvalue_endpoint_shape_for_host_based_family_resolution(self) -> None:
        # APK-derived endpoint evidence indicates SmartValue AT collectors use
        # CLDSRVHOST1 / parameter 21 endpoint semantics with m2m.eybond.com.
        parsed = inspect_collector_server_endpoint(
            "m2m.eybond.com,18899,TCP",
            require_explicit_port=True,
            require_explicit_protocol=True,
            require_tcp=True,
        )

        self.assertEqual(parsed.host, "m2m.eybond.com")
        self.assertEqual(parsed.port, 18899)
        self.assertEqual(parsed.protocol, "TCP")
        self.assertEqual(resolve_collector_cloud_family_by_host(parsed.host), "smartvalue_at")

    def test_parse_preserves_valuecloud_endpoint_shape_for_host_based_family_resolution(self) -> None:
        parsed = inspect_collector_server_endpoint(
            "iot.eybond.com,18899,TCP",
            require_explicit_port=True,
            require_explicit_protocol=True,
            require_tcp=True,
        )

        self.assertEqual(parsed.host, "iot.eybond.com")
        self.assertEqual(parsed.port, 18899)
        self.assertEqual(parsed.protocol, "TCP")
        self.assertEqual(resolve_collector_cloud_family_by_host(parsed.host), "valuecloud_at")

    def test_resolve_uses_family_default_for_host_only_legacy_endpoint(self) -> None:
        self.assertEqual(default_collector_server_port(cloud_family="legacy_binary"), 502)
        self.assertEqual(default_collector_server_port(cloud_family="smartess_at"), 18899)
        self.assertEqual(
            resolve_collector_server_endpoint(
                "ess.eybond.com",
                require_explicit_port=False,
                require_explicit_protocol=False,
                require_tcp=True,
                cloud_family="legacy_binary",
            ),
            ("ess.eybond.com", 502, "TCP"),
        )
        self.assertEqual(
            resolve_collector_server_endpoint(
                "ess.eybond.com,18899,TCP",
                require_explicit_port=False,
                require_explicit_protocol=False,
                require_tcp=True,
                cloud_family="legacy_binary",
            ),
            ("ess.eybond.com", 18899, "TCP"),
        )

    def test_normalize_can_preserve_compact_endpoint_shape(self) -> None:
        self.assertEqual(
            normalize_collector_server_endpoint(
                "ess.eybond.com",
                require_explicit_port=False,
                require_explicit_protocol=False,
                require_tcp=True,
                preserve_shape=True,
            ),
            "ess.eybond.com",
        )
        self.assertEqual(
            normalize_collector_server_endpoint(
                "collector-cloud.smartess.example,18899",
                require_explicit_port=True,
                require_explicit_protocol=False,
                require_tcp=True,
                preserve_shape=True,
            ),
            "collector-cloud.smartess.example,18899",
        )

    def test_normalize_preserves_existing_non_tcp_runtime_values(self) -> None:
        self.assertEqual(
            normalize_collector_server_endpoint(
                "legacy.example,18899,UDP",
                require_explicit_port=True,
                require_explicit_protocol=True,
                require_tcp=False,
            ),
            "legacy.example,18899,UDP",
        )


if __name__ == "__main__":
    unittest.main()
