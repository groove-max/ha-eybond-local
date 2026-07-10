from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.collector_registry import (  # noqa: E402
    collector_registry_path,
    get_collector_registry_record,
    get_collector_registry_record_by_last_seen_ip,
    load_collector_registry,
    remember_collector_original_endpoint,
)


class CollectorRegistryTests(unittest.TestCase):
    def test_remember_and_load_collector_original_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)

            record = remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn="PN12345",
                original_endpoint_raw="ess.eybond.com",
                cloud_profile_key="legacy_binary",
                source="test",
                observed_at="2026-06-22T10:00:00+00:00",
                last_seen_ip="192.168.1.55",
            )

            self.assertEqual(record.collector_pn, "PN12345")
            self.assertEqual(record.original_endpoint_raw, "ess.eybond.com")
            self.assertEqual(collector_registry_path(config_dir).name, "eybond_local.collectors")

            loaded = get_collector_registry_record(
                config_dir=config_dir,
                collector_pn="PN12345",
            )
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.original_endpoint_raw, "ess.eybond.com")
            self.assertEqual(loaded.cloud_profile_key, "legacy_binary")
            self.assertEqual(loaded.source, "test")
            self.assertEqual(loaded.observed_at, "2026-06-22T10:00:00+00:00")
            self.assertEqual(loaded.last_seen_ip, "192.168.1.55")

    def test_existing_endpoint_is_sticky(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn="PN12345",
                original_endpoint_raw="ess.eybond.com",
                cloud_profile_key="legacy_binary",
            )

            record = remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn="PN12345",
                original_endpoint_raw="dtu_ess.eybond.com,18899,TCP",
                cloud_profile_key="smartess_at",
            )

            self.assertEqual(record.original_endpoint_raw, "ess.eybond.com")
            loaded = get_collector_registry_record(
                config_dir=config_dir,
                collector_pn="PN12345",
            )
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.original_endpoint_raw, "ess.eybond.com")
            self.assertEqual(loaded.cloud_profile_key, "legacy_binary")

    def test_existing_endpoint_keeps_endpoint_but_refreshes_last_seen_ip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn="PN12345",
                original_endpoint_raw="iot.eybond.com,18899,TCP",
                cloud_profile_key="valuecloud_at",
                last_seen_ip="192.168.8.110",
            )

            record = remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn="PN12345",
                original_endpoint_raw="dtu_ess.eybond.com,18899,TCP",
                cloud_profile_key="smartess_at",
                last_seen_ip="192.168.8.111",
            )

            self.assertEqual(record.original_endpoint_raw, "iot.eybond.com,18899,TCP")
            self.assertEqual(record.cloud_profile_key, "valuecloud_at")
            self.assertEqual(record.last_seen_ip, "192.168.8.111")

    def test_lookup_by_last_seen_ip_requires_unique_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn="PN12345",
                original_endpoint_raw="iot.eybond.com,18899,TCP",
                cloud_profile_key="valuecloud_at",
                last_seen_ip="192.168.8.110",
            )

            record = get_collector_registry_record_by_last_seen_ip(
                config_dir=config_dir,
                last_seen_ip="192.168.8.110",
            )

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.collector_pn, "PN12345")
            self.assertEqual(record.cloud_profile_key, "valuecloud_at")

    def test_lookup_by_last_seen_ip_fails_closed_when_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            for pn in ("PN12345", "PN67890"):
                remember_collector_original_endpoint(
                    config_dir=config_dir,
                    collector_pn=pn,
                    original_endpoint_raw="iot.eybond.com,18899,TCP",
                    cloud_profile_key="valuecloud_at",
                    last_seen_ip="192.168.8.110",
                )

            self.assertIsNone(
                get_collector_registry_record_by_last_seen_ip(
                    config_dir=config_dir,
                    last_seen_ip="192.168.8.110",
                )
            )

    def test_malformed_registry_loads_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            path = collector_registry_path(config_dir)
            path.parent.mkdir(parents=True)
            path.write_text("{not-json", encoding="utf-8")

            self.assertEqual(load_collector_registry(config_dir), {})

    def test_invalid_pn_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "collector_pn_invalid"):
                remember_collector_original_endpoint(
                    config_dir=Path(tmp),
                    collector_pn="../bad",
                    original_endpoint_raw="ess.eybond.com",
                )


if __name__ == "__main__":
    unittest.main()
