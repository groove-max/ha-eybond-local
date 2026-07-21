"""CP2B.2 Test A/D: the cloud rollback catalog adapter (writable profiles + builders)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.cloud_rollback_catalog import (  # noqa: E402
    CloudRollbackCatalogOption,
    cloud_rollback_selection_from_candidate,
    cloud_rollback_selection_from_catalog_key,
    cloud_rollback_selection_from_manual,
    ROLLBACK_SELECTION_INVALID,
    ROLLBACK_SELECTION_STALE,
    validate_cloud_rollback_selection,
    writable_cloud_rollback_catalog_options,
)
from custom_components.eybond_local.connection.strategy_transition_context import (  # noqa: E402
    CLOUD_PROVENANCE_EXPLICIT_USER,
    CLOUD_PROVENANCE_NONE,
    CLOUD_PROVENANCE_ORIGINAL,
    CloudRollbackEndpoint,
    ROLLBACK_SELECTION_CATALOG,
    ROLLBACK_SELECTION_CONFIRMED_CANDIDATE,
    ROLLBACK_SELECTION_MANUAL,
    CloudRollbackSelection,
)


class WritableCatalogOptions(unittest.TestCase):
    def test_lists_writable_profiles_with_stable_keys_and_valid_endpoints(self) -> None:
        options = writable_cloud_rollback_catalog_options()
        self.assertTrue(options)
        keys = [o.key for o in options]
        self.assertEqual(len(keys), len(set(keys)), "keys must be stable/unique")
        for option in options:
            self.assertIsInstance(option, CloudRollbackCatalogOption)
            self.assertTrue(option.key)
            self.assertTrue(option.label)
            # Every offered endpoint is a valid writable rollback target.
            CloudRollbackEndpoint(option.endpoint, CLOUD_PROVENANCE_EXPLICIT_USER)

    def test_offers_a_host_only_write_format_profile(self) -> None:
        # The catalog contains a host_only profile (legacy_binary): its endpoint
        # is offered host-only, not expanded to a guessed port/protocol.
        endpoints = {o.key: o.endpoint for o in writable_cloud_rollback_catalog_options()}
        self.assertIn("legacy_binary", endpoints)
        self.assertNotIn(",", endpoints["legacy_binary"])  # host-only


class CatalogKeySelection(unittest.TestCase):
    def test_valid_key_builds_catalog_selection(self) -> None:
        key = writable_cloud_rollback_catalog_options()[0].key
        selection = cloud_rollback_selection_from_catalog_key(key)
        self.assertIsNotNone(selection)
        self.assertEqual(selection.selection_kind, ROLLBACK_SELECTION_CATALOG)
        self.assertEqual(selection.catalog_profile_key, key)
        self.assertEqual(selection.endpoint.provenance, CLOUD_PROVENANCE_EXPLICIT_USER)

    def test_stale_and_non_string_keys_fail_closed(self) -> None:
        for bad in ("___removed___", "", 123, None, object()):
            self.assertIsNone(cloud_rollback_selection_from_catalog_key(bad))


class ManualSelection(unittest.TestCase):
    def test_all_shapes_accepted_and_preserved(self) -> None:
        for raw, expect in (
            ("cloud.example", "cloud.example"),
            ("cloud.example,18899", "cloud.example,18899"),
            ("cloud.example,18899,TCP", "cloud.example,18899,TCP"),
            ("  cloud.example,18899,TCP  ", "cloud.example,18899,TCP"),
        ):
            s = cloud_rollback_selection_from_manual(raw)
            self.assertIsNotNone(s, raw)
            self.assertEqual(s.selection_kind, ROLLBACK_SELECTION_MANUAL)
            self.assertEqual(s.endpoint_value, expect)

    def test_malformed_and_wildcard_and_non_string_fail_closed(self) -> None:
        for bad in ("bad###ep", "0.0.0.0,18899,TCP", "", "   ", 123, None, object()):
            self.assertIsNone(cloud_rollback_selection_from_manual(bad))


class CandidateSelection(unittest.TestCase):
    def test_resolver_candidate_becomes_confirmed_selection(self) -> None:
        endpoint = CloudRollbackEndpoint("ess.eybond.com,18899,TCP", CLOUD_PROVENANCE_ORIGINAL)
        s = cloud_rollback_selection_from_candidate(endpoint)
        self.assertIsNotNone(s)
        self.assertEqual(s.selection_kind, ROLLBACK_SELECTION_CONFIRMED_CANDIDATE)
        self.assertEqual(s.candidate_provenance, CLOUD_PROVENANCE_ORIGINAL)

    def test_explicit_user_and_none_and_duck_rejected(self) -> None:
        self.assertIsNone(
            cloud_rollback_selection_from_candidate(
                CloudRollbackEndpoint("x.y,1,TCP", CLOUD_PROVENANCE_EXPLICIT_USER)
            )
        )
        self.assertIsNone(cloud_rollback_selection_from_candidate(CloudRollbackEndpoint.none()))
        self.assertIsNone(cloud_rollback_selection_from_candidate("ess.eybond.com,18899,TCP"))
        self.assertIsNone(cloud_rollback_selection_from_candidate(None))


class AuthorityRevalidation(unittest.TestCase):
    def test_catalog_key_cannot_be_paired_with_a_foreign_endpoint(self) -> None:
        key = writable_cloud_rollback_catalog_options()[0].key
        forged = CloudRollbackSelection(
            endpoint=CloudRollbackEndpoint(
                "foreign.example,18899,TCP", CLOUD_PROVENANCE_EXPLICIT_USER
            ),
            selection_kind=ROLLBACK_SELECTION_CATALOG,
            catalog_profile_key=key,
            user_confirmed=True,
        )
        self.assertEqual(
            validate_cloud_rollback_selection(forged),
            ROLLBACK_SELECTION_INVALID,
        )

    def test_confirmed_candidate_change_is_stale_not_substituted(self) -> None:
        shown = CloudRollbackEndpoint(
            "shown.example,18899,TCP", CLOUD_PROVENANCE_ORIGINAL
        )
        changed = CloudRollbackEndpoint(
            "changed.example,18899,TCP", CLOUD_PROVENANCE_ORIGINAL
        )
        selection = cloud_rollback_selection_from_candidate(shown)
        self.assertEqual(
            validate_cloud_rollback_selection(
                selection, confirmed_candidate=changed
            ),
            ROLLBACK_SELECTION_STALE,
        )


if __name__ == "__main__":
    unittest.main()
