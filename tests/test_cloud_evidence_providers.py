"""Provider-isolation tests for the neutral cloud-evidence contract.

Proves that selecting one provider runs ONLY that provider's code, that an
unsupported provider fails closed, that credentials are never stored on a
provider or exposed in diagnostics, and that provider evidence resolves into
normalized (provider-neutral) draft candidates.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support import cloud_evidence_providers as providers  # noqa: E402
from custom_components.eybond_local.support.cloud_evidence_providers import (  # noqa: E402
    DRAFT_KIND_KNOWN_FAMILY,
    DRAFT_KIND_SMG_BRIDGE,
    CloudEvidenceContext,
    SmartEssCloudEvidenceProvider,
    UnavailableCloudEvidenceProvider,
    ValueCloudCloudEvidenceProvider,
    cloud_evidence_provider_supported,
    resolve_cloud_evidence_provider,
    supported_cloud_evidence_providers,
)
from custom_components.eybond_local.support.cloud_evidence import CloudEvidenceRecord  # noqa: E402
from custom_components.eybond_local.smartess_cloud import SmartEssCloudError  # noqa: E402


def _context(**overrides) -> CloudEvidenceContext:
    base = dict(
        config_dir=Path("/tmp/eybond-test"),
        entry_id="entry-1",
        collector_pn="E5000020000000",
    )
    base.update(overrides)
    return CloudEvidenceContext(**base)


class RegistryTests(unittest.TestCase):
    def test_registry_resolves_known_providers(self) -> None:
        self.assertIsInstance(resolve_cloud_evidence_provider("smartess"), SmartEssCloudEvidenceProvider)
        self.assertIsInstance(resolve_cloud_evidence_provider("valuecloud"), ValueCloudCloudEvidenceProvider)

    def test_registry_fails_closed_for_unknown(self) -> None:
        impl = resolve_cloud_evidence_provider("nope")
        self.assertIsInstance(impl, UnavailableCloudEvidenceProvider)
        self.assertFalse(impl.export_available(_context()))
        self.assertIsNone(impl.load_latest(_context()))
        self.assertEqual(impl.resolve_draft_candidates(_context(), None), ())
        with self.assertRaisesRegex(RuntimeError, "cloud_evidence_provider_not_supported"):
            impl.export(_context(), username="u", password="p")

    def test_supported_set_and_membership(self) -> None:
        self.assertEqual(supported_cloud_evidence_providers(), ("smartess", "valuecloud"))
        self.assertTrue(cloud_evidence_provider_supported("smartess"))
        self.assertFalse(cloud_evidence_provider_supported("nope"))
        self.assertFalse(cloud_evidence_provider_supported(""))


class ProviderIsolationTests(unittest.TestCase):
    def test_control_discovery_classifier_accepts_only_provider_owned_errors(self) -> None:
        provider = SmartEssCloudEvidenceProvider()

        self.assertEqual(
            provider.classify_control_discovery_error(
                SmartEssCloudError("network_error:timed out")
            ),
            "timeout",
        )
        self.assertEqual(
            provider.classify_control_discovery_error(TimeoutError()),
            "timeout",
        )
        self.assertEqual(
            provider.classify_control_discovery_error(
                RuntimeError("runtime_route_failed")
            ),
            "",
        )

    def test_smartess_export_runs_only_smartess_code(self) -> None:
        record = CloudEvidenceRecord(path=Path("/tmp/x.json"), payload={"provider": "smartess"})
        with patch.object(
            providers, "fetch_and_export_smartess_device_bundle_cloud_evidence", return_value=record
        ) as smartess_fetch, patch.object(
            providers, "fetch_and_export_valuecloud_device_bundle_cloud_evidence"
        ) as valuecloud_fetch:
            out = SmartEssCloudEvidenceProvider().export(_context(), username="u", password="p")
        self.assertIs(out, record)
        smartess_fetch.assert_called_once()
        valuecloud_fetch.assert_not_called()
        # Provider-specific source label + collector PN are passed; creds are used
        # for the fetch only.
        self.assertEqual(smartess_fetch.call_args.kwargs["source"], "smartess_cloud_diagnostics")
        self.assertEqual(smartess_fetch.call_args.kwargs["collector_pn"], "E5000020000000")

    def test_valuecloud_export_runs_only_valuecloud_code(self) -> None:
        record = CloudEvidenceRecord(path=Path("/tmp/x.json"), payload={"provider": "valuecloud"})
        with patch.object(
            providers, "fetch_and_export_valuecloud_device_bundle_cloud_evidence", return_value=record
        ) as valuecloud_fetch, patch.object(
            providers, "fetch_and_export_smartess_device_bundle_cloud_evidence"
        ) as smartess_fetch:
            out = ValueCloudCloudEvidenceProvider().export(
                _context(), username="u", password="p"
            )
        self.assertIs(out, record)
        valuecloud_fetch.assert_called_once()
        smartess_fetch.assert_not_called()
        self.assertEqual(valuecloud_fetch.call_args.kwargs["source"], "valuecloud_cloud_diagnostics")


class CredentialSafetyTests(unittest.TestCase):
    def test_provider_stores_no_credentials(self) -> None:
        provider = SmartEssCloudEvidenceProvider()
        record = CloudEvidenceRecord(path=Path("/tmp/x.json"), payload={})
        with patch.object(
            providers, "fetch_and_export_smartess_device_bundle_cloud_evidence", return_value=record
        ):
            provider.export(_context(), username="secretuser", password="secretpass")
        # The provider is stateless: no attribute retains the credentials.
        blob = repr(vars(provider))
        self.assertNotIn("secretuser", blob)
        self.assertNotIn("secretpass", blob)

    def test_diagnostics_carry_no_credentials(self) -> None:
        for provider in (SmartEssCloudEvidenceProvider(), ValueCloudCloudEvidenceProvider()):
            diag = provider.diagnostics(_context())
            text = str(diag).lower()
            self.assertNotIn("password", text)
            self.assertNotIn("username", text)
            self.assertIn("provider", diag)
            self.assertIn("export_available", diag)


class DraftCandidateNormalizationTests(unittest.TestCase):
    def test_smartess_resolves_neutral_candidates(self) -> None:
        class _KnownPlan:
            reason = "known_family_0925"

        class _SmgPlan:
            reason = "smg_bridge_match"
            bridge_label = "SMG 6200"

        with patch.object(
            providers, "resolve_smartess_known_family_draft_plan", return_value=_KnownPlan()
        ), patch.object(
            providers, "resolve_smartess_smg_bridge_plan", return_value=_SmgPlan()
        ):
            record = CloudEvidenceRecord(
                path=Path("/tmp/x.json"), payload={"provider": "smartess", "fields": []}
            )
            candidates = SmartEssCloudEvidenceProvider().resolve_draft_candidates(_context(), record)

        kinds = {candidate.kind for candidate in candidates}
        self.assertEqual(kinds, {DRAFT_KIND_KNOWN_FAMILY, DRAFT_KIND_SMG_BRIDGE})
        smg = next(c for c in candidates if c.kind == DRAFT_KIND_SMG_BRIDGE)
        self.assertEqual(smg.label, "SMG 6200")
        self.assertIsInstance(smg.plan, _SmgPlan)  # opaque provider plan carried through

    def test_smartess_create_draft_dispatches_by_kind(self) -> None:
        provider = SmartEssCloudEvidenceProvider()
        record = CloudEvidenceRecord(path=Path("/tmp/x.json"), payload={"provider": "smartess"})
        candidate = providers.CloudEvidenceDraftCandidate(
            kind=DRAFT_KIND_SMG_BRIDGE, label="l", reason="r", plan=object()
        )
        with patch.object(
            providers, "create_smartess_smg_bridge_draft", return_value=("p.json", "s.json")
        ) as smg_create, patch.object(
            providers, "create_smartess_known_family_draft"
        ) as known_create:
            out = provider.create_draft(
                _context(), record, candidate,
                output_profile_name=None, output_schema_name=None, overwrite=True,
            )
        self.assertEqual(out, ("p.json", "s.json"))
        smg_create.assert_called_once()
        known_create.assert_not_called()

    def test_valuecloud_has_no_draft_candidates(self) -> None:
        record = CloudEvidenceRecord(path=Path("/tmp/x.json"), payload={})
        provider = ValueCloudCloudEvidenceProvider()
        self.assertEqual(provider.resolve_draft_candidates(_context(), record), ())
        with self.assertRaisesRegex(RuntimeError, "cloud_evidence_draft_not_supported"):
            provider.create_draft(
                _context(), record,
                providers.CloudEvidenceDraftCandidate(kind="x", label="", reason="", plan=None),
                output_profile_name=None, output_schema_name=None, overwrite=True,
            )


class RecordOwnershipTests(unittest.TestCase):
    def _rec(self, **payload):
        return CloudEvidenceRecord(path=Path("/tmp/x.json"), payload=dict(payload))

    def test_owns_record_only_for_own_provenance(self) -> None:
        smartess = SmartEssCloudEvidenceProvider()
        self.assertTrue(smartess.owns_record(self._rec(provider="smartess")))
        self.assertTrue(smartess.owns_record(None))  # absent, not foreign
        self.assertFalse(smartess.owns_record(self._rec(provider="valuecloud")))
        self.assertFalse(smartess.owns_record(self._rec(source="mystery")))  # unknown
        # Contradictory: explicit unknown + smartess source.
        self.assertFalse(
            smartess.owns_record(
                self._rec(provider="unknown", source="smartess_cloud_diagnostics")
            )
        )

    def test_resolve_draft_candidates_refuses_foreign_record(self) -> None:
        smartess = SmartEssCloudEvidenceProvider()
        with patch.object(
            providers, "resolve_smartess_known_family_draft_plan", return_value=object()
        ):
            # A ValueCloud record must never be interpreted by SmartESS.
            self.assertEqual(
                smartess.resolve_draft_candidates(
                    _context(), self._rec(provider="valuecloud")
                ),
                (),
            )
            # Unknown-provenance record is also refused.
            self.assertEqual(
                smartess.resolve_draft_candidates(_context(), self._rec(source="mystery")),
                (),
            )

    def test_create_draft_rejects_foreign_and_missing_record(self) -> None:
        smartess = SmartEssCloudEvidenceProvider()
        candidate = providers.CloudEvidenceDraftCandidate(
            kind=DRAFT_KIND_KNOWN_FAMILY, label="l", reason="r", plan=object()
        )
        with self.assertRaisesRegex(RuntimeError, "cloud_evidence_record_not_owned"):
            smartess.create_draft(
                _context(), self._rec(provider="valuecloud"), candidate,
                output_profile_name=None, output_schema_name=None, overwrite=True,
            )
        with self.assertRaisesRegex(RuntimeError, "cloud_evidence_record_not_available"):
            smartess.create_draft(
                _context(), None, candidate,
                output_profile_name=None, output_schema_name=None, overwrite=True,
            )


class OnboardingAssistTests(unittest.TestCase):
    def test_smartess_build_onboarding_assist_returns_normalized_dto(self) -> None:
        record = CloudEvidenceRecord(
            path=Path("/tmp/onboarding.json"),
            payload={
                "summary": {"settings_field_count": 39, "settings_mapped_field_count": 28},
                "device_identity": {"pn": "E1", "sn": "S1"},
                "payload": {"normalized": {}},
            },
        )
        with patch.object(
            providers, "fetch_and_export_smartess_device_bundle_cloud_evidence",
            return_value=record,
        ) as smartess_fetch, patch.object(
            providers, "resolve_smartess_known_family_draft_plan", return_value=None
        ):
            assist = SmartEssCloudEvidenceProvider().build_onboarding_assist(
                _context(collector_pn="E1"), username="u", password="p"
            )
        self.assertEqual(assist.collector_pn, "E1")
        self.assertEqual(assist.evidence_path, "/tmp/onboarding.json")
        self.assertEqual(assist.device_pn, "E1")
        self.assertEqual(assist.total_field_count, 39)
        # Onboarding uses its own source; still only the SmartESS fetch runs.
        self.assertEqual(smartess_fetch.call_args.kwargs["source"], "smartess_cloud_onboarding")

    def test_valuecloud_onboarding_assist_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "cloud_evidence_onboarding_assist_not_supported"):
            ValueCloudCloudEvidenceProvider().build_onboarding_assist(
                _context(), username="u", password="p"
            )

    def test_unknown_provider_onboarding_assist_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "cloud_evidence_onboarding_assist_not_supported"):
            resolve_cloud_evidence_provider("nope").build_onboarding_assist(
                _context(), username="u", password="p"
            )


if __name__ == "__main__":
    unittest.main()
