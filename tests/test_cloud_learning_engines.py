from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_evidence_providers import (  # noqa: E402
    CloudEvidenceProvider,
)
from custom_components.eybond_local.support.cloud_learning_engines import (  # noqa: E402
    CREDENTIAL_REALM_EYBOND,
    LEARNING_SOURCE_DESSMONITOR,
    LEARNING_SOURCE_SMARTESS,
    CloudLearningCapabilities,
    CloudLearningSource,
    SmartEssCloudLearningEngine,
    UnavailableCloudLearningEngine,
    ValueCloudCloudLearningEngine,
    compatible_cloud_learning_sources,
    default_cloud_learning_source,
    resolve_cloud_learning_engine,
    supported_cloud_learning_sources,
)
from custom_components.eybond_local.support.cloud_learning_runner import (  # noqa: E402
    CloudLearningOutcome,
    CloudLearningRunner,
)


class CloudLearningModelTests(unittest.TestCase):
    def test_outcome_direct_constructor_is_strict_and_detached(self) -> None:
        identity = {"pn": "E50000200000000001"}
        result = {"metadata_only": True}
        outcome = CloudLearningOutcome(identity=identity, result=result)
        identity["pn"] = "FOREIGN"
        result["metadata_only"] = False

        self.assertEqual(outcome.identity["pn"], "E50000200000000001")
        self.assertTrue(outcome.result["metadata_only"])
        for field, malformed in (
            ("identity", object()),
            ("result", []),
            ("read_bindings", object()),
            ("metadata_evidence", types.MappingProxyType({})),
        ):
            with self.subTest(field=field):
                values = {"identity": {}, "result": {}, field: malformed}
                with self.assertRaises(TypeError):
                    CloudLearningOutcome(**values)

        self.assertNotIn("control_discovery_runner", CloudLearningRunner.__dict__)

    def test_source_and_capabilities_direct_constructors_are_strict(self) -> None:
        capabilities = CloudLearningCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=False,
            history=False,
            local_register_snapshot=False,
            local_register_series=False,
            requires_shadow_route=True,
            requires_control_consent=True,
        )
        source = CloudLearningSource(
            source_id="smartess",
            provider_id="smartess",
            credential_realm_id="eybond",
            label="SmartESS-compatible cloud",
            capabilities=capabilities,
        )
        self.assertEqual(source.credential_realm_id, CREDENTIAL_REALM_EYBOND)

        for field, malformed in (
            ("source_id", " smartess"),
            ("provider_id", ""),
            ("credential_realm_id", object()),
            ("label", "label "),
            ("capabilities", object()),
        ):
            with self.subTest(field=field):
                values = {
                    "source_id": "smartess",
                    "provider_id": "smartess",
                    "credential_realm_id": "eybond",
                    "label": "SmartESS-compatible cloud",
                    "capabilities": capabilities,
                    field: malformed,
                }
                with self.assertRaises((TypeError, ValueError)):
                    CloudLearningSource(**values)
        with self.assertRaises(TypeError):
            CloudLearningSource(
                source_id="source",
                provider_id="provider",
                credential_realm_id="realm",
                label="Label",
                capabilities=capabilities,
                default_for_provider=1,  # type: ignore[arg-type]
            )

        with self.assertRaises(TypeError):
            CloudLearningCapabilities(
                metadata=1,
                control_actions=True,
                raw_packets=False,
                history=False,
                local_register_snapshot=False,
                local_register_series=False,
                requires_shadow_route=True,
                requires_control_consent=True,
            )
        with self.assertRaises(ValueError):
            CloudLearningCapabilities(
                metadata=True,
                control_actions=False,
                raw_packets=False,
                history=False,
                local_register_snapshot=False,
                local_register_series=True,
                requires_shadow_route=True,
                requires_control_consent=False,
            )

    def test_registry_separates_sources_from_evidence_providers(self) -> None:
        sources = supported_cloud_learning_sources()
        self.assertEqual(
            tuple(source.source_id for source in sources),
            ("dessmonitor", "smartess", "valuecloud"),
        )
        self.assertEqual(
            tuple(source.source_id for source in compatible_cloud_learning_sources("smartess")),
            (LEARNING_SOURCE_DESSMONITOR, LEARNING_SOURCE_SMARTESS),
        )
        self.assertEqual(default_cloud_learning_source("smartess"), "smartess")
        self.assertEqual(default_cloud_learning_source("valuecloud"), "valuecloud")
        dessmonitor = resolve_cloud_learning_engine(LEARNING_SOURCE_DESSMONITOR)
        self.assertTrue(dessmonitor.available)
        self.assertFalse(dessmonitor.source.capabilities.control_actions)
        self.assertFalse(dessmonitor.source.capabilities.requires_shadow_route)
        self.assertTrue(dessmonitor.source.capabilities.raw_packets)
        self.assertTrue(dessmonitor.source.capabilities.local_register_snapshot)
        # History is bounded, read-only provider evidence.  The separate local
        # correlation/activation boundary remains disabled.
        self.assertTrue(dessmonitor.source.capabilities.history)
        for malformed in (" smartess", "SMARTESS", b"smartess", None, object()):
            with self.subTest(malformed=malformed):
                self.assertEqual(compatible_cloud_learning_sources(malformed), ())
                self.assertEqual(default_cloud_learning_source(malformed), "")

    def test_resolution_is_exact_and_fail_closed(self) -> None:
        self.assertIsInstance(
            resolve_cloud_learning_engine("smartess"),
            SmartEssCloudLearningEngine,
        )
        self.assertIsInstance(
            resolve_cloud_learning_engine("valuecloud"),
            ValueCloudCloudLearningEngine,
        )
        for malformed in ("nope", " smartess", b"smartess", None, object()):
            with self.subTest(malformed=malformed):
                engine = resolve_cloud_learning_engine(malformed)
                self.assertIsInstance(engine, UnavailableCloudLearningEngine)
                self.assertFalse(engine.available)

    def test_evidence_provider_no_longer_owns_learning_execution(self) -> None:
        self.assertNotIn("learning_runner", CloudEvidenceProvider.__dict__)
        self.assertNotIn("control_discovery_runner", CloudEvidenceProvider.__dict__)
        self.assertNotIn("control_discovery_available", CloudEvidenceProvider.__dict__)
        self.assertNotIn("classify_control_discovery_error", CloudEvidenceProvider.__dict__)


if __name__ == "__main__":
    unittest.main()
