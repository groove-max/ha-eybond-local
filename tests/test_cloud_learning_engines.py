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
from custom_components.eybond_local.support.cloud_api_adapters import (  # noqa: E402
    CREDENTIAL_REALM_EYBOND,
    LEARNING_SOURCE_DESSMONITOR,
    LEARNING_SOURCE_SMARTESS,
)
from custom_components.eybond_local.support.cloud_learning_engines import (  # noqa: E402
    DessMonitorActiveCloudLearningEngine,
    DessMonitorCloudLearningEngine,
    SmartEssCloudLearningEngine,
    SmartEssReadOnlyCloudLearningEngine,
    UnavailableCloudLearningEngine,
    ValueCloudCloudLearningEngine,
    compatible_cloud_learning_methods,
    compatible_cloud_learning_methods_for_provider,
    compatible_cloud_learning_sources,
    compatible_cloud_learning_sources_for_method,
    compatible_cloud_learning_sources_for_method_any_provider,
    default_cloud_learning_method,
    default_cloud_learning_source_for_method,
    default_cloud_learning_source_for_method_any_provider,
    resolve_cloud_learning_selection,
    supported_cloud_learning_methods,
    supported_cloud_learning_selections,
    supported_cloud_learning_sources,
)
from custom_components.eybond_local.support.cloud_learning_models import (  # noqa: E402
    ACTIVE_CORRELATION_METHOD,
    LEARNING_METHOD_ACTIVE_CORRELATION,
    LEARNING_METHOD_READ_ONLY_EVIDENCE,
    LOCAL_SERIES_EVIDENCE,
    LOCAL_SNAPSHOT_EVIDENCE,
    NO_LOCAL_EVIDENCE,
    READ_ONLY_EVIDENCE_METHOD,
    CloudApiCapabilities,
    CloudApiSource,
    CloudLearningEvidenceCapabilities,
    CloudLearningMethod,
    CloudLearningSelection,
    source_supports_method,
)
from custom_components.eybond_local.support.cloud_learning_runner import (  # noqa: E402
    CloudLearningOutcome,
    CloudLearningRunner,
)
from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorCloudError,
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

    def test_source_and_api_capabilities_direct_constructors_are_strict(self) -> None:
        capabilities = CloudApiCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=False,
            history=False,
        )
        source = CloudApiSource(
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
                    CloudApiSource(**values)
        with self.assertRaises(TypeError):
            CloudApiCapabilities(
                metadata=1,
                control_actions=True,
                raw_packets=False,
                history=False,
            )
        self.assertEqual(
            set(CloudApiCapabilities.__dataclass_fields__),
            {"metadata", "control_actions", "raw_packets", "history"},
        )

        for malformed in (1, None, "yes", object()):
            with self.subTest(malformed=malformed):
                with self.assertRaises(TypeError):
                    CloudLearningEvidenceCapabilities(
                        local_register_snapshot=malformed,
                        local_register_series=False,
                    )
        with self.assertRaises(ValueError):
            CloudLearningEvidenceCapabilities(
                local_register_snapshot=False,
                local_register_series=True,
            )
        self.assertEqual(NO_LOCAL_EVIDENCE.local_register_snapshot, False)
        self.assertEqual(LOCAL_SNAPSHOT_EVIDENCE.local_register_series, False)
        self.assertEqual(LOCAL_SERIES_EVIDENCE.local_register_series, True)

        self.assertNotIn("requires_shadow_route", CloudApiCapabilities.__dataclass_fields__)
        self.assertNotIn("requires_control_consent", CloudApiCapabilities.__dataclass_fields__)

    def test_method_and_selection_direct_constructors_are_strict(self) -> None:
        self.assertTrue(ACTIVE_CORRELATION_METHOD.requires_shadow_route)
        self.assertTrue(ACTIVE_CORRELATION_METHOD.requires_control_consent)
        self.assertFalse(READ_ONLY_EVIDENCE_METHOD.requires_shadow_route)
        self.assertFalse(READ_ONLY_EVIDENCE_METHOD.requires_control_consent)

        for field, malformed in (
            ("method_id", " active_correlation"),
            ("requires_metadata", 1),
            ("requires_control_actions", None),
            ("requires_shadow_route", "yes"),
            ("requires_control_consent", object()),
        ):
            values = {
                "method_id": "active_correlation",
                "requires_metadata": True,
                "requires_control_actions": True,
                "requires_shadow_route": True,
                "requires_control_consent": True,
                field: malformed,
            }
            with self.subTest(field=field):
                with self.assertRaises((TypeError, ValueError)):
                    CloudLearningMethod(**values)
        with self.assertRaises(ValueError):
            CloudLearningMethod(
                method_id="invalid",
                requires_metadata=True,
                requires_control_actions=False,
                requires_shadow_route=True,
                requires_control_consent=False,
            )
        with self.assertRaises(ValueError):
            CloudLearningMethod(
                method_id="future_method",
                requires_metadata=True,
                requires_control_actions=False,
                requires_shadow_route=False,
                requires_control_consent=False,
            )
        for field, malformed in (
            ("method_id", ""),
            ("method_id", b"active_correlation"),
            ("method_id", "future_method"),
            ("source_id", "smartess "),
            ("source_id", object()),
        ):
            values = {
                "method_id": LEARNING_METHOD_ACTIVE_CORRELATION,
                "source_id": LEARNING_SOURCE_SMARTESS,
                field: malformed,
            }
            with self.subTest(field=field):
                with self.assertRaises((TypeError, ValueError)):
                    CloudLearningSelection(**values)

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
        self.assertEqual(
            default_cloud_learning_source_for_method(
                "smartess", LEARNING_METHOD_READ_ONLY_EVIDENCE
            ),
            "dessmonitor",
        )
        self.assertEqual(
            default_cloud_learning_source_for_method(
                "smartess", LEARNING_METHOD_ACTIVE_CORRELATION
            ),
            "smartess",
        )
        dessmonitor = resolve_cloud_learning_selection(
            CloudLearningSelection(
                method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
                source_id=LEARNING_SOURCE_DESSMONITOR,
            )
        )
        self.assertTrue(dessmonitor.available)
        self.assertTrue(dessmonitor.source.capabilities.control_actions)
        self.assertFalse(dessmonitor.method.requires_shadow_route)
        self.assertTrue(dessmonitor.source.capabilities.raw_packets)
        self.assertTrue(dessmonitor.evidence_capabilities.local_register_snapshot)
        # History is bounded, read-only provider evidence.  The separate local
        # correlation/activation boundary remains disabled.
        self.assertTrue(dessmonitor.source.capabilities.history)
        smartess_read_only = resolve_cloud_learning_selection(
            CloudLearningSelection(
                method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
                source_id=LEARNING_SOURCE_SMARTESS,
            )
        )
        self.assertTrue(smartess_read_only.source.capabilities.history)
        self.assertTrue(
            smartess_read_only.evidence_capabilities.local_register_series
        )
        for malformed in (" smartess", "SMARTESS", b"smartess", None, object()):
            with self.subTest(malformed=malformed):
                self.assertEqual(compatible_cloud_learning_sources(malformed), ())
                self.assertEqual(default_cloud_learning_method(malformed), "")

    def test_registry_separates_method_from_source(self) -> None:
        self.assertEqual(
            tuple(method.method_id for method in supported_cloud_learning_methods()),
            (
                LEARNING_METHOD_ACTIVE_CORRELATION,
                LEARNING_METHOD_READ_ONLY_EVIDENCE,
            ),
        )
        self.assertEqual(
            supported_cloud_learning_selections(),
            (
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                    source_id="dessmonitor",
                ),
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                    source_id="smartess",
                ),
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                    source_id="valuecloud",
                ),
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
                    source_id="dessmonitor",
                ),
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
                    source_id="smartess",
                ),
            ),
        )
        self.assertEqual(
            tuple(
                source.source_id
                for source in compatible_cloud_learning_sources_for_method(
                    "smartess",
                    LEARNING_METHOD_ACTIVE_CORRELATION,
                )
            ),
            ("dessmonitor", "smartess"),
        )
        self.assertEqual(
            tuple(
                source.source_id
                for source in compatible_cloud_learning_sources_for_method_any_provider(
                    LEARNING_METHOD_READ_ONLY_EVIDENCE
                )
            ),
            ("dessmonitor", "smartess"),
        )
        self.assertEqual(
            default_cloud_learning_source_for_method_any_provider(
                LEARNING_METHOD_READ_ONLY_EVIDENCE
            ),
            "dessmonitor",
        )
        self.assertEqual(
            tuple(
                source.source_id
                for source in compatible_cloud_learning_sources_for_method(
                    "smartess",
                    LEARNING_METHOD_READ_ONLY_EVIDENCE,
                )
            ),
            ("dessmonitor", "smartess"),
        )
        self.assertEqual(
            compatible_cloud_learning_methods("smartess"),
            (ACTIVE_CORRELATION_METHOD, READ_ONLY_EVIDENCE_METHOD),
        )
        self.assertEqual(
            compatible_cloud_learning_methods("dessmonitor"),
            (ACTIVE_CORRELATION_METHOD, READ_ONLY_EVIDENCE_METHOD),
        )
        self.assertTrue(
            source_supports_method(
                SmartEssCloudLearningEngine().source,
                ACTIVE_CORRELATION_METHOD,
            )
        )
        self.assertTrue(
            source_supports_method(
                DessMonitorCloudLearningEngine().source,
                ACTIVE_CORRELATION_METHOD,
            )
        )
        self.assertIsInstance(
            resolve_cloud_learning_selection(
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                    source_id=LEARNING_SOURCE_DESSMONITOR,
                )
            ),
            DessMonitorActiveCloudLearningEngine,
        )

    def test_dessmonitor_unknown_login_code_is_not_called_bad_credentials(self) -> None:
        engine = resolve_cloud_learning_selection(
            CloudLearningSelection(
                method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                source_id=LEARNING_SOURCE_DESSMONITOR,
            )
        )

        self.assertEqual(
            engine.classify_error(
                DessMonitorCloudError("login_failed:10", stage="authSource")
            ),
            "auth_failed",
        )
        self.assertEqual(
            engine.classify_error(
                DessMonitorCloudError("login_failed:16", stage="authSource")
            ),
            "unexpected",
        )
        self.assertEqual(
            engine.classify_error(
                DessMonitorCloudError(
                    "login_failed:missing_dat",
                    stage="authSource",
                )
            ),
            "unexpected",
        )

    def test_selection_is_explicit_and_typed(self) -> None:
        smartess = CloudLearningSelection(
            method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
            source_id="smartess",
        )
        dessmonitor = CloudLearningSelection(
            method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
            source_id="dessmonitor",
        )
        self.assertEqual(
            smartess,
            CloudLearningSelection(
                method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                source_id="smartess",
            ),
        )
        self.assertEqual(
            dessmonitor,
            CloudLearningSelection(
                method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
                source_id="dessmonitor",
            ),
        )
        self.assertIsInstance(
            resolve_cloud_learning_selection(smartess),
            SmartEssCloudLearningEngine,
        )
        for malformed in (
            object(),
            {"method_id": "active_correlation", "source_id": "smartess"},
            types.SimpleNamespace(
                method_id="active_correlation",
                source_id="smartess",
            ),
        ):
            with self.subTest(malformed=malformed):
                resolved = resolve_cloud_learning_selection(malformed)
                self.assertIsInstance(resolved, UnavailableCloudLearningEngine)
                self.assertFalse(resolved.available)

        self.assertIsInstance(
            resolve_cloud_learning_selection(
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
                    source_id="smartess",
                )
            ),
            SmartEssReadOnlyCloudLearningEngine,
        )

    def test_batch_one_preserves_the_existing_source_runner_matrix(self) -> None:
        expected = {
            (LEARNING_METHOD_ACTIVE_CORRELATION, "dessmonitor"): (
                LEARNING_METHOD_ACTIVE_CORRELATION,
                "ActiveCorrelationWorkflowRunner",
                True,
                True,
            ),
            (LEARNING_METHOD_ACTIVE_CORRELATION, "smartess"): (
                LEARNING_METHOD_ACTIVE_CORRELATION,
                "ActiveCorrelationWorkflowRunner",
                True,
                True,
            ),
            (LEARNING_METHOD_READ_ONLY_EVIDENCE, "dessmonitor"): (
                LEARNING_METHOD_READ_ONLY_EVIDENCE,
                "ReadOnlyEvidenceWorkflowRunner",
                False,
                False,
            ),
            (LEARNING_METHOD_READ_ONLY_EVIDENCE, "smartess"): (
                LEARNING_METHOD_READ_ONLY_EVIDENCE,
                "ReadOnlyEvidenceWorkflowRunner",
                False,
                False,
            ),
            (LEARNING_METHOD_ACTIVE_CORRELATION, "valuecloud"): (
                LEARNING_METHOD_ACTIVE_CORRELATION,
                "ActiveCorrelationWorkflowRunner",
                True,
                True,
            ),
        }
        for (selection_method, source_id), matrix in expected.items():
            with self.subTest(method=selection_method, source_id=source_id):
                method_id, runner_name, route, consent = matrix
                engine = resolve_cloud_learning_selection(
                    CloudLearningSelection(
                        method_id=selection_method,
                        source_id=source_id,
                    )
                )
                runner = engine.learning_runner()
                self.assertTrue(engine.available)
                self.assertEqual(engine.selection.source_id, source_id)
                self.assertEqual(engine.selection.method_id, method_id)
                self.assertEqual(type(runner).__name__, runner_name)
                self.assertEqual(engine.method.requires_shadow_route, route)
                self.assertEqual(engine.method.requires_control_consent, consent)
                self.assertEqual(runner.source_id, source_id)
                self.assertEqual(runner.provider_id, engine.source.provider_id)

    def test_resolution_is_exact_and_fail_closed(self) -> None:
        self.assertIsInstance(
            resolve_cloud_learning_selection(
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                    source_id="smartess",
                )
            ),
            SmartEssCloudLearningEngine,
        )
        self.assertIsInstance(
            resolve_cloud_learning_selection(
                CloudLearningSelection(
                    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
                    source_id="valuecloud",
                )
            ),
            ValueCloudCloudLearningEngine,
        )
        for malformed in ("nope", " smartess", b"smartess", None, object()):
            with self.subTest(malformed=malformed):
                engine = resolve_cloud_learning_selection(malformed)
                self.assertIsInstance(engine, UnavailableCloudLearningEngine)
                self.assertFalse(engine.available)

    def test_evidence_provider_no_longer_owns_learning_execution(self) -> None:
        self.assertNotIn("learning_runner", CloudEvidenceProvider.__dict__)
        self.assertNotIn("control_discovery_runner", CloudEvidenceProvider.__dict__)
        self.assertNotIn("control_discovery_available", CloudEvidenceProvider.__dict__)
        self.assertNotIn("classify_control_discovery_error", CloudEvidenceProvider.__dict__)


if __name__ == "__main__":
    unittest.main()
