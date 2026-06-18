from __future__ import annotations

import unittest

from custom_components.eybond_local.metadata.compiled_detection_catalog import (
    PROBE_ACTION_ASCII_COMMAND,
    PROBE_ACTION_COLLECTOR_METADATA,
    PROBE_ACTION_MODBUS_READ,
    PROBE_ACTION_SMARTESS_QUERY,
    RESOLUTION_COMPATIBLE_GROUP,
    RESOLUTION_EXACT,
    RESOLUTION_FAMILY,
    RESOLUTION_UNRESOLVED,
    clear_compiled_detection_catalog_cache,
    compile_detection_catalog,
    load_compiled_detection_catalog,
)
from custom_components.eybond_local.metadata.detection_descriptor_loader import (
    DetectionAnchorCondition,
    DetectionBindingDescriptor,
    DetectionDescriptorCatalog,
    DetectionDeviceDescriptor,
    clear_detection_descriptor_catalog_cache,
    load_detection_descriptor_catalog,
)


class CompiledDetectionCatalogTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_compiled_detection_catalog_cache()
        clear_detection_descriptor_catalog_cache()
        super().tearDown()

    def test_compiles_smg_protocol_actions_and_indexes(self) -> None:
        catalog = load_compiled_detection_catalog()

        protocol = catalog.protocols["modbus_smg"]

        self.assertEqual(
            tuple((action.kind, action.register, action.count) for action in protocol.probe_actions),
            (
                (PROBE_ACTION_MODBUS_READ, 171, 14),
                (PROBE_ACTION_MODBUS_READ, 186, 12),
                (PROBE_ACTION_MODBUS_READ, 643, 2),
            ),
        )
        self.assertIn("smg_6200", catalog.devices_by_protocol["modbus_smg"])
        self.assertIn(
            "smg_6200",
            catalog.exact_evidence_index[
                ("modbus_smg", "fingerprint.model_code", 7680)
            ],
        )
        self.assertIn(
            "smg_6200",
            catalog.devices_by_evidence_key["fingerprint.layout_code"],
        )
        self.assertEqual(
            catalog.protocols_by_transport["eybond_ascii"],
            ("pi18", "pi30"),
        )
        self.assertIn("smg_6200", catalog.devices_by_alias["smg 6200"])
        surface = catalog.surfaces["smg_6200_full"]
        self.assertTrue(surface.default_for_driver)
        self.assertIn((700, 45), surface.support_capture_ranges)

    def test_resolves_exact_and_family_surfaces(self) -> None:
        catalog = load_compiled_detection_catalog()

        exact = catalog.resolve(
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 7680,
                "fingerprint.rated_power": 6200,
            },
        )
        family = catalog.resolve(
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 9999,
            },
        )

        self.assertEqual(exact.resolution, RESOLUTION_EXACT)
        self.assertEqual(exact.candidate_keys, ("smg_6200",))
        self.assertTrue(exact.resolved)
        self.assertEqual(family.resolution, RESOLUTION_FAMILY)
        self.assertEqual(family.candidate_keys, ("modbus_smg.family_fallback",))
        self.assertTrue(family.resolved)

    def test_compiles_pi_actions_and_resolves_catalog_variants(self) -> None:
        catalog = load_compiled_detection_catalog()
        pi30 = catalog.protocols["pi30"]
        action_by_key = {action.key: action for action in pi30.probe_actions}

        self.assertEqual(action_by_key["pi30.qpi"].kind, PROBE_ACTION_ASCII_COMMAND)
        self.assertEqual(action_by_key["pi30.qpigs"].command, "QPIGS")
        self.assertEqual(
            action_by_key["pi30.collector.smartess"].kind,
            PROBE_ACTION_COLLECTOR_METADATA,
        )
        self.assertEqual(
            action_by_key["pi30.smartess.asset"].kind,
            PROBE_ACTION_SMARTESS_QUERY,
        )

        vmii = catalog.resolve(
            protocol_key="pi30",
            evidence={
                "protocol.protocol_id": "PI30",
                "identity.model_number": "VMII-NXPW5KW",
            },
        )
        pi30_max = catalog.resolve(
            protocol_key="pi30",
            evidence={
                "protocol.protocol_id": "PI30",
                "identity.model_number": "OTHER",
                "shape.qpiri_field_count": 28,
                "shape.capability_flags": "adz",
                "shape.qpigs_field_count": 20,
                "shape.qpiws_bit_count": 35,
            },
        )
        pi18 = catalog.resolve(
            protocol_key="pi18",
            evidence={"protocol.protocol_id": "PI18"},
        )

        self.assertEqual(vmii.surface_key, "pi30_vmii_full")
        self.assertEqual(pi30_max.resolution, RESOLUTION_COMPATIBLE_GROUP)
        self.assertEqual(pi30_max.surface_key, "pi30_max_full")
        self.assertEqual(pi18.resolution, RESOLUTION_FAMILY)
        self.assertEqual(pi18.surface_key, "pi18_read_only")
        self.assertEqual(pi30.probe_targets[0], (0x0994, 0xFF, 0))
        self.assertEqual(pi30.probe_timeout, 12.0)
        self.assertEqual(pi30.signature_timeout, 4.0)

    def test_missing_evidence_does_not_select_lower_priority_fallback(self) -> None:
        result = load_compiled_detection_catalog().resolve(
            protocol_key="pi30",
            evidence={"protocol.protocol_id": "PI30"},
        )

        self.assertEqual(result.resolution, RESOLUTION_UNRESOLVED)
        self.assertIn("pi30_vmii_nxpw5kw", result.candidate_keys)
        self.assertIn("pi30_family", result.candidate_keys)

    def test_local_pi_variant_outranks_smartess_supporting_evidence(self) -> None:
        result = load_compiled_detection_catalog().resolve(
            protocol_key="pi30",
            evidence={
                "protocol.protocol_id": "PI30",
                "identity.model_number": "VMII-NXPW5KW",
                "collector.smartess_protocol_asset_id": "0925",
            },
        )

        self.assertEqual(result.surface_key, "pi30_vmii_full")

    def test_same_surface_ambiguity_resolves_as_compatible_group(self) -> None:
        source = load_detection_descriptor_catalog()
        binding = DetectionBindingDescriptor(
            driver_key="modbus_smg",
            variant_key="shared",
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )
        devices = (
            self._synthetic_device("model_a", binding),
            self._synthetic_device("model_b", binding),
        )
        catalog = compile_detection_catalog(
            DetectionDescriptorCatalog(
                protocols=source.protocols,
                devices=devices,
            ),
            schema_version=1,
            catalog_version="test",
        )

        result = catalog.resolve(
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 42,
            },
        )

        self.assertEqual(result.resolution, RESOLUTION_COMPATIBLE_GROUP)
        self.assertEqual(result.candidate_keys, ("model_a", "model_b"))
        self.assertTrue(result.resolved)

    def test_different_surface_ambiguity_stays_unresolved(self) -> None:
        source = load_detection_descriptor_catalog()
        devices = (
            self._synthetic_device(
                "model_a",
                DetectionBindingDescriptor(
                    driver_key="modbus_smg",
                    variant_key="a",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                ),
            ),
            self._synthetic_device(
                "model_b",
                DetectionBindingDescriptor(
                    driver_key="modbus_smg",
                    variant_key="b",
                    profile_name="modbus_smg/models/anenji_op2_6200.json",
                    register_schema_name="modbus_smg/models/anenji_op2_6200.json",
                ),
            ),
        )
        catalog = compile_detection_catalog(
            DetectionDescriptorCatalog(
                protocols=source.protocols,
                devices=devices,
            ),
            schema_version=1,
            catalog_version="test",
        )

        result = catalog.resolve(
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 42,
            },
        )

        self.assertEqual(result.resolution, RESOLUTION_UNRESOLVED)
        self.assertFalse(result.resolved)

    def test_conflicting_declared_surface_is_rejected(self) -> None:
        source = load_detection_descriptor_catalog()
        shared_key = "synthetic_surface"
        devices = (
            self._synthetic_device(
                "model_a",
                DetectionBindingDescriptor(
                    driver_key="modbus_smg",
                    variant_key="a",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    surface_key=shared_key,
                ),
            ),
            self._synthetic_device(
                "model_b",
                DetectionBindingDescriptor(
                    driver_key="modbus_smg",
                    variant_key="b",
                    profile_name="modbus_smg/models/anenji_op2_6200.json",
                    register_schema_name="modbus_smg/models/anenji_op2_6200.json",
                    surface_key=shared_key,
                ),
            ),
        )

        with self.assertRaisesRegex(ValueError, "conflicting_surface"):
            compile_detection_catalog(
                DetectionDescriptorCatalog(
                    protocols=source.protocols,
                    devices=devices,
                ),
                schema_version=2,
                catalog_version="test",
            )

    @staticmethod
    def _synthetic_device(
        key: str,
        binding: DetectionBindingDescriptor,
    ) -> DetectionDeviceDescriptor:
        return DetectionDeviceDescriptor(
            key=key,
            protocol_family="modbus_smg",
            model_name=key,
            tier="full",
            binding=binding,
            anchors=(
                DetectionAnchorCondition(
                    key="fingerprint.layout_code",
                    source="test",
                    equals=1,
                ),
                DetectionAnchorCondition(
                    key="fingerprint.model_code",
                    source="test",
                    equals=42,
                ),
            ),
        )
