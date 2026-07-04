from __future__ import annotations

import unittest

from custom_components.eybond_local.metadata.detection_descriptor_loader import (
    DetectionAnchorCondition,
    DetectionBindingDescriptor,
    DetectionDescriptorCatalog,
    DetectionDeviceDescriptor,
    clear_detection_descriptor_catalog_cache,
    detection_anchor_cost,
    load_detection_descriptor_catalog,
    validate_detection_descriptor_catalog,
)
from custom_components.eybond_local.metadata.device_catalog_loader import (
    load_device_catalog,
)


class DetectionDescriptorLoaderTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_detection_descriptor_catalog_cache()
        super().tearDown()

    def test_loads_protocol_descriptor_from_identity_probe_catalog(self) -> None:
        catalog = load_detection_descriptor_catalog()

        protocol = catalog.protocols["modbus_smg"]

        self.assertEqual(protocol.transport_key, "eybond_modbus")
        self.assertIn((171, 14), [(block.start, block.count) for block in protocol.read_blocks])
        self.assertIn((186, 12), [(block.start, block.count) for block in protocol.read_blocks])
        fields = {field.key: (field.register, field.words) for field in protocol.fields}
        self.assertEqual(fields["fingerprint.layout_code"], (184, 1))
        self.assertEqual(fields["fingerprint.model_code"], (171, 1))
        self.assertEqual(fields["fingerprint.rated_power"], (643, 1))
        self.assertEqual(fields["identity.serial_ascii"], (186, 12))
        self.assertIn(1, protocol.layout_codes)
        self.assertIn(11, protocol.layout_codes)

    def test_device_catalog_entries_have_matching_descriptors(self) -> None:
        descriptor_catalog = load_detection_descriptor_catalog()
        device_catalog = load_device_catalog()

        for entry in device_catalog.devices:
            descriptor = descriptor_catalog.descriptor_for_key(entry.entry_key)

            self.assertIsNotNone(descriptor, entry.entry_key)
            assert descriptor is not None
            self.assertEqual(
                descriptor.protocol_family,
                device_catalog.surfaces[entry.surface_key].protocol_key,
            )
            self.assertEqual(descriptor.model_name, entry.model_name)
            self.assertEqual(descriptor.tier, entry.tier)
            self.assertEqual(descriptor.binding.driver_key, entry.binding.driver_key)
            self.assertEqual(descriptor.binding.variant_key, entry.binding.variant_key)
            self.assertEqual(descriptor.binding.profile_name, entry.binding.profile_name)
            self.assertEqual(
                descriptor.binding.register_schema_name,
                entry.binding.register_schema_name,
            )
            anchor_conditions = {anchor.key: anchor for anchor in descriptor.anchors}
            if not entry.anchors:
                self.assertEqual(
                    anchor_conditions["fingerprint.layout_code"].equals,
                    entry.fingerprint.layout_code,
                )
                self.assertEqual(
                    anchor_conditions["fingerprint.model_code"].equals,
                    entry.fingerprint.model_code,
                )
            if entry.fingerprint.rated_power_one_of:
                self.assertEqual(
                    anchor_conditions["fingerprint.rated_power"].one_of,
                    entry.fingerprint.rated_power_one_of,
                )
            for rule in entry.runtime_probe.validation:
                runtime_anchor = anchor_conditions[f"runtime.{rule.key}"]
                self.assertFalse(runtime_anchor.required)
                self.assertEqual(runtime_anchor.equals, rule.equals)
                self.assertEqual(runtime_anchor.one_of, rule.one_of)
            self.assertEqual(
                tuple((item.key, item.register, item.word_count) for item in descriptor.optional_registers),
                tuple(
                    (item.key, item.register, item.word_count)
                    for item in entry.runtime_probe.optional_registers
                ),
            )
            self.assertEqual(
                tuple((item.key, item.register, item.word_count) for item in descriptor.optional_ascii),
                tuple(
                    (item.key, item.register, item.word_count)
                    for item in entry.runtime_probe.optional_ascii
                ),
            )

    def test_family_fallback_descriptor_is_partial_and_read_only(self) -> None:
        catalog = load_detection_descriptor_catalog()

        fallback = catalog.descriptor_for_key("modbus_smg.family_fallback")

        self.assertIsNotNone(fallback)
        assert fallback is not None
        self.assertTrue(fallback.family_fallback)
        self.assertTrue(fallback.read_only)
        self.assertEqual(fallback.tier, "partial")
        self.assertEqual(fallback.binding.variant_key, "family_fallback")
        self.assertEqual(fallback.binding.profile_name, "")
        self.assertEqual(fallback.anchors[0].key, "fingerprint.layout_code")
        self.assertEqual(fallback.anchors[0].one_of, (1, 2, 11))

    def test_anchor_costs_are_stable_for_planner_inputs(self) -> None:
        self.assertLess(
            detection_anchor_cost("fingerprint.layout_code"),
            detection_anchor_cost("variant.protocol_number"),
        )




    def test_validation_rejects_required_anchor_without_condition(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "required_anchor_without_condition:bad:anchor",
        ):
            validate_detection_descriptor_catalog(
                DetectionDescriptorCatalog(
                    protocols=load_detection_descriptor_catalog().protocols,
                    devices=(
                        DetectionDeviceDescriptor(
                            key="bad",
                            protocol_family="modbus_smg",
                            model_name="Bad",
                            tier="full",
                            binding=DetectionBindingDescriptor(
                                driver_key="modbus_smg",
                                variant_key="bad",
                                profile_name="smg_modbus.json",
                                register_schema_name="modbus_smg/models/smg_6200.json",
                            ),
                            anchors=(
                                DetectionAnchorCondition(
                                    key="anchor",
                                    source="test",
                                ),
                            ),
                        ),
                    ),
                )
            )


if __name__ == "__main__":
    unittest.main()
