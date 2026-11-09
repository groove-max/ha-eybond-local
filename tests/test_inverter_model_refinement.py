"""Generated model labels may be enriched, not used to replace a binding."""

from dataclasses import replace
import unittest

from custom_components.eybond_local.drivers.registry import model_name_refines
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.runtime.hub.common import _inverter_identities_conflict


class InverterModelRefinementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.current = DetectedInverter(
            driver_key="pi30", protocol_family="pi30", model_name="PI30 6200",
            serial_number="", probe_target=ProbeTarget(0x0994, 0xFF, 0),
            variant_key="pi30_max", profile_name="pi30_ascii/models/pi30_max.json",
            register_schema_name="pi30_ascii/models/pi30_max.json",
            details={"runtime_detection_status": "startup_persisted_identity"},
        )
        self.live = replace(
            self.current, model_name="OEM-PLATFORM-X",
            details={"model_number": "OEM-PLATFORM-X", "output_rating_active_power": 6200},
        )

    def test_live_report_enriches_generated_label_without_serial(self):
        self.assertTrue(model_name_refines(self.current, self.live))
        self.assertFalse(_inverter_identities_conflict(self.current, self.live))

    def test_not_an_oem_model_or_power_allowlist(self):
        for power, model in ((4200, "PLATFORM-A"), (11000, "PLATFORM-B")):
            with self.subTest(power=power):
                old = replace(self.current, model_name=f"PI30 {power}")
                live = replace(self.live, model_name=model, details={
                    "model_number": model, "output_rating_active_power": power,
                })
                self.assertFalse(_inverter_identities_conflict(old, live))

    def test_missing_or_changed_technical_binding_still_conflicts(self):
        for key in ("driver_key", "protocol_family", "variant_key", "profile_name", "register_schema_name"):
            for value in ("", "different"):
                with self.subTest(key=key, value=value):
                    live = replace(self.live, **{key: value})
                    self.assertTrue(_inverter_identities_conflict(self.current, live))
                    self.assertFalse(model_name_refines(self.current, live))
        unknown = replace(self.current, profile_name="")
        self.assertFalse(model_name_refines(unknown, replace(self.live, profile_name="")))

    def test_serial_conflict_or_loss_is_never_overridden(self):
        old = replace(self.current, serial_number="99000000000001")
        for serial in ("99000000000002", ""):
            with self.subTest(serial=serial):
                live = replace(self.live, serial_number=serial)
                self.assertTrue(_inverter_identities_conflict(old, live))
                self.assertFalse(model_name_refines(old, live))

    def test_newly_available_serial_can_enrich_unknown_identity(self):
        live = replace(self.live, serial_number="99000000000001")
        self.assertFalse(_inverter_identities_conflict(self.current, live))

    def test_concrete_model_does_not_become_an_alias(self):
        old = replace(self.current, model_name="A different retail model")
        self.assertTrue(_inverter_identities_conflict(old, self.live))
        # A generated-looking name previously returned by the device is not
        # proof of our fallback rule either.
        old = replace(self.current, details={"model_number": "PI30 6200"})
        self.assertTrue(_inverter_identities_conflict(old, self.live))

    def test_missing_model_query_cannot_downgrade_a_concrete_name(self):
        self.assertFalse(model_name_refines(self.live, self.current))
        self.assertTrue(_inverter_identities_conflict(self.live, self.current))

    def test_label_and_read_evidence_must_agree(self):
        for details in (
            {}, {"output_rating_active_power": 6200},
            {"model_number": ""}, {"model_number": "ANOTHER-PLATFORM", "output_rating_active_power": 6200},
            {"model_number": "OEM-PLATFORM-X", "output_rating_active_power": 4200},
            {"model_number": "OEM-PLATFORM-X", "output_rating_active_power": "6200"},
            {"model_number": "OEM-PLATFORM-X", "output_rating_active_power": True},
            {"model_number": "OEM-PLATFORM-X", "output_rating_active_power": 0},
        ):
            with self.subTest(details=details):
                self.assertTrue(_inverter_identities_conflict(
                    self.current, replace(self.live, details=details),
                ))

    def test_protocol_only_label_is_not_enough(self):
        self.assertTrue(_inverter_identities_conflict(
            replace(self.current, model_name="PI30"), self.live,
        ))

    def test_other_or_unknown_driver_does_not_opt_in_implicitly(self):
        for key in ("modbus_smg", "unknown"):
            with self.subTest(driver=key):
                old = replace(self.current, driver_key=key, protocol_family=key)
                live = replace(self.live, driver_key=key, protocol_family=key)
                self.assertTrue(_inverter_identities_conflict(old, live))

    def test_partial_objects_cannot_authorize_refinement(self):
        self.assertFalse(model_name_refines(None, self.live))
        self.assertFalse(model_name_refines(self.current, object()))
