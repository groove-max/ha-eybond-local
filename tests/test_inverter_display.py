"""OEM labels are presentation data, not profile-selection evidence."""

from copy import deepcopy
from types import SimpleNamespace
import json
from pathlib import Path
from string import Formatter
import unittest

from custom_components.eybond_local.const import (
    CONF_INVERTER_DISPLAY_MANUFACTURER as BRAND,
    CONF_INVERTER_DISPLAY_MODEL as MODEL,
)
from custom_components.eybond_local.inverter_display import (
    inverter_display_labels,
    normalize_display_label,
    with_inverter_display_labels,
)
from custom_components.eybond_local.runtime.device_projection import build_inverter_device_info_payload
from custom_components.eybond_local.support.bundle import build_support_bundle_payload


class InverterDisplayTests(unittest.TestCase):
    def test_all_ui_locales_expose_labels_and_technical_context(self):
        component = Path(__file__).resolve().parents[1] / "custom_components/eybond_local"
        for file in (component / "strings.json", *sorted((component / "translations").glob("*.json"))):
            with self.subTest(file=file.name):
                options = json.loads(file.read_text(encoding="utf-8"))["options"]
                step = options["step"]["inverter_display"]
                self.assertEqual(set(step["data"]), {BRAND, MODEL})
                self.assertEqual(set(step["data_description"]), {BRAND, MODEL})
                self.assertIn("inverter_display", options["step"]["init"]["menu_options"])
                self.assertIn("invalid_inverter_display_label", options["error"])
                self.assertEqual({key for _, key, _, _ in Formatter().parse(step["description"]) if key},
                                 {"detected_model", "profile", "schema"})

    def test_merge_and_clear_only_presentation_options(self):
        original = {"control_mode": "read_only", "driver_hint": "modbus_smg",
                    "effective_metadata_snapshot": {"profile_name": "sandisolar.json"},
                    "device_scoped_overlay_activation": {"profile": "learned.json"}}
        before = deepcopy(original)
        updated = with_inverter_display_labels(original, {
            BRAND: " Anenji ", MODEL: " ANJ-11KW-48V-WIFI-P ",
            "control_mode": "full", "detected_model": "Anenji",
        })
        self.assertEqual(original, before)
        self.assertEqual(updated, original | {BRAND: "Anenji", MODEL: "ANJ-11KW-48V-WIFI-P"})
        self.assertEqual(with_inverter_display_labels(updated, {BRAND: "   "}), original)

    def test_validation_and_defensive_saved_read(self):
        for invalid in (None, 15, {}, ["brand"], "a" * 129, "a\nb", "a\rb", "\x00", "a\tb", "\u202eb", "\u2028"):
            with self.subTest(value=repr(invalid)):
                with self.assertRaises(ValueError):
                    normalize_display_label(invalid)
                self.assertEqual(inverter_display_labels({BRAND: invalid, MODEL: "Valid model"}), {MODEL: "Valid model"})
        self.assertEqual(normalize_display_label("   Модель 48 В   "), "Модель 48 В")
        self.assertEqual(normalize_display_label("a" * 128), "a" * 128)

    def test_projection_changes_only_labels_online_and_offline(self):
        live = SimpleNamespace(model_name="Sandisolar", serial_number="synthetic-serial")
        for inverter in (live, None):
            with self.subTest(live=inverter is not None):
                kwargs = dict(entry_id="synthetic-entry", entry_title="Collector",
                              detected_model="Sandisolar", detected_serial="synthetic-serial", inverter=inverter)
                baseline = build_inverter_device_info_payload(**kwargs)
                shown = build_inverter_device_info_payload(**kwargs, entry_options={BRAND: "Anenji", MODEL: "ANJ"})
                self.assertEqual(shown, baseline | {"name": "ANJ", "model": "ANJ", "manufacturer": "Anenji"})
                brand_only = build_inverter_device_info_payload(**kwargs, entry_options={BRAND: "Anenji"})
                self.assertEqual(brand_only, baseline | {"manufacturer": "Anenji"})
                self.assertEqual(live.model_name, "Sandisolar")

    def test_labels_do_not_resurrect_an_untrusted_serial(self):
        shown = build_inverter_device_info_payload(
            entry_id="synthetic-entry", entry_title="Collector",
            detected_model="PI30", detected_serial="55355535553555",
            inverter=SimpleNamespace(model_name="PI30", serial_number="",
                                     details={"serial_identity_trust": "untrusted"}),
            entry_options={MODEL: "Retail model"},
        )
        self.assertNotIn("serial_number", shown)

    def test_support_keeps_user_labels_separate_from_detected_identity(self):
        options = {BRAND: "Anenji", MODEL: "ANJ"}
        inverter = {"model_name": "Sandisolar", "driver_key": "modbus_smg"}
        bundle = build_support_bundle_payload(
            entry_id="synthetic-entry", entry_title="Collector", connected=False,
            collector=None, inverter=inverter, values={},
            data={"detected_model": "Sandisolar"}, options=options,
            profile_name="sandisolar.json", register_schema_name="protocol4.json",
        )
        self.assertEqual(bundle["inverter_display"], {
            "source": "user", "scope": "config_entry",
            "overrides": {"manufacturer": "Anenji", "model": "ANJ"},
        })
        self.assertEqual(bundle["runtime"]["inverter"], inverter)
        self.assertEqual(bundle["source_metadata"]["profile_name"], "sandisolar.json")
        self.assertEqual(bundle["source_metadata"]["register_schema_name"], "protocol4.json")
        self.assertEqual(bundle["entry"]["data"]["detected_model"], "Sandisolar")


if __name__ == "__main__":
    unittest.main()
