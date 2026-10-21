"""Owner-confirmed #62 PCP enum is NOT a generic PI30 MAX change."""
import unittest

from test_pi30_driver import _FakeTransport
from custom_components.eybond_local.drivers.pi30 import Pi30Driver, _translate_capability_enum_values
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import ProbeTarget


PROFILE = "pi30_ascii/models/vmii_6200_31_07.json"
QPIRI = "240.0 25.8 240.0 50.0 25.8 6200 6200 48.0 50.0 48.5 54.8 53.7 2 020 040 1 1 1 1 01 0 0 52.0 0 1 50.0 22 51.2"
CHOICES = {0: "Solar first", 1: "Solar + Utility", 2: "Only solar charging permitted"}


def transport(firmware="VERFW:00031.07", model="VMII-NXPW5KW", rating=QPIRI):
    replies = {"QPI": "PI30", "QID": "SYNTHETIC06200", "QPIRI": rating,
               "QMN": model, "QFLAG": "EabuvyzglDjkxd", "QVFW": firmware,
               "QPIGS": "226.2 49.9 226.2 49.9 0565 0442 009 426 53.70 000 100 0039 02.2 398.3 00.00 00000 10010111 00 00 00900 110",
               "QPIWS": "0" * 32}
    return _FakeTransport({(0x0102, 255, key): value for key, value in replies.items() if value is not None})


class ChargerVariantTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_identity_selects_scoped_three_choice_profile(self):
        device = await Pi30Driver().async_probe(transport(), ProbeTarget(0x0102, 255, 0))
        self.assertIsNotNone(device)
        self.assertEqual(device.profile_name, PROFILE)
        cap = next(c for c in device.capabilities if c.key == "charger_source_priority")
        self.assertEqual(cap.enum_value_map, CHOICES)
        self.assertEqual(device.details["charger_source_priority"], "Solar + Utility")

    async def test_missing_or_other_identity_never_inherits_owner_mapping(self):
        for kwargs in ({"firmware": None}, {"firmware": "VERFW:00031.08"},
                       {"model": "OTHER"}, {"rating": QPIRI.replace("6200", "4200")},
                       {"rating": " ".join(QPIRI.split()[:25])}):
            with self.subTest(kwargs=kwargs):
                device = await Pi30Driver().async_probe(transport(**kwargs), ProbeTarget(0x0102, 255, 0))
                self.assertIsNotNone(device)
                self.assertNotEqual(device.profile_name, PROFILE)
                cap = next(c for c in device.capabilities if c.key == "charger_source_priority")
                self.assertEqual(cap.enum_value_map[0], "Utility first")
                self.assertIn(3, cap.enum_value_map)

    def test_model_choices_override_generic_schema_labels_and_unknown_is_not_option(self):
        profile = load_driver_profile(PROFILE)
        for code, label in (*CHOICES.items(), (3, "Unknown (3)")):
            values = {"charger_source_priority_code": code, "charger_source_priority": "generic schema label"}
            _translate_capability_enum_values(values, profile.capabilities)
            self.assertEqual(values["charger_source_priority"], label)

    async def test_each_label_writes_its_confirmed_code_and_rejects_removed_option(self):
        driver, link = Pi30Driver(), transport()
        device = await driver.async_probe(link, ProbeTarget(0x0102, 255, 0))
        for code, label in CHOICES.items():
            command = f"PCP{code:02d}"
            link._responses[(0x0102, 255, command)] = "ACK"
            self.assertEqual(await driver.async_write_capability(link, device, "charger_source_priority", label), label)
            self.assertEqual(link.commands[-1], command)
        before = list(link.commands)
        for invalid in (3, "Utility first"):
            with self.assertRaises(ValueError):
                await driver.async_write_capability(link, device, "charger_source_priority", invalid)
        self.assertEqual(link.commands, before)
