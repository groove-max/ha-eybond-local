from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.serial_identity import (
    SerialIdentityEvidence,
    SerialIdentitySource,
    SerialIdentityTrust,
    serial_report_is_known_untrusted,
)


class SerialIdentityEvidenceTests(unittest.TestCase):
    def test_valid_shapes_have_one_canonical_boundary(self) -> None:
        trusted = SerialIdentityEvidence.trusted(
            "ABC12345678901234567",
            source=SerialIdentitySource.QSID,
        )
        untrusted = SerialIdentityEvidence.untrusted(
            "55355535553555",
            source=SerialIdentitySource.QID,
            reason="known_placeholder",
        )
        unavailable = SerialIdentityEvidence.unavailable(
            reason="serial_query_unavailable"
        )

        self.assertEqual(trusted.canonical, trusted.reported)
        self.assertEqual(untrusted.canonical, "")
        self.assertEqual(unavailable.canonical, "")
        self.assertEqual(
            untrusted.as_details(),
            {
                "reported_serial_number": "55355535553555",
                "serial_identity_source": "qid",
                "serial_identity_trust": "untrusted",
                "serial_identity_reason": "known_placeholder",
            },
        )

    def test_direct_constructor_rejects_crossed_or_duck_shapes(self) -> None:
        invalid = (
            {
                "reported": "55355535553555",
                "canonical": "55355535553555",
                "source": SerialIdentitySource.QID,
                "trust": SerialIdentityTrust.UNTRUSTED,
                "reason": "known_placeholder",
            },
            {
                "reported": "ABC123",
                "canonical": "ABC123",
                "source": SerialIdentitySource.QID,
                "trust": SerialIdentityTrust.TRUSTED,
                "reason": "unexpected",
            },
            {
                "reported": "",
                "canonical": "",
                "source": SerialIdentitySource.NONE,
                "trust": SerialIdentityTrust.UNAVAILABLE,
                "reason": "",
            },
            {
                "reported": "ABC123",
                "canonical": "ABC123",
                "source": "qid",
                "trust": SerialIdentityTrust.TRUSTED,
                "reason": "",
            },
        )
        for fields in invalid:
            with self.subTest(fields=fields), self.assertRaises((TypeError, ValueError)):
                SerialIdentityEvidence(**fields)

    def test_tokens_are_not_silently_normalized(self) -> None:
        with self.assertRaises(ValueError):
            SerialIdentityEvidence.trusted(
                " ABC123 ",
                source=SerialIdentitySource.QID,
            )
        with self.assertRaises(TypeError):
            SerialIdentityEvidence(
                reported=123,  # type: ignore[arg-type]
                canonical="",
                source=SerialIdentitySource.QID,
                trust=SerialIdentityTrust.UNTRUSTED,
                reason="invalid_type",
            )
        with self.assertRaises(ValueError):
            SerialIdentityEvidence.trusted(
                "ABC123\x00",
                source=SerialIdentitySource.QID,
            )

    def test_known_untrusted_report_requires_exact_value(self) -> None:
        self.assertTrue(serial_report_is_known_untrusted("55355535553555"))
        with self.assertRaises(ValueError):
            SerialIdentityEvidence.trusted(
                "55355535553555",
                source=SerialIdentitySource.QID,
            )
        for value in ("55355535553555 ", 55355535553555, None):
            with self.subTest(value=value):
                self.assertFalse(serial_report_is_known_untrusted(value))

    def test_serial_identity_model_has_no_driver_or_runtime_dependency(self) -> None:
        source_path = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "serial_identity.py"
        )
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )

        self.assertFalse(
            any(
                token in module_name
                for module_name in imported
                for token in ("drivers", "runtime", "homeassistant")
            )
        )


if __name__ == "__main__":
    unittest.main()
