from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.diagnostic_export import (
    build_shareable_payload,
    diagnostic_run_download_url,
    export_diagnostic_run,
)
from custom_components.eybond_local.support.diagnostic_runner import DiagnosticRunResult


# Synthetic identifier only (matches the project's documented synthetic PN).
SYNTHETIC_SERIAL = "E5000020000000"
ENTRY_ID = "entry-secret-0001"


def _result() -> DiagnosticRunResult:
    return DiagnosticRunResult(
        success=True,
        output=f"[1] ascii QID\nstatus: ok\npayload: {SYNTHETIC_SERIAL}\n",
        results=[
            {
                "line": 1,
                "command": "ascii QID",
                "kind": "ascii",
                "status": "ok",
                "duration_ms": 5,
                "request": {"command": "QID"},
                "response": {
                    "raw_hex": "2829",
                    "payload": SYNTHETIC_SERIAL,
                    "decode_error": None,
                },
                "error": None,
            }
        ],
        context={
            "integration_version": "0.2.0-test",
            "entry_id": ENTRY_ID,
            "selected_driver_key": "pi30",
            "driver_source": "scenario_override",
            "probe_target": {"devcode": 1, "collector_addr": 255, "device_addr": 4},
            "catalog_detection": {},
        },
        started_at="2026-06-19T00:00:00+00:00",
        finished_at="2026-06-19T00:00:01+00:00",
        error=None,
    )


class ShareablePayloadTests(unittest.TestCase):
    def test_entry_id_is_dropped_and_serial_masked(self) -> None:
        shareable = build_shareable_payload(_result())
        self.assertNotIn("entry_id", shareable["context"])
        text = str(shareable)
        self.assertNotIn(SYNTHETIC_SERIAL, text)
        self.assertNotIn(ENTRY_ID, text)
        # Driver context that is not personal is preserved.
        self.assertEqual(shareable["context"]["selected_driver_key"], "pi30")

    def test_numeric_serial_and_known_identity_words_are_redacted(self) -> None:
        numeric_serial = "99432601103265"
        words = [
            int.from_bytes(numeric_serial.encode("ascii")[offset : offset + 2], "big")
            for offset in range(0, len(numeric_serial), 2)
        ]
        hex_words = [f"0x{word:04X}" for word in words]
        result = DiagnosticRunResult(
            success=True,
            output=(
                "[1] read 186 7\n"
                "status: ok\n"
                f"decimal: {' '.join(str(word) for word in words)}\n"
                f"hex: {' '.join(hex_words)}\n"
                f'ascii: \"{numeric_serial}\"\n'
            ),
            results=[
                {
                    "line": 1,
                    "command": "read 186 7",
                    "kind": "modbus_read",
                    "status": "ok",
                    "duration_ms": 5,
                    "request": {"register": 186, "count": 7},
                    "response": {
                        "decimal": words,
                        "hex": hex_words,
                        "ascii": numeric_serial,
                    },
                    "error": None,
                }
            ],
            context={"entry_id": ENTRY_ID},
            started_at="2026-06-19T00:00:00+00:00",
            finished_at="2026-06-19T00:00:01+00:00",
            error=None,
        )

        shareable = build_shareable_payload(result)
        response = shareable["results"][0]["response"]
        self.assertEqual(response["decimal"], [None] * 7)
        self.assertEqual(response["hex"], ["REDACTED"] * 7)
        self.assertEqual(response["ascii"], "REDACTED_IDENTITY_RANGE")
        serialized = str(shareable)
        self.assertNotIn(numeric_serial, serialized)
        self.assertNotIn("0x3939", serialized)
        self.assertNotIn("14649", serialized)
        self.assertIn("hex: REDACTED_IDENTITY_RANGE", shareable["output"])


class ExportTests(unittest.TestCase):
    def test_export_writes_three_files_without_public_download_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            export = export_diagnostic_run(
                config_dir=config_dir,
                entry_id=ENTRY_ID,
                result=_result(),
                now=datetime(2026, 6, 19, tzinfo=timezone.utc),
            )

            self.assertTrue(export.result_path.exists())
            self.assertTrue(export.text_path.exists())
            self.assertTrue(export.shareable_path.exists())

            local_text = export.result_path.read_text(encoding="utf-8")
            shareable_text = export.shareable_path.read_text(encoding="utf-8")

            # Local raw keeps the serial (owner-only diagnostics).
            self.assertIn(SYNTHETIC_SERIAL, local_text)
            self.assertIn(ENTRY_ID, local_text)
            # Shareable scrubs the serial and drops entry_id.
            self.assertNotIn(SYNTHETIC_SERIAL, shareable_text)
            self.assertNotIn(ENTRY_ID, shareable_text)

            # Nothing is exposed via /local unless the caller opts in.
            self.assertIsNone(export.download_path)
            self.assertIsNone(export.download_url)
            self.assertFalse((config_dir / "www").exists())

    def test_export_with_publish_exposes_only_shareable_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            export = export_diagnostic_run(
                config_dir=config_dir,
                entry_id=ENTRY_ID,
                result=_result(),
                now=datetime(2026, 6, 19, tzinfo=timezone.utc),
                publish_download_copy=True,
            )

            self.assertIsNotNone(export.download_path)
            self.assertTrue(export.download_path.exists())
            self.assertEqual(export.download_path.name, export.shareable_path.name)
            self.assertEqual(
                export.download_url,
                diagnostic_run_download_url(export.shareable_path.name),
            )
            self.assertTrue(export.download_url.startswith("/local/eybond_local/diagnostic_runs/"))

            # The published copy lives under www/, the raw files do not.
            self.assertIn("www", export.download_path.parts)
            self.assertNotIn("www", export.result_path.parts)

    def test_export_without_publish_returns_no_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            export = export_diagnostic_run(
                config_dir=Path(tmp),
                entry_id=ENTRY_ID,
                result=_result(),
                now=datetime(2026, 6, 19, tzinfo=timezone.utc),
                publish_download_copy=False,
            )
            self.assertIsNone(export.download_path)
            self.assertIsNone(export.download_url)
            self.assertFalse((Path(tmp) / "www").exists())


if __name__ == "__main__":
    unittest.main()
