from __future__ import annotations

import importlib.util
from pathlib import Path
import textwrap
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "render_release_notes.py"

_SPEC = importlib.util.spec_from_file_location("render_release_notes", SCRIPT_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError("Could not load render_release_notes tool")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


class RenderReleaseNotesTests(unittest.TestCase):
    def test_normalize_version_accepts_tag_prefix(self) -> None:
        self.assertEqual(_MODULE.normalize_version("v0.1.43"), "0.1.43")
        self.assertEqual(_MODULE.normalize_version("0.1.43"), "0.1.43")

    def test_extract_release_notes_returns_requested_section_body(self) -> None:
        changelog = textwrap.dedent(
            """\
            # Changelog

            ## [Unreleased]

            ### Added

            - Future work.

            ## [0.1.43] - 2026-04-15

            ### Added

            - First public release.

            ### Fixed

            - Workflow validation.

            ## [0.1.42] - 2026-04-14

            ### Fixed

            - Earlier fix.
            """
        )

        rendered = _MODULE.extract_release_notes(changelog, "v0.1.43")

        self.assertEqual(
            rendered,
            "### Added\n\n- First public release.\n\n### Fixed\n\n- Workflow validation.\n",
        )

    def test_extract_release_notes_raises_for_missing_version(self) -> None:
        changelog = "# Changelog\n\n## [0.1.43] - 2026-04-15\n\n### Added\n\n- First public release.\n"

        with self.assertRaisesRegex(ValueError, "Version 0.1.44"):
            _MODULE.extract_release_notes(changelog, "0.1.44")


if __name__ == "__main__":
    unittest.main()