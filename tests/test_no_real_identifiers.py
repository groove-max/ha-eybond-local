from __future__ import annotations

import re
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Privacy guard (2026-06-13 incident): real collector PNs, gateway SNs,
# credentials, IPs, and SSIDs were found embedded in tests, catalog provenance,
# and docstrings — some already in pushed public history. Tracked files must
# only ever contain SYNTHETIC identifiers.
#
# Every PN-shaped token (letter + 13+ digits, any case) in a tracked text file
# must be on this allowlist. If this test fails on a new token, do NOT add a
# real identifier here — replace it in the source with a synthetic one
# (structure-preserving: keep length/format/prefix relations), THEN allowlist
# the synthetic value. Full-fidelity donor mappings belong only in the
# gitignored .local/ corpus index.
_ALLOWED_SYNTHETIC_TOKENS = {
    # Own-device stand-in family (PN / truncated-PN prefix / gateway SN).
    "E50000200000000001",
    "E5000020000000",
    "E50000200000000001000001",
    "E50000200000009777",
    # Donor corpus stand-ins used by contribution tests.
    "E5000025000005",
    # Generic fixture PNs.
    "E5000099990001",
    "E5000099990002",
    "E5000099990003",
    "A0000000000001",
    "A9999999999999",
    "A1234567890123",
    "Z9999999999999",
    # Synthetic stand-in for the legacy Q-collector fixture family (the
    # original fixture value turned out to be a REAL collector PN; scrubbed
    # 2026-06-13).
    "Q0000000000001",
    "Q00000000000010001",
}

_PN_SHAPED = re.compile(r"\b[A-Za-z][0-9]{13,}\b")

_SCAN_SUFFIXES = {".py", ".json", ".md", ".txt", ".yaml", ".yml"}
_SKIP_DIR_NAMES = {"__pycache__", ".local", ".git"}


def _iter_tracked_text_files() -> list[Path]:
    files: list[Path] = []
    for root in (REPO_ROOT / "custom_components", REPO_ROOT / "tests"):
        for path in root.rglob("*"):
            if any(part in _SKIP_DIR_NAMES for part in path.parts):
                continue
            if path.is_file() and path.suffix in _SCAN_SUFFIXES:
                files.append(path)
    files.extend(
        path for path in REPO_ROOT.glob("*")
        if path.is_file() and path.suffix in _SCAN_SUFFIXES
    )
    return files


class NoRealIdentifiersTest(unittest.TestCase):
    """Every PN-shaped token in tracked text files must be a known synthetic."""

    def test_only_allowlisted_pn_shaped_tokens_present(self) -> None:
        offenders: list[str] = []
        allowed_upper = {token.upper() for token in _ALLOWED_SYNTHETIC_TOKENS}
        for path in _iter_tracked_text_files():
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            for match in _PN_SHAPED.finditer(text):
                token = match.group(0)
                if token.upper() not in allowed_upper:
                    line = text.count("\n", 0, match.start()) + 1
                    offenders.append(
                        f"{path.relative_to(REPO_ROOT)}:{line}: {token}"
                    )
        self.assertEqual(
            offenders,
            [],
            "PN-shaped tokens outside the synthetic allowlist found. NEVER add a "
            "real identifier to the allowlist — replace it in the source with a "
            "synthetic stand-in first:\n" + "\n".join(offenders),
        )

    def test_scan_actually_covers_the_repo(self) -> None:
        # Guard the guard: the scanner must see a meaningful file set and the
        # known synthetic PN family must actually occur in it.
        files = _iter_tracked_text_files()
        self.assertGreater(len(files), 200)
        joined = "\n".join(str(f) for f in files)
        self.assertIn("inverter_catalog.json", joined)
        hits = 0
        for path in files:
            try:
                if "E50000200000000001" in path.read_text(encoding="utf-8"):
                    hits += 1
            except (OSError, UnicodeDecodeError):
                continue
        self.assertGreater(hits, 5)


if __name__ == "__main__":
    unittest.main()
