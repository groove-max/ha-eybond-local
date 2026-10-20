from __future__ import annotations

import re
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch


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
    # Invented I300 stand-in for collector-only ValueCloud support readiness.
    "I30000200000000001",
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
    "V0000000000001",
}

_PN_SHAPED = re.compile(r"\b[A-Za-z][0-9]{13,}\b")

# MAC addresses: a real, globally-administered (registered-OUI) MAC is a real
# device identifier (the 2026-06-19 review found a real Espressif-OUI collector
# MAC committed in fixtures). A locally-administered MAC (the 0x02 bit of the
# first octet is set) is synthetic/private by definition and is allowed; any
# other MAC must be an explicitly allowlisted placeholder.
_MAC_SHAPED = re.compile(r"\b(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}\b")
_ALLOWED_MACS = {
    # Globally-administered-SHAPED but obviously synthetic placeholder.
    "11:22:33:44:55:66",
}

# NOTE on IPs: this codebase is full of dotted-quad-SHAPED values that are NOT
# IPs — collector firmware versions (e.g. 8.50.12.3) and BLE layout codes
# (7.x.x.x). A generic IPv4 scan would be hopelessly noisy here, so real-IP
# hygiene is enforced by review + the synthetic-by-construction fixtures rather
# than a pattern. (Known public infra IPs like the SmartESS cloud endpoint are
# intentionally referenced in tests.)

_SCAN_SUFFIXES = {".py", ".json", ".md", ".txt", ".yaml", ".yml"}
_SKIP_DIR_NAMES = {"__pycache__", ".local", ".git"}


def _is_synthetic_mac(mac: str) -> bool:
    if mac.upper() in {allowed.upper() for allowed in _ALLOWED_MACS}:
        return True
    try:
        return bool(int(mac.split(":")[0], 16) & 0x02)
    except ValueError:  # pragma: no cover - regex guarantees hex
        return False


def _iter_repository_text_files() -> list[Path]:
    """Scan tracked and new non-ignored text files; never inspect private .local.

    Driven by `git ls-files` so the scan covers ALL public surfaces that can
    hold user data (custom_components, tests, catalog, tools, docs, .github,
    root) rather than a hand-maintained root allowlist. Falls back to a repo
    walk when git is unavailable. Including untracked candidates is important:
    a pre-commit gate must not silently skip newly written test files.
    """

    paths: list[Path]
    try:
        listing = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=REPO_ROOT,
            capture_output=True,
            check=True,
        ).stdout.decode("utf-8", "replace")
        paths = [REPO_ROOT / name for name in listing.split("\0") if name]
    except (OSError, subprocess.CalledProcessError):  # pragma: no cover - git present in CI
        paths = list(REPO_ROOT.rglob("*"))

    files: list[Path] = []
    for path in paths:
        # Exclusions apply INSIDE this checkout. An ancestor named .local is
        # a valid worktree location, not grounds to skip every tracked file.
        if any(part in _SKIP_DIR_NAMES for part in path.relative_to(REPO_ROOT).parts):
            continue
        if path.is_file() and path.suffix in _SCAN_SUFFIXES:
            files.append(path)
    return files


class NoRealIdentifiersTest(unittest.TestCase):
    """Every PN-shaped token in tracked text files must be a known synthetic."""

    def test_local_ancestor_does_not_hide_worktree_from_git_or_fallback_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / ".local" / "worktrees" / "project"
            public = root / "tests" / "public.py"
            private = root / ".local" / "private.py"
            for path in (public, private):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# fixture\n", encoding="utf-8")
            listing = subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=b"tests/public.py\0.local/private.py\0",
            )
            with patch.dict(globals(), {"REPO_ROOT": root}):
                with patch.object(subprocess, "run", return_value=listing):
                    self.assertEqual(_iter_repository_text_files(), [public])
                with patch.object(subprocess, "run", side_effect=OSError("no git")):
                    self.assertEqual(_iter_repository_text_files(), [public])

    def test_only_allowlisted_pn_shaped_tokens_present(self) -> None:
        offenders: list[str] = []
        allowed_upper = {token.upper() for token in _ALLOWED_SYNTHETIC_TOKENS}
        for path in _iter_repository_text_files():
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

    def test_only_synthetic_macs_present(self) -> None:
        offenders: list[str] = []
        for path in _iter_repository_text_files():
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            for match in _MAC_SHAPED.finditer(text):
                mac = match.group(0)
                if not _is_synthetic_mac(mac):
                    line = text.count("\n", 0, match.start()) + 1
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{line}: {mac}")
        self.assertEqual(
            offenders,
            [],
            "Real (globally-administered) MAC addresses found. Replace them with a "
            "locally-administered synthetic MAC (first octet bit 0x02 set, e.g. "
            "AA:BB:CC:DD:EE:NN):\n" + "\n".join(offenders),
        )

    def test_scan_actually_covers_the_repo(self) -> None:
        # Guard the guard: the scanner must see a meaningful file set, including
        # the contributor-facing surfaces (tools/, docs/, .github/) that the old
        # 4-root allowlist missed, and the known synthetic PN family must occur.
        files = _iter_repository_text_files()
        self.assertGreater(len(files), 200)
        relative = {str(path.relative_to(REPO_ROOT)) for path in files}
        joined = "\n".join(sorted(relative))
        self.assertIn("inverter_catalog.json", joined)
        for required_root in ("tools/", "docs/", ".github/"):
            self.assertTrue(
                any(name.startswith(required_root) for name in relative),
                f"privacy scan no longer covers {required_root}",
            )
        hits = 0
        for path in files:
            try:
                if "E50000200000000001" in path.read_text(encoding="utf-8"):
                    hits += 1
            except (OSError, UnicodeDecodeError):
                continue
        self.assertGreater(hits, 5)

    def test_scan_requests_new_files_and_still_excludes_private_context(self) -> None:
        with (
            patch("subprocess.run", return_value=SimpleNamespace(
                stdout=b"tests/new_case.py\0.local/private_context.md\0",
            )) as run,
            patch.object(Path, "is_file", return_value=True),
        ):
            self.assertEqual(_iter_repository_text_files(), [REPO_ROOT / "tests/new_case.py"])
        command = run.call_args.args[0]
        for flag in ("--cached", "--others", "--exclude-standard"):
            self.assertIn(flag, command)


if __name__ == "__main__":
    unittest.main()
