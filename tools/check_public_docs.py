#!/usr/bin/env python3
"""Validate repository documentation, its audience index, and screenshots."""

from __future__ import annotations

from collections import defaultdict
import hashlib
from html import unescape
from pathlib import Path
import re
import sys
from urllib.parse import unquote


REPO_ROOT = Path(__file__).resolve().parents[1]

ROOT_MARKDOWN = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "README.uk.md",
    REPO_ROOT / "CHANGELOG.md",
    REPO_ROOT / "CONTRIBUTING.md",
    REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "release_checklist.md",
)

DOCS_INDEX = REPO_ROOT / "docs" / "README.md"
USER_GUIDE_MARKDOWN = (
    REPO_ROOT / "docs" / "user" / "COLLECTOR_MANAGEMENT.md",
    REPO_ROOT / "docs" / "user" / "DEVICE_LEARNING.md",
    REPO_ROOT / "docs" / "user" / "DIAGNOSTIC_COMMANDS.md",
    REPO_ROOT / "docs" / "user" / "INTERFACE_SCREENSHOTS.md",
    REPO_ROOT / "docs" / "user" / "PROXY_CAPTURE.md",
    REPO_ROOT / "docs" / "user" / "REMOTE_SETUP.md",
    REPO_ROOT / "docs" / "user" / "RUNTIME_AND_INVERTER.md",
    REPO_ROOT / "docs" / "user" / "SETUP_AND_DISCOVERY.md",
    REPO_ROOT / "docs" / "user" / "SUPPORT_ARCHIVE.md",
    REPO_ROOT / "docs" / "generated" / "INVERTER_MODEL_CATALOG.generated.md",
)
MAINTAINER_MARKDOWN = (
    REPO_ROOT / "docs" / "maintainer" / "ADDING_DRIVERS.md",
    REPO_ROOT / "docs" / "maintainer" / "GRAPHIFY.md",
    REPO_ROOT / "docs" / "maintainer" / "GRAPHIFY_ARCHITECTURE_AUDIT.md",
    REPO_ROOT / "docs" / "maintainer" / "RELEASING.md",
    REPO_ROOT / "docs" / "maintainer" / "VALIDATION.md",
)
ARCHITECTURE_MARKDOWN = (
    REPO_ROOT / "docs" / "architecture" / "CLOUD_LEARNING_ARCHITECTURE.md",
    REPO_ROOT / "docs" / "architecture" / "CONNECTION_ARCHITECTURE.md",
    REPO_ROOT / "docs" / "architecture" / "TYPED_TELEMETRY.md",
)
DOCUMENTATION_MARKDOWN = tuple(
    sorted(
        (*ROOT_MARKDOWN, *(REPO_ROOT / "docs").rglob("*.md")),
        key=lambda path: path.as_posix(),
    )
)

_MARKDOWN_LINK_RE = re.compile(
    r"!?\[[^\]]*\]\((?P<target><[^>]+>|[^)\s]+)(?:\s+[\"'][^\"']*[\"'])?\)"
)
_HTML_SRC_RE = re.compile(r"<(?:img|source)\b[^>]*\bsrc=[\"'](?P<target>[^\"']+)[\"']", re.I)
_HEADING_RE = re.compile(r"^#{1,6}\s+(?P<title>.+?)\s*#*\s*$", re.MULTILINE)
_EXPLICIT_ANCHOR_RE = re.compile(r"<(?:a|[^>]+)\b(?:id|name)=[\"'](?P<anchor>[^\"']+)[\"']", re.I)

_SCREENSHOT_ROOT = REPO_ROOT / "docs" / "images"

_REQUIRED_USER_GUIDE_MARKERS = {
    REPO_ROOT / "docs" / "user" / "SETUP_AND_DISCOVERY.md": (
        "Background discovery",
        "Pending device",
        "Remote / NAT Setup Guide",
    ),
    REPO_ROOT / "docs" / "user" / "RUNTIME_AND_INVERTER.md": (
        "Fast: first confirmed protocol",
        "Full scan: check all protocols",
        "Control mode",
        "Disabled by the integration",
    ),
    REPO_ROOT / "docs" / "user" / "COLLECTOR_MANAGEMENT.md": (
        "Change collector Wi-Fi",
        "Restart collector",
        "Change inverter UART speed",
        "Cloud + Home Assistant",
        "Home Assistant only",
    ),
    REPO_ROOT / "docs" / "user" / "DEVICE_LEARNING.md": (
        "Analyze device data",
        "Verify additional local controls",
        "Read-only analysis",
    ),
    REPO_ROOT / "docs" / "user" / "SUPPORT_ARCHIVE.md": (
        "Use saved cloud evidence",
        "Fetch or refresh cloud evidence now",
        "Create the archive without cloud evidence",
        "short-lived",
    ),
}


def _github_slug(title: str) -> str:
    """Return the GitHub-style base anchor for one Markdown heading."""

    normalized = unescape(title).strip().lower()
    normalized = re.sub(r"<[^>]+>", "", normalized)
    normalized = re.sub(r"!?\[([^\]]+)\]\([^)]+\)", r"\1", normalized)
    normalized = normalized.replace("`", "")
    normalized = re.sub(r"[^\w\- ]", "", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", "-", normalized)
    return normalized


def _anchors(path: Path) -> frozenset[str]:
    text = path.read_text(encoding="utf-8")
    anchors: set[str] = {
        unquote(match.group("anchor")) for match in _EXPLICIT_ANCHOR_RE.finditer(text)
    }
    seen: defaultdict[str, int] = defaultdict(int)
    for match in _HEADING_RE.finditer(text):
        base = _github_slug(match.group("title"))
        if not base:
            continue
        count = seen[base]
        seen[base] += 1
        anchors.add(base if count == 0 else f"{base}-{count}")
    return frozenset(anchors)


def _targets(text: str) -> tuple[str, ...]:
    return tuple(
        match.group("target").strip("<>")
        for regex in (_MARKDOWN_LINK_RE, _HTML_SRC_RE)
        for match in regex.finditer(text)
    )


def _validate_link(source: Path, line: int, target: str) -> list[str]:
    if not target or target.startswith(("http://", "https://", "mailto:", "data:")):
        return []

    path_text, separator, fragment = target.partition("#")
    path_text = unquote(path_text)
    target_path = source if not path_text else (source.parent / path_text).resolve()
    if not target_path.exists():
        return [f"{source.relative_to(REPO_ROOT)}:{line}: missing target {target}"]
    if separator and fragment and target_path.suffix.lower() == ".md":
        normalized_fragment = unquote(fragment)
        if normalized_fragment not in _anchors(target_path):
            return [
                f"{source.relative_to(REPO_ROOT)}:{line}: missing anchor "
                f"{normalized_fragment!r} in {target_path.relative_to(REPO_ROOT)}"
            ]
    return []


def _indexed_docs() -> tuple[Path, ...]:
    return (*USER_GUIDE_MARKDOWN, *MAINTAINER_MARKDOWN, *ARCHITECTURE_MARKDOWN)


def _referenced_local_paths() -> frozenset[Path]:
    referenced: set[Path] = set()
    for source in DOCUMENTATION_MARKDOWN:
        if not source.exists():
            continue
        for target in _targets(source.read_text(encoding="utf-8")):
            if not target or target.startswith(("http://", "https://", "mailto:", "data:")):
                continue
            path_text = unquote(target.partition("#")[0])
            if path_text:
                referenced.add((source.parent / path_text).resolve())
    return frozenset(referenced)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_screenshot_inventory() -> list[str]:
    """Require every unique screenshot to have documentation context.

    Exact duplicate files are accepted when one copy is documented. Stable
    duplicate paths can remain available to old issue reports without forcing
    the same image to be rendered twice in the current guides.
    """

    screenshots = tuple(sorted(_SCREENSHOT_ROOT.glob("*")))
    referenced = _referenced_local_paths()
    documented_hashes = {
        _sha256(path)
        for path in screenshots
        if path.is_file() and path.resolve() in referenced
    }
    return [
        f"{path.relative_to(REPO_ROOT)}: screenshot has no documentation context"
        for path in screenshots
        if path.is_file()
        and path.resolve() not in referenced
        and _sha256(path) not in documented_hashes
    ]


def validate_public_docs() -> tuple[str, ...]:
    """Return every repository-documentation validation error."""

    errors: list[str] = []
    for source in DOCUMENTATION_MARKDOWN:
        if not source.exists():
            errors.append(f"missing document: {source.relative_to(REPO_ROOT)}")
            continue
        text = source.read_text(encoding="utf-8")
        for target in _targets(text):
            offset = text.find(target)
            line = text.count("\n", 0, offset) + 1 if offset >= 0 else 1
            errors.extend(_validate_link(source, line, target))

    docs_tree = frozenset((REPO_ROOT / "docs").rglob("*.md"))
    classified = frozenset((DOCS_INDEX, *_indexed_docs()))
    for path in sorted(docs_tree - classified):
        errors.append(f"docs/README.md: unclassified document {path.relative_to(REPO_ROOT)}")
    for path in sorted(classified - docs_tree):
        errors.append(f"docs/README.md: classified document is missing {path.relative_to(REPO_ROOT)}")

    if DOCS_INDEX.exists():
        index = DOCS_INDEX.read_text(encoding="utf-8")
        for path in _indexed_docs():
            target = path.relative_to(DOCS_INDEX.parent).as_posix()
            if f"]({target})" not in index:
                errors.append(f"docs/README.md: missing audience index link to {target}")

    for path, markers in _REQUIRED_USER_GUIDE_MARKERS.items():
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker not in text:
                errors.append(
                    f"{path.relative_to(REPO_ROOT)}: missing required user-workflow "
                    f"documentation marker {marker!r}"
                )

    errors.extend(_validate_screenshot_inventory())

    return tuple(errors)


def main() -> int:
    errors = validate_public_docs()
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(
        "Documentation OK: "
        f"{len(DOCUMENTATION_MARKDOWN)} Markdown files and "
        f"{len(tuple(_SCREENSHOT_ROOT.glob('*')))} screenshots checked"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
