#!/usr/bin/env python3
"""Render GitHub release notes for one version from CHANGELOG.md."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
CHANGELOG_PATH = REPO_ROOT / "CHANGELOG.md"
VERSION_HEADER_RE = re.compile(r"^## \[(?P<version>[^\]]+)\](?: - .+)?$", re.MULTILINE)


def normalize_version(version: str) -> str:
    normalized = version.strip()
    if normalized.startswith("v"):
        normalized = normalized[1:]
    return normalized


def extract_release_notes(changelog_text: str, version: str) -> str:
    target = normalize_version(version)
    matches = list(VERSION_HEADER_RE.finditer(changelog_text))

    for index, match in enumerate(matches):
        if normalize_version(match.group("version")) != target:
            continue

        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(changelog_text)
        body = changelog_text[start:end].strip()
        if not body:
            raise ValueError(f"Changelog section for version {target} is empty")
        return body + "\n"

    raise ValueError(f"Version {target} was not found in {CHANGELOG_PATH.name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="version or tag, for example 0.1.43 or v0.1.43")
    parser.add_argument(
        "--changelog",
        default=str(CHANGELOG_PATH),
        help="path to the changelog file",
    )
    parser.add_argument(
        "--output",
        help="optional output file path; prints to stdout when omitted",
    )
    args = parser.parse_args()

    changelog_path = Path(args.changelog).expanduser()
    try:
        changelog_text = changelog_path.read_text(encoding="utf-8")
        rendered = extract_release_notes(changelog_text, args.version)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.output:
        output_path = Path(args.output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(output_path)
        return 0

    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())