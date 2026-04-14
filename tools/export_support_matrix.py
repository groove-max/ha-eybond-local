#!/usr/bin/env python3
"""Export one declarative driver support matrix as JSON or Markdown."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.metadata.profile_loader import load_driver_profile  # noqa: E402
from custom_components.eybond_local.support.matrix import (  # noqa: E402
    build_profile_support_matrix,
    render_support_matrix_markdown,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        default="smg_modbus.json",
        help="profile JSON file name from custom_components/eybond_local/profiles/",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="output format",
    )
    parser.add_argument(
        "--output",
        help="optional output file path; prints to stdout when omitted",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="check that --output already matches the current generated content",
    )
    args = parser.parse_args()

    if args.check and not args.output:
        parser.error("--check requires --output")

    profile = load_driver_profile(args.profile)
    matrix = build_profile_support_matrix(profile)
    rendered: str
    if args.format == "markdown":
        rendered = render_support_matrix_markdown(matrix)
    else:
        rendered = json.dumps(matrix, ensure_ascii=False, indent=2, sort_keys=True)

    if args.output:
        output_path = Path(args.output).expanduser()
        expected = rendered + ("\n" if not rendered.endswith("\n") else "")

        if args.check:
            if not output_path.exists():
                print(f"missing:{output_path}")
                return 1
            current = output_path.read_text(encoding="utf-8")
            if current != expected:
                print(f"out_of_sync:{output_path}")
                return 1
            print(f"in_sync:{output_path}")
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(expected, encoding="utf-8")
        print(output_path)
        return 0

    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
