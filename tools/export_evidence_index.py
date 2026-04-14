#!/usr/bin/env python3
"""Export combined driver evidence index as JSON or Markdown."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.evidence import (  # noqa: E402
    build_evidence_index,
    render_evidence_index_markdown,
)


async def _run(args: argparse.Namespace) -> int:
    index = await build_evidence_index()
    if args.format == "markdown":
        rendered = render_evidence_index_markdown(index)
    else:
        rendered = json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True)

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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
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
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
