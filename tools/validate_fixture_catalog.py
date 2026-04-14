#!/usr/bin/env python3
"""Replay and validate every imported fixture in the repository catalog."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.const import DRIVER_HINT_AUTO  # noqa: E402
from custom_components.eybond_local.fixtures.catalog import (  # noqa: E402
    rebuild_catalog_index,
)
from custom_components.eybond_local.fixtures.validation import (  # noqa: E402
    build_fixture_validation_overview,
)


async def _run(args: argparse.Namespace) -> int:
    selected_slugs = set(args.slug or [])
    overview = await build_fixture_validation_overview(
        driver_hint=args.driver_hint,
        selected_slugs=selected_slugs,
    )

    if args.rebuild_index:
        rebuild_catalog_index()

    print(json.dumps(overview, ensure_ascii=False, indent=2, sort_keys=True))
    ok = all(entry["status"] == "ok" for entry in overview["entries"])
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver-hint", default=DRIVER_HINT_AUTO)
    parser.add_argument("--slug", action="append", help="restrict validation to one slug")
    parser.add_argument("--rebuild-index", action="store_true")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
