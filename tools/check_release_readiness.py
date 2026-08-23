#!/usr/bin/env python3
"""Fail when local release evidence is absent or has replay blockers."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.fixtures.catalog import (  # noqa: E402
    catalog_has_entries,
)
from custom_components.eybond_local.support.release import (  # noqa: E402
    build_release_readiness,
)


async def _run(*, require_local_fixtures: bool) -> int:
    if not catalog_has_entries():
        message = "release-readiness skipped: local fixture catalog is empty"
        if require_local_fixtures:
            print(f"blocked: {message}", file=sys.stderr)
            return 1
        print(message)
        return 0

    report = await build_release_readiness()
    print(
        "release-readiness: "
        f"status={report['status']} "
        f"fixtures={report['summary']['fixtures']} "
        f"ok={report['summary']['validated_ok']} "
        f"mismatch={report['summary']['validated_mismatch']} "
        f"error={report['summary']['validated_error']}"
    )
    for blocker in report["blockers"]:
        print(f"blocked: {blocker}", file=sys.stderr)
    return 1 if report["blockers"] else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--require-local-fixtures",
        action="store_true",
        help="fail instead of skipping when .local/fixtures/catalog is empty",
    )
    args = parser.parse_args()
    return asyncio.run(_run(require_local_fixtures=args.require_local_fixtures))


if __name__ == "__main__":
    raise SystemExit(main())
