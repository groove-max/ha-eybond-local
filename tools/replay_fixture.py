#!/usr/bin/env python3
"""Replay one saved fixture through the real driver stack without live hardware."""

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
from custom_components.eybond_local.fixtures.replay import (  # noqa: E402
    apply_fixture_preset,
    build_fixture_snapshot,
    detect_fixture_path,
    read_fixture_values,
)


async def _run(args: argparse.Namespace) -> int:
    try:
        context = await detect_fixture_path(args.fixture, driver_hint=args.driver_hint)
    except RuntimeError:
        print(json.dumps({"error": "no_supported_driver_matched"}, ensure_ascii=False, indent=2))
        return 1

    if args.preset:
        payload = await apply_fixture_preset(context, args.preset)
    elif args.key:
        if args.value is None:
            raise SystemExit("--value is required when --key is used")
        written_value = await context.driver.async_write_capability(
            context.transport,
            context.inverter,
            args.key,
            args.value,
        )
        values = await read_fixture_values(context)
        payload = {
            "write": {
                "key": args.key,
                "requested_value": args.value,
                "written_value": written_value,
                "current_value": values.get(args.key),
            }
        }
    else:
        values = await read_fixture_values(context)
        payload = build_fixture_snapshot(
            context,
            values=values,
            full_snapshot=args.full_snapshot,
        )

    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--driver-hint", default=DRIVER_HINT_AUTO)
    parser.add_argument("--full-snapshot", action="store_true")
    parser.add_argument("--preset", default="")
    parser.add_argument("--key", default="")
    parser.add_argument("--value")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
