#!/usr/bin/env python3
"""Anonymize a saved fixture so it can be shared safely."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.fixtures.utils import (  # noqa: E402
    DEFAULT_SMG_SERIAL_RANGES,
    anonymize_fixture_json,
    load_fixture_json,
    save_fixture_json,
)


def _parse_range(value: str) -> tuple[int, int]:
    try:
        start_raw, count_raw = value.split(":", 1)
        return int(start_raw, 0), int(count_raw, 0)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid range '{value}', expected START:COUNT") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--serial-range",
        action="append",
        type=_parse_range,
        help="additional ASCII register range START:COUNT to pseudonymize",
    )
    parser.add_argument(
        "--no-default-smg-serial-range",
        action="store_true",
        help="disable the default SMG serial block redaction at 186:12",
    )
    parser.add_argument("--keep-name", action="store_true")
    args = parser.parse_args()

    serial_ranges: list[tuple[int, int]] = []
    if not args.no_default_smg_serial_range:
        serial_ranges.extend(DEFAULT_SMG_SERIAL_RANGES)
    if args.serial_range:
        serial_ranges.extend(args.serial_range)

    fixture = load_fixture_json(args.input)
    anonymized = anonymize_fixture_json(
        fixture,
        serial_ranges=tuple(serial_ranges),
        keep_name=args.keep_name,
    )
    save_fixture_json(args.output, anonymized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
