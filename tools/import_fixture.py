#!/usr/bin/env python3
"""Import a fixture into the repository catalog with standard metadata."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.const import DRIVER_HINT_AUTO  # noqa: E402
from custom_components.eybond_local.fixtures.catalog import (  # noqa: E402
    FIXTURE_META_SCHEMA_VERSION,
    catalog_entry_paths,
    ensure_catalog_dir,
    rebuild_catalog_index,
    save_catalog_metadata,
    slugify_fixture_name,
)
from custom_components.eybond_local.fixtures.replay import (  # noqa: E402
    detect_fixture_payload,
    read_fixture_values,
)
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


async def _run(args: argparse.Namespace) -> int:
    ensure_catalog_dir()
    raw = load_fixture_json(args.input)

    serial_ranges: list[tuple[int, int]] = []
    if not args.no_default_smg_serial_range:
        serial_ranges.extend(DEFAULT_SMG_SERIAL_RANGES)
    if args.serial_range:
        serial_ranges.extend(args.serial_range)

    processed = raw
    if not args.keep_identifiers and not bool(raw.get("anonymized", False)):
        processed = anonymize_fixture_json(
            raw,
            serial_ranges=tuple(serial_ranges),
            keep_name=args.keep_name,
        )

    context = await detect_fixture_payload(processed, driver_hint=args.driver_hint)
    values = await read_fixture_values(context)

    default_title = str(processed.get("name", args.input.stem))
    title = args.title or default_title
    slug = slugify_fixture_name(args.slug or title)
    entry = catalog_entry_paths(slug)

    if entry.directory.exists() and not args.force:
        raise SystemExit(f"catalog entry already exists: {entry.directory}")

    entry.directory.mkdir(parents=True, exist_ok=True)
    save_fixture_json(entry.fixture_path, processed)

    metadata = {
        "schema_version": FIXTURE_META_SCHEMA_VERSION,
        "slug": slug,
        "title": title,
        "fixture_file": entry.fixture_path.name,
        "source_file": args.input.name,
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "anonymized": bool(processed.get("anonymized", False)),
        "driver_key": context.inverter.driver_key,
        "protocol_family": context.inverter.protocol_family,
        "model_name": context.inverter.model_name,
        "serial_number": context.inverter.serial_number,
        "collector_profile_key": str(processed.get("collector", {}).get("profile_key", "")),
        "probe_target": {
            "devcode": context.inverter.probe_target.devcode,
            "collector_addr": context.inverter.probe_target.collector_addr,
            "device_addr": context.inverter.probe_target.device_addr,
        },
        "ranges": [
            {
                "start": int(item["start"]),
                "count": int(item["count"]),
            }
            for item in processed.get("ranges", [])
        ],
        "notes": args.notes,
        "sample_values": {
            "operating_mode": values.get("operating_mode"),
            "output_source_priority": values.get("output_source_priority"),
            "charge_source_priority": values.get("charge_source_priority"),
            "warning_code": values.get("warning_code"),
        },
    }
    save_catalog_metadata(entry, metadata)
    index_path = rebuild_catalog_index()

    print(
        json.dumps(
            {
                "entry_dir": str(entry.directory),
                "fixture_path": str(entry.fixture_path),
                "metadata_path": str(entry.metadata_path),
                "index_path": str(index_path),
                "metadata": metadata,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--slug", default="")
    parser.add_argument("--title", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--driver-hint", default=DRIVER_HINT_AUTO)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--keep-identifiers", action="store_true")
    parser.add_argument("--keep-name", action="store_true")
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
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
