#!/usr/bin/env python3
"""Read-only ValueCloud (SmartValue) probe.

Fetches one device bundle from the SmartValue backend for a collector PN and
prints the identity fields the local integration needs (devcode / devaddr)
plus the normalized cloud readings.

READ-ONLY: login + listTDeviceInfo + querySPDeviceLastData + read-only field
reads. It never writes to the collector, never reroutes it and never sends a
control command to the inverter.

Usage:
    SMARTVALUE_USER='you@example.com' SMARTVALUE_PASS='secret' \\
        python3 tools/valuecloud_probe.py I30000200000000001

    # optional: dump the full normalized bundle
    ... python3 tools/valuecloud_probe.py I30000200000000001 --dump bundle.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.valuecloud_cloud import (  # noqa: E402
    DEFAULT_BASE_URL,
    ValueCloudError,
    fetch_device_bundle_for_collector,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collector_pn", help="collector PN, e.g. I30000200000000001")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("SMARTVALUE_BASE_URL") or DEFAULT_BASE_URL,
        help=f"ValueCloud API base url (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--language",
        default=os.environ.get("SMARTVALUE_LANG", "en_US"),
        help="i18n/language header (default: en_US)",
    )
    parser.add_argument(
        "--dump",
        metavar="PATH",
        help="write the full normalized bundle JSON to PATH",
    )
    args = parser.parse_args(argv)

    username = os.environ.get("SMARTVALUE_USER", "").strip()
    password = os.environ.get("SMARTVALUE_PASS", "")
    if not username or not password:
        print(
            "error: set SMARTVALUE_USER and SMARTVALUE_PASS in the environment",
            file=sys.stderr,
        )
        return 2

    try:
        bundle = fetch_device_bundle_for_collector(
            username=username,
            password=password,
            collector_pn=args.collector_pn,
            base_url=args.base_url,
            language=args.language,
        )
    except ValueCloudError as exc:
        diagnostics = exc.safe_diagnostics() if hasattr(exc, "safe_diagnostics") else {}
        print(f"error: {exc}", file=sys.stderr)
        print(json.dumps(diagnostics, indent=2, ensure_ascii=False), file=sys.stderr)
        return 1

    identity = bundle.get("device_identity") or {}
    print("device_identity:")
    for key in ("pn", "sn", "devcode", "devaddr"):
        value = identity.get(key)
        if isinstance(value, int):
            print(f"  {key:<9} {value}  (0x{value:04X})")
        else:
            print(f"  {key:<9} {value!r}")

    detail = (
        (bundle.get("device_detail") or {}).get("normalized")
        if isinstance(bundle.get("device_detail"), dict)
        else None
    )
    fields = (detail or {}).get("fields") or []
    if fields:
        print(f"\nreadings ({len(fields)} fields):")
        for field in fields[:80]:
            name = field.get("name") or field.get("field_key") or "?"
            value = field.get("value")
            unit = field.get("unit") or ""
            print(f"  {name:<38} {value} {unit}".rstrip())

    if args.dump:
        Path(args.dump).write_text(
            json.dumps(bundle, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"\nfull bundle written to {args.dump}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
