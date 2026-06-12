#!/usr/bin/env python3
"""Vet a device contribution record (or build one from a support archive).

Usage:
  tools/vet_contribution.py record.json
  tools/vet_contribution.py --from-archive support_package.zip
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import zipfile

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.contribution_record import (  # noqa: E402
    build_contribution_record,
)
from custom_components.eybond_local.support.contribution_vetting import (  # noqa: E402
    RESULT_FAIL,
    vet_contribution_record,
)


def _record_from_archive(archive_path: Path) -> dict:
    """Build a contribution record from an existing support-package zip."""

    with zipfile.ZipFile(archive_path) as archive:
        bundle = json.loads(archive.read("support_bundle.json"))
    runtime_values = ((bundle.get("runtime") or {}).get("values")) or {}
    fingerprint = runtime_values.get("device_catalog") or {}
    overlay = _find_overlay_manifest(bundle)
    return build_contribution_record(
        fingerprint=fingerprint,
        manifest=overlay,
        collector_version=str(runtime_values.get("smartess_collector_version") or ""),
    )


def _find_overlay_manifest(bundle: dict) -> dict:
    evidence = bundle.get("evidence") if isinstance(bundle.get("evidence"), dict) else {}
    shadow = evidence.get("shadow_learning") if isinstance(evidence.get("shadow_learning"), dict) else {}
    manifest = shadow.get("generated_overlay_manifest")
    if isinstance(manifest, dict):
        return manifest
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="contribution record JSON, or a support package zip with --from-archive")
    parser.add_argument(
        "--from-archive",
        action="store_true",
        help="build the record from a support package zip before vetting",
    )
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args()

    path = Path(args.path)
    if args.from_archive:
        record = _record_from_archive(path)
    else:
        record = json.loads(path.read_text(encoding="utf-8"))

    report = vet_contribution_record(record)
    if args.json:
        print(json.dumps(report.to_json_dict(), ensure_ascii=False, indent=2))
    else:
        print(f"verdict: {report.verdict}")
        for check in report.checks:
            line = f"  [{check.result}] {check.name}"
            if check.detail:
                line += f" — {check.detail}"
            print(line)
    return 1 if report.verdict == RESULT_FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
