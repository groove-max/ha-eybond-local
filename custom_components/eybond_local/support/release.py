"""Helpers for exporting one release-readiness report for the integration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .evidence import build_evidence_index

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "manifest.json"


async def build_release_readiness() -> dict[str, Any]:
    """Build one release-readiness payload from manifest metadata and evidence."""

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    evidence = await build_evidence_index()
    entries = [entry for entry in evidence["entries"] if str(entry.get("evidence_scope", "release")) != "experimental"]
    summary = _build_release_summary(entries)
    blockers = _collect_blockers(entries, summary)
    status = _derive_release_status(entries, blockers)

    return {
        "integration": {
            "domain": manifest.get("domain", ""),
            "name": manifest.get("name", ""),
            "version": manifest.get("version", ""),
        },
        "status": status,
        "blockers": blockers,
        "summary": {
            "drivers": summary["drivers"],
            "profiles": evidence["summary"]["profiles"],
            "fixtures": summary["fixtures"],
            "validated_ok": summary["validated_ok"],
            "validated_mismatch": summary["validated_mismatch"],
            "validated_error": summary["validated_error"],
            "readiness_counts": summary["readiness_counts"],
        },
        "drivers": entries,
    }


def render_release_readiness_markdown(report: dict[str, Any]) -> str:
    """Render a compact Markdown release-readiness report."""

    blockers = report["blockers"]
    lines = [
        "# Release Readiness",
        "",
        "> Generated from manifest metadata and local evidence. Do not edit this export manually.",
        "",
        f"- integration: `{report['integration']['name']}`",
        f"- domain: `{report['integration']['domain']}`",
        f"- version: `{report['integration']['version']}`",
        f"- status: `{report['status']}`",
        f"- drivers: `{report['summary']['drivers']}`",
        f"- fixtures: `{report['summary']['fixtures']}`",
        f"- replay ok: `{report['summary']['validated_ok']}`",
        f"- replay mismatches: `{report['summary']['validated_mismatch']}`",
        f"- replay errors: `{report['summary']['validated_error']}`",
        f"- driver readiness: `{report['summary']['readiness_counts']}`",
        "",
        "## Blockers",
        "",
    ]

    if blockers:
        lines.extend(f"- {item}" for item in blockers)
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Drivers",
            "",
            "| Driver | Profile | Readiness | Tested | Untested | Fixtures | Replay OK | Mismatch | Error |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for entry in report["drivers"]:
        lines.append(
            "| `{driver}` | `{profile}` | `{readiness}` | `{tested}` | `{untested}` | `{fixtures}` | `{ok}` | `{mismatch}` | `{error}` |".format(
                driver=entry["driver_key"],
                profile=entry["profile_key"] or "-",
                readiness=entry["readiness"],
                tested=entry["tested_capabilities"],
                untested=entry["untested_capabilities"],
                fixtures=entry["fixture_count"],
                ok=entry["validated_ok"],
                mismatch=entry["validated_mismatch"],
                error=entry["validated_error"],
            )
        )

    lines.append("")
    return "\n".join(lines)


def _derive_release_status(entries: list[dict[str, Any]], blockers: list[str]) -> str:
    if blockers:
        return "blocked"
    if all(str(entry["readiness"]) == "evidence_backed" for entry in entries):
        return "ready"
    return "incomplete"


def _collect_blockers(entries: list[dict[str, Any]], summary: dict[str, Any]) -> list[str]:
    blockers: list[str] = []

    if int(summary["validated_error"]) > 0:
        blockers.append("Fixture replay currently has at least one error.")
    if int(summary["validated_mismatch"]) > 0:
        blockers.append("Fixture replay currently has at least one metadata mismatch.")

    for entry in entries:
        readiness = str(entry["readiness"])
        driver_key = str(entry["driver_key"])
        if readiness == "needs_attention":
            blockers.append(f"Driver {driver_key} has fixtures that need attention.")
        elif readiness == "unmapped":
            blockers.append(f"Driver {driver_key} has fixture evidence but no declarative profile mapping.")

    return blockers


def _build_release_summary(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "drivers": len(entries),
        "fixtures": sum(int(entry.get("fixture_count", 0)) for entry in entries),
        "validated_ok": sum(int(entry.get("validated_ok", 0)) for entry in entries),
        "validated_mismatch": sum(int(entry.get("validated_mismatch", 0)) for entry in entries),
        "validated_error": sum(int(entry.get("validated_error", 0)) for entry in entries),
        "readiness_counts": _count_readiness(entries),
    }


def _count_readiness(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        readiness = str(entry.get("readiness", ""))
        counts[readiness] = counts.get(readiness, 0) + 1
    return dict(sorted(counts.items()))
