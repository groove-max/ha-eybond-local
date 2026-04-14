"""Helpers for exporting combined support evidence across profiles and local fixtures."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from ..fixtures.coverage import build_fixture_coverage_overview
from ..fixtures.validation import build_fixture_validation_overview
from .overview import build_support_overview


async def build_evidence_index() -> dict[str, Any]:
    """Build one combined evidence index across profiles and local fixtures."""

    support = build_support_overview()
    coverage = build_fixture_coverage_overview()
    validation = await build_fixture_validation_overview()

    coverage_by_driver: dict[str, list[dict[str, Any]]] = defaultdict(list)
    validation_by_driver: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in coverage["entries"]:
        coverage_by_driver[str(entry["driver_key"])].append(entry)
    for entry in validation["entries"]:
        validation_by_driver[str(entry.get("driver_key", "unknown"))].append(entry)

    rows: list[dict[str, Any]] = []
    seen_drivers: set[str] = set()

    for profile in support["profiles"]:
        driver_key = str(profile["driver_key"])
        seen_drivers.add(driver_key)
        driver_coverage = coverage_by_driver.get(driver_key, [])
        driver_validation = validation_by_driver.get(driver_key, [])
        validation_counts = _count_validation_statuses(driver_validation)
        evidence_scope = _derive_driver_evidence_scope(driver_coverage)
        rows.append(
            {
                "profile_key": profile["profile_key"],
                "title": profile["title"],
                "driver_key": driver_key,
                "protocol_family": profile["protocol_family"],
                "evidence_scope": evidence_scope,
                "capabilities": profile["capabilities"],
                "tested_capabilities": int(profile["validation_state_counts"].get("tested", 0)),
                "untested_capabilities": int(profile["validation_state_counts"].get("untested", 0)),
                "blocked_capabilities": int(profile["support_tier_counts"].get("blocked", 0)),
                "fixture_count": len(driver_coverage),
                "validated_ok": validation_counts["ok"],
                "validated_mismatch": validation_counts["mismatch"],
                "validated_error": validation_counts["error"],
                "models": sorted({str(item["model_name"]) for item in driver_coverage}),
                "collector_profiles": sorted(
                    {str(item["collector_profile_key"]) for item in driver_coverage}
                ),
                "readiness": _derive_readiness(
                    evidence_scope=evidence_scope,
                    fixture_count=len(driver_coverage),
                    ok_count=validation_counts["ok"],
                    mismatch_count=validation_counts["mismatch"],
                    error_count=validation_counts["error"],
                    tested_count=int(profile["validation_state_counts"].get("tested", 0)),
                ),
            }
        )

    for driver_key, driver_coverage in coverage_by_driver.items():
        if driver_key in seen_drivers:
            continue
        driver_validation = validation_by_driver.get(driver_key, [])
        validation_counts = _count_validation_statuses(driver_validation)
        evidence_scope = _derive_driver_evidence_scope(driver_coverage)
        rows.append(
            {
                "profile_key": "",
                "title": "Unmapped Fixture Coverage",
                "driver_key": driver_key,
                "protocol_family": driver_coverage[0].get("protocol_family", ""),
                "evidence_scope": evidence_scope,
                "capabilities": 0,
                "tested_capabilities": 0,
                "untested_capabilities": 0,
                "blocked_capabilities": 0,
                "fixture_count": len(driver_coverage),
                "validated_ok": validation_counts["ok"],
                "validated_mismatch": validation_counts["mismatch"],
                "validated_error": validation_counts["error"],
                "models": sorted({str(item["model_name"]) for item in driver_coverage}),
                "collector_profiles": sorted(
                    {str(item["collector_profile_key"]) for item in driver_coverage}
                ),
                "readiness": "experimental" if evidence_scope == "experimental" else "unmapped",
            }
        )

    rows.sort(key=lambda item: (item["driver_key"], item["profile_key"], item["title"]))
    return {
        "entries": rows,
        "summary": {
            "drivers": len(rows),
            "profiles": support["summary"]["profiles"],
            "fixtures": coverage["summary"]["fixtures"],
            "validated_ok": sum(int(item["validated_ok"]) for item in rows),
            "validated_mismatch": sum(int(item["validated_mismatch"]) for item in rows),
            "validated_error": sum(int(item["validated_error"]) for item in rows),
            "readiness_counts": _count_readiness(rows),
        },
    }


def render_evidence_index_markdown(index: dict[str, Any]) -> str:
    """Render a compact Markdown export from one evidence index payload."""

    lines = [
        "# Driver Evidence Index",
        "",
        "> Generated from declarative profiles, local fixture coverage, and replay validation. Do not edit this export manually.",
        "",
        f"- drivers: `{index['summary']['drivers']}`",
        f"- profiles: `{index['summary']['profiles']}`",
        f"- fixtures: `{index['summary']['fixtures']}`",
        f"- replay ok: `{index['summary']['validated_ok']}`",
        f"- replay mismatches: `{index['summary']['validated_mismatch']}`",
        f"- replay errors: `{index['summary']['validated_error']}`",
        f"- readiness: `{index['summary']['readiness_counts']}`",
        "",
        "| Driver | Profile | Family | Tested | Untested | Fixtures | Replay OK | Mismatch | Error | Readiness | Models |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]

    for entry in index["entries"]:
        models = ", ".join(entry["models"])
        lines.append(
            "| `{driver}` | `{profile}` | `{family}` | `{tested}` | `{untested}` | `{fixtures}` | `{ok}` | `{mismatch}` | `{error}` | `{readiness}` | {models} |".format(
                driver=entry["driver_key"],
                profile=entry["profile_key"] or "-",
                family=entry["protocol_family"],
                tested=entry["tested_capabilities"],
                untested=entry["untested_capabilities"],
                fixtures=entry["fixture_count"],
                ok=entry["validated_ok"],
                mismatch=entry["validated_mismatch"],
                error=entry["validated_error"],
                readiness=entry["readiness"],
                models=models or "",
            )
        )

    lines.append("")
    return "\n".join(lines)


def _count_validation_statuses(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"ok": 0, "mismatch": 0, "error": 0}
    for entry in entries:
        status = str(entry.get("status", ""))
        if status in counts:
            counts[status] += 1
    return counts


def _count_readiness(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        key = str(entry["readiness"])
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _derive_readiness(
    *,
    evidence_scope: str,
    fixture_count: int,
    ok_count: int,
    mismatch_count: int,
    error_count: int,
    tested_count: int,
) -> str:
    if evidence_scope == "experimental":
        return "experimental"
    if fixture_count == 0:
        return "profile_only"
    if error_count or mismatch_count:
        return "needs_attention"
    if ok_count == fixture_count and tested_count > 0:
        return "evidence_backed"
    return "partial"


def _derive_driver_evidence_scope(entries: list[dict[str, Any]]) -> str:
    scopes = {str(entry.get("evidence_scope", "release")) for entry in entries}
    if not scopes:
        return "release"
    if scopes == {"experimental"}:
        return "experimental"
    return "release"
