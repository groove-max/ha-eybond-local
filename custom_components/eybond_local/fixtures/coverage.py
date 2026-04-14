"""Helpers for exporting local fixture coverage from the catalog."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .catalog import iter_catalog_entries, load_catalog_metadata


def build_fixture_coverage_overview() -> dict[str, Any]:
    """Build one machine-readable overview from the local fixture catalog."""

    rows: list[dict[str, Any]] = []
    driver_counts: Counter[str] = Counter()
    model_counts: Counter[str] = Counter()
    collector_counts: Counter[str] = Counter()
    anonymized_count = 0

    for entry in iter_catalog_entries():
        metadata = load_catalog_metadata(entry)
        driver_key = str(metadata.get("driver_key", "unknown"))
        model_name = str(metadata.get("model_name", "unknown"))
        collector_profile_key = str(metadata.get("collector_profile_key", "unknown"))
        anonymized = bool(metadata.get("anonymized", False))
        if anonymized:
            anonymized_count += 1

        driver_counts[driver_key] += 1
        model_counts[model_name] += 1
        collector_counts[collector_profile_key] += 1
        rows.append(
            {
                "slug": metadata.get("slug", entry.slug),
                "title": metadata.get("title", entry.slug),
                "driver_key": driver_key,
                "protocol_family": str(metadata.get("protocol_family", "")),
                "evidence_scope": str(metadata.get("evidence_scope", "release")),
                "model_name": model_name,
                "collector_profile_key": collector_profile_key,
                "anonymized": anonymized,
                "ranges": list(metadata.get("ranges", [])),
                "capture_kind": str(metadata.get("capture_kind", "ranges")),
                "capture_count": int(metadata.get("capture_count", len(metadata.get("ranges", [])))),
                "sample_values": dict(metadata.get("sample_values", {})),
            }
        )

    rows.sort(key=lambda item: (item["model_name"], item["driver_key"], item["slug"]))
    return {
        "entries": rows,
        "summary": {
            "fixtures": len(rows),
            "drivers": dict(sorted(driver_counts.items())),
            "models": dict(sorted(model_counts.items())),
            "collector_profiles": dict(sorted(collector_counts.items())),
            "anonymized_fixtures": anonymized_count,
        },
    }


def render_fixture_coverage_markdown(overview: dict[str, Any]) -> str:
    """Render a compact Markdown export from one fixture coverage payload."""

    lines = [
        "# Fixture Coverage Overview",
        "",
        "> Generated from local fixture catalog metadata. Do not edit this export manually.",
        "",
        f"- fixtures: `{overview['summary']['fixtures']}`",
        f"- drivers: `{overview['summary']['drivers']}`",
        f"- models: `{overview['summary']['models']}`",
        f"- collector profiles: `{overview['summary']['collector_profiles']}`",
        f"- anonymized fixtures: `{overview['summary']['anonymized_fixtures']}`",
        "",
        "| Fixture | Driver | Model | Collector Profile | Scope | Anonymized | Evidence | Sample |",
        "|---|---|---|---|---|---:|---|---|",
    ]

    for entry in overview["entries"]:
        sample = ", ".join(
            f"{key}={value}" for key, value in sorted(entry["sample_values"].items())
        )
        lines.append(
            "| `{title}` | `{driver}` | `{model}` | `{collector}` | `{scope}` | `{anonymized}` | `{evidence}` | {sample} |".format(
                title=entry["title"],
                driver=entry["driver_key"],
                model=entry["model_name"],
                collector=entry["collector_profile_key"],
                scope=entry["evidence_scope"],
                anonymized=str(entry["anonymized"]).lower(),
                evidence=f"{entry['capture_kind']}={entry['capture_count']}",
                sample=sample or "",
            )
        )

    lines.append("")
    return "\n".join(lines)
