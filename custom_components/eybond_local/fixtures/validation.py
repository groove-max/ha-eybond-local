"""Helpers for validating local fixtures through the replay stack."""

from __future__ import annotations

from collections import Counter
from typing import Any

from ..const import DRIVER_HINT_AUTO
from .catalog import CatalogEntryPaths, iter_catalog_entries, load_catalog_metadata
from .replay import detect_fixture_path, read_fixture_values


def metadata_mismatches(metadata: dict[str, object], context: Any) -> list[str]:
    """Compare one detected replay context against checked-in catalog metadata."""

    mismatches: list[str] = []
    if metadata.get("driver_key") != context.inverter.driver_key:
        mismatches.append(
            f"driver_key expected={metadata.get('driver_key')!r} actual={context.inverter.driver_key!r}"
        )
    if metadata.get("protocol_family") != context.inverter.protocol_family:
        mismatches.append(
            "protocol_family "
            f"expected={metadata.get('protocol_family')!r} actual={context.inverter.protocol_family!r}"
        )
    if metadata.get("model_name") != context.inverter.model_name:
        mismatches.append(
            f"model_name expected={metadata.get('model_name')!r} actual={context.inverter.model_name!r}"
        )
    return mismatches


async def validate_catalog_entry(
    entry: CatalogEntryPaths,
    *,
    driver_hint: str = DRIVER_HINT_AUTO,
) -> dict[str, object]:
    """Replay one fixture catalog entry and return one validation result row."""

    metadata = load_catalog_metadata(entry)
    try:
        context = await detect_fixture_path(entry.fixture_path, driver_hint=driver_hint)
        values = await read_fixture_values(context)
        mismatches = metadata_mismatches(metadata, context)
        return {
            "slug": entry.slug,
            "status": "ok" if not mismatches else "mismatch",
            "fixture_path": str(entry.fixture_path),
            "metadata_path": str(entry.metadata_path),
            "driver_key": context.inverter.driver_key,
            "protocol_family": context.inverter.protocol_family,
            "model_name": context.inverter.model_name,
            "serial_number": context.inverter.serial_number,
            "operating_mode": values.get("operating_mode"),
            "warning_code": values.get("warning_code"),
            "mismatches": mismatches,
        }
    except Exception as exc:
        return {
            "slug": entry.slug,
            "status": "error",
            "fixture_path": str(entry.fixture_path),
            "metadata_path": str(entry.metadata_path),
            "error": str(exc),
        }


async def build_fixture_validation_overview(
    *,
    driver_hint: str = DRIVER_HINT_AUTO,
    selected_slugs: set[str] | None = None,
) -> dict[str, Any]:
    """Replay all selected catalog entries and return one validation overview."""

    entries: list[dict[str, object]] = []
    status_counts: Counter[str] = Counter()
    driver_counts: Counter[str] = Counter()

    for entry in iter_catalog_entries():
        if selected_slugs and entry.slug not in selected_slugs:
            continue
        result = await validate_catalog_entry(entry, driver_hint=driver_hint)
        entries.append(result)
        status_counts.update([str(result["status"])])
        if result.get("driver_key"):
            driver_counts.update([str(result["driver_key"])])

    entries.sort(key=lambda item: str(item["slug"]))
    return {
        "entries": entries,
        "summary": {
            "fixtures": len(entries),
            "status_counts": dict(sorted(status_counts.items())),
            "drivers": dict(sorted(driver_counts.items())),
        },
    }


def render_fixture_validation_markdown(overview: dict[str, Any]) -> str:
    """Render a compact Markdown export from one fixture validation overview."""

    lines = [
        "# Fixture Validation Overview",
        "",
        "> Generated from replaying local fixtures through the current driver stack. Do not edit this export manually.",
        "",
        f"- fixtures: `{overview['summary']['fixtures']}`",
        f"- statuses: `{overview['summary']['status_counts']}`",
        f"- drivers: `{overview['summary']['drivers']}`",
        "",
        "| Fixture | Status | Driver | Model | Operating Mode | Warning Code | Notes |",
        "|---|---|---|---|---|---:|---|",
    ]

    for entry in overview["entries"]:
        notes = ""
        if entry["status"] == "mismatch":
            notes = "; ".join(str(item) for item in entry.get("mismatches", []))
        elif entry["status"] == "error":
            notes = str(entry.get("error", ""))
        lines.append(
            "| `{slug}` | `{status}` | `{driver}` | `{model}` | `{mode}` | `{warning}` | {notes} |".format(
                slug=entry["slug"],
                status=entry["status"],
                driver=entry.get("driver_key", ""),
                model=entry.get("model_name", ""),
                mode=entry.get("operating_mode", ""),
                warning=entry.get("warning_code", ""),
                notes=notes,
            )
        )

    lines.append("")
    return "\n".join(lines)
