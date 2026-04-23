"""Helpers for exporting project-wide support overview from declarative profiles."""

from __future__ import annotations

from collections import Counter
from typing import Any

from ..metadata.profile_loader import load_driver_profile
from ..runtime_labels import runtime_profile_label
from .matrix import build_profile_support_matrix


_DEFAULT_SUPPORT_OVERVIEW_PROFILES = (
    "pi30_ascii.json",
    "smg_modbus.json",
    "modbus_smg/models/anenji_4200_protocol_1.json",
    "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
    "modbus_smg/family_fallback.json",
)


def build_support_overview(profile_names: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Build one machine-readable support overview across all declarative profiles."""

    selected_profiles = profile_names or _DEFAULT_SUPPORT_OVERVIEW_PROFILES
    profile_rows: list[dict[str, Any]] = []
    total_capabilities = 0
    validation_counts: Counter[str] = Counter()
    support_tier_counts: Counter[str] = Counter()

    for profile_name in selected_profiles:
        profile = load_driver_profile(profile_name)
        matrix = build_profile_support_matrix(profile)
        summary = matrix["summary"]
        total_capabilities += int(summary["capabilities"])
        validation_counts.update(summary["validation_state_counts"])
        support_tier_counts.update(summary["support_tier_counts"])
        profile_rows.append(
            {
                "profile_name": profile_name,
                "profile_key": matrix["profile_key"],
                "title": matrix["title"],
                "implementation_title": matrix.get("implementation_title", profile.title),
                "driver_key": profile.driver_key,
                "protocol_family": profile.protocol_family,
                "capabilities": summary["capabilities"],
                "validation_state_counts": summary["validation_state_counts"],
                "support_tier_counts": summary["support_tier_counts"],
                "group_counts": summary["group_counts"],
            }
        )

    profile_rows.sort(key=lambda item: (item["title"], item["profile_name"]))
    return {
        "profiles": profile_rows,
        "summary": {
            "profiles": len(profile_rows),
            "capabilities": total_capabilities,
            "validation_state_counts": dict(sorted(validation_counts.items())),
            "support_tier_counts": dict(sorted(support_tier_counts.items())),
        },
    }


def render_support_overview_markdown(overview: dict[str, Any]) -> str:
    """Render a compact Markdown export from one support overview payload."""

    lines = [
        "# Project Runtime Profile Overview",
        "",
        "> Generated from declarative profile metadata. This is an implementation-level profile report, not a commercial hardware compatibility list. Do not edit this export manually.",
        "",
        f"- profiles: `{overview['summary']['profiles']}`",
        f"- capabilities: `{overview['summary']['capabilities']}`",
        f"- validation states: `{overview['summary']['validation_state_counts']}`",
        f"- support tiers: `{overview['summary']['support_tier_counts']}`",
        "",
        "| Runtime Profile | Profile Key | Runtime Path Key | Family Key | Capabilities | Tested | Untested | Conditional | Blocked |",
        "|---|---|---|---|---:|---:|---:|---:|---:|",
    ]

    for profile in overview["profiles"]:
        validation = profile["validation_state_counts"]
        support = profile["support_tier_counts"]
        lines.append(
            "| `{title}` | `{key}` | `{driver}` | `{family}` | `{capabilities}` | `{tested}` | `{untested}` | `{conditional}` | `{blocked}` |".format(
                title=profile["title"],
                key=profile["profile_key"],
                driver=profile["driver_key"],
                family=profile["protocol_family"],
                capabilities=profile["capabilities"],
                tested=validation.get("tested", 0),
                untested=validation.get("untested", 0),
                conditional=support.get("conditional", 0),
                blocked=support.get("blocked", 0),
            )
        )

    lines.append("")
    return "\n".join(lines)
