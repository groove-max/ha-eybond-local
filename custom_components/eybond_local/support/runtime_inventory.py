"""Build an internal inventory of profiles referenced by runtime surfaces."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.profile_loader import load_driver_profile
from .matrix import build_profile_support_matrix


def runtime_profile_names() -> tuple[str, ...]:
    """Return unique non-empty profile names used by compiled runtime surfaces."""

    catalog = load_compiled_detection_catalog()
    return tuple(
        sorted(
            {
                surface.profile_name
                for surface in catalog.surfaces.values()
                if surface.profile_name
            }
        )
    )


def build_runtime_profile_inventory(
    profile_names: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Build machine-readable capability totals for runtime-referenced profiles."""

    selected_profiles = (
        tuple(profile_names) if profile_names is not None else runtime_profile_names()
    )
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
                "implementation_title": matrix.get(
                    "implementation_title", profile.title
                ),
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
