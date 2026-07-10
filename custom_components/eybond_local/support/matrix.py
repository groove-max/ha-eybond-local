"""Helpers for exporting capability support metadata from declarative profiles."""

from __future__ import annotations

from collections import Counter
from typing import Any

from ..metadata.profile_loader import DriverProfileMetadata
from ..runtime_labels import runtime_profile_label


def build_profile_support_matrix(profile: DriverProfileMetadata) -> dict[str, Any]:
    """Build a machine-readable support matrix from one loaded driver profile."""

    display_title = runtime_profile_label(
        profile_key=profile.key,
        driver_key=profile.driver_key,
        title=profile.title,
    )
    capabilities: list[dict[str, Any]] = []
    for capability in sorted(
        profile.capabilities,
        key=lambda item: (item.group, item.order, item.display_name),
    ):
        capabilities.append(
            {
                "key": capability.key,
                "title": capability.display_name,
                "group": capability.group,
                "register": capability.register,
                "value_kind": capability.value_kind,
                "validation_state": capability.validation_state,
                "support_tier": capability.resolved_support_tier,
                "support_notes": capability.support_notes,
                "tested": capability.tested,
                "enabled_default": capability.enabled_default,
                "advanced": capability.advanced,
                "requires_confirm": capability.requires_confirm,
                "reboot_required": capability.reboot_required,
                "unsafe_while_running": capability.unsafe_while_running,
                "depends_on": list(capability.depends_on),
                "affects": list(capability.affects),
            }
        )

    validation_counts = Counter(item["validation_state"] for item in capabilities)
    support_tier_counts = Counter(item["support_tier"] for item in capabilities)
    group_counts = Counter(item["group"] for item in capabilities)

    return {
        "profile_key": profile.key,
        "title": display_title,
        "implementation_title": profile.title,
        "summary": {
            "capabilities": len(capabilities),
            "validation_state_counts": dict(sorted(validation_counts.items())),
            "support_tier_counts": dict(sorted(support_tier_counts.items())),
            "group_counts": dict(sorted(group_counts.items())),
        },
        "capabilities": capabilities,
    }
