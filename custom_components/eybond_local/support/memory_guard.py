"""Runtime memory guards for expensive local workflows."""

from __future__ import annotations

import re

# Admission headroom, not a measured allocation or a PI30-specific requirement.
# Retained until representative live peak measurements justify a new budget.
SHADOW_LEARNING_MIN_AVAILABLE_MIB = 400


def read_available_memory_mib() -> int | None:
    """Return MemAvailable (MiB) from /proc/meminfo, or None when unavailable."""

    try:
        with open("/proc/meminfo", "r", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def shadow_learning_memory_blocker(available_mib: int | None) -> str:
    """Return a shadow-learning blocker string when memory is below the floor."""

    if available_mib is None:
        return ""
    if available_mib >= SHADOW_LEARNING_MIN_AVAILABLE_MIB:
        return ""
    return f"insufficient_memory:{available_mib}MiB"


def shadow_learning_memory_failure_details(reason: object) -> dict[str, int]:
    """Extract only bounded numeric memory evidence, never exception text."""
    prefix = "shadow_learning_preflight_blocked:"
    if type(reason) is not str or len(reason) > 4096 or not reason.startswith(prefix):
        return {}
    for blocker in reason[len(prefix):].split(","):
        match = re.fullmatch(r"insufficient_memory:([0-9]{1,6})MiB", blocker)
        if match and 0 <= (available := int(match[1])) < SHADOW_LEARNING_MIN_AVAILABLE_MIB:
            return {"available_mib": available, "required_mib": SHADOW_LEARNING_MIN_AVAILABLE_MIB}
    return {}
