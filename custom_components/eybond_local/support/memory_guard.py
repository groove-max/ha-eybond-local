"""Runtime memory guards for expensive local workflows."""

from __future__ import annotations

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
