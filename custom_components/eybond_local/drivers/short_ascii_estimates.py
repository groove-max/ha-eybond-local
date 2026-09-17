"""Labelled short-ASCII estimates derived from wire measurements + ratings.

MX2: %×VA watts are an estimate, never measured active power. Keep wire load%
intact; do not publish tip-style ``load_power`` / source swaps here.
"""

from __future__ import annotations


def estimated_ac_load_values(values: dict[str, object]) -> dict[str, object]:
    """Return ``estimated_ac_load_power`` from load% × rated VA, or omit."""
    percent = values.get("load_percent")
    rated_v = values.get("short_ascii_rated_voltage")
    rated_a = values.get("short_ascii_rated_current")
    if not isinstance(percent, (int, float)):
        return {}
    if not isinstance(rated_v, (int, float)) or not isinstance(rated_a, (int, float)):
        return {}
    return {
        "estimated_ac_load_power": round(float(rated_v) * float(rated_a) * float(percent) / 100.0, 1),
    }
