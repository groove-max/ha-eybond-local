"""Driver-owned periodic reads, independent of wire protocol and value cache.

The caller owns the transaction: pass a working copy and commit only after a
live, completed cycle. Skipping a group never produces a fresh measurement.
"""

from __future__ import annotations

from typing import Any


class PeriodicReadCadence:
    """Select oldest-due groups fairly; bound work after the initial snapshot."""

    def __init__(self, state: dict[str, Any], *, now: float, interval: float = 60.0):
        self.state = state
        self.now = now
        self.interval = interval
        self.due = state.setdefault("due", {})
        self.success = state.setdefault("success", {})

    def select(self, groups: tuple[str, ...], *, limit: int, forced=()) -> set[str]:
        active = set(groups)
        for mapping in (self.due, self.success):
            for key in tuple(mapping):
                if key not in active:
                    del mapping[key]
        selected = active.intersection(forced)
        pending = sorted(
            (key for key in groups if self.due.get(key, self.now) <= self.now
             and key not in selected),
            key=lambda key: self.due.get(key, float("-inf")),
        )
        # Only the initial snapshot is unbounded. Subsequent newly learned
        # controls join the same fair queue, without starving existing ones.
        selected.update(pending if not self.state.get("initialized") else pending[:limit])
        return selected

    def attempted(self, key: str, *, success: bool) -> None:
        # Pace failures too; a silent optional read must not monopolize polls.
        self.due[key] = self.now + self.interval
        if success:
            self.success[key] = self.now

    def finish(self, groups: tuple[str, ...]) -> dict[str, Any]:
        self.state["initialized"] = True
        known = [self.success[key] for key in groups if key in self.success]
        return {
            "due_groups": sum(self.due.get(key, self.now) <= self.now for key in groups),
            "oldest_due_age_s": round(max(
                (max(0.0, self.now - self.due.get(key, self.now)) for key in groups),
                default=0.0,
            ), 1),
            "unconfirmed_groups": sum(key not in self.success for key in groups),
            "oldest_success_age_s": round(max(0.0, self.now - min(known)), 1) if known else None,
            "refresh_interval_s": self.interval,
        }
