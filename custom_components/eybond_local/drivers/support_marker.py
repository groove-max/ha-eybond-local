"""Neutral driver contract for special runtime support states.

Some drivers can detect that a bound inverter is in a special state that support
tooling must surface -- e.g. an SMG-family read-only unverified fallback. The
*decision* is driver/model policy; the support layers (bundle, package,
workflow) only render the neutral marker this contract produces. They never
infer the state from driver key, variant key or profile path.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DriverSupportWorkflow:
    """Ready-to-render support-workflow guidance for one special runtime state.

    Every field the support-workflow presentation needs is declared explicitly,
    so an incomplete or foreign mapping can never cross the driver -> support
    boundary. The object is frozen and slotted (no mutable nested state).
    """

    level: str
    level_label: str
    summary: str
    next_action: str
    primary_action: str
    step_1: str
    step_2: str
    step_3: str
    advanced_hint: str


@dataclass(frozen=True, slots=True)
class DriverSupportMarker:
    """One machine-readable support marker owned by a driver.

    ``as_payload`` is the public, archive-stable marker embedded in the support
    bundle (key/label/read_only/verification/summary). ``workflow`` carries the
    ready-to-render support-workflow guidance for this state so the support
    workflow layer needs no model knowledge.
    """

    key: str
    label: str
    read_only: bool
    verification: str
    summary: str
    workflow: DriverSupportWorkflow | None = None

    def as_payload(self) -> dict[str, object]:
        """Return the archive-stable marker payload for the support bundle."""

        return {
            "key": self.key,
            "label": self.label,
            "read_only": self.read_only,
            "verification": self.verification,
            "summary": self.summary,
        }
