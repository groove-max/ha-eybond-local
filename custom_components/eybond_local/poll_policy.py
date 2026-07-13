"""Neutral adaptive-polling contract shared by runtime and drivers.

This module is deliberately provider/driver/protocol-agnostic. It defines ONLY
the ``PollPolicy`` value object and the neutral ``DEFAULT_POLL_POLICY`` used
before any driver is known and by any driver that needs no tighter guardrails.

Concrete, driver-specific policies live in their driver modules (e.g. the PI30
floor in ``drivers/pi30.py``, the SMG envelope in ``drivers/smg.py``); a driver
declares its policy with the ``poll_policy`` class attribute (or overrides
``poll_policy_for`` when the policy depends on the detected model). Nothing here
knows about specific driver keys, models, or providers.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PollPolicy:
    """Protocol-neutral guardrails for adaptive polling."""

    min_auto_interval: float
    max_auto_interval: float
    min_manual_interval: float = 2.0
    safety_factor: float = 1.3
    sample_window: int = 10
    percentile: float = 0.75
    grow_step_limit: float = 1.5
    shrink_step_limit: float = 0.9


# Generic default used before a driver is known and by any driver that does not
# need tighter guardrails. This is the only policy instance that is genuinely
# protocol-neutral; everything more specific belongs in a driver module.
DEFAULT_POLL_POLICY = PollPolicy(
    min_auto_interval=10.0,
    max_auto_interval=120.0,
)
