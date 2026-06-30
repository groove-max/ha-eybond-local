"""Polling policy presets for runtime drivers/transports."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PollPolicy:
    """Protocol-specific guardrails for adaptive polling."""

    min_auto_interval: float
    max_auto_interval: float
    safety_factor: float = 1.3
    sample_window: int = 10
    percentile: float = 0.75
    grow_step_limit: float = 1.5
    shrink_step_limit: float = 0.9


DEFAULT_POLL_POLICY = PollPolicy(
    min_auto_interval=10.0,
    max_auto_interval=120.0,
)

SMG_MODBUS_POLL_POLICY = PollPolicy(
    min_auto_interval=3.0,
    max_auto_interval=60.0,
)

FAST_MODBUS_POLL_POLICY = PollPolicy(
    min_auto_interval=5.0,
    max_auto_interval=90.0,
)

ASCII_POLL_POLICY = PollPolicy(
    min_auto_interval=10.0,
    max_auto_interval=120.0,
)


_DRIVER_POLICIES: dict[str, PollPolicy] = {
    "modbus_smg": SMG_MODBUS_POLL_POLICY,
    "srne_modbus": FAST_MODBUS_POLL_POLICY,
    "must_pv_ph18": FAST_MODBUS_POLL_POLICY,
    "smartess_local": FAST_MODBUS_POLL_POLICY,
    "eybond_g_ascii": ASCII_POLL_POLICY,
    "pi18": ASCII_POLL_POLICY,
    "pi30": ASCII_POLL_POLICY,
}


def poll_policy_for_driver(driver_key: object) -> PollPolicy:
    """Return the adaptive polling policy for a detected runtime driver."""

    key = str(driver_key or "").strip()
    if not key or key == "auto":
        return DEFAULT_POLL_POLICY
    return _DRIVER_POLICIES.get(key, DEFAULT_POLL_POLICY)
