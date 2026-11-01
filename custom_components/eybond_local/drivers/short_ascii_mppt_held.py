"""Labelled MPPT held estimates for a soft aux miss.

Live PV and MPPT keys stay omitted when the 0200 read fails. These
``*_held_estimate`` keys keep the last good sample until the MPPT TTL (60 s)
so dashboards do not treat a frozen live ``pv_power`` as a fresh measurement.
"""

from __future__ import annotations

from dataclasses import dataclass, field

STATE_KEY = "short_ascii_mppt_held"
HELD_TTL_S = 60.0

MPPT_HOLD_KEYS: tuple[str, ...] = (
    "pv_voltage",
    "pv_power",
    "mppt_battery_voltage",
    "mppt_temperature",
    "dc_load_current",
    "mppt_work_mode_code",
    "mppt_daily_energy",
    "mppt_total_energy",
    "mppt_error_code",
    "mppt_error",
)


def held_estimate_key(source_key: str) -> str:
    return f"{source_key}_held_estimate"


@dataclass
class MpptHeldEstimates:
    values: dict[str, object] = field(default_factory=dict)
    sampled_at: float | None = None

    def observe(self, live: dict[str, object], now: float) -> None:
        snap = {key: live[key] for key in MPPT_HOLD_KEYS if key in live}
        if not snap:
            return
        self.values = snap
        self.sampled_at = now

    def project(self, now: float) -> dict[str, object]:
        if self.sampled_at is None or not 0.0 <= now - self.sampled_at < HELD_TTL_S:
            self.values = {}
            self.sampled_at = None
            return {}
        age = round(now - self.sampled_at, 3)
        mode = "live" if age == 0.0 else "held"
        out: dict[str, object] = {
            held_estimate_key(key): value for key, value in self.values.items()
        }
        out["mppt_held_estimate_age_seconds"] = age
        out["mppt_held_estimate_mode"] = mode
        return out


def mppt_held_for(runtime_state: dict) -> MpptHeldEstimates:
    held = runtime_state.get(STATE_KEY)
    if not isinstance(held, MpptHeldEstimates):
        held = MpptHeldEstimates()
        runtime_state[STATE_KEY] = held
    return held
