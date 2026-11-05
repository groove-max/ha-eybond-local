"""Labelled BMS held estimates — mirror live, hold across link-loss gaps.

MX2/ADR 0003: measurement keys stay omit-on-loss (never look fresh). These
``*_held_estimate`` keys are explicitly labelled estimates for dashboards.
House-load inference does not read them.

TTL 180 s ≈ site p90–p95 BMS-unavailable gap coverage without holding forever.

Partial live RB (V/SoC present, currents RH-gated) must **merge** into the
cache — never replace it — or battery_power_held is wiped while mode stays live.
"""

from __future__ import annotations

from dataclasses import dataclass, field

STATE_KEY = "short_ascii_bms_held"
HELD_TTL_S = 180.0

# Keys withdrawn when RB reports link-loss (V=0∧SoC=0) / omit path.
BMS_HOLD_KEYS: tuple[str, ...] = (
    "bms_total_voltage",
    "battery_soc",
    "bms_charging_current",
    "bms_discharging_current",
    "battery_power",
    "bms_cell_temperature",
    "bms_cycle_count",
    "bms_internal_temperature",
    "bms_mos_temperature",
    "bms_max_cell_voltage",
    "bms_min_cell_voltage",
    "bms_low_protection_voltage",
    "bms_charge_cutoff_voltage",
    "short_ascii_bms_charge_path_enabled",
    "short_ascii_bms_discharge_path_enabled",
)

BINARY_HOLD_KEYS = frozenset({
    "short_ascii_bms_charge_path_enabled",
    "short_ascii_bms_discharge_path_enabled",
})

# Currents / DC watts may be absent while V/SoC remain live (RH/F gate).
_CURRENT_POWER_HOLD_KEYS = frozenset({
    "bms_charging_current",
    "bms_discharging_current",
    "battery_power",
})


def held_estimate_key(source_key: str) -> str:
    return f"{source_key}_held_estimate"


def _live_bms_snapshot(values: dict[str, object]) -> dict[str, object] | None:
    """Return holdable live keys, or None when BMS measurements are absent."""
    if values.get("short_ascii_bms_data_available") is not True:
        return None
    voltage = values.get("bms_total_voltage")
    if not isinstance(voltage, (int, float)):
        return None
    snap = {key: values[key] for key in BMS_HOLD_KEYS if key in values}
    return snap or None


@dataclass
class BmsHeldEstimates:
    """Runtime cache of last-good BMS fields (not persisted).

    Per-key ``sampled_at`` so RH-gated omission of currents does not expire V/SoC
    or wipe last-good watts before the 180 s hold TTL.
    """

    values: dict[str, object] = field(default_factory=dict)
    key_sampled_at: dict[str, float] = field(default_factory=dict)

    def clear(self) -> None:
        self.values.clear()
        self.key_sampled_at.clear()

    def _expire(self, now: float) -> None:
        expired = [
            key for key, sampled in self.key_sampled_at.items()
            if not 0.0 <= now - sampled < HELD_TTL_S
        ]
        for key in expired:
            self.values.pop(key, None)
            self.key_sampled_at.pop(key, None)

    def observe(self, live_values: dict[str, object], now: float) -> None:
        """Merge one newly read live sample. Cached TTL replays must not call this."""
        live = _live_bms_snapshot(live_values)
        if live is None:
            return
        for key, value in live.items():
            self.values[key] = value
            self.key_sampled_at[key] = now

    def project(self, now: float) -> dict[str, object]:
        """Labelled mirrors. Mode is live only for keys stamped at ``now``."""
        self._expire(now)
        if not self.values:
            self.clear()
            return {}

        out: dict[str, object] = {}
        ages: list[float] = []
        fresh_keys: set[str] = set()
        for key, value in self.values.items():
            sampled = self.key_sampled_at.get(key, now)
            age = max(0.0, now - sampled)
            ages.append(age)
            out[held_estimate_key(key)] = value
            if age == 0.0:
                fresh_keys.add(key)
        if fresh_keys and len(fresh_keys) == len(self.values):
            mode = "live"
        elif fresh_keys and any(
            key in self.values and key not in fresh_keys
            for key in _CURRENT_POWER_HOLD_KEYS
        ):
            # Live pack link, but watts/currents are held from last RH-gated sample.
            mode = "live_partial"
        else:
            mode = "held"
        out["bms_held_estimate_age_seconds"] = round(max(ages) if ages else 0.0, 1)
        out["bms_held_estimate_mode"] = mode
        return out

    def apply(self, live_values: dict[str, object], now: float) -> dict[str, object]:
        """Observe a new live sample when present, then project mirrors."""
        if _live_bms_snapshot(live_values) is not None:
            self.observe(live_values, now)
        return self.project(now)


def bms_held_for(runtime_state: dict) -> BmsHeldEstimates:
    held = runtime_state.get(STATE_KEY)
    if type(held) is not BmsHeldEstimates:
        held = BmsHeldEstimates()
        runtime_state[STATE_KEY] = held
    return held
