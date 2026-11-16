"""RB field hard-rejects and link-loss omit (ADR 0003 / G.V2 G.I1 G.L1).

Order: envelope (``parse_rb``) → field hard-rejects here → publish.
Checksum/length failures never reach this module; optional clears them and must
not invent a last-good hold.

G.V2: no Pack-V hard-reject until F battery rating is known; then an F-derived
LFP-ish window (0.75×–4/3× rated), not a universal 30–70 V band.
G.I1: I ceiling = 3× VA / Vbat (aligned with power's 3× VA).
G.L1: link-loss signature → omit BMS keys + ``available=False`` (no 180 s hold).
"""

from __future__ import annotations

from dataclasses import dataclass

# LFP-ish envelope around F rated battery V (gap MX3.F1 table).
PACK_V_MIN_FACTOR = 0.75  # 12→9, 24→18, 48→36
PACK_V_MAX_FACTOR = 4.0 / 3.0  # 12→16, 24→32, 48→64
# Q4.O2 / G.I1: same overload factor for power and current ceilings.
POWER_OVERLOAD_FACTOR = 3.0

_CURRENT_KEYS = (
    "battery_current",
    "bms_charging_current",
    "bms_discharging_current",
    "bms_discharge_current",
    "bms_charge_current",
)
_POWER_KEYS = ("battery_power", "bms_battery_power")


def is_link_loss_signature(parsed: dict[str, object]) -> bool:
    """Good-envelope RB with primary V/SoC withdrawn (parse_rb link-loss path)."""
    return (
        parsed.get("short_ascii_bms_data_available") is False
        and "bms_total_voltage" not in parsed
        and "battery_soc" not in parsed
    )


def pack_voltage_window(rated_battery_voltage: float) -> tuple[float, float]:
    """Inclusive F-derived Pack-V band (G.V2)."""
    return (
        rated_battery_voltage * PACK_V_MIN_FACTOR,
        rated_battery_voltage * PACK_V_MAX_FACTOR,
    )


def hard_reject_reason(
    values: dict[str, object],
    *,
    rated_voltage: float | None = None,
    rated_current: float | None = None,
    rated_battery_voltage: float | None = None,
) -> str | None:
    """Return a short reject tag, or None if values may publish.

    SoC 0 is allowed when pack V is present. Pack-V is gated only when F is
    known (G.V2). Power and current reject only above 3× F VA (G.I1 / Q4.O2).
    """
    soc = values.get("battery_soc")
    if soc is not None:
        if not isinstance(soc, (int, float)) or not 0 <= float(soc) <= 100:
            return "soc"

    voltage = values.get("bms_total_voltage")
    if (
        voltage is not None
        and isinstance(rated_battery_voltage, (int, float))
        and float(rated_battery_voltage) > 0
    ):
        if not isinstance(voltage, (int, float)):
            return "pack_voltage"
        lo, hi = pack_voltage_window(float(rated_battery_voltage))
        if not lo <= float(voltage) <= hi:
            return "pack_voltage"

    rated_va: float | None = None
    if (
        isinstance(rated_voltage, (int, float))
        and isinstance(rated_current, (int, float))
        and float(rated_voltage) > 0
        and float(rated_current) > 0
    ):
        rated_va = float(rated_voltage) * float(rated_current)

    max_current: float | None = None
    if (
        rated_va is not None
        and isinstance(rated_battery_voltage, (int, float))
        and float(rated_battery_voltage) > 0
    ):
        max_current = POWER_OVERLOAD_FACTOR * rated_va / float(rated_battery_voltage)

    if max_current is not None:
        for key in _CURRENT_KEYS:
            amps = values.get(key)
            if amps is None:
                continue
            if not isinstance(amps, (int, float)) or abs(float(amps)) > max_current:
                return "current"

    if rated_va is not None:
        power_ceiling = POWER_OVERLOAD_FACTOR * rated_va
        for key in _POWER_KEYS:
            watts = values.get(key)
            if watts is None:
                continue
            if not isinstance(watts, (int, float)) or abs(float(watts)) > power_ceiling:
                return "power"

    return None


@dataclass
class RbFilterDecision:
    """How optional RB storage should treat one successful parse."""

    outcome: str
    values: dict[str, object] | None = None
    refresh_sampled_at: bool = False
    keep_previous: bool = False


@dataclass
class RbPublishFilter:
    """Runtime link-loss / reject counters (not persisted). No last-good hold."""

    bms_link_loss_count: int = 0
    rb_hard_reject_count: int = 0
    reading_hold_pending_count: int = 0
    _in_link_loss: bool = False

    def clear(self) -> None:
        # Counters and link-loss streak survive envelope drops / TTL clears so
        # one dropout is not double-counted after a checksum fail mid-streak.
        return

    def diagnostic_counters(self) -> dict[str, int]:
        """Quiet MX2 counters published every optional refresh (diagnostics)."""
        return {
            "bms_link_loss_count": self.bms_link_loss_count,
            "rb_hard_reject_count": self.rb_hard_reject_count,
            # ADR 0001 / Q3.P1: confirmation-hold deferred; contract stays at 0.
            "reading_hold_pending_count": self.reading_hold_pending_count,
        }

    def decide(
        self,
        parsed: dict[str, object],
        *,
        now: float,
        rated_voltage: float | None = None,
        rated_current: float | None = None,
        rated_battery_voltage: float | None = None,
    ) -> RbFilterDecision:
        del now  # retained for call-site symmetry with optional refresh clock
        if is_link_loss_signature(parsed):
            if not self._in_link_loss:
                self.bms_link_loss_count += 1
                self._in_link_loss = True
            # G.L1: omit BMS keys; do not republish last-good as fresh.
            return RbFilterDecision(
                outcome="no_data",
                values={"short_ascii_bms_data_available": False},
                refresh_sampled_at=True,
            )

        reason = hard_reject_reason(
            parsed,
            rated_voltage=rated_voltage,
            rated_current=rated_current,
            rated_battery_voltage=rated_battery_voltage,
        )
        if reason is not None:
            # Corrupt/impossible after a good envelope: drop this frame only.
            # Omit live BMS keys the same way link-loss does so the failed
            # read is not republished as a fresh measurement. Do not exit a
            # link-loss streak — junk mid-dropout must not re-count.
            self.rb_hard_reject_count += 1
            return RbFilterDecision(
                outcome="rejected",
                values={"short_ascii_bms_data_available": False},
                refresh_sampled_at=True,
            )

        if parsed.get("short_ascii_bms_data_available") is False:
            return RbFilterDecision(
                outcome="no_data",
                values=dict(parsed),
                refresh_sampled_at=True,
            )

        # True recovery only — ends link-loss streak for counting.
        self._in_link_loss = False
        return RbFilterDecision(
            outcome="ok",
            values=dict(parsed),
            refresh_sampled_at=True,
        )
