"""RB field hard-rejects and link-loss last-good hold (ADR 0003).

Order: envelope (``parse_rb``) → field hard-rejects here → publish.
Checksum/length failures never reach this module; optional clears them and must
not start the 180 s hold.

Dual clock with ``short_ascii_optional``: normal RB TTL stays ~60 s; link-loss
signature (V=0 ∧ SoC=0) alone may keep last-good publishable while
``age_from_last_good < 180 s``. Hold is capped from last-good time — continuous
link-loss frames must not refresh ``hold_until``. Held values never look fresh
(``sampled_at`` stays on the last good sample).
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Weak universal pack window until a tighter F/RB-derived band exists (Q7.A).
PACK_VOLTAGE_MIN_V = 30.0
PACK_VOLTAGE_MAX_V = 70.0
LINK_LOSS_HOLD_S = 180.0
# Q4.O2: spec surge + real inrush — reject only above 3× rated VA.
POWER_OVERLOAD_FACTOR = 3.0

_CURRENT_KEYS = (
    "battery_current",
    "bms_charging_current",
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


def hard_reject_reason(
    values: dict[str, object],
    *,
    rated_voltage: float | None = None,
    rated_current: float | None = None,
    rated_battery_voltage: float | None = None,
) -> str | None:
    """Return a short reject tag, or None if values may publish.

    SoC 0 is allowed when pack V is present and sane. Power rejects only above
    3× F VA so 1×–3× inrush still publishes (Q4.O2).
    """
    soc = values.get("battery_soc")
    if soc is not None:
        if not isinstance(soc, (int, float)) or not 0 <= float(soc) <= 100:
            return "soc"

    voltage = values.get("bms_total_voltage")
    if voltage is not None:
        if not isinstance(voltage, (int, float)):
            return "pack_voltage"
        volts = float(voltage)
        if not PACK_VOLTAGE_MIN_V <= volts <= PACK_VOLTAGE_MAX_V:
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
        max_current = rated_va / float(rated_battery_voltage)

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
    """Runtime last-good + link-loss hold clock (not persisted)."""

    last_good: dict[str, object] = field(default_factory=dict)
    last_good_at: float | None = None
    hold_until: float | None = None

    def clear(self) -> None:
        self.last_good.clear()
        self.last_good_at = None
        self.hold_until = None

    def clear_hold(self) -> None:
        self.hold_until = None

    def holding(self, now: float) -> bool:
        return self.hold_until is not None and now < self.hold_until

    def decide(
        self,
        parsed: dict[str, object],
        *,
        now: float,
        rated_voltage: float | None = None,
        rated_current: float | None = None,
        rated_battery_voltage: float | None = None,
    ) -> RbFilterDecision:
        if is_link_loss_signature(parsed):
            # Q1.H5: hold at most 180 s from last good — never refresh on each
            # link-loss frame.
            if (
                self.last_good
                and self.last_good_at is not None
                and (now - self.last_good_at) < LINK_LOSS_HOLD_S
            ):
                self.hold_until = self.last_good_at + LINK_LOSS_HOLD_S
                return RbFilterDecision(
                    outcome="link_loss_hold",
                    values=dict(self.last_good),
                    keep_previous=True,
                )
            self.clear()
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
            # Do not start 180 s hold; leave prior sample for the ~60 s TTL.
            return RbFilterDecision(outcome="rejected", keep_previous=True)

        self.clear_hold()
        if parsed.get("short_ascii_bms_data_available") is False:
            self.last_good.clear()
            self.last_good_at = None
            return RbFilterDecision(
                outcome="no_data",
                values=dict(parsed),
                refresh_sampled_at=True,
            )

        self.last_good = dict(parsed)
        self.last_good_at = now
        return RbFilterDecision(
            outcome="ok",
            values=dict(parsed),
            refresh_sampled_at=True,
        )
