"""Labelled short-ASCII estimates derived from wire measurements + ratings.

MX2: %×VA watts are an estimate, never measured active power. Keep wire load%
intact; do not publish tip-style ``load_power`` / silent source swaps here.
``best_available_ac_load_estimate`` is an explicitly labelled composite for
dashboards:

(1) Q1 %×VA when the inverter reports load (> 0 W) → ``q1_percent`` (only hard stop);
(2) else energy balance from known BMS watts (+ PV):
    ``P_house ≈ max(0, P_pv − P_battery)`` with pack sign +charge / −discharge
    → ``bms_dc`` / ``bms_dc_plus_pv`` / ``pv_minus_charge`` / ``pv_idle``;
(3) else PV alone if BMS watts missing → ``pv_only``;
(4) else Q1 including 0 W, or omit.

BMS watts are the live ``battery_power`` measurement only. A missing
``pv_power`` is not treated as 0 W: discharge may omit the PV term, and
charge/idle without PV does not publish a residual built from that hole.
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


def _q1_load_watts(values: dict[str, object]) -> float | None:
    q1_w = values.get("estimated_ac_load_power")
    if not isinstance(q1_w, (int, float)):
        q1_w = estimated_ac_load_values(values).get("estimated_ac_load_power")
    if isinstance(q1_w, (int, float)):
        return float(q1_w)
    return None


def _battery_watts(values: dict[str, object]) -> float | None:
    """Live pack watts only. Held estimates are a later, labelled bridge."""
    live = values.get("battery_power")
    if isinstance(live, (int, float)):
        return float(live)
    return None


def _pv_watts(values: dict[str, object]) -> float | None:
    """Known PV watts, or None when the measurement is absent.

    An explicit 0 is a real reading. A missing key is a hole, not 0 W.
    """
    if "pv_power" not in values:
        return None
    pv_w = values.get("pv_power")
    if not isinstance(pv_w, (int, float)):
        return None
    return float(pv_w) if float(pv_w) > 0 else 0.0


def _balance_source(battery_w: float, pv_w: float) -> str:
    """Diagnostic label for ``max(0, pv − battery)`` (battery +charge / −discharge)."""
    if battery_w < 0:
        return "bms_dc_plus_pv" if pv_w > 0 else "bms_dc"
    if battery_w > 0:
        return "pv_minus_charge"
    return "pv_idle" if pv_w > 0 else "balance_zero"


def best_available_ac_load_estimate_values(values: dict[str, object]) -> dict[str, object]:
    """Composite dashboard estimate for house AC demand.

    Only a reporting Q1 load (> 0 W) stops BMS/PV inference. Otherwise:

    ``P_house ≈ max(0, P_pv − P_battery)`` with BMS sign +charge / −discharge
    (so PV 500 W + 100 W discharge → 600 W). Idle pack + PV → full PV.

    Battery watts are live ``battery_power`` only. When ``pv_power`` is absent,
    discharge omits the PV term and charge/idle omits the estimate instead of
    treating the hole as 0 W.
    """
    q1_w = _q1_load_watts(values)
    # Inverter reporting real load: trust Q1 for house AC demand.
    if q1_w is not None and q1_w > 0:
        return {
            "best_available_ac_load_estimate": q1_w,
            "best_available_ac_load_estimate_source": "q1_percent",
        }

    battery_w = _battery_watts(values)
    pv_w = _pv_watts(values)

    if battery_w is not None and pv_w is not None:
        # P_house ≈ P_pv − P_batt  (+charge stores PV; −discharge adds to house).
        estimate = max(0.0, round(pv_w - battery_w, 1))
        return {
            "best_available_ac_load_estimate": estimate,
            "best_available_ac_load_estimate_source": _balance_source(battery_w, pv_w),
        }

    if battery_w is not None and battery_w < 0:
        # PV was not read. Use the measured discharge and do not invent 0 W of PV.
        return {
            "best_available_ac_load_estimate": round(-battery_w, 1),
            "best_available_ac_load_estimate_source": "bms_dc",
        }

    if pv_w is not None and pv_w > 0:
        # Pack watts missing; still better than publishing a silent Q1 zero.
        return {
            "best_available_ac_load_estimate": round(pv_w, 1),
            "best_available_ac_load_estimate_source": "pv_only",
        }

    if q1_w is not None:
        return {
            "best_available_ac_load_estimate": q1_w,
            "best_available_ac_load_estimate_source": "q1_percent",
        }
    return {}
