"""Render model-authored advice after a driver rejection, without device IO."""

from __future__ import annotations

import math

from ..models import RuntimeSnapshot, WriteCapability
from ..telemetry import TelemetryFreshness, TelemetryOrigin

# Age covers the whole pre-write refresh and write attempt. A reconnect drops
# the observation entirely. This is a display freshness budget, not a limit on
# inverter settings or on the duration of a write operation.
REJECTION_CONTEXT_MAX_AGE_SECONDS = 30.0


def _numeric(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def with_write_rejection_hint(
    error: Exception,
    capability: WriteCapability,
    *,
    requested_value: object,
    snapshot: RuntimeSnapshot,
    observation_age_seconds: float | None,
) -> Exception:
    """Append qualified advice, never diagnose a dependency from exception alone.

    Only current-cycle driver points can supply numbers. Startup details, broad
    metadata, carried DELTA values and failed/offline snapshots are not evidence.
    The original error and its machine-readable prefix remain intact.
    """
    if not capability.write_rejection_hint:
        return error
    parts = [str(error), "Model manual guidance (not a confirmed cause):",
             capability.write_rejection_hint]
    if _numeric(requested_value):
        parts.append(f"Requested value: {requested_value:g}{capability.unit or ''}.")
    fresh = (
        snapshot.connected and not snapshot.last_error
        and snapshot.inverter is not None
        and snapshot.telemetry.driver_key == snapshot.inverter.driver_key
        and _numeric(observation_age_seconds)
        and 0 <= observation_age_seconds <= REJECTION_CONTEXT_MAX_AGE_SECONDS
    )
    readings = []
    if fresh:
        for key in capability.write_rejection_context:
            try:
                related = snapshot.inverter.get_capability(key)
            except KeyError:
                continue
            point = snapshot.telemetry_point(related.value_key)
            if (point is None or point.freshness is not TelemetryFreshness.FRESH
                    or point.origin is not TelemetryOrigin.DRIVER
                    or not _numeric(point.value)):
                continue
            readings.append(f"{related.display_name!r} = {point.value:g}{related.unit or ''}")
    if readings:
        parts.append("Recently read before this write: " + "; ".join(readings) + ".")
    elif capability.write_rejection_context:
        parts.append("No fresh readings of the related settings are available.")
    return ValueError(" ".join(parts))
