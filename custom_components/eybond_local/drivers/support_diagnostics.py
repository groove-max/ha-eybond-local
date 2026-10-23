"""Bounded read-only evidence; never changes runtime maps or enables writes."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import math
from typing import Any

from ..payload.modbus import ModbusError, ModbusSession


async def capture_support_reads(
    session: ModbusSession,
    ranges: tuple[tuple[int, int, str], ...],
    *,
    timeout_seconds: float,
    source: str,
    purpose: str,
    read_spacing_seconds: float = 0.0,
) -> dict[str, Any]:
    """Read a fixed driver-owned list; only illegal addresses allow continuing."""

    if not math.isfinite(read_spacing_seconds) or read_spacing_seconds < 0:
        raise ValueError("invalid_support_read_spacing")
    result: dict[str, Any] = {
        "source": source,
        "purpose": purpose,
        "time_budget_seconds": timeout_seconds,
        "read_spacing_seconds": read_spacing_seconds,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "planned_ranges": [
            {"start": start, "count": count, "group": group}
            for start, count, group in ranges
        ],
        "status": "completed",
        "captured_ranges": [],
        "range_failures": [],
        "read_attempts": [],
    }
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + timeout_seconds
    for index, (start, count, group) in enumerate(ranges):
        if index and read_spacing_seconds:
            if deadline - loop.time() <= read_spacing_seconds:
                result["status"] = "budget_exhausted"
                break
            await asyncio.sleep(read_spacing_seconds)
        if loop.time() >= deadline:
            result["status"] = "budget_exhausted"
            break
        attempt = {
            "start": start, "count": count, "group": group,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_start_seconds": round(loop.time() - started, 6),
        }
        timeout = asyncio.timeout_at(deadline)
        try:
            async with timeout:
                words = await session.read_holding(start, count)
        except Exception as exc:
            expired = timeout.expired()
            result["range_failures"].append({
                "start": start, "count": count,
                "error": "diagnostic_budget_exhausted" if expired else str(exc),
            })
            result["read_attempts"].append({
                **attempt, "outcome": "failed",
                "elapsed_end_seconds": round(loop.time() - started, 6),
            })
            if isinstance(exc, ModbusError) and str(exc) == "exception_code:2":
                continue
            result["status"] = "budget_exhausted" if expired else "stopped_on_error"
            break
        # Keep the existing data/error row contract; timing is separate from
        # register observations so older fixture consumers remain compatible.
        result["captured_ranges"].append({"start": start, "count": count, "words": list(words)})
        result["read_attempts"].append({
            **attempt, "outcome": "captured",
            "elapsed_end_seconds": round(loop.time() - started, 6),
        })
    return result
