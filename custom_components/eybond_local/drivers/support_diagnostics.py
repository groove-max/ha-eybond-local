"""Bounded read-only evidence; never changes runtime maps or enables writes."""

from __future__ import annotations

import asyncio
from typing import Any

from ..payload.modbus import ModbusError, ModbusSession


async def capture_support_reads(
    session: ModbusSession,
    ranges: tuple[tuple[int, int, str], ...],
    *,
    timeout_seconds: float,
    source: str,
    purpose: str,
) -> dict[str, Any]:
    """Read a fixed driver-owned list; only illegal addresses allow continuing."""

    result: dict[str, Any] = {
        "source": source,
        "purpose": purpose,
        "time_budget_seconds": timeout_seconds,
        "planned_ranges": [
            {"start": start, "count": count, "group": group}
            for start, count, group in ranges
        ],
        "status": "completed",
        "captured_ranges": [],
        "range_failures": [],
    }
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    for start, count, _group in ranges:
        if loop.time() >= deadline:
            result["status"] = "budget_exhausted"
            break
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
            if isinstance(exc, ModbusError) and str(exc) == "exception_code:2":
                continue
            result["status"] = "budget_exhausted" if expired else "stopped_on_error"
            break
        result["captured_ranges"].append({"start": start, "count": count, "words": list(words)})
    return result
