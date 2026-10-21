"""Bind split learning routes to the driver's successfully captured read plans.

A probe target is not necessarily the only route used by a driver. Never treat
0xff as a universal alias: require the current driver's exact read plan and a
successful block from this run's seed capture, for the same device and devcode.
"""

from __future__ import annotations

from ...drivers.registry import get_driver


def captured_learning_targets(inverter, capture: object) -> tuple[tuple[int, int], ...]:
    """Return additional tunnel coordinates proved by the seed acquisition."""
    target = getattr(inverter, "probe_target", None)
    driver_key = getattr(inverter, "driver_key", "")
    if target is None or not isinstance(capture, dict) or capture.get("driver_key") != driver_key:
        return ()
    if capture.get("probe_target") != {
        "devcode": target.devcode, "collector_addr": target.collector_addr,
        "device_addr": target.device_addr,
    }:
        return ()
    blocks = capture.get("captured_ranges")
    if not isinstance(blocks, list):
        return ()
    try:
        plans = get_driver(driver_key).local_register_read_plans(inverter)
    except (KeyError, ValueError, TypeError):
        return ()
    routes = set()
    for plan in plans:
        if (plan.devcode != target.devcode or plan.device_addr != target.device_addr
                or plan.function != 3):
            continue
        for block in blocks:
            if not isinstance(block, dict):
                continue
            coordinates = (block.get("collector_addr"), block.get("start"),
                           block.get("count"), block.get("function", 3))
            words = block.get("words")
            if (any(type(value) is not int for value in coordinates)
                    or coordinates != (plan.collector_addr, plan.start, plan.count, plan.function)
                    or not isinstance(words, list) or len(words) != plan.count
                    or any(type(word) is not int or not 0 <= word <= 65535 for word in words)
                    or block.get("error")):
                continue
            routes.add((plan.devcode, plan.collector_addr))
    return tuple(sorted(routes))
