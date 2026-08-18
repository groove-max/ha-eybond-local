"""Shared offline replay helpers built on top of fixture transports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..const import DRIVER_HINT_AUTO
from ..drivers.read_result import DriverReadResult, coerce_driver_read_result
from ..drivers.registry import iter_replay_drivers
from .transport import FixtureTransport, load_fixture, load_fixture_payload
from ..schema import build_runtime_ui_schema


@dataclass(slots=True)
class FixtureReplayContext:
    """Resolved replay context for one fixture and one detected driver."""

    fixture: dict[str, Any]
    transport: FixtureTransport
    driver: Any
    inverter: Any


async def detect_fixture_path(
    path: str | Path,
    *,
    driver_hint: str = DRIVER_HINT_AUTO,
) -> FixtureReplayContext:
    """Load a fixture from disk and detect a matching driver."""

    transport, raw = load_fixture(path)
    return await detect_fixture_payload(raw, transport=transport, driver_hint=driver_hint)


async def detect_fixture_payload(
    raw: dict[str, Any],
    *,
    transport: FixtureTransport | None = None,
    driver_hint: str = DRIVER_HINT_AUTO,
) -> FixtureReplayContext:
    """Detect the matching driver for an in-memory fixture payload."""

    if transport is None:
        transport, raw = load_fixture_payload(raw, name=str(raw.get("name", "fixture")))

    context = await _async_detect_fixture_inverter(
        transport,
        driver_hint=_driver_hint_for_fixture(raw, driver_hint=driver_hint),
    )
    return FixtureReplayContext(
        fixture=raw,
        transport=transport,
        driver=context.driver,
        inverter=context.inverter,
    )


async def read_fixture_result(context: FixtureReplayContext) -> DriverReadResult:
    """Read one typed driver result through the fixture transport."""

    raw = await context.driver.async_read_values(context.transport, context.inverter)
    return coerce_driver_read_result(
        raw, driver_key=getattr(context.inverter, "driver_key", "")
    )


async def apply_fixture_preset(
    context: FixtureReplayContext,
    preset_key: str,
) -> dict[str, object]:
    """Apply a preset to the in-memory fixture transport."""

    preset = context.inverter.get_capability_preset(preset_key)
    initial_values = (await read_fixture_result(context)).values
    runtime_state = preset.runtime_state(context.inverter, initial_values)
    if not runtime_state.visible:
        raise ValueError(f"preset_not_visible:{preset_key}")
    if not runtime_state.applicable:
        reasons = "; ".join(runtime_state.reasons or runtime_state.warnings) or "preset_not_applicable"
        raise ValueError(f"preset_not_applicable:{preset_key}:{reasons}")

    results: list[dict[str, object]] = []
    current_values = dict(initial_values)
    for item in sorted(preset.items, key=lambda item: (item.order, item.capability_key)):
        capability = context.inverter.get_capability(item.capability_key)
        current_value = current_values.get(capability.value_key)
        target_label = capability.enum_value_map.get(item.value, item.value)
        if current_value == item.value or current_value == target_label:
            results.append(
                {
                    "key": capability.key,
                    "status": "unchanged",
                    "current_value": current_value,
                    "target_value": target_label,
                }
            )
            continue

        written_value = await context.driver.async_write_capability(
            context.transport,
            context.inverter,
            capability.key,
            item.value,
        )
        current_values = (await read_fixture_result(context)).values
        results.append(
            {
                "key": capability.key,
                "status": "written",
                "current_value": current_value,
                "target_value": target_label,
                "written_value": written_value,
            }
        )

    final_result = await read_fixture_result(context)
    final_values = final_result.values
    return {
        "preset": {
            "preset_key": preset.key,
            "title": preset.title,
            "warnings": list(runtime_state.warnings),
            "results": results,
        },
        "values": final_values,
        "read_result": _fixture_read_result_payload(final_result),
        "ui_schema": build_runtime_ui_schema(context.inverter, final_values),
    }


def build_fixture_snapshot(
    context: FixtureReplayContext,
    *,
    read_result: DriverReadResult,
    full_snapshot: bool,
) -> dict[str, object]:
    """Build a standard replay payload for CLI consumers."""

    if type(read_result) is not DriverReadResult:
        raise TypeError("fixture_read_result_invalid")
    values = read_result.values
    if not full_snapshot:
        return values
    return {
        "fixture": context.fixture,
        "inverter": {
            "driver_key": context.inverter.driver_key,
            "protocol_family": context.inverter.protocol_family,
            "model_name": context.inverter.model_name,
            "serial_number": context.inverter.serial_number,
            "probe_target": {
                "devcode": context.inverter.probe_target.devcode,
                "collector_addr": context.inverter.probe_target.collector_addr,
                "device_addr": context.inverter.probe_target.device_addr,
            },
        },
        "values": values,
        "read_result": _fixture_read_result_payload(read_result),
        "ui_schema": build_runtime_ui_schema(context.inverter, values),
    }


def _fixture_read_result_payload(result: DriverReadResult) -> dict[str, object]:
    """Serialize fixture read semantics separately from produced values."""

    return {
        "mode": result.mode.value,
        "removed_keys": sorted(result.removed_keys),
        "diagnostics": dict(result.diagnostics),
    }


def _driver_hint_for_fixture(raw: dict[str, Any], *, driver_hint: str) -> str:
    if driver_hint != DRIVER_HINT_AUTO:
        return driver_hint
    fixture_driver_key = str(raw.get("driver_key", "")).strip()
    if fixture_driver_key:
        return fixture_driver_key
    return DRIVER_HINT_AUTO


async def _async_detect_fixture_inverter(
    transport: Any,
    *,
    driver_hint: str,
):
    errors: list[str] = []

    for driver in iter_replay_drivers(driver_hint):
        for target in driver.probe_targets:
            try:
                inverter = await driver.async_probe(transport, target)
            except Exception as exc:
                errors.append(f"{driver.key}:{exc}")
                continue

            if inverter is None:
                continue

            return FixtureReplayContext(
                fixture={},
                transport=transport,
                driver=driver,
                inverter=inverter,
            )

    raise RuntimeError(errors[-1] if errors else "no_supported_driver_matched")
