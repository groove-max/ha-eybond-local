"""Sequential executor for the diagnostic command runner.

Home-Assistant-independent core: it takes a parsed scenario plus a
:class:`DiagnosticRuntimeContext` (transport, active driver/target defaults,
connection check, action defaults) and runs the commands one at a time, building
both a structured result and a human-readable text rendering.

The per-entry lock, task registration, and config-entry plumbing live in the
coordinator, which owns per-entry state. This module only guarantees that
``asyncio.CancelledError`` (unload/cancellation) propagates without leaving the
run in a half-applied state, and that no transport operation runs until the
whole scenario has been validated.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from ..models import ProbeTarget
from .diagnostic_commands import (
    AsciiCommand,
    Command,
    Directives,
    ReadCommand,
    ScenarioError,
    SleepCommand,
    WriteBitCommand,
    WriteCommand,
    ParsedScenario,
    parse_scenario,
)
from .diagnostic_targets import (
    AsciiOutcome,
    WriteBitOutcome,
    build_diagnostic_target,
    has_diagnostic_target,
    supported_kinds_for_driver,
)


class DiagnosticRunError(Exception):
    """Raised for non-line runtime preconditions (e.g. collector not connected)."""


_EYBOND_DEVCODE_MAX = 0xFFFF
_EYBOND_COLLECTOR_ADDR_MAX = 0xFF
_MODBUS_DEVICE_ADDR_MAX = 0xFF
_MODBUS_READ_MAX_REGISTERS = 125
_MODBUS_WRITE_MAX_REGISTERS = 123
_MODBUS_REGISTER_SPACE_SIZE = 0x10000


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class DiagnosticRuntimeContext:
    """Everything the runner needs from the live integration runtime."""

    transport: object
    active_driver_key: str | None = None
    active_probe_target: ProbeTarget | None = None
    configured_driver_hint: str | None = None
    driver_default_probe_target: Callable[[str], ProbeTarget | None] = lambda key: None
    is_connected: Callable[[], bool] = lambda: True
    entry_id: str = ""
    integration_version: str = ""
    catalog_detection: dict | None = None
    runtime_debug: dict | None = None
    default_stop_on_error: bool = True
    default_operation_timeout: float | None = None
    confirm_write: bool = False
    clock: Callable[[], datetime] = _default_clock


@dataclass(frozen=True, slots=True)
class DiagnosticRunResult:
    success: bool
    output: str
    results: list[dict]
    context: dict
    started_at: str
    finished_at: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _PreparedRun:
    commands: tuple[Command, ...]
    driver_key: str
    driver_source: str
    probe_target: ProbeTarget
    target: object
    stop_on_error: bool
    operation_timeout: float | None


# --- Resolution -----------------------------------------------------------


def _resolve_driver(
    directives: Directives, context: DiagnosticRuntimeContext
) -> tuple[str, str]:
    """Return (driver_key, driver_source)."""

    if directives.driver:
        driver_key, source = directives.driver, "scenario_override"
    elif context.active_driver_key:
        driver_key, source = context.active_driver_key, "active_runtime"
    elif context.configured_driver_hint and context.configured_driver_hint != "auto":
        driver_key, source = context.configured_driver_hint, "configured_hint"
    else:
        raise ScenarioError(
            "driver is required: no active runtime driver; add a 'driver' directive"
        )

    if not has_diagnostic_target(driver_key):
        # Refine the message only on the error path (avoids importing the driver
        # registry for the common, valid case).
        try:
            from ..drivers.registry import get_driver

            get_driver(driver_key)
        except KeyError:
            raise ScenarioError(f"driver '{driver_key}' is not a known driver")
        except Exception:  # pragma: no cover - registry import guard
            pass
        raise ScenarioError(
            f"driver '{driver_key}' does not support diagnostic commands"
        )
    return driver_key, source


def _resolve_probe_target(
    scenario: ParsedScenario,
    driver_key: str,
    context: DiagnosticRuntimeContext,
) -> ProbeTarget:
    directives = scenario.directives
    base = context.active_probe_target
    if base is None:
        base = context.driver_default_probe_target(driver_key)

    def pick(field_name: str, override: int | None) -> int | None:
        if override is not None:
            return override
        if base is not None:
            return getattr(base, field_name)
        return None

    devcode = pick("devcode", directives.devcode)
    collector_addr = pick("collector_addr", directives.collector_addr)
    device_addr = pick("device_addr", directives.device_addr)

    for name, value in (
        ("devcode", devcode),
        ("collector_addr", collector_addr),
        ("device_addr", device_addr),
    ):
        if value is None:
            raise ScenarioError(
                f"{name} is required: no active runtime target; "
                f"add a '{name}' directive"
            )
    _check_route_wire_limits(
        driver_key=driver_key,
        devcode=int(devcode),
        collector_addr=int(collector_addr),
        device_addr=int(device_addr),
        directive_lines=scenario.directive_lines,
    )
    return ProbeTarget(
        devcode=int(devcode),
        collector_addr=int(collector_addr),
        device_addr=int(device_addr),
    )


def _check_route_wire_limits(
    *,
    driver_key: str,
    devcode: int,
    collector_addr: int,
    device_addr: int,
    directive_lines: dict[str, int],
) -> None:
    if devcode > _EYBOND_DEVCODE_MAX:
        raise ScenarioError(
            f"devcode must be between 0 and {_EYBOND_DEVCODE_MAX}",
            line=directive_lines.get("devcode"),
        )
    if collector_addr > _EYBOND_COLLECTOR_ADDR_MAX:
        raise ScenarioError(
            f"collector_addr must be between 0 and {_EYBOND_COLLECTOR_ADDR_MAX}",
            line=directive_lines.get("collector_addr"),
        )
    if driver_key == "modbus_smg" and device_addr > _MODBUS_DEVICE_ADDR_MAX:
        raise ScenarioError(
            f"device_addr must be between 0 and {_MODBUS_DEVICE_ADDR_MAX} "
            "for driver 'modbus_smg'",
            line=directive_lines.get("device_addr"),
        )


def _check_command_support(
    commands: tuple[Command, ...], driver_key: str
) -> None:
    supported = supported_kinds_for_driver(driver_key)
    for command in commands:
        needs = command.requires
        if needs is not None and needs not in supported:
            raise ScenarioError(
                f"command '{needs}' is not supported by driver '{driver_key}'",
                line=command.line,
            )


def _check_command_wire_limits(
    commands: tuple[Command, ...],
    driver_key: str,
) -> None:
    if driver_key != "modbus_smg":
        return
    for command in commands:
        if isinstance(command, ReadCommand):
            if command.count > _MODBUS_READ_MAX_REGISTERS:
                raise ScenarioError(
                    f"read count must not exceed {_MODBUS_READ_MAX_REGISTERS}",
                    line=command.line,
                )
            if command.register + command.count > _MODBUS_REGISTER_SPACE_SIZE:
                raise ScenarioError(
                    "read range exceeds register 65535",
                    line=command.line,
                )
        elif isinstance(command, WriteCommand):
            if len(command.values) > _MODBUS_WRITE_MAX_REGISTERS:
                raise ScenarioError(
                    f"write count must not exceed {_MODBUS_WRITE_MAX_REGISTERS}",
                    line=command.line,
                )
            if command.register + len(command.values) > _MODBUS_REGISTER_SPACE_SIZE:
                raise ScenarioError(
                    "write range exceeds register 65535",
                    line=command.line,
                )


def _check_write_confirmation(
    commands: tuple[Command, ...], confirm_write: bool
) -> None:
    """Require explicit confirmation before any scenario that writes to the device."""

    if confirm_write:
        return
    for command in commands:
        if command.requires in ("write", "write_bit"):
            raise ScenarioError(
                "scenario contains write commands; set confirm_write=true to run "
                "writes that can change device settings",
                line=command.line,
            )


def _prepare(text: str, context: DiagnosticRuntimeContext) -> _PreparedRun:
    scenario = parse_scenario(text)  # syntax validation (raises ScenarioError)
    driver_key, driver_source = _resolve_driver(scenario.directives, context)
    probe_target = _resolve_probe_target(scenario, driver_key, context)
    _check_command_support(scenario.commands, driver_key)
    _check_command_wire_limits(scenario.commands, driver_key)
    _check_write_confirmation(scenario.commands, context.confirm_write)

    target = build_diagnostic_target(driver_key, context.transport, probe_target)

    if not context.is_connected():
        raise DiagnosticRunError("collector_not_connected")

    stop_on_error = (
        scenario.directives.stop_on_error
        if scenario.directives.stop_on_error is not None
        else context.default_stop_on_error
    )
    operation_timeout = (
        scenario.directives.operation_timeout
        if scenario.directives.operation_timeout is not None
        else context.default_operation_timeout
    )
    return _PreparedRun(
        commands=scenario.commands,
        driver_key=driver_key,
        driver_source=driver_source,
        probe_target=probe_target,
        target=target,
        stop_on_error=stop_on_error,
        operation_timeout=operation_timeout,
    )


# --- Execution ------------------------------------------------------------


async def _maybe_timeout(coro, timeout: float | None):
    """Await ``coro`` with an optional per-operation timeout."""

    if timeout is None:
        return await coro
    return await asyncio.wait_for(coro, timeout)


async def _execute_command(
    command: Command, target: object, timeout: float | None
) -> tuple[dict, dict]:
    """Return (request, response) dicts for one command, raising on failure."""

    if isinstance(command, ReadCommand):
        words = await _maybe_timeout(
            target.read_holding(command.register, command.count), timeout
        )
        decimal, hexwords, ascii_repr = _format_words(words)
        request = {"register": command.register, "count": command.count}
        response = {
            "register": command.register,
            "count": command.count,
            "decimal": decimal,
            "hex": hexwords,
            "ascii": ascii_repr,
        }
        return request, response

    if isinstance(command, WriteCommand):
        await _maybe_timeout(
            target.write_holding(command.register, list(command.values)), timeout
        )
        request = {
            "register": command.register,
            "values": list(command.values),
            "count": len(command.values),
        }
        return request, {"register": command.register, "written": len(command.values)}

    if isinstance(command, WriteBitCommand):
        outcome: WriteBitOutcome = await _maybe_timeout(
            target.write_bit(command.register, command.bit_index, command.bit_value),
            timeout,
        )
        request = {
            "register": outcome.register,
            "bit_index": outcome.bit_index,
            "bit_value": outcome.bit_value,
            "mask": f"0x{outcome.mask:04X}",
        }
        response = {
            "before": {"decimal": outcome.before, "hex": f"0x{outcome.before:04X}"},
            "written": {"decimal": outcome.written, "hex": f"0x{outcome.written:04X}"},
        }
        return request, response

    if isinstance(command, AsciiCommand):
        outcome: AsciiOutcome = await _maybe_timeout(
            target.send_ascii(command.command, request_timeout=timeout), timeout
        )
        request = {"command": command.command}
        response = {
            "raw_hex": outcome.raw.hex(),
            "payload": outcome.payload,
            "decode_error": outcome.decode_error,
        }
        return request, response

    if isinstance(command, SleepCommand):
        # Sleep is a deliberate delay, not a transport operation: it is not
        # wrapped by operation_timeout.
        await asyncio.sleep(command.milliseconds / 1000.0)
        return {"milliseconds": command.milliseconds}, {"milliseconds": command.milliseconds}

    raise DiagnosticRunError(f"unhandled_command:{type(command).__name__}")  # pragma: no cover


async def _execute(prepared: _PreparedRun) -> tuple[list[dict], bool]:
    results: list[dict] = []
    success = True
    for command in prepared.commands:
        start = time.monotonic()
        step: dict = {
            "line": command.line,
            "command": command.source,
            "kind": command.kind,
            "status": "ok",
            "duration_ms": 0,
            "request": {},
            "response": {},
            "error": None,
        }
        try:
            request, response = await _execute_command(
                command, prepared.target, prepared.operation_timeout
            )
            step["request"] = request
            step["response"] = response
        except asyncio.TimeoutError:
            step["status"] = "error"
            step["error"] = "operation_timeout"
            success = False
        except Exception as exc:  # noqa: BLE001 - report any device/transport error per step
            step["status"] = "error"
            step["error"] = str(exc) or type(exc).__name__
            success = False
        finally:
            step["duration_ms"] = int((time.monotonic() - start) * 1000)
        results.append(step)
        if step["status"] == "error" and prepared.stop_on_error:
            break
    return results, success


# --- Formatting -----------------------------------------------------------


def _format_words(words: list[int]) -> tuple[list[int], list[str], str | None]:
    decimal = [int(w) for w in words]
    hexwords = [f"0x{int(w) & 0xFFFF:04X}" for w in words]
    raw = b"".join((int(w) & 0xFFFF).to_bytes(2, "big") for w in words)
    text = "".join(chr(b) for b in raw if 0x20 <= b <= 0x7E)
    return decimal, hexwords, (text or None)


def _render_text(prepared: _PreparedRun, results: list[dict]) -> str:
    lines: list[str] = []
    lines.append(f"driver: {prepared.driver_key} ({prepared.driver_source})")
    pt = prepared.probe_target
    lines.append(
        f"route: devcode={pt.devcode} collector_addr={pt.collector_addr} "
        f"device_addr={pt.device_addr}"
    )
    lines.append("")
    for step in results:
        lines.append(f"[{step['line']}] {step['command']}")
        lines.append(f"status: {step['status']}")
        lines.append(f"duration_ms: {step['duration_ms']}")
        if step["status"] == "error":
            lines.append(f"error: {step['error']}")
        else:
            _render_step_body(step, lines)
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def _render_step_body(step: dict, lines: list[str]) -> None:
    kind = step["kind"]
    response = step["response"]
    if kind == "modbus_read":
        lines.append("decimal: " + " ".join(str(v) for v in response["decimal"]))
        lines.append("hex: " + " ".join(response["hex"]))
        if response.get("ascii"):
            lines.append(f'ascii: "{response["ascii"]}"')
    elif kind == "modbus_write":
        request = step["request"]
        lines.append(
            f"register: {request['register']} values: "
            + " ".join(str(v) for v in request["values"])
        )
    elif kind == "modbus_write_bit":
        before = response["before"]
        written = response["written"]
        lines.append(f"mask: {step['request']['mask']}")
        lines.append(f"before: {before['decimal']} ({before['hex']})")
        lines.append(f"written: {written['decimal']} ({written['hex']})")
    elif kind == "ascii":
        if response.get("payload") is not None:
            lines.append(f"payload: {response['payload']}")
        if response.get("decode_error"):
            lines.append(f"decode_error: {response['decode_error']}")
        lines.append(f"raw_hex: {response['raw_hex']}")
    elif kind == "sleep":
        lines.append(f"slept_ms: {response['milliseconds']}")


def _build_context(prepared: _PreparedRun, context: DiagnosticRuntimeContext) -> dict:
    pt = prepared.probe_target
    return {
        "integration_version": context.integration_version,
        "entry_id": context.entry_id,
        "selected_driver_key": prepared.driver_key,
        "driver_source": prepared.driver_source,
        "probe_target": {
            "devcode": pt.devcode,
            "collector_addr": pt.collector_addr,
            "device_addr": pt.device_addr,
        },
        "catalog_detection": context.catalog_detection or {},
        "runtime_debug": context.runtime_debug or {},
    }


# --- Public entry point ---------------------------------------------------


async def run_scenario(
    text: str, context: DiagnosticRuntimeContext
) -> DiagnosticRunResult:
    """Validate then run one scenario, returning a structured result.

    Validation problems (syntax, unknown driver, unsupported command, missing
    route field, not connected) are reported as ``success=False`` with no steps
    executed — no transport operation runs until the whole scenario validates.
    """

    started_at = context.clock().isoformat()
    try:
        prepared = _prepare(text, context)
    except (ScenarioError, DiagnosticRunError) as exc:
        finished_at = context.clock().isoformat()
        return DiagnosticRunResult(
            success=False,
            output=str(exc),
            results=[],
            context={
                "integration_version": context.integration_version,
                "entry_id": context.entry_id,
            },
            started_at=started_at,
            finished_at=finished_at,
            error=str(exc),
        )

    results, success = await _execute(prepared)
    finished_at = context.clock().isoformat()
    return DiagnosticRunResult(
        success=success,
        output=_render_text(prepared, results),
        results=results,
        context=_build_context(prepared, context),
        started_at=started_at,
        finished_at=finished_at,
        error=None if success else "one or more commands failed",
    )


class DiagnosticSingleFlight:
    """Run at most one diagnostic scenario at a time, with cancellation support.

    Owns the per-entry guard the spec assigns to the runner: a second run while
    one is in flight is rejected (not queued), the active state is signalled to
    the caller via callbacks (so it can quiesce polling), and :meth:`cancel`
    aborts the in-flight run on unload without leaving the lock held.
    """

    def __init__(self, *, busy_error: str = "diagnostic_run_in_progress") -> None:
        self._lock = asyncio.Lock()
        self._task: "asyncio.Future | None" = None
        self._busy_error = busy_error

    @property
    def running(self) -> bool:
        return self._lock.locked()

    async def run(
        self,
        factory: Callable[[], Awaitable],
        *,
        on_start: Callable[[], None] | None = None,
        on_finish: Callable[[], None] | None = None,
    ):
        if self._lock.locked():
            raise RuntimeError(self._busy_error)
        async with self._lock:
            if on_start is not None:
                on_start()
            try:
                task = asyncio.ensure_future(factory())
                self._task = task
                try:
                    return await task
                finally:
                    self._task = None
            finally:
                if on_finish is not None:
                    on_finish()

    async def cancel(self) -> None:
        task = self._task
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
