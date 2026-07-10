"""Parser and models for the diagnostic command runner scenario language.

This module is intentionally free of Home Assistant imports. It turns one
multi-line scenario into validated directives and commands, raising
:class:`ScenarioError` with the original 1-based line number on any syntax
problem. Semantic checks that need runtime state (driver existence, command vs
driver support, effective-target completeness) live in the runner and reuse the
same :class:`ScenarioError` type so every error renders as ``line N: message``.
"""

from __future__ import annotations

from dataclasses import dataclass


REGISTER_MIN = 0
REGISTER_MAX = 0xFFFF
VALUE_MIN = 0
VALUE_MAX = 0xFFFF
BIT_INDEX_MIN = 0
BIT_INDEX_MAX = 15


class ScenarioError(ValueError):
    """One scenario problem, optionally tied to a source line number."""

    def __init__(self, message: str, *, line: int | None = None) -> None:
        self.line = line
        self.raw_message = message
        super().__init__(f"line {line}: {message}" if line else message)


# --- Command models -------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Command:
    """Base executable command with its source location."""

    line: int
    source: str

    @property
    def kind(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    @property
    def requires(self) -> str | None:
        """Target primitive this command needs, or None when always available."""

        return None


@dataclass(frozen=True, slots=True)
class ReadCommand(Command):
    register: int = 0
    count: int = 1

    @property
    def kind(self) -> str:
        return "modbus_read"

    @property
    def requires(self) -> str | None:
        return "read"


@dataclass(frozen=True, slots=True)
class WriteCommand(Command):
    register: int = 0
    values: tuple[int, ...] = ()

    @property
    def kind(self) -> str:
        return "modbus_write"

    @property
    def requires(self) -> str | None:
        return "write"


@dataclass(frozen=True, slots=True)
class WriteBitCommand(Command):
    register: int = 0
    bit_index: int = 0
    bit_value: int = 0

    @property
    def kind(self) -> str:
        return "modbus_write_bit"

    @property
    def requires(self) -> str | None:
        return "write_bit"


@dataclass(frozen=True, slots=True)
class AsciiCommand(Command):
    command: str = ""

    @property
    def kind(self) -> str:
        return "ascii"

    @property
    def requires(self) -> str | None:
        return "ascii"


@dataclass(frozen=True, slots=True)
class SleepCommand(Command):
    milliseconds: int = 0

    @property
    def kind(self) -> str:
        return "sleep"


# --- Directives -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Directives:
    """Run-scoped overrides parsed from leading directive lines."""

    driver: str | None = None
    devcode: int | None = None
    collector_addr: int | None = None
    device_addr: int | None = None
    stop_on_error: bool | None = None
    operation_timeout: float | None = None


@dataclass(frozen=True, slots=True)
class ParsedScenario:
    directives: Directives
    commands: tuple[Command, ...]
    directive_lines: dict[str, int]


DIRECTIVE_KEYWORDS = frozenset(
    {
        "driver",
        "devcode",
        "collector_addr",
        "device_addr",
        "stop_on_error",
        "operation_timeout",
    }
)
COMMAND_KEYWORDS = frozenset({"read", "write", "write_bit", "ascii", "sleep"})


# --- Token helpers --------------------------------------------------------


def _parse_int(token: str, *, line: int, label: str) -> int:
    text = token.strip()
    try:
        if text.lower().startswith("0x") or text.lower().startswith("-0x"):
            return int(text, 16)
        return int(text, 10)
    except ValueError:
        raise ScenarioError(f"{label} must be an integer, got '{token}'", line=line)


def _parse_float(token: str, *, line: int, label: str) -> float:
    try:
        return float(token)
    except ValueError:
        raise ScenarioError(f"{label} must be a number, got '{token}'", line=line)


def _require_args(args: list[str], count: int, *, line: int, usage: str) -> None:
    if len(args) != count:
        raise ScenarioError(f"expected {usage}", line=line)


# --- Directive parsing ----------------------------------------------------


def _apply_directive(
    builder: dict[str, object],
    keyword: str,
    args: list[str],
    *,
    line: int,
) -> None:
    if keyword == "driver":
        _require_args(args, 1, line=line, usage="driver <driver_key>")
        builder["driver"] = args[0]
        return
    if keyword in {"devcode", "collector_addr", "device_addr"}:
        _require_args(args, 1, line=line, usage=f"{keyword} <integer>")
        value = _parse_int(args[0], line=line, label=keyword)
        if value < 0:
            raise ScenarioError(f"{keyword} must not be negative", line=line)
        builder[keyword] = value
        return
    if keyword == "stop_on_error":
        _require_args(args, 1, line=line, usage="stop_on_error <true|false>")
        token = args[0].lower()
        if token not in {"true", "false"}:
            raise ScenarioError("stop_on_error must be true or false", line=line)
        builder["stop_on_error"] = token == "true"
        return
    if keyword == "operation_timeout":
        _require_args(args, 1, line=line, usage="operation_timeout <seconds>")
        seconds = _parse_float(args[0], line=line, label="operation_timeout")
        if seconds <= 0:
            raise ScenarioError("operation_timeout must be positive", line=line)
        builder["operation_timeout"] = seconds
        return
    raise ScenarioError(f"unknown directive '{keyword}'", line=line)  # pragma: no cover


# --- Command parsing ------------------------------------------------------


def _parse_command(
    keyword: str,
    args: list[str],
    *,
    line: int,
    source: str,
    ascii_remainder: str,
) -> Command:
    if keyword == "read":
        if len(args) not in (1, 2):
            raise ScenarioError("expected read <register> [count]", line=line)
        register = _parse_int(args[0], line=line, label="register")
        _check_register(register, line=line)
        count = _parse_int(args[1], line=line, label="count") if len(args) == 2 else 1
        if count < 1:
            raise ScenarioError("read count must be positive", line=line)
        return ReadCommand(line=line, source=source, register=register, count=count)

    if keyword == "write":
        if len(args) < 2:
            raise ScenarioError("expected write <register> <value> [value ...]", line=line)
        register = _parse_int(args[0], line=line, label="register")
        _check_register(register, line=line)
        values: list[int] = []
        for token in args[1:]:
            value = _parse_int(token, line=line, label="value")
            if value < VALUE_MIN or value > VALUE_MAX:
                raise ScenarioError(
                    f"value must be between {VALUE_MIN} and {VALUE_MAX}", line=line
                )
            values.append(value)
        return WriteCommand(
            line=line, source=source, register=register, values=tuple(values)
        )

    if keyword == "write_bit":
        _require_args(args, 3, line=line, usage="write_bit <register> <bit_index> <0|1>")
        register = _parse_int(args[0], line=line, label="register")
        _check_register(register, line=line)
        bit_index = _parse_int(args[1], line=line, label="bit index")
        if bit_index < BIT_INDEX_MIN or bit_index > BIT_INDEX_MAX:
            raise ScenarioError(
                f"bit index must be between {BIT_INDEX_MIN} and {BIT_INDEX_MAX}", line=line
            )
        bit_value = _parse_int(args[2], line=line, label="bit value")
        if bit_value not in (0, 1):
            raise ScenarioError("bit value must be 0 or 1", line=line)
        return WriteBitCommand(
            line=line,
            source=source,
            register=register,
            bit_index=bit_index,
            bit_value=bit_value,
        )

    if keyword == "ascii":
        command = ascii_remainder.strip()
        if not command:
            raise ScenarioError("expected ascii <command>", line=line)
        if not command.isascii():
            raise ScenarioError("ascii command must be ASCII", line=line)
        return AsciiCommand(line=line, source=source, command=command)

    if keyword == "sleep":
        _require_args(args, 1, line=line, usage="sleep <milliseconds>")
        milliseconds = _parse_int(args[0], line=line, label="sleep")
        if milliseconds < 0:
            raise ScenarioError("sleep must not be negative", line=line)
        return SleepCommand(line=line, source=source, milliseconds=milliseconds)

    raise ScenarioError(f"unknown command '{keyword}'", line=line)  # pragma: no cover


def _check_register(register: int, *, line: int) -> None:
    if register < REGISTER_MIN or register > REGISTER_MAX:
        raise ScenarioError(
            f"register must be between {REGISTER_MIN} and {REGISTER_MAX}", line=line
        )


# --- Top-level parse ------------------------------------------------------


def parse_scenario(text: str) -> ParsedScenario:
    """Parse and syntactically validate one scenario.

    Raises :class:`ScenarioError` with a line number on the first problem.
    """

    directive_builder: dict[str, object] = {}
    directive_lines: dict[str, int] = {}
    commands: list[Command] = []
    seen_command = False

    for index, raw_line in enumerate((text or "").splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped or stripped[0] == "#":
            continue

        keyword = stripped.split(None, 1)[0]
        # Preserve everything after the keyword verbatim for `ascii` (it may carry
        # spaces/args), while whitespace-splitting it for the structured commands.
        rest = stripped[len(keyword):]
        ascii_remainder = rest.strip()
        args = rest.split()

        if keyword in DIRECTIVE_KEYWORDS:
            if seen_command:
                raise ScenarioError(
                    f"directive '{keyword}' must appear before the first command",
                    line=index,
                )
            _apply_directive(directive_builder, keyword, args, line=index)
            directive_lines[keyword] = index
            continue

        if keyword in COMMAND_KEYWORDS:
            seen_command = True
            commands.append(
                _parse_command(
                    keyword,
                    args,
                    line=index,
                    source=stripped,
                    ascii_remainder=ascii_remainder,
                )
            )
            continue

        raise ScenarioError(f"unknown command '{keyword}'", line=index)

    directives = Directives(
        driver=directive_builder.get("driver"),  # type: ignore[arg-type]
        devcode=directive_builder.get("devcode"),  # type: ignore[arg-type]
        collector_addr=directive_builder.get("collector_addr"),  # type: ignore[arg-type]
        device_addr=directive_builder.get("device_addr"),  # type: ignore[arg-type]
        stop_on_error=directive_builder.get("stop_on_error"),  # type: ignore[arg-type]
        operation_timeout=directive_builder.get("operation_timeout"),  # type: ignore[arg-type]
    )
    return ParsedScenario(
        directives=directives,
        commands=tuple(commands),
        directive_lines=directive_lines,
    )
