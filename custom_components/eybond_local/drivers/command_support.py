"""Per-device negative cache for commands an inverter does not answer.

Every unanswered command costs a full request timeout, and retrying the whole
set each poll cycle turns a ~2-second poll into a ~60-second poll on devices
that lack the optional/energy command set. A command that fails several times
in a row is marked unsupported for this device and stays that way: the coordinator
persists the set into the config entry, and re-probing happens only when the
user explicitly asks for it (the "Re-check supported commands" diagnostic
button) — not on a timer.

Shared by the ASCII protocol drivers (PI30, PI18, EyeBond G-ASCII).
"""

from __future__ import annotations

from typing import Any

UNSUPPORTED_COMMANDS_STATE_KEY = "driver_unsupported_commands"
UNSUPPORTED_COMMANDS_VALUE_KEY = "driver_unsupported_commands"
UNSUPPORTED_COMMAND_STRIKES = 4
_PENDING_FAILURES_KEY = "driver_unsupported_pending_failures"
_CYCLE_SUCCESS_KEY = "driver_cycle_had_success"


def _table(runtime_state: dict[str, Any] | None) -> dict[str, int] | None:
    if runtime_state is None:
        return None
    return runtime_state.setdefault(UNSUPPORTED_COMMANDS_STATE_KEY, {})


def command_skipped_as_unsupported(
    runtime_state: dict[str, Any] | None,
    cache_key: str,
) -> bool:
    """Return whether one command is known-unsupported for this device."""

    table = _table(runtime_state)
    if not table:
        return False
    return int(table.get(cache_key, 0)) >= UNSUPPORTED_COMMAND_STRIKES


def record_command_failure(
    runtime_state: dict[str, Any] | None,
    cache_key: str,
) -> None:
    """Stage one failure; it only counts if the same cycle answered something.

    A cycle where every command fails is a link problem, not evidence that
    the commands are unsupported — those staged failures are discarded at
    :func:`commit_cycle_failures`.
    """

    if runtime_state is None:
        return
    pending = runtime_state.setdefault(_PENDING_FAILURES_KEY, [])
    if cache_key not in pending:
        pending.append(cache_key)


def record_command_success(
    runtime_state: dict[str, Any] | None,
    cache_key: str,
) -> None:
    if runtime_state is None:
        return
    runtime_state[_CYCLE_SUCCESS_KEY] = True
    table = _table(runtime_state)
    if table is not None:
        table.pop(cache_key, None)


def commit_cycle_failures(runtime_state: dict[str, Any] | None) -> None:
    """Apply staged failures when the cycle proved the device link works."""

    if runtime_state is None:
        return
    pending = runtime_state.pop(_PENDING_FAILURES_KEY, [])
    had_success = bool(runtime_state.pop(_CYCLE_SUCCESS_KEY, False))
    if not pending or not had_success:
        return
    table = _table(runtime_state)
    if table is None:
        return
    for cache_key in pending:
        table[cache_key] = min(
            int(table.get(cache_key, 0)) + 1,
            UNSUPPORTED_COMMAND_STRIKES,
        )


def unsupported_commands(runtime_state: dict[str, Any] | None) -> tuple[str, ...]:
    """Return the sorted set of commands marked unsupported."""

    table = _table(runtime_state)
    if not table:
        return ()
    return tuple(
        sorted(
            cache_key
            for cache_key, strikes in table.items()
            if int(strikes) >= UNSUPPORTED_COMMAND_STRIKES
        )
    )


def seed_unsupported_commands(
    runtime_state: dict[str, Any] | None,
    cache_keys: tuple[str, ...] | list[str],
) -> None:
    """Mark commands unsupported directly (persisted set from the entry)."""

    table = _table(runtime_state)
    if table is None:
        return
    for cache_key in cache_keys:
        key = str(cache_key or "").strip()
        if key:
            table[key] = UNSUPPORTED_COMMAND_STRIKES


def clear_unsupported_commands(runtime_state: dict[str, Any] | None) -> None:
    """Forget the unsupported set so every command is probed again."""

    if runtime_state is not None:
        runtime_state.pop(UNSUPPORTED_COMMANDS_STATE_KEY, None)


def apply_unsupported_diagnostics(
    values: dict[str, Any],
    runtime_state: dict[str, Any] | None,
) -> None:
    """Expose the skipped set in the runtime values for diagnostics."""

    skipped = unsupported_commands(runtime_state)
    if skipped:
        values[UNSUPPORTED_COMMANDS_VALUE_KEY] = ", ".join(skipped)
