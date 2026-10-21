"""Factory for selecting one runtime manager from a typed connection spec."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from ..connection.branch_registry import get_connection_branch_for_spec
from ..connection.models import ConnectionSpec, EybondConnectionSpec
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    PI30_FLAG_CASE_PROFILE,
)
from .hub import EybondHub
from .manager import RuntimeManager
from ..models import ProbeTarget


def create_runtime_manager(
    connection: ConnectionSpec,
    *,
    driver_hint: str,
    inverter_probe_target: ProbeTarget | None = None,
    driver_detection_strategy: str = DEFAULT_DRIVER_DETECTION_STRATEGY,
    pi30_flag_command_case: str = PI30_FLAG_CASE_PROFILE,
    connection_mode: str = "",
    collector_mutation_guard: Callable[[], Awaitable[None]] | None = None,
) -> RuntimeManager:
    """Create the concrete runtime manager for one connection branch."""

    branch = get_connection_branch_for_spec(connection)
    if branch.connection_type != CONNECTION_TYPE_EYBOND or not isinstance(
        connection, EybondConnectionSpec
    ):
        raise ValueError(
            "connection_spec_branch_mismatch:"
            f"{branch.connection_type}:{type(connection).__name__}"
        )
    return EybondHub(
        connection=connection,
        driver_hint=driver_hint,
        inverter_probe_target=inverter_probe_target,
        driver_detection_strategy=driver_detection_strategy,
        pi30_flag_command_case=pi30_flag_command_case,
        connection_mode=connection_mode,
        collector_mutation_guard=collector_mutation_guard,
    )
