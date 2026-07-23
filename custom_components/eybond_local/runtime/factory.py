"""Factory for selecting one runtime manager from a typed connection spec."""

from __future__ import annotations

from ..connection.branch_registry import get_connection_branch_for_spec
from ..connection.models import ConnectionSpec
from ..const import DEFAULT_DRIVER_DETECTION_STRATEGY
from .manager import RuntimeManager


def create_runtime_manager(
    connection: ConnectionSpec,
    *,
    driver_hint: str,
    driver_detection_strategy: str = DEFAULT_DRIVER_DETECTION_STRATEGY,
    connection_mode: str = "",
) -> RuntimeManager:
    """Create the concrete runtime manager for one connection branch."""

    branch = get_connection_branch_for_spec(connection)
    return branch.create_runtime_manager(
        connection,
        driver_hint=driver_hint,
        driver_detection_strategy=driver_detection_strategy,
        connection_mode=connection_mode,
    )
