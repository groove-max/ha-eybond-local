"""Factory for selecting one runtime manager from a typed connection spec."""

from __future__ import annotations

from ..connection.branch_registry import get_connection_branch_for_spec
from ..connection.models import ConnectionSpec
from .manager import RuntimeManager


def create_runtime_manager(
    connection: ConnectionSpec,
    *,
    driver_hint: str,
    connection_mode: str = "",
) -> RuntimeManager:
    """Create the concrete runtime manager for one connection branch."""

    branch = get_connection_branch_for_spec(connection)
    return branch.create_runtime_manager(
        connection,
        driver_hint=driver_hint,
        connection_mode=connection_mode,
    )
