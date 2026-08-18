"""Factory for selecting one runtime manager from a typed connection spec."""

from __future__ import annotations

from ..connection.branch_registry import get_connection_branch_for_spec
from ..connection.models import ConnectionSpec, EybondConnectionSpec
from ..const import CONNECTION_TYPE_EYBOND, DEFAULT_DRIVER_DETECTION_STRATEGY
from .hub import EybondHub
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
        driver_detection_strategy=driver_detection_strategy,
        connection_mode=connection_mode,
    )
