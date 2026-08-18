"""Factory for selecting one onboarding manager from a typed connection spec."""

from __future__ import annotations

from ..connection.branch_registry import get_connection_branch_for_spec
from ..connection.models import ConnectionSpec, EybondConnectionSpec
from ..const import CONNECTION_TYPE_EYBOND
from .eybond import OnboardingDetector
from .manager import OnboardingManager


def create_onboarding_manager(
    connection: ConnectionSpec,
) -> OnboardingManager:
    """Create the concrete onboarding manager for one connection branch."""

    branch = get_connection_branch_for_spec(connection)
    if branch.connection_type != CONNECTION_TYPE_EYBOND or not isinstance(
        connection, EybondConnectionSpec
    ):
        raise ValueError(
            "connection_spec_branch_mismatch:"
            f"{branch.connection_type}:{type(connection).__name__}"
        )
    return OnboardingDetector(connection=connection)
