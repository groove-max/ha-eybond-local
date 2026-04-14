"""Factory for selecting one onboarding manager from a typed connection spec."""

from __future__ import annotations

from ..connection.branch_registry import get_connection_branch_for_spec
from ..connection.models import ConnectionSpec
from .manager import OnboardingManager


def create_onboarding_manager(
    connection: ConnectionSpec,
    *,
    driver_hint: str,
) -> OnboardingManager:
    """Create the concrete onboarding manager for one connection branch."""

    branch = get_connection_branch_for_spec(connection)
    return branch.create_onboarding_manager(
        connection,
        driver_hint=driver_hint,
    )
