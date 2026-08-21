"""Public coordinator package without eager Home Assistant side effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .root import EybondLocalCoordinator

__all__ = ["EybondLocalCoordinator"]


def __getattr__(name: str) -> Any:
    """Load the Home Assistant composition root only when it is requested."""

    if name == "EybondLocalCoordinator":
        from .root import EybondLocalCoordinator

        return EybondLocalCoordinator
    raise AttributeError(name)
