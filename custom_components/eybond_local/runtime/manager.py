"""Generic runtime-manager contract for connection-specific runtime branches."""

from __future__ import annotations

from typing import Any, Protocol

from ..models import RuntimeSnapshot


class RuntimeManager(Protocol):
    """Runtime orchestration contract shared by all future connection branches."""

    async def async_start(self) -> None:
        ...

    async def async_stop(self) -> None:
        ...

    async def async_refresh(self, *, poll_interval: float | None = None) -> RuntimeSnapshot:
        ...

    async def async_write_capability(self, capability_key: str, value: Any) -> Any:
        ...

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        ...

    async def async_capture_support_evidence(self) -> dict[str, object]:
        ...
