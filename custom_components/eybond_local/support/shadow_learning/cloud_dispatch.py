"""Cancellation-safe cloud-action dispatch for shadow learning."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any, Callable


async def async_dispatch_cloud_action(
    callable_: Callable[..., Any],
    **kwargs: Any,
) -> Any:
    """Keep one blocking cloud action owned until its terminal outcome.

    Cancelling ``asyncio.to_thread`` does not stop its underlying HTTP request.
    Repeated cancellation is therefore deferred until that exact worker has
    completed, keeping the fail-closed shadow route active in the meantime.
    """

    task = asyncio.create_task(asyncio.to_thread(callable_, **kwargs))
    pending_cancel: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if pending_cancel is None:
                pending_cancel = exc
    if pending_cancel is not None:
        with suppress(Exception):
            task.result()
        raise pending_cancel
    return task.result()


__all__ = ["async_dispatch_cloud_action"]
