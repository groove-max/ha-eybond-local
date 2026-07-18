"""Batch 8B.2A: ``_await_critical`` cancellation-safety (HA lane -- config flow).

The finalization helper runs its critical coroutine to COMPLETION even when the
caller is cancelled, then re-raises ``CancelledError`` -- a successful critical
result never turns a cancelled task into a normal completion, and repeated
cancels never interrupt the cleanup.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.config_flow import (  # noqa: E402
    EybondLocalOptionsFlow,
)


async def test_await_critical_completes_then_reraises_cancel() -> None:
    released = asyncio.Event()
    completed: list[bool] = []

    async def _critical() -> str:
        await released.wait()
        completed.append(True)
        return "done"

    async def _caller() -> str:
        return await EybondLocalOptionsFlow._await_critical(_critical())

    task = asyncio.ensure_future(_caller())
    await asyncio.sleep(0.05)  # let the critical coroutine start + block
    task.cancel()
    await asyncio.sleep(0.05)  # the cancel is delivered; critical still blocked
    task.cancel()  # repeated cancels must NOT interrupt the cleanup
    await asyncio.sleep(0.05)
    assert completed == []  # the shielded critical work was NOT interrupted

    released.set()  # release the critical coroutine
    try:
        await task
        raised = False
    except asyncio.CancelledError:
        raised = True
    assert raised, "a cancelled caller must re-raise CancelledError"
    assert completed == [True]  # the critical work ran to completion
    assert task.cancelled()  # a success never turned the cancelled task normal


async def test_await_critical_returns_result_when_not_cancelled() -> None:
    async def _critical() -> str:
        return "ok"

    assert await EybondLocalOptionsFlow._await_critical(_critical()) == "ok"
