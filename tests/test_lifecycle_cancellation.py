"""Batch 8B.2A listener-acquire cancellation-safety (fast, no HA runtime).

The shared-listener acquire must never leak a refcount / registry entry / bound
port when it is cancelled mid-bind. (The ``_await_critical`` unit tests live in
``tests_ha`` because they import the config flow, which needs a current HA.)
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import socket
import sys
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.mark.asyncio
async def test_acquire_shared_listener_no_leak_on_cancel_mid_bind() -> None:
    from custom_components.eybond_local.collector import transport

    port = _free_port()
    started = asyncio.Event()
    real_start_server = asyncio.start_server

    async def _blocking_start_server(*args, **kwargs):
        # Block DURING the bind (after acquire() reserved the refcount), then
        # honor a cancel raised at this await.
        started.set()
        await asyncio.Event().wait()  # never set -> only a cancel unblocks it
        return await real_start_server(*args, **kwargs)

    with patch("asyncio.start_server", _blocking_start_server):
        task = asyncio.ensure_future(
            transport._acquire_shared_listener("127.0.0.1", port)
        )
        await asyncio.wait_for(started.wait(), timeout=5)  # mid-bind, refcount++
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # No leaked listener / refcount / registry entry.
    assert ("127.0.0.1", port) not in transport._LISTENERS

    # The port is genuinely free: a real acquire binds + releases cleanly.
    listener = await transport._acquire_shared_listener("127.0.0.1", port)
    closed = await listener.release()
    assert closed is True
    transport._LISTENERS.pop(("127.0.0.1", port), None)
