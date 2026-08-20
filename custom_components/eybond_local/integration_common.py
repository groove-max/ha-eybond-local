"""Shared Home Assistant lifecycle compatibility and task helpers."""

from __future__ import annotations

import asyncio
import logging

try:
    from homeassistant.const import (
        EVENT_COMPONENT_LOADED,
        EVENT_HOMEASSISTANT_STARTED,
        EVENT_HOMEASSISTANT_STOP,
    )
except (ImportError, ModuleNotFoundError):
    EVENT_COMPONENT_LOADED = "component_loaded"
    EVENT_HOMEASSISTANT_STARTED = "homeassistant_started"
    EVENT_HOMEASSISTANT_STOP = "homeassistant_stop"

try:
    from homeassistant.exceptions import ConfigEntryError, ConfigEntryNotReady
except ModuleNotFoundError:
    class ConfigEntryNotReady(Exception):
        """Fallback used by local tooling when Home Assistant is unavailable."""

    class ConfigEntryError(Exception):
        """Fallback used by local tooling when Home Assistant is unavailable."""


logger = logging.getLogger(__name__)


def _cancel_task_callback(task: asyncio.Task) -> None:
    """Cancel one background task from a Home Assistant unload callback."""

    task.cancel()


def _log_abandoned_shutdown_result(task: asyncio.Task) -> None:
    """Retrieve the result of a shutdown task abandoned after its timeout."""

    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Abandoned EyeBond shutdown task failed: %s", exc, exc_info=exc)
