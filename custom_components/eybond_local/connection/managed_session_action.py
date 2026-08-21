"""Exact-session management action used by verified connection transitions."""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ManagedSessionRestartChannel:
    """One managed reset action over one immutable physical session.

    The controlled-reset verifier uses the same exact-session channel for
    identity, the destructive action, and teardown. The action is either a
    plain reboot or an endpoint write+apply; the latter is itself the reset
    boundary, so callers never write the endpoint and then reboot a second
    session.
    """

    def __init__(
        self,
        *,
        session_channel: Any,
        expected_session_id: str,
        endpoint: str = "",
        on_confirmed: Callable[[Any], None] | None = None,
    ) -> None:
        if (
            type(expected_session_id) is not str
            or not expected_session_id
            or expected_session_id != expected_session_id.strip()
        ):
            raise ValueError("managed_session_id_invalid")
        if type(endpoint) is not str or endpoint != endpoint.strip():
            raise ValueError("managed_endpoint_invalid")
        if on_confirmed is not None and not callable(on_confirmed):
            raise TypeError("managed_confirmation_callback_invalid")
        self._session_channel = session_channel
        self._expected_session_id = expected_session_id
        self._endpoint = endpoint
        self._on_confirmed = on_confirmed

    async def async_send_restart(self) -> None:
        if self._endpoint:
            result = await self._session_channel.async_write_endpoint(
                self._endpoint,
                apply_changes=True,
            )
        else:
            result = await self._session_channel.async_send_restart()
        receipt_session_id = (
            str(result.get("management_session_id") or "").strip()
            if isinstance(result, dict)
            else ""
        )
        logger.info(
            "Recovery management action completed "
            "expected_session=%s receipt_session=%s action=%s",
            self._expected_session_id,
            receipt_session_id or "missing",
            "endpoint_apply" if self._endpoint else "reboot",
        )
        if receipt_session_id != self._expected_session_id:
            raise RuntimeError("transition_management_session_receipt_invalid")
        if self._on_confirmed is not None:
            self._on_confirmed(result)

    def observed_wire(self) -> str:
        return str(self._session_channel.observed_wire() or "").strip()

    async def async_probe_identity(self) -> str:
        return await self._session_channel.async_probe_identity()

    def is_connected(self) -> bool:
        try:
            return bool(self._session_channel.is_connected())
        except Exception:
            return False

    async def async_close(self) -> None:
        await self._session_channel.async_close()


__all__ = ["ManagedSessionRestartChannel"]
