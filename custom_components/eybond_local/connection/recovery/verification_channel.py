"""Restart and exact-session identity channel over negotiated collector wire."""

from __future__ import annotations

from contextlib import suppress
from typing import Any, Callable

from ...collector.management import (
    CollectorManagementUnsupportedError,
    select_collector_management_adapter,
)
from .verification_models import FAILURE_SESSION_UNAVAILABLE, SessionUnavailableError

class ObservedSessionRestartChannel:
    """Restart channel over an already-observed passive listener session.

    Claims exactly the registry-owned ``session_id`` through the shared
    transports' claimed-session mechanism (never peer IP, never a PN index) and
    performs BOTH management operations through neutral, wire-negotiated seams:

    * the REBOOT goes through :func:`select_collector_management_adapter`,
      keyed ONLY by the live ``SessionHandle.collector_management_adapter``
      resolved from ``handle_provider`` -- a framed session reboots via the
      framed adapter and an AT-text session through its negotiated AT adapter;
      all wire-specific command details remain inside those adapters;
    * the IDENTITY probe goes through the shared
      :class:`collector.session_identity_reader.SessionPinnedIdentityReader`
      on the negotiated wire.

    No SmartESS session class, no raw reboot wire helper, no FC/AT literals.
    Sends NO UDP.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        collector_pn: str,
        session_id: str,
        session_id_provider: Callable[[], str] | None = None,
        handle_provider: Callable[[], Any] | None = None,
        request_timeout: float = 5.0,
        heartbeat_interval: float = 60.0,
        claim_timeout: float = 5.0,
    ) -> None:
        self._host = str(host or "0.0.0.0")
        self._port = int(port)
        self._collector_pn = str(collector_pn or "").strip()
        self._session_id = str(session_id or "").strip()
        # Registry-owned claims are the ownership authority: when a provider is
        # given (the config flow's registry claim resolver), it decides which
        # session id the transports may claim.
        self._session_id_provider = session_id_provider
        # The live negotiated SessionHandle for the claimed session -- the ONLY
        # source of the management-adapter decision.
        self._handle_provider = handle_provider
        self._request_timeout = float(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._claim_timeout = float(claim_timeout)
        self._framed_transport: Any = None
        self._at_transport: Any = None

    def _resolve_session_id(self) -> str:
        """Resolve the registry-owned session id, with NO ownership fallback.

        The provider (the registry claim resolver) is the ONLY ownership
        source: without one, or with an empty result, this is an ERROR. The
        statically observed ``session_id`` is display/bookkeeping context and
        must never act as ownership -- the transport may not fall back to it,
        nor be allowed to pick some other socket by PN/IP.
        """

        provider = self._session_id_provider
        if provider is None:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        try:
            resolved = str(provider() or "").strip()
        except Exception as exc:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE) from exc
        if not resolved:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return resolved

    def observed_wire(self) -> str:
        """The live negotiated wire of the trusted handle, or "" fail-closed.

        The ONLY legitimate source of a silent-reconnect probe authority: the
        REAL observed, non-conflicting SessionHandle of the session this
        channel claims. No fallback of any kind.
        """

        try:
            handle = self._resolve_trusted_handle()
        except SessionUnavailableError:
            return ""
        if handle.uses_framed_wire:
            return "eybond_framed"
        if handle.uses_at_text_wire:
            return "at_text"
        return ""

    def _resolve_trusted_handle(self) -> Any:
        """Resolve the REAL, trusted, live negotiated SessionHandle -- or fail.

        Fail-closed BEFORE any transport exists, with no default of any kind:

        * no ``handle_provider`` -> error (nothing is "assumed framed");
        * provider error / ``None`` (e.g. the claimed socket is closed and the
          registry refuses to negotiate it) -> error;
        * a forged/duck object -> error (strict ``type() is SessionHandle`` --
          attribute look-alikes must never pick a wire);
        * ``observed`` False or a non-empty ``conflict`` -> error.
        """

        from ..session_handle import SessionHandle

        provider = self._handle_provider
        if provider is None:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        try:
            handle = provider()
        except Exception as exc:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE) from exc
        if type(handle) is not SessionHandle:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        if not handle.observed or handle.conflict:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        # The handle and transport claim must name the SAME physical socket.
        # A provider that moved to a same-PN sibling between these two reads is
        # not authority to continue on the sibling.
        if handle.session_id != self._resolve_session_id():
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return handle

    async def _async_ensure_framed_transport(self):
        """Activate and return the exact registry-owned framed session."""

        if self._framed_transport is not None:
            return self._framed_transport

        from ...collector.transport import SharedEybondTransport

        # Resolve strictly BEFORE touching any socket; a missing registry claim
        # aborts here and no transport is created at all.
        resolved_session_id = self._resolve_session_id()
        transport = SharedEybondTransport(
            host=self._host,
            port=self._port,
            request_timeout=self._request_timeout,
            heartbeat_interval=self._heartbeat_interval,
            collector_ip="",
            collector_pn="",
        )
        transport.set_claimed_session_provider(lambda: resolved_session_id)
        await transport.start()
        self._framed_transport = transport
        connected = await transport.wait_until_connected(timeout=self._claim_timeout)
        if not connected:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return transport

    async def _async_ensure_at_transport(self):
        """Activate and return the exact registry-owned AT session."""

        if self._at_transport is not None:
            return self._at_transport

        from ...collector.transport import SharedCollectorAtTransport
        from ..session_handle import WIRE_AT_TEXT

        resolved_session_id = self._resolve_session_id()
        transport = SharedCollectorAtTransport(
            host=self._host,
            port=self._port,
            request_timeout=self._request_timeout,
            collector_ip="",
            collector_pn="",
            collector_session_protocol=WIRE_AT_TEXT,
        )
        transport.set_claimed_session_provider(lambda: resolved_session_id)
        await transport.start()
        self._at_transport = transport
        connected = await transport.wait_until_connected(timeout=self._claim_timeout)
        if not connected:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return transport

    async def async_probe_identity(self) -> str:
        """Read full collector identity over the claimed session's live wire."""

        from ...collector.session_identity_reader import SessionPinnedIdentityReader
        from ..session_handle import WIRE_AT_TEXT, WIRE_FRAMED

        # The trusted live handle is the ONLY wire source: no provider, a
        # forged handle, an unobserved/conflicting one -- all fail typed here,
        # before any transport exists. Nothing is ever "assumed framed".
        handle = self._resolve_trusted_handle()
        wire = str(handle.wire_framing or "")
        if wire not in (WIRE_FRAMED, WIRE_AT_TEXT):
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        resolved_session_id = self._resolve_session_id()
        reader = SessionPinnedIdentityReader(
            host=self._host, request_timeout=self._request_timeout
        )
        full_pn, _source = await reader.async_read_full_pn(
            session_id=resolved_session_id,
            session_protocol=wire,
            listener_port=self._port,
        )
        return full_pn

    async def async_send_restart(self) -> dict[str, object]:
        """Reboot through the ONE management-adapter switch (negotiated wire)."""

        from ..session_handle import (
            ADAPTER_COLLECTOR_AT_COMMANDS,
            ADAPTER_COLLECTOR_FRAMED_COMMANDS,
        )

        # The trusted live handle is the ONLY adapter source (see
        # _resolve_trusted_handle): fail-closed before any transport exists.
        handle = self._resolve_trusted_handle()
        adapter_id = str(handle.collector_management_adapter or "")

        # Capability gate NEXT: an adapter that honestly cannot reboot must
        # fail typed before any socket is claimed or byte written.
        probe_adapter = select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: None,
            at_transport_provider=lambda: None,
        )
        if not probe_adapter.capabilities.reboot:
            raise CollectorManagementUnsupportedError(
                "collector_reboot_unsupported_on_negotiated_wire"
            )

        if adapter_id == ADAPTER_COLLECTOR_FRAMED_COMMANDS:
            transport = await self._async_ensure_framed_transport()
        elif adapter_id == ADAPTER_COLLECTOR_AT_COMMANDS:
            transport = await self._async_ensure_at_transport()
        else:  # pragma: no cover - the capability gate above already refused
            raise CollectorManagementUnsupportedError(
                "collector_reboot_unsupported_on_negotiated_wire"
            )
        adapter = select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: transport,
            at_transport_provider=lambda: transport,
        )
        result = await adapter.async_reboot()
        return {
            "status": "reboot_requested",
            "action": result.action,
            "performed": result.performed,
            "management_session_id": self._resolve_session_id(),
        }

    async def async_write_endpoint(
        self,
        endpoint: str,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        """Write/apply an endpoint on this exact physical session.

        This is the transition management boundary: unlike the generic runtime
        management path it never asks for an ``active_transport`` and never
        re-resolves by PN, peer IP or a mutable registry claim after the exact
        transport has been created.
        """

        from ..session_handle import (
            ADAPTER_COLLECTOR_AT_COMMANDS,
            ADAPTER_COLLECTOR_FRAMED_COMMANDS,
        )

        if type(endpoint) is not str or not endpoint or endpoint != endpoint.strip():
            raise ValueError("collector_endpoint_invalid")
        if type(apply_changes) is not bool:
            raise TypeError("apply_changes_must_be_bool")

        handle = self._resolve_trusted_handle()
        adapter_id = str(handle.collector_management_adapter or "")
        probe_adapter = select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: None,
            at_transport_provider=lambda: None,
        )
        capabilities = probe_adapter.capabilities
        if not capabilities.write_endpoint or (
            apply_changes and not capabilities.apply_changes
        ):
            raise CollectorManagementUnsupportedError(
                "collector_endpoint_write_unsupported_on_negotiated_wire"
            )

        if adapter_id == ADAPTER_COLLECTOR_FRAMED_COMMANDS:
            transport = await self._async_ensure_framed_transport()
        elif adapter_id == ADAPTER_COLLECTOR_AT_COMMANDS:
            transport = await self._async_ensure_at_transport()
        else:  # pragma: no cover - capability gate refuses first
            raise CollectorManagementUnsupportedError(
                "collector_endpoint_write_unsupported_on_negotiated_wire"
            )
        adapter = select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: transport,
            at_transport_provider=lambda: transport,
        )
        result = await adapter.async_write_endpoint(
            endpoint,
            apply_changes=apply_changes,
        )
        out: dict[str, object] = {
            "status": "applied" if result.apply_performed else "staged",
            "requested_endpoint": result.requested_endpoint,
            "readback_endpoint": result.readback_endpoint,
            "apply_changes": result.apply_requested,
            "write_confirmed": result.write_confirmed,
            "apply_performed": result.apply_performed,
            "confirmation_source": result.confirmation_source,
        }
        if result.previous_endpoint:
            out["previous_endpoint"] = result.previous_endpoint
        if result.reboot_or_apply_required:
            out["reboot_required"] = result.reboot_or_apply_required
        if result.warnings:
            out["warning"] = result.warnings[0]
        out.update(dict(result.extra or {}))
        # Set after adapter extras so a transport result can never forge the
        # load-bearing receipt for the physical socket that carried the write.
        out["management_session_id"] = self._resolve_session_id()
        return out

    def is_connected(self) -> bool:
        for transport in (self._framed_transport, self._at_transport):
            if transport is None:
                continue
            try:
                if bool(transport.connected):
                    return True
            except Exception:
                continue
        return False

    async def async_close(self) -> None:
        transports = (self._framed_transport, self._at_transport)
        self._framed_transport = None
        self._at_transport = None
        preserve_session_id = ""
        try:
            preserve_session_id = self._resolve_session_id()
        except SessionUnavailableError:
            # The owner may already have retargeted after a successful proof.
            # The static session id is bookkeeping only during operation, but
            # is safe as a teardown exclusion: it grants no wire access and
            # merely prevents this temporary facade from closing that socket.
            preserve_session_id = self._session_id
        for transport in transports:
            if transport is None:
                continue
            with suppress(Exception):
                await transport.stop(preserve_session_id=preserve_session_id)
