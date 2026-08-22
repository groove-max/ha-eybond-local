"""Provider-neutral collector-management adapters.

The generic runtime (hub/coordinator) asks for collector-management ACTIONS
(read/write the collector's upstream endpoint, apply, reboot) but must never know
which wire protocol carries them. The negotiated
``SessionHandle.collector_management_adapter`` / ``ConfirmedWireBinding`` id is the
ONLY thing that selects an implementation here.

Each adapter encapsulates its wire (framed FC=2/FC=3, or AT-text CLDSRVHOST1 /
INTPARA) entirely; the parameter numbers and AT command strings live only inside
the implementations. Every operation returns the SAME normalized model, and an
operation a wire cannot confirm fails with a typed error under
``CollectorManagementError`` -- it never simulates success. Adapters resolve the
live transport lazily through a provider callable, so a reconnect/handover never
leaves them holding a stale socket.

Metadata polling is NOT part of this contract: it remains the hub/runtime's
existing responsibility (with its own dual-channel cadence/bootstrap/dead-channel
learning). Explicit user-initiated collector settings reads/writes do belong
here, because they must use the same negotiated management wire as endpoint
actions. This module carries no cloud-family / provider / collector-kind
knowledge.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from ..connection.session_handle import (
    ADAPTER_COLLECTOR_AT_COMMANDS,
    ADAPTER_COLLECTOR_FRAMED_COMMANDS,
    ADAPTER_NONE,
)
from ..collector_endpoint import normalize_collector_server_endpoint
from .at import CollectorAtError
from .collector_wire import (
    CollectorManagementError,
    CollectorManagementUnsupportedError,
    CollectorWireError,
    CollectorWireManagementSession,
    QUERY_REBOOT_REQUIRED,
    QUERY_SERIAL_BAUDRATE,
    SET_REBOOT_OR_APPLY,
    SET_SERIAL_BAUDRATE,
    SET_SERVER_ENDPOINT,
)

_AT_ENDPOINT_COMMAND = "CLDSRVHOST1"
_AT_APPLY_COMMAND = "INTPARA"
_AT_APPLY_VALUE = "29,1"
# The project's confirmed AT success status (see smartess_ble.extract_at_status_code).
_AT_SUCCESS_STATUS = "W000"

# Delivery failures (never a command-level fault). A wire/parser error whose
# ``__cause__`` is one of these is a transport failure, not a malformed response.
_TRANSPORT_FAILURES = (
    asyncio.TimeoutError,
    TimeoutError,
    OSError,
    ConnectionError,
    EOFError,
    asyncio.IncompleteReadError,
    asyncio.LimitOverrunError,
)


def _field_strip(endpoint: str) -> str:
    """Neutral field-level normalization used when an endpoint is not canonical."""

    return ",".join(part.strip() for part in str(endpoint or "").strip().split(",")).casefold()


def _endpoints_match(left: str, right: str) -> bool:
    """Provider-neutral endpoint equality.

    Prefers the shared canonical endpoint normalizer; for non-canonical forms it
    accepts (e.g. host-only) it falls back to a neutral field strip. No
    cloud-family / hostname / collector-kind branching -- both sides go through
    the SAME normalization and are compared as strings.
    """

    if not left or not right:
        return False
    try:
        return normalize_collector_server_endpoint(left) == (
            normalize_collector_server_endpoint(right)
        )
    except ValueError:
        return _field_strip(left) == _field_strip(right)


# ---------------------------------------------------------------------------
# Typed errors (all under CollectorManagementError, defined in collector_wire)
# ---------------------------------------------------------------------------


class CollectorManagementCommandError(CollectorManagementError):
    """A management command was rejected or returned an unexpected shape."""


class CollectorManagementTransportError(CollectorManagementError):
    """The management transport failed to carry the command."""


class CollectorManagementConfirmationError(CollectorManagementError):
    """A management command was sent but the collector did not confirm it."""


# ---------------------------------------------------------------------------
# Normalized models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CollectorManagementCapabilities:
    """Which management operations the selected adapter can actually confirm."""

    read_endpoint_state: bool = False
    write_endpoint: bool = False
    apply_changes: bool = False
    reboot: bool = False


@dataclass(frozen=True, slots=True)
class CollectorEndpointState:
    """The collector's current upstream endpoint and reboot-required flag."""

    current_endpoint: str
    reboot_required: str
    adapter_id: str


@dataclass(frozen=True, slots=True)
class CollectorEndpointWriteResult:
    """The unified, HONEST result of staging/applying a collector endpoint.

    ``readback_endpoint`` is ONLY the value actually read back from the collector
    (empty when the wire does not echo it) -- it is never the requested value in
    disguise. ``requested_endpoint`` is what we asked to write.
    ``write_confirmed`` means the write was acknowledged; ``confirmation_source``
    records HOW ("set_ack" / "readback" / "at_command_echo"). ``apply_performed``
    is True only when a requested apply was confirmed (an unconfirmed apply raises
    instead of returning).
    """

    previous_endpoint: str
    requested_endpoint: str
    readback_endpoint: str
    write_performed: bool
    write_confirmed: bool
    confirmation_source: str
    apply_requested: bool
    apply_performed: bool
    reboot_or_apply_required: str
    adapter_id: str
    warnings: tuple[str, ...] = ()
    extra: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CollectorSystemActionResult:
    """The unified result of a standalone apply/reboot system action."""

    action: str
    current_endpoint: str
    reboot_required_before: str
    performed: bool
    adapter_id: str
    warnings: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Adapter contract
# ---------------------------------------------------------------------------


class CollectorManagementAdapter(ABC):
    """Provider-neutral collector-management operations for one negotiated wire."""

    adapter_id: str = ADAPTER_NONE

    @property
    @abstractmethod
    def capabilities(self) -> CollectorManagementCapabilities:
        """Return which operations this adapter can confirm."""

    @abstractmethod
    async def async_read_endpoint_state(self) -> CollectorEndpointState:
        ...

    @abstractmethod
    async def async_write_endpoint(
        self, endpoint: str, *, apply_changes: bool = True
    ) -> CollectorEndpointWriteResult:
        ...

    @abstractmethod
    async def async_apply_changes(self) -> CollectorSystemActionResult:
        ...

    @abstractmethod
    async def async_reboot(self) -> CollectorSystemActionResult:
        ...

    @abstractmethod
    async def async_query_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        """Read explicit collector settings through this negotiated wire."""

    @abstractmethod
    async def async_set_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        """Write and apply collector Wi-Fi credentials when supported."""

    @abstractmethod
    async def async_set_uart_baudrate(self, baudrate: str) -> str:
        """Write and read back the collector UART baudrate when supported."""


def _wrap_wire_call(exc: Exception) -> CollectorManagementError:
    """Map a low-level wire/transport/parser exception to a typed management error.

    Taxonomy:
    * a MALFORMED response (``CollectorWireError`` / ``CollectorAtError`` from
      parsing) -> ``CollectorManagementCommandError``;
    * a DELIVERY failure (timeout / disconnect / OSError, including one the wire
      layer wrapped, e.g. an AT read timeout) -> ``CollectorManagementTransportError``.

    ``asyncio.CancelledError`` is a ``BaseException`` and is never routed here.
    """

    if isinstance(exc, CollectorManagementError):
        return exc
    if isinstance(exc, _TRANSPORT_FAILURES):
        return CollectorManagementTransportError(str(exc) or type(exc).__name__)
    if isinstance(exc, (CollectorWireError, CollectorAtError)):
        # A wire/parser error is command-level UNLESS it wraps a delivery failure
        # (e.g. CollectorAtError("at_response_timeout") from asyncio.TimeoutError).
        if isinstance(exc.__cause__, _TRANSPORT_FAILURES):
            return CollectorManagementTransportError(str(exc) or type(exc).__name__)
        return CollectorManagementCommandError(str(exc) or type(exc).__name__)
    return CollectorManagementTransportError(str(exc) or type(exc).__name__)


def _confirm_write(requested: str, readback: str, *, ack_source: str) -> str:
    """Return the confirmation source, or raise if the readback contradicts the write.

    * empty readback + a confirmed write ACK -> confirmed via ``ack_source``;
    * readback that matches the requested endpoint -> confirmed via "readback";
    * a NON-empty readback that does NOT match -> ``CollectorManagementConfirmationError``.

    A mismatched readback is never treated as confirmation. The error carries no
    endpoint value (kept out of diagnostics).
    """

    if not readback:
        return ack_source
    if _endpoints_match(readback, requested):
        return "readback"
    raise CollectorManagementConfirmationError("collector_readback_mismatch")


def _confirm_at_write(requested: str, readback: str, *, write_ack: bool) -> str:
    """AT endpoint-write confirmation (documented CLDSRVHOST1 semantics).

    * a matching readback is the STRONGEST confirmation -> "readback";
    * a non-empty readback that does NOT match -> ``CollectorManagementConfirmationError``;
    * an EMPTY readback is confirmed ONLY when the write response itself confirmed
      success (``write_ack`` -- a ``W000`` status or an exact endpoint echo) ->
      "at_command_echo"; a ``Wxxx != W000`` write status is not an ack, so an
      empty write ack + empty readback -> ``CollectorManagementConfirmationError``.
    """

    if readback:
        if _endpoints_match(readback, requested):
            return "readback"
        raise CollectorManagementConfirmationError("collector_readback_mismatch")
    if write_ack:
        return "at_command_echo"
    raise CollectorManagementConfirmationError("collector_at_write_unconfirmed")


# ---------------------------------------------------------------------------
# Framed (FC=2 / FC=3) implementation
# ---------------------------------------------------------------------------


class FramedCollectorManagementAdapter(CollectorManagementAdapter):
    """Collector management over the framed EyeBond FC=2/FC=3 wire.

    Parameter numbers (21 endpoint / 30 reboot-required / 29 apply) live only
    inside this class.
    """

    adapter_id = ADAPTER_COLLECTOR_FRAMED_COMMANDS
    _APPLY_WARNING = (
        "collector redirect apply accepted; the current session may disconnect "
        "before the next refresh"
    )
    _ACTION_WARNING = (
        "collector system action accepted; the current session may disconnect "
        "before the next refresh"
    )

    def __init__(self, transport_provider: Callable[[], object | None]) -> None:
        self._transport_provider = transport_provider

    @property
    def capabilities(self) -> CollectorManagementCapabilities:
        return CollectorManagementCapabilities(
            read_endpoint_state=True,
            write_endpoint=True,
            apply_changes=True,
            reboot=True,
        )

    def _resolve_transport(self) -> object:
        transport = self._transport_provider()
        if transport is None or not hasattr(transport, "async_send_collector"):
            raise CollectorManagementUnsupportedError(
                "collector_local_management_not_supported"
            )
        return transport

    def _session(self) -> tuple[CollectorWireManagementSession, object]:
        transport = self._resolve_transport()
        return CollectorWireManagementSession(transport), transport

    async def _query_confirmed(
        self, session: CollectorWireManagementSession, parameter: int
    ) -> str:
        """Return a STRICTLY validated FC=2 read: code 0 AND matching parameter.

        A non-zero code, a foreign parameter, or a malformed response is a
        ``CollectorManagementCommandError``; transport/parser failures are wrapped
        as ``CollectorManagementTransportError``. Never returns "" to mask a
        rejected read (unlike the lenient wire helper), so it is safe for a
        confirmed management result.
        """

        try:
            response = await session.query_collector(parameter)
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise _wrap_wire_call(exc) from exc
        code = getattr(response, "code", None)
        got_parameter = getattr(response, "parameter", None)
        if code != 0:
            raise CollectorManagementCommandError(
                f"collector_query_failed:parameter={parameter}:code={code}"
            )
        if got_parameter != parameter:
            raise CollectorManagementCommandError(
                f"collector_query_wrong_parameter:expected={parameter}:got={got_parameter}"
            )
        return str(getattr(response, "text", "") or "").strip().strip("\x00")

    async def _apply(self, session: CollectorWireManagementSession) -> None:
        """Send the FC=3 param-29 restart/apply and require confirmation."""

        try:
            response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001 - wrapped, never swallowed
            raise _wrap_wire_call(exc) from exc
        if response.status != 0 or response.parameter != SET_REBOOT_OR_APPLY:
            raise CollectorManagementConfirmationError(
                f"collector_apply_unconfirmed:parameter={SET_REBOOT_OR_APPLY}:"
                f"status={response.status}"
            )

    async def _query_flag(
        self, session: CollectorWireManagementSession, parameter: int
    ) -> str:
        """Read a secondary FLAG (e.g. reboot-required / previous endpoint).

        Best-effort ONLY for an unsupported/non-zero secondary flag: a non-zero
        code or a foreign parameter yields "" (the flag is treated as absent, not
        as a failure). A transport OR parser failure is NOT best-effort -- it is
        wrapped typed and DELIBERATELY interrupts the operation (a broken wire or a
        malformed frame is not "the flag is absent").
        """

        try:
            response = await session.query_collector(parameter)
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001 - wrapped typed; interrupts the op
            raise _wrap_wire_call(exc) from exc
        if getattr(response, "code", None) != 0 or getattr(response, "parameter", None) != parameter:
            return ""
        return str(getattr(response, "text", "") or "").strip().strip("\x00")

    async def async_read_endpoint_state(self) -> CollectorEndpointState:
        session, _ = self._session()
        current = await self._query_confirmed(session, SET_SERVER_ENDPOINT)
        reboot_required = await self._query_flag(session, QUERY_REBOOT_REQUIRED)
        return CollectorEndpointState(
            current_endpoint=current,
            reboot_required=reboot_required,
            adapter_id=self.adapter_id,
        )

    async def async_write_endpoint(
        self, endpoint: str, *, apply_changes: bool = True
    ) -> CollectorEndpointWriteResult:
        session, _ = self._session()
        previous = await self._query_flag(session, SET_SERVER_ENDPOINT)
        try:
            set_response = await session.set_collector(SET_SERVER_ENDPOINT, endpoint)
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise _wrap_wire_call(exc) from exc
        if set_response.status != 0 or set_response.parameter != SET_SERVER_ENDPOINT:
            raise CollectorManagementConfirmationError(
                f"collector_set_unconfirmed:parameter={SET_SERVER_ENDPOINT}:"
                f"status={set_response.status}"
            )
        readback = await self._query_confirmed(session, SET_SERVER_ENDPOINT)
        reboot_required = await self._query_flag(session, QUERY_REBOOT_REQUIRED)
        confirmation_source = _confirm_write(endpoint, readback, ack_source="set_ack")

        apply_performed = False
        warnings: tuple[str, ...] = ()
        if apply_changes:
            await self._apply(session)
            apply_performed = True
            warnings = (self._APPLY_WARNING,)

        return CollectorEndpointWriteResult(
            previous_endpoint=previous,
            requested_endpoint=endpoint,
            readback_endpoint=readback,
            write_performed=True,
            write_confirmed=True,
            confirmation_source=confirmation_source,
            apply_requested=apply_changes,
            apply_performed=apply_performed,
            reboot_or_apply_required=reboot_required,
            adapter_id=self.adapter_id,
            warnings=warnings,
        )

    async def _system_action(self, action: str) -> CollectorSystemActionResult:
        session, _ = self._session()
        current = await self._query_flag(session, SET_SERVER_ENDPOINT)
        reboot_required = await self._query_flag(session, QUERY_REBOOT_REQUIRED)
        await self._apply(session)
        return CollectorSystemActionResult(
            action=action,
            current_endpoint=current,
            reboot_required_before=reboot_required,
            performed=True,
            adapter_id=self.adapter_id,
            warnings=(self._ACTION_WARNING,),
        )

    async def async_apply_changes(self) -> CollectorSystemActionResult:
        return await self._system_action("apply")

    async def async_reboot(self) -> CollectorSystemActionResult:
        return await self._system_action("reboot")

    async def async_query_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        if (
            type(parameters) is not tuple
            or not parameters
            or any(type(parameter) is not int for parameter in parameters)
        ):
            raise TypeError("collector_parameter_tuple_required")
        session, _ = self._session()
        return {
            parameter: await self._query_confirmed(session, parameter)
            for parameter in parameters
        }

    async def async_set_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        if (
            type(ssid) is not str
            or not ssid
            or type(password) is not str
            or not password
            or type(ssid_parameter) is not int
            or type(password_parameter) is not int
        ):
            raise TypeError("collector_wifi_write_arguments_invalid")
        session, _ = self._session()
        for parameter, value in (
            (ssid_parameter, ssid),
            (password_parameter, password),
        ):
            try:
                response = await session.set_collector(parameter, value)
            except CollectorManagementError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise _wrap_wire_call(exc) from exc
            if response.status != 0 or response.parameter != parameter:
                raise CollectorManagementConfirmationError(
                    f"collector_set_unconfirmed:parameter={parameter}:"
                    f"status={response.status}"
                )
        readback = await self._query_confirmed(session, ssid_parameter)
        if readback != ssid:
            raise CollectorManagementConfirmationError(
                "collector_wifi_readback_mismatch"
            )
        await self._apply(session)
        return ssid

    async def async_set_uart_baudrate(self, baudrate: str) -> str:
        if (
            type(baudrate) is not str
            or not baudrate
            or baudrate != baudrate.strip()
            or not baudrate.isdecimal()
        ):
            raise TypeError("collector_uart_baudrate_invalid")
        session, _ = self._session()
        try:
            response = await session.set_collector(SET_SERIAL_BAUDRATE, baudrate)
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise _wrap_wire_call(exc) from exc
        if response.status != 0 or response.parameter != SET_SERIAL_BAUDRATE:
            raise CollectorManagementConfirmationError(
                f"collector_set_unconfirmed:parameter={SET_SERIAL_BAUDRATE}:"
                f"status={response.status}"
            )
        readback = await self._query_confirmed(session, QUERY_SERIAL_BAUDRATE)
        if readback.split(",", 1)[0].strip() != baudrate:
            raise CollectorManagementConfirmationError(
                "collector_uart_readback_mismatch"
            )
        return readback


# ---------------------------------------------------------------------------
# AT-text implementation
# ---------------------------------------------------------------------------


class AtTextCollectorManagementAdapter(CollectorManagementAdapter):
    """Collector management over the AT-text wire (CLDSRVHOST1 / INTPARA).

    The AT command strings live only inside this class. Endpoint read/write and
    apply are real and confirmed (command echo); reboot has no confirmed AT
    command yet, so it stays honestly unsupported.
    """

    adapter_id = ADAPTER_COLLECTOR_AT_COMMANDS
    _APPLY_WARNING = (
        "collector AT endpoint write accepted; the current session may disconnect "
        "before the next refresh"
    )
    _ACTION_WARNING = (
        "collector AT apply accepted; the current session may disconnect before "
        "the next refresh"
    )

    def __init__(self, transport_provider: Callable[[], object | None]) -> None:
        self._transport_provider = transport_provider

    @property
    def capabilities(self) -> CollectorManagementCapabilities:
        return CollectorManagementCapabilities(
            read_endpoint_state=True,
            write_endpoint=True,
            apply_changes=True,
            reboot=False,
        )

    def _resolve_transport(self, *, needs_write: bool = False) -> object:
        transport = self._transport_provider()
        if transport is None or not hasattr(transport, "async_query"):
            raise CollectorManagementUnsupportedError(
                "collector_local_management_not_supported"
            )
        if needs_write and not hasattr(transport, "async_write"):
            raise CollectorManagementUnsupportedError(
                "collector_local_management_not_supported"
            )
        return transport

    async def _query(self, transport: object, command: str) -> str:
        """Query one AT command and require the answer to be FOR that command.

        A foreign or empty ``response.command`` (with a response present) is a
        ``CollectorManagementCommandError`` -- an arbitrary ``response.value`` is
        never accepted as the answer to the requested query.
        """

        try:
            response = await transport.async_query(command)
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise _wrap_wire_call(exc) from exc
        response_command = str(getattr(response, "command", "") or "").strip().upper()
        if response_command != command:
            raise CollectorManagementCommandError(
                f"collector_at_wrong_response_command:expected={command}:got={response_command}"
            )
        return str(getattr(response, "value", "") or "").strip()

    async def _send_write(self, transport: object, command: str, value: str) -> str:
        """Send one AT write; require the echoed command; return the response value.

        A foreign echoed command means the collector answered a DIFFERENT command
        -> ``CollectorManagementConfirmationError``. Transport/parser failures are
        wrapped typed (delivery -> transport, malformed -> command).
        """

        try:
            response = await transport.async_write(command, value)
        except CollectorManagementError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise _wrap_wire_call(exc) from exc
        if str(getattr(response, "command", "") or "").strip().upper() != command:
            raise CollectorManagementConfirmationError(
                f"collector_at_unconfirmed:command={command}"
            )
        return str(getattr(response, "value", "") or "").strip()

    async def _apply(self, transport: object) -> str:
        """Send INTPARA 29,1 and require a confirmed ``W000`` status.

        Status semantics (project-wide, see smartess_ble.extract_at_status_code):
        ``W000`` is success; any other ``Wxxx`` is a rejected command; an empty
        status is unconfirmed.
        """

        status = await self._send_write(transport, _AT_APPLY_COMMAND, _AT_APPLY_VALUE)
        if not status:
            raise CollectorManagementConfirmationError(
                f"collector_at_apply_unconfirmed:command={_AT_APPLY_COMMAND}"
            )
        if status != _AT_SUCCESS_STATUS:
            raise CollectorManagementCommandError(
                f"collector_at_apply_rejected:command={_AT_APPLY_COMMAND}:status={status}"
            )
        return status

    async def async_read_endpoint_state(self) -> CollectorEndpointState:
        transport = self._resolve_transport()
        current = await self._query(transport, _AT_ENDPOINT_COMMAND)
        # The AT wire has no confirmed reboot-required read.
        return CollectorEndpointState(
            current_endpoint=current, reboot_required="", adapter_id=self.adapter_id
        )

    async def async_write_endpoint(
        self, endpoint: str, *, apply_changes: bool = True
    ) -> CollectorEndpointWriteResult:
        transport = self._resolve_transport(needs_write=True)
        previous = await self._query(transport, _AT_ENDPOINT_COMMAND)
        # CLDSRVHOST1 write ack: a W000 status OR a documented exact endpoint echo.
        # A Wxxx != W000 is NOT an ack (readback must then confirm).
        write_value = await self._send_write(transport, _AT_ENDPOINT_COMMAND, endpoint)
        write_ack = write_value == _AT_SUCCESS_STATUS or _endpoints_match(write_value, endpoint)
        readback = await self._query(transport, _AT_ENDPOINT_COMMAND)
        confirmation_source = _confirm_at_write(endpoint, readback, write_ack=write_ack)

        warnings: tuple[str, ...] = ()
        extra: dict[str, object] = {}
        apply_performed = False
        if apply_changes:
            # An unconfirmed apply raises (never swallowed into a warning).
            extra["at_apply_response"] = await self._apply(transport)
            apply_performed = True
            warnings = (self._APPLY_WARNING,)

        return CollectorEndpointWriteResult(
            previous_endpoint=previous,
            requested_endpoint=endpoint,
            readback_endpoint=readback,
            write_performed=True,
            write_confirmed=True,
            confirmation_source=confirmation_source,
            apply_requested=apply_changes,
            apply_performed=apply_performed,
            reboot_or_apply_required="",
            adapter_id=self.adapter_id,
            warnings=warnings,
            extra=extra,
        )

    async def _system_action(self, action: str) -> CollectorSystemActionResult:
        transport = self._resolve_transport(needs_write=True)
        current = await self._query(transport, _AT_ENDPOINT_COMMAND)
        await self._apply(transport)
        return CollectorSystemActionResult(
            action=action,
            current_endpoint=current,
            reboot_required_before="",
            performed=True,
            adapter_id=self.adapter_id,
            warnings=(self._ACTION_WARNING,),
        )

    async def async_apply_changes(self) -> CollectorSystemActionResult:
        return await self._system_action("apply")

    async def async_reboot(self) -> CollectorSystemActionResult:
        raise CollectorManagementUnsupportedError(
            "collector_reboot_unsupported_on_at_wire"
        )

    async def async_query_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        del parameters
        raise CollectorManagementUnsupportedError(
            "collector_parameter_query_unsupported_on_at_wire"
        )

    async def async_set_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        del ssid, password, ssid_parameter, password_parameter
        raise CollectorManagementUnsupportedError(
            "collector_wifi_write_unsupported_on_at_wire"
        )

    async def async_set_uart_baudrate(self, baudrate: str) -> str:
        del baudrate
        raise CollectorManagementUnsupportedError(
            "collector_uart_write_unsupported_on_at_wire"
        )


# ---------------------------------------------------------------------------
# Unavailable implementation (conflict / unknown / nothing negotiated)
# ---------------------------------------------------------------------------


class UnavailableCollectorManagementAdapter(CollectorManagementAdapter):
    """Fail-closed adapter: no capability, every operation raises typed."""

    adapter_id = ADAPTER_NONE

    @property
    def capabilities(self) -> CollectorManagementCapabilities:
        return CollectorManagementCapabilities()

    def _fail(self) -> None:
        raise CollectorManagementUnsupportedError(
            "collector_local_management_not_supported"
        )

    async def async_read_endpoint_state(self) -> CollectorEndpointState:
        self._fail()

    async def async_write_endpoint(
        self, endpoint: str, *, apply_changes: bool = True
    ) -> CollectorEndpointWriteResult:
        self._fail()

    async def async_apply_changes(self) -> CollectorSystemActionResult:
        self._fail()

    async def async_reboot(self) -> CollectorSystemActionResult:
        self._fail()

    async def async_query_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        del parameters
        self._fail()

    async def async_set_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        del ssid, password, ssid_parameter, password_parameter
        self._fail()

    async def async_set_uart_baudrate(self, baudrate: str) -> str:
        del baudrate
        self._fail()


# ---------------------------------------------------------------------------
# The single selection switch
# ---------------------------------------------------------------------------


def select_collector_management_adapter(
    adapter_id: str,
    *,
    framed_transport_provider: Callable[[], object | None],
    at_transport_provider: Callable[[], object | None],
) -> CollectorManagementAdapter:
    """Return the management adapter for a NEGOTIATED management-adapter id.

    ``adapter_id`` must come from the live trusted ``SessionHandle`` (or the
    ``ConfirmedWireBinding`` during handover); a conflict/unknown resolves to
    ``ADAPTER_NONE`` upstream and yields the fail-closed unavailable adapter.
    Nothing here branches on collector kind, hostname, cloud family, peer IP, or
    driver key -- only the negotiated adapter id.
    """

    if adapter_id == ADAPTER_COLLECTOR_FRAMED_COMMANDS:
        return FramedCollectorManagementAdapter(framed_transport_provider)
    if adapter_id == ADAPTER_COLLECTOR_AT_COMMANDS:
        return AtTextCollectorManagementAdapter(at_transport_provider)
    return UnavailableCollectorManagementAdapter()
