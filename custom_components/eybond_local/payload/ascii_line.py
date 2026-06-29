"""Generic ASCII line payload helpers.

This module is intentionally smaller than the PI30 helpers.  It models devices
that exchange plain ASCII commands terminated by ``\r`` and return plain ASCII
lines terminated by ``\r`` without PI30 CRC bytes.
"""

from __future__ import annotations

import asyncio

from ..link_models import EybondLinkRoute, LinkRoute
from ..link_transport import PayloadLinkTransport, async_send_payload, select_payload_route


class AsciiLineError(Exception):
    """Raised when an ASCII line frame cannot be encoded or decoded."""


def build_ascii_line_request(command: str) -> bytes:
    """Build one no-CRC ASCII command frame."""

    if not command or not command.isascii():
        raise AsciiLineError("invalid_command")
    return command.encode("ascii") + b"\r"


def parse_ascii_line_response(frame: bytes) -> str:
    """Decode one no-CRC ASCII response frame.

    The common inverter response shape is ``(...\r``.  Some commands answer with
    ``#...\r`` or bare tokens such as ``NAK\r``/``BL050\r``; those are kept as
    plain payload text.
    """

    if len(frame) < 2:
        raise AsciiLineError("response_too_short")
    if frame[-1] != 0x0D:
        raise AsciiLineError("missing_terminator")

    body = frame[:-1]
    if body.startswith(b"("):
        body = body[1:]

    try:
        return body.decode("ascii")
    except UnicodeDecodeError as exc:
        raise AsciiLineError("invalid_ascii") from exc


def parse_space_fields(payload: str) -> list[str]:
    """Split one ASCII payload into space-delimited fields."""

    text = payload.strip()
    if not text:
        return []
    return [field for field in text.split(" ") if field != ""]


class AsciiLineSession:
    """No-CRC ASCII command session routed through one generic payload link."""

    def __init__(
        self,
        transport: PayloadLinkTransport,
        *,
        route: LinkRoute | None = None,
        devcode: int | None = None,
        collector_addr: int | None = None,
        payload_family: str = "ascii_line",
    ) -> None:
        self._transport = transport
        if route is None:
            if devcode is None or collector_addr is None:
                raise TypeError("ascii_line_route_required")
            route = EybondLinkRoute(
                devcode=devcode,
                collector_addr=collector_addr,
            )
        self._route: LinkRoute = route
        self._payload_family = str(payload_family or "").strip()

    async def request(self, command: str) -> str:
        """Send one ASCII command and return the decoded payload."""

        response = await self.request_raw(command)
        payload = parse_ascii_line_response(response)
        if payload in {"NAK", "NOA", "ERCRC"}:
            raise AsciiLineError(payload.lower())
        return payload

    async def request_raw(self, command: str) -> bytes:
        """Send one ASCII command and return the raw response frame."""

        try:
            route = select_payload_route(
                self._transport,
                self._route,
                payload_family=self._payload_family,
            )
            response = await async_send_payload(
                self._transport,
                build_ascii_line_request(command),
                route=route,
            )
        except asyncio.TimeoutError as exc:
            raise AsciiLineError("request_timeout") from exc

        return response

    def last_transport_timing(self) -> dict[str, int]:
        """Return timing metrics from transports that expose raw passthrough data."""

        collector = getattr(self._transport, "collector_info", None)
        if collector is None:
            return {}
        timing: dict[str, int] = {}
        for attr, key in (
            ("raw_last_spacing_wait_ms", "spacing_wait_ms"),
            ("raw_last_response_duration_ms", "response_duration_ms"),
            ("raw_last_total_duration_ms", "transport_total_duration_ms"),
        ):
            value = getattr(collector, attr, None)
            if isinstance(value, int):
                timing[key] = value
        return timing
