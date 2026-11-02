"""Plain AT command helpers for collector cloud/bootstrap sessions."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import re


class CollectorAtError(Exception):
    """Raised when a collector AT command or response is invalid."""


class CollectorAtQueryStatus(CollectorAtError):
    """A query echo with a read-status code, not a returned setting value.

    Older firmware's unknown-command handler formats ``%s:%c001``, where
    ``%s`` can retain the query's CR/LF. Do not mistake that status for data
    (or a write acknowledgement). Named replies like AT+ATVER:R001 remain
    ordinary values; only the explicit query-echo envelope qualifies.
    """

    def __init__(self, command: str, status: str) -> None:
        self.command = command
        self.status = status
        super().__init__(f"collector_at_query_status_{status}")


def is_at_query_echo(payload: bytes) -> bool:
    """Recognize one bounded, complete query echo at a text boundary."""
    return re.fullmatch(rb"AT\+[A-Z][A-Z0-9]{0,31}\?\r\n", payload) is not None


@dataclass(frozen=True, slots=True)
class CollectorAtCommand:
    """One decoded AT command line."""

    command: str
    operation: str
    value: str
    raw: str


@dataclass(frozen=True, slots=True)
class CollectorAtResponse:
    """One decoded AT response line."""

    command: str
    value: str
    raw: str


def normalize_at_command(command: str) -> str:
    """Return one normalized AT command name without the prefix or suffix."""

    normalized = str(command or "").strip().upper()
    if normalized.startswith("AT+"):
        normalized = normalized[3:]
    normalized = normalized.rstrip("?=")
    if not normalized or not normalized.isascii():
        raise CollectorAtError("at_command_invalid")
    return normalized


def build_at_query(command: str) -> bytes:
    """Build one read-only collector AT query line."""

    normalized = normalize_at_command(command)
    return f"AT+{normalized}?\r\n".encode("ascii")


def build_at_write(command: str, value: str) -> bytes:
    """Build one collector AT write line."""

    normalized = normalize_at_command(command)
    rendered_value = str(value or "")
    if not rendered_value.isascii():
        raise CollectorAtError("at_value_not_ascii")
    return f"AT+{normalized}={rendered_value}\r\n".encode("ascii")


def build_at_response(command: str, value: str) -> bytes:
    """Build one collector AT response line."""

    normalized = normalize_at_command(command)
    rendered_value = str(value or "")
    if not rendered_value.isascii():
        raise CollectorAtError("at_value_not_ascii")
    return f"AT+{normalized}:{rendered_value}\r\n".encode("ascii")


def parse_at_command(payload: bytes | str) -> CollectorAtCommand:
    """Parse one AT query or write line."""

    if isinstance(payload, bytes):
        raw = payload.decode("ascii", errors="ignore")
    else:
        raw = str(payload)

    normalized = raw.strip()
    prefix = "AT+"
    if not normalized.startswith(prefix):
        raise CollectorAtError("at_command_invalid")

    remainder = normalized[len(prefix) :]
    if remainder.endswith("?"):
        command = normalize_at_command(remainder[:-1])
        return CollectorAtCommand(command=command, operation="query", value="", raw=normalized)

    command_text, separator, value = remainder.partition("=")
    if not separator:
        raise CollectorAtError("at_command_invalid")

    command = normalize_at_command(command_text)
    return CollectorAtCommand(command=command, operation="write", value=value.strip(), raw=normalized)


def parse_at_response(
    payload: bytes | str,
    *,
    expected_command: str | None = None,
) -> CollectorAtResponse:
    """Parse one AT response line like ``AT+WFSS:-55``."""

    if isinstance(payload, bytes):
        raw = payload.decode("ascii", errors="ignore")
    else:
        raw = str(payload)

    normalized = raw.strip()
    prefix = "AT+"
    if not normalized.startswith(prefix):
        raise CollectorAtError("at_response_invalid")

    status = re.fullmatch(r"AT\+([A-Z][A-Z0-9]{0,31})\?(?:\r\n)?:(R[0-9]{3})", normalized)
    if status is not None:
        raise CollectorAtQueryStatus(status[1], status[2])
    command_with_value = normalized[len(prefix) :]
    command_text, separator, value = command_with_value.partition(":")
    if not separator:
        raise CollectorAtError("at_response_invalid")

    command = normalize_at_command(command_text)
    if expected_command is not None and command != normalize_at_command(expected_command):
        raise CollectorAtError("at_response_command_mismatch")

    return CollectorAtResponse(command=command, value=value.strip(), raw=normalized)


class CollectorAtStreamSession:
    """Read-only AT session over one plain text collector stream."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout: float = 3.0,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._timeout = float(timeout)

    async def query(self, command: str) -> CollectorAtResponse:
        """Send one AT query and parse its response line."""

        self._writer.write(build_at_query(command))
        await self._writer.drain()
        return await self.read_response(expected_command=command)

    async def read_response(self, *, expected_command: str | None = None) -> CollectorAtResponse:
        """Read and decode one AT response line from the stream."""

        try:
            payload = await asyncio.wait_for(self._reader.readuntil(b"\n"), timeout=self._timeout)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
            raise CollectorAtError("at_response_timeout") from exc
        return parse_at_response(payload, expected_command=expected_command)
