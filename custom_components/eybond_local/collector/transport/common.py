"""Shared collector transport primitives for single- and multi-collector listeners."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable, Protocol

from ..at import CollectorAtResponse, build_at_write, is_at_query_echo, parse_at_response
from ..cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_endpoint,
)
from ...collector_identity import (
    identity_source_is_strong,
    prefer_identity_source,
    reconcile_pn,
    validated_collector_pn,
)
from ...link_models import EybondLinkRoute, LinkRoute, RawSerialLinkRoute
from ...link_transport import PayloadLinkTransport
from ...models import CollectorInfo
from ..profile import apply_collector_profile
from ..protocol import (
    EybondHeader,
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    FC_SET_COLLECTOR,
    FC_SET_DEVICE_REG,
    FC_TRIGGER_QUERY_HISTORY,
    FC_TRIGGER_QUERY_REAL_TIME,
    HEADER_SIZE,
    TIDCounter,
    decode_header,
    parse_heartbeat_pn,
)
from .binary_framing import (
    BinaryFramingError,
    MAX_EYBOND_PAYLOAD_SIZE,
    RUNTIME_EYBOND_FCODES,
    runtime_eybond_header_error,
)

logger = logging.getLogger(__name__)

class CollectorListenerBindError(RuntimeError):
    """Raised when the shared collector listener cannot bind its socket."""

    def __init__(self, host: str, port: int, error: OSError) -> None:
        self.host = str(host)
        self.port = int(port)
        self.error = error
        self.errno = getattr(error, "errno", None)
        super().__init__(
            f"collector_listener_bind_failed:{self.host}:{self.port}:{error}"
        )


# Strong references to session/sniff tasks: asyncio keeps only weak ones, so
# an unreferenced task can be garbage-collected mid-flight, and a crash in an
# unobserved task surfaces only as a contextless "exception was never
# retrieved" at GC time.
_BACKGROUND_TASKS: set["asyncio.Task[Any]"] = set()


def _reap_tracked_task(task: "asyncio.Task[Any]") -> None:
    _BACKGROUND_TASKS.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "Collector background task %s crashed: %s",
            task.get_name(),
            exc,
            exc_info=exc,
        )


def _spawn_tracked_task(coro: Any, *, name: str) -> "asyncio.Task[Any]":
    task = asyncio.create_task(coro, name=name)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_reap_tracked_task)
    return task


# Bounds every writer teardown: wait_closed() on a peer that vanished with
# unflushed data (collector rebooting mid-frame) otherwise blocks until the
# OS-level TCP timeout — minutes, observed hanging Home Assistant shutdown.
_WRITER_CLOSE_TIMEOUT = 5.0


async def _cancel_and_join_task(task: "asyncio.Task[Any]") -> None:
    """Cancel a session task and wait for it, re-cancelling until it dies.

    A single cancel() is not enough: on Python < 3.12 ``asyncio.wait_for``
    swallows a cancellation that races its inner future completing
    (gh-86296), so a task cancelled mid-write keeps running and a bare
    ``await task`` then blocks for as long as the task stays alive — the
    heartbeat loop, for one, never exits on a healthy socket.
    """

    attempts = 0
    while not task.done():
        task.cancel()
        await asyncio.wait({task}, timeout=0.25)
        attempts += 1
        if attempts >= 20 and not task.done():
            # A task that survives 20 cancellations is swallowing
            # CancelledError; waiting longer would recreate the very hang
            # this helper exists to prevent.
            logger.error(
                "Session task %s ignored %d cancellations; abandoning join",
                task.get_name(),
                attempts,
            )
            return
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


async def _close_writer_bounded(writer: Any) -> None:
    """Close a stream writer without inheriting a dead peer's TCP timeout."""

    try:
        writer.close()
    except Exception:
        return
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=_WRITER_CLOSE_TIMEOUT)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


async def _finish_cleanup_on_cancel(awaitable: Awaitable[Any]) -> Any:
    """Finish critical cleanup even if the caller is already being cancelled."""

    future = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        try:
            await future
        except Exception:
            raise
        raise


def _looks_like_at_traffic(chunk: bytes) -> bool:
    return chunk.lstrip().startswith(b"AT+")


def _looks_like_eybond_frame_start(chunk: bytes) -> bool:
    """Return whether initial bytes plausibly start an EyeBond framed packet."""

    if len(chunk) < HEADER_SIZE:
        return False
    try:
        header = decode_header(chunk)
    except Exception:
        return False
    return _runtime_eybond_header_error(header) == ""


def _classify_initial_protocol_shape(chunk: bytes) -> str:
    """Classify initial callback bytes without using owner/collector metadata."""

    if _looks_like_at_traffic(chunk):
        return "at_text"
    if 0 < len(chunk) < HEADER_SIZE:
        # TCP can deliver a partial EyeBond header. A short non-AT prefix is not
        # enough evidence for raw passthrough and must remain unknown until more
        # bytes arrive or the route explicitly proves otherwise.
        return "unknown"
    if _looks_like_eybond_frame_start(chunk):
        return "eybond_framed"
    if chunk:
        return "raw_tcp"
    return "unknown"


def _looks_like_uart_passthrough_value(value: str) -> bool:
    normalized = str(value or "").strip()
    if not normalized.isascii():
        return False
    parts = [part.strip() for part in normalized.split(",")]
    if len(parts) != 4:
        return False
    baud, data_bits, stop_bits, parity = parts
    if not baud.isdigit() or not data_bits.isdigit() or not stop_bits.isdigit():
        return False
    if parity.upper() not in {"NONE", "N", "ODD", "EVEN", "O", "E"}:
        return False
    return True


def _looks_like_plain_raw_response_start(chunk: bytes) -> bool:
    if not chunk:
        return False
    first = chunk[:1]
    if first in {b"(", b"^"}:
        return True
    value = first[0]
    return 0x20 <= value <= 0x7E


def _short_ascii(value: bytes, *, limit: int = 160) -> str:
    text = "".join(chr(byte) if 0x20 <= byte <= 0x7E else "." for byte in value[:limit])
    if len(value) > limit:
        text += "..."
    return text


_AT_TEXT_MIXED_FRAME_READ_TIMEOUT = 0.05
_AT_TEXT_RESPONSE_IDLE_TIMEOUT = 0.2
_AT_QUERY_STATUS_COMPLETION_TIMEOUT = 2.0
_AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN = MAX_EYBOND_PAYLOAD_SIZE
_AT_TEXT_MIXED_FRAME_FCODES = RUNTIME_EYBOND_FCODES
_AT_LINE_MAX_LEN = 512


def _runtime_eybond_header_error(header: EybondHeader) -> str:
    """Return a typed reason when a runtime EyeBond header is implausible.

    ``decode_header`` deliberately stays a mechanical wire decoder because the
    transparent cloud tooling understands more function codes than the local
    runtime.  The runtime reader, protocol sniffing, and the mixed AT reader do
    need one shared fail-closed definition, however: accepting an arbitrary
    eight-byte sequence as a header can turn one raw RTU response into a large
    pseudo-frame and consume every valid frame that follows it.
    """

    return runtime_eybond_header_error(header)


def _mask_identity_token(value: str) -> str:
    token = str(value or "").strip()
    if len(token) <= 6:
        return "*" * len(token)
    return f"{token[:3]}{'*' * max(len(token) - 6, 3)}{token[-3:]}"


def _seed_connection_collector_pn(connection: object, collector_pn: str) -> None:
    """Seed a PN-owned connection's visible CollectorInfo before first heartbeat.

    PN ownership is already decided by the caller/listener route. This only keeps
    facade state honest during the short window between claiming the socket and
    the read-loop parsing the collector's own identity frame.
    """

    normalized = str(collector_pn or "").strip()
    if not normalized:
        return
    collector = getattr(connection, "_collector", None)
    if collector is None:
        return
    current = str(getattr(collector, "collector_pn", "") or "").strip()
    seeded = reconcile_pn(current, normalized)
    if not seeded:
        return
    collector.collector_pn = seeded
    collector.collector_pn_prefix = seeded[:1]
    collector.collector_pn_digits = seeded[1:]


_AT_DTUPN_RE = re.compile(
    rb"AT\+DTUPN\s*[:=]\s*([A-Za-z0-9][A-Za-z0-9._-]{5,63})(?![A-Za-z0-9._-])"
)


def _parse_fc2_collector_pn(payload: bytes) -> str:
    if len(payload) < 2:
        return ""
    if payload[1] != 2:
        return ""
    try:
        candidate = payload[2:].rstrip(b"\x00").decode("ascii")
    except UnicodeDecodeError:
        return ""
    return validated_collector_pn(candidate)


def _framed_pn_at_offset(payload: bytes, offset: int) -> tuple[str, str]:
    """Return a heartbeat or FC2 PN whose header starts at offset."""

    window = payload[offset:]
    if len(window) < HEADER_SIZE:
        return "", ""
    try:
        header = decode_header(window[:HEADER_SIZE])
    except Exception:
        return "", ""
    if offset and _runtime_eybond_header_error(header):
        return "", ""
    payload_len = max(header.payload_len, 0)
    # A short frame is not a PN: cutting bytes off an FC2 PN can still leave a
    # valid-looking shorter PN. The caller keeps the bytes for the next read.
    if len(window) < HEADER_SIZE + payload_len:
        return "", ""
    frame_payload = window[HEADER_SIZE : HEADER_SIZE + payload_len]
    if header.fcode == FC_HEARTBEAT:
        collector_pn = parse_heartbeat_pn(frame_payload)
        return (collector_pn, "framed_heartbeat") if collector_pn else ("", "")
    if header.fcode == FC_QUERY_COLLECTOR:
        collector_pn = _parse_fc2_collector_pn(frame_payload)
        return (collector_pn, "fc2_parameter_2") if collector_pn else ("", "")
    return "", ""


def _leading_at_lines_end(payload: bytes) -> int | None:
    """Return the offset just past the complete AT lines that start payload.

    A line is AT+ and an uppercase command letter at the current offset, then
    only printable ASCII or CR, ended by a newline within _AT_LINE_MAX_LEN
    bytes. Anything else ends the walk, so newlines inside a later frame or
    arbitrary text are never boundaries. None means the walk stopped on an AT
    line that is not complete yet.
    """

    offset = 0
    while payload.startswith(b"AT+", offset):
        if len(payload) <= offset + 3:
            return None
        if not 0x41 <= payload[offset + 3] <= 0x5A:
            break
        limit = min(len(payload), offset + _AT_LINE_MAX_LEN)
        newline = -1
        for index in range(offset + 4, limit):
            byte = payload[index]
            if byte == 0x0A:
                newline = index
                break
            if byte != 0x0D and not 0x20 <= byte <= 0x7E:
                return offset
        if newline < 0:
            return None if limit < offset + _AT_LINE_MAX_LEN else offset
        offset = newline + 1
    return offset


def _collector_pn_from_initial_chunk(chunk: bytes) -> tuple[str, str]:
    payload = bytes(chunk or b"")
    if not payload:
        return "", ""

    match = _AT_DTUPN_RE.search(payload)
    if match:
        collector_pn = validated_collector_pn(match.group(1).decode("ascii"))
        if collector_pn:
            return collector_pn, "at_dtupn"

    pn, source = _framed_pn_at_offset(payload, 0)
    if pn:
        return pn, source
    # A non-DTUPN AT line can share the first read with the heartbeat or FC2
    # that follows it. Only the frame directly after those lines can carry it.
    frame_start = _leading_at_lines_end(payload)
    if not frame_start:
        return "", ""
    return _framed_pn_at_offset(payload, frame_start)


def _bounded_write_timeout(request_timeout: float) -> float:
    return max(0.5, min(float(request_timeout), 1.5))


def _parse_ip_address(value: str) -> ipaddress._BaseAddress | None:
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def _is_hairpin_alias_candidate(expected_ip: str, remote_ip: str) -> bool:
    expected = _parse_ip_address(expected_ip)
    remote = _parse_ip_address(remote_ip)
    if expected is None or remote is None:
        return False
    return bool(expected.is_global and not remote.is_global)


def _is_default_broadcast_alias_candidate(expected_ip: str, remote_ip: str) -> bool:
    expected = _parse_ip_address(expected_ip)
    remote = _parse_ip_address(remote_ip)
    if not isinstance(expected, ipaddress.IPv4Address) or not isinstance(remote, ipaddress.IPv4Address):
        return False
    if expected == ipaddress.IPv4Address("255.255.255.255"):
        return True
    return expected == ipaddress.IPv4Address(int(remote) | 0xFF)


def _is_ipv4_broadcast_placeholder(value: str) -> bool:
    parsed = _parse_ip_address(value)
    return isinstance(parsed, ipaddress.IPv4Address) and (
        parsed == ipaddress.IPv4Address("255.255.255.255")
        or int(parsed) & 0xFF == 0xFF
    )


def _disconnect_reason_from_exception(exc: BaseException) -> str:
    if isinstance(exc, ConnectionResetError):
        return "collector_connection_reset"
    if isinstance(exc, BrokenPipeError):
        return "collector_broken_pipe"
    if isinstance(exc, OSError):
        return "collector_os_error"
    return "collector_disconnected"


def _copy_collector_info(collector: CollectorInfo) -> CollectorInfo:
    return apply_collector_profile(
        CollectorInfo(
            remote_ip=collector.remote_ip,
            remote_port=collector.remote_port,
            connection_count=collector.connection_count,
            connection_replace_count=collector.connection_replace_count,
            disconnect_count=collector.disconnect_count,
            pending_request_drop_count=collector.pending_request_drop_count,
            last_disconnect_reason=collector.last_disconnect_reason,
            discovery_restart_count=collector.discovery_restart_count,
            last_discovery_reason=collector.last_discovery_reason,
            collector_pn=collector.collector_pn,
            last_devcode=collector.last_devcode,
            heartbeat_devcode=collector.heartbeat_devcode,
            heartbeat_payload_hex=collector.heartbeat_payload_hex,
            last_udp_reply=collector.last_udp_reply,
            last_udp_reply_from=collector.last_udp_reply_from,
            profile_key=collector.profile_key,
            profile_name=collector.profile_name,
            heartbeat_ascii=collector.heartbeat_ascii,
            heartbeat_payload_len=collector.heartbeat_payload_len,
            heartbeat_format_key=collector.heartbeat_format_key,
            heartbeat_suffix_ascii=collector.heartbeat_suffix_ascii,
            heartbeat_suffix_kind=collector.heartbeat_suffix_kind,
            heartbeat_suffix_uint=collector.heartbeat_suffix_uint,
            devcode_major=collector.devcode_major,
            devcode_minor=collector.devcode_minor,
            collector_pn_prefix=collector.collector_pn_prefix,
            collector_pn_digits=collector.collector_pn_digits,
            heartbeat_age_seconds=collector.heartbeat_age_seconds,
            heartbeat_fresh=collector.heartbeat_fresh,
            raw_request_count=collector.raw_request_count,
            raw_response_count=collector.raw_response_count,
            raw_timeout_count=collector.raw_timeout_count,
            raw_unhandled_line_count=collector.raw_unhandled_line_count,
            raw_last_request_ascii=collector.raw_last_request_ascii,
            raw_last_request_hex=collector.raw_last_request_hex,
            raw_last_response_ascii=collector.raw_last_response_ascii,
            raw_last_response_hex=collector.raw_last_response_hex,
            raw_last_timeout_request_ascii=collector.raw_last_timeout_request_ascii,
            raw_last_parser=collector.raw_last_parser,
            raw_last_frame_format=collector.raw_last_frame_format,
            raw_last_spacing_wait_ms=collector.raw_last_spacing_wait_ms,
            raw_last_response_duration_ms=collector.raw_last_response_duration_ms,
            raw_last_total_duration_ms=collector.raw_last_total_duration_ms,
            inverter_forward_mode=collector.inverter_forward_mode,
            collector_cloud_family=collector.collector_cloud_family,
            collector_cloud_family_source=collector.collector_cloud_family_source,
            collector_cloud_family_confidence=collector.collector_cloud_family_confidence,
            collector_server_endpoint=collector.collector_server_endpoint,
            collector_cloud_profile_key=collector.collector_cloud_profile_key,
            collector_cloud_profile_label=collector.collector_cloud_profile_label,
            collector_cloud_profile_source=collector.collector_cloud_profile_source,
            collector_cloud_profile_confidence=collector.collector_cloud_profile_confidence,
            smartess_collector_version=collector.smartess_collector_version,
            smartess_protocol_raw_id=collector.smartess_protocol_raw_id,
            smartess_protocol_asset_id=collector.smartess_protocol_asset_id,
            smartess_protocol_asset_name=collector.smartess_protocol_asset_name,
            smartess_protocol_suffix=collector.smartess_protocol_suffix,
            smartess_protocol_profile_key=collector.smartess_protocol_profile_key,
            smartess_protocol_name=collector.smartess_protocol_name,
            smartess_device_address=collector.smartess_device_address,
            collector_virtual_bridge=collector.collector_virtual_bridge,
            collector_bridge_kind=collector.collector_bridge_kind,
            collector_bridge_version=collector.collector_bridge_version,
        )
    )


class _PrefixedAsyncReader:
    def __init__(self, reader: asyncio.StreamReader, initial_bytes: bytes = b"") -> None:
        self._reader = reader
        self._buffer = bytearray(initial_bytes)
        # Parallel to ``_buffer``: 1 if that byte is still from ``initial_bytes``.
        self._from_prefix = bytearray(b"\x01" * len(initial_bytes))
        self._recent_from_prefix = bytearray()
        self._line_live = 0
        self._at_query_echo = b""

    def begin_line(self) -> None:
        """Start a response whose prefix-vs-socket origin is still unknown."""
        self._line_live = 0

    def line_from_prefix_only(self) -> bool:
        """True when this line has not consumed a byte from the live socket.

        Lookahead pushed back with ``unread`` does not count. An accept-time
        AT identity line therefore stays prefix-only, while a later reply does
        not.
        """

        return self._line_live == 0

    def unread(self, data: bytes) -> None:
        """Return bounded lookahead to the SAME reader at a protocol boundary."""
        count = len(data)
        if count <= 0:
            return
        if count <= len(self._recent_from_prefix):
            origins = self._recent_from_prefix[-count:]
            del self._recent_from_prefix[-count:]
        else:
            origins = b"\x00" * count
        self._line_live -= origins.count(0)
        if self._line_live < 0:
            self._line_live = 0
        self._buffer[:0] = data
        self._from_prefix[:0] = origins

    def _remember(self, origins: bytes) -> None:
        if not origins:
            return
        self._line_live += origins.count(0)
        self._recent_from_prefix += origins
        if len(self._recent_from_prefix) > 8192:
            del self._recent_from_prefix[:-4096]

    def _take(self, size: int) -> bytes:
        data = bytes(self._buffer[:size])
        origins = bytes(self._from_prefix[:size])
        del self._buffer[:size]
        del self._from_prefix[:size]
        self._remember(origins)
        return data

    async def readexactly(self, size: int) -> bytes:
        if size <= 0:
            return b""
        if len(self._buffer) >= size:
            return self._take(size)

        data = self._take(len(self._buffer))
        if len(data) == size:
            return data
        rest = await self._reader.readexactly(size - len(data))
        self._remember(b"\x00" * len(rest))
        return data + rest

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        index = self._buffer.find(separator)
        if index >= 0:
            return self._take(index + len(separator))

        data = self._take(len(self._buffer))
        rest = await self._reader.readuntil(separator)
        self._remember(b"\x00" * len(rest))
        return data + rest

    async def read_framed_prefix(self, first: bytes) -> bytes:
        """Read a bounded EyeBond/AT prefix at an established frame boundary.

        A leading CR/LF is a separator ONLY before AT+ and an uppercase command
        letter. That letter occupies the binary function-code position, where
        no supported EyeBond function is an ASCII letter. Otherwise preserve
        every byte, including valid binary TID 0x0D0A and devcode 0x4154.
        Never scan ahead or skip arbitrary whitespace. The caller owns the
        first-byte header deadline and cancellation for the whole lookahead.
        """
        prefix = first + await self.readexactly(2)
        if prefix == b"\r\nA":
            prefix += await self.readexactly(3)
            if prefix[:5] == b"\r\nAT+" and 0x41 <= prefix[5] <= 0x5A:
                return prefix[2:]
        return prefix

    async def read_at_query_status(self, first: bytes) -> bytes | None:
        """Read only a status immediately after a complete query echo (#70).

        Keep the optional continuation on THIS reader across an idle gap, not
        in a collector/IP cache. Any intervening frame clears it. A mismatch
        restores every lookahead byte so even a binary TID ':R' is untouched.
        Seven bytes is the entire grammar: :Rddd CRLF. No resynchronization,
        arbitrary line skip or speculative metadata value is allowed.
        """
        echo, self._at_query_echo = self._at_query_echo, b""
        if not echo or first != b":":
            return None
        tail = bytearray()
        try:
            async with asyncio.timeout(_AT_QUERY_STATUS_COMPLETION_TIMEOUT):
                for allowed in (b"R", b"0123456789", b"0123456789", b"0123456789", b"\r", b"\n"):
                    byte = await self.readexactly(1)
                    tail.extend(byte)
                    if byte[0] not in allowed:
                        self.unread(bytes(tail))
                        return None
        except asyncio.TimeoutError as exc:
            self.unread(bytes(tail))
            raise BinaryFramingError("collector_at_status_timeout") from exc
        except (asyncio.CancelledError, asyncio.IncompleteReadError):
            self.unread(bytes(tail))
            raise
        return echo + first + bytes(tail)

    async def read_at_response(self, *, prefix: bytes = b"AT+") -> bytes:
        """Read one AT response terminated by newline or a bounded idle gap.

        Some collector firmware omits CR/LF for otherwise valid AT responses
        (observed for ``AT+INTPARA:41,...``). Waiting unconditionally for a
        newline consumes every later framed response into the AT record and
        permanently desynchronizes the shared stream. A complete plausible
        EyeBond header is kept for the next parser iteration; otherwise a short
        inter-byte idle gap is the only framing information such firmware
        provides.
        """

        data = bytearray()
        self._at_query_echo = b""
        binary_suspected = False
        while len(data) < _AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN:
            try:
                byte = await asyncio.wait_for(
                    self.readexactly(1),
                    timeout=_AT_TEXT_RESPONSE_IDLE_TIMEOUT,
                )
            except asyncio.TimeoutError:
                return bytes(data)

            data.extend(byte)

            # Do not assume that a framed packet starts with NUL: after enough
            # requests its two-byte transaction id can be entirely printable.
            # Wait for a complete, plausible EyeBond header and push the whole
            # header back atomically so no binary prefix is lost.
            if len(data) >= HEADER_SIZE:
                frame_start = len(data) - HEADER_SIZE
                candidate = bytes(data[frame_start:])
                if _looks_like_eybond_frame_start(candidate):
                    self.unread(candidate)
                    return bytes(data[:frame_start])

            if byte == b"\n":
                if not binary_suspected and data.endswith(b"\r\n"):
                    line = prefix + bytes(data)
                    if is_at_query_echo(line):
                        self._at_query_echo = line
                    return bytes(data)
                # LF-only firmware remains supported through the idle-gap
                # boundary. A lone LF may also be the high TID byte of the
                # following binary frame and therefore is not consumed early.
                binary_suspected = True
                continue
            if byte and byte[0] < 0x20 and byte not in (b"\r", b"\t"):
                binary_suspected = True

        raise asyncio.LimitOverrunError(
            "collector_at_response_too_large",
            len(data),
        )


class CollectorTransport(PayloadLinkTransport, Protocol):
    @property
    def connected(self) -> bool:
        ...

    @property
    def collector_info(self) -> CollectorInfo:
        ...

    async def wait_until_connected(self, timeout: float) -> bool:
        ...

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        ...

    async def wait_until_liveness(self, timeout: float) -> bool:
        ...

    async def disconnect(self) -> None:
        ...

    async def async_disconnect_owned_session(
        self, *, session_id: str, listener_port: int, owns_session: Callable[[], bool],
    ) -> None:
        ...

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        ...

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
    ) -> bytes:
        ...

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
        ...


class CollectorAtTransport(Protocol):
    @property
    def connected(self) -> bool:
        ...

    @property
    def collector_info(self) -> CollectorInfo:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    async def wait_until_connected(self, timeout: float) -> bool:
        ...

    async def async_query(self, command: str) -> CollectorAtResponse:
        ...

    async def async_write(self, command: str, value: str) -> CollectorAtResponse:
        ...
