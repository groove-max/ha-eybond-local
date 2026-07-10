#!/usr/bin/env python3
"""Transparent TCP proxy for EyeBond collector cloud traffic with frame logging."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import socket
import sys
from typing import Awaitable, Callable, TextIO

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.protocol import (  # noqa: E402
    FC_SET_COLLECTOR,
    HEADER_SIZE,
    TIDCounter,
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.discovery import DiscoveryAnnouncer  # noqa: E402
from custom_components.eybond_local.collector.at import (  # noqa: E402
    build_at_response,
    build_at_write,
    parse_at_command,
    parse_at_response,
)
from custom_components.eybond_local.collector.smartess_local import (  # noqa: E402
    SET_REBOOT_OR_APPLY,
    SET_SERVER_ENDPOINT,
    build_set_collector_payload,
)
from custom_components.eybond_local.collector_endpoint import (  # noqa: E402
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint,
)

LOGGER = logging.getLogger(__name__)

FCODE_NAMES = {
    1: "FC_HEARTBEAT",
    2: "FC_QUERY_COLLECTOR",
    3: "FC_SET_COLLECTOR",
    4: "FC_FORWARD_TO_DEVICE",
    17: "FC_TRIGGER_QUERY_REAL_TIME",
    18: "FC_SET_DEVICE_REG",
    19: "FC_TRIGGER_QUERY_HISTORY",
    22: "FC_COLLECTOR_DATA_UPLOAD",
    23: "FC_DEVICE_DATA_UPLOAD",
    24: "FC_COLLECTOR_GIS_UPLOAD",
    31: "FC_PVCURVESCAN",
    32: "FC_STATE_GRID_FILE_UPLOAD",
    33: "FC_COLLECTOR_FIRMWARE_UPGRADE",
    34: "FC_DEVICE_FIRMWARE_UPGRADE",
    35: "FC_QUERY_DEVICE_FIRMWARE_UPGRADE_PROGRESS",
    36: "FC_CANCEL_DEVICE_FIRMWARE_UPGRADE",
    37: "FC_QUERY_FIRMWARE_INFO",
    38: "FC_SEND_FIRMWARE_BLOCK",
    39: "FC_QUERY_FIRMWARE_BLOCK_RECV_INFO",
    40: "FC_CHECK_FIRMWARE_WRITE_STATUS",
    41: "FC_EXIT_FILE_TRANSFER_STATUS",
    50: "FC_DEVICE_REALTIME_DATA_UPLOAD",
    0x91: "FC_DATALOGPN_IMPORT_PN",
    0x92: "FC_DATALOGTEST_AUTH",
    0x93: "FC_DATALOGTEST_PN_PRODUCE",
    0x94: "FC_DATALOGTEST_QUERY_ORDERNUM",
    0x95: "FC_DATALOGTEST_GET_PN",
    0x96: "FC_DATALOGTEST_QUERY_PN",
    0x97: "FC_DATALOGTEST_QUERY_ORDER",
    0x98: "FC_DATALOGTEST_DELIVERY_ORDER_PRODUCE",
    0x99: "FC_DATALOGTEST_LOGIN_AUTH",
}


@dataclass(frozen=True, slots=True)
class FrameRecord:
    timestamp: str
    direction: str
    remote: str
    chunk_len: int
    frame_len: int
    tid: int
    devcode: int
    devaddr: int
    fcode: int
    fcode_name: str
    payload_hex: str
    payload_ascii: str


@dataclass(frozen=True, slots=True)
class RestoreTarget:
    host: str
    port: int
    protocol: str
    raw_endpoint: str = field(default="", compare=False)

    @property
    def endpoint(self) -> str:
        return self.raw_endpoint or f"{self.host},{self.port},{self.protocol}"


@dataclass(slots=True)
class ObservedCollectorAddress:
    devcode: int | None = None
    collector_addr: int | None = None


@dataclass(slots=True)
class SessionObservation:
    collector: ObservedCollectorAddress
    saw_at_traffic: bool = False
    pending_masked_endpoint_response: bool = False


class AtChunkProxyFilter:
    def __init__(
        self,
        *,
        direction: str,
        remote: str,
        observed: SessionObservation,
        masked_endpoint: str = "",
    ) -> None:
        self._direction = direction
        self._remote = remote
        self._observed = observed
        self._masked_endpoint = str(masked_endpoint or "").strip()
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> tuple[bytes, list[dict[str, object]]]:
        if not chunk:
            return b"", []

        if not self._buffer and not _looks_like_at_traffic(chunk):
            return chunk, []

        self._buffer.extend(chunk)
        forwarded = bytearray()
        events: list[dict[str, object]] = []

        while True:
            newline = self._buffer.find(b"\n")
            if newline < 0:
                break
            line = bytes(self._buffer[: newline + 1])
            del self._buffer[: newline + 1]
            transformed, event = self._transform_line(line)
            forwarded.extend(transformed)
            if event is not None:
                events.append(event)

        if self._buffer and not _looks_like_at_traffic(bytes(self._buffer)):
            forwarded.extend(self._buffer)
            self._buffer.clear()

        return bytes(forwarded), events

    def flush(self) -> bytes:
        if not self._buffer:
            return b""
        remainder = bytes(self._buffer)
        self._buffer.clear()
        return remainder

    def _transform_line(self, line: bytes) -> tuple[bytes, dict[str, object] | None]:
        if self._direction == "collector_to_cloud":
            return self._handle_collector_line(line)
        if self._direction == "cloud_to_collector":
            return self._handle_cloud_line(line)
        return line, None

    def _handle_collector_line(self, line: bytes) -> tuple[bytes, dict[str, object] | None]:
        try:
            response = parse_at_response(line)
        except Exception:
            return line, None

        self._observed.saw_at_traffic = True
        if not self._observed.pending_masked_endpoint_response:
            return line, None

        self._observed.pending_masked_endpoint_response = False
        if response.command != "CLDSRVHOST1" or not self._masked_endpoint:
            return line, None

        masked = build_at_response("CLDSRVHOST1", self._masked_endpoint)
        if masked == line:
            return line, None
        return (
            masked,
            {
                "kind": "masked_endpoint_response",
                "timestamp": _utc_timestamp(),
                "direction": self._direction,
                "remote": self._remote,
                "command": response.command,
                "forwarded_ascii": _safe_ascii(masked),
                "original_ascii": _safe_ascii(line),
            },
        )

    def _handle_cloud_line(self, line: bytes) -> tuple[bytes, dict[str, object] | None]:
        try:
            command = parse_at_command(line)
        except Exception:
            return line, None

        self._observed.saw_at_traffic = True
        if command.command == "CLDSRVHOST1" and command.operation == "query" and self._masked_endpoint:
            self._observed.pending_masked_endpoint_response = True
        return line, None


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_ascii(data: bytes) -> str:
    return data.decode("ascii", errors="replace")


def _looks_like_at_traffic(chunk: bytes) -> bool:
    return chunk.lstrip().startswith(b"AT+")


def build_restore_at_commands(
    *,
    target: RestoreTarget,
    followup_command: str = "",
) -> list[tuple[str, bytes]]:
    commands = [("restore_endpoint_at", build_at_write("CLDSRVHOST1", target.endpoint))]
    normalized_followup = followup_command.strip()
    if normalized_followup:
        if not normalized_followup.isascii():
            raise ValueError("restore_at_followup_not_ascii")
        if not normalized_followup.startswith("AT+"):
            raise ValueError("restore_at_followup_invalid")
        commands.append(("restore_followup_at", (normalized_followup + "\r\n").encode("ascii")))
    return commands


class JsonLineWriter:
    def __init__(self, output: TextIO) -> None:
        self._output = output
        self._lock = asyncio.Lock()

    async def write(self, payload: dict[str, object]) -> None:
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        async with self._lock:
            await asyncio.to_thread(self._write_line, line)

    def _write_line(self, line: str) -> None:
        self._output.write(line + "\n")
        self._output.flush()


def parse_restore_target(value: str) -> RestoreTarget:
    try:
        parsed = inspect_collector_server_endpoint(
            value,
            require_explicit_port=False,
            require_explicit_protocol=False,
            require_tcp=True,
        )
        raw_endpoint = normalize_collector_server_endpoint(
            value,
            require_explicit_port=False,
            require_explicit_protocol=False,
            require_tcp=True,
            preserve_shape=True,
        )
    except ValueError as exc:
        raise ValueError("restore_endpoint_invalid") from exc

    return RestoreTarget(
        host=parsed.host,
        port=parsed.port,
        protocol=parsed.protocol,
        raw_endpoint=raw_endpoint,
    )


def build_restore_frames(
    *,
    target: RestoreTarget,
    devcode: int,
    collector_addr: int,
    tid_counter: TIDCounter | None = None,
) -> list[tuple[str, bytes]]:
    counter = tid_counter or TIDCounter()
    return [
        (
            "restore_endpoint",
            build_collector_request(
                counter.next(),
                build_set_collector_payload(SET_SERVER_ENDPOINT, target.endpoint),
                devcode=devcode,
                collector_addr=collector_addr,
                fcode=FC_SET_COLLECTOR,
            ),
        ),
        (
            "apply_restore",
            build_collector_request(
                counter.next(),
                build_set_collector_payload(SET_REBOOT_OR_APPLY, "1"),
                devcode=devcode,
                collector_addr=collector_addr,
                fcode=FC_SET_COLLECTOR,
            ),
        ),
    ]


class FrameExtractor:
    def __init__(
        self,
        *,
        direction: str,
        remote: str,
        writer: JsonLineWriter,
        frame_callback: Callable[[FrameRecord], Awaitable[None] | None] | None = None,
    ) -> None:
        self._direction = direction
        self._remote = remote
        self._writer = writer
        self._frame_callback = frame_callback
        self._buffer = bytearray()

    async def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buffer.extend(chunk)
        await self._drain_frames(chunk_len=len(chunk))

    async def flush_tail(self) -> None:
        if not self._buffer:
            return
        await self._writer.write(
            {
                "kind": "tail",
                "timestamp": _utc_timestamp(),
                "direction": self._direction,
                "remote": self._remote,
                "remaining_hex": bytes(self._buffer).hex(),
                "remaining_ascii": _safe_ascii(bytes(self._buffer)),
            }
        )
        self._buffer.clear()

    async def _drain_frames(self, *, chunk_len: int) -> None:
        while True:
            if len(self._buffer) < HEADER_SIZE:
                return

            try:
                header = decode_header(bytes(self._buffer[:HEADER_SIZE]))
                total_len = header.total_len
            except Exception as exc:
                await self._writer.write(
                    {
                        "kind": "decode_error",
                        "timestamp": _utc_timestamp(),
                        "direction": self._direction,
                        "remote": self._remote,
                        "error": f"{type(exc).__name__}: {exc}",
                        "buffer_hex": bytes(self._buffer).hex(),
                    }
                )
                self._buffer.clear()
                return

            if total_len < HEADER_SIZE:
                await self._writer.write(
                    {
                        "kind": "invalid_length",
                        "timestamp": _utc_timestamp(),
                        "direction": self._direction,
                        "remote": self._remote,
                        "wire_len": header.wire_len,
                        "buffer_hex": bytes(self._buffer).hex(),
                    }
                )
                self._buffer.clear()
                return

            if len(self._buffer) < total_len:
                return

            frame = bytes(self._buffer[:total_len])
            del self._buffer[:total_len]
            payload = frame[HEADER_SIZE:total_len]
            record = FrameRecord(
                timestamp=_utc_timestamp(),
                direction=self._direction,
                remote=self._remote,
                chunk_len=chunk_len,
                frame_len=total_len,
                tid=header.tid,
                devcode=header.devcode,
                devaddr=header.devaddr,
                fcode=header.fcode,
                fcode_name=FCODE_NAMES.get(header.fcode, f"UNKNOWN_{header.fcode}"),
                payload_hex=payload.hex(),
                payload_ascii=_safe_ascii(payload),
            )
            await self._writer.write({"kind": "frame", **asdict(record)})
            if self._frame_callback is not None:
                callback_result = self._frame_callback(record)
                if callback_result is not None:
                    await callback_result


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    direction: str,
    remote: str,
    frame_writer: JsonLineWriter,
    chunk_callback: Callable[[bytes], Awaitable[None] | None] | None = None,
    frame_callback: Callable[[FrameRecord], Awaitable[None] | None] | None = None,
    chunk_filter: AtChunkProxyFilter | None = None,
    close_writer_on_exit: bool = True,
) -> None:
    extractor = FrameExtractor(
        direction=direction,
        remote=remote,
        writer=frame_writer,
        frame_callback=frame_callback,
    )
    try:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break
            await frame_writer.write(
                {
                    "kind": "chunk",
                    "timestamp": _utc_timestamp(),
                    "direction": direction,
                    "remote": remote,
                    "chunk_len": len(chunk),
                    "chunk_hex": chunk.hex(),
                    "chunk_ascii": _safe_ascii(chunk),
                }
            )
            if chunk_callback is not None:
                callback_result = chunk_callback(chunk)
                if callback_result is not None:
                    await callback_result
            await extractor.feed(chunk)
            forwarded = chunk
            if chunk_filter is not None:
                forwarded, events = chunk_filter.feed(chunk)
                for event in events:
                    await frame_writer.write(event)
            if forwarded:
                writer.write(forwarded)
                await writer.drain()
    finally:
        await extractor.flush_tail()
        if chunk_filter is not None:
            tail = chunk_filter.flush()
            if tail:
                writer.write(tail)
                await writer.drain()
        if close_writer_on_exit:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


async def _cancel_task(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def _drain_reader(
    reader: asyncio.StreamReader,
    *,
    direction: str,
    remote: str,
    frame_writer: JsonLineWriter,
    timeout: float = 0.2,
) -> None:
    extractor = FrameExtractor(direction=direction, remote=remote, writer=frame_writer)
    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=timeout)
        except asyncio.TimeoutError:
            return
        if not chunk:
            await extractor.flush_tail()
            return
        await frame_writer.write(
            {
                "kind": "restore_drain_chunk",
                "timestamp": _utc_timestamp(),
                "direction": direction,
                "remote": remote,
                "chunk_len": len(chunk),
                "chunk_hex": chunk.hex(),
                "chunk_ascii": _safe_ascii(chunk),
            }
        )
        await extractor.feed(chunk)


async def _drain_raw_reader(
    reader: asyncio.StreamReader,
    *,
    direction: str,
    remote: str,
    frame_writer: JsonLineWriter,
    timeout: float = 0.2,
) -> None:
    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=timeout)
        except asyncio.TimeoutError:
            return
        if not chunk:
            return
        await frame_writer.write(
            {
                "kind": "restore_drain_chunk",
                "timestamp": _utc_timestamp(),
                "direction": direction,
                "remote": remote,
                "chunk_len": len(chunk),
                "chunk_hex": chunk.hex(),
                "chunk_ascii": _safe_ascii(chunk),
            }
        )


async def _read_one_frame(
    reader: asyncio.StreamReader,
    *,
    timeout: float,
) -> bytes | None:
    try:
        header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError):
        return None

    decoded = decode_header(header)
    payload_len = decoded.payload_len
    try:
        payload = await asyncio.wait_for(reader.readexactly(payload_len), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError):
        return None
    return header + payload


async def _read_one_at_response(
    reader: asyncio.StreamReader,
    *,
    timeout: float,
) -> bytes | None:
    try:
        return await asyncio.wait_for(reader.readuntil(b"\n"), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        return None


async def _write_restore_at_commands(
    *,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    remote: str,
    frame_writer: JsonLineWriter,
    restore_target: RestoreTarget,
    followup_command: str,
) -> None:
    await _drain_raw_reader(
        reader,
        direction="collector_restore_drain",
        remote=remote,
        frame_writer=frame_writer,
    )

    for label, command in build_restore_at_commands(
        target=restore_target,
        followup_command=followup_command,
    ):
        await frame_writer.write(
            {
                "kind": "restore_inject_request",
                "timestamp": _utc_timestamp(),
                "remote": remote,
                "label": label,
                "command_ascii": _safe_ascii(command),
                "command_hex": command.hex(),
            }
        )
        writer.write(command)
        await writer.drain()
        response = await _read_one_at_response(reader, timeout=3.0)
        if response is None:
            await frame_writer.write(
                {
                    "kind": "restore_inject_response_missing",
                    "timestamp": _utc_timestamp(),
                    "remote": remote,
                    "label": label,
                }
            )
            continue
        response_payload: dict[str, object] = {
            "kind": "restore_inject_response",
            "timestamp": _utc_timestamp(),
            "remote": remote,
            "label": label,
            "payload_ascii": _safe_ascii(response),
            "payload_hex": response.hex(),
        }
        try:
            parsed = parse_at_response(response)
        except Exception:
            parsed = None
        if parsed is not None:
            response_payload["response_command"] = parsed.command
            response_payload["response_value"] = parsed.value
        await frame_writer.write(
            response_payload
        )
        await asyncio.sleep(0.25)


async def _write_restore_frames(
    *,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    remote: str,
    frame_writer: JsonLineWriter,
    observed: SessionObservation,
    restore_target: RestoreTarget,
    at_followup_command: str,
) -> None:
    if observed.saw_at_traffic:
        await _write_restore_at_commands(
            reader=reader,
            writer=writer,
            remote=remote,
            frame_writer=frame_writer,
            restore_target=restore_target,
            followup_command=at_followup_command,
        )
        return

    if observed.collector.devcode is None or observed.collector.collector_addr is None:
        await frame_writer.write(
            {
                "kind": "restore_skipped",
                "timestamp": _utc_timestamp(),
                "remote": remote,
                "reason": "collector_address_unobserved",
            }
        )
        return

    await _drain_reader(
        reader,
        direction="collector_restore_drain",
        remote=remote,
        frame_writer=frame_writer,
    )

    frames = build_restore_frames(
        target=restore_target,
        devcode=observed.collector.devcode,
        collector_addr=observed.collector.collector_addr,
    )
    for label, frame in frames:
        await frame_writer.write(
            {
                "kind": "restore_inject_request",
                "timestamp": _utc_timestamp(),
                "remote": remote,
                "label": label,
                "frame_hex": frame.hex(),
                "frame_ascii": _safe_ascii(frame[HEADER_SIZE:]),
            }
        )
        writer.write(frame)
        await writer.drain()
        response = await _read_one_frame(reader, timeout=3.0)
        if response is None:
            await frame_writer.write(
                {
                    "kind": "restore_inject_response_missing",
                    "timestamp": _utc_timestamp(),
                    "remote": remote,
                    "label": label,
                }
            )
            continue

        header = decode_header(response[:HEADER_SIZE])
        payload = response[HEADER_SIZE:]
        await frame_writer.write(
            {
                "kind": "restore_inject_response",
                "timestamp": _utc_timestamp(),
                "remote": remote,
                "label": label,
                "tid": header.tid,
                "devcode": header.devcode,
                "devaddr": header.devaddr,
                "fcode": header.fcode,
                "payload_hex": payload.hex(),
                "payload_ascii": _safe_ascii(payload),
            }
        )
        await asyncio.sleep(0.25)


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    upstream_host: str,
    upstream_port: int,
    frame_writer: JsonLineWriter,
    restore_target: RestoreTarget | None,
    restore_after: float,
    restore_at_followup: str,
    restore_trigger_file: Path | None,
) -> None:
    client_peer = client_writer.get_extra_info("peername") or ("", 0)
    client_ip = client_peer[0] or ""
    client_port = client_peer[1] or 0
    remote_label = f"{client_ip}:{client_port}"

    await frame_writer.write(
        {
            "kind": "connect",
            "timestamp": _utc_timestamp(),
            "client": remote_label,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
        }
    )

    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, upstream_port)
    except Exception as exc:
        await frame_writer.write(
            {
                "kind": "upstream_connect_error",
                "timestamp": _utc_timestamp(),
                "client": remote_label,
                "upstream_host": upstream_host,
                "upstream_port": upstream_port,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        client_writer.close()
        await client_writer.wait_closed()
        return

    observed = SessionObservation(collector=ObservedCollectorAddress())
    masked_endpoint = restore_target.endpoint if restore_target is not None else ""

    collector_at_filter = AtChunkProxyFilter(
        direction="collector_to_cloud",
        remote=remote_label,
        observed=observed,
        masked_endpoint=masked_endpoint,
    )
    cloud_at_filter = AtChunkProxyFilter(
        direction="cloud_to_collector",
        remote=remote_label,
        observed=observed,
        masked_endpoint=masked_endpoint,
    )

    def _remember_chunk_kind(chunk: bytes) -> None:
        if _looks_like_at_traffic(chunk):
            observed.saw_at_traffic = True

    def _remember_collector_address(record: FrameRecord) -> None:
        observed.collector.devcode = record.devcode
        observed.collector.collector_addr = record.devaddr

    collector_task = asyncio.create_task(
        _pipe(
            client_reader,
            upstream_writer,
            direction="collector_to_cloud",
            remote=remote_label,
            frame_writer=frame_writer,
            chunk_callback=_remember_chunk_kind,
            frame_callback=_remember_collector_address,
            chunk_filter=collector_at_filter,
            close_writer_on_exit=restore_target is None,
        )
    )
    cloud_task = asyncio.create_task(
        _pipe(
            upstream_reader,
            client_writer,
            direction="cloud_to_collector",
            remote=remote_label,
            frame_writer=frame_writer,
            chunk_filter=cloud_at_filter,
            close_writer_on_exit=restore_target is None,
        )
    )

    try:
        if restore_target is None or (restore_after <= 0 and restore_trigger_file is None):
            await asyncio.gather(collector_task, cloud_task)
        else:
            done: set[asyncio.Task[None]] = set()
            pending: set[asyncio.Task[None]] = {collector_task, cloud_task}
            deadline = asyncio.get_running_loop().time() + restore_after if restore_after > 0 else None
            restore_requested = False
            while pending:
                timeout = 0.25
                if deadline is not None:
                    timeout = max(0.0, min(timeout, deadline - asyncio.get_running_loop().time()))
                done, pending = await asyncio.wait(
                    pending,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    break
                if restore_trigger_file is not None and restore_trigger_file.exists():
                    restore_requested = True
                    break
                if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                    restore_requested = True
                    break
            if done:
                for task in pending:
                    await _cancel_task(task)
            else:
                await frame_writer.write(
                    {
                        "kind": "restore_trigger_seen" if restore_requested else "restore_window_reached",
                        "timestamp": _utc_timestamp(),
                        "remote": remote_label,
                        "restore_endpoint": restore_target.endpoint,
                    }
                )
                await _cancel_task(collector_task)
                await _cancel_task(cloud_task)
                try:
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                except Exception:
                    pass
                await _write_restore_frames(
                    reader=client_reader,
                    writer=client_writer,
                    remote=remote_label,
                    frame_writer=frame_writer,
                    observed=observed,
                    restore_target=restore_target,
                    at_followup_command=restore_at_followup,
                )
    finally:
        await frame_writer.write(
            {
                "kind": "disconnect",
                "timestamp": _utc_timestamp(),
                "client": remote_label,
            }
        )
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass
        try:
            upstream_writer.close()
            await upstream_writer.wait_closed()
        except Exception:
            pass


async def _handle_passive_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    frame_writer: JsonLineWriter,
) -> None:
    """Record one collector connection without opening any cloud upstream.

    This is intentionally receive-only from the collector side: it logs chunks
    and decoded EyeBond frames, but it does not forward traffic to SmartESS and
    does not synthesize responses back to the collector.
    """

    client_peer = client_writer.get_extra_info("peername") or ("", 0)
    client_ip = client_peer[0] or ""
    client_port = client_peer[1] or 0
    remote_label = f"{client_ip}:{client_port}"

    await frame_writer.write(
        {
            "kind": "passive_connect",
            "timestamp": _utc_timestamp(),
            "client": remote_label,
        }
    )
    extractor = FrameExtractor(
        direction="collector_to_local",
        remote=remote_label,
        writer=frame_writer,
    )
    try:
        while True:
            chunk = await client_reader.read(4096)
            if not chunk:
                break
            await frame_writer.write(
                {
                    "kind": "chunk",
                    "timestamp": _utc_timestamp(),
                    "direction": "collector_to_local",
                    "remote": remote_label,
                    "chunk_len": len(chunk),
                    "chunk_hex": chunk.hex(),
                    "chunk_ascii": _safe_ascii(chunk),
                }
            )
            await extractor.feed(chunk)
    finally:
        await extractor.flush_tail()
        await frame_writer.write(
            {
                "kind": "passive_disconnect",
                "timestamp": _utc_timestamp(),
                "client": remote_label,
            }
        )
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass


async def handle_proxy_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    upstream_host: str,
    upstream_port: int,
    frame_writer: JsonLineWriter,
    restore_target: RestoreTarget | None,
    restore_after: float,
    restore_at_followup: str,
    restore_trigger_file: Path | None,
) -> None:
    """Public compatibility wrapper for the in-process proxy session helper."""

    await _handle_client(
        client_reader,
        client_writer,
        upstream_host=upstream_host,
        upstream_port=upstream_port,
        frame_writer=frame_writer,
        restore_target=restore_target,
        restore_after=restore_after,
        restore_at_followup=restore_at_followup,
        restore_trigger_file=restore_trigger_file,
    )


async def _run(args: argparse.Namespace) -> int:
    output_handle: TextIO
    close_output = False
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_handle = output_path.open("a", encoding="utf-8")
        close_output = True
    else:
        output_handle = sys.stdout

    frame_writer = JsonLineWriter(output_handle)
    restore_target = parse_restore_target(args.restore_endpoint) if args.restore_endpoint else None
    no_upstream = bool(args.no_upstream)
    announcer: DiscoveryAnnouncer | None = None

    if not no_upstream and not args.upstream_host:
        raise SystemExit("--upstream-host is required unless --no-upstream is used")
    if args.announce_collector:
        if not no_upstream:
            raise SystemExit("--announce-collector is only available with --no-upstream")
        if not args.collector_ip:
            raise SystemExit("--collector-ip is required with --announce-collector")
        if not args.advertise_host:
            raise SystemExit("--advertise-host is required with --announce-collector")

    server = await asyncio.start_server(
        (
            (lambda reader, writer: _handle_passive_client(
                reader,
                writer,
                frame_writer=frame_writer,
            ))
            if no_upstream
            else (lambda reader, writer: _handle_client(
                reader,
                writer,
                upstream_host=args.upstream_host,
                upstream_port=args.upstream_port,
                frame_writer=frame_writer,
                restore_target=restore_target,
                restore_after=args.restore_after,
                restore_at_followup=args.restore_at_followup,
                restore_trigger_file=Path(args.restore_trigger_file) if args.restore_trigger_file else None,
            ))
        ),
        args.listen_host,
        args.listen_port,
    )

    sockets = server.sockets or []
    bound = ", ".join(str(sock.getsockname()) for sock in sockets)
    if no_upstream:
        LOGGER.info("Passive collector capture listening on %s with no upstream", bound)
    else:
        LOGGER.info("Cloud proxy listening on %s -> %s:%d", bound, args.upstream_host, args.upstream_port)

    try:
        if args.announce_collector:
            announcer = DiscoveryAnnouncer(
                bind_ip=args.advertise_host,
                advertised_server_ip=args.advertise_host,
                advertised_server_port=args.listen_port,
                target_ip=args.collector_ip,
                udp_port=args.udp_port,
                interval=args.announce_interval,
            )
            await frame_writer.write(
                {
                    "kind": "passive_discovery_started",
                    "timestamp": _utc_timestamp(),
                    "collector_ip": args.collector_ip,
                    "advertised_server_ip": args.advertise_host,
                    "advertised_server_port": args.listen_port,
                    "udp_port": args.udp_port,
                    "interval": args.announce_interval,
                }
            )
            await announcer.start()
        if args.duration > 0:
            await asyncio.sleep(args.duration)
        else:
            async with server:
                await server.serve_forever()
    finally:
        if announcer is not None:
            await announcer.stop()
            await frame_writer.write(
                {
                    "kind": "passive_discovery_stopped",
                    "timestamp": _utc_timestamp(),
                    "last_reply": announcer.last_reply,
                    "last_reply_from": announcer.last_reply_from,
                }
            )
        server.close()
        await server.wait_closed()
        if close_output:
            output_handle.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=18899)
    parser.add_argument("--upstream-host", default="")
    parser.add_argument("--upstream-port", type=int, default=18899)
    parser.add_argument(
        "--no-upstream",
        action="store_true",
        help="passively record collector traffic without connecting to or forwarding to an upstream cloud server",
    )
    parser.add_argument(
        "--announce-collector",
        action="store_true",
        help="with --no-upstream, periodically announce the passive listener to one HA-only collector via UDP discovery",
    )
    parser.add_argument("--collector-ip", default="", help="collector IP used by --announce-collector")
    parser.add_argument("--advertise-host", default="", help="local IPv4 address to advertise in set>server=...")
    parser.add_argument("--udp-port", type=int, default=58899)
    parser.add_argument("--announce-interval", type=float, default=2.0)
    parser.add_argument("--duration", type=int, default=0, help="seconds to run; 0 means forever")
    parser.add_argument(
        "--restore-endpoint",
        default="",
        help="collector endpoint to restore after capture, formatted as host,port,protocol; framed sessions use FC=3 restore and plain AT sessions use AT+CLDSRVHOST1=...",
    )
    parser.add_argument(
        "--restore-after",
        type=float,
        default=0,
        help="seconds to forward traffic before attempting restore handling",
    )
    parser.add_argument(
        "--restore-at-followup",
        default="",
        help="optional extra AT command to send after AT+CLDSRVHOST1=..., for example AT+RESET=S or AT+INTPARA=29,1",
    )
    parser.add_argument(
        "--restore-trigger-file",
        default="",
        help="sidecar file whose presence asks an active proxied session to restore the endpoint",
    )
    parser.add_argument("--output", default="")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.no_upstream:
        try:
            socket.gethostbyname(args.upstream_host)
        except OSError as exc:
            raise SystemExit(f"cannot resolve upstream host {args.upstream_host}: {exc}") from exc

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
