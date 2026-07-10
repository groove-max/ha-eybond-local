"""Runtime helpers for collector proxy capture sessions."""

from __future__ import annotations

import asyncio
from collections import deque
import json
from pathlib import Path
import sys
from typing import Any, Awaitable, Callable, TextIO

from ..collector.at import parse_at_command, parse_at_response
from ..collector.protocol import HEADER_SIZE, decode_header
from ..payload.modbus import crc16_modbus
from .collector_cloud_proxy import (
    JsonLineWriter,
    handle_proxy_client,
    parse_restore_target,
)
from .proxy_trace import proxy_trace_root

AsyncOutputCloser = Callable[[TextIO], Awaitable[None]]
AsyncOutputOpener = Callable[[Path], Awaitable[TextIO]]


def open_proxy_trace_output_file(path: Path) -> TextIO:
    """Create parent directories and open one proxy trace stream for appending."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path.open("a", encoding="utf-8")


async def _default_async_open_output(path: Path) -> TextIO:
    return await asyncio.to_thread(open_proxy_trace_output_file, path)


async def _default_async_close_output(output: TextIO) -> None:
    await asyncio.to_thread(output.close)


class InProcessProxyCaptureHandler:
    """Shared-listener proxy capture handler for one collector session."""

    def __init__(
        self,
        *,
        upstream_host: str,
        upstream_port: int,
        output_path: Path,
        masked_endpoint: str = "",
        restore_trigger_path: Path | None = None,
        async_open_output: AsyncOutputOpener | None = None,
        async_close_output: AsyncOutputCloser | None = None,
    ) -> None:
        self._upstream_host = str(upstream_host)
        self._upstream_port = int(upstream_port)
        self._output_path = Path(output_path)
        self._masked_endpoint = str(masked_endpoint or "").strip()
        self._restore_trigger_path = restore_trigger_path
        self._async_open_output = async_open_output or _default_async_open_output
        self._async_close_output = async_close_output or _default_async_close_output
        self._output_handle: TextIO | None = None
        self._frame_writer: JsonLineWriter | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._running = False

    @property
    def running(self) -> bool:
        """Return whether the handler can accept routed collector connections."""

        return self._running

    async def start(self) -> None:
        """Open the trace stream and make the handler ready for listener routing."""

        if self._running:
            return
        self._output_handle = await self._async_open_output(self._output_path)
        self._frame_writer = JsonLineWriter(self._output_handle)
        self._running = True

    async def stop(self) -> None:
        """Cancel active proxy tasks and close the trace stream."""

        self._running = False
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._tasks.clear()

        output_handle = self._output_handle
        self._output_handle = None
        self._frame_writer = None
        if output_handle is not None:
            await self._async_close_output(output_handle)

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Proxy one collector connection accepted by the shared ingress listener."""

        if not self._running or self._frame_writer is None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            self._tasks.add(current_task)
        try:
            await handle_proxy_client(
                reader,
                writer,
                upstream_host=self._upstream_host,
                upstream_port=self._upstream_port,
                frame_writer=self._frame_writer,
                restore_target=(
                    parse_restore_target(self._masked_endpoint)
                    if self._masked_endpoint
                    else None
                ),
                restore_after=0,
                restore_at_followup="",
                restore_trigger_file=self._restore_trigger_path,
            )
        finally:
            if current_task is not None:
                self._tasks.discard(current_task)


def build_proxy_capture_trace_path(
    *,
    config_dir: Path,
    entry_id: str = "",
    collector_pn: str = "",
    timestamp: str,
) -> Path:
    """Return the JSONL output path for one proxy capture session."""

    root = proxy_trace_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)
    stem = _slugify(str(entry_id or "").strip() or str(collector_pn or "").strip() or "proxy_capture")
    return root / f"{stem}_{timestamp}.jsonl"


def build_proxy_capture_command(
    *,
    listen_host: str,
    listen_port: int,
    upstream_host: str,
    upstream_port: int,
    output_path: Path,
    masked_endpoint: str = "",
    restore_trigger_path: Path | None = None,
    python_executable: str | None = None,
) -> list[str]:
    """Build the subprocess command that starts the TCP proxy capture tool."""

    executable = python_executable or sys.executable
    script_path = Path(__file__).resolve().with_name("collector_cloud_proxy.py")
    command = [
        executable,
        "-u",
        str(script_path),
        "--listen-host",
        str(listen_host),
        "--listen-port",
        str(int(listen_port)),
        "--upstream-host",
        str(upstream_host),
        "--upstream-port",
        str(int(upstream_port)),
        "--output",
        str(output_path),
    ]
    normalized_masked_endpoint = str(masked_endpoint or "").strip()
    if normalized_masked_endpoint:
        command.extend(["--restore-endpoint", normalized_masked_endpoint])
    if restore_trigger_path is not None:
        command.extend(["--restore-trigger-file", str(restore_trigger_path)])
    return command


def build_proxy_capture_restore_trigger_path(trace_path: Path) -> Path:
    """Return the sidecar file used to ask the proxy subprocess to restore."""

    return trace_path.with_suffix(trace_path.suffix + ".restore")


def inspect_proxy_capture_start_status(trace_path: Path) -> dict[str, Any]:
    """Return startup/restore markers from one JSONL proxy trace."""

    status: dict[str, Any] = {
        "connected": False,
        "upstream_error": "",
        "restore_confirmed": False,
        "restore_missing": False,
    }
    if not trace_path.exists():
        return status
    try:
        handle = trace_path.open("r", encoding="utf-8")
    except OSError:
        return status
    with handle:
        for raw_line in handle:
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            kind = str(payload.get("kind") or "").strip()
            if kind == "connect":
                status["connected"] = True
            elif kind == "upstream_connect_error":
                status["upstream_error"] = str(payload.get("error") or "upstream_connect_error")
            elif kind == "restore_inject_response_missing":
                status["restore_missing"] = True
            elif kind == "restore_inject_response":
                value = str(payload.get("response_value") or payload.get("payload_ascii") or "")
                label = str(payload.get("label") or "")
                if label in {"restore_endpoint", "restore_endpoint_at"} and ("W000" in value or value):
                    status["restore_confirmed"] = True
    return status


def summarize_proxy_capture_trace(trace_path: Path) -> dict[str, Any]:
    """Return a lightweight summary of one JSONL trace artifact."""

    if not trace_path.exists():
        return {
            "exists": False,
            "line_count": 0,
            "kind_counts": {},
        }

    line_count = 0
    kind_counts: dict[str, int] = {}
    g_ascii_command_counts: dict[str, int] = {}
    g_ascii_response_counts: dict[str, int] = {}
    invalid_lines = 0
    with trace_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            line_count += 1
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            if not isinstance(payload, dict):
                invalid_lines += 1
                continue
            kind = str(payload.get("kind") or "unknown").strip() or "unknown"
            kind_counts[kind] = kind_counts.get(kind, 0) + 1
            for line in _extract_g_ascii_lines_from_trace_payload(payload):
                if _looks_like_g_ascii_command(line):
                    g_ascii_command_counts[line] = g_ascii_command_counts.get(line, 0) + 1
                else:
                    label = _g_ascii_response_label(line)
                    g_ascii_response_counts[label] = g_ascii_response_counts.get(label, 0) + 1

    summary: dict[str, Any] = {
        "exists": True,
        "line_count": line_count,
        "kind_counts": kind_counts,
    }
    if g_ascii_command_counts:
        summary["g_ascii_command_counts"] = g_ascii_command_counts
    if g_ascii_response_counts:
        summary["g_ascii_response_counts"] = g_ascii_response_counts
    if invalid_lines:
        summary["invalid_lines"] = invalid_lines
    return summary


def inspect_proxy_capture_trace(
    trace_path: Path,
    *,
    recent_limit: int = 5,
    live_log_limit: int = 40,
) -> dict[str, Any]:
    """Return a compact live-inspection view for one JSONL proxy trace."""

    summary = summarize_proxy_capture_trace(trace_path)
    if not summary.get("exists"):
        return {
            **summary,
            "kind_summary": "",
            "recent_kinds": "",
            "recent_events": "",
            "last_timestamp": "",
        }

    recent: list[str] = []
    recent_events: list[str] = []
    live_window = max(int(live_log_limit), max(int(recent_limit), 1))
    live_log: deque[str] = deque(maxlen=live_window)
    last_timestamp = ""
    recent_window = max(int(recent_limit), 1)
    with trace_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            kind = str(payload.get("kind") or "unknown").strip() or "unknown"
            recent.append(kind)
            if len(recent) > recent_window:
                recent.pop(0)
            formatted = _format_trace_event(payload)
            if formatted:
                live_log.append(formatted)
                recent_events.append(formatted)
                if len(recent_events) > recent_window:
                    recent_events.pop(0)
            timestamp = str(payload.get("timestamp") or "").strip()
            if timestamp:
                last_timestamp = timestamp

    ordered_counts = sorted(
        (summary.get("kind_counts") or {}).items(),
        key=lambda item: (-int(item[1]), str(item[0])),
    )
    return {
        **summary,
        "kind_summary": ", ".join(f"{kind}={count}" for kind, count in ordered_counts),
        "recent_kinds": " -> ".join(recent),
        "recent_events": "\n".join(recent_events),
        "live_log": "\n".join(live_log),
        "last_timestamp": last_timestamp,
    }


def _format_trace_event(payload: dict[str, Any]) -> str:
    timestamp = str(payload.get("timestamp") or "").strip()
    kind = str(payload.get("kind") or "unknown").strip() or "unknown"
    direction = str(payload.get("direction") or "").strip()
    prefix = _event_prefix(timestamp, direction)

    if kind == "connect":
        client = str(payload.get("client") or "collector").strip()
        upstream = f"{payload.get('upstream_host', '')}:{payload.get('upstream_port', '')}".strip(":")
        return f"{prefix}connected {client} -> {upstream}"
    if kind == "disconnect":
        client = str(payload.get("client") or "collector").strip()
        return f"{prefix}disconnected {client}"
    if kind == "chunk":
        description = _describe_chunk_payload(
            payload,
            hex_key="chunk_hex",
            ascii_key="chunk_ascii",
            length_key="chunk_len",
            skip_complete_eybond=True,
        )
        return f"{prefix}{description}".strip() if description else ""
    if kind == "frame":
        return _format_eybond_frame(prefix, payload)
    if kind == "masked_endpoint_response":
        forwarded = _single_line_preview(str(payload.get("forwarded_ascii") or ""))
        return f"{prefix}masked AT+CLDSRVHOST1 response as {forwarded}".strip()
    if kind == "restore_inject_request":
        label = str(payload.get("label") or "restore").strip()
        command = _single_line_preview(str(payload.get("command_ascii") or payload.get("frame_ascii") or ""))
        return f"{prefix}injected {label} {command}".strip()
    if kind == "restore_inject_response":
        value = _single_line_preview(str(payload.get("payload_ascii") or payload.get("response_value") or ""))
        return f"{prefix}restore response {value}".strip()
    if kind == "restore_drain_chunk":
        description = _describe_chunk_payload(
            payload,
            hex_key="chunk_hex",
            ascii_key="chunk_ascii",
            length_key="chunk_len",
        )
        return f"{prefix}restore drain {description}".strip()
    if kind == "tail":
        description = _describe_chunk_payload(
            payload,
            hex_key="remaining_hex",
            ascii_key="remaining_ascii",
            length_key="remaining_len",
        )
        return f"{prefix}stream tail {description}".strip()
    if kind.endswith("error"):
        error = _single_line_preview(str(payload.get("error") or ""))
        return f"{prefix}{kind} {error}".strip()
    return f"{prefix}{kind}".strip()


def _event_prefix(timestamp: str, direction: str) -> str:
    parts: list[str] = []
    if timestamp:
        parts.append(timestamp)
    if direction:
        parts.append(f"[{_direction_label(direction)}]")
    return (" ".join(parts) + " ") if parts else ""


def _direction_label(direction: str) -> str:
    return {
        "collector_to_cloud": "collector -> cloud",
        "cloud_to_collector": "cloud -> collector",
        "collector_restore_drain": "collector restore drain",
    }.get(str(direction or "").strip(), str(direction or "").strip())


def _describe_chunk_payload(
    payload: dict[str, Any],
    *,
    hex_key: str,
    ascii_key: str,
    length_key: str,
    skip_complete_eybond: bool = False,
) -> str:
    data = _decode_hex_bytes(str(payload.get(hex_key) or ""))
    ascii_preview = str(payload.get(ascii_key) or "")
    if skip_complete_eybond and _looks_like_complete_eybond_frame(data):
        return ""

    description = _describe_transport_payload(data, ascii_preview=ascii_preview)
    if description:
        return description

    length = payload.get(length_key)
    if length in (None, ""):
        length = len(data)
    return f"unrecognized binary {length} bytes hex={_hex_preview(data)}".strip()


def _format_eybond_frame(prefix: str, payload: dict[str, Any]) -> str:
    fcode = str(payload.get("fcode_name") or payload.get("fcode") or "frame").strip()
    tid = payload.get("tid")
    devcode = payload.get("devcode")
    devaddr = payload.get("devaddr")
    payload_bytes = _decode_hex_bytes(str(payload.get("payload_hex") or ""))
    nested = _describe_transport_payload(
        payload_bytes,
        ascii_preview=str(payload.get("payload_ascii") or ""),
    )
    header = f"EyeBond {fcode} tid={tid} devcode={devcode} addr={devaddr}"
    if nested:
        return f"{prefix}{header} payload={nested}".strip()
    return f"{prefix}{header} payload_hex={_hex_preview(payload_bytes)}".strip()


def _describe_transport_payload(data: bytes, *, ascii_preview: str = "") -> str:
    at_description = _describe_at_payload(data, ascii_preview=ascii_preview)
    if at_description:
        return at_description

    g_ascii_description = _describe_g_ascii_payload(data, ascii_preview=ascii_preview)
    if g_ascii_description:
        return g_ascii_description

    rtu_description = _describe_modbus_rtu_payload(data)
    if rtu_description:
        return rtu_description

    return ""


def _describe_g_ascii_payload(data: bytes, *, ascii_preview: str) -> str:
    text = ascii_preview or data.decode("ascii", errors="ignore")
    lines = _extract_g_ascii_lines(text)
    if not lines:
        return ""

    rendered: list[str] = []
    for line in lines:
        if _looks_like_g_ascii_command(line):
            rendered.append(f"G-ASCII command {line}")
        else:
            rendered.append(f"G-ASCII response {_single_line_preview(line)}")
    return " ; ".join(rendered)


def _describe_at_payload(data: bytes, *, ascii_preview: str) -> str:
    text = ascii_preview or data.decode("ascii", errors="ignore")
    lines = [line.strip() for line in text.replace("\r", "\n").split("\n") if line.strip()]
    if not lines or not all(line.startswith("AT+") for line in lines):
        return ""

    rendered: list[str] = []
    for line in lines:
        try:
            command = parse_at_command(line)
        except Exception:
            try:
                response = parse_at_response(line)
            except Exception:
                rendered.append(f"AT {line}")
            else:
                rendered.append(f"AT response {response.command}:{response.value}")
        else:
            if command.operation == "query":
                rendered.append(f"AT query {command.command}?")
            else:
                rendered.append(f"AT write {command.command}={command.value}")
    return " ; ".join(rendered)


def _describe_modbus_rtu_payload(data: bytes) -> str:
    if len(data) < 4:
        return ""

    crc_received = int.from_bytes(data[-2:], "little")
    crc_expected = crc16_modbus(data[:-2])
    if crc_received != crc_expected:
        return ""

    slave = data[0]
    function_code = data[1]
    if function_code & 0x80 and len(data) == 5:
        return (
            f"RTU exception slave={slave} fc=0x{function_code & 0x7F:02x} code=0x{data[2]:02x} "
            f"hex={_hex_preview(data)}"
        )

    if function_code in {0x03, 0x04}:
        if len(data) == 8:
            address = int.from_bytes(data[2:4], "big")
            count = int.from_bytes(data[4:6], "big")
            return f"RTU read request slave={slave} fc=0x{function_code:02x} addr=0x{address:04x} count={count}"
        byte_count = data[2]
        if len(data) == 3 + byte_count + 2:
            return (
                f"RTU read response slave={slave} fc=0x{function_code:02x} bytes={byte_count} "
                f"data={_hex_preview(data[3:-2])}"
            )

    if function_code == 0x10:
        if len(data) == 8:
            address = int.from_bytes(data[2:4], "big")
            count = int.from_bytes(data[4:6], "big")
            return f"RTU write response slave={slave} fc=0x10 addr=0x{address:04x} count={count}"
        if len(data) >= 9:
            address = int.from_bytes(data[2:4], "big")
            count = int.from_bytes(data[4:6], "big")
            byte_count = data[6]
            return (
                f"RTU write request slave={slave} fc=0x10 addr=0x{address:04x} count={count} "
                f"bytes={byte_count} data={_hex_preview(data[7:-2])}"
            )

    return f"RTU frame slave={slave} fc=0x{function_code:02x} hex={_hex_preview(data)}"


def _looks_like_complete_eybond_frame(data: bytes) -> bool:
    if len(data) < HEADER_SIZE:
        return False
    try:
        header = decode_header(data[:HEADER_SIZE])
    except Exception:
        return False
    return header.total_len == len(data) and header.total_len >= HEADER_SIZE


def _decode_hex_bytes(value: str) -> bytes:
    normalized = str(value or "").strip()
    if not normalized:
        return b""
    try:
        return bytes.fromhex(normalized)
    except ValueError:
        return b""


def _hex_preview(data: bytes, *, limit: int = 24) -> str:
    if not data:
        return ""
    preview = data[:limit].hex(" ")
    if len(data) <= limit:
        return preview
    return f"{preview} ..."


def _single_line_preview(value: str, *, limit: int = 120) -> str:
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _extract_g_ascii_lines_from_trace_payload(payload: dict[str, Any]) -> list[str]:
    data_fields = ("chunk_hex", "payload_hex", "remaining_hex")
    ascii_fields = ("chunk_ascii", "payload_ascii", "remaining_ascii")
    lines: list[str] = []
    for key in ascii_fields:
        lines.extend(_extract_g_ascii_lines(str(payload.get(key) or "")))
    for key in data_fields:
        data = _decode_hex_bytes(str(payload.get(key) or ""))
        if data:
            lines.extend(_extract_g_ascii_lines(data.decode("ascii", errors="ignore")))

    nested = payload.get("payload")
    if isinstance(nested, dict):
        lines.extend(_extract_g_ascii_lines_from_trace_payload(nested))
    return lines


def _extract_g_ascii_lines(text: str) -> list[str]:
    if not text:
        return []
    lines: list[str] = []
    for raw_line in str(text).replace("\r", "\n").split("\n"):
        line = raw_line.strip()
        if not line or line.startswith("AT+"):
            continue
        if not all(0x20 <= ord(char) <= 0x7E for char in line):
            continue
        if line.startswith(("#", "(", "ACK", "NAK", "NOA", "ERCRC", "BL")) or _looks_like_g_ascii_command(line):
            lines.append(line)
    return lines


def _looks_like_g_ascii_command(line: str) -> bool:
    text = str(line or "").strip().upper()
    if not text or text.startswith(("AT+", "ACK", "NAK", "NOA", "ERCRC", "BL", "#", "(")):
        return False
    return (
        text in {"F", "GMOD", "SVFW", "GTMP", "GLINE", "GBAT", "GBUS", "GCHG", "GOP", "GINV", "GWS", "GPV"}
        or (text.startswith("GPDAT") and text[5:].isdigit())
        or text.endswith("?")
    )


def _g_ascii_response_label(line: str) -> str:
    text = str(line or "").strip()
    if text.startswith("("):
        return "data"
    if text.startswith("#"):
        return "firmware"
    upper = text.upper()
    if upper.startswith("BL"):
        return "battery_level"
    if upper in {"ACK", "NAK", "NOA", "ERCRC"}:
        return upper
    return "data"


def _slugify(value: str) -> str:
    cleaned = [char if char.isalnum() else "_" for char in str(value or "").strip()]
    collapsed = "".join(cleaned).strip("_")
    return collapsed or "proxy_capture"
