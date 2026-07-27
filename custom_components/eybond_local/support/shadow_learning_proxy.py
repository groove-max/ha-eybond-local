"""Fail-closed shadow-learning proxy bridge between collector and cloud."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from ..collector.protocol import HEADER_SIZE, decode_header
from ..collector.smartess_local import parse_query_collector_response, parse_set_collector_response
from .cloud_session_wire import consume_cloud_message
from .collector_cloud_proxy import JsonLineWriter
from .shadow_learning_backend import InProcessShadowLearningHandler, ShadowLearningSeed, utc_now_iso


_COLLECTOR_FORWARD_FCODES = frozenset({1, 2, 3, 22, 23, 24, 31, 32, 50})
_CLOUD_CORRELATED_RESPONSE_FCODES = frozenset({2, 3})


def route_status_indicates_control_ready(status: dict[str, Any]) -> bool:
    """Whether the collector has reconnected to OUR proxy after the endpoint switch and is
    speaking our protocol -- i.e. its new (post-reboot) param-21 endpoint is applied and it is
    live on us. This is the same moment proxy capture keys off (its ``connect`` event).

    It deliberately does NOT require the short-lived upstream proxy->cloud socket
    (``upstream_connected`` / the full ``ready`` flag): that socket connects on demand and
    SmartESS closes it after bootstrap, so requiring it made the reconnect wait false-timeout and
    trigger a premature restore of the collector to the real server. Per-write control uses the
    stricter ``route_status_indicates_control_write_ready`` predicate below. Callers handle
    ``running`` / ``upstream_error`` separately (the wait raises on them; the gate returns False).
    """

    if not bool(status.get("collector_connected")):
        return False
    return bool(
        status.get("ready")
        or status.get("route_protocol_activity")
        or status.get("collector_protocol_ingress")
    )


def route_status_indicates_control_write_ready(status: dict[str, Any]) -> bool:
    """Whether it is safe to send one live SmartESS control command now.

    Start-up readiness and per-write readiness are intentionally different.
    The start path may proceed once the collector has reconnected and spoken to
    our proxy, because the proxy->cloud socket can be short-lived while SmartESS
    registration settles. A live ``ctrlDevice`` write is stricter: SmartESS can
    only be intercepted while the proxy currently has both the collector socket
    and the upstream cloud socket. If upstream is gone, the cloud may deliver the
    command over the collector's real-server connection instead.
    """

    if not route_status_indicates_control_ready(status):
        return False
    return bool(status.get("ready") or status.get("upstream_connected"))


@dataclass(frozen=True, slots=True)
class _PendingCollectorRequest:
    tid: int
    fcode: int
    devcode: int
    devaddr: int
    request_payload: bytes


class InProcessFailClosedShadowProxyHandler:
    """Shared-listener fail-closed shadow proxy for one collector session."""

    def __init__(
        self,
        *,
        upstream_host: str,
        upstream_port: int,
        seed: ShadowLearningSeed,
        output_path: Path,
    ) -> None:
        self._upstream_host = str(upstream_host)
        self._upstream_port = int(upstream_port)
        self._output_path = Path(output_path)
        self._output_handle: TextIO | None = None
        self._writer: JsonLineWriter | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._running = False
        self._collector_connected = False
        self._collector_connection_sequence = 0
        self._collector_protocol_ingress = False
        self._route_protocol_activity = False
        self._upstream_connected = False
        self._last_error = ""
        self._backend = InProcessShadowLearningHandler(seed=seed, output_path=output_path)

    @property
    def running(self) -> bool:
        """Return whether the handler can accept routed collector connections."""

        return self._running

    @property
    def backend(self) -> InProcessShadowLearningHandler:
        """Return the local synthetic backend used for cloud command interception."""

        return self._backend

    @property
    def collector_connected(self) -> bool:
        """Return whether one collector socket is currently attached."""

        return self._collector_connected

    @property
    def upstream_connected(self) -> bool:
        """Return whether one upstream SmartESS cloud socket is connected."""

        return self._upstream_connected

    @property
    def collector_protocol_ingress(self) -> bool:
        """Return whether collector protocol bytes were validated on this route."""

        return self._collector_protocol_ingress

    @property
    def ready(self) -> bool:
        """Return whether the proxy route is protocol-ready for live learning."""

        return (
            self._running
            and self._collector_connected
            and self._route_protocol_activity
            and self._upstream_connected
        )

    def status(self) -> dict[str, Any]:
        """Return one compact status snapshot for runtime lifecycle checks."""

        return {
            "running": self._running,
            "collector_connected": self._collector_connected,
            "collector_connection_sequence": self._collector_connection_sequence,
            "collector_protocol_ingress": self._collector_protocol_ingress,
            "route_protocol_activity": self._route_protocol_activity,
            "upstream_connected": self._upstream_connected,
            "ready": self.ready,
            "upstream_error": self._last_error,
        }

    def observation_cursor(self) -> int:
        """Return one cursor pointing to the current write-observation tail."""

        return self._backend.observation_cursor()

    def observations_since(self, cursor: int):
        """Return observations at or after one cursor."""

        return self._backend.observations_since(cursor)

    @property
    def read_map(self) -> dict:
        """Return the aggregated cloud read map from the backend."""

        return self._backend.read_map

    async def wait_for_observations_since(self, cursor: int, *, timeout_seconds: float):
        """Wait for observations at or after one cursor."""

        return await self._backend.wait_for_observations_since(
            cursor,
            timeout_seconds=timeout_seconds,
        )

    async def start(self) -> None:
        """Open trace streams and make the handler ready for route traffic."""

        if self._running:
            return
        self._output_handle = await asyncio.to_thread(
            _open_append_text_file,
            self._output_path,
        )
        self._writer = JsonLineWriter(self._output_handle)
        await self._backend.start()
        self._collector_connected = False
        self._collector_protocol_ingress = False
        self._route_protocol_activity = False
        self._upstream_connected = False
        self._last_error = ""
        self._running = True
        await self._append_event(
            "shadow_proxy_started",
            "shadow_proxy",
            {
                "upstream_host": self._upstream_host,
                "upstream_port": self._upstream_port,
            },
        )

    async def stop(self) -> None:
        """Cancel active client tasks and close trace streams."""

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
        self._collector_connected = False
        self._collector_protocol_ingress = False
        self._route_protocol_activity = False
        self._upstream_connected = False
        await self._backend.stop()
        output_handle = self._output_handle
        self._output_handle = None
        self._writer = None
        if output_handle is not None:
            await asyncio.to_thread(output_handle.close)

    async def handle_client(
        self,
        collector_reader: asyncio.StreamReader,
        collector_writer: asyncio.StreamWriter,
    ) -> None:
        """Proxy one collector callback with cloud fail-closed interception policy."""

        if not self._running:
            collector_writer.close()
            try:
                await collector_writer.wait_closed()
            except Exception:
                pass
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            self._tasks.add(current_task)

        collector_peer = collector_writer.get_extra_info("peername") or ("", 0)
        remote = f"{collector_peer[0] or ''}:{collector_peer[1] or 0}"
        self._collector_connection_sequence += 1
        self._collector_connected = True
        self._collector_protocol_ingress = False
        self._upstream_connected = False
        self._last_error = ""

        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self._upstream_host,
                self._upstream_port,
            )
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}:{exc}"
            await self._append_event(
                "shadow_proxy_upstream_connect_error",
                "shadow_proxy",
                {
                    "remote": remote,
                    "upstream_host": self._upstream_host,
                    "upstream_port": self._upstream_port,
                    "error": f"{type(exc).__name__}:{exc}",
                },
            )
            self._collector_connected = False
            collector_writer.close()
            try:
                await collector_writer.wait_closed()
            except Exception:
                pass
            if current_task is not None:
                self._tasks.discard(current_task)
            return
        self._upstream_connected = True

        pending_requests: dict[int, _PendingCollectorRequest] = {}
        collector_buffer = bytearray()
        cloud_buffer = bytearray()
        upstream_write_lock = asyncio.Lock()
        collector_write_lock = asyncio.Lock()

        await self._append_event(
            "shadow_proxy_connected",
            "shadow_proxy",
            {
                "remote": remote,
                "upstream_host": self._upstream_host,
                "upstream_port": self._upstream_port,
            },
        )

        async def _collector_to_cloud() -> None:
            while True:
                chunk = await collector_reader.read(4096)
                if not chunk:
                    return
                collector_buffer.extend(chunk)
                await self._append_event(
                    "shadow_proxy_collector_chunk",
                    "collector_to_shadow_proxy",
                    {"remote": remote, "chunk_hex": chunk.hex(), "chunk_len": len(chunk)},
                )
                while True:
                    try:
                        message = consume_cloud_message(collector_buffer)
                    except Exception as exc:
                        await self._append_event(
                            "shadow_proxy_parser_error",
                            "collector_to_cloud",
                            {
                                "remote": remote,
                                "error": f"{type(exc).__name__}:{exc}",
                                "buffer_hex": bytes(collector_buffer).hex(),
                            },
                        )
                        raise
                    if message is None:
                        break
                    kind, payload = message
                    self._collector_protocol_ingress = True
                    self._route_protocol_activity = True
                    if kind == "at":
                        async with upstream_write_lock:
                            upstream_writer.write(payload)
                            await upstream_writer.drain()
                        await self._append_event(
                            "shadow_proxy_forward_collector_at",
                            "collector_to_cloud",
                            {"remote": remote, "payload_ascii": payload.decode("ascii", errors="replace")},
                        )
                        continue
                    if kind == "modbus":
                        # Genuine device-originated Modbus RTU (uncommon in
                        # observe-only mode) is forwarded upstream unchanged.
                        async with upstream_write_lock:
                            upstream_writer.write(payload)
                            await upstream_writer.drain()
                        await self._append_event(
                            "shadow_proxy_forward_collector_modbus",
                            "collector_to_cloud",
                            {"remote": remote, "payload_hex": payload.hex()},
                        )
                        continue
                    if kind == "ascii":
                        async with upstream_write_lock:
                            upstream_writer.write(payload)
                            await upstream_writer.drain()
                        await self._append_event(
                            "shadow_proxy_forward_collector_ascii",
                            "collector_to_cloud",
                            {
                                "remote": remote,
                                "payload_ascii": payload.decode("ascii", errors="replace").strip(),
                            },
                        )
                        continue
                    header = decode_header(payload[:HEADER_SIZE])
                    if header.fcode in _COLLECTOR_FORWARD_FCODES:
                        pending_requests[int(header.tid)] = _PendingCollectorRequest(
                            tid=int(header.tid),
                            fcode=int(header.fcode),
                            devcode=int(header.devcode),
                            devaddr=int(header.devaddr),
                            request_payload=payload[HEADER_SIZE:],
                        )
                        async with upstream_write_lock:
                            upstream_writer.write(payload)
                            await upstream_writer.drain()
                        await self._append_event(
                            "shadow_proxy_forward_collector_frame",
                            "collector_to_cloud",
                            {
                                "remote": remote,
                                "tid": header.tid,
                                "fcode": header.fcode,
                                "devcode": header.devcode,
                                "devaddr": header.devaddr,
                            },
                        )
                    else:
                        await self._append_event(
                            "shadow_proxy_block_collector_unclassified",
                            "collector_to_cloud",
                            {
                                "remote": remote,
                                "tid": header.tid,
                                "fcode": header.fcode,
                                "devcode": header.devcode,
                                "devaddr": header.devaddr,
                            },
                        )

        async def _cloud_to_collector() -> None:
            while True:
                chunk = await upstream_reader.read(4096)
                if not chunk:
                    return
                cloud_buffer.extend(chunk)
                await self._append_event(
                    "shadow_proxy_cloud_chunk",
                    "cloud_to_shadow_proxy",
                    {"remote": remote, "chunk_hex": chunk.hex(), "chunk_len": len(chunk)},
                )
                while True:
                    try:
                        message = consume_cloud_message(cloud_buffer)
                    except Exception as exc:
                        await self._append_event(
                            "shadow_proxy_parser_error",
                            "cloud_to_collector",
                            {
                                "remote": remote,
                                "error": f"{type(exc).__name__}:{exc}",
                                "buffer_hex": bytes(cloud_buffer).hex(),
                            },
                        )
                        raise
                    if message is None:
                        break
                    kind, payload = message
                    self._route_protocol_activity = True
                    if kind == "at":
                        if self._backend.should_forward_cloud_at(payload):
                            # Identity/telemetry queries the shadow can't answer
                            # (e.g. DTUPN, INTPARA58) go to the real collector;
                            # its genuine reply returns upstream via
                            # _collector_to_cloud so cloud registration succeeds.
                            async with collector_write_lock:
                                collector_writer.write(payload)
                                await collector_writer.drain()
                            await self._append_event(
                                "shadow_proxy_forward_cloud_at",
                                "cloud_to_collector",
                                {"remote": remote, "payload_ascii": payload.decode("ascii", errors="replace")},
                            )
                            continue
                        response = await self._backend._handle_at_line(payload, remote=remote)
                        await self._append_event(
                            "shadow_proxy_intercept_cloud_at",
                            "cloud_to_shadow",
                            {"remote": remote, "payload_ascii": payload.decode("ascii", errors="replace")},
                        )
                        if response is not None:
                            async with upstream_write_lock:
                                upstream_writer.write(response)
                                await upstream_writer.drain()
                        continue

                    if kind == "modbus":
                        # Bare Modbus RTU control/poll traffic: answer reads from
                        # the shadow bank and observe + NACK writes locally. It
                        # is never forwarded to the inverter.
                        response = await self._backend._handle_modbus_frame(payload, remote=remote)
                        if response is not None:
                            async with upstream_write_lock:
                                upstream_writer.write(response)
                                await upstream_writer.drain()
                        continue

                    if kind == "ascii":
                        response = await self._backend._handle_ascii_frame(payload, remote=remote)
                        if response is not None:
                            async with upstream_write_lock:
                                upstream_writer.write(response)
                                await upstream_writer.drain()
                        continue

                    header = decode_header(payload[:HEADER_SIZE])
                    pending = pending_requests.get(int(header.tid))
                    if pending is not None:
                        allow_forward, reason = _is_allowlisted_correlated_response(
                            header=header,
                            frame=payload,
                            pending=pending,
                        )
                        if allow_forward:
                            pending_requests.pop(int(header.tid), None)
                            async with collector_write_lock:
                                collector_writer.write(payload)
                                await collector_writer.drain()
                            await self._append_event(
                                "shadow_proxy_forward_cloud_correlated_response",
                                "cloud_to_collector",
                                {
                                    "remote": remote,
                                    "tid": header.tid,
                                    "fcode": header.fcode,
                                    "devcode": header.devcode,
                                    "devaddr": header.devaddr,
                                },
                            )
                            continue
                        await self._append_event(
                            "shadow_proxy_block_cloud_correlated_unallowlisted",
                            "cloud_to_shadow",
                            {
                                "remote": remote,
                                "tid": header.tid,
                                "fcode": header.fcode,
                                "devcode": header.devcode,
                                "devaddr": header.devaddr,
                                "reason": reason,
                            },
                        )

                    try:
                        response = await self._backend._handle_frame(payload, remote=remote)
                    except Exception as exc:
                        await self._append_event(
                            "shadow_proxy_parser_error",
                            "cloud_to_collector",
                            {
                                "remote": remote,
                                "error": f"{type(exc).__name__}:{exc}",
                                "payload_hex": payload.hex(),
                            },
                        )
                        raise
                    await self._append_event(
                        "shadow_proxy_intercept_cloud_unclassified",
                        "cloud_to_shadow",
                        {
                            "remote": remote,
                            "tid": header.tid,
                            "fcode": header.fcode,
                            "devcode": header.devcode,
                            "devaddr": header.devaddr,
                        },
                    )
                    if response is not None:
                        async with upstream_write_lock:
                            upstream_writer.write(response)
                            await upstream_writer.drain()

        collector_task = asyncio.create_task(_collector_to_cloud())
        cloud_task = asyncio.create_task(_cloud_to_collector())
        try:
            done, pending = await asyncio.wait(
                {collector_task, cloud_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done | pending:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        finally:
            self._collector_connected = False
            self._upstream_connected = False
            await self._append_event(
                "shadow_proxy_disconnected",
                "shadow_proxy",
                {"remote": remote},
            )
            try:
                collector_writer.close()
                await collector_writer.wait_closed()
            except Exception:
                pass
            try:
                upstream_writer.close()
                await upstream_writer.wait_closed()
            except Exception:
                pass
            if current_task is not None:
                self._tasks.discard(current_task)

    async def _append_event(self, kind: str, direction: str, payload: dict[str, Any]) -> None:
        writer = self._writer
        if writer is None:
            return
        await writer.write(
            {
                "kind": kind,
                "timestamp": utc_now_iso(),
                "direction": direction,
                "payload": dict(payload),
            }
        )


def _is_allowlisted_correlated_response(
    *,
    header: Any,
    frame: bytes,
    pending: _PendingCollectorRequest,
) -> tuple[bool, str]:
    payload = frame[HEADER_SIZE:]
    payload_len = len(payload)
    if int(header.fcode) not in _CLOUD_CORRELATED_RESPONSE_FCODES:
        return False, "fcode_not_response_gated"
    if int(header.fcode) != int(pending.fcode):
        return False, "fcode_mismatch"
    if int(header.devcode) != int(pending.devcode):
        return False, "devcode_mismatch"
    if int(header.devaddr) != int(pending.devaddr):
        return False, "devaddr_mismatch"
    if payload_len <= 0:
        return False, "empty_payload"
    if payload_len > 512:
        return False, "payload_too_large"
    if int(header.fcode) == 2:
        return _matches_fc2_query_response(payload=payload, pending=pending)
    if int(header.fcode) == 3:
        return _matches_fc3_set_response(payload=payload, pending=pending)
    return False, "fcode_not_response_gated"


def _matches_fc2_query_response(
    *,
    payload: bytes,
    pending: _PendingCollectorRequest,
) -> tuple[bool, str]:
    if not pending.request_payload:
        return False, "fc2_request_missing_parameter"
    expected_parameters = set(pending.request_payload)
    try:
        response = parse_query_collector_response(payload)
    except Exception:
        return False, "fc2_response_invalid"

    # FC=2 correlated forwarding is strict fail-closed: only explicit success/fail
    # status codes are accepted as response grammar.
    if int(response.code) not in {0, 1}:
        return False, "fc2_response_code_invalid"
    if response.parameter not in expected_parameters:
        return False, "fc2_response_parameter_mismatch"
    if int(response.code) == 0 and len(response.data) <= 0:
        return False, "fc2_response_data_missing"
    if int(response.code) == 1 and len(response.data) != 0:
        return False, "fc2_response_fail_shape_invalid"
    return True, "fc2_response"


def _matches_fc3_set_response(
    *,
    payload: bytes,
    pending: _PendingCollectorRequest,
) -> tuple[bool, str]:
    if len(pending.request_payload) < 1:
        return False, "fc3_request_missing_parameter"
    if len(payload) != 2:
        return False, "fc3_response_length_invalid"
    try:
        response = parse_set_collector_response(payload)
    except Exception:
        return False, "fc3_response_invalid"
    if response.parameter != pending.request_payload[0]:
        return False, "fc3_response_parameter_mismatch"
    return True, "fc3_response"


def _open_append_text_file(path: Path) -> TextIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("a", encoding="utf-8")


__all__ = [
    "InProcessFailClosedShadowProxyHandler",
]
