"""In-process SmartESS shadow-learning backend and seed helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import asyncio
import re
from pathlib import Path
from typing import Any

from ..collector.at import build_at_response, parse_at_command
from ..collector.protocol import HEADER_SIZE, build_collector_request, decode_header
from ..const import LOCAL_METADATA_DIR
from ..fixtures.utils import build_command_fixture_responses
from ..payload.modbus import crc16_modbus, decode_read_request, decode_write_request
from .collector_cloud_proxy import JsonLineWriter
from .shadow_learning import ShadowWriteObservation, write_observation_from_modbus_request


_SHADOW_TRACE_DIR = "shadow_learning_traces"

# Cloud-issued AT commands that control where the DTU connects. These are always
# answered from the shadow seed (never forwarded to the collector) so the cloud
# cannot redirect the DTU away from the local proxy.
_CLOUD_REDIRECT_AT_COMMAND_PREFIX = "CLDSRVHOST"

# Bounded distinct value samples kept per register in the in-memory read map.
_READ_SAMPLE_LIMIT = 8


def utc_now_iso() -> str:
    """Return one UTC ISO-8601 timestamp."""

    return datetime.now(timezone.utc).isoformat()


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    normalized = normalized.strip("_")
    return normalized or "shadow_learning"


def shadow_learning_trace_root(config_dir: Path) -> Path:
    """Return the JSONL output root for shadow-learning sessions."""

    return Path(config_dir) / LOCAL_METADATA_DIR / _SHADOW_TRACE_DIR


def build_shadow_learning_trace_path(
    *,
    config_dir: Path,
    entry_id: str = "",
    collector_pn: str = "",
    timestamp: str,
) -> Path:
    """Return the JSONL output path for one shadow-learning session."""

    root = shadow_learning_trace_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)
    stem = _slugify(str(entry_id or "").strip() or str(collector_pn or "").strip() or "shadow_learning")
    return root / f"{stem}_{timestamp}.jsonl"


@dataclass(frozen=True, slots=True)
class ShadowLearningSessionManifest:
    """One shadow-learning session identity record."""

    session_id: str
    entry_id: str
    collector_pn: str
    collector_cloud_profile_key: str
    collector_cloud_profile_label: str
    collector_cloud_profile_source: str
    collector_cloud_profile_confidence: str
    collector_callback_endpoint: str
    write_response_mode: str = "exception"
    created_at: str = field(default_factory=utc_now_iso)
    schema_version: int = 1

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "session_id": str(self.session_id),
            "entry_id": str(self.entry_id),
            "collector_pn": str(self.collector_pn),
            "collector_cloud_profile_key": str(self.collector_cloud_profile_key),
            "collector_cloud_profile_label": str(self.collector_cloud_profile_label),
            "collector_cloud_profile_source": str(self.collector_cloud_profile_source),
            "collector_cloud_profile_confidence": str(self.collector_cloud_profile_confidence),
            "collector_callback_endpoint": str(self.collector_callback_endpoint),
            "write_response_mode": str(self.write_response_mode),
            "created_at": str(self.created_at),
        }


@dataclass(frozen=True, slots=True)
class ShadowLearningSeed:
    """One normalized shadow-learning backend seed."""

    session_id: str
    entry_id: str
    collector_pn: str
    collector_cloud_profile_key: str
    collector_cloud_profile_label: str
    collector_cloud_profile_source: str
    collector_cloud_profile_confidence: str
    collector_callback_endpoint: str
    effective_metadata_snapshot: dict[str, Any]
    command_responses: dict[str, str]
    register_bank: dict[int, int]
    latest_support_evidence: dict[str, Any] | None = None
    write_response_mode: str = "exception"
    allow_ack_writes: bool = False


@dataclass(frozen=True, slots=True)
class ShadowLearningPreflight:
    """One shadow-learning start preflight result."""

    can_start: bool
    blockers: tuple[str, ...] = ()


def build_shadow_learning_seed(
    *,
    session_id: str,
    entry_id: str,
    collector_pn: str,
    collector_cloud_profile_key: str,
    collector_cloud_profile_label: str,
    collector_cloud_profile_source: str,
    collector_cloud_profile_confidence: str,
    collector_callback_endpoint: str,
    effective_metadata_snapshot: Any,
    raw_capture: dict[str, Any] | None = None,
    command_responses: dict[str, str] | None = None,
    register_bank: dict[int, int] | None = None,
    write_response_mode: str = "exception",
    allow_ack_writes: bool = False,
) -> tuple[ShadowLearningSeed, tuple[str, ...]]:
    """Build one normalized shadow-learning seed and return its preflight blockers."""

    normalized_snapshot = _snapshot_to_dict(effective_metadata_snapshot)
    normalized_responses = _build_command_responses(
        collector_pn=collector_pn,
        collector_callback_endpoint=collector_callback_endpoint,
        raw_capture=raw_capture,
        command_responses=command_responses,
    )
    normalized_register_bank = _build_register_bank(
        raw_capture=raw_capture,
        register_bank=register_bank,
    )
    normalized_mode = "ack" if allow_ack_writes and str(write_response_mode or "").strip().lower() == "ack" else "exception"
    seed = ShadowLearningSeed(
        session_id=str(session_id or "").strip(),
        entry_id=str(entry_id or "").strip(),
        collector_pn=str(collector_pn or "").strip(),
        collector_cloud_profile_key=str(collector_cloud_profile_key or "").strip(),
        collector_cloud_profile_label=str(collector_cloud_profile_label or "").strip(),
        collector_cloud_profile_source=str(collector_cloud_profile_source or "").strip(),
        collector_cloud_profile_confidence=str(collector_cloud_profile_confidence or "").strip(),
        collector_callback_endpoint=str(collector_callback_endpoint or "").strip(),
        effective_metadata_snapshot=normalized_snapshot,
        command_responses=normalized_responses,
        register_bank=normalized_register_bank,
        latest_support_evidence=raw_capture if isinstance(raw_capture, dict) else None,
        write_response_mode=normalized_mode,
        allow_ack_writes=bool(allow_ack_writes),
    )
    preflight = build_shadow_learning_preflight(seed)
    return seed, preflight.blockers


def build_shadow_learning_preflight(seed: ShadowLearningSeed) -> ShadowLearningPreflight:
    """Return one preflight result for a shadow-learning seed."""

    blockers: list[str] = []
    if not seed.collector_pn:
        blockers.append("missing_collector_pn")
    if not (seed.collector_cloud_profile_key or seed.collector_cloud_profile_label):
        blockers.append("missing_collector_cloud_profile")
    if not _snapshot_is_valid(seed.effective_metadata_snapshot):
        blockers.append("missing_effective_metadata_snapshot")
    if not seed.register_bank:
        blockers.append("missing_register_seed")
    return ShadowLearningPreflight(can_start=not blockers, blockers=tuple(blockers))


class InProcessShadowLearningHandler:
    """Shared-listener shadow backend for one SmartESS shadow-learning session."""

    def __init__(
        self,
        *,
        seed: ShadowLearningSeed,
        output_path: Path,
    ) -> None:
        self._seed = seed
        self._output_path = Path(output_path)
        self._output_handle = None
        self._writer: JsonLineWriter | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._running = False
        self._at_responses = dict(seed.command_responses)
        self._register_bank = dict(seed.register_bank)
        self._write_observations: list[ShadowWriteObservation] = []
        self._observation_condition = asyncio.Condition()
        self._read_block_counts: dict[tuple[int, int], int] = {}
        self._read_register_samples: dict[int, list[int]] = {}
        self._read_event_count = 0

    @property
    def running(self) -> bool:
        """Return whether the handler can accept routed collector connections."""

        return self._running

    @property
    def register_bank_snapshot(self) -> dict[int, int]:
        """Return the current synthetic register bank."""

        return dict(self._register_bank)

    @property
    def write_observations(self) -> tuple[ShadowWriteObservation, ...]:
        """Return the captured write observations."""

        return tuple(self._write_observations)

    @property
    def read_map(self) -> dict[str, Any]:
        """Return the aggregated cloud read map observed during this session.

        Addresses are authoritative (exactly what the official cloud polls for
        this device). Values come from the synthetic SEED register bank, not a
        live inverter — a single snapshot, flagged via ``value_source`` so
        downstream labeling never mistakes them for multi-snapshot evidence.
        """

        return {
            "read_blocks": [
                [address, count, occurrences]
                for (address, count), occurrences in sorted(self._read_block_counts.items())
            ],
            "registers": {
                str(register): list(samples)
                for register, samples in sorted(self._read_register_samples.items())
            },
            "read_event_count": self._read_event_count,
            "value_source": "seed_bank",
        }

    def _record_read_observation(self, address: int, count: int, values: list[int]) -> None:
        self._read_event_count += 1
        block_key = (int(address), int(count))
        self._read_block_counts[block_key] = self._read_block_counts.get(block_key, 0) + 1
        for offset, value in enumerate(values):
            samples = self._read_register_samples.setdefault(int(address) + offset, [])
            if value not in samples and len(samples) < _READ_SAMPLE_LIMIT:
                samples.append(int(value))

    def observation_cursor(self) -> int:
        """Return one cursor that points to the current end of observations."""

        return len(self._write_observations)

    def observations_since(self, cursor: int) -> tuple[ShadowWriteObservation, ...]:
        """Return observations captured at or after one cursor."""

        start = max(0, int(cursor))
        if start >= len(self._write_observations):
            return ()
        return tuple(self._write_observations[start:])

    async def wait_for_observations_since(
        self,
        cursor: int,
        *,
        timeout_seconds: float,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Wait up to one bounded timeout for new observations after one cursor."""

        start = max(0, int(cursor))
        if start < len(self._write_observations):
            return tuple(self._write_observations[start:])

        timeout = max(float(timeout_seconds), 0.0)
        if timeout <= 0:
            return ()

        try:
            async with asyncio.timeout(timeout):
                async with self._observation_condition:
                    while start >= len(self._write_observations):
                        await self._observation_condition.wait()
        except TimeoutError:
            return ()
        return tuple(self._write_observations[start:])

    async def start(self) -> None:
        """Open the trace stream and make the handler ready for listener routing."""

        if self._running:
            return
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._output_handle = self._output_path.open("a", encoding="utf-8")
        self._writer = JsonLineWriter(self._output_handle)
        self._running = True
        await self._writer.write(
            {
                "kind": "shadow_session_manifest",
                "timestamp": utc_now_iso(),
                **ShadowLearningSessionManifest(
                    session_id=self._seed.session_id,
                    entry_id=self._seed.entry_id,
                    collector_pn=self._seed.collector_pn,
                    collector_cloud_profile_key=self._seed.collector_cloud_profile_key,
                    collector_cloud_profile_label=self._seed.collector_cloud_profile_label,
                    collector_cloud_profile_source=self._seed.collector_cloud_profile_source,
                    collector_cloud_profile_confidence=self._seed.collector_cloud_profile_confidence,
                    collector_callback_endpoint=self._seed.collector_callback_endpoint,
                    write_response_mode=self._seed.write_response_mode,
                ).to_json_dict(),
            }
        )

    async def stop(self) -> None:
        """Cancel active shadow tasks and close the trace stream."""

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

        writer = self._writer
        self._writer = None
        output_handle = self._output_handle
        self._output_handle = None
        async with self._observation_condition:
            self._observation_condition.notify_all()
        if writer is not None:
            try:
                await writer.write({"kind": "shadow_session_stopped", "timestamp": utc_now_iso()})
            except Exception:
                pass
        if output_handle is not None:
            output_handle.close()

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle one routed collector connection in-process."""

        if not self._running or self._writer is None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            self._tasks.add(current_task)

        peer = writer.get_extra_info("peername") or ("", 0)
        remote = f"{peer[0] or ''}:{peer[1] or 0}"
        await self._append_event(
            "shadow_connect",
            "cloud_to_shadow",
            {"remote": remote},
        )

        buffer = bytearray()
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                await self._append_event(
                    "shadow_chunk",
                    "cloud_to_shadow",
                    {"remote": remote, "chunk_hex": chunk.hex(), "chunk_len": len(chunk)},
                )

                while True:
                    message = self._consume_next_message(buffer)
                    if message is None:
                        break
                    kind, payload = message
                    if kind == "at":
                        response = await self._handle_at_line(payload, remote=remote)
                        if response is not None:
                            writer.write(response)
                            await writer.drain()
                        continue
                    response = await self._handle_frame(payload, remote=remote)
                    if response is not None:
                        writer.write(response)
                        await writer.drain()
        finally:
            await self._append_event(
                "shadow_disconnect",
                "cloud_to_shadow",
                {"remote": remote},
            )
            if current_task is not None:
                self._tasks.discard(current_task)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _consume_next_message(self, buffer: bytearray) -> tuple[str, bytes] | None:
        if not buffer:
            return None
        if buffer.startswith(b"AT+"):
            newline = buffer.find(b"\n")
            if newline < 0:
                return None
            line = bytes(buffer[: newline + 1])
            del buffer[: newline + 1]
            return "at", line
        if len(buffer) < HEADER_SIZE:
            return None
        try:
            header = decode_header(bytes(buffer[:HEADER_SIZE]))
        except Exception as exc:
            buffer.clear()
            raise RuntimeError(f"shadow_frame_decode_error:{type(exc).__name__}:{exc}") from exc
        total_len = header.total_len
        if len(buffer) < total_len:
            return None
        frame = bytes(buffer[:total_len])
        del buffer[:total_len]
        return "frame", frame

    async def _handle_at_line(self, line: bytes, *, remote: str) -> bytes | None:
        try:
            command = parse_at_command(line)
        except Exception as exc:
            await self._append_event(
                "shadow_at_decode_error",
                "cloud_to_shadow",
                {"remote": remote, "payload_hex": line.hex(), "error": f"{type(exc).__name__}:{exc}"},
            )
            return None

        if command.operation == "write":
            self._at_responses[command.command] = command.value
            await self._append_event(
                "shadow_at_write",
                "cloud_to_shadow",
                {"remote": remote, "command": command.command, "value": command.value},
            )
        else:
            await self._append_event(
                "shadow_at_query",
                "cloud_to_shadow",
                {"remote": remote, "command": command.command},
            )

        response_value = self._at_response_value(command.command)
        response = build_at_response(command.command, response_value)
        await self._append_event(
            "shadow_at_response",
            "shadow_to_cloud",
            {"remote": remote, "command": command.command, "value": response_value},
        )
        return response

    async def _handle_frame(self, frame: bytes, *, remote: str) -> bytes | None:
        header = decode_header(frame[:HEADER_SIZE])
        payload = frame[HEADER_SIZE:]
        read_request = decode_read_request(payload)
        if read_request is not None:
            values = self._read_register_values(read_request.address, read_request.count)
            self._record_read_observation(read_request.address, read_request.count, values)
            response_payload = self._build_modbus_read_response(read_request, values)
            await self._append_event(
                "shadow_modbus_read_request",
                "cloud_to_shadow",
                {
                    "remote": remote,
                    "tid": header.tid,
                    "devcode": header.devcode,
                    "devaddr": header.devaddr,
                    "function_code": read_request.function_code,
                    "address": read_request.address,
                    "count": read_request.count,
                    "values": values,
                },
            )
            await self._append_event(
                "shadow_modbus_read_response",
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "tid": header.tid,
                    "devcode": header.devcode,
                    "devaddr": header.devaddr,
                    "function_code": read_request.function_code,
                    "address": read_request.address,
                    "count": read_request.count,
                    "values": values,
                },
            )
            return build_collector_request(
                header.tid,
                response_payload,
                devcode=header.devcode,
                collector_addr=header.devaddr,
                fcode=header.fcode,
            )

        write_request = decode_write_request(payload)
        if write_request is None:
            await self._append_event(
                "shadow_unknown_frame",
                "cloud_to_shadow",
                {"remote": remote, "payload_hex": payload.hex(), "fcode": header.fcode},
            )
            return self._build_exception_frame(header, exception_code=0x01)

        observation = write_observation_from_modbus_request(
            frame=payload,
            devcode=header.devcode,
            devaddr=header.devaddr,
            timestamp=utc_now_iso(),
            source="shadow_learning",
        )
        if observation is not None:
            self._write_observations.append(observation)
            async with self._observation_condition:
                self._observation_condition.notify_all()
            await self._append_event(
                "shadow_modbus_write_observation",
                "cloud_to_shadow",
                observation.to_json_dict(),
            )
        else:
            await self._append_event(
                "shadow_modbus_write_request",
                "cloud_to_shadow",
                {
                    "remote": remote,
                    "payload_hex": payload.hex(),
                    "function_code": write_request.function_code,
                    "address": write_request.address,
                    "values": list(write_request.values),
                },
            )

        if self._seed.write_response_mode == "ack":
            self._apply_write_request(write_request)
            response_payload = self._build_modbus_write_ack_response(write_request)
            await self._append_event(
                "shadow_modbus_write_response",
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "function_code": write_request.function_code,
                    "address": write_request.address,
                    "values": list(write_request.values),
                    "response_mode": "ack",
                },
            )
        else:
            response_payload = self._build_modbus_exception_response(write_request, exception_code=0x01)
            await self._append_event(
                "shadow_modbus_write_response",
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "function_code": write_request.function_code,
                    "address": write_request.address,
                    "values": list(write_request.values),
                    "response_mode": "exception",
                },
            )

        return build_collector_request(
            header.tid,
            response_payload,
            devcode=header.devcode,
            collector_addr=header.devaddr,
            fcode=header.fcode,
        )

    async def _handle_modbus_frame(self, frame: bytes, *, remote: str) -> bytes | None:
        """Handle one bare Modbus RTU frame from the cloud (no collector header).

        This DTU exchanges raw Modbus RTU on the data plane after AT
        registration, so these frames carry no eybond collector wrapper. Reads
        are answered from the synthetic register bank; writes are observed for
        learning and NACK'd with a Modbus exception (the proven exception-mode
        path) so the cloud records no successful write. Nothing is forwarded to
        the physical inverter, and the response is returned as raw Modbus RTU.
        """

        read_request = decode_read_request(frame)
        if read_request is not None:
            values = self._read_register_values(read_request.address, read_request.count)
            self._record_read_observation(read_request.address, read_request.count, values)
            await self._append_event(
                "shadow_modbus_read_request",
                "cloud_to_shadow",
                {
                    "remote": remote,
                    "function_code": read_request.function_code,
                    "address": read_request.address,
                    "count": read_request.count,
                    "values": values,
                },
            )
            response = self._build_modbus_read_response(read_request, values)
            await self._append_event(
                "shadow_modbus_read_response",
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "function_code": read_request.function_code,
                    "address": read_request.address,
                    "count": read_request.count,
                    "values": values,
                },
            )
            return response

        write_request = decode_write_request(frame)
        if write_request is None:
            await self._append_event(
                "shadow_unknown_frame",
                "cloud_to_shadow",
                {"remote": remote, "payload_hex": frame.hex()},
            )
            return self._build_raw_modbus_exception(frame, exception_code=0x01)

        observation = write_observation_from_modbus_request(
            frame=frame,
            devcode=None,
            devaddr=write_request.slave_id,
            timestamp=utc_now_iso(),
            source="shadow_learning",
        )
        if observation is not None:
            self._write_observations.append(observation)
            async with self._observation_condition:
                self._observation_condition.notify_all()
            await self._append_event(
                "shadow_modbus_write_observation",
                "cloud_to_shadow",
                observation.to_json_dict(),
            )
        else:
            await self._append_event(
                "shadow_modbus_write_request",
                "cloud_to_shadow",
                {
                    "remote": remote,
                    "payload_hex": frame.hex(),
                    "function_code": write_request.function_code,
                    "address": write_request.address,
                    "values": list(write_request.values),
                },
            )

        if self._seed.write_response_mode == "ack":
            self._apply_write_request(write_request)
            response = self._build_modbus_write_ack_response(write_request)
            response_mode = "ack"
        else:
            response = self._build_modbus_exception_response(write_request, exception_code=0x01)
            response_mode = "exception"
        await self._append_event(
            "shadow_modbus_write_response",
            "shadow_to_cloud",
            {
                "remote": remote,
                "function_code": write_request.function_code,
                "address": write_request.address,
                "values": list(write_request.values),
                "response_mode": response_mode,
            },
        )
        return response

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

    def should_forward_cloud_at(self, line: bytes) -> bool:
        """Return whether a cloud-issued AT line should be forwarded to the collector.

        The shadow answers AT queries it can satisfy authoritatively (identity
        such as ``QID``, and the ``CLDSRVHOST*`` redirect family it must keep
        pointed at the local proxy). Any *query* it would only answer with an
        empty value — e.g. ``DTUPN`` or ``INTPARA58`` the cloud uses to confirm
        the device during registration — is forwarded to the real collector so
        the cloud receives the device's genuine response. AT writes and
        ``CLDSRVHOST*`` are never forwarded, so the cloud can neither reconfigure
        the DTU nor redirect it off the proxy.
        """

        try:
            command = parse_at_command(line)
        except Exception:
            return False
        if command.operation != "query":
            return False
        normalized = str(command.command or "").strip().upper()
        if normalized.startswith(_CLOUD_REDIRECT_AT_COMMAND_PREFIX):
            return False
        return not self._at_response_value(normalized)

    def _at_response_value(self, command: str) -> str:
        normalized = str(command or "").strip().upper()
        if normalized == "CLDSRVHOST1":
            return self._at_responses.get(normalized, self._seed.collector_callback_endpoint)
        if normalized in self._at_responses:
            return self._at_responses[normalized]
        if normalized == "QID":
            return self._seed.collector_pn
        return self._seed.collector_pn if normalized.endswith("ID") else ""

    def _read_register_values(self, address: int, count: int) -> list[int]:
        values: list[int] = []
        for offset in range(max(0, int(count))):
            values.append(int(self._register_bank.get(int(address) + offset, 0)))
        return values

    def _apply_write_request(self, request) -> None:
        if request.function_code == 0x06 and request.values:
            self._register_bank[request.address] = int(request.values[0])
            return
        for offset, value in enumerate(request.values):
            self._register_bank[request.address + offset] = int(value)

    def _build_modbus_read_response(self, request, values: list[int]) -> bytes:
        payload = bytearray([request.slave_id, request.function_code, len(values) * 2])
        for value in values:
            payload.extend(int(value).to_bytes(2, "big", signed=False))
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def _build_modbus_write_ack_response(self, request) -> bytes:
        payload = bytearray([request.slave_id, request.function_code])
        payload.extend(int(request.address).to_bytes(2, "big", signed=False))
        if request.function_code == 0x06 and request.values:
            payload.extend(int(request.values[0]).to_bytes(2, "big", signed=False))
        else:
            payload.extend(int(len(request.values)).to_bytes(2, "big", signed=False))
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def _build_modbus_exception_response(self, request, *, exception_code: int) -> bytes:
        payload = bytearray([request.slave_id, request.function_code | 0x80, exception_code])
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def _build_raw_modbus_exception(self, frame: bytes, *, exception_code: int) -> bytes:
        """Build a bare Modbus RTU exception for an undecodable raw frame."""

        unit = frame[0] if len(frame) > 0 else 0
        function = frame[1] if len(frame) > 1 else 0
        payload = bytearray([unit, function | 0x80, exception_code])
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return bytes(payload)

    def _build_exception_frame(self, header, *, exception_code: int) -> bytes:
        payload = bytearray([header.devaddr, header.fcode | 0x80, exception_code])
        payload.extend(crc16_modbus(payload).to_bytes(2, "little"))
        return build_collector_request(
            header.tid,
            bytes(payload),
            devcode=header.devcode,
            collector_addr=header.devaddr,
            fcode=header.fcode,
        )


def _snapshot_to_dict(snapshot: Any) -> dict[str, Any]:
    if snapshot is None:
        return {}
    if isinstance(snapshot, dict):
        return dict(snapshot)
    if hasattr(snapshot, "as_dict"):
        try:
            payload = snapshot.as_dict()
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            return dict(payload)
    if hasattr(snapshot, "to_json_dict"):
        try:
            payload = snapshot.to_json_dict()
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            return dict(payload)
    return {}



def _snapshot_is_valid(snapshot: dict[str, Any]) -> bool:
    # Learning's whole purpose is the partial / unidentified tier, where the
    # device binds a base register schema but NO controls profile by design
    # (controls stay locked until learning discovers them — the
    # family-default "writes locked" invariant). The snapshot is only a
    # preflight sanity signal here (the live session consumes register_bank
    # and command_responses, never this snapshot), so gate on the register
    # schema alone: requiring profile_name would block exactly the devices
    # learning exists to serve.
    return bool(str(snapshot.get("register_schema_name") or "").strip())



def _build_command_responses(
    *,
    collector_pn: str,
    collector_callback_endpoint: str,
    raw_capture: dict[str, Any] | None,
    command_responses: dict[str, str] | None,
) -> dict[str, str]:
    responses = dict(build_command_fixture_responses(raw_capture or {}))
    if command_responses:
        responses.update({str(command): str(value) for command, value in command_responses.items()})
    if collector_callback_endpoint:
        responses.setdefault("CLDSRVHOST1", str(collector_callback_endpoint))
    if collector_pn:
        responses.setdefault("QID", str(collector_pn))
    return responses



def _build_register_bank(
    *,
    raw_capture: dict[str, Any] | None,
    register_bank: dict[int, int] | None,
) -> dict[int, int]:
    if register_bank:
        return {int(register): int(value) for register, value in register_bank.items()}
    if not isinstance(raw_capture, dict):
        return {}
    if isinstance(raw_capture.get("fixture_ranges"), list):
        direct_bank = _register_bank_from_ranges(raw_capture.get("fixture_ranges") or [])
        if direct_bank:
            return direct_bank
    best_capture = _best_capture(raw_capture)
    if best_capture is None:
        return {}
    return _register_bank_from_ranges(best_capture.get("fixture_ranges") or [])



def _register_bank_from_ranges(ranges: list[Any]) -> dict[int, int]:
    bank: dict[int, int] = {}
    for range_item in ranges:
        if not isinstance(range_item, dict):
            continue
        start = _maybe_int(range_item.get("start"))
        values = range_item.get("values")
        if start is None or not isinstance(values, list):
            continue
        for offset, value in enumerate(values):
            maybe_value = _maybe_int(value)
            if maybe_value is None:
                continue
            bank[start + offset] = maybe_value
    return bank



def _best_capture(raw_capture: dict[str, Any]) -> dict[str, Any] | None:
    captures = list(raw_capture.get("captures") or [])
    best_capture: dict[str, Any] | None = None
    best_score: tuple[int, int] | None = None
    for capture in captures:
        if not isinstance(capture, dict):
            continue
        fixture_ranges = capture.get("fixture_ranges")
        if not isinstance(fixture_ranges, list) or not fixture_ranges:
            continue
        range_failures = capture.get("range_failures")
        failure_count = len(range_failures) if isinstance(range_failures, list) else 0
        score = (len(fixture_ranges), -failure_count)
        if best_score is None or score > best_score:
            best_score = score
            best_capture = capture
    return best_capture



def _maybe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "InProcessShadowLearningHandler",
    "ShadowLearningPreflight",
    "ShadowLearningSeed",
    "ShadowLearningSessionManifest",
    "build_shadow_learning_preflight",
    "build_shadow_learning_seed",
    "build_shadow_learning_trace_path",
    "shadow_learning_trace_root",
]
