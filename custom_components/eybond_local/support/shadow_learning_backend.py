"""In-process SmartESS shadow-learning backend and seed helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
from pathlib import Path
from typing import Any, TextIO

from ..collector.at import build_at_response, parse_at_command
from ..collector.protocol import (
    FC_FORWARD_TO_DEVICE,
    HEADER_SIZE,
    build_collector_request,
    decode_header,
)
from ..const import LOCAL_METADATA_DIR
from ..fixtures.utils import build_command_fixture_responses
from ..payload.modbus import crc16_modbus
from .collector_cloud_proxy import JsonLineWriter
from .shadow_learning import (
    ShadowWriteObservation,
    coerce_optional_int as _maybe_int,
    shadow_learning_slug as _slugify,
    utc_now_iso,
)
from .shadow_learning_protocol import resolve_shadow_learning_protocol_adapter


_SHADOW_TRACE_DIR = "shadow_learning_traces"
_ASCII_INCOMPLETE = object()

# Cloud-issued AT commands that control where the DTU connects. These are always
# answered from the shadow seed (never forwarded to the collector) so the cloud
# cannot redirect the DTU away from the local proxy.
_CLOUD_REDIRECT_AT_COMMAND_PREFIX = "CLDSRVHOST"

# Bounded distinct value samples kept per register in the in-memory read map.
_READ_SAMPLE_LIMIT = 8
_ASCII_FIELD_SAMPLE_LIMIT = 8
_G_ASCII_RUNTIME_FIELD_COMMANDS = {
    "eybond_g_ascii_gdat0_fields": "GPDAT0",
    "eybond_g_ascii_gpv_fields": "GPV",
    "eybond_g_ascii_gbat_fields": "GBAT",
    "eybond_g_ascii_gline_fields": "GLINE",
    "eybond_g_ascii_gop_fields": "GOP",
    "eybond_g_ascii_gchg_fields": "GCHG",
    "eybond_g_ascii_gws_fields": "GWS",
    "valuecloud_gdat0_fields": "GPDAT0",
    "valuecloud_gpv_fields": "GPV",
    "valuecloud_gbat_fields": "GBAT",
}


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
    collector_cloud_family: str = ""
    raw_passthrough_frame_format: str = ""
    protocol_adapter_key: str = "modbus_rtu"
    write_response_mode: str = "exception"
    created_at: str = field(default_factory=utc_now_iso)
    schema_version: int = 1

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "session_id": str(self.session_id),
            "entry_id": str(self.entry_id),
            "collector_pn": str(self.collector_pn),
            "collector_cloud_family": str(self.collector_cloud_family),
            "collector_cloud_profile_key": str(self.collector_cloud_profile_key),
            "collector_cloud_profile_label": str(self.collector_cloud_profile_label),
            "collector_cloud_profile_source": str(self.collector_cloud_profile_source),
            "collector_cloud_profile_confidence": str(self.collector_cloud_profile_confidence),
            "collector_callback_endpoint": str(self.collector_callback_endpoint),
            "raw_passthrough_frame_format": str(self.raw_passthrough_frame_format),
            "protocol_adapter_key": str(self.protocol_adapter_key),
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
    collector_cloud_family: str = ""
    raw_passthrough_frame_format: str = ""
    protocol_adapter_key: str = "modbus_rtu"
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
    collector_cloud_family: str = "",
    raw_passthrough_frame_format: str = "",
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
    normalized_cloud_family = str(collector_cloud_family or "").strip()
    normalized_raw_frame_format = str(raw_passthrough_frame_format or "").strip().lower()
    adapter = resolve_shadow_learning_protocol_adapter(
        normalized_snapshot,
        collector_cloud_family=normalized_cloud_family,
        raw_passthrough_frame_format=normalized_raw_frame_format,
    )
    normalized_mode = "ack" if allow_ack_writes and str(write_response_mode or "").strip().lower() == "ack" else "exception"
    seed = ShadowLearningSeed(
        session_id=str(session_id or "").strip(),
        entry_id=str(entry_id or "").strip(),
        collector_pn=str(collector_pn or "").strip(),
        collector_cloud_family=normalized_cloud_family,
        raw_passthrough_frame_format=normalized_raw_frame_format,
        collector_cloud_profile_key=str(collector_cloud_profile_key or "").strip(),
        collector_cloud_profile_label=str(collector_cloud_profile_label or "").strip(),
        collector_cloud_profile_source=str(collector_cloud_profile_source or "").strip(),
        collector_cloud_profile_confidence=str(collector_cloud_profile_confidence or "").strip(),
        collector_callback_endpoint=str(collector_callback_endpoint or "").strip(),
        effective_metadata_snapshot=normalized_snapshot,
        command_responses=normalized_responses,
        register_bank=normalized_register_bank,
        latest_support_evidence=raw_capture if isinstance(raw_capture, dict) else None,
        protocol_adapter_key=str(adapter.key),
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
    # The collector cloud PROFILE (key/label) is NOT a blocker: it is
    # session-manifest metadata only. The proxy redirect uses the cloud FAMILY
    # endpoint and the learn plan comes from the cloud device settings -- neither
    # needs the per-collector protocol asset. That asset is firmware-dependent
    # (fw 8.50.12.3 reports a "0000" placeholder, 8.50.18.3 reports nothing), so
    # gating the scan on it blocked perfectly learnable devices arbitrarily.
    if not _snapshot_is_valid(seed.effective_metadata_snapshot):
        blockers.append("missing_effective_metadata_snapshot")
    adapter = resolve_shadow_learning_protocol_adapter(
        seed.effective_metadata_snapshot,
        collector_cloud_family=seed.collector_cloud_family,
        raw_passthrough_frame_format=seed.raw_passthrough_frame_format,
    )
    if not adapter.supported:
        blockers.append(adapter.blocker)
    if adapter.key == "eybond_g_ascii":
        if not _has_g_ascii_command_seed(seed.command_responses):
            blockers.append("missing_command_seed")
    elif not seed.register_bank:
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
        self._protocol_adapter = resolve_shadow_learning_protocol_adapter(
            seed.effective_metadata_snapshot,
            collector_cloud_family=seed.collector_cloud_family,
            raw_passthrough_frame_format=seed.raw_passthrough_frame_format,
        )
        self._write_observations: list[ShadowWriteObservation] = []
        self._observation_condition = asyncio.Condition()
        self._read_block_counts: dict[tuple[int, int], int] = {}
        self._read_register_samples: dict[int, list[int]] = {}
        self._ascii_command_counts: dict[str, int] = {}
        self._ascii_field_samples: dict[str, list[str]] = {}
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

        payload = {
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
        if self._ascii_command_counts:
            payload["ascii_commands"] = [
                [command, occurrences]
                for command, occurrences in sorted(self._ascii_command_counts.items())
            ]
            payload["ascii_fields"] = {
                command: list(samples)
                for command, samples in sorted(self._ascii_field_samples.items())
            }
            payload["value_source"] = "seed_command_responses"
        return payload

    def _record_read_observation(self, address: int, count: int, values: list[int]) -> None:
        self._read_event_count += 1
        block_key = (int(address), int(count))
        self._read_block_counts[block_key] = self._read_block_counts.get(block_key, 0) + 1
        for offset, value in enumerate(values):
            samples = self._read_register_samples.setdefault(int(address) + offset, [])
            if value not in samples and len(samples) < _READ_SAMPLE_LIMIT:
                samples.append(int(value))

    def _record_ascii_read_observation(self, command: str, response_payload: bytes) -> None:
        self._read_event_count += 1
        normalized = str(command or "").strip().upper()
        if not normalized:
            return
        self._ascii_command_counts[normalized] = self._ascii_command_counts.get(normalized, 0) + 1
        sample = _normalize_ascii_response_sample(response_payload)
        if not sample:
            return
        samples = self._ascii_field_samples.setdefault(normalized, [])
        if sample not in samples and len(samples) < _ASCII_FIELD_SAMPLE_LIMIT:
            samples.append(sample)

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

        async def _wait_for_new_observation() -> None:
            async with self._observation_condition:
                while start >= len(self._write_observations):
                    await self._observation_condition.wait()

        # asyncio.wait_for (3.8+) instead of asyncio.timeout (3.11+): the project
        # ships a deliberate Python 3.10 fallback elsewhere, so the backend must
        # not hard-require 3.11 in the middle of a live learning sweep.
        try:
            await asyncio.wait_for(_wait_for_new_observation(), timeout=timeout)
        except (asyncio.TimeoutError, TimeoutError):
            return ()
        return tuple(self._write_observations[start:])

    async def start(self) -> None:
        """Open the trace stream and make the handler ready for listener routing."""

        if self._running:
            return
        self._output_handle = await asyncio.to_thread(
            _open_append_text_file,
            self._output_path,
        )
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
                    collector_cloud_family=self._seed.collector_cloud_family,
                    collector_cloud_profile_key=self._seed.collector_cloud_profile_key,
                    collector_cloud_profile_label=self._seed.collector_cloud_profile_label,
                    collector_cloud_profile_source=self._seed.collector_cloud_profile_source,
                    collector_cloud_profile_confidence=self._seed.collector_cloud_profile_confidence,
                    collector_callback_endpoint=self._seed.collector_callback_endpoint,
                    raw_passthrough_frame_format=self._seed.raw_passthrough_frame_format,
                    protocol_adapter_key=self._protocol_adapter.key,
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
            await asyncio.to_thread(output_handle.close)

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
                    if kind == "ascii":
                        response = await self._handle_ascii_frame(payload, remote=remote)
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
        ascii_frame = _consume_g_ascii_frame(buffer)
        if ascii_frame is _ASCII_INCOMPLETE:
            frame = _consume_eybond_frame_if_complete(buffer)
            if frame is not None:
                return "frame", frame
            return None
        if ascii_frame is not None:
            return "ascii", ascii_frame
        if len(buffer) < HEADER_SIZE:
            return None
        try:
            header = decode_header(bytes(buffer[:HEADER_SIZE]))
        except Exception as exc:
            buffer.clear()
            raise RuntimeError(f"shadow_frame_decode_error:{type(exc).__name__}:{exc}") from exc
        total_len = header.total_len
        if total_len < HEADER_SIZE:
            # A header claiming a body shorter than the header itself (wire_len
            # < 2) would otherwise yield a 6-7 byte "frame" that crashes
            # decode_header downstream with a struct.error and desyncs the
            # stream. Drop the buffer and fail cleanly, like the proxy does.
            buffer.clear()
            raise RuntimeError(f"shadow_frame_invalid_length:{total_len}")
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
        read_request = self._protocol_adapter.decode_read_request(payload)
        if read_request is not None:
            values = self._read_register_values(read_request.address, read_request.count)
            response_payload = self._protocol_adapter.build_seeded_read_response(
                read_request,
                values,
                self._at_responses,
            )
            if read_request.command:
                self._record_ascii_read_observation(read_request.command, response_payload)
            else:
                self._record_read_observation(read_request.address, read_request.count, values)
            await self._append_event(
                self._protocol_event_kind("read_request"),
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
                    "command": read_request.command,
                },
            )
            await self._append_event(
                self._protocol_event_kind("read_response"),
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
                    "command": read_request.command,
                    "response_ascii": response_payload.decode("ascii", errors="replace").strip(),
                },
            )
            return build_collector_request(
                header.tid,
                response_payload,
                devcode=header.devcode,
                collector_addr=header.devaddr,
                fcode=header.fcode,
            )

        write_request = self._protocol_adapter.decode_write_request(payload)
        if write_request is None:
            await self._append_event(
                "shadow_unknown_frame",
                "cloud_to_shadow",
                {"remote": remote, "payload_hex": payload.hex(), "fcode": header.fcode},
            )
            return self._build_exception_frame(header, payload, exception_code=0x01)

        observation = self._protocol_adapter.write_observation(
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
                self._protocol_event_kind("write_observation"),
                "cloud_to_shadow",
                observation.to_json_dict(),
            )
        else:
            await self._append_event(
                self._protocol_event_kind("write_request"),
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
            self._protocol_adapter.apply_write_to_register_bank(
                write_request, self._register_bank
            )
            response_payload = self._protocol_adapter.build_write_ack_response(
                write_request
            )
            await self._append_event(
                self._protocol_event_kind("write_response"),
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "function_code": write_request.function_code,
                    "address": write_request.address,
                    "values": list(write_request.values),
                    "command": write_request.command,
                    "value": write_request.value,
                    "response_mode": "ack",
                },
            )
        else:
            response_payload = self._protocol_adapter.build_write_exception_response(
                write_request, exception_code=0x01
            )
            await self._append_event(
                self._protocol_event_kind("write_response"),
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "function_code": write_request.function_code,
                    "address": write_request.address,
                    "values": list(write_request.values),
                    "command": write_request.command,
                    "value": write_request.value,
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

        read_request = self._protocol_adapter.decode_read_request(frame)
        if read_request is not None:
            values = self._read_register_values(read_request.address, read_request.count)
            self._record_read_observation(read_request.address, read_request.count, values)
            await self._append_event(
                self._protocol_event_kind("read_request"),
                "cloud_to_shadow",
                {
                    "remote": remote,
                    "function_code": read_request.function_code,
                    "address": read_request.address,
                    "count": read_request.count,
                    "values": values,
                },
            )
            response = self._protocol_adapter.build_seeded_read_response(
                read_request,
                values,
                self._at_responses,
            )
            await self._append_event(
                self._protocol_event_kind("read_response"),
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

        write_request = self._protocol_adapter.decode_write_request(frame)
        if write_request is None:
            await self._append_event(
                "shadow_unknown_frame",
                "cloud_to_shadow",
                {"remote": remote, "payload_hex": frame.hex()},
            )
            return self._protocol_adapter.build_raw_exception(frame, exception_code=0x01)

        observation = self._protocol_adapter.write_observation(
            frame=frame,
            devcode=None,
            devaddr=write_request.unit,
            timestamp=utc_now_iso(),
            source="shadow_learning",
        )
        if observation is not None:
            self._write_observations.append(observation)
            async with self._observation_condition:
                self._observation_condition.notify_all()
            await self._append_event(
                self._protocol_event_kind("write_observation"),
                "cloud_to_shadow",
                observation.to_json_dict(),
            )
        else:
            await self._append_event(
                self._protocol_event_kind("write_request"),
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
            self._protocol_adapter.apply_write_to_register_bank(
                write_request, self._register_bank
            )
            response = self._protocol_adapter.build_write_ack_response(write_request)
            response_mode = "ack"
        else:
            response = self._protocol_adapter.build_write_exception_response(
                write_request, exception_code=0x01
            )
            response_mode = "exception"
        await self._append_event(
            self._protocol_event_kind("write_response"),
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

    async def _handle_ascii_frame(self, frame: bytes, *, remote: str) -> bytes | None:
        """Handle one bare G-ASCII command from the cloud.

        Reads are answered from command-response seed data. Unknown commands and
        writes are NAK'd locally and never forwarded to the physical inverter.
        """

        read_request = self._protocol_adapter.decode_read_request(frame)
        if read_request is not None:
            response = self._protocol_adapter.build_seeded_read_response(
                read_request,
                [],
                self._at_responses,
            )
            self._record_ascii_read_observation(read_request.command, response)
            await self._append_event(
                self._protocol_event_kind("read_request"),
                "cloud_to_shadow",
                {
                    "remote": remote,
                    "command": read_request.command,
                    "payload_hex": frame.hex(),
                },
            )
            await self._append_event(
                self._protocol_event_kind("read_response"),
                "shadow_to_cloud",
                {
                    "remote": remote,
                    "command": read_request.command,
                    "response_ascii": response.decode("ascii", errors="replace").strip(),
                },
            )
            return response

        write_request = self._protocol_adapter.decode_write_request(frame)
        if write_request is None:
            await self._append_event(
                "shadow_unknown_frame",
                "cloud_to_shadow",
                {"remote": remote, "payload_hex": frame.hex()},
            )
            return self._protocol_adapter.build_raw_exception(frame, exception_code=0x01)

        observation = self._protocol_adapter.write_observation(
            frame=frame,
            devcode=None,
            devaddr=None,
            timestamp=utc_now_iso(),
            source="shadow_learning",
        )
        if observation is not None:
            self._write_observations.append(observation)
            async with self._observation_condition:
                self._observation_condition.notify_all()
            await self._append_event(
                self._protocol_event_kind("write_observation"),
                "cloud_to_shadow",
                observation.to_json_dict(),
            )

        response = self._protocol_adapter.build_write_exception_response(
            write_request,
            exception_code=0x01,
        )
        await self._append_event(
            self._protocol_event_kind("write_response"),
            "shadow_to_cloud",
            {
                "remote": remote,
                "command": write_request.command,
                "value": write_request.value,
                "response_mode": "exception",
            },
        )
        return response

    async def _append_event(self, kind: str, direction: str, payload: dict[str, Any]) -> None:
        writer = self._writer
        if writer is None:
            return
        event_payload = dict(payload)
        event_payload.setdefault("protocol_adapter_key", self._protocol_adapter.key)
        await writer.write(
            {
                "kind": kind,
                "timestamp": utc_now_iso(),
                "direction": direction,
                "payload": event_payload,
            }
        )

    def _protocol_event_kind(self, suffix: str) -> str:
        """Return a trace event kind for the active protocol adapter.

        Modbus keeps the historical ``shadow_modbus_*`` vocabulary so existing
        support-package parsers and fixtures remain valid. Non-Modbus adapters
        use the protocol-neutral prefix.
        """

        normalized_suffix = str(suffix or "").strip()
        if self._protocol_adapter.key == "modbus_rtu":
            return f"shadow_modbus_{normalized_suffix}"
        return f"shadow_protocol_{normalized_suffix}"

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

    def _build_exception_frame(self, header, inner_payload: bytes, *, exception_code: int) -> bytes:
        if self._protocol_adapter.key != "modbus_rtu":
            return build_collector_request(
                header.tid,
                self._protocol_adapter.build_raw_exception(inner_payload, exception_code=exception_code),
                devcode=header.devcode,
                collector_addr=header.devaddr,
                fcode=header.fcode,
            )
        if header.fcode == FC_FORWARD_TO_DEVICE and len(inner_payload) >= 2:
            # A modbus-to-device wrapper carries a real Modbus PDU: the NACK's
            # inner bytes must echo the INNER slave id and function code
            # (inner_payload[0]/[1]), not the eybond wrapper's devaddr/fcode
            # (FC_FORWARD_TO_DEVICE is not a Modbus function) -- otherwise the
            # cloud sees a function-code-mismatched response and treats it as a
            # protocol error instead of a clean rejection.
            unit = inner_payload[0]
            function = inner_payload[1]
        else:
            # Non-modbus wrapper (set-collector, query, ...): there is no inner
            # Modbus function, so keep the wrapper-based generic reject.
            unit = header.devaddr
            function = header.fcode
        modbus = bytearray([unit, function | 0x80, exception_code])
        modbus.extend(crc16_modbus(modbus).to_bytes(2, "little"))
        return build_collector_request(
            header.tid,
            bytes(modbus),
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
    responses.update(_build_g_ascii_command_responses(raw_capture or {}))
    if command_responses:
        responses.update({str(command): str(value) for command, value in command_responses.items()})
    if collector_callback_endpoint:
        responses.setdefault("CLDSRVHOST1", str(collector_callback_endpoint))
    if collector_pn:
        responses.setdefault("QID", str(collector_pn))
    return responses


def _build_g_ascii_command_responses(raw_capture: dict[str, Any]) -> dict[str, str]:
    responses: dict[str, str] = {}
    for key, value in _walk_mapping_values(raw_capture):
        normalized_key = str(key or "").strip().lower()
        command = _G_ASCII_RUNTIME_FIELD_COMMANDS.get(normalized_key)
        if command and value not in (None, ""):
            responses.setdefault(command, str(value).strip())
    return responses


def _walk_mapping_values(value: Any) -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            found.append((str(key), item))
            found.extend(_walk_mapping_values(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_walk_mapping_values(item))
    return found


def _has_g_ascii_command_seed(command_responses: dict[str, str]) -> bool:
    for command, value in (command_responses or {}).items():
        normalized = str(command or "").strip().upper()
        if not value:
            continue
        if normalized.startswith(("GPDAT", "GPV", "GBAT", "GLINE", "GOP", "GCHG", "GWS")):
            return True
    return False


def _normalize_ascii_response_sample(response_payload: bytes) -> str:
    try:
        text = response_payload.decode("ascii", errors="replace").strip()
    except Exception:
        return ""
    if text.startswith("("):
        text = text[1:]
    return text



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
        # The learning bank models the HOLDING register space only.  Input
        # registers (function 4) share addresses with holding registers but
        # are a separate space; mixing them here would corrupt inference.
        # Fail closed: a range with an unparseable function is not provably
        # holding data either.
        if _maybe_int(range_item.get("function", 3)) != 3:
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


def _consume_g_ascii_frame(buffer: bytearray) -> bytes | object | None:
    if not buffer:
        return None
    first = buffer[0]
    if not (first == 0x23 or first == 0x28 or 0x41 <= first <= 0x5A):
        return None
    carriage = buffer.find(b"\r")
    if carriage < 0:
        if len(buffer) < 64:
            return _ASCII_INCOMPLETE
        return None
    if carriage > 128:
        return None
    frame = bytes(buffer[: carriage + 1])
    body = frame[:-1]
    if not body or body.startswith(b"AT+"):
        return None
    if any(byte < 0x20 or byte > 0x7E for byte in body):
        return None
    del buffer[: carriage + 1]
    return frame


def _consume_eybond_frame_if_complete(buffer: bytearray) -> bytes | None:
    if len(buffer) < HEADER_SIZE:
        return None
    try:
        header = decode_header(bytes(buffer[:HEADER_SIZE]))
    except Exception:
        return None
    total_len = header.total_len
    if total_len < HEADER_SIZE or len(buffer) < total_len:
        return None
    frame = bytes(buffer[:total_len])
    del buffer[:total_len]
    return frame


def _open_append_text_file(path: Path) -> TextIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("a", encoding="utf-8")




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
