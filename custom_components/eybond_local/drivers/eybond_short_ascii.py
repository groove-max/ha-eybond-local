"""Generic read-only surface for qualified FC4 short-ASCII replies."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.device_catalog_loader import resolve_catalog_surface_binding
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.short_ascii import (
    READ_COMMANDS, ShortAsciiError, ShortAsciiSession,
    parse_md, parse_mp, parse_q1,
)
from .base import InverterDriver
from .catalog_probe import async_probe_ascii_catalog, catalog_model_name
from .read_result import DriverReadMode, DriverReadResult
from .support_marker import DriverSupportMarker
from .short_ascii_estimates import estimated_ac_load_values
from .short_ascii_optional import optional_reads_for


_PARSERS = {"short_ascii.md": parse_md, "short_ascii.mp": parse_mp, "short_ascii.q1": parse_q1}


class EybondShortAsciiDriver(InverterDriver):
    key = "eybond_short_ascii"
    name = "EyeBond Short-ASCII (read-only)"
    signature_timeout = 4.0

    @property
    def probe_timeout(self) -> float:
        return load_compiled_detection_catalog().protocols[self.key].probe_timeout

    @property
    def probe_targets(self) -> tuple[ProbeTarget, ...]:
        return tuple(ProbeTarget(*target) for target in
                     load_compiled_detection_catalog().protocols[self.key].probe_targets)

    @property
    def register_schema_name(self) -> str:
        binding = resolve_catalog_surface_binding(self.key, variant_key="urtu1920_checksum")
        if binding is None:
            raise RuntimeError("short_ascii_catalog_binding_missing")
        return binding.register_schema_name

    @property
    def measurements(self):
        return load_register_schema(self.register_schema_name).measurement_descriptions

    @property
    def binary_sensors(self):
        return load_register_schema(self.register_schema_name).binary_sensor_descriptions

    def serial_is_stable(self, inverter: DetectedInverter | None = None) -> bool:
        return False

    def support_marker(self, *, variant_key: str = "", profile_name: str = ""):
        return DriverSupportMarker(
            key="short_ascii_read_only_family", label="Read-only protocol family",
            read_only=True, verification="capture_qualified",
            summary="Protocol telemetry is qualified; retail model and controls are not identified.",
        )

    async def async_probe_signature(self, transport, target: ProbeTarget) -> bool:
        try:
            parse_q1(await self._session(transport, target).request("Q1"))
        except (ShortAsciiError, ConnectionError, asyncio.TimeoutError):
            return False
        return True

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        try:
            probe = await async_probe_ascii_catalog(
                protocol_key=self.key, session=self._session(transport, target),
                parsers=_PARSERS,
            )
        except (ShortAsciiError, ConnectionError, RuntimeError, asyncio.TimeoutError):
            return None
        if not probe.resolution.resolved:
            return None
        surface = load_compiled_detection_catalog().surfaces[probe.resolution.surface_key]
        # Keep detection-time measurements OUT of persisted identity. Firmware
        # is not a serial, and neither firmware nor collector PN identifies a
        # commercial inverter model.
        return DetectedInverter(
            driver_key=self.key, protocol_family=self.key,
            model_name=catalog_model_name(
                protocol_key=self.key, resolution=probe.resolution, values=probe.values,
            ),
            serial_number="", probe_target=target, variant_key=surface.variant_key,
            register_schema_name=surface.register_schema_name,
            details={
                key: probe.values[key] for key in (
                    "short_ascii_firmware", "protocol_id", "short_ascii_wire_dialect",
                )
            } | {"catalog_detection": probe.as_details()},
        )

    async def async_read_values(
        self, transport, inverter: DetectedInverter, *,
        runtime_state: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        now_monotonic: float | None = None,
    ) -> DriverReadResult:
        started = time.monotonic()
        now = started if now_monotonic is None else float(now_monotonic)
        if not math.isfinite(now):
            raise ValueError("short_ascii_clock_invalid")
        clock = lambda: now + max(0, time.monotonic() - started)
        state = runtime_state if runtime_state is not None else {}
        optional = optional_reads_for(state, transport, inverter, now)
        session = self._session(transport, inverter.probe_target)
        try:
            values = parse_q1(await session.request("Q1"))
            values.pop("short_ascii_q1_length")
            extra, diagnostics = await optional.refresh_one(session, state, clock)
        except BaseException:
            # No previous optional sample may reappear after a failed/cancelled
            # mandatory cycle, reconnect or new inverter binding.
            optional.clear()
            raise
        # FULL absence invalidates expired/failed optional fields in the hub.
        # Q1 and RB have distinct owners: reference V is never pack/BMS V.
        # Estimated AC load needs fresh F ratings; omit when ratings are absent.
        merged = values | extra
        merged.update(estimated_ac_load_values(merged))
        return DriverReadResult(values=merged, mode=DriverReadMode.FULL, diagnostics=diagnostics)

    async def async_capture_support_evidence(self, transport, inverter):
        session = self._session(transport, inverter.probe_target)
        responses, failures = {}, {}
        for command in READ_COMMANDS:
            try:
                responses[command] = (await session.request(command)).hex()
            except (ShortAsciiError, ConnectionError, asyncio.TimeoutError) as exc:
                failures[command] = type(exc).__name__
        return {
            "capture_kind": "short_ascii_read_only", "responses_hex": responses,
            "failures": failures,
        }

    async def async_write_capability(
        self, transport, inverter, capability_key, value, *, runtime_state=None,
    ):
        raise ValueError(f"unsupported_capability:{self.key}:{capability_key}")

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ShortAsciiSession:
        return ShortAsciiSession(transport, target.link_route, target.device_addr)
