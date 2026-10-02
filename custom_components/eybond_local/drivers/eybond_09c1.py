"""Read-only 09C1 family; no retail-model inference or borrowed write map."""

from __future__ import annotations

import asyncio
import math
import time

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.device_catalog_loader import resolve_catalog_surface_binding
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.urtu09c1 import (
    PROTOCOL_ID, READ_COMMANDS, Urtu09C1Error, Urtu09C1Session,
    parse_f, parse_pv, parse_q1, parse_qf,
)
from .base import InverterDriver
from .catalog_probe import async_probe_ascii_catalog, catalog_model_name
from .eybond_09c1_pv import pv_reads_for
from .read_result import DriverReadMode, DriverReadResult
from .support_marker import DriverSupportMarker

_PARSERS = {"09c1.q1": parse_q1, "09c1.qf": parse_qf, "09c1.pv": parse_pv, "09c1.f": parse_f}
_READ_ERRORS = (Urtu09C1Error, ConnectionError, asyncio.TimeoutError)


class Eybond09C1Driver(InverterDriver):
    key = "eybond_09c1"
    name = "EyeBond 09C1 (read-only)"
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
        binding = resolve_catalog_surface_binding(self.key, variant_key="urtu09c1")
        if binding is None:
            raise RuntimeError("09c1_catalog_binding_missing")
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
            key="09c1_read_only_family", label="Read-only protocol family",
            read_only=True, verification="capture_qualified",
            summary="09C1 telemetry is qualified; retail model and controls are not identified.",
        )

    async def async_probe_signature(self, transport, target: ProbeTarget) -> bool:
        try:
            parse_q1(await self._session(transport, target).request("Q1"))
        except _READ_ERRORS:
            return False
        return True

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        try:
            probe = await async_probe_ascii_catalog(
                protocol_key=self.key, session=self._session(transport, target), parsers=_PARSERS,
            )
        except (*_READ_ERRORS, RuntimeError):
            return None
        if not probe.resolution.resolved:
            return None
        surface = load_compiled_detection_catalog().surfaces[probe.resolution.surface_key]
        return DetectedInverter(
            driver_key=self.key, protocol_family=self.key,
            model_name=catalog_model_name(
                protocol_key=self.key, resolution=probe.resolution, values=probe.values,
            ),
            serial_number="", probe_target=target, variant_key=surface.variant_key,
            register_schema_name=surface.register_schema_name,
            # Runtime measurements never become persisted identity or defaults.
            details={"protocol_id": PROTOCOL_ID, "catalog_detection": probe.as_details()},
        )

    async def async_read_values(
        self, transport, inverter: DetectedInverter, *, runtime_state=None,
        poll_interval=None, now_monotonic=None,
    ) -> DriverReadResult:
        started = time.monotonic()
        now = started if now_monotonic is None else float(now_monotonic)
        if not math.isfinite(now):
            raise ValueError("09c1_clock_invalid")
        clock = lambda: now + max(0, time.monotonic() - started)
        # Stateless callers retain the old read plan: without device-scoped
        # state there is no safe negative cache or fair channel scheduling.
        optional = (
            pv_reads_for(runtime_state, transport, inverter, now)
            if runtime_state is not None else None
        )
        session = self._session(transport, inverter.probe_target)
        failures, diagnostics = {}, {}
        link_available = True
        try:
            values = parse_q1(await session.request("Q1"))
            # Preserve the legacy every-cycle reads and failure isolation.
            # Their support is never changed by PV1/PV2 negative caching.
            for command, parser in (("QF", parse_qf), ("PV?", parse_pv), ("F", parse_f)):
                try:
                    values.update(parser(await session.request(command)))
                except _READ_ERRORS as exc:
                    failures[command] = type(exc).__name__
                    if isinstance(exc, ConnectionError) or not transport.connected:
                        link_available = False
            if optional is not None:
                interval = float(poll_interval) if poll_interval is not None else 0.0
                ttl = max(60.0, 3 * interval) if math.isfinite(interval) else 60.0
                extra, diagnostics = await optional.refresh_one(
                    session, runtime_state, clock, ttl=ttl, link_available=link_available,
                )
                values.update(extra)
        except BaseException:
            if optional is not None:
                optional.clear()
            raise
        values = {key: value for key, value in values.items() if not key.endswith("_length")}
        values["protocol_id"] = PROTOCOL_ID
        # Legacy groups never carry over; new channels have explicit freshness.
        # Neither PV? nor differently timed PV1/PV2 samples are a combined total.
        return DriverReadResult(
            values=values, mode=DriverReadMode.FULL,
            diagnostics={"urtu09c1_read_failures": failures, **diagnostics},
        )

    async def async_capture_support_evidence(self, transport, inverter):
        session = self._session(transport, inverter.probe_target)
        responses, failures = {}, {}
        for command in READ_COMMANDS:
            try:
                responses[command] = (await session.request(command)).hex()
            except _READ_ERRORS as exc:
                failures[command] = type(exc).__name__
        return {"capture_kind": "09c1_read_only", "responses_hex": responses, "failures": failures}

    async def async_write_capability(self, transport, inverter, capability_key, value, *, runtime_state=None):
        raise ValueError(f"unsupported_capability:{self.key}:{capability_key}")

    @staticmethod
    def _session(transport, target: ProbeTarget) -> Urtu09C1Session:
        return Urtu09C1Session(transport, target.link_route)
