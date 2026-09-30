"""Read-only 09C1 family; no retail-model inference or borrowed write map."""

from __future__ import annotations

import asyncio

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
        session = self._session(transport, inverter.probe_target)
        values = parse_q1(await session.request("Q1"))
        failures = {}
        for command, parser in (("QF", parse_qf), ("PV?", parse_pv), ("F", parse_f)):
            try:
                values.update(parser(await session.request(command)))
            except _READ_ERRORS as exc:
                failures[command] = type(exc).__name__
        values = {key: value for key, value in values.items() if not key.endswith("_length")}
        values["protocol_id"] = PROTOCOL_ID
        # No cross-cycle cache: failed optional groups disappear, while Q1
        # remains available. In particular, never substitute input Hz for QF.
        return DriverReadResult(
            values=values, mode=DriverReadMode.FULL,
            diagnostics={"urtu09c1_read_failures": failures},
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
