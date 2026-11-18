"""Explicit, experimental Q1-only telemetry; not automatic 09C1 detection."""

from __future__ import annotations

import asyncio

from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.q1 import STATUS_REQUEST, Q1Error, Q1Sample, Q1Session, decode_q1
from .base import InverterDriver
from .read_result import DriverReadMode, DriverReadResult
from .support_marker import DriverSupportMarker

_READ_ERRORS = (Q1Error, ConnectionError, asyncio.TimeoutError)
PROTOCOL_ID = "EYBOND_Q1_READ_ONLY"


def _evidence(raw: bytes, sample: Q1Sample) -> dict[str, object]:
    return {
        "response_hex": raw.hex(), "response_length": len(raw),
        "battery_field": sample.battery_text,
        "battery_voltage_unscaled": sample.battery_value,
        "status_flags": sample.flags, "fault_voltage": sample.fault_voltage,
        "field_map_verification": "experimental_replay_only",
    }


class EybondQ1Driver(InverterDriver):
    key = "eybond_q1"
    name = "Q1-compatible (experimental; read-only)"
    register_schema_name = "eybond_q1/base.json"
    auto_detection_supported = False
    probe_timeout = 5.0
    probe_targets = (ProbeTarget(devcode=1, collector_addr=255, device_addr=1),)

    @property
    def measurements(self):
        return load_register_schema(self.register_schema_name).measurement_descriptions

    def serial_is_stable(self, inverter: DetectedInverter | None = None) -> bool:
        return False

    def support_marker(self, *, variant_key="", profile_name=""):
        return DriverSupportMarker(
            key="q1_experimental_read_only", label="Experimental Q1-only telemetry",
            read_only=True, verification="experimental_replay_only",
            summary="Basic Q1 telemetry replayed against the owner capture. "
                    "No model identity, battery scaling, flags, controls or full 09C1 qualification.",
        )

    @staticmethod
    def _session(transport, target):
        return Q1Session(transport, target.link_route)

    async def async_probe(self, transport, target):
        try:
            decode_q1(await self._session(transport, target).read_status())
        except _READ_ERRORS:
            return None
        return DetectedInverter(
            driver_key=self.key, protocol_family=self.key,
            model_name="Q1-compatible (experimental read-only)", serial_number="",
            probe_target=target, variant_key="q1", register_schema_name=self.register_schema_name,
            details={"protocol_id": PROTOCOL_ID, "selection": "manual_read_map",
                     "field_map_verification": "experimental_replay_only"},
        )

    async def async_read_values(self, transport, inverter, *, runtime_state=None,
                                poll_interval=None, now_monotonic=None):
        raw = await self._session(transport, inverter.probe_target).read_status()
        sample = decode_q1(raw)
        return DriverReadResult(
            values={"protocol_id": PROTOCOL_ID, "grid_voltage": sample.input_voltage,
                    "output_voltage": sample.output_voltage, "load_percent": sample.load_percent,
                    "q1_frequency": sample.frequency, "temperature": sample.temperature},
            mode=DriverReadMode.FULL,
            diagnostics={"q1_status_evidence": _evidence(raw, sample)},
        )

    async def async_capture_support_evidence(self, transport, inverter):
        evidence = {
            "capture_kind": "q1_read_only", "request_hex": STATUS_REQUEST.hex(),
            "responses_hex": {}, "failures": {},
        }
        try:
            raw = await self._session(transport, inverter.probe_target).read_status()
            evidence["responses_hex"]["Q1"] = raw.hex()
            evidence["q1_status_evidence"] = _evidence(raw, decode_q1(raw))
        except _READ_ERRORS as exc:
            evidence["failures"]["Q1"] = type(exc).__name__
        return evidence

    async def async_write_capability(self, transport, inverter, capability_key, value, *, runtime_state=None):
        raise NotImplementedError("q1_read_only")
