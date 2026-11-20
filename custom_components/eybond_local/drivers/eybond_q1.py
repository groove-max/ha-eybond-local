"""Explicit, experimental Q1-only telemetry; not automatic 09C1 detection."""

from __future__ import annotations

import asyncio

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.explicit_selection import explicitly_selected_catalog_entry
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.q1 import STATUS_REQUEST, Q1Error, Q1Sample, Q1Session, decode_q1
from .base import InverterDriver
from .catalog_probe import async_probe_ascii_catalog, catalog_model_name
from .read_result import DriverReadMode, DriverReadResult
from .support_marker import DriverSupportMarker

_READ_ERRORS = (Q1Error, ConnectionError, asyncio.TimeoutError)
PROTOCOL_ID = "EYBOND_Q1_READ_ONLY"
_VS_VARIANT = "vs10048amn_24_cell"
_VS_SCHEMA = "eybond_q1/models/vs10048amn_24_cell.json"
_VS_SERIES_CELLS = 24


def _probe_shape(raw: bytes) -> dict[str, int]:
    return {"battery_decimals": decode_q1(raw).battery_decimals}


def _pack_voltage(inverter: DetectedInverter, sample: Q1Sample) -> float | None:
    if (inverter.variant_key != _VS_VARIANT or inverter.register_schema_name != _VS_SCHEMA
            or sample.battery_decimals != 2):
        return None
    return round(sample.battery_value * _VS_SERIES_CELLS, 2)


def _evidence(raw: bytes, sample: Q1Sample, pack_voltage: float | None) -> dict[str, object]:
    return {
        "response_hex": raw.hex(), "response_length": len(raw),
        "battery_field": sample.battery_text,
        "battery_voltage_unscaled": sample.battery_value,
        "status_flags": sample.flags, "fault_voltage": sample.fault_voltage,
        "field_map_verification": "experimental_replay_only",
        **({"battery_voltage_scale": _VS_SERIES_CELLS, "battery_voltage_basis": "selected_model_lcd_comparison"}
           if pack_voltage is not None else {}),
    }


class EybondQ1Driver(InverterDriver):
    key = "eybond_q1"
    name = "Q1-compatible (experimental; read-only)"
    register_schema_name = "eybond_q1/base.json"
    auto_detection_supported = False

    @property
    def probe_timeout(self) -> float:
        return load_compiled_detection_catalog().protocols[self.key].probe_timeout

    @property
    def probe_targets(self) -> tuple[ProbeTarget, ...]:
        return tuple(ProbeTarget(*target) for target in
                     load_compiled_detection_catalog().protocols[self.key].probe_targets)

    @property
    def measurements(self):
        return load_register_schema(self.register_schema_name).measurement_descriptions

    def serial_is_stable(self, inverter: DetectedInverter | None = None) -> bool:
        return False

    def support_marker(self, *, variant_key="", profile_name=""):
        if variant_key == _VS_VARIANT:
            return DriverSupportMarker(
                key="q1_vs10048amn_24_cell_read_only", label="VS10048AMN 24-cell Q1 telemetry",
                read_only=True, verification="owner_lcd_comparison",
                summary="Owner-selected read map; battery voltage compared with the LCD on a 48 V, "
                        "24-cell bank. No automatic model identification, status flags or controls.",
            )
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
            probe = await async_probe_ascii_catalog(
                protocol_key=self.key, session=self._session(transport, target), parsers={"q1.status": _probe_shape},
            )
        except (*_READ_ERRORS, RuntimeError):
            return None
        if selected := explicitly_selected_catalog_entry():
            if not probe.resolution.resolved or selected not in probe.resolution.candidate_keys:
                return None
            surface = load_compiled_detection_catalog().surfaces[probe.resolution.surface_key]
            return DetectedInverter(
                driver_key=self.key, protocol_family=self.key, serial_number="", probe_target=target,
                model_name=catalog_model_name(protocol_key=self.key, resolution=probe.resolution, values=probe.values),
                variant_key=surface.variant_key, register_schema_name=surface.register_schema_name,
                details={"protocol_id": PROTOCOL_ID, "selection": "explicit_catalog_map",
                         "catalog_detection": probe.as_details()},
            )
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
        voltage = _pack_voltage(inverter, sample)
        return DriverReadResult(
            values={"protocol_id": PROTOCOL_ID, "grid_voltage": sample.input_voltage,
                    "output_voltage": sample.output_voltage, "load_percent": sample.load_percent,
                    "q1_frequency": sample.frequency, "temperature": sample.temperature,
                    **({"battery_voltage": voltage} if voltage is not None else {})},
            mode=DriverReadMode.FULL,
            diagnostics={"q1_status_evidence": _evidence(raw, sample, voltage)},
        )

    async def async_capture_support_evidence(self, transport, inverter):
        evidence = {
            "capture_kind": "q1_read_only", "request_hex": STATUS_REQUEST.hex(),
            "responses_hex": {}, "failures": {},
        }
        try:
            raw = await self._session(transport, inverter.probe_target).read_status()
            evidence["responses_hex"]["Q1"] = raw.hex()
            sample = decode_q1(raw)
            evidence["q1_status_evidence"] = _evidence(raw, sample, _pack_voltage(inverter, sample))
        except _READ_ERRORS as exc:
            evidence["failures"]["Q1"] = type(exc).__name__
        return evidence

    async def async_write_capability(self, transport, inverter, capability_key, value, *, runtime_state=None):
        raise NotImplementedError("q1_read_only")
