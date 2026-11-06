"""Manually selected experimental VPHB telemetry; no controls or model guess."""

from __future__ import annotations

import asyncio

from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.vphb import PROTOCOL_ID, STATUS_REQUEST, VphbError, VphbSession, VphbStatus
from .base import InverterDriver
from .read_result import DriverReadMode, DriverReadResult
from .support_marker import DriverSupportMarker

_READ_ERRORS = (VphbError, ConnectionError, asyncio.TimeoutError)


class Eybond099EDriver(InverterDriver):
    key = "eybond_099e"
    name = "EyeBond 099E / VPHB (experimental; read-only)"
    register_schema_name = "eybond_099e/base.json"
    auto_detection_supported = False
    probe_timeout = 5.0
    # Owner capture confirms this FC4 route. Cloud devcode 2462 is metadata,
    # NOT permission to substitute 0x099E as the collector tunnel address.
    probe_targets = (ProbeTarget(devcode=1, collector_addr=255, device_addr=1),)

    @property
    def measurements(self):
        return load_register_schema(self.register_schema_name).measurement_descriptions

    def serial_is_stable(self, inverter: DetectedInverter | None = None) -> bool:
        return False

    def support_marker(self, *, variant_key="", profile_name=""):
        return DriverSupportMarker(
            key="vphb_experimental_read_only", label="Experimental VPHB telemetry",
            read_only=True, verification="experimental_replay_only",
            summary="Common telemetry decoded from public V-Series evidence and a Manyi capture; "
                    "field confirmation is pending. No controls or automatic model detection.",
        )

    @staticmethod
    def _session(transport, target):
        return VphbSession(transport, target.link_route)

    async def async_probe(self, transport, target):
        try:
            status = VphbStatus(await self._session(transport, target).read_status())
        except _READ_ERRORS:
            return None
        return DetectedInverter(
            driver_key=self.key, protocol_family=self.key,
            model_name="EyeBond VPHB (experimental read-only)", serial_number="",
            probe_target=target, variant_key="vphb", register_schema_name=self.register_schema_name,
            # Never persist a probe's changing readings/raw bytes as identity.
            details={"protocol_id": PROTOCOL_ID, "selection": "manual_read_map",
                     "observed_frame_length": len(status.raw),
                     "field_map_verification": "experimental_replay_only"},
        )

    async def async_read_values(self, transport, inverter, *, runtime_state=None,
                                poll_interval=None, now_monotonic=None):
        status = VphbStatus(await self._session(transport, inverter.probe_target).read_status())
        # Exactly one status read per cycle. A failed/partial frame raises;
        # there is no stale cache, Q1 fallback, settings read or inferred power.
        return DriverReadResult(
            values=status.values(), mode=DriverReadMode.FULL,
            diagnostics={"vphb_status_evidence": status.evidence()},
        )

    async def async_capture_support_evidence(self, transport, inverter):
        evidence = {
            "capture_kind": "vphb_read_only", "request_hex": STATUS_REQUEST.hex(),
            "responses_hex": {}, "failures": {},
        }
        try:
            raw = await self._session(transport, inverter.probe_target).read_status()
            # Preserve even a rejected variant to allow extending the map later.
            evidence["responses_hex"]["status"] = raw.hex()
            evidence["vphb_status_evidence"] = VphbStatus(raw).evidence()
        except _READ_ERRORS as exc:
            evidence["failures"]["status"] = type(exc).__name__
        return evidence

    async def async_write_capability(self, transport, inverter, capability_key, value, *, runtime_state=None):
        raise NotImplementedError("vphb_read_only")
