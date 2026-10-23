"""Explicit, narrow MarsRock grid-tie read map; not a generic SUN detector.

The captured header differs from the older EyeBond Modbus V1.0 document.
Only four corroborated fields are qualified. Zero-filled unqualified fields
must not become frequency, temperature, battery or control entities.
"""

from __future__ import annotations

import asyncio

from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusError, ModbusSession
from ..payload.register_decode import decode_block
from ..poll_policy import PollPolicy
from .base import InverterDriver
from .read_result import DriverReadMode, DriverReadResult
from .support_marker import DriverSupportMarker
from .support_diagnostics import capture_support_reads

SCHEMA_NAME = "marsrock/grid_tie.json"
_READ_ERRORS = (ModbusError, ConnectionError, asyncio.TimeoutError)
# Read-only investigation, separate from the qualified runtime schema. The
# optional 94/99/107 meanings come from GTIL2-SmartSolar, NOT a confirmed WAL
# firmware map; see ADDING_DRIVERS.md for source and scaling conflicts.
SUPPORT_CANDIDATES = (
    (60, "daily_or_resettable_energy"),
    (94, "temperature_variant"),
    (99, "limiter_power_variant"),
    (107, "output_power_variant"),
    (110, "dc_input_current"),
)
SUPPORT_READ_RANGES = (
    (0, 20, "identity_baseline"),
    (59, 66, "runtime_baseline"),
    *((address, 1, key) for address, key in SUPPORT_CANDIDATES),
    # Small samples only, not a sweep of the entire history space.
    (151, 10, "intraday_energy_sample"),
    (271, 7, "month_daily_energy_sample"),
    (305, 2, "year_monthly_energy_sample"),
    (331, 2, "annual_energy_sample"),
    (451, 5, "first_fault_record_sample"),
)
SUPPORT_READ_BUDGET_SECONDS = 25.0
SUPPORT_READ_SPACING_SECONDS = 1.0


def decode_marsrock_identity(words: list[int]) -> str:
    """Validate this observed header and full ASCII serial, without byte swaps."""
    if len(words) != 20 or any(type(w) is not int or not 0 <= w <= 65535 for w in words):
        raise ModbusError("marsrock_identity_length_or_word")
    if words[:3] != [2, 256, 1]:
        raise ModbusError("marsrock_header_not_qualified")
    raw = b"".join(w.to_bytes(2, "big") for w in words[3:8])
    if not all(48 <= c <= 57 or 65 <= c <= 90 for c in raw):
        raise ModbusError("marsrock_identity_not_ascii")
    return raw.decode("ascii")


def decode_marsrock_live(words: list[int]) -> dict[str, object]:
    """Wire words are BE; the two U32 values have their LOW word first."""
    if len(words) != 66 or any(type(w) is not int or not 0 <= w <= 65535 for w in words):
        raise ModbusError("marsrock_live_length_or_word")
    return decode_block(59, words, load_register_schema(SCHEMA_NAME).spec_set("runtime"))


class MarsRockDriver(InverterDriver):
    key = "marsrock_grid_tie"
    name = "MarsRock grid-tie (manual; read-only)"
    register_schema_name = SCHEMA_NAME
    auto_detection_supported = False
    probe_timeout = 20.0
    poll_policy = PollPolicy(min_auto_interval=10.0, max_auto_interval=90.0)
    # Standard EyeBond Modbus route, not an observed cloud devcode. The capture
    # establishes RTU slave 1; live callback transport still needs owner testing.
    # ModbusSession delegates raw/framed selection to the existing link adapter.
    probe_targets = (ProbeTarget(devcode=1, collector_addr=255, device_addr=1),)

    @property
    def measurements(self):
        return load_register_schema(SCHEMA_NAME).measurement_descriptions

    def support_marker(self, *, variant_key="", profile_name=""):
        return DriverSupportMarker(
            key="marsrock_manual_read", label="MarsRock manual read map",
            read_only=True, verification="capture_qualified",
            summary="Four protocol measurements qualified by capture and document; "
                    "local polling needs field validation. No controls or generic SUN detection.",
        )

    @staticmethod
    def _session(transport, target):
        return ModbusSession(transport, route=target.link_route, slave_id=target.payload_address)

    async def async_probe(self, transport, target):
        session = self._session(transport, target)
        try:
            serial = decode_marsrock_identity(await session.read_holding(0, 20))
            decode_marsrock_live(await session.read_holding(59, 66))
        except _READ_ERRORS:
            return None
        return DetectedInverter(
            driver_key=self.key, protocol_family=self.key,
            model_name="MarsRock grid-tie (manual map)", serial_number=serial,
            probe_target=target, variant_key="grid_tie", register_schema_name=SCHEMA_NAME,
            details={"selection": "manual_read_map", "protocol_id": "MARSROCK_GRID_TIE",
                     "qualified_fields": [d.key for d in self.measurements]},
        )

    async def async_read_values(self, transport, inverter, *, runtime_state=None,
                                poll_interval=None, now_monotonic=None):
        # Single qualified live block: reject failures instead of relabelling
        # old values as a fresh partial update. No settings reads or writes.
        words = await self._session(transport, inverter.probe_target).read_holding(59, 66)
        return DriverReadResult(values=decode_marsrock_live(words), mode=DriverReadMode.FULL)

    async def async_capture_support_evidence(self, transport, inverter):
        evidence = await capture_support_reads(
            self._session(transport, inverter.probe_target), SUPPORT_READ_RANGES,
            timeout_seconds=SUPPORT_READ_BUDGET_SECONDS,
            read_spacing_seconds=SUPPORT_READ_SPACING_SECONDS,
            source="SUN1000/2000 V1.0 and GTIL2-SmartSolar optional RS232 fields",
            purpose="Compare block and point reads; sample unqualified history without writes",
        )
        evidence.update({
            "capture_kind": "marsrock_read_only",
            "driver_key": self.key,
            "slave_id": inverter.probe_target.payload_address,
            "function": 3,
            "fixture_serial_ranges": [{"start": 3, "count": 5}],
            "capture_notes": [
                "Candidate fields and history are unqualified raw evidence, not measurements.",
                "Zero does not distinguish an unsupported field from a valid zero.",
                "Reads are sequential: differing block/point values may reflect a changing input.",
                "Read timestamps are HA request times, not inverter measurement timestamps.",
            ],
            # Keep later point reads separate: they must not overwrite the
            # baseline in a replay fixture or be promoted to runtime values.
            "fixture_ranges": [
                {"start": row["start"], "count": row["count"],
                 "function": 3, "values": list(row["words"])}
                for row in evidence["captured_ranges"]
                if (row["start"], row["count"]) in {(0, 20), (59, 66)}
            ],
        })
        baseline = next((row["words"] for row in evidence["captured_ranges"]
                         if (row["start"], row["count"]) == (59, 66)), None)
        points = {row["start"]: row["words"][0] for row in evidence["captured_ranges"]
                  if row["count"] == 1}
        evidence["candidate_comparisons"] = [
            {
                "register": address, "candidate": key, "qualification": "unverified",
                "block_word": baseline[address - 59] if baseline is not None else None,
                "point_word": points.get(address),
                "comparison": (
                    "not_available" if baseline is None or address not in points else
                    "same_raw_value" if baseline[address - 59] == points[address] else
                    "different_raw_value"
                ),
            }
            for address, key in SUPPORT_CANDIDATES
        ]
        return evidence

    async def async_write_capability(self, transport, inverter, capability_key, value, *, runtime_state=None):
        raise NotImplementedError("marsrock_read_only")
