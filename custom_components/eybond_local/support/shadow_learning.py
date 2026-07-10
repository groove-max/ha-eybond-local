"""Production-safe SmartESS shadow-learning evidence models and helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any

from ..payload.modbus import decode_write_request


DEFAULT_VOLATILE_KEYS = frozenset({"timestamp", "created_at", "observed_at", "updated_at"})


def utc_now_iso() -> str:
    """Return one UTC ISO-8601 timestamp for evidence records."""

    return datetime.now(timezone.utc).isoformat()


def shadow_learning_slug(value: Any) -> str:
    """Slugify one value for shadow-learning artifact keys/paths.

    Shared so learned capability keys (overlay generator) and trace directory
    names (backend) slug identically -- artifacts must cross-reference. Empty
    input falls back to the literal 'shadow_learning'.
    """

    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    return normalized.strip("_") or "shadow_learning"


def compact_json_dumps(payload: Any) -> str:
    """Return deterministic compact JSON for one payload."""

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def compact_jsonl_line(payload: Any) -> str:
    """Return one compact JSONL line."""

    return compact_json_dumps(payload) + "\n"


def parse_jsonl_line(line: str) -> dict[str, Any]:
    """Parse one JSONL line into a dictionary payload."""

    parsed = json.loads(str(line or "").strip())
    if not isinstance(parsed, dict):
        raise ValueError("jsonl_line_not_object")
    return parsed


def deterministic_evidence_hash(
    payload: dict[str, Any],
    *,
    exclude_keys: set[str] | frozenset[str] = DEFAULT_VOLATILE_KEYS,
) -> str:
    """Return deterministic SHA256 hash for one evidence payload."""

    normalized = _normalized_for_hash(payload, exclude_keys=exclude_keys)
    return hashlib.sha256(compact_json_dumps(normalized).encode("utf-8")).hexdigest()


def _normalized_for_hash(
    value: Any,
    *,
    exclude_keys: set[str] | frozenset[str],
) -> Any:
    if isinstance(value, dict):
        output: dict[str, Any] = {}
        for key in sorted(value.keys()):
            if key in exclude_keys:
                continue
            output[str(key)] = _normalized_for_hash(value[key], exclude_keys=exclude_keys)
        return output
    if isinstance(value, tuple):
        return [_normalized_for_hash(item, exclude_keys=exclude_keys) for item in value]
    if isinstance(value, list):
        return [_normalized_for_hash(item, exclude_keys=exclude_keys) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class ShadowSessionManifest:
    """One shadow-learning session identity and metadata record."""

    session_id: str
    collector_pn: str
    cloud_pn: str
    cloud_sn: str
    devcode: int | None
    devaddr: int | None
    write_response_mode: str = "exception"
    created_at: str = field(default_factory=utc_now_iso)
    schema_version: int = 1

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "session_id": str(self.session_id),
            "collector_pn": str(self.collector_pn),
            "cloud_pn": str(self.cloud_pn),
            "cloud_sn": str(self.cloud_sn),
            "devcode": self.devcode,
            "devaddr": self.devaddr,
            "write_response_mode": str(self.write_response_mode),
            "created_at": str(self.created_at),
        }

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> ShadowSessionManifest:
        return cls(
            schema_version=int(payload.get("schema_version", 1)),
            session_id=str(payload.get("session_id") or ""),
            collector_pn=str(payload.get("collector_pn") or ""),
            cloud_pn=str(payload.get("cloud_pn") or ""),
            cloud_sn=str(payload.get("cloud_sn") or ""),
            devcode=_maybe_int(payload.get("devcode")),
            devaddr=_maybe_int(payload.get("devaddr")),
            write_response_mode=str(payload.get("write_response_mode") or "exception"),
            created_at=str(payload.get("created_at") or ""),
        )


@dataclass(frozen=True, slots=True)
class ShadowEventRecord:
    """One compact shadow session event entry."""

    kind: str
    direction: str
    payload: dict[str, Any]
    timestamp: str = field(default_factory=utc_now_iso)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "timestamp": str(self.timestamp),
            "kind": str(self.kind),
            "direction": str(self.direction),
            "payload": dict(self.payload),
        }

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> ShadowEventRecord:
        return cls(
            timestamp=str(payload.get("timestamp") or ""),
            kind=str(payload.get("kind") or ""),
            direction=str(payload.get("direction") or ""),
            payload=dict(payload.get("payload") or {}),
        )


@dataclass(frozen=True, slots=True)
class ShadowWriteObservation:
    """One observed write captured during shadow learning."""

    register: int
    values: tuple[int, ...]
    function_code: int
    devcode: int | None
    devaddr: int | None
    raw_payload_hex: str
    unit: int = 1
    source: str = "shadow_cloud"
    timestamp: str = field(default_factory=utc_now_iso)
    protocol: str = ""
    command: str = ""
    value: str = ""

    def to_json_dict(self) -> dict[str, Any]:
        payload = {
            "timestamp": str(self.timestamp),
            "source": str(self.source),
            "unit": int(self.unit),
            "function_code": int(self.function_code),
            "register": int(self.register),
            "values": [int(value) for value in self.values],
            "devcode": self.devcode,
            "devaddr": self.devaddr,
            "raw_payload_hex": str(self.raw_payload_hex),
        }
        if self.protocol:
            payload["protocol"] = str(self.protocol)
        if self.command:
            payload["command"] = str(self.command)
        if self.value:
            payload["value"] = str(self.value)
        return payload

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> ShadowWriteObservation:
        return cls(
            timestamp=str(payload.get("timestamp") or ""),
            source=str(payload.get("source") or "shadow_cloud"),
            unit=int(payload.get("unit", 1)),
            function_code=int(payload.get("function_code", 16)),
            register=int(payload.get("register", 0)),
            values=tuple(int(value) for value in payload.get("values", [])),
            devcode=_maybe_int(payload.get("devcode")),
            devaddr=_maybe_int(payload.get("devaddr")),
            raw_payload_hex=str(payload.get("raw_payload_hex") or ""),
            protocol=str(payload.get("protocol") or ""),
            command=str(payload.get("command") or ""),
            value=str(payload.get("value") or ""),
        )


@dataclass(frozen=True, slots=True)
class ShadowCorrelationInput:
    """One correlation record between cloud write intent and observed Modbus write."""

    field_id: str
    field_name: str
    requested_value: str
    write_observation: ShadowWriteObservation
    sequence_index: int = 0

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "sequence_index": int(self.sequence_index),
            "field_id": str(self.field_id),
            "field_name": str(self.field_name),
            "requested_value": str(self.requested_value),
            "write_observation": self.write_observation.to_json_dict(),
        }

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> ShadowCorrelationInput:
        return cls(
            sequence_index=int(payload.get("sequence_index", 0)),
            field_id=str(payload.get("field_id") or ""),
            field_name=str(payload.get("field_name") or ""),
            requested_value=str(payload.get("requested_value") or ""),
            write_observation=ShadowWriteObservation.from_json_dict(
                dict(payload.get("write_observation") or {})
            ),
        )


def event_to_jsonl_line(event: ShadowEventRecord) -> str:
    """Serialize one shadow event to compact JSONL."""

    return compact_jsonl_line(event.to_json_dict())


def write_observation_to_jsonl_line(observation: ShadowWriteObservation) -> str:
    """Serialize one shadow write observation to compact JSONL."""

    return compact_jsonl_line(observation.to_json_dict())


def correlation_to_jsonl_line(correlation: ShadowCorrelationInput) -> str:
    """Serialize one correlation input to compact JSONL."""

    return compact_jsonl_line(correlation.to_json_dict())


def write_observation_from_modbus_request(
    *,
    frame: bytes,
    devcode: int | None,
    devaddr: int | None,
    timestamp: str = "",
    source: str = "shadow_cloud",
) -> ShadowWriteObservation | None:
    """Decode one Modbus RTU write request frame into a shadow observation."""

    decoded = decode_write_request(frame)
    if decoded is None:
        return None
    return ShadowWriteObservation(
        timestamp=str(timestamp or utc_now_iso()),
        source=str(source or "shadow_cloud"),
        unit=decoded.slave_id,
        function_code=decoded.function_code,
        register=decoded.address,
        values=decoded.values,
        devcode=devcode,
        devaddr=devaddr,
        raw_payload_hex=frame.hex(),
    )


def coerce_optional_int(value: Any) -> int | None:
    """Best-effort int coercion; returns None for empty / non-numeric values."""

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# Internal alias kept so this module's existing call sites need no churn.
_maybe_int = coerce_optional_int
