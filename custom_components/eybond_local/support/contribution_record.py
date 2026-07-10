"""Build an anonymized device contribution record from a learning session.

A contribution record is the small JSON a user submits (via the GitHub
device-contribution template) so a maintainer can promote their device into the
built-in catalog. It is derived entirely from artifacts already produced during
a learning session — fingerprint, read map, correlation verdicts, and the
generated overlay — and is scrubbed of every identifier by construction, not by
user diligence:

- no pn / sn / serial ASCII / IP / account / collector_pn;
- the serial register words (186-197 on the SMG/anenji layouts) are dropped
  from the read map and any embedded payload;
- the immutable IDENTITY registers (model_code@171, layout_code@184,
  rated_power@643) are preserved — they are the whole point of the record.
"""

from __future__ import annotations

from typing import Any


RECORD_VERSION = 1

# Serial ASCII lives here on the SMG/anenji layouts; never leaves the device.
_SERIAL_REGISTER_RANGE = range(186, 198)

# Keys whose values are device/account identifiers and must never be emitted.
_IDENTIFIER_KEY_FRAGMENTS = (
    "pn",
    "sn",
    "serial",
    "remote_ip",
    "ip",
    "uid",
    "usr",
    "token",
    "secret",
    "session_id",
)


def build_contribution_record(
    *,
    fingerprint: dict[str, Any] | None,
    manifest: dict[str, Any] | None,
    proposed_profile: dict[str, Any] | None = None,
    proposed_schema: dict[str, Any] | None = None,
    collector_version: str = "",
    integration_version: str = "",
) -> dict[str, Any]:
    """Assemble one anonymized contribution record."""

    manifest = manifest if isinstance(manifest, dict) else {}
    fingerprint = fingerprint if isinstance(fingerprint, dict) else {}

    read_map = _scrub_read_map(manifest.get("read_map"))
    record: dict[str, Any] = {
        "record_version": RECORD_VERSION,
        "fingerprint": _clean_fingerprint(fingerprint),
        "register_coverage": [list(block[:2]) for block in read_map.get("read_blocks", [])],
        "read_map_registers": read_map.get("registers", {}),
        "ascii_command_coverage": read_map.get("ascii_commands", []),
        "read_map_ascii_fields": read_map.get("ascii_fields", {}),
        "label_evidence": _label_evidence(manifest),
        "cloud_hints": _cloud_hints(fingerprint, manifest),
        "capture_meta": {
            "collector_fw": str(collector_version or ""),
            "integration_version": str(integration_version or ""),
            "value_source": str((manifest.get("read_map") or {}).get("value_source") or ""),
        },
    }
    if proposed_profile is not None:
        record["proposed_profile"] = _scrub(proposed_profile)
    if proposed_schema is not None:
        record["proposed_schema"] = _scrub(proposed_schema)
    return record


def record_contains_identifier(record: Any, *, _depth: int = 0) -> bool:
    """Defensive audit: True if any identifier key survived scrubbing.

    The generator never emits identifiers; this is a belt-and-braces check the
    vetting tool (and tests) run before a record is ever shared.
    """

    if _depth > 12:
        return False
    if isinstance(record, dict):
        for key, value in record.items():
            if _is_identifier_key(str(key)):
                if value not in (None, "", {}, []):
                    return True
            if record_contains_identifier(value, _depth=_depth + 1):
                return True
    elif isinstance(record, list):
        return any(record_contains_identifier(item, _depth=_depth + 1) for item in record)
    return False


def _clean_fingerprint(fingerprint: dict[str, Any]) -> dict[str, Any]:
    return {
        "layout_code": fingerprint.get("layout_code"),
        "model_code": fingerprint.get("model_code"),
        "rated_power": fingerprint.get("rated_power"),
        "layout_key": fingerprint.get("layout_key"),
        "tier": fingerprint.get("tier"),
        "entry_key": fingerprint.get("entry_key"),
    }


def _label_evidence(manifest: dict[str, Any]) -> dict[str, Any]:
    read_bindings = manifest.get("read_bindings")
    read_enum_bindings = manifest.get("read_enum_bindings")
    learned_read = manifest.get("learned_read_sensors")
    return {
        "numeric": _scrub(read_bindings) if isinstance(read_bindings, dict) else {},
        "enum": _scrub(read_enum_bindings) if isinstance(read_enum_bindings, dict) else {},
        "materialized": _scrub(learned_read) if isinstance(learned_read, list) else [],
    }


def _cloud_hints(fingerprint: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    devcodes = fingerprint.get("devcodes")
    read_bindings = manifest.get("read_bindings")
    label_count = 0
    if isinstance(read_bindings, dict):
        label_count = len(read_bindings.get("bindings", []) or [])
    return {
        "devcodes": list(devcodes) if isinstance(devcodes, (list, tuple)) else [],
        "label_count": label_count,
    }


def _scrub_read_map(read_map: Any) -> dict[str, Any]:
    if not isinstance(read_map, dict):
        return {"read_blocks": [], "registers": {}}
    registers = {}
    for key, samples in (read_map.get("registers") or {}).items():
        try:
            register = int(key)
        except (TypeError, ValueError):
            continue
        if register in _SERIAL_REGISTER_RANGE:
            continue
        registers[str(register)] = samples
    return {
        "read_blocks": list(read_map.get("read_blocks") or []),
        "registers": registers,
        "ascii_commands": _scrub(read_map.get("ascii_commands") or []),
        "ascii_fields": _scrub(read_map.get("ascii_fields") or {}),
    }


def _scrub(payload: Any, *, _depth: int = 0) -> Any:
    """Recursively drop identifier keys and serial register entries."""

    if _depth > 14:
        return payload
    if isinstance(payload, dict):
        cleaned: dict[str, Any] = {}
        for key, value in payload.items():
            key_str = str(key)
            if _is_identifier_key(key_str):
                continue
            if key_str == "registers" and isinstance(value, dict):
                cleaned[key_str] = {
                    str(reg): samples
                    for reg, samples in value.items()
                    if not _is_serial_register(reg)
                }
                continue
            cleaned[key_str] = _scrub(value, _depth=_depth + 1)
        return cleaned
    if isinstance(payload, list):
        return [_scrub(item, _depth=_depth + 1) for item in payload]
    return payload


def _is_identifier_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in {"devcode", "devcodes", "devaddr"}:
        # devcode is a diagnostics-only model hint, not a personal identifier.
        return False
    return any(fragment in lowered for fragment in _IDENTIFIER_KEY_FRAGMENTS)


def _is_serial_register(register: Any) -> bool:
    try:
        return int(register) in _SERIAL_REGISTER_RANGE
    except (TypeError, ValueError):
        return False
