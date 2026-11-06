"""Persist diagnostic run results.

Writes a local raw result (JSON + TXT, kept under the HA config directory for
owner-only diagnostics) and a shareable JSON with the project's redaction
helpers applied. The shareable file can be exposed through the integration's
short-lived authenticated API endpoint; nothing is copied to public ``/local``.

Documented limitation (per design decision): the shareable copy scrubs
serial-looking tokens from text/ASCII fields and hex blobs, but raw Modbus
register words in the ``decimal``/``hex`` arrays are NOT scrubbed — a serial
sitting in arbitrary registers can still appear there. That residual risk is why
the raw result stays local and only the shareable file can be downloaded.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ..build_identity import build_runtime_values
from ..const import LOCAL_DIAGNOSTIC_RUNS_DIR, LOCAL_METADATA_DIR
from ..payload.modbus import decode_read_request
from .diagnostic_evidence import DiagnosticEvidenceScope, save_diagnostic_evidence
from .diagnostic_runner import DiagnosticRunResult
from .protocol_candidates import analyze_protocol_candidates
from .redaction import sanitize_support_payload
from .masking import (
    _NUMERIC_IDENTIFIER_RE,
    mask_identifier_token as _mask_identifier_token,
    mask_numeric_identifiers as _mask_numeric_identifiers,
)
from .proxy_capture.trace import anonymize_proxy_trace_line


_SMG_SERIAL_REGISTER_START = 186
_SMG_SERIAL_REGISTER_END = 197


def diagnostic_runs_root(config_dir: Path) -> Path:
    """Return the diagnostic run output directory under one HA config dir."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_DIAGNOSTIC_RUNS_DIR


@dataclass(frozen=True, slots=True)
class DiagnosticExportResult:
    result_path: Path
    text_path: Path
    shareable_path: Path
    download_path: Path | None = None


def build_local_payload(result: DiagnosticRunResult) -> dict:
    """Return the full, un-redacted local result payload."""

    return {
        "success": result.success,
        "output": result.output,
        "results": result.results,
        "context": result.context,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "error": result.error,
    }


def build_shareable_payload(result: DiagnosticRunResult) -> dict:
    """Return the redacted, share-safe result payload."""

    payload = build_local_payload(result)
    context = dict(payload.get("context") or {})
    context.pop("entry_id", None)
    payload["context"] = context
    # Do this BEFORE generic masking: serial scrubbing can change CRC bytes.
    # The new byte trace must not re-expose an identity range hidden below.
    events = []
    for item in context.get("wire_events") or []:
        event = dict(item)
        try:
            request = decode_read_request(bytes.fromhex(event.get("request_hex", "")))
        except (TypeError, ValueError):
            request = None
        if request is not None and (
            request.address <= _SMG_SERIAL_REGISTER_END
            and request.address + request.count > _SMG_SERIAL_REGISTER_START
        ):
            for key in ("response_hex", "received_fragment_hex"):
                if key in event:
                    event[key] = "REDACTED_IDENTITY_RANGE"
        events.append(event)
    if "wire_events" in context:
        context["wire_events"] = events
    _redact_opaque_identity_reads(payload)
    payload = anonymize_proxy_trace_line(payload)
    payload = _mask_numeric_identifiers(payload)
    _redact_known_identity_registers(payload)
    return sanitize_support_payload(payload)


def _redact_opaque_identity_reads(payload: dict) -> None:
    """Opaque requests must not bypass known Modbus identity-range masking.

    Decode before generic masking can alter a request's CRC. Copy changed
    records so the private, unredacted run remains intact.
    """

    steps = []
    sensitive_lines: set[int] = set()
    for item in payload.get("results") or []:
        step = dict(item)
        steps.append(step)
        if step.get("kind") != "payload":
            continue
        try:
            request = decode_read_request(bytes.fromhex(
                (step.get("request") or {}).get("raw_hex", ""),
            ))
        except (TypeError, ValueError):
            request = None
        if request is None or not (
            request.address <= _SMG_SERIAL_REGISTER_END
            and request.address + request.count > _SMG_SERIAL_REGISTER_START
        ):
            continue
        response = dict(step.get("response") or {})
        if "raw_hex" in response:
            response["raw_hex"] = "REDACTED_IDENTITY_RANGE"
            step["response"] = response
        if isinstance(step.get("line"), int):
            sensitive_lines.add(step["line"])
    payload["results"] = steps
    if isinstance(payload.get("output"), str):
        payload["output"] = _redact_identity_blocks_from_output(
            payload["output"], sensitive_lines,
        )


def _redact_known_identity_registers(payload: dict) -> None:
    """Remove raw SMG serial-register words from a shareable result."""

    sensitive_lines: set[int] = set()
    for step in payload.get("results") or []:
        if not isinstance(step, dict) or step.get("kind") != "modbus_read":
            continue
        request = step.get("request")
        response = step.get("response")
        if not isinstance(request, dict) or not isinstance(response, dict):
            continue
        try:
            start = int(request.get("register"))
            count = int(request.get("count"))
        except (TypeError, ValueError):
            continue
        overlap_start = max(start, _SMG_SERIAL_REGISTER_START)
        overlap_end = min(start + count - 1, _SMG_SERIAL_REGISTER_END)
        if overlap_start > overlap_end:
            continue
        try:
            sensitive_lines.add(int(step.get("line")))
        except (TypeError, ValueError):
            pass
        for key in ("decimal", "hex"):
            values = response.get(key)
            if not isinstance(values, list):
                continue
            for register in range(overlap_start, overlap_end + 1):
                offset = register - start
                if 0 <= offset < len(values):
                    values[offset] = None if key == "decimal" else "REDACTED"
        response["ascii"] = "REDACTED_IDENTITY_RANGE"

    output = payload.get("output")
    if isinstance(output, str):
        masked_output = _NUMERIC_IDENTIFIER_RE.sub(
            lambda match: _mask_identifier_token(match.group(1)),
            output,
        )
        payload["output"] = _redact_identity_blocks_from_output(
            masked_output,
            sensitive_lines,
        )


def _redact_identity_blocks_from_output(output: str, sensitive_lines: set[int]) -> str:
    if not sensitive_lines:
        return output
    redacted: list[str] = []
    current_sensitive = False
    for line in output.splitlines(keepends=True):
        header = re.match(r"^\[(\d+)\]\s", line)
        if header is not None:
            current_sensitive = int(header.group(1)) in sensitive_lines
            redacted.append(line)
            continue
        if current_sensitive and line.startswith(("decimal:", "hex:", "ascii:", "raw_hex:")):
            label = line.split(":", 1)[0]
            suffix = "\n" if line.endswith("\n") else ""
            redacted.append(f"{label}: REDACTED_IDENTITY_RANGE{suffix}")
            continue
        redacted.append(line)
    return "".join(redacted)


def _safe_stem_part(value: str) -> str:
    cleaned = [char if (char.isalnum() or char in "-_") else "_" for char in str(value or "")]
    return "".join(cleaned).strip("_") or "entry"


def export_diagnostic_run(
    *,
    config_dir: Path,
    entry_id: str,
    result: DiagnosticRunResult,
    now: datetime | None = None,
    publish_download_copy: bool = False,
    device_scope: DiagnosticEvidenceScope | None = None,
) -> DiagnosticExportResult:
    """Write the local raw + shareable result files and optionally publish one."""

    root = diagnostic_runs_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)

    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%S%fZ")
    stem = f"diagnostic_{_safe_stem_part(entry_id)}_{timestamp}"

    # All disk work and passive analysis run in the existing export executor.
    result.context["integration_build"] = build_runtime_values()
    try:
        protocol_report = analyze_protocol_candidates(build_local_payload(result))
    except (OSError, ValueError, KeyError, TypeError):
        protocol_report = {"status": "analysis_unavailable", "additional_requests_sent": 0}
    result.context["protocol_candidates"] = protocol_report
    local_payload = build_local_payload(result)
    shareable_payload = build_shareable_payload(result)

    try:
        cache_status = save_diagnostic_evidence(
            config_dir, entry_id, device_scope, shareable_payload, protocol_report,
        )
    except (OSError, ValueError, TypeError, RecursionError):
        # Optional archive cache must never hide the original diagnostic result.
        cache_status = "cache_write_failed"
    result.context["support_archive_cache_status"] = cache_status
    shareable_payload["context"]["support_archive_cache_status"] = cache_status

    result_path = root / f"{stem}.json"
    text_path = root / f"{stem}.txt"
    shareable_path = root / f"{stem}.share.json"

    result_path.write_text(
        json.dumps(local_payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    text_path.write_text(result.output, encoding="utf-8")
    shareable_path.write_text(
        json.dumps(shareable_payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    download_path: Path | None = None
    if publish_download_copy:
        # The coordinator exposes this redacted file through a short-lived,
        # authenticated API URL. Never copy it into unauthenticated /local.
        download_path = shareable_path

    return DiagnosticExportResult(
        result_path=result_path,
        text_path=text_path,
        shareable_path=shareable_path,
        download_path=download_path,
    )
