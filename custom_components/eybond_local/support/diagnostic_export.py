"""Persist diagnostic run results.

Writes a local raw result (JSON + TXT, kept under the HA config directory for
owner-only diagnostics) and a shareable JSON with the project's redaction
helpers applied. Only the shareable copy is optionally published to ``/local``
so the un-redacted raw responses never leave the owner's host through the
download URL.

Documented limitation (per design decision): the shareable copy scrubs
serial-looking tokens from text/ASCII fields and hex blobs, but raw Modbus
register words in the ``decimal``/``hex`` arrays are NOT scrubbed — a serial
sitting in arbitrary registers can still appear there. That residual risk is why
the raw result stays local and only the shareable copy is published.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ..const import LOCAL_DIAGNOSTIC_RUNS_DIR, LOCAL_METADATA_DIR
from .diagnostic_runner import DiagnosticRunResult
from .masking import (
    _NUMERIC_IDENTIFIER_RE,
    mask_identifier_token as _mask_identifier_token,
    mask_numeric_identifiers as _mask_numeric_identifiers,
)
from .proxy_trace import anonymize_proxy_trace_line


_SMG_SERIAL_REGISTER_START = 186
_SMG_SERIAL_REGISTER_END = 197


def diagnostic_runs_root(config_dir: Path) -> Path:
    """Return the diagnostic run output directory under one HA config dir."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_DIAGNOSTIC_RUNS_DIR


def diagnostic_runs_public_root(config_dir: Path) -> Path:
    """Return the Home Assistant static-file directory for shareable exports."""

    return config_dir / "www" / LOCAL_METADATA_DIR / LOCAL_DIAGNOSTIC_RUNS_DIR


def diagnostic_run_download_url(filename: str) -> str:
    """Return the Home Assistant `/local` URL for one shareable export."""

    return f"/local/{LOCAL_METADATA_DIR}/{LOCAL_DIAGNOSTIC_RUNS_DIR}/{filename}"


@dataclass(frozen=True, slots=True)
class DiagnosticExportResult:
    result_path: Path
    text_path: Path
    shareable_path: Path
    download_path: Path | None = None
    download_url: str | None = None


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
    payload = anonymize_proxy_trace_line(payload)
    payload = _mask_numeric_identifiers(payload)
    _redact_known_identity_registers(payload)
    return payload


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
        if current_sensitive and line.startswith(("decimal:", "hex:", "ascii:")):
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
) -> DiagnosticExportResult:
    """Write the local raw + shareable result files and optionally publish one."""

    root = diagnostic_runs_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)

    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%S%fZ")
    stem = f"diagnostic_{_safe_stem_part(entry_id)}_{timestamp}"

    local_payload = build_local_payload(result)
    shareable_payload = build_shareable_payload(result)

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
    download_url: str | None = None
    if publish_download_copy:
        public_root = diagnostic_runs_public_root(config_dir)
        public_root.mkdir(parents=True, exist_ok=True)
        # Only the redacted shareable copy is exposed through /local.
        download_path = public_root / shareable_path.name
        shutil.copy2(shareable_path, download_path)
        download_url = diagnostic_run_download_url(shareable_path.name)

    return DiagnosticExportResult(
        result_path=result_path,
        text_path=text_path,
        shareable_path=shareable_path,
        download_path=download_path,
        download_url=download_url,
    )
