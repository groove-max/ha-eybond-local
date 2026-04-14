"""Helpers for loading, saving, and anonymizing offline fixtures."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

DEFAULT_SMG_SERIAL_RANGES: tuple[tuple[int, int], ...] = ((186, 12),)


def load_fixture_json(path: str | Path) -> dict[str, Any]:
    """Load one raw fixture JSON document."""

    fixture_path = Path(path)
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def save_fixture_json(path: str | Path, payload: dict[str, Any]) -> None:
    """Write one raw fixture JSON document."""

    fixture_path = Path(path)
    fixture_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def anonymize_fixture_json(
    payload: dict[str, Any],
    *,
    serial_ranges: tuple[tuple[int, int], ...] = DEFAULT_SMG_SERIAL_RANGES,
    keep_name: bool = False,
) -> dict[str, Any]:
    """Return an anonymized copy of a fixture while keeping replay compatibility."""

    anonymized = deepcopy(payload)
    redactions: list[dict[str, Any]] = []

    if not keep_name:
        original_name = str(anonymized.get("name", "fixture"))
        anonymized["name"] = f"anon_{_stable_token(original_name, length=10, alphabet='hex')}"
        redactions.append({"field": "name", "kind": "replaced"})

    collector = anonymized.get("collector")
    if isinstance(collector, dict):
        remote_ip = collector.get("remote_ip")
        if remote_ip:
            collector["remote_ip"] = "REDACTED"
            redactions.append({"field": "collector.remote_ip", "kind": "removed"})

        collector_pn = collector.get("collector_pn")
        if collector_pn:
            collector["collector_pn"] = _mask_text_by_shape(str(collector_pn))
            redactions.append({"field": "collector.collector_pn", "kind": "pseudonymized"})

    for start, count in serial_ranges:
        changed = _anonymize_range(anonymized, start=start, count=count)
        if changed:
            redactions.append(
                {
                    "field": "ranges",
                    "kind": "ascii_words_pseudonymized",
                    "start": start,
                    "count": count,
                }
            )

    command_redactions = _anonymize_command_responses(anonymized)
    redactions.extend(command_redactions)

    anonymized["anonymized"] = True
    anonymized["redactions"] = redactions
    return anonymized


def build_command_fixture_responses(raw_capture: dict[str, Any]) -> dict[str, str]:
    """Build replay-compatible command responses from a raw command capture."""

    responses = raw_capture.get("responses")
    if not isinstance(responses, dict) or not responses:
        return {}

    command_responses = {
        str(command): str(payload)
        for command, payload in responses.items()
        if isinstance(command, str) and payload is not None
    }
    failures = raw_capture.get("failures")
    if isinstance(failures, dict):
        for command, failure in failures.items():
            normalized = str(failure).strip().upper()
            if normalized in {"NAK", "NOA", "ERCRC"}:
                command_responses.setdefault(str(command), normalized)
    return command_responses


def _anonymize_command_responses(payload: dict[str, Any]) -> list[dict[str, Any]]:
    command_responses = payload.get("command_responses")
    if not isinstance(command_responses, dict):
        return []

    redactions: list[dict[str, Any]] = []
    for command in ("QID", "^P005ID"):
        raw = command_responses.get(command)
        if not raw:
            continue
        masked = _mask_command_serial_response(command, str(raw))
        if masked == raw:
            continue
        command_responses[command] = masked
        redactions.append(
            {
                "field": f"command_responses.{command}",
                "kind": "pseudonymized",
            }
        )
    return redactions


def _mask_command_serial_response(command: str, payload: str) -> str:
    text = payload.strip()
    if not text:
        return payload
    if command == "QID":
        return _mask_text_by_shape(text)
    if command == "^P005ID" and len(text) >= 2 and text[:2].isdigit():
        available = int(text[:2])
        serial = text[2 : 2 + available]
        if not serial:
            return payload
        masked_serial = _mask_text_by_shape(serial)
        return f"{text[:2]}{masked_serial}{text[2 + available:]}"
    return payload


def _anonymize_range(payload: dict[str, Any], *, start: int, count: int) -> bool:
    """Pseudonymize one ASCII-bearing register range in-place."""

    changed = False
    for range_item in payload.get("ranges", []):
        range_start = int(range_item.get("start", 0))
        values = list(range_item.get("values", []))
        if not values:
            continue

        range_end = range_start + len(values)
        target_end = start + count
        overlap_start = max(range_start, start)
        overlap_end = min(range_end, target_end)
        if overlap_start >= overlap_end:
            continue

        offset = overlap_start - range_start
        word_count = overlap_end - overlap_start
        original_words = values[offset : offset + word_count]
        original_ascii = words_to_ascii(original_words)
        if not original_ascii:
            continue

        masked_ascii = _mask_text_by_shape(original_ascii)
        values[offset : offset + word_count] = ascii_to_words(masked_ascii, word_count=word_count)
        range_item["values"] = values
        changed = True
    return changed


def words_to_ascii(values: list[int]) -> str:
    """Decode printable ASCII from 16-bit register words."""

    chars: list[str] = []
    for value in values:
        for byte in ((value >> 8) & 0xFF, value & 0xFF):
            if byte in (0x00, 0xFF):
                continue
            if 32 <= byte <= 126:
                chars.append(chr(byte))
    return "".join(chars)


def ascii_to_words(text: str, *, word_count: int) -> list[int]:
    """Encode printable ASCII back into Modbus register words."""

    raw = text.encode("ascii", errors="ignore")[: word_count * 2]
    raw = raw.ljust(word_count * 2, b"\x00")
    values: list[int] = []
    for offset in range(0, len(raw), 2):
        values.append((raw[offset] << 8) | raw[offset + 1])
    return values


def _mask_text_by_shape(text: str) -> str:
    """Replace a string with a deterministic pseudonym while preserving character classes."""

    masked: list[str] = []
    for index, char in enumerate(text):
        if char.isdigit():
            masked.append(_stable_token(f"{text}:{index}", length=1, alphabet="digits"))
            continue
        if "A" <= char <= "Z":
            masked.append(_stable_token(f"{text}:{index}", length=1, alphabet="upper"))
            continue
        if "a" <= char <= "z":
            masked.append(_stable_token(f"{text}:{index}", length=1, alphabet="lower"))
            continue
        masked.append(char)
    return "".join(masked)


def _stable_token(seed: str, *, length: int, alphabet: str) -> str:
    """Generate a deterministic token using the selected alphabet."""

    chars_by_alphabet = {
        "digits": "0123456789",
        "upper": "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "lower": "abcdefghijklmnopqrstuvwxyz",
        "hex": "0123456789abcdef",
    }
    chars = chars_by_alphabet[alphabet]
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    output: list[str] = []
    position = 0
    while len(output) < length:
        if position >= len(digest):
            digest = hashlib.sha256(digest).digest()
            position = 0
        output.append(chars[digest[position] % len(chars)])
        position += 1
    return "".join(output)
