"""Passive protocol leads from saved exchanges; no driver or transport authority.

A matching request, length and checksum establish only structural similarity.
They do not establish a model, field semantics, or permission to poll/write.
"""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path


CATALOG_PATH = Path(__file__).resolve().parents[1] / "protocol_catalogs/diagnostic_candidates.json"
MAX_EVENTS = 128
MAX_PAYLOAD_BYTES = 512


@lru_cache(maxsize=1)
def _catalog_text() -> str:
    # Called only from the diagnostic export executor, never from live polling.
    return CATALOG_PATH.read_text(encoding="utf-8")


def _hex_bytes(value: object) -> bytes | None:
    if not isinstance(value, str) or len(value) > MAX_PAYLOAD_BYTES * 2:
        return None
    try:
        return bytes.fromhex(value)
    except ValueError:
        return None


def _validate_catalog(catalog: dict) -> None:
    """Reject inconsistent packaged descriptors rather than infer wire rules."""

    if not isinstance(catalog, dict) or catalog.get("schema_version") != 1:
        raise ValueError("unsupported_diagnostic_catalog_version")
    if not isinstance(catalog.get("source"), dict):
        raise ValueError("diagnostic_catalog_source_missing")
    candidates = catalog.get("candidates")
    if not isinstance(candidates, list) or len(candidates) > 32:
        raise ValueError("diagnostic_catalog_candidates_invalid")
    keys = set()
    for item in candidates:
        if not isinstance(item, dict):
            raise ValueError("diagnostic_catalog_descriptor_invalid")
        strings = ("key", "vendor_code", "source_class", "length_basis", "checksum")
        if any(not isinstance(item.get(key), str) or not item[key] for key in strings):
            raise ValueError("diagnostic_catalog_descriptor_invalid")
        if item["key"] in keys or not _hex_bytes(item.get("request_hex")):
            raise ValueError("diagnostic_catalog_request_or_key_invalid")
        keys.add(item["key"])
        sizes = ("response_size", "body_offset", "body_size")
        if any(type(item.get(key)) is not int for key in sizes):
            raise ValueError("diagnostic_catalog_size_invalid")
        trailer = {"sum8_body": 1, "sum16be_before_trailer3": 3}.get(item["checksum"])
        if (trailer is None or not 0 < item["response_size"] <= MAX_PAYLOAD_BYTES
                or item["body_offset"] < 0 or item["body_size"] <= 0
                or item["body_offset"] + item["body_size"] + trailer != item["response_size"]):
            raise ValueError("diagnostic_catalog_layout_invalid")
        if not isinstance(item.get("unverified"), list) or any(
            not isinstance(value, str) for value in item["unverified"]
        ):
            raise ValueError("diagnostic_catalog_limitations_invalid")
        variants = item.get("response_variants", [])
        if not isinstance(variants, list) or len(variants) > 8:
            raise ValueError("diagnostic_catalog_variants_invalid")
        lengths = {item["response_size"]}
        for variant in variants:
            if not isinstance(variant, dict) or set(variant) != {
                "response_size", "body_size", "response_prefix_hex", "length_basis", "evidence_url",
            }:
                raise ValueError("diagnostic_catalog_variant_invalid")
            prefix = _hex_bytes(variant["response_prefix_hex"])
            if (type(variant["response_size"]) is not int or type(variant["body_size"]) is not int
                    or not 0 < variant["response_size"] <= MAX_PAYLOAD_BYTES
                    or variant["response_size"] in lengths
                    or variant["body_size"] < item["body_size"]
                    or item["body_offset"] + variant["body_size"] + trailer != variant["response_size"]
                    or not prefix or len(prefix) > item["body_offset"]
                    or not isinstance(variant["length_basis"], str) or not variant["length_basis"]
                    or not isinstance(variant["evidence_url"], str)
                    or not variant["evidence_url"].startswith("https://")):
                raise ValueError("diagnostic_catalog_variant_invalid")
            lengths.add(variant["response_size"])


def _response_layout(descriptor: dict, size: object) -> dict | None:
    """Only explicit layouts, never arbitrary lengths inferred from a checksum."""
    if type(size) is not int:
        return None
    if size == descriptor["response_size"]:
        return descriptor
    for variant in descriptor.get("response_variants", []):
        if variant["response_size"] == size:
            return descriptor | variant
    return None


def _check_response(event: dict, descriptor: dict) -> tuple[str, str]:
    if event.get("wire") != "fc4" or event.get("route_family") != "eybond":
        return "not_validated", "wire_not_qualified"
    if event.get("request_truncated") or event.get("response_truncated"):
        return "not_validated", "truncated_exchange"
    if event.get("status") != "reply":
        return "not_validated", "no_completed_exchange"
    progress = event.get("transport_progress")
    if isinstance(progress, dict) and progress.get("response_received") is False:
        return "not_validated", "response_not_correlated"
    response = _hex_bytes(event.get("response_hex"))
    if response is None:
        return "not_validated", "response_unavailable_or_redacted"
    if event.get("response_size") != len(response):
        return "not_validated", "response_size_mismatch"
    if not response:
        return "not_validated", "empty_reply"
    if response == bytes.fromhex(descriptor["request_hex"]):
        return "not_validated", "request_echo"
    layout = _response_layout(descriptor, len(response))
    if layout is None:
        return "not_validated", "unexpected_length"
    prefix = layout.get("response_prefix_hex")
    if prefix is not None and not response.startswith(bytes.fromhex(prefix)):
        return "not_validated", "unexpected_header"
    if descriptor["checksum"] == "sum8_body":
        valid = (sum(response[descriptor["body_offset"]:-1]) & 0xFF) == response[-1]
    elif descriptor["checksum"] == "sum16be_before_trailer3":
        valid = (sum(response[:-3]) & 0xFFFF) == int.from_bytes(response[-3:-1], "big")
    else:
        return "not_validated", "checksum_rule_unknown"
    return ("structural_candidate", "request_length_checksum_match") if valid else (
        "not_validated", "checksum_mismatch",
    )


def analyze_protocol_candidates(payload: dict, *, catalog: dict | None = None) -> dict:
    """Analyze bounded, already-correlated diagnostic wire events offline.

    Deliberately do not guess from a response length, cloud ID, envelope
    devcode or ASCII substring alone. Multiple candidates remain multiple.
    The report contains no copied raw payload or device identifiers.
    """

    catalog = json.loads(_catalog_text()) if catalog is None else catalog
    _validate_catalog(catalog)
    context = payload.get("context")
    events = context.get("wire_events", []) if isinstance(context, dict) else []
    events = events if isinstance(events, list) else []
    observations = []
    for index, event in enumerate(events[:MAX_EVENTS]):
        if not isinstance(event, dict):
            continue
        request = _hex_bytes(event.get("request_hex"))
        if request is None or event.get("request_size") != len(request):
            continue
        for descriptor in catalog["candidates"]:
            if request != bytes.fromhex(descriptor["request_hex"]):
                continue
            status, reason = _check_response(event, descriptor)
            layout = _response_layout(descriptor, event.get("response_size")) or descriptor
            observations.append({
                "event_index": index,
                "candidate_key": descriptor["key"],
                "vendor_code": descriptor["vendor_code"],
                "source_class": descriptor["source_class"],
                "status": status, "reason": reason,
                "expected_response_size": layout["response_size"],
                "length_basis": layout["length_basis"],
                "body_offset": descriptor["body_offset"],
                "body_size": layout["body_size"],
                "vendor_parse_body_size": descriptor["body_size"],
                "extension_size": layout["body_size"] - descriptor["body_size"],
                **({"layout_evidence_url": layout["evidence_url"]} if "evidence_url" in layout else {}),
                "unverified": list(descriptor["unverified"]),
            })
    candidates = sorted({item["candidate_key"] for item in observations
                         if item["status"] == "structural_candidate"})
    return {
        "schema_version": 1,
        "analysis_basis": "original_local_diagnostic_exchanges_before_redaction",
        "source": dict(catalog["source"]),
        "status": "candidates_found" if candidates else "no_structural_match",
        "candidate_keys": candidates,
        "ambiguous": len(candidates) > 1,
        "observations": observations,
        "events_examined": min(len(events), MAX_EVENTS),
        "events_omitted": max(0, len(events) - MAX_EVENTS),
        "runtime_support_confirmed": False,
        "telemetry_decoded": False,
        "additional_requests_sent": 0,
    }
