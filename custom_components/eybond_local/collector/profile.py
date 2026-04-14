"""Collector-level profile detection and heartbeat decoding."""

from __future__ import annotations

from ..models import CollectorInfo


def apply_collector_profile(info: CollectorInfo) -> CollectorInfo:
    """Return a copy of collector info enriched with decoded profile fields."""

    payload_hex = info.heartbeat_payload_hex
    if not payload_hex:
        return info

    try:
        payload = bytes.fromhex(payload_hex)
    except ValueError:
        return info

    ascii_payload = payload.decode("ascii", errors="ignore")
    info.heartbeat_ascii = ascii_payload
    info.heartbeat_payload_len = len(payload)

    if info.heartbeat_devcode is not None:
        info.devcode_major = (info.heartbeat_devcode >> 8) & 0xFF
        info.devcode_minor = info.heartbeat_devcode & 0xFF

    if _looks_like_ascii_pn_payload(ascii_payload, info.heartbeat_devcode):
        return _apply_ascii_pn_v1(info, ascii_payload)

    if info.heartbeat_devcode is not None:
        info.profile_key = f"unknown_0x{info.heartbeat_devcode:04X}"
        info.profile_name = f"Unknown Collector 0x{info.heartbeat_devcode:04X}"

    return info


def _looks_like_ascii_pn_payload(ascii_payload: str, heartbeat_devcode: int | None) -> bool:
    if heartbeat_devcode != 0x0102:
        return False
    if len(ascii_payload) < 14:
        return False
    pn = ascii_payload[:14]
    return pn[:1].isalpha() and pn[1:].isdigit()


def _apply_ascii_pn_v1(info: CollectorInfo, ascii_payload: str) -> CollectorInfo:
    info.profile_key = "eybond_ascii_pn_v1"
    info.profile_name = "EyeBond ASCII PN v1"
    info.heartbeat_format_key = "ascii_14pn_plus_suffix"
    info.collector_pn = ascii_payload[:14]
    info.collector_pn_prefix = info.collector_pn[:1]
    info.collector_pn_digits = info.collector_pn[1:]

    suffix = ascii_payload[14:]
    info.heartbeat_suffix_ascii = suffix
    if suffix:
        if suffix.isdigit():
            info.heartbeat_suffix_kind = "digits"
            info.heartbeat_suffix_uint = int(suffix)
        elif suffix.isascii() and suffix.isprintable():
            info.heartbeat_suffix_kind = "ascii"
        else:
            info.heartbeat_suffix_kind = "binary"

    return info
