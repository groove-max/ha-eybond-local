"""Explicit scan addresses: bounded routes, never device identities or networks."""
from __future__ import annotations

from ipaddress import IPv4Address
import re

MAX_KNOWN_COLLECTOR_IPS = 8


def parse_known_collector_ips(raw: str, *, excluded: tuple[str, ...] = ()) -> tuple[str, ...]:
    """Accept a small list of literal host addresses, with stable deduplication.

    No DNS, CIDR expansion, port parsing, inferred subnet or IO belongs here.
    Explicit addresses supplement (not replace) the normal local scan.
    """
    if type(raw) is not str or len(raw) > 512:
        raise ValueError("invalid_collector_targets")
    if not raw.strip():
        return ()
    result = []
    for token in re.split(r"[,\s]+", raw.strip()):
        try:
            address = IPv4Address(token)
        except ValueError as exc:
            raise ValueError("invalid_collector_targets") from exc
        value = str(address)
        if (address.is_unspecified or address.is_multicast or address.is_loopback
                or address.is_reserved or value in excluded):
            raise ValueError("invalid_collector_targets")
        if value not in result:
            result.append(value)
        if len(result) > MAX_KNOWN_COLLECTOR_IPS:
            raise ValueError("invalid_collector_targets")
    return tuple(result)
