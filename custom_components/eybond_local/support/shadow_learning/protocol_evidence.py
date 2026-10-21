"""Bounded protocol evidence, independent of the current inverter profile.

Only validated read requests may authorize read-only compatibility probes.
Unknown traffic is represented by size/digest, never arbitrary credential text.
Neither a codec match nor a request is evidence of a register map or write support.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import re

from ...collector.protocol import HEADER_SIZE, decode_header
from ...models import ProbeTarget
from ...payload.modbus import decode_read_request


def read_protocols(payload: bytes) -> dict[str, int]:
    """Return every matching codec and its payload address; never first-match."""
    from .pi30_protocol import Pi30ShadowLearningAdapter
    from .protocol import EybondGAsciiShadowLearningAdapter
    from ...payload.pi18 import build_request as build_pi18
    from ...payload.short_ascii import READ_COMMANDS as SHORT_READS
    from ...payload.urtu09c1 import READ_COMMANDS as URTU_READS

    if type(payload) is not bytes or not 1 <= len(payload) <= 512:
        return {}
    found = {}
    read = decode_read_request(payload)
    if (read is not None and 1 <= read.slave_id <= 247
            and 1 <= read.count <= 125 and read.address + read.count <= 65536):
        found["modbus_rtu"] = read.slave_id
    if Pi30ShadowLearningAdapter().decode_read_request(payload) is not None:
        found["pi30_ascii"] = 0
    if EybondGAsciiShadowLearningAdapter().decode_read_request(payload) is not None:
        found["eybond_g_ascii"] = 0
    if len(payload) >= 9:
        command = payload[:-3].decode("ascii", errors="replace")
        if (re.fullmatch(r"\^P[0-9]{3}[A-Z0-9]+", command)
                and int(command[2:5]) == len(command[5:]) + 3
                and build_pi18(command) == payload):
            found["pi18_ascii"] = 0
    if payload[-1:] == b"\r":
        if payload[:-1] in {command.encode() for command in URTU_READS}:
            found["eybond_09c1"] = 0
        if len(payload) >= 3 and payload[:-2] in {command.encode() for command in SHORT_READS}:
            found["eybond_short_ascii"] = payload[-2]
    return found


@dataclass(frozen=True, slots=True)
class CloudProtocolEvidence:
    """A complete validated read, bound to its observed EyeBond route."""

    devcode: int
    collector_addr: int
    payload: bytes

    def __post_init__(self):
        if (type(self.devcode) is not int or not 0 <= self.devcode <= 65535
                or type(self.collector_addr) is not int or not 0 <= self.collector_addr <= 255
                or not read_protocols(self.payload)):
            raise ValueError("invalid_cloud_protocol_evidence")

    @property
    def protocols(self) -> tuple[str, ...]:
        return tuple(sorted(read_protocols(self.payload)))

    def target(self, codec: str) -> ProbeTarget:
        return ProbeTarget(self.devcode, self.collector_addr, read_protocols(self.payload)[codec])

    def to_mapping(self) -> dict:
        return {"devcode": self.devcode, "collector_addr": self.collector_addr,
                "request_hex": self.payload.hex(), "protocols": list(self.protocols),
                "status": "recognized" if len(self.protocols) == 1 else "ambiguous"}

    @classmethod
    def from_mapping(cls, raw):
        if not isinstance(raw, dict):
            return None
        value = raw.get("request_hex")
        if type(value) is not str or len(value) > 1024:
            return None
        try:
            return cls(raw["devcode"], raw["collector_addr"], bytes.fromhex(value))
        except (KeyError, TypeError, ValueError):
            return None

    @classmethod
    def from_frame(cls, frame: bytes):
        if not HEADER_SIZE < len(frame) <= HEADER_SIZE + 512:
            return None
        header = decode_header(frame[:HEADER_SIZE])
        if header.fcode != 4 or header.total_len != len(frame):
            return None
        try:
            return cls(header.devcode, header.devaddr, frame[HEADER_SIZE:])
        except ValueError:
            return None


def protocol_sample(payload: bytes, *, direction: str, carrier: str) -> dict:
    """Safe structural evidence, including unrecognized and ambiguous input."""
    protocols = read_protocols(payload)
    sample = {"direction": direction, "carrier": carrier, "length": len(payload),
              "sha256": sha256(payload).hexdigest(), "protocols": sorted(protocols),
              "status": "recognized" if len(protocols) == 1 else "ambiguous" if protocols else "unknown"}
    # Only validated bounded read commands contain no setting/credential values.
    # Replies, writes and unknown payloads stay hashed, not silently hex-encoded.
    if protocols and direction == "cloud_to_collector":
        sample["request_hex"] = payload.hex()
    return sample
