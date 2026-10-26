"""Payload-free observations of an identity query, never routing authority."""

from dataclasses import dataclass


@dataclass(slots=True)
class IdentityProbeProgress:
    """Local write/drain completion does NOT prove remote receipt or execution."""

    protocol: str
    probe_kind: str
    phase: str = "before_write"
    outcome: str = "in_progress"
    write_started: bool = False
    write_returned: bool = False
    drain_completed: bool = False
    # Bytes returned by reads (including echo/invalid replies), not TCP ACKs
    # or a packet capture. Partial readexactly buffers on timeout are not counted.
    response_bytes: int = 0
    peer_eof: bool = False
    duration_ms: int = 0

    def diagnostics(self) -> dict[str, object]:
        # Deliberately no request/reply bytes, PN, remote address or exception
        # messages. Copies also keep support consumers out of the live record.
        return {
            "protocol": self.protocol,
            "probe_kind": self.probe_kind,
            "phase": self.phase,
            "outcome": self.outcome,
            "write_started": self.write_started,
            "write_returned": self.write_returned,
            "drain_completed": self.drain_completed,
            "response_bytes": self.response_bytes,
            "peer_eof": self.peer_eof,
            "duration_ms": self.duration_ms,
        }
