"""Opt-in, per-request diagnostics; never evidence of device-side execution."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SocketRequestTrace:
    """Record local socket progress without payloads or connection identifiers.

    A successful writer.write/drain only describes the local stream. It does
    not prove that the collector or inverter received or executed a command.
    Each explicit diagnostic exchange owns a new instance, including on error.
    """

    phase: str = "checking_session"
    tid: int | None = None
    write_started: bool = False
    write_returned: bool = False
    drain_completed: bool = False
    response_received: bool = False

    def snapshot(self) -> dict[str, object]:
        result: dict[str, object] = {
            "phase": self.phase,
            "write_started": self.write_started,
            "write_returned": self.write_returned,
            "drain_completed": self.drain_completed,
            "response_received": self.response_received,
        }
        if self.tid is not None:
            result["tid"] = self.tid
        return result
