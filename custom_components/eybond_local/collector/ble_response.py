"""Bounded AT reply assembly: notification streams are not GATT snapshots.

This is collector management framing, not an inverter payload decoder. AT has
no transaction identifier: serializing exchanges cannot prove freshness of a
same-command snapshot or a late reply after a timeout.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal


MAX_REPLY_BYTES = 16384
ReplyFeedDisposition = Literal[
    "accepted", "placeholder", "binary_or_control", "command_echo", "unmatched_text",
]


@dataclass
class _Reply:
    data: bytes = b""
    changed_at: float = 0.0
    ended: bool = False


class VendorAtReply:
    """Keep read and notify evidence separate until one complete reply exists."""

    def __init__(self, command: str) -> None:
        command = command.strip()
        self.is_write = "=" in command
        self.query_echo = command.encode("ascii") if not self.is_write and command.endswith("?") else b""
        name = command.split("=", 1)[0].rstrip("?")
        parameter = re.fullmatch(r"AT\+INTPARA(\d+)", name)
        self.prefix = (
            f"AT+INTPARA:{parameter[1]}," if parameter else f"{name}:"
        ).encode("ascii")
        self.scan = name == "AT+INTPARA49"
        self.allow_empty = name == "AT+HTBT" and not self.is_write
        self.query_status_prefix = (name + "?:").encode("ascii") if not self.is_write and not parameter else b""
        self.notify = _Reply()
        self.read = _Reply()

    @property
    def pending(self) -> bool:
        return bool(self.notify.data or self.read.data)

    def feed(self, payload: bytes, *, source: str, now: float) -> ReplyFeedDisposition:
        """Assemble a reply and classify ignored bytes without retaining their contents."""
        state = self.notify if source == "notify" else self.read
        # A placeholder can finish a scan but cannot complete a bare AT marker.
        terminator_fragment = bool(state.data and payload and not payload.strip(b"\r\n"))
        if not terminator_fragment and (not payload.strip() or not payload.strip(b"-\r\n ")):
            if self.scan and state.data:
                state.ended = True
            return "placeholder"
        if len(payload) > MAX_REPLY_BYTES:
            raise ValueError("ble_response_too_large")
        # Reject binary/control traffic, including inverter RTU notifications.
        if any(byte < 32 and byte not in (10, 13) for byte in payload):
            return "binary_or_control"
        if self.query_echo and payload.strip(b"\r\n") == self.query_echo:
            return "command_echo"
        if not state.data:
            payload = payload.lstrip(b"\r\n")
        if self.query_status_prefix and payload.startswith(self.query_status_prefix):
            # Some firmware includes '?' in a query-status response. An echoed
            # query without ':value' is still not a reply.
            payload = self.prefix + payload[len(self.query_status_prefix):]
        if self.scan:
            for alias in (b"AT+INTPARA49:", b"49,"):
                if payload.startswith(alias):
                    payload = self.prefix + payload[len(alias):]
                    break
        starts_reply = payload.startswith(self.prefix) or self.prefix.startswith(payload)
        if starts_reply:
            if self.scan and state.data and payload.startswith(self.prefix):
                # Scan firmware can return successive complete network entries.
                # A cumulative/repeated ReadValue is not an additional entry.
                if source == "read" and state.data.startswith(payload):
                    return "accepted"
                combined = payload if payload.startswith(state.data) else state.data + payload
            elif source == "read" and state.data.startswith(payload):
                return "accepted"  # Repeated snapshot must not reset the idle timer.
            else:
                combined = payload
        elif state.data and not payload.startswith(b"AT+"):
            # ReadValue is normally a snapshot. Preserve the observed legacy
            # split-header response, but never append a value to another value.
            header_incomplete = self.prefix.startswith(state.data)
            if source == "read" and not header_incomplete and not self.scan:
                return "unmatched_text"
            combined = state.data + payload
        else:
            return "unmatched_text"
        if not (combined.startswith(self.prefix) or self.prefix.startswith(combined)):
            return "unmatched_text"
        if len(combined) > MAX_REPLY_BYTES:
            raise ValueError("ble_response_too_large")
        if combined != state.data:
            state.data = combined
            state.changed_at = now
            state.ended = False
        return "accepted"

    def result(self, *, now: float, settle: float, deadline: bool = False) -> str | None:
        """Return a framed/settled reply; partial notification owns its response."""
        state = self.notify if self.notify.data else self.read
        data = state.data
        if not data.startswith(self.prefix):
            return None
        if self.scan:
            # Scans are multi-record messages, sometimes with a marker per AP.
            if data.count(b"[") != data.count(b"]"):
                return None
            if not (state.ended or deadline or now - state.changed_at >= settle):
                return None
        else:
            terminated = data.endswith(b"\r\n")
            body = data.rstrip(b"\r\n")
            if b"\r" in body or b"\n" in body:
                return None  # ReadValue can contain the previous value's tail.
            value = body[len(self.prefix):]
            if not value and not (self.allow_empty and terminated):
                return None
            if self.is_write and not re.fullmatch(rb"[RW]\d{3}", value):
                return None
            if not terminated and now - state.changed_at < settle:
                return None
        try:
            return data.rstrip(b"\r\n").decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return None
