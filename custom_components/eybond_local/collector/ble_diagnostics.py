"""Payload-free observations for one explicitly requested BLE text exchange."""

from dataclasses import asdict, dataclass, field

from ..payload.modbus import ModbusError, parse_read_registers_response


def _binary_shape(payload: bytes) -> str:
    """Classify a single discarded fragment without retaining any received value.

    This is diagnostic evidence only, not a decoder/transport selection. A
    complete FC03/04 reply needs valid length and CRC. Split/coalesced/unknown
    traffic stays unclassified; absence of a match does not rule out a protocol.
    Neither a match nor a CRC proves identity, freshness or request ownership.
    """

    if len(payload) > 512:
        return "oversize"
    prefixed = payload.startswith(b"11111111")
    frame = payload[8:] if prefixed else payload
    if (
        7 <= len(frame) <= 255
        and 1 <= frame[0] <= 247
        and frame[1] in (3, 4)
        and 2 <= frame[2] <= 250
        and frame[2] % 2 == 0
    ):
        try:
            parse_read_registers_response(
                frame, slave_id=frame[0], count=frame[2] // 2, function=frame[1],
            )
        except ModbusError:
            pass
        else:
            return "vendor_prefixed_rtu_read" if prefixed else "rtu_read"
    if prefixed:
        return "vendor_prefix_other"
    if b"AT+" in payload:
        return "at_marker_with_control"
    return "other_binary"


@dataclass(slots=True)
class BleExchangeDiagnostics:
    """Only stages, counts, fixed shapes and durations; never received values.

    Received notifications can be unrelated or stale. These counters are not
    evidence of collector identity, a fresh reply or an applied setting.
    """

    phase: str = "queued"
    write_completed: bool = False
    notifications_received: int = 0
    notification_bytes: int = 0
    notifications_consumed: int = 0
    read_attempts: int = 0
    read_completed: int = 0
    read_timeouts: int = 0
    read_errors: int = 0
    read_empty: int = 0
    read_bytes: int = 0
    matched_notify_bytes: int = 0
    matched_read_bytes: int = 0
    # Keys are the assembler's fixed disposition labels, never received text.
    ignored_notify: dict[str, int] = field(default_factory=dict)
    ignored_read: dict[str, int] = field(default_factory=dict)
    # Fixed shape names only; no hex, prefixes, hashes, decoded data or errors.
    binary_notify_shapes: dict[str, int] = field(default_factory=dict)
    binary_read_shapes: dict[str, int] = field(default_factory=dict)
    reply_source: str = "none"
    elapsed_ms: int = 0

    def observe_binary(self, payload: bytes, *, source: str) -> None:
        """Count a discarded fragment; never change reply acceptance or retries."""
        shapes = self.binary_notify_shapes if source == "notify" else self.binary_read_shapes
        shape = _binary_shape(payload)
        shapes[shape] = shapes.get(shape, 0) + 1

    def snapshot(self) -> dict:
        """Return an independent, fixed-field export, with no raw wire bytes."""
        return asdict(self)
