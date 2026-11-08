"""Payload-free observations for one explicitly requested BLE text exchange."""

from dataclasses import asdict, dataclass, field


@dataclass(slots=True)
class BleExchangeDiagnostics:
    """Only stages, counts and durations; never commands, values or exceptions.

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
    reply_source: str = "none"
    elapsed_ms: int = 0

    def snapshot(self) -> dict:
        """Return an independent, fixed-field export, with no raw wire bytes."""
        return asdict(self)
