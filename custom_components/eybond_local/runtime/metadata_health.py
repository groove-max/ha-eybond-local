"""Per-channel health for collector-metadata TELEMETRY channels.

This is deliberately SEPARATE from the driver's unsupported-command negative
cache (``drivers.command_support``): a metadata channel being unanswered is a
fact about the collector's metadata surface, not about the inverter's command
set, and the two must never commit each other's verdicts. A metadata channel
becomes "dead" only after it has DELIVERED commands over a live link yet
returned no metadata ``UNSUPPORTED_METADATA_CHANNEL_STRIKES`` times in a row --
a timeout/disconnect is a link fact and never a strike.

Persistence model: the persisted state is the DEAD SET (channels that reached
the threshold); the running consecutive-failure count is transient. A same-PN
reconnect keeps the health; a durable identity change clears it.
"""

from __future__ import annotations

UNSUPPORTED_METADATA_CHANNEL_STRIKES = 4


class CollectorMetadataHealth:
    """Per-channel consecutive-failure counts + a persisted dead-channel set."""

    def __init__(self, *, dead_threshold: int = UNSUPPORTED_METADATA_CHANNEL_STRIKES) -> None:
        self._threshold = int(dead_threshold)
        self._failures: dict[str, int] = {}
        self._dead: set[str] = set()

    def is_dead(self, channel_id: str) -> bool:
        return channel_id in self._dead

    def failure_count(self, channel_id: str) -> int:
        return int(self._failures.get(channel_id, 0))

    def record_empty(self, channel_id: str) -> None:
        """One delivered-but-unanswered read: a dead-channel strike."""

        count = self._failures.get(channel_id, 0) + 1
        self._failures[channel_id] = count
        if count >= self._threshold:
            self._dead.add(channel_id)

    def record_alive(self, channel_id: str) -> None:
        """A read produced metadata: reset ONLY this channel's health."""

        self._failures.pop(channel_id, None)
        self._dead.discard(channel_id)

    def clear(self) -> None:
        """Explicit recheck / durable identity change: forget everything."""

        self._failures.clear()
        self._dead.clear()

    def seed_dead(self, channels: tuple[str, ...] | list[str]) -> None:
        """Seed the persisted dead set (from the config entry)."""

        for channel in channels:
            key = str(channel or "").strip()
            if key:
                self._dead.add(key)
                # Persistence stores the verdict, not the transient path that
                # led to it.  Reconstruct the minimum count consistent with a
                # dead verdict so diagnostics never claim ``0 / threshold``.
                self._failures[key] = max(
                    self._failures.get(key, 0),
                    self._threshold,
                )

    def dead_channels(self) -> tuple[str, ...]:
        """Return the sorted dead-channel set for persistence/diagnostics."""

        return tuple(sorted(self._dead))

    @property
    def threshold(self) -> int:
        return self._threshold
