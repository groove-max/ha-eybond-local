"""Neutral driver contract for classifying a failed capability write.

The runtime hub is protocol-neutral: when a capability write fails it asks the
active driver to classify the failure and then acts on the verdict. The verdict
is an immutable :class:`WriteErrorClassification` -- never an ambiguous tuple --
so the hub never inspects protocol error types, exception codes, or builds
protocol-specific user messages.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..models import CapabilityBlocker


@dataclass(frozen=True, slots=True)
class WriteErrorClassification:
    """One driver verdict on a failed capability write.

    * ``blocker`` -- a durable :class:`CapabilityBlocker` the hub should persist
      until its ``clear_on`` condition, or ``None``.
    * ``user_error`` -- a non-persistent, user-facing error the hub should raise
      instead of the raw exception (e.g. an out-of-range value), or ``None``.

    An *empty* classification (both ``None``) means the driver has no protocol
    opinion about the failure; the hub then re-raises the original exception.
    """

    blocker: CapabilityBlocker | None = None
    user_error: Exception | None = None

    @property
    def is_empty(self) -> bool:
        """Return whether the driver expressed no opinion about the failure."""

        return self.blocker is None and self.user_error is None


EMPTY_WRITE_ERROR_CLASSIFICATION = WriteErrorClassification()
