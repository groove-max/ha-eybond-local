"""Typed serial-identity evidence shared by drivers and runtime projections.

The wire may expose a field named "serial number" without guaranteeing that
it is initialized or unique.  Drivers classify that report before the neutral
runtime sees it: only ``TRUSTED`` evidence may become the canonical
``DetectedInverter.serial_number``.  Raw reports remain diagnostics, never an
implicit identity fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


KNOWN_UNTRUSTED_SERIAL_REPORTS: frozenset[str] = frozenset(
    {
        # Observed verbatim on multiple unrelated PI30 inverters and collector
        # PNs.  It is a factory/default report, not a device identity.
        "55355535553555",
    }
)


def serial_report_is_known_untrusted(value: object) -> bool:
    """Return whether an exact raw serial report is known to be non-unique."""

    return type(value) is str and value in KNOWN_UNTRUSTED_SERIAL_REPORTS


class SerialIdentitySource(Enum):
    """Wire source that reported a candidate inverter serial."""

    NONE = "none"
    QID = "qid"
    QSID = "qsid"


class SerialIdentityTrust(Enum):
    """Whether a reported serial may cross the driver identity boundary."""

    TRUSTED = "trusted"
    UNTRUSTED = "untrusted"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class SerialIdentityEvidence:
    """One strictly validated driver-owned serial classification."""

    reported: str
    canonical: str
    source: SerialIdentitySource
    trust: SerialIdentityTrust
    reason: str = ""

    def __post_init__(self) -> None:
        for field_name in ("reported", "canonical", "reason"):
            value = getattr(self, field_name)
            if type(value) is not str:
                raise TypeError(f"serial_identity_{field_name}_not_string")
            if value != value.strip():
                raise ValueError(f"serial_identity_{field_name}_not_normalized")
        if self.reported and (
            not self.reported.isascii()
            or any(not character.isprintable() for character in self.reported)
        ):
            raise ValueError("serial_identity_reported_invalid_ascii")
        if type(self.source) is not SerialIdentitySource:
            raise TypeError("serial_identity_source_invalid")
        if type(self.trust) is not SerialIdentityTrust:
            raise TypeError("serial_identity_trust_invalid")

        if self.trust is SerialIdentityTrust.TRUSTED:
            if (
                not self.reported
                or self.canonical != self.reported
                or serial_report_is_known_untrusted(self.reported)
                or self.source is SerialIdentitySource.NONE
                or self.reason
            ):
                raise ValueError("serial_identity_trusted_shape_invalid")
            return
        if self.trust is SerialIdentityTrust.UNTRUSTED:
            if (
                not self.reported
                or self.canonical
                or self.source is SerialIdentitySource.NONE
                or not self.reason
            ):
                raise ValueError("serial_identity_untrusted_shape_invalid")
            return
        if (
            self.reported
            or self.canonical
            or self.source is not SerialIdentitySource.NONE
            or not self.reason
        ):
            raise ValueError("serial_identity_unavailable_shape_invalid")

    @classmethod
    def trusted(
        cls,
        reported: str,
        *,
        source: SerialIdentitySource,
    ) -> "SerialIdentityEvidence":
        """Build accepted serial evidence."""

        return cls(
            reported=reported,
            canonical=reported,
            source=source,
            trust=SerialIdentityTrust.TRUSTED,
        )

    @classmethod
    def untrusted(
        cls,
        reported: str,
        *,
        source: SerialIdentitySource,
        reason: str,
    ) -> "SerialIdentityEvidence":
        """Build a raw report that must not become inverter identity."""

        return cls(
            reported=reported,
            canonical="",
            source=source,
            trust=SerialIdentityTrust.UNTRUSTED,
            reason=reason,
        )

    @classmethod
    def unavailable(cls, *, reason: str) -> "SerialIdentityEvidence":
        """Build evidence for a device that exposed no usable serial report."""

        return cls(
            reported="",
            canonical="",
            source=SerialIdentitySource.NONE,
            trust=SerialIdentityTrust.UNAVAILABLE,
            reason=reason,
        )

    def as_details(self) -> dict[str, str]:
        """Return the stable support-diagnostics projection."""

        return {
            "reported_serial_number": self.reported,
            "serial_identity_source": self.source.value,
            "serial_identity_trust": self.trust.value,
            "serial_identity_reason": self.reason,
        }


__all__ = [
    "KNOWN_UNTRUSTED_SERIAL_REPORTS",
    "SerialIdentityEvidence",
    "SerialIdentitySource",
    "SerialIdentityTrust",
    "serial_report_is_known_untrusted",
]
