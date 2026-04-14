"""Base abstractions for inverter drivers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..link_transport import PayloadLinkTransport
from ..models import (
    BinarySensorDescription,
    CapabilityGroup,
    CapabilityPreset,
    DetectedInverter,
    MeasurementDescription,
    ProbeTarget,
    WriteCapability,
)


class InverterDriver(ABC):
    """A probeable inverter payload driver."""

    key: str
    name: str
    profile_name: str = ""
    register_schema_name: str = ""
    probe_targets: tuple[ProbeTarget, ...]
    measurements: tuple[MeasurementDescription, ...]
    binary_sensors: tuple[BinarySensorDescription, ...] = ()
    capability_groups: tuple[CapabilityGroup, ...] = ()
    write_capabilities: tuple[WriteCapability, ...] = ()
    capability_presets: tuple[CapabilityPreset, ...] = ()

    @property
    def profile_metadata(self):
        """Return effective declarative capability metadata when available."""

        if not self.profile_name:
            return None
        from ..metadata.profile_loader import load_driver_profile

        return load_driver_profile(self.profile_name)

    @property
    def register_schema_metadata(self):
        """Return effective declarative register schema metadata when available."""

        if not self.register_schema_name:
            return None
        from ..metadata.register_schema_loader import load_register_schema

        return load_register_schema(self.register_schema_name)

    async def async_capture_support_evidence(
        self,
        transport: PayloadLinkTransport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        """Return driver-specific raw evidence for support/debug packages."""

        return {}

    @abstractmethod
    async def async_probe(
        self,
        transport: PayloadLinkTransport,
        target: ProbeTarget,
    ) -> DetectedInverter | None:
        """Try to identify a matching inverter behind the collector."""

    @abstractmethod
    async def async_read_values(
        self,
        transport: PayloadLinkTransport,
        inverter: DetectedInverter,
        *,
        runtime_state: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        now_monotonic: float | None = None,
    ) -> dict[str, Any]:
        """Read and decode the current inverter state."""

    @abstractmethod
    async def async_write_capability(
        self,
        transport: PayloadLinkTransport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        """Validate and write one logical capability value to the inverter."""
