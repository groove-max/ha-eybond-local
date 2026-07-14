"""Base abstractions for inverter drivers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

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
from ..poll_policy import DEFAULT_POLL_POLICY, PollPolicy
from .support_marker import DriverSupportMarker
from .support_probe import SupportProbeRequest
from .write_error import EMPTY_WRITE_ERROR_CLASSIFICATION, WriteErrorClassification

if TYPE_CHECKING:
    from .read_result import DriverReadResult


class InverterDriver(ABC):
    """A probeable inverter payload driver."""

    key: str
    name: str
    profile_name: str = ""
    register_schema_name: str = ""
    signature_timeout: float | None = None
    probe_timeout: float | None = None
    probe_targets: tuple[ProbeTarget, ...]
    measurements: tuple[MeasurementDescription, ...]
    binary_sensors: tuple[BinarySensorDescription, ...] = ()
    capability_groups: tuple[CapabilityGroup, ...] = ()
    write_capabilities: tuple[WriteCapability, ...] = ()
    capability_presets: tuple[CapabilityPreset, ...] = ()

    # A driver with a single, model-independent timing envelope just sets this
    # class attribute (e.g. ``poll_policy = PI30_POLL_POLICY`` in pi30.py). A
    # driver whose policy depends on the detected model overrides
    # ``poll_policy_for`` instead. The base default is the neutral policy.
    poll_policy: PollPolicy = DEFAULT_POLL_POLICY

    def poll_policy_for(
        self,
        inverter: DetectedInverter | None = None,
    ) -> PollPolicy:
        """Return the adaptive polling policy for this driver / detected model.

        The base returns the declared ``poll_policy`` class attribute. It is a
        method (not just the attribute) so a single catalog driver can serve
        several models with different timing envelopes by overriding it and
        reading model identity from ``inverter``. The argument is any object that
        exposes model identity (a ``DetectedInverter`` at runtime, or a
        ``DriverMatch`` during onboarding -- both carry ``variant_key`` /
        ``model_name``), or ``None`` before identity is known. The runtime never
        selects the policy -- it only consumes the returned value.
        """

        return self.poll_policy

    def serial_is_stable(self, inverter: DetectedInverter | None = None) -> bool:
        """Return whether this driver/model exposes a STABLE device serial.

        Model policy owned by the driver: most inverters report a stable serial,
        so the base returns ``True``. A family that has no stable serial (so a
        captured serial must not be persisted as the entry's detected serial)
        overrides this and reads model identity from ``inverter``. The runtime
        only consumes the neutral answer -- it never encodes a variant rule.
        """

        return True

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

    def classify_write_error(
        self,
        capability: WriteCapability,
        exc: BaseException,
        *,
        operating_mode: object = None,
    ) -> WriteErrorClassification:
        """Classify a failed capability write. Base drivers have no opinion.

        Protocol policy: a driver that understands its wire error format (e.g. a
        Modbus driver) overrides this -- typically by mixing in a shared
        classifier -- to return a durable blocker or a user-facing error. The
        neutral runtime only consumes the returned verdict.
        """

        return EMPTY_WRITE_ERROR_CLASSIFICATION

    def support_probe_plan(self) -> tuple[SupportProbeRequest, ...]:
        """Return bounded read-only support-probe requests. Base: none.

        A driver whose inverters answer a raw diagnostic sweep (e.g. ASCII query
        commands) owns and builds its request bytes here. The hub is a neutral
        executor and never constructs protocol requests itself.
        """

        return ()

    def support_marker(
        self,
        *,
        variant_key: str = "",
        profile_name: str = "",
    ) -> DriverSupportMarker | None:
        """Return a special support-state marker for this bound identity, if any.

        Base drivers expose no special marker. A driver that owns a read-only /
        unverified fallback state decides it here from its own model identity;
        the support layers only render the returned neutral marker.
        """

        return None

    async def async_capture_support_evidence(
        self,
        transport: PayloadLinkTransport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        """Return driver-specific raw evidence for support/debug packages."""

        return {}

    async def async_probe_signature(
        self,
        transport: PayloadLinkTransport,
        target: ProbeTarget,
    ) -> bool:
        """Return whether a cheap protocol signature matches this driver."""

        return False

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
    ) -> "dict[str, Any] | DriverReadResult":
        """Read and decode the current inverter state.

        A driver returns either:

        * a bare ``dict`` -- interpreted as a FULL runtime snapshot (the legacy
          contract; the runtime replaces its measurement state with it), or
        * a :class:`~.read_result.DriverReadResult` -- an explicit FULL or DELTA
          update. A driver whose cycle may omit some measurements (e.g. PI30,
          whose optional/energy commands can fail transiently, be skipped as
          unsupported, or early-exit) MUST return ``DELTA`` so the runtime
          overlays the read values onto the last-good ones instead of reverting
          the omitted ones to detection-time defaults, and invalidates removed
          values explicitly via ``removed_keys``.

        The runtime never guesses full-vs-delta from the driver key; the mode is
        carried in the typed result.
        """

    async def async_read_onboarding_values(
        self,
        transport: PayloadLinkTransport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        """Read only the values needed to enrich onboarding confirmation UI."""

        from .read_result import coerce_driver_read_result

        raw = await self.async_read_values(transport, inverter)
        return coerce_driver_read_result(raw, driver_key=self.key).values

    @abstractmethod
    async def async_write_capability(
        self,
        transport: PayloadLinkTransport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        """Validate and write one logical capability value to the inverter."""
