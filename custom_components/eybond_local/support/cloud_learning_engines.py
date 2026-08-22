"""Typed registry for provider-specific cloud learning engines.

Cloud evidence providers answer which cloud ecosystem owns persisted evidence.
Learning engines answer which API surface performs one transient discovery run.
Those axes intentionally differ: more than one learning source may serve the
same provider and credential realm without sharing algorithms or evidence.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..dessmonitor_cloud import (
    DessMonitorCloudError,
)
from ..smartess_cloud import SmartEssCloudError, classify_smartess_cloud_error
from .cloud_control_discovery import (
    CloudControlDiscoveryRunner,
    SmartEssControlDiscoveryRunner,
    UnavailableControlDiscoveryRunner,
    ValueCloudControlDiscoveryRunner,
)
from .dessmonitor_learning import DessMonitorReadOnlyLearningRunner


LEARNING_SOURCE_SMARTESS = "smartess"
LEARNING_SOURCE_DESSMONITOR = "dessmonitor"
LEARNING_SOURCE_VALUECLOUD = "valuecloud"

CREDENTIAL_REALM_EYBOND = "eybond"
CREDENTIAL_REALM_VALUECLOUD = "valuecloud"


def _required_token(value: object, *, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True)
class CloudLearningCapabilities:
    """Closed capability declaration for one learning source."""

    metadata: bool
    control_actions: bool
    raw_packets: bool
    history: bool
    requires_shadow_route: bool
    requires_control_consent: bool

    def __post_init__(self) -> None:
        for value in (
            self.metadata,
            self.control_actions,
            self.raw_packets,
            self.history,
            self.requires_shadow_route,
            self.requires_control_consent,
        ):
            if type(value) is not bool:
                raise TypeError("cloud_learning_capability_invalid")
        if self.requires_shadow_route and not self.control_actions:
            raise ValueError("cloud_learning_shadow_route_without_controls")
        if self.requires_control_consent and not self.control_actions:
            raise ValueError("cloud_learning_consent_without_controls")


@dataclass(frozen=True, slots=True)
class CloudLearningSource:
    """Presentation and trust metadata for one selectable API surface."""

    source_id: str
    provider_id: str
    credential_realm_id: str
    label: str
    capabilities: CloudLearningCapabilities
    default_for_provider: bool = False

    def __post_init__(self) -> None:
        _required_token(self.source_id, reason="cloud_learning_source_id_invalid")
        _required_token(self.provider_id, reason="cloud_learning_provider_id_invalid")
        _required_token(
            self.credential_realm_id,
            reason="cloud_learning_credential_realm_invalid",
        )
        _required_token(self.label, reason="cloud_learning_label_invalid")
        if type(self.capabilities) is not CloudLearningCapabilities:
            raise TypeError("cloud_learning_capabilities_invalid")
        if type(self.default_for_provider) is not bool:
            raise TypeError("cloud_learning_default_invalid")


class CloudLearningEngine(ABC):
    """One isolated API-specific learning implementation."""

    source: CloudLearningSource

    @property
    def available(self) -> bool:
        return True

    @abstractmethod
    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        """Return a fresh provider-owned runner for one transient flow run."""

    def classify_error(self, exc: BaseException) -> str:
        """Return a stable provider-owned error code, or empty if unrelated."""

        return ""


class SmartEssCloudLearningEngine(CloudLearningEngine):
    source = CloudLearningSource(
        source_id=LEARNING_SOURCE_SMARTESS,
        provider_id="smartess",
        credential_realm_id=CREDENTIAL_REALM_EYBOND,
        label="SmartESS-compatible cloud",
        capabilities=CloudLearningCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=False,
            history=False,
            requires_shadow_route=True,
            requires_control_consent=True,
        ),
        default_for_provider=True,
    )

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return SmartEssControlDiscoveryRunner()

    def classify_error(self, exc: BaseException) -> str:
        if not isinstance(exc, (SmartEssCloudError, TimeoutError)):
            return ""
        return classify_smartess_cloud_error(exc)


class ValueCloudCloudLearningEngine(CloudLearningEngine):
    source = CloudLearningSource(
        source_id=LEARNING_SOURCE_VALUECLOUD,
        provider_id="valuecloud",
        credential_realm_id=CREDENTIAL_REALM_VALUECLOUD,
        label="ValueCloud",
        capabilities=CloudLearningCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=False,
            history=False,
            requires_shadow_route=True,
            requires_control_consent=True,
        ),
        default_for_provider=True,
    )

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return ValueCloudControlDiscoveryRunner()


class DessMonitorCloudLearningEngine(CloudLearningEngine):
    """DESSMonitor metadata learning; never redirects or writes the collector."""

    source = CloudLearningSource(
        source_id=LEARNING_SOURCE_DESSMONITOR,
        provider_id="smartess",
        credential_realm_id=CREDENTIAL_REALM_EYBOND,
        label="DESSMonitor API (read-only)",
        capabilities=CloudLearningCapabilities(
            metadata=True,
            control_actions=False,
            raw_packets=True,
            history=False,
            requires_shadow_route=False,
            requires_control_consent=False,
        ),
        default_for_provider=False,
    )

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return DessMonitorReadOnlyLearningRunner()

    def classify_error(self, exc: BaseException) -> str:
        if isinstance(exc, TimeoutError):
            return "timeout"
        if not isinstance(exc, DessMonitorCloudError):
            return ""
        message = str(exc)
        if message.startswith("login_failed") or message.startswith("http_error:40"):
            return "auth_failed"
        if message.startswith("http_error:429"):
            return "rate_limited"
        if message.startswith("network_error"):
            return "network"
        if message.startswith("http_error:5") or message.startswith("invalid_"):
            return "unavailable"
        return "unexpected"


class UnavailableCloudLearningEngine(CloudLearningEngine):
    def __init__(self, requested_source_id: str = "") -> None:
        requested = (
            requested_source_id
            if type(requested_source_id) is str
            and requested_source_id == requested_source_id.strip()
            else ""
        )
        self._requested = requested
        self.source = CloudLearningSource(
            source_id=requested or "unknown",
            provider_id="unavailable",
            credential_realm_id="unavailable",
            label="Unavailable cloud source",
            capabilities=CloudLearningCapabilities(
                metadata=False,
                control_actions=False,
                raw_packets=False,
                history=False,
                requires_shadow_route=False,
                requires_control_consent=False,
            ),
            default_for_provider=False,
        )

    @property
    def available(self) -> bool:
        return False

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return UnavailableControlDiscoveryRunner(self._requested)


_ENGINES: dict[str, CloudLearningEngine] = {
    LEARNING_SOURCE_DESSMONITOR: DessMonitorCloudLearningEngine(),
    LEARNING_SOURCE_SMARTESS: SmartEssCloudLearningEngine(),
    LEARNING_SOURCE_VALUECLOUD: ValueCloudCloudLearningEngine(),
}


def supported_cloud_learning_sources() -> tuple[CloudLearningSource, ...]:
    """Return every real source in stable source-id order."""

    return tuple(_ENGINES[key].source for key in sorted(_ENGINES))


def compatible_cloud_learning_sources(provider_id: object) -> tuple[CloudLearningSource, ...]:
    """Return sources owned by one exact normalized evidence provider."""

    if type(provider_id) is not str or provider_id != provider_id.strip():
        return ()
    return tuple(
        source
        for source in supported_cloud_learning_sources()
        if source.provider_id == provider_id
    )


def default_cloud_learning_source(provider_id: object) -> str:
    """Return the sole declared default, fail closed when none/ambiguous."""

    compatible = compatible_cloud_learning_sources(provider_id)
    defaults = tuple(source for source in compatible if source.default_for_provider)
    return defaults[0].source_id if len(defaults) == 1 else ""


def resolve_cloud_learning_engine(source_id: object) -> CloudLearningEngine:
    """Resolve one exact source id; malformed/unknown values fail closed."""

    if type(source_id) is not str or source_id != source_id.strip():
        return UnavailableCloudLearningEngine()
    return _ENGINES.get(source_id, UnavailableCloudLearningEngine(source_id))


__all__ = [
    "CREDENTIAL_REALM_EYBOND",
    "CREDENTIAL_REALM_VALUECLOUD",
    "LEARNING_SOURCE_DESSMONITOR",
    "LEARNING_SOURCE_SMARTESS",
    "LEARNING_SOURCE_VALUECLOUD",
    "CloudLearningCapabilities",
    "CloudLearningEngine",
    "CloudLearningSource",
    "DessMonitorCloudLearningEngine",
    "SmartEssCloudLearningEngine",
    "UnavailableCloudLearningEngine",
    "ValueCloudCloudLearningEngine",
    "compatible_cloud_learning_sources",
    "default_cloud_learning_source",
    "resolve_cloud_learning_engine",
    "supported_cloud_learning_sources",
]
