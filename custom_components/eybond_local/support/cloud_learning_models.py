"""Strict neutral models for cloud-learning source and workflow selection."""

from __future__ import annotations

from dataclasses import dataclass


LEARNING_METHOD_READ_ONLY_EVIDENCE = "read_only_evidence"
LEARNING_METHOD_ACTIVE_CORRELATION = "active_correlation"

_METHOD_SHAPES: dict[str, tuple[bool, bool, bool, bool]] = {
    LEARNING_METHOD_READ_ONLY_EVIDENCE: (True, False, False, False),
    LEARNING_METHOD_ACTIVE_CORRELATION: (True, True, True, True),
}


def _required_token(value: object, *, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True)
class CloudApiCapabilities:
    """Provider API features, independent of any learning workflow."""

    metadata: bool
    control_actions: bool
    raw_packets: bool
    history: bool

    def __post_init__(self) -> None:
        for value in (
            self.metadata,
            self.control_actions,
            self.raw_packets,
            self.history,
        ):
            if type(value) is not bool:
                raise TypeError("cloud_api_capability_invalid")


@dataclass(frozen=True, slots=True)
class CloudApiSource:
    """Identity, credentials and capabilities of one exact cloud API."""

    source_id: str
    provider_id: str
    credential_realm_id: str
    label: str
    capabilities: CloudApiCapabilities

    def __post_init__(self) -> None:
        _required_token(self.source_id, reason="cloud_api_source_id_invalid")
        _required_token(self.provider_id, reason="cloud_api_provider_id_invalid")
        _required_token(
            self.credential_realm_id,
            reason="cloud_api_credential_realm_invalid",
        )
        _required_token(self.label, reason="cloud_api_label_invalid")
        if type(self.capabilities) is not CloudApiCapabilities:
            raise TypeError("cloud_api_capabilities_invalid")


@dataclass(frozen=True, slots=True)
class CloudLearningEvidenceCapabilities:
    """Local evidence features of one exact method/source binding."""

    local_register_snapshot: bool
    local_register_series: bool

    def __post_init__(self) -> None:
        for value in (self.local_register_snapshot, self.local_register_series):
            if type(value) is not bool:
                raise TypeError("cloud_learning_evidence_capability_invalid")
        if self.local_register_series and not self.local_register_snapshot:
            raise ValueError("cloud_learning_series_without_snapshot")


@dataclass(frozen=True, slots=True)
class CloudLearningMethod:
    """Workflow policy, independent of the API used to execute it."""

    method_id: str
    requires_metadata: bool
    requires_control_actions: bool
    requires_shadow_route: bool
    requires_control_consent: bool

    def __post_init__(self) -> None:
        method_id = _required_token(
            self.method_id,
            reason="cloud_learning_method_id_invalid",
        )
        for value in (
            self.requires_metadata,
            self.requires_control_actions,
            self.requires_shadow_route,
            self.requires_control_consent,
        ):
            if type(value) is not bool:
                raise TypeError("cloud_learning_method_capability_invalid")
        expected = _METHOD_SHAPES.get(method_id)
        if expected is None:
            raise ValueError("cloud_learning_method_unknown")
        actual = (
            self.requires_metadata,
            self.requires_control_actions,
            self.requires_shadow_route,
            self.requires_control_consent,
        )
        if actual != expected:
            raise ValueError("cloud_learning_method_shape_invalid")
        if self.requires_shadow_route and not self.requires_control_actions:
            raise ValueError("cloud_learning_shadow_route_without_controls")
        if self.requires_control_consent and not self.requires_control_actions:
            raise ValueError("cloud_learning_consent_without_controls")


@dataclass(frozen=True, slots=True)
class CloudLearningSelection:
    """One explicit product method bound to one exact API source."""

    method_id: str
    source_id: str

    def __post_init__(self) -> None:
        method_id = _required_token(
            self.method_id,
            reason="cloud_learning_selection_method_invalid",
        )
        if method_id not in _METHOD_SHAPES:
            raise ValueError("cloud_learning_selection_method_unknown")
        _required_token(self.source_id, reason="cloud_learning_selection_source_invalid")


READ_ONLY_EVIDENCE_METHOD = CloudLearningMethod(
    method_id=LEARNING_METHOD_READ_ONLY_EVIDENCE,
    requires_metadata=True,
    requires_control_actions=False,
    requires_shadow_route=False,
    requires_control_consent=False,
)

ACTIVE_CORRELATION_METHOD = CloudLearningMethod(
    method_id=LEARNING_METHOD_ACTIVE_CORRELATION,
    requires_metadata=True,
    requires_control_actions=True,
    requires_shadow_route=True,
    requires_control_consent=True,
)

NO_LOCAL_EVIDENCE = CloudLearningEvidenceCapabilities(
    local_register_snapshot=False,
    local_register_series=False,
)

LOCAL_SNAPSHOT_EVIDENCE = CloudLearningEvidenceCapabilities(
    local_register_snapshot=True,
    local_register_series=False,
)

LOCAL_SERIES_EVIDENCE = CloudLearningEvidenceCapabilities(
    local_register_snapshot=True,
    local_register_series=True,
)


def source_supports_method(
    source: CloudApiSource,
    method: CloudLearningMethod,
) -> bool:
    """Return whether one API can satisfy one workflow's declared inputs."""

    if type(source) is not CloudApiSource or type(method) is not CloudLearningMethod:
        return False
    capabilities = source.capabilities
    if method.requires_metadata and not capabilities.metadata:
        return False
    if method.requires_control_actions and not capabilities.control_actions:
        return False
    return True


__all__ = [
    "ACTIVE_CORRELATION_METHOD",
    "LEARNING_METHOD_ACTIVE_CORRELATION",
    "LEARNING_METHOD_READ_ONLY_EVIDENCE",
    "LOCAL_SERIES_EVIDENCE",
    "LOCAL_SNAPSHOT_EVIDENCE",
    "NO_LOCAL_EVIDENCE",
    "READ_ONLY_EVIDENCE_METHOD",
    "CloudApiCapabilities",
    "CloudApiSource",
    "CloudLearningMethod",
    "CloudLearningEvidenceCapabilities",
    "CloudLearningSelection",
    "source_supports_method",
]
