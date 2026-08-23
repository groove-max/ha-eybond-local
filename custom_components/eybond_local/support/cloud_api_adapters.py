"""Typed metadata and error adapters for supported cloud API surfaces."""

from __future__ import annotations

from abc import ABC

from ..dessmonitor_cloud import DessMonitorCloudError
from ..smartess_cloud import SmartEssCloudError, classify_smartess_cloud_error
from .cloud_learning_models import CloudApiCapabilities, CloudApiSource


LEARNING_SOURCE_SMARTESS = "smartess"
LEARNING_SOURCE_DESSMONITOR = "dessmonitor"
LEARNING_SOURCE_VALUECLOUD = "valuecloud"

CREDENTIAL_REALM_EYBOND = "eybond"
CREDENTIAL_REALM_VALUECLOUD = "valuecloud"


class CloudApiAdapter(ABC):
    """Identity, capabilities and error vocabulary of one cloud API."""

    source: CloudApiSource

    def classify_error(self, exc: BaseException) -> str:
        """Return a stable source-owned error code, or empty if unrelated."""

        return ""


class SmartEssCloudApiAdapter(CloudApiAdapter):
    source = CloudApiSource(
        source_id=LEARNING_SOURCE_SMARTESS,
        provider_id="smartess",
        credential_realm_id=CREDENTIAL_REALM_EYBOND,
        label="SmartESS-compatible cloud",
        capabilities=CloudApiCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=False,
            history=True,
        ),
    )

    def classify_error(self, exc: BaseException) -> str:
        if not isinstance(exc, (SmartEssCloudError, TimeoutError)):
            return ""
        return classify_smartess_cloud_error(exc)


class ValueCloudApiAdapter(CloudApiAdapter):
    source = CloudApiSource(
        source_id=LEARNING_SOURCE_VALUECLOUD,
        provider_id="valuecloud",
        credential_realm_id=CREDENTIAL_REALM_VALUECLOUD,
        label="ValueCloud",
        capabilities=CloudApiCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=False,
            history=False,
        ),
    )


class DessMonitorCloudApiAdapter(CloudApiAdapter):
    source = CloudApiSource(
        source_id=LEARNING_SOURCE_DESSMONITOR,
        provider_id="smartess",
        credential_realm_id=CREDENTIAL_REALM_EYBOND,
        label="DESSMonitor API",
        capabilities=CloudApiCapabilities(
            metadata=True,
            control_actions=True,
            raw_packets=True,
            history=True,
        ),
    )

    def classify_error(self, exc: BaseException) -> str:
        if isinstance(exc, TimeoutError):
            return "timeout"
        if not isinstance(exc, DessMonitorCloudError):
            return ""
        message = str(exc)
        # ERR_NO_AUTH (10) and HTTP authentication failures have known auth
        # semantics. Other provider login codes are undocumented; production
        # has returned code 16 transiently and then accepted the same credentials.
        # Never tell the user to change a valid password based on an unknown code.
        if message == "login_failed:10" or message.startswith("http_error:40"):
            return "auth_failed"
        if message.startswith("login_failed"):
            return "unexpected"
        if message.startswith("http_error:429"):
            return "rate_limited"
        if message.startswith("network_error"):
            return "network"
        if message.startswith("http_error:5") or message.startswith("invalid_"):
            return "unavailable"
        return "unexpected"


class UnavailableCloudApiAdapter(CloudApiAdapter):
    """Fail-closed API metadata for an unknown exact source id."""

    def __init__(self, source_id: str) -> None:
        self.source = CloudApiSource(
            source_id=source_id,
            provider_id="unavailable",
            credential_realm_id="unavailable",
            label="Unavailable cloud source",
            capabilities=CloudApiCapabilities(
                metadata=False,
                control_actions=False,
                raw_packets=False,
                history=False,
            ),
        )


__all__ = [
    "CREDENTIAL_REALM_EYBOND",
    "CREDENTIAL_REALM_VALUECLOUD",
    "LEARNING_SOURCE_DESSMONITOR",
    "LEARNING_SOURCE_SMARTESS",
    "LEARNING_SOURCE_VALUECLOUD",
    "CloudApiAdapter",
    "DessMonitorCloudApiAdapter",
    "SmartEssCloudApiAdapter",
    "UnavailableCloudApiAdapter",
    "ValueCloudApiAdapter",
]
