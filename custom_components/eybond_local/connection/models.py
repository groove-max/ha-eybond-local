"""Connection-type models for future multi-link onboarding/runtime support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .confirmed_session_protocol import ConfirmedSessionProtocolEvidence


ConnectionType = Literal["eybond"]


@dataclass(frozen=True, slots=True)
class ConnectionSpec:
    """Base connection metadata shared by all future link types."""

    type: ConnectionType


@dataclass(frozen=True, slots=True)
class EybondConnectionSpec(ConnectionSpec):
    """Physical/discovery settings for one EyeBond collector-based link."""

    server_ip: str
    advertised_server_ip: str
    tcp_port: int
    advertised_tcp_port: int
    udp_port: int
    collector_ip: str
    collector_pn: str
    collector_cloud_family: str
    # Metadata-derived candidate for one exact-session, read-only identity
    # challenge. It is deliberately separate from the confirmed wire below and
    # can never route or own a socket by itself.
    collector_identity_challenge_protocol: str
    # Session profile selected from validated, PN-bound live wire evidence.
    # Cloud/driver/endpoint metadata can never populate this field by itself.
    collector_configured_session_protocol: str
    collector_identity_strategy: str
    collector_raw_passthrough_bootstrap: str
    collector_raw_passthrough_frame_format: str
    collector_raw_passthrough_min_interval_ms: int
    # CONFIRMED-live wire evidence, typed so the field itself is the guarantee:
    # the constructor drops anything that is not a genuine
    # ``ConfirmedSessionProtocolEvidence`` to ``None``, and the seed path
    # re-validates provenance again at its trust boundary. A direct ConnectionSpec
    # construction therefore cannot inject a duck-typed/forged confirmed protocol.
    confirmed_session_protocol_evidence: ConfirmedSessionProtocolEvidence | None
    discovery_target: str
    discovery_interval: int
    heartbeat_interval: int
    request_timeout: float

    def __init__(
        self,
        *,
        server_ip: str,
        advertised_server_ip: str = "",
        tcp_port: int,
        advertised_tcp_port: int = 0,
        udp_port: int,
        collector_ip: str = "",
        collector_pn: str = "",
        collector_cloud_family: str = "",
        collector_identity_challenge_protocol: str = "",
        collector_configured_session_protocol: str = "",
        collector_identity_strategy: str = "",
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
        confirmed_session_protocol_evidence: ConfirmedSessionProtocolEvidence | None = None,
        discovery_target: str = "",
        discovery_interval: int,
        heartbeat_interval: int,
        request_timeout: float,
    ) -> None:
        object.__setattr__(self, "type", "eybond")
        object.__setattr__(self, "server_ip", server_ip)
        object.__setattr__(self, "advertised_server_ip", advertised_server_ip)
        object.__setattr__(self, "tcp_port", int(tcp_port))
        object.__setattr__(self, "advertised_tcp_port", int(advertised_tcp_port or 0))
        object.__setattr__(self, "udp_port", int(udp_port))
        object.__setattr__(self, "collector_ip", collector_ip)
        object.__setattr__(self, "collector_pn", collector_pn)
        object.__setattr__(self, "collector_cloud_family", collector_cloud_family)
        object.__setattr__(
            self,
            "collector_identity_challenge_protocol",
            collector_identity_challenge_protocol,
        )
        object.__setattr__(
            self,
            "collector_configured_session_protocol",
            collector_configured_session_protocol,
        )
        object.__setattr__(self, "collector_identity_strategy", collector_identity_strategy)
        object.__setattr__(
            self,
            "collector_raw_passthrough_bootstrap",
            collector_raw_passthrough_bootstrap,
        )
        object.__setattr__(
            self,
            "collector_raw_passthrough_frame_format",
            collector_raw_passthrough_frame_format,
        )
        object.__setattr__(
            self,
            "collector_raw_passthrough_min_interval_ms",
            max(0, int(collector_raw_passthrough_min_interval_ms or 0)),
        )
        # Type invariant enforced here: only a genuine validated evidence
        # instance is stored; a forged/duck-typed object is dropped to None so it
        # can never reach the seed path. Provenance (source/protocol/PN/identity)
        # is re-validated again at the seed trust boundary.
        object.__setattr__(
            self,
            "confirmed_session_protocol_evidence",
            confirmed_session_protocol_evidence
            if isinstance(
                confirmed_session_protocol_evidence, ConfirmedSessionProtocolEvidence
            )
            else None,
        )
        object.__setattr__(self, "discovery_target", discovery_target)
        object.__setattr__(self, "discovery_interval", int(discovery_interval))
        object.__setattr__(self, "heartbeat_interval", int(heartbeat_interval))
        object.__setattr__(self, "request_timeout", float(request_timeout))

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the endpoint IP that will be advertised to the collector."""

        return self.advertised_server_ip or self.server_ip

    @property
    def effective_advertised_tcp_port(self) -> int:
        """Return the endpoint TCP port that will be advertised to the collector."""

        return self.advertised_tcp_port or self.tcp_port
