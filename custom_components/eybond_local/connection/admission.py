"""Neutral typed models for the config-flow collector-admission boundary.

These types are the ONE trust boundary both admission source adapters
(integration discovery and the passive phase of a user-started scan) share.
They live in the neutral ``connection`` layer -- NOT in the base ``models``
module -- specifically so the base data models take no back-dependency on the
``onboarding`` package. The only thing this module reaches for is the
centralized identity rule in :mod:`.session_registry`; it imports nothing from
``onboarding`` and is never a RecoveryProof.
"""

from __future__ import annotations

from dataclasses import dataclass

from .recovery.verification import CallbackRecoveryRoute


@dataclass(frozen=True, slots=True)
class ObservedCollectorSession:
    """One physical collector callback session observed on the shared listener.

    A NEUTRAL record of observed facts only -- the single typed carrier the
    config-flow admission boundary trusts instead of a loose
    ``detection.details`` dict. It deliberately does NOT:

    * decide a ``connection_strategy`` -- an observed inbound socket is only the
      STARTING point of a controlled recovery experiment, never proof of a
      permanent inbound configuration;
    * turn a weak heartbeat PN into durable identity -- ``identity_source`` is
      recorded verbatim and strong/weak is decided ONLY by the centralized
      ``identity_source_is_strong`` (see :attr:`has_strong_identity`);
    * treat ``peer_hint`` as identity or a callback route -- it is a display /
      diagnostic hint only, and a NAT/router address is never ownership;
    * carry any RecoveryProof, and it is never serialized into an entry.

    Strict trust boundary: every string field must already be normalized
    (stripped, no padding), ``collector_pn`` and ``session_id`` are required,
    and ``listener_port`` must be a valid TCP port. Duck-typed / padded /
    non-string values are rejected so a caller cannot smuggle unvalidated data
    across the boundary.
    """

    collector_pn: str
    identity_source: str
    session_id: str
    listener_port: int
    protocol_shape: str = ""
    peer_hint: str = ""

    def __post_init__(self) -> None:
        for label, value, required in (
            ("collector_pn", self.collector_pn, True),
            ("identity_source", self.identity_source, False),
            ("session_id", self.session_id, True),
            ("protocol_shape", self.protocol_shape, False),
            ("peer_hint", self.peer_hint, False),
        ):
            if type(value) is not str:
                raise TypeError(f"observed_collector_session_{label}_must_be_str")
            if value != value.strip():
                raise ValueError(f"observed_collector_session_{label}_not_normalized")
            if required and not value:
                raise ValueError(f"observed_collector_session_{label}_required")
        # ``type(x) is int`` also rejects ``bool`` (a subclass of ``int``).
        if type(self.listener_port) is not int:
            raise TypeError("observed_collector_session_listener_port_must_be_int")
        if not 1 <= self.listener_port <= 65535:
            raise ValueError("observed_collector_session_listener_port_invalid")

    @property
    def has_strong_identity(self) -> bool:
        """Whether this observation is authoritative identity.

        Never inferred here: delegated to the ONE centralized rule so a weak
        heartbeat PN is never silently promoted to a durable identity.
        """

        from .session_registry import identity_source_is_strong

        return identity_source_is_strong(self.identity_source)


@dataclass(frozen=True, slots=True)
class CollectorAdmissionRequest:
    """The ONE typed input to the config-flow collector-admission entrypoint.

    Both source adapters -- integration discovery and the passive phase of a
    user-started scan -- build this and nothing else; once constructed, the
    admission algorithm cannot tell where it came from. ``origin`` is a
    DIAGNOSTIC label only and must never influence the algorithm. This request
    is not a RecoveryProof and is never serialized into an entry.

    ``callback_route`` is present only when an ACTIVE scan actually exercised
    that exact route. Its presence authorizes the existing callback recovery
    transaction; it is never inferred from ``origin`` or a TCP peer address.
    """

    observed_session: ObservedCollectorSession
    origin: str = ""
    callback_route: CallbackRecoveryRoute | None = None

    def __post_init__(self) -> None:
        if type(self.observed_session) is not ObservedCollectorSession:
            raise TypeError("collector_admission_request_observed_session_invalid")
        if type(self.origin) is not str:
            raise TypeError("collector_admission_request_origin_must_be_str")
        if self.origin != self.origin.strip():
            raise ValueError("collector_admission_request_origin_not_normalized")
        if self.callback_route is not None:
            if type(self.callback_route) is not CallbackRecoveryRoute:
                raise TypeError("collector_admission_request_callback_route_invalid")
            invalid = self.callback_route.invalid_reason()
            if invalid:
                raise ValueError(
                    f"collector_admission_request_callback_route_invalid:{invalid}"
                )
            if self.callback_route.listener_port != self.observed_session.listener_port:
                raise ValueError(
                    "collector_admission_request_listener_port_mismatch"
                )


__all__ = ["CollectorAdmissionRequest", "ObservedCollectorSession"]
