"""Read-only user-facing collector operating-profile projection.

The profile is not a third architecture axis and is never persisted.  It is a
strict projection of the existing authorities:

* ``connection_strategy`` -- how Home Assistant obtains a collector session;
* ``endpoint_control_policy`` -- who owns endpoint changes;
* ``RecoveryContract`` -- whether an inbound route was actually verified;
* collector capabilities -- whether a cloud side can exist at all.

Keeping this projection neutral prevents UI wording such as "SmartESS + Home
Assistant" from becoming another writer that can drift from the wire state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..const import (
    CONF_STRATEGY_TRANSITION_STATE,
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
    ENDPOINT_CONTROL_POLICIES,
)
from .connection_policy import (
    resolve_connection_strategy,
    resolve_endpoint_control_policy,
)
from .recovery_contract import RecoveryContract


OPERATING_PROFILE_SMARTESS_AND_HA = "smartess_cloud_home_assistant"
OPERATING_PROFILE_HA_ONLY = "home_assistant_only"
OPERATING_PROFILE_CUSTOM = "custom"
OPERATING_PROFILES = frozenset(
    {
        OPERATING_PROFILE_SMARTESS_AND_HA,
        OPERATING_PROFILE_HA_ONLY,
        OPERATING_PROFILE_CUSTOM,
    }
)

PROFILE_REASON_CALLBACK_EXTERNAL = "callback_external"
PROFILE_REASON_INBOUND_MANAGED = "inbound_managed"
PROFILE_REASON_INBOUND_VERIFIED = "inbound_verified"
PROFILE_REASON_HA_ONLY_CAPABILITY = "ha_only_capability"
PROFILE_REASON_TRANSITION_PENDING = "transition_pending"
PROFILE_REASON_AXIS_MISMATCH = "axis_mismatch"
PROFILE_REASONS = frozenset(
    {
        PROFILE_REASON_CALLBACK_EXTERNAL,
        PROFILE_REASON_INBOUND_MANAGED,
        PROFILE_REASON_INBOUND_VERIFIED,
        PROFILE_REASON_HA_ONLY_CAPABILITY,
        PROFILE_REASON_TRANSITION_PENDING,
        PROFILE_REASON_AXIS_MISMATCH,
    }
)


def _strict_token(value: object, *, allowed: frozenset[str] | set[str]) -> str:
    if type(value) is not str:
        raise TypeError("operating_profile_token_not_string")
    if not value or value != value.strip() or value not in allowed:
        raise ValueError("operating_profile_token_invalid")
    return value


@dataclass(frozen=True, slots=True)
class CollectorOperatingProfile:
    """One immutable user-facing projection of collector operation."""

    profile: str
    connection_strategy: str
    endpoint_control_policy: str
    reason: str

    def __post_init__(self) -> None:
        profile = _strict_token(self.profile, allowed=OPERATING_PROFILES)
        strategy = _strict_token(
            self.connection_strategy,
            allowed=CONNECTION_STRATEGIES,
        )
        policy = _strict_token(
            self.endpoint_control_policy,
            allowed=ENDPOINT_CONTROL_POLICIES,
        )
        reason = _strict_token(self.reason, allowed=PROFILE_REASONS)

        if reason == PROFILE_REASON_CALLBACK_EXTERNAL:
            valid = (
                profile == OPERATING_PROFILE_SMARTESS_AND_HA
                and strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
                and policy == ENDPOINT_CONTROL_EXTERNAL
            )
        elif reason == PROFILE_REASON_INBOUND_MANAGED:
            valid = (
                profile == OPERATING_PROFILE_HA_ONLY
                and strategy == CONNECTION_STRATEGY_INBOUND
                and policy == ENDPOINT_CONTROL_INTEGRATION_MANAGED
            )
        elif reason == PROFILE_REASON_INBOUND_VERIFIED:
            valid = (
                profile == OPERATING_PROFILE_HA_ONLY
                and strategy == CONNECTION_STRATEGY_INBOUND
                and policy == ENDPOINT_CONTROL_EXTERNAL
            )
        elif reason == PROFILE_REASON_HA_ONLY_CAPABILITY:
            valid = (
                profile == OPERATING_PROFILE_HA_ONLY
                and strategy == CONNECTION_STRATEGY_INBOUND
            )
        else:
            valid = profile == OPERATING_PROFILE_CUSTOM

        if not valid:
            raise ValueError("operating_profile_shape_invalid")

    @property
    def stable(self) -> bool:
        """Return whether the profile is one of the two normal product states."""

        return self.profile != OPERATING_PROFILE_CUSTOM


def resolve_collector_operating_profile(
    *,
    connection_strategy: str,
    endpoint_control_policy: str,
    inbound_verified: bool = False,
    ha_only_required: bool = False,
    transition_pending: bool = False,
) -> CollectorOperatingProfile:
    """Resolve a user profile without mutating or inferring an architecture axis."""

    strategy = _strict_token(connection_strategy, allowed=CONNECTION_STRATEGIES)
    policy = _strict_token(
        endpoint_control_policy,
        allowed=ENDPOINT_CONTROL_POLICIES,
    )
    if type(inbound_verified) is not bool:
        raise TypeError("operating_profile_inbound_verified_not_bool")
    if type(ha_only_required) is not bool:
        raise TypeError("operating_profile_ha_only_required_not_bool")
    if type(transition_pending) is not bool:
        raise TypeError("operating_profile_transition_pending_not_bool")

    if transition_pending:
        profile = OPERATING_PROFILE_CUSTOM
        reason = PROFILE_REASON_TRANSITION_PENDING
    elif ha_only_required and strategy == CONNECTION_STRATEGY_INBOUND:
        profile = OPERATING_PROFILE_HA_ONLY
        reason = PROFILE_REASON_HA_ONLY_CAPABILITY
    elif (
        strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        and policy == ENDPOINT_CONTROL_EXTERNAL
        and not ha_only_required
    ):
        profile = OPERATING_PROFILE_SMARTESS_AND_HA
        reason = PROFILE_REASON_CALLBACK_EXTERNAL
    elif (
        strategy == CONNECTION_STRATEGY_INBOUND
        and policy == ENDPOINT_CONTROL_INTEGRATION_MANAGED
    ):
        profile = OPERATING_PROFILE_HA_ONLY
        reason = PROFILE_REASON_INBOUND_MANAGED
    elif strategy == CONNECTION_STRATEGY_INBOUND and inbound_verified:
        # A collector whose endpoint was configured outside the integration may
        # still have a typed autonomous-reconnect proof.  It is honestly HA-only
        # even though endpoint ownership remains external.
        profile = OPERATING_PROFILE_HA_ONLY
        reason = PROFILE_REASON_INBOUND_VERIFIED
    else:
        profile = OPERATING_PROFILE_CUSTOM
        reason = PROFILE_REASON_AXIS_MISMATCH

    return CollectorOperatingProfile(
        profile=profile,
        connection_strategy=strategy,
        endpoint_control_policy=policy,
        reason=reason,
    )


def collector_operating_profile_from_entry(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
    *,
    ha_only_required: bool = False,
) -> CollectorOperatingProfile:
    """Project one entry through the typed architecture readers."""

    options = options or {}
    contract = RecoveryContract.from_entry_data(dict(data))
    return resolve_collector_operating_profile(
        connection_strategy=resolve_connection_strategy(data, options),
        endpoint_control_policy=resolve_endpoint_control_policy(data, options),
        inbound_verified=bool(contract is not None and contract.inbound_verified),
        ha_only_required=ha_only_required,
        transition_pending=CONF_STRATEGY_TRANSITION_STATE in data,
    )


__all__ = [
    "CollectorOperatingProfile",
    "OPERATING_PROFILE_CUSTOM",
    "OPERATING_PROFILE_HA_ONLY",
    "OPERATING_PROFILE_SMARTESS_AND_HA",
    "collector_operating_profile_from_entry",
    "resolve_collector_operating_profile",
]
