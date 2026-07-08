"""Single source of truth for the three collector-connection architecture axes.

The integration used to infer transport ownership and endpoint control from the
operation mode, the endpoint hostname, the peer IP, or the collector type. That
coupling is what this module replaces. Each collector-backed config entry now
carries three explicit, independent axes:

- ``connection_strategy`` -- how Home Assistant obtains the TCP session
  (``inbound`` vs ``callback_on_demand``);
- ``endpoint_control_policy`` -- whether the integration may manage the endpoint
  (``external`` vs ``integration_managed``);
- ``proxy_enabled`` -- whether an accepted inbound session should be proxied.

Every runtime/discovery decision that used to branch on operation mode or on an
endpoint hostname should branch on these resolvers and predicates instead. The
resolvers read the explicit persisted field when present and otherwise derive it
from the legacy fields using exactly the same rule as :func:`migrate_entry_axes`,
so behavior is stable whether or not the entry has been migrated yet.

Nothing in this module inspects endpoint hostnames, ports, or peer IPs. The
endpoint string is an opaque configuration value; peer IP is diagnostic only.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..const import (
    COLLECTOR_OPERATION_HA_ONLY,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_CONNECTION_MODE,
    CONF_CONNECTION_STRATEGY,
    CONF_ENDPOINT_CONTROL_POLICY,
    CONF_ENDPOINT_WRITTEN_AT,
    CONF_ENDPOINT_WRITTEN_VALUE,
    CONF_PROXY_ENABLED,
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    DEFAULT_CONNECTION_STRATEGY,
    DEFAULT_ENDPOINT_CONTROL_POLICY,
    DEFAULT_PROXY_ENABLED,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
    ENDPOINT_CONTROL_POLICIES,
)

# The legacy connection_mode value used for collectors that already dial the
# Home Assistant listener (passive callback). Kept as a private literal so the
# rest of the code base can stop matching this string directly.
_CONNECTION_MODE_CALLBACK_LISTENER = "callback_listener"

# Original-endpoint provenance sources that mean *the integration itself wrote
# the endpoint* (as opposed to merely observing it). Only these promote an entry
# to integration_managed during migration.
_INTEGRATION_WRITE_ENDPOINT_SOURCES = frozenset({"config_flow_pre_bind"})


def _first_present(
    key: str,
    data: Mapping[str, Any],
    options: Mapping[str, Any],
) -> Any:
    """Return options-over-data for one key, or ``None`` when absent/blank."""

    for source in (options, data):
        if key in source:
            value = source.get(key)
            if value is not None and str(value).strip() != "":
                return value
    return None


# --- connection_strategy ------------------------------------------------------


def _derive_connection_strategy(
    data: Mapping[str, Any],
    options: Mapping[str, Any],
) -> str:
    """Derive the connection strategy from the legacy fields (hostname-free)."""

    connection_mode = str(
        _first_present(CONF_CONNECTION_MODE, data, options) or ""
    ).strip()
    if connection_mode == _CONNECTION_MODE_CALLBACK_LISTENER:
        # The collector already dials the Home Assistant listener.
        return CONNECTION_STRATEGY_INBOUND

    operation_mode = str(
        _first_present(CONF_COLLECTOR_OPERATION_MODE, data, options) or ""
    ).strip()
    if operation_mode == COLLECTOR_OPERATION_HA_ONLY:
        # HA owns the endpoint; the collector dials Home Assistant on its own.
        return CONNECTION_STRATEGY_INBOUND
    if operation_mode:
        # SmartESS+HA (cloud-primary): the collector normally points elsewhere
        # and Home Assistant borrows a callback session on demand.
        return CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
    return DEFAULT_CONNECTION_STRATEGY


def resolve_connection_strategy(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> str:
    """Return the effective connection strategy for one entry."""

    options = options or {}
    explicit = str(_first_present(CONF_CONNECTION_STRATEGY, data, options) or "").strip()
    if explicit in CONNECTION_STRATEGIES:
        return explicit
    return _derive_connection_strategy(data, options)


def is_inbound(strategy: str) -> bool:
    """Return whether a strategy value is the inbound strategy."""

    return str(strategy or "").strip() == CONNECTION_STRATEGY_INBOUND


def is_callback_on_demand(strategy: str) -> bool:
    """Return whether a strategy value is the callback-on-demand strategy."""

    return str(strategy or "").strip() == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND


def may_send_callback_trigger(strategy: str) -> bool:
    """Return whether runtime may send a UDP callback trigger for this strategy.

    Only ``callback_on_demand`` may ask the collector to dial back. ``inbound``
    must claim or wait for an already-inbound session and never touch the wire.
    """

    return is_callback_on_demand(strategy)


def may_run_steady_reverse_discovery(strategy: str) -> bool:
    """Return whether steady-state UDP reverse discovery may run for a strategy."""

    return is_callback_on_demand(strategy)


# --- endpoint_control_policy --------------------------------------------------


def _derive_endpoint_control_policy(
    data: Mapping[str, Any],
    options: Mapping[str, Any],
) -> str:
    """Derive the endpoint control policy from endpoint write provenance.

    Only strong evidence that the integration itself wrote the endpoint promotes
    an entry to ``integration_managed``: an explicit ``endpoint_written_value``,
    or an original-endpoint provenance source that means "remembered right
    before we wrote it". Merely observing an endpoint (e.g. registry backfill)
    stays ``external``.
    """

    if _first_present(CONF_ENDPOINT_WRITTEN_VALUE, data, options) is not None:
        return ENDPOINT_CONTROL_INTEGRATION_MANAGED

    source = str(
        _first_present(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE, data, options)
        or ""
    ).strip()
    if source in _INTEGRATION_WRITE_ENDPOINT_SOURCES:
        return ENDPOINT_CONTROL_INTEGRATION_MANAGED
    return DEFAULT_ENDPOINT_CONTROL_POLICY


def resolve_endpoint_control_policy(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> str:
    """Return the effective endpoint control policy for one entry."""

    options = options or {}
    explicit = str(
        _first_present(CONF_ENDPOINT_CONTROL_POLICY, data, options) or ""
    ).strip()
    if explicit in ENDPOINT_CONTROL_POLICIES:
        return explicit
    return _derive_endpoint_control_policy(data, options)


def is_integration_managed_endpoint(policy: str) -> bool:
    """Return whether an endpoint control policy is integration-managed."""

    return str(policy or "").strip() == ENDPOINT_CONTROL_INTEGRATION_MANAGED


def may_auto_manage_endpoint(policy: str) -> bool:
    """Return whether runtime may write/restore/auto-heal the endpoint.

    Only ``integration_managed`` allows the steady-state reconcile to write or
    restore the collector endpoint. Under ``external`` the integration may read
    and display the endpoint and surface drift, but must never mutate it.
    """

    return is_integration_managed_endpoint(policy)


# --- proxy_enabled ------------------------------------------------------------


def resolve_proxy_enabled(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> bool:
    """Return whether proxy mode is enabled for one entry."""

    options = options or {}
    for source in (options, data):
        if CONF_PROXY_ENABLED in source:
            return bool(source.get(CONF_PROXY_ENABLED))
    return DEFAULT_PROXY_ENABLED


# --- migration ----------------------------------------------------------------


def migrate_entry_axes(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the axis fields to persist into entry *data* for one legacy entry.

    The mapping is deterministic and hostname-free. Only missing fields are
    filled; an explicit persisted axis is preserved. Endpoint provenance
    (``endpoint_written_value`` / ``endpoint_written_at``) is intentionally not
    fabricated during migration -- it is written only when the integration
    actually writes the endpoint through an explicit action.
    """

    options = options or {}
    axes: dict[str, Any] = {
        CONF_CONNECTION_STRATEGY: resolve_connection_strategy(data, options),
        CONF_ENDPOINT_CONTROL_POLICY: resolve_endpoint_control_policy(data, options),
        CONF_PROXY_ENABLED: resolve_proxy_enabled(data, options),
    }
    return axes


def entry_axis_diagnostics(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a compact, opaque view of the three axes for support bundles."""

    options = options or {}
    strategy = resolve_connection_strategy(data, options)
    policy = resolve_endpoint_control_policy(data, options)
    return {
        "connection_strategy": strategy,
        "endpoint_control_policy": policy,
        "proxy_enabled": resolve_proxy_enabled(data, options),
        "original_endpoint": str(
            _first_present(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT, data, options) or ""
        ),
        "endpoint_written_value": str(
            _first_present(CONF_ENDPOINT_WRITTEN_VALUE, data, options) or ""
        ),
        "endpoint_written_at": str(
            _first_present(CONF_ENDPOINT_WRITTEN_AT, data, options) or ""
        ),
        "may_send_callback_trigger": may_send_callback_trigger(strategy),
        "may_auto_manage_endpoint": may_auto_manage_endpoint(policy),
    }
