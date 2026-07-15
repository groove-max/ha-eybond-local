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
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_PN,
    CONF_ENTRY_ROLE,
    ENTRY_ROLE_LISTENER,
    CONF_CONNECTION_STRATEGY_EVIDENCE,
    CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
    CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT,
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

# Legacy connection_mode values that mean the entry was onboarded as a
# USER-TRIGGERED callback collector: Home Assistant reached it only by sending a
# one-shot UDP callback trigger (manual address or known-IP onboarding), and it
# does NOT dial Home Assistant in on its own. Such an entry is callback_on_demand
# regardless of the legacy operation mode -- operation mode is not the authority
# for the connection strategy.
_USER_TRIGGERED_CALLBACK_CONNECTION_MODES = frozenset({"manual", "known_ip"})

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
    """Derive the connection strategy from the legacy fields (hostname-free).

    Precedence:

    1. **Verified strategy evidence** -- a behavioral proof recorded during
       onboarding (a genuine reboot/reconnect dial-in, or a one-shot callback
       trigger) is authoritative over any legacy inference.
    2. **Cloud-primary operation mode** -- checked before ``connection_mode`` so a
       stale ``callback_listener`` artifact cannot force a cloud-primary factory
       collector to ``inbound`` (E500/SMG family-A).
    3. **User-triggered callback connection_mode** -- a ``manual``/``known_ip``
       entry was reached by a UDP trigger and never dials in on its own, so it is
       callback_on_demand even when the legacy operation mode is HA-only.
    4. **HA-only operation mode** -- a collector whose endpoint points at Home
       Assistant dials in on its own (inbound).
    5. **Passive callback_listener** / other legacy fallbacks.

    Operation mode is never the sole authority for the strategy.
    """

    evidence = str(
        _first_present(CONF_CONNECTION_STRATEGY_EVIDENCE, data, options) or ""
    ).strip()
    if evidence == CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER:
        return CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
    if evidence == CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT:
        return CONNECTION_STRATEGY_INBOUND

    operation_mode = str(
        _first_present(CONF_COLLECTOR_OPERATION_MODE, data, options) or ""
    ).strip()
    connection_mode = str(
        _first_present(CONF_CONNECTION_MODE, data, options) or ""
    ).strip()

    if operation_mode == COLLECTOR_OPERATION_SMARTESS_AND_HA:
        # Cloud-primary factory collector: it normally points at the vendor cloud,
        # so Home Assistant borrows a callback session on demand (one UDP trigger
        # per attempt) rather than waiting for an inbound dial-in.
        return CONNECTION_STRATEGY_CALLBACK_ON_DEMAND

    if connection_mode in _USER_TRIGGERED_CALLBACK_CONNECTION_MODES:
        # A manual / known-IP onboarded callback collector: reached only via a
        # one-shot UDP trigger, never a spontaneous dial-in -- even when a legacy
        # HA-only operation mode was recorded. This is the offline PN-less
        # manual/known-IP case: inbound would wait forever.
        return CONNECTION_STRATEGY_CALLBACK_ON_DEMAND

    if operation_mode == COLLECTOR_OPERATION_HA_ONLY:
        # HA owns the endpoint; the collector dials Home Assistant on its own.
        return CONNECTION_STRATEGY_INBOUND

    if connection_mode == _CONNECTION_MODE_CALLBACK_LISTENER:
        # A genuine passive-callback entry with no operation-mode signal: the
        # collector already dials the Home Assistant listener (inbound).
        return CONNECTION_STRATEGY_INBOUND

    if operation_mode:
        # Any other legacy operation mode: borrow a session on demand.
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


def correct_migrated_connection_strategy(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> str | None:
    """Return a corrected connection strategy for a provably-broken entry, else None.

    A v2 migration derived ``connection_strategy=inbound`` for a cloud-primary
    factory collector (operation mode SmartESS+HA) because a stale
    ``callback_listener`` connection_mode used to take precedence over the
    operation mode. Such an entry can never connect as inbound: the collector
    points at the vendor cloud and will not dial Home Assistant on its own, so HA
    waits forever (offline). It is corrected to ``callback_on_demand``.

    The correction is deliberately NARROW (item 6):

    - It applies ONLY to the cloud-primary SmartESS+HA shape. It is NOT extended
      to manual/known-IP entries: those are user-triggered callback collectors
      whose strategy is decided by derivation/verification, and a genuinely
      explicit ``inbound`` value there is never overwritten just because the
      legacy connection_mode looks user-triggered.
    - ``callback_on_demand`` is NOT a free "safe superset": it emits a UDP
      callback trigger the collector would not otherwise receive, so it is only
      chosen where inbound is provably unreachable (the cloud-primary case).
    - A behaviorally-verified inbound entry carries
      ``connection_strategy_evidence=reboot_reconnect`` and is left alone.
    - An integration-managed endpoint means the collector really does dial in;
      inbound is correct and left alone.

    No endpoint is written. Returns ``None`` when there is nothing to correct.
    """

    options = options or {}
    strategy = str(_first_present(CONF_CONNECTION_STRATEGY, data, options) or "").strip()
    if strategy != CONNECTION_STRATEGY_INBOUND:
        return None
    evidence = str(
        _first_present(CONF_CONNECTION_STRATEGY_EVIDENCE, data, options) or ""
    ).strip()
    if evidence == CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT:
        return None
    if is_integration_managed_endpoint(resolve_endpoint_control_policy(data, options)):
        return None
    operation_mode = str(
        _first_present(CONF_COLLECTOR_OPERATION_MODE, data, options) or ""
    ).strip()
    # Correct ONLY the cloud-primary SmartESS+HA shape: a factory collector that
    # points at the vendor cloud and can never dial in as inbound. This is
    # deliberately NOT extended to manual/known-IP: those are user-triggered
    # callback collectors whose strategy is decided by derivation/verification,
    # and a genuinely-explicit inbound value there must not be flipped (item 6).
    if operation_mode != COLLECTOR_OPERATION_SMARTESS_AND_HA:
        return None
    return CONNECTION_STRATEGY_CALLBACK_ON_DEMAND


def is_collector_backed_callback_entry(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> bool:
    """Return whether an entry owns its session through the callback registry.

    True for every collector-backed entry -- both ``inbound`` and
    ``callback_on_demand`` claim/own via the :class:`CallbackSessionRegistry` by
    durable PN. False ONLY for the integration listener/bootstrap entry, which is
    the single explicit non-collector role (identified by ``entry_role`` -- an
    architectural role, never a hostname/IP). Direct transports, if any ever
    exist, must be excluded here by their own explicit role, never by address.
    """

    options = options or {}
    role = str(_first_present(CONF_ENTRY_ROLE, data, options) or "").strip()
    if role == ENTRY_ROLE_LISTENER:
        return False
    return True


def collector_identity_binding_required(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> bool:
    """Return whether a collector-backed entry is missing its durable identity.

    Both ``inbound`` and ``callback_on_demand`` entries claim their session
    through the callback registry by durable full collector PN (peer IP is never
    ownership); without a PN neither can own any session and setup fails closed
    (:func:`_register_entry_callback_session_claim` records no claim). Such an
    entry must not be presented as a normal ``waiting_for_collector`` entry -- it
    needs identity re-binding (the manual/known-IP callback repair flow). The
    listener/bootstrap entry has no collector identity and is excluded.
    """

    options = options or {}
    if not is_collector_backed_callback_entry(data, options):
        return False
    collector_pn = str(_first_present(CONF_COLLECTOR_PN, data, options) or "").strip()
    return not collector_pn


def _connection_strategy_source(
    data: Mapping[str, Any],
    options: Mapping[str, Any],
) -> str:
    """Return why the effective connection strategy has its value (provenance)."""

    if str(_first_present(CONF_CONNECTION_STRATEGY, data, options) or "").strip() in CONNECTION_STRATEGIES:
        return "explicit"
    operation_mode = str(
        _first_present(CONF_COLLECTOR_OPERATION_MODE, data, options) or ""
    ).strip()
    if operation_mode == COLLECTOR_OPERATION_HA_ONLY:
        return "derived_operation_mode_ha_only"
    if operation_mode == COLLECTOR_OPERATION_SMARTESS_AND_HA:
        return "derived_operation_mode_cloud"
    if str(_first_present(CONF_CONNECTION_MODE, data, options) or "").strip() == _CONNECTION_MODE_CALLBACK_LISTENER:
        return "derived_connection_mode_callback_listener"
    if operation_mode:
        return "derived_operation_mode_other"
    return "default"


def _endpoint_control_policy_source(
    data: Mapping[str, Any],
    options: Mapping[str, Any],
) -> str:
    """Return why the effective endpoint control policy has its value (provenance)."""

    if str(_first_present(CONF_ENDPOINT_CONTROL_POLICY, data, options) or "").strip() in ENDPOINT_CONTROL_POLICIES:
        return "explicit"
    if _first_present(CONF_ENDPOINT_WRITTEN_VALUE, data, options) is not None:
        return "derived_endpoint_written_value"
    if str(
        _first_present(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE, data, options) or ""
    ).strip() in _INTEGRATION_WRITE_ENDPOINT_SOURCES:
        return "derived_original_endpoint_source"
    return "default"


def migration_diagnostics(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return migration status/provenance for an entry (safe, read-only).

    Surfaces WHY each axis resolved the way it did, whether a deterministic
    correction applies, and a human-readable warning for ambiguous/corrected
    entries. Never mutates state and never inspects hostnames/peer IPs.
    """

    options = options or {}
    strategy_source = _connection_strategy_source(data, options)
    policy_source = _endpoint_control_policy_source(data, options)
    correction = correct_migrated_connection_strategy(data, options)

    warnings: list[str] = []
    status = "ok"
    if correction is not None:
        status = "corrected"
        warnings.append(
            "connection_strategy=inbound with a cloud-primary operation mode is "
            f"unreachable; using {correction} instead."
        )
    if collector_identity_binding_required(data, options):
        # A collector-backed entry (inbound OR callback_on_demand) with no durable
        # collector PN cannot own any session: do not present it as a normal
        # 'waiting for collector'. It needs the manual/known-IP callback identity
        # re-binding (repair/reconfigure) flow. A separate correction verdict keeps
        # priority for the status label (a corrected entry still needs its
        # correction applied first), but the binding warning is always surfaced.
        if correction is None:
            status = "identity_binding_required"
        warnings.append(
            "collector-backed entry has no durable collector PN; run the "
            "callback re-verification (reconfigure) to bind the collector identity."
        )

    strategy_explicit = strategy_source == "explicit"
    policy_explicit = policy_source == "explicit"
    if strategy_explicit and policy_explicit:
        axes_source = "explicit"
    elif strategy_explicit or policy_explicit:
        axes_source = "mixed"
    else:
        axes_source = "derived"

    return {
        "migration_status": status,
        "migration_warning": "; ".join(warnings),
        "migration_axes_source": axes_source,
        "connection_strategy_source": strategy_source,
        "endpoint_control_policy_source": policy_source,
    }


def simulate_migration(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run derivation/correction on a (data, options) pair and return the decision.

    A pure inspection helper for reviewing how a legacy entry maps to the explicit
    axes. It never mutates state and never writes an endpoint -- use it to audit a
    dumped config entry, and it backs the migration-matrix regression tests.
    """

    options = options or {}
    correction = correct_migrated_connection_strategy(data, options)
    strategy = correction if correction is not None else resolve_connection_strategy(data, options)
    policy = resolve_endpoint_control_policy(data, options)
    result: dict[str, Any] = {
        CONF_CONNECTION_STRATEGY: strategy,
        CONF_ENDPOINT_CONTROL_POLICY: policy,
        CONF_PROXY_ENABLED: resolve_proxy_enabled(data, options),
        "may_send_callback_trigger": may_send_callback_trigger(strategy),
        "may_auto_manage_endpoint": may_auto_manage_endpoint(policy),
    }
    result.update(migration_diagnostics(data, options))
    return result


def entry_axis_diagnostics(
    data: Mapping[str, Any],
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a compact, opaque view of the three axes for support bundles."""

    options = options or {}
    correction = correct_migrated_connection_strategy(data, options)
    strategy = correction if correction is not None else resolve_connection_strategy(data, options)
    policy = resolve_endpoint_control_policy(data, options)
    diagnostics: dict[str, Any] = {
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
        "connection_strategy_evidence": str(
            _first_present(CONF_CONNECTION_STRATEGY_EVIDENCE, data, options) or ""
        ),
        "collector_pn_bound": bool(
            str(_first_present(CONF_COLLECTOR_PN, data, options) or "").strip()
        ),
        "collector_identity_binding_required": collector_identity_binding_required(
            data, options
        ),
        "may_send_callback_trigger": may_send_callback_trigger(strategy),
        "may_auto_manage_endpoint": may_auto_manage_endpoint(policy),
    }
    diagnostics.update(migration_diagnostics(data, options))
    return diagnostics
