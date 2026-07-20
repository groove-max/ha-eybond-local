"""THE verified connection-strategy transition authority (Batch 8).

``connection_strategy`` is canonical user intent, but flipping it on a LIVE
entry is an architectural transition: the collector's real behavior must be
re-proven before the axis may change. Before this module, several UI surfaces
(runtime options, the legacy operation-mode select, bind/rollback buttons)
each combined endpoint writes, axis writes and RecoveryContract state in their
own way. Now every high-level strategy switch runs through this ONE
orchestrator; the UI surfaces are facades.

What this module deliberately REUSES (never re-implements):

* :class:`InboundRecoveryVerifier` / :class:`CallbackRecoveryVerifier` — the
  single controlled-reset recovery engine (baseline, causality lease, the
  exactly-one-``set>server`` gate, silent-reconnect probing, typed proofs);
* :class:`RecoveryTerminalInput` / ``merge_recovery_contract`` — the single
  RecoveryContract writer;
* the ``CallbackSessionRegistry`` claim primitives — the transition runs
  UNDER the entry's own durable claim (``owner_id`` = the entry id), so no
  ownership ever moves: retarget/promote are the same idempotent registry
  calls the runtime itself uses.

Hard rules encoded here:

* the TARGET strategy is persisted only on COMPLETE success (valid typed
  proof + contract merge accepted) — never on user selection, never on a
  partial step;
* endpoint provenance is persisted the moment a write/restore is CONFIRMED —
  honestly kept even when the later verification fails ("only really earned
  facts survive");
* exactly ONE controlled apply/restart lifecycle per transition: the endpoint
  write (``apply_changes=True``) IS the verifier's restart step, so the
  endpoint apply and the recovery reboot can never double-boot the collector;
* the inbound path sends ZERO UDP; the callback path proves exactly one
  causal ``set>server`` sequence (the engine's lease + ledger gate);
* nothing here derives anything from peer IP / hostname / cloud family /
  provider / collector kind — every address is caller-confirmed input.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from ..const import (
    CONF_CONNECTION_STRATEGY,
    CONF_CONTROL_MODE,
    CONF_ENDPOINT_CONTROL_POLICY,
    CONF_ENDPOINT_WRITTEN_AT,
    CONF_ENDPOINT_WRITTEN_VALUE,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
)
from .recovery.terminal import RecoveryTerminalInput
from .recovery.verification import (
    CallbackRecoveryRoute,
    CallbackRecoveryVerifier,
    InboundRecoveryVerifier,
    registry_sessions_projection,
)
from ..timeout_policy import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    OnboardingTimeoutPolicy,
)
from .session_registry import normalize_pn
from .strategy_transition_recovery import (
    RECOVERY_PHASE_PENDING,
    RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    StrategyTransitionRecoveryState,
)

logger = logging.getLogger(__name__)

# Typed transition failures. Preflight refusals happen BEFORE any collector
# side effect; the recovery-stage failures pass the engine's own typed reason
# through unchanged (one vocabulary, no re-labeling).
TRANSITION_TARGET_INVALID = "transition_target_invalid"
TRANSITION_NOT_REQUIRED = "transition_not_required"
TRANSITION_SESSION_UNAVAILABLE = "transition_session_unavailable"
TRANSITION_ENDPOINT_REQUIRED = "transition_endpoint_required"
TRANSITION_CALLBACK_ROUTE_REQUIRED = "transition_callback_route_required"
TRANSITION_ROLLBACK_ENDPOINT_UNAVAILABLE = (
    "transition_rollback_endpoint_unavailable"
)
TRANSITION_ENDPOINT_WRITE_FAILED = "transition_endpoint_write_failed"
TRANSITION_INBOUND_RECOVERED_INSTEAD = "transition_inbound_recovered_instead"
TRANSITION_INBOUND_RECOVERED_AFTER_RESTORE = (
    "transition_inbound_recovered_after_restore"
)
TRANSITION_ALREADY_RUNNING = "transition_already_running"
TRANSITION_PAYLOAD_FORBIDDEN = "transition_payload_forbidden"
TRANSITION_OWNER_CERTIFICATION_STALE = "transition_owner_certification_stale"
TRANSITION_RECOVERY_STATE_UNAVAILABLE = "transition_recovery_state_unavailable"
# A recovery state WAS supplied but is not the exact typed capability this
# transition requires (wrong type / foreign PN / foreign route / wrong target /
# non-startable phase). Distinct from UNAVAILABLE (nothing supplied) so the
# failure is not confused with a build/wiring gap.
TRANSITION_RECOVERY_STATE_INVALID = "transition_recovery_state_invalid"
# The write-ahead ``persist_pending`` hook refused or raised: durable intent
# could not be established, so NO collector side effect is allowed to follow.
TRANSITION_PERSIST_PENDING_FAILED = "transition_persist_pending_failed"
# An integration-managed restore was requested but the caller supplied no
# ``persist_confirmed`` hook. Refused BEFORE ``persist_pending`` and any side
# effect: without it the confirmed-restore phase could never be written, so the
# restore must not begin at all.
TRANSITION_PERSIST_CONFIRMED_UNAVAILABLE = "transition_persist_confirmed_unavailable"

# The typed persisted degraded marker (stored under
# ``const.CONF_STRATEGY_TRANSITION_STATE``): the collector's endpoint was
# CONFIRMED restored to the external target, but the callback strategy was
# never proven — the canonical strategy temporarily does not match the wire
# reality, and the runtime/UI must say so instead of pretending health.
DEGRADED_CALLBACK_RESTORE_UNPROVEN = "callback_restore_unproven"

# ALLOWLIST (not a blacklist): the ONLY orthogonal runtime-option keys a
# transition may carry into its success commit. Everything else — the
# architecture axes, identity, provenance, contract, AND every connection
# topology field (bind/server IP, TCP/UDP ports, collector target, advertised
# host/port, discovery route) — is refused. Topology cannot be staged and
# proven against the old runtime spec in the same step; the facade must save
# connection settings separately first. ``collector_operation_mode`` is added
# by the authority itself from the strictly-validated ``legacy_operation_mode``
# enum, never from the caller's payload.
_ALLOWED_OPTION_KEYS = frozenset(
    {
        CONF_POLL_MODE,
        CONF_POLL_INTERVAL,
        CONF_CONTROL_MODE,
    }
)


class StrategyTransitionLease:
    """Per-entry exclusive transition lease — synchronous, cancellation-safe.

    ``acquire`` is a plain synchronous check-and-set, so between two awaits it
    is atomic under the event loop: callers MUST acquire before their first
    await/side effect. ``release`` is idempotent and safe from ``finally``
    even after cancellation, so a held-but-unowned lease cannot exist.
    Different entries never contend (per-entry keys, no global lock).
    """

    def __init__(self) -> None:
        self._held: set[str] = set()

    def acquire(self, entry_id: str) -> bool:
        key = str(entry_id or "").strip()
        if not key or key in self._held:
            return False
        self._held.add(key)
        return True

    def release(self, entry_id: str) -> None:
        self._held.discard(str(entry_id or "").strip())


# THE one production per-entry transition lease. Module-level so a config-entry
# reload cannot orphan a lease held by a stale coordinator instance, and so the
# production facade and the tests exercise the SAME implementation.
STRATEGY_TRANSITION_LEASES = StrategyTransitionLease()


def trusted_transition_wire(registry: Any, entry_id: str, session_id: str) -> str:
    """The ONE wire authority a transition may use: a trusted SessionHandle.

    Session-PINNED and fail-closed. All three of the registry claim owner,
    ``claim.session_id`` and the ``SessionHandle.session_id`` must agree
    (``session_handle_for_owned_session``), the handle must literally be a
    ``SessionHandle`` (no ducks), OBSERVED on the live wire, with no conflict,
    and its id must equal the requested ``session_id``. There is NO PN search
    and NO fallback to another same-PN session; if the claim is not yet pinned
    to this exact socket the caller must run the explicit
    ``pin_owner_claim_to_session`` registry op first. An inventory
    ``session_protocol`` string or an expected/persisted protocol can never
    become the authority.
    """

    from .session_handle import SessionHandle

    sid = str(session_id or "").strip()
    if not sid:
        return ""
    try:
        handle = registry.session_handle_for_owned_session(entry_id, sid)
    except Exception:
        return ""
    if type(handle) is not SessionHandle:
        return ""
    if handle.session_id != sid:
        return ""
    if not handle.observed or handle.conflict:
        return ""
    if handle.uses_framed_wire:
        return "eybond_framed"
    if handle.uses_at_text_wire:
        return "at_text"
    return ""


def _disallowed_payload_key(payload: Any) -> str:
    """The first payload key NOT on the orthogonal allowlist, or "".

    Allowlist, not blacklist: a new architecture/identity/topology field added
    to the entry model in the future is refused by default rather than
    silently smuggled through the transition.
    """

    if not payload:
        return ""
    try:
        keys = tuple(payload.keys())
    except Exception:
        return "payload_not_a_mapping"
    for key in keys:
        if str(key) not in _ALLOWED_OPTION_KEYS:
            return str(key)
    return ""


@dataclass(frozen=True, slots=True)
class StrategyTransitionResult:
    """Typed outcome of ONE strategy transition attempt.

    ``endpoint_written`` / ``endpoint_restored`` are FACTS about confirmed
    collector writes — they stay true (and their provenance stays persisted)
    even when ``success`` is False, so a partial failure is honest and
    repeatable instead of pretending nothing happened.
    """

    success: bool
    target_strategy: str
    failure_reason: str = ""
    endpoint_written: bool = False
    endpoint_written_value: str = ""
    endpoint_restored: bool = False
    # Non-empty when this attempt left a typed persisted degraded marker
    # (confirmed restore without a proven callback strategy).
    degraded_state: str = ""
    outcome: Any = None


class _ManagedRestartChannel:
    """The verifier's restart channel bound to the RUNTIME's own management path.

    ``async_send_restart`` performs the transition's single controlled
    apply/restart lifecycle — an endpoint write with apply, an endpoint
    restore with apply, or a plain reboot — through the coordinator-supplied
    callable. A confirmed write immediately fires ``on_confirmed`` so the
    endpoint provenance is persisted BEFORE the verification outcome exists.

    ``observed_wire`` returns the live claimed session's negotiated wire (the
    registry's own live observation — never persisted evidence, never a
    hostname/cloud guess), which is the only silent-reconnect probe authority
    the engine accepts.
    """

    def __init__(
        self,
        *,
        restart: Callable[[], Awaitable[Any]],
        wire_provider: Callable[[], str],
        on_confirmed: Callable[[Any], None] | None = None,
    ) -> None:
        self._restart = restart
        self._wire_provider = wire_provider
        self._on_confirmed = on_confirmed

    async def async_send_restart(self) -> None:
        result = await self._restart()
        if self._on_confirmed is not None:
            self._on_confirmed(result)

    def observed_wire(self) -> str:
        try:
            return str(self._wire_provider() or "").strip()
        except Exception:  # pragma: no cover - fail-closed observation
            return ""

    def is_connected(self) -> bool:
        return False

    async def async_close(self) -> None:
        return None


def _applied_endpoint_from_result(result: Any, fallback: str) -> str:
    """The endpoint value the collector CONFIRMED (readback preferred)."""

    if isinstance(result, dict):
        value = str(
            result.get("readback_endpoint")
            or result.get("requested_endpoint")
            or ""
        ).strip()
        if value:
            return value
    return fallback


def _registry_retarget_hook(
    registry: Any, owner_id: str
) -> Callable[[str], bool]:
    def _retarget(session_id: str) -> bool:
        try:
            return bool(
                registry.retarget_claim_to_reconnected_session(
                    owner_id, session_id
                )
            )
        except ValueError:
            # previous socket still live / candidate owned elsewhere: typed
            # registry refusals mean "not retargeted", never a crash.
            return False

    return _retarget


def _registry_promote_hook(registry: Any, owner_id: str) -> Callable[[str], None]:
    def _promote(full_pn: str) -> None:
        registry.promote_claim_to_full_pn(owner_id, full_pn)

    return _promote


async def async_run_strategy_transition(
    *,
    target_strategy: str,
    current_strategy: str,
    collector_pn: str,
    owner_id: str,
    registry: Any,
    claimed_session_id: Callable[[], str],
    live_wire: Callable[[], str],
    clock: Callable[[], str],
    commit: Callable[[dict[str, Any], RecoveryTerminalInput], Awaitable[str]],
    policy: OnboardingTimeoutPolicy | None = None,
    ledger: Any = None,
    poll_interval: float = 0.2,
    # --- to inbound -------------------------------------------------------
    inbound_endpoint: str = "",
    endpoint_needs_write: bool = False,
    write_endpoint: Callable[[str], Awaitable[Any]] | None = None,
    reboot: Callable[[], Awaitable[Any]] | None = None,
    prepare_listener: Callable[[int], Awaitable[None]] | None = None,
    local_listener_port: int = 0,
    on_endpoint_written: Callable[[str], None] | None = None,
    # --- to callback_on_demand -------------------------------------------
    callback_route: CallbackRecoveryRoute | None = None,
    trigger_sender: Any = None,
    endpoint_control_policy: str = "",
    restore_endpoint: str = "",
    on_endpoint_restored: Callable[[str], None] | None = None,
    # Write-ahead persistence hooks. The AUTHORITY owns their ORDER (pending
    # BEFORE the first side effect; confirmed as ONE local write AFTER a
    # confirmed restore); the facade only supplies the physical writes.
    persist_pending: Callable[[StrategyTransitionRecoveryState], Any] | None = None,
    persist_confirmed: (
        Callable[[StrategyTransitionRecoveryState], Any] | None
    ) = None,
    # The TYPED recovery state, PRE-BUILT in the startable pending phase by the
    # facade BEFORE any side effect. For a callback transition it is mandatory
    # and fully validated (exact type, this durable PN, this exact route,
    # callback target, pending phase) before any endpoint write / reboot / UDP.
    recovery_state: StrategyTransitionRecoveryState | None = None,
    # --- typed facade payload (the ONLY compatibility pass-through) --------
    legacy_operation_mode: str = "",
    option_payload: Any = None,
    # --- shared -----------------------------------------------------------
    silent_session_probe: Any = None,
) -> StrategyTransitionResult:
    """Run ONE verified strategy transition end to end.

    The caller (the coordinator facade) supplies the runtime-bound callables;
    this function owns the ORDER and the axis-write policy. See the module
    docstring for the rules. ``commit(axis_updates, terminal, option_updates)``
    must merge the RecoveryContract and persist data+options atomically with
    exactly one reload, returning ``""`` or a typed refusal.

    Payload trust boundary: the ONLY axis-adjacent value a facade may pass is
    ``legacy_operation_mode`` (the legacy select's compatibility mirror).
    ``option_payload`` (the runtime form's staged options) is screened against
    ``_FORBIDDEN_PAYLOAD_KEYS`` — the canonical strategy, endpoint policy,
    provenance, identity and contract can never be smuggled past the proof.

    NAT split: ``inbound_endpoint`` / the route's advertised host+port are
    what the COLLECTOR is told (opaque, verbatim); ``local_listener_port`` is
    where Home Assistant actually listens and is the only thing
    ``prepare_listener`` may bind.
    """

    if policy is None:
        # Resolved at CALL time (module attribute lookup) so tests patching
        # this module's DEFAULT_ONBOARDING_TIMEOUT_POLICY take effect.
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY

    target = str(target_strategy or "").strip()
    if target not in CONNECTION_STRATEGIES:
        return StrategyTransitionResult(
            success=False,
            target_strategy=target,
            failure_reason=TRANSITION_TARGET_INVALID,
        )
    if target == str(current_strategy or "").strip():
        return StrategyTransitionResult(
            success=False,
            target_strategy=target,
            failure_reason=TRANSITION_NOT_REQUIRED,
        )

    forbidden = _disallowed_payload_key(option_payload)
    if forbidden:
        logger.info(
            "Strategy transition refused: disallowed payload key %r", forbidden
        )
        return StrategyTransitionResult(
            success=False,
            target_strategy=target,
            failure_reason=TRANSITION_PAYLOAD_FORBIDDEN,
        )
    options_updates: dict[str, Any] = dict(option_payload or {})
    axis_extra: dict[str, Any] = {}
    mode = str(legacy_operation_mode or "").strip()
    if mode:
        axis_extra["collector_operation_mode"] = mode
        options_updates["collector_operation_mode"] = mode

    session_id = str(claimed_session_id() or "").strip()
    if not session_id:
        # Preflight: no live claimed session -> the controlled restart (and
        # therefore any reconnect proof) is impossible. Nothing was touched.
        return StrategyTransitionResult(
            success=False,
            target_strategy=target,
            failure_reason=TRANSITION_SESSION_UNAVAILABLE,
        )

    if target == CONNECTION_STRATEGY_INBOUND:
        return await _async_transition_to_inbound(
            collector_pn=collector_pn,
            owner_id=owner_id,
            registry=registry,
            session_id=session_id,
            live_wire=live_wire,
            clock=clock,
            commit=commit,
            policy=policy,
            ledger=ledger,
            poll_interval=poll_interval,
            inbound_endpoint=inbound_endpoint,
            endpoint_needs_write=endpoint_needs_write,
            write_endpoint=write_endpoint,
            reboot=reboot,
            prepare_listener=prepare_listener,
            local_listener_port=local_listener_port,
            on_endpoint_written=on_endpoint_written,
            silent_session_probe=silent_session_probe,
            axis_extra=axis_extra,
            options_updates=options_updates,
        )
    return await _async_transition_to_callback(
        collector_pn=collector_pn,
        owner_id=owner_id,
        registry=registry,
        session_id=session_id,
        live_wire=live_wire,
        clock=clock,
        commit=commit,
        policy=policy,
        ledger=ledger,
        poll_interval=poll_interval,
        callback_route=callback_route,
        trigger_sender=trigger_sender,
        endpoint_control_policy=endpoint_control_policy,
        restore_endpoint=restore_endpoint,
        write_endpoint=write_endpoint,
        reboot=reboot,
        on_endpoint_restored=on_endpoint_restored,
        persist_pending=persist_pending,
        persist_confirmed=persist_confirmed,
        recovery_state=recovery_state,
        silent_session_probe=silent_session_probe,
        axis_extra=axis_extra,
        options_updates=options_updates,
    )


async def _async_transition_to_inbound(
    *,
    collector_pn: str,
    owner_id: str,
    registry: Any,
    session_id: str,
    live_wire: Callable[[], str],
    clock: Callable[[], str],
    commit: Callable[[dict[str, Any], RecoveryTerminalInput], Awaitable[str]],
    policy: OnboardingTimeoutPolicy,
    ledger: Any,
    poll_interval: float,
    inbound_endpoint: str,
    endpoint_needs_write: bool,
    write_endpoint: Callable[[str], Awaitable[Any]] | None,
    reboot: Callable[[], Awaitable[Any]] | None,
    prepare_listener: Callable[[int], Awaitable[None]] | None,
    local_listener_port: int,
    on_endpoint_written: Callable[[str], None] | None,
    silent_session_probe: Any,
    axis_extra: dict[str, Any],
    options_updates: dict[str, Any],
) -> StrategyTransitionResult:
    endpoint = str(inbound_endpoint or "").strip()
    if not endpoint or (endpoint_needs_write and write_endpoint is None):
        # The USER-CONFIRMED Home Assistant endpoint is mandatory input: it is
        # never derived from peer IP / hostname / local-vs-external guessing.
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            failure_reason=TRANSITION_ENDPOINT_REQUIRED,
        )
    if not endpoint_needs_write and reboot is None:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            failure_reason=TRANSITION_ENDPOINT_REQUIRED,
        )

    if prepare_listener is not None and int(local_listener_port or 0) > 0:
        # NAT split: the integration prepares its LOCAL listener bind port —
        # never the advertised/forwarded port the collector was told.
        await prepare_listener(int(local_listener_port))

    written: dict[str, str] = {}

    async def _restart() -> Any:
        if endpoint_needs_write:
            if write_endpoint is None:  # preflight guarantees this
                raise RuntimeError(TRANSITION_ENDPOINT_REQUIRED)
            result = await write_endpoint(endpoint)
            written["value"] = _applied_endpoint_from_result(result, endpoint)
            return result
        if reboot is None:  # preflight guarantees this
            raise RuntimeError(TRANSITION_ENDPOINT_REQUIRED)
        return await reboot()

    def _confirmed(_result: Any) -> None:
        # Fires only after the write/apply came back confirmed: the endpoint
        # provenance is earned NOW, independent of the verification outcome.
        if written and on_endpoint_written is not None:
            on_endpoint_written(written["value"])

    channel = _ManagedRestartChannel(
        restart=_restart, wire_provider=live_wire, on_confirmed=_confirmed
    )
    verifier = InboundRecoveryVerifier(
        collector_pn=collector_pn,
        session_id=session_id,
        restart_channel=channel,
        sessions_source=registry_sessions_projection(registry),
        clock=clock,
        policy=policy,
        ledger=ledger,
        poll_interval=poll_interval,
        promote_claim=_registry_promote_hook(registry, owner_id),
        retarget_claim=_registry_retarget_hook(registry, owner_id),
        silent_session_probe=silent_session_probe,
    )
    try:
        outcome = await verifier.async_verify()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # The restart callable (endpoint write) failing surfaces here; the
        # engine's own failures come back as typed outcomes instead.
        logger.info("Strategy transition to inbound failed on restart: %s", exc)
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            failure_reason=TRANSITION_ENDPOINT_WRITE_FAILED,
            endpoint_written=bool(written),
            endpoint_written_value=written.get("value", ""),
        )

    if not outcome.inbound_verified:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            failure_reason=str(outcome.failure_reason or "")
            or "inbound_reconnect_timeout",
            endpoint_written=bool(written),
            endpoint_written_value=written.get("value", ""),
            outcome=outcome,
        )

    terminal = RecoveryTerminalInput.from_inbound_outcome(outcome)
    updates: dict[str, Any] = {
        CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_INBOUND
    }
    updates.update(axis_extra)
    if written:
        # The write really happened: the integration now manages the endpoint
        # and the durable data mirrors the already-persisted provenance.
        updates[CONF_ENDPOINT_CONTROL_POLICY] = ENDPOINT_CONTROL_INTEGRATION_MANAGED
        updates[CONF_ENDPOINT_WRITTEN_VALUE] = written["value"]
        updates[CONF_ENDPOINT_WRITTEN_AT] = clock()
    # No write: policy is deliberately NOT touched — external stays external
    # (integration_managed can never be claimed retroactively), and a prior
    # honest integration_managed record stays what it is.
    refusal = await commit(updates, terminal, options_updates)
    if refusal:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_INBOUND,
            failure_reason=refusal,
            endpoint_written=bool(written),
            endpoint_written_value=written.get("value", ""),
            outcome=outcome,
        )
    return StrategyTransitionResult(
        success=True,
        target_strategy=CONNECTION_STRATEGY_INBOUND,
        endpoint_written=bool(written),
        endpoint_written_value=written.get("value", ""),
        outcome=outcome,
    )


async def _async_transition_to_callback(
    *,
    collector_pn: str,
    owner_id: str,
    registry: Any,
    session_id: str,
    live_wire: Callable[[], str],
    clock: Callable[[], str],
    commit: Callable[[dict[str, Any], RecoveryTerminalInput], Awaitable[str]],
    policy: OnboardingTimeoutPolicy,
    ledger: Any,
    poll_interval: float,
    callback_route: CallbackRecoveryRoute | None,
    trigger_sender: Any,
    endpoint_control_policy: str,
    restore_endpoint: str,
    write_endpoint: Callable[[str], Awaitable[Any]] | None,
    reboot: Callable[[], Awaitable[Any]] | None,
    on_endpoint_restored: Callable[[str], None] | None,
    persist_pending: Callable[[StrategyTransitionRecoveryState], Any] | None,
    persist_confirmed: Callable[[StrategyTransitionRecoveryState], Any] | None,
    recovery_state: StrategyTransitionRecoveryState | None,
    silent_session_probe: Any,
    axis_extra: dict[str, Any],
    options_updates: dict[str, Any],
) -> StrategyTransitionResult:
    if callback_route is None or callback_route.invalid_reason():
        # Explicit trigger target + advertised HA endpoint are mandatory
        # inputs (typed "input required") — never a heuristic.
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_CALLBACK_ROUTE_REQUIRED,
        )

    needs_restore = (
        str(endpoint_control_policy or "").strip()
        == ENDPOINT_CONTROL_INTEGRATION_MANAGED
    )
    restore_target = str(restore_endpoint or "").strip()
    if needs_restore and (not restore_target or write_endpoint is None):
        # integration_managed means WE pointed the collector here; handing
        # control back requires the REALLY SAVED previous endpoint. Guessing a
        # vendor/cloud endpoint by hostname/provider/kind is forbidden.
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_ROLLBACK_ENDPOINT_UNAVAILABLE,
        )
    if not needs_restore and reboot is None:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_CALLBACK_ROUTE_REQUIRED,
        )

    # --- TYPED recovery-state trust boundary (Blocker 2) ------------------
    # A callback transition mutates the collector; before ANY side effect the
    # write-ahead intent must be a valid typed capability. This runs for BOTH
    # the integration-managed restore AND the already-external reboot path.
    # Every check is a pure comparison (no writes, no UDP, no reboot): a dict /
    # SimpleNamespace / duck-typed object / foreign-PN / foreign-route /
    # non-callback-target / non-pending state is refused HERE, so no endpoint
    # write, no reboot, no trigger and no commit can ever run behind an
    # untrusted or mismatched state.
    if type(recovery_state) is not StrategyTransitionRecoveryState:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=(
                TRANSITION_RECOVERY_STATE_UNAVAILABLE
                if recovery_state is None
                else TRANSITION_RECOVERY_STATE_INVALID
            ),
        )
    if (
        recovery_state.collector_pn != normalize_pn(collector_pn)
        or recovery_state.target_strategy != CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        or recovery_state.phase != RECOVERY_PHASE_PENDING
        or recovery_state.callback_route() != callback_route
    ):
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_RECOVERY_STATE_INVALID,
        )

    # --- CONFIRMED-restore hook must exist BEFORE write-ahead (Blocker 4) --
    # An integration-managed restore WILL confirm the endpoint external and MUST
    # then persist the confirmed-unproven phase in one local write. If the
    # facade supplied no ``persist_confirmed`` hook, that write could never
    # happen -- so the restore is refused HERE, before ``persist_pending`` and
    # before any endpoint write / reboot / UDP / commit, leaving entry data
    # byte-for-byte unchanged. The already-external reboot path needs no such
    # hook: its persistent endpoint never changes, so there is nothing to
    # confirm.
    if needs_restore and persist_confirmed is None:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_PERSIST_CONFIRMED_UNAVAILABLE,
        )

    # --- WRITE-AHEAD durable intent (Blockers 3 & 4) ----------------------
    # The pending state is persisted BEFORE the first destructive side effect
    # (endpoint restore OR reboot OR the verifier's UDP trigger), keeping the
    # OLD strategy/policy/provenance. The AUTHORITY owns this order; the facade
    # only supplies the physical write. If it is missing, refuses or raises, no
    # collector side effect follows and the caller's lease is released by its
    # own finally when we return. A crash after this point leaves a repairable
    # typed pending state, never a silent strategy/policy flip.
    if persist_pending is None:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_RECOVERY_STATE_UNAVAILABLE,
        )
    try:
        pending_refusal = persist_pending(recovery_state)
    except Exception as exc:
        logger.info("Write-ahead recovery-state persist failed: %s", exc)
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_PERSIST_PENDING_FAILED,
        )
    if pending_refusal:
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_PERSIST_PENDING_FAILED,
        )

    restored: dict[str, str] = {}

    def _degraded_marker() -> str:
        """The UI/result marker after a CONFIRMED restore with no proven
        callback strategy. The DURABLE confirmed-unproven state was already
        written by ``persist_confirmed`` at the restore boundary (ONE write);
        this only labels the result so the runtime never shows plain inbound
        health for an endpoint that is already external."""

        return DEGRADED_CALLBACK_RESTORE_UNPROVEN if restored else ""

    async def _restart() -> Any:
        if needs_restore:
            if write_endpoint is None:  # preflight guarantees this
                raise RuntimeError(TRANSITION_ROLLBACK_ENDPOINT_UNAVAILABLE)
            result = await write_endpoint(restore_target)
            restored["value"] = _applied_endpoint_from_result(
                result, restore_target
            )
            return result
        if reboot is None:  # preflight guarantees this
            raise RuntimeError(TRANSITION_CALLBACK_ROUTE_REQUIRED)
        return await reboot()

    def _confirmed(_result: Any) -> None:
        # The restore is CONFIRMED: control is honestly external again (and
        # stays that way even if the later proof fails). ONE durable local
        # write advances the recovery state to the confirmed-unproven phase,
        # flips the policy to external and clears the write provenance; the
        # separate snapshot hook is UI-only.
        if not restored:
            return
        if persist_confirmed is not None:
            confirmed_state = recovery_state.with_phase(
                RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=clock()
            )
            persist_confirmed(confirmed_state)
        if on_endpoint_restored is not None:
            on_endpoint_restored(restored["value"])

    channel = _ManagedRestartChannel(
        restart=_restart, wire_provider=live_wire, on_confirmed=_confirmed
    )

    def _certify_permanent_owner(_full_pn: str) -> Any:
        # The HONEST permanent-owner capability (never a fake onboarding
        # handoff): the registry ISSUES a typed
        # ``PermanentOwnedSessionCertification`` for this owner's claim on the
        # EXACT retargeted live strong-identity session of its own durable PN.
        # None = engine-level typed refusal.
        current_sid = str(registry.claimed_session_id(owner_id) or "")
        return registry.certify_permanent_owned_session(owner_id, current_sid)

    verifier = CallbackRecoveryVerifier(
        route=callback_route,
        trigger_sender=trigger_sender,
        collector_pn=collector_pn,
        session_id=session_id,
        restart_channel=channel,
        sessions_source=registry_sessions_projection(registry),
        clock=clock,
        policy=policy,
        ledger=ledger,
        poll_interval=poll_interval,
        promote_claim=_registry_promote_hook(registry, owner_id),
        retarget_claim=_registry_retarget_hook(registry, owner_id),
        owner_certifier=_certify_permanent_owner,
        silent_session_probe=silent_session_probe,
    )

    def _certification_reverified(outcome: Any) -> bool:
        # The registry re-verifies the capability against LIVE state right
        # before the merge/commit: a certification that went stale between the
        # engine's success and now (claim retargeted away, socket closed) is
        # rejected instead of committing a strategy on a dead capability.
        return bool(
            registry.reverify_permanent_owned_session(
                getattr(outcome, "owner_certification", None)
            )
        )
    try:
        outcome = await verifier.async_verify()
    except asyncio.CancelledError:
        # Cancelled AFTER the restore may already have confirmed: the durable
        # state is already intact -- ``persist_confirmed`` wrote the
        # confirmed-unproven phase at the restore boundary, and a still-pending
        # attempt keeps its write-ahead pending state. Nothing is deleted; just
        # propagate the cancellation unchanged (never an ordinary failure).
        raise
    except Exception as exc:
        logger.info("Strategy transition to callback failed on restart: %s", exc)
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=TRANSITION_ENDPOINT_WRITE_FAILED,
            endpoint_restored=bool(restored),
            degraded_state=_degraded_marker(),
        )

    if outcome.callback_verified:
        if not _certification_reverified(outcome):
            # The capability went stale between success and commit: refuse
            # rather than persist a strategy on a dead certification. The
            # confirmed restore is still a fact -> degraded marker stays.
            return StrategyTransitionResult(
                success=False,
                target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                failure_reason=TRANSITION_OWNER_CERTIFICATION_STALE,
                endpoint_restored=bool(restored),
                degraded_state=_degraded_marker(),
                outcome=outcome,
            )
        terminal = RecoveryTerminalInput.from_permanent_owner_transaction(outcome)
        updates: dict[str, Any] = {
            CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        }
        updates.update(axis_extra)
        if restored:
            updates[CONF_ENDPOINT_CONTROL_POLICY] = ENDPOINT_CONTROL_EXTERNAL
        refusal = await commit(updates, terminal, options_updates)
        if refusal:
            return StrategyTransitionResult(
                success=False,
                target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                failure_reason=refusal,
                endpoint_restored=bool(restored),
                degraded_state=_degraded_marker(),
                outcome=outcome,
            )
        return StrategyTransitionResult(
            success=True,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_restored=bool(restored),
            outcome=outcome,
        )

    if outcome.inbound_recovered:
        # The collector came back on its own: callback was NOT proven, the
        # strategy stays what it was — but the inbound proof was really
        # earned, so it is merged as a fact (no axis changes). A merge
        # refusal is a TERMINAL failure: claiming "the proof was kept" when
        # the persistence boundary refused would be a lie.
        if not _certification_reverified(outcome):
            return StrategyTransitionResult(
                success=False,
                target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                failure_reason=TRANSITION_OWNER_CERTIFICATION_STALE,
                endpoint_restored=bool(restored),
                degraded_state=_degraded_marker(),
                outcome=outcome,
            )
        terminal = RecoveryTerminalInput.from_permanent_owner_transaction(outcome)
        refusal = await commit({}, terminal, {})
        if refusal:
            return StrategyTransitionResult(
                success=False,
                target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                failure_reason=refusal,
                endpoint_restored=bool(restored),
                outcome=outcome,
            )
        # The collector DEMONSTRABLY reconnected, so no degraded marker; but
        # after a confirmed restore the user must never be told the endpoint
        # "still points at Home Assistant" — it does not.
        return StrategyTransitionResult(
            success=False,
            target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            failure_reason=(
                TRANSITION_INBOUND_RECOVERED_AFTER_RESTORE
                if restored
                else TRANSITION_INBOUND_RECOVERED_INSTEAD
            ),
            endpoint_restored=bool(restored),
            outcome=outcome,
        )

    return StrategyTransitionResult(
        success=False,
        target_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        failure_reason=str(outcome.failure_reason or "")
        or "callback_recovery_timeout",
        endpoint_restored=bool(restored),
        degraded_state=_degraded_marker(),
        outcome=outcome,
    )


__all__ = [
    "DEGRADED_CALLBACK_RESTORE_UNPROVEN",
    "StrategyTransitionLease",
    "StrategyTransitionResult",
    "TRANSITION_ALREADY_RUNNING",
    "TRANSITION_CALLBACK_ROUTE_REQUIRED",
    "TRANSITION_ENDPOINT_REQUIRED",
    "TRANSITION_ENDPOINT_WRITE_FAILED",
    "TRANSITION_INBOUND_RECOVERED_AFTER_RESTORE",
    "TRANSITION_INBOUND_RECOVERED_INSTEAD",
    "TRANSITION_NOT_REQUIRED",
    "TRANSITION_PAYLOAD_FORBIDDEN",
    "TRANSITION_PERSIST_CONFIRMED_UNAVAILABLE",
    "TRANSITION_PERSIST_PENDING_FAILED",
    "TRANSITION_RECOVERY_STATE_INVALID",
    "TRANSITION_RECOVERY_STATE_UNAVAILABLE",
    "TRANSITION_ROLLBACK_ENDPOINT_UNAVAILABLE",
    "TRANSITION_SESSION_UNAVAILABLE",
    "TRANSITION_TARGET_INVALID",
    "async_run_strategy_transition",
    "trusted_transition_wire",
]
