"""The ONE neutral authority for exclusive collector-configuration mutation.

Every production path that writes persistent collector configuration or changes
its connection lifecycle -- endpoint transitions/restores, proxy/shadow routes,
Wi-Fi/UART writes, apply/reboot, and the automatic runtime UART sweep -- must own
this per-entry authority for the whole transaction. It ONLY coordinates: it
never writes the wire, changes the strategy, touches the RecoveryContract, or
inspects endpoint/IP/credentials.

Design (fail-closed, no wait, no queue, no ContextVar, no background release):

* a CLOSED set of operation kinds;
* a strict immutable ``CollectorEndpointOperationToken`` (entry_id + kind + a
  reconstructible ``owner_ref`` string);
* ``acquire`` is a synchronous check-and-set -- atomic between two awaits, so a
  caller acquires BEFORE its first persistence/wire side effect;
* a busy acquire returns the ACTIVE operation kind only (never an address);
* ``release`` frees ONLY on an exact-value token match -- a foreign / duck /
  stale token frees nothing;
* ``adopt`` re-establishes ownership for a long-lived mode after a reload/restart
  from its persisted ``owner_ref`` without ever stealing a different owner;
* different config entries never contend (per-entry keys, no global lock).
"""

from __future__ import annotations

from dataclasses import dataclass

# ---- closed operation-kind vocabulary ----------------------------------------
OPERATION_STRATEGY_TRANSITION = "strategy_transition"
OPERATION_STRATEGY_REPAIR = "strategy_repair"
OPERATION_PROXY_CAPTURE = "proxy_capture"
OPERATION_SHADOW_LEARNING = "shadow_learning"
OPERATION_MANUAL_ENDPOINT_WRITE = "manual_endpoint_write"
OPERATION_ENDPOINT_BIND = "endpoint_bind"
OPERATION_ENDPOINT_ROLLBACK = "endpoint_rollback"
# The automatic operation-mode endpoint reconcile (best-effort; SILENTLY skips
# its write when another operation owns the entry, never breaking the refresh).
OPERATION_RECONCILE_ENDPOINT = "reconcile_endpoint"
# The public Full Control collector system actions (apply / reboot / rediscovery
# trigger). They typed-refuse BEFORE the apply/reboot/UDP when the entry is busy.
OPERATION_COLLECTOR_SYSTEM_ACTION = "collector_system_action"
# Runtime full detection may temporarily rewrite the collector UART speed and
# later restore it.  That is a collector-configuration mutation just like an
# explicit UART write, so it participates in the same per-entry authority for
# the WHOLE read/change/probe/restore transaction.
OPERATION_RUNTIME_LINK_BAUD_SWEEP = "runtime_link_baud_sweep"

_OPERATION_KINDS = frozenset(
    {
        OPERATION_STRATEGY_TRANSITION,
        OPERATION_STRATEGY_REPAIR,
        OPERATION_PROXY_CAPTURE,
        OPERATION_SHADOW_LEARNING,
        OPERATION_MANUAL_ENDPOINT_WRITE,
        OPERATION_ENDPOINT_BIND,
        OPERATION_ENDPOINT_ROLLBACK,
        OPERATION_RECONCILE_ENDPOINT,
        OPERATION_COLLECTOR_SYSTEM_ACTION,
        OPERATION_RUNTIME_LINK_BAUD_SWEEP,
    }
)

# The ONE typed busy reason surfaced to any conflicting caller.
COLLECTOR_ENDPOINT_OPERATION_BUSY = "collector_endpoint_operation_busy"


def _exact_normalized_str(value: object) -> str | None:
    """Return an EXACT, non-empty, already-normalized ``str`` or ``None``.

    Trust boundary: a bool / int / duck / padded / empty value never passes and
    is NEVER coerced (no ``str()``), so it can never create or match an owner.
    """

    if type(value) is not str:
        return None
    if not value or value != value.strip():
        return None
    return value


@dataclass(frozen=True, slots=True)
class CollectorEndpointOperationToken:
    """An immutable proof of exclusive endpoint-operation ownership for one entry.

    ``owner_ref`` is a non-empty string chosen by the owner. A long-lived mode
    passes its persisted route owner id so the SAME token can be reconstructed
    after a reload/restart; a transient op lets the authority generate one.
    """

    entry_id: str
    operation_kind: str
    owner_ref: str

    def __post_init__(self) -> None:
        # STRICT: exact, non-empty, already-normalized strings only -- never a
        # coerced / padded / duck value. A malformed token cannot exist.
        if _exact_normalized_str(self.entry_id) is None:
            raise ValueError("collector_endpoint_operation_entry_invalid")
        if (
            type(self.operation_kind) is not str
            or self.operation_kind not in _OPERATION_KINDS
        ):
            raise ValueError("collector_endpoint_operation_kind_invalid")
        if _exact_normalized_str(self.owner_ref) is None:
            raise ValueError("collector_endpoint_operation_owner_ref_invalid")


@dataclass(frozen=True, slots=True)
class CollectorEndpointOperationAcquire:
    """Result of one acquire attempt: a token, or the busy operation kind."""

    token: CollectorEndpointOperationToken | None
    busy_operation: str = ""

    @property
    def acquired(self) -> bool:
        return self.token is not None


class CollectorEndpointOperationAuthority:
    """Per-entry exclusive owner of endpoint/route mutation. Process singleton."""

    def __init__(self) -> None:
        self._held: dict[str, CollectorEndpointOperationToken] = {}
        self._counter = 0

    def acquire(
        self,
        entry_id: object,
        operation_kind: str,
        *,
        owner_ref: str = "",
    ) -> CollectorEndpointOperationAcquire:
        """Fail-closed check-and-set. Never waits; never queues.

        Returns an ``acquired`` result with a token when the entry is free, else a
        busy result carrying the ACTIVE operation kind (no address/credentials).
        An invalid operation kind is a programming error and raises.
        """

        # The kind is always a module constant -> an invalid one (or a duck whose
        # __eq__/__hash__ collides with a member) is a programming error, raised
        # before any state change. The exact-type check runs BEFORE the membership
        # lookup so a non-str can never match via a frozenset hash collision.
        if type(operation_kind) is not str or operation_kind not in _OPERATION_KINDS:
            raise ValueError("collector_endpoint_operation_kind_invalid")
        # entry_id is a trust boundary: a bool/int/duck/padded value fails closed
        # (no owner created) instead of being coerced into a key.
        key = _exact_normalized_str(entry_id)
        if key is None:
            return CollectorEndpointOperationAcquire(token=None, busy_operation="")
        # owner_ref auto-generation is allowed ONLY for the EXACT empty string --
        # never a duck whose __eq__("") is True, nor bytes/None/int/bool. Anything
        # else must be an exact normalized string, else fail closed (no owner).
        if type(owner_ref) is str and owner_ref == "":
            ref = self._generate_owner_ref()
        else:
            ref = _exact_normalized_str(owner_ref)
            if ref is None:
                return CollectorEndpointOperationAcquire(token=None, busy_operation="")
        held = self._held.get(key)
        if held is not None:
            return CollectorEndpointOperationAcquire(
                token=None, busy_operation=held.operation_kind
            )
        token = CollectorEndpointOperationToken(
            entry_id=key,
            operation_kind=operation_kind,
            owner_ref=ref,
        )
        self._held[key] = token
        return CollectorEndpointOperationAcquire(token=token, busy_operation="")

    def adopt(
        self,
        entry_id: object,
        operation_kind: str,
        owner_ref: str,
    ) -> CollectorEndpointOperationToken | None:
        """Re-establish ownership of a long-lived mode from its persisted owner ref.

        For a reload/restart of a still-active mode: if the entry is free the token
        is re-acquired; if it is already held by the SAME (kind, owner_ref) the
        existing token is returned. A different owner is never stolen (``None``).
        """

        if type(operation_kind) is not str or operation_kind not in _OPERATION_KINDS:
            raise ValueError("collector_endpoint_operation_kind_invalid")
        # Fail-closed: a malformed entry_id or owner_ref cannot prove ownership,
        # so it never adopts or re-acquires anything (no coercion).
        key = _exact_normalized_str(entry_id)
        ref = _exact_normalized_str(owner_ref)
        if key is None or ref is None:
            return None
        reconstructed = CollectorEndpointOperationToken(
            entry_id=key, operation_kind=operation_kind, owner_ref=ref
        )
        held = self._held.get(key)
        if held is None:
            self._held[key] = reconstructed
            return reconstructed
        if held == reconstructed:
            return held
        return None

    def release(self, entry_id: object, token: object) -> bool:
        """Free the entry ONLY on an exact-value token match. Idempotent-safe.

        A foreign / duck / stale / ``None`` token frees nothing (returns False),
        so a ``finally`` release after cancellation can never drop another owner.
        """

        if type(token) is not CollectorEndpointOperationToken:
            return False
        key = _exact_normalized_str(entry_id)
        if key is None:
            return False
        held = self._held.get(key)
        if held is None or held != token:
            return False
        del self._held[key]
        return True

    def active_operation(self, entry_id: object) -> str:
        """The active operation kind for one entry, or ``""`` when free (read-only)."""

        key = _exact_normalized_str(entry_id)
        if key is None:
            return ""
        held = self._held.get(key)
        return held.operation_kind if held is not None else ""

    def is_held(self, entry_id: object) -> bool:
        key = _exact_normalized_str(entry_id)
        return key is not None and key in self._held

    def _generate_owner_ref(self) -> str:
        self._counter += 1
        return f"auto:{self._counter}"


# THE one production authority. Module-level so a config-entry reload cannot
# orphan ownership held by a stale coordinator instance, and so production and
# tests exercise the SAME instance.
COLLECTOR_ENDPOINT_OPERATION_AUTHORITY = CollectorEndpointOperationAuthority()


__all__ = [
    "COLLECTOR_ENDPOINT_OPERATION_AUTHORITY",
    "COLLECTOR_ENDPOINT_OPERATION_BUSY",
    "CollectorEndpointOperationAcquire",
    "CollectorEndpointOperationAuthority",
    "CollectorEndpointOperationToken",
    "OPERATION_COLLECTOR_SYSTEM_ACTION",
    "OPERATION_ENDPOINT_BIND",
    "OPERATION_ENDPOINT_ROLLBACK",
    "OPERATION_MANUAL_ENDPOINT_WRITE",
    "OPERATION_PROXY_CAPTURE",
    "OPERATION_RECONCILE_ENDPOINT",
    "OPERATION_RUNTIME_LINK_BAUD_SWEEP",
    "OPERATION_SHADOW_LEARNING",
    "OPERATION_STRATEGY_REPAIR",
    "OPERATION_STRATEGY_TRANSITION",
]
